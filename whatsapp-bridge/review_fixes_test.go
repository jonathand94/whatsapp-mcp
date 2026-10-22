package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func seedDraft(t *testing.T, s *Server, id, status string, createdAt int64, filePath string) {
	t.Helper()
	var whatsappID, sentAt, errMsg any
	switch status {
	case "sent":
		whatsappID, sentAt = "3EB0SENT", int64(1792557300)
	case "failed":
		errMsg = "socket closed"
	}
	if _, err := s.db.Exec(`
		INSERT INTO sends (draft_id, recipient_jid, recipient_display, send_type, content_text,
		                   content_file_path, status, created_at, whatsapp_message_id, sent_at, error_message)
		VALUES (?, '5215555555555@s.whatsapp.net', 'Juan', 'file', '', ?, ?, ?, ?, ?, ?)`,
		id, nullString(filePath), status, createdAt, whatsappID, sentAt, errMsg,
	); err != nil {
		t.Fatalf("seeding draft %s: %v", id, err)
	}
}

func confirm(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/sends/"+id+"/confirm?wait=0", nil)
	req.SetPathValue("draft_id", id)
	rec := httptest.NewRecorder()
	s.handleConfirmSend(rec, req)
	return rec
}

// TestConfirmIsIdempotent pins the review's top finding: a large send answers
// 202, the caller confirms again to learn the outcome, and the old answer for
// an already-sent draft — 409 "create a new draft to retry" — is exactly what
// leads to sending the same file twice.
func TestConfirmIsIdempotent(t *testing.T) {
	s, _ := newMediaTestServer(t)
	now := time.Now().Unix()
	seedDraft(t, s, "sent-1", "sent", now, "")
	seedDraft(t, s, "failed-1", "failed", now, "")
	seedDraft(t, s, "inflight-1", "confirmed", now, "")

	rec := confirm(t, s, "sent-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("re-confirming a sent draft: %d %s", rec.Code, rec.Body)
	}
	var got confirmResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "sent" || got.WhatsAppMessageID != "3EB0SENT" {
		t.Fatalf("got %+v, want the original delivery", got)
	}

	if rec := confirm(t, s, "failed-1"); rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "socket closed") {
		t.Fatalf("a failed draft should say why: %d %s", rec.Code, rec.Body)
	}
	if rec := confirm(t, s, "inflight-1"); rec.Code != http.StatusConflict ||
		!strings.Contains(rec.Body.String(), "Revisa el chat") {
		t.Fatalf("a confirmed draft should send the user to check the chat: %d %s", rec.Code, rec.Body)
	}
}

// TestConfirmThatLostTheRaceAnswersWithTheDelivery: two confirms of one
// draft, the second waiting for its moment while the first sends. The second
// finds the draft already sent and must say so, not report a conflict that
// reads as "not sent".
func TestConfirmThatLostTheRaceAnswersWithTheDelivery(t *testing.T) {
	s, _ := newMediaTestServer(t)
	seedDraft(t, s, "raced-1", "sent", time.Now().Unix(), "")

	g := s.sendGov()
	ticket := &sendTicket{gov: g, decision: g.Authorize("chat-a", false, time.Now())}
	release, err := g.ClaimSend(t.Context(), "chat-a", -1)
	if err != nil {
		t.Fatal(err)
	}
	ticket.release = release
	code, body := s.commitSend(t.Context(), "raced-1", types.JID{}, "5215555555555@s.whatsapp.net", "Juan", "", nil, ticket)
	got, ok := body.(confirmResponse)
	if code != http.StatusOK || !ok || got.WhatsAppMessageID != "3EB0SENT" {
		t.Fatalf("got %d %+v, want the first confirm's delivery", code, body)
	}
	// It sent nothing: its turn passed on and its count was given back.
	if n := lineLength(g, "chat-a"); n != 0 {
		t.Fatalf("the losing confirm still holds the line (%d)", n)
	}
	if sent := g.Snapshot(time.Now())["sent_last_hour"].(int); sent != 0 {
		t.Fatalf("the losing confirm is still counted: %d", sent)
	}
}

// TestTextHeldBackByALongLineSaysWhenToRetry: a text refused by the chat's
// line says when to confirm again in the details, the only part the MCP
// layer passes on.
func TestTextHeldBackByALongLineSaysWhenToRetry(t *testing.T) {
	full := &sendQueueFull{Ahead: 3, MaxWait: 10 * time.Second, Gap: 5 * time.Second}
	got := rateLimited(full.Error(), full.RetryAfter(), 50*time.Minute)
	if !strings.HasPrefix(got.Details, "3 message(s) to this chat are ahead of this one") ||
		!strings.HasSuffix(got.Details, "; confirm it again in 5s.") || got.RetryAfterMS != 5000 {
		t.Fatalf("got %+v", got)
	}
}

func TestRateLimitedSaysWhenToConfirmAgain(t *testing.T) {
	for _, c := range []struct {
		retry, left time.Duration
		want        string
	}{
		{9 * time.Second, 50 * time.Minute, "; confirm it again in 9s."},
		{0, 50 * time.Minute, " and can be confirmed again later."},
		{17 * time.Hour, 50 * time.Minute, ", but it expires before then: create a new one in 17h0m."},
		{3*time.Hour + 12*time.Minute + 20*time.Second, 0, "; confirm it again in 3h12m."},
	} {
		got := rateLimited("limit", c.retry, c.left)
		if got.Details != "limit. The draft was not consumed"+c.want || got.RetryAfterMS != c.retry.Milliseconds() {
			t.Errorf("retry %s, draft left %s: %+v", c.retry, c.left, got)
		}
	}
}

func TestConfirmReturnsTheFinishedUploadsAnswer(t *testing.T) {
	s, _ := newMediaTestServer(t)
	j, _ := s.jobs().start("send:up-done", func(j *mediaJob) (int, any) {
		return http.StatusOK, confirmResponse{DraftID: "up-done", Status: "sent", WhatsAppMessageID: "3EB0X"}
	})
	j.wait(t.Context(), time.Second)

	rec := confirm(t, s, "up-done")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "3EB0X") {
		t.Fatalf("confirming after the upload finished: %d %s", rec.Code, rec.Body)
	}
}

func TestConfirmExplainsWhyPreparationFailed(t *testing.T) {
	s, _ := newMediaTestServer(t)
	j, _ := s.jobs().start("draft:prep-bad", func(j *mediaJob) (int, any) {
		return http.StatusBadRequest, errorResponse{Error: "media rejected", Details: "file_url respondio 403 Forbidden"}
	})
	j.wait(t.Context(), time.Second)

	rec := confirm(t, s, "prep-bad")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "403 Forbidden") {
		t.Fatalf("want the preparation's own error instead of 'not found': %d %s", rec.Code, rec.Body)
	}
}

func TestGrowingFileCountsEveryWrite(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "x.part"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var n atomic.Int64
	g := growingFile{File: f, written: &n}
	g.Write([]byte("abc"))
	g.WriteAt([]byte("ab"), 0) // a retry rewriting the start: no new size, still activity
	g.ReadFrom(strings.NewReader("defg"))
	if n.Load() != 9 {
		t.Fatalf("written = %d, want 9", n.Load())
	}
	if st, _ := f.Stat(); st.Size() != 7 {
		t.Fatalf("size = %d, want 7", st.Size())
	}
}

func TestFileURLStallIsCaughtBeforeTheFirstByte(t *testing.T) {
	old := stallTimeout
	stallTimeout = 300 * time.Millisecond
	defer func() { stallTimeout = old }()

	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release // headers, then nothing
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	_, _, err := fetchToMemory(t.Context(), srv.URL+"/slow.pdf", 1<<20, testFetcher(t, srv))
	if err == nil || !strings.Contains(err.Error(), "se detuvo") {
		t.Fatalf("want a stall error, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("the stall took %s to notice", time.Since(start))
	}
}

func TestMaterializeRefusesNonRegularFiles(t *testing.T) {
	cfg := testConfig(t)
	_, err := materializeOutboundFile(t.Context(), cfg, "d", createDraftRequest{
		SendType: "file", FilePath: os.DevNull,
	})
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("a device file must be refused, got %v", err)
	}
}

func TestDownloadErrorStatusSeparatesGoneFromUnreachable(t *testing.T) {
	gone := fmt.Errorf("%w (x), y %v", errMediaGone, errNotOnPhone)
	busy := fmt.Errorf("%w (x): el telefono no respondio", errPhoneUnreachable)
	if downloadErrorStatus(gone) != http.StatusGone {
		t.Fatal("a file the phone no longer has is gone for good: 410")
	}
	if downloadErrorStatus(busy) != http.StatusServiceUnavailable {
		t.Fatal("a phone that did not answer may answer later: 503")
	}
	if !strings.Contains(downloadErrorBody(busy).Details, "Vuelve a intentarlo") {
		t.Fatal("the 503 should invite a retry")
	}
	if errors.Is(busy, errMediaGone) {
		t.Fatal("the two must not be confused")
	}
}

func TestSweepExpiresAbandonedDraftsButNotRunningUploads(t *testing.T) {
	s, db := newMediaTestServer(t)
	dir := outboundDir(s.cfg)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	old := time.Now().Unix() - draftTTLSeconds - 60
	abandoned, uploading, fresh := file("old.pdf"), file("busy.mp4"), file("new.pdf")
	seedDraft(t, s, "old", "draft", old, abandoned)
	seedDraft(t, s, "busy", "draft", old, uploading)
	seedDraft(t, s, "new", "draft", time.Now().Unix(), fresh)

	release := make(chan struct{})
	defer close(release)
	s.jobs().start("send:busy", func(j *mediaJob) (int, any) { <-release; return 200, nil })

	s.sweepOutbound(t.Context())

	status := func(id string) (st string) {
		db.QueryRow(`SELECT status FROM sends WHERE draft_id = ?`, id).Scan(&st)
		return
	}
	if status("old") != "expired" {
		t.Fatalf("abandoned draft status = %q, want expired", status("old"))
	}
	if _, err := os.Stat(abandoned); !os.IsNotExist(err) {
		t.Fatal("the abandoned draft's file should be deleted")
	}
	if status("busy") != "draft" || status("new") != "draft" {
		t.Fatalf("busy=%q new=%q, both should stay drafts", status("busy"), status("new"))
	}
	for _, p := range []string{uploading, fresh} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s should be kept: %v", p, err)
		}
	}
}

func TestRemoveOrphansClearsThePreviousRunsScratch(t *testing.T) {
	s, _ := newMediaTestServer(t)
	dir := outboundDir(s.cfg)
	os.MkdirAll(dir, 0o700)
	orphans := []string{
		filepath.Join(dir, "d1.part"),
		filepath.Join(dir, ".upload-123.enc"),
		filepath.Join(dir, "d2.voice.ogg"),
		filepath.Join(s.cfg.MediaPath, "3EB0.mp4.part"),
	}
	keep := []string{filepath.Join(dir, "d3.pdf"), filepath.Join(s.cfg.MediaPath, "3EB0.mp4")}
	for _, p := range append(orphans, keep...) {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s.removeOrphans()
	for _, p := range orphans {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should be gone", p)
		}
	}
	for _, p := range keep {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s should be kept", p)
		}
	}
}

// TestOwnChatIsNeverNew pins the 2026-10-21 bug: the user's own chat has no
// inbound messages by construction, so every send to it counted as opening a
// conversation with a stranger, by either of its JIDs.
func TestOwnChatIsNeverNew(t *testing.T) {
	s, db := newMediaTestServer(t)
	own := types.NewJID("5215550175084", types.DefaultUserServer)
	ownDevice := own
	ownDevice.Device = 62
	ownLID := types.NewJID("99990069399676", types.HiddenUserServer)
	s.bridge.client = &whatsmeow.Client{Store: &store.Device{ID: &ownDevice, LID: ownLID}}
	if _, err := db.Exec(`INSERT INTO jid_aliases (jid_a, jid_b, discovered_at, source) VALUES (?, ?, 1, 't'), (?, ?, 1, 't')`,
		own.String(), ownLID.String(), ownLID.String(), own.String()); err != nil {
		t.Fatal(err)
	}

	keys := map[string]bool{}
	for _, jid := range []string{own.String(), ownLID.String()} {
		key, isNew, err := s.classifyRecipient(t.Context(), jid)
		if err != nil {
			t.Fatal(err)
		}
		if isNew {
			t.Fatalf("%s: the user's own chat was classified as a new conversation", jid)
		}
		keys[key] = true
	}
	if len(keys) != 1 {
		t.Fatalf("the phone and @lid forms of one chat got different keys: %v", keys)
	}

	// A real stranger still counts as new.
	if _, isNew, _ := s.classifyRecipient(t.Context(), "5215550000001@s.whatsapp.net"); !isNew {
		t.Fatal("a chat with no inbound messages must still be new")
	}
}

func TestChatSortTimeClampsTheFuture(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	if got := chatSortTime(2359569747, now); got != now.Unix() {
		t.Fatalf("a 2044 timestamp sorted the chat at %d, want now", got)
	}
	if skew := now.Add(time.Hour).Unix(); chatSortTime(skew, now) != skew {
		t.Fatal("an hour of clock skew is not the future; it must be kept")
	}
	if chatSortTime(1784309593, now) != 1784309593 {
		t.Fatal("a past timestamp must be kept as is")
	}
}

// TestMigration008RepairsChatsFromTheFuture: the chat pinned at the top by an
// image dated 2044 goes back to the time of its newest believable message,
// and the message itself is left alone.
// TestHistoryLeavesFutureDatesOutOfTheChatList: in history a message dated
// in the future is a bad timestamp, not news; clamping it to "now" sent a
// years-old chat to the top of the list on every resync. Messages the live
// path keeps out of previews stay out here too.
func TestHistoryLeavesFutureDatesOutOfTheChatList(t *testing.T) {
	now := time.Unix(1792557000, 0)
	hsID := func(id string, ts int64, m *waE2E.Message) *waHistorySync.HistorySyncMsg {
		return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
			Key:              &waCommon.MessageKey{ID: proto.String(id)},
			MessageTimestamp: proto.Uint64(uint64(ts)), Message: m}}
	}
	hs := func(ts int64, m *waE2E.Message) *waHistorySync.HistorySyncMsg { return hsID("", ts, m) }
	msgs := []*waHistorySync.HistorySyncMsg{
		hsID("H-OK", 1784309593, &waE2E.Message{Conversation: proto.String("hola")}),
		hs(2359569747, &waE2E.Message{Conversation: proto.String("del 2044")}),
		hs(1784309700, &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_REVOKE.Enum()}}),
		{},
	}
	ts, id, preview := newestForChatList(msgs, now)
	if ts != 1784309593 || id != "H-OK" || preview != "hola" {
		t.Fatalf("got (%d, %q, %q), want the newest believable, visible message", ts, id, preview)
	}
	if ts, _, _ := newestForChatList(msgs[1:2], now); ts != 0 {
		t.Fatalf("a chunk with only a future message sorted its chat at %d", ts)
	}
}

// TestHistoryMovesTimeIdAndPreviewTogether: history that brings a newer
// message moves the chat's time, id and preview together; older history, or
// a chunk with nothing to show, leaves a live chat's values alone.
func TestHistoryMovesTimeIdAndPreviewTogether(t *testing.T) {
	_, db := newMediaTestServer(t)
	b := &Bridge{db: db}
	const chat = "5215550000001@s.whatsapp.net"
	read := func() (ts int64, id, preview string) {
		db.QueryRow(`SELECT COALESCE(last_message_time, 0), COALESCE(last_message_id, ''), COALESCE(last_message_preview, '')
			FROM chats WHERE jid = ?`, chat).Scan(&ts, &id, &preview)
		return
	}
	steps := []struct {
		ts          int64
		id, preview string
		wantID      string
	}{
		{1000, "A", "primero", "A"},    // new chat
		{2000, "B", "segundo", "B"},    // newer history moves everything
		{1500, "C", "intermedio", "B"}, // older history moves nothing
		{0, "", "", "B"},               // nothing to show
	}
	for _, st := range steps {
		if err := b.upsertHistoryChat(chat, "Ana", st.ts, st.id, st.preview); err != nil {
			t.Fatal(err)
		}
		_, id, preview := read()
		if id != st.wantID || (st.wantID == "B" && preview != "segundo") {
			t.Fatalf("after history at %d: id %q preview %q, want %s", st.ts, id, preview, st.wantID)
		}
	}
	if ts, _, _ := read(); ts != 2000 {
		t.Fatalf("time = %d, want 2000", ts)
	}
}

func TestMigration008RepairsChatsFromTheFuture(t *testing.T) {
	_, db := newMediaTestServer(t)
	const pedro = "5215550227630@s.whatsapp.net"
	seedChat(t, db, pedro, "direct", "Pedro")
	seedChat(t, db, "5215550000001@s.whatsapp.net", "direct", "Otro")
	const onlyFuture = "5215550000009@s.whatsapp.net"
	seedChat(t, db, onlyFuture, "direct", "Solo futuro")
	if _, err := db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, timestamp, type, content_text, is_from_me) VALUES
		  ('OK1', ?, '', 1784309593, 'image', 'Foto del recibo', 1),
		  ('BLANK', ?, '', 1784309600, 'system', '', 0),
		  ('REVOKE', ?, '', 1784309650, 'system', '[unsupported: protocolMessage]', 0),
		  ('BOGUS', ?, '', 2359569747, 'image', '', 1),
		  ('BOGUS2', ?, '', 2359569747, 'text', 'del futuro', 1)`, pedro, pedro, pedro, pedro, onlyFuture); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE chats SET last_message_time = 2359569747, last_message_id = 'BOGUS', last_message_preview = ''
		WHERE jid IN (?, ?)`, pedro, onlyFuture); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE chats SET last_message_time = 1780000000 WHERE jid = '5215550000001@s.whatsapp.net'`); err != nil {
		t.Fatal(err)
	}

	sqlBytes, err := migrationsFS.ReadFile("migrations/008_future_chat_times.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(sqlBytes)); err != nil {
		t.Fatal(err)
	}

	var got, other, msgTS int64
	db.QueryRow(`SELECT last_message_time FROM chats WHERE jid = ?`, pedro).Scan(&got)
	db.QueryRow(`SELECT last_message_time FROM chats WHERE jid = '5215550000001@s.whatsapp.net'`).Scan(&other)
	db.QueryRow(`SELECT timestamp FROM messages WHERE id = 'BOGUS'`).Scan(&msgTS)
	if got != 1784309593 {
		t.Fatalf("repaired chat time = %d, want the newest believable message (1784309593)", got)
	}
	var lastID, preview string
	db.QueryRow(`SELECT last_message_id, last_message_preview FROM chats WHERE jid = ?`, pedro).Scan(&lastID, &preview)
	if lastID != "OK1" || preview != "Foto del recibo" {
		t.Fatalf("repaired chat still points at %q with preview %q, want OK1's", lastID, preview)
	}
	var futureTime sql.NullInt64
	db.QueryRow(`SELECT last_message_time FROM chats WHERE jid = ?`, onlyFuture).Scan(&futureTime)
	if futureTime.Valid {
		t.Fatalf("a chat with only a message from the future should lose its time, got %d", futureTime.Int64)
	}
	if other != 1780000000 {
		t.Fatalf("a healthy chat was changed: %d", other)
	}
	if msgTS != 2359569747 {
		t.Fatal("the message's own timestamp must be left as it came")
	}
}
