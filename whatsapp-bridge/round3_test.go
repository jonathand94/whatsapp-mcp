package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// TestAFailedDownloadIsAnsweredOnce is the 2026-10-22 test run's top finding:
// asking the phone takes up to two minutes, longer than any request waits,
// and a failed download job was dropped the moment it ended, so every poll
// started over and the caller saw "downloading" for over an hour.
func TestAFailedDownloadIsAnsweredOnce(t *testing.T) {
	s, _ := newMediaTestServer(t)
	const id = "3A0000000000000000BE"
	// A download that failed while nobody was waiting for it.
	j, _ := s.jobs().start("download:"+id, func(*mediaJob) (int, any) {
		return http.StatusServiceUnavailable, errorResponse{Error: "phone did not supply the media", Details: "SENTINEL"}
	})
	<-j.done

	post := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.handleDownloadMedia(rec, httptest.NewRequest(http.MethodPost, "/api/media/download?wait=5",
			strings.NewReader(`{"message_id":"`+id+`"}`)))
		return rec
	}
	if rec := post(); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "SENTINEL") {
		t.Fatalf("the next request should get the failure: %d %s", rec.Code, rec.Body)
	}

	// A failure from long ago is no answer to a request made now.
	old, _ := s.jobs().start("download:"+id, func(*mediaJob) (int, any) {
		return http.StatusServiceUnavailable, errorResponse{Error: "phone did not supply the media", Details: "OLD"}
	})
	<-old.done
	old.finished = old.finished.Add(-time.Hour)
	if rec := post(); strings.Contains(rec.Body.String(), "OLD") {
		t.Fatalf("an hour-old failure was replayed: %d %s", rec.Code, rec.Body)
	}
	// Handed out once: the request after it tries again (here: the message is
	// not in the database, so a fresh attempt answers 404).
	if rec := post(); rec.Code != http.StatusNotFound || strings.Contains(rec.Body.String(), "SENTINEL") {
		t.Fatalf("a failure already handed out was repeated: %d %s", rec.Code, rec.Body)
	}
}

// TestACopyThatDoesNotMatchIsAskedFromThePhone: "hash of media ciphertext
// doesn't match" came from every CDN host on old attachments; it is not worth
// retrying the same copy, and the phone can upload the right one.
func TestACopyThatDoesNotMatchIsAskedFromThePhone(t *testing.T) {
	cdn := fmt.Errorf("failed to download media from last host: %w", whatsmeow.ErrInvalidMediaEncSHA256)
	if !isExpiredMedia(cdn) || !isMismatchedMedia(cdn) {
		t.Fatal("a mismatched copy should go to the phone")
	}
	// A MAC or plaintext mismatch can come from the transfer (or from a row
	// missing its hash): not sent to the phone, not blamed on the sender —
	// said plainly, with "try again".
	hmac := fmt.Errorf("whatsmeow download: %w", whatsmeow.ErrInvalidMediaHMAC)
	if isExpiredMedia(hmac) || downloadErrorStatus(hmac) != http.StatusInternalServerError ||
		!strings.Contains(downloadErrorBody(hmac).Details, "vuelve a intentarlo") {
		t.Fatalf("hmac mismatch: expired=%t %d %+v", isExpiredMedia(hmac), downloadErrorStatus(hmac), downloadErrorBody(hmac))
	}
	if got := cdnProblem(cdn); !strings.Contains(got, "no coincide") {
		t.Fatalf("cdnProblem = %q", got)
	}
	if got := cdnProblem(whatsmeow.ErrMediaDownloadFailedWith404); !strings.Contains(got, "ya no lo tiene") {
		t.Fatalf("cdnProblem(404) = %q", got)
	}
	err := fmt.Errorf("%w: %s y el telefono ya no lo tiene", errMediaGone, cdnProblem(cdn))
	body := downloadErrorBody(err)
	if downloadErrorStatus(err) != http.StatusGone || strings.Contains(body.Details, "ciphertext") ||
		strings.Count(body.Details, "WhatsApp") > 1 {
		t.Fatalf("want a 410 in plain words, said once: %d %+v", downloadErrorStatus(err), body)
	}
	unreachable := fmt.Errorf("%w: %s y el telefono no respondio en 2m0s; tiene que estar encendido y con internet",
		errPhoneUnreachable, cdnProblem(whatsmeow.ErrMediaDownloadFailedWith404))
	if d := downloadErrorBody(unreachable).Details; strings.Count(d, "con internet") != 1 {
		t.Fatalf("the advice is repeated: %q", d)
	}
}

// TestHiddenMarkersMatchMigration009 keeps the migration's conditions equal to
// the ones list_messages uses.
func TestHiddenMarkersMatchMigration009(t *testing.T) {
	raw, err := migrationsFS.ReadFile("migrations/009_hidden_previews.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{"m.content_text", "last_message_preview"} {
		if !strings.Contains(string(raw), hiddenMarkerSQL(col)) {
			t.Errorf("migration 009 does not use hiddenMarkerSQL(%q):\n%s", col, hiddenMarkerSQL(col))
		}
	}
}

// TestHiddenMarkerSQL: a marker is hidden when every field it names is.
func TestHiddenMarkerSQL(t *testing.T) {
	db := undecodedTestDB(t)
	for text, hidden := range map[string]bool{
		"[unsupported: protocolMessage]":                                            true,
		"[unsupported: fastRatchetKeySenderKeyDistributionMessage,protocolMessage]": true,
		"[unsupported: pollUpdateMessage,templateMessage]":                          false,
		"[unsupported: templateMessage]":                                            false,
		"[unsupported: protocolMessageV2]":                                          false,
		"hola, protocolMessage":                                                     false,
	} {
		var got bool
		if err := db.QueryRow(`SELECT `+hiddenMarkerSQL("?"), text, text).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != hidden {
			t.Errorf("%q hidden = %t, want %t", text, got, hidden)
		}
	}
}

func TestMigration009RepointsHiddenPreviews(t *testing.T) {
	_, db := newMediaTestServer(t)
	const own, other = "5215550175084@s.whatsapp.net", "5215550000001@s.whatsapp.net"
	seedChat(t, db, own, "direct", "Yo")
	seedChat(t, db, other, "direct", "Otro")
	if _, err := db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, timestamp, type, content_text, is_from_me) VALUES
		  ('TXT', ?, '', 1792500000, 'text', 'Hola de verdad', 1),
		  ('UNDO', ?, '', 1792540000, 'reaction', '', 1),
		  ('REV', ?, '', 1792550000, 'system', '[unsupported: protocolMessage]', 1),
		  ('OTHER', ?, '', 1792540000, 'text', 'Intacto', 0)`, own, own, own, other); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE chats SET last_message_id = 'REV', last_message_time = 1792550000,
		last_message_preview = '[unsupported: fastRatchetKeySenderKeyDistributionMessage,protocolMessage]'
		WHERE jid = ?`, own); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE chats SET last_message_id = 'OTHER', last_message_time = 1792540000,
		last_message_preview = 'Intacto' WHERE jid = ?`, other); err != nil {
		t.Fatal(err)
	}
	raw, _ := migrationsFS.ReadFile("migrations/009_hidden_previews.sql")
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatal(err)
	}
	var id, preview string
	var ts int64
	db.QueryRow(`SELECT last_message_id, last_message_time, last_message_preview FROM chats WHERE jid = ?`, own).Scan(&id, &ts, &preview)
	if id != "TXT" || ts != 1792500000 || preview != "Hola de verdad" {
		t.Fatalf("own chat = (%s, %d, %q), want the text before the deletion", id, ts, preview)
	}
	db.QueryRow(`SELECT last_message_id FROM chats WHERE jid = ?`, other).Scan(&id)
	if id != "OTHER" {
		t.Fatalf("a chat with a real preview was changed: %s", id)
	}
}

// TestListMessagesSpeaksForTheChatAskedAbout: hidden markers stay out, every
// message carries the chat_jid asked for, and a sender with no name gets one.
func TestListMessagesSpeaksForTheChatAskedAbout(t *testing.T) {
	s, db := newMediaTestServer(t)
	const phone, lid = "5215550000001@s.whatsapp.net", "900000000000001@lid"
	const stranger = "99990000002312@lid"
	seedChatAt(t, db, phone, "direct", "Ana", 300, 0)
	seedChatAt(t, db, lid, "direct", "", 200, 0)
	aliasPair(t, db, phone, lid)
	if _, err := db.Exec(`INSERT INTO contacts (jid, push_name, normalized_name, created_at, updated_at)
		VALUES (?, 'Ana Torres', 'ana torres', 1, 1)`, phone); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, is_from_me) VALUES
		  ('NEW', ?, ?, 'Ana Torres', 300, 'text', 'reciente', 0),
		  ('OLD', ?, ?, '900000000000001', 200, 'text', 'antiguo', 0),
		  ('GONE', ?, ?, '', 250, 'system', '[unsupported: protocolMessage]', 0),
		  ('UNDO', ?, ?, '', 240, 'reaction', '', 0),
		  ('HIST', ?, ?, '900000000000001@lid', 180, 'text', 'del historial', 0),
		  ('WHO', ?, ?, '99990000002312', 150, 'text', 'de un desconocido', 0)`,
		phone, phone, lid, lid, lid, lid, lid, lid, lid, lid, lid, stranger); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	s.handleListMessages(rec, httptest.NewRequest(http.MethodGet, "/api/messages?chat_jid="+phone+"&limit=10", nil))
	var out messageListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("%v: %s", err, rec.Body)
	}
	got := map[string]messageRow{}
	for _, m := range out.Messages {
		got[m.ID] = m
		if m.ChatJID != phone {
			t.Errorf("%s answers for chat %s, want %s", m.ID, m.ChatJID, phone)
		}
	}
	if _, ok := got["GONE"]; ok || len(got) != 4 {
		t.Fatalf("want NEW, OLD, HIST and WHO, without the protocol marker or the withdrawn reaction; got %v", out.Messages)
	}
	if got["OLD"].SenderDisplay != "Ana Torres" || got["HIST"].SenderDisplay != "Ana Torres" {
		t.Errorf("a sender known under her other JID shows as %q (live) and %q (history)",
			got["OLD"].SenderDisplay, got["HIST"].SenderDisplay)
	}
	if got["WHO"].SenderDisplay != "Desconocido (…2312)" {
		t.Errorf("an unknown @lid sender shows as %q", got["WHO"].SenderDisplay)
	}
}

// TestHistoryRedecodesOldMarkers: rows stored as markers before the decoder
// knew their type are fixed when history brings them again — a template
// becomes its text, an album photo becomes a photo.
func TestHistoryRedecodesOldMarkers(t *testing.T) {
	db := undecodedTestDB(t)
	b := &Bridge{db: db}
	const chat = "12147735814-1589465137@g.us"
	insertTestMessage(t, db, "TPL", chat, "system", "[unsupported: templateMessage]", 100)
	insertTestMessage(t, db, "KID", chat, "system", "[unsupported: associatedChildMessage]", 101)
	insertTestMessage(t, db, "NEW", chat, "system", "[unsupported: someFutureMessage]", 102)

	template := &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{HydratedTemplate: bankAlert()}}
	child := &waE2E.Message{AssociatedChildMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
		ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}}}}
	future := withUnknownField(&waE2E.Message{}, 988)
	for id, m := range map[string]*waE2E.Message{"TPL": template, "KID": child, "NEW": future} {
		if _, err := b.backfillDecodedContent(id, m); err != nil {
			t.Fatal(err)
		}
	}
	row := func(id string) (typ, text string) {
		db.QueryRow(`SELECT type, COALESCE(content_text, '') FROM messages WHERE id = ?`, id).Scan(&typ, &text)
		return
	}
	if typ, text := row("TPL"); typ == "system" || !strings.Contains(text, "Compra aprobada") {
		t.Errorf("template still (%s, %q)", typ, text)
	}
	if typ, text := row("KID"); typ != "image" || text != "" {
		t.Errorf("album photo is (%s, %q), want an image", typ, text)
	}
	if typ, text := row("NEW"); typ != "system" || text != "[unsupported: someFutureMessage]" {
		t.Errorf("a marker replaced by another marker: (%s, %q)", typ, text)
	}
}

// TestHistoryRewritesOnlyRowsItCouldNotRead: the re-decode leaves every other
// system row alone, and never turns a hidden blank into a visible empty one.
func TestHistoryRewritesOnlyRowsItCouldNotRead(t *testing.T) {
	db := undecodedTestDB(t)
	b := &Bridge{db: db}
	const chat = "12147735814-1589465137@g.us"
	insertTestMessage(t, db, "ALBUM", chat, "system", "Álbum: 3 fotos", 100)
	insertTestMessage(t, db, "BLANK", chat, "system", "", 101)
	insertTestMessage(t, db, "LOCKED", chat, "system", undecryptableMarker("decrypt-failed"), 102)

	hola := &waE2E.Message{Conversation: proto.String("hola")}
	withdrawn := &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("")}}}}
	for id, m := range map[string]*waE2E.Message{"ALBUM": hola, "BLANK": withdrawn, "LOCKED": hola} {
		if _, err := b.backfillDecodedContent(id, m); err != nil {
			t.Fatal(err)
		}
	}
	row := func(id string) (typ, text string) {
		db.QueryRow(`SELECT type, COALESCE(content_text, '') FROM messages WHERE id = ?`, id).Scan(&typ, &text)
		return
	}
	if typ, text := row("ALBUM"); typ != "system" || text != "Álbum: 3 fotos" {
		t.Errorf("an album summary was rewritten: (%s, %q)", typ, text)
	}
	if typ, text := row("BLANK"); typ != "system" || text != "" {
		t.Errorf("a withdrawn reaction became a visible row: (%s, %q)", typ, text)
	}
	if typ, text := row("LOCKED"); typ != "text" || text != "hola" {
		t.Errorf("a message history delivered readable is still (%s, %q)", typ, text)
	}
}
