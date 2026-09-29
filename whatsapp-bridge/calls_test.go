package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func newCallsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := OpenDB(&Config{DBPath: filepath.Join(t.TempDir(), "calls.db")}, "")
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seedChat(t *testing.T, db *sql.DB, jid, chatType, name string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO chats (jid, chat_type, name, created_at, updated_at) VALUES (?, ?, ?, 1, 1)`,
		jid, chatType, nullIfEmpty(name)); err != nil {
		t.Fatalf("seeding chat %s: %v", jid, err)
	}
}

func callRow(t *testing.T, db *sql.DB, id string) (chatJID sql.NullString, callType string, result sql.NullString) {
	t.Helper()
	err := db.QueryRow(`SELECT chat_jid, call_type, result FROM calls WHERE id = ?`, id).Scan(&chatJID, &callType, &result)
	if err == sql.ErrNoRows {
		t.Fatalf("call %s was not recorded at all", id)
	}
	if err != nil {
		t.Fatalf("reading call %s: %v", id, err)
	}
	return
}

var (
	callerPN  = types.NewJID("5214421111111", types.DefaultUserServer)
	callerLID = types.NewJID("99887766554433", types.HiddenUserServer)
	stranger  = types.NewJID("5219999999999", types.DefaultUserServer)
)

// TestTheOldOfferValueViolatesTheSchema pins the root cause, so that nobody
// later "fixes" the call log by widening the constraint instead: the schema is
// right, the value was wrong. This is the exact error the production log shows
// 43 times.
func TestTheOldOfferValueViolatesTheSchema(t *testing.T) {
	db := newCallsTestDB(t)
	_, err := db.Exec(`INSERT INTO calls (id, caller_jid, timestamp, call_type, result)
	                   VALUES ('old', 'x@s.whatsapp.net', 1, 'voice', 'offered')`)
	if err == nil {
		t.Fatal("'offered' was accepted; the constraint changed and this test's premise with it")
	}
}

// TestARingingCallIsRecorded is the regression test for the bug itself: a call
// that has only been offered must produce a row. Before, it produced nothing.
func TestARingingCallIsRecorded(t *testing.T) {
	db := newCallsTestDB(t)
	seedChat(t, db, callerPN.String(), "direct", "Caller")

	err := recordCall(context.Background(), db, callEvent{
		ID: "ring-1", Creator: callerPN, At: time.Unix(1790000000, 0),
	})
	if err != nil {
		t.Fatalf("recording an offer failed: %v", err)
	}
	_, _, result := callRow(t, db, "ring-1")
	if result.Valid {
		t.Fatalf("a call that is still ringing has no outcome, got result %q", result.String)
	}
}

// TestAStrangersCallOpensNoChat: someone who has never written calls. The call
// is recorded, but no chat is invented for them — WhatsApp itself does not
// open a conversation for a call, and list_chats would otherwise fill with
// nameless entries from every spam call.
func TestAStrangersCallOpensNoChat(t *testing.T) {
	db := newCallsTestDB(t)
	var before, after int
	db.QueryRow(`SELECT COUNT(*) FROM chats`).Scan(&before)

	if err := recordCall(context.Background(), db, callEvent{ID: "spam-1", Creator: stranger, At: time.Now()}); err != nil {
		t.Fatalf("recording a stranger's call failed: %v", err)
	}

	db.QueryRow(`SELECT COUNT(*) FROM chats`).Scan(&after)
	if after != before {
		t.Fatalf("a stranger's call created %d chat row(s)", after-before)
	}
	chatJID, _, _ := callRow(t, db, "spam-1")
	if chatJID.Valid {
		t.Fatalf("chat_jid should be NULL for a caller with no chat, got %q", chatJID.String)
	}
}

// TestCallsAreForeignKeySafe runs against a database that DOES enforce foreign
// keys. Production does not today (PRAGMA foreign_keys = 0), which is exactly
// why this has to be tested deliberately: storing the stranger's own JID in
// chat_jid would pass every other test and only break the day enforcement is
// switched on.
func TestCallsAreForeignKeySafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fk.db")
	migrated, err := OpenDB(&Config{DBPath: path}, "")
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	migrated.Close()
	db, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Prove the premise, or this test proves nothing: a dangling chat_jid must
	// actually be rejected on this connection.
	if _, err := db.Exec(`INSERT INTO calls (id, chat_jid, caller_jid, timestamp, call_type)
	                      VALUES ('dangling', 'nobody@s.whatsapp.net', 'nobody@s.whatsapp.net', 1, 'voice')`); err == nil {
		t.Fatal("foreign keys are not being enforced here; the rest of this test would be meaningless")
	}

	seedChat(t, db, callerPN.String(), "direct", "Caller")
	for _, e := range []callEvent{
		{ID: "fk-stranger", Creator: stranger, At: time.Now()},
		{ID: "fk-known", Creator: callerPN, At: time.Now()},
	} {
		if err := recordCall(context.Background(), db, e); err != nil {
			t.Fatalf("%s: rejected under foreign-key enforcement: %v", e.ID, err)
		}
	}
}

// TestACallFindsTheChatUnderEitherForm: the caller arrives as a @lid, but the
// conversation with them is filed under their phone number. The call must land
// in that existing chat, not be treated as a stranger's.
func TestACallFindsTheChatUnderEitherForm(t *testing.T) {
	db := newCallsTestDB(t)
	seedChat(t, db, callerPN.String(), "direct", "Caller")

	if err := recordCall(context.Background(), db, callEvent{
		ID: "lid-1", Creator: callerLID, CreatorAlt: callerPN, At: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	chatJID, _, _ := callRow(t, db, "lid-1")
	if chatJID.String != callerPN.String() {
		t.Fatalf("chat_jid = %q, want the existing phone-number chat %q", chatJID.String, callerPN.String())
	}
}

// TestAnAnsweredCallStaysAnswered: terminate arrives for every call, answered
// ones included. It must fill an empty outcome, never overwrite a real one.
func TestAnAnsweredCallStaysAnswered(t *testing.T) {
	db := newCallsTestDB(t)
	ctx := context.Background()
	base := callEvent{ID: "ans-1", Creator: callerPN, At: time.Now()}

	steps := []callEvent{base, base, base}
	steps[1].Result, steps[1].Definitive = callAnswered, true
	steps[2].Result, _ = mapTerminateReason("timeout")
	for _, e := range steps {
		if err := recordCall(ctx, db, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, r := callRow(t, db, "ans-1"); r.String != callAnswered {
		t.Fatalf("result = %q after accept then terminate, want %q", r.String, callAnswered)
	}
}

// TestAnUnansweredCallIsMissed: nobody picks up, WhatsApp terminates with
// reason "timeout". That is what a missed call looks like on the wire.
func TestAnUnansweredCallIsMissed(t *testing.T) {
	db := newCallsTestDB(t)
	ctx := context.Background()
	offer := callEvent{ID: "miss-1", Creator: callerPN, At: time.Now()}
	end := offer
	end.Result, _ = mapTerminateReason("timeout")

	for _, e := range []callEvent{offer, end} {
		if err := recordCall(ctx, db, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, r := callRow(t, db, "miss-1"); r.String != callMissed {
		t.Fatalf("result = %q, want %q", r.String, callMissed)
	}
}

// TestAMissedOfferDoesNotLoseTheCall: the bridge was restarting when the phone
// rang, so the offer never arrived — only the terminate did. Under the old
// insert-then-update split the call vanished silently. Now the first event to
// arrive creates the row.
func TestAMissedOfferDoesNotLoseTheCall(t *testing.T) {
	db := newCallsTestDB(t)
	end := callEvent{ID: "late-1", Creator: callerPN, At: time.Now()}
	end.Result, _ = mapTerminateReason("")

	if err := recordCall(context.Background(), db, end); err != nil {
		t.Fatal(err)
	}
	if _, _, r := callRow(t, db, "late-1"); r.String != callEnded {
		t.Fatalf("result = %q, want %q", r.String, callEnded)
	}
}

// TestVideoIsNotDowngradedByLaterEvents: only the offer knows the call is
// video. The terminate that follows carries no media information and must not
// flip it back to voice.
func TestVideoIsNotDowngradedByLaterEvents(t *testing.T) {
	db := newCallsTestDB(t)
	ctx := context.Background()
	offer := callEvent{ID: "vid-1", Creator: callerPN, At: time.Now(), IsVideo: true}
	end := callEvent{ID: "vid-1", Creator: callerPN, At: time.Now(), Result: callEnded}

	for _, e := range []callEvent{offer, end} {
		if err := recordCall(ctx, db, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, typ, _ := callRow(t, db, "vid-1"); typ != "video" {
		t.Fatalf("call_type = %q after terminate, want video", typ)
	}
}

func TestMapTerminateReason(t *testing.T) {
	cases := []struct {
		in    string
		want  string
		known bool
	}{
		{"", callEnded, true},
		{"timeout", callMissed, true},
		{" Timeout ", callMissed, true},
		{"reject", callRejected, true},
		{"rejected_elsewhere", callRejected, true},
		{"accepted_elsewhere", callAnswered, true},
		// Unknown vocabulary: recorded as a valid outcome, and reported as unknown
		// so it gets logged. Writing it raw is what would fail the CHECK.
		{"something_new", callEnded, false},
	}
	for _, c := range cases {
		got, known := mapTerminateReason(c.in)
		if got != c.want || known != c.known {
			t.Errorf("mapTerminateReason(%q) = (%q, %t), want (%q, %t)", c.in, got, known, c.want, c.known)
		}
	}
	// Whatever comes back must be a value the table accepts.
	allowed := map[string]bool{callAnswered: true, callMissed: true, callRejected: true, callEnded: true, "failed": true}
	for _, in := range []string{"", "timeout", "reject", "accept", "busy", "weird", "ERROR", "ñ"} {
		if got, _ := mapTerminateReason(in); !allowed[got] {
			t.Errorf("mapTerminateReason(%q) = %q, which the calls.result CHECK rejects", in, got)
		}
	}
}

func TestOfferIsVideo(t *testing.T) {
	voice := &waBinary.Node{Tag: "offer", Content: []waBinary.Node{{Tag: "audio"}, {Tag: "audio"}, {Tag: "net"}}}
	video := &waBinary.Node{Tag: "offer", Content: []waBinary.Node{{Tag: "audio"}, {Tag: "video"}, {Tag: "net"}}}
	if offerIsVideo(voice) {
		t.Error("an audio-only offer was classified as video")
	}
	if !offerIsVideo(video) {
		t.Error("an offer with a <video> child was classified as voice")
	}
	if offerIsVideo(nil) {
		t.Error("a nil node must fall back to voice")
	}
}

// TestListCallsEndpoint covers the read side, which did not exist: newest
// first, names resolved through aliases, and filtering by any form of a person.
func TestListCallsEndpoint(t *testing.T) {
	db := newCallsTestDB(t)
	ctx := context.Background()
	seedChat(t, db, callerPN.String(), "direct", "")
	if _, err := db.Exec(`INSERT INTO contacts (jid, full_name, created_at, updated_at) VALUES (?, 'Mi Amor', 1, 1)`,
		callerPN.String()); err != nil {
		t.Fatal(err)
	}
	recordJIDAlias(ctx, db, callerLID, callerPN, "test", 1)

	for i, e := range []callEvent{
		{ID: "c-old", Creator: callerPN, At: time.Unix(1790000000, 0), Result: callMissed},
		{ID: "c-lid", Creator: callerLID, CreatorAlt: callerPN, At: time.Unix(1790000100, 0), Result: callAnswered, Definitive: true},
		{ID: "c-spam", Creator: stranger, At: time.Unix(1790000200, 0)},
	} {
		if err := recordCall(ctx, db, e); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	s := &Server{db: db}
	get := func(url string) []callItem {
		t.Helper()
		rec := httptest.NewRecorder()
		s.handleListCalls(rec, httptest.NewRequest(http.MethodGet, url, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: %d %s", url, rec.Code, rec.Body.String())
		}
		var body struct {
			Calls []callItem `json:"calls"`
			Count int        `json:"count"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Count != len(body.Calls) {
			t.Fatalf("count %d disagrees with %d calls", body.Count, len(body.Calls))
		}
		return body.Calls
	}

	all := get("/api/calls")
	if len(all) != 3 || all[0].ID != "c-spam" || all[2].ID != "c-old" {
		t.Fatalf("want 3 calls newest first, got %+v", all)
	}
	if all[1].CallerName != "Mi Amor" {
		t.Errorf("a call from the @lid form should resolve to the address-book name, got %q", all[1].CallerName)
	}
	if all[0].CallerName != "" {
		t.Errorf("a stranger has no name, got %q", all[0].CallerName)
	}
	if all[0].Result != "" {
		t.Errorf("a call with no outcome should omit result, got %q", all[0].Result)
	}

	// Filtering by the phone form returns both calls — but that alone proves
	// nothing about alias resolution: the @lid call was filed under the phone
	// chat by callChatJID, so it matches on chat_jid regardless. It passed with
	// the filter's alias lookup deleted.
	if mine := get("/api/calls?jid=" + callerPN.String()); len(mine) != 2 {
		t.Fatalf("filtering by the phone number returned %d calls, want 2", len(mine))
	}
	// The direction that needs the lookup: filter by the @lid. The phone-number
	// call has no @lid anywhere in its row, so only alias resolution finds it.
	if viaLID := get("/api/calls?jid=" + callerLID.String()); len(viaLID) != 2 {
		t.Fatalf("filtering by the @lid returned %d calls, want both forms (2); the filter is not resolving aliases", len(viaLID))
	}

	if got := get("/api/calls?limit=1"); len(got) != 1 {
		t.Fatalf("limit=1 returned %d", len(got))
	}
}
