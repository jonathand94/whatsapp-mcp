package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func aliasPair(t *testing.T, db *sql.DB, a, b string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO jid_aliases (jid_a, jid_b, discovered_at, source) VALUES (?, ?, 1, 't'), (?, ?, 1, 't')`,
		a, b, b, a); err != nil {
		t.Fatal(err)
	}
}

func seedChatAt(t *testing.T, db *sql.DB, jid, chatType, name string, last int64, unread int) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO chats (jid, chat_type, name, normalized_name, created_at, updated_at, last_message_time, unread_count)
		VALUES (?, ?, ?, ?, 1, 1, ?, ?)`, jid, chatType, nullIfEmpty(name), nullIfEmpty(Normalize(name)), last, unread); err != nil {
		t.Fatalf("seeding chat %s: %v", jid, err)
	}
}

func getChats(t *testing.T, s *Server, handler func(http.ResponseWriter, *http.Request), url string) chatListResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, url, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", url, rec.Code, rec.Body)
	}
	var out chatListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestListChatsShowsOnePersonOnce: the same person under a phone JID and a
// @lid used to appear twice in list_chats (the user's own chat included).
func TestListChatsShowsOnePersonOnce(t *testing.T) {
	s, db := newMediaTestServer(t)
	seedChatAt(t, db, "5215550000001@s.whatsapp.net", "direct", "Carlos Ibarra", 300, 1)
	seedChatAt(t, db, "100000000000001@lid", "direct", "", 200, 2)
	seedChatAt(t, db, "5215550000002@s.whatsapp.net", "direct", "Otra persona", 100, 0)
	aliasPair(t, db, "5215550000001@s.whatsapp.net", "100000000000001@lid")

	out := getChats(t, s, s.handleListChats, "/api/chats?limit=10")
	if out.Count != 2 {
		t.Fatalf("want 2 chats, got %d: %+v", out.Count, out.Chats)
	}
	carlos := out.Chats[0]
	if carlos.Name != "Carlos Ibarra" || carlos.UnreadCount != 3 || len(carlos.MergedJIDs) != 2 {
		t.Fatalf("merged chat = %+v", carlos)
	}

	// Pagination counts people, not rows.
	second := getChats(t, s, s.handleListChats, "/api/chats?limit=1&offset=1")
	if second.Count != 1 || second.Chats[0].Name != "Otra persona" {
		t.Fatalf("page 2 = %+v", second.Chats)
	}
}

// TestListChatsPagesThroughEveryChat: pages are counted in people across
// batches, past the 1,500 rows the old single window stopped at, and a
// person whose other row is far down the list is folded into their entry
// instead of turning up again on a later page.
func TestListChatsPagesThroughEveryChat(t *testing.T) {
	s, db := newMediaTestServer(t)
	defer func(n int) { chatScanBatch = n }(chatScanBatch)
	chatScanBatch = 7

	const people = 40
	for i := 0; i < people; i++ {
		seedChatAt(t, db, fmt.Sprintf("52155500%05d@s.whatsapp.net", i), "direct", fmt.Sprintf("Persona %d", i), int64(10000-i), 1)
	}
	// Persona 3 also wrote from a @lid long ago: last in the list.
	seedChatAt(t, db, "100000000000003@lid", "direct", "", 5, 2)
	aliasPair(t, db, "5215550000003@s.whatsapp.net", "100000000000003@lid")

	var names []string
	for offset := 0; ; offset += 6 {
		out := getChats(t, s, s.handleListChats, fmt.Sprintf("/api/chats?limit=6&offset=%d", offset))
		if out.Count == 0 {
			break
		}
		for _, c := range out.Chats {
			names = append(names, c.Name)
			if c.Name == "Persona 3" && (c.UnreadCount != 3 || len(c.MergedJIDs) != 2) {
				t.Fatalf("Persona 3 not merged with the @lid row further down: %+v", c)
			}
		}
	}
	if len(names) != people {
		t.Fatalf("paged through %d entries, want %d people: %v", len(names), people, names)
	}
	for i, n := range names {
		if n != fmt.Sprintf("Persona %d", i) {
			t.Fatalf("entry %d is %q: %v", i, n, names)
		}
	}

	// Out-of-range paging answers empty pages instead of failing.
	for _, q := range []string{"limit=5&offset=-3", "limit=0", "limit=-1&offset=2", "limit=5&offset=100"} {
		out := getChats(t, s, s.handleListChats, "/api/chats?"+q)
		if q == "limit=5&offset=-3" && (out.Count != 5 || out.Chats[0].Name != "Persona 0") {
			t.Fatalf("%s: %+v", q, out.Chats)
		}
		if q != "limit=5&offset=-3" && out.Count != 0 {
			t.Fatalf("%s: want an empty page, got %+v", q, out.Chats)
		}
	}
}

// TestSearchChatsFindsGroupsWithoutAccents: "Sociedad Médica del Norte" was not
// findable at all; search_contacts only searches people.
func TestSearchChatsFindsGroupsWithoutAccents(t *testing.T) {
	s, db := newMediaTestServer(t)
	seedChatAt(t, db, "120363000000815132@g.us", "group", "Sociedad Medica del Norte", 500, 0)
	seedChatAt(t, db, "5215550000003@s.whatsapp.net", "direct", "Médica Ana", 400, 0)

	out := getChats(t, s, s.handleSearchChats, "/api/chats/search?q=Sociedad%20M%C3%A9dica%20del%20Norte")
	if out.Count != 1 || out.Chats[0].JID != "120363000000815132@g.us" {
		t.Fatalf("group search = %+v", out.Chats)
	}
	both := getChats(t, s, s.handleSearchChats, "/api/chats/search?q=medica")
	if both.Count != 2 {
		t.Fatalf("accent-insensitive search should find both, got %+v", both.Chats)
	}
	groups := getChats(t, s, s.handleSearchChats, "/api/chats/search?q=medica&chat_type=group")
	if groups.Count != 1 || groups.Chats[0].ChatType != "group" {
		t.Fatalf("chat_type filter = %+v", groups.Chats)
	}
}

// TestSearchContactsShowsOnePersonOnce: one person came back five times — the
// @lid plus a row per linked device (":79", ":80"...) — and once more per
// alias.
func TestSearchContactsShowsOnePersonOnce(t *testing.T) {
	s, db := newMediaTestServer(t)
	for _, jid := range []string{"999909829054513@lid", "999909829054513:79@lid", "999909829054513:80@lid"} {
		if _, err := db.Exec(`INSERT INTO contacts (jid, push_name, normalized_name, created_at, updated_at) VALUES (?, 'Rodrigo', 'rodrigo', 1, 1)`, jid); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO contacts (jid, phone, full_name, normalized_full_name, created_at, updated_at)
		VALUES ('5214420000000@s.whatsapp.net', '5214420000000', 'Rodrigo Abogado', 'rodrigo abogado', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	aliasPair(t, db, "999909829054513@lid", "5214420000000@s.whatsapp.net")

	rec := httptest.NewRecorder()
	s.handleSearchContacts(rec, httptest.NewRequest(http.MethodGet, "/api/contacts/search?q=Rodrigo", nil))
	var out contactListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Count != 1 {
		t.Fatalf("want one Rodrigo, got %d: %+v", out.Count, out.Contacts)
	}
	c := out.Contacts[0]
	if c.JID != "5214420000000@s.whatsapp.net" || c.FullName != "Rodrigo Abogado" || c.PushName != "Rodrigo" {
		t.Fatalf("merged contact = %+v", c)
	}
	if len(c.Aliases) != 1 || c.Aliases[0] != "999909829054513@lid" {
		t.Fatalf("aliases = %v", c.Aliases)
	}
}

func TestListMessagesCarriesMediaAndFilters(t *testing.T) {
	s, db := newMediaTestServer(t)
	const chat = "5215550000009@s.whatsapp.net"
	seedChatAt(t, db, chat, "direct", "Rodrigo", 400, 0)
	if _, err := db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, is_from_me,
		                      media_mime, media_file_length, media_filename) VALUES
		  ('D1', ?, ?, 'Rodrigo', 100, 'document', '', 0,
		   'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 17000, 'Estatutos.docx'),
		  ('T1', ?, ?, 'Rodrigo', 200, 'text', 'hola', 0, NULL, NULL, NULL),
		  ('I1', ?, '', '99990069399676', 300, 'image', '', 1, 'image/jpeg', 5000, NULL)`,
		chat, chat, chat, chat, chat); err != nil {
		t.Fatal(err)
	}

	get := func(query string) messageListResponse {
		t.Helper()
		rec := httptest.NewRecorder()
		s.handleListMessages(rec, httptest.NewRequest(http.MethodGet, "/api/messages?chat_jid="+chat+query, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", query, rec.Code, rec.Body)
		}
		var out messageListResponse
		json.Unmarshal(rec.Body.Bytes(), &out)
		return out
	}

	docs := get("&media_type=document")
	if docs.Count != 1 || docs.Messages[0].Filename != "Estatutos.docx" || docs.Messages[0].Size != 17000 {
		t.Fatalf("document filter / metadata = %+v", docs.Messages)
	}
	if media := get("&media_type=media"); media.Count != 2 {
		t.Fatalf("media filter = %d, want 2", media.Count)
	}
	if window := get("&since=150&until=250"); window.Count != 1 || window.Messages[0].ID != "T1" {
		t.Fatalf("date window = %+v", window.Messages)
	}
	mine := get("&from_me=true")
	if mine.Count != 1 || mine.Messages[0].SenderDisplay != "Yo" {
		t.Fatalf("own message should be labelled as the user's, got %+v", mine.Messages)
	}

	rec := httptest.NewRecorder()
	s.handleListMessages(rec, httptest.NewRequest(http.MethodGet, "/api/messages?chat_jid="+chat+"&media_type=gif", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("an unknown media_type should be refused, got %d", rec.Code)
	}
}

func TestHTMLIsSavedAsHTML(t *testing.T) {
	if got := extensionForMime("text/html", "document"); got != ".html" {
		t.Fatalf("text/html → %s, want .html (Linux lists .ehtml first)", got)
	}
	if got := extensionForMime("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "document"); got != ".docx" {
		t.Fatalf("docx → %s", got)
	}
}

// TestHiddenMessagesKeepTheChatPreview: a revoke ("message deleted") used to
// become the chat's last message, shown as "[unsupported: protocolMessage]".
// It is still stored; the chat keeps showing the last real message.
func TestHiddenMessagesKeepTheChatPreview(t *testing.T) {
	db := undecodedTestDB(t)
	b := &Bridge{db: db}
	const ts = 1792557000

	b.handleEvent(&events.Message{
		Info:    parityTestInfo(t, "real-1", ts),
		Message: &waE2E.Message{Conversation: proto.String("nos vemos a las 5")},
	})
	b.handleEvent(&events.Message{
		Info: parityTestInfo(t, "revoke-1", ts+60),
		Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_REVOKE.Enum(),
			Key:  &waCommon.MessageKey{ID: proto.String("real-1")},
		}},
	})

	var preview, lastID string
	if err := db.QueryRow(`SELECT COALESCE(last_message_preview, ''), COALESCE(last_message_id, '') FROM chats
		WHERE jid = '12147735814-1589465137@g.us'`).Scan(&preview, &lastID); err != nil {
		t.Fatal(err)
	}
	if preview != "nos vemos a las 5" || lastID != "real-1" {
		t.Fatalf("the revoke replaced the chat's preview: %q (%s)", preview, lastID)
	}
	var stored int
	db.QueryRow(`SELECT COUNT(*) FROM messages WHERE id = 'revoke-1'`).Scan(&stored)
	if stored != 1 {
		t.Fatal("a hidden message must still be stored")
	}
}

func TestVideoNoteKeysAreKept(t *testing.T) {
	f, ok := extractDownloadableFieldsFromProto(&waE2E.Message{
		PtvMessage: &waE2E.VideoMessage{MediaKey: []byte("k"), DirectPath: proto.String("/v/ptv")},
	})
	if !ok || string(f.MediaKey) != "k" || f.MediaDirectPath.String != "/v/ptv" {
		t.Fatalf("video note fields = %+v (ok=%v)", f, ok)
	}
}

func withUnknownField(m *waE2E.Message, field protowire.Number) *waE2E.Message {
	raw := protowire.AppendTag(nil, field, protowire.BytesType)
	raw = protowire.AppendBytes(raw, []byte{0x0a, 0x01, 'x'})
	m.ProtoReflect().SetUnknown(raw)
	return m
}

// TestNewerWhatsAppContentIsKept: a payload whatsmeow cannot parse has no
// known fields, like plumbing, but must not be dropped like plumbing — it
// gets a marker and stays visible, plain or inside a disappearing-message
// envelope.
func TestNewerWhatsAppContentIsKept(t *testing.T) {
	db := undecodedTestDB(t)
	b := &Bridge{db: db}
	const ts = 1792557000

	plain := withUnknownField(&waE2E.Message{MessageContextInfo: &waE2E.MessageContextInfo{}}, 999)
	wrapped := &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{
		Message: withUnknownField(&waE2E.Message{}, 998)}}
	for id, m := range map[string]*waE2E.Message{"new-1": plain, "new-2": wrapped} {
		if isPlumbing(m) {
			t.Fatalf("%s: newer content classified as plumbing", id)
		}
		b.handleEvent(&events.Message{Info: parityTestInfo(t, id, ts), Message: m})
	}
	var markers int
	db.QueryRow(`SELECT COUNT(*) FROM messages WHERE id IN ('new-1','new-2') AND content_text LIKE '[unsupported: unknown_field_%'`).Scan(&markers)
	if markers != 2 {
		t.Fatalf("want both stored with an unknown_field marker, got %d", markers)
	}

	// The same inside the other envelopes the decoder opens: a comment on a
	// community announcement, and an edit (2026-10-21 review: both were
	// still dropped). And a newer field on the outer message around a payload
	// of carriers only is kept with a marker, not stored as a blank row.
	comment := &waE2E.Message{CommentMessage: &waE2E.CommentMessage{Message: withUnknownField(&waE2E.Message{}, 997)}}
	edit := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), EditedMessage: withUnknownField(&waE2E.Message{}, 996)}}
	outer := withUnknownField(&waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
		SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{}}}}, 995)
	for id, m := range map[string]*waE2E.Message{"new-3": comment, "new-4": edit, "new-5": outer} {
		if isPlumbing(m) || hiddenFromChatPreview(m) {
			t.Fatalf("%s: newer content classified as plumbing or hidden from the preview", id)
		}
		b.handleEvent(&events.Message{Info: parityTestInfo(t, id, ts), Message: m})
	}
	db.QueryRow(`SELECT COUNT(*) FROM messages WHERE id IN ('new-3','new-4','new-5') AND content_text LIKE '[unsupported: unknown_field_%'`).Scan(&markers)
	if markers != 3 {
		t.Fatalf("want the comment, the edit and the outer field stored with a marker, got %d", markers)
	}

	// Plumbing whose metadata has fields newer than whatsmeow is still plumbing:
	// only the envelope decides, not a carrier's insides.
	ctx := &waE2E.MessageContextInfo{}
	raw := protowire.AppendTag(nil, 500, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1)
	ctx.ProtoReflect().SetUnknown(raw)
	if !isPlumbing(&waE2E.Message{MessageContextInfo: ctx, SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{}}) {
		t.Fatal("new fields inside messageContextInfo made key distribution look like content")
	}
}

// TestPlumbingIsWhatTheDecoderFindsEmpty pins the agreement the 2026-10-21
// review found broken: a message is dropped as plumbing exactly when the
// decoder has nothing to store for it — no text and no marker. Any other
// combination either drops content or stores a blank row.
func TestPlumbingIsWhatTheDecoderFindsEmpty(t *testing.T) {
	skd := &waE2E.SenderKeyDistributionMessage{}
	cases := map[string]*waE2E.Message{
		"sender keys":           {SenderKeyDistributionMessage: skd},
		"context only":          {MessageContextInfo: &waE2E.MessageContextInfo{}},
		"empty":                 {},
		"text":                  {Conversation: proto.String("hola")},
		"keys in an ephemeral":  {EphemeralMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{SenderKeyDistributionMessage: skd}}},
		"unknown at top":        withUnknownField(&waE2E.Message{}, 990),
		"unknown in ephemeral":  {EphemeralMessage: &waE2E.FutureProofMessage{Message: withUnknownField(&waE2E.Message{}, 991)}},
		"unknown in comment":    {CommentMessage: &waE2E.CommentMessage{Message: withUnknownField(&waE2E.Message{}, 992)}},
		"unknown around keys":   withUnknownField(&waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{SenderKeyDistributionMessage: skd}}}, 993),
		"revoke":                {ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum()}},
		"keys beside real text": {SenderKeyDistributionMessage: skd, Conversation: proto.String("hola")},
	}
	for _, shape := range parityShapes() {
		cases["parity "+shape.name] = shape.msg
	}
	for name, m := range cases {
		text, typ := extractContentFromProto(m)
		if empty := typ == "system" && text == ""; isPlumbing(m) != empty {
			t.Errorf("%s: isPlumbing=%t but the decoder gives (%q, %q)", name, isPlumbing(m), typ, text)
		}
		if strings.Contains(text, "unknown_field_") && hiddenFromChatPreview(m) {
			t.Errorf("%s: content from a newer WhatsApp is hidden from the chat preview", name)
		}
	}
}
