package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
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
