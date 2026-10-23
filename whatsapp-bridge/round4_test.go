package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// TestHistorySenderReadsTheParticipantLikeWhatsmeow: history sync puts a group
// message's sender on the message more often than in its key; reading only the
// key filed 130 of 200 messages of one group as sent by the group (2026-10-23).
func TestHistorySenderReadsTheParticipantLikeWhatsmeow(t *testing.T) {
	const group, own = "120363000000018375@g.us", "5215550175084@s.whatsapp.net"
	msg := func(fromMe bool, onMessage, inKey string) *waWeb.WebMessageInfo {
		wm := &waWeb.WebMessageInfo{Key: &waCommon.MessageKey{FromMe: proto.Bool(fromMe)}}
		if onMessage != "" {
			wm.Participant = proto.String(onMessage)
		}
		if inKey != "" {
			wm.Key.Participant = proto.String(inKey)
		}
		return wm
	}
	for _, c := range []struct {
		chat string
		wm   *waWeb.WebMessageInfo
		want string
	}{
		{group, msg(false, "111@lid", ""), "111@lid"},
		{group, msg(false, "", "222@lid"), "222@lid"},
		{group, msg(false, "111@lid", "222@lid"), "111@lid"},
		{group, msg(true, "111@lid", ""), own},
		{"5215550000001@s.whatsapp.net", msg(false, "111@lid", ""), "5215550000001@s.whatsapp.net"},
		{group, msg(false, "", ""), group},
	} {
		if got := historySender(c.chat, own, c.wm); got != c.want {
			t.Errorf("historySender(%s, %+v) = %s, want %s", c.chat, c.wm, got, c.want)
		}
	}
}

func TestGroupSenderRepairOnlyTouchesGroupFiledRows(t *testing.T) {
	db := undecodedTestDB(t)
	b := &Bridge{db: db}
	const group = "120363000000018375@g.us"
	insertTestMessage(t, db, "FILED", group, "text", "hola", 100)
	insertTestMessage(t, db, "KNOWN", group, "text", "adios", 101)
	if _, err := db.Exec(`UPDATE messages SET sender_jid = chat_jid WHERE id = 'FILED'`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"FILED", "KNOWN"} {
		if err := b.repairGroupSender(id, group, "333@lid", "Ana"); err != nil {
			t.Fatal(err)
		}
	}
	var filed, known string
	db.QueryRow(`SELECT sender_jid FROM messages WHERE id = 'FILED'`).Scan(&filed)
	db.QueryRow(`SELECT sender_jid FROM messages WHERE id = 'KNOWN'`).Scan(&known)
	if filed != "333@lid" || known == "333@lid" {
		t.Fatalf("FILED sender %s (want 333@lid), KNOWN sender %s (must not change)", filed, known)
	}
}

// TestNoRetryReceiptWithoutTheParticipant: the phone ignores a receipt that
// names the group as the sender, so the bridge waited two minutes for nothing.
func TestNoRetryReceiptWithoutTheParticipant(t *testing.T) {
	const group = "120363000000018375@g.us"
	if _, err := messageInfoForRetry("X", group, group, false); err == nil || !strings.Contains(err.Error(), "quien envio") {
		t.Fatalf("want a refusal naming the missing sender, got %v", err)
	}
	if _, err := messageInfoForRetry("X", group, "333@lid", false); err != nil {
		t.Fatalf("a group message with its participant: %v", err)
	}
}

// TestASendMovesItsChatUp: on 2026-10-23 the user's own chat still showed an
// older file after ten sends.
func TestASendMovesItsChatUp(t *testing.T) {
	s, db := newMediaTestServer(t)
	own := types.NewJID("5215550175084", types.DefaultUserServer)
	seedChatAt(t, db, own.String(), "direct", "Yo", 1000, 0)
	text := &waE2E.Message{Conversation: proto.String("Prueba ronda 3")}
	if err := s.recordSendInChat(t.Context(), own.String(), own, text, "3EB0NEW", 2000, "Prueba ronda 3"); err != nil {
		t.Fatal(err)
	}
	// A reaction is not a preview; an older send does not move the chat back.
	reaction := &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("")}}
	if err := s.recordSendInChat(t.Context(), own.String(), own, reaction, "3EB0R", 3000, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.recordSendInChat(t.Context(), own.String(), own, text, "3EB0OLD", 1500, "viejo"); err != nil {
		t.Fatal(err)
	}
	var id, preview string
	var ts int64
	db.QueryRow(`SELECT last_message_id, last_message_time, last_message_preview FROM chats WHERE jid = ?`, own.String()).
		Scan(&id, &ts, &preview)
	if id != "3EB0NEW" || ts != 2000 || preview != "Prueba ronda 3" {
		t.Fatalf("own chat = (%s, %d, %q), want the send", id, ts, preview)
	}
	// A first message to someone creates the chat.
	stranger := types.NewJID("5215550009999", types.DefaultUserServer)
	if err := s.recordSendInChat(t.Context(), stranger.String(), stranger, text, "3EB0FIRST", 2100, "hola"); err != nil {
		t.Fatal(err)
	}
	var n int
	db.QueryRow(`SELECT COUNT(*) FROM chats WHERE jid = ? AND chat_type = 'direct'`, stranger.String()).Scan(&n)
	if n != 1 {
		t.Fatal("the first send to someone did not create their chat")
	}
}

func TestOldMarkersSayWhatTheyWere(t *testing.T) {
	for text, want := range map[string]string{
		"[unsupported: albumMessage]":                   "[Álbum de fotos o videos: mensaje antiguo, su contenido no se guardó]",
		"[unsupported: pollCreationMessageV3]":          "[Encuesta: mensaje antiguo, su contenido no se guardó]",
		"[unsupported: templateMessage]":                "[Mensaje de plantilla de una empresa: mensaje antiguo, su contenido no se guardó]",
		"[unsupported: someFutureMessage]":              "",
		"[unsupported: albumMessage,someFutureMessage]": "",
		"hola": "",
	} {
		if got := friendlyMarker(text); got != want {
			t.Errorf("friendlyMarker(%q) = %q, want %q", text, got, want)
		}
	}
}

// TestListsSpeakInWords: unnamed direct chats get a name, previews and
// messages that are old markers say what they were, and a group message filed
// under the group is from an unknown participant, not a fake person.
func TestListsSpeakInWords(t *testing.T) {
	s, db := newMediaTestServer(t)
	const lid, phone, group = "99990000002312@lid", "5215550000777@s.whatsapp.net", "120363000000018375@g.us"
	seedChatAt(t, db, lid, "direct", "99990000002312", 300, 0)
	seedChatAt(t, db, group, "group", "Socios", 200, 0)
	aliasPair(t, db, lid, phone)
	if _, err := db.Exec(`UPDATE chats SET last_message_preview = '[unsupported: albumMessage]' WHERE jid = ?`, group); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, is_from_me) VALUES
		  ('ALB', ?, ?, '', 200, 'system', '[unsupported: albumMessage]', 0)`, group, group); err != nil {
		t.Fatal(err)
	}

	chats := getChats(t, s, s.handleListChats, "/api/chats?limit=10")
	byJID := map[string]chatRow{}
	for _, c := range chats.Chats {
		byJID[c.JID] = c
	}
	if byJID[lid].Name != "+5215550000777" {
		t.Errorf("unnamed @lid chat is named %q, want its phone", byJID[lid].Name)
	}
	if !strings.HasPrefix(byJID[group].LastMessagePreview, "[Álbum de fotos o videos") {
		t.Errorf("group preview = %q", byJID[group].LastMessagePreview)
	}

	rec := httptest.NewRecorder()
	s.handleListMessages(rec, httptest.NewRequest(http.MethodGet, "/api/messages?chat_jid="+group, nil))
	var out messageListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Messages) != 1 {
		t.Fatalf("%v: %s", err, rec.Body)
	}
	m := out.Messages[0]
	if m.RawMarker != "[unsupported: albumMessage]" || !strings.HasPrefix(m.ContentText, "[Álbum") ||
		m.SenderDisplay != "Participante desconocido" {
		t.Fatalf("message = %+v", m)
	}
}
