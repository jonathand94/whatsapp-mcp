package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

// baileysStore is the shape of baileys_store.json, the raw export
// written by the prior @whiskeysockets/baileys sync tool.
type baileysStore struct {
	Contacts map[string]baileysContact   `json:"contacts"`
	Messages map[string][]baileysMessage `json:"messages"`
}

type baileysContact struct {
	ID           string `json:"id"`
	Name         string `json:"name,omitempty"`
	Notify       string `json:"notify,omitempty"`
	VerifiedName string `json:"verifiedName,omitempty"`
}

type baileysMessage struct {
	Key              baileysKey     `json:"key"`
	Message          map[string]any `json:"message,omitempty"`
	MessageTimestamp any            `json:"messageTimestamp,omitempty"`
	PushName         string         `json:"pushName,omitempty"`
	MessageStubType  any            `json:"messageStubType,omitempty"`
	Status           any            `json:"status,omitempty"`
}

type baileysKey struct {
	RemoteJID   string `json:"remoteJid"`
	FromMe      bool   `json:"fromMe"`
	ID          string `json:"id"`
	Participant string `json:"participant,omitempty"`
}

// RunBaileysImport reads a baileys_store.json file and inserts its contacts,
// chats, and messages into our SQLite database. Idempotent via ON CONFLICT DO NOTHING.
// Returns counts for a final summary.
func RunBaileysImport(cfg *Config, db *sql.DB, storePath string) error {
	log.Printf("reading baileys store: %s", storePath)
	raw, err := os.ReadFile(storePath)
	if err != nil {
		return fmt.Errorf("read store file: %w", err)
	}

	var store baileysStore
	if err := json.Unmarshal(raw, &store); err != nil {
		return fmt.Errorf("parse store JSON: %w", err)
	}

	log.Printf("baileys store loaded: %d contacts, %d chat threads",
		len(store.Contacts), len(store.Messages))

	now := time.Now().Unix()

	// --- Import contacts ---
	contactsInserted := 0
	for jid, c := range store.Contacts {
		phone := extractPhone(jid)
		displayName := strings.TrimSpace(c.Notify)
		if displayName == "" {
			displayName = strings.TrimSpace(c.Name)
		}
		if displayName == "" {
			displayName = strings.TrimSpace(c.VerifiedName)
		}
		if displayName == "" {
			if phone != "" {
				displayName = "+" + phone
			} else {
				displayName = jid
			}
		}

		_, err := db.Exec(`
			INSERT INTO contacts (jid, phone, push_name, verified_name, normalized_name, is_business, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(jid) DO UPDATE SET
				push_name = COALESCE(NULLIF(excluded.push_name, ''), push_name),
				verified_name = COALESCE(NULLIF(excluded.verified_name, ''), verified_name),
				normalized_name = excluded.normalized_name,
				updated_at = excluded.updated_at
		`, jid, phone, displayName, c.VerifiedName, Normalize(displayName), 0, now, now)
		if err != nil {
			log.Printf("contact insert failed for %s: %v", jid, err)
			continue
		}
		contactsInserted++
	}
	log.Printf("contacts imported: %d", contactsInserted)

	// --- Import chats and messages ---
	chatsInserted := 0
	messagesInserted := 0
	messagesSkipped := 0
	groupsSkipped := 0

	for jid, msgs := range store.Messages {
		if len(msgs) == 0 {
			continue
		}
		chatType := chatTypeFromServerSuffix(jid)
		if chatType == "broadcast" {
			groupsSkipped++
			continue
		}

		// Derive a chat name: prefer contact displayName for directs, else group id suffix.
		var chatName string
		if c, ok := store.Contacts[jid]; ok {
			chatName = strings.TrimSpace(c.Notify)
			if chatName == "" {
				chatName = strings.TrimSpace(c.Name)
			}
		}
		if chatName == "" {
			if phone := extractPhone(jid); phone != "" {
				chatName = "+" + phone
			} else {
				chatName = jid
			}
		}

		// Walk messages and track last one for chat preview.
		var lastTs int64
		var lastPreview string
		msgsForChat := 0
		for _, m := range msgs {
			ts := coerceInt64(m.MessageTimestamp)
			if ts < 1000 {
				messagesSkipped++
				continue
			}
			content, msgType := baileysExtractContent(m.Message)
			if content == "" && msgType == "" {
				messagesSkipped++
				continue
			}
			if msgType == "" {
				msgType = "system"
			}

			senderJID := m.Key.RemoteJID
			if m.Key.Participant != "" {
				senderJID = m.Key.Participant
			}
			if m.Key.FromMe {
				// own messages: sender is empty in baileys key for directs
				senderJID = "" // we could fill in own device JID, not known from the store
			}

			senderDisplay := m.PushName
			if senderDisplay == "" {
				if c, ok := store.Contacts[senderJID]; ok {
					senderDisplay = c.Notify
					if senderDisplay == "" {
						senderDisplay = c.Name
					}
				}
			}

			normalized := Normalize(content)
			scrubbed, flags := Scrub(content)

			_, err := db.Exec(`
				INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, content_normalized, is_from_me, scrubbed_text, scrub_flags_json, raw_type)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(id) DO NOTHING
			`, m.Key.ID, jid, senderJID, senderDisplay, ts, msgType, content, normalized,
				boolToInt(m.Key.FromMe), scrubbed, ScrubFlagsJSON(flags), rawTypeNullable(msgType, content))
			if err != nil {
				log.Printf("message insert failed for %s/%s: %v", jid, m.Key.ID, err)
				messagesSkipped++
				continue
			}
			messagesInserted++
			msgsForChat++

			// A message dated in the future is a bad timestamp, not the
			// chat's newest message: it keeps its row but does not sort or
			// preview the chat (chat_time.go).
			if believableTime(ts, time.Now()) && ts > lastTs {
				lastTs = ts
				lastPreview = truncate(content, 120)
			}
		}

		if msgsForChat == 0 {
			continue
		}

		_, err := db.Exec(`
			INSERT INTO chats (jid, chat_type, name, normalized_name, created_at, updated_at, last_message_time, last_message_preview)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(jid) DO UPDATE SET
				last_message_time = MAX(excluded.last_message_time, last_message_time),
				last_message_preview = CASE WHEN excluded.last_message_time > last_message_time THEN excluded.last_message_preview ELSE last_message_preview END,
				name = COALESCE(NULLIF(excluded.name, ''), name),
				normalized_name = excluded.normalized_name,
				updated_at = excluded.updated_at
		`, jid, chatType, chatName, Normalize(chatName), now, now, lastTs, lastPreview)
		if err != nil {
			log.Printf("chat upsert failed for %s: %v", jid, err)
			continue
		}
		chatsInserted++
	}

	log.Printf("import complete: chats=%d messages=%d (skipped=%d, broadcasts_skipped=%d)",
		chatsInserted, messagesInserted, messagesSkipped, groupsSkipped)
	return nil
}

// --- baileys-shape helpers --------------------------------------------------

// baileysExtractContent returns (text, type) given a baileys message.message object.
// Text is empty for pure-media messages with no caption; type still set so we persist the row.
func baileysExtractContent(m map[string]any) (string, string) {
	if m == nil {
		return "", ""
	}
	// Unwrap ephemeral and viewOnce layers.
	if inner, ok := m["ephemeralMessage"].(map[string]any); ok {
		if msg, ok := inner["message"].(map[string]any); ok {
			return baileysExtractContent(msg)
		}
	}
	if inner, ok := m["viewOnceMessage"].(map[string]any); ok {
		if msg, ok := inner["message"].(map[string]any); ok {
			t, _ := baileysExtractContent(msg)
			return t, "image" // best guess; view-once is most commonly image
		}
	}

	if v, ok := m["conversation"].(string); ok && v != "" {
		return v, "text"
	}
	if ext, ok := m["extendedTextMessage"].(map[string]any); ok {
		if t, ok := ext["text"].(string); ok && t != "" {
			return t, "text"
		}
	}
	if img, ok := m["imageMessage"].(map[string]any); ok {
		caption, _ := img["caption"].(string)
		return caption, "image"
	}
	if vid, ok := m["videoMessage"].(map[string]any); ok {
		caption, _ := vid["caption"].(string)
		return caption, "video"
	}
	if aud, ok := m["audioMessage"].(map[string]any); ok {
		if ptt, _ := aud["ptt"].(bool); ptt {
			return "", "voice"
		}
		return "", "audio"
	}
	if doc, ok := m["documentMessage"].(map[string]any); ok {
		fname, _ := doc["fileName"].(string)
		caption, _ := doc["caption"].(string)
		if caption != "" {
			return caption, "document"
		}
		if fname != "" {
			return "[" + fname + "]", "document"
		}
		return "", "document"
	}
	if _, ok := m["stickerMessage"]; ok {
		return "", "sticker"
	}
	if loc, ok := m["locationMessage"].(map[string]any); ok {
		comment, _ := loc["comment"].(string)
		return comment, "location"
	}
	if ct, ok := m["contactMessage"].(map[string]any); ok {
		dn, _ := ct["displayName"].(string)
		return dn, "contact"
	}
	if react, ok := m["reactionMessage"].(map[string]any); ok {
		text, _ := react["text"].(string)
		return text, "reaction"
	}
	if poll, ok := m["pollCreationMessage"].(map[string]any); ok {
		name, _ := poll["name"].(string)
		return name, "text"
	}
	if raw := unsupportedBaileysType(m); raw != "" {
		// Fail LOUD (MYC-3284): a Baileys message carrying an undecoded content
		// field is imported with an explicit marker naming the raw type, never a
		// silent empty row. Metadata-only / empty stays a plain "system".
		// The marker rides in content_text — see extractContent for why the
		// `type` column cannot carry it (CHECK constraint, migrations/001).
		log.Printf("baileysExtractContent: undecoded imported message type %q — stored with an explicit unsupported marker (no text captured)", raw)
		return unsupportedMarker(raw), "system"
	}
	return "", "system"
}

// unsupportedBaileysType names the populated content key(s) of a Baileys message
// object baileysExtractContent does not decode (MYC-3284 sibling of
// unsupportedMessageType). messageContextInfo rides alongside real content, so
// it is skipped; no other key ⇒ genuinely textless ⇒ "" (caller keeps "system").
func unsupportedBaileysType(m map[string]any) string {
	var names []string
	for k := range m {
		if k != "messageContextInfo" {
			names = append(names, k)
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// chatTypeFromServerSuffix maps baileys JID suffixes to our chat_type enum.
// @s.whatsapp.net or @c.us -> direct
// @g.us -> group
// @broadcast -> broadcast
// @newsletter -> broadcast
func chatTypeFromServerSuffix(jid string) string {
	switch {
	case strings.HasSuffix(jid, "@g.us"):
		return "group"
	case strings.HasSuffix(jid, "@broadcast"):
		return "broadcast"
	case strings.HasSuffix(jid, "@newsletter"):
		return "broadcast"
	default:
		return "direct"
	}
}

func extractPhone(jid string) string {
	at := strings.IndexByte(jid, '@')
	if at < 0 {
		return ""
	}
	left := jid[:at]
	// Strip any :N device-suffix
	if colon := strings.IndexByte(left, ':'); colon >= 0 {
		left = left[:colon]
	}
	// Keep only digits
	var b strings.Builder
	for _, r := range left {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// coerceInt64 handles baileys timestamps which may be int64, float64, or object with "low" field.
func coerceInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		var n int64
		_, err := fmt.Sscanf(x, "%d", &n)
		if err != nil {
			return 0
		}
		return n
	case map[string]any:
		// protobuf long-int encoded as {low, high, unsigned}
		if low, ok := x["low"].(float64); ok {
			return int64(low)
		}
	}
	return 0
}
