// undecryptable.go — persist a row for a message that never reached the decoder
// (MYC-3569).
//
// MYC-3284 fixed the case where a message is RECEIVED and DECODED into an empty
// row: it now keeps a loud "[unsupported: <rawType>]" marker. This file covers
// the strictly worse case one layer earlier.
//
// whatsmeow dispatches events.UndecryptableMessage and RETURNS when a message
// cannot be decrypted — no events.Message follows. Until now handleEvent had no
// case for it, so the bridge wrote nothing: no row, no marker, no counter. The
// message exists for the human in the WhatsApp client and does not exist for the
// brain in any form. MYC-3284's fail-loud guarantee cannot reach it, because
// extractContent is never called. There is not even an empty row to count, so
// export, /healthcheck and retrieval could not report the absence either.
//
// The fix mirrors MYC-3284's idiom deliberately: a row built from evt.Info alone
// (id, chat, sender, timestamp, is_from_me) carrying a distinct, queryable
// marker in content_text. Same reasoning as there for why the signal rides in
// content_text and not in a new `type` value: messages.type has a CHECK
// constraint (migrations/001) that SQLite cannot widen without a full table
// rebuild, which is a disproportionate risk to a live 113k-row store.
//
// The rows are RECOVERABLE, which is what makes the upgrade path load-bearing.
// whatsmeow asks the sender for a resend on our behalf (sendRetryReceipt /
// immediateRequestMessageFromPhone), so a share of these arrive later as a
// normal events.Message on the SAME message id. onMessage's upsert is what
// turns that arrival into an in-place upgrade rather than a no-op — see the
// ON CONFLICT clause in bridge.go, which is written against this file's marker.
package main

import (
	"context"
	"log"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types/events"
)

// The content_text marker stored for a message that could not be decrypted.
// ONE declaration shared by the writer (onUndecryptableMessage) and every reader
// (vault export, the /healthcheck by-mode counts, onMessage's upgrade
// predicate), so what is written and what is parsed back cannot drift — the same
// contract MYC-3284 established for unsupportedPrefix.
const (
	undecryptablePrefix = "[undecryptable: "
	undecryptableSuffix = "]"
)

// undecryptableMarker renders the content_text stored for an undecryptable message.
func undecryptableMarker(mode string) string {
	return undecryptablePrefix + mode + undecryptableSuffix
}

// undecryptableFailMode recovers the failure mode from a stored marker, or ""
// when the text is not one (a decoded message, an [unsupported: ...] row, or a
// legacy row from before this shipped).
func undecryptableFailMode(text string) string {
	if !strings.HasPrefix(text, undecryptablePrefix) || !strings.HasSuffix(text, undecryptableSuffix) {
		return ""
	}
	return text[len(undecryptablePrefix) : len(text)-len(undecryptableSuffix)]
}

// decryptFailureMode names the failure using the fields the event actually
// carries. whatsmeow's WARN log distinguishes finer causes ("no sender key for
// <lid>") but does NOT put them on the event, so naming one here would be
// inventing detail the bridge cannot observe. These four fields are what
// events.UndecryptableMessage exposes:
//
//	unavailable      — the sender's device never sent this device a ciphertext
//	decrypt-failed   — a ciphertext arrived and would not decrypt
//	:<type>          — whatsmeow flagged the type as intentionally unavailable
//	:hide            — WhatsApp asks clients to hide this failure from the user
//
// The :hide rows are still persisted. Hiding a failure is a rendering decision
// for a chat UI; for a brain whose contract is "no silent loss", the row must
// exist and say why. The mode is on it so a reader can choose.
func decryptFailureMode(evt *events.UndecryptableMessage) string {
	if evt == nil {
		return "unknown"
	}
	mode := "decrypt-failed"
	if evt.IsUnavailable {
		mode = "unavailable"
	}
	// UnavailableType is server-supplied. It is sanitized because it flows into
	// a stored marker that readers parse back: an unsanitized "]" would truncate
	// the marker and make the row unrecoverable by undecryptableFailMode, which
	// is the same silent-loss class this ticket exists to close.
	if t := sanitizeModeToken(string(evt.UnavailableType)); t != "" {
		mode += ":" + t
	}
	if evt.DecryptFailMode == events.DecryptFailHide {
		mode += ":hide"
	}
	return mode
}

// sanitizeModeToken keeps a server-supplied token to the conservative alphabet
// the marker round-trip is safe for. Anything else is dropped rather than
// escaped — the token is a label, not data we need to reproduce faithfully.
func sanitizeModeToken(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		}
	}
	if b.Len() > 40 {
		return b.String()[:40]
	}
	return b.String()
}

// onUndecryptableMessage persists a row for a message the bridge could not
// decrypt, so the gap is visible and countable while whatsmeow's retry request
// is outstanding — and permanently, if the retry never lands.
//
// The INSERT is ON CONFLICT DO NOTHING: it must never DOWNGRADE a row that
// already holds real content. The reverse direction (placeholder replaced by the
// recovered message) is handled by onMessage's upsert.
func (b *Bridge) onUndecryptableMessage(evt *events.UndecryptableMessage) {
	if evt == nil {
		return
	}
	chatJID := evt.Info.Chat.String()
	senderJID := evt.Info.Sender.String()
	id := evt.Info.ID
	ts := evt.Info.Timestamp.Unix()
	// messages.id is the PRIMARY KEY. An empty id would make every id-less
	// event collide onto ONE row, so a second undecryptable message would be
	// swallowed by ON CONFLICT DO NOTHING — silently, which is the exact
	// failure this ticket exists to remove. Refuse loudly instead.
	if id == "" {
		log.Printf("onUndecryptableMessage: dropping an undecryptable event with no message id (chat=%s) — cannot key a row on it", chatJID)
		return
	}
	mode := decryptFailureMode(evt)
	marker := undecryptableMarker(mode)

	senderDisplay := evt.Info.PushName
	if senderDisplay == "" {
		senderDisplay = evt.Info.Sender.User
	}

	// Chat row first: messages.chat_jid is a FOREIGN KEY onto chats(jid), so a
	// message in a chat we have not seen would be rejected outright — which
	// would reintroduce the silent drop through the back door.
	//
	// The placeholder DOES take over last_message_preview. It is genuinely the
	// most recent message in that chat, and showing the member an honest "we
	// could not read this" beats showing a stale preview that implies nothing
	// arrived.
	//
	// Like onMessage, the sender's push name only names DIRECT chats: a group
	// named after one member is the MYC-3555 person-named-group-file bug.
	chatName := senderDisplay
	if chatTypeFromJID(evt.Info.Chat) != "direct" {
		chatName = ""
	}
	_, err := b.db.Exec(`
		INSERT INTO chats (jid, chat_type, name, normalized_name, created_at, updated_at, last_message_id, last_message_time, last_message_preview)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET
			last_message_id = excluded.last_message_id,
			last_message_time = excluded.last_message_time,
			last_message_preview = excluded.last_message_preview,
			updated_at = excluded.updated_at
	`, chatJID, chatTypeFromJID(evt.Info.Chat), chatName, Normalize(chatName),
		ts, ts, id, chatSortTime(ts, time.Now()), marker)
	if err != nil {
		log.Printf("onUndecryptableMessage: chat upsert failed: %v", err)
	}

	res, err := b.db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, content_normalized, is_from_me, scrubbed_text, scrub_flags_json, raw_type)
		VALUES (?, ?, ?, ?, ?, 'system', ?, ?, ?, ?, NULL, ?)
		ON CONFLICT(id) DO NOTHING
	`, id, chatJID, senderJID, senderDisplay, ts, marker, Normalize(marker),
		boolToInt(evt.Info.IsFromMe), marker, rawTypeNullable("system", marker))
	if err != nil {
		log.Printf("onUndecryptableMessage: message insert failed: %v", err)
		return
	}

	// Only WARN when a row was actually created. whatsmeow can dispatch the same
	// undecryptable id more than once (a retry that also fails); logging every
	// dispatch would overstate the loss rate that /healthcheck reports.
	if n, rowsErr := res.RowsAffected(); rowsErr == nil && n == 0 {
		return
	}
	log.Printf("onUndecryptableMessage: message %s in %s could not be decrypted (mode=%s) — stored with an explicit undecryptable marker; a successful retry will upgrade this row in place", id, chatJID, mode)

	// Record the alias edge for direct chats, same as onMessage: these senders
	// are overwhelmingly @lid, and skipping it here would leave orphaned rows
	// that search and CRM lookups miss (see aliases.go).
	if !evt.Info.IsGroup {
		recordJIDAlias(context.Background(), b.db, evt.Info.Sender, evt.Info.SenderAlt, "message_sender", ts)
	}
}

// The by-mode counting that used to live here moved to splitRawTypeCount in
// raw_type.go (MYC-3577). The counters now read the indexed messages.raw_type
// column rather than re-parsing a marker out of content_text on every call, so
// there is one place that turns a stored value into a counter bucket.
