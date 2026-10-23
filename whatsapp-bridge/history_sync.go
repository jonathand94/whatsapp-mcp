// history_sync.go — programmatic history-sync backfill for media keys.
//
// Background: the bridge originally lost media-key fields for image / video /
// document / sticker messages received before the 2026-05-08 patch (it only
// persisted them for audio). The follow-up patch fixes the live receive path,
// but does NOT recover historical messages that already exist in the messages
// table with NULL media-key fields.
//
// This file fixes the historical gap programmatically. WhatsApp's history-sync
// protocol delivers the same waE2E.Message protos that arrive via live receive,
// inside HistorySyncMsg wrappers attached to Conversation entries. Processing
// those events lets us backfill media-key fields without flipping the global
// AutoDownloadMedia flag, without asking the user to manually export a chat,
// and without storing duplicate media bytes anywhere.
//
// Two halves:
//
//   1. processHistorySyncEvent — invoked from the live event handler when
//      whatsmeow delivers a *events.HistorySync. Walks every Conversation's
//      messages, extracts media-key fields, and runs an UPDATE that only
//      writes when the existing row's media_key is NULL (so we don't clobber
//      good values with stale ones).
//
//   2. RequestChatHistory — finds the oldest message we have for a chat and
//      sends a peer HistorySyncOnDemandRequest asking WhatsApp for `count`
//      messages immediately before that one. WhatsApp delivers the response
//      asynchronously over the events.HistorySync stream, which #1 picks up.
//
// Net effect: a single POST /api/admin/request-history call against a chat
// triggers the backfill, and the receipts pipeline (or any other consumer)
// can immediately decrypt the historical media via the existing
// /api/media/download endpoint.

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// extractFromMessage is the inner-type variant of extractDownloadableFields.
// Live receive (events.Message) wraps the proto in evt.Message; HistorySync
// delivery wraps it in HistorySyncMsg.GetMessage().GetMessage(). Both reach
// the same *waE2E.Message, so both paths share one extractor.
//
// It used to be a copy of that extractor, minus the envelope unwrapping that
// whatsmeow only does for live events. A history-sync document with a
// caption, or any media in a disappearing chat, arrives wrapped — so its row
// got type 'document' from the content decoder (which does unwrap) and no
// media keys from here, and download_media failed on it with "media key not
// available". Delegating keeps the two from drifting again.
func extractFromMessage(m *waE2E.Message) (mediaFields, bool) {
	return extractDownloadableFieldsFromProto(m)
}

// processHistorySyncEvent updates messages.media_key for any rows currently
// holding NULL keys when WhatsApp re-delivers them via history sync.
//
// COALESCE in the UPDATE makes this a no-op for rows that already have keys,
// so re-running on overlapping history chunks is safe.
func (b *Bridge) processHistorySyncEvent(evt *events.HistorySync) {
	if evt == nil || evt.Data == nil {
		return
	}
	chunk := evt.Data
	convs := chunk.GetConversations()
	if len(convs) == 0 {
		return
	}

	// Our own JID, used to attribute is_from_me=1 messages to a sender. Empty
	// only if somehow unpaired mid-sync; then from-me rows get an empty sender.
	var ownJID string
	if id := b.client.Store.ID; id != nil {
		ownJID = id.ToNonAD().String()
	}

	inserted := 0
	updated := 0
	scanned := 0
	mediaSeen := 0
	decoded := 0
	for _, conv := range convs {
		chatJID := conv.GetID()
		msgs := conv.GetMessages()
		log.Printf("history_sync: conversation %s has %d messages", chatJID, len(msgs))
		if chatJID == "" {
			continue
		}

		// Ensure the chat row exists BEFORE inserting messages: messages.chat_jid
		// has a FOREIGN KEY to chats(jid) and the message DB runs with foreign_keys
		// ON, so a missing parent row would fail every insert.
		newestTS, newestID, newestPreview := newestForChatList(msgs, time.Now())
		if err := b.upsertHistoryChat(chatJID, conv.GetName(), newestTS, newestID, newestPreview); err != nil {
			log.Printf("history_sync: chat upsert %s failed: %v", chatJID, err)
			continue
		}

		// Track the OLDEST message in this conversation's chunk. It becomes
		// the anchor for the next step backwards (backfill_walk.go); without
		// it the walk would re-request the same window forever.
		var oldest chunkAnchor

		for _, hsMsg := range msgs {

			scanned++
			wm := hsMsg.GetMessage()
			if wm == nil {
				continue
			}
			key := wm.GetKey()
			if key == nil || key.GetID() == "" {
				continue
			}

			content, msgType := extractContentFromProto(wm.GetMessage())
			ts := int64(wm.GetMessageTimestamp())
			fromMe := key.GetFromMe()

			senderJID := chatJID
			switch {
			case fromMe:
				senderJID = ownJID
			case key.GetParticipant() != "":
				senderJID = key.GetParticipant()
			}
			senderDisplay := wm.GetPushName()
			if senderDisplay == "" {
				senderDisplay = senderJID
			}

			normalized := Normalize(content)
			scrubbed, flags := Scrub(content)
			mfields, _ := extractFromMessage(wm.GetMessage())

			// Insert the historical message. ON CONFLICT(id) DO NOTHING makes
			// overlapping history chunks (and re-pairing) idempotent, and leaves
			// any live-received row untouched. Columns mirror onMessage exactly,
			// so a backfilled row is indistinguishable from a live one.
			// Plumbing (see isPlumbing) is not stored; the walk below still
			// counts it, since it is part of the chunk's history.
			if !isPlumbing(wm.GetMessage()) {
				res, err := b.db.Exec(`
					INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, content_normalized, is_from_me, scrubbed_text, scrub_flags_json,
						media_key, media_direct_path, media_url, media_enc_sha256, media_sha256, media_file_length, media_key_timestamp, media_mime, media_filename, raw_type)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
					ON CONFLICT(id) DO NOTHING
				`, key.GetID(), chatJID, senderJID, senderDisplay, ts, msgType, content, normalized,
					boolToInt(fromMe), scrubbed, ScrubFlagsJSON(flags),
					mfields.MediaKey, mfields.MediaDirectPath, mfields.MediaURL,
					mfields.MediaEncSHA, mfields.MediaSHA, mfields.MediaFileLength,
					mfields.MediaKeyTimestamp, mfields.MediaMime, mfields.MediaFilename,
					// Without raw_type a marker stored from history never
					// reached /healthcheck's decode counters (found 2026-10-21).
					rawTypeNullable(msgType, content))
				if err != nil {
					log.Printf("history_sync: insert %s failed: %v", key.GetID(), err)
					continue
				}
				if n, _ := res.RowsAffected(); n > 0 {
					inserted += int(n)
				}
			}

			// Extend the chunk's backward anchor to this message if it is the
			// oldest seen so far (see the `oldest` declaration above).
			if ts > 0 && (oldest.TS == 0 || ts < oldest.TS) {
				oldest = chunkAnchor{ID: key.GetID(), TS: ts, FromMe: fromMe}
			}

			// Re-decode rows the pre-MYC-3284 decoder stored empty. Runs BEFORE
			// the media branch and must not share its `continue`: a recoverable
			// row is usually not media-bearing at all — the common case is text
			// that arrived inside an envelope the old switch could not see.
			if n, err := b.backfillDecodedContent(key.GetID(), wm.GetMessage()); err != nil {
				log.Printf("history_sync: content backfill %s failed: %v", key.GetID(), err)
			} else {
				decoded += n
			}

			// For rows that already existed with NULL media keys (pre-patch live
			// receives), still backfill the media fields via COALESCE — the
			// file's original purpose. A no-op for the row we just inserted.

			fields, ok := extractFromMessage(wm.GetMessage())
			if !ok || len(fields.MediaKey) == 0 {
				continue
			}
			mediaSeen++

			// Rows stored before migration 007 have their keys but no
			// document name, so the keys-only UPDATE below skips them. A
			// re-delivered document is the only chance to recover the name
			// the sender gave it.
			if fields.MediaFilename.Valid {
				if _, err := b.db.Exec(`
					UPDATE messages SET media_filename = ?
					 WHERE id = ? AND media_filename IS NULL
				`, fields.MediaFilename, key.GetID()); err != nil {
					log.Printf("history_sync: filename backfill %s failed: %v", key.GetID(), err)
				}
			}
			ures, err := b.db.Exec(`
				UPDATE messages
				   SET media_key            = COALESCE(media_key, ?),
				       media_direct_path    = COALESCE(media_direct_path, ?),
				       media_url            = COALESCE(media_url, ?),
				       media_enc_sha256     = COALESCE(media_enc_sha256, ?),
				       media_sha256         = COALESCE(media_sha256, ?),
				       media_file_length    = COALESCE(media_file_length, ?),
				       media_key_timestamp  = COALESCE(media_key_timestamp, ?),
				       media_mime           = COALESCE(media_mime, ?)
				 WHERE id = ?
				   AND media_key IS NULL
			`,
				fields.MediaKey, fields.MediaDirectPath, fields.MediaURL,
				fields.MediaEncSHA, fields.MediaSHA, fields.MediaFileLength,
				fields.MediaKeyTimestamp, fields.MediaMime, key.GetID(),
			)
			if err != nil {
				log.Printf("history_sync: update %s failed: %v", key.GetID(), err)
				continue
			}
			n, _ := ures.RowsAffected()
			updated += int(n)
		}

		// Decide whether this chat keeps walking backwards. Every stop
		// condition lives in the walker, so delivery just reports where the
		// chunk ended and obeys the verdict.
		if conv.GetID() != "" {
			b.continueWalk(context.Background(), conv.GetID(), oldest)
		}
	}
	log.Printf("history_sync: scanned %d msgs across %d conversations (progress=%d), inserted %d new, saw %d media-bearing, backfilled %d media rows, re-decoded %d empty rows",
		scanned, len(convs), chunk.GetProgress(), inserted, mediaSeen, updated, decoded)
}

// newestForChatList picks the message a history chunk sorts and previews its
// chat by: the newest one, leaving out what the live path keeps out of chat
// previews (hiddenFromChatPreview) and anything dated more than futureSlack
// ahead, which in history is a bad timestamp rather than news (chat_time.go).
// ts is 0 when no message qualifies.
func newestForChatList(msgs []*waHistorySync.HistorySyncMsg, now time.Time) (ts int64, id, preview string) {
	for _, hsMsg := range msgs {
		wm := hsMsg.GetMessage()
		if wm == nil || hiddenFromChatPreview(wm.GetMessage()) {
			continue
		}
		t := int64(wm.GetMessageTimestamp())
		if !believableTime(t, now) || t < ts {
			continue
		}
		ts, id = t, wm.GetKey().GetID()
		c, _ := extractContentFromProto(wm.GetMessage())
		preview = truncate(c, 120)
	}
	return ts, id, preview
}

// upsertHistoryChat creates a history chunk's chat row, or brings an existing
// one forward to the chunk's newest message (ts, id, preview from
// newestForChatList) when that is newer than what the row has. A live chat's
// fresher values are never regressed, and the time, id and preview move
// together: until 2026-10-21 only the time moved, leaving a preview from an
// older message under it.
func (b *Bridge) upsertHistoryChat(chatJID, name string, ts int64, id, preview string) error {
	_, err := b.db.Exec(`
		INSERT INTO chats (jid, chat_type, name, normalized_name, last_message_id, last_message_time, last_message_preview, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET
			name                 = COALESCE(chats.name, excluded.name),
			normalized_name      = COALESCE(chats.normalized_name, excluded.normalized_name),
			last_message_id      = CASE WHEN excluded.last_message_time > COALESCE(chats.last_message_time, 0)
			                            THEN excluded.last_message_id ELSE chats.last_message_id END,
			last_message_preview = CASE WHEN excluded.last_message_time > COALESCE(chats.last_message_time, 0)
			                            THEN excluded.last_message_preview ELSE chats.last_message_preview END,
			last_message_time    = MAX(COALESCE(chats.last_message_time, 0), excluded.last_message_time),
			updated_at           = excluded.updated_at
	`, chatJID, chatTypeFromJIDString(chatJID), nullIfEmpty(name), nullIfEmpty(Normalize(name)),
		nullIfEmpty(id), ts, nullIfEmpty(preview), ts, ts)
	return err
}

// chatTypeFromJIDString parses a raw JID string and returns the chats.chat_type
// value. Falls back to "direct" when the JID cannot be parsed, so the CHECK
// constraint on chats.chat_type is always satisfied.
func chatTypeFromJIDString(jid string) string {
	j, err := types.ParseJID(jid)
	if err != nil {
		return "direct"
	}
	return chatTypeFromJID(j)
}

// nullIfEmpty maps "" to a NULL so nullable TEXT columns stay NULL rather than
// storing empty strings (keeps COALESCE-based upserts meaningful).
func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// RequestChatHistory triggers WhatsApp to deliver `count` messages immediately
// before the oldest message we currently have for `chatJID`. The response
// arrives asynchronously as one or more events.HistorySync events, processed
// by processHistorySyncEvent.
//
// Returns the oldest known message id we anchored on, plus the SendResponse
// from whatsmeow so the operator can correlate.
//
// If no messages exist for the chat yet, returns an error — there's nothing
// to anchor the request on. (For brand-new chats, just wait for live receive.)
func (b *Bridge) RequestChatHistory(ctx context.Context, chatJID string, count int) (anchorID string, resp whatsmeow.SendResponse, err error) {
	if !b.IsConnected() {
		err = errors.New("bridge not connected; cannot request history")
		return
	}
	if count <= 0 {
		count = 50
	}
	if count > 200 {
		count = 200
	}

	// Find the NEWEST message we have for this chat. WhatsApp's
	// HistorySyncOnDemandRequest returns the `count` messages immediately
	// BEFORE the given anchor — so anchoring on the newest gives us the
	// historical chunk we already have rows for (but where media-key
	// fields are NULL because they weren't persisted at receive time).
	// WhatsApp re-delivers the full proto including media keys, and
	// processHistorySyncEvent fills them in via COALESCE.
	//
	// Earlier mistake: anchoring on the OLDEST message asks for pre-history
	// (messages before the chat started), which always returns zero results.
	//
	// Note this reaches only the most recent `count` messages, and re-calling
	// it returns the SAME window every time. Walking further back is
	// RequestHistoryBefore's job — see backfill_walk.go.
	var (
		msgID    string
		ts       int64
		isFromMe int
	)
	err = b.db.QueryRowContext(ctx, `
		SELECT id, timestamp, is_from_me
		FROM messages
		WHERE chat_jid = ?
		ORDER BY timestamp DESC
		LIMIT 1
	`, chatJID).Scan(&msgID, &ts, &isFromMe)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = fmt.Errorf("no existing messages for chat %s; cannot anchor history request", chatJID)
			return
		}
		err = fmt.Errorf("query oldest message: %w", err)
		return
	}
	anchorID = msgID

	resp, err = b.RequestHistoryBefore(ctx, chatJID, msgID, ts, isFromMe == 1, count)
	return
}

// RequestHistoryBefore asks WhatsApp for the `count` messages immediately
// before an EXPLICIT anchor, rather than before the newest row we hold.
//
// This is what makes walking backwards possible. RequestChatHistory always
// anchors on the newest message, so calling it repeatedly returns the same
// most-recent window forever — measured live 2026-08-01: a second sweep of 40
// chats delivered 8 more chunks and recovered 0 additional rows. Feeding the
// OLDEST message of each delivered chunk back in as the next anchor is what
// actually advances through a chat's history.
func (b *Bridge) RequestHistoryBefore(ctx context.Context, chatJID, anchorID string, anchorTS int64, anchorFromMe bool, count int) (resp whatsmeow.SendResponse, err error) {
	if !b.IsConnected() {
		err = errors.New("bridge not connected; cannot request history")
		return
	}
	if anchorID == "" {
		err = errors.New("anchor message id required")
		return
	}
	if count <= 0 {
		count = 50
	}
	if count > 200 {
		count = 200
	}

	msgID, ts := anchorID, anchorTS
	isFromMe := 0
	if anchorFromMe {
		isFromMe = 1
	}

	chat, parseErr := types.ParseJID(chatJID)
	if parseErr != nil {
		err = fmt.Errorf("invalid chat_jid: %w", parseErr)
		return
	}

	mi := &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     chat,
			IsFromMe: isFromMe == 1,
		},
		ID:        msgID,
		Timestamp: time.Unix(ts, 0),
	}
	reqMsg := b.client.BuildHistorySyncRequest(mi, count)

	// Self-peer send. The HistorySyncOnDemandRequest is a peer-data-operation
	// message addressed to our own device; WhatsApp interprets it server-side
	// and replies via the history-sync stream. Setting Peer=true on the
	// SendRequestExtra is required for whatsmeow to route as a peer message.
	ownJID := b.client.Store.ID
	if ownJID == nil {
		err = errors.New("bridge has no device id; cannot send peer message")
		return
	}
	resp, err = b.client.SendMessage(ctx, ownJID.ToNonAD(), reqMsg, whatsmeow.SendRequestExtra{Peer: true})
	if err != nil {
		err = fmt.Errorf("send history-sync request: %w", err)
		return
	}
	log.Printf("history_sync: requested %d messages for %s before %s (ts=%d)",
		count, chatJID, msgID, ts)
	return
}

// --- MYC-3284 content backfill ---------------------------------------------

// backfillDecodedContent re-decodes one history-sync message and repairs the
// stored row if — and only if — that row was written empty by the pre-MYC-3284
// decoder.
//
// The WHERE clause is the entire safety argument, so it is worth stating
// plainly. A row is eligible only when BOTH hold:
//
//	type = 'system'                          the old catch-all bucket
//	content_text IS NULL OR content_text=''  it actually has no payload
//
// So this can only ever turn a blank into something. It cannot overwrite text,
// cannot touch a media row, and cannot disturb a row the current decoder wrote
// (those carry either real text or an "[unsupported: …]" marker, and a marker
// is non-empty). Overlapping history chunks are therefore idempotent, and a
// stale re-delivery cannot clobber a fresher live row.
//
// A genuinely textless protocol carrier re-decodes to ("", "system") and the
// UPDATE is a no-op write of identical values, which is the correct outcome:
// key-distribution rows stay silent rather than gaining vault noise.
func (b *Bridge) backfillDecodedContent(msgID string, m *waE2E.Message) (int, error) {
	if msgID == "" || m == nil {
		return 0, nil
	}

	// Same decoder as live receive (see extractContentFromProto), so a
	// backfilled row is byte-identical to what it would have been had the
	// message arrived today.
	text, msgType := extractContentFromProto(m)
	if text == "" && msgType == "system" {
		// Nothing recovered — leave the row exactly as it is rather than
		// rewriting it with the same emptiness. A photo or voice note with no
		// caption is something recovered: its type.
		return 0, nil
	}
	// A row stored as a marker by a decoder that did not know its type yet
	// (6,798 of them on 2026-10-22: templates, albums and their photos,
	// polls...) is re-decoded too — but only when the new answer is not a
	// marker again, and the message delivered now really carries the type
	// the marker names, so text from any other message can never land there.
	var stored string
	if err := b.db.QueryRow(`SELECT COALESCE(content_text, '') FROM messages WHERE id = ? AND type = 'system'`,
		msgID).Scan(&stored); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	if raw := unsupportedRawType(stored); raw != "" &&
		(unsupportedRawType(text) != "" || !carriesFields(m, strings.Split(raw, ","))) {
		return 0, nil
	}

	scrubbed, flags := Scrub(text)
	res, err := b.db.Exec(`
		UPDATE messages
		   SET type               = ?,
		       content_text       = ?,
		       content_normalized = ?,
		       scrubbed_text      = ?,
		       scrub_flags_json   = ?,
		       -- Set alongside content_text, never separately (MYC-3577). This
		       -- row is moving from the legacy-empty population into either
		       -- "decoded" or "undecodable", and raw_type is what the counters
		       -- read; leaving it stale here would make the backfill's own
		       -- progress invisible to /healthcheck.
		       raw_type           = ?
		 WHERE id = ?
		   AND type = 'system'
		   AND COALESCE(content_text, '') = ?
	`, msgType, text, Normalize(text), scrubbed, ScrubFlagsJSON(flags), rawTypeNullable(msgType, text), msgID, stored)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// CountEmptySystemRows reports how many rows still carry the pre-MYC-3284
// shape. The backfill's progress is measured by watching this fall.
func (b *Bridge) CountEmptySystemRows(ctx context.Context) (int, error) {
	var n int
	err := b.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM messages
		 WHERE type = 'system' AND (content_text IS NULL OR content_text = '')
	`).Scan(&n)
	return n, err
}

// SweepDecodeBackfill asks WhatsApp to re-deliver history for the chats holding
// the most empty rows. The repair itself happens asynchronously in
// processHistorySyncEvent as those chunks arrive, which is why this reports
// what it REQUESTED plus the current outstanding count rather than "fixed N" —
// a number it cannot honestly know yet.
//
// status@broadcast is excluded: WhatsApp does not answer on-demand history
// requests for it (verified live — the request sends, no chunk ever arrives),
// so including it burns request budget for nothing.
//
// Setting opts.ChatJID repairs ONE named chat instead of the top-N. Without it
// the only reachable population is "whichever chats happen to hold the most
// empty rows", which leaves a specific chat someone is actually asking about
// unrepairable when it sits outside that cut — measured 2026-08-01: the chat
// this ticket was opened on was not in the top 30, so no sweep ever touched it.
func (b *Bridge) SweepDecodeBackfill(ctx context.Context, opts DecodeBackfillOptions) (requested int, skipped int, remaining int, err error) {
	if !b.IsConnected() {
		return 0, 0, 0, errors.New("bridge not connected; cannot request history")
	}
	opts.applyDefaults()

	remaining, err = b.CountEmptySystemRows(ctx)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("count empty rows: %w", err)
	}

	targets, tErr := b.decodeBackfillTargets(ctx, opts)
	if tErr != nil {
		return 0, 0, remaining, tErr
	}

	// Re-arm the walker for this sweep: fresh per-chat state and a fresh
	// global request budget. Also release any chat left pinned by an
	// unanswered request from a previous sweep.
	b.walker.sweepStale()
	b.walker.reset(opts.WalkBudget, opts.MaxRounds, opts.PerChat)

	for _, jid := range targets {
		// Anchor the FIRST request on the newest message, then register the
		// chat as walking. begin() returns false when the global budget is
		// already spent, in which case no request is issued at all.
		var anchorTS int64
		if tsErr := b.db.QueryRowContext(ctx,
			`SELECT timestamp FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT 1`, jid,
		).Scan(&anchorTS); tsErr != nil {
			log.Printf("decode backfill: no anchor for %s: %v", jid, tsErr)
			skipped++
			continue
		}
		if !b.walker.begin(jid, anchorTS, opts.PerChat) {
			skipped++
			continue
		}
		if _, _, reqErr := b.RequestChatHistory(ctx, jid, opts.PerChat); reqErr != nil {
			// One chat failing (left group, deleted, unparseable JID) must not
			// abort the sweep for the rest.
			log.Printf("decode backfill: history request for %s failed: %v", jid, reqErr)
			skipped++
			continue
		}
		requested++
	}
	log.Printf("decode backfill: requested history for %d chats (%d skipped); %d empty rows outstanding",
		requested, skipped, remaining)
	return requested, skipped, remaining, nil
}

// DecodeBackfillOptions parameterizes a decode-backfill sweep. It is a struct
// rather than positional arguments because four of the five knobs are ints:
// SweepDecodeBackfill(ctx, 30, 200, 400, 20) is trivially transposable at a
// call site and the compiler cannot catch it.
type DecodeBackfillOptions struct {
	// ChatJID repairs exactly this chat. Empty means "the top MaxChats chats
	// by empty-row count", which is the bulk-drain mode.
	ChatJID string

	MaxChats   int
	PerChat    int
	WalkBudget int
	MaxRounds  int
}

func (o *DecodeBackfillOptions) applyDefaults() {
	if o.MaxChats <= 0 {
		o.MaxChats = 20
	}
	if o.PerChat <= 0 {
		o.PerChat = 100
	}
	// WalkBudget / MaxRounds are left at zero when unset; walker.reset keeps
	// its own defaults for those rather than duplicating the constants here.
}

// decodeBackfillTargets picks which chats this sweep will walk.
//
// A named chat is honored even when it holds few empty rows — that is the
// entire point of naming it — but it must still HAVE at least one, otherwise
// the sweep would spend a request and a walk slot on a chat with nothing to
// repair.
func (b *Bridge) decodeBackfillTargets(ctx context.Context, opts DecodeBackfillOptions) ([]string, error) {
	if opts.ChatJID != "" {
		var n int
		if err := b.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM messages
			 WHERE chat_jid = ?
			   AND type = 'system' AND (content_text IS NULL OR content_text = '')
		`, opts.ChatJID).Scan(&n); err != nil {
			return nil, fmt.Errorf("count empty rows for %s: %w", opts.ChatJID, err)
		}
		if n == 0 {
			return nil, nil
		}
		return []string{opts.ChatJID}, nil
	}

	rows, err := b.db.QueryContext(ctx, `
		SELECT chat_jid, COUNT(*) AS n
		  FROM messages
		 WHERE type = 'system' AND (content_text IS NULL OR content_text = '')
		   AND chat_jid <> 'status@broadcast'
		 GROUP BY chat_jid
		 ORDER BY n DESC
		 LIMIT ?
	`, opts.MaxChats)
	if err != nil {
		return nil, fmt.Errorf("select affected chats: %w", err)
	}
	defer rows.Close()

	var targets []string
	for rows.Next() {
		var jid string
		var n int
		if err := rows.Scan(&jid, &n); err != nil {
			return nil, fmt.Errorf("scan affected chat: %w", err)
		}
		targets = append(targets, jid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate affected chats: %w", err)
	}
	return targets, nil
}
