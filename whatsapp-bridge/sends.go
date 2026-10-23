package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// Send flow: two-step draft then confirm.
//
// 1. POST /api/sends creates a draft row in the `sends` table with status='draft'.
//    Returns draft_id plus a resolved recipient display (so Claude and the user can
//    verify WHO they are about to message before committing).
// 2. POST /api/sends/{draft_id}/confirm flips status to 'confirmed', calls
//    whatsmeow.SendMessage, flips to 'sent' on success. The confirm is the only
//    place an actual outbound network send happens.
//
// Drafts expire after 1 hour so stale drafts can't be confirmed from a forgotten state.

const draftTTLSeconds = 3600

// --- Draft (pre-send) -------------------------------------------------------

type createDraftRequest struct {
	SendType        string `json:"send_type"` // "text" | "reply_quote" | "reaction" | "file" | "audio"
	RecipientJID    string `json:"recipient_jid"`
	Text            string `json:"text,omitempty"`
	QuotedMessageID string `json:"quoted_message_id,omitempty"`
	ReactionEmoji   string `json:"reaction_emoji,omitempty"`  // for reaction: the emoji (e.g., "❤️")
	ReactionTarget  string `json:"reaction_target,omitempty"` // for reaction: the message ID being reacted to

	// Media (send_type "file" or "audio"). Exactly one of FilePath, FileBase64
	// or FileURL must be set: a local client can name a path on this machine, a
	// remote one has no filesystem here and can only send small content inline,
	// and anything larger than the context can carry arrives as a URL the
	// bridge downloads itself (see fetch_url.go).
	FilePath   string `json:"file_path,omitempty"`
	FileBase64 string `json:"file_base64,omitempty"`
	FileURL    string `json:"file_url,omitempty"`
	Filename   string `json:"filename,omitempty"`   // shown to the recipient; required with file_base64 for documents
	MediaMIME  string `json:"media_mime,omitempty"` // overrides extension/sniffing when the caller knows better
	Caption    string `json:"caption,omitempty"`    // rendered under an image or video

	// AsDocument forces the document path for something that would otherwise
	// be sent as an image. WhatsApp recompresses images; a document keeps the
	// original bytes, which matters for receipts, designs and scans.
	AsDocument bool `json:"as_document,omitempty"`

	// VoiceNote renders audio as a PTT bubble instead of an audio attachment.
	// Requires Opus/Ogg, so anything else is transcoded via ffmpeg at confirm.
	VoiceNote bool `json:"voice_note,omitempty"`
}

type createDraftResponse struct {
	DraftID             string `json:"draft_id"`
	RecipientJID        string `json:"recipient_jid"`
	RecipientDisplay    string `json:"recipient_display"`
	Preview             string `json:"preview"`
	ExpiresAt           int64  `json:"expires_at"`
	RecipientContactHit bool   `json:"recipient_contact_hit"`
}

func (s *Server) handleCreateDraft(w http.ResponseWriter, r *http.Request) {
	var req createDraftRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body", Details: err.Error()})
		return
	}
	if req.RecipientJID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "recipient_jid required"})
		return
	}
	if req.SendType == "" {
		req.SendType = "text"
	}
	switch req.SendType {
	case "text":
		if req.Text == "" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "text required for send_type=text"})
			return
		}
	case "reply_quote":
		if req.Text == "" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "text required for send_type=reply_quote"})
			return
		}
		if req.QuotedMessageID == "" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "quoted_message_id required for send_type=reply_quote"})
			return
		}
	case "reaction":
		if req.ReactionEmoji == "" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "reaction_emoji required for send_type=reaction"})
			return
		}
		if req.ReactionTarget == "" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "reaction_target required for send_type=reaction (message ID to react to)"})
			return
		}
	case "file", "audio":
		switch fileSourceCount(req) {
		case 0:
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error:   fmt.Sprintf("file_path, file_base64 or file_url required for send_type=%s", req.SendType),
				Details: "file_path names a file on the bridge host; file_base64 carries the bytes inline for clients with no filesystem here; file_url is an https link the bridge downloads itself",
			})
			return
		case 1:
			// The one valid shape.
		default:
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error: "pass exactly one of file_path, file_base64 or file_url",
			})
			return
		}
		if req.SendType == "audio" && req.AsDocument {
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error:   "as_document is not valid for send_type=audio",
				Details: "send the file with send_type=file to deliver it as a document instead",
			})
			return
		}
		if req.SendType == "file" && req.VoiceNote {
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error:   "voice_note is only valid for send_type=audio",
				Details: "use send_type=audio with voice_note=true for a PTT bubble",
			})
			return
		}
	default:
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error:   fmt.Sprintf("send_type=%q not supported", req.SendType),
			Details: "supported: 'text', 'reply_quote', 'reaction', 'file', 'audio'.",
		})
		return
	}

	// Resolve recipient display name so Claude/user can verify before confirming.
	recipientDisplay, hit := s.resolveRecipient(r.Context(), req.RecipientJID)

	draftID := uuid.New().String()
	now := time.Now().Unix()

	// For reactions, use the reaction target as the "quoted" message and the emoji as text.
	// That way all drafts fit the same row schema.
	contentText := req.Text
	quotedID := req.QuotedMessageID
	if req.SendType == "reaction" {
		contentText = req.ReactionEmoji
		quotedID = req.ReactionTarget
	}

	// Media drafts land on local disk now and are uploaded only at confirm,
	// so no bytes reach WhatsApp before the user approves the send. For
	// file_url this is also where the download happens, so a fetch that is
	// refused or oversized fails here — before a row exists.
	var media outboundFile
	if req.SendType == "file" || req.SendType == "audio" {
		var mErr error
		media, mErr = materializeOutboundFile(r.Context(), s.cfg, draftID, req)
		if mErr != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "media rejected", Details: mErr.Error()})
			return
		}
		// The caption is the message body for media, so it rides in the same
		// column every other send type uses for its text.
		contentText = req.Caption
	}

	_, err := s.db.ExecContext(r.Context(), `
		INSERT INTO sends (draft_id, recipient_jid, recipient_display, send_type, content_text,
		                   content_file_path, content_media_mime, media_filename, media_as_document, media_voice_note,
		                   quoted_message_id, reaction_emoji, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?)
	`, draftID, req.RecipientJID, recipientDisplay, req.SendType, contentText,
		nullString(media.Path), nullString(media.MIME), nullString(media.Filename), boolToInt(req.AsDocument), boolToInt(req.VoiceNote),
		nullString(quotedID), nullString(req.ReactionEmoji), now)
	if err != nil {
		// Don't leave the bytes behind if the row that would reference them
		// never existed.
		if rmErr := removeOutboundFile(media.Path); rmErr != nil {
			log.Printf("draft %s: insert failed and cleanup failed too: %v", draftID, rmErr)
		}
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "draft insert failed", Details: err.Error()})
		return
	}

	preview := truncate(req.Text, 200)
	switch req.SendType {
	case "reaction":
		preview = fmt.Sprintf("Reaction %s to message %s", req.ReactionEmoji, truncate(req.ReactionTarget, 32))
	case "file", "audio":
		kind := media.MIME
		if req.SendType == "audio" && req.VoiceNote {
			kind = "voice note (" + media.MIME + ")"
		} else if req.AsDocument {
			kind = "document (" + media.MIME + ")"
		}
		// Show the name the RECIPIENT will see, not the draft_id-based name
		// the bytes happen to be stored under. The preview exists so the
		// user can check what they are approving; showing them an internal
		// identifier instead of the real filename defeats that. It is the
		// same string that goes into media_filename, so what is approved and
		// what is delivered cannot drift apart.
		shown := media.Filename
		if shown == "" || shown == "." {
			shown = filepath.Base(media.Path)
		}
		preview = fmt.Sprintf("%s, %s", kind, shown)
		if req.Caption != "" {
			preview += " — " + truncate(req.Caption, 120)
		}
	}

	writeJSON(w, http.StatusOK, createDraftResponse{
		DraftID:             draftID,
		RecipientJID:        req.RecipientJID,
		RecipientDisplay:    recipientDisplay,
		Preview:             preview,
		ExpiresAt:           now + draftTTLSeconds,
		RecipientContactHit: hit,
	})
}

// --- Confirm (commits the send) ---------------------------------------------

type confirmResponse struct {
	DraftID           string `json:"draft_id"`
	Status            string `json:"status"`
	WhatsAppMessageID string `json:"whatsapp_message_id,omitempty"`
	SentAt            int64  `json:"sent_at,omitempty"`
	RecipientJID      string `json:"recipient_jid"`
	RecipientDisplay  string `json:"recipient_display"`
	Error             string `json:"error,omitempty"`
}

func (s *Server) handleConfirmSend(w http.ResponseWriter, r *http.Request) {
	draftID := r.PathValue("draft_id")
	if draftID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "draft_id required"})
		return
	}
	if s.bridge == nil || !s.bridge.IsConnected() {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{
			Error:   "bridge not connected to WhatsApp",
			Details: "cannot send while disconnected; wait for reconnect and retry",
		})
		return
	}

	// Load the draft, validate status and TTL.
	var (
		recipientJID, recipientDisplay                 string
		sendType, contentText, quotedID, reactionEmoji string
		mediaPath, mediaMIME, mediaFilename            string
		mediaAsDocument, mediaVoiceNote                int
		status                                         string
		createdAt                                      int64
	)
	err := s.db.QueryRowContext(r.Context(),
		`SELECT recipient_jid, COALESCE(recipient_display, ''), send_type, COALESCE(content_text, ''),
		        COALESCE(content_file_path, ''), COALESCE(content_media_mime, ''), COALESCE(media_filename, ''),
		        media_as_document, media_voice_note,
		        COALESCE(quoted_message_id, ''), COALESCE(reaction_emoji, ''), status, created_at
		 FROM sends WHERE draft_id = ?`, draftID,
	).Scan(&recipientJID, &recipientDisplay, &sendType, &contentText,
		&mediaPath, &mediaMIME, &mediaFilename, &mediaAsDocument, &mediaVoiceNote,
		&quotedID, &reactionEmoji, &status, &createdAt)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "draft not found", Details: draftID})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "draft lookup failed", Details: err.Error()})
		return
	}
	if status != "draft" {
		writeJSON(w, http.StatusConflict, errorResponse{
			Error:   fmt.Sprintf("draft already in state %q, expected 'draft'", status),
			Details: "each draft can only be confirmed once; create a new draft to retry",
		})
		return
	}
	if time.Now().Unix() > createdAt+draftTTLSeconds {
		// Mark expired. An expired media draft will never be sent, so its
		// bytes are dead weight on disk from this point on.
		_, _ = s.db.ExecContext(r.Context(), `UPDATE sends SET status='expired' WHERE draft_id=?`, draftID)
		if rmErr := removeOutboundFile(mediaPath); rmErr != nil {
			log.Printf("draft %s expired; removing %s failed: %v", draftID, mediaPath, rmErr)
		}
		writeJSON(w, http.StatusGone, errorResponse{
			Error:   "draft expired",
			Details: fmt.Sprintf("drafts expire after %d seconds; create a new draft", draftTTLSeconds),
		})
		return
	}

	// Parse JID.
	recipient, err := types.ParseJID(recipientJID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid recipient JID", Details: err.Error()})
		return
	}

	// The rate limit is enforced HERE, and the position is the whole point: the
	// recipient is known, but the message has not been built, no media has been
	// uploaded, and — critically — the draft has not yet flipped to 'confirmed'.
	//
	// Refusing after that flip would strand the draft in a state that
	// ReconcileInFlightSends later reports with "DELIVERY IS UNKNOWN — check the
	// chat in WhatsApp before resending", sending someone to look for a message
	// that was never sent. Refusing here leaves the draft in 'draft', reusable
	// once the window clears.
	isNewChat, classifyErr := s.chatHasNoInbound(r.Context(), recipientJID)
	if classifyErr != nil {
		// Fail closed. The draft was read from this same database moments ago,
		// so a failure here is genuinely exceptional — and guessing "not new"
		// would spend the protection this check exists to provide.
		log.Printf("send blocked draft=%s: cannot classify recipient %s: %v", draftID, recipientJID, classifyErr)
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error:   "cannot classify recipient",
			Details: "refusing to send without knowing whether this is a first contact: " + classifyErr.Error(),
		})
		return
	}
	if d := s.sendGov().Authorize(recipientJID, isNewChat, time.Now()); !d.Allow {
		log.Printf("send throttled draft=%s recipient=%s new_chat=%t: %s", draftID, recipientJID, isNewChat, d.Reason)
		if d.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(d.RetryAfter.Seconds())+1))
		}
		writeJSON(w, http.StatusTooManyRequests, errorResponse{
			Error:   "send rate limit",
			Details: d.Reason + ". The draft was not consumed and can be confirmed again later.",
		})
		return
	}

	// Build the outbound message based on send type.
	var msg *waE2E.Message
	switch sendType {
	case "text":
		msg = &waE2E.Message{
			Conversation: proto.String(contentText),
		}
	case "reply_quote":
		// Look up the quoted message to populate ContextInfo.Participant correctly.
		var quotedSender string
		var quotedFromMe int
		_ = s.db.QueryRowContext(r.Context(),
			`SELECT COALESCE(sender_jid, ''), is_from_me FROM messages WHERE id = ?`, quotedID,
		).Scan(&quotedSender, &quotedFromMe)
		if quotedFromMe == 1 && s.bridge.client.Store.ID != nil {
			quotedSender = s.bridge.client.Store.ID.String()
		}
		msg = &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String(contentText),
				ContextInfo: &waE2E.ContextInfo{
					StanzaID:    proto.String(quotedID),
					Participant: proto.String(quotedSender),
				},
			},
		}
	case "reaction":
		var targetFromMe int
		_ = s.db.QueryRowContext(r.Context(),
			`SELECT is_from_me FROM messages WHERE id = ?`, quotedID,
		).Scan(&targetFromMe)
		msg = &waE2E.Message{
			ReactionMessage: &waE2E.ReactionMessage{
				Key: &waCommon.MessageKey{
					RemoteJID: proto.String(recipientJID),
					FromMe:    proto.Bool(targetFromMe == 1),
					ID:        proto.String(quotedID),
				},
				Text:              proto.String(reactionEmoji),
				SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
			},
		}
	case "file", "audio":
		// Upload happens here, not at draft time, so the user's bytes never
		// leave the machine until they confirm. Given its own timeout: a
		// large document can legitimately outlast the 30s the send itself
		// gets, and reusing that budget would fail slow uploads as if the
		// send had failed.
		upCtx, upCancel := context.WithTimeout(r.Context(), 5*time.Minute)
		msg, err = buildMediaMessage(upCtx, s.bridge.client, mediaMessageOpts{
			SendType:   sendType,
			Path:       mediaPath,
			MIME:       mediaMIME,
			Filename:   mediaFilename,
			Caption:    contentText,
			AsDocument: mediaAsDocument == 1,
			VoiceNote:  mediaVoiceNote == 1,
			FFmpegBin:  s.cfg.FFmpegBinPath,
		})
		upCancel()
		if err != nil {
			// The draft is still in 'draft' at this point, so a transient
			// upload failure can simply be retried on the same draft rather
			// than forcing the caller to rebuild it.
			log.Printf("media upload failed draft=%s: %v", draftID, err)
			writeJSON(w, http.StatusBadGateway, errorResponse{
				Error:   "media upload failed",
				Details: err.Error(),
			})
			return
		}
	default:
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error:   fmt.Sprintf("unsupported send_type %q reached confirm", sendType),
			Details: "this is a bug; the draft insert validator should have rejected it",
		})
		return
	}

	// Flip to confirmed BEFORE sending, so double-confirm races are blocked.
	now := time.Now().Unix()
	res, err := s.db.ExecContext(r.Context(),
		`UPDATE sends SET status='confirmed', confirmed_at=? WHERE draft_id=? AND status='draft'`,
		now, draftID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "confirm update failed", Details: err.Error()})
		return
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "race: draft changed state during confirm"})
		return
	}

	// Execute the send. This is the only place an actual outbound network call happens.
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sendResp, sendErr := s.bridge.client.SendMessage(ctx, recipient, msg)
	if sendErr != nil {
		log.Printf("send failed draft=%s recipient=%s: %v", draftID, recipientJID, sendErr)
		_, _ = s.db.ExecContext(r.Context(),
			`UPDATE sends SET status='failed', error_message=? WHERE draft_id=?`,
			sendErr.Error(), draftID)
		writeJSON(w, http.StatusBadGateway, errorResponse{
			Error:   "send failed",
			Details: sendErr.Error(),
		})
		return
	}

	sentAt := time.Now().Unix()
	whatsappID := sendResp.ID

	// PAST THE POINT OF NO RETURN: the message is delivered. Everything below is
	// the only record that will ever exist of that, so none of it may be
	// abandoned because the HTTP client hung up or timed out.
	//
	// These writes used to use r.Context(). When that was cancelled mid-send — a
	// client disconnect, an MCP-layer timeout, or the process exiting — the send
	// had already succeeded but the `sends` row stayed 'confirmed' forever and no
	// `messages` row was written: the recipient has a message the local archive
	// does not know about. This deployment's logs already show `context canceled`
	// killing other queries, so it was never hypothetical.
	//
	// WithoutCancel detaches from the request; the fresh timeout keeps it
	// bounded, so a wedged DB cannot hang the handler either. What no handler can
	// prevent is the process stopping here — see ReconcileInFlightSends.
	persistCtx, cancelPersist := context.WithTimeout(
		context.WithoutCancel(r.Context()), 15*time.Second)
	defer cancelPersist()

	if _, err := s.db.ExecContext(persistCtx,
		`UPDATE sends SET status='sent', sent_at=?, whatsapp_message_id=? WHERE draft_id=?`,
		sentAt, whatsappID, draftID); err != nil {
		// Loud: this is the row startup reconciliation will otherwise find stuck.
		log.Printf("draft %s: SENT as %s but marking it sent FAILED: %v", draftID, whatsappID, err)
	}

	// Also persist the sent message in messages table so it shows up in chat history.
	//
	// The row is classified by the SAME extractor the receive path uses, rather
	// than by a literal here. The type used to be hardcoded 'text', so every
	// image, document and voice note the bridge sent was archived as a text
	// message — and because the insert is ON CONFLICT DO NOTHING, nothing could
	// ever correct it. WhatsApp does not echo a message back to the device that
	// sent it, so this row is the only record that will ever exist: whatever it
	// gets wrong stays wrong.
	//
	// extractDownloadableFieldsFromProto reads the upload result back off the
	// protobuf we just sent, so download_media works on our own sent media
	// instead of finding a media row with no keys.
	senderJID := ""
	if s.bridge.client.Store.ID != nil {
		senderJID = s.bridge.client.Store.ID.String()
	}
	persistText, persistType := extractContentFromProto(msg)
	mfields, _ := extractDownloadableFieldsFromProto(msg)
	scrubbed, flags := Scrub(persistText)
	_, err = s.db.ExecContext(persistCtx, `
		INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, content_normalized, is_from_me, scrubbed_text, scrub_flags_json,
			media_key, media_direct_path, media_url, media_enc_sha256, media_sha256, media_file_length, media_key_timestamp, media_mime)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING
	`, whatsappID, recipientJID, senderJID, nullString(s.bridge.client.Store.PushName), sentAt, persistType,
		persistText, Normalize(persistText), scrubbed, ScrubFlagsJSON(flags),
		mfields.MediaKey, mfields.MediaDirectPath, mfields.MediaURL,
		mfields.MediaEncSHA, mfields.MediaSHA, mfields.MediaFileLength,
		mfields.MediaKeyTimestamp, mfields.MediaMime)
	if err != nil {
		log.Printf("sent-message persist failed: %v", err)
	}

	// The draft's bytes have served their purpose: Upload happened above, and a
	// confirmed draft can never be sent again. Nothing else deletes them —
	// removeOutboundFile is otherwise only reached by a failed insert or by an
	// attempt to confirm an already-expired draft — so without this every file
	// ever sent accumulates in the outbound directory forever.
	if rmErr := removeOutboundFile(mediaPath); rmErr != nil {
		log.Printf("draft %s sent; removing %s failed: %v", draftID, mediaPath, rmErr)
	}

	writeJSON(w, http.StatusOK, confirmResponse{
		DraftID:           draftID,
		Status:            "sent",
		WhatsAppMessageID: whatsappID,
		SentAt:            sentAt,
		RecipientJID:      recipientJID,
		RecipientDisplay:  recipientDisplay,
	})
}

// --- Recipient resolution ---------------------------------------------------

// resolveRecipient looks up a human-readable display for a JID from our contacts table.
// Returns (display, true) if we found a contact; (jid, false) otherwise.
// resolveRecipient turns a JID into the label shown in a draft preview, which
// is the string the user reads before approving a send. Getting it wrong does
// not corrupt anything, but it removes the only check the two-step flow exists
// to provide.
//
// Two things it used to get wrong, both visible on a real send:
//
//   - It looked up the JID exactly, ignoring jid_aliases. A contact row keyed
//     by @lid and a draft addressed to the phone-number JID are the same human,
//     and the lookup missed. The preview then echoed the raw JID and reported
//     recipient_contact_hit=false, while the information sat one join away.
//   - It never consulted the address book. verified_name and push_name are what
//     the CONTACT chose; full_name is what the USER chose, and it is the name
//     they will recognise. Preferring it means the preview says "Mi Cielo" where
//     the user thinks "Mi Cielo".
func (s *Server) resolveRecipient(ctx context.Context, jid string) (string, bool) {
	var fullName, pushName, verifiedName, phone sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT full_name, push_name, verified_name, phone
		FROM contacts
		WHERE jid = ? OR jid IN (SELECT jid_b FROM jid_aliases WHERE jid_a = ?)
		ORDER BY (jid = ?) DESC, updated_at DESC
		LIMIT 1
	`, jid, jid, jid).Scan(&fullName, &pushName, &verifiedName, &phone)
	if err != nil {
		return jid, false
	}
	if fullName.Valid && fullName.String != "" {
		return fullName.String, true
	}
	if verifiedName.Valid && verifiedName.String != "" {
		return verifiedName.String, true
	}
	if pushName.Valid && pushName.String != "" {
		return pushName.String, true
	}
	if phone.Valid && phone.String != "" {
		return "+" + phone.String, true
	}
	return jid, true
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
