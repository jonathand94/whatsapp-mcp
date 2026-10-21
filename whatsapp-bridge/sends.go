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
	"strings"
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
	Status              string `json:"status"` // always "draft": the row exists and can be confirmed
	RecipientJID        string `json:"recipient_jid"`
	RecipientDisplay    string `json:"recipient_display"`
	Preview             string `json:"preview"`
	ExpiresAt           int64  `json:"expires_at"`
	RecipientContactHit bool   `json:"recipient_contact_hit"`

	// Media drafts only. DeliveredAs is the envelope confirm will use (photo,
	// video, audio, voice_note or document); Note explains it when it is not
	// what the caller would expect, e.g. a 90 MB video going as a document.
	DeliveredAs string `json:"delivered_as,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	Note        string `json:"note,omitempty"`
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

	// Media drafts land on local disk now and are uploaded only at confirm,
	// so no bytes reach WhatsApp before the user approves the send. For
	// file_url this is also where the download happens, so a fetch that is
	// refused or oversized fails here — before a row exists.
	//
	// Getting the bytes can take minutes for a large file, so it runs as a
	// job: a small file is answered exactly as before, a large one gets 202
	// and GET /api/sends/{draft_id} reports progress until the draft exists.
	if req.SendType == "file" || req.SendType == "audio" {
		j, _ := s.jobs().start("draft:"+draftID, func(j *mediaJob) (int, any) {
			ctx, cancel := context.WithTimeout(s.rootContext(), mediaJobTimeout)
			defer cancel()
			return s.createMediaDraft(ctx, j, draftID, req, recipientDisplay, hit)
		})
		answerJob(w, r, j, jobPending{
			DraftID:          draftID,
			RecipientJID:     req.RecipientJID,
			RecipientDisplay: recipientDisplay,
			Status:           "preparing",
			Hint:             "el archivo se esta preparando; consulta el estado del borrador y confirmalo cuando diga 'draft'",
		})
		return
	}

	now := time.Now().Unix()

	// For reactions, use the reaction target as the "quoted" message and the emoji as text.
	// That way all drafts fit the same row schema.
	contentText := req.Text
	quotedID := req.QuotedMessageID
	if req.SendType == "reaction" {
		contentText = req.ReactionEmoji
		quotedID = req.ReactionTarget
	}

	_, err := s.db.ExecContext(r.Context(), `
		INSERT INTO sends (draft_id, recipient_jid, recipient_display, send_type, content_text,
		                   content_file_path, content_media_mime, media_filename, media_as_document, media_voice_note,
		                   quoted_message_id, reaction_emoji, status, created_at)
		VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, 0, 0, ?, ?, 'draft', ?)
	`, draftID, req.RecipientJID, recipientDisplay, req.SendType, contentText,
		nullString(quotedID), nullString(req.ReactionEmoji), now)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "draft insert failed", Details: err.Error()})
		return
	}

	preview := truncate(req.Text, 200)
	if req.SendType == "reaction" {
		preview = fmt.Sprintf("Reaction %s to message %s", req.ReactionEmoji, truncate(req.ReactionTarget, 32))
	}

	writeJSON(w, http.StatusOK, createDraftResponse{
		DraftID:             draftID,
		Status:              "draft",
		RecipientJID:        req.RecipientJID,
		RecipientDisplay:    recipientDisplay,
		Preview:             preview,
		ExpiresAt:           now + draftTTLSeconds,
		RecipientContactHit: hit,
	})
}

// createMediaDraft gets a media draft's bytes onto local disk and inserts its
// row. It runs as the draft's job, so it answers with a status and body
// rather than writing a response.
func (s *Server) createMediaDraft(ctx context.Context, j *mediaJob, draftID string, req createDraftRequest, recipientDisplay string, hit bool) (int, any) {
	media, err := materializeWithProgress(ctx, s.cfg, draftID, req, j)
	if err != nil {
		return http.StatusBadRequest, errorResponse{Error: "media rejected", Details: err.Error()}
	}

	// The draft's hour starts when its bytes are ready, not when the request
	// arrived: a long download must not produce a draft that expires moments
	// after it could first be confirmed.
	now := time.Now().Unix()

	// The caption is the message body for media, so it rides in the same
	// column every other send type uses for its text. media_as_document holds
	// the planned envelope — asked for, or forced by size — so confirm sends
	// exactly what the preview showed.
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO sends (draft_id, recipient_jid, recipient_display, send_type, content_text,
		                   content_file_path, content_media_mime, media_filename, media_as_document, media_voice_note,
		                   quoted_message_id, reaction_emoji, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'draft', ?)
	`, draftID, req.RecipientJID, recipientDisplay, req.SendType, req.Caption,
		media.Path, nullString(media.MIME), nullString(media.Filename),
		boolToInt(media.AsDocument), boolToInt(req.VoiceNote), now)
	if err != nil {
		// Don't leave the bytes behind if the row that would reference them
		// never existed.
		if rmErr := removeOutboundFile(media.Path); rmErr != nil {
			log.Printf("draft %s: insert failed and cleanup failed too: %v", draftID, rmErr)
		}
		return http.StatusInternalServerError, errorResponse{Error: "draft insert failed", Details: err.Error()}
	}

	as := deliveredAs(req.SendType, media.MIME, media.delivery, req.VoiceNote)
	return http.StatusOK, createDraftResponse{
		DraftID:             draftID,
		Status:              "draft",
		RecipientJID:        req.RecipientJID,
		RecipientDisplay:    recipientDisplay,
		Preview:             mediaPreview(req, media, as),
		ExpiresAt:           now + draftTTLSeconds,
		RecipientContactHit: hit,
		DeliveredAs:         as,
		SizeBytes:           media.Size,
		Note:                deliveryNote(req, media),
	}
}

// mediaPreview is the line the user reads before approving a media send.
func mediaPreview(req createDraftRequest, media outboundFile, as string) string {
	kind := media.MIME
	switch as {
	case "voice_note":
		kind = "voice note (" + media.MIME + ")"
	case "document":
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
	preview := fmt.Sprintf("%s, %s, %s", kind, shown, humanBytes(media.Size))
	if req.Caption != "" {
		preview += " — " + truncate(req.Caption, 120)
	}
	return preview
}

// deliveryNote explains an envelope the caller did not ask for.
func deliveryNote(req createDraftRequest, media outboundFile) string {
	if !media.Downgraded {
		return ""
	}
	what := "archivo"
	switch {
	case req.SendType == "audio":
		what = "audio"
	case strings.HasPrefix(media.MIME, "image/"):
		what = "foto"
	case strings.HasPrefix(media.MIME, "video/"):
		what = "video"
	}
	return fmt.Sprintf("va como documento: pesa %s y WhatsApp solo acepta %s para enviarlo como %s; "+
		"el destinatario lo recibe completo, sin compresion, y lo abre con un toque",
		humanBytes(media.Size), humanBytes(nativeCeiling(req.SendType, media.MIME)), what)
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

	// A media draft whose bytes are still arriving has no row yet, so the
	// lookup below would call it "not found". Say what is actually going on —
	// and if preparing it failed, say why, since there is no row to say so.
	if prep := s.jobs().get("draft:" + draftID); prep != nil {
		if prep.running() {
			p := prep.pending(jobPending{})
			writeJSON(w, http.StatusConflict, errorResponse{
				Error: "draft still preparing",
				Details: fmt.Sprintf("el archivo todavia se esta preparando (%s de %s); "+
					"confirma cuando el estado del borrador diga 'draft'",
					humanBytes(p.BytesDone), humanBytes(p.BytesTotal)),
			})
			return
		}
		if prep.status != http.StatusOK {
			writeJSON(w, prep.status, prep.body)
			return
		}
	}

	// Confirm is idempotent, because for a large file the caller has to ask
	// more than once: the first confirm answers 202 while the upload runs.
	// An upload still running is joined, never repeated — the draft stays in
	// 'draft' until its upload finishes, so without this a second confirm
	// would pass every check and upload the same file again. An upload that
	// already delivered the message answers with that delivery.
	if up := s.jobs().get("send:" + draftID); up != nil {
		if up.running() {
			answerJob(w, r, up, jobPending{DraftID: draftID, Status: "uploading", Hint: uploadingHint})
			return
		}
		if up.status == http.StatusOK {
			writeJSON(w, up.status, up.body)
			return
		}
	}
	if code, body, settled := s.settledConfirm(r.Context(), draftID); settled {
		writeJSON(w, code, body)
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
		// Settled between the check above and this read: answer from the row.
		code, body, _ := s.settledConfirm(r.Context(), draftID)
		writeJSON(w, code, body)
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
	chatKey, isNewChat, classifyErr := s.classifyRecipient(r.Context(), recipientJID)
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
	d := s.sendGov().Authorize(chatKey, isNewChat, time.Now())
	if !d.Allow {
		log.Printf("send throttled draft=%s recipient=%s new_chat=%t: %s", draftID, recipientJID, isNewChat, d.Reason)
		if d.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(d.RetryAfter.Seconds())+1))
		}
		writeJSON(w, http.StatusTooManyRequests, errorResponse{
			Error:        "send rate limit",
			Details:      d.Reason + ". The draft was not consumed and can be confirmed again later.",
			RetryAfterMS: d.RetryAfter.Milliseconds(),
		})
		return
	}
	// The turn this send was given in its chat; commitSend waits for it.
	turn := sendTurn{chat: chatKey, at: d.SendAt}

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
		// leave the machine until they confirm. It runs as a job, detached
		// from this request: a 2 GB upload can take hours, and answering
		// "failed" when the HTTP client gave up — while the upload carried on
		// and the message went out — is how a duplicate send happens.
		//
		// The draft stays in 'draft' for the whole upload; commitSend flips it
		// only once the bytes are on WhatsApp's servers. So a failed or
		// interrupted upload leaves a draft that can simply be confirmed
		// again, and a restart mid-upload cannot leave a send in limbo.
		opts := mediaMessageOpts{
			SendType:   sendType,
			Path:       mediaPath,
			MIME:       mediaMIME,
			Filename:   mediaFilename,
			Caption:    contentText,
			AsDocument: mediaAsDocument == 1,
			VoiceNote:  mediaVoiceNote == 1,
			FFmpegBin:  s.cfg.FFmpegBinPath,
		}
		up, _ := s.jobs().start("send:"+draftID, func(j *mediaJob) (int, any) {
			ctx, cancel := context.WithTimeout(s.rootContext(), mediaJobTimeout)
			defer cancel()

			// Another confirm can have sent this draft, and spent its file,
			// between this request's checks and this job starting. The row
			// knows; the request that started the job does not.
			if code, body, settled := s.settledConfirm(ctx, draftID); settled {
				return code, body
			}

			// The stall watch gets the upload's own context, never the one
			// the send runs under: a watch that fires as the upload returns
			// must not be able to cancel the send that follows.
			upCtx, upCancel := context.WithCancel(ctx)
			stop, stalled := watchStall(upCancel, j.activity, stallTimeout)
			built, err := buildMediaMessage(upCtx, s.bridge.client, opts, j)
			stop()
			upCancel()
			if err != nil {
				if stalled.Load() {
					err = fmt.Errorf("la subida se detuvo: no se movieron datos en %s", stallTimeout)
				}
				log.Printf("media upload failed draft=%s: %v", draftID, err)
				return http.StatusBadGateway, errorResponse{
					Error:   "media upload failed",
					Details: err.Error() + ". Nothing was sent and the draft can be confirmed again.",
				}
			}

			// An upload can take hours, and the connection can drop in the
			// meantime. Sending now would fail before anything left this
			// machine and burn the draft as 'failed'; waiting a little for
			// the reconnect, and otherwise leaving the draft a draft, keeps it
			// usable.
			if !s.awaitConnection(ctx, reconnectGrace) {
				return http.StatusServiceUnavailable, errorResponse{
					Error: "bridge not connected to WhatsApp",
					Details: "el archivo se subio, pero WhatsApp se desconecto antes de enviarlo. " +
						"Nada se envio y el borrador sigue vigente: confirmalo de nuevo cuando vuelva la conexion.",
				}
			}
			j.setPhase("enviando el mensaje")
			return s.commitSend(ctx, draftID, recipient, recipientJID, recipientDisplay, mediaPath, built, turn)
		})
		answerJob(w, r, up, jobPending{
			DraftID:          draftID,
			RecipientJID:     recipientJID,
			RecipientDisplay: recipientDisplay,
			Status:           "uploading",
			Hint:             uploadingHint,
		})
		return
	default:
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error:   fmt.Sprintf("unsupported send_type %q reached confirm", sendType),
			Details: "this is a bug; the draft insert validator should have rejected it",
		})
		return
	}

	code, body := s.commitSend(r.Context(), draftID, recipient, recipientJID, recipientDisplay, "", msg, turn)
	writeJSON(w, code, body)
}

// sendTurn is a send's place in its chat's queue: the governor's key for the
// chat, and the moment Authorize scheduled it for.
type sendTurn struct {
	chat string
	at   time.Time
}

// uploadingHint tells a caller holding a 202 from confirm what to do next.
const uploadingHint = "el archivo se esta subiendo a WhatsApp. Vuelve a confirmar el mismo borrador (o consulta su estado) " +
	"para saber cuando se envie: confirmar otra vez solo se une a esta subida. No crees un borrador nuevo"

// reconnectGrace is how long a finished upload waits for WhatsApp to come
// back before giving up on sending for now.
const reconnectGrace = 2 * time.Minute

// settledConfirm answers a confirm for a draft that is not a plain draft any
// more, from its row: the delivery that already happened, or why there will
// not be one. settled is false when the draft is still 'draft' and the
// confirm should go ahead.
//
// Before confirms could take minutes this was just "409, already confirmed".
// Now a caller re-confirms to learn how its upload ended, and telling it
// "create a new draft" about a message that was delivered is how the same
// file reaches someone twice.
func (s *Server) settledConfirm(ctx context.Context, draftID string) (code int, body any, settled bool) {
	var status, recipientJID, recipientDisplay, whatsappID, errMsg string
	var sentAt int64
	err := s.db.QueryRowContext(ctx, `
		SELECT status, recipient_jid, COALESCE(recipient_display, ''), COALESCE(whatsapp_message_id, ''),
		       COALESCE(sent_at, 0), COALESCE(error_message, '')
		FROM sends WHERE draft_id = ?`, draftID,
	).Scan(&status, &recipientJID, &recipientDisplay, &whatsappID, &sentAt, &errMsg)
	switch {
	case err == sql.ErrNoRows:
		return http.StatusNotFound, errorResponse{Error: "draft not found", Details: draftID}, true
	case err != nil:
		return http.StatusInternalServerError, errorResponse{Error: "draft lookup failed", Details: err.Error()}, true
	}
	switch status {
	case "draft":
		return 0, nil, false
	case "sent":
		return http.StatusOK, confirmResponse{
			DraftID:           draftID,
			Status:            "sent",
			WhatsAppMessageID: whatsappID,
			SentAt:            sentAt,
			RecipientJID:      recipientJID,
			RecipientDisplay:  recipientDisplay,
		}, true
	case "confirmed":
		return http.StatusConflict, errorResponse{
			Error: "send in progress or interrupted",
			Details: "este envio ya se confirmo y no ha terminado, o lo interrumpio un reinicio. " +
				"Revisa el chat en WhatsApp antes de crear otro borrador",
		}, true
	case "failed":
		return http.StatusConflict, errorResponse{
			Error:   "send failed",
			Details: errMsg + ". Este borrador ya no se puede usar; crea uno nuevo para reintentar",
		}, true
	case "expired":
		return http.StatusGone, errorResponse{
			Error:   "draft expired",
			Details: fmt.Sprintf("drafts expire after %d seconds; create a new draft", draftTTLSeconds),
		}, true
	default:
		return http.StatusConflict, errorResponse{
			Error:   fmt.Sprintf("draft in state %q", status),
			Details: "create a new draft to retry",
		}, true
	}
}

// awaitConnection reports whether WhatsApp is connected, waiting up to grace
// for a reconnect when it is not.
func (s *Server) awaitConnection(ctx context.Context, grace time.Duration) bool {
	deadline := time.Now().Add(grace)
	for {
		if s.bridge != nil && s.bridge.IsConnected() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
}

// commitSend flips a draft to 'confirmed', sends msg, and records the result.
// Every send type ends here: text inline from the handler, media from the job
// that uploaded its file. ctx bounds the flip and the send; the bookkeeping
// after a successful send is deliberately detached from it (see persistCtx).
func (s *Server) commitSend(ctx context.Context, draftID string, recipient types.JID, recipientJID, recipientDisplay, mediaPath string, msg *waE2E.Message, turn sendTurn) (int, any) {
	// Wait for this send's turn in its chat, while the draft is still a
	// draft: giving up here (the caller hung up) leaves nothing half-done.
	// Authorize scheduled the turn; ClaimSend also keeps the gap from a send
	// that went out later than its turn, as a long upload's does.
	if wait := time.Until(turn.at); wait > 0 {
		select {
		case <-ctx.Done():
			return http.StatusServiceUnavailable, errorResponse{
				Error: "send cancelled", Details: "the request ended while waiting its turn; nothing was sent and the draft can be confirmed again"}
		case <-time.After(wait):
		}
	}
	if err := s.sendGov().ClaimSend(ctx, turn.chat); err != nil {
		return http.StatusServiceUnavailable, errorResponse{
			Error: "send cancelled", Details: "the request ended while waiting its turn; nothing was sent and the draft can be confirmed again"}
	}

	// Flip to confirmed BEFORE sending, so double-confirm races are blocked.
	now := time.Now().Unix()
	res, err := s.db.ExecContext(ctx,
		`UPDATE sends SET status='confirmed', confirmed_at=? WHERE draft_id=? AND status='draft'`,
		now, draftID)
	if err != nil {
		return http.StatusInternalServerError, errorResponse{Error: "confirm update failed", Details: err.Error()}
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return http.StatusConflict, errorResponse{Error: "race: draft changed state during confirm"}
	}

	// Execute the send. This is the only place an actual outbound network call happens.
	sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	sendResp, sendErr := s.bridge.client.SendMessage(sendCtx, recipient, msg)
	if sendErr != nil {
		log.Printf("send failed draft=%s recipient=%s: %v", draftID, recipientJID, sendErr)
		_, _ = s.db.ExecContext(ctx,
			`UPDATE sends SET status='failed', error_message=? WHERE draft_id=?`,
			sendErr.Error(), draftID)
		// A failed draft can never be confirmed again, so its bytes — up to
		// 2 GB now — are dead weight from here on.
		if rmErr := removeOutboundFile(mediaPath); rmErr != nil {
			log.Printf("draft %s failed; removing %s failed: %v", draftID, mediaPath, rmErr)
		}
		return http.StatusBadGateway, errorResponse{
			Error:   "send failed",
			Details: sendErr.Error(),
		}
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
		context.WithoutCancel(ctx), 15*time.Second)
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
			media_key, media_direct_path, media_url, media_enc_sha256, media_sha256, media_file_length, media_key_timestamp, media_mime, media_filename)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING
	`, whatsappID, recipientJID, senderJID, nullString(s.bridge.client.Store.PushName), sentAt, persistType,
		persistText, Normalize(persistText), scrubbed, ScrubFlagsJSON(flags),
		mfields.MediaKey, mfields.MediaDirectPath, mfields.MediaURL,
		mfields.MediaEncSHA, mfields.MediaSHA, mfields.MediaFileLength,
		mfields.MediaKeyTimestamp, mfields.MediaMime, mfields.MediaFilename)
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

	return http.StatusOK, confirmResponse{
		DraftID:           draftID,
		Status:            "sent",
		WhatsAppMessageID: whatsappID,
		SentAt:            sentAt,
		RecipientJID:      recipientJID,
		RecipientDisplay:  recipientDisplay,
	}
}

// --- Status (where a draft is in its life) -----------------------------------

// sendStatusResponse answers GET /api/sends/{draft_id}. It exists for the
// callers that got 202: to learn when a preparing draft can be confirmed, and
// whether an upload ended with the message sent.
type sendStatusResponse struct {
	DraftID string `json:"draft_id"`
	// Status is one of: preparing, rejected (preparation failed, so there is
	// no draft), draft, uploading, confirmed, sent, failed, expired.
	Status            string `json:"status"`
	RecipientJID      string `json:"recipient_jid,omitempty"`
	RecipientDisplay  string `json:"recipient_display,omitempty"`
	SendType          string `json:"send_type,omitempty"`
	Preview           string `json:"preview,omitempty"`
	DeliveredAs       string `json:"delivered_as,omitempty"`
	SizeBytes         int64  `json:"size_bytes,omitempty"`
	Note              string `json:"note,omitempty"`
	ExpiresAt         int64  `json:"expires_at,omitempty"`
	WhatsAppMessageID string `json:"whatsapp_message_id,omitempty"`
	SentAt            int64  `json:"sent_at,omitempty"`
	Error             string `json:"error,omitempty"`
	// LastAttemptError is why the most recent confirm failed before anything
	// was sent. The draft is still usable; confirming again retries.
	LastAttemptError string `json:"last_attempt_error,omitempty"`
}

func (s *Server) handleSendStatus(w http.ResponseWriter, r *http.Request) {
	draftID := r.PathValue("draft_id")
	if draftID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "draft_id required"})
		return
	}

	var prepared *createDraftResponse
	if prep := s.jobs().get("draft:" + draftID); prep != nil {
		if prep.running() {
			writeJSON(w, http.StatusOK, prep.pending(jobPending{DraftID: draftID, Status: "preparing"}))
			return
		}
		if prep.status != http.StatusOK {
			// The draft never came to exist, and this is the only place the
			// reason survives.
			writeJSON(w, http.StatusOK, sendStatusResponse{
				DraftID: draftID, Status: "rejected", Error: errorText(prep.body),
			})
			return
		}
		if cdr, ok := prep.body.(createDraftResponse); ok {
			prepared = &cdr
		}
	}
	up := s.jobs().get("send:" + draftID)
	if up != nil && up.running() {
		writeJSON(w, http.StatusOK, up.pending(jobPending{DraftID: draftID, Status: "uploading"}))
		return
	}

	var (
		resp                            sendStatusResponse
		mediaMIME                       string
		mediaAsDocument, mediaVoiceNote int
		createdAt                       int64
	)
	err := s.db.QueryRowContext(r.Context(), `
		SELECT status, recipient_jid, COALESCE(recipient_display, ''), send_type,
		       COALESCE(content_media_mime, ''), media_as_document, media_voice_note, created_at,
		       COALESCE(sent_at, 0), COALESCE(whatsapp_message_id, ''), COALESCE(error_message, '')
		FROM sends WHERE draft_id = ?`, draftID,
	).Scan(&resp.Status, &resp.RecipientJID, &resp.RecipientDisplay, &resp.SendType,
		&mediaMIME, &mediaAsDocument, &mediaVoiceNote, &createdAt,
		&resp.SentAt, &resp.WhatsAppMessageID, &resp.Error)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "draft not found", Details: draftID})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "draft lookup failed", Details: err.Error()})
		return
	}
	resp.DraftID = draftID

	if resp.SendType == "file" || resp.SendType == "audio" {
		resp.DeliveredAs = deliveredAs(resp.SendType, mediaMIME,
			delivery{AsDocument: mediaAsDocument == 1}, mediaVoiceNote == 1)
	}
	if prepared != nil {
		resp.Preview, resp.SizeBytes, resp.Note = prepared.Preview, prepared.SizeBytes, prepared.Note
	}
	if resp.Status == "draft" {
		resp.ExpiresAt = createdAt + draftTTLSeconds
		// Expiry is only written to the row when someone tries to confirm,
		// so report it truthfully here instead of calling it a draft.
		if time.Now().Unix() > resp.ExpiresAt {
			resp.Status = "expired"
		}
		if up != nil && up.status != http.StatusOK {
			resp.LastAttemptError = errorText(up.body)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// errorText flattens a job's error body into one line.
func errorText(body any) string {
	if e, ok := body.(errorResponse); ok {
		if e.Details != "" {
			return e.Error + ": " + e.Details
		}
		return e.Error
	}
	return fmt.Sprint(body)
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
