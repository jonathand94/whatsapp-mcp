// calls.go — records WhatsApp calls, and makes them readable.
//
// Until this file existed the call log had never recorded a single call. The
// old onCallOffer inserted result = 'offered', a value the table's CHECK
// constraint does not allow:
//
//	result TEXT CHECK (result IN ('answered','missed','rejected','ended','failed'))
//
// so every insert failed — 43 times between 2026-09-15 and 2026-09-28 alone,
// with capture_calls switched on the whole time. What kept that invisible was
// the other half: onCallTerminate ran `UPDATE calls SET result=? WHERE id=?`
// against a row that never existed, and an UPDATE that matches nothing is not
// an error in SQL. The log held 43 lines from one side and none from the other.
//
// Three changes:
//
//  1. A ringing call stores result = NULL. It has no outcome yet, and a CHECK
//     constraint passes on NULL.
//  2. Every call event UPSERTS. The old split — offer inserts, everything else
//     updates — meant one missed offer (bridge restarting, socket down) lost the
//     whole call, silently. Now whichever event arrives first creates the row.
//  3. GET /api/calls reads the table back. Before this nothing read it at all,
//     so even a working log would have been invisible.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// The only values calls.result accepts. Anything written must be one of these
// or NULL, or the whole insert fails — which is exactly how this broke.
const (
	callAnswered = "answered"
	callMissed   = "missed"
	callRejected = "rejected"
	callEnded    = "ended"
)

// mapTerminateReason turns the `reason` attribute of a <terminate> node into an
// outcome the table accepts.
//
// whatsmeow passes that attribute through untouched (cag.String("reason")), so
// its vocabulary is whatever WhatsApp's servers send: undocumented and
// unbounded. Writing it raw was the second latent bug here — the first "busy"
// or "accepted_elsewhere" would have failed the CHECK just like 'offered' did.
//
// Only "timeout" has a well-established meaning: nobody picked up, so the call
// was missed. The rest is matched conservatively and everything unrecognized
// becomes "ended", which is true of every call that has terminated. The second
// return value reports whether the reason was recognized, so the caller can log
// the unknown ones and the mapping can grow from what this account actually
// receives rather than from guesses.
func mapTerminateReason(reason string) (string, bool) {
	r := strings.ToLower(strings.TrimSpace(reason))
	switch {
	case r == "":
		return callEnded, true
	case r == "timeout":
		return callMissed, true
	case strings.Contains(r, "reject"):
		return callRejected, true
	case strings.Contains(r, "accept"):
		// "accepted elsewhere"-style reasons: picked up, just on another device.
		return callAnswered, true
	default:
		return callEnded, false
	}
}

// offerIsVideo reports whether an offer is a video call: the offer node carries
// a <video> child for video and only <audio> children for voice. Anything this
// does not understand is recorded as voice — what every call was recorded as
// before — so a wrong guess can only reproduce the old behaviour, never break
// the insert.
func offerIsVideo(node *waBinary.Node) bool {
	if node == nil {
		return false
	}
	_, ok := node.GetOptionalChildByTag("video")
	return ok
}

// callEvent is one call event, reduced to what the table stores.
type callEvent struct {
	ID         string
	Creator    types.JID // who started the call; may be a @lid
	CreatorAlt types.JID // the other form of the same person, when WhatsApp sends it
	GroupJID   types.JID
	IsGroup    bool
	At         time.Time
	IsVideo    bool
	Outbound   bool

	// Result is "" while the call has no outcome yet.
	Result string
	// Definitive outcomes (accept, reject) overwrite whatever is there. A
	// terminate only fills an empty result: it arrives for EVERY call, including
	// answered ones, and must not turn "answered" into "ended".
	Definitive bool
}

// recordCall upserts one call event.
func recordCall(ctx context.Context, db *sql.DB, e callEvent) error {
	if e.ID == "" {
		return fmt.Errorf("call event without a call-id")
	}
	chatJID, err := callChatJID(ctx, db, e)
	if err != nil {
		return err
	}

	callType := "voice"
	if e.IsVideo {
		callType = "video"
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO calls (id, chat_jid, caller_jid, timestamp, call_type, is_group, is_outbound, result)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			result = CASE
				WHEN excluded.result IS NULL THEN calls.result
				WHEN ? = 1                   THEN excluded.result
				ELSE COALESCE(calls.result, excluded.result)
			END,
			-- Only the offer knows the call is video; a later event must not
			-- downgrade it back to voice.
			call_type = CASE WHEN excluded.call_type = 'video' THEN 'video' ELSE calls.call_type END,
			chat_jid  = COALESCE(calls.chat_jid, excluded.chat_jid)
	`, e.ID, nullIfEmpty(chatJID), e.Creator.ToNonAD().String(), e.At.Unix(), callType,
		boolInt(e.IsGroup), boolInt(e.Outbound), nullIfEmpty(e.Result), boolInt(e.Definitive))
	if err != nil {
		return fmt.Errorf("upserting call %s: %w", e.ID, err)
	}
	return nil
}

// callChatJID picks the chat a call belongs to, or "" when there is none.
//
// A group call belongs to the group. A one-to-one call belongs to the caller's
// chat — but the caller can arrive as a @lid while the conversation is filed
// under their phone number, or the other way round, so both forms WhatsApp
// supplies are tried and whichever already has a chat wins.
//
// When neither does — someone who has never written — this returns "" and the
// call is stored with chat_jid NULL. It deliberately does NOT create a chat.
// WhatsApp itself lists a stranger's call under Calls without opening a
// conversation, and creating one would put a nameless, messageless entry into
// list_chats for every spam call. The caller is still in caller_jid.
//
// NULL rather than the caller's JID, even though nothing would reject it
// today: the schema declares chat_jid a FOREIGN KEY onto chats(jid), but this
// database runs with PRAGMA foreign_keys = 0 (measured, 2026-09-28), so SQLite
// does not enforce it. A dangling reference would be accepted now and become a
// violation the day enforcement is switched on. NULL is valid either way.
func callChatJID(ctx context.Context, db *sql.DB, e callEvent) (string, error) {
	var candidates []types.JID
	if !e.GroupJID.IsEmpty() {
		candidates = []types.JID{e.GroupJID}
	} else {
		candidates = []types.JID{e.Creator, e.CreatorAlt}
	}
	for _, j := range candidates {
		if j.IsEmpty() {
			continue
		}
		jid := j.ToNonAD().String()
		var found string
		err := db.QueryRowContext(ctx, `SELECT jid FROM chats WHERE jid = ?`, jid).Scan(&found)
		if err == nil {
			return found, nil
		}
		if err != sql.ErrNoRows {
			return "", fmt.Errorf("looking up chat %s: %w", jid, err)
		}
	}
	return "", nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// Event handlers.

// onCallEvent records one event, if call capture is on.
func (b *Bridge) onCallEvent(e callEvent) {
	if !b.cfg.CaptureCalls {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// WhatsApp hands over both forms of the caller on every call. That pairing
	// is exactly what name resolution needs, and it costs nothing to keep.
	recordJIDAlias(ctx, b.db, e.Creator.ToNonAD(), e.CreatorAlt.ToNonAD(), "call", e.At.Unix())

	if err := recordCall(ctx, b.db, e); err != nil {
		log.Printf("calls: %v", err)
	}
}

// isOwnJID reports whether j is this account, in either of its forms. An offer
// created by our own number is a call placed from the phone, not an incoming
// one.
func (b *Bridge) isOwnJID(j types.JID) bool {
	if b.client == nil || b.client.Store == nil || j.IsEmpty() {
		return false
	}
	nj := j.ToNonAD()
	if id := b.client.Store.ID; id != nil && nj == id.ToNonAD() {
		return true
	}
	if lid := b.client.Store.LID; !lid.IsEmpty() && nj == lid.ToNonAD() {
		return true
	}
	return false
}

func (b *Bridge) callEventFrom(meta types.BasicCallMeta) callEvent {
	return callEvent{
		ID:         meta.CallID,
		Creator:    meta.CallCreator,
		CreatorAlt: meta.CallCreatorAlt,
		GroupJID:   meta.GroupJID,
		IsGroup:    !meta.GroupJID.IsEmpty(),
		At:         meta.Timestamp,
		Outbound:   b.isOwnJID(meta.CallCreator),
	}
}

// ---------------------------------------------------------------------------
// GET /api/calls

type callItem struct {
	ID         string `json:"id"`
	ChatJID    string `json:"chat_jid,omitempty"`
	CallerJID  string `json:"caller_jid"`
	CallerName string `json:"caller_name,omitempty"`
	Timestamp  int64  `json:"timestamp"`
	CallType   string `json:"call_type"`
	IsGroup    bool   `json:"is_group"`
	IsOutbound bool   `json:"is_outbound"`
	// Empty while the call is still ringing, or if no outcome ever arrived.
	Result string `json:"result,omitempty"`
}

// handleListCalls lists recent calls, newest first.
//
//	GET /api/calls?limit=20&jid=<any form of a contact or group>
//
// The jid filter matches every alias of the person, for the same reason
// callChatJID tries both: the calls from someone may be filed under their @lid
// and their phone number interchangeably.
func (s *Server) handleListCalls(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := parseIntDefault(q.Get("limit"), 20)
	if limit < 1 {
		limit = 1
	}
	if limit > 200 {
		limit = 200
	}

	sqlStr := `SELECT id, COALESCE(chat_jid, ''), COALESCE(caller_jid, ''), timestamp, call_type,
	                  is_group, is_outbound, COALESCE(result, '')
	             FROM calls`
	var args []any
	if jid := strings.TrimSpace(q.Get("jid")); jid != "" {
		forms, err := resolveAliases(r.Context(), s.db, jid)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "alias lookup failed", Details: err.Error()})
			return
		}
		if len(forms) == 0 {
			forms = []string{jid}
		}
		ph := inClausePlaceholders(len(forms))
		sqlStr += ` WHERE caller_jid IN (` + ph + `) OR chat_jid IN (` + ph + `)`
		args = append(args, jidsToArgs(forms)...)
		args = append(args, jidsToArgs(forms)...)
	}
	sqlStr += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(r.Context(), sqlStr, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "calls query failed", Details: err.Error()})
		return
	}
	defer rows.Close()

	items := []callItem{}
	for rows.Next() {
		var c callItem
		var isGroup, isOutbound int
		if err := rows.Scan(&c.ID, &c.ChatJID, &c.CallerJID, &c.Timestamp, &c.CallType,
			&isGroup, &isOutbound, &c.Result); err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "calls scan failed", Details: err.Error()})
			return
		}
		c.IsGroup, c.IsOutbound = isGroup == 1, isOutbound == 1
		items = append(items, c)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "calls query failed", Details: err.Error()})
		return
	}
	rows.Close() // release the connection before the per-row name lookups

	for i := range items {
		items[i].CallerName = s.callerDisplayName(r.Context(), items[i].CallerJID)
	}
	out := callListResponse{Calls: items, Count: len(items)}
	// Same envelope as the other list endpoints, for the same reason: an empty
	// call list from a disconnected bridge must not read as "nobody called".
	// Guarded because tests build Server without a bridge.
	if s.bridge != nil {
		out.BridgeState = s.currentBridgeState()
	}
	writeJSON(w, http.StatusOK, out)
}

type callListResponse struct {
	Calls       []callItem        `json:"calls"`
	Count       int               `json:"count"`
	BridgeState *bridgeStateBrief `json:"_bridge_state,omitempty"`
}

// callerDisplayName finds a human name for a caller across all its aliases:
// the address-book name first, then the chat name, then whatever the person
// set for themselves. "" when nothing is known — a stranger.
func (s *Server) callerDisplayName(ctx context.Context, jid string) string {
	if jid == "" {
		return ""
	}
	forms, err := resolveAliases(ctx, s.db, jid)
	if err != nil || len(forms) == 0 {
		forms = []string{jid}
	}
	ph := inClausePlaceholders(len(forms))
	a := jidsToArgs(forms)
	var args []any
	for i := 0; i < 4; i++ {
		args = append(args, a...)
	}
	var name string
	err = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(
			(SELECT full_name     FROM contacts WHERE jid IN (`+ph+`) AND full_name     <> '' LIMIT 1),
			(SELECT name          FROM chats    WHERE jid IN (`+ph+`) AND name          <> '' LIMIT 1),
			(SELECT push_name     FROM contacts WHERE jid IN (`+ph+`) AND push_name     <> '' LIMIT 1),
			(SELECT verified_name FROM contacts WHERE jid IN (`+ph+`) AND verified_name <> '' LIMIT 1),
			'')`, args...).Scan(&name)
	if err != nil {
		return ""
	}
	return name
}
