package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Server is the HTTP REST API the Python MCP server consumes.
// Runs on the loopback interface only (enforced in config.Validate()).
type Server struct {
	cfg        *Config
	db         *sql.DB
	bridge     *Bridge
	backfiller *TranscriptBackfiller
	mux        *http.ServeMux
	server     *http.Server

	// Outbound rate limiting. Always reached through s.sendGov(), never as a
	// bare field read — see the note on that method for why.
	governor *sendGovernor
	govOnce  sync.Once

	// Long media work (jobs.go). Reached through s.jobs() for the same reason
	// the governor is: tests build Server as a bare literal.
	jobTbl  *jobTable
	jobOnce sync.Once
}

// jobs returns the job table, building it on first use.
func (s *Server) jobs() *jobTable {
	s.jobOnce.Do(func() {
		if s.jobTbl == nil {
			s.jobTbl = newJobTable()
		}
	})
	return s.jobTbl
}

func NewServer(cfg *Config, db *sql.DB, bridge *Bridge, backfiller *TranscriptBackfiller) *Server {
	s := &Server{cfg: cfg, db: db, bridge: bridge, backfiller: backfiller, mux: http.NewServeMux()}
	s.registerRoutes()
	s.server = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.BridgeHost, cfg.BridgePort),
		Handler:           newOriginGuard(s.mux, cfg.AllowedOrigins),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET /healthcheck", s.handleHealthcheck)
	s.mux.HandleFunc("GET /api/status", s.handleStatus)

	// Headless auth surface: lets a supervisor (Mycelium Studio, a wrapper
	// script, curl) drive first-run pairing and post-logout recovery without
	// scraping terminal QR art. All additive; the terminal QR keeps working.
	s.mux.HandleFunc("GET /api/auth/qr", s.handleAuthQR)
	s.mux.HandleFunc("POST /api/auth/pair-phone", s.handlePairPhone)
	s.mux.HandleFunc("POST /api/auth/reconnect", s.handleAuthReconnect)

	s.mux.HandleFunc("GET /api/chats", s.handleListChats)
	s.mux.HandleFunc("GET /api/calls", s.handleListCalls)
	s.mux.HandleFunc("GET /api/messages", s.handleListMessages)
	s.mux.HandleFunc("GET /api/contacts/search", s.handleSearchContacts)
	s.mux.HandleFunc("GET /api/chats/search", s.handleSearchChats)
	s.mux.HandleFunc("GET /api/groups", s.handleListGroups)

	s.mux.HandleFunc("POST /api/sends", s.handleCreateDraft)
	s.mux.HandleFunc("GET /api/sends/{draft_id}", s.handleSendStatus)
	s.mux.HandleFunc("POST /api/sends/{draft_id}/confirm", s.handleConfirmSend)

	s.mux.HandleFunc("POST /api/presence/mark_read", s.handleMarkRead)
	s.mux.HandleFunc("POST /api/presence/typing", s.handleTyping)
	s.mux.HandleFunc("POST /api/presence/online", s.handleOnline)

	s.mux.HandleFunc("POST /api/admin/backfill-transcripts", s.handleBackfillTranscripts)

	// On-demand media download. Lets the receipts pipeline (and any other
	// Python consumer) materialize a single message's bytes without
	// flipping the global WHATSAPP_AUTO_DOWNLOAD_MEDIA flag.
	s.mux.HandleFunc("POST /api/media/download", s.handleDownloadMedia)

	// On-demand history-sync request. Asks WhatsApp for older messages in
	// a chat so processHistorySyncEvent can backfill media-key fields.
	// Recovers historical media for messages received before the
	// media-key-on-all-types patch.
	s.mux.HandleFunc("POST /api/admin/request-history", s.handleRequestHistory)
	s.mux.HandleFunc("POST /api/admin/backfill-decode", s.handleBackfillDecode)
	s.mux.HandleFunc("GET /api/admin/backfill-decode/status", s.handleBackfillDecodeStatus)
}

// handleRequestHistory triggers a peer HistorySyncOnDemandRequest for a chat.
// WhatsApp delivers the response asynchronously over the history-sync stream;
// this endpoint returns as soon as the request is sent.
func (s *Server) handleRequestHistory(w http.ResponseWriter, r *http.Request) {
	type req struct {
		ChatJID string `json:"chat_jid"`
		Count   int    `json:"count"`
	}
	var body req
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<14)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if body.ChatJID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "chat_jid required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	anchorID, resp, err := s.bridge.RequestChatHistory(ctx, body.ChatJID, body.Count)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error:   "history request failed",
			Details: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"chat_jid":        body.ChatJID,
		"anchor_message":  anchorID,
		"requested_count": body.Count,
		"sent_message_id": resp.ID,
		"sent_at_unix":    resp.Timestamp.Unix(),
		"hint":            "WhatsApp delivers the response asynchronously over the history-sync stream. Watch ~/Library/Logs/whatsapp-bridge.stdout.log for 'history_sync: backfilled media-key for N rows' lines, then re-run any consumers that need the historical media (e.g. POST /api/media/download for individual receipts).",
	})
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	// Announce the bound port for supervisors. With WHATSAPP_BRIDGE_PORT=0
	// the OS picks a free port (multi-instance installs); the one greppable
	// stdout line + the sidecar file are how they learn it.
	port := ln.Addr().(*net.TCPAddr).Port
	log.Printf("BRIDGE_LISTENING port=%d", port)
	s.writePortFile(port)
	err = s.server.Serve(ln)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// writePortFile drops the bound port next to the database so a supervisor
// can discover it without parsing stdout. Best-effort; never fatal.
func (s *Server) writePortFile(port int) {
	path := filepath.Join(filepath.Dir(s.cfg.DBPath), "bridge.port")
	if err := os.WriteFile(path, []byte(strconv.Itoa(port)), 0o600); err != nil {
		log.Printf("port file write failed (non-fatal): %v", err)
	}
}

func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// --- handlers --------------------------------------------------------------

type healthcheckResponse struct {
	Status      string `json:"status"`
	Version     string `json:"version"`
	DBEncrypted bool   `json:"db_encrypted"`
	SchemaVer   int    `json:"schema_version"`
	// MYC-3698 — the live journal mode, READ from the database rather than
	// remembered from startup. WAL is what lets this endpoint answer while
	// ingest is writing; anything else here means reads are serializing behind
	// writes and the latency below is not representative.
	JournalMode   string         `json:"journal_mode"`
	Timestamp     int64          `json:"timestamp"`
	Transcription map[string]any `json:"transcription,omitempty"`
	AliasCoverage AliasCoverage  `json:"alias_coverage"`
	// MYC-3284 — what the bridge could not read, made measurable.
	UndecodedTotal    int            `json:"undecoded_total"`
	UndecodedByType   map[string]int `json:"undecoded_by_type"`
	LegacyEmptySystem int            `json:"legacy_empty_system"`
	// MYC-3569 — what the bridge could not DECRYPT. Counted separately from
	// undecoded_total on purpose: these are a different failure at a different
	// layer (the message never reached the decoder), and a share of them are
	// recoverable by whatsmeow's resend request, so the two rates move
	// independently and merging them would hide both.
	UndecryptableTotal  int            `json:"undecryptable_total"`
	UndecryptableByMode map[string]int `json:"undecryptable_by_mode"`
}

// decodeStats is what the bridge could not read or could not decrypt. Returned
// as one value because every field comes from the SAME single pass over the
// `system` rows — see undecodedStats.
type decodeStats struct {
	UndecodedTotal      int
	UndecodedByType     map[string]int
	LegacyEmptySystem   int
	UndecryptableTotal  int
	UndecryptableByMode map[string]int
}

// undecodedStats counts what the bridge could not read (MYC-3284), so the loss
// is MEASURABLE instead of discovered by accident:
//   - total / byType: messages stored with the explicit unsupported marker,
//     keyed by the raw proto type recovered from it.
//   - legacyEmpty: PRE-floor rows that were silently dropped to an empty
//     "system" row and are still in the store. This is exactly the size of the
//     remaining backfill, so it can be driven to zero instead of guessed at.
//
// A failed count is logged and reported as zero: the healthcheck must still
// answer, and a logged error is not a silent one.
func (s *Server) undecodedStats(ctx context.Context) decodeStats {
	st := decodeStats{
		UndecodedByType:     map[string]int{},
		UndecryptableByMode: map[string]int{},
	}
	// MYC-3577 — these counters used to be `content_text LIKE '[unsupported: %'`
	// over every `system` row. Correct, and unscalable: five consecutive calls
	// on the live store measured 14.75s, 8.65s, 2.88s, 3.19s and 1.48s. That
	// spread is a scan warming its page cache, and a monitoring poll after an
	// idle period always pays the top of it.
	//
	// The decode outcome now lives in the indexed messages.raw_type column
	// (migration 006), written by every writer through the one shared
	// rawTypeForStorage helper. So the by-type breakdown is a GROUP BY that
	// `idx_messages_type_rawtype` COVERS: SQLite answers it from the index
	// alone and never reads a table row.
	//
	// Both counter families share this single aggregate, namespaced rather than
	// split in SQL, and the prefix routing happens in Go over the GROUPED
	// result (dozens of rows) instead of over the message table (tens of
	// thousands). The totals are then summed from those same rows, so a total
	// can no longer disagree with its own breakdown — which the previous
	// two-query shape allowed.
	rows, err := s.db.QueryContext(ctx,
		`SELECT raw_type, COUNT(*) FROM messages
		  WHERE type = 'system' AND raw_type IS NOT NULL
		  GROUP BY raw_type`)
	if err != nil {
		log.Printf("healthcheck: decode by-type query failed: %v", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var rawType string
			var n int
			if err := rows.Scan(&rawType, &n); err != nil {
				log.Printf("healthcheck: decode by-type scan failed: %v", err)
				continue
			}
			splitRawTypeCount(rawType, n, &st)
		}
		if err := rows.Err(); err != nil {
			log.Printf("healthcheck: decode by-type iteration failed: %v", err)
		}
	}

	return st
}

func (s *Server) handleHealthcheck(w http.ResponseWriter, r *http.Request) {
	var schemaVer int
	_ = s.db.QueryRowContext(r.Context(), "SELECT MAX(version) FROM schema_version").Scan(&schemaVer)

	// Transcription health: surface the actual data points an operator
	// needs to know whether voice notes are flowing through. Hides nothing.
	tx := map[string]any{
		"backend":    s.cfg.WhisperBackend,
		"language":   s.cfg.WhisperLanguage,
		"model_path": s.cfg.WhisperModelPath,
		"bin_path":   s.cfg.WhisperBinPath,
	}
	var pending int
	_ = s.db.QueryRowContext(r.Context(),
		`SELECT COUNT(*) FROM messages WHERE type IN ('voice','audio') AND voice_note_transcript IS NULL AND media_key IS NOT NULL`,
	).Scan(&pending)
	tx["pending_with_keys"] = pending

	var orphans int
	_ = s.db.QueryRowContext(r.Context(),
		`SELECT COUNT(*) FROM messages WHERE type IN ('voice','audio') AND voice_note_transcript IS NULL AND media_key IS NULL`,
	).Scan(&orphans)
	tx["orphan_no_keys"] = orphans

	var lastTranscript int64
	_ = s.db.QueryRowContext(r.Context(),
		`SELECT COALESCE(MAX(voice_note_transcript_at), 0) FROM messages WHERE voice_note_transcript IS NOT NULL`,
	).Scan(&lastTranscript)
	tx["last_transcript_at"] = lastTranscript

	if s.backfiller != nil {
		for k, v := range s.backfiller.Stats() {
			tx["sweeper_"+k] = v
		}
	}

	// Alias coverage. SuspiciousLIDPhones > 0 means the recurring-class bug
	// is present in the data: at least one @lid contact has its `phone`
	// column populated with the LID number itself (the legacy ingestion
	// pattern). Operators can spot regressions immediately from healthcheck.
	cov, covErr := computeAliasCoverage(r.Context(), s.db)
	if covErr != nil {
		log.Printf("healthcheck: alias coverage query failed: %v", covErr)
	}

	st := s.undecodedStats(r.Context())

	writeJSON(w, http.StatusOK, healthcheckResponse{
		Status:              "ok",
		JournalMode:         JournalMode(s.db),
		Version:             bridgeVersion,
		DBEncrypted:         s.cfg.EncryptDB,
		SchemaVer:           schemaVer,
		Timestamp:           time.Now().Unix(),
		Transcription:       tx,
		AliasCoverage:       cov,
		UndecodedTotal:      st.UndecodedTotal,
		UndecodedByType:     st.UndecodedByType,
		LegacyEmptySystem:   st.LegacyEmptySystem,
		UndecryptableTotal:  st.UndecryptableTotal,
		UndecryptableByMode: st.UndecryptableByMode,
	})
}

// handleBackfillTranscripts fires one immediate sweep. Returns the count
// enqueued. The periodic sweeper continues to run on its own cadence;
// this is purely an on-demand trigger (e.g. operator just received voice
// notes during an outage and wants them transcribed now).
func (s *Server) handleBackfillTranscripts(w http.ResponseWriter, r *http.Request) {
	if s.backfiller == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "backfiller not configured"})
		return
	}
	n, err := s.backfiller.SweepOnce(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "sweep failed", Details: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"enqueued":  n,
		"timestamp": time.Now().Unix(),
	})
}

type statusResponse struct {
	Connected         bool   `json:"connected"`
	Authenticated     bool   `json:"authenticated"`
	DeviceJID         string `json:"device_jid,omitempty"`
	LastSyncTime      int64  `json:"last_sync_time,omitempty"`
	WhisperBackend    string `json:"whisper_backend"`
	VaultCRMEnabled   bool   `json:"vault_crm_enabled"`
	CaptureCalls      bool   `json:"capture_calls"`
	AutoDownloadMedia bool   `json:"auto_download_media"`

	// Auth lifecycle (additive; see auth.go). auth_state values:
	// unauthenticated | qr_pending | pairing_pending | paired | logged_out | timed_out
	AuthState          string `json:"auth_state"`
	QRExpiresAt        int64  `json:"qr_expires_at,omitempty"`
	PairingCodePending bool   `json:"pairing_code_pending,omitempty"`
	LoggedOutReason    string `json:"logged_out_reason,omitempty"`

	// SendBudget is the outbound rate-limit consumption (see pacing.go).
	// Reported here so a caller can see how close the ceiling is BEFORE a send
	// is refused, rather than discovering the limits only through the 429 that
	// announces them. An agent that can read this can also pace itself.
	SendBudget map[string]any `json:"send_budget,omitempty"`
	// DisconnectedSeconds is how long the WhatsApp socket has been down, absent
	// while it is up. `connected: false` on its own cannot distinguish a
	// two-second blip from the 24-hour outage of 2026-08-24, and an AI agent
	// reading this endpoint reported "authenticated but disconnected" without
	// being able to say it had been that way for a day. The duration is what
	// makes the state actionable. See watchdog.go.
	DisconnectedSeconds int64 `json:"disconnected_seconds,omitempty"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	connected, authed, deviceJID, lastSync := s.bridge.Status()
	snap := s.bridge.AuthSnapshot()
	resp := statusResponse{
		Connected:          connected,
		Authenticated:      authed,
		DeviceJID:          deviceJID,
		LastSyncTime:       lastSync,
		WhisperBackend:     s.cfg.WhisperBackend,
		VaultCRMEnabled:    s.cfg.VaultCRMPath != "",
		CaptureCalls:       s.cfg.CaptureCalls,
		AutoDownloadMedia:  s.cfg.AutoDownloadMedia,
		AuthState:          string(snap.State),
		PairingCodePending: snap.PairingCode != "",
		LoggedOutReason:    snap.LoggedOutReason,
		SendBudget:         s.sendGov().Snapshot(time.Now()),
	}
	if !snap.QRExpiresAt.IsZero() && snap.QRCode != "" {
		resp.QRExpiresAt = snap.QRExpiresAt.Unix()
	}
	if down := s.bridge.DisconnectedFor(); down > 0 {
		resp.DisconnectedSeconds = int64(down.Seconds())
	}
	writeJSON(w, http.StatusOK, resp)
}

// bridgeStateBrief rides along on read responses so a consumer can tell
// live data from a stale cache: the read endpoints serve SQLite even when
// the WhatsApp session is logged out, which used to be indistinguishable
// from a healthy sync.
type bridgeStateBrief struct {
	Connected     bool   `json:"connected"`
	Authenticated bool   `json:"authenticated"`
	AuthState     string `json:"auth_state"`
}

func (s *Server) currentBridgeState() *bridgeStateBrief {
	connected, authed, _, _ := s.bridge.Status()
	snap := s.bridge.AuthSnapshot()
	return &bridgeStateBrief{Connected: connected, Authenticated: authed, AuthState: string(snap.State)}
}

func (s *Server) handleAuthQR(w http.ResponseWriter, r *http.Request) {
	snap := s.bridge.AuthSnapshot()
	if snap.QRCode == "" {
		writeJSON(w, http.StatusConflict, errorResponse{
			Error:   "no_active_qr_code",
			Details: fmt.Sprintf("auth_state is %q; POST /api/auth/reconnect to start pairing", snap.State),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"qr_code":    snap.QRCode,
		"expires_at": snap.QRExpiresAt.Unix(),
		"state":      string(snap.State),
	})
}

func (s *Server) handlePairPhone(w http.ResponseWriter, r *http.Request) {
	type req struct {
		PhoneNumber string `json:"phone_number"`
	}
	var body req
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if body.PhoneNumber == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "phone_number required", Details: "international format, e.g. +15551234567"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	code, err := s.bridge.RequestPairingCode(ctx, body.PhoneNumber)
	if err != nil {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "pairing_code_unavailable", Details: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pairing_code": code,
		"formatted":    FormatPairingCode(code),
		"state":        string(AuthStatePairingPending),
		"hint":         "On the phone: WhatsApp > Settings > Linked Devices > Link a Device > 'Link with phone number instead', then type the code. Works on Android and iOS.",
	})
}

func (s *Server) handleAuthReconnect(w http.ResponseWriter, r *http.Request) {
	snap, err := s.bridge.Reconnect(r.Context())
	if err != nil {
		writeJSON(w, http.StatusBadGateway, errorResponse{Error: "reconnect_failed", Details: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"auth_state": string(snap.State)})
}

type chatRow struct {
	JID                string `json:"jid"`
	ChatType           string `json:"chat_type"`
	Name               string `json:"name"`
	LastMessageTime    int64  `json:"last_message_time"`
	LastMessagePreview string `json:"last_message_preview"`
	UnreadCount        int    `json:"unread_count"`
	// MergedJIDs lists every JID of this chat when it is known under more
	// than one (a phone JID and a @lid); see chat_lookup.go.
	MergedJIDs []string `json:"merged_jids,omitempty"`
}

type chatListResponse struct {
	Chats       []chatRow         `json:"chats"`
	Count       int               `json:"count"`
	BridgeState *bridgeStateBrief `json:"_bridge_state,omitempty"`
}

func (s *Server) handleListChats(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := parseIntDefault(q.Get("limit"), 20)
	if limit > 200 {
		limit = 200
	}
	offset := parseIntDefault(q.Get("offset"), 0)
	unreadOnly := q.Get("unread_only") == "true"

	where := ""
	if unreadOnly {
		where = "unread_count > 0"
	}
	chats, err := s.chatsPage(r.Context(), where, nil, offset, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "query failed", Details: err.Error()})
		return
	}
	out := chatListResponse{Chats: chats}
	out.Count = len(out.Chats)
	out.BridgeState = s.currentBridgeState()
	writeJSON(w, http.StatusOK, out)
}

type messageRow struct {
	ID            string  `json:"id"`
	ChatJID       string  `json:"chat_jid"`
	SenderJID     string  `json:"sender_jid"`
	SenderDisplay string  `json:"sender_display"`
	Timestamp     int64   `json:"timestamp"`
	Type          string  `json:"type"`
	ContentText   string  `json:"content_text"`
	IsFromMe      bool    `json:"is_from_me"`
	Transcript    *string `json:"voice_note_transcript,omitempty"`
	QuotedID      string  `json:"quoted_message_id,omitempty"`
	// What a media message carries, known without downloading it: a document
	// with no caption used to be just "document", so finding "the .docx Rodrigo
	// sent" meant downloading every attachment in the chat. Filename is only
	// the sender's own name for it (documents); photos and voice notes have none.
	Filename string `json:"filename,omitempty"`
	Mime     string `json:"mime,omitempty"`
	Size     int64  `json:"size,omitempty"`
}

// mediaTypeFilters are the values list_messages accepts for media_type.
var mediaTypeFilters = map[string][]string{
	"image":    {"image"},
	"video":    {"video"},
	"audio":    {"audio", "voice"},
	"voice":    {"voice"},
	"document": {"document"},
	"sticker":  {"sticker"},
	"media":    {"image", "video", "audio", "voice", "document", "sticker"},
}

type messageListResponse struct {
	Messages    []messageRow      `json:"messages"`
	Count       int               `json:"count"`
	Chat        *chatRow          `json:"chat,omitempty"`
	MergedJIDs  []string          `json:"merged_jids,omitempty"`
	BridgeState *bridgeStateBrief `json:"_bridge_state,omitempty"`
}

func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	chatJID := q.Get("chat_jid")
	if chatJID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "chat_jid required"})
		return
	}
	limit := parseIntDefault(q.Get("limit"), 20)
	if limit > 500 {
		limit = 500
	}

	// Expand the requested JID through jid_aliases so a query for either the
	// LID form or the @s.whatsapp.net form returns the merged history.
	// Without this, a contact whose recent traffic moved to LID looks silent
	// when queried by their legacy phone JID.
	allJIDs, err := resolveAliases(r.Context(), s.db, chatJID)
	if err != nil {
		log.Printf("handleListMessages: alias resolve failed (continuing with single jid): %v", err)
		allJIDs = []string{chatJID}
	}

	// Pick the chat row to return: prefer the alias whose chats row has the
	// most recent activity, so callers get a sensible "current" name + preview.
	var chat chatRow
	{
		args := jidsToArgs(allJIDs)
		query := fmt.Sprintf(`
			SELECT jid, chat_type, COALESCE(name, ''), COALESCE(last_message_time, 0),
			       COALESCE(last_message_preview, ''), unread_count
			FROM chats WHERE jid IN (%s)
			ORDER BY last_message_time DESC
			LIMIT 1
		`, inClausePlaceholders(len(allJIDs)))
		_ = s.db.QueryRowContext(r.Context(), query, args...).Scan(
			&chat.JID, &chat.ChatType, &chat.Name, &chat.LastMessageTime, &chat.LastMessagePreview, &chat.UnreadCount,
		)
	}

	// Keyset pagination. `before` is a message ID; return messages strictly
	// older than it, ordered by (timestamp, id) so messages sharing a
	// one-second WhatsApp timestamp are neither skipped nor repeated across
	// pages. `before` was already sent by the Python MCP layer but silently
	// ignored here, so every page returned the newest slice and any chat past
	// `limit` messages was unreachable.
	where := fmt.Sprintf("chat_jid IN (%s)", inClausePlaceholders(len(allJIDs)))
	args := jidsToArgs(allJIDs)
	if before := q.Get("before"); before != "" {
		var beforeTS int64
		anchorArgs := append(jidsToArgs(allJIDs), before)
		anchorQ := fmt.Sprintf("SELECT timestamp FROM messages WHERE chat_jid IN (%s) AND id = ? LIMIT 1",
			inClausePlaceholders(len(allJIDs)))
		if err := s.db.QueryRowContext(r.Context(), anchorQ, anchorArgs...).Scan(&beforeTS); err == nil {
			where += " AND (timestamp < ? OR (timestamp = ? AND id < ?))"
			args = append(args, beforeTS, beforeTS, before)
		} else {
			// Unknown anchor id → treat as end-of-history (return no older
			// rows) rather than silently restarting from the newest page.
			where += " AND 1 = 0"
		}
	}
	// Rows of plumbing stored before the bridge stopped keeping them (see
	// isPlumbing) are blank; they are left in the database but not shown.
	where += " AND NOT (type = 'system' AND COALESCE(content_text, '') = '')"

	// Filters, so finding an attachment does not mean paging through a chat:
	// media_type (see mediaTypeFilters), since/until (unix seconds, inclusive)
	// and from_me (true: only the user's messages; false: only the others').
	if mt := q.Get("media_type"); mt != "" {
		kinds, ok := mediaTypeFilters[mt]
		if !ok {
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error:   fmt.Sprintf("media_type %q not supported", mt),
				Details: "use image, video, audio, voice, document, sticker or media"})
			return
		}
		where += fmt.Sprintf(" AND type IN (%s)", inClausePlaceholders(len(kinds)))
		args = append(args, jidsToArgs(kinds)...)
	}
	if v := q.Get("since"); v != "" {
		where += " AND timestamp >= ?"
		args = append(args, parseIntDefault(v, 0))
	}
	if v := q.Get("until"); v != "" {
		where += " AND timestamp <= ?"
		args = append(args, parseIntDefault(v, 0))
	}
	switch q.Get("from_me") {
	case "true":
		where += " AND is_from_me = 1"
	case "false":
		where += " AND is_from_me = 0"
	}

	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT id, chat_jid, COALESCE(sender_jid, ''), COALESCE(sender_display, ''), timestamp, type,
		       COALESCE(scrubbed_text, COALESCE(content_text, '')),
		       is_from_me, voice_note_transcript, COALESCE(quoted_message_id, ''),
		       COALESCE(media_filename, ''), COALESCE(media_mime, ''), COALESCE(media_file_length, 0)
		FROM messages
		WHERE %s
		ORDER BY timestamp DESC, id DESC
		LIMIT ?
	`, where)

	rows, err := s.db.QueryContext(r.Context(), query, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "query failed", Details: err.Error()})
		return
	}
	defer rows.Close()

	out := messageListResponse{Messages: []messageRow{}}
	ownName := s.ownDisplayName()
	for rows.Next() {
		var m messageRow
		var fromMe int
		if err := rows.Scan(&m.ID, &m.ChatJID, &m.SenderJID, &m.SenderDisplay, &m.Timestamp, &m.Type,
			&m.ContentText, &fromMe, &m.Transcript, &m.QuotedID,
			&m.Filename, &m.Mime, &m.Size); err != nil {
			continue
		}
		m.IsFromMe = fromMe == 1
		// The user's own messages synced from the phone often carry no push
		// name, and were shown as a bare number ("99990069399676"). They are
		// the user's; say so.
		if m.IsFromMe && isBareNumber(m.SenderDisplay) {
			m.SenderDisplay = ownName
		}
		out.Messages = append(out.Messages, m)
	}
	out.Count = len(out.Messages)
	if chat.JID != "" {
		out.Chat = &chat
	}
	if len(allJIDs) > 1 {
		out.MergedJIDs = allJIDs
	}
	out.BridgeState = s.currentBridgeState()
	writeJSON(w, http.StatusOK, out)
}

type contactRow struct {
	JID   string `json:"jid"`
	LID   string `json:"lid,omitempty"`
	Phone string `json:"phone,omitempty"`
	// FullName is the name from the USER's address book; PushName is the one
	// the contact chose for themselves. Both are reported rather than collapsed:
	// they routinely disagree ("Mi Cielo" versus "Dra Laura De La Rosa") and a
	// caller deciding who to message needs to see the label it was asked about,
	// not a merged best guess.
	FullName     string   `json:"full_name,omitempty"`
	PushName     string   `json:"push_name"`
	VerifiedName string   `json:"verified_name,omitempty"`
	IsBusiness   bool     `json:"is_business"`
	Aliases      []string `json:"aliases,omitempty"` // other JIDs known to refer to the same human
}

type contactListResponse struct {
	Contacts    []contactRow      `json:"contacts"`
	Count       int               `json:"count"`
	BridgeState *bridgeStateBrief `json:"_bridge_state,omitempty"`
}

func (s *Server) handleSearchContacts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := q.Get("q")
	limit := parseIntDefault(q.Get("limit"), 10)
	if limit > 100 {
		limit = 100
	}

	if query == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "q parameter required"})
		return
	}
	norm := Normalize(query)

	// Initial match by name/phone/lid. We then expand each match through
	// jid_aliases so callers see every JID known to refer to the same human
	// (LID + phone-JID forms). Without this, a name search returns only the
	// row whose stored push_name happens to match exactly, hiding the alias.
	//
	// normalized_full_name is searched alongside normalized_name because the
	// address-book label is usually the only name the user knows. While that
	// column went unread, searching "Mi Cielo" returned zero and the caller was
	// pushed into supplying a phone number from memory — which is exactly the
	// job an address book exists to do. See contacts_sync.go.
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT jid, COALESCE(lid, ''), COALESCE(phone, ''), COALESCE(full_name, ''),
		       COALESCE(push_name, ''), COALESCE(verified_name, ''), is_business
		FROM contacts
		WHERE normalized_name LIKE ? OR normalized_full_name LIKE ? OR phone LIKE ? OR lid LIKE ?
		ORDER BY updated_at DESC
		LIMIT ?
	`, "%"+norm+"%", "%"+norm+"%", "%"+query+"%", "%"+query+"%", limit*5) // device rows fold away below
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "query failed", Details: err.Error()})
		return
	}

	// Buffer initial matches; we'll expand and dedupe before responding.
	type match struct {
		c     contactRow
		isBiz int
	}
	var matches []match
	for rows.Next() {
		var m match
		if err := rows.Scan(&m.c.JID, &m.c.LID, &m.c.Phone, &m.c.FullName, &m.c.PushName, &m.c.VerifiedName, &m.isBiz); err != nil {
			continue
		}
		m.c.IsBusiness = m.isBiz == 1
		matches = append(matches, m)
	}
	rows.Close()

	// One entry per person: device variants and alias rows (phone JID and
	// @lid) fold together, with the other forms listed under aliases. Callers
	// can list_messages with any of them; the bridge merges the history.
	// Rows for an alias that did not match the query are pulled in so their
	// names can fill gaps. See chat_lookup.go.
	rowsOut := make([]contactRow, 0, len(matches))
	seen := map[string]bool{}
	for _, m := range matches {
		rowsOut = append(rowsOut, m.c)
		seen[nonADJID(m.c.JID)] = true
	}
	for _, m := range matches {
		aliases, _ := resolveAliases(r.Context(), s.db, nonADJID(m.c.JID))
		for _, alt := range aliases {
			if seen[alt] {
				continue
			}
			seen[alt] = true
			if altRow, ok := loadContactRow(r.Context(), s.db, alt); ok {
				rowsOut = append(rowsOut, altRow)
			}
		}
	}
	out := contactListResponse{Contacts: mergeContacts(r.Context(), s.db, rowsOut)}
	if len(out.Contacts) > limit {
		out.Contacts = out.Contacts[:limit]
	}

	out.Count = len(out.Contacts)
	out.BridgeState = s.currentBridgeState()
	writeJSON(w, http.StatusOK, out)
}

// loadContactRow fetches a single contacts row by JID. Returns (row, true)
// when present. Used by handleSearchContacts to surface alias rows that did
// not match the name query directly.
func loadContactRow(ctx context.Context, db *sql.DB, jid string) (contactRow, bool) {
	var c contactRow
	var isBiz int
	err := db.QueryRowContext(ctx, `
		SELECT jid, COALESCE(lid, ''), COALESCE(phone, ''), COALESCE(full_name, ''),
		       COALESCE(push_name, ''), COALESCE(verified_name, ''), is_business
		FROM contacts WHERE jid = ?
	`, jid).Scan(&c.JID, &c.LID, &c.Phone, &c.FullName, &c.PushName, &c.VerifiedName, &isBiz)
	if err != nil {
		return c, false
	}
	c.IsBusiness = isBiz == 1
	return c, true
}

// --- helpers ----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("encode response: %v", err)
	}
}

type errorResponse struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
	// RetryAfterMS, on a rate-limit refusal, is how long until the limit that
	// refused would let the request through (also sent as Retry-After).
	RetryAfterMS int64 `json:"retry_after_ms,omitempty"`
}

func notImplemented(w http.ResponseWriter, tool, details string) {
	writeJSON(w, http.StatusNotImplemented, errorResponse{
		Error:   fmt.Sprintf("%s: not implemented in current version", tool),
		Details: details,
	})
}

func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

// handleBackfillDecode drives the MYC-3284 content backfill: it asks WhatsApp
// to re-deliver history for the chats still holding empty rows. The repair
// lands asynchronously as those chunks arrive (processHistorySyncEvent ->
// backfillDecodedContent), so the response reports what was REQUESTED plus the
// outstanding count — deliberately not "fixed N", which this handler cannot
// know yet. Poll /healthcheck decoding.legacy_empty_system to watch it fall.
func (s *Server) handleBackfillDecode(w http.ResponseWriter, r *http.Request) {
	if s.bridge == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "bridge not configured"})
		return
	}
	maxChats := atoiDefaultPositive(r.URL.Query().Get("max_chats"), 20)
	perChat := atoiDefaultPositive(r.URL.Query().Get("per_chat"), 100)
	walkBudget := atoiDefaultPositive(r.URL.Query().Get("walk_budget"), defaultWalkBudget)
	maxRounds := atoiDefaultPositive(r.URL.Query().Get("max_rounds"), defaultMaxWalkRounds)

	requested, skipped, remaining, err := s.bridge.SweepDecodeBackfill(r.Context(), DecodeBackfillOptions{
		ChatJID:    r.URL.Query().Get("chat_jid"),
		MaxChats:   maxChats,
		PerChat:    perChat,
		WalkBudget: walkBudget,
		MaxRounds:  maxRounds,
	})
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "decode backfill sweep failed", Details: err.Error()})
		return
	}
	active, spent, budget, stopped := s.bridge.WalkStats()
	writeJSON(w, http.StatusOK, map[string]any{
		"chats_requested": requested,
		"chats_skipped":   skipped,
		"empty_rows_now":  remaining,
		"per_chat":        perChat,
		"walk": map[string]any{
			"active_chats":    active,
			"requests_spent":  spent,
			"request_budget":  budget,
			"max_rounds":      maxRounds,
			"stopped_reasons": stopped,
		},
		"note":      "each delivered chunk anchors the next step backwards; poll GET /api/admin/backfill-decode/status",
		"timestamp": time.Now().Unix(),
	})
}

// handleBackfillDecodeStatus reports walk progress WITHOUT starting a sweep.
// A sweep is otherwise the only way to learn where the walk reached, and using
// one to check progress would itself spend request budget — so observing has to
// be separable from acting.
func (s *Server) handleBackfillDecodeStatus(w http.ResponseWriter, r *http.Request) {
	if s.bridge == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "bridge not configured"})
		return
	}
	remaining, err := s.bridge.CountEmptySystemRows(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "count failed", Details: err.Error()})
		return
	}
	active, spent, budget, stopped := s.bridge.WalkStats()
	writeJSON(w, http.StatusOK, map[string]any{
		"empty_rows_now": remaining,
		"walk": map[string]any{
			"active_chats":    active,
			"requests_spent":  spent,
			"request_budget":  budget,
			"stopped_reasons": stopped,
		},
		"timestamp": time.Now().Unix(),
	})
}

// atoiDefaultPositive parses a positive query-string integer, falling back on
// anything missing or invalid.
func atoiDefaultPositive(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
