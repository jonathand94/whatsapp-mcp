package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// --- names ------------------------------------------------------------------

func TestCleanMediaFilename(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"Contrato firmado.pdf", "Contrato firmado.pdf"},
		{"Cotización [v2] 100%.xlsx", "Cotización [v2] 100%.xlsx"},
		// Sender-chosen text: traversal must not survive as path structure.
		{"../../.bashrc", "_.._.bashrc"},
		{`..\..\Windows\evil.exe`, `_.._Windows_evil.exe`},
		{"a:b*c?d\"e<f>g|h.txt", "a_b_c_d_e_f_g_h.txt"},
		{"line\nbreak\t.pdf", "linebreak.pdf"},
		{"  .hidden  ", "hidden"},
		{"...", ""},
		{"", ""},
	} {
		if got := cleanMediaFilename(tc.in); got != tc.want {
			t.Errorf("cleanMediaFilename(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCleanMediaFilenameBoundsLengthAndKeepsExtension(t *testing.T) {
	long := strings.Repeat("ñ", 300) + ".pdf"
	got := cleanMediaFilename(long)
	if len(got) > maxFilenameBytes {
		t.Fatalf("len = %d, want <= %d", len(got), maxFilenameBytes)
	}
	if !strings.HasSuffix(got, ".pdf") {
		t.Fatalf("the extension was lost: %q", got)
	}
	if !strings.HasPrefix(got, "ñ") || strings.ContainsRune(got, '�') {
		t.Fatalf("truncation split a character: %q", got)
	}
}

func TestDisplayFilename(t *testing.T) {
	ts := time.Date(2026, 10, 21, 12, 34, 56, 0, time.Local).Unix()

	name, original := displayFilename("Factura 2026.pdf", "document", "application/pdf", ts)
	if name != "Factura 2026.pdf" || !original {
		t.Fatalf("a document keeps its own name: %q %v", name, original)
	}
	name, _ = displayFilename("Reporte", "document", "application/pdf", ts)
	if name != "Reporte.pdf" {
		t.Fatalf("a name with no extension gets one from the type: %q", name)
	}
	name, original = displayFilename("", "image", "image/jpeg", ts)
	if name != "Imagen 2026-10-21 12.34.56.jpg" || original {
		t.Fatalf("a photo gets a generated name: %q %v", name, original)
	}
	name, _ = displayFilename("", "voice", "audio/ogg; codecs=opus", ts)
	if name != "Nota de voz 2026-10-21 12.34.56.ogg" {
		t.Fatalf("voice note name: %q", name)
	}
}

// --- capturing the name and unwrapping envelopes -----------------------------

func TestExtractCapturesDocumentNameThroughEnvelopes(t *testing.T) {
	doc := &waE2E.DocumentMessage{
		MediaKey:   []byte("k"),
		DirectPath: proto.String("/v/t62/doc"),
		Mimetype:   proto.String("application/pdf"),
		FileName:   proto.String("Contrato firmado.pdf"),
	}
	for name, msg := range map[string]*waE2E.Message{
		"bare": {DocumentMessage: doc},
		// How a history-sync document with a caption arrives: one level down.
		// These rows used to get no media keys at all.
		"with caption": {DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{DocumentMessage: doc},
		}},
		"in a disappearing chat": {EphemeralMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{DocumentMessage: doc},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			f, ok := extractFromMessage(msg)
			if !ok || string(f.MediaKey) != "k" {
				t.Fatalf("no media fields extracted (ok=%v)", ok)
			}
			if f.MediaFilename.String != "Contrato firmado.pdf" {
				t.Fatalf("filename = %q", f.MediaFilename.String)
			}
		})
	}
	if f, _ := extractFromMessage(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{MediaKey: []byte("k")}}); f.MediaFilename.Valid {
		t.Fatal("a photo has no sender-given name to capture")
	}
}

// --- media retry plumbing ----------------------------------------------------

func TestIsExpiredMedia(t *testing.T) {
	for _, err := range []error{
		whatsmeow.ErrMediaDownloadFailedWith403,
		whatsmeow.ErrMediaDownloadFailedWith404,
		fmt.Errorf("wrapped: %w", whatsmeow.ErrMediaDownloadFailedWith410),
	} {
		if !isExpiredMedia(err) {
			t.Fatalf("%v should count as expired media", err)
		}
	}
	for _, err := range []error{nil, whatsmeow.ErrInvalidMediaHMAC, whatsmeow.ErrNoURLPresent} {
		if isExpiredMedia(err) {
			t.Fatalf("%v is not something a media retry can fix", err)
		}
	}
}

func TestMessageInfoForRetry(t *testing.T) {
	info, err := messageInfoForRetry("ABC", "5215555555555@s.whatsapp.net", "5215555555555@s.whatsapp.net", false)
	if err != nil || info.ID != "ABC" || info.IsGroup || info.Chat.User != "5215555555555" {
		t.Fatalf("direct chat: %+v %v", info, err)
	}
	info, err = messageInfoForRetry("ABC", "120363000000334521@g.us", "5215555555555@s.whatsapp.net", false)
	if err != nil || !info.IsGroup || info.Sender.User != "5215555555555" {
		t.Fatalf("group: %+v %v", info, err)
	}
	// The receipt has to name the participant in a group; without one the
	// phone cannot find the message, so fail before asking.
	if _, err := messageInfoForRetry("ABC", "120363000000334521@g.us", "", false); err == nil {
		t.Fatal("a group message with no sender should be refused")
	}
}

func TestMediaRetryWaitersDeliverToTheRightDownload(t *testing.T) {
	var w mediaRetryWaiters
	if w.deliver(&events.MediaRetry{MessageID: "nobody"}) {
		t.Fatal("delivery with no waiter should report false")
	}
	ch := w.add(types.MessageID("M1"))
	if !w.deliver(&events.MediaRetry{MessageID: "M1"}) {
		t.Fatal("the waiting download should receive its retry")
	}
	// A duplicate notification must not block the event loop.
	w.deliver(&events.MediaRetry{MessageID: "M1"})
	select {
	case evt := <-ch:
		if evt.MessageID != "M1" {
			t.Fatalf("got %s", evt.MessageID)
		}
	default:
		t.Fatal("nothing was delivered")
	}
	w.remove("M1", ch)
	if w.deliver(&events.MediaRetry{MessageID: "M1"}) {
		t.Fatal("a removed waiter should not receive anything")
	}
}

// --- the download endpoint ---------------------------------------------------

func newMediaTestServer(t *testing.T) (*Server, *sql.DB) {
	t.Helper()
	db := newCallsTestDB(t)
	cfg := &Config{MediaPath: t.TempDir()}
	return &Server{cfg: cfg, db: db, bridge: &Bridge{cfg: cfg, db: db}}, db
}

func TestDownloadMediaReturnsNamedFileFromCache(t *testing.T) {
	s, db := newMediaTestServer(t)
	seedChat(t, db, "5215555555555@s.whatsapp.net", "direct", "Juan Pérez")
	onDisk := filepath.Join(s.cfg.MediaPath, "3EB0AA.pdf")
	if err := os.WriteFile(onDisk, []byte("%PDF-1.7"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, is_from_me,
		                      media_key, media_mime, media_path, media_filename)
		VALUES ('3EB0AA', '5215555555555@s.whatsapp.net', '5215555555555@s.whatsapp.net', 'Juan', 1792557215,
		        'document', '', 0, x'01', 'application/pdf', ?, 'Contrato firmado.pdf')`, onDisk); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	s.handleDownloadMedia(rec, httptest.NewRequest(http.MethodPost, "/api/media/download",
		strings.NewReader(`{"message_id":"3EB0AA"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	var got downloadMediaResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Path != onDisk || !got.CachedHit || got.Size != 8 {
		t.Fatalf("file: %+v", got)
	}
	if got.Filename != "Contrato firmado.pdf" || !got.OriginalFilename {
		t.Fatalf("the sender's name should come back: %q %v", got.Filename, got.OriginalFilename)
	}
	if got.ChatName != "Juan Pérez" || got.SenderDisplay != "Juan" || got.Type != "document" || got.Timestamp != 1792557215 {
		t.Fatalf("metadata: %+v", got)
	}
	// The download's job is forgotten once it ends; its answer lives in the
	// messages table, so a later request takes the fast path again.
	if s.jobs().get("download:3EB0AA") != nil {
		t.Fatal("a finished download should not linger in the job table")
	}
}

// TestDownloadMediaFindsFilesBehindStalePaths pins the migration fix: rows
// carried over from Windows point at C:\Users\..., and the bridge used to ask
// WhatsApp for those files again (404 for older ones) although they were on
// this disk.
func TestDownloadMediaFindsFilesBehindStalePaths(t *testing.T) {
	s, db := newMediaTestServer(t)
	seedChat(t, db, "5215555555555@s.whatsapp.net", "direct", "")
	here := filepath.Join(s.cfg.MediaPath, "3EB0E71A.jfif")
	for name, body := range map[string]string{
		"3EB0E71A.jfif":      "jpeg bytes",
		"3EB0E71A.jfif.part": "half",  // an unfinished download is not a match...
		"3EB0E71A.voice.ogg": "other", // ...and neither is another file's transcode
	} {
		if err := os.WriteFile(filepath.Join(s.cfg.MediaPath, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// No media key at all: it must still be served, since nothing needs
	// downloading.
	if _, err := db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, timestamp, type, content_text, is_from_me, media_mime, media_path)
		VALUES ('3EB0E71A', '5215555555555@s.whatsapp.net', '', 1, 'image', '', 0, 'image/jpeg',
		        'C:\Users\jonat\.claude\whatsapp-mcp\media\3EB0E71A.jfif')`); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	s.handleDownloadMedia(rec, httptest.NewRequest(http.MethodPost, "/api/media/download",
		strings.NewReader(`{"message_id":"3EB0E71A"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	var got downloadMediaResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Path != here || !got.CachedHit || got.Size != int64(len("jpeg bytes")) {
		t.Fatalf("got %+v, want the local file", got)
	}
	var stored string
	if err := db.QueryRow(`SELECT media_path FROM messages WHERE id = '3EB0E71A'`).Scan(&stored); err != nil || stored != here {
		t.Fatalf("media_path should be repointed here, got %q (%v)", stored, err)
	}
}

func TestDownloadMediaWithoutKeysIs404(t *testing.T) {
	s, db := newMediaTestServer(t)
	seedChat(t, db, "5215555555555@s.whatsapp.net", "direct", "")
	if _, err := db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, timestamp, type, content_text, is_from_me)
		VALUES ('OLD1', '5215555555555@s.whatsapp.net', '', 1, 'image', '', 0)`); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	s.handleDownloadMedia(rec, httptest.NewRequest(http.MethodPost, "/api/media/download",
		strings.NewReader(`{"message_id":"OLD1"}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404: %s", rec.Code, rec.Body)
	}
}

// --- drafts, status and confirm ----------------------------------------------

func postDraft(t *testing.T, s *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleCreateDraft(rec, httptest.NewRequest(http.MethodPost, "/api/sends", strings.NewReader(body)))
	return rec
}

func getStatus(t *testing.T, s *Server, draftID string) sendStatusResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/sends/"+draftID, nil)
	req.SetPathValue("draft_id", draftID)
	rec := httptest.NewRecorder()
	s.handleSendStatus(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d: %s", rec.Code, rec.Body)
	}
	var got sendStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestFileDraftReportsDeliveryAndStatus(t *testing.T) {
	s, _ := newMediaTestServer(t)
	src := filepath.Join(t.TempDir(), "presentacion.mp4")
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(120 << 20); err != nil {
		t.Fatal(err)
	}
	f.Close()

	body, _ := json.Marshal(map[string]any{
		"send_type": "file", "recipient_jid": "5215555555555@s.whatsapp.net", "file_path": src,
	})
	rec := postDraft(t, s, string(body))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body)
	}
	var draft createDraftResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &draft); err != nil {
		t.Fatal(err)
	}
	if draft.Status != "draft" || draft.DeliveredAs != "document" || draft.SizeBytes != 120<<20 {
		t.Fatalf("draft = %+v", draft)
	}
	if !strings.Contains(draft.Note, "documento") || !strings.Contains(draft.Preview, "presentacion.mp4") {
		t.Fatalf("a downgraded video should say so: note=%q preview=%q", draft.Note, draft.Preview)
	}

	st := getStatus(t, s, draft.DraftID)
	if st.Status != "draft" || st.DeliveredAs != "document" || st.Preview != draft.Preview || st.ExpiresAt == 0 {
		t.Fatalf("status = %+v", st)
	}
}

func TestRejectedDraftKeepsItsReason(t *testing.T) {
	s, _ := newMediaTestServer(t)
	rec := postDraft(t, s, `{"send_type":"file","recipient_jid":"5215555555555@s.whatsapp.net","file_path":"/no/such/file.pdf"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400: %s", rec.Code, rec.Body)
	}
}

func TestConfirmWhilePreparingOrUploadingNeverStartsASecondUpload(t *testing.T) {
	s, _ := newMediaTestServer(t)
	release := make(chan struct{})
	defer close(release)

	// A draft whose bytes are still arriving.
	s.jobs().start("draft:prep1", func(j *mediaJob) (int, any) {
		j.bytesTotal.Store(1000)
		<-release
		return http.StatusOK, nil
	})
	if st := getStatus(t, s, "prep1"); st.Status != "preparing" {
		t.Fatalf("status = %q, want preparing", st.Status)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/sends/prep1/confirm", nil)
	req.SetPathValue("draft_id", "prep1")
	rec := httptest.NewRecorder()
	s.handleConfirmSend(rec, req)
	if rec.Code != http.StatusConflict || !bytes.Contains(rec.Body.Bytes(), []byte("preparing")) {
		t.Fatalf("confirming a preparing draft: %d %s", rec.Code, rec.Body)
	}

	// A draft already uploading: a second confirm joins the running upload.
	uploading, _ := s.jobs().start("send:up1", func(j *mediaJob) (int, any) {
		j.setPhase("subiendo a WhatsApp")
		<-release
		return http.StatusOK, nil
	})
	req = httptest.NewRequest(http.MethodPost, "/api/sends/up1/confirm?wait=0", nil)
	req.SetPathValue("draft_id", "up1")
	rec = httptest.NewRecorder()
	s.handleConfirmSend(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("a second confirm should join the upload with 202, got %d %s", rec.Code, rec.Body)
	}
	if again, created := s.jobs().start("send:up1", nil); created || again != uploading {
		t.Fatal("the upload job was replaced")
	}
	if st := getStatus(t, s, "up1"); st.Status != "uploading" {
		t.Fatalf("status = %q, want uploading", st.Status)
	}
}
