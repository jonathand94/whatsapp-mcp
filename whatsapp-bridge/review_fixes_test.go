package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func seedDraft(t *testing.T, s *Server, id, status string, createdAt int64, filePath string) {
	t.Helper()
	var whatsappID, sentAt, errMsg any
	switch status {
	case "sent":
		whatsappID, sentAt = "3EB0SENT", int64(1792557300)
	case "failed":
		errMsg = "socket closed"
	}
	if _, err := s.db.Exec(`
		INSERT INTO sends (draft_id, recipient_jid, recipient_display, send_type, content_text,
		                   content_file_path, status, created_at, whatsapp_message_id, sent_at, error_message)
		VALUES (?, '5215555555555@s.whatsapp.net', 'Juan', 'file', '', ?, ?, ?, ?, ?, ?)`,
		id, nullString(filePath), status, createdAt, whatsappID, sentAt, errMsg,
	); err != nil {
		t.Fatalf("seeding draft %s: %v", id, err)
	}
}

func confirm(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/sends/"+id+"/confirm?wait=0", nil)
	req.SetPathValue("draft_id", id)
	rec := httptest.NewRecorder()
	s.handleConfirmSend(rec, req)
	return rec
}

// TestConfirmIsIdempotent pins the review's top finding: a large send answers
// 202, the caller confirms again to learn the outcome, and the old answer for
// an already-sent draft — 409 "create a new draft to retry" — is exactly what
// leads to sending the same file twice.
func TestConfirmIsIdempotent(t *testing.T) {
	s, _ := newMediaTestServer(t)
	now := time.Now().Unix()
	seedDraft(t, s, "sent-1", "sent", now, "")
	seedDraft(t, s, "failed-1", "failed", now, "")
	seedDraft(t, s, "inflight-1", "confirmed", now, "")

	rec := confirm(t, s, "sent-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("re-confirming a sent draft: %d %s", rec.Code, rec.Body)
	}
	var got confirmResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "sent" || got.WhatsAppMessageID != "3EB0SENT" {
		t.Fatalf("got %+v, want the original delivery", got)
	}

	if rec := confirm(t, s, "failed-1"); rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "socket closed") {
		t.Fatalf("a failed draft should say why: %d %s", rec.Code, rec.Body)
	}
	if rec := confirm(t, s, "inflight-1"); rec.Code != http.StatusConflict ||
		!strings.Contains(rec.Body.String(), "Revisa el chat") {
		t.Fatalf("a confirmed draft should send the user to check the chat: %d %s", rec.Code, rec.Body)
	}
}

func TestConfirmReturnsTheFinishedUploadsAnswer(t *testing.T) {
	s, _ := newMediaTestServer(t)
	j, _ := s.jobs().start("send:up-done", func(j *mediaJob) (int, any) {
		return http.StatusOK, confirmResponse{DraftID: "up-done", Status: "sent", WhatsAppMessageID: "3EB0X"}
	})
	j.wait(t.Context(), time.Second)

	rec := confirm(t, s, "up-done")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "3EB0X") {
		t.Fatalf("confirming after the upload finished: %d %s", rec.Code, rec.Body)
	}
}

func TestConfirmExplainsWhyPreparationFailed(t *testing.T) {
	s, _ := newMediaTestServer(t)
	j, _ := s.jobs().start("draft:prep-bad", func(j *mediaJob) (int, any) {
		return http.StatusBadRequest, errorResponse{Error: "media rejected", Details: "file_url respondio 403 Forbidden"}
	})
	j.wait(t.Context(), time.Second)

	rec := confirm(t, s, "prep-bad")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "403 Forbidden") {
		t.Fatalf("want the preparation's own error instead of 'not found': %d %s", rec.Code, rec.Body)
	}
}

func TestGrowingFileCountsEveryWrite(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "x.part"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var n atomic.Int64
	g := growingFile{File: f, written: &n}
	g.Write([]byte("abc"))
	g.WriteAt([]byte("ab"), 0) // a retry rewriting the start: no new size, still activity
	g.ReadFrom(strings.NewReader("defg"))
	if n.Load() != 9 {
		t.Fatalf("written = %d, want 9", n.Load())
	}
	if st, _ := f.Stat(); st.Size() != 7 {
		t.Fatalf("size = %d, want 7", st.Size())
	}
}

func TestFileURLStallIsCaughtBeforeTheFirstByte(t *testing.T) {
	old := stallTimeout
	stallTimeout = 300 * time.Millisecond
	defer func() { stallTimeout = old }()

	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release // headers, then nothing
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	_, _, err := fetchToMemory(t.Context(), srv.URL+"/slow.pdf", 1<<20, testFetcher(t, srv))
	if err == nil || !strings.Contains(err.Error(), "se detuvo") {
		t.Fatalf("want a stall error, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("the stall took %s to notice", time.Since(start))
	}
}

func TestMaterializeRefusesNonRegularFiles(t *testing.T) {
	cfg := testConfig(t)
	_, err := materializeOutboundFile(t.Context(), cfg, "d", createDraftRequest{
		SendType: "file", FilePath: os.DevNull,
	})
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("a device file must be refused, got %v", err)
	}
}

func TestDownloadErrorStatusSeparatesGoneFromUnreachable(t *testing.T) {
	gone := fmt.Errorf("%w (x), y %v", errMediaGone, errNotOnPhone)
	busy := fmt.Errorf("%w (x): el telefono no respondio", errPhoneUnreachable)
	if downloadErrorStatus(gone) != http.StatusGone {
		t.Fatal("a file the phone no longer has is gone for good: 410")
	}
	if downloadErrorStatus(busy) != http.StatusServiceUnavailable {
		t.Fatal("a phone that did not answer may answer later: 503")
	}
	if !strings.Contains(downloadErrorBody(busy).Details, "Vuelve a intentarlo") {
		t.Fatal("the 503 should invite a retry")
	}
	if errors.Is(busy, errMediaGone) {
		t.Fatal("the two must not be confused")
	}
}

func TestSweepExpiresAbandonedDraftsButNotRunningUploads(t *testing.T) {
	s, db := newMediaTestServer(t)
	dir := outboundDir(s.cfg)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	old := time.Now().Unix() - draftTTLSeconds - 60
	abandoned, uploading, fresh := file("old.pdf"), file("busy.mp4"), file("new.pdf")
	seedDraft(t, s, "old", "draft", old, abandoned)
	seedDraft(t, s, "busy", "draft", old, uploading)
	seedDraft(t, s, "new", "draft", time.Now().Unix(), fresh)

	release := make(chan struct{})
	defer close(release)
	s.jobs().start("send:busy", func(j *mediaJob) (int, any) { <-release; return 200, nil })

	s.sweepOutbound(t.Context())

	status := func(id string) (st string) {
		db.QueryRow(`SELECT status FROM sends WHERE draft_id = ?`, id).Scan(&st)
		return
	}
	if status("old") != "expired" {
		t.Fatalf("abandoned draft status = %q, want expired", status("old"))
	}
	if _, err := os.Stat(abandoned); !os.IsNotExist(err) {
		t.Fatal("the abandoned draft's file should be deleted")
	}
	if status("busy") != "draft" || status("new") != "draft" {
		t.Fatalf("busy=%q new=%q, both should stay drafts", status("busy"), status("new"))
	}
	for _, p := range []string{uploading, fresh} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s should be kept: %v", p, err)
		}
	}
}

func TestRemoveOrphansClearsThePreviousRunsScratch(t *testing.T) {
	s, _ := newMediaTestServer(t)
	dir := outboundDir(s.cfg)
	os.MkdirAll(dir, 0o700)
	orphans := []string{
		filepath.Join(dir, "d1.part"),
		filepath.Join(dir, ".upload-123.enc"),
		filepath.Join(dir, "d2.voice.ogg"),
		filepath.Join(s.cfg.MediaPath, "3EB0.mp4.part"),
	}
	keep := []string{filepath.Join(dir, "d3.pdf"), filepath.Join(s.cfg.MediaPath, "3EB0.mp4")}
	for _, p := range append(orphans, keep...) {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s.removeOrphans()
	for _, p := range orphans {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should be gone", p)
		}
	}
	for _, p := range keep {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s should be kept", p)
		}
	}
}
