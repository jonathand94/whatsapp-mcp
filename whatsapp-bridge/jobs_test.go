package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestJobTableJoinsRunningJobAndReplacesFinishedOne(t *testing.T) {
	tbl := newJobTable()
	release := make(chan struct{})
	var runs atomic.Int32

	first, created := tbl.start("send:x", func(j *mediaJob) (int, any) {
		runs.Add(1)
		<-release
		return http.StatusOK, "first"
	})
	if !created {
		t.Fatal("the first start should create the job")
	}
	// A second confirm of the same draft while the first is uploading must
	// join it, not start a second upload.
	again, created := tbl.start("send:x", func(j *mediaJob) (int, any) {
		runs.Add(1)
		return http.StatusOK, "second"
	})
	if created || again != first {
		t.Fatal("a running job must be joined, not started twice")
	}

	close(release)
	if !first.wait(t.Context(), time.Second) {
		t.Fatal("job did not finish")
	}
	if first.status != http.StatusOK || first.body != "first" {
		t.Fatalf("answer = %d %v", first.status, first.body)
	}

	// After it finishes, starting again is a retry and gets a new job.
	retry, created := tbl.start("send:x", func(j *mediaJob) (int, any) {
		runs.Add(1)
		return http.StatusOK, "retry"
	})
	if !created || retry == first {
		t.Fatal("a finished job should be replaced by a new one")
	}
	retry.wait(t.Context(), time.Second)
	if got := runs.Load(); got != 2 {
		t.Fatalf("the work ran %d times, want 2", got)
	}
}

func TestJobTableForgetOnlyDropsTheSameJob(t *testing.T) {
	tbl := newJobTable()
	a, _ := tbl.start("download:m", func(j *mediaJob) (int, any) { return 200, nil })
	a.wait(t.Context(), time.Second)
	b, _ := tbl.start("download:m", func(j *mediaJob) (int, any) { return 200, nil })
	b.wait(t.Context(), time.Second)

	tbl.forget("download:m", a) // stale handle: must not drop b
	if tbl.get("download:m") != b {
		t.Fatal("forget with a stale job removed the current one")
	}
	tbl.forget("download:m", b)
	if tbl.get("download:m") != nil {
		t.Fatal("forget did not remove the job")
	}
}

func TestJobPanicStillAnswers(t *testing.T) {
	tbl := newJobTable()
	j, _ := tbl.start("k", func(j *mediaJob) (int, any) { panic("boom") })
	if !j.wait(t.Context(), time.Second) {
		t.Fatal("a panicking job must still finish, or every waiter hangs")
	}
	if j.status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", j.status)
	}
}

func TestFinishedJobsAreSwept(t *testing.T) {
	tbl := newJobTable()
	j, _ := tbl.start("k", func(j *mediaJob) (int, any) { return 200, nil })
	j.wait(t.Context(), time.Second)
	j.finished = time.Now().Add(-finishedJobRetention - time.Minute)
	if tbl.get("k") != nil {
		t.Fatal("a job past retention should be swept")
	}
}

func TestAnswerJobWritesResultOrProgress(t *testing.T) {
	tbl := newJobTable()
	release, ready := make(chan struct{}), make(chan struct{})
	j, _ := tbl.start("k", func(j *mediaJob) (int, any) {
		j.bytesTotal.Store(100)
		j.bytesDone.Store(40)
		j.setPhase("subiendo a WhatsApp")
		close(ready)
		<-release
		return http.StatusOK, map[string]string{"status": "sent"}
	})
	<-ready

	// Not done within ?wait=0: 202 with progress.
	rec := httptest.NewRecorder()
	answerJob(rec, httptest.NewRequest(http.MethodPost, "/x?wait=0", nil), j,
		jobPending{DraftID: "d1", Status: "uploading"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code = %d, want 202", rec.Code)
	}
	var p jobPending
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.DraftID != "d1" || p.Status != "uploading" || p.BytesTotal != 100 {
		t.Fatalf("pending = %+v", p)
	}

	// Done within the wait: the job's own answer, unchanged.
	close(release)
	rec = httptest.NewRecorder()
	answerJob(rec, httptest.NewRequest(http.MethodPost, "/x?wait=5", nil), j, jobPending{})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
}

func TestJobWaitIsClamped(t *testing.T) {
	for _, tc := range []struct {
		q    string
		want time.Duration
	}{
		{"", defaultJobWait},
		{"?wait=0", 0},
		{"?wait=5", 5 * time.Second},
		{"?wait=600", maxJobWait},
		{"?wait=-3", defaultJobWait},
		{"?wait=abc", defaultJobWait},
	} {
		if got := jobWait(httptest.NewRequest(http.MethodGet, "/x"+tc.q, nil)); got != tc.want {
			t.Fatalf("jobWait(%q) = %s, want %s", tc.q, got, tc.want)
		}
	}
}

func TestWatchStallFiresOnlyWhenNothingMoves(t *testing.T) {
	// Stuck: the counter never changes, so the watch cancels.
	ctx, cancel := context.WithCancel(context.Background())
	stop, fired := watchStall(cancel, func() int64 { return 7 }, 60*time.Millisecond)
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a stalled transfer was not cancelled")
	}
	if !fired.Load() {
		t.Fatal("the stall flag should be set when the watch cancels")
	}

	// Moving: the counter keeps changing, so nothing is cancelled.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	var n atomic.Int64
	stop2, fired2 := watchStall(cancel2, func() int64 { return n.Add(1) }, 60*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	stop2()
	if ctx2.Err() != nil || fired2.Load() {
		t.Fatal("a transfer that keeps moving must not be treated as stalled")
	}
}
