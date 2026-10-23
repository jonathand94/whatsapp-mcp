// jobs.go — media work that outlives the HTTP request that started it.
//
// Once files may reach WhatsApp's 2 GB ceiling, three operations can take
// minutes: downloading an attachment, preparing a draft from a URL, and the
// upload a media confirm performs. Nothing in front of the bridge waits that
// long. The Python MCP layer gives each bridge call 30 s, and claude.ai gives a
// whole tool call about a minute. Holding the request open had two failure
// modes, both real:
//
//   - The server's 60 s WriteTimeout fired while a large upload was still
//     running. The send then succeeded with nobody left to tell, and the
//     caller saw an error for a message that was delivered — the setup for a
//     duplicate send.
//   - Tying the work to the request context meant a client that gave up took
//     the half-finished upload down with it.
//
// So each of them runs as a job: started by a request, detached from it, and
// waited on for at most a few seconds. Work that finishes in time answers
// exactly as it always did; work that does not answers 202 with progress, and
// the caller polls. Jobs live only in memory. Nothing here has to survive a
// restart: a draft flips to 'confirmed' only after its upload, so a restart
// mid-upload leaves it a plain draft, and a download is simply started again.

package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultJobWait is how long a request waits for its job before answering
	// 202. It sits under the Python layer's 30 s client timeout with room for
	// the rest of the handler.
	defaultJobWait = 20 * time.Second

	// maxJobWait caps ?wait=, so no request can hold its connection past the
	// server's 60 s WriteTimeout.
	maxJobWait = 25 * time.Second

	// finishedJobRetention is how long a finished job's answer stays available
	// to pollers: long enough for someone to come back to a conversation, short
	// enough that the table cannot grow without bound.
	finishedJobRetention = 2 * time.Hour

	// mediaJobTimeout bounds a whole download, preparation or upload. It is a
	// backstop, not the expected duration: 2 GB over a 1 Mbit/s uplink takes
	// about 4.5 hours, and a transfer that stops moving is caught much sooner
	// by watchStall.
	mediaJobTimeout = 8 * time.Hour
)

// stallTimeout is how long a transfer may go without moving a byte before it
// is abandoned. A variable only so tests can shorten it.
var stallTimeout = 3 * time.Minute

// mediaJob is one long operation and, once it ends, its final answer.
type mediaJob struct {
	started time.Time
	done    chan struct{}

	// Progress, readable while the job runs.
	bytesDone  atomic.Int64
	bytesTotal atomic.Int64
	phase      atomic.Value // string, in the user's language
	// progressFile, when set, is a file the job is filling inside a library
	// that does not report progress; its size is the progress.
	progressFile atomic.Value // string
	// moves counts activity that is not progress a poller should see: phase
	// changes, and bytes a library rewrites when it retries from the start.
	// Without it a job that moved on to another step, or is re-covering
	// ground it already covered, would look stalled to watchStall.
	moves atomic.Int64

	// The final answer: written once, before done is closed, and read only
	// after done is closed.
	finished time.Time
	status   int
	body     any
	// delivered: a caller has been handed the final answer.
	delivered atomic.Bool
}

func (j *mediaJob) setPhase(p string) {
	if j != nil {
		j.phase.Store(p)
		j.moves.Add(1)
	}
}

// activity changes whenever the job makes progress of any kind — bytes moved,
// its progress file grew, or it changed phase. It is what watchStall watches.
func (j *mediaJob) activity() int64 {
	return j.pending(jobPending{}).BytesDone + j.moves.Load()
}

// rootContext is what jobs run under: the bridge's process-lifetime context,
// so a shutdown stops them, or Background when there is no bridge (tests).
func (s *Server) rootContext() context.Context {
	if s.bridge != nil && s.bridge.rootCtx != nil {
		return s.bridge.rootCtx
	}
	return context.Background()
}

func (j *mediaJob) running() bool {
	select {
	case <-j.done:
		return false
	default:
		return true
	}
}

// wait blocks until the job finishes, d elapses or ctx ends, and reports
// whether the job finished.
func (j *mediaJob) wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return !j.running()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-j.done:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// jobPending is the answer for work still running. It doubles as the status
// body, which is why it carries identifying fields for every kind of job.
type jobPending struct {
	MessageID        string `json:"message_id,omitempty"`
	DraftID          string `json:"draft_id,omitempty"`
	RecipientJID     string `json:"recipient_jid,omitempty"`
	RecipientDisplay string `json:"recipient_display,omitempty"`
	Status           string `json:"status"`
	Phase            string `json:"phase,omitempty"`
	BytesDone        int64  `json:"bytes_done"`
	BytesTotal       int64  `json:"bytes_total,omitempty"`
	ElapsedSeconds   int64  `json:"elapsed_seconds"`
	Hint             string `json:"hint,omitempty"`
}

// pending fills p's progress fields from the job.
func (j *mediaJob) pending(p jobPending) jobPending {
	p.BytesDone = j.bytesDone.Load()
	if path, _ := j.progressFile.Load().(string); path != "" {
		if st, err := os.Stat(path); err == nil {
			p.BytesDone = st.Size()
		}
	}
	p.BytesTotal = j.bytesTotal.Load()
	p.Phase, _ = j.phase.Load().(string)
	p.ElapsedSeconds = int64(time.Since(j.started).Seconds())
	return p
}

// jobTable holds the jobs that are running or recently finished, by key.
type jobTable struct {
	mu   sync.Mutex
	jobs map[string]*mediaJob
}

func newJobTable() *jobTable {
	return &jobTable{jobs: make(map[string]*mediaJob)}
}

// start runs fn as the job for key, unless a job for key is still running —
// then that job is returned and fn is never called, which is what stops a
// second confirm from starting a second upload of the same draft. A finished
// job is replaced: starting again after a failure is how a retry happens.
//
// fn returns the final HTTP status and JSON body. It runs on its own
// goroutine and must not use the starting request's context.
func (t *jobTable) start(key string, fn func(*mediaJob) (int, any)) (*mediaJob, bool) {
	t.mu.Lock()
	t.sweepLocked(time.Now())
	if j, ok := t.jobs[key]; ok && j.running() {
		t.mu.Unlock()
		return j, false
	}
	j := &mediaJob{started: time.Now(), done: make(chan struct{})}
	t.jobs[key] = j
	t.mu.Unlock()

	go func() {
		status := http.StatusInternalServerError
		var body any = errorResponse{Error: "internal error", Details: "the job ended without an answer"}
		defer func() {
			if r := recover(); r != nil {
				log.Printf("job %s panicked: %v", key, r)
			}
			j.finished = time.Now()
			j.status, j.body = status, body
			close(j.done)
		}()
		status, body = fn(j)
	}()
	return j, true
}

// get returns the job for key — running, or finished within the retention
// window — or nil.
func (t *jobTable) get(key string) *mediaJob {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweepLocked(time.Now())
	return t.jobs[key]
}

// forget drops key's entry if it is still j. Used by jobs whose result is
// recorded elsewhere (a download's lives in the messages table), so the next
// request starts fresh instead of being served a stale answer.
func (t *jobTable) forget(key string, j *mediaJob) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.jobs[key] == j {
		delete(t.jobs, key)
	}
}

func (t *jobTable) sweepLocked(now time.Time) {
	for k, j := range t.jobs {
		if !j.running() && now.Sub(j.finished) > finishedJobRetention {
			delete(t.jobs, k)
		}
	}
}

// jobWait reads ?wait=<seconds>: how long the caller is willing to wait before
// getting 202. Missing or malformed means defaultJobWait; it is clamped to
// [0, maxJobWait].
func jobWait(r *http.Request) time.Duration {
	v := r.URL.Query().Get("wait")
	if v == "" {
		return defaultJobWait
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return defaultJobWait
	}
	if d := time.Duration(n) * time.Second; d < maxJobWait {
		return d
	}
	return maxJobWait
}

// answerJob writes the job's final answer if it finishes within the caller's
// wait, and 202 with its progress otherwise.
func answerJob(w http.ResponseWriter, r *http.Request, j *mediaJob, p jobPending) {
	if j.wait(r.Context(), jobWait(r)) {
		j.delivered.Store(true)
		writeJSON(w, j.status, j.body)
		return
	}
	writeJSON(w, http.StatusAccepted, j.pending(p))
}

// watchStall cancels a transfer that stops moving. progress must return a
// number that changes while bytes flow; when it has not changed for idle,
// cancel is called and the returned flag is set, so the caller can say
// "stalled" instead of reporting a bare "context canceled".
//
// stop ends the watch and returns only once the watcher has exited, so after
// stop() returns, cancel can no longer be called — which is what lets a caller
// hand the same context on to work the watch must never interrupt.
func watchStall(cancel context.CancelFunc, progress func() int64, idle time.Duration) (stop func(), fired *atomic.Bool) {
	fired = new(atomic.Bool)
	quit, exited := make(chan struct{}), make(chan struct{})
	var once sync.Once
	tick := idle / 6
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	go func() {
		defer close(exited)
		t := time.NewTicker(tick)
		defer t.Stop()
		last, lastMove := progress(), time.Now()
		for {
			select {
			case <-quit:
				return
			case now := <-t.C:
				if cur := progress(); cur != last {
					last, lastMove = cur, now
					continue
				}
				if now.Sub(lastMove) >= idle {
					fired.Store(true)
					cancel()
					return
				}
			}
		}
	}()
	return func() {
		once.Do(func() { close(quit) })
		<-exited
	}, fired
}
