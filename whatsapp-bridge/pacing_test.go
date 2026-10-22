package main

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSpacingQueuesThenStopsARunawayLoop covers the failure this was written
// for: a retry loop, or a caller that misreads an instruction, hammering one
// chat. That is both the most visible automation tell to WhatsApp and the
// fastest way to make a real person block and report the number.
//
// A few sends to one chat at once are queued, MinGapSameChat apart — that is
// an assistant sending five files to one person. Past MaxWaitSameChat it is a
// burst, and it is refused.
func TestSpacingQueuesThenStopsARunawayLoop(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	now := time.Now()
	const chat = "5215550647337@s.whatsapp.net"

	queued := int(lim.MaxWaitSameChat/lim.MinGapSameChat) + 1 // 0 s, 5 s, 10 s, 15 s
	for i := 0; i < queued; i++ {
		d := g.Authorize(chat, false, now)
		if !d.Allow {
			t.Fatalf("send %d refused although it fits the queue: %s", i, d.Reason)
		}
		if want := now.Add(time.Duration(i) * lim.MinGapSameChat); !d.SendAt.Equal(want) {
			t.Fatalf("send %d scheduled at +%s, want +%s", i, d.SendAt.Sub(now), want.Sub(now))
		}
	}

	// A loop retrying immediately gets nothing more through, no matter how
	// many times it tries.
	for i := 0; i < 50; i++ {
		d := g.Authorize(chat, false, now.Add(time.Duration(i)*10*time.Millisecond))
		if d.Allow {
			t.Fatalf("attempt %d slipped through a full queue", i)
		}
		if d.RetryAfter <= 0 || d.Limit != refusedSpacing {
			t.Fatalf("a spacing refusal must say how long to wait: %+v", d)
		}
	}

	// And once the queue has drained, the same chat works again at once: this
	// throttles, it does not blacklist.
	later := now.Add(time.Duration(queued) * lim.MinGapSameChat)
	if d := g.Authorize(chat, false, later); !d.Allow || !d.SendAt.Equal(later) {
		t.Fatalf("refused or delayed after the queue drained: %+v", d)
	}
}

// TestNewChatLimitIsTheOneThatMatters pins the limit protecting against the
// scenario that actually gets accounts banned — reaching strangers, who report
// it — as opposed to volume inside conversations already underway.
func TestNewChatLimitIsTheOneThatMatters(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	base := time.Now()

	// Messaging people who have never written first stops at the limit.
	allowed := 0
	for i := 0; i < 40; i++ {
		// Distinct chats and well past the spacing gap, so the ONLY limit that
		// can bite here is the new-conversation one.
		at := base.Add(time.Duration(i) * time.Minute)
		if g.Authorize(newJID(i), true, at).Allow {
			allowed++
		}
	}
	if allowed != lim.MaxNewChatsPerDay {
		t.Fatalf("opened %d new conversations, limit is %d", allowed, lim.MaxNewChatsPerDay)
	}

	// Meanwhile existing conversations are untouched: being at the stranger
	// limit must not break replying to the people who did write first.
	at := base.Add(41 * time.Minute)
	if d := g.Authorize("5215550227630@s.whatsapp.net", false, at); !d.Allow {
		t.Fatalf("an established chat was refused because of the new-chat limit: %s", d.Reason)
	}
}

// TestRefusalCostsNothing is the one that would silently ruin the limits: if a
// blocked attempt still consumed budget, a caller retrying in a loop would burn
// the entire daily allowance without delivering a single message, and the
// account would go quiet for a day for no reason.
func TestRefusalCostsNothing(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	now := time.Now()
	const chat = "5215550647337@s.whatsapp.net"

	queued := int(lim.MaxWaitSameChat/lim.MinGapSameChat) + 1
	for i := 0; i < queued+100; i++ {
		g.Authorize(chat, false, now) // the first few queue, the rest are refused
	}

	snap := g.Snapshot(now)
	if got := snap["sent_last_day"].(int); got != queued {
		t.Fatalf("refused attempts consumed budget: sent_last_day = %d, want %d", got, queued)
	}
}

// TestNewChatCountsOncePerChat pins the 2026-10-21 bug: five files sent to one
// new contact counted as five new conversations and used up the day.
func TestNewChatCountsOncePerChat(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	base := time.Now()
	const stranger = "5215550000001@s.whatsapp.net"

	for i := 0; i < 10; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		if d := g.Authorize(stranger, true, at); !d.Allow {
			t.Fatalf("message %d to the same new chat refused: %s", i, d.Reason)
		}
	}
	if got := g.Snapshot(base.Add(10 * time.Minute))["new_chats_today"].(int); got != 1 {
		t.Fatalf("one new chat counted as %d", got)
	}
}

// TestNewChatRefusalSaysWhenItFrees: the window rolls, so "today" is not a
// calendar day, and a caller told only "limit reached" cannot know when to
// come back.
func TestNewChatRefusalSaysWhenItFrees(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	base := time.Date(2026, 10, 21, 9, 0, 0, 0, time.Local)
	for i := 0; i < lim.MaxNewChatsPerDay; i++ {
		g.Authorize(newJID(i), true, base.Add(time.Duration(i)*time.Hour))
	}
	now := base.Add(10 * time.Hour)
	d := g.Authorize(newJID(99), true, now)
	if d.Allow || d.Limit != refusedNewChat {
		t.Fatalf("want a new-chat refusal, got %+v", d)
	}
	frees := base.Add(24 * time.Hour)
	if d.RetryAfter != frees.Sub(now) || !strings.Contains(d.Reason, frees.Format("2006-01-02 15:04")) {
		t.Fatalf("refusal should name %s: %q (retry after %s)", frees, d.Reason, d.RetryAfter)
	}
	if got := g.Snapshot(now)["new_chats_next_slot_at"]; got != frees.Format(time.RFC3339) {
		t.Fatalf("snapshot next slot = %v, want %s", got, frees.Format(time.RFC3339))
	}
}

// TestClaimSendKeepsTheGapForLateSends: a media send goes out when its upload
// finishes, possibly at the same moment as another upload to the same chat.
func TestClaimSendKeepsTheGapForLateSends(t *testing.T) {
	lim := defaultSendLimits()
	lim.MinGapSameChat = 200 * time.Millisecond
	g := newSendGovernor(lim)
	const chat = "5215550647337@s.whatsapp.net"

	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var sent []time.Time
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.ClaimSend(t.Context(), chat, -1); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			sent = append(sent, time.Now())
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(sent, func(i, j int) bool { return sent[i].Before(sent[j]) })
	for i := 1; i < len(sent); i++ {
		if gap := sent[i].Sub(sent[i-1]); gap < lim.MinGapSameChat-10*time.Millisecond {
			t.Fatalf("sends %d and %d went out %s apart, gap is %s", i-1, i, gap, lim.MinGapSameChat)
		}
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("ClaimSend waited far longer than the gaps require")
	}
}

// TestSendsGoOutInTheOrderTheyAsk: moments are booked under the lock, so the
// order is the order of asking, and a send that would wait too long books
// nothing — it does not push the sends behind it further out.
func TestSendsGoOutInTheOrderTheyAsk(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	const chat = "5215550647337@s.whatsapp.net"
	now := time.Now()

	for i := 0; i < 3; i++ {
		at, err := g.book(chat, now, -1)
		if want := now.Add(time.Duration(i) * lim.MinGapSameChat); err != nil || !at.Equal(want) {
			t.Fatalf("booking %d: at +%s (%v), want +%s", i, at.Sub(now), err, want.Sub(now))
		}
	}
	// The next moment is 15 s out: a send allowed to wait 10 s is refused,
	// told it is 5 s short, and books nothing.
	_, err := g.book(chat, now, 10*time.Second)
	var full *sendQueueFull
	if !errors.As(err, &full) || full.RetryAfter() != 5*time.Second {
		t.Fatalf("want a refusal 5 s short, got %v", err)
	}
	if at, _ := g.book(chat, now, -1); !at.Equal(now.Add(3 * lim.MinGapSameChat)) {
		t.Fatalf("the refused send still took a moment: next booking at +%s", at.Sub(now))
	}
}

// TestATextIsNotHeldBehindUploadsStillRunning is the 2026-10-21 review case:
// files confirmed first hold the first turns in the chat's line, but send
// nothing until their uploads finish. A text confirmed after them goes out at
// once instead of sitting out turns nobody is using, and the files still go
// out spaced when their uploads finish together.
func TestATextIsNotHeldBehindUploadsStillRunning(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	const chat = "5215550647337@s.whatsapp.net"
	now := time.Now()

	for i := 0; i < 4; i++ { // three files, then the text
		if d := g.Authorize(chat, false, now); !d.Allow {
			t.Fatalf("confirm %d refused: %s", i, d.Reason)
		}
	}
	if at, err := g.book(chat, now, lim.MaxWaitSameChat); err != nil || !at.Equal(now) {
		t.Fatalf("text booked at +%s (%v), want now", at.Sub(now), err)
	}
	for i := 1; i <= 3; i++ {
		at, err := g.book(chat, now.Add(time.Second), -1)
		if want := now.Add(time.Duration(i) * lim.MinGapSameChat); err != nil || !at.Equal(want) {
			t.Fatalf("file %d at +%s (%v), want +%s", i, at.Sub(now), err, want.Sub(now))
		}
	}
}

// TestVolumeRefusalsSayWhenTheyFree: the hourly and daily caps answer with
// the moment the oldest send leaves the window, not a flat hour or day.
func TestVolumeRefusalsSayWhenTheyFree(t *testing.T) {
	lim := defaultSendLimits()
	base := time.Now()

	g := newSendGovernor(lim)
	for i := 0; i < lim.MaxPerHour; i++ {
		g.Authorize(newJID(i), false, base.Add(time.Duration(i)*time.Minute))
	}
	d := g.Authorize(newJID(999), false, base.Add(50*time.Minute))
	if d.Allow || d.Limit != refusedHourly || d.RetryAfter != 10*time.Minute {
		t.Fatalf("want the hourly cap freeing in 10m, got %+v", d)
	}

	g = newSendGovernor(lim)
	for i := 0; i < lim.MaxPerDay; i++ {
		if d := g.Authorize(newJID(i), false, base.Add(time.Duration(i)*2*time.Minute)); !d.Allow {
			t.Fatalf("send %d refused under the daily cap: %s", i, d.Reason)
		}
	}
	d = g.Authorize(newJID(999), false, base.Add(7*time.Hour))
	if d.Allow || d.Limit != refusedDaily || d.RetryAfter != 17*time.Hour {
		t.Fatalf("want the daily cap freeing in 17h, got %+v", d)
	}
}

// TestWindowsRoll covers the other direction: the caps are rolling windows, not
// a permanent ceiling. A day of normal use must not leave the account unable to
// reply tomorrow.
func TestWindowsRoll(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	base := time.Now()

	// Spend the hourly budget across distinct established chats.
	for i := 0; i < lim.MaxPerHour; i++ {
		if d := g.Authorize(newJID(i), false, base.Add(time.Duration(i)*time.Second)); !d.Allow {
			t.Fatalf("send %d refused while under the hourly cap: %s", i, d.Reason)
		}
	}
	if d := g.Authorize(newJID(999), false, base.Add(time.Minute)); d.Allow {
		t.Fatal("the hourly cap did not apply")
	}

	// An hour later the window has rolled and sending resumes.
	if d := g.Authorize(newJID(999), false, base.Add(61*time.Minute)); !d.Allow {
		t.Fatalf("still refused after the hour rolled: %s", d.Reason)
	}
}

// TestConcurrentSendsCannotOverspend covers the race that made recording happen
// before the network call rather than after: two requests arriving together
// must not both read the same count and both pass.
func TestConcurrentSendsCannotOverspend(t *testing.T) {
	lim := defaultSendLimits()
	lim.MinGapSameChat = 0 // isolate the volume cap from the spacing one
	g := newSendGovernor(lim)
	now := time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < lim.MaxPerHour*3; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if g.Authorize(newJID(n), false, now).Allow {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if allowed > lim.MaxPerHour {
		t.Fatalf("%d sends authorized concurrently, cap is %d", allowed, lim.MaxPerHour)
	}
}

// TestStateStaysBounded matters because this runs for months at a time. Without
// trimming, the per-chat maps gain an entry per chat ever written to and the send
// slices grow forever — a slow leak in the one process that must not be
// restarted casually.
func TestStateStaysBounded(t *testing.T) {
	lim := defaultSendLimits()
	g := newSendGovernor(lim)
	base := time.Now()

	// Simulate a year of steady use: one send a minute to a rotating set of
	// chats. Every entry point trims, so the state must reflect 24h, not a year.
	for i := 0; i < 60*24*365; i += 37 {
		g.Authorize(newJID(i%500), false, base.Add(time.Duration(i)*time.Minute))
	}
	end := base.Add(time.Duration(60*24*365) * time.Minute)
	g.Snapshot(end)

	if n := len(g.sent); n > 24*60 {
		t.Fatalf("send history holds %d entries after a year; the 24h window is not trimming", n)
	}
	if n := len(g.nextTurn) + len(g.nextSlot); n > 2 {
		t.Fatalf("per-chat maps hold %d entries; entries past the spacing window are not being dropped", n)
	}

	// The assertion that actually matters, and the one a bounded-memory check
	// alone silently misses. If trim stops pruning g.sent, the daily counter
	// never falls: from send 200 onward EVERY send is refused, forever, and the
	// account goes mute. Memory stays flat in that state — growth stops at the
	// cap — so the leak check above passes while the bridge is dead. Liveness
	// is the property; bounded memory is a side effect of it.
	if d := g.Authorize("someone@s.whatsapp.net", false, end); !d.Allow {
		t.Fatalf("after a year of ordinary use the governor refuses everything: %s", d.Reason)
	}
}

func newJID(n int) string {
	return string(rune('a'+n%26)) + string(rune('a'+(n/26)%26)) + string(rune('a'+(n/676)%26)) + "@s.whatsapp.net"
}
