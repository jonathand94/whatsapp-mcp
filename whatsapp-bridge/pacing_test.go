package main

import (
	"sync"
	"testing"
	"time"
)

// TestSpacingStopsARunawayLoop covers the failure this was written for: a retry
// loop, or a caller that misreads an instruction, hammering one chat. That is
// both the most visible automation tell to WhatsApp and the fastest way to make
// a real person block and report the number.
func TestSpacingStopsARunawayLoop(t *testing.T) {
	g := newSendGovernor(defaultSendLimits())
	now := time.Now()
	const chat = "5215550647337@s.whatsapp.net"

	if d := g.Authorize(chat, false, now); !d.Allow {
		t.Fatalf("first send refused: %s", d.Reason)
	}

	// A loop retrying immediately gets nothing through, no matter how many
	// times it tries.
	for i := 0; i < 50; i++ {
		d := g.Authorize(chat, false, now.Add(time.Duration(i)*10*time.Millisecond))
		if d.Allow {
			t.Fatalf("attempt %d slipped through inside the minimum gap", i)
		}
		if d.RetryAfter <= 0 {
			t.Fatal("a spacing refusal must say how long to wait")
		}
	}

	// And once the gap has genuinely passed, the same chat works again: this
	// throttles, it does not blacklist.
	if d := g.Authorize(chat, false, now.Add(6*time.Second)); !d.Allow {
		t.Fatalf("refused after the gap elapsed: %s", d.Reason)
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
	g := newSendGovernor(defaultSendLimits())
	now := time.Now()
	const chat = "5215550647337@s.whatsapp.net"

	g.Authorize(chat, false, now) // one real send
	for i := 0; i < 100; i++ {
		g.Authorize(chat, false, now) // all refused: inside the gap
	}

	snap := g.Snapshot(now)
	if got := snap["sent_last_day"].(int); got != 1 {
		t.Fatalf("100 refused attempts consumed budget: sent_last_day = %d, want 1", got)
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
// trimming, lastByChat gains an entry per chat ever written to and the send
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
	if n := len(g.lastByChat); n > 2 {
		t.Fatalf("per-chat map holds %d entries; entries past the spacing window are not being dropped", n)
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
