// pacing.go — puts a ceiling on outbound traffic.
//
// Until this file existed the send path had no limit of any kind: no gap
// between messages, no hourly or daily cap, and nothing stopping a loop from
// messaging a hundred people who had never written first. A bug, a retry loop,
// or a misread instruction could have emptied the address book at machine
// speed, and the account being risked is a personal +52 number — the same one
// the family and the doctor's office use. A WhatsApp ban is not appealable in
// any practical sense, so the cost of the worst case is total and permanent
// while the cost of being throttled is a retry a few seconds later.
//
// That asymmetry decides every trade-off here: this refuses rather than
// queues. Queuing would smooth a burst into a slow burst and still deliver
// every message; refusing turns the burst into an error the caller has to
// notice. A queue also hides the bug that produced the burst.
//
// The limits are deliberately not tuned to WhatsApp's real (unpublished)
// thresholds, which nobody outside Meta knows. They are tuned to what this
// account actually does: a handful of conversations a day, replies to people
// who wrote first. Anything far above that is much more likely to be a defect
// than a genuine need, and the failure mode of guessing low is an error
// message.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// sendLimits is the whole policy, in one struct, so a future config file or
// endpoint can override it without touching the decision logic.
type sendLimits struct {
	// Minimum spacing between two sends to the SAME chat. Consecutive machine
	// speed messages to one person is the most visible automation tell there
	// is, and it is also what a runaway loop produces first.
	MinGapSameChat time.Duration

	// Rolling-window ceilings across all chats.
	MaxPerHour int
	MaxPerDay  int

	// Chats that have never sent US anything, per day. This is the limit that
	// matters most: volume inside existing conversations reads as ordinary
	// use, while reaching out to strangers is the shape of spam and is what
	// gets reported. Reports, not volume, are what get accounts banned.
	MaxNewChatsPerDay int
}

func defaultSendLimits() sendLimits {
	return sendLimits{
		MinGapSameChat:    5 * time.Second,
		MaxPerHour:        40,
		MaxPerDay:         200,
		MaxNewChatsPerDay: 5,
	}
}

// sendDecision is the answer. RetryAfter is advisory and only meaningful when
// the refusal is a spacing or window problem rather than a policy one — a
// caller that has hit MaxNewChatsPerDay should not retry at all today.
type sendDecision struct {
	Allow      bool
	Reason     string
	RetryAfter time.Duration
}

// evaluateSend is the entire policy as a pure function: no clock, no mutex, no
// database. Everything it needs is an argument, which is what makes the limits
// testable at all — the alternative is a test that sleeps for an hour.
//
// lastToThisChat is the zero Time when this chat has never been written to.
// sentLastHour / sentLastDay / newChatsToday are counts already trimmed to
// their windows by the caller.
func evaluateSend(
	limits sendLimits,
	now time.Time,
	lastToThisChat time.Time,
	sentLastHour, sentLastDay, newChatsToday int,
	isNewChat bool,
) sendDecision {
	if !lastToThisChat.IsZero() {
		if gap := now.Sub(lastToThisChat); gap < limits.MinGapSameChat {
			wait := limits.MinGapSameChat - gap
			return sendDecision{
				Reason: fmt.Sprintf("only %s since the last message to this chat; minimum spacing is %s",
					gap.Round(time.Millisecond), limits.MinGapSameChat),
				RetryAfter: wait,
			}
		}
	}

	// The strictest limit is checked before the volume ones so its reason is
	// the one reported: "you are messaging too many strangers" is actionable,
	// "you hit the daily cap" is not, when both are true.
	if isNewChat && newChatsToday >= limits.MaxNewChatsPerDay {
		return sendDecision{
			Reason: fmt.Sprintf("already opened %d new conversations today (limit %d); this chat has never written first",
				newChatsToday, limits.MaxNewChatsPerDay),
		}
	}

	if sentLastHour >= limits.MaxPerHour {
		return sendDecision{
			Reason:     fmt.Sprintf("%d messages sent in the last hour (limit %d)", sentLastHour, limits.MaxPerHour),
			RetryAfter: time.Hour,
		}
	}
	if sentLastDay >= limits.MaxPerDay {
		return sendDecision{
			Reason:     fmt.Sprintf("%d messages sent in the last 24h (limit %d)", sentLastDay, limits.MaxPerDay),
			RetryAfter: 24 * time.Hour,
		}
	}

	return sendDecision{Allow: true}
}

// sendGovernor holds the counts evaluateSend needs and serializes access.
//
// State is in memory on purpose. A restart resets the windows, which is the
// wrong direction for safety — but the alternative, persisting to the same
// database the send path already writes, buys little: a process that restarts
// often enough to matter has a bigger problem, and the per-chat spacing (the
// limit a runaway loop hits first) is re-established after one send.
type sendGovernor struct {
	mu         sync.Mutex
	limits     sendLimits
	lastByChat map[string]time.Time
	sent       []time.Time // every send, trimmed to 24h
	newChats   []time.Time // sends that opened a conversation, trimmed to 24h
}

func newSendGovernor(limits sendLimits) *sendGovernor {
	return &sendGovernor{
		limits:     limits,
		lastByChat: make(map[string]time.Time),
	}
}

// Authorize decides, and on approval immediately records the send.
//
// Recording before the network call rather than after is deliberate. If the
// send then fails, a slot is consumed that produced no message — but the
// alternative leaks: two concurrent requests would both read a count of zero,
// both pass, and both send. Spending a slot on a failure is the cheap error.
func (g *sendGovernor) Authorize(chatJID string, isNewChat bool, now time.Time) sendDecision {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.trim(now)

	d := evaluateSend(g.limits, now, g.lastByChat[chatJID],
		len(withinWindow(g.sent, now, time.Hour)), len(g.sent), len(g.newChats), isNewChat)
	if !d.Allow {
		return d
	}

	g.lastByChat[chatJID] = now
	g.sent = append(g.sent, now)
	if isNewChat {
		g.newChats = append(g.newChats, now)
	}
	return d
}

// Snapshot reports current consumption for /api/status, so the ceiling is
// visible before it is hit rather than only in the error that announces it.
func (g *sendGovernor) Snapshot(now time.Time) map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.trim(now)
	return map[string]any{
		"sent_last_hour":        len(withinWindow(g.sent, now, time.Hour)),
		"max_per_hour":          g.limits.MaxPerHour,
		"sent_last_day":         len(g.sent),
		"max_per_day":           g.limits.MaxPerDay,
		"new_chats_today":       len(g.newChats),
		"max_new_chats_per_day": g.limits.MaxNewChatsPerDay,
		"min_gap_same_chat_ms":  g.limits.MinGapSameChat.Milliseconds(),
	}
}

// trim drops everything outside the widest window. Called under the lock by
// every entry point, so the slices cannot grow without bound on a long-running
// process — this loop has to be safe to leave running for months.
func (g *sendGovernor) trim(now time.Time) {
	g.sent = withinWindow(g.sent, now, 24*time.Hour)
	g.newChats = withinWindow(g.newChats, now, 24*time.Hour)

	// Per-chat timestamps are only ever compared against MinGapSameChat, so
	// anything older than that window is dead weight. Without this the map
	// grows by one entry per chat ever written to and never shrinks.
	for jid, t := range g.lastByChat {
		if now.Sub(t) > g.limits.MinGapSameChat {
			delete(g.lastByChat, jid)
		}
	}
}

// withinWindow returns the suffix of ts newer than now-window. ts is kept in
// ascending order by construction (appends happen in clock order), so this is
// a scan from the front rather than a filter.
func withinWindow(ts []time.Time, now time.Time, window time.Duration) []time.Time {
	cutoff := now.Add(-window)
	i := 0
	for i < len(ts) && !ts[i].After(cutoff) {
		i++
	}
	return ts[i:]
}

// ---------------------------------------------------------------------------
// Wiring into the send path.

// sendGov returns the governor, building it on first use.
//
// This is a lazy initializer rather than a plain field read because a nil
// governor would mean "no limits at all" — exactly the state this file exists
// to prevent, and an invisible one, because sends would simply keep working.
// Three tests already build Server as a bare struct literal, so the zero value
// has to be safe or a fourth will eventually ship an unlimited bridge without
// anyone noticing.
func (s *Server) sendGov() *sendGovernor {
	s.govOnce.Do(func() {
		if s.governor == nil {
			s.governor = newSendGovernor(defaultSendLimits())
		}
	})
	return s.governor
}

// chatHasNoInbound reports whether this chat has never written to us, which is
// what MaxNewChatsPerDay is counting.
//
// Aliases are the subtlety. The same person exists as both a phone JID and a
// @lid, and their inbound messages may be filed under either form — this
// deployment has 1,978 such pairs. Matching only the literal recipient JID
// would classify a years-old conversation as cold outreach the first time it is
// addressed by its other form, spending the day's new-contact budget on people
// who are not new at all, and eventually refusing to message family.
func (s *Server) chatHasNoInbound(ctx context.Context, chatJID string) (bool, error) {
	jids, err := resolveAliases(ctx, s.db, chatJID)
	if err != nil {
		return false, fmt.Errorf("resolving aliases for %s: %w", chatJID, err)
	}
	if len(jids) == 0 {
		// resolveAliases puts the JID itself first, so an empty result means the
		// lookup found nothing at all. Treat that as new: the strict direction.
		return true, nil
	}

	var found int
	err = s.db.QueryRowContext(ctx,
		`SELECT 1 FROM messages
		  WHERE chat_jid IN (`+inClausePlaceholders(len(jids))+`)
		    AND is_from_me = 0
		  LIMIT 1`, jidsToArgs(jids)...).Scan(&found)
	if err == sql.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("checking inbound history for %s: %w", chatJID, err)
	}
	return false, nil
}
