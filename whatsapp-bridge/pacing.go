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
// That asymmetry decides every trade-off here: the volume and new-conversation
// limits refuse rather than queue. Queuing would smooth a burst into a slow
// burst and still deliver every message; refusing turns the burst into an
// error the caller has to notice. A queue also hides the bug that produced the
// burst.
//
// The one exception is the spacing between sends to the same chat, and it is
// bounded. An assistant asked to send five files to one person confirms the
// five drafts at once; refusing four of them for arriving 2 s apart (as the
// 2026-10-21 test run saw) is not safety, it is a chore. So a send that only
// lacks spacing waits its turn — the gap is still enforced, at the moment of
// sending (ClaimSend) — but only a few sends may be lined up in one chat at a
// time (MaxWaitSameChat). Past that the queue is a burst, and the burst is
// refused like any other.
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

	// How far ahead sends to one chat may be lined up before the next one is
	// refused instead: four sends 5 s apart; a fifth is a burst. It is also
	// the longest a text waits for its moment (ClaimSend), which keeps a text
	// confirm well inside the 30 s its HTTP caller waits.
	MaxWaitSameChat time.Duration

	// Rolling-window ceilings across all chats.
	MaxPerHour int
	MaxPerDay  int

	// Chats that have never sent US anything, per day. This is the limit that
	// matters most: volume inside existing conversations reads as ordinary
	// use, while reaching out to strangers is the shape of spam and is what
	// gets reported. Reports, not volume, are what get accounts banned.
	//
	// Counted per CHAT, not per message: the second message to someone new is
	// the same conversation as the first. Until 2026-10-21 every message
	// counted, so five files sent to one new contact used up the whole day.
	MaxNewChatsPerDay int
}

func defaultSendLimits() sendLimits {
	return sendLimits{
		MinGapSameChat:    5 * time.Second,
		MaxWaitSameChat:   15 * time.Second,
		MaxPerHour:        40,
		MaxPerDay:         200,
		MaxNewChatsPerDay: 5,
	}
}

// Which limit a refusal came from.
const (
	refusedSpacing = "spacing"
	refusedNewChat = "new_chat"
	refusedHourly  = "hourly"
	refusedDaily   = "daily"
)

// sendDecision is the answer. RetryAfter is advisory: how long until the
// limit that refused would let this send through.
type sendDecision struct {
	Allow      bool
	Reason     string
	RetryAfter time.Duration
	// Limit names the limit that refused (refusedSpacing, ...).
	Limit string
	// SendAt is the turn an allowed send was given in its chat's line: now,
	// or later when earlier sends to the same chat hold the turns before it
	// (MinGapSameChat apart, at most MaxWaitSameChat ahead). It decides what
	// the line can still take; the moment the send actually goes out is
	// booked by ClaimSend once the send is ready.
	SendAt time.Time
}

// evaluateSend is the entire policy as a pure function: no clock, no mutex, no
// database. Everything it needs is an argument, which is what makes the limits
// testable at all — the alternative is a test that sleeps for an hour.
//
// nextTurn is the earliest moment the next send to this chat may go out (the
// zero Time when nothing is queued for it). sentLastHour / sentLastDay /
// newChatsToday are counts already trimmed to their windows by the caller.
// alreadyCounted means this chat already opened a conversation within the
// window, so sending to it again opens nothing new.
func evaluateSend(
	limits sendLimits,
	now time.Time,
	nextTurn time.Time,
	sentLastHour, sentLastDay, newChatsToday int,
	isNewChat, alreadyCounted bool,
) sendDecision {
	sendAt := now
	if nextTurn.After(now) {
		sendAt = nextTurn
	}
	if wait := sendAt.Sub(now); wait > limits.MaxWaitSameChat {
		return sendDecision{
			Limit: refusedSpacing,
			Reason: fmt.Sprintf("there are already messages queued for this chat: sends to one chat go out %s apart, "+
				"and none waits more than %s for its turn", limits.MinGapSameChat, limits.MaxWaitSameChat),
			RetryAfter: wait - limits.MaxWaitSameChat,
		}
	}

	// The strictest limit is checked before the volume ones so its reason is
	// the one reported: "you are messaging too many strangers" is actionable,
	// "you hit the daily cap" is not, when both are true.
	if isNewChat && !alreadyCounted && newChatsToday >= limits.MaxNewChatsPerDay {
		return sendDecision{
			Limit: refusedNewChat,
			Reason: fmt.Sprintf("already opened %d new conversations in the last 24h (limit %d); this chat has never written first",
				newChatsToday, limits.MaxNewChatsPerDay),
		}
	}

	if sentLastHour >= limits.MaxPerHour {
		return sendDecision{
			Limit:      refusedHourly,
			Reason:     fmt.Sprintf("%d messages sent in the last hour (limit %d)", sentLastHour, limits.MaxPerHour),
			RetryAfter: time.Hour,
		}
	}
	if sentLastDay >= limits.MaxPerDay {
		return sendDecision{
			Limit:      refusedDaily,
			Reason:     fmt.Sprintf("%d messages sent in the last 24h (limit %d)", sentLastDay, limits.MaxPerDay),
			RetryAfter: 24 * time.Hour,
		}
	}

	return sendDecision{Allow: true, SendAt: sendAt}
}

// sendGovernor holds the counts evaluateSend needs and serializes access.
//
// Chats are identified by the key classifyRecipient gives them, which is the
// same for a chat's phone JID and its @lid: spacing and the new-conversation
// count must not treat one person as two.
//
// State is in memory on purpose. A restart resets the windows, which is the
// wrong direction for safety — but the alternative, persisting to the same
// database the send path already writes, buys little: a process that restarts
// often enough to matter has a bigger problem, and the per-chat spacing (the
// limit a runaway loop hits first) is re-established after one send.
type sendGovernor struct {
	mu     sync.Mutex
	limits sendLimits
	// nextTurn: the next turn Authorize can hand out in a chat's line.
	nextTurn map[string]time.Time
	// nextSlot: the earliest moment ClaimSend can book for the next send that
	// actually goes out to a chat.
	nextSlot map[string]time.Time
	sent     []time.Time // every authorized send, trimmed to 24h
	// newChats: chats that opened a conversation, by when they first did,
	// trimmed to 24h. A map so a chat counts once however often it is written.
	newChats map[string]time.Time
}

func newSendGovernor(limits sendLimits) *sendGovernor {
	return &sendGovernor{
		limits:   limits,
		nextTurn: make(map[string]time.Time),
		nextSlot: make(map[string]time.Time),
		newChats: make(map[string]time.Time),
	}
}

// Authorize decides, and on approval immediately records the send and
// reserves its turn in the chat.
//
// Recording before the network call rather than after is deliberate. If the
// send then fails, a slot is consumed that produced no message — but the
// alternative leaks: two concurrent requests would both read a count of zero,
// both pass, and both send. Spending a slot on a failure is the cheap error.
func (g *sendGovernor) Authorize(chat string, isNewChat bool, now time.Time) sendDecision {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.trim(now)

	_, counted := g.newChats[chat]
	d := evaluateSend(g.limits, now, g.nextTurn[chat],
		len(withinWindow(g.sent, now, time.Hour)), len(g.sent), len(g.newChats), isNewChat, counted)
	if !d.Allow {
		// Say exactly when it frees up: the windows roll, so "today" is not
		// a calendar day, and a caller told only "limit reached" retried for
		// hours (2026-10-21) not knowing when.
		switch d.Limit {
		case refusedNewChat:
			frees := g.oldestNewChat().Add(24 * time.Hour)
			d.RetryAfter = frees.Sub(now)
			d.Reason += fmt.Sprintf("; the next new conversation is allowed from %s", frees.Local().Format("2006-01-02 15:04 MST"))
		case refusedHourly:
			if hour := withinWindow(g.sent, now, time.Hour); len(hour) > 0 {
				d.RetryAfter = hour[0].Add(time.Hour).Sub(now)
			}
		case refusedDaily:
			if len(g.sent) > 0 {
				d.RetryAfter = g.sent[0].Add(24 * time.Hour).Sub(now)
			}
		}
		return d
	}

	g.nextTurn[chat] = d.SendAt.Add(g.limits.MinGapSameChat)
	g.sent = append(g.sent, now)
	if isNewChat && !counted {
		g.newChats[chat] = now
	}
	return d
}

// ClaimSend books the moment a send to chat goes out, and waits for it.
//
// Moments are booked under the lock, MinGapSameChat apart, in the order sends
// ask for them, and a send asks when it is ready to go: a text when it is
// confirmed, a file when its upload finishes. So sends to one chat go out in
// the order they became ready, each at least MinGapSameChat after the one
// before, and each knows its moment the instant it asks. Until 2026-10-21 a
// text first sat out the turns of uploads that had not sent anything yet,
// then polled against them, and whichever waiter woke first went: a text
// could lose that race to every upload finishing around it.
//
// A send whose moment would be more than maxWait away books nothing and gets
// a *sendQueueFull; a negative maxWait waits as long as ctx allows. A booking
// abandoned because ctx ended is not given back: it leaves one extra gap in
// the chat, which is the cheap error.
func (g *sendGovernor) ClaimSend(ctx context.Context, chat string, maxWait time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	at, err := g.book(chat, time.Now(), maxWait)
	if err != nil {
		return err
	}
	wait := time.Until(at)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// book is ClaimSend's decision, without the waiting.
func (g *sendGovernor) book(chat string, now time.Time, maxWait time.Duration) (time.Time, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	at := now
	if next := g.nextSlot[chat]; next.After(at) {
		at = next
	}
	if wait := at.Sub(now); maxWait >= 0 && wait > maxWait {
		return time.Time{}, &sendQueueFull{Wait: wait, MaxWait: maxWait, Gap: g.limits.MinGapSameChat}
	}
	g.nextSlot[chat] = at.Add(g.limits.MinGapSameChat)
	return at, nil
}

// sendQueueFull is ClaimSend refusing a send whose moment is further away
// than it may wait. Nothing was booked for it.
type sendQueueFull struct {
	Wait, MaxWait, Gap time.Duration
}

func (e *sendQueueFull) Error() string {
	return fmt.Sprintf("this chat already has messages going out over the next %s: sends to one chat go out %s apart, "+
		"and a message does not wait more than %s for its turn", e.Wait.Round(time.Second), e.Gap, e.MaxWait)
}

// RetryAfter is how much later the send would fit.
func (e *sendQueueFull) RetryAfter() time.Duration { return e.Wait - e.MaxWait }

func (g *sendGovernor) oldestNewChat() time.Time {
	var oldest time.Time
	for _, t := range g.newChats {
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	return oldest
}

// Snapshot reports current consumption for /api/status, so the ceiling is
// visible before it is hit rather than only in the error that announces it.
func (g *sendGovernor) Snapshot(now time.Time) map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.trim(now)
	snap := map[string]any{
		"sent_last_hour":        len(withinWindow(g.sent, now, time.Hour)),
		"max_per_hour":          g.limits.MaxPerHour,
		"sent_last_day":         len(g.sent),
		"max_per_day":           g.limits.MaxPerDay,
		"new_chats_today":       len(g.newChats),
		"max_new_chats_per_day": g.limits.MaxNewChatsPerDay,
		"min_gap_same_chat_ms":  g.limits.MinGapSameChat.Milliseconds(),
	}
	if len(g.newChats) > 0 {
		// When the oldest counted conversation leaves the 24h window, and
		// with it a new-conversation slot.
		snap["new_chats_next_slot_at"] = g.oldestNewChat().Add(24 * time.Hour).Local().Format(time.RFC3339)
	}
	return snap
}

// trim drops everything outside the widest window. Called under the lock by
// every entry point, so the state cannot grow without bound on a long-running
// process — this loop has to be safe to leave running for months.
func (g *sendGovernor) trim(now time.Time) {
	g.sent = withinWindow(g.sent, now, 24*time.Hour)
	cutoff := now.Add(-24 * time.Hour)
	for chat, t := range g.newChats {
		if !t.After(cutoff) {
			delete(g.newChats, chat)
		}
	}

	// Turns and booked moments only matter until they come. Without this the
	// maps grow by one entry per chat ever written to and never shrink.
	for chat, t := range g.nextTurn {
		if !t.After(now) {
			delete(g.nextTurn, chat)
		}
	}
	for chat, t := range g.nextSlot {
		if !t.After(now) {
			delete(g.nextSlot, chat)
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

// classifyRecipient returns the key the governor tracks a chat under, and
// whether sending to it opens a new conversation — that is, whether the chat
// has never written to us, which is what MaxNewChatsPerDay is counting.
//
// Aliases are the subtlety. The same person exists as both a phone JID and a
// @lid, and their inbound messages may be filed under either form — this
// deployment has 1,978 such pairs. Matching only the literal recipient JID
// would classify a years-old conversation as cold outreach the first time it is
// addressed by its other form, spending the day's new-contact budget on people
// who are not new at all, and eventually refusing to message family. For the
// same reason the key is shared by every alias (the smallest of them, so it
// does not depend on which form the caller used).
//
// The user's own chat is never new. Every message in it is from the user, so
// it has no inbound history by construction, and until 2026-10-21 each send
// to it counted as opening a conversation with a stranger: five files sent to
// oneself used up the day. There is nobody on the other side to report it.
func (s *Server) classifyRecipient(ctx context.Context, chatJID string) (key string, isNew bool, err error) {
	jids, err := resolveAliases(ctx, s.db, chatJID)
	if err != nil {
		return "", false, fmt.Errorf("resolving aliases for %s: %w", chatJID, err)
	}
	if len(jids) == 0 {
		jids = []string{chatJID}
	}
	key = jids[0]
	for _, j := range jids[1:] {
		if j < key {
			key = j
		}
	}

	own := s.ownChatJIDs()
	for _, j := range jids {
		if own[j] {
			return key, false, nil
		}
	}

	var found int
	err = s.db.QueryRowContext(ctx,
		`SELECT 1 FROM messages
		  WHERE chat_jid IN (`+inClausePlaceholders(len(jids))+`)
		    AND is_from_me = 0
		  LIMIT 1`, jidsToArgs(jids)...).Scan(&found)
	if err == sql.ErrNoRows {
		return key, true, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("checking inbound history for %s: %w", chatJID, err)
	}
	return key, false, nil
}

// ownChatJIDs is the user's own chat, in both forms: the phone JID and the
// @lid, without a device part.
func (s *Server) ownChatJIDs() map[string]bool {
	own := map[string]bool{}
	if s.bridge == nil || s.bridge.client == nil || s.bridge.client.Store == nil {
		return own
	}
	if id := s.bridge.client.Store.ID; id != nil && !id.IsEmpty() {
		own[id.ToNonAD().String()] = true
	}
	if lid := s.bridge.client.Store.LID; !lid.IsEmpty() {
		own[lid.ToNonAD().String()] = true
	}
	return own
}
