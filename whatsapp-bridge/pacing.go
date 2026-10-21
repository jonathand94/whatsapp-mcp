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
// sending — but only up to MaxWaitSameChat. Past that the queue is a burst,
// and the burst is refused like any other.
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

	// How long a send may wait for its turn in one chat before it is refused
	// instead. Three sends queued behind one another at 5 s each; a fourth is
	// a burst.
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
	// SendAt is when an allowed send may go out: now, or later when earlier
	// sends to the same chat hold the turn (MinGapSameChat apart, at most
	// MaxWaitSameChat ahead).
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
	// nextTurn: the earliest moment the next send to a chat may be scheduled.
	nextTurn map[string]time.Time
	// lastSent: when a send to a chat actually went out (see ClaimSend).
	lastSent map[string]time.Time
	sent     []time.Time // every authorized send, trimmed to 24h
	// newChats: chats that opened a conversation, by when they first did,
	// trimmed to 24h. A map so a chat counts once however often it is written.
	newChats map[string]time.Time
}

func newSendGovernor(limits sendLimits) *sendGovernor {
	return &sendGovernor{
		limits:   limits,
		nextTurn: make(map[string]time.Time),
		lastSent: make(map[string]time.Time),
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
		if d.Limit == refusedNewChat {
			// Say when it frees up: the window rolls, so "today" is not a
			// calendar day, and a caller told only "limit reached" retried
			// for hours (2026-10-21) not knowing when.
			frees := g.oldestNewChat().Add(24 * time.Hour)
			d.RetryAfter = frees.Sub(now)
			d.Reason += fmt.Sprintf("; the next new conversation is allowed from %s", frees.Local().Format("2006-01-02 15:04 MST"))
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

// ClaimSend waits until a send to chat keeps MinGapSameChat from the previous
// one that actually went out, then records this one as going out now.
//
// Authorize already spaces the turns it hands out, which is all a text send
// needs. A media send goes out when its upload finishes, which can be long
// after its turn and at nearly the same moment as another upload to the same
// chat; this is what keeps those sends apart.
func (g *sendGovernor) ClaimSend(ctx context.Context, chat string) error {
	for {
		g.mu.Lock()
		now := time.Now()
		next := g.lastSent[chat].Add(g.limits.MinGapSameChat)
		if !now.Before(next) {
			g.lastSent[chat] = now
			g.mu.Unlock()
			return nil
		}
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(next.Sub(now)):
		}
	}
}

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

	// Turns and send times only matter while they are inside the spacing
	// window. Without this the maps grow by one entry per chat ever written
	// to and never shrink.
	for chat, t := range g.nextTurn {
		if !t.After(now) {
			delete(g.nextTurn, chat)
		}
	}
	for chat, t := range g.lastSent {
		if now.Sub(t) > g.limits.MinGapSameChat {
			delete(g.lastSent, chat)
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
