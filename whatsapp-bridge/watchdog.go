// watchdog.go — notices when the WhatsApp socket has been down too long.
//
// The gap this closes, from a real 24-hour outage on 2026-08-24: WhatsApp began
// rejecting the pinned whatsmeow version with `Client outdated (405)`. Every
// layer behaved correctly and the combination was silent.
//
//   - whatsmeow stopped retrying, because isRetryableConnectError excludes 405.
//     Retrying a version the server refuses is pointless.
//   - No LoggedOut event fired, because 405 is not a logged-out reason. The
//     device row survived, auth_state stayed "paired", and the ErrDeviceDeleted
//     recovery path never applied — correctly, nothing was deleted.
//   - The process stayed healthy: HTTP serving, database open, transcript
//     backfiller ticking every 15 minutes. A process supervisor sees nothing
//     wrong, because nothing about the PROCESS is wrong.
//
// So the bridge sat at connected=false, authenticated=true for a day with the
// log showing only backfiller sweeps. Nothing was watching the one thing that
// mattered, and there was no signal to act on.
//
// Two deliberate non-goals:
//
//  1. It does NOT restart the process. Short disconnects are normal and
//     self-healing — the same incident's log has four of them (13:58, 21:30,
//     03:50, 09:36) that all recovered inside two seconds. A watchdog that
//     restarted on those would be far worse than the failure it prevents. And
//     for a version rejection, restarting is futile forever.
//
//  2. It does NOT escalate aggressively. Reconnect attempts are rate-limited and
//     stretch out over time: roughly ten in the first half hour, then two an
//     hour indefinitely. An earlier review of the supervisor flagged that ~109k
//     auth attempts per week from one device risks an account-level block, which
//     would be far worse than the outage being fixed.
//
// What it does instead is make the failure SELF-DESCRIBING. Each nudge logs its
// own outcome, so the reason lands in the log unprompted. That matters because
// the 405 above was only ever diagnosed by a human manually POSTing
// /api/auth/reconnect and reading the error it produced — the bridge already
// knew, it just never said so.

package main

import (
	"context"
	"log"
	"time"
)

const (
	// How often the loop wakes. Cheap: a mutex read and a clock comparison.
	watchdogTick = 30 * time.Second

	// Nothing is reported below this. Comfortably above the observed
	// self-healing disconnects, which recovered in ~2s, so ordinary churn stays
	// out of the log entirely.
	watchdogGrace = 2 * time.Minute
)

// nextNudgeDelay is how long to wait after the previous reconnect attempt
// before making another, given how many have already been made in this outage.
//
// Split out as a pure function so the escalation is testable without a clock,
// a client, or a network: the schedule is the part worth pinning, and the rest
// of the loop is plumbing around it.
func nextNudgeDelay(nudges int) time.Duration {
	switch {
	case nudges < 6:
		// The first half hour, when a transient failure is most likely to clear
		// on its own — a DNS blip, a TLS handshake timeout, a laptop resuming
		// from sleep. Attempts here are the ones that actually recover things.
		return 5 * time.Minute
	default:
		// Past that, the cause is probably structural (a rejected client
		// version, revoked credentials, no route to WhatsApp at all) and no
		// amount of retrying will fix it. Keep going anyway — recovery is still
		// possible and each attempt refreshes the reason in the log — but at a
		// rate that could run for months without being a problem.
		return 30 * time.Minute
	}
}

// RunDisconnectionWatchdog blocks until ctx ends. main runs it in a goroutine.
func (b *Bridge) RunDisconnectionWatchdog(ctx context.Context) {
	ticker := time.NewTicker(watchdogTick)
	defer ticker.Stop()

	var (
		nudges    int
		lastNudge time.Time
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		down := b.DisconnectedFor()
		if down == 0 {
			// Connected, or never connected in the first place. Reset so the
			// next outage starts from the fast end of the schedule rather than
			// inheriting the previous one's backoff.
			nudges, lastNudge = 0, time.Time{}
			continue
		}
		if down < watchdogGrace {
			continue
		}

		// Only nudge a bridge that HAS a device identity. Without one the right
		// path is pairing (loginLoop), not reconnection, and calling Connect here
		// would fight it for the socket.
		//
		// Keyed on the identity itself, NOT on the `authenticated` flag. That
		// flag is only true once a connection has already succeeded, so gating on
		// it made the watchdog useless in the one case it most needed to act: a
		// bridge that has valid credentials but has never managed to connect at
		// all. On 2026-08-28 that is precisely what happened — startup raced DNS,
		// the first connect failed, and the watchdog then skipped the bridge for
		// the next 50 minutes because a flag that only a successful connection
		// can set had never been set.
		if !b.HasDeviceIdentity() {
			continue
		}

		if !lastNudge.IsZero() && time.Since(lastNudge) < nextNudgeDelay(nudges) {
			continue
		}

		nudges++
		lastNudge = time.Now()
		log.Printf("WATCHDOG: WhatsApp socket has been down for %s (attempt %d to reconnect)",
			down.Round(time.Second), nudges)

		// Reconnect, not Connect: it is idempotent, it re-checks IsConnected
		// first, and it takes the paired branch for a device that still exists.
		if _, err := b.Reconnect(ctx); err != nil {
			// The whole point. whatsmeow logs the underlying refusal (405 client
			// outdated, TLS timeout, DNS failure) as a side effect of this call,
			// which is how a silent outage becomes a diagnosable one.
			log.Printf("WATCHDOG: reconnect attempt %d failed: %v", nudges, err)
			continue
		}
		if connected, _, _, _ := b.Status(); connected {
			log.Printf("WATCHDOG: reconnected after %s and %d attempt(s)",
				down.Round(time.Second), nudges)
		}
	}
}
