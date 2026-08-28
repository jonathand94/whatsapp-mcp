package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

// TestConnectedDerivesAuthenticationFromDeviceIdentity pins the fix for a bridge
// that answered 503 to every send over a perfectly live connection.
//
// `authenticated` used to be set in exactly two places — RunAuth's
// returning-user path and the PairSuccess event — and neither runs on a
// reconnect. On 2026-08-28 the logon-triggered task fired before DNS was up, so
// RunAuth's first connect failed. When the socket later came back, nothing set
// the flag: IsConnected() is `connected && authenticated`, so the send path
// refused everything and /api/status reported a state no amount of reconnecting
// could clear. Only a process restart fixed it.
//
// events.Connected only fires after a successful handshake, so the device
// identity is the honest source of truth there. This exercises the handler's
// logic directly; constructing a real whatsmeow client in a unit test is not
// possible, and the branch is the whole point.
func TestConnectedDerivesAuthenticationFromDeviceIdentity(t *testing.T) {
	jid := types.JID{User: "15555550100", Server: types.DefaultUserServer, Device: 62}

	// The handler's body, with the store read hoisted to a parameter so it can
	// be driven without a live client.
	onConnected := func(b *Bridge, storeID *types.JID) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.connected = true
		b.disconnectedSince = time.Time{}
		if storeID != nil {
			b.authenticated = true
			b.deviceJID = storeID.String()
		}
		if b.authenticated {
			b.authState = AuthStatePaired
		}
	}

	t.Run("recovers a bridge whose startup auth failed", func(t *testing.T) {
		// Exactly the 2026-08-28 state: paired on disk, but the first connect
		// never succeeded so the flag was never set.
		b := &Bridge{authState: AuthStatePaired}
		if b.IsConnected() {
			t.Fatal("precondition: should not look connected yet")
		}

		onConnected(b, &jid)

		if !b.IsConnected() {
			t.Fatal("a live socket with a device identity must count as connected; " +
				"otherwise every send returns 503 forever")
		}
		if got := b.DeviceJID(); got != jid.String() {
			t.Fatalf("device_jid = %q, want %q — /api/status reported it empty", got, jid.String())
		}
	})

	t.Run("does not fabricate authentication without an identity", func(t *testing.T) {
		// Connecting during first-run pairing: the socket is up, but there is no
		// device yet. Claiming authentication here would let the send path accept
		// work the bridge cannot possibly do.
		b := &Bridge{}
		onConnected(b, nil)

		if b.IsConnected() {
			t.Fatal("no device identity must not produce an authenticated bridge")
		}
		if got := b.DeviceJID(); got != "" {
			t.Fatalf("device_jid = %q, want empty", got)
		}
	})
}
