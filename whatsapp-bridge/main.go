// Package main is the whatsapp-mcp Go bridge.
//
// Responsibilities:
//   - Wrap go.mau.fi/whatsmeow for the WhatsApp Web multidevice protocol.
//   - Own an SQLCipher-encrypted SQLite database of contacts, chats, messages, calls.
//   - Expose a localhost-only REST API consumed by the Python MCP server.
//   - Handle QR and pairing-code authentication.
//   - Log every tool call to audit.log.
//
// Security:
//   - Binds to 127.0.0.1 only. See config.Validate().
//   - DB encrypted at rest with SQLCipher. Key from macOS Keychain.
//   - No outbound network calls except to WhatsApp itself (and optional OpenAI Whisper if opted in).
//   - See SECURITY.md for the full threat model.
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	os.Exit(run())
}

// run is main's body, split out so the exit path is a `return` rather than
// os.Exit. That distinction is load-bearing: os.Exit skips every deferred
// db.Close / transcriber.Close / bridge.Disconnect below, and the fatal-shutdown
// path (see Bridge.Fatal) needs them to actually run. main() is now the only
// place that calls os.Exit, with a code the supervisor records.
func run() int {
	importBaileys := flag.String("import-baileys", "",
		"Path to a baileys_store.json from the prior Baileys sync. If set, the bridge imports that store into SQLite and exits without connecting to WhatsApp.")
	exportVault := flag.String("export-to-vault", "",
		"Path to an output folder. If set, the bridge regenerates one markdown file per chat from SQLite into that folder and exits without connecting to WhatsApp.")
	exportIncludeGroups := flag.Bool("export-include-groups", false,
		"When set with --export-to-vault, also exports group chats (default exports direct chats only, matching Baileys behavior).")
	exportMinMessages := flag.Int("export-min-messages", 0,
		"With --export-to-vault: only export chats with at least this many messages (default 0 = export everything; recommended 5 to skip drive-by contacts). Counted across a person's merged JID forms.")
	reconcileVault := flag.String("reconcile-vault", "",
		"Path to an exported vault folder. If set, the bridge compares the SQLite DB against the chat files (honoring --export-include-groups / --export-min-messages), prints MISSING/MISFILED/DRIFT/DUPLICATE/ORPHAN findings, and exits non-zero when any exist. Exits without connecting to WhatsApp.")
	pairPhone := flag.String("pair-phone", "",
		"Pair by typed code instead of QR scanning: your own phone number in international format (e.g. +15551234567). The 8-char code prints here; on the phone: WhatsApp > Settings > Linked Devices > Link a Device > 'Link with phone number instead'. Works on Android and iOS.")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Println("whatsapp-mcp bridge starting")

	// SETUP.md tells users to `cp .env.example .env`; actually honor it.
	// Process env always wins over the file.
	loadDotEnv()

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	log.Printf("config loaded: bind=%s:%d db=%s crm=%s whisper=%s",
		cfg.BridgeHost, cfg.BridgePort, cfg.DBPath, truncateForLog(cfg.VaultCRMPath), cfg.WhisperBackend)

	// One-time visibility for the v0.2.0 default flip: users who relied on
	// the old local-cpp default would otherwise lose transcription silently.
	if cfg.WhisperBackend == "off" {
		log.Println("voice-note transcription is OFF (WHATSAPP_WHISPER_BACKEND=off, default since v0.2.0); set local-cpp or openai-api to enable — media keys are still stored, so recent notes backfill once enabled")
	}

	var dbKey string
	if cfg.EncryptDB {
		if cfg.DBKey != "" {
			// Explicit override (headless/CI/custom secret manager). This was
			// documented for a long time but never actually implemented.
			if len(cfg.DBKey) != 64 {
				log.Fatalf("WHATSAPP_DB_KEY must be 64 hex chars (32 bytes); got %d chars", len(cfg.DBKey))
			}
			if _, err := hex.DecodeString(cfg.DBKey); err != nil {
				log.Fatalf("WHATSAPP_DB_KEY is not valid hex: %v", err)
			}
			dbKey = cfg.DBKey
			log.Println("DB key: explicit WHATSAPP_DB_KEY override")
		} else {
			dbKey, err = GetOrCreateDBKey(cfg.KeychainService, cfg.KeychainAccount, cfg.DBPath)
			if err != nil {
				log.Fatalf("DB key: %v", err)
			}
			log.Println("DB key resolved from platform secret store")
		}
	}

	db, err := OpenDB(cfg, dbKey)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()
	log.Printf("DB opened: %s (encrypted=%t)", cfg.DBPath, cfg.EncryptDB)

	// Import-only path: no whatsmeow connection, just read the Baileys store into SQLite.
	if *importBaileys != "" {
		if err := RunBaileysImport(cfg, db, *importBaileys); err != nil {
			log.Fatalf("import failed: %v", err)
		}
		log.Println("baileys import done")
		return 0
	}

	// Export-only path: regenerate markdown vault files from SQLite and exit.
	// ExportVault fails (→ non-zero exit) when any enumerated chat produced
	// neither a file nor a legitimate skip; rc=0 means the export is complete.
	if *exportVault != "" {
		if err := ExportVault(db, *exportVault, *exportIncludeGroups, *exportMinMessages); err != nil {
			log.Fatalf("export failed: %v", err)
		}
		log.Println("export done")
		return 0
	}

	// Reconcile-only path: audit an exported vault against the DB and exit.
	if *reconcileVault != "" {
		findings, err := ReconcileVault(db, *reconcileVault, *exportIncludeGroups, *exportMinMessages)
		if err != nil {
			log.Fatalf("reconcile failed: %v", err)
		}
		if len(findings) > 0 {
			log.Fatalf("reconcile: %d finding(s) — see report above", len(findings))
		}
		log.Println("reconcile clean: vault matches DB")
		return 0
	}

	// CRM name-enrichment runs asynchronously after startup so it does not block
	// HTTP server bring-up or the whatsmeow connection. Vault folders are often
	// in iCloud and demand-paging slows file reads; we do not want that latency
	// on the main startup path. Safe to re-run; idempotent.
	if cfg.VaultCRMPath != "" {
		go func() {
			updated, err := EnrichContactsFromVault(db, cfg.VaultCRMPath)
			if err != nil {
				log.Printf("crm enrich failed (continuing): %v", err)
			} else {
				log.Printf("crm enrich: %d contacts updated with vault names", updated)
			}
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the voice-note transcription worker pool. Always created; only
	// actually does work when voice messages arrive and the configured
	// whisper binary + model are present.
	transcriber := NewTranscriber(cfg, db)
	defer transcriber.Close()

	bridge, err := NewBridge(ctx, cfg, db, dbKey, transcriber)
	if err != nil {
		// Printf + return, not Fatalf: db and transcriber are already open above
		// and their defers need to run.
		log.Printf("bridge init: %v", err)
		return 1
	}
	defer bridge.Disconnect()
	if *pairPhone != "" {
		bridge.SetPairPhoneOnStart(*pairPhone)
	}

	// Self-healing voice-note transcript backfill. Periodically re-enqueues
	// any voice/audio rows that have re-download keys but NULL transcripts.
	// Recovers from any transient failure mode (validation, queue full,
	// transient whisper-cli, ffmpeg crash, network blip) automatically.
	backfiller := NewTranscriptBackfiller(db, bridge.Client(), transcriber, cfg.BackfillIntervalSeconds, cfg.BackfillWindowDays)
	go backfiller.Run(ctx)
	log.Printf("transcript backfiller started: interval=%ds window=%dd", cfg.BackfillIntervalSeconds, cfg.BackfillWindowDays)

	// Disconnection watchdog. Reports — and slowly retries — a WhatsApp socket
	// that has been down long enough to not be ordinary churn. It deliberately
	// never restarts the process: short disconnects heal themselves in seconds,
	// and the failure this exists for (a client version WhatsApp refuses) is one
	// no restart can fix. See watchdog.go.
	go bridge.RunDisconnectionWatchdog(ctx)
	log.Printf("disconnection watchdog started: reports after %s, then retries on a widening schedule", watchdogGrace)

	// JID-alias backfill. Asks whatsmeow's local LID store for the alt form of
	// every contact and records it in jid_aliases. Also repairs the legacy
	// "LID stored as phone" rows. Runs once at startup, async so it can't
	// block HTTP serving. Idempotent; cheap (single round-trip per contact
	// against an in-memory store). See aliases.go.
	go func() {
		// Small delay so whatsmeow has a chance to populate its LID store
		// from the initial connection before we ask it to enumerate.
		select {
		case <-time.After(15 * time.Second):
		case <-ctx.Done():
			return
		}
		written, repaired, err := BackfillJIDAliases(ctx, db, bridge.Client())
		if err != nil {
			log.Printf("jid alias backfill failed (non-fatal): %v", err)
		} else {
			log.Printf("jid alias backfill: %d new edges written, %d phone columns repaired", written, repaired)
		}

		// Group metadata sync: real subjects + participant counts into chats.
		// Heals rows the pre-MYC-3555 live path named after a message sender
		// and gives the vault export a real participants_count to emit.
		//
		// Non-fatal like its neighbors below: each of these three syncs covers
		// a disjoint slice of the chats table (aliases, groups, address-book
		// direct chats) and one's failure says nothing about whether the
		// others can still make progress, so no failure here returns out of
		// the goroutine and skips the rest.
		groups, err := SyncGroupMetadata(ctx, db, bridge.Client())
		if err != nil {
			log.Printf("group metadata sync failed (non-fatal): %v", err)
		} else {
			log.Printf("group metadata sync: %d groups updated", groups)
		}

		// Address-book names, after the alias backfill and deliberately so:
		// naming a chat walks jid_aliases to reach the row the user actually
		// sees, so every edge just written is one more chat this can name.
		// Reversing the order would silently name fewer of them.
		// See contacts_sync.go.
		named, renamed, err := SyncContactNames(ctx, db, bridge.Client())
		if err != nil {
			log.Printf("contact name sync failed (non-fatal): %v", err)
		} else {
			log.Printf("contact name sync: %d address-book entries, %d chat(s) named", named, renamed)
		}
	}()

	server := NewServer(cfg, db, bridge, backfiller)

	// Reconcile sends interrupted by a previous run BEFORE serving. A row left
	// 'confirmed' means the bridge stopped between delivering a message and
	// recording it; nothing else in the codebase ever reads that state, so it
	// would stay invisible and permanent. Startup is the only correct moment —
	// no send can legitimately be in flight yet. See sends_reconcile.go.
	if n, err := ReconcileInFlightSends(ctx, db); err != nil {
		log.Printf("in-flight send reconciliation failed (continuing): %v", err)
	} else if n > 0 {
		log.Printf("in-flight send reconciliation: %d interrupted send(s) marked failed (NOT resent)", n)
	}

	// Free what drafts leave on disk: the previous run's scratch files now,
	// before serving (after that a new draft's partial file looks the same),
	// and abandoned drafts now and hourly. See outbound_sweep.go.
	server.removeOrphans()
	go server.runOutboundSweeper(ctx)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		// Fatal is the bridge asking for a restart it cannot perform itself —
		// currently a WhatsApp-side device deletion, which poisons the whatsmeow
		// client permanently. It takes the SAME path as a signal on purpose, so
		// the deferred db.Close/transcriber.Close/bridge.Disconnect all run and
		// the HTTP server drains instead of dying mid-handler.
		select {
		case sig := <-sigs:
			log.Printf("received signal %s; shutting down", sig)
		case <-bridge.Fatal():
			log.Println("fatal condition reported; shutting down for a supervisor restart")
		}
		cancel()
		bridge.Disconnect()
		if err := server.Shutdown(); err != nil {
			log.Printf("server shutdown: %v", err)
		}
	}()

	// Authentication runs in the background so the HTTP API is live DURING
	// first-run pairing — a supervisor drives /api/auth/* exactly then, and
	// the terminal QR loop re-requests fresh codes on timeout instead of
	// killing the process (auth.go). Auth errors no longer take the API down.
	go func() {
		if err := bridge.RunAuth(ctx); err != nil && ctx.Err() == nil {
			log.Printf("auth: %v — HTTP API stays up; POST /api/auth/reconnect to retry", err)
		}
	}()

	log.Printf("HTTP server listening on http://%s:%d", cfg.BridgeHost, cfg.BridgePort)
	if err := server.ListenAndServe(ctx); err != nil {
		log.Printf("server: %v", err)
		return 1
	}

	// Reading the closed channel is how the exit code gets out of the shutdown
	// goroutine without a shared variable and a data race: a channel close is a
	// synchronisation point, and ListenAndServe has already returned by now.
	select {
	case <-bridge.Fatal():
		log.Println("bridge stopped for a supervisor restart (fatal condition)")
		return 1
	default:
		log.Println("bridge stopped cleanly")
		return 0
	}
}

// truncateForLog returns a short version of a path for logging, redacting if empty.
func truncateForLog(p string) string {
	if p == "" {
		return "(unset)"
	}
	if len(p) > 60 {
		return p[:60] + "..."
	}
	return p
}
