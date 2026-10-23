# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **History filed group messages as sent by the group itself** (130 of 200 in one group, 2026-10-23 test run): the sender was read only from the message key, while history sync usually carries it on the message, which is where whatsmeow reads it. Group senders now come out right, rows stored the old way are repaired when history delivers them again, and until then they read "Participante desconocido". Asking the phone for such a message's media named the group as the sender and was silently ignored for two minutes; it is now refused at once, with the reason.
- **Sending did not move the chat**: its time and preview stayed on the last message received. A send now updates them, and creates the chat for a first message to someone.
- Direct chats known only by a number get the person's name or phone; old markers of types the decoder now reads ("[unsupported: albumMessage]") read as what they were, with the marker kept under `raw_marker`; download errors are labelled in Spanish; `list_messages` refuses a limit under 1.

- **A download that needed the phone could stay "downloading" forever** (2026-10-22 test run). Asking the phone takes up to two minutes, longer than any request waits, and a failed download job was dropped the moment it ended, so every poll started a fresh attempt. A failure now reaches the next request, once; the request after that tries again.
- **Old attachments failed with "hash of media ciphertext doesn't match".** WhatsApp's copy was not the one the message describes, on every CDN host. That copy is no longer retried three times; the phone is asked for the original, and if that fails too the answer says so in plain words (410).
- **Chat previews read "[unsupported: protocolMessage]"** (the user's own chat among them): migration 009 points them at the newest message with something to show. Markers of messages with nothing to read (deletions, encrypted votes, reactions and edits, key shares) are no longer listed by `list_messages`, and rows stored as markers before the decoder knew their type (templates, albums and their photos, polls) are re-decoded when history brings them again — only when the message really carries that type.
- `list_messages` gives every message the chat_jid asked for, also in merged chats, and names senders who had only a number: their name under any of their JIDs, else their phone, else "Desconocido".
- `list_chats` refuses a negative offset or a limit under 1 instead of quietly answering.

- **The new-conversation limit counted messages, not conversations, and counted the user's own chat.** Five files sent to oneself used up the day's five "new conversations" and blocked every further send (found by the 2026-10-21 test run). The own chat has no inbound messages by construction, so it always looked like a stranger. Now a chat counts once per 24 h, the own chat (phone and @lid forms) never counts, and the refusal says when the next slot frees up (`new_chats_next_slot_at` in the send budget).
- **Parallel confirms to one chat were refused for arriving 2 s apart.** A send now waits its turn in the chat's line instead: sends to one chat go out one at a time, in the order they became ready (a text when it is confirmed, a file when its upload finishes), each 5 s after the previous one actually went out — so uploads finishing together still go out spaced, in order, and a text is not held behind uploads still running. Up to four uploads may be lined up per chat and a text waits at most 10 s; past that the send is refused as a burst and the draft stays usable. A send that ends without anything going out gives its count back, so retries do not use up the hourly budget.
- **A text could be marked failed after it had been delivered.** The MCP layer gives up after 30 s; the cancelled request then cancelled the send after the message had left, the draft was marked failed and the answer invited sending it again. Once a draft is confirmed the send now runs to its end regardless, and a send whose failure does not prove the message stayed home — no acknowledgement in time, or the connection dropping while waiting for one — is reported as "delivery unknown — check the chat" (504) instead of failed.
- A finished upload checks the connection right before sending, after waiting its turn, so a disconnect during the wait no longer marks the draft failed and deletes its file.
- **Rate-limit refusals did not say when to try again where the assistant could see it.** The wait was only in `Retry-After` and `retry_after_ms`, which the MCP layer drops. The message now says "confirm it again in 9s" (or that the draft expires first and a new one is needed), and the hourly and daily caps report when their oldest send actually leaves the window instead of a flat hour or day.
- A confirm that lost a race with another confirm of the same draft answers with that confirm's outcome — the delivery, if it went out — instead of a bare 409.
- **A chat dated 2044 was pinned to the top of the chat list.** One imported message carried a timestamp from the future and the import keeps `MAX()`. A live message dated more than a day ahead sorts its chat as of now; history sync and imports leave such messages out of the chat's time and preview, which come from the newest believable message. Migration 008 repairs the rows already written, `last_message_id` and preview included. The message keeps its own timestamp. History sync also moves a chat's preview and last message id along with its time, instead of the time alone.
- **Blank "system" messages kept piling up** (24,111 and growing): every Signal group-key distribution was stored as an empty row and blanked its chat's preview. Messages that only carry keys or metadata are no longer stored; the old rows are hidden from `list_messages`. Content from a newer WhatsApp than this whatsmeow understands is told apart from them on the protobuf itself, anywhere along the envelope (comments on announcements and edits included), and is kept with an `unknown_field_N` marker.
- **`list_chats` came back empty from about the 1,500th chat on**, and a person whose other JID sat further down the list showed up again on a later page. Paging now counts people across the whole list, each listed once with all their JIDs and unread counts; a negative `offset` or `limit` no longer fails the request.
- `list_messages` `since`/`until` accept unix timestamps in milliseconds, microseconds or nanoseconds; a 13-digit value used to be read as seconds, a date thousands of years out that matched nothing.
- **One person appeared several times** in `list_chats` (phone JID and @lid) and in contact search (plus one row per linked device, `:79`, `:80`…). Both now return one entry per person, with the other forms under `merged_jids` / `aliases`; a person is everything linked through `jid_aliases`, so a @lid seen with an old and a new number is one person too, in the lists, in `list_messages` and for the send limits. The user's own messages show the user's name instead of a bare number.
- Received HTML files were saved as `.ehtml` (Linux's mime database lists it first); common types now have pinned extensions.

- **Media downloaded on another machine was fetched from WhatsApp again, and old files failed.** After moving the bridge from Windows to Linux, `media_path` still pointed at `C:\Users\...`, so the fast path missed and the bridge asked WhatsApp for the bytes — which answers 404/410 for older media. Three of the download failures in the deployment's audit log were files already on disk. `DownloadMediaFile` now looks for the message's file in the media folder by stem (the extension depends on the OS that downloaded it: `.jfif`, `.dtsconfig`) and repoints the row.
- **Expired media is recovered from the phone.** A 403/404/410 from the CDN now triggers a media retry receipt (`SendMediaRetryReceipt`), and the download continues with the fresh direct path the phone sends back. When the phone is offline or no longer has the file, the error says so (HTTP 410) instead of a bare status code.
- **A large media send could report failure after it had been delivered.** Confirm ran the upload inside the HTTP request, and the server's 60 s `WriteTimeout` fired first — the message went out with nobody left to tell, the caller saw an error, and the obvious next step was sending it again. Uploads now run as jobs detached from the request (see Added).
- **History-sync documents with a caption, and media in disappearing chats, had no media keys.** The history path extracted media fields without unwrapping envelopes (the live path gets them unwrapped by whatsmeow), so those rows were typed `document` but could never be downloaded. Both paths now share one extractor that unwraps first.

### Changed

- **Files up to WhatsApp's 2 GB document limit, without holding them in memory.** Downloads stream to disk (`DownloadToFile`), uploads encrypt from disk to disk (`UploadReader`), and `file_path`/`file_url` drafts copy in streaming fashion. The document cap goes from 100 MiB to 2 GB.
- **Photos and audio over 16 MiB, and videos over 100 MiB, are sent as documents instead of being refused.** Videos keep the 100 MiB they effectively had before, so nothing that used to play inline now arrives as a file. The planned envelope is decided at draft time, stored on the row, and explained in the draft's `note`/`delivered_as` so the user approves what will actually be sent. A voice note, which has no document form, is the one case still refused (after transcoding).
- **Confirm is idempotent.** A large send answers 202 and the caller confirms again to learn the outcome, so confirming a draft that was already sent now returns that delivery (200, same `whatsapp_message_id`) instead of 409 "create a new draft to retry" — the answer that invited sending the same file twice. A failed draft says why; a draft whose preparation failed returns the reason instead of 404. `confirm_send` in main.py re-confirms for up to 30 s while an upload runs.
- A finished upload waits up to 2 minutes for WhatsApp to reconnect before sending, and otherwise leaves the draft a draft; a send that does fail frees the draft's file.
- Media the phone could not supply this time (offline, no answer) is 503, kept apart from media it no longer has (410), so a temporary failure is not reported as permanent.
- `file_path` must be a regular file and is copied with a byte limit (`/dev/zero` or a FIFO used to fill the disk or hang the draft).
- Google Drive `file_url`s carry `confirm=t`, skipping the virus-scan interstitial Drive shows for files over ~100 MB.
- `file_url` downloads have no 3-minute ceiling any more; a download that stops moving for 3 minutes is abandoned instead, counting from the first byte.
- A download that fails midway starts over from an empty file, up to 3 times. whatsmeow's own retries rewrite the file without truncating it, and its fallback to another CDN host does not rewind at all, so an interrupted large download used to end in an integrity error.

### Added

- **Long media work runs as jobs** (`jobs.go`). `POST /api/media/download`, media drafts and media confirms answer within `?wait=` seconds (default 20, max 25): 200 with the result as before, or 202 with progress. Calling the download or confirm endpoint again joins the running job — a second confirm can never start a second upload.
- `GET /api/sends/{draft_id}`: a draft's state (`preparing`, `draft`, `uploading`, `sent`, `failed`, `expired`, `rejected`), with progress while a job runs and the reason when a confirm's upload failed.
- `messages.media_filename` (migration 007): the name a document was sent with, captured on receive, on history sync and on send. `POST /api/media/download` now also returns `filename` (original, or generated like `Imagen 2026-10-21 12.34.56.jpg`), chat, sender and timestamp.
- Abandoned drafts are swept (`outbound_sweep.go`): at startup the previous run's partial and scratch files are deleted before the API serves, and every hour drafts past their hour are marked expired and their files freed. With drafts of up to 2 GB, a few abandoned ones would otherwise fill the disk the message database lives on.
- `GET /api/chats/search`: find chats, groups included, by name — accent-insensitive, with a `chat_type` filter. Groups could not be found by name before.
- `GET /api/messages` returns each media message's `filename`, `mime` and `size`, and accepts `media_type`, `since`, `until` and `from_me` filters.

## [0.4.0] - 2026-08-19

### Fixed

- **A control character in a chat name failed the ENTIRE vault export on Windows.** `sanitizeFilename` replaced the Win32 forbidden punctuation set, appended `_` to reserved device stems, trimmed trailing dots/spaces and NFC-folded for APFS — but never stripped C0 controls. A WhatsApp group SUBJECT and a contact PUSH NAME can both contain a newline, and both are set by other people. On macOS/Linux `Team
Standup.md` is a legal filename, so the export wrote it and the run was green; Windows rejects the path outright, the unit lands in `errs[]`, and reconciliation counts a dropped chat — which fails the whole export by design. One awkward group subject took every other chat down with it. Measured on the same commit: FAIL on Windows, `ok` on Linux.
- **A sender-chosen message ID wrote files outside the media directory.** `media_download.go` built its output path as `filepath.Join(MediaPath, messageID+ext)` with no validation, and that ID is not ours — whatsmeow lifts it verbatim off the wire and the bridge stores it unmodified, so the SENDING client chooses it. `filepath.Join` cleans a path; it does not confine one, so `..\..\evil` escaped (and on Windows `\` separates too). `safeMediaStem` now reduces the ID to an allowlisted stem, appending a hash of the original whenever anything changed so two hostile IDs cannot collide onto one file. Benign IDs pass through byte-identical, so media already on disk still resolves.
- **A keychain authorization prompt could hang the bridge at startup, forever (macOS/Linux).** No secret-store subprocess had a timeout, and `GetOrCreateDBKey` runs before the HTTP server binds — so a `security` or `secret-tool` call that decided it needed user authorization waited for a human indefinitely, with nothing in the log to say why. Measured on the macOS CI runner: `security add-generic-password` blocked and was killed after 9m56s. Every such call now runs under a deadline (`WHATSAPP_KEYCHAIN_TIMEOUT_SECONDS`, default 120s) — generous enough for a human to answer a real dialog, bounded so headless installs get a named error pointing at `WHATSAPP_DB_KEY`. Windows is unaffected (CredReadW/CredWriteW syscalls, not a subprocess).
- **`scripts/check_prerequisites.sh` told people they needed Go when they did not.** It treated Go as required and exited on the FIRST miss, so anyone installing the documented way — prebuilt binary, no compiler — met the project with a red `FAIL Go not found`, and never saw the python/uv checks that actually matter. Now matches `check_prerequisites.ps1`: required = python 3.11+ and uv; go and a C compiler are source-build only; reports everything, never stops early.
- **Shell scripts were CRLF-mangled on every Windows clone, and failed open.** With no `.gitattributes`, Git for Windows' default `core.autocrlf=true` rewrote the tracked scripts, so `set -euo pipefail` never applied and `check_prerequisites.sh` exited 0 having verified nothing. `.gitattributes` now pins `eol=lf` for text and `eol=crlf` for `.ps1`.
- Loopback is not a trust boundary against the browser (#34): a cross-origin `Content-Type: text/plain` POST is a CORS simple request, so it is never preflighted and every state-changing route ran on the way in. Verified against the running bridge before the fix. An origin guard now refuses non-allowlisted `Origin` headers and pins `Host` to loopback literals against DNS rebinding. Non-browser clients (the Python MCP server, curl, supervisors) send no `Origin` and are unaffected; `WHATSAPP_ALLOWED_ORIGINS` allows a browser-based pairing GUI through.
- The release smoke test could not fail — `--help 2>&1 | head -5 || true` discarded the exit status twice, so a binary that crashed on startup still shipped. It now captures the output and asserts on it.
- `manifest.json` launched `python main.py` with no dependency resolution, failing on any machine without fastmcp/httpx/python-frontmatter/unidecode already installed. Now uses `uv run`, matching `.mcp.json`.
- `.mcp.json` set `WHATSAPP_WHISPER_BACKEND=local-cpp` on the MCP server process, which never reads that variable (it belongs to the Go bridge) — inert, contradicting the documented `off` default, and naming a backend unavailable on Windows. Removed.
- `.gitignore` covered `whatsapp-bridge/whatsapp-bridge` but not the Windows `.exe`, so `go build ./...` left a 45 MB untracked binary.

### Added

- **Prebuilt binaries for Intel Macs (`darwin-amd64`).** The release matrix built only `darwin-arm64`, so the "no compiler needed" install path silently did not exist for Intel Macs — those users were routed into a Go + Xcode source build without being told.
- Real-Keychain round-trip test on the macOS CI leg (`keychain_darwin_test.go`), mirroring the existing Windows one. The macOS and Linux keychain tests use a PATH shim, which proves the exit-code classification but never touches a real store — so the only key store proven end to end was the one fewest users run. This test is what surfaced the startup hang above.
- `gofmt` CI gate. Nothing checked formatting, which is how 21 of 91 tracked `.go` files drifted; all swept.
- `WHATSAPP_ALLOWED_ORIGINS` and `WHATSAPP_KEYCHAIN_TIMEOUT_SECONDS` (both documented in `.env.example`).

### Changed

- Version is now sourced from one place (`whatsapp-bridge/version.go`, overridable via `-ldflags`). `/healthcheck` reported `0.3.0` while the newest tag was `v0.3.1`, `plugin.json` said `1.0.0`, `manifest.json` `0.1.0` and `pyproject.toml` `0.1.1` — five numbers, no two alike. All aligned on 0.4.0.

- **A transient secret-store read failure can no longer destroy the DB key (MYC-3694).** The bridge minted a fresh SQLCipher key on ANY failed keychain read and wrote it with overwrite semantics (`security add-generic-password -U`), so a locked keychain (exit 51) silently replaced the real key and permanently orphaned the encrypted message store. Reads are now classified on all three platforms — macOS exit 44 / Windows `ERROR_NOT_FOUND` / libsecret silent miss are the only "mint" states; every other failure exits loudly, writes nothing, and names the remedy (unlock the store, or set `WHATSAPP_DB_KEY`). The macOS write dropped `-U` (create-only), and minting is refused outright while a non-empty store database exists.
- **Vault export dropped entire direct chats and filed group chats under person names, while exiting 0 (MYC-3555).** Three interacting defects: output filenames were keyed on display name alone, so a group whose stored name was a person's push name collided with that person's direct chat on one path; the unchanged-skip trusted the `last_message_ts` of whatever file sat at the path without checking whose `jid` it carried, so an active group occupying a person's filename suppressed that person's export forever (counted as "skipped-unchanged", rc=0); and the exporter never consulted `jid_aliases`, so the LID and phone forms of the same human exported as two colliding files each holding half the history. Now: one human = one file with both JID forms merged (`alias_jids` frontmatter lists the other forms), non-direct chats always carry their type in the filename (`Name (group).md`, JID digits appended on residual collisions), the unchanged-skip only fires when the existing file provably belongs to the chat, files left under pre-fix names are healed by rename, `--export-min-messages` counts across a person's merged forms, and a run that drops any enumerated chat exits non-zero NAMING the dropped chats plus logs a machine-checkable `export: reconcile ...` counts line. rc=0 now means complete.
- Group chats born from a live message are no longer named after whichever member sent the first message the bridge saw; the real subject and participant count now come from group metadata (startup sync + JoinedGroup event).
- Exported group files no longer fabricate a `phone:` from the group JID (and LID digits are never rendered as a phone); `participants_count` is now populated from stored group metadata, with a distinct-sender floor as fallback instead of the old constant 0 that slid under every group-size policy.

- `--reconcile-vault <dir>`: offline audit of an exported vault against the bridge DB. Prints greppable `MISSING` / `MISFILED` / `DRIFT` / `DUPLICATE` / `ORPHAN` findings (chat with messages but no file; group sitting in a person-named file; file whose `message_count` disagrees with the DB; one JID claimed by two files; file whose JID left the DB) and exits non-zero when any exist. Honors the same `--export-include-groups` / `--export-min-messages` filters as the export.
- Startup group-metadata sync (`SyncGroupMetadata`) writing every joined group's real subject + participant count into `chats`, healing rows the old live path had person-named.

- `scripts/check_prerequisites.sh` no longer hard-fails on `ffmpeg` or the `sqlcipher` CLI: transcription is off by default (ffmpeg is only needed for `local-cpp`), and the bridge compiles its own SQLCipher (the CLI was never a runtime requirement — the checker itself was a leftover stuck-point). Both downgraded to warnings with accurate guidance, matching `check_prerequisites.ps1`.
- The bridge logs one line at startup when transcription is off, so users upgrading from the old `local-cpp` default see the behavior change instead of losing transcripts silently.
- README manual-install block aligned with v0.2.0 reality: prebuilt binaries first, FFmpeg/whisper listed as optional, pairing-code alternative mentioned.

### Removed

- Tracked `whatsapp-mcp.mcpb` at the repo root — it was a stale v0.1.x build that nothing referenced; the canonical bundle is the release asset (`releases/latest/download/whatsapp-mcp.mcpb`), rebuilt per release.

### Known issues

- **macOS: a Keychain prompt on the second and later boots (#70).** The stored key item is created with `-T ""` (an empty trusted-application list), which per `security(1)` removes the default trust the creating app would get — so `/usr/bin/security` may not read the key back without the user approving it. First boot mints the key and works; later boots raise a Keychain dialog. Clicking **Always Allow** once resolves it permanently. Headless installs should set `WHATSAPP_DB_KEY`. Whether to keep that posture is a security tradeoff tracked in #70; the timeout above makes either answer survivable.

## [0.2.0] - 2026-07-13

### Added

- **Windows support, end to end (MYC-3033).** Native Windows Credential Manager storage for the SQLCipher key (direct advapi32 syscalls, no new dependency) with a real round-trip test on the windows CI runner; the long-documented `WHATSAPP_DB_KEY` env override is now actually implemented (it previously existed only in error messages and docs); Windows-safe terminal QR rendering (ANSI block fallback outside Windows Terminal, drawn through go-colorable so legacy conhost translates it); `scripts/check_prerequisites.ps1`; SETUP.md rewritten with per-OS commands (PowerShell/winget/MSYS2 UCRT64, correct `.exe` paths, `%USERPROFILE%` layouts); `.mcpb` manifest `platform_overrides` launches via `python` on win32 instead of the usually-absent `python3`; the install-ping hook falls back `python3 → py -3 → python`.
- **Live-refreshing QR + pairing-code auth.** The terminal QR redraws in place on every rotation (attempt counter + expiry note) instead of stacking stale codes down the scrollback; when a whole batch expires the bridge requests fresh codes automatically instead of exiting; `--pair-phone +15551234567` pairs by typed 8-char code (identical on Android and iOS) for terminals that can't render a QR at all; non-TTY runs emit one parseable log line per rotation instead of ANSI art.
- **Headless auth API (feeds the Mycelium Studio connector, MYC-961).** The HTTP server now starts BEFORE pairing, so the API is live during first-run auth: `auth_state` machine + `logged_out_reason` + `qr_expires_at` on `GET /api/status`; `GET /api/auth/qr` (raw rotating code for a GUI to render); `POST /api/auth/pair-phone`; `POST /api/auth/reconnect`. WhatsApp-side logouts re-enter pairing automatically without a process restart. Read responses carry a `_bridge_state` envelope so cached SQLite data is distinguishable from a live session. `WHATSAPP_BRIDGE_PORT=0` picks a free port, announced via a `BRIDGE_LISTENING port=N` line + a `bridge.port` sidecar file for multi-instance supervisors.
- **`.env` file support.** SETUP has always said `cp .env.example .env`, but nothing ever read that file — installs silently ran on defaults. The bridge now loads `.env` from the working directory or next to the binary; explicit process env always wins.
- **Cross-platform CI + release binaries.** `tests.yml` now runs the Go + Python suites on ubuntu/macos/windows (MSYS2 UCRT64 gcc provides CGO on the Windows runner); new `release.yml` builds and attaches prebuilt bridge binaries (+ SHA256 files) for all three OSes to every published GitHub release — Windows users skip the C toolchain entirely.

- **Python test suite** under `whatsapp-mcp-server/tests/`. Mirrors the Go scrubber test corpus pattern: known-pattern coverage in lowercase / mixed-case / prose-sandwich variants, false-positive corpus, multi-pattern in one message, and a documented "known gaps" set (skipped, parity with Go). Also covers `lookup_crm_context` (tempdir-backed CRM fixtures) and `_normalize_phone`. 98 passing / 10 skip-known-gaps.
- **Go test suite for `normalize.go` and `aliases.go`.** `normalize_test.go` covers ASCII, Spanish accents, European diacritics, idempotence, and non-Latin passthrough. `aliases_test.go` covers the pure helpers (`inClausePlaceholders`, `jidsToArgs`) and the DB-backed `recordJIDAlias` + `resolveAliases` against in-memory SQLite, including the symmetric-edge invariant that was the original LID/phone JID split regression.
- **`.github/workflows/tests.yml`** runs the full Go + Python test suite on every PR + push (broader than `scrubber-eval.yml`, which only fires on scrubber-file changes).
- **`docs/TROUBLESHOOTING.md`** documents operational pain points: QR pairing failures, `StreamReplaced`, LID/phone alias split, voice-note transcription prereqs, SQLCipher Keychain backup, `uv sync` parent-dir quirk, bridge `/healthcheck` 502, audit-log rotation.
- **`SECURITY.md` webhook integrity contract.** Documents the three controls (HMAC signature, per-delivery idempotency key, retry queue) that the webhook feature MUST ship with — codified before implementation so the feature can't ship without them.

### Changed

- **`WHATSAPP_WHISPER_BACKEND` defaults to `off` (was `local-cpp`).** The old default made a ~3 GB whisper model a boot requirement — `Config.Validate()` refused to start without `WHATSAPP_WHISPER_MODEL_PATH`, stranding every fresh install on every OS. Transcription stays fail-loud once explicitly enabled, and media keys are persisted regardless, so enabling a backend later backfills recent voice notes. Set `WHATSAPP_WHISPER_BACKEND=local-cpp` explicitly if you relied on the old default.
- Vault-export filenames escape Windows-reserved device stems (`CON`, `NUL`, `COM1`…) and trailing dots/spaces.
- whisper/ffmpeg runtime error messages give per-OS install hints (previously Homebrew-only).
- Python MCP: bridge failures now surface the Go side's structured `{error, details}` body instead of an opaque HTTP status string, and stderr is forced to UTF-8 on Windows so Spanish text in log lines isn't silently dropped.

### Security

- **Sync Python scrubber pattern list with Go.** The Go scrubber gained 5 patterns (`ignore above instructions`, `disregard prior instructions`, `dump your system prompt`, `tell me your instructions`, `what are your instructions`) in the 2026-05-19 hardening pass; the Python layer kept the older 13-pattern list. Both scrubbers run in production (Go on incoming-from-protocol before DB write; Python before Claude sees text); drift between them was a defense-in-depth defect. New `test_pattern_count_matches_go` parity test fails CI on future drift.

### Fixed

- **pip-audit CI gate** had been failing on every push to main for at least 5 commits before this PR. Root cause: `whatsapp-mcp-server/pyproject.toml` referenced `readme = "../README.md"` and `license = { file = "../LICENSE" }`, and setuptools 77+ rejects any file ref outside the package root. The publish pipeline staged the files before build, so the published wheel was fine, but local builds + CI's `pip-audit` could never resolve the package. Fix: inline `readme = { text = "...", content-type = "text/markdown" }` and `license = "MIT"` (PEP 639 SPDX). PyPI page becomes minimal; the full README still lives at the repo root and is linked from the `[project.urls]` Homepage. The TROUBLESHOOTING.md `uv sync` entry now describes a fixed historical issue rather than a current one.

## [0.1.1] - 2026-05-19

### Added

- **Published to [MCP Registry](https://registry.modelcontextprotocol.io)** as `io.github.adelaidasofia/whatsapp-mcp` v0.1.1. PyPI path. Discovery-surface live for all MCP-aware clients.
- **PyPI package `adelaidasofia-whatsapp-mcp` v0.1.1** published via OIDC trusted-publisher. Description shortened to fit the registry's 100-char limit (v0.1.0 failed validation on the long-form description).
- **Scrubber CI gate** (`.github/workflows/scrubber-eval.yml`) running a 73-case eval corpus on every PR + push + weekly cron. Backs the security claim in SECURITY.md item 5 with actual enforcement.
- **whatsmeow upgrade-review workflow** (`.github/workflows/whatsmeow-upgrade-review.yml`) that intercepts any PR moving the whatsmeow pin, posts the upstream commit log as a PR comment, and applies a `whatsmeow-diff-review-required` label that must be manually cleared before merge.

## [0.1.0] - 2026-05-19

### Added

- **`.mcpb` bundle** built by the Mycelium MCP publishing pipeline and released as a GitHub artifact at [releases/v0.1.0](../../releases/tag/v0.1.0). One-click install in Claude Desktop / Cursor via the MCPB format.
- **Initial PyPI release** of `adelaidasofia-whatsapp-mcp` v0.1.0 (superseded by 0.1.1 for the registry's description-length constraint).
- **`server.json` registry manifest** with full 9-env-var schema covering bridge host/port, vault CRM path, Whisper backend selection, scrubber toggle, audit log toggle, and SQLCipher encryption toggle.

## [Unreleased]

### Fixed

- **JID alias resolution for LID + phone-number forms.** WhatsApp's privacy
  rollout migrated most direct chats to LID-form JIDs (`<opaque>@lid`) while
  legacy threads stayed on `<phone>@s.whatsapp.net`. Recent traffic for the
  same human can arrive under either form depending on the contact's privacy
  settings, and whatsmeow surfaces both forms via
  `MessageSource.SenderAlt` / `RecipientAlt`. The bridge previously stored
  each form as a separate contact + chat row with no link, so
  `search_contacts` returned only the row whose stored push_name matched the
  query string and `list_messages` returned only that JID's history — a
  contact whose recent thread had moved to LID looked silent under their
  legacy JID. Fix:
  - New migration `003_jid_aliases.sql` adds a symmetric `jid_aliases` edge
    table.
  - `aliases.go` introduces `BackfillJIDAliases`, which walks contacts at
    startup, asks `client.Store.LIDs.GetPNForLID` / `GetLIDForPN` for the
    counterpart form, and writes the edge.
  - `onMessage` now records alias edges from `Info.SenderAlt` (incoming DMs)
    and `Info.RecipientAlt` (outgoing DMs) on every event.
  - `onMessage` no longer stores `Sender.User` as `phone` for LID-form JIDs
    (where `User` is the opaque LID number, not a phone). Falls back to
    `SenderAlt.User` when the alt is the phone form, otherwise leaves
    `phone` NULL until the startup backfill resolves it.
  - `BackfillJIDAliases` repairs existing rows whose `phone` column was
    populated with the LID number from the legacy ingestion path.
  - `handleSearchContacts` returns both JID rows for any matched human, each
    with the other(s) listed in a new `aliases` field.
  - `handleListMessages` resolves the requested JID through
    `jid_aliases` and queries `WHERE chat_jid IN (jid + aliases)`. Adds a
    `merged_jids` field to the response when more than one form contributed.
  - `/healthcheck` exposes a new `alias_coverage` block:
    `total_edges`, `contacts_with_alias`, `direct_chats_lid`,
    `direct_chats_phone`, and the regression guard
    `suspicious_lid_phones` — non-zero means at least one LID contact still
    has its `phone` column populated with the LID number itself.

### Added

- Initial project scaffold.
- Repository structure: `whatsapp-bridge/` (Go), `whatsapp-mcp-server/` (Python), `migrations/`, `scripts/`, `docs/`.
- MIT license.
- `SECURITY.md` with full threat model and tool risk-tier classification.
- `SETUP.md` with step-by-step install for macOS, with Linux and Windows notes.
- `.env.example` documenting all configurable environment variables.
- `scripts/check_prerequisites.sh` validator for Go, Python, FFmpeg, SQLCipher, uv, and whisper.cpp.
- SQL schema in `whatsapp-bridge/migrations/001_initial.sql`: tables for contacts, chats, messages, calls, media_downloads, sends, audit_log.

### Architecture decisions

- Go bridge built directly on `go.mau.fi/whatsmeow`, not forked from any existing MCP.
- Python MCP layer uses `FastMCP` 3.x, matches the pattern used by other MCPs in the author's stack.
- SQLite with SQLCipher for encrypted-at-rest storage; key from macOS Keychain.
- Bridge binds to `127.0.0.1` only, never `0.0.0.0`.
- Voice-note transcription defaults to local `whisper.cpp` with `large-v3` model; OpenAI API is opt-in only.
- Vault CRM auto-injection reads from a configurable path (env var), defaulting to empty.
- Send tools require a two-step `draft` then `confirm_send` pattern. No one-shot sends.
- Prompt-injection scrubber on all incoming message text.
- Public GitHub repository from day 1.

## [0.1.0] - 2026-04-23

Initial public scaffold. Not yet functional end-to-end; auth and core tools land in subsequent commits.
