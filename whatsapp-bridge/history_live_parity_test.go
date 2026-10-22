package main

import (
	"database/sql"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// MYC-3580 — a message that reaches the store through HISTORY SYNC must decode
// identically to the same message arriving LIVE.
//
// Why this exists. Three community PRs (#46, #47, #48) each add a history-sync
// write path, and each shipped its OWN copy of a proto-level content extractor
// named `extractContentFromProto`. Main already has one, from PR #52, and
// main's is materially better: it unwraps envelopes (ephemeral, view-once,
// device-sent, edits) and runs the second-tier decoders (polls, events, live
// location, contact arrays, the commerce family) before falling back to the
// fail-loud marker.
//
// Two copies of a decode rule is the drift hazard the shared decoder was
// created to remove, and the drift would be INVISIBLE: rows written by one path
// would decode differently from rows arriving beside them, with nothing
// comparing the two. That is what this test compares.
//
// It is deliberately keyed on the OBSERVABLE (the stored `type` +
// `content_text`), not on which function got called. A writer is free to be
// implemented however it likes; it is not free to disagree with live receive
// about what a message says.
//
// WHEN #46's history-sync INSERT lands, add it to writeParityCases as a third
// writer. The invariant is per-WRITER, not per-PR: any new path that puts a row
// in `messages` belongs in this table.

// parityShape is one message shape exercised through every writer.
type parityShape struct {
	name string
	msg  *waE2E.Message
	// wantType/wantText document what BOTH writers must agree on, so the test
	// fails loudly if the shared decoder changes meaning, not just if the two
	// paths drift apart from each other.
	wantType string
	wantText string
}

func parityShapes() []parityShape {
	return []parityShape{
		{
			name:     "plain text",
			msg:      &waE2E.Message{Conversation: proto.String("hola, nos vemos el jueves")},
			wantType: "text",
			wantText: "hola, nos vemos el jueves",
		},
		{
			// The ENVELOPE tier. A plain text message sent in a chat with
			// disappearing messages enabled has no top-level `conversation`.
			// A writer that skips unwrapping stores it as an unsupported
			// marker and loses the text — visible, but wrong.
			name: "text inside an ephemeral envelope",
			msg: &waE2E.Message{
				EphemeralMessage: &waE2E.FutureProofMessage{
					Message: &waE2E.Message{Conversation: proto.String("este mensaje desaparece")},
				},
			},
			wantType: "text",
			wantText: "este mensaje desaparece",
		},
		{
			// The SECOND-DECODER tier. A writer using only the top-level
			// switch marks this unsupported instead of reading the question.
			name: "poll (second-tier decoder)",
			msg: &waE2E.Message{
				PollCreationMessageV3: &waE2E.PollCreationMessage{
					Name: proto.String("Que dia nos reunimos?"),
					Options: []*waE2E.PollCreationMessage_Option{
						{OptionName: proto.String("jueves")},
						{OptionName: proto.String("viernes")},
					},
				},
			},
			wantType: "text",
			wantText: "📊 Encuesta: Que dia nos reunimos?\n- jueves\n- viernes",
		},
		{
			name: "image with caption",
			msg: &waE2E.Message{
				ImageMessage: &waE2E.ImageMessage{Caption: proto.String("la foto del venue")},
			},
			wantType: "image",
			wantText: "la foto del venue",
		},
		{
			// The FAIL-LOUD tier (MYC-3284). Both writers must produce the same
			// marker, or /healthcheck's undecoded counts depend on which path
			// happened to write the row.
			name: "undecodable type keeps the same marker",
			msg: &waE2E.Message{
				PollUpdateMessage: &waE2E.PollUpdateMessage{},
			},
			wantType: "system",
			wantText: unsupportedMarker("pollUpdateMessage"),
		},
	}
}

func parityTestInfo(t *testing.T, id string, ts int64) types.MessageInfo {
	t.Helper()
	chat, err := types.ParseJID("12147735814-1589465137@g.us")
	if err != nil {
		t.Fatalf("parse chat jid: %v", err)
	}
	sender, err := types.ParseJID("31628239888478:3@lid")
	if err != nil {
		t.Fatalf("parse sender jid: %v", err)
	}
	return types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsGroup: true},
		ID:            id,
		PushName:      "Martha",
		Timestamp:     time.Unix(ts, 0),
	}
}

// TestHistorySyncAndLiveReceiveDecodeIdentically is the MYC-3580 invariant.
func TestHistorySyncAndLiveReceiveDecodeIdentically(t *testing.T) {
	for i, shape := range parityShapes() {
		t.Run(shape.name, func(t *testing.T) {
			db := undecodedTestDB(t)
			b := &Bridge{db: db}
			ts := int64(1784846600 + i)

			// --- writer 1: LIVE RECEIVE -------------------------------------
			liveID := "live-" + shape.name
			b.handleEvent(&events.Message{
				Info:    parityTestInfo(t, liveID, ts),
				Message: shape.msg,
			})

			// --- writer 2: HISTORY-SYNC CONTENT BACKFILL ---------------------
			// Reproduces the real precondition: a row that was written before
			// the decoder could read it, sitting as an empty `system` row.
			histID := "hist-" + shape.name
			insertTestMessage(t, db, histID, "12147735814-1589465137@g.us", "system", "", ts)
			if _, err := b.backfillDecodedContent(histID, shape.msg); err != nil {
				t.Fatalf("backfillDecodedContent: %v", err)
			}

			liveType, liveText := parityRow(t, db, liveID)
			histType, histText := parityRow(t, db, histID)

			// The two writers must agree with EACH OTHER...
			if liveType != histType || liveText != histText {
				t.Fatalf("history-sync and live receive disagree on the same message:\n  live:    type=%q text=%q\n  history: type=%q text=%q",
					liveType, liveText, histType, histText)
			}
			// ...and both must agree with the decoder contract, so a shared
			// regression that moves both paths together still fails.
			if liveType != shape.wantType || liveText != shape.wantText {
				t.Fatalf("decoder contract changed: want (type=%q, text=%q), got (type=%q, text=%q)",
					shape.wantType, shape.wantText, liveType, liveText)
			}
		})
	}
}

// A message with no readable content must ALSO agree across writers. Since
// 2026-10-21 neither writer stores one (isPlumbing); a row an earlier version
// stored must be left alone by the backfill rather than rewritten.
func TestHistorySyncAndLiveAgreeOnAContentFreeMessage(t *testing.T) {
	db := undecodedTestDB(t)
	b := &Bridge{db: db}
	const ts = 1784846700

	// Live: a message carrying only group-key distribution is plumbing, and
	// is not stored at all (isPlumbing) — no blank row, no blank preview.
	b.handleEvent(&events.Message{
		Info: parityTestInfo(t, "live-empty", ts),
		Message: &waE2E.Message{
			SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
		},
	})
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages WHERE id = 'live-empty'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("a content-free live message was stored (%d rows, %v)", n, err)
	}

	// An empty row an earlier version stored stays exactly as it was: the
	// backfill must not turn plumbing into content either.
	insertTestMessage(t, db, "hist-empty", "12147735814-1589465137@g.us", "system", "", ts)
	updated, err := b.backfillDecodedContent("hist-empty", &waE2E.Message{
		SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
	})
	if err != nil {
		t.Fatalf("backfillDecodedContent: %v", err)
	}
	if updated != 0 {
		t.Fatalf("a content-free message must not rewrite the row; %d row(s) updated", updated)
	}
	histType, histText := parityRow(t, db, "hist-empty")
	if histType != "system" || histText != "" {
		t.Fatalf("the old row changed: (%q, %q)", histType, histText)
	}
	if !isPlumbing(&waE2E.Message{SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{}}) {
		t.Fatal("a key-distribution-only message is plumbing")
	}
	if isPlumbing(&waE2E.Message{Conversation: proto.String("hola")}) {
		t.Fatal("a text is not plumbing")
	}
}

// The repo must hold exactly ONE proto-level decoder. This is the structural
// guard behind the invariant above: PRs #46/#47/#48 each added a second
// definition of extractContentFromProto, and a second definition is how the two
// write paths silently drift apart in the first place. A merge that resolves
// those PRs by keeping BOTH copies would fail to compile in Go, but a merge
// that keeps the WRONG copy would compile and quietly lose envelope unwrapping
// and the second-tier decoders. This test catches that second case.
func TestSingleSharedDecoderIsTheOneWithEnvelopeUnwrapping(t *testing.T) {
	// The weaker copies in #46/#47/#48 test top-level fields ONLY, so an
	// ephemeral-wrapped text decodes to an "[unsupported: ephemeralMessage]"
	// marker rather than its text. If extractContentFromProto ever resolves to
	// such a copy, this fails.
	text, msgType := extractContentFromProto(&waE2E.Message{
		EphemeralMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{Conversation: proto.String("inner text")},
		},
	})
	if text != "inner text" || msgType != "text" {
		t.Fatalf("the shared decoder lost envelope unwrapping: got (%q, %q) — a weaker extractContentFromProto has replaced main's", text, msgType)
	}

	// Same check for the second-tier decoders.
	text, msgType = extractContentFromProto(&waE2E.Message{
		PollCreationMessageV3: &waE2E.PollCreationMessage{Name: proto.String("q?")},
	})
	if text != "📊 Encuesta: q?" || msgType != "text" {
		t.Fatalf("the shared decoder lost its second-tier decoders: got (%q, %q)", text, msgType)
	}
}

// parityRow reads the two columns the invariant is stated over.
func parityRow(t *testing.T, db *sql.DB, id string) (msgType, text string) {
	t.Helper()
	var ct sql.NullString
	if err := db.QueryRow(`SELECT type, content_text FROM messages WHERE id = ?`, id).Scan(&msgType, &ct); err != nil {
		t.Fatalf("read row %s: %v", id, err)
	}
	return msgType, ct.String
}
