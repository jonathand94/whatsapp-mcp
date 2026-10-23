// chat_preview_repair.go — stored messages with nothing to show.
//
// Rows stored as markers for messages that carry nothing a person can read —
// a deletion or other protocol message, an encrypted poll vote, reaction,
// event answer or edit, a key share — are kept in the archive but left out of
// list_messages and chat previews, like the blank rows of plumbing. The live
// path stopped making previews of them on 2026-10-21 (hiddenFromChatPreview);
// these markers are what the rows stored before then, and before the decoder
// knew better, look like. Migration 009 repairs the previews already written.

package main

import "strings"

// hiddenMarkerTypes are the message types whose stored markers are hidden:
// previewlessFields, plus encrypted messages, which the bridge cannot read and
// which are almost all edits of another message.
var hiddenMarkerTypes = []string{
	"protocolMessage",
	"pollUpdateMessage",
	"encReactionMessage",
	"encEventResponseMessage",
	"secretEncryptedMessage",
	"fastRatchetKeySenderKeyDistributionMessage",
	"groupRootKeyShare",
	"rootSecretDistributeMessage",
	"stickerSyncRmrMessage",
}

// hiddenMarkers is the content_text of each hidden type's marker.
func hiddenMarkers() []string {
	out := make([]string, len(hiddenMarkerTypes))
	for i, t := range hiddenMarkerTypes {
		out[i] = unsupportedMarker(t)
	}
	return out
}

// hiddenRowsSQL is a WHERE condition that leaves out blank system rows and
// hidden markers, with its arguments.
func hiddenRowsSQL() (string, []any) {
	markers := hiddenMarkers()
	return "NOT (type = 'system' AND (COALESCE(content_text, '') = '' OR content_text IN (" +
		strings.TrimSuffix(strings.Repeat("?,", len(markers)), ",") + ")))", jidsToArgs(markers)
}
