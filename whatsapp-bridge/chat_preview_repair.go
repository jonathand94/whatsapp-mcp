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

import (
	"fmt"
	"sort"
)

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

// hiddenMarkerSQL is an SQL condition true when col holds a marker every one
// of whose fields is a hidden type: "[unsupported: protocolMessage]" and also
// "[unsupported: fastRatchetKeySenderKeyDistributionMessage,protocolMessage]".
// Markers list their fields sorted and comma-joined (unsupportedMessageType),
// so removing every hidden name and the commas leaves the bare brackets.
// Longest names go first, so none is cut out of a longer one; a field that
// is not hidden leaves text behind, and the marker stays visible.
func hiddenMarkerSQL(col string) string {
	names := append([]string(nil), hiddenMarkerTypes...)
	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
	expr := col
	for _, n := range names {
		expr = fmt.Sprintf("REPLACE(%s, '%s', '')", expr, n)
	}
	return fmt.Sprintf("(%s LIKE '%s%%' AND REPLACE(%s, ',', '') = '%s')",
		col, unsupportedPrefix, expr, unsupportedPrefix+unsupportedSuffix)
}

// hiddenRowsSQL is a WHERE condition that leaves out rows with nothing to
// show: blank system rows, hidden markers, and withdrawn reactions (a
// reaction with no emoji).
func hiddenRowsSQL() string {
	return "NOT ((type = 'system' AND (COALESCE(content_text, '') = '' OR " + hiddenMarkerSQL("content_text") + "))" +
		" OR (type = 'reaction' AND COALESCE(content_text, '') = ''))"
}
