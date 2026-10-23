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
	"strings"
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

// legacyMarkerLabels say what a stored marker was, for types the decoder reads
// today. Rows stored before it did keep their marker until history delivers
// the message again (backfillDecodedContent); until then a reader sees what
// kind of message it was instead of "[unsupported: albumMessage]", which the
// 2026-10-23 test run still found in the user's own chat.
var legacyMarkerLabels = map[string]string{
	"albumMessage":               "Álbum de fotos o videos",
	"associatedChildMessage":     "Foto o video de un álbum",
	"templateMessage":            "Mensaje de plantilla de una empresa",
	"templateButtonReplyMessage": "Respuesta a un botón de una empresa",
	"buttonsMessage":             "Mensaje con botones de una empresa",
	"buttonsResponseMessage":     "Respuesta a un botón",
	"listMessage":                "Mensaje con lista de opciones",
	"listResponseMessage":        "Respuesta a una lista de opciones",
	"interactiveMessage":         "Mensaje interactivo de una empresa",
	"interactiveResponseMessage": "Respuesta a un mensaje interactivo",
	"pollCreationMessage":        "Encuesta",
	"pollCreationMessageV2":      "Encuesta",
	"pollCreationMessageV3":      "Encuesta",
	"pollCreationMessageV5":      "Encuesta",
	"pollCreationMessageV6":      "Encuesta",
	"liveLocationMessage":        "Ubicación en tiempo real",
	"contactsArrayMessage":       "Varios contactos",
	"encCommentMessage":          "Comentario cifrado en un anuncio de comunidad",
	"eventMessage":               "Evento",
	"groupInviteMessage":         "Invitación a un grupo",
	"ptvMessage":                 "Videomensaje",
}

// friendlyMarker is the text shown for a stored marker whose every field has
// a label, or "" for anything else (real text, a marker of a type still
// unknown, which keeps its marker).
func friendlyMarker(text string) string {
	raw := unsupportedRawType(text)
	if raw == "" {
		return ""
	}
	var labels []string
	for _, name := range strings.Split(raw, ",") {
		label, ok := legacyMarkerLabels[strings.TrimSpace(name)]
		if !ok {
			return ""
		}
		labels = append(labels, label)
	}
	return "[" + strings.Join(labels, "; ") + ": mensaje antiguo, su contenido no se guardó]"
}
