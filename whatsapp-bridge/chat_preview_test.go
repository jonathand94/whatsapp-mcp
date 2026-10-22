package main

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// hiddenFromChatPreview decides whether a message may replace its chat's
// last-message preview. It is not wired into the write paths here; these tests
// pin the contract its callers will rely on: what is hidden, what never is, and
// that a hidden message is always one the decoder had nothing readable for.

func protocolMsg(pm *waE2E.ProtocolMessage) *waE2E.Message {
	return &waE2E.Message{ProtocolMessage: pm}
}

func encryptedSecret(kind waE2E.SecretEncryptedMessage_SecretEncType) *waE2E.Message {
	return &waE2E.Message{SecretEncryptedMessage: &waE2E.SecretEncryptedMessage{
		TargetMessageKey: &waCommon.MessageKey{ID: proto.String("target-1")},
		SecretEncType:    kind.Enum(),
	}}
}

// previewHiddenCases are messages with nothing of their own to read in the chat.
// Shared by the test that hides them and the one that checks the decoder agrees.
func previewHiddenCases() map[string]*waE2E.Message {
	revoke := &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_REVOKE.Enum(),
		Key:  &waCommon.MessageKey{ID: proto.String("revoked-1")},
	}
	return map[string]*waE2E.Message{
		// Protocol messages, the 3107 markers that were showing up as chats'
		// last message.
		"revoke": protocolMsg(revoke),
		"disappearing-messages setting": protocolMsg(&waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_EPHEMERAL_SETTING.Enum(), EphemeralExpiration: proto.Uint32(604800),
		}),
		"history-sync notification": protocolMsg(&waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION.Enum(), HistorySyncNotification: &waE2E.HistorySyncNotification{},
		}),
		"app-state key share": protocolMsg(&waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_SHARE.Enum(), AppStateSyncKeyShare: &waE2E.AppStateSyncKeyShare{},
		}),
		"edit that arrived without its body":             protocolMsg(&waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum()}),
		"revoke inside a disappearing-messages envelope": {EphemeralMessage: &waE2E.FutureProofMessage{Message: protocolMsg(revoke)}},

		// Nothing at all, or only carriers.
		"nil":                     nil,
		"empty":                   {},
		"metadata only":           {MessageContextInfo: &waE2E.MessageContextInfo{}},
		"sender key distribution": {SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{}},
		"fast-ratchet key distribution": {
			FastRatchetKeySenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
			MessageContextInfo:                         &waE2E.MessageContextInfo{},
		},
		"group root key share": {GroupRootKeyShare: &waE2E.GroupRootKeyShare{}},

		// Encrypted updates aimed at another message.
		"poll vote":                            {PollUpdateMessage: &waE2E.PollUpdateMessage{}},
		"reaction on a community announcement": {EncReactionMessage: &waE2E.EncReactionMessage{}},
		"event RSVP":                           {EncEventResponseMessage: &waE2E.EncEventResponseMessage{}},
		"encrypted edit":                       encryptedSecret(waE2E.SecretEncryptedMessage_MESSAGE_EDIT),
		"encrypted event edit":                 encryptedSecret(waE2E.SecretEncryptedMessage_EVENT_EDIT),
		"encrypted poll edit":                  encryptedSecret(waE2E.SecretEncryptedMessage_POLL_EDIT),
		"encrypted poll option":                encryptedSecret(waE2E.SecretEncryptedMessage_POLL_ADD_OPTION),

		// An empty reaction is how a reaction is withdrawn.
		"reaction withdrawn": {ReactionMessage: &waE2E.ReactionMessage{
			Key: &waCommon.MessageKey{ID: proto.String("reacted-1")}, Text: proto.String(""),
		}},
	}
}

func TestChatPreviewHidesMessagesWithNothingToRead(t *testing.T) {
	for name, msg := range previewHiddenCases() {
		t.Run(name, func(t *testing.T) {
			if !hiddenFromChatPreview(msg) {
				text, _ := extractContentFromProto(msg)
				t.Errorf("not hidden: the chat list would show %q as this chat's last message", text)
			}
		})
	}
}

// TestChatPreviewNeverHidesReadableContent ties the helper to the decoder: for
// everything it hides, the decoder stores nothing or a marker. If a decoder
// later learns to read one of these (decrypting votes, say), this fails and the
// hidden set gets revisited, instead of a readable message quietly staying out
// of the chat list.
func TestChatPreviewNeverHidesReadableContent(t *testing.T) {
	for name, msg := range previewHiddenCases() {
		text, _ := extractContentFromProto(msg)
		if text != "" && unsupportedRawType(text) == "" {
			t.Errorf("%s: hidden from the chat list, but the decoder reads it as %q", name, text)
		}
	}
}

// TestChatPreviewShowsEdits: an edit that carries its replacement body is
// resolved by unwrapEnvelope to the edited content, which is what the decoder
// stores, so it previews as the new text in every envelope it arrives in.
func TestChatPreviewShowsEdits(t *testing.T) {
	const body = "nos vemos a las 6, no a las 5"
	edit := &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), EditedMessage: convo(body)}
	cases := map[string]*waE2E.Message{
		"protocol edit":   protocolMsg(edit),
		"edited envelope": {EditedMessage: &waE2E.FutureProofMessage{Message: convo(body)}},
		"edit in a disappearing-messages chat": {
			EphemeralMessage: &waE2E.FutureProofMessage{Message: protocolMsg(edit)},
		},
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			if hiddenFromChatPreview(msg) {
				t.Error("an edit with its body was hidden; the chat list would keep showing the old text")
			}
			if text, _ := extractContentFromProto(msg); text != body {
				t.Errorf("what it would preview = %q, want the edited text %q", text, body)
			}
		})
	}
}

// TestChatPreviewShowsWhatAPersonReads: everything the decoder turns into
// something a person reads keeps moving the chat list, including a photo with
// no caption and a reaction.
func TestChatPreviewShowsWhatAPersonReads(t *testing.T) {
	cases := map[string]*waE2E.Message{
		"text":                  convo("hola"),
		"image without caption": {ImageMessage: &waE2E.ImageMessage{}},
		"reaction":              {ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("👍")}},
		"bank template":         {TemplateMessage: &waE2E.TemplateMessage{HydratedTemplate: bankAlert()}},
		"poll":                  {PollCreationMessageV3: &waE2E.PollCreationMessage{Name: proto.String("¿Vamos?")}},
		"album parent":          {AlbumMessage: &waE2E.AlbumMessage{ExpectedImageCount: proto.Uint32(3)}},
		"album child":           {AssociatedChildMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}}},
		"live location": {LiveLocationMessage: &waE2E.LiveLocationMessage{
			DegreesLatitude: proto.Float64(19.43), DegreesLongitude: proto.Float64(-99.13),
		}},
		"comment on a community announcement": {CommentMessage: &waE2E.CommentMessage{Message: convo("¡Felicidades!")}},
		// A carrier riding along must never hide the content it rides with.
		"text with key distribution riding along": {
			Conversation:                 proto.String("hola a todos"),
			SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
			MessageContextInfo:           &waE2E.MessageContextInfo{},
		},
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			if hiddenFromChatPreview(msg) {
				t.Error("readable content was hidden from the chat list")
			}
		})
	}
}

// TestChatPreviewShowsUndecodedTypes is the fail-loud half. Anything outside the
// hidden sets may be something a person did — pinned a message, kept one, asked
// for a phone number. Hiding it would make the chat look idle when it was not;
// its marker in the preview is the visible answer. The same holds for an
// encrypted message that may be NEW content rather than an update to another.
func TestChatPreviewShowsUndecodedTypes(t *testing.T) {
	cases := map[string]*waE2E.Message{
		"pin in chat":                          {PinInChatMessage: &waE2E.PinInChatMessage{}},
		"keep in chat":                         {KeepInChatMessage: &waE2E.KeepInChatMessage{}},
		"request phone number":                 {RequestPhoneNumberMessage: &waE2E.RequestPhoneNumberMessage{}},
		"encrypted comment, a new entry":       {EncCommentMessage: &waE2E.EncCommentMessage{}},
		"encrypted scheduled message":          encryptedSecret(waE2E.SecretEncryptedMessage_MESSAGE_SCHEDULE),
		"encrypted message of an unknown kind": encryptedSecret(waE2E.SecretEncryptedMessage_UNKNOWN),
		// One hidden field does not hide a message that also carries another.
		"vote riding with an unknown type": {
			PollUpdateMessage: &waE2E.PollUpdateMessage{},
			PinInChatMessage:  &waE2E.PinInChatMessage{},
		},
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			if hiddenFromChatPreview(msg) {
				t.Error("hidden from the chat list, but it may be something a person did")
			}
		})
	}
}

// TestChatPreviewAgreesWithTheDecoderOnFieldsNewerThanWhatsmeow: a field this
// whatsmeow version does not know is invisible to both protoreflect walks. The
// decoder stores a message made only of such fields as an empty row, and the
// helper must hide exactly that — and stop hiding it the day the decoder learns
// to mark it.
func TestChatPreviewAgreesWithTheDecoderOnFieldsNewerThanWhatsmeow(t *testing.T) {
	msg := &waE2E.Message{}
	// Field 999, length-delimited: a message type from a newer WhatsApp.
	raw := protowire.AppendTag(nil, 999, protowire.BytesType)
	raw = protowire.AppendBytes(raw, []byte{0x0a, 0x01, 'x'})
	msg.ProtoReflect().SetUnknown(raw)

	text, _ := extractContentFromProto(msg)
	if hidden := hiddenFromChatPreview(msg); hidden != (text == "") {
		t.Errorf("hiddenFromChatPreview = %v, but the decoder stored %q: the two disagree about this message", hidden, text)
	}
}
