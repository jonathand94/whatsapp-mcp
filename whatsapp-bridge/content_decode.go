// content_decode.go — envelope unwrapping and the second tier of content
// decoders, layered on top of extractContent (MYC-3284 follow-up).
//
// MYC-3284 made an undecodable message LOUD: instead of collapsing to an empty
// `system` row, it keeps a queryable "[unsupported: <rawType>]" marker naming
// the proto field. That fixed the visibility half of the bug — the loss is no
// longer silent.
//
// This file addresses the other half: several of the types now showing up as
// markers are not actually undecodable, they were simply never decoded. Read
// off the live bridge after MYC-3284 shipped:
//
//	[unsupported: albumMessage]           12
//	[unsupported: pollCreationMessageV3]   5
//	[unsupported: liveLocationMessage]     2
//	[unsupported: contactsArrayMessage]    1
//
// Each of those carries text a human reads in the chat. A marker is the right
// answer for a type we genuinely cannot read; it is the wrong answer for one we
// simply have not written a decoder for yet.
//
// The larger correctness gap is ENVELOPES. whatsmeow nests the real message
// inside a wrapper for disappearing messages, view-once, device-sent,
// document-with-caption and edits. extractContent tests only top-level fields,
// so a plain text message sent in a chat with disappearing messages enabled has
// no top-level `conversation` and falls through to the default branch. Today
// that stores "[unsupported: ephemeralMessage]" — visible, but the text itself
// is still not captured. Unwrapping first means such a message decodes as what
// it actually is.
//
// Nothing here weakens the fail-loud property. Unwrapping runs BEFORE the
// existing switch, the new decoders run AFTER it, and anything still unmatched
// falls through to the same default branch and the same marker as before.
//
// Types map onto the existing messages.type CHECK values (a poll is stored as
// `text`, a live location as `location`), so this needs no migration and no
// change to the CHECK constraint — deliberately, per the reasoning recorded in
// extractContent's default branch.
//
// A second pass came from the same signal three months on. /healthcheck's
// undecoded_by_type, which counts every marker row in the store, read on
// 2026-10-21:
//
//	[unsupported: protocolMessage]         3107
//	[unsupported: templateMessage]         1348
//	[unsupported: albumMessage]             949
//	[unsupported: associatedChildMessage]   546
//	[unsupported: secretEncryptedMessage]   316
//	[unsupported: pollUpdateMessage]        105
//	[unsupported: liveLocationMessage]       39
//	[unsupported: buttonsMessage]            37
//	[unsupported: commentMessage]            22
//
// Most of that volume was, again, envelopes and messages a person reads:
//
//   - associatedChildMessage is an ENVELOPE. Every photo and video of an album
//     arrives wrapped in one, so those pictures were stored as markers and got
//     no media keys either. Nineteen more envelope fields of this whatsmeow
//     version were missing as well: nothing compared unwrapEnvelope's list
//     against the proto, so each gap could only surface as markers.
//     TestEveryFutureProofEnvelopeIsUnwrapped now makes that comparison, and
//     fails on the whatsmeow bump that adds the next one.
//   - albumMessage is the album's parent: no text, only how many photos and
//     videos to expect. It now gets a summary line.
//   - templateMessage, buttonsMessage and interactiveMessage are how banks and
//     couriers send notifications. Their text is spread over a title, a body, a
//     footer and button labels.
//   - A live location without a caption decoded to empty text and so fell
//     through to the marker. Its coordinates are its content now.
//
// What stays a marker, on purpose: protocolMessage is plumbing (revokes,
// setting changes, key shares) with nothing to read, and pollUpdateMessage,
// encReactionMessage and secretEncryptedMessage are encrypted with the secret
// of the message they target, which only the whatsmeow client can open. Their
// markers keep /healthcheck honest; hiddenFromChatPreview keeps them out of the
// chat list instead.
//
// Rows already stored are not rewritten by any of this: the history backfill
// repairs EMPTY rows only, never marker rows. The counts above stop growing for
// the types decoded here; they do not fall.

package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// maxUnwrapDepth bounds envelope descent. Real nesting is one or two deep (an
// edit of a disappearing message); the bound is here so a malformed or hostile
// payload cannot spin the decoder.
const maxUnwrapDepth = 8

// unwrapEnvelope descends wrapper types that nest the real message and returns
// the innermost payload. Returns the original message unchanged when it is not
// an envelope, so callers can apply it unconditionally.
func unwrapEnvelope(m *waE2E.Message) *waE2E.Message {
	for depth := 0; m != nil && depth < maxUnwrapDepth; depth++ {
		inner := unwrapOnce(m)
		if inner == nil {
			return m
		}
		m = inner
	}
	return m
}

// unwrapOnce is one step of unwrapEnvelope: the message m wraps, or nil when
// m is not an envelope.
func unwrapOnce(m *waE2E.Message) *waE2E.Message {
	var inner *waE2E.Message
	switch {
	case m.GetEphemeralMessage().GetMessage() != nil:
		inner = m.GetEphemeralMessage().GetMessage()
	case m.GetViewOnceMessage().GetMessage() != nil:
		inner = m.GetViewOnceMessage().GetMessage()
	case m.GetViewOnceMessageV2().GetMessage() != nil:
		inner = m.GetViewOnceMessageV2().GetMessage()
	case m.GetViewOnceMessageV2Extension().GetMessage() != nil:
		inner = m.GetViewOnceMessageV2Extension().GetMessage()
	case m.GetDocumentWithCaptionMessage().GetMessage() != nil:
		inner = m.GetDocumentWithCaptionMessage().GetMessage()
	case m.GetLottieStickerMessage().GetMessage() != nil:
		inner = m.GetLottieStickerMessage().GetMessage()
	case m.GetDeviceSentMessage().GetMessage() != nil:
		inner = m.GetDeviceSentMessage().GetMessage()
	case m.GetEditedMessage().GetMessage() != nil:
		inner = m.GetEditedMessage().GetMessage()
	// An edit delivered as a protocol message carries the replacement body,
	// so the store ends up holding the edited text rather than a bare
	// protocol row.
	case m.GetProtocolMessage().GetEditedMessage() != nil:
		inner = m.GetProtocolMessage().GetEditedMessage()

	// Album children. Each photo or video of an album is sent as a message
	// of its own inside this envelope, which is how the client knows to
	// group it under the album's parent (albumMessage, decoded below). The
	// child IS the media: unwrapped, it is stored as the image or video it
	// is, and extractDownloadableFieldsFromProto — which unwraps first —
	// reaches its media keys as well.
	case m.GetAssociatedChildMessage().GetMessage() != nil:
		inner = m.GetAssociatedChildMessage().GetMessage()
	// The same parent/child arrangement for an event's cover image and for
	// the picture on a poll option.
	case m.GetEventCoverImage().GetMessage() != nil:
		inner = m.GetEventCoverImage().GetMessage()
	case m.GetPollCreationOptionImageMessage().GetMessage() != nil:
		inner = m.GetPollCreationOptionImageMessage().GetMessage()

	// Flags on an ordinary message: it mentions a group, invokes a bot, or
	// hides behind a spoiler. They change how the client presents the
	// message, not what it says. (whatsmeow strips botInvokeMessage itself
	// on live events; history sync delivers it raw.)
	case m.GetGroupMentionedMessage().GetMessage() != nil:
		inner = m.GetGroupMentionedMessage().GetMessage()
	case m.GetBotInvokeMessage().GetMessage() != nil:
		inner = m.GetBotInvokeMessage().GetMessage()
	case m.GetSpoilerMessage().GetMessage() != nil:
		inner = m.GetSpoilerMessage().GetMessage()

	// Status content delivered into a chat: a mention in someone's status,
	// a status posted to a group, an "add yours" prompt. What is wrapped is
	// the status itself.
	case m.GetStatusMentionMessage().GetMessage() != nil:
		inner = m.GetStatusMentionMessage().GetMessage()
	case m.GetGroupStatusMentionMessage().GetMessage() != nil:
		inner = m.GetGroupStatusMentionMessage().GetMessage()
	case m.GetGroupStatusMessage().GetMessage() != nil:
		inner = m.GetGroupStatusMessage().GetMessage()
	case m.GetGroupStatusMessageV2().GetMessage() != nil:
		inner = m.GetGroupStatusMessageV2().GetMessage()
	case m.GetStatusAddYours().GetMessage() != nil:
		inner = m.GetStatusAddYours().GetMessage()

	// Poll V4 is the one poll version typed as an envelope rather than as a
	// PollCreationMessage; whatever it carries goes on to the poll decoders
	// below, or to a marker named after the inner type.
	case m.GetPollCreationMessageV4().GetMessage() != nil:
		inner = m.GetPollCreationMessageV4().GetMessage()

	// Bot, channel-question, newsletter-admin and limit-sharing variants.
	// Typed FutureProofMessage like every case above, so their payload is a
	// complete message too.
	case m.GetBotTaskMessage().GetMessage() != nil:
		inner = m.GetBotTaskMessage().GetMessage()
	case m.GetBotForwardedMessage().GetMessage() != nil:
		inner = m.GetBotForwardedMessage().GetMessage()
	case m.GetBotPlatformRegistrationSuccessMessage().GetMessage() != nil:
		inner = m.GetBotPlatformRegistrationSuccessMessage().GetMessage()
	case m.GetQuestionMessage().GetMessage() != nil:
		inner = m.GetQuestionMessage().GetMessage()
	case m.GetQuestionReplyMessage().GetMessage() != nil:
		inner = m.GetQuestionReplyMessage().GetMessage()
	case m.GetNewsletterAdminProfileMessage().GetMessage() != nil:
		inner = m.GetNewsletterAdminProfileMessage().GetMessage()
	case m.GetNewsletterAdminProfileStatusMessage().GetMessage() != nil:
		inner = m.GetNewsletterAdminProfileStatusMessage().GetMessage()
	case m.GetLimitSharingMessage().GetMessage() != nil:
		inner = m.GetLimitSharingMessage().GetMessage()

	// A comment on a community announcement: the comment as a complete
	// message plus the key of the post it answers — the deviceSentMessage
	// shape. The store has no column for that link (a quoted reply loses
	// its quote the same way), so the comment is kept as what it says.
	case m.GetCommentMessage().GetMessage() != nil:
		inner = m.GetCommentMessage().GetMessage()
	}
	return inner
}

// envelopeUnknownFields names the fields this whatsmeow cannot parse
// (protobuf "unknown fields", "unknown_field_N") on m and on every message
// unwrapEnvelope passes through on the way to the payload: content from a
// newer WhatsApp. Only the messages themselves are examined, never the
// wrappers between them or a known content message's insides: a field
// WhatsApp adds to a wrapper or to an image is metadata about something
// already classified, while a field it adds to a message is a new kind of
// message. Walking the same path as unwrapEnvelope keeps isPlumbing, the
// decoder and hiddenFromChatPreview from ever disagreeing about one.
func envelopeUnknownFields(m *waE2E.Message) []string {
	var names []string
	seen := map[protowire.Number]bool{}
	// <=: unwrapEnvelope descends maxUnwrapDepth times, so the message it
	// returns is at that depth, and is checked too.
	for depth := 0; m != nil && depth <= maxUnwrapDepth; depth++ {
		names = append(names, unknownFieldNames(m.ProtoReflect(), seen)...)
		m = unwrapOnce(m)
	}
	return names
}

// carriesFields reports whether m, or a message along its envelope, has every
// one of the named fields set; unknown fields count under their
// "unknown_field_N" names, as in markers.
func carriesFields(m *waE2E.Message, names []string) bool {
	set := map[string]bool{}
	seen := map[protowire.Number]bool{}
	for depth := 0; m != nil && depth <= maxUnwrapDepth; depth++ {
		m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			set[string(fd.Name())] = true
			return true
		})
		for _, n := range unknownFieldNames(m.ProtoReflect(), seen) {
			set[n] = true
		}
		m = unwrapOnce(m)
	}
	for _, n := range names {
		if !set[strings.TrimSpace(n)] {
			return false
		}
	}
	return true
}

// unknownFieldNames names r's unknown fields by number, skipping numbers
// already in seen.
func unknownFieldNames(r protoreflect.Message, seen map[protowire.Number]bool) []string {
	var names []string
	for unknown := r.GetUnknown(); len(unknown) > 0; {
		num, typ, n := protowire.ConsumeTag(unknown)
		if n < 0 {
			break
		}
		unknown = unknown[n:]
		if n = protowire.ConsumeFieldValue(num, typ, unknown); n < 0 {
			break
		}
		unknown = unknown[n:]
		if !seen[num] {
			seen[num] = true
			names = append(names, fmt.Sprintf("unknown_field_%d", num))
		}
	}
	return names
}

// decodeExtraContent handles content types the main switch does not, returning
// ok=false when it has nothing to offer so the caller falls through to the
// unchanged fail-loud default.
//
// msgType values are restricted to those already permitted by the
// messages.type CHECK constraint.
func decodeExtraContent(m *waE2E.Message) (text, msgType string, ok bool) {
	if m == nil {
		return "", "", false
	}

	switch {
	// Polls. Six versions ship concurrently and all but V4 share this shape
	// (V4 is an envelope; see unwrapEnvelope). The question and its options
	// are text a human sees in the chat.
	case m.GetPollCreationMessage() != nil:
		return pollText(m.GetPollCreationMessage()), "text", true
	case m.GetPollCreationMessageV2() != nil:
		return pollText(m.GetPollCreationMessageV2()), "text", true
	case m.GetPollCreationMessageV3() != nil:
		return pollText(m.GetPollCreationMessageV3()), "text", true
	case m.GetPollCreationMessageV5() != nil:
		return pollText(m.GetPollCreationMessageV5()), "text", true
	case m.GetPollCreationMessageV6() != nil:
		return pollText(m.GetPollCreationMessageV6()), "text", true

	case m.GetEventMessage() != nil:
		return eventText(m.GetEventMessage()), "text", true

	case m.GetLiveLocationMessage() != nil:
		return liveLocationText(m.GetLiveLocationMessage()), "location", true

	case m.GetContactsArrayMessage() != nil:
		return contactsArrayText(m.GetContactsArrayMessage()), "contact", true

	// Album parent. It carries no text of its own, only how many photos and
	// videos the client should expect; the media arrive as separate children
	// (associatedChildMessage, unwrapped above). The summary is what tells a
	// reader that the pictures after it belong together. `system`, not
	// `text`: nobody typed it, and it is not a marker either, so it lands in
	// no /healthcheck counter.
	case m.GetAlbumMessage() != nil:
		return albumText(m.GetAlbumMessage()), "system", true

	case m.GetGroupInviteMessage() != nil:
		return joinNonEmpty("\n",
			m.GetGroupInviteMessage().GetGroupName(),
			m.GetGroupInviteMessage().GetCaption()), "text", true

	// Video note (the round "view once"-style video). Same payload as a video.
	case m.GetPtvMessage() != nil:
		return m.GetPtvMessage().GetCaption(), "video", true

	// Commerce and interactive. These render as text in the client, so the
	// text is what we store.
	case m.GetProductMessage() != nil:
		return productText(m.GetProductMessage()), "text", true
	case m.GetOrderMessage() != nil:
		return firstNonEmpty(m.GetOrderMessage().GetMessage(), m.GetOrderMessage().GetOrderTitle()), "text", true
	case m.GetListMessage() != nil:
		return firstNonEmpty(m.GetListMessage().GetDescription(), m.GetListMessage().GetTitle()), "text", true
	case m.GetListResponseMessage() != nil:
		return firstNonEmpty(m.GetListResponseMessage().GetTitle(), m.GetListResponseMessage().GetDescription()), "text", true
	// Business notifications — a bank's purchase alert, a courier's tracking
	// update — arrive as one of these three. What the client shows is a
	// title, a body, a footer and a row of buttons, so that is what is kept.
	case m.GetTemplateMessage() != nil:
		return templateText(m.GetTemplateMessage()), "text", true
	case m.GetInteractiveMessage() != nil:
		return interactiveText(m.GetInteractiveMessage()), "text", true
	case m.GetButtonsMessage() != nil:
		return buttonsText(m.GetButtonsMessage()), "text", true
	case m.GetButtonsResponseMessage() != nil:
		return m.GetButtonsResponseMessage().GetSelectedDisplayText(), "text", true
	case m.GetTemplateButtonReplyMessage() != nil:
		return m.GetTemplateButtonReplyMessage().GetSelectedDisplayText(), "text", true
	case m.GetInteractiveResponseMessage() != nil:
		return m.GetInteractiveResponseMessage().GetBody().GetText(), "text", true
	}

	return "", "", false
}

// --- per-type text builders ------------------------------------------------

// pollLabel heads a decoded poll so a reader of the chat list or the vault
// can tell a poll from a question somebody typed.
const pollLabel = "📊 Encuesta"

func pollText(p *waE2E.PollCreationMessage) string {
	if p == nil {
		return ""
	}
	var options []string
	for _, opt := range p.GetOptions() {
		if name := opt.GetOptionName(); name != "" {
			options = append(options, "- "+name)
		}
	}
	// Nothing to show must stay "" so the caller falls through to the marker
	// (TestEmptyPayloadOfDecodableTypeStillFailsLoud); the label alone is not
	// content.
	if p.GetName() == "" && len(options) == 0 {
		return ""
	}
	head := pollLabel
	if q := p.GetName(); q != "" {
		head += ": " + q
	}
	return strings.Join(append([]string{head}, options...), "\n")
}

func eventText(e *waE2E.EventMessage) string {
	if e == nil {
		return ""
	}
	parts := []string{e.GetName(), e.GetDescription(), e.GetLocation().GetName()}
	if e.GetIsCanceled() {
		parts = append(parts, "(cancelado)")
	}
	return joinNonEmpty("\n", parts...)
}

func productText(p *waE2E.ProductMessage) string {
	if p == nil {
		return ""
	}
	snap := p.GetProduct()
	return joinNonEmpty("\n", snap.GetTitle(), snap.GetDescription(), p.GetBody())
}

func contactsArrayText(c *waE2E.ContactsArrayMessage) string {
	if c == nil {
		return ""
	}
	names := []string{c.GetDisplayName()}
	for _, ct := range c.GetContacts() {
		names = append(names, ct.GetDisplayName())
	}
	return joinNonEmpty("\n", names...)
}

// liveLocationLabel heads a decoded live location. The row's type already says
// `location`, but the chat-list preview shows only the text.
const liveLocationLabel = "📍 Ubicación en tiempo real"

// liveLocationText keeps the coordinates. Unlike a pinned location, a live one
// rarely has a caption, and before the coordinates were kept a captionless one
// decoded to "" and fell through to the marker: the most common live location
// was the one stored as unreadable.
func liveLocationText(l *waE2E.LiveLocationMessage) string {
	if l == nil {
		return ""
	}
	// Presence, not value: 0,0 is a real place, and an unset coordinate is
	// not one.
	hasCoords := l.DegreesLatitude != nil || l.DegreesLongitude != nil
	if !hasCoords && l.GetCaption() == "" {
		return ""
	}
	head := liveLocationLabel
	if hasCoords {
		head += ": " + formatDegrees(l.GetDegreesLatitude()) + ", " + formatDegrees(l.GetDegreesLongitude())
	}
	return joinNonEmpty("\n", head, l.GetCaption())
}

// formatDegrees prints the shortest form that round-trips, so neither
// precision WhatsApp sent is lost nor precision it did not send is invented.
func formatDegrees(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// albumText summarizes an album parent. Never empty: even without counts the
// message is unambiguously an album, which is what a reader needs to know.
func albumText(a *waE2E.AlbumMessage) string {
	var counts []string
	if n := a.GetExpectedImageCount(); n > 0 {
		counts = append(counts, countOf(n, "foto", "fotos"))
	}
	if n := a.GetExpectedVideoCount(); n > 0 {
		counts = append(counts, countOf(n, "video", "videos"))
	}
	if len(counts) == 0 {
		return "[álbum]"
	}
	return "[álbum: " + strings.Join(counts, ", ") + "]"
}

func countOf(n uint32, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.FormatUint(uint64(n), 10) + " " + many
}

// templateText renders a business template as the client shows it.
//
// A template can hold the same content twice — the legacy hydratedTemplate
// slot beside the `format` oneof — so the first rendering that yields text
// wins instead of the two being joined into a duplicate.
func templateText(t *waE2E.TemplateMessage) string {
	if t == nil {
		return ""
	}
	return firstNonEmpty(
		hydratedTemplateText(t),
		fourRowTemplateText(t.GetFourRowTemplate()),
	)
}

// hydratedTemplateText covers the forms of a template that carry their text
// ready to show. It never descends into fourRowTemplate, which is what bounds
// the template -> HSM -> template recursion at one step without a counter.
func hydratedTemplateText(t *waE2E.TemplateMessage) string {
	return firstNonEmpty(
		hydratedFourRowText(t.GetHydratedTemplate()),
		hydratedFourRowText(t.GetHydratedFourRowTemplate()),
		interactiveText(t.GetInteractiveMessageTemplate()),
	)
}

func hydratedFourRowText(h *waE2E.TemplateMessage_HydratedFourRowTemplate) string {
	if h == nil {
		return ""
	}
	var labels []string
	for _, b := range h.GetHydratedButtons() {
		labels = append(labels, firstNonEmpty(
			b.GetQuickReplyButton().GetDisplayText(),
			b.GetUrlButton().GetDisplayText(),
			b.GetCallButton().GetDisplayText()))
	}
	// A media title (document, image, video, location) has no text of its
	// own; the body is what explains it.
	return joinNonEmpty("\n",
		h.GetHydratedTitleText(),
		h.GetHydratedContentText(),
		h.GetHydratedFooterText(),
		buttonRow(labels))
}

func fourRowTemplateText(f *waE2E.TemplateMessage_FourRowTemplate) string {
	if f == nil {
		return ""
	}
	var labels []string
	for _, b := range f.GetButtons() {
		labels = append(labels, firstNonEmpty(
			hsmText(b.GetQuickReplyButton().GetDisplayText()),
			hsmText(b.GetUrlButton().GetDisplayText()),
			hsmText(b.GetCallButton().GetDisplayText())))
	}
	return joinNonEmpty("\n",
		hsmText(f.GetHighlyStructuredMessage()),
		hsmText(f.GetContent()),
		hsmText(f.GetFooter()),
		buttonRow(labels))
}

// hsmText reads a legacy highly-structured message. An HSM is a REFERENCE to a
// template the client renders from its own template pack (namespace, element
// name, parameters), so on its own it holds no sentence to show; only its
// hydrated form does. Joining the bare parameters would store fragments like
// "1234 500" as if they were a message.
func hsmText(h *waE2E.HighlyStructuredMessage) string {
	if h.GetHydratedHsm() == nil {
		return ""
	}
	return hydratedTemplateText(h.GetHydratedHsm())
}

// interactiveText renders an interactive message, then each carousel card
// beneath it. Cards are rendered without their own carousels, which bounds the
// recursion at one level.
func interactiveText(im *waE2E.InteractiveMessage) string {
	if im == nil {
		return ""
	}
	parts := []string{interactiveCardText(im)}
	for _, card := range im.GetCarouselMessage().GetCards() {
		parts = append(parts, interactiveCardText(card))
	}
	return joinNonEmpty("\n", parts...)
}

func interactiveCardText(im *waE2E.InteractiveMessage) string {
	var labels []string
	for _, b := range im.GetNativeFlowMessage().GetButtons() {
		labels = append(labels, buttonParamsLabel(b.GetButtonParamsJSON()))
	}
	return joinNonEmpty("\n",
		im.GetHeader().GetTitle(),
		im.GetHeader().GetSubtitle(),
		im.GetBody().GetText(),
		im.GetFooter().GetText(),
		buttonRow(labels))
}

func buttonsText(b *waE2E.ButtonsMessage) string {
	if b == nil {
		return ""
	}
	var labels []string
	for _, btn := range b.GetButtons() {
		labels = append(labels, firstNonEmpty(
			btn.GetButtonText().GetDisplayText(),
			buttonParamsLabel(btn.GetNativeFlowInfo().GetParamsJSON())))
	}
	// GetText is the header when the header is text; a media header has no
	// text of its own.
	return joinNonEmpty("\n",
		b.GetText(),
		b.GetContentText(),
		b.GetFooterText(),
		buttonRow(labels))
}

// buttonParamsLabel recovers the label a person sees on a native-flow button.
// It is not a field of the button: the button's `name` is its machine kind
// ("cta_url", "quick_reply", "single_select") and is never shown, while the
// label travels inside the params JSON under a key that depends on that kind.
// Unparseable params yield "", which drops the button rather than the message.
func buttonParamsLabel(paramsJSON string) string {
	if paramsJSON == "" {
		return ""
	}
	var params map[string]any
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		return ""
	}
	for _, key := range []string{"display_text", "title", "flow_cta"} {
		if s, ok := params[key].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// buttonRow renders button labels on one line, bracketed the way the client
// draws them as separate buttons. Empty labels are skipped.
func buttonRow(labels []string) string {
	out := make([]string, 0, len(labels))
	for _, l := range labels {
		if l != "" {
			out = append(out, "["+l+"]")
		}
	}
	return strings.Join(out, " ")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func joinNonEmpty(sep string, vals ...string) string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if v != "" {
			out = append(out, v)
		}
	}
	return strings.Join(out, sep)
}

// --- chat-list preview -----------------------------------------------------

// hiddenFromChatPreview reports whether m must NOT become its chat's
// last-message preview, because it adds nothing to the conversation that a
// person could read. It decides the preview only: the message is stored, and
// counted by /healthcheck, exactly as before.
//
// Contract:
//
//   - Pass the message as received, wrapped or not. Envelopes are unwrapped
//     here exactly as extractContentFromProto unwraps them, so the answer
//     always describes the same payload the decoder stored.
//   - true for protocol messages: revokes, disappearing-message setting
//     changes, history-sync notifications, app-state key shares and requests,
//     peer-data operations and the rest of ProtocolMessage_Type. An EDIT is
//     the exception: when it carries its replacement body, unwrapEnvelope
//     resolves it to the edited content, so it previews as that content and
//     this returns false.
//   - true for nil and empty messages, and for messages made only of
//     key-distribution, sync and metadata carriers (messageContextInfo,
//     senderKeyDistributionMessage and the rest of carrierFields and
//     previewlessFields).
//   - true for updates that only change ANOTHER message and are encrypted with
//     that message's secret, so the bridge stores them as markers: a poll vote
//     (pollUpdateMessage), a reaction on a community announcement
//     (encReactionMessage), an event RSVP (encEventResponseMessage), an
//     encrypted edit (secretEncryptedMessage of an edit kind). Also a
//     reactionMessage with no emoji, which is how a reaction is withdrawn.
//   - false for everything else: decoded content, and every field the decoder
//     leaves undecoded and marks. An undecoded type may be something a person
//     did, and a chat list that silently stops moving would hide it; showing
//     its marker is the fail-loud answer.
//
// A message carrying several fields is hidden only if EVERY populated field is
// one of the above, so a carrier riding along with real content never hides it.
// Fields newer than this whatsmeow version, anywhere along the envelope, mean
// content from a newer WhatsApp: it is stored with an unknown_field_N marker
// and stays visible (envelopeUnknownFields).
func hiddenFromChatPreview(m *waE2E.Message) bool {
	if len(envelopeUnknownFields(m)) > 0 {
		return false
	}
	m = unwrapEnvelope(m)
	if m == nil {
		return true
	}
	hidden := true
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		hidden = previewless(m, string(fd.Name()))
		return hidden
	})
	return hidden
}

// previewlessFields are fields with nothing of their own to read, ON TOP of the
// decoder's carrierFields. They are kept apart because the two lists answer
// different questions: carrierFields decides that a row may be stored SILENT,
// which demands certainty, so these still store a marker and stay counted.
// Leaving them out of a chat-list preview costs nothing if one ever turns out
// to matter — the row is still there.
var previewlessFields = map[string]bool{
	// Key distribution and device-to-device sync.
	"fastRatchetKeySenderKeyDistributionMessage": true,
	"groupRootKeyShare":                          true,
	"rootSecretDistributeMessage":                true,
	"stickerSyncRmrMessage":                      true,
	// Still a protocol message after unwrapEnvelope means not an edit with a
	// body: that one was replaced by the edited content already.
	"protocolMessage": true,
	// Encrypted updates aimed at another message.
	"pollUpdateMessage":       true,
	"encReactionMessage":      true,
	"encEventResponseMessage": true,
}

// encryptedEditKinds are the secretEncryptedMessage kinds that edit an
// existing message. A scheduled message, or a kind added later, may be new
// content and stays visible.
var encryptedEditKinds = map[waE2E.SecretEncryptedMessage_SecretEncType]bool{
	waE2E.SecretEncryptedMessage_MESSAGE_EDIT:    true,
	waE2E.SecretEncryptedMessage_EVENT_EDIT:      true,
	waE2E.SecretEncryptedMessage_POLL_EDIT:       true,
	waE2E.SecretEncryptedMessage_POLL_ADD_OPTION: true,
}

func previewless(m *waE2E.Message, field string) bool {
	switch {
	case carrierFields[field], previewlessFields[field]:
		return true
	case field == "secretEncryptedMessage":
		return encryptedEditKinds[m.GetSecretEncryptedMessage().GetSecretEncType()]
	case field == "reactionMessage":
		return m.GetReactionMessage().GetText() == ""
	}
	return false
}
