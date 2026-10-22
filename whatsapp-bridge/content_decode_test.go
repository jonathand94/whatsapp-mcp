package main

import (
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func msgEvent(m *waE2E.Message) *events.Message { return &events.Message{Message: m} }

func convo(s string) *waE2E.Message {
	return &waE2E.Message{Conversation: proto.String(s)}
}

// --- envelopes -------------------------------------------------------------

// TestEnvelopeWrappedTextIsDecodedNotMarked is the regression this change
// exists for. Before unwrapping, a text message inside any of these wrappers
// matched no case and was stored as "[unsupported: <wrapper>]" — visible, per
// MYC-3284, but with the text itself never captured.
func TestEnvelopeWrappedTextIsDecodedNotMarked(t *testing.T) {
	const body = "the message a human can plainly read in the client"

	cases := []struct {
		name string
		msg  *waE2E.Message
	}{
		{"ephemeral (disappearing messages)", &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"view once", &waE2E.Message{ViewOnceMessage: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"view once v2", &waE2E.Message{ViewOnceMessageV2: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"view once v2 extension", &waE2E.Message{ViewOnceMessageV2Extension: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"document with caption", &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"device sent", &waE2E.Message{DeviceSentMessage: &waE2E.DeviceSentMessage{Message: convo(body)}}},
		{"edited", &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"protocol edit", &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{EditedMessage: convo(body)}}},
		// Envelopes the list above was missing. Every FutureProofMessage field
		// is covered by TestEveryFutureProofEnvelopeIsUnwrapped; these are the
		// ones a chat visibly shows, named so a failure reads as that case.
		{"bot invocation (raw in history sync)", &waE2E.Message{BotInvokeMessage: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"group mention", &waE2E.Message{GroupMentionedMessage: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"status mention", &waE2E.Message{StatusMentionMessage: &waE2E.FutureProofMessage{Message: convo(body)}}},
		{"spoiler", &waE2E.Message{SpoilerMessage: &waE2E.FutureProofMessage{Message: convo(body)}}},
		// Not a FutureProofMessage: the comment plus the key of the post it
		// answers, the deviceSentMessage shape.
		{"comment on a community announcement", &waE2E.Message{CommentMessage: &waE2E.CommentMessage{
			Message:          convo(body),
			TargetMessageKey: &waCommon.MessageKey{ID: proto.String("announcement-1")},
		}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, typ := extractContent(msgEvent(tc.msg))

			if unsupportedRawType(text) != "" {
				t.Fatalf("wrapped text was marked %q instead of decoded: the message is visible "+
					"but its text is still not captured", text)
			}
			if text != body {
				t.Errorf("text = %q, want %q", text, body)
			}
			if typ != "text" {
				t.Errorf("type = %q, want \"text\"", typ)
			}
		})
	}
}

// TestNestedEnvelopeUnwraps covers an edit of a disappearing message, the
// realistic two-deep case.
func TestNestedEnvelopeUnwraps(t *testing.T) {
	const body = "edited inside an ephemeral envelope"
	text, typ := extractContent(msgEvent(&waE2E.Message{
		EphemeralMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				EditedMessage: &waE2E.FutureProofMessage{Message: convo(body)},
			},
		},
	}))
	if text != body || typ != "text" {
		t.Errorf("got (%q, %q), want (%q, \"text\")", text, typ, body)
	}
}

// TestEnvelopeWrappedMediaKeepsItsType: unwrapping must not flatten a wrapped
// image into a bare text row.
func TestEnvelopeWrappedMediaKeepsItsType(t *testing.T) {
	const caption = "caption inside a view-once envelope"
	text, typ := extractContent(msgEvent(&waE2E.Message{
		ViewOnceMessageV2: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				ImageMessage: &waE2E.ImageMessage{Caption: proto.String(caption)},
			},
		},
	}))
	if typ != "image" {
		t.Errorf("type = %q, want \"image\"", typ)
	}
	if text != caption {
		t.Errorf("text = %q, want %q", text, caption)
	}
}

// TestEmptyEnvelopeStillFailsLoud: a wrapper with nothing inside must not
// become a silent empty row just because unwrapping ran.
func TestEmptyEnvelopeStillFailsLoud(t *testing.T) {
	text, _ := extractContent(msgEvent(&waE2E.Message{
		EphemeralMessage: &waE2E.FutureProofMessage{},
	}))
	if text == "" {
		t.Fatal("an empty envelope collapsed to a silent empty row — the MYC-3284 " +
			"fail-loud property must survive unwrapping")
	}
}

// TestAlbumChildIsStoredAsItsMediaWithKeys: every photo and video of an album
// arrives inside associatedChildMessage. Before that envelope was unwrapped,
// each one was stored as "[unsupported: associatedChildMessage]" (546 on the
// live bridge) and — because the media extractor unwraps with the same
// function — got no media keys either, so not even download_media could
// recover the picture later.
func TestAlbumChildIsStoredAsItsMediaWithKeys(t *testing.T) {
	key := []byte("album-child-media-key")
	cases := []struct {
		name     string
		child    *waE2E.Message
		wantType string
		wantText string
	}{
		{
			name: "photo",
			child: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
				Caption: proto.String("la cascada"), Mimetype: proto.String("image/jpeg"),
				MediaKey: key, DirectPath: proto.String("/v/t62.7118-24/album-1"),
			}},
			wantType: "image",
			wantText: "la cascada",
		},
		{
			name: "video",
			child: &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
				Mimetype: proto.String("video/mp4"),
				MediaKey: key, DirectPath: proto.String("/v/t62.7161-24/album-2"),
			}},
			wantType: "video",
			wantText: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &waE2E.Message{AssociatedChildMessage: &waE2E.FutureProofMessage{Message: tc.child}}

			text, typ := extractContentFromProto(msg)
			if raw := unsupportedRawType(text); raw != "" {
				t.Fatalf("album child stored as marker %q instead of as its media", text)
			}
			if typ != tc.wantType || text != tc.wantText {
				t.Errorf("got (%q, %q), want (%q, %q)", text, typ, tc.wantText, tc.wantType)
			}

			fields, ok := extractDownloadableFieldsFromProto(msg)
			if !ok {
				t.Fatal("album child has no media fields: the picture could never be downloaded")
			}
			if string(fields.MediaKey) != string(key) {
				t.Errorf("media key not carried: %q", fields.MediaKey)
			}
			if !fields.MediaDirectPath.Valid {
				t.Error("direct path not carried")
			}
		})
	}
}

// TestEveryFutureProofEnvelopeIsUnwrapped is the guard for how
// associatedChildMessage went unnoticed, along with nineteen other envelopes:
// nothing compared unwrapEnvelope's list against the proto, so the store found
// out one marker at a time. A field typed FutureProofMessage holds, by that
// type, a complete message and nothing else, so every one must unwrap. A
// whatsmeow bump that adds another fails here and names it.
func TestEveryFutureProofEnvelopeIsUnwrapped(t *testing.T) {
	envelopeType := (&waE2E.FutureProofMessage{}).ProtoReflect().Descriptor().FullName()
	fields := (&waE2E.Message{}).ProtoReflect().Descriptor().Fields()

	checked := 0
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Kind() != protoreflect.MessageKind || fd.Message().FullName() != envelopeType {
			continue
		}
		checked++

		inner := convo("payload of " + string(fd.Name()))
		outer := &waE2E.Message{}
		envelope := &waE2E.FutureProofMessage{Message: inner}
		outer.ProtoReflect().Set(fd, protoreflect.ValueOfMessage(envelope.ProtoReflect()))

		if got := unwrapEnvelope(outer); got != inner {
			t.Errorf("%s is a FutureProofMessage envelope that unwrapEnvelope does not descend: whatever it "+
				"carries is stored as \"[unsupported: %s]\" instead of as what it says — add a case for it",
				fd.Name(), fd.Name())
		}
	}
	if checked == 0 {
		t.Fatal("found no FutureProofMessage fields on waE2E.Message: this guard is checking nothing")
	}
}

// --- second-tier decoders --------------------------------------------------

// TestPreviouslyMarkedTypesNowDecode covers the types observed as live markers
// on the bridge after MYC-3284 shipped. Each carries text a human reads in the
// chat, so a marker was the wrong answer for them.
func TestPreviouslyMarkedTypesNowDecode(t *testing.T) {
	cases := []struct {
		name     string
		msg      *waE2E.Message
		wantType string
		wantSubs []string
	}{
		{
			name: "poll v3 (seen live)",
			msg: &waE2E.Message{PollCreationMessageV3: &waE2E.PollCreationMessage{
				Name: proto.String("Ship it?"),
				Options: []*waE2E.PollCreationMessage_Option{
					{OptionName: proto.String("Yes")}, {OptionName: proto.String("No")},
				},
			}},
			wantType: "text",
			wantSubs: []string{"Ship it?", "Yes", "No"},
		},
		{
			name:     "live location (seen live)",
			msg:      &waE2E.Message{LiveLocationMessage: &waE2E.LiveLocationMessage{Caption: proto.String("on my way")}},
			wantType: "location",
			wantSubs: []string{"on my way"},
		},
		{
			name: "contacts array (seen live)",
			msg: &waE2E.Message{ContactsArrayMessage: &waE2E.ContactsArrayMessage{
				DisplayName: proto.String("Team"),
				Contacts:    []*waE2E.ContactMessage{{DisplayName: proto.String("Ada")}},
			}},
			wantType: "contact",
			wantSubs: []string{"Team", "Ada"},
		},
		{
			name: "event",
			msg: &waE2E.Message{EventMessage: &waE2E.EventMessage{
				Name: proto.String("Launch review"), Description: proto.String("Go/no-go"),
			}},
			wantType: "text",
			wantSubs: []string{"Launch review", "Go/no-go"},
		},
		{
			name:     "group invite",
			msg:      &waE2E.Message{GroupInviteMessage: &waE2E.GroupInviteMessage{GroupName: proto.String("Founders")}},
			wantType: "text",
			wantSubs: []string{"Founders"},
		},
		{
			name:     "video note",
			msg:      &waE2E.Message{PtvMessage: &waE2E.VideoMessage{Caption: proto.String("quick take")}},
			wantType: "video",
			wantSubs: []string{"quick take"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, typ := extractContent(msgEvent(tc.msg))
			if unsupportedRawType(text) != "" {
				t.Fatalf("still marked unsupported (%q); a decoder exists for this type now", text)
			}
			if typ != tc.wantType {
				t.Errorf("type = %q, want %q", typ, tc.wantType)
			}
			for _, sub := range tc.wantSubs {
				if !strings.Contains(text, sub) {
					t.Errorf("text %q missing %q", text, sub)
				}
			}
		})
	}
}

// TestAlbumParentSummarizesItsExpectedMedia: the album's parent carries no text,
// only how many photos and videos to expect; 949 of them sat as markers on the
// live bridge. It must read as an album, and must land in NO /healthcheck
// counter: it is neither undecoded nor a silent empty row.
func TestAlbumParentSummarizesItsExpectedMedia(t *testing.T) {
	cases := []struct {
		name           string
		images, videos uint32
		want           string
	}{
		{"photos and a video", 3, 1, "[álbum: 3 fotos, 1 video]"},
		{"one photo", 1, 0, "[álbum: 1 foto]"},
		{"videos only", 0, 2, "[álbum: 2 videos]"},
		{"no counts still reads as an album", 0, 0, "[álbum]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			album := &waE2E.AlbumMessage{}
			if tc.images > 0 {
				album.ExpectedImageCount = proto.Uint32(tc.images)
			}
			if tc.videos > 0 {
				album.ExpectedVideoCount = proto.Uint32(tc.videos)
			}

			text, typ := extractContent(msgEvent(&waE2E.Message{AlbumMessage: album}))
			if text != tc.want {
				t.Errorf("text = %q, want %q", text, tc.want)
			}
			if typ != "system" {
				t.Errorf("type = %q, want \"system\": nobody typed this line", typ)
			}
			if rt := rawTypeForStorage(typ, text); rt != "" {
				t.Errorf("the album summary would be counted by /healthcheck as %q, but it was decoded", rt)
			}
		})
	}
}

// --- business messages -----------------------------------------------------

func hydratedQuickReply(label string) *waE2E.HydratedTemplateButton {
	return &waE2E.HydratedTemplateButton{HydratedButton: &waE2E.HydratedTemplateButton_QuickReplyButton{
		QuickReplyButton: &waE2E.HydratedTemplateButton_HydratedQuickReplyButton{DisplayText: proto.String(label), ID: proto.String("id-" + label)},
	}}
}

func hydratedURLButton(label string) *waE2E.HydratedTemplateButton {
	return &waE2E.HydratedTemplateButton{HydratedButton: &waE2E.HydratedTemplateButton_UrlButton{
		UrlButton: &waE2E.HydratedTemplateButton_HydratedURLButton{DisplayText: proto.String(label), URL: proto.String("https://example.com/r")},
	}}
}

// bankAlert is the shape of a bank's purchase notification.
func bankAlert() *waE2E.TemplateMessage_HydratedFourRowTemplate {
	return &waE2E.TemplateMessage_HydratedFourRowTemplate{
		Title:               &waE2E.TemplateMessage_HydratedFourRowTemplate_HydratedTitleText{HydratedTitleText: "Compra aprobada"},
		HydratedContentText: proto.String("Se realizó un cargo por $1,234.00 MXN con tu tarjeta terminación 1234."),
		HydratedFooterText:  proto.String("Banco Ejemplo"),
		HydratedButtons:     []*waE2E.HydratedTemplateButton{hydratedQuickReply("No reconozco este cargo"), hydratedURLButton("Ver detalle")},
	}
}

func nativeFlowButton(kind, paramsJSON string) *waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton {
	return &waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{Name: proto.String(kind), ButtonParamsJSON: proto.String(paramsJSON)}
}

func withNativeFlow(im *waE2E.InteractiveMessage, buttons ...*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton) *waE2E.InteractiveMessage {
	im.InteractiveMessage = &waE2E.InteractiveMessage_NativeFlowMessage_{
		NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{Buttons: buttons},
	}
	return im
}

// TestBusinessMessagesReadAsTheClientShowsThem: banks and couriers notify
// through templates, buttons and interactive messages; 1348 templates alone sat
// as markers on the live bridge. The pinned strings are the contract: title,
// body and footer as a person reads them, then the buttons, bracketed on one
// line the way the client draws them.
func TestBusinessMessagesReadAsTheClientShowsThem(t *testing.T) {
	cases := []struct {
		name string
		msg  *waE2E.Message
		want string
	}{
		{
			name: "bank alert as a hydrated template",
			msg:  &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{HydratedTemplate: bankAlert()}},
			want: "Compra aprobada\nSe realizó un cargo por $1,234.00 MXN con tu tarjeta terminación 1234.\nBanco Ejemplo\n[No reconozco este cargo] [Ver detalle]",
		},
		{
			name: "courier update as a hydrated four-row template",
			msg: &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
				Format: &waE2E.TemplateMessage_HydratedFourRowTemplate_{HydratedFourRowTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
					HydratedContentText: proto.String("Tu paquete MX123456789 va en camino. Llega el jueves."),
					HydratedButtons:     []*waE2E.HydratedTemplateButton{hydratedURLButton("Rastrear envío")},
				}},
			}},
			want: "Tu paquete MX123456789 va en camino. Llega el jueves.\n[Rastrear envío]",
		},
		{
			name: "legacy four-row template with a hydrated HSM",
			msg: &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
				Format: &waE2E.TemplateMessage_FourRowTemplate_{FourRowTemplate: &waE2E.TemplateMessage_FourRowTemplate{
					Content: &waE2E.HighlyStructuredMessage{HydratedHsm: &waE2E.TemplateMessage{
						HydratedTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
							HydratedContentText: proto.String("Tu cita es mañana a las 10:00."),
						},
					}},
				}},
			}},
			want: "Tu cita es mañana a las 10:00.",
		},
		{
			name: "interactive message template",
			msg: &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
				Format: &waE2E.TemplateMessage_InteractiveMessageTemplate{InteractiveMessageTemplate: withNativeFlow(&waE2E.InteractiveMessage{
					Body: &waE2E.InteractiveMessage_Body{Text: proto.String("Tu pago de $450.00 fue recibido.")},
				}, nativeFlowButton("cta_url", `{"display_text":"Ver recibo","url":"https://example.com/r"}`))},
			}},
			want: "Tu pago de $450.00 fue recibido.\n[Ver recibo]",
		},
		{
			name: "buttons message",
			msg: &waE2E.Message{ButtonsMessage: &waE2E.ButtonsMessage{
				Header:      &waE2E.ButtonsMessage_Text{Text: "Confirma tu cita"},
				ContentText: proto.String("Lunes 10:00 con la Dra. López"),
				FooterText:  proto.String("Clínica Ejemplo"),
				Buttons: []*waE2E.ButtonsMessage_Button{
					{ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("Confirmar")}},
					{ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("Cancelar")}},
				},
			}},
			want: "Confirma tu cita\nLunes 10:00 con la Dra. López\nClínica Ejemplo\n[Confirmar] [Cancelar]",
		},
		{
			name: "interactive message with native-flow buttons",
			msg: &waE2E.Message{InteractiveMessage: withNativeFlow(&waE2E.InteractiveMessage{
				Header: &waE2E.InteractiveMessage_Header{Title: proto.String("Pedido enviado")},
				Body:   &waE2E.InteractiveMessage_Body{Text: proto.String("Tu pedido #4521 salió del almacén.")},
				Footer: &waE2E.InteractiveMessage_Footer{Text: proto.String("Paquetería Ejemplo")},
			},
				nativeFlowButton("cta_url", `{"display_text":"Rastrear","url":"https://example.com/t/4521"}`),
				nativeFlowButton("quick_reply", `{"display_text":"Hablar con un agente","id":"agent"}`),
			)},
			want: "Pedido enviado\nTu pedido #4521 salió del almacén.\nPaquetería Ejemplo\n[Rastrear] [Hablar con un agente]",
		},
		{
			name: "carousel: the message, then each card",
			msg: &waE2E.Message{InteractiveMessage: &waE2E.InteractiveMessage{
				Body: &waE2E.InteractiveMessage_Body{Text: proto.String("Elige tu plan")},
				InteractiveMessage: &waE2E.InteractiveMessage_CarouselMessage_{CarouselMessage: &waE2E.InteractiveMessage_CarouselMessage{
					Cards: []*waE2E.InteractiveMessage{
						withNativeFlow(&waE2E.InteractiveMessage{
							Header: &waE2E.InteractiveMessage_Header{Title: proto.String("Básico")},
							Body:   &waE2E.InteractiveMessage_Body{Text: proto.String("$99 al mes")},
						}, nativeFlowButton("quick_reply", `{"display_text":"Elegir básico","id":"basic"}`)),
						{
							Header: &waE2E.InteractiveMessage_Header{Title: proto.String("Plus")},
							Body:   &waE2E.InteractiveMessage_Body{Text: proto.String("$199 al mes")},
						},
					},
				}},
			}},
			want: "Elige tu plan\nBásico\n$99 al mes\n[Elegir básico]\nPlus\n$199 al mes",
		},
		{
			name: "template button reply",
			msg:  &waE2E.Message{TemplateButtonReplyMessage: &waE2E.TemplateButtonReplyMessage{SelectedDisplayText: proto.String("No reconozco este cargo")}},
			want: "No reconozco este cargo",
		},
		{
			name: "buttons response",
			msg: &waE2E.Message{ButtonsResponseMessage: &waE2E.ButtonsResponseMessage{
				Response: &waE2E.ButtonsResponseMessage_SelectedDisplayText{SelectedDisplayText: "Confirmar"},
			}},
			want: "Confirmar",
		},
		{
			name: "interactive response",
			msg: &waE2E.Message{InteractiveResponseMessage: &waE2E.InteractiveResponseMessage{
				Body: &waE2E.InteractiveResponseMessage_Body{Text: proto.String("Hablar con un agente")},
			}},
			want: "Hablar con un agente",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, typ := extractContent(msgEvent(tc.msg))
			if raw := unsupportedRawType(text); raw != "" {
				t.Fatalf("still marked unsupported (%q): the notification text is not captured", text)
			}
			if typ != "text" {
				t.Errorf("type = %q, want \"text\"", typ)
			}
			if text != tc.want {
				t.Errorf("text:\n got %q\nwant %q", text, tc.want)
			}
		})
	}
}

// TestTemplateCarriedTwiceReadsOnce: a template can carry the same content in
// the legacy hydratedTemplate slot AND in the format oneof. Joining both would
// store every bank alert twice over.
func TestTemplateCarriedTwiceReadsOnce(t *testing.T) {
	text, _ := extractContent(msgEvent(&waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
		HydratedTemplate: bankAlert(),
		Format:           &waE2E.TemplateMessage_HydratedFourRowTemplate_{HydratedFourRowTemplate: bankAlert()},
	}}))
	if n := strings.Count(text, "Compra aprobada"); n != 1 {
		t.Errorf("the template's title appears %d times, want 1: %q", n, text)
	}
}

// TestNativeFlowButtonShowsItsLabelNotItsKind: a native-flow button's `name` is
// its machine kind ("cta_copy", "single_select") and never appears on screen;
// the label travels inside its params JSON. A button whose params cannot be
// parsed costs that one button, never the message.
func TestNativeFlowButtonShowsItsLabelNotItsKind(t *testing.T) {
	text, typ := extractContent(msgEvent(&waE2E.Message{InteractiveMessage: withNativeFlow(&waE2E.InteractiveMessage{
		Body: &waE2E.InteractiveMessage_Body{Text: proto.String("Tu código de verificación está listo.")},
	},
		nativeFlowButton("cta_copy", `{"display_text":"Copiar código","copy_code":"482913"}`),
		nativeFlowButton("single_select", `{"title":"Ver opciones","sections":[]}`),
		nativeFlowButton("cta_url", `{not json`),
	)}))

	const want = "Tu código de verificación está listo.\n[Copiar código] [Ver opciones]"
	if text != want || typ != "text" {
		t.Errorf("got (%q, %q), want (%q, \"text\")", text, typ, want)
	}
	for _, kind := range []string{"cta_copy", "single_select", "cta_url"} {
		if strings.Contains(text, kind) {
			t.Errorf("machine kind %q leaked into the stored text: %q", kind, text)
		}
	}
}

// TestLiveLocationKeepsItsCoordinates: a live location rarely has a caption, and
// before its coordinates were kept a captionless one decoded to "" and fell
// through to the marker — the common case was the one stored as unreadable.
func TestLiveLocationKeepsItsCoordinates(t *testing.T) {
	lat, lng := proto.Float64(19.4326077), proto.Float64(-99.133208)
	cases := []struct {
		name string
		loc  *waE2E.LiveLocationMessage
		want string
	}{
		{
			name: "coordinates and caption",
			loc:  &waE2E.LiveLocationMessage{DegreesLatitude: lat, DegreesLongitude: lng, Caption: proto.String("voy en camino")},
			want: "📍 Ubicación en tiempo real: 19.4326077, -99.133208\nvoy en camino",
		},
		{
			name: "coordinates only, the common case",
			loc:  &waE2E.LiveLocationMessage{DegreesLatitude: lat, DegreesLongitude: lng},
			want: "📍 Ubicación en tiempo real: 19.4326077, -99.133208",
		},
		{
			name: "caption only",
			loc:  &waE2E.LiveLocationMessage{Caption: proto.String("aquí estoy")},
			want: "📍 Ubicación en tiempo real\naquí estoy",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, typ := extractContent(msgEvent(&waE2E.Message{LiveLocationMessage: tc.loc}))
			if typ != "location" {
				t.Errorf("type = %q, want \"location\"", typ)
			}
			if text != tc.want {
				t.Errorf("text = %q, want %q", text, tc.want)
			}
		})
	}
}

// TestPollReadsAsAPoll pins the poll text in every version. Without the label,
// a poll's question reads in the chat list and in the vault exactly like a
// question somebody typed, and its options like a list typed under it.
func TestPollReadsAsAPoll(t *testing.T) {
	poll := func() *waE2E.PollCreationMessage {
		return &waE2E.PollCreationMessage{
			Name: proto.String("¿Qué día nos reunimos?"),
			Options: []*waE2E.PollCreationMessage_Option{
				{OptionName: proto.String("jueves")}, {OptionName: proto.String("")}, {OptionName: proto.String("viernes")},
			},
		}
	}
	const want = "📊 Encuesta: ¿Qué día nos reunimos?\n- jueves\n- viernes"

	cases := map[string]*waE2E.Message{
		"v1": {PollCreationMessage: poll()},
		"v2": {PollCreationMessageV2: poll()},
		"v3": {PollCreationMessageV3: poll()},
		"v5": {PollCreationMessageV5: poll()},
		"v6": {PollCreationMessageV6: poll()},
		// V4 is an envelope; what it carries reaches the same decoder.
		"v4 envelope": {PollCreationMessageV4: &waE2E.FutureProofMessage{Message: &waE2E.Message{PollCreationMessageV3: poll()}}},
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			text, typ := extractContent(msgEvent(msg))
			if text != want || typ != "text" {
				t.Errorf("got (%q, %q), want (%q, \"text\")", text, typ, want)
			}
		})
	}

	t.Run("options without a question", func(t *testing.T) {
		p := poll()
		p.Name = nil
		text, _ := extractContent(msgEvent(&waE2E.Message{PollCreationMessage: p}))
		if want := "📊 Encuesta\n- jueves\n- viernes"; text != want {
			t.Errorf("text = %q, want %q", text, want)
		}
	})
}

// TestContactsArrayListsEveryContact pins the format: the card's own display
// name, then each contact's, one per line — contactMessage's rule (display name
// only) applied to every contact on the card.
func TestContactsArrayListsEveryContact(t *testing.T) {
	text, typ := extractContent(msgEvent(&waE2E.Message{ContactsArrayMessage: &waE2E.ContactsArrayMessage{
		DisplayName: proto.String("3 contactos"),
		Contacts: []*waE2E.ContactMessage{
			{DisplayName: proto.String("Ana López")},
			{DisplayName: proto.String("Pedro Pérez")},
			{Vcard: proto.String("BEGIN:VCARD\nVERSION:3.0\nEND:VCARD")},
		},
	}}))
	const want = "3 contactos\nAna López\nPedro Pérez"
	if text != want || typ != "contact" {
		t.Errorf("got (%q, %q), want (%q, \"contact\")", text, typ, want)
	}
}

// TestDecodedTypesUseAllowedCheckValues guards the constraint the whole design
// leans on: these decoders deliberately map onto messages.type values already
// permitted by the CHECK in migrations/001, so no table rebuild is needed. A
// new type value here would be rejected at INSERT time in production while
// every unit test still passed.
func TestDecodedTypesUseAllowedCheckValues(t *testing.T) {
	allowed := map[string]bool{
		"text": true, "image": true, "video": true, "audio": true, "voice": true,
		"document": true, "sticker": true, "location": true, "contact": true,
		"call": true, "system": true, "reaction": true,
	}
	probes := []*waE2E.Message{
		{PollCreationMessage: &waE2E.PollCreationMessage{Name: proto.String("q")}},
		{PollCreationMessageV2: &waE2E.PollCreationMessage{Name: proto.String("q")}},
		{PollCreationMessageV3: &waE2E.PollCreationMessage{Name: proto.String("q")}},
		{PollCreationMessageV5: &waE2E.PollCreationMessage{Name: proto.String("q")}},
		{PollCreationMessageV6: &waE2E.PollCreationMessage{Name: proto.String("q")}},
		{EventMessage: &waE2E.EventMessage{Name: proto.String("e")}},
		{LiveLocationMessage: &waE2E.LiveLocationMessage{}},
		{ContactsArrayMessage: &waE2E.ContactsArrayMessage{}},
		{GroupInviteMessage: &waE2E.GroupInviteMessage{}},
		{PtvMessage: &waE2E.VideoMessage{}},
		{ProductMessage: &waE2E.ProductMessage{}},
		{OrderMessage: &waE2E.OrderMessage{}},
		{ListMessage: &waE2E.ListMessage{}},
		{ListResponseMessage: &waE2E.ListResponseMessage{}},
		{ButtonsMessage: &waE2E.ButtonsMessage{}},
		{ButtonsResponseMessage: &waE2E.ButtonsResponseMessage{}},
		{TemplateButtonReplyMessage: &waE2E.TemplateButtonReplyMessage{}},
		{InteractiveResponseMessage: &waE2E.InteractiveResponseMessage{}},
		{TemplateMessage: &waE2E.TemplateMessage{}},
		{InteractiveMessage: &waE2E.InteractiveMessage{}},
		{AlbumMessage: &waE2E.AlbumMessage{}},
	}
	for _, p := range probes {
		_, typ, ok := decodeExtraContent(p)
		if !ok {
			t.Errorf("decodeExtraContent returned ok=false for %T probe", p)
			continue
		}
		if !allowed[typ] {
			t.Errorf("type %q is not in the messages.type CHECK allow-list; "+
				"production INSERTs would fail while unit tests pass", typ)
		}
	}
}

// --- fail-loud must survive ------------------------------------------------

// TestUnknownTypeStillFailsLoud is the negative control carried over from
// MYC-3284: a type NEITHER tier decodes must still keep its marker. This is the
// property most at risk from adding decode tiers, so it is asserted here too
// rather than assumed from the older test file.
func TestUnknownTypeStillFailsLoud(t *testing.T) {
	cases := []struct {
		name    string
		msg     *waE2E.Message
		rawType string
	}{
		{"encrypted reaction", &waE2E.Message{EncReactionMessage: &waE2E.EncReactionMessage{}}, "encReactionMessage"},
		{"encrypted poll vote", &waE2E.Message{PollUpdateMessage: &waE2E.PollUpdateMessage{}}, "pollUpdateMessage"},
		{"request phone number", &waE2E.Message{RequestPhoneNumberMessage: &waE2E.RequestPhoneNumberMessage{}}, "requestPhoneNumberMessage"},
		// Encrypted with the target message's secret, like the vote and the
		// reaction above; only the whatsmeow client holds the key.
		{"encrypted edit", &waE2E.Message{SecretEncryptedMessage: &waE2E.SecretEncryptedMessage{
			SecretEncType: waE2E.SecretEncryptedMessage_MESSAGE_EDIT.Enum(),
		}}, "secretEncryptedMessage"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, typ := extractContent(msgEvent(tc.msg))
			if text == "" {
				t.Fatalf("SILENT DROP: %s produced an empty row; the fail-loud property "+
					"from MYC-3284 was lost", tc.rawType)
			}
			if got := unsupportedRawType(text); got != tc.rawType {
				t.Errorf("marker raw type = %q, want %q", got, tc.rawType)
			}
			if typ != "system" {
				t.Errorf("type = %q, want \"system\"", typ)
			}
		})
	}
}

// TestPollVoteKeepsItsMarker pins a decision, not an accident. A vote is
// encrypted with its poll's secret, so this decoder cannot tell which option was
// chosen. Storing a friendlier "[voto en una encuesta]" was considered and
// rejected: raw_type is derived from content_text (rawTypeForStorage), so any
// text that is not a marker takes the row out of undecoded_by_type — while the
// votes already stored keep their markers, because the backfill never rewrites
// a marker. The counter would freeze and claim votes are read when none are.
// The marker keeps /healthcheck honest; hiddenFromChatPreview keeps it out of
// the chat list, which was the visible complaint.
func TestPollVoteKeepsItsMarker(t *testing.T) {
	vote := &waE2E.Message{PollUpdateMessage: &waE2E.PollUpdateMessage{
		PollCreationMessageKey: &waCommon.MessageKey{ID: proto.String("poll-1")},
		Vote:                   &waE2E.PollEncValue{EncPayload: []byte{1, 2, 3}, EncIV: []byte{4, 5, 6}},
	}}

	text, typ := extractContent(msgEvent(vote))
	if got := unsupportedRawType(text); got != "pollUpdateMessage" {
		t.Fatalf("a vote this decoder cannot read must keep its marker; got %q", text)
	}
	if rt := rawTypeForStorage(typ, text); rt != "pollUpdateMessage" {
		t.Errorf("raw_type = %q: the vote would drop out of /healthcheck's undecoded_by_type while still unread", rt)
	}
	if !hiddenFromChatPreview(vote) {
		t.Error("the vote's marker would replace its chat's preview in the chat list")
	}
}

// TestCarrierOnlyMessageStaysSilent: group key rotation carries no user text
// and must not gain a placeholder just because new tiers were added.
func TestCarrierOnlyMessageStaysSilent(t *testing.T) {
	text, typ := extractContent(msgEvent(&waE2E.Message{
		SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
	}))
	if text != "" {
		t.Errorf("text = %q, want empty: a protocol carrier must not write vault noise", text)
	}
	if typ != "system" {
		t.Errorf("type = %q, want \"system\"", typ)
	}
}

// TestRealContentBeatsCarrier: senderKeyDistributionMessage rides along with
// ordinary group messages, so it must never mask the real payload.
func TestRealContentBeatsCarrier(t *testing.T) {
	const body = "group message with a key distribution riding along"
	text, typ := extractContent(msgEvent(&waE2E.Message{
		Conversation:                 proto.String(body),
		SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{},
		MessageContextInfo:           &waE2E.MessageContextInfo{},
	}))
	if text != body || typ != "text" {
		t.Errorf("got (%q, %q), want (%q, \"text\")", text, typ, body)
	}
}

func TestUnwrapEnvelopeHandlesNil(t *testing.T) {
	if got := unwrapEnvelope(nil); got != nil {
		t.Errorf("unwrapEnvelope(nil) = %v, want nil", got)
	}
}

// TestEmptyPayloadOfDecodableTypeStillFailsLoud is the guard for the mistake
// this change made on its first attempt. Adding a decoder for a type is not the
// same as being able to READ every instance of it: a poll with no question or
// an event with no fields decodes to empty text. If the decoder claims the
// message anyway, the row is stored empty and the MYC-3284 marker is lost —
// a decode tier silently undoing the fix it was built on top of.
//
// So a decoder only claims a message when it produced text; otherwise the
// message falls through and stays loud.
func TestEmptyPayloadOfDecodableTypeStillFailsLoud(t *testing.T) {
	cases := []struct {
		name    string
		msg     *waE2E.Message
		rawType string
	}{
		{"poll with no question", &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{}}, "pollCreationMessage"},
		{"event with no fields", &waE2E.Message{EventMessage: &waE2E.EventMessage{}}, "eventMessage"},
		{"group invite with no name", &waE2E.Message{GroupInviteMessage: &waE2E.GroupInviteMessage{}}, "groupInviteMessage"},
		{"contacts array with no contacts", &waE2E.Message{ContactsArrayMessage: &waE2E.ContactsArrayMessage{}}, "contactsArrayMessage"},
		{"live location with no coordinates and no caption", &waE2E.Message{LiveLocationMessage: &waE2E.LiveLocationMessage{}}, "liveLocationMessage"},
		{"template with nothing to read", &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{}}, "templateMessage"},
		// A media header has no text of its own; claiming the message would
		// store an empty `text` row.
		{"buttons message with only an image header", &waE2E.Message{ButtonsMessage: &waE2E.ButtonsMessage{
			Header: &waE2E.ButtonsMessage_ImageMessage{ImageMessage: &waE2E.ImageMessage{}},
		}}, "buttonsMessage"},
		{"interactive message with only an image header", &waE2E.Message{InteractiveMessage: &waE2E.InteractiveMessage{
			Header: &waE2E.InteractiveMessage_Header{Media: &waE2E.InteractiveMessage_Header_ImageMessage{ImageMessage: &waE2E.ImageMessage{}}},
		}}, "interactiveMessage"},
		// An HSM that was never hydrated is a reference to a template the
		// client renders from its own pack; its bare parameters are fragments,
		// not a message, and must not be stored as if they were one.
		{"legacy template whose HSM was never hydrated", &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
			Format: &waE2E.TemplateMessage_FourRowTemplate_{FourRowTemplate: &waE2E.TemplateMessage_FourRowTemplate{
				Content: &waE2E.HighlyStructuredMessage{
					Namespace: proto.String("bank_ns"), ElementName: proto.String("purchase_alert"),
					Params: []string{"1234", "500.00"},
				},
			}},
		}}, "templateMessage"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, msgType := extractContent(msgEvent(tc.msg))
			if text == "" {
				t.Fatalf("SILENT DROP: %s decoded to an empty %q row — a decode tier must never "+
					"trade the fail-loud marker for a blank row", tc.rawType, msgType)
			}
			if got := unsupportedRawType(text); got != tc.rawType {
				t.Errorf("marker raw type = %q, want %q", got, tc.rawType)
			}
		})
	}
}
