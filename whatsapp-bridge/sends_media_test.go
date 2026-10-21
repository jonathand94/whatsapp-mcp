package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// A minimal but real PNG. Used where the test needs bytes that content
// sniffing will actually recognise as an image, rather than a string that
// only looks like one.
var tinyPNG = []byte{
	0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A,
	0x00, 0x00, 0x00, 0x0D, 'I', 'H', 'D', 'R',
	0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
	0x89,
}

func testConfig(t *testing.T) *Config {
	t.Helper()
	return &Config{MediaPath: t.TempDir()}
}

// --- pickMediaType ----------------------------------------------------------

func TestPickMediaType(t *testing.T) {
	tests := []struct {
		name       string
		sendType   string
		mime       string
		asDocument bool
		voiceNote  bool
		want       whatsmeow.MediaType
	}{
		{"image goes as photo by default", "file", "image/jpeg", false, false, whatsmeow.MediaImage},
		{"image forced to document", "file", "image/jpeg", true, false, whatsmeow.MediaDocument},
		{"video goes as video", "file", "video/mp4", false, false, whatsmeow.MediaVideo},
		{"video forced to document", "file", "video/mp4", true, false, whatsmeow.MediaDocument},
		{"pdf is a document", "file", "application/pdf", false, false, whatsmeow.MediaDocument},
		{"unknown type falls back to document", "file", "application/octet-stream", false, false, whatsmeow.MediaDocument},
		{"audio ignores mime", "audio", "audio/mpeg", false, false, whatsmeow.MediaAudio},
		// An audio file over the native ceiling is stored with as_document
		// set (planDelivery), and has to actually go as a document.
		{"oversize audio goes as a document", "audio", "audio/mpeg", true, false, whatsmeow.MediaDocument},
		// A voice note only exists as audio. as_document is rejected for audio
		// at the validator and planDelivery never sets it for a voice note, but
		// the mapping must still not turn one into a document if it slipped
		// through.
		{"voice note ignores as_document", "audio", "audio/ogg", true, true, whatsmeow.MediaAudio},
		{"empty mime is a document", "file", "", false, false, whatsmeow.MediaDocument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickMediaType(tc.sendType, tc.mime, tc.asDocument, tc.voiceNote); got != tc.want {
				t.Fatalf("pickMediaType(%q,%q,%v,%v) = %q, want %q",
					tc.sendType, tc.mime, tc.asDocument, tc.voiceNote, got, tc.want)
			}
		})
	}
}

// --- resolveMIME ------------------------------------------------------------

func TestResolveMIMEPrecedence(t *testing.T) {
	// An explicit type must beat both the extension and the bytes: the caller
	// knows things the file does not, and a .bin full of PDF is still a PDF.
	if got := resolveMIME("application/pdf", "thing.png", tinyPNG); got != "application/pdf" {
		t.Fatalf("explicit mime should win, got %q", got)
	}
	// Extension beats sniffing.
	if got := resolveMIME("", "doc.pdf", []byte("not really a pdf")); !strings.HasPrefix(got, "application/pdf") {
		t.Fatalf("extension should win over sniffing, got %q", got)
	}
	// Sniffing is the last resort.
	if got := resolveMIME("", "noextension", tinyPNG); got != "image/png" {
		t.Fatalf("sniffing should identify png, got %q", got)
	}
	// Case and padding from a caller-supplied value are normalised.
	if got := resolveMIME("  IMAGE/JPEG  ", "", nil); got != "image/jpeg" {
		t.Fatalf("explicit mime should be trimmed and lowercased, got %q", got)
	}
}

// --- planDelivery -----------------------------------------------------------

func TestPlanDelivery(t *testing.T) {
	const native = maxNativeMediaBytes
	tests := []struct {
		name       string
		sendType   string
		mime       string
		size       int64
		asDocument bool
		voiceNote  bool
		want       delivery
		wantErr    bool
	}{
		{"small photo stays a photo", "file", "image/jpeg", 1024, false, false, delivery{}, false},
		{"photo at the native ceiling stays a photo", "file", "image/jpeg", native, false, false, delivery{}, false},
		// The old rule refused this outright. WhatsApp takes the same bytes as
		// a document, so that is what happens, and the preview says why.
		{"photo over the native ceiling goes as a document", "file", "image/jpeg", native + 1, false, false,
			delivery{AsDocument: true, Downgraded: true}, false},
		// Videos keep the 100 MiB they always had here: one that used to play
		// inline must not start arriving as a file.
		{"90 MB video stays a video", "file", "video/mp4", 90 << 20, false, false, delivery{}, false},
		{"video over its native ceiling goes as a document", "file", "video/mp4", maxNativeVideoBytes + 1, false, false,
			delivery{AsDocument: true, Downgraded: true}, false},
		{"audio over the native ceiling goes as a document", "audio", "audio/mpeg", native + 1, false, false,
			delivery{AsDocument: true, Downgraded: true}, false},
		// Asked for, so not a downgrade: the preview has nothing to explain.
		{"explicit document is not a downgrade", "file", "image/png", native + 1, true, false,
			delivery{AsDocument: true}, false},
		{"pdf stays an ordinary document", "file", "application/pdf", 1_500_000_000, false, false, delivery{}, false},
		{"document at WhatsApp's ceiling is accepted", "file", "application/zip", maxDocumentBytes, false, false, delivery{}, false},
		{"anything over WhatsApp's ceiling is refused", "file", "application/zip", maxDocumentBytes + 1, false, false, delivery{}, true},
		{"even as an explicit document", "file", "video/mp4", maxDocumentBytes + 1, true, false, delivery{}, true},
		// A voice note cannot fall back to a document.
		{"oversize opus voice note is refused", "audio", "audio/ogg", native + 1, false, true, delivery{}, true},
		// mp3 is transcoded at confirm, which shrinks it; the real check happens
		// on the converted file.
		{"oversize mp3 voice note waits for the transcode", "audio", "audio/mpeg", native + 1, false, true, delivery{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := planDelivery(tc.sendType, tc.mime, tc.size, tc.asDocument, tc.voiceNote)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %d bytes", tc.size)
				}
				if !strings.Contains(err.Error(), "WhatsApp") {
					t.Fatalf("the error should say the limit is WhatsApp's: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("planDelivery = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestHumanBytes(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{
		{999, "999 B"},
		{1000, "1.0 kB"},
		{16 << 20, "16.8 MB"},
		{maxDocumentBytes, "2.0 GB"},
	} {
		if got := humanBytes(tc.n); got != tc.want {
			t.Fatalf("humanBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

// --- materializeOutboundFile ------------------------------------------------

func TestMaterializeFromPath(t *testing.T) {
	cfg := testConfig(t)
	src := filepath.Join(t.TempDir(), "photo.png")
	if err := os.WriteFile(src, tinyPNG, 0o600); err != nil {
		t.Fatal(err)
	}

	got1, err := materializeOutboundFile(t.Context(), cfg, "draft-1", createDraftRequest{
		SendType: "file",
		FilePath: src,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got1.MIME != "image/png" {
		t.Fatalf("mime = %q, want image/png", got1.MIME)
	}
	// The name the recipient sees comes from the source file, not from the
	// draft id the bytes are stored under.
	if got1.Filename != "photo.png" {
		t.Fatalf("filename = %q, want photo.png", got1.Filename)
	}
	// The bytes must be copied into the bridge's own directory, not merely
	// referenced: the caller is free to delete or overwrite their file
	// between draft and confirm.
	if !strings.HasPrefix(got1.Path, outboundDir(cfg)) {
		t.Fatalf("file should live under the outbound dir, got %q", got1.Path)
	}
	got, err := os.ReadFile(got1.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(tinyPNG) {
		t.Fatal("copied bytes differ from the source")
	}
}

func TestMaterializeFromBase64(t *testing.T) {
	cfg := testConfig(t)
	got, err := materializeOutboundFile(t.Context(), cfg, "draft-2", createDraftRequest{
		SendType:   "file",
		FileBase64: base64.StdEncoding.EncodeToString(tinyPNG),
		Filename:   "recibo.png",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.MIME != "image/png" {
		t.Fatalf("mime = %q, want image/png", got.MIME)
	}
	if filepath.Ext(got.Path) != ".png" {
		t.Fatalf("extension should come from the filename, got %q", got.Path)
	}
}

func TestMaterializeRejectsBadInput(t *testing.T) {
	cfg := testConfig(t)
	existing := filepath.Join(t.TempDir(), "f.png")
	if err := os.WriteFile(existing, tinyPNG, 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		req  createDraftRequest
		want string
	}{
		{
			"no source at all",
			createDraftRequest{SendType: "file"},
			"required",
		},
		{
			"both path and bytes",
			createDraftRequest{SendType: "file", FilePath: existing, FileBase64: "aGk="},
			"exactly one",
		},
		{
			"both path and url",
			createDraftRequest{SendType: "file", FilePath: existing, FileURL: "https://example.com/a.png"},
			"exactly one",
		},
		{
			"all three sources",
			createDraftRequest{
				SendType: "file", FilePath: existing, FileBase64: "aGk=",
				FileURL: "https://example.com/a.png",
			},
			"exactly one",
		},
		{
			"missing file",
			createDraftRequest{SendType: "file", FilePath: filepath.Join(t.TempDir(), "nope.png")},
			"file_path",
		},
		{
			"directory instead of file",
			createDraftRequest{SendType: "file", FilePath: t.TempDir()},
			"directory",
		},
		{
			"invalid base64",
			createDraftRequest{SendType: "file", FileBase64: "!!!not base64!!!"},
			"base64",
		},
		{
			"empty payload",
			createDraftRequest{SendType: "file", FileBase64: ""},
			"required",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := materializeOutboundFile(t.Context(), cfg, "d", tc.req)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("error %q should mention %q", err, tc.want)
			}
		})
	}
}

func TestMaterializeRejectsEmptyFile(t *testing.T) {
	cfg := testConfig(t)
	empty := filepath.Join(t.TempDir(), "empty.pdf")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// An empty file uploads "successfully" and arrives as an unopenable
	// attachment, so it is worth failing before the network call.
	if _, err := materializeOutboundFile(t.Context(), cfg, "d", createDraftRequest{
		SendType: "file", FilePath: empty,
	}); err == nil {
		t.Fatal("expected an empty file to be rejected")
	}
}

func TestMaterializeSendsOversizeImageAsDocument(t *testing.T) {
	cfg := testConfig(t)
	// One byte past the native ceiling: big enough to be downgraded, small
	// enough to keep the test's allocation modest.
	big := make([]byte, maxNativeMediaBytes+1)
	copy(big, tinyPNG)
	got, err := materializeOutboundFile(t.Context(), cfg, "d", createDraftRequest{
		SendType:   "file",
		FileBase64: base64.StdEncoding.EncodeToString(big),
		Filename:   "huge.jpg",
	})
	if err != nil {
		t.Fatalf("an oversize photo should be accepted as a document, got %v", err)
	}
	if !got.AsDocument || !got.Downgraded {
		t.Fatalf("delivery = %+v, want a size-forced document", got.delivery)
	}
	if got.Size != int64(len(big)) {
		t.Fatalf("size = %d, want %d", got.Size, len(big))
	}
	if as := deliveredAs("file", got.MIME, got.delivery, false); as != "document" {
		t.Fatalf("deliveredAs = %q, want document", as)
	}
}

func TestMaterializeRefusesFilesOverWhatsAppsCeiling(t *testing.T) {
	cfg := testConfig(t)
	// A sparse file: it reports 2 GB + 1 byte without occupying it, so the
	// test proves the refusal happens before any copying starts.
	src := filepath.Join(t.TempDir(), "backup.zip")
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(maxDocumentBytes + 1); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = materializeOutboundFile(t.Context(), cfg, "d", createDraftRequest{
		SendType: "file",
		FilePath: src,
	})
	if err == nil {
		t.Fatal("expected a file over 2 GB to be refused")
	}
	if !strings.Contains(err.Error(), "WhatsApp") {
		t.Fatalf("the error should explain the limit is WhatsApp's: %v", err)
	}
	if entries, err := os.ReadDir(outboundDir(cfg)); err == nil && len(entries) != 0 {
		t.Fatalf("a refused draft left %d file(s) behind", len(entries))
	}
}

func TestMaterializeStreamsLargeFilesFromPath(t *testing.T) {
	cfg := testConfig(t)
	// Past both the old 100 MiB document cap and the native ceiling, written
	// sparse so it costs no disk. Copying it is the streaming path the old
	// os.ReadFile implementation could not take for a 2 GB file.
	src := filepath.Join(t.TempDir(), "video.mp4")
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	const size = 120 << 20
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	f.Close()

	job := &mediaJob{done: make(chan struct{})}
	got, err := materializeWithProgress(t.Context(), cfg, "draft-big", createDraftRequest{
		SendType: "file",
		FilePath: src,
	}, job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Size != size || job.bytesDone.Load() != size || job.bytesTotal.Load() != size {
		t.Fatalf("size=%d progress=%d/%d, want all %d",
			got.Size, job.bytesDone.Load(), job.bytesTotal.Load(), size)
	}
	if got.MIME != "video/mp4" || !got.AsDocument || !got.Downgraded {
		t.Fatalf("got %+v, want a video/mp4 forced to document", got)
	}
	if st, err := os.Stat(got.Path); err != nil || st.Size() != size {
		t.Fatalf("copied file: %v (size %v)", err, st)
	}
	if _, err := os.Stat(filepath.Join(outboundDir(cfg), "draft-big.part")); !os.IsNotExist(err) {
		t.Fatalf("the .part file should be gone after a successful draft, stat err = %v", err)
	}
}

// --- how a sent message gets archived ---------------------------------------

// TestSentMessageClassification pins the fix for outbound rows being archived
// as text.
//
// The confirm handler used to write the literal 'text' into messages.type, so
// every image, document and voice note the bridge sent was recorded in the
// user's own history as a text message. Nothing could correct it afterwards:
// the insert is ON CONFLICT DO NOTHING, and WhatsApp does not echo a message
// back to the device that originated it, so that row is the only record that
// will ever exist. A message sent from the phone came out as type=image while
// the same message sent through the bridge came out as type=text.
//
// The fix classifies the outbound protobuf with the SAME extractor the receive
// path uses. So what this test asserts is that the two agree by construction —
// not that some parallel mapping table happens to be correct today.
func TestSentMessageClassification(t *testing.T) {
	const url, dpath = "https://mmg.whatsapp.net/d", "/v/t62/f"
	key, encSHA, sha := []byte("mediakey"), []byte("encsha"), []byte("sha")

	tests := []struct {
		name      string
		msg       *waE2E.Message
		wantType  string
		wantMedia bool
	}{
		{
			name: "image",
			msg: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
				Mimetype: proto.String("image/png"), URL: proto.String(url), DirectPath: proto.String(dpath),
				MediaKey: key, FileEncSHA256: encSHA, FileSHA256: sha, FileLength: proto.Uint64(2150029),
			}},
			wantType: "image", wantMedia: true,
		},
		{
			name: "video",
			msg: &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
				Mimetype: proto.String("video/mp4"), URL: proto.String(url), DirectPath: proto.String(dpath),
				MediaKey: key, FileEncSHA256: encSHA, FileSHA256: sha,
			}},
			wantType: "video", wantMedia: true,
		},
		{
			name: "document",
			msg: &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
				FileName: proto.String("contrato.pdf"), Mimetype: proto.String("application/pdf"),
				URL: proto.String(url), DirectPath: proto.String(dpath),
				MediaKey: key, FileEncSHA256: encSHA, FileSHA256: sha,
			}},
			wantType: "document", wantMedia: true,
		},
		{
			name: "audio attachment",
			msg: &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
				Mimetype: proto.String("audio/mpeg"), PTT: proto.Bool(false),
				URL: proto.String(url), DirectPath: proto.String(dpath),
				MediaKey: key, FileEncSHA256: encSHA, FileSHA256: sha,
			}},
			wantType: "audio", wantMedia: true,
		},
		{
			// The PTT flag is the only thing separating these two, and it is the
			// bit send_audio's voice_note parameter exists to set.
			name: "voice note",
			msg: &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
				Mimetype: proto.String("audio/ogg; codecs=opus"), PTT: proto.Bool(true),
				URL: proto.String(url), DirectPath: proto.String(dpath),
				MediaKey: key, FileEncSHA256: encSHA, FileSHA256: sha,
			}},
			wantType: "voice", wantMedia: true,
		},
		{
			// The one case where 'text' is the right answer.
			name:     "plain text",
			msg:      &waE2E.Message{Conversation: proto.String("hola")},
			wantType: "text", wantMedia: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, gotType := extractContentFromProto(tc.msg)
			if gotType != tc.wantType {
				t.Fatalf("type = %q, want %q", gotType, tc.wantType)
			}
			if gotType == "text" && tc.wantType != "text" {
				t.Fatal("this is the regression: media archived as a text message")
			}

			fields, ok := extractDownloadableFieldsFromProto(tc.msg)
			if ok != tc.wantMedia {
				t.Fatalf("media fields present = %v, want %v", ok, tc.wantMedia)
			}
			if !tc.wantMedia {
				return
			}
			// Without these the row is a media message with no way to fetch it,
			// which is what download_media reports as an orphan.
			if string(fields.MediaKey) != string(key) {
				t.Fatalf("media key not carried: %q", fields.MediaKey)
			}
			if !fields.MediaDirectPath.Valid || fields.MediaDirectPath.String != dpath {
				t.Fatalf("direct path not carried: %+v", fields.MediaDirectPath)
			}
			if !fields.MediaURL.Valid || fields.MediaURL.String != url {
				t.Fatalf("url not carried: %+v", fields.MediaURL)
			}
		})
	}
}

func TestExtractDownloadableFieldsFromProtoHandlesNil(t *testing.T) {
	// The confirm path calls this on whatever buildMediaMessage returned; a nil
	// message must be a clean "no media", not a panic mid-send.
	if _, ok := extractDownloadableFieldsFromProto(nil); ok {
		t.Fatal("nil message should report no media fields")
	}
}

// --- nonEmpty ---------------------------------------------------------------

func TestNonEmptyCaption(t *testing.T) {
	// An empty caption must be nil, not a pointer to "": WhatsApp renders the
	// latter as a blank line beneath the media.
	if nonEmpty("") != nil {
		t.Fatal("empty caption should be nil")
	}
	got := nonEmpty("hola")
	if got == nil || *got != "hola" {
		t.Fatalf("caption not preserved: %v", got)
	}
}

// --- removeOutboundFile -----------------------------------------------------

func TestRemoveOutboundFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "x.bin")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeOutboundFile(f); err != nil {
		t.Fatalf("removing an existing file: %v", err)
	}
	if _, err := os.Stat(f); !os.IsNotExist(err) {
		t.Fatal("file should be gone")
	}
	// Idempotent: expiry sweeps and the insert-failure path can both target
	// the same draft, and the second one must not report a failure.
	if err := removeOutboundFile(f); err != nil {
		t.Fatalf("removing a missing file should be a no-op: %v", err)
	}
	if err := removeOutboundFile(""); err != nil {
		t.Fatalf("empty path should be a no-op: %v", err)
	}
}

// --- voice notes ------------------------------------------------------------

func TestOpusOggRecognised(t *testing.T) {
	// These skip transcoding entirely; getting the set wrong means every
	// voice note pays for an ffmpeg round trip, or worse, fails on a machine
	// without ffmpeg despite already being in the right format.
	for _, m := range []string{"audio/ogg", "audio/opus"} {
		if !opusOggMIMEs[m] {
			t.Fatalf("%q should be accepted as-is for a voice note", m)
		}
	}
	for _, m := range []string{"audio/mpeg", "audio/mp4", "audio/wav", ""} {
		if opusOggMIMEs[m] {
			t.Fatalf("%q should require transcoding", m)
		}
	}
}

func TestTranscodeReportsMissingFFmpeg(t *testing.T) {
	src := filepath.Join(t.TempDir(), "a.mp3")
	if err := os.WriteFile(src, []byte("fake"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := transcodeToOpusOgg(t.Context(), "definitely-not-a-real-binary-xyz", src)
	if err == nil {
		t.Fatal("expected an error when ffmpeg is absent")
	}
	// The message has to tell the user how to proceed, not just what broke.
	if !strings.Contains(err.Error(), "voice_note=false") {
		t.Fatalf("the error should offer the no-transcode escape hatch: %v", err)
	}
	if !strings.Contains(err.Error(), "WHATSAPP_FFMPEG_BIN_PATH") {
		t.Fatalf("the error should name the config var: %v", err)
	}
}
