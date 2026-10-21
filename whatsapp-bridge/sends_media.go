// sends_media.go — outbound media for the draft/confirm send flow.
//
// The `sends` table has carried `content_file_path` and `content_media_mime`
// columns, and allowed send_type 'file' and 'audio', since the initial
// migration; only the code was missing. This file fills that gap.
//
// Two invariants shape the design:
//
//  1. The confirm step is the ONLY place an outbound network call happens
//     (see the contract at the top of sends.go). Uploading at draft time
//     would push the user's bytes to WhatsApp's servers before they approved
//     the send, quietly weakening the guarantee the two-step flow exists to
//     provide. So drafts only ever touch the local disk; Upload runs in
//     confirm.
//
//  2. Nothing here mutates a draft's stored bytes; deletion is the caller's
//     job, and sends.go does it at three points: a failed draft insert, a
//     confirm that finds the draft already expired, and a successful send
//     (where Upload has already happened, so the file is spent).
//
//     One gap remains, stated rather than implied: a draft that is created and
//     then simply abandoned keeps its bytes until someone tries to confirm it.
//     There is no periodic sweeper. That path leaks one file per abandoned
//     draft, unlike the send path, which used to leak one per message sent.
//
// Files of any size up to WhatsApp's own ceiling move through here without
// ever being held in memory: drafts are copied or downloaded straight to disk,
// and confirm encrypts from disk to disk and uploads from there. Until
// 2026-10 every input was read into a byte slice first, and documents were
// capped at 100 MiB, a twentieth of what WhatsApp accepts.
//
// Voice notes are the one place a plain file is not enough: WhatsApp renders
// a message as a voice note (PTT) only when the audio is Opus in an Ogg
// container. Anything else has to be transcoded, which needs ffmpeg — already
// a dependency of the transcription path, so ffmpegBin() is reused rather
// than introducing a second way to find it.

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// WhatsApp's own ceilings. Checking locally turns a confusing remote failure
// into a clear message before anything is uploaded.
const (
	// maxNativeMediaBytes is the ceiling for a photo, audio or voice note
	// delivered as itself, the bubble that shows or plays inline. Past it the
	// same bytes can still travel as a document, which is what planDelivery
	// does instead of refusing.
	maxNativeMediaBytes = 16 << 20 // 16 MiB

	// maxNativeVideoBytes is the same for a video. It is the ceiling videos
	// already had here before files grew to 2 GB (they fell under the old
	// 100 MiB document limit and went out as video); keeping it means no
	// video that used to play inline now arrives as a file.
	maxNativeVideoBytes = 100 << 20 // 100 MiB

	// maxDocumentBytes is the ceiling for a document. WhatsApp states it as
	// "2 GB"; it is taken in decimal so that a file just under 2 GiB is
	// refused here rather than after an upload that could take hours.
	maxDocumentBytes = 2_000_000_000
)

// opusOggMIMEs are the containers WhatsApp accepts for a voice note as-is.
var opusOggMIMEs = map[string]bool{
	"audio/ogg":              true,
	"audio/ogg; codecs=opus": true,
	"audio/opus":             true,
}

// outboundDir is where draft bytes wait between create and confirm. Kept
// separate from the inbound media folder so a cleanup sweep over abandoned
// drafts can never touch downloaded history.
func outboundDir(cfg *Config) string {
	return filepath.Join(cfg.MediaPath, "outbound")
}

// outboundFile is what a materialized draft body amounts to: bytes on local
// disk, a content type, the name the recipient should see, and how it will be
// delivered.
//
// Filename is carried out of here rather than read back off the request
// because a URL download is the one input mode that can discover a name the
// caller never supplied — from Content-Disposition. Without it a document
// fetched from Drive would reach the recipient named after the draft id.
type outboundFile struct {
	Path     string
	MIME     string
	Filename string
	Size     int64
	delivery
}

// delivery is how a draft's bytes will reach the recipient. It is decided once,
// at draft time, and stored on the row, so the preview the user approves and
// what confirm sends cannot disagree.
type delivery struct {
	AsDocument bool
	// Downgraded is true when the document envelope was forced by size rather
	// than asked for, so the preview can say why a video arrives as a file.
	Downgraded bool
}

// fileSourceCount reports how many of the three input modes the caller used.
// Exactly one is valid; the count is what lets "none" and "more than one" give
// different messages.
func fileSourceCount(req createDraftRequest) int {
	n := 0
	for _, src := range []string{req.FilePath, req.FileBase64, req.FileURL} {
		if src != "" {
			n++
		}
	}
	return n
}

// materializeOutboundFile writes the draft's bytes to disk and reports what to
// store on the draft row.
//
// Three input modes, each covering a client the others cannot. A local stdio
// client can name a path on this machine. A remote client (claude.ai) has no
// filesystem here, so small generated content arrives inline as base64 — but
// base64 crosses the model's context, which caps out around a hundred
// kilobytes. file_url is how anything larger gets here: the bridge fetches it
// itself, which is why fetch_url.go exists and why its address guard is not
// optional. (The remote MCP layer adds its own sources — Drive and upload
// links — by staging the file on this machine and passing file_path.)
func materializeOutboundFile(ctx context.Context, cfg *Config, draftID string, req createDraftRequest) (outboundFile, error) {
	return materializeWithProgress(ctx, cfg, draftID, req, nil)
}

// materializeWithProgress is materializeOutboundFile reporting progress to job,
// which may be nil.
func materializeWithProgress(ctx context.Context, cfg *Config, draftID string, req createDraftRequest, job *mediaJob) (outboundFile, error) {
	if fileSourceCount(req) > 1 {
		return outboundFile{}, fmt.Errorf("pass exactly one of file_path, file_base64 or file_url")
	}

	dir := outboundDir(cfg)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return outboundFile{}, fmt.Errorf("creating outbound dir: %w", err)
	}
	// Bytes land in a .part file and get their final name only once they are
	// accepted, so a rejected or interrupted draft never leaves a file that
	// looks complete. The Remove is a no-op after the rename.
	part := filepath.Join(dir, draftID+".part")
	out, err := os.OpenFile(part, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return outboundFile{}, fmt.Errorf("creating outbound file: %w", err)
	}
	defer os.Remove(part)
	defer out.Close()

	w := &countingWriter{w: out, job: job}
	var nameHint string

	switch {
	case req.FilePath != "":
		clean := filepath.Clean(req.FilePath)
		info, err := os.Stat(clean)
		if err != nil {
			return outboundFile{}, fmt.Errorf("file_path %q: %w", req.FilePath, err)
		}
		if info.IsDir() {
			return outboundFile{}, fmt.Errorf("file_path %q is a directory", req.FilePath)
		}
		// Regular files only. /dev/zero reports a size of 0 and never ends;
		// a FIFO blocks until someone writes to it. Either would fill the
		// disk the message database lives on, or hang the draft.
		if !info.Mode().IsRegular() {
			return outboundFile{}, fmt.Errorf("file_path %q is not a regular file", req.FilePath)
		}
		// Refused before copying: there is no point moving 3 GB to find out
		// WhatsApp takes 2.
		if info.Size() > maxDocumentBytes {
			return outboundFile{}, tooLargeError(info.Size())
		}
		if job != nil {
			job.bytesTotal.Store(info.Size())
		}
		job.setPhase("copiando el archivo")
		src, err := os.Open(clean)
		if err != nil {
			return outboundFile{}, fmt.Errorf("reading file_path %q: %w", req.FilePath, err)
		}
		// Capped while copying too: the size above is only what the file was
		// when it was looked at, and a file can grow after that.
		_, err = io.Copy(w, io.LimitReader(ctxReader{ctx, src}, maxDocumentBytes+1))
		src.Close()
		if err != nil {
			return outboundFile{}, fmt.Errorf("reading file_path %q: %w", req.FilePath, err)
		}
		if w.n > maxDocumentBytes {
			return outboundFile{}, tooLargeError(w.n)
		}
		nameHint = filepath.Base(clean)

	case req.FileBase64 != "":
		data, err := base64.StdEncoding.DecodeString(req.FileBase64)
		if err != nil {
			return outboundFile{}, fmt.Errorf("file_base64 is not valid base64: %w", err)
		}
		if _, err := w.Write(data); err != nil {
			return outboundFile{}, fmt.Errorf("writing outbound file: %w", err)
		}
		nameHint = req.Filename

	case req.FileURL != "":
		job.setPhase("descargando file_url")
		// The zero fetcher is the guarded one; see the type's doc comment.
		nameHint, _, err = fetchRemoteFileTo(ctx, req.FileURL, maxDocumentBytes, fetcher{}, w, job)
		if err != nil {
			return outboundFile{}, err
		}

	default:
		return outboundFile{}, fmt.Errorf(
			"file_path, file_base64 or file_url required for send_type=%q", req.SendType)
	}

	if w.n == 0 {
		return outboundFile{}, fmt.Errorf("the file is empty")
	}
	if err := out.Close(); err != nil {
		return outboundFile{}, fmt.Errorf("writing outbound file: %w", err)
	}
	if req.Filename != "" {
		nameHint = req.Filename
	}

	head, err := readHead(part, 512)
	if err != nil {
		return outboundFile{}, fmt.Errorf("reading outbound file: %w", err)
	}
	mimeType := resolveMIME(req.MediaMIME, nameHint, head)
	plan, err := planDelivery(req.SendType, mimeType, w.n, req.AsDocument, req.VoiceNote)
	if err != nil {
		return outboundFile{}, err
	}

	ext := filepath.Ext(nameHint)
	if ext == "" {
		if exts, _ := mime.ExtensionsByType(mimeType); len(exts) > 0 {
			ext = exts[0]
		}
	}
	final := filepath.Join(dir, draftID+ext)
	if err := os.Rename(part, final); err != nil {
		return outboundFile{}, fmt.Errorf("writing outbound file: %w", err)
	}
	return outboundFile{Path: final, MIME: mimeType, Filename: nameHint, Size: w.n, delivery: plan}, nil
}

// countingWriter counts what passes through it, for the empty-file check and
// for progress.
type countingWriter struct {
	w   io.Writer
	n   int64
	job *mediaJob
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	if c.job != nil {
		c.job.bytesDone.Add(int64(n))
	}
	return n, err
}

// ctxReader stops a local copy when its job is cancelled. A disk read never
// blocks long enough to need more than a check between reads.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// readHead returns up to n bytes from the start of a file, for sniffing.
func readHead(path string, n int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	got, err := io.ReadFull(f, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, err
	}
	return buf[:got], nil
}

// resolveMIME picks the most trustworthy answer available: an explicit
// caller-supplied type, then the filename extension, then content sniffing.
// Sniffing is last because it returns application/octet-stream for plenty of
// real formats, which would push files that are really images down the
// document path.
func resolveMIME(explicit, nameHint string, data []byte) string {
	if explicit != "" {
		return strings.ToLower(strings.TrimSpace(explicit))
	}
	if ext := filepath.Ext(nameHint); ext != "" {
		if byExt := mime.TypeByExtension(ext); byExt != "" {
			return strings.ToLower(byExt)
		}
	}
	return strings.ToLower(http.DetectContentType(data))
}

// planDelivery decides how a file of this type and size reaches WhatsApp, or
// refuses it.
//
// Only one size is a hard limit: WhatsApp's 2 GB for a document. A photo or
// audio file over the 16 MiB native ceiling, or a video over 100 MiB, is not
// refused — it goes as a document, which arrives with its original bytes,
// opens in the recipient's player or viewer, and is what the WhatsApp app
// itself offers for large files. The previous rules refused a 17 MB photo
// outright.
//
// A voice note has no document form to fall back on, so it is the exception.
// Opus/Ogg is sent as it is and its size is final now; anything else is
// transcoded at confirm, which shrinks it, so its check waits for the real
// output (buildMediaMessage).
func planDelivery(sendType, mimeType string, size int64, asDocument, voiceNote bool) (delivery, error) {
	if size > maxDocumentBytes {
		return delivery{}, tooLargeError(size)
	}
	if voiceNote {
		if opusOggMIMEs[mimeType] && size > maxNativeMediaBytes {
			return delivery{}, voiceNoteTooLargeError(size)
		}
		return delivery{}, nil
	}
	if asDocument {
		return delivery{AsDocument: true}, nil
	}
	if ceiling := nativeCeiling(sendType, mimeType); ceiling > 0 && size > ceiling {
		return delivery{AsDocument: true, Downgraded: true}, nil
	}
	return delivery{}, nil
}

// nativeCeiling is the largest file this kind of media can be and still go as
// itself rather than as a document, or 0 for anything that is a document
// already.
func nativeCeiling(sendType, mimeType string) int64 {
	switch {
	case sendType == "audio", strings.HasPrefix(mimeType, "image/"):
		return maxNativeMediaBytes
	case strings.HasPrefix(mimeType, "video/"):
		return maxNativeVideoBytes
	default:
		return 0
	}
}

func tooLargeError(size int64) error {
	return fmt.Errorf("el archivo pesa %s y WhatsApp acepta hasta %s por archivo",
		humanBytes(size), humanBytes(maxDocumentBytes))
}

func voiceNoteTooLargeError(size int64) error {
	return fmt.Errorf("la nota de voz pesa %s y WhatsApp acepta hasta %s; "+
		"enviala como audio normal (voice_note=false), que viaja como documento",
		humanBytes(size), humanBytes(maxNativeMediaBytes))
}

// humanBytes renders a size the way a person reads it: "85.3 MB", "1.9 GB".
func humanBytes(n int64) string {
	const unit = 1000
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "kMGT"[exp])
}

// deliveredAs names the envelope a draft will use, for the preview and the
// status answer.
func deliveredAs(sendType, mimeType string, d delivery, voiceNote bool) string {
	switch pickMediaType(sendType, mimeType, d.AsDocument, voiceNote) {
	case whatsmeow.MediaImage:
		return "photo"
	case whatsmeow.MediaVideo:
		return "video"
	case whatsmeow.MediaAudio:
		if voiceNote {
			return "voice_note"
		}
		return "audio"
	default:
		return "document"
	}
}

// pickMediaType maps a draft onto one of whatsmeow's four upload kinds.
//
// Split out from buildMediaMessage so it can be tested without a live
// client: the branch that decides whether a JPEG becomes a photo or a
// document is exactly the one worth having tests on, and it is unreachable
// in a unit test if it only exists inline next to a network call.
//
// as_document deliberately wins over the MIME type. WhatsApp recompresses
// anything sent as an image, so "send this receipt as a document" has to be
// able to override what the bytes look like. A voice note is the exception:
// it only exists as audio, so nothing turns it into a document.
func pickMediaType(sendType, mimeType string, asDocument, voiceNote bool) whatsmeow.MediaType {
	if sendType == "audio" && voiceNote {
		return whatsmeow.MediaAudio
	}
	if asDocument {
		return whatsmeow.MediaDocument
	}
	if sendType == "audio" {
		return whatsmeow.MediaAudio
	}
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		return whatsmeow.MediaImage
	case strings.HasPrefix(mimeType, "video/"):
		return whatsmeow.MediaVideo
	default:
		return whatsmeow.MediaDocument
	}
}

// mediaMessageOpts carries the per-draft choices confirm needs. Grouped into
// a struct because threading six positional arguments through a build
// function is how the wrong bool ends up in the wrong slot.
type mediaMessageOpts struct {
	SendType   string // "file" | "audio"
	Path       string
	MIME       string
	Filename   string
	Caption    string
	AsDocument bool // deliver as a document: asked for, or forced by size
	VoiceNote  bool // render as PTT rather than an audio attachment
	FFmpegBin  string
}

// buildMediaMessage uploads the draft's bytes and returns the protobuf to
// send. This is the only function here that touches the network, and it is
// called exclusively from the confirm path.
//
// job, when non-nil, receives the phase and upload progress, and is how a
// stalled upload gets noticed (see uploadTracker).
func buildMediaMessage(ctx context.Context, cli *whatsmeow.Client, o mediaMessageOpts, job *mediaJob) (*waE2E.Message, error) {
	path := o.Path
	mimeType := o.MIME

	// A voice note has to be Opus/Ogg. Transcode only when it is not already,
	// so a correctly-formatted file never pays for ffmpeg — and a deployment
	// without ffmpeg can still send those.
	if o.SendType == "audio" && o.VoiceNote && !opusOggMIMEs[mimeType] {
		job.setPhase("convirtiendo a nota de voz")
		converted, err := transcodeToOpusOgg(ctx, o.FFmpegBin, path)
		if err != nil {
			return nil, err
		}
		defer os.Remove(converted)
		if st, err := os.Stat(converted); err == nil && st.Size() > maxNativeMediaBytes {
			return nil, voiceNoteTooLargeError(st.Size())
		}
		path = converted
		mimeType = "audio/ogg; codecs=opus"
	}

	src, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("reading draft file: %w", err)
	}
	defer src.Close()
	if st, err := src.Stat(); err == nil && job != nil {
		job.bytesTotal.Store(st.Size())
	}

	// UploadReader encrypts into a scratch file and uploads from it, so the
	// plaintext is read once and nothing is ever held in memory. The scratch
	// file sits next to the draft, on the same disk and with the same 0700
	// directory, rather than in a world-readable temp dir.
	scratch, err := os.CreateTemp(filepath.Dir(path), ".upload-*.enc")
	if err != nil {
		return nil, fmt.Errorf("creating upload scratch file: %w", err)
	}
	defer func() {
		scratch.Close()
		os.Remove(scratch.Name())
	}()

	mediaType := pickMediaType(o.SendType, mimeType, o.AsDocument, o.VoiceNote)

	job.setPhase("cifrando")
	up, err := cli.UploadReader(ctx, src, &uploadTracker{f: scratch, job: job}, mediaType)
	if err != nil {
		return nil, fmt.Errorf("uploading media: %w", err)
	}

	switch mediaType {
	case whatsmeow.MediaImage:
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			Caption:       nonEmpty(o.Caption),
			Mimetype:      proto.String(mimeType),
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
		}}, nil

	case whatsmeow.MediaVideo:
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
			Caption:       nonEmpty(o.Caption),
			Mimetype:      proto.String(mimeType),
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
		}}, nil

	case whatsmeow.MediaAudio:
		return &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
			Mimetype:      proto.String(mimeType),
			PTT:           proto.Bool(o.VoiceNote),
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
		}}, nil

	default:
		name := o.Filename
		if name == "" {
			name = filepath.Base(o.Path)
		}
		return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
			Caption:       nonEmpty(o.Caption),
			FileName:      proto.String(name),
			Mimetype:      proto.String(mimeType),
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
		}}, nil
	}
}

// uploadTracker is the scratch file UploadReader encrypts into and then
// uploads from. It exists to see that second half: whatsmeow reports no
// progress of its own, but every byte it uploads is read through here, after
// the one Seek back to the start that separates encrypting from uploading.
type uploadTracker struct {
	f         *os.File
	job       *mediaJob
	uploading atomic.Bool
}

func (u *uploadTracker) Write(p []byte) (int, error) {
	n, err := u.f.Write(p)
	if u.job != nil {
		// Encrypting counts as activity, so a slow disk is not mistaken for
		// a stalled upload; it is not shown as upload progress.
		u.job.moves.Add(1)
	}
	return n, err
}

func (u *uploadTracker) Read(p []byte) (int, error) {
	n, err := u.f.Read(p)
	if n > 0 && u.job != nil && u.uploading.Load() {
		u.job.bytesDone.Add(int64(n))
	}
	return n, err
}

func (u *uploadTracker) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekStart && u.job != nil && !u.uploading.Swap(true) {
		u.job.bytesDone.Store(0)
		u.job.setPhase("subiendo a WhatsApp")
	}
	return u.f.Seek(offset, whence)
}

// nonEmpty returns nil for an empty caption rather than a pointer to "".
// WhatsApp renders an empty-string caption as a blank line under the media.
func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}

// transcodeToOpusOgg converts any ffmpeg-readable audio into the one format
// WhatsApp accepts for voice notes. Mirrors the ogg → wav call in whisper.go,
// including reusing its binary lookup, so there is a single answer to "where
// is ffmpeg" across the project.
func transcodeToOpusOgg(ctx context.Context, ffmpegBin, src string) (string, error) {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if _, err := exec.LookPath(ffmpegBin); err != nil {
		return "", fmt.Errorf(
			"las notas de voz requieren Opus/Ogg y el archivo no lo es; ffmpeg %q no esta en el PATH (%s, o define WHATSAPP_FFMPEG_BIN_PATH). "+
				"Envialo con voice_note=false para mandarlo como audio adjunto sin convertir",
			ffmpegBin, ffmpegInstallHint())
	}

	dst := strings.TrimSuffix(src, filepath.Ext(src)) + ".voice.ogg"
	cmd := exec.CommandContext(ctx, ffmpegBin,
		"-y",
		"-i", src,
		"-vn",      // discard any cover art; a video stream breaks the ogg
		"-ac", "1", // WhatsApp voice notes are mono
		"-ar", "48000", // Opus's native rate; anything else gets resampled anyway
		"-c:a", "libopus",
		"-b:a", "32k", // what the WhatsApp client itself records at
		dst,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("ffmpeg no pudo convertir a Opus/Ogg: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return dst, nil
}

// removeOutboundFile deletes a draft's bytes. Errors are returned rather than
// swallowed so callers can log them; a leaked file is harmless but silently
// leaking every abandoned draft is not.
func removeOutboundFile(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
