// media_download.go — generalized media-field capture + on-demand download
// for any message type (image, video, document, sticker, audio).
//
// Background: the bridge originally only persisted media_key + related
// download fields for AUDIO messages, because voice-note transcription
// was the only consumer. The receipts pipeline (and any other tool that
// needs image/document content) requires the same treatment for other
// media types.
//
// This file provides:
//
//   1. extractDownloadableFields(evt) — pulls media_key + URL + sha + length
//      + mime out of any media-bearing message uniformly. Used by onMessage
//      so all image/video/document/sticker/audio rows persist the fields
//      needed to re-download later.
//
//   2. Bridge.DownloadMediaFile(ctx, messageID, job) — reads the persisted
//      fields from the messages row, reconstitutes the right whatsmeow proto
//      message, streams the decrypted file into the configured media folder,
//      and updates messages.media_path. Idempotent. When WhatsApp's CDN has
//      already dropped the file it asks the phone for it (media_retry.go).
//
//   3. Server.handleDownloadMedia — POST /api/media/download endpoint that
//      lets the receipts pipeline (or any other Python consumer) materialize
//      a single message's bytes on demand without flipping the global
//      WHATSAPP_AUTO_DOWNLOAD_MEDIA flag (which would auto-download every
//      media in every chat — unwanted blast radius for one workflow). Large
//      files run as a job (jobs.go): the endpoint answers 202 while one is
//      still downloading, and calling it again with the same id joins it.

package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
	"golang.org/x/text/unicode/norm"
)

// mediaFields holds the persisted columns the bridge needs to re-download
// any media-bearing message via whatsmeow.Client.Download.
type mediaFields struct {
	MediaKey          []byte
	MediaURL          sql.NullString
	MediaDirectPath   sql.NullString
	MediaEncSHA       []byte
	MediaSHA          []byte
	MediaFileLength   sql.NullInt64
	MediaKeyTimestamp sql.NullInt64
	MediaMime         sql.NullString
	// MediaFilename is the name the sender gave a document. Only documents
	// carry one; see migrations/007_media_filename.sql.
	MediaFilename sql.NullString
}

// downloadable is the minimum interface every media-bearing protobuf message
// implements. Pulled out only to deduplicate the per-type field copy in
// extractDownloadableFields without losing concrete types.
type downloadable interface {
	GetMediaKey() []byte
	GetFileEncSHA256() []byte
	GetFileSHA256() []byte
	GetURL() string
	GetDirectPath() string
	GetFileLength() uint64
	GetMediaKeyTimestamp() int64
	GetMimetype() string
}

func fieldsFrom(m downloadable) mediaFields {
	var f mediaFields
	f.MediaKey = m.GetMediaKey()
	f.MediaEncSHA = m.GetFileEncSHA256()
	f.MediaSHA = m.GetFileSHA256()
	if u := m.GetURL(); u != "" {
		f.MediaURL = sql.NullString{String: u, Valid: true}
	}
	if dp := m.GetDirectPath(); dp != "" {
		f.MediaDirectPath = sql.NullString{String: dp, Valid: true}
	}
	if fl := m.GetFileLength(); fl > 0 {
		f.MediaFileLength = sql.NullInt64{Int64: int64(fl), Valid: true}
	}
	if mkt := m.GetMediaKeyTimestamp(); mkt > 0 {
		f.MediaKeyTimestamp = sql.NullInt64{Int64: mkt, Valid: true}
	}
	if mt := m.GetMimetype(); mt != "" {
		f.MediaMime = sql.NullString{String: mt, Valid: true}
	}
	return f
}

// extractDownloadableFields pulls the download-relevant fields out of any
// media-bearing message. Returns ok=false for text/system/reaction/etc.
func extractDownloadableFields(evt *events.Message) (mediaFields, bool) {
	return extractDownloadableFieldsFromProto(evt.Message)
}

// extractDownloadableFieldsFromProto is the proto-level extractor shared by the
// receive path (extractDownloadableFields) and the confirm path in sends.go.
//
// The confirm path needs it because a message the bridge sends itself is never
// echoed back by WhatsApp — the server does not return your own send to the
// device that originated it. So the only chance to record the media keys for an
// outbound file is right after Upload, from the protobuf that carries them.
// Mirrors the extractContent / extractContentFromProto split for the same
// reason: one classifier, so a sent row and a received row cannot disagree.
//
// The message is unwrapped first. whatsmeow unwraps live events itself, but
// history-sync messages arrive raw, and a document with a caption or any media
// in a disappearing chat is nested one level down; without this those rows got
// no keys at all. Unwrapping an already-unwrapped message is a no-op.
func extractDownloadableFieldsFromProto(m *waE2E.Message) (mediaFields, bool) {
	m = unwrapEnvelope(m)
	if m == nil {
		return mediaFields{}, false
	}
	if x := m.GetImageMessage(); x != nil {
		return fieldsFrom(x), true
	}
	if x := m.GetVideoMessage(); x != nil {
		return fieldsFrom(x), true
	}
	// A video note is a VideoMessage in its own field. It is stored as type
	// "video", and without this its keys were never kept, so it could not be
	// downloaded later.
	if x := m.GetPtvMessage(); x != nil {
		return fieldsFrom(x), true
	}
	if x := m.GetDocumentMessage(); x != nil {
		f := fieldsFrom(x)
		if name := strings.TrimSpace(x.GetFileName()); name != "" {
			f.MediaFilename = sql.NullString{String: name, Valid: true}
		}
		return f, true
	}
	if x := m.GetAudioMessage(); x != nil {
		return fieldsFrom(x), true
	}
	if x := m.GetStickerMessage(); x != nil {
		return fieldsFrom(x), true
	}
	return mediaFields{}, false
}

// errMediaGone marks a download that failed for good: WhatsApp's CDN no
// longer has the file and the phone does not have it either.
var errMediaGone = errors.New("no se puede recuperar el archivo")

// errPhoneUnreachable marks a download WhatsApp's CDN could not serve and the
// phone did not supply this time — offline, slow, or the request did not get
// through. Unlike errMediaGone it is worth trying again later.
var errPhoneUnreachable = errors.New("el telefono no entrego el archivo")

// downloadAttempts is how many times a download that fails is started over
// from an empty file. whatsmeow retries network errors itself, but from the
// start of the same file without truncating it, and its fallback to another
// CDN host does not rewind at all — so a large download interrupted midway
// ends in an integrity error instead of a retry. Starting clean recovers it.
const downloadAttempts = 3

// DownloadMediaFile fetches and decrypts the media of a stored message into
// the configured media folder, records the path in messages.media_path, and
// describes the file.
//
// Idempotent: if the message already has a media_path that exists on disk,
// that file is returned without downloading again.
//
// The file streams to disk. The previous version held the whole download in
// a byte slice and then wrote it out, so the largest file it could handle was
// bounded by memory, not by WhatsApp. job, when non-nil, receives progress.
func (b *Bridge) DownloadMediaFile(ctx context.Context, messageID string, job *mediaJob) (downloadMediaResponse, error) {
	var (
		msgType, chatJID                   string
		senderJID, senderDisplay, chatName string
		isFromMe                           int
		ts                                 int64
		mediaPath, mediaFilename           sql.NullString
		mediaKey, mediaEncSha, mediaSha    []byte
		directPath, mediaURL, mediaMime    sql.NullString
		fileLength, mediaKeyTimestamp      sql.NullInt64
	)
	err := b.db.QueryRowContext(ctx, `
		SELECT m.type, m.chat_jid, COALESCE(m.sender_jid, ''), COALESCE(m.sender_display, ''),
		       COALESCE(c.name, ''), m.is_from_me, m.timestamp,
		       m.media_path, m.media_filename, m.media_key, m.media_direct_path, m.media_url,
		       m.media_enc_sha256, m.media_sha256, m.media_file_length,
		       m.media_key_timestamp, m.media_mime
		FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid
		WHERE m.id = ?
	`, messageID).Scan(&msgType, &chatJID, &senderJID, &senderDisplay,
		&chatName, &isFromMe, &ts,
		&mediaPath, &mediaFilename, &mediaKey, &directPath, &mediaURL,
		&mediaEncSha, &mediaSha, &fileLength,
		&mediaKeyTimestamp, &mediaMime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return downloadMediaResponse{}, fmt.Errorf("message not found: %s", messageID)
		}
		return downloadMediaResponse{}, fmt.Errorf("query message: %w", err)
	}

	mt := ""
	if mediaMime.Valid {
		mt = mediaMime.String
	}
	resp := downloadMediaResponse{
		MessageID:     messageID,
		Mime:          mt,
		Type:          msgType,
		ChatJID:       chatJID,
		ChatName:      chatName,
		SenderJID:     senderJID,
		SenderDisplay: senderDisplay,
		IsFromMe:      isFromMe == 1,
		Timestamp:     ts,
	}
	resp.Filename, resp.OriginalFilename = displayFilename(mediaFilename.String, msgType, mt, ts)

	// Idempotent fast-path: already downloaded and file still exists.
	if mediaPath.Valid && mediaPath.String != "" {
		if st, statErr := os.Stat(mediaPath.String); statErr == nil && !st.IsDir() {
			resp.Path, resp.Size, resp.CachedHit = mediaPath.String, st.Size(), true
			return resp, nil
		}
	}

	// The row's path can be stale while the file sits right here. Rows carried
	// over from the Windows deployment point at C:\Users\..., so every file
	// downloaded there was fetched from WhatsApp again after the move — and
	// for older messages WhatsApp answered 404 although the bytes were on this
	// disk (three of the failures in the audit log as of 2026-10-21). This is
	// where the file would be if this machine had downloaded it, so look here
	// before asking WhatsApp for bytes it may no longer have.
	if found := b.findLocalMedia(messageID); found != "" {
		if st, statErr := os.Stat(found); statErr == nil {
			if _, perr := b.db.ExecContext(ctx,
				`UPDATE messages SET media_path = ? WHERE id = ?`, found, messageID); perr != nil {
				log.Printf("DownloadMediaFile: repointing media_path for %s failed: %v", messageID, perr)
			}
			resp.Path, resp.Size, resp.CachedHit = found, st.Size(), true
			return resp, nil
		}
	}

	if len(mediaKey) == 0 {
		return resp, fmt.Errorf("media key not available for message %s (likely received before media-key persistence patch)", messageID)
	}

	dl, err := buildDownloadable(msgType, mediaKey, mediaEncSha, mediaSha,
		mediaURL, directPath, fileLength, mediaKeyTimestamp, mt)
	if err != nil {
		return resp, err
	}

	if err := os.MkdirAll(b.cfg.MediaPath, 0o700); err != nil {
		return resp, fmt.Errorf("mkdir media path: %w", err)
	}
	outPath := filepath.Join(b.cfg.MediaPath, safeMediaStem(messageID)+extensionForMime(mt, msgType))
	// Bytes land in a .part file and are renamed into place only once they
	// decrypt and verify, so a reader can never pick up half a file under
	// the final name. The Remove is a no-op after the rename.
	part := outPath + ".part"
	defer os.Remove(part)

	// written only ever grows, so it shows activity even while a retry is
	// re-covering bytes the file already holds; the file's size, which is
	// what pollers see as progress, stays flat during that stretch.
	written := new(atomic.Int64)
	if job != nil {
		job.bytesTotal.Store(fileLength.Int64)
		job.progressFile.Store(part)
		written = &job.moves
	}
	job.setPhase("descargando de WhatsApp")
	err = b.downloadWithRetries(ctx, dl, part, written, job)

	if isExpiredMedia(err) {
		cdnErr := err
		why := cdnProblem(cdnErr)
		job.setPhase(why + "; pidiendoselo al telefono")
		info, ierr := messageInfoForRetry(messageID, chatJID, senderJID, isFromMe == 1)
		if ierr != nil {
			return resp, fmt.Errorf("%w: %s y no se le puede pedir al telefono (%v)", errMediaGone, why, ierr)
		}
		newPath, rerr := b.requestMediaReupload(ctx, info, mediaKey)
		if errors.Is(rerr, errNotOnPhone) {
			return resp, fmt.Errorf("%w: %s y el telefono ya no lo tiene", errMediaGone, why)
		}
		if rerr != nil {
			return resp, fmt.Errorf("%w: %s y %v", errPhoneUnreachable, why, rerr)
		}
		// Recorded straight away, so if the second attempt fails for some
		// unrelated reason the next one starts from the fresh path instead of
		// asking the phone again. The old URL points at the dropped copy.
		if _, perr := b.db.ExecContext(ctx,
			`UPDATE messages SET media_direct_path = ?, media_url = NULL WHERE id = ?`,
			newPath, messageID); perr != nil {
			log.Printf("DownloadMediaFile: persisting the re-uploaded path for %s failed: %v", messageID, perr)
		}
		dl, err = buildDownloadable(msgType, mediaKey, mediaEncSha, mediaSha,
			sql.NullString{}, sql.NullString{String: newPath, Valid: true},
			fileLength, mediaKeyTimestamp, mt)
		if err != nil {
			return resp, err
		}
		job.setPhase("descargando de WhatsApp")
		err = b.downloadWithRetries(ctx, dl, part, written, job)
		resp.RecoveredFromPhone = err == nil
		if errors.Is(err, whatsmeow.ErrInvalidMediaEncSHA256) {
			err = fmt.Errorf("%w: la copia que el telefono volvio a subir tampoco coincide con la del mensaje", errMediaGone)
		}
	}
	if errors.Is(err, errMediaGone) || errors.Is(err, errPhoneUnreachable) {
		return resp, err // already in words a person can act on
	}
	if err != nil {
		return resp, fmt.Errorf("whatsmeow download: %w", err)
	}

	if err := os.Rename(part, outPath); err != nil {
		return resp, fmt.Errorf("write media file: %w", err)
	}
	st, err := os.Stat(outPath)
	if err != nil {
		return resp, fmt.Errorf("write media file: %w", err)
	}

	if _, perr := b.db.ExecContext(ctx,
		`UPDATE messages SET media_path = ?, media_mime = COALESCE(media_mime, ?) WHERE id = ?`,
		outPath, sql.NullString{String: mt, Valid: mt != ""}, messageID,
	); perr != nil {
		// Bytes are on disk; persistence is best-effort. Log + continue.
		log.Printf("DownloadMediaFile: persist media_path failed for %s: %v", messageID, perr)
	}

	resp.Path, resp.Size = outPath, st.Size()
	return resp, nil
}

// findLocalMedia returns a finished download of the message already in the
// media folder, or "". It matches on the name stem alone: the extension
// depends on the OS that did the download (Windows maps image/jpeg to .jfif
// and some JSON types to .dtsconfig), so recomputing it here would miss them.
func (b *Bridge) findLocalMedia(messageID string) string {
	stem := safeMediaStem(messageID)
	// The stem is limited to [A-Za-z0-9._-], none of which Glob treats as a
	// pattern, so the only wildcard is the one added here.
	matches, _ := filepath.Glob(filepath.Join(b.cfg.MediaPath, stem+".*"))
	for _, m := range matches {
		rest := strings.TrimPrefix(filepath.Base(m), stem)
		// ".part" is an unfinished download; anything with a second dot
		// (".voice.ogg", a transcode) belongs to another file.
		if rest == ".part" || strings.Count(rest, ".") != 1 {
			continue
		}
		if st, err := os.Stat(m); err == nil && st.Mode().IsRegular() && st.Size() > 0 {
			return m
		}
	}
	return ""
}

// downloadWithRetries runs downloadToFile up to downloadAttempts times,
// starting from an empty file each time. Expired media is not retried — that
// takes a phone, not another attempt — and neither is a cancelled download.
func (b *Bridge) downloadWithRetries(ctx context.Context, dl whatsmeow.DownloadableMessage, path string, written *atomic.Int64, job *mediaJob) error {
	var err error
	for attempt := 1; attempt <= downloadAttempts; attempt++ {
		err = b.downloadToFile(ctx, dl, path, written)
		if err == nil || isGoneFromCDN(err) || ctx.Err() != nil || errors.Is(err, whatsmeow.ErrNoURLPresent) {
			return err
		}
		if attempt < downloadAttempts {
			log.Printf("download attempt %d of %d failed, starting over: %v", attempt, downloadAttempts, err)
			job.setPhase(fmt.Sprintf("reintentando la descarga (%d de %d)", attempt+1, downloadAttempts))
			select {
			case <-time.After(time.Duration(attempt) * 3 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}

// downloadToFile streams one attachment into path, replacing whatever a
// previous attempt left there. Every byte written is added to written.
func (b *Bridge) downloadToFile(ctx context.Context, dl whatsmeow.DownloadableMessage, path string, written *atomic.Int64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open media file: %w", err)
	}
	defer f.Close()
	return b.client.DownloadToFile(ctx, dl, growingFile{File: f, written: written})
}

// growingFile hides that it is an *os.File, and counts what is written to it.
//
// Hidden because whatsmeow preallocates the full length when handed an
// *os.File, which would make the file's size — the progress a poller sees —
// jump to 100% before the first byte arrives. Behind this wrapper the file
// grows as the download does. Counted because the size alone cannot show a
// retry rewriting the start of the file, which watchStall must not mistake
// for a stall.
type growingFile struct {
	*os.File
	written *atomic.Int64
}

func (g growingFile) Write(p []byte) (int, error) {
	n, err := g.File.Write(p)
	g.written.Add(int64(n))
	return n, err
}

func (g growingFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := g.File.WriteAt(p, off)
	g.written.Add(int64(n))
	return n, err
}

// ReadFrom is overridden so io.Copy cannot reach *os.File's own ReadFrom,
// which would write without passing through Write and go uncounted.
func (g growingFile) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(struct{ io.Writer }{g}, r)
}

// buildDownloadable reconstitutes the right whatsmeow proto message for the
// given DB-stored fields, suitable for client.Download. Mirrors the audio
// reconstitution in backfill.go.
func buildDownloadable(
	msgType string,
	mediaKey, mediaEncSha, mediaSha []byte,
	mediaURL, directPath sql.NullString,
	fileLength, mediaKeyTimestamp sql.NullInt64,
	mt string,
) (whatsmeow.DownloadableMessage, error) {
	switch msgType {
	case "image":
		m := &waE2E.ImageMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	case "video":
		m := &waE2E.VideoMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	case "document":
		m := &waE2E.DocumentMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	case "audio", "voice":
		m := &waE2E.AudioMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	case "sticker":
		m := &waE2E.StickerMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	default:
		return nil, fmt.Errorf("message type %q is not downloadable", msgType)
	}
}

func assignDownloadFields(
	urlPtr **string, directPathPtr **string,
	fileLengthPtr **uint64, mediaKeyTimestampPtr **int64,
	mediaURL, directPath sql.NullString,
	fileLength, mediaKeyTimestamp sql.NullInt64,
) {
	if mediaURL.Valid {
		s := mediaURL.String
		*urlPtr = &s
	}
	if directPath.Valid {
		s := directPath.String
		*directPathPtr = &s
	}
	if fileLength.Valid {
		fl := uint64(fileLength.Int64)
		*fileLengthPtr = &fl
	}
	if mediaKeyTimestamp.Valid {
		mkt := mediaKeyTimestamp.Int64
		*mediaKeyTimestampPtr = &mkt
	}
}

func strPtr(s string) *string { return &s }

// canonicalExtensions pins the extension for the mime types WhatsApp actually
// sends, so the result does not depend on the host's mime database.
//
// This exists because mime.ExtensionsByType is platform-dependent: on Windows
// it reads HKEY_CLASSES_ROOT\MIME\Database\Content Type\<type> and returns the
// matches sorted alphabetically. For image/jpeg that yields ".jfif" before
// ".jpeg"/".jpg", so downloads landed as .jfif — an extension many tools refuse
// to treat as an image, even though the bytes are ordinary JPEG. On Linux/macOS
// the same call returns ".jpeg", which is why this went unnoticed upstream.
//
// The same is true beyond Windows: Linux's mime database lists text/html as
// ".ehtml" first, so a received HTML file was saved as "....ehtml" — an
// extension nothing opens — and forwarded under that name. Every type people
// commonly send is pinned here for that reason.
var canonicalExtensions = map[string]string{
	"image/jpeg":                   ".jpg",
	"image/png":                    ".png",
	"image/gif":                    ".gif",
	"image/webp":                   ".webp",
	"image/heic":                   ".heic",
	"video/mp4":                    ".mp4",
	"video/3gpp":                   ".3gp",
	"video/quicktime":              ".mov",
	"audio/ogg":                    ".ogg",
	"audio/mpeg":                   ".mp3",
	"audio/mp4":                    ".m4a",
	"audio/aac":                    ".aac",
	"audio/amr":                    ".amr",
	"audio/wav":                    ".wav",
	"audio/x-wav":                  ".wav",
	"application/pdf":              ".pdf",
	"text/html":                    ".html",
	"text/plain":                   ".txt",
	"text/csv":                     ".csv",
	"text/xml":                     ".xml",
	"application/xml":              ".xml",
	"application/json":             ".json",
	"text/markdown":                ".md",
	"application/rtf":              ".rtf",
	"text/rtf":                     ".rtf",
	"application/zip":              ".zip",
	"application/x-zip-compressed": ".zip",
	"application/vnd.rar":          ".rar",
	"application/x-rar-compressed": ".rar",
	"application/x-7z-compressed":  ".7z",
	"application/msword":           ".doc",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
	"application/vnd.ms-excel": ".xls",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         ".xlsx",
	"application/vnd.ms-powerpoint":                                             ".ppt",
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
	"application/vnd.oasis.opendocument.text":                                   ".odt",
	"application/vnd.oasis.opendocument.spreadsheet":                            ".ods",
	"application/vnd.oasis.opendocument.presentation":                           ".odp",
	"application/vnd.android.package-archive":                                   ".apk",
}

// extensionForMime returns a filesystem extension for a given mime type,
// falling back to type-based defaults and finally ".bin".
func extensionForMime(mt, msgType string) string {
	if mt != "" {
		if i := strings.IndexByte(mt, ';'); i >= 0 {
			mt = strings.TrimSpace(mt[:i])
		}
		mt = strings.ToLower(strings.TrimSpace(mt))
		if ext, ok := canonicalExtensions[mt]; ok {
			return ext
		}
		if exts, _ := mime.ExtensionsByType(mt); len(exts) > 0 {
			return exts[0]
		}
	}
	switch msgType {
	case "image":
		return ".jpg"
	case "video":
		return ".mp4"
	case "document":
		return ".bin"
	case "audio", "voice":
		return ".ogg"
	case "sticker":
		return ".webp"
	}
	return ".bin"
}

// downloadMediaResponse is the JSON returned by POST /api/media/download.
type downloadMediaResponse struct {
	MessageID string `json:"message_id"`
	Path      string `json:"path"`
	Mime      string `json:"mime,omitempty"`
	Size      int64  `json:"size"`
	CachedHit bool   `json:"cached_hit,omitempty"`

	// What a caller needs to name, file or show the download without a second
	// query: a copy saved to Drive is named after Filename, and a download
	// link serves it under that name.
	Filename string `json:"filename"`
	// OriginalFilename is false when Filename was generated, which is always
	// the case for photos, videos and voice notes, and for documents stored
	// before migration 007.
	OriginalFilename   bool   `json:"original_filename"`
	Type               string `json:"type"`
	ChatJID            string `json:"chat_jid"`
	ChatName           string `json:"chat_name,omitempty"`
	SenderJID          string `json:"sender_jid,omitempty"`
	SenderDisplay      string `json:"sender_display,omitempty"`
	IsFromMe           bool   `json:"is_from_me"`
	Timestamp          int64  `json:"timestamp"`
	RecoveredFromPhone bool   `json:"recovered_from_phone,omitempty"`
}

// handleDownloadMedia fetches + decrypts media bytes for a single message
// and persists them under the bridge's media folder. The Python pipeline
// (receipts watcher) calls this after seeing a new image/document in
// list_messages to materialize the file before sending to vision.
//
// The download runs as a job. A file that arrives within the wait is answered
// exactly as before; a larger one gets 202 with progress, and calling again
// with the same message_id joins the running download instead of starting a
// second one. Once it lands, the next call takes the fast path above.
func (s *Server) handleDownloadMedia(w http.ResponseWriter, r *http.Request) {
	type req struct {
		MessageID string `json:"message_id"`
	}
	var body req
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<14)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if body.MessageID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "message_id required"})
		return
	}
	if s.bridge == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "bridge not ready"})
		return
	}

	key := "download:" + body.MessageID
	// A download that failed after its caller stopped waiting keeps its
	// answer for the next request, once. Asking the phone alone can take two
	// minutes, longer than any request waits, and until 2026-10-22 a failed
	// job was dropped the moment it ended: every poll started a fresh
	// attempt, and a caller saw "downloading" for over an hour. A failure a
	// caller already got is not repeated; the request after it tries again.
	if prev := s.jobs().get(key); prev != nil && !prev.running() {
		s.jobs().forget(key, prev)
		// Only a recent one: a "turn on the phone" from an hour ago is no
		// answer to a request made with the phone on.
		if time.Since(prev.finished) < failedDownloadReplay && !prev.delivered.Swap(true) {
			writeJSON(w, prev.status, prev.body)
			return
		}
	}
	j, _ := s.jobs().start(key, func(j *mediaJob) (int, any) {
		ctx, cancel := context.WithTimeout(s.rootContext(), mediaJobTimeout)
		defer cancel()
		stop, stalled := watchStall(cancel, j.activity, stallTimeout)
		resp, err := s.bridge.DownloadMediaFile(ctx, body.MessageID, j)
		stop()
		if err != nil {
			if stalled.Load() {
				err = fmt.Errorf("la descarga se detuvo: no llegaron datos en %s", stallTimeout)
			}
			return downloadErrorStatus(err), downloadErrorBody(err)
		}
		// The outcome is in the messages table now, so the next request
		// takes the fast path instead of this job's answer.
		s.jobs().forget(key, j)
		return http.StatusOK, resp
	})
	answerJob(w, r, j, jobPending{
		MessageID: body.MessageID,
		Status:    "downloading",
		Hint:      "sigue descargando; vuelve a pedir el mismo message_id para obtener el archivo",
	})
}

// failedDownloadReplay is how long a failed download's answer waits for the
// caller who asked for it (see handleDownloadMedia).
const failedDownloadReplay = 10 * time.Minute

// downloadErrorStatus keeps the statuses callers already rely on: 404 for a
// message that has nothing to download, 410 for media WhatsApp no longer has,
// 500 for everything else.
func downloadErrorStatus(err error) int {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "message not found"),
		strings.Contains(msg, "media key not available"),
		strings.Contains(msg, "is not downloadable"):
		return http.StatusNotFound
	case errors.Is(err, errMediaGone):
		return http.StatusGone
	case errors.Is(err, errPhoneUnreachable):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func downloadErrorBody(err error) errorResponse {
	switch downloadErrorStatus(err) {
	case http.StatusNotFound:
		return errorResponse{Error: err.Error()}
	case http.StatusGone:
		return errorResponse{
			Error:   "media no longer available",
			Details: err.Error() + ". Pidele a quien lo envio que lo mande otra vez.",
		}
	case http.StatusServiceUnavailable:
		next := ". Vuelve a intentarlo con el telefono encendido y con internet."
		if strings.Contains(err.Error(), "con internet") {
			next = ". Vuelve a intentarlo en unos minutos."
		}
		return errorResponse{Error: "phone did not supply the media", Details: err.Error() + next}
	default:
		if isMismatchedMedia(err) {
			// whatsmeow's words ("invalid media hmac") tell a person nothing,
			// and the cause may be the transfer rather than the file.
			return errorResponse{Error: "download failed", Details: "el archivo que llego de WhatsApp no paso la " +
				"verificacion (pudo danarse en el camino); vuelve a intentarlo. Si sigue fallando, pidele a quien " +
				"lo envio que lo mande otra vez"}
		}
		return errorResponse{Error: "download failed", Details: err.Error()}
	}
}

// mediaTypeLabels name a generated file after what it is, in the user's
// language.
var mediaTypeLabels = map[string]string{
	"image":    "Imagen",
	"video":    "Video",
	"audio":    "Audio",
	"voice":    "Nota de voz",
	"document": "Documento",
	"sticker":  "Sticker",
}

// displayFilename returns the name to show for a message's media and whether
// it is the sender's own. Documents carry a name; photos, videos and voice
// notes never do, so they are named like WhatsApp names them on export — what
// they are plus when — which also sorts sensibly in a folder.
func displayFilename(stored, msgType, mimeType string, ts int64) (string, bool) {
	ext := extensionForMime(mimeType, msgType)
	if name := cleanMediaFilename(stored); name != "" {
		if filepath.Ext(name) == "" {
			name += ext
		}
		return name, true
	}
	label := mediaTypeLabels[msgType]
	if label == "" {
		label = "Archivo"
	}
	return label + " " + time.Unix(ts, 0).Local().Format("2006-01-02 15.04.05") + ext, false
}

// maxFilenameBytes keeps names well inside every filesystem's 255-byte limit,
// with room for the " (2)" a Drive copy may need.
const maxFilenameBytes = 180

// cleanMediaFilename makes a sender-chosen name safe to use as a filename on
// any OS and in a Content-Disposition header. The name is attacker-chosen
// text: anyone can send a document called "../../.bashrc". Path separators and
// the characters Windows refuses become "_", control characters are dropped,
// leading and trailing dots and spaces are trimmed, and the result is bounded.
// Returns "" when nothing usable is left.
//
// Not export_vault.go's sanitizeFilename: that one builds vault note names and
// also rewrites brackets and percent signs, which a document name the user
// will recognise should keep. Both fold to NFC, for the reason given there.
func cleanMediaFilename(name string) string {
	var b strings.Builder
	for _, r := range norm.NFC.String(name) {
		switch {
		case r < 0x20 || r == 0x7f:
			// Dropped: a newline in a filename is never intended.
		case strings.ContainsRune(`/\:*?"<>|`, r):
			b.WriteRune('_')
		default:
			b.WriteRune(r)
		}
	}
	s := strings.Trim(b.String(), ". ")
	if len(s) > maxFilenameBytes {
		ext := filepath.Ext(s)
		if len(ext) > 16 {
			ext = ""
		}
		stem := s[:len(s)-len(ext)]
		n := maxFilenameBytes - len(ext)
		for n > 0 && !utf8.RuneStart(stem[n]) {
			n--
		}
		s = strings.TrimRight(stem[:n], ". ") + ext
	}
	return s
}

// safeMediaStem reduces a WhatsApp message ID to a filename component that
// cannot leave MediaPath.
//
// The ID is NOT ours: whatsmeow lifts it verbatim off the wire
// (message.go `info.ID = types.MessageID(ag.String("id"))`) and bridge.go
// stores it unmodified, so it is chosen by the SENDING client. Real IDs are
// hex-ish, but nothing in the protocol enforces that, and filepath.Join
// CLEANS a path rather than confining it — a stem of `..\..\evil` resolved
// outside the media directory on Windows (where `\` separates too) and
// `../../evil` did the same everywhere. Combined with the unauthenticated
// loopback API (issue #34), that is an arbitrary file write with
// attacker-supplied bytes.
//
// Conservative allowlist rather than a `..` blocklist: anything outside
// [A-Za-z0-9._-] becomes `_`, leading dots are stripped so no stem can be
// `.` or `..`, and the result is length-capped. Distinctness is preserved by
// appending a hash of the original whenever anything changed, so two hostile
// IDs cannot collide onto one file.
func safeMediaStem(id string) string {
	safe := make([]rune, 0, len(id))
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			safe = append(safe, r)
		default:
			safe = append(safe, '_')
		}
	}
	stem := strings.TrimLeft(string(safe), ".")
	if len(stem) > 128 {
		stem = stem[:128]
	}
	if stem != id {
		sum := sha256.Sum256([]byte(id))
		if stem == "" {
			stem = "media"
		}
		stem = fmt.Sprintf("%s-%x", stem, sum[:8])
	}
	return stem
}
