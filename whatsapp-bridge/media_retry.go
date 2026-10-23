// media_retry.go — getting an attachment back after WhatsApp's CDN dropped it.
//
// WhatsApp keeps media on its servers for a limited time. After that the
// stored direct path answers 403, 404 or 410, and on this deployment that was
// the most common way download_media failed: ten of the failures in the audit
// log as of 2026-10-21, all on older messages. The official clients recover by
// asking the phone, which usually still holds the file, to upload it again. A
// "media retry" receipt goes to our own primary device, and the phone answers
// with a notification carrying a fresh direct path. whatsmeow implements both
// halves (SendMediaRetryReceipt, DecryptMediaRetryNotification); this file
// connects them to the download path.
//
// It only works while the phone is online and still has the file. When it
// does not, the error says which, because asking the sender to send the file
// again is then the only fix left.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// mediaRetryTimeout is how long to wait for the phone. It has to find the file
// and upload it again over its own connection, which for a large video on
// mobile data is not quick.
const mediaRetryTimeout = 2 * time.Minute

// errNotOnPhone is the one answer from the phone that is final: it no longer
// has the file. Every other failure — no answer, a receipt that could not go
// out, an unreadable reply — may well succeed later, and is reported as such.
var errNotOnPhone = errors.New("el telefono ya no tiene el archivo")

// mediaRetryWaiters routes each MediaRetry notification to the download that
// asked for it. The zero value is ready to use.
type mediaRetryWaiters struct {
	mu sync.Mutex
	m  map[types.MessageID]chan *events.MediaRetry
}

func (w *mediaRetryWaiters) add(id types.MessageID) chan *events.MediaRetry {
	ch := make(chan *events.MediaRetry, 1)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.m == nil {
		w.m = make(map[types.MessageID]chan *events.MediaRetry)
	}
	w.m[id] = ch
	return ch
}

func (w *mediaRetryWaiters) remove(id types.MessageID, ch chan *events.MediaRetry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.m[id] == ch {
		delete(w.m, id)
	}
}

// deliver hands evt to whoever is waiting for its message and reports whether
// anyone was.
func (w *mediaRetryWaiters) deliver(evt *events.MediaRetry) bool {
	w.mu.Lock()
	ch := w.m[evt.MessageID]
	w.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- evt:
	default: // a duplicate notification; the first one is already queued
	}
	return true
}

func (b *Bridge) onMediaRetry(evt *events.MediaRetry) {
	if !b.retries.deliver(evt) {
		// Nothing asked for this one, e.g. a retry whose download already
		// timed out. Harmless, but worth a line when debugging.
		log.Printf("media retry for %s arrived with no download waiting for it", evt.MessageID)
	}
}

// isGoneFromCDN reports whether WhatsApp's CDN no longer has the file at all:
// starting the download over cannot help.
func isGoneFromCDN(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

// isExpiredMedia reports whether a download failed in a way a fresh upload
// from the phone can fix: the CDN no longer has the file, or still serves a
// copy whose ciphertext hash does not match after the download was started
// over from scratch. That second case is what old attachments failed with on
// 2026-10-22, from every CDN host alike. A MAC or plaintext mismatch is not
// included: whatsmeow reports those for a row missing its plaintext hash and
// for a download that changed hosts mid-file, neither of which the phone's
// copy would fix.
func isExpiredMedia(err error) bool {
	return isGoneFromCDN(err) || errors.Is(err, whatsmeow.ErrInvalidMediaEncSHA256)
}

// isMismatchedMedia reports whether the bytes WhatsApp served are not the ones
// the message describes, whatever the check that caught it.
func isMismatchedMedia(err error) bool {
	return errors.Is(err, whatsmeow.ErrInvalidMediaEncSHA256) ||
		errors.Is(err, whatsmeow.ErrInvalidMediaHMAC) ||
		errors.Is(err, whatsmeow.ErrInvalidMediaSHA256)
}

// cdnProblem says, in the user's language, why WhatsApp's copy could not be
// used: the raw errors ("hash of media ciphertext doesn't match") tell a
// person nothing.
func cdnProblem(err error) string {
	if isMismatchedMedia(err) {
		return "la copia del archivo en WhatsApp no coincide con la del mensaje (cambio o se dano)"
	}
	return "WhatsApp ya no lo tiene en sus servidores"
}

// requestMediaReupload asks our own phone to upload a message's media again
// and returns the fresh direct path.
func (b *Bridge) requestMediaReupload(ctx context.Context, info *types.MessageInfo, mediaKey []byte) (string, error) {
	// Registered before the receipt goes out, so an answer that arrives
	// faster than this goroutine is scheduled is not lost.
	ch := b.retries.add(info.ID)
	defer b.retries.remove(info.ID, ch)

	if err := b.client.SendMediaRetryReceipt(ctx, info, mediaKey); err != nil {
		return "", fmt.Errorf("no se pudo pedir el archivo al telefono: %w", err)
	}

	timer := time.NewTimer(mediaRetryTimeout)
	defer timer.Stop()
	select {
	case evt := <-ch:
		notif, err := whatsmeow.DecryptMediaRetryNotification(evt, mediaKey)
		if errors.Is(err, whatsmeow.ErrMediaNotAvailableOnPhone) {
			return "", errNotOnPhone
		}
		if err != nil {
			return "", fmt.Errorf("la respuesta del telefono no se pudo leer: %w", err)
		}
		switch notif.GetResult() {
		case waMmsRetry.MediaRetryNotification_SUCCESS:
		case waMmsRetry.MediaRetryNotification_NOT_FOUND:
			return "", errNotOnPhone
		default:
			return "", fmt.Errorf("el telefono no pudo volver a subir el archivo (%s)", notif.GetResult())
		}
		if notif.GetDirectPath() == "" {
			return "", errors.New("el telefono respondio sin una ruta nueva para el archivo")
		}
		return notif.GetDirectPath(), nil
	case <-timer.C:
		return "", fmt.Errorf("el telefono no respondio en %s; tiene que estar encendido y con internet", mediaRetryTimeout)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// messageInfoForRetry rebuilds the MessageInfo a retry receipt needs from what
// the messages table stores. The receipt names the chat and, in a group, the
// participant who sent the message, so the phone can find it.
func messageInfoForRetry(id, chatJID, senderJID string, fromMe bool) (*types.MessageInfo, error) {
	chat, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("chat %q: %w", chatJID, err)
	}
	info := &types.MessageInfo{
		ID: types.MessageID(id),
		MessageSource: types.MessageSource{
			Chat:     chat,
			IsFromMe: fromMe,
			// whatsmeow counts broadcast lists and status updates as groups
			// too (message.go), and a receipt for one has to name the sender
			// the same way.
			IsGroup: chat.Server == types.GroupServer || chat.Server == types.BroadcastServer,
		},
	}
	if senderJID != "" {
		if sender, err := types.ParseJID(senderJID); err == nil {
			info.Sender = sender
		}
	}
	if info.IsGroup && info.Sender.IsEmpty() {
		return nil, fmt.Errorf("message %s in %s has no sender to name in the retry", id, chatJID)
	}
	return info, nil
}
