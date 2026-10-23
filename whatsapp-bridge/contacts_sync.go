// contacts_sync.go — the user's own address book.
//
// The bridge knew every contact by the name that contact chose for themselves
// (push_name) and by nothing else. That is rarely the name the user thinks in:
// a partner saved as "Mi Cielo" broadcasts "Dra Laura De La Rosa", a plumber
// saved as "Plomero Casa" broadcasts a company slogan. Asking the bridge for
// "Mi Cielo" returned zero results, and the fallback — supply a phone number
// instead — asks the user to remember the one thing an address book exists so
// they never have to.
//
// The data was already on this machine the whole time. WhatsApp syncs the
// address book through app state and whatsmeow keeps it in its own
// ContactStore, where types.ContactInfo carries FullName and FirstName next to
// PushName. The bridge had zero references to Store.Contacts. This file is the
// missing read.
//
// Two entry points, one write path:
//
//   - SyncContactNames, once at startup, sweeps the whole store. This is what
//     retroactively names chats that have been anonymous for months.
//   - onContactUpdate, on events.Contact, keeps a rename made on the phone from
//     waiting for the next restart.
//
// The address book beats push_name for a second reason beyond being the name
// the user recognises: it does not need the other person to say anything. A
// chat where every message is outgoing — 0 incoming out of 40 is a real case on
// this account — can never learn a push_name, because push_name only arrives
// attached to an inbound message. The address book names it anyway.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// execer is satisfied by both *sql.DB and *sql.Tx, so the startup sweep (one
// transaction over thousands of contacts) and the live event (a single
// statement) share one write path and cannot drift apart.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// addressBookName picks the label to show for an address-book entry.
//
// FullName first, FirstName as the fallback: WhatsApp stores both when the
// phone's contact record has them, and a user who saved someone as just
// "Mariana" has that in FirstName with FullName empty.
func addressBookName(fullName, firstName string) string {
	if n := strings.TrimSpace(fullName); n != "" {
		return n
	}
	return strings.TrimSpace(firstName)
}

// writeContactName records one address-book entry and renames the chats it
// belongs to. Returns how many chat rows were renamed.
//
// An empty name is meaningful, not a no-op: it means the entry was removed from
// the address book, so the columns go back to NULL and naming falls through to
// push_name again. Keeping a label the user deleted would be worse than having
// none.
//
// Groups are excluded explicitly. A contact JID should never match a group row,
// but the cost of being wrong is overwriting a group's subject with a member's
// name, which is the exact failure this whole line of work started from.
func writeContactName(ctx context.Context, ex execer, jid, name string, now int64) (int64, error) {
	var full, norm sql.NullString
	if name != "" {
		full = sql.NullString{String: name, Valid: true}
		norm = sql.NullString{String: Normalize(name), Valid: true}
	}

	if _, err := ex.ExecContext(ctx, `
		INSERT INTO contacts (jid, full_name, normalized_full_name, is_business, created_at, updated_at)
		VALUES (?, ?, ?, 0, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET
			full_name            = excluded.full_name,
			normalized_full_name = excluded.normalized_full_name,
			updated_at           = excluded.updated_at
	`, jid, full, norm, now, now); err != nil {
		return 0, fmt.Errorf("contact %s: %w", jid, err)
	}

	// The alias subquery is what makes this land on the chat the user actually
	// sees. Most direct chats now live under a @lid JID while the address-book
	// entry is keyed by the phone-number JID (or the reverse), so matching on
	// jid alone would name a row nobody is looking at. See aliases.go.
	res, err := ex.ExecContext(ctx, `
		UPDATE chats
		   SET name = ?, normalized_name = ?, updated_at = ?
		 WHERE chat_type != 'group'
		   AND (jid = ? OR jid IN (SELECT jid_b FROM jid_aliases WHERE jid_a = ?))
	`, full, norm, now, jid, jid)
	if err != nil {
		return 0, fmt.Errorf("renaming chats for %s: %w", jid, err)
	}
	renamed, _ := res.RowsAffected()
	return renamed, nil
}

// SyncContactNames copies the whole address book out of whatsmeow's store into
// the bridge's own tables. Idempotent; safe to run on every start.
//
// Runs in one transaction. Thousands of contacts means thousands of statement
// pairs, and SQLite in autocommit mode would fsync after each one.
func SyncContactNames(ctx context.Context, db *sql.DB, client *whatsmeow.Client) (named, chatsRenamed int, err error) {
	if client == nil || client.Store == nil || client.Store.Contacts == nil {
		return 0, 0, fmt.Errorf("SyncContactNames: whatsmeow contact store not initialised")
	}

	all, err := client.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("SyncContactNames: reading whatsmeow contact store: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("SyncContactNames: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().Unix()
	for jid, info := range all {
		name := addressBookName(info.FullName, info.FirstName)
		if name == "" {
			// Not in the address book — whatsmeow also caches push names here,
			// and those are already the bridge's job to record from messages.
			continue
		}
		renamed, wErr := writeContactName(ctx, tx, jid.ToNonAD().String(), name, now)
		if wErr != nil {
			return named, chatsRenamed, wErr
		}
		named++
		chatsRenamed += int(renamed)
	}

	if err := tx.Commit(); err != nil {
		return named, chatsRenamed, fmt.Errorf("SyncContactNames: commit: %w", err)
	}
	return named, chatsRenamed, nil
}

// onContactUpdate applies a single address-book change made on another device.
func (b *Bridge) onContactUpdate(evt *events.Contact) {
	// The getters are nil-safe, so a removal (which arrives with no action)
	// yields "" and correctly clears the stored name.
	name := addressBookName(evt.Action.GetFullName(), evt.Action.GetFirstName())
	jid := evt.JID.ToNonAD().String()

	renamed, err := writeContactName(b.rootCtx, b.db, jid, name, time.Now().Unix())
	if err != nil {
		log.Printf("onContactUpdate %s: %v", jid, err)
		return
	}
	if name == "" {
		log.Printf("contact %s removed from the address book; %d chat(s) reverted to push name", jid, renamed)
		return
	}
	log.Printf("contact %s renamed to %q; %d chat(s) updated", jid, name, renamed)
}

// addressBookNameFor returns the user's own name for a JID, or "" when the
// contact is not in the address book.
//
// Reads the bridge's tables rather than whatsmeow's store so that the store is
// touched in exactly one place (the sync above) and the hot receive path keeps
// a single database dependency. Costs one indexed lookup per direct message.
func (b *Bridge) addressBookNameFor(jid types.JID) string {
	key := jid.ToNonAD().String()
	var name sql.NullString
	err := b.db.QueryRow(`
		SELECT full_name FROM contacts
		 WHERE full_name IS NOT NULL
		   AND (jid = ? OR jid IN (SELECT jid_b FROM jid_aliases WHERE jid_a = ?))
		 LIMIT 1
	`, key, key).Scan(&name)
	if err != nil || !name.Valid {
		return ""
	}
	return strings.TrimSpace(name.String)
}

// chatNameFor picks the best name available for a chat at this moment.
//
// Order matches what the WhatsApp client itself shows: the user's own label
// wins, the contact's self-chosen push name is the fallback, and nothing is an
// acceptable answer. Non-direct chats always return "" — their subject comes
// from history sync / SyncGroupMetadata, and a participant's name is never a
// group's (or broadcast's, or newsletter's) name.
//
// Gated on chatTypeFromJID rather than evt.Info.IsGroup deliberately (MYC-3555):
// whatsmeow's IsGroup flag only covers the group-server case, but
// chatTypeFromJID — the same function that decides the chat_type column two
// lines below in onMessage, and the same boundary export_vault.go
// disambiguates filenames on — treats broadcast and newsletter chats as
// equally non-direct. A broadcast/newsletter chat named after whoever's
// message the bridge happened to see first is the identical
// collides-with-a-direct-chat failure a group named that way was.
func (b *Bridge) chatNameFor(evt *events.Message) string {
	if chatTypeFromJID(evt.Info.Chat) != "direct" {
		return ""
	}
	// Keyed on the CHAT, not the sender, which is why this works where
	// push_name cannot: on an outgoing message the sender is the account owner
	// and says nothing about the other party, but the chat JID still identifies
	// them and the address book still knows their name.
	if n := b.addressBookNameFor(evt.Info.Chat); n != "" {
		return n
	}
	return chatNameFromMessage(evt.Info.IsGroup, evt.Info.IsFromMe, evt.Info.PushName)
}
