// JID alias bookkeeping.
//
// WhatsApp now exposes most direct chats under TWO JID forms simultaneously:
// the legacy phone-number form (`<phone>@s.whatsapp.net`) and the privacy-by-
// default LID form (`<opaque>@lid`). Recent traffic for a given human can
// land on either, depending on how the contact's account is configured. The
// bridge cannot collapse the two at storage time without re-keying historical
// rows, so we keep them as separate contact / chat rows and maintain a
// separate `jid_aliases` table that records "these JIDs are the same human."
//
// Lookups (search_contacts, list_messages) join through this table to merge
// the two histories at query time.
//
// Sources of alias edges:
//   1. Live message events. `MessageSource.SenderAlt` and `RecipientAlt` are
//      populated by whatsmeow whenever the server includes the dual form.
//   2. Startup backfill. We walk the contacts table and ask
//      `client.Store.LIDs.GetPNForLID` / `GetLIDForPN` for each entry; any
//      mapping the local store already knows about gets recorded.
//
// Edges are stored symmetrically (both (A,B) and (B,A)) so a single PK lookup
// resolves either direction.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// recordJIDAlias inserts a symmetric alias edge between two JIDs. No-ops when
// either side is empty / identical. Idempotent via PRIMARY KEY.
func recordJIDAlias(ctx context.Context, db *sql.DB, a, b types.JID, source string, ts int64) {
	if a.IsEmpty() || b.IsEmpty() {
		return
	}
	aStr := a.String()
	bStr := b.String()
	if aStr == "" || bStr == "" || aStr == bStr {
		return
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO jid_aliases (jid_a, jid_b, discovered_at, source)
		VALUES (?, ?, ?, ?), (?, ?, ?, ?)
		ON CONFLICT(jid_a, jid_b) DO NOTHING
	`, aStr, bStr, ts, source, bStr, aStr, ts, source)
	if err != nil {
		log.Printf("recordJIDAlias: insert failed (%s ↔ %s): %v", aStr, bStr, err)
	}
}

// resolveAliases returns every JID known to refer to the same human as `jid`,
// including `jid` itself: everything connected to it through jid_aliases (see
// aliasGroups). `jid` comes first and the rest are sorted, so callers can pass
// the slice straight into a parameterised IN-clause.
func resolveAliases(ctx context.Context, db *sql.DB, jid string) ([]string, error) {
	if jid == "" {
		return nil, nil
	}
	keys, members, err := aliasGroups(ctx, db, []string{jid})
	out := []string{jid}
	for _, alt := range members[keys[jid]] {
		if alt != jid {
			out = append(out, alt)
		}
	}
	if err != nil {
		return out, fmt.Errorf("resolveAliases: %w", err)
	}
	return out, nil
}

// maxAliasHops bounds how far aliasGroups follows jid_aliases from a JID. A
// person is usually two JIDs, a phone JID and a @lid; a @lid seen with an old
// and a new number links three, two hops apart. Nothing real is further.
const maxAliasHops = 4

// aliasBatch is the most JIDs one alias or chat lookup puts in an IN clause.
const aliasBatch = 500

// aliasGroups returns, for each of jids, the key of the person it belongs to,
// and each key's JIDs, sorted: everything connected to it through jid_aliases,
// not only its direct aliases. The key is the smallest of them, so every form
// of one person gets the same key whichever form is at hand.
//
// Direct aliases were enough while a person was one phone JID and one @lid.
// A @lid linked to an old and a new number is three, and keying each JID by
// its direct aliases alone split the two numbers whenever the @lid did not
// sort first: lists showed the person twice, or lost one of their chats
// (2026-10-21 review).
//
// A failed lookup leaves the JIDs it concerned ungrouped and is returned
// after everything else has been resolved: a person shown twice beats a list
// that fails.
func aliasGroups(ctx context.Context, db *sql.DB, jids []string) (keys map[string]string, members map[string][]string, err error) {
	// Union-find whose roots are the smallest JID of their group.
	parent := map[string]string{}
	find := func(j string) string {
		for parent[j] != j {
			parent[j] = parent[parent[j]]
			j = parent[j]
		}
		return j
	}
	add := func(j string) bool {
		if _, ok := parent[j]; ok {
			return false
		}
		parent[j] = j
		return true
	}
	union := func(a, b string) {
		ra, rb := find(a), find(b)
		if rb < ra {
			ra, rb = rb, ra
		}
		parent[rb] = ra
	}

	var frontier []string
	for _, j := range jids {
		if j != "" && add(j) {
			frontier = append(frontier, j)
		}
	}
	for hop := 0; hop < maxAliasHops && len(frontier) > 0; hop++ {
		var next []string
		for start := 0; start < len(frontier); start += aliasBatch {
			batch := frontier[start:min(start+aliasBatch, len(frontier))]
			rows, qErr := db.QueryContext(ctx,
				`SELECT jid_a, jid_b FROM jid_aliases WHERE jid_a IN (`+inClausePlaceholders(len(batch))+`)`,
				jidsToArgs(batch)...)
			if qErr != nil {
				if err == nil {
					err = qErr
				}
				continue
			}
			for rows.Next() {
				var a, b string
				if rows.Scan(&a, &b) != nil || b == "" {
					continue
				}
				if add(b) {
					next = append(next, b)
				}
				union(a, b)
			}
			if rErr := rows.Err(); rErr != nil && err == nil {
				err = rErr
			}
			rows.Close()
		}
		frontier = next
	}

	keys = make(map[string]string, len(jids))
	members = map[string][]string{}
	for j := range parent {
		root := find(j)
		members[root] = append(members[root], j)
	}
	for _, group := range members {
		sort.Strings(group)
	}
	for _, j := range jids {
		if j != "" {
			keys[j] = find(j)
		}
	}
	return keys, members, err
}

// inClausePlaceholders returns "?,?,?" for n=3 etc. for parameterised IN().
func inClausePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

// jidsToArgs widens []string into []interface{} for sql.QueryContext varargs.
func jidsToArgs(jids []string) []any {
	args := make([]any, len(jids))
	for i, j := range jids {
		args[i] = j
	}
	return args
}

// BackfillJIDAliases walks every contact in the bridge DB and asks
// whatsmeow's local LID store for the alternative form. Any mapping the
// store already knows gets written into jid_aliases. Whatsmeow populates
// the LID store opportunistically as messages flow through, so this
// backfill captures everything seen since the last bridge restart.
//
// As a side-effect, BackfillJIDAliases also REPAIRS the contacts row for
// any LID-form contact whose `phone` column was previously populated with
// the LID number (the bug introduced in onMessage when whatsmeow's
// `Sender.User` was stored verbatim regardless of server type). When we
// learn the real phone via the alias, we update the contacts row.
//
// Returns (aliasesWritten, repairsApplied, error).
func BackfillJIDAliases(ctx context.Context, db *sql.DB, client *whatsmeow.Client) (int, int, error) {
	if client == nil || client.Store == nil || client.Store.LIDs == nil {
		return 0, 0, fmt.Errorf("BackfillJIDAliases: whatsmeow store not initialised")
	}

	rows, err := db.QueryContext(ctx, `SELECT jid FROM contacts`)
	if err != nil {
		return 0, 0, fmt.Errorf("query contacts: %w", err)
	}
	defer rows.Close()

	var contactJIDs []string
	for rows.Next() {
		var j string
		if err := rows.Scan(&j); err != nil {
			continue
		}
		contactJIDs = append(contactJIDs, j)
	}
	rows.Close()

	now := time.Now().Unix()
	written := 0
	repaired := 0

	for _, jidStr := range contactJIDs {
		parsed, err := types.ParseJID(jidStr)
		if err != nil {
			continue
		}
		var alt types.JID
		switch parsed.Server {
		case types.HiddenUserServer:
			alt, err = client.Store.LIDs.GetPNForLID(ctx, parsed)
		case types.DefaultUserServer:
			alt, err = client.Store.LIDs.GetLIDForPN(ctx, parsed)
		default:
			continue
		}
		if err != nil || alt.IsEmpty() {
			continue
		}

		// Write both directions. Track whether we actually inserted (for log
		// accuracy) by checking row affected count via a separate statement.
		res, err := db.ExecContext(ctx, `
			INSERT INTO jid_aliases (jid_a, jid_b, discovered_at, source)
			VALUES (?, ?, ?, ?), (?, ?, ?, ?)
			ON CONFLICT(jid_a, jid_b) DO NOTHING
		`, parsed.String(), alt.String(), now, "lid_store_lookup",
			alt.String(), parsed.String(), now, "lid_store_lookup")
		if err == nil {
			n, _ := res.RowsAffected()
			written += int(n)
		}

		// Phone-column repair. If `parsed` is a LID and `alt` is a phone JID,
		// the contacts row for `parsed` likely has phone=<lid_number>. Fix it.
		if parsed.Server == types.HiddenUserServer && alt.Server == types.DefaultUserServer {
			res, repairErr := db.ExecContext(ctx, `
				UPDATE contacts
				SET phone = ?, updated_at = ?
				WHERE jid = ? AND (phone IS NULL OR phone = ?)
			`, alt.User, now, parsed.String(), parsed.User)
			if repairErr == nil {
				if n, _ := res.RowsAffected(); n > 0 {
					repaired += int(n)
				}
			}
		}
	}

	return written, repaired, nil
}

// AliasCoverage returns counts used by the healthcheck endpoint to surface
// alias-table health. Cheap; runs against indexed columns only.
type AliasCoverage struct {
	TotalEdges          int `json:"total_edges"`
	ContactsWithAlias   int `json:"contacts_with_alias"`
	DirectChatsLID      int `json:"direct_chats_lid"`
	DirectChatsPhone    int `json:"direct_chats_phone"`
	SuspiciousLIDPhones int `json:"suspicious_lid_phones"`
}

// computeAliasCoverage runs the diagnostic queries. SuspiciousLIDPhones is the
// guard counter: contacts where the row is a LID-form JID but the `phone`
// column equals the LID's user portion (i.e. the legacy ingestion bug we
// just fixed). When non-zero, BackfillJIDAliases hasn't yet healed every row
// — usually because whatsmeow's local LID store doesn't know that pair yet.
func computeAliasCoverage(ctx context.Context, db *sql.DB) (AliasCoverage, error) {
	var c AliasCoverage
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jid_aliases`).Scan(&c.TotalEdges); err != nil {
		return c, err
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT jid_a) FROM jid_aliases
	`).Scan(&c.ContactsWithAlias); err != nil {
		return c, err
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM chats WHERE chat_type='direct' AND jid LIKE '%@lid'
	`).Scan(&c.DirectChatsLID); err != nil {
		return c, err
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM chats WHERE chat_type='direct' AND jid LIKE '%@s.whatsapp.net'
	`).Scan(&c.DirectChatsPhone); err != nil {
		return c, err
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM contacts
		WHERE jid LIKE '%@lid'
		  AND phone IS NOT NULL
		  AND phone = SUBSTR(jid, 1, INSTR(jid,'@')-1)
	`).Scan(&c.SuspiciousLIDPhones); err != nil {
		return c, err
	}
	return c, nil
}
