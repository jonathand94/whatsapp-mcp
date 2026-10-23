// chat_lookup.go — one person, one entry: merging aliases in chat and contact
// lists, and finding chats (groups included) by name.
//
// WhatsApp knows most people under two JIDs, a phone JID and a @lid, and the
// bridge records the pair in jid_aliases. list_messages has always merged the
// two (merged_jids); the lists did not. list_chats showed the same person
// twice, the user's own chat included, and search_contacts returned one person
// up to five times: the @lid plus a row per linked device (":79", ":80"...).
// An assistant picking "the chat with Carlos" from that has to guess.
//
// Groups had no lookup at all: search_contacts searches people, so finding
// "Sociedad Médica del Norte" meant listing 200 chats and reading them.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"go.mau.fi/whatsmeow/types"
)

// nonADJID drops the device part of a JID ("999909829054513:79@lid" →
// "999909829054513@lid"). A device is not a person, and contacts keyed by one
// are the same contact.
func nonADJID(jid string) string {
	if parsed, err := types.ParseJID(jid); err == nil {
		return parsed.ToNonAD().String()
	}
	return jid
}

// chatScanBatch is the most chat rows chatsPage reads per query; a first
// page reads fewer (twice what it shows, at least 50).
var chatScanBatch = 500

// chatsPage reads chats newest first, one entry per person, and paginates by
// person: offset and limit count people, not rows. A person is listed where
// their most recent chat row falls, and their other rows (the same person's
// phone JID or @lid) are folded into that entry wherever those rows are.
//
// It scans from the newest chat until the page is full, in batches: knowing
// who was already listed on earlier pages means reading them. Until
// 2026-10-21 it read one window of at most 1,500 rows, so list_chats came
// back empty from about the 1,500th chat on, and a person whose other row
// fell outside the window showed up again on a later page.
func (s *Server) chatsPage(ctx context.Context, where string, args []any, offset, limit int) ([]chatRow, error) {
	if offset < 0 {
		offset = 0
	}
	page := []chatRow{}
	if limit < 1 {
		return page, nil
	}
	query := `SELECT jid, chat_type, COALESCE(name, ''), COALESCE(last_message_time, 0),
	                 COALESCE(last_message_preview, ''), unread_count
	          FROM chats`
	if where != "" {
		query += " WHERE " + where
	}
	query += " ORDER BY last_message_time DESC, jid LIMIT ? OFFSET ?"

	batchSize := min(max(2*(offset+limit), 50), chatScanBatch)
	listed := map[string]bool{} // alias keys of the people counted so far
	for scanned := 0; len(page) < limit; scanned += batchSize {
		batchArgs := append(append([]any{}, args...), batchSize, scanned)
		rows, err := s.db.QueryContext(ctx, query, batchArgs...)
		if err != nil {
			return nil, err
		}
		var batch []chatRow
		for rows.Next() {
			var c chatRow
			if err := rows.Scan(&c.JID, &c.ChatType, &c.Name, &c.LastMessageTime, &c.LastMessagePreview, &c.UnreadCount); err != nil {
				rows.Close()
				return nil, err
			}
			batch = append(batch, c)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}

		jids := make([]string, len(batch))
		for i, c := range batch {
			jids[i] = c.JID
		}
		keys, _, _ := aliasGroups(ctx, s.db, jids)
		for _, c := range batch {
			if key := keys[c.JID]; !listed[key] {
				listed[key] = true
				if len(listed) > offset && len(page) < limit {
					page = append(page, c)
				}
			}
		}
		if len(batch) < batchSize {
			break
		}
	}
	s.fillMerged(ctx, page)
	return page, nil
}

// fillMerged completes each listed person with the chat rows they have under
// their other JIDs, wherever those rows sit in the list: merged_jids names
// them all and unread counts add up. The listed row keeps its own name and
// preview, and takes the other rows' name when it has none.
func (s *Server) fillMerged(ctx context.Context, page []chatRow) {
	if len(page) == 0 {
		return
	}
	heads := make([]string, len(page))
	for i, c := range page {
		heads[i] = c.JID
	}
	keys, members, _ := aliasGroups(ctx, s.db, heads)
	owner := map[string]int{}
	var others []string
	for i, c := range page {
		for _, j := range members[keys[c.JID]] {
			if j != c.JID {
				owner[j] = i
				others = append(others, j)
			}
		}
	}
	for start := 0; start < len(others); start += aliasBatch {
		batch := others[start:min(start+aliasBatch, len(others))]
		rows, err := s.db.QueryContext(ctx,
			`SELECT jid, unread_count, COALESCE(name, '') FROM chats
			  WHERE jid IN (`+inClausePlaceholders(len(batch))+`)
			  ORDER BY last_message_time DESC, jid`, jidsToArgs(batch)...)
		if err != nil {
			return
		}
		for rows.Next() {
			var jid, name string
			var unread int
			if rows.Scan(&jid, &unread, &name) != nil {
				continue
			}
			c := &page[owner[jid]]
			if len(c.MergedJIDs) == 0 {
				c.MergedJIDs = []string{c.JID}
			}
			c.MergedJIDs = append(c.MergedJIDs, jid)
			c.UnreadCount += unread
			if c.Name == "" {
				c.Name = name
			}
		}
		rows.Close()
	}
}

// handleSearchChats finds chats — groups, people, broadcast lists — by name,
// ignoring accents and case, newest first. chat_type narrows to "group",
// "direct" or "broadcast". A JID fragment matches too.
func (s *Server) handleSearchChats(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := strings.TrimSpace(q.Get("q"))
	if query == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "q parameter required"})
		return
	}
	limit := parseIntDefault(q.Get("limit"), 20)
	if limit > 100 {
		limit = 100
	}

	where := "(normalized_name LIKE ? OR jid LIKE ?)"
	args := []any{"%" + Normalize(query) + "%", "%" + query + "%"}
	if chatType := q.Get("chat_type"); chatType != "" {
		switch chatType {
		case "group", "direct", "broadcast":
			where += " AND chat_type = ?"
			args = append(args, chatType)
		default:
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error: fmt.Sprintf("chat_type %q not supported", chatType), Details: "use group, direct or broadcast"})
			return
		}
	}
	chats, err := s.chatsPage(r.Context(), where, args, 0, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "query failed", Details: err.Error()})
		return
	}
	out := chatListResponse{Chats: chats, Count: len(chats), BridgeState: s.currentBridgeState()}
	writeJSON(w, http.StatusOK, out)
}

// mergeContacts collapses a contact search to one entry per person: device
// variants fold into their JID, and alias rows (phone JID and @lid) into one
// entry that lists the other forms under aliases. The entry shown is the phone
// form when there is one — the number is what a person recognises — and its
// names fall back to the other rows' when it has none.
func mergeContacts(ctx context.Context, db *sql.DB, rows []contactRow) []contactRow {
	var jids []string
	for i := range rows {
		rows[i].JID = nonADJID(rows[i].JID)
		jids = append(jids, rows[i].JID)
	}
	keys, people, _ := aliasGroups(ctx, db, jids)

	groups := map[string][]contactRow{}
	var order []string
	for _, c := range rows {
		key := keys[c.JID]
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], c)
	}

	out := make([]contactRow, 0, len(order))
	for _, key := range order {
		members := groups[key]
		sort.SliceStable(members, func(i, j int) bool {
			return strings.HasSuffix(members[i].JID, "@"+types.DefaultUserServer) &&
				!strings.HasSuffix(members[j].JID, "@"+types.DefaultUserServer)
		})
		head := members[0]
		forms := map[string]bool{head.JID: true}
		for _, m := range members[1:] {
			forms[m.JID] = true
			if head.FullName == "" {
				head.FullName = m.FullName
			}
			if head.PushName == "" {
				head.PushName = m.PushName
			}
			if head.VerifiedName == "" {
				head.VerifiedName = m.VerifiedName
			}
			if head.Phone == "" {
				head.Phone = m.Phone
			}
			if head.LID == "" {
				head.LID = m.LID
			}
			head.IsBusiness = head.IsBusiness || m.IsBusiness
		}
		for _, a := range people[key] {
			forms[a] = true
		}
		head.Aliases = nil
		for f := range forms {
			if f != head.JID {
				head.Aliases = append(head.Aliases, f)
			}
		}
		sort.Strings(head.Aliases)
		out = append(out, head)
	}
	return out
}

// senderNames labels senders whose messages carry no name, only a number:
// for each JID, a name from the address book or the sender's own profile under
// any of their JIDs, else their phone number, else "Desconocido". A bare @lid
// number ("99990000002312") means nothing to a reader (2026-10-22 test run).
func (s *Server) senderNames(ctx context.Context, jids []string) map[string]string {
	seen := map[string]bool{}
	unique := jids[:0:0]
	for _, j := range jids {
		if !seen[j] {
			seen[j] = true
			unique = append(unique, j)
		}
	}
	jids = unique
	keys, members, _ := aliasGroups(ctx, s.db, jids)
	var all []string
	for _, j := range jids {
		all = append(all, members[keys[j]]...)
	}
	type known struct{ name, phone string }
	found := map[string]known{}
	for start := 0; start < len(all); start += aliasBatch {
		batch := all[start:min(start+aliasBatch, len(all))]
		rows, err := s.db.QueryContext(ctx,
			`SELECT jid, COALESCE(NULLIF(full_name, ''), NULLIF(push_name, ''), NULLIF(verified_name, ''), ''),
			        COALESCE(phone, '')
			   FROM contacts WHERE jid IN (`+inClausePlaceholders(len(batch))+`)`, jidsToArgs(batch)...)
		if err != nil {
			break
		}
		for rows.Next() {
			var jid string
			var k known
			if rows.Scan(&jid, &k.name, &k.phone) == nil {
				found[jid] = k
			}
		}
		rows.Close()
	}
	names := make(map[string]string, len(jids))
	for _, j := range jids {
		var name, phone string
		for _, m := range members[keys[j]] {
			k := found[m]
			if name == "" && !isBareNumber(k.name) {
				name = k.name
			}
			if phone == "" {
				phone = k.phone
			}
			if phone == "" && strings.HasSuffix(m, "@"+types.DefaultUserServer) {
				phone = strings.TrimSuffix(m, "@"+types.DefaultUserServer)
			}
		}
		switch {
		case name != "":
			names[j] = name
		case phone != "":
			names[j] = "+" + strings.TrimPrefix(phone, "+")
		default:
			// The end of the number tells two unknown senders apart.
			user, _, _ := strings.Cut(j, "@")
			if len(user) > 4 {
				user = user[len(user)-4:]
			}
			names[j] = "Desconocido (…" + user + ")"
		}
	}
	return names
}

// isUnnamed reports whether a stored sender label is no name at all: empty, a
// bare number, or a JID — history sync stores "900000000000001@lid" when the
// sender has no push name, live receive stores the number.
func isUnnamed(display string) bool {
	if isBareNumber(display) {
		return true
	}
	user, _, found := strings.Cut(display, "@")
	user, _, _ = strings.Cut(user, ":")
	return found && user != "" && isBareNumber(user)
}

// ownDisplayName is how the user's own messages are labelled: their WhatsApp
// name, or "Yo" when the bridge does not know it.
func (s *Server) ownDisplayName() string {
	if s.bridge != nil && s.bridge.client != nil && s.bridge.client.Store != nil {
		if name := strings.TrimSpace(s.bridge.client.Store.PushName); name != "" {
			return name
		}
	}
	return "Yo"
}

// isBareNumber reports whether a display name is empty or only a number — a
// JID's user part standing in for a name.
func isBareNumber(name string) bool {
	name = strings.TrimPrefix(strings.TrimSpace(name), "+")
	for _, r := range name {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
