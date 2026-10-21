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

// aliasKeys maps each JID to the key its person is grouped under: the smallest
// JID among it and its aliases, so every form of one person gets the same key
// whichever form is at hand. One query for the whole set.
func aliasKeys(ctx context.Context, db *sql.DB, jids []string) map[string]string {
	keys := make(map[string]string, len(jids))
	for _, j := range jids {
		keys[j] = j
	}
	if len(jids) == 0 {
		return keys
	}
	rows, err := db.QueryContext(ctx,
		`SELECT jid_a, jid_b FROM jid_aliases WHERE jid_a IN (`+inClausePlaceholders(len(jids))+`)`,
		jidsToArgs(jids)...)
	if err != nil {
		return keys
	}
	defer rows.Close()
	for rows.Next() {
		var a, b string
		if rows.Scan(&a, &b) == nil && b < keys[a] {
			keys[a] = b
		}
	}
	return keys
}

// mergeChatRows folds chats that are the same person into the first (most
// recent) of them, which keeps its name and preview; unread counts add up,
// and merged_jids lists every JID folded in. rows must be newest first.
func mergeChatRows(ctx context.Context, db *sql.DB, rows []chatRow) []chatRow {
	jids := make([]string, len(rows))
	for i, c := range rows {
		jids[i] = c.JID
	}
	keys := aliasKeys(ctx, db, jids)
	index := map[string]int{}
	var out []chatRow
	for _, c := range rows {
		key := keys[c.JID]
		i, seen := index[key]
		if !seen {
			index[key] = len(out)
			out = append(out, c)
			continue
		}
		head := &out[i]
		if len(head.MergedJIDs) == 0 {
			head.MergedJIDs = []string{head.JID}
		}
		head.MergedJIDs = append(head.MergedJIDs, c.JID)
		head.UnreadCount += c.UnreadCount
		if head.Name == "" {
			head.Name = c.Name
		}
	}
	return out
}

// chatsPage reads chats newest first and merges aliases before paginating, so
// offset and limit count people rather than rows. It reads enough rows to fill
// the page after merging (a person is at most a handful of rows).
func (s *Server) chatsPage(ctx context.Context, where string, args []any, offset, limit int) ([]chatRow, error) {
	window := (offset + limit) * 3
	if window > 1500 {
		window = 1500
	}
	query := `SELECT jid, chat_type, COALESCE(name, ''), COALESCE(last_message_time, 0),
	                 COALESCE(last_message_preview, ''), unread_count
	          FROM chats`
	if where != "" {
		query += " WHERE " + where
	}
	query += " ORDER BY last_message_time DESC LIMIT ?"
	rows, err := s.db.QueryContext(ctx, query, append(args, window)...)
	if err != nil {
		return nil, err
	}
	var all []chatRow
	for rows.Next() {
		var c chatRow
		if rows.Scan(&c.JID, &c.ChatType, &c.Name, &c.LastMessageTime, &c.LastMessagePreview, &c.UnreadCount) == nil {
			all = append(all, c)
		}
	}
	rows.Close()

	merged := mergeChatRows(ctx, s.db, all)
	if offset >= len(merged) {
		return []chatRow{}, nil
	}
	end := offset + limit
	if end > len(merged) {
		end = len(merged)
	}
	return merged[offset:end], nil
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
	keys := aliasKeys(ctx, db, jids)

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
		if aliases, err := resolveAliases(ctx, db, head.JID); err == nil {
			for _, a := range aliases[1:] {
				forms[a] = true
			}
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
