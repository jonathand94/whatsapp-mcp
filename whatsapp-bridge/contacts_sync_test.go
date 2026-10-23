package main

import (
	"database/sql"
	"testing"

	_ "github.com/mutecomm/go-sqlcipher/v4"
)

// newContactTestDB runs the real migration chain rather than hand-rolling a
// schema, so 005 is exercised here too: a migration that does not apply cleanly
// would fail every test in this file instead of being discovered on a user's
// database at startup.
func newContactTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applyMigrations(db); err != nil {
		t.Fatalf("applyMigrations: %v", err)
	}
	return db
}

func insertChat(t *testing.T, db *sql.DB, jid, chatType, name string) {
	t.Helper()
	var n any
	if name != "" {
		n = name
	}
	if _, err := db.Exec(`
		INSERT INTO chats (jid, chat_type, name, created_at, updated_at)
		VALUES (?, ?, ?, 0, 0)
	`, jid, chatType, n); err != nil {
		t.Fatalf("insert chat %s: %v", jid, err)
	}
}

func chatName(t *testing.T, db *sql.DB, jid string) (string, bool) {
	t.Helper()
	var n sql.NullString
	if err := db.QueryRow(`SELECT name FROM chats WHERE jid = ?`, jid).Scan(&n); err != nil {
		t.Fatalf("read chat %s: %v", jid, err)
	}
	return n.String, n.Valid
}

// --- addressBookName --------------------------------------------------------

func TestAddressBookName(t *testing.T) {
	tests := []struct {
		full, first, want string
	}{
		{"Mi Cielo", "Laura", "Mi Cielo"}, // FullName wins
		{"", "Mariana", "Mariana"},       // saved with only a first name
		{"  Mi Cielo  ", "", "Mi Cielo"},   // trimmed
		{"   ", "Mariana", "Mariana"},    // whitespace-only is not a name
		{"", "", ""},                     // not in the address book at all
	}
	for _, tc := range tests {
		if got := addressBookName(tc.full, tc.first); got != tc.want {
			t.Fatalf("addressBookName(%q, %q) = %q, want %q", tc.full, tc.first, got, tc.want)
		}
	}
}

// --- writeContactName -------------------------------------------------------

func TestWriteContactNameNamesTheChat(t *testing.T) {
	db := newContactTestDB(t)
	const jid = "15555550100@s.whatsapp.net"
	insertChat(t, db, jid, "direct", "")

	renamed, err := writeContactName(t.Context(), db, jid, "Mi Cielo", 100)
	if err != nil {
		t.Fatalf("writeContactName: %v", err)
	}
	if renamed != 1 {
		t.Fatalf("renamed %d chats, want 1", renamed)
	}
	if got, ok := chatName(t, db, jid); !ok || got != "Mi Cielo" {
		t.Fatalf("chat name = %q (valid=%v), want Mi Cielo", got, ok)
	}

	var full, norm sql.NullString
	if err := db.QueryRow(`SELECT full_name, normalized_full_name FROM contacts WHERE jid = ?`, jid).
		Scan(&full, &norm); err != nil {
		t.Fatalf("read contact: %v", err)
	}
	if full.String != "Mi Cielo" {
		t.Fatalf("full_name = %q", full.String)
	}
	// Without the normalized column, accent-insensitive search reaches
	// push_name but not the address-book name — the one the user searches by.
	if norm.String != Normalize("Mi Cielo") {
		t.Fatalf("normalized_full_name = %q, want %q", norm.String, Normalize("Mi Cielo"))
	}
}

func TestWriteContactNameFollowsAliases(t *testing.T) {
	db := newContactTestDB(t)
	const phoneJID = "15555550100@s.whatsapp.net"
	const lidJID = "0a1b2c3d4e5f6071@lid"

	// The shape that actually occurs: the live chat is the @lid row, while the
	// address-book entry is keyed by the phone-number JID. Matching on jid
	// alone would name a row the user never looks at.
	insertChat(t, db, lidJID, "direct", "")
	if _, err := db.Exec(`
		INSERT INTO jid_aliases (jid_a, jid_b, discovered_at, source)
		VALUES (?, ?, 0, 'test'), (?, ?, 0, 'test')
	`, phoneJID, lidJID, lidJID, phoneJID); err != nil {
		t.Fatalf("insert aliases: %v", err)
	}

	renamed, err := writeContactName(t.Context(), db, phoneJID, "Mi Cielo", 100)
	if err != nil {
		t.Fatalf("writeContactName: %v", err)
	}
	if renamed != 1 {
		t.Fatalf("renamed %d chats, want 1 (the alias row)", renamed)
	}
	if got, _ := chatName(t, db, lidJID); got != "Mi Cielo" {
		t.Fatalf("the @lid chat should have been named, got %q", got)
	}
}

func TestWriteContactNameNeverRenamesAGroup(t *testing.T) {
	db := newContactTestDB(t)
	const jid = "15555550100@s.whatsapp.net"
	// A contact JID should never collide with a group row. This asserts the
	// guard anyway, because the cost of being wrong is overwriting a group's
	// subject with one member's name — the exact defect this work started from.
	insertChat(t, db, jid, "group", "Primos Hernández")

	renamed, err := writeContactName(t.Context(), db, jid, "Mi Cielo", 100)
	if err != nil {
		t.Fatalf("writeContactName: %v", err)
	}
	if renamed != 0 {
		t.Fatalf("renamed %d group chats, want 0", renamed)
	}
	if got, _ := chatName(t, db, jid); got != "Primos Hernández" {
		t.Fatalf("group subject was overwritten: %q", got)
	}
}

func TestWriteContactNameClearsWhenRemovedFromAddressBook(t *testing.T) {
	db := newContactTestDB(t)
	const jid = "15555550100@s.whatsapp.net"
	insertChat(t, db, jid, "direct", "")

	if _, err := writeContactName(t.Context(), db, jid, "Mi Cielo", 100); err != nil {
		t.Fatalf("first write: %v", err)
	}
	// Deleting the contact on the phone arrives as an event with no name. The
	// label has to go back to NULL so naming falls through to push_name;
	// keeping a name the user deleted would be worse than having none.
	if _, err := writeContactName(t.Context(), db, jid, "", 200); err != nil {
		t.Fatalf("clearing write: %v", err)
	}
	if got, ok := chatName(t, db, jid); ok {
		t.Fatalf("chat name should be NULL after removal, got %q", got)
	}
	var full sql.NullString
	if err := db.QueryRow(`SELECT full_name FROM contacts WHERE jid = ?`, jid).Scan(&full); err != nil {
		t.Fatalf("read contact: %v", err)
	}
	if full.Valid {
		t.Fatalf("full_name should be NULL after removal, got %q", full.String)
	}
}

func TestWriteContactNameIsIdempotent(t *testing.T) {
	db := newContactTestDB(t)
	const jid = "15555550100@s.whatsapp.net"
	insertChat(t, db, jid, "direct", "")

	// The startup sweep runs on every boot; a second pass must not duplicate
	// the contact row or fail on the primary key.
	for i := 0; i < 3; i++ {
		if _, err := writeContactName(t.Context(), db, jid, "Mi Cielo", int64(100+i)); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM contacts WHERE jid = ?`, jid).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("contact rows = %d, want 1", n)
	}
}

// --- the search this feature exists to make work ----------------------------

func TestSearchMatchesTheAddressBookName(t *testing.T) {
	db := newContactTestDB(t)
	const jid = "15555550100@s.whatsapp.net"
	insertChat(t, db, jid, "direct", "")

	// The contact broadcasts a professional name; the user saved a nickname.
	if _, err := db.Exec(`
		INSERT INTO contacts (jid, push_name, normalized_name, is_business, created_at, updated_at)
		VALUES (?, ?, ?, 0, 0, 0)
	`, jid, "Dra Laura De La Rosa", Normalize("Dra Laura De La Rosa")); err != nil {
		t.Fatalf("seed contact: %v", err)
	}
	if _, err := writeContactName(t.Context(), db, jid, "Mi Cielo", 100); err != nil {
		t.Fatalf("writeContactName: %v", err)
	}

	// Mirrors the WHERE clause in handleSearchContacts. Searching for the only
	// name the user knows used to return zero rows.
	search := func(q string) int {
		t.Helper()
		norm := Normalize(q)
		var n int
		if err := db.QueryRow(`
			SELECT COUNT(*) FROM contacts
			WHERE normalized_name LIKE ? OR normalized_full_name LIKE ?
		`, "%"+norm+"%", "%"+norm+"%").Scan(&n); err != nil {
			t.Fatalf("search %q: %v", q, err)
		}
		return n
	}

	if got := search("Mi Cielo"); got != 1 {
		t.Fatalf("searching the address-book name returned %d, want 1", got)
	}
	// The self-chosen name must keep working; this adds a way in, it does not
	// replace one.
	if got := search("Laura"); got != 1 {
		t.Fatalf("searching the push name returned %d, want 1", got)
	}
	// Accent- and case-insensitively, same as every other name search.
	if got := search("mi cielo"); got != 1 {
		t.Fatalf("case-insensitive search returned %d, want 1", got)
	}
}
