-- 005: contacts.full_name — the name from the USER's address book.
--
-- Background: contacts.push_name is what a contact set for THEMSELVES. It is
-- not what the user calls them, and on a real account the two diverge
-- constantly: a partner saved as "Mi Cielo" broadcasts "Dra Laura De La Rosa".
-- The bridge only ever had somewhere to put the second one.
--
-- The consequences were not cosmetic. search_contacts could not find anyone by
-- the only name the user actually knows, so looking up "Mi Cielo" returned zero
-- and the user had to supply a phone number instead — and got it wrong the
-- first time, because remembering a number is exactly what an address book
-- exists to avoid.
--
-- The data was already on the machine. WhatsApp syncs the address book through
-- app state and whatsmeow stores it in its own ContactStore (types.ContactInfo
-- carries FullName and FirstName alongside PushName). The bridge simply never
-- read it — there were zero references to Store.Contacts in the whole package.
-- See contacts_sync.go.
--
-- A separate column rather than overwriting push_name: they are two different
-- facts about the same person and both stay useful. A contact who is not in the
-- address book has full_name NULL and still has a push_name; a contact who
-- never set a push name has the reverse, and before this could not be named at
-- all no matter how many messages arrived.
--
-- normalized_full_name mirrors the existing normalized_name so accent-
-- insensitive search reaches the address-book name too: without it, "Mi Cielo"
-- would be findable but "José" would not match "jose", which is the behaviour
-- normalize.go exists to prevent.
--
-- Numbered 005 rather than 004 deliberately: 004_outbound_media.sql is in
-- flight on another branch and the runner tracks a set of applied versions
-- rather than a high-water mark, so a gap is harmless and a collision is not.

ALTER TABLE contacts ADD COLUMN full_name TEXT;
ALTER TABLE contacts ADD COLUMN normalized_full_name TEXT;

CREATE INDEX IF NOT EXISTS idx_contacts_normalized_full_name ON contacts(normalized_full_name);

INSERT OR IGNORE INTO schema_version (version, applied_at, description)
VALUES (5, strftime('%s', 'now'), 'contacts.full_name: address-book name from whatsmeow ContactStore');
