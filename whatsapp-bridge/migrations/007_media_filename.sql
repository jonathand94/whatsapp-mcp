-- 007: keep the name a document arrived with.
--
-- Background: a DocumentMessage carries the sender's own filename ("Contrato
-- firmado.pdf"), but the bridge only ever stored the bytes' MIME type. A
-- downloaded document landed on disk as <message id>.pdf and the name was gone
-- for good. Everything that hands a file back to a person — a copy saved to
-- Drive, a download link opened on a phone — then showed
-- "3EB0C23D95D7AA7075BDFA.pdf" instead of what the sender called it.
--
-- Captured from here on by the live and history-sync ingest paths, and by the
-- confirm path for documents the bridge sends itself. Rows that predate the
-- column stay NULL until history sync re-delivers them; callers fall back to a
-- generated name for those.
--
-- Additive and nullable, with no backfill: nothing to rebuild, nothing to copy.

ALTER TABLE messages ADD COLUMN media_filename TEXT;

INSERT OR IGNORE INTO schema_version (version, applied_at, description)
VALUES (7, strftime('%s', 'now'), 'messages.media_filename: original document name');
