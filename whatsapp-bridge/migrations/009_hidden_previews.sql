-- 009: chat previews that show a message the chat list now leaves out.
--
-- Since 2026-10-21 messages with nothing of their own to show — a deletion or
-- another protocol message, an encrypted poll vote, reaction, event answer or
-- edit, a key share — no longer become a chat's preview (hiddenFromChatPreview).
-- Chats whose preview was set before that still read
-- "[unsupported: protocolMessage]" (the user's own chat among them, found by
-- the 2026-10-22 test run). This points them at their newest message that does
-- have something to show, the way migration 008 does, and sorts them by it. A
-- chat with no such message gets no time, which sorts it last.
--
-- The marker conditions are hiddenMarkerSQL in chat_preview_repair.go (a test
-- keeps them equal): a marker is hidden when every field it names is.

UPDATE chats
   SET (last_message_id, last_message_time, last_message_preview) = (
         SELECT m.id, m.timestamp,
                CASE WHEN length(COALESCE(m.content_text, '')) > 120
                     THEN substr(m.content_text, 1, 120) || '...'
                     ELSE COALESCE(m.content_text, '') END
           FROM messages m
          WHERE m.chat_jid = chats.jid
            AND m.timestamp <= CAST(strftime('%s', 'now') AS INTEGER) + 86400
            AND NOT (m.type = 'system' AND (COALESCE(m.content_text, '') = '' OR
                     (m.content_text LIKE '[unsupported: %' AND REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(m.content_text, 'fastRatchetKeySenderKeyDistributionMessage', ''), 'rootSecretDistributeMessage', ''), 'encEventResponseMessage', ''), 'secretEncryptedMessage', ''), 'stickerSyncRmrMessage', ''), 'encReactionMessage', ''), 'pollUpdateMessage', ''), 'groupRootKeyShare', ''), 'protocolMessage', ''), ',', '') = '[unsupported: ]')))
            AND NOT (m.type = 'reaction' AND COALESCE(m.content_text, '') = '')
          ORDER BY m.timestamp DESC
          LIMIT 1)
 WHERE (last_message_preview LIKE '[unsupported: %' AND REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(last_message_preview, 'fastRatchetKeySenderKeyDistributionMessage', ''), 'rootSecretDistributeMessage', ''), 'encEventResponseMessage', ''), 'secretEncryptedMessage', ''), 'stickerSyncRmrMessage', ''), 'encReactionMessage', ''), 'pollUpdateMessage', ''), 'groupRootKeyShare', ''), 'protocolMessage', ''), ',', '') = '[unsupported: ]');

INSERT OR IGNORE INTO schema_version (version, applied_at, description)
VALUES (9, strftime('%s', 'now'), 'chats: previews no longer point at hidden messages');
