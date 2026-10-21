-- 008: chats sorted by a time from the future.
--
-- A chat whose last_message_time is more than a day ahead was pinned to the top
-- of the chat list by one message with a bogus timestamp (an image dated 2044 on
-- this deployment). New writes are clamped (chat_time.go); this repairs the rows
-- already written, using the newest message in the chat that is not from the
-- future, or the migration time when there is none.
--
-- The messages themselves keep their timestamps: their real time is unknown.

UPDATE chats
   SET last_message_time = COALESCE(
         (SELECT MAX(m.timestamp) FROM messages m
           WHERE m.chat_jid = chats.jid
             AND m.timestamp <= CAST(strftime('%s', 'now') AS INTEGER) + 86400),
         CAST(strftime('%s', 'now') AS INTEGER))
 WHERE last_message_time > CAST(strftime('%s', 'now') AS INTEGER) + 86400;

INSERT OR IGNORE INTO schema_version (version, applied_at, description)
VALUES (8, strftime('%s', 'now'), 'chats.last_message_time: repair times from the future');
