-- 008: chats sorted by a time from the future.
--
-- A chat whose last_message_time is more than a day ahead was pinned to the top
-- of the chat list by one message with a bogus timestamp (an image dated 2044 on
-- this deployment), and its preview and last_message_id pointed at that
-- message. New writes no longer let such a time sort a chat (chat_time.go);
-- this repairs the rows already written from the newest message in the chat
-- that is not from the future and has something to show (blank system rows
-- are hidden from list_messages too). A chat with no such message gets no
-- time, which sorts it last: its real time is unknown.
--
-- The messages themselves keep their timestamps: their real time is unknown.

UPDATE chats
   SET (last_message_id, last_message_time, last_message_preview) = (
         SELECT m.id, m.timestamp,
                CASE WHEN length(COALESCE(m.content_text, '')) > 120
                     THEN substr(m.content_text, 1, 120) || '...'
                     ELSE COALESCE(m.content_text, '') END
           FROM messages m
          WHERE m.chat_jid = chats.jid
            AND m.timestamp <= CAST(strftime('%s', 'now') AS INTEGER) + 86400
            AND NOT (m.type = 'system' AND COALESCE(m.content_text, '') = '')
          ORDER BY m.timestamp DESC
          LIMIT 1)
 WHERE last_message_time > CAST(strftime('%s', 'now') AS INTEGER) + 86400;

INSERT OR IGNORE INTO schema_version (version, applied_at, description)
VALUES (8, strftime('%s', 'now'), 'chats.last_message_time: repair times from the future');
