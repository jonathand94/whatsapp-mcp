// chat_time.go — keeping a bad timestamp from pinning a chat to the top.
//
// The chat list sorts by chats.last_message_time. One message stored with a
// time far in the future — on this deployment an image dated 2044, most
// likely carried in by the history import — fixed its chat at the top of the
// list for good: every later message was "older", and the import and history
// paths take MAX() of the old and new values. Live timestamps come from
// WhatsApp's servers and are sound; imported and history-synced ones are
// whatever the source held.
//
// Only the chat's sort key is corrected. The message keeps the time it came
// with: there is no way to know the real one, and inventing it would make the
// archive say something it does not know.

package main

import "time"

// futureSlack is how far ahead of this machine's clock a message may be dated
// and still be trusted to sort its chat. A day covers any clock skew; a time
// past that is not a clock problem.
const futureSlack = 24 * time.Hour

// chatSortTime is ts, unless ts is more than futureSlack ahead of now, in
// which case it is now.
func chatSortTime(ts int64, now time.Time) int64 {
	if ts > now.Add(futureSlack).Unix() {
		return now.Unix()
	}
	return ts
}
