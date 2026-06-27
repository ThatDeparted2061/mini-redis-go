package cmd

import (
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Publish implements PUBLISH channel message.
//
// It hands the message to the broker, which fans it out to every connection
// subscribed to channel, and replies with the number that received it. PUBLISH
// is an ordinary command — it needs only the broker (reachable via the DB it is
// handed), not the publishing connection — so it lives here as a normal handler.
// SUBSCRIBE/UNSUBSCRIBE, by contrast, need the connection itself and are handled
// in the server's connection loop, not through this dispatch table.
//
// PUBLISH is NOT a write command: pub/sub messages are ephemeral and change no
// keyspace state, so nothing is appended to the AOF.
func Publish(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgs("publish")
	}
	channel := string(args[0].Bulk)
	received := database.PubSub().Publish(channel, args[1].Bulk)
	return integerValue(int64(received))
}
