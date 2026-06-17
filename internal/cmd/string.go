package cmd

import (
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Set implements SET key value.
//
// In this phase SET takes EXACTLY two arguments and always overwrites any
// existing value, replying with "+OK". The optional clauses real Redis supports
// (EX/PX expiry, NX/XX conditional set, GET, ...) are deliberately out of scope
// here and will be layered on later.
//
// The value is taken as raw bytes (args[1].Bulk) so it stays binary-safe; the
// store copies it internally, so we can hand it the decoder's slice directly.
func Set(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgs("set")
	}

	key := string(args[0].Bulk)
	value := args[1].Bulk

	database.Set(key, value)
	return okValue()
}

// Get implements GET key.
//
// It replies with the stored value as a bulk string, or with a null bulk string
// ("$-1") if the key does not exist — that null is how a Redis client learns the
// key is absent, as distinct from a key holding an empty value.
func Get(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("get")
	}

	value, ok := database.Get(string(args[0].Bulk))
	if !ok {
		return nullBulkValue()
	}
	return bulkValue(value)
}
