package cmd

import (
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Del implements DEL key [key ...].
//
// It removes every named key that exists and replies with an integer: the count
// of keys actually deleted (missing keys are simply skipped). At least one key
// is required. DEL is variadic so a client can delete many keys in a single
// round trip.
func Del(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return wrongArgs("del")
	}

	// Convert the RESP bulk-string arguments into plain string keys for the
	// store's variadic Del. We size the slice exactly to avoid re-allocations.
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg.Bulk)
	}

	return integerValue(int64(database.Del(keys...)))
}

// Exists implements EXISTS key [key ...].
//
// It replies with an integer: how many of the named keys exist. Matching Redis,
// a key listed multiple times is counted once per occurrence (EXISTS k k is 2
// when k exists), which is why we pass every argument through rather than
// de-duplicating. At least one key is required.
func Exists(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return wrongArgs("exists")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg.Bulk)
	}

	return integerValue(int64(database.Exists(keys...)))
}
