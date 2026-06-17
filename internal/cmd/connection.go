package cmd

import (
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Ping implements PING [message].
//
// With no argument it replies with the simple string "PONG"; with a single
// argument it echoes that argument back as a bulk string. Redis uses PING both
// as a liveness probe ("are you still there?" -> "PONG") and, with a payload,
// as a round-trip check, which is why it accepts an optional message.
//
// The database parameter is named "_" because PING does not read or write any
// keys — it only depends on the connection being alive.
func Ping(_ *db.DB, args []protocol.Value) protocol.Value {
	switch len(args) {
	case 0:
		return simpleStringValue("PONG")
	case 1:
		return bulkValue(args[0].Bulk)
	default:
		// PING accepts at most one argument; more is a usage error.
		return wrongArgs("ping")
	}
}

// Echo implements ECHO message: it returns its single argument unchanged as a
// bulk string. It is the simplest possible request/response command and is handy
// for testing that the decode -> dispatch -> encode pipeline is wired correctly.
//
// Like PING it touches no keys, so the database is ignored.
func Echo(_ *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("echo")
	}
	return bulkValue(args[0].Bulk)
}
