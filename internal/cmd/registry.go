// Package cmd holds the command handlers and the dispatch table that maps a
// command name (PING, SET, GET, ...) to the function that implements it.
//
// The flow for one client request is:
//
//	server decodes a RESP array  ->  cmd.Dispatch  ->  the matching CmdFunc
//	                                                 ->  a RESP reply Value
//
// Handlers never touch the network or the RESP wire format directly; they
// receive already-decoded arguments and return a Value that the server encodes.
// That keeps the protocol, the dispatch logic, and the storage layer cleanly
// separated.
package cmd

import (
	"fmt"
	"strings"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// CmdFunc is the signature every command handler implements.
//
// It receives the shared database and the command's arguments (the elements
// AFTER the command name — so for "SET k v", args is ["k", "v"]). It returns the
// reply as a protocol.Value, which the caller encodes back to the client. A
// handler reports user-facing problems by returning a RESP error Value rather
// than a Go error, because every outcome — success or failure — is just another
// reply on the wire.
type CmdFunc func(database *db.DB, args []protocol.Value) protocol.Value

// commands is the dispatch table: it maps an upper-cased command name to its
// handler. Lookups go through Dispatch, which upper-cases the incoming name so
// the table only needs the canonical form. New commands are wired in by adding
// a line here and writing the handler in one of the sibling files.
var commands = map[string]CmdFunc{
	"PING":   Ping,
	"ECHO":   Echo,
	"SET":    Set,
	"GET":    Get,
	"DEL":    Del,
	"EXISTS": Exists,
}

// Dispatch routes a decoded client request to the right handler and returns the
// reply to send back.
//
// A request is always a RESP array of bulk strings — that is how Redis clients
// frame commands, e.g. ["SET", "k", "v"]. The first element is the command
// name; everything after it is passed to the handler as args.
func Dispatch(database *db.DB, request protocol.Value) protocol.Value {
	// Clients must send commands as a non-empty array. Anything else (a bare
	// string, an empty array, ...) is malformed.
	if request.Type != protocol.TypeArray || len(request.Array) == 0 {
		return errorValue("ERR invalid request: expected a non-empty array")
	}

	// The command name is the first element. We read it as a bulk string and
	// upper-case it so lookups are case-insensitive: Redis treats "get", "GET"
	// and "Get" as the same command.
	name := strings.ToUpper(string(request.Array[0].Bulk))

	handler, ok := commands[name]
	if !ok {
		return errorValue(fmt.Sprintf("ERR unknown command '%s'", name))
	}

	// Hand the handler everything after the command name as its arguments.
	return handler(database, request.Array[1:])
}

// ---------------------------------------------------------------------------
// Reply constructors.
//
// These tiny helpers build the protocol.Value variants handlers return. They
// exist purely so the handlers read as intent ("return okValue()") instead of
// repeating the struct literal and its Type discriminator everywhere.
// ---------------------------------------------------------------------------

// okValue is the canonical "+OK" success reply (a RESP simple string).
func okValue() protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, Str: "OK"}
}

// simpleStringValue builds a RESP simple string (e.g. "+PONG"). Use this only
// for short, known-good text with no CRLF in it; arbitrary/binary data must go
// through bulkValue instead.
func simpleStringValue(s string) protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, Str: s}
}

// errorValue builds a RESP error reply (e.g. "-ERR ..."). By convention the
// message starts with an upper-case error code such as ERR.
func errorValue(msg string) protocol.Value {
	return protocol.Value{Type: protocol.TypeError, Str: msg}
}

// integerValue builds a RESP integer reply (e.g. ":3"). Used by DEL and EXISTS.
func integerValue(n int64) protocol.Value {
	return protocol.Value{Type: protocol.TypeInteger, Int: n}
}

// bulkValue builds a RESP bulk string from arbitrary bytes — the binary-safe
// reply type used to return stored values and echoed payloads.
func bulkValue(b []byte) protocol.Value {
	return protocol.Value{Type: protocol.TypeBulkString, Bulk: b}
}

// nullBulkValue is the RESP null bulk string ("$-1"), the standard "no such
// key / no value" reply (GET on a missing key returns this).
func nullBulkValue() protocol.Value {
	return protocol.Value{Type: protocol.TypeBulkString, Bulk: nil}
}

// wrongArgs builds the standard Redis error for a command invoked with the
// wrong number of arguments. name is lower-cased to match Redis's own wording.
func wrongArgs(name string) protocol.Value {
	return errorValue(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
}
