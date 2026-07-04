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
	"errors"
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
	"PING":      Ping,
	"ECHO":      Echo,
	"SET":       Set,
	"GET":       Get,
	"DEL":       Del,
	"EXISTS":    Exists,
	"LPUSH":     LPush,
	"RPUSH":     RPush,
	"LPOP":      LPop,
	"RPOP":      RPop,
	"LRANGE":    LRange,
	"LLEN":      LLen,
	"HSET":      HSet,
	"HGET":      HGet,
	"HDEL":      HDel,
	"HGETALL":   HGetAll,
	"HKEYS":     HKeys,
	"HVALS":     HVals,
	"HLEN":      HLen,
	"SADD":      SAdd,
	"SREM":      SRem,
	"SISMEMBER": SIsMember,
	"SMEMBERS":  SMembers,
	"SCARD":     SCard,
	"EXPIRE":    Expire,
	"PEXPIRE":   PExpire,
	"TTL":       TTL,
	"PTTL":      PTTL,
	"PERSIST":   Persist,
	"PUBLISH":   Publish,
}

// writeCommands names every command that MUTATES the keyspace, and so must be
// recorded in the append-only log to survive a restart. Read-only commands (GET,
// LRANGE, TTL, PING, ...) are intentionally absent: persisting them would be
// wasted work on replay and buys nothing, since they change no state.
//
// Keep this in lock-step with `commands` above: a new write handler added there
// must be listed here too, or its effect will silently vanish on restart.
var writeCommands = map[string]struct{}{
	"SET":     {},
	"DEL":     {},
	"LPUSH":   {},
	"RPUSH":   {},
	"LPOP":    {},
	"RPOP":    {},
	"HSET":    {},
	"HDEL":    {},
	"SADD":    {},
	"SREM":    {},
	"EXPIRE":  {},
	"PEXPIRE": {},
	"PERSIST": {},
}

// IsWrite reports whether name (in any case) is a write command — one whose
// effect must be appended to the AOF. The server consults it after decoding a
// request to decide whether the command needs persisting. Names are matched
// case-insensitively, exactly as Dispatch resolves them.
func IsWrite(name string) bool {
	_, ok := writeCommands[strings.ToUpper(name)]
	return ok
}

// Known reports whether name (in any case) is a registered command. The server
// uses it to bound metric label cardinality: an unrecognised name is recorded as
// "unknown" rather than as its own series, so a client spraying garbage command
// names can't blow up the metrics registry.
func Known(name string) bool {
	_, ok := commands[strings.ToUpper(name)]
	return ok
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

// boolReply maps a Go bool to the RESP integer reply Redis uses for boolean
// outcomes: 1 for true, 0 for false (e.g. EXPIRE set / not set, PERSIST,
// SISMEMBER).
func boolReply(b bool) protocol.Value {
	if b {
		return integerValue(1)
	}
	return integerValue(0)
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

// arrayValue builds a RESP array reply from already-constructed element Values
// (used by LRANGE). A non-nil but empty slice encodes as the empty array "*0",
// which is what Redis returns for, e.g., LRANGE on a missing key.
func arrayValue(items []protocol.Value) protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: items}
}

// notInteger is Redis's standard error for a numeric argument that does not
// parse as an integer (e.g. the start/stop indices of LRANGE).
func notInteger() protocol.Value {
	return errorValue("ERR value is not an integer or out of range")
}

// bulkArrayValue builds a RESP array whose elements are bulk strings — the reply
// shape shared by LRANGE, HKEYS, HVALS, and SMEMBERS. A non-nil empty input
// encodes as the empty array "*0".
func bulkArrayValue(items [][]byte) protocol.Value {
	out := make([]protocol.Value, len(items))
	for i, it := range items {
		out[i] = bulkValue(it)
	}
	return arrayValue(out)
}

// replyForErr maps a store error to the matching RESP error reply. A type
// mismatch becomes the canonical WRONGTYPE error (the wording real Redis uses,
// which interview tests check for); anything else is surfaced as a generic ERR.
func replyForErr(err error) protocol.Value {
	if errors.Is(err, db.ErrWrongType) {
		return errorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return errorValue("ERR " + err.Error())
}
