package cmd

import (
	"strconv"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// bulkArgs extracts the raw bytes from a slice of RESP arguments, e.g. to turn
// the values of "RPUSH key a b c" into the [][]byte the store wants. The slices
// alias the decoder's buffer; the store copies them when it takes ownership.
func bulkArgs(args []protocol.Value) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = a.Bulk
	}
	return out
}

// LPush implements LPUSH key value [value ...].
//
// It prepends each value to the head of the list (creating the list if needed)
// and replies with the list's new length. Pushing several values in one call
// inserts them one at a time, so "LPUSH k a b c" yields [c b a]. At least one
// value is required. WRONGTYPE if the key holds a non-list value.
func LPush(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgs("lpush")
	}

	n, err := database.LPush(string(args[0].Bulk), bulkArgs(args[1:])...)
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}

// RPush implements RPUSH key value [value ...]: like LPush but appends to the
// tail, so "RPUSH k a b c" yields [a b c]. Replies with the new length.
func RPush(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgs("rpush")
	}

	n, err := database.RPush(string(args[0].Bulk), bulkArgs(args[1:])...)
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}

// LPop implements LPOP key.
//
// It removes and returns the head element as a bulk string, or a null bulk
// string if the key is absent or the list is empty. WRONGTYPE if the key holds a
// non-list value. (Redis 6.2's optional count argument is out of scope here.)
func LPop(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("lpop")
	}

	v, ok, err := database.LPop(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	if !ok {
		return nullBulkValue()
	}
	return bulkValue(v)
}

// RPop implements RPOP key: like LPop but removes and returns the tail element.
func RPop(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("rpop")
	}

	v, ok, err := database.RPop(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	if !ok {
		return nullBulkValue()
	}
	return bulkValue(v)
}

// LLen implements LLEN key: it replies with the list's length as an integer, or
// 0 if the key does not exist. WRONGTYPE if the key holds a non-list value.
func LLen(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("llen")
	}

	n, err := database.LLen(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}

// LRange implements LRANGE key start stop.
//
// It replies with the elements between start and stop inclusive, as an array of
// bulk strings. Indices may be negative to count from the end (-1 is the last
// element) and are clamped into range; a missing key yields an empty array.
// start and stop must parse as integers. WRONGTYPE if the key holds a non-list.
func LRange(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return wrongArgs("lrange")
	}

	start, err := strconv.Atoi(string(args[1].Bulk))
	if err != nil {
		return notInteger()
	}
	stop, err := strconv.Atoi(string(args[2].Bulk))
	if err != nil {
		return notInteger()
	}

	items, err := database.LRange(string(args[0].Bulk), start, stop)
	if err != nil {
		return replyForErr(err)
	}

	out := make([]protocol.Value, len(items))
	for i, it := range items {
		out[i] = bulkValue(it)
	}
	return arrayValue(out)
}
