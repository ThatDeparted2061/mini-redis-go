package cmd

import (
	"strconv"
	"time"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Expire implements EXPIRE key seconds.
//
// It sets key to expire after `seconds` seconds, replacing any existing TTL, and
// replies 1 if the timeout was set or 0 if the key does not exist. A non-positive
// timeout deletes the key immediately (and still replies 1), matching Redis.
func Expire(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgs("expire")
	}

	secs, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return notInteger()
	}
	return boolReply(database.Expire(string(args[0].Bulk), time.Duration(secs)*time.Second))
}

// PExpire implements PEXPIRE key milliseconds: like EXPIRE but the timeout is
// given in milliseconds.
func PExpire(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgs("pexpire")
	}

	ms, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return notInteger()
	}
	return boolReply(database.Expire(string(args[0].Bulk), time.Duration(ms)*time.Millisecond))
}

// TTL implements TTL key: it replies with the key's remaining time to live in
// seconds, or the Redis sentinels -2 (no such key) and -1 (key exists but has no
// TTL).
func TTL(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("ttl")
	}

	remaining, exists, hasTTL := database.TTL(string(args[0].Bulk))
	return integerValue(ttlReply(remaining, exists, hasTTL, time.Second))
}

// PTTL implements PTTL key: like TTL but the remaining time is in milliseconds.
func PTTL(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("pttl")
	}

	remaining, exists, hasTTL := database.TTL(string(args[0].Bulk))
	return integerValue(ttlReply(remaining, exists, hasTTL, time.Millisecond))
}

// Persist implements PERSIST key: it removes the key's TTL so it never expires,
// replying 1 if a TTL was removed and 0 if the key is missing or already had no
// TTL.
func Persist(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("persist")
	}
	return boolReply(database.Persist(string(args[0].Bulk)))
}

// ttlReply maps the store's TTL result to the integer the wire expects: -2 when
// the key is gone, -1 when it has no expiry, otherwise the remaining time in
// `unit`, rounded to the nearest unit (as Redis rounds TTL/PTTL).
func ttlReply(remaining time.Duration, exists, hasTTL bool, unit time.Duration) int64 {
	switch {
	case !exists:
		return -2
	case !hasTTL:
		return -1
	default:
		return int64((remaining + unit/2) / unit)
	}
}
