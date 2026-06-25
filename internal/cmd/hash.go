package cmd

import (
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// HSet implements HSET key field value [field value ...].
//
// It sets each field to its value in the hash at key (creating the hash if
// needed) and replies with the number of fields that were newly created; fields
// that already existed are overwritten but not counted, matching Redis. At least
// one field/value pair is required, and the field/value arguments must come in
// pairs — an odd count is a usage error. WRONGTYPE if the key holds a non-hash.
func HSet(database *db.DB, args []protocol.Value) protocol.Value {
	// args is [key, field, value, field, value, ...], so a valid call has an
	// odd length of at least 3. An even length means a field is missing its value.
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgs("hset")
	}

	rest := args[1:]
	fields := make([][]byte, 0, len(rest)/2)
	values := make([][]byte, 0, len(rest)/2)
	for i := 0; i+1 < len(rest); i += 2 {
		fields = append(fields, rest[i].Bulk)
		values = append(values, rest[i+1].Bulk)
	}

	n, err := database.HSet(string(args[0].Bulk), fields, values)
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}

// HGet implements HGET key field: it replies with the field's value as a bulk
// string, or a null bulk string if either the key or the field is absent.
// WRONGTYPE if the key holds a non-hash value.
func HGet(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgs("hget")
	}

	v, ok, err := database.HGet(string(args[0].Bulk), string(args[1].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	if !ok {
		return nullBulkValue()
	}
	return bulkValue(v)
}

// HDel implements HDEL key field [field ...]: it removes the named fields and
// replies with the count actually removed (absent fields and a missing key
// contribute nothing). WRONGTYPE if the key holds a non-hash value.
func HDel(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgs("hdel")
	}

	fields := make([]string, len(args)-1)
	for i, a := range args[1:] {
		fields[i] = string(a.Bulk)
	}

	n, err := database.HDel(string(args[0].Bulk), fields...)
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}

// HGetAll implements HGETALL key: it replies with a flat array alternating field
// and value — [field1, value1, field2, value2, ...] — which is how Redis returns
// a whole hash. The field order is unspecified, and a missing key yields an empty
// array. WRONGTYPE if the key holds a non-hash value.
func HGetAll(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("hgetall")
	}

	fields, values, err := database.HGetAll(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}

	out := make([]protocol.Value, 0, len(fields)*2)
	for i := range fields {
		out = append(out, bulkValue(fields[i]), bulkValue(values[i]))
	}
	return arrayValue(out)
}

// HKeys implements HKEYS key: it replies with an array of the hash's field names
// (unspecified order), or an empty array for a missing key. WRONGTYPE if the key
// holds a non-hash value.
func HKeys(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("hkeys")
	}

	keys, err := database.HKeys(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	return bulkArrayValue(keys)
}

// HVals implements HVALS key: like HKEYS but replies with the hash's values
// instead of its field names.
func HVals(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("hvals")
	}

	vals, err := database.HVals(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	return bulkArrayValue(vals)
}

// HLen implements HLEN key: it replies with the number of fields in the hash as
// an integer, or 0 if the key does not exist. WRONGTYPE if the key holds a
// non-hash value.
func HLen(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("hlen")
	}

	n, err := database.HLen(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}
