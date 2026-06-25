package cmd

import (
	"reflect"
	"sort"
	"testing"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// nullBulk is the reply HGET/LPOP/etc. return for an absent value.
var nullBulk = protocol.Value{Type: protocol.TypeBulkString, Bulk: nil}

// sortedBulks pulls the elements out of an array-of-bulk-strings reply and sorts
// them, so tests can assert the contents of HKEYS/HVALS/SMEMBERS without
// depending on Go's randomised map iteration order.
func sortedBulks(v protocol.Value) []string {
	out := make([]string, len(v.Array))
	for i, e := range v.Array {
		out[i] = string(e.Bulk)
	}
	sort.Strings(out)
	return out
}

// TestHashCommands drives one shared DB through the hash commands, asserting the
// HSET new-field count, HGET hits and misses, length/keys/values, the flat
// HGETALL array, and that emptying a hash via HDEL deletes the key.
func TestHashCommands(t *testing.T) {
	database := db.New()

	// HSET reports newly-created fields: two new -> 2; then update one + add one -> 1.
	if got := Dispatch(database, request("HSET", "h", "f1", "v1", "f2", "v2")); !reflect.DeepEqual(got, integer(2)) {
		t.Fatalf("HSET new = %#v, want 2", got)
	}
	if got := Dispatch(database, request("HSET", "h", "f1", "V1", "f3", "v3")); !reflect.DeepEqual(got, integer(1)) {
		t.Fatalf("HSET update+add = %#v, want 1", got)
	}

	// HGET returns the latest value; a missing field or key is a null bulk.
	if got := Dispatch(database, request("HGET", "h", "f1")); !reflect.DeepEqual(got, bulk("V1")) {
		t.Errorf("HGET f1 = %#v, want V1", got)
	}
	if got := Dispatch(database, request("HGET", "h", "nope")); !reflect.DeepEqual(got, nullBulk) {
		t.Errorf("HGET missing field = %#v, want null bulk", got)
	}
	if got := Dispatch(database, request("HGET", "missing", "f1")); !reflect.DeepEqual(got, nullBulk) {
		t.Errorf("HGET missing key = %#v, want null bulk", got)
	}

	// HLEN / HKEYS / HVALS (the latter two order-independent).
	if got := Dispatch(database, request("HLEN", "h")); !reflect.DeepEqual(got, integer(3)) {
		t.Errorf("HLEN = %#v, want 3", got)
	}
	if got := sortedBulks(Dispatch(database, request("HKEYS", "h"))); !reflect.DeepEqual(got, []string{"f1", "f2", "f3"}) {
		t.Errorf("HKEYS = %v, want [f1 f2 f3]", got)
	}
	if got := sortedBulks(Dispatch(database, request("HVALS", "h"))); !reflect.DeepEqual(got, []string{"V1", "v2", "v3"}) {
		t.Errorf("HVALS = %v, want [V1 v2 v3]", got)
	}

	// HGETALL is a flat [field, value, ...] array; rebuild a map to compare.
	all := Dispatch(database, request("HGETALL", "h"))
	if len(all.Array)%2 != 0 {
		t.Fatalf("HGETALL returned odd-length array: %d", len(all.Array))
	}
	gotMap := map[string]string{}
	for i := 0; i < len(all.Array); i += 2 {
		gotMap[string(all.Array[i].Bulk)] = string(all.Array[i+1].Bulk)
	}
	wantMap := map[string]string{"f1": "V1", "f2": "v2", "f3": "v3"}
	if !reflect.DeepEqual(gotMap, wantMap) {
		t.Errorf("HGETALL = %v, want %v", gotMap, wantMap)
	}

	// HDEL removes present fields and ignores absent ones; emptying deletes the key.
	if got := Dispatch(database, request("HDEL", "h", "f1", "f2", "nope")); !reflect.DeepEqual(got, integer(2)) {
		t.Errorf("HDEL = %#v, want 2", got)
	}
	if got := Dispatch(database, request("HDEL", "h", "f3")); !reflect.DeepEqual(got, integer(1)) {
		t.Errorf("HDEL last = %#v, want 1", got)
	}
	if got := Dispatch(database, request("EXISTS", "h")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("EXISTS after emptying hash = %#v, want 0", got)
	}
}

// TestHashWrongType verifies every hash command rejects a key that holds a
// string, replying with the canonical WRONGTYPE error.
func TestHashWrongType(t *testing.T) {
	database := db.New()
	wrongType := protocol.Value{
		Type: protocol.TypeError,
		Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
	}

	Dispatch(database, request("SET", "str", "v"))
	for _, req := range []protocol.Value{
		request("HSET", "str", "f", "v"),
		request("HGET", "str", "f"),
		request("HDEL", "str", "f"),
		request("HGETALL", "str"),
		request("HKEYS", "str"),
		request("HVALS", "str"),
		request("HLEN", "str"),
	} {
		if got := Dispatch(database, req); !reflect.DeepEqual(got, wrongType) {
			t.Errorf("%v on string = %#v, want WRONGTYPE", req, got)
		}
	}
}
