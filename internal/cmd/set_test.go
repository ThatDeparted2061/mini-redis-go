package cmd

import (
	"reflect"
	"testing"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// TestSetCommands drives one shared DB through the set commands, asserting the
// SADD new-member count (with duplicates collapsed), membership tests, the
// member listing, and that emptying a set via SREM deletes the key.
func TestSetCommands(t *testing.T) {
	database := db.New()

	// SADD counts only NEW members; the duplicate "a" within the call collapses.
	if got := Dispatch(database, request("SADD", "s", "a", "b", "a")); !reflect.DeepEqual(got, integer(2)) {
		t.Fatalf("SADD = %#v, want 2", got)
	}
	// "b" already present, only "c" is new.
	if got := Dispatch(database, request("SADD", "s", "b", "c")); !reflect.DeepEqual(got, integer(1)) {
		t.Fatalf("SADD existing+new = %#v, want 1", got)
	}

	if got := Dispatch(database, request("SCARD", "s")); !reflect.DeepEqual(got, integer(3)) {
		t.Errorf("SCARD = %#v, want 3", got)
	}

	// SISMEMBER replies 1 for a member, 0 otherwise.
	if got := Dispatch(database, request("SISMEMBER", "s", "a")); !reflect.DeepEqual(got, integer(1)) {
		t.Errorf("SISMEMBER a = %#v, want 1", got)
	}
	if got := Dispatch(database, request("SISMEMBER", "s", "z")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("SISMEMBER z = %#v, want 0", got)
	}
	if got := Dispatch(database, request("SISMEMBER", "missing", "a")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("SISMEMBER on missing key = %#v, want 0", got)
	}

	if got := sortedBulks(Dispatch(database, request("SMEMBERS", "s"))); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("SMEMBERS = %v, want [a b c]", got)
	}

	// SREM removes present members and ignores absent ones; emptying deletes the key.
	if got := Dispatch(database, request("SREM", "s", "a", "z")); !reflect.DeepEqual(got, integer(1)) {
		t.Errorf("SREM = %#v, want 1", got)
	}
	if got := Dispatch(database, request("SREM", "s", "b", "c")); !reflect.DeepEqual(got, integer(2)) {
		t.Errorf("SREM rest = %#v, want 2", got)
	}
	if got := Dispatch(database, request("EXISTS", "s")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("EXISTS after emptying set = %#v, want 0", got)
	}
	if got := Dispatch(database, request("SCARD", "s")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("SCARD after emptying = %#v, want 0", got)
	}
}

// TestSetWrongType verifies every set command rejects a key that holds a string,
// replying with the canonical WRONGTYPE error.
func TestSetWrongType(t *testing.T) {
	database := db.New()
	wrongType := protocol.Value{
		Type: protocol.TypeError,
		Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
	}

	Dispatch(database, request("SET", "str", "v"))
	for _, req := range []protocol.Value{
		request("SADD", "str", "m"),
		request("SREM", "str", "m"),
		request("SISMEMBER", "str", "m"),
		request("SMEMBERS", "str"),
		request("SCARD", "str"),
	} {
		if got := Dispatch(database, req); !reflect.DeepEqual(got, wrongType) {
			t.Errorf("%v on string = %#v, want WRONGTYPE", req, got)
		}
	}
}
