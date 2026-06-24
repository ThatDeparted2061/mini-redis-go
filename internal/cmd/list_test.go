package cmd

import (
	"reflect"
	"testing"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// bulks builds the array-of-bulk-strings reply LRANGE returns, so test cases can
// state the expected elements as plain strings.
func bulks(items ...string) protocol.Value {
	arr := make([]protocol.Value, len(items))
	for i, s := range items {
		arr[i] = protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte(s)}
	}
	return protocol.Value{Type: protocol.TypeArray, Array: arr}
}

func integer(n int64) protocol.Value {
	return protocol.Value{Type: protocol.TypeInteger, Int: n}
}

func bulk(s string) protocol.Value {
	return protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte(s)}
}

// TestListCommands drives one shared DB through the list commands in order,
// asserting push lengths, pop values, ordering (LPUSH reverses, RPUSH appends),
// negative-index LRANGE, and that a list is deleted once fully popped.
func TestListCommands(t *testing.T) {
	database := db.New()

	tests := []struct {
		name string
		req  protocol.Value
		want protocol.Value
	}{
		{"RPUSH appends, returns length", request("RPUSH", "l", "a", "b", "c"), integer(3)},
		{"LPUSH prepends in reverse", request("LPUSH", "l", "x", "y"), integer(5)}, // [y x a b c]
		{"LLEN", request("LLEN", "l"), integer(5)},
		{"LRANGE whole list", request("LRANGE", "l", "0", "-1"), bulks("y", "x", "a", "b", "c")},
		{"LRANGE sub-range", request("LRANGE", "l", "1", "3"), bulks("x", "a", "b")},
		{"LRANGE negative start", request("LRANGE", "l", "-2", "-1"), bulks("b", "c")},
		{"LRANGE start past end -> empty", request("LRANGE", "l", "10", "20"), bulks()},
		{"LPOP head", request("LPOP", "l"), bulk("y")},
		{"RPOP tail", request("RPOP", "l"), bulk("c")},
		{"LLEN after pops", request("LLEN", "l"), integer(3)}, // [x a b]
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Dispatch(database, tt.req)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Dispatch(%v) = %#v, want %#v", tt.req, got, tt.want)
			}
		})
	}
}

// TestListEmptiesDeleteKey checks that popping the last element removes the key,
// so a subsequent LPOP/RPOP sees a missing key (null bulk) and LLEN reports 0.
func TestListEmptiesDeleteKey(t *testing.T) {
	database := db.New()

	Dispatch(database, request("RPUSH", "k", "only"))
	if got := Dispatch(database, request("LPOP", "k")); !reflect.DeepEqual(got, bulk("only")) {
		t.Fatalf("LPOP = %#v, want bulk(only)", got)
	}

	nullBulk := protocol.Value{Type: protocol.TypeBulkString, Bulk: nil}
	if got := Dispatch(database, request("LPOP", "k")); !reflect.DeepEqual(got, nullBulk) {
		t.Errorf("LPOP on emptied key = %#v, want null bulk", got)
	}
	if got := Dispatch(database, request("LLEN", "k")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("LLEN on emptied key = %#v, want 0", got)
	}
	if got := Dispatch(database, request("EXISTS", "k")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("EXISTS on emptied key = %#v, want 0", got)
	}
}

// TestWrongType verifies the cross-type guard both ways: a string command on a
// list key and a list command on a string key must both reply WRONGTYPE.
func TestWrongType(t *testing.T) {
	database := db.New()
	wrongType := protocol.Value{
		Type: protocol.TypeError,
		Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
	}

	// GET on a list key.
	Dispatch(database, request("RPUSH", "list", "a"))
	if got := Dispatch(database, request("GET", "list")); !reflect.DeepEqual(got, wrongType) {
		t.Errorf("GET on list = %#v, want WRONGTYPE", got)
	}

	// LPUSH (and friends) on a string key.
	Dispatch(database, request("SET", "str", "v"))
	for _, req := range []protocol.Value{
		request("LPUSH", "str", "x"),
		request("RPUSH", "str", "x"),
		request("LPOP", "str"),
		request("RPOP", "str"),
		request("LLEN", "str"),
		request("LRANGE", "str", "0", "-1"),
	} {
		if got := Dispatch(database, req); !reflect.DeepEqual(got, wrongType) {
			t.Errorf("%v on string = %#v, want WRONGTYPE", req, got)
		}
	}

	// SET overwrites a list key with a string (no type error), per Redis.
	Dispatch(database, request("RPUSH", "k", "a"))
	if got := Dispatch(database, request("SET", "k", "v")); !reflect.DeepEqual(got, protocol.Value{Type: protocol.TypeSimpleString, Str: "OK"}) {
		t.Errorf("SET over list = %#v, want OK", got)
	}
}
