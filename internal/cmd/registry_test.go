package cmd

import (
	"reflect"
	"testing"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// request builds a RESP command frame (an array of bulk strings) the same way a
// real client would, e.g. request("SET", "k", "v"). It keeps the test cases
// terse and readable.
func request(parts ...string) protocol.Value {
	arr := make([]protocol.Value, len(parts))
	for i, p := range parts {
		arr[i] = protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte(p)}
	}
	return protocol.Value{Type: protocol.TypeArray, Array: arr}
}

// TestDispatch walks one shared DB through the six phase-1 commands and checks
// each reply. Cases run in order against the same database so that, for example,
// GET sees what SET stored.
func TestDispatch(t *testing.T) {
	database := db.New()

	tests := []struct {
		name string
		req  protocol.Value
		want protocol.Value
	}{
		{
			name: "PING no arg -> PONG",
			req:  request("PING"),
			want: protocol.Value{Type: protocol.TypeSimpleString, Str: "PONG"},
		},
		{
			name: "PING with message echoes it",
			req:  request("PING", "hello"),
			want: protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte("hello")},
		},
		{
			name: "ECHO returns its argument",
			req:  request("ECHO", "world"),
			want: protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte("world")},
		},
		{
			name: "lower-case command name still dispatches",
			req:  request("echo", "case"),
			want: protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte("case")},
		},
		{
			name: "GET on missing key -> null bulk",
			req:  request("GET", "k"),
			want: protocol.Value{Type: protocol.TypeBulkString, Bulk: nil},
		},
		{
			name: "SET -> OK",
			req:  request("SET", "k", "v"),
			want: protocol.Value{Type: protocol.TypeSimpleString, Str: "OK"},
		},
		{
			name: "GET after SET returns the value",
			req:  request("GET", "k"),
			want: protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte("v")},
		},
		{
			name: "EXISTS counts present keys (with repeats)",
			req:  request("EXISTS", "k", "k", "missing"),
			want: protocol.Value{Type: protocol.TypeInteger, Int: 2},
		},
		{
			name: "DEL removes and counts real deletions",
			req:  request("DEL", "k", "missing"),
			want: protocol.Value{Type: protocol.TypeInteger, Int: 1},
		},
		{
			name: "GET after DEL is null again",
			req:  request("GET", "k"),
			want: protocol.Value{Type: protocol.TypeBulkString, Bulk: nil},
		},
		{
			name: "unknown command -> error",
			req:  request("NOPE"),
			want: protocol.Value{Type: protocol.TypeError, Str: "ERR unknown command 'NOPE'"},
		},
		{
			name: "wrong arity -> error",
			req:  request("SET", "only-key"),
			want: protocol.Value{Type: protocol.TypeError, Str: "ERR wrong number of arguments for 'set' command"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Dispatch(database, tt.req)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Dispatch() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
