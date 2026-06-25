package cmd

import (
	"reflect"
	"testing"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// TestExpireAndTTL walks the EXPIRE/TTL/PTTL/PERSIST reply mapping, including the
// -1 (no TTL) and -2 (no key) sentinels.
func TestExpireAndTTL(t *testing.T) {
	database := db.New()
	Dispatch(database, request("SET", "k", "v"))

	// A key with no TTL -> -1; a missing key -> -2.
	if got := Dispatch(database, request("TTL", "k")); !reflect.DeepEqual(got, integer(-1)) {
		t.Errorf("TTL no-expiry = %#v, want -1", got)
	}
	if got := Dispatch(database, request("TTL", "missing")); !reflect.DeepEqual(got, integer(-2)) {
		t.Errorf("TTL missing = %#v, want -2", got)
	}

	// EXPIRE on an existing key -> 1; on a missing key -> 0.
	if got := Dispatch(database, request("EXPIRE", "k", "100")); !reflect.DeepEqual(got, integer(1)) {
		t.Errorf("EXPIRE = %#v, want 1", got)
	}
	if got := Dispatch(database, request("EXPIRE", "missing", "100")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("EXPIRE missing = %#v, want 0", got)
	}

	// TTL now reports ~100s (rounding may yield 99 or 100); PTTL ~100000ms.
	if got := Dispatch(database, request("TTL", "k")); got.Type != protocol.TypeInteger || got.Int < 99 || got.Int > 100 {
		t.Errorf("TTL after EXPIRE 100 = %#v, want ~100", got)
	}
	if got := Dispatch(database, request("PTTL", "k")); got.Type != protocol.TypeInteger || got.Int < 99000 || got.Int > 100000 {
		t.Errorf("PTTL after EXPIRE 100 = %#v, want ~100000", got)
	}

	// PERSIST removes the TTL -> 1; TTL is -1 again; a second PERSIST -> 0.
	if got := Dispatch(database, request("PERSIST", "k")); !reflect.DeepEqual(got, integer(1)) {
		t.Errorf("PERSIST = %#v, want 1", got)
	}
	if got := Dispatch(database, request("TTL", "k")); !reflect.DeepEqual(got, integer(-1)) {
		t.Errorf("TTL after PERSIST = %#v, want -1", got)
	}
	if got := Dispatch(database, request("PERSIST", "k")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("PERSIST again = %#v, want 0", got)
	}
}

// TestExpireNonPositiveDeletes covers the rule that a non-positive timeout
// deletes the key immediately (and still replies 1).
func TestExpireNonPositiveDeletes(t *testing.T) {
	database := db.New()
	Dispatch(database, request("SET", "k", "v"))

	if got := Dispatch(database, request("EXPIRE", "k", "0")); !reflect.DeepEqual(got, integer(1)) {
		t.Errorf("EXPIRE 0 = %#v, want 1", got)
	}
	if got := Dispatch(database, request("EXISTS", "k")); !reflect.DeepEqual(got, integer(0)) {
		t.Errorf("EXISTS after EXPIRE 0 = %#v, want 0", got)
	}
	if got := Dispatch(database, request("GET", "k")); !reflect.DeepEqual(got, nullBulk) {
		t.Errorf("GET after EXPIRE 0 = %#v, want null bulk", got)
	}
}

// TestExpireArgErrors covers arity and non-integer argument handling.
func TestExpireArgErrors(t *testing.T) {
	database := db.New()
	notInt := protocol.Value{Type: protocol.TypeError, Str: "ERR value is not an integer or out of range"}

	if got := Dispatch(database, request("EXPIRE", "k")); got.Type != protocol.TypeError {
		t.Errorf("EXPIRE arity = %#v, want error", got)
	}
	Dispatch(database, request("SET", "k", "v"))
	if got := Dispatch(database, request("EXPIRE", "k", "soon")); !reflect.DeepEqual(got, notInt) {
		t.Errorf("EXPIRE non-int = %#v, want not-integer error", got)
	}
	if got := Dispatch(database, request("TTL")); got.Type != protocol.TypeError {
		t.Errorf("TTL arity = %#v, want error", got)
	}
}
