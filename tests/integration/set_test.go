package integration

// Set-command coverage driven by the upstream go-redis/v9 client, proving the
// server is wire-compatible for SADD/SREM/SISMEMBER/SMEMBERS/SCARD and that it
// returns WRONGTYPE the way real Redis does. Uses the startServer helper from
// basic_test.go.

import (
	"sort"
	"strings"
	"testing"
)

func TestSetRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	// SADD counts only new members; the duplicate "a" collapses, so 2.
	if n, err := client.SAdd(ctx, "s", "a", "b", "a").Result(); err != nil || n != 2 {
		t.Fatalf("SADD = %d, %v; want 2, nil", n, err)
	}
	if n, err := client.SCard(ctx, "s").Result(); err != nil || n != 2 {
		t.Fatalf("SCARD = %d, %v; want 2, nil", n, err)
	}

	if ok, err := client.SIsMember(ctx, "s", "a").Result(); err != nil || !ok {
		t.Fatalf("SISMEMBER a = %v, %v; want true, nil", ok, err)
	}
	if ok, err := client.SIsMember(ctx, "s", "z").Result(); err != nil || ok {
		t.Fatalf("SISMEMBER z = %v, %v; want false, nil", ok, err)
	}

	members, err := client.SMembers(ctx, "s").Result()
	if err != nil {
		t.Fatalf("SMEMBERS: %v", err)
	}
	sort.Strings(members)
	if strings.Join(members, ",") != "a,b" {
		t.Fatalf("SMEMBERS = %v; want [a b]", members)
	}

	if n, err := client.SRem(ctx, "s", "a").Result(); err != nil || n != 1 {
		t.Fatalf("SREM = %d, %v; want 1, nil", n, err)
	}
}

// TestSetWrongType confirms WRONGTYPE crosses the wire: a set op on a string key
// fails with a real Redis-style error.
func TestSetWrongType(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	if err := client.Set(ctx, "str", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := client.SAdd(ctx, "str", "m").Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("SADD on string err = %v; want WRONGTYPE", err)
	}
}
