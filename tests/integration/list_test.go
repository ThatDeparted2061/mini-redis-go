package integration

// List-command coverage driven by the upstream go-redis/v9 client, proving the
// server is wire-compatible for LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN and that it
// returns WRONGTYPE the way real Redis does. Uses the startServer helper from
// basic_test.go.

import (
	"strings"
	"testing"
)

func TestListRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	// RPUSH appends; LPUSH prepends in reverse. Final order: [y x a b c].
	if n, err := client.RPush(ctx, "l", "a", "b", "c").Result(); err != nil || n != 3 {
		t.Fatalf("RPUSH = %d, %v; want 3, nil", n, err)
	}
	if n, err := client.LPush(ctx, "l", "x", "y").Result(); err != nil || n != 5 {
		t.Fatalf("LPUSH = %d, %v; want 5, nil", n, err)
	}

	if n, err := client.LLen(ctx, "l").Result(); err != nil || n != 5 {
		t.Fatalf("LLEN = %d, %v; want 5, nil", n, err)
	}

	got, err := client.LRange(ctx, "l", 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE: %v", err)
	}
	want := []string{"y", "x", "a", "b", "c"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("LRANGE = %v; want %v", got, want)
	}

	if v, err := client.LPop(ctx, "l").Result(); err != nil || v != "y" {
		t.Fatalf("LPOP = %q, %v; want \"y\", nil", v, err)
	}
	if v, err := client.RPop(ctx, "l").Result(); err != nil || v != "c" {
		t.Fatalf("RPOP = %q, %v; want \"c\", nil", v, err)
	}
}

// TestListWrongType confirms WRONGTYPE crosses the wire as a real Redis-style
// error: GET on a list key, and a list op on a string key, both fail with it.
func TestListWrongType(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	if err := client.RPush(ctx, "list", "a").Err(); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	if err := client.Get(ctx, "list").Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("GET on list err = %v; want WRONGTYPE", err)
	}

	if err := client.Set(ctx, "str", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := client.LPush(ctx, "str", "x").Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("LPUSH on string err = %v; want WRONGTYPE", err)
	}
}
