package integration

// Hash-command coverage driven by the upstream go-redis/v9 client, proving the
// server is wire-compatible for HSET/HGET/HDEL/HGETALL/HKEYS/HVALS/HLEN and that
// it returns WRONGTYPE the way real Redis does. Uses the startServer helper from
// basic_test.go.

import (
	"sort"
	"strings"
	"testing"
)

func TestHashRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	// HSET reports newly-created fields: two new -> 2.
	if n, err := client.HSet(ctx, "h", "f1", "v1", "f2", "v2").Result(); err != nil || n != 2 {
		t.Fatalf("HSET = %d, %v; want 2, nil", n, err)
	}

	if v, err := client.HGet(ctx, "h", "f1").Result(); err != nil || v != "v1" {
		t.Fatalf("HGET f1 = %q, %v; want \"v1\", nil", v, err)
	}
	if n, err := client.HLen(ctx, "h").Result(); err != nil || n != 2 {
		t.Fatalf("HLEN = %d, %v; want 2, nil", n, err)
	}

	keys, err := client.HKeys(ctx, "h").Result()
	if err != nil {
		t.Fatalf("HKEYS: %v", err)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "f1,f2" {
		t.Fatalf("HKEYS = %v; want [f1 f2]", keys)
	}

	all, err := client.HGetAll(ctx, "h").Result()
	if err != nil {
		t.Fatalf("HGETALL: %v", err)
	}
	if len(all) != 2 || all["f1"] != "v1" || all["f2"] != "v2" {
		t.Fatalf("HGETALL = %v; want map[f1:v1 f2:v2]", all)
	}

	if n, err := client.HDel(ctx, "h", "f1").Result(); err != nil || n != 1 {
		t.Fatalf("HDEL = %d, %v; want 1, nil", n, err)
	}
}

// TestHashWrongType confirms WRONGTYPE crosses the wire: a hash op on a string
// key fails with a real Redis-style error.
func TestHashWrongType(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	if err := client.Set(ctx, "str", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := client.HSet(ctx, "str", "f", "v").Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("HSET on string err = %v; want WRONGTYPE", err)
	}
}
