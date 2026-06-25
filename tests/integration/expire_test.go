package integration

// TTL / expiry coverage driven by the upstream go-redis/v9 client: EXPIRE/TTL/
// PERSIST round-trips and end-to-end eviction of an expired key. Uses the
// startServer helper from basic_test.go.

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestExpireRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}

	// EXPIRE sets a TTL; TTL reads it back within range.
	if ok, err := client.Expire(ctx, "k", 100*time.Second).Result(); err != nil || !ok {
		t.Fatalf("EXPIRE = %v, %v; want true, nil", ok, err)
	}
	if ttl, err := client.TTL(ctx, "k").Result(); err != nil || ttl <= 0 || ttl > 100*time.Second {
		t.Fatalf("TTL = %v, %v; want (0, 100s]", ttl, err)
	}

	// PERSIST removes it; TTL then reports "no expiry" as a negative duration.
	if ok, err := client.Persist(ctx, "k").Result(); err != nil || !ok {
		t.Fatalf("PERSIST = %v, %v; want true, nil", ok, err)
	}
	if ttl, err := client.TTL(ctx, "k").Result(); err != nil || ttl >= 0 {
		t.Fatalf("TTL after PERSIST = %v, %v; want negative (no expiry)", ttl, err)
	}
}

// TestExpireEviction proves an expired key becomes invisible over the wire: once
// a short TTL elapses, GET reports the key missing and EXISTS counts it as gone.
func TestExpireEviction(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := client.PExpire(ctx, "k", 30*time.Millisecond).Err(); err != nil {
		t.Fatalf("PEXPIRE: %v", err)
	}

	time.Sleep(80 * time.Millisecond)

	if _, err := client.Get(ctx, "k").Result(); err != redis.Nil {
		t.Fatalf("GET after expiry err = %v; want redis.Nil", err)
	}
	if n, err := client.Exists(ctx, "k").Result(); err != nil || n != 0 {
		t.Fatalf("EXISTS after expiry = %d, %v; want 0, nil", n, err)
	}
}
