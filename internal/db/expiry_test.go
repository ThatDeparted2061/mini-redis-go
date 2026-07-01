package db

import (
	"context"
	"testing"
	"time"
)

// put inserts a raw entry directly, bypassing the public API, so tests can plant
// keys with a specific expireAt (including ones already in the past) and exercise
// the expiry machinery deterministically without sleeping.
func put(d *DB, key string, e *Entry) {
	sh := d.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.data[key] = e
}

// rawLen reports how many keys physically remain in the store, expired or not, so
// tests can assert actual reclamation rather than just external visibility. It
// sums every shard's map, RLocking one at a time.
func rawLen(d *DB) int {
	n := 0
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.RLock()
		n += len(sh.data)
		sh.mu.RUnlock()
	}
	return n
}

func TestExpiredPredicate(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		e    *Entry
		want bool
	}{
		{"zero expireAt never expires", &Entry{}, false},
		{"future expiry not expired", &Entry{expireAt: now.Add(time.Minute)}, false},
		{"past expiry is expired", &Entry{expireAt: now.Add(-time.Minute)}, true},
	}
	for _, c := range cases {
		if got := c.e.expired(now); got != c.want {
			t.Errorf("%s: expired = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestLazyExpiryOnGet plants an already-expired string and checks GET both reports
// it absent AND physically deletes it (the lazy "delete on access" path).
func TestLazyExpiryOnGet(t *testing.T) {
	d := New()
	put(d, "k", &Entry{kind: kindString, str: []byte("v"), expireAt: time.Now().Add(-time.Second)})

	if _, ok, err := d.Get("k"); ok || err != nil {
		t.Fatalf("Get expired = ok %v, err %v; want absent, nil", ok, err)
	}
	if n := rawLen(d); n != 0 {
		t.Errorf("after GET, store holds %d keys; want 0 (lazily deleted)", n)
	}
}

// TestExpireTTLPersist walks the TTL command surface at the store level.
func TestExpireTTLPersist(t *testing.T) {
	d := New()
	d.Set("k", []byte("v"))

	// Fresh key: exists, but no TTL.
	if _, exists, hasTTL := d.TTL("k"); !exists || hasTTL {
		t.Fatalf("TTL fresh key = exists %v hasTTL %v; want true, false", exists, hasTTL)
	}

	if !d.Expire("k", time.Minute) {
		t.Fatal("Expire on existing key = false; want true")
	}
	remaining, exists, hasTTL := d.TTL("k")
	if !exists || !hasTTL || remaining <= 0 || remaining > time.Minute {
		t.Fatalf("TTL after Expire = %v (exists %v hasTTL %v); want ~1m", remaining, exists, hasTTL)
	}

	if !d.Persist("k") {
		t.Fatal("Persist on key with TTL = false; want true")
	}
	if _, _, hasTTL := d.TTL("k"); hasTTL {
		t.Fatal("TTL after Persist still reports a TTL; want none")
	}
	if d.Persist("k") {
		t.Fatal("Persist on key without TTL = true; want false")
	}

	// Expire on a missing key reports false.
	if d.Expire("nope", time.Minute) {
		t.Fatal("Expire on missing key = true; want false")
	}
	// A non-positive TTL deletes the key immediately (and still reports true).
	if !d.Expire("k", -time.Second) {
		t.Fatal("Expire negative on existing key = false; want true")
	}
	if _, exists, _ := d.TTL("k"); exists {
		t.Fatal("key still present after negative Expire; want deleted")
	}
}

// TestWriteResurrectsExpiredKey checks the liveEntry path: writing to an expired
// key starts FRESH rather than appending to stale data or returning WRONGTYPE.
func TestWriteResurrectsExpiredKey(t *testing.T) {
	d := New()
	put(d, "l", &Entry{kind: kindList, list: [][]byte{[]byte("stale")}, expireAt: time.Now().Add(-time.Second)})

	n, err := d.RPush("l", []byte("fresh"))
	if err != nil || n != 1 {
		t.Fatalf("RPush onto expired list = %d, %v; want 1, nil (fresh list)", n, err)
	}
	got, _ := d.LRange("l", 0, -1)
	if len(got) != 1 || string(got[0]) != "fresh" {
		t.Fatalf("list after resurrect = %q; want [fresh]", got)
	}
}

// TestActiveExpiryPass checks one sampling pass deletes expired keys, keeps live
// and persistent ones, and reports "hot" when the sample is mostly expired.
func TestActiveExpiryPass(t *testing.T) {
	d := New()
	past := time.Now().Add(-time.Second)
	future := time.Now().Add(time.Minute)

	// 3 expired (with TTL), 1 live (with TTL), 1 persistent (must be ignored).
	put(d, "e1", &Entry{kind: kindString, expireAt: past})
	put(d, "e2", &Entry{kind: kindString, expireAt: past})
	put(d, "e3", &Entry{kind: kindString, expireAt: past})
	put(d, "live", &Entry{kind: kindString, expireAt: future})
	put(d, "persist", &Entry{kind: kindString})

	if !d.activeExpiryPass() {
		t.Error("pass with 3/4 expired TTL keys = not hot; want hot (>25%)")
	}
	if _, exists, _ := d.TTL("live"); !exists {
		t.Error("live key was evicted; want kept")
	}
	if n := rawLen(d); n != 2 {
		t.Errorf("after pass, store holds %d keys; want 2 (live + persistent)", n)
	}
}

// TestRunActiveExpiryReapsAndStops proves the background loop evicts a
// set-and-forgotten expired key (ACTIVE, not lazy, since nothing accesses it) and
// that it returns promptly when ctx is cancelled.
func TestRunActiveExpiryReapsAndStops(t *testing.T) {
	d := New()
	put(d, "k", &Entry{kind: kindString, expireAt: time.Now().Add(-time.Second)})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.RunActiveExpiry(ctx)
		close(done)
	}()

	// The reaper ticks every 100ms; within a second it must reclaim the key with
	// nobody accessing it.
	deadline := time.After(time.Second)
	for rawLen(d) != 0 {
		select {
		case <-deadline:
			t.Fatal("active expiry did not reap the key within 1s")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunActiveExpiry did not return after ctx cancel")
	}
}
