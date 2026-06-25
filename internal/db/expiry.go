package db

// This file holds everything to do with key expiry (TTLs):
//
//   - the Entry.expired predicate,
//   - the EXPIRE/PEXPIRE/TTL/PTTL/PERSIST store operations,
//   - expireIfNeeded, the lazy "delete on access" path GET escalates to, and
//   - RunActiveExpiry, the background reaper.
//
// Redis uses TWO complementary expiry strategies and so do we:
//
//   - LAZY (passive): a key is checked when it is accessed and deleted if it has
//     expired. Cheap, but on its own it leaks memory for keys that are written
//     with a TTL and then never read again.
//   - ACTIVE: a background loop periodically samples the keyspace and evicts
//     expired keys even if nothing touches them, bounding that leak.
//
// Expiry is stored inline on the Entry (expireAt); a zero expireAt means the key
// is persistent. This is simpler than Redis's separate "expires" dictionary, at
// the cost of skipping persistent keys while sampling (see activeExpiryPass).

import (
	"context"
	"time"
)

// expired reports whether the entry carries a TTL that has elapsed by t. A zero
// expireAt means the key is persistent and never expires. The boundary is
// inclusive: the key is expired the instant t reaches expireAt.
func (e *Entry) expired(t time.Time) bool {
	return !e.expireAt.IsZero() && !t.Before(e.expireAt)
}

// expireIfNeeded lazily evicts key if it has expired, reporting whether it
// deleted it. It takes the WRITE lock and RE-CHECKS expiry under it, because
// between a caller's read-lock observation (e.g. in Get) and this call the key
// may have been refreshed (EXPIRE/PERSIST) or replaced (SET). Callers must hold
// no lock.
func (db *DB) expireIfNeeded(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if e, ok := db.data[key]; ok && e.expired(time.Now()) {
		delete(db.data, key)
		return true
	}
	return false
}

// Expire sets key to expire after d, replacing any existing TTL, and reports
// whether the key existed (and so received the TTL). A non-positive d deletes the
// key immediately and still reports true, matching Redis EXPIRE with a past
// deadline. An already-expired key is treated as absent. Write lock.
func (db *DB) Expire(key string, d time.Duration) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	e, ok := db.liveEntry(key)
	if !ok {
		return false
	}
	if d <= 0 {
		delete(db.data, key)
		return true
	}
	e.expireAt = time.Now().Add(d)
	return true
}

// Persist removes key's TTL so it never expires, reporting whether a TTL was
// actually cleared: false if the key is absent/expired or already had none.
// Write lock.
func (db *DB) Persist(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	e, ok := db.liveEntry(key)
	if !ok || e.expireAt.IsZero() {
		return false
	}
	e.expireAt = time.Time{}
	return true
}

// TTL returns key's remaining time to live and two flags the TTL/PTTL commands
// turn into their wire sentinels: exists is false when the key is absent or
// expired (reported as -2), and hasTTL is false when the key exists but is
// persistent (reported as -1). When both are true, remaining is the time left.
// Read lock.
func (db *DB) TTL(key string) (remaining time.Duration, exists, hasTTL bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	e, ok := db.peek(key)
	if !ok {
		return 0, false, false
	}
	if e.expireAt.IsZero() {
		return 0, true, false
	}
	return time.Until(e.expireAt), true, true
}

// Active-expiry tuning, mirroring Redis's defaults closely enough to reason about
// in the same terms.
const (
	// activeExpiryInterval is how often a sampling cycle runs.
	activeExpiryInterval = 100 * time.Millisecond
	// activeExpirySample is how many TTL-bearing keys a single pass inspects.
	activeExpirySample = 20
	// activeExpiryThreshold is the expired fraction of a sample above which the
	// keyspace is judged "still dirty", triggering another pass in the same cycle.
	activeExpiryThreshold = 0.25
)

// RunActiveExpiry runs the active-expiry loop until ctx is cancelled, then
// returns. Launch one per DB for the process lifetime, e.g. from the server:
//
//	go db.RunActiveExpiry(ctx)
//
// It COMPLEMENTS lazy expiry. Lazy expiry only reclaims a key when something
// touches it, so a key set with a TTL and then never accessed would sit in
// memory forever; this loop bounds that by sampling the keyspace and evicting
// expired keys even when no client asks for them.
//
// Each tick runs at least one sampling pass and keeps running passes while they
// stay "hot" (see activeExpiryPass) — Redis's adaptive heuristic: if a sample is
// mostly expired the keyspace is probably full of dead keys, so drain harder now
// instead of waiting for the next tick.
func (db *DB) RunActiveExpiry(ctx context.Context) {
	ticker := time.NewTicker(activeExpiryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for db.activeExpiryPass() {
				// Keep draining within this tick while passes stay hot. This
				// terminates: every hot pass deletes the expired keys it sampled,
				// so the population of expired keys strictly shrinks.
			}
		}
	}
}

// activeExpiryPass samples up to activeExpirySample keys that carry a TTL,
// deletes the expired ones, and reports whether the sample was "hot" — more than
// activeExpiryThreshold expired — so the caller knows to run another pass.
//
// The sample is drawn simply by ranging the map: Go randomises map iteration
// order, so each pass inspects a different, effectively random slice of the
// keyspace without us maintaining any extra index. Only keys WITH a TTL are
// sampled — they are the meaningful denominator for the 25% ratio — so persistent
// keys are skipped, mirroring how Redis samples from its separate "expires" dict.
// Write lock.
func (db *DB) activeExpiryPass() bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now()
	sampled, expired := 0, 0
	for key, e := range db.data {
		if e.expireAt.IsZero() {
			continue // persistent key: not a candidate
		}

		sampled++
		if e.expired(now) {
			delete(db.data, key) // safe to delete the current key while ranging
			expired++
		}
		if sampled >= activeExpirySample {
			break
		}
	}

	return sampled > 0 && float64(expired)/float64(sampled) > activeExpiryThreshold
}
