package db

// This file holds the SHARDING layer: the keyspace is not one map under one lock
// but shardCount independent maps, each under its own RWMutex.
//
// Why: a single global lock means two clients touching two completely unrelated
// keys still queue behind each other — every write serialises against every other
// read and write in the whole store. Splitting the keyspace into N shards, and
// sending each key to a shard by hashing its name, means unrelated keys almost
// always land in different shards and so take different locks. On a multi-core
// machine that lets N cores mutate N different keys at literally the same time,
// which is where the throughput win comes from.
//
// The routing rule is: shard = FNV-1a(key) % shardCount. Hashing the key's bytes
// gives an even spread across shards AND is deterministic, so a given key always
// resolves to the same shard — required, or two operations on the same key could
// take different locks and race.

import (
	"hash/fnv"
	"sync"
	"time"
)

// shardCount is how many independent slices the keyspace is split across. 32 is
// the Redis-ish default from the spec: comfortably above typical core counts, so
// the shard a core wants is almost never the shard another core is holding, yet
// small enough that iterating all shards (Snapshot, active expiry) stays cheap.
// A power of two keeps the hash % shardCount distribution even.
const shardCount = 32

// shard is one slice of the keyspace: a map of keys to entries, plus the lock
// that guards it. Callers must hold mu in the right mode (RLock read, Lock write)
// before touching data — exactly the discipline the old single DB.mu required,
// now per shard. The expiry-aware lookup helpers (peek, liveEntry, and the typed
// listEntry/hashEntry/setEntry) hang off *shard because each only ever touches
// one shard's map: the caller has already resolved and locked the right one.
type shard struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

// shardFor returns the shard that owns key. Every keyed operation routes through
// here to find the single lock it must take.
func (db *DB) shardFor(key string) *shard {
	return &db.shards[shardIndex(key)]
}

// shardIndex hashes key with FNV-1a and folds the result into a shard number.
// FNV-1a is a fast non-cryptographic hash — we only need an even spread, not
// collision resistance — and hashing the key's bytes makes the mapping stable,
// so the same key always routes to the same shard.
//
// ponytail: uses hash/fnv per the spec, which allocates a hasher (and a []byte
// copy of the key) per call — on THE hottest path, one call per command. If the
// benchmark shows it, inline the ~5-line FNV-1a loop over a local uint32 reading
// key directly; that's the alloc-free upgrade.
func shardIndex(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % shardCount
}

// peek returns the entry stored at key if it exists and has not expired,
// reporting an expired key as absent WITHOUT deleting it. Callers must already
// hold the shard's read lock, so it cannot reclaim the expired key itself; that
// is left to GET's lazy path, the write helpers, and the active reaper (see
// expiry.go). Every read command funnels its lookup through here so expiry is
// honoured uniformly across types.
func (sh *shard) peek(key string) (*Entry, bool) {
	e, ok := sh.data[key]
	if !ok || e.expired(time.Now()) {
		return nil, false
	}
	return e, true
}

// liveEntry returns the entry stored at key if it exists and has not expired,
// lazily DELETING it if it has. Callers must hold the shard's write lock. Every
// write command funnels its lookup through here, which is also what makes a write
// to an expired key resurrect it as a fresh value instead of mutating stale data.
func (sh *shard) liveEntry(key string) (*Entry, bool) {
	e, ok := sh.data[key]
	if !ok {
		return nil, false
	}
	if e.expired(time.Now()) {
		delete(sh.data, key)
		return nil, false
	}
	return e, true
}
