// Package db is the in-memory key/value store at the heart of the server.
//
// Phase 1 deliberately uses a SINGLE sync.RWMutex guarding ONE map for the
// entire keyspace. That is the simplest thing that is correct, but it is also a
// scalability bottleneck: every write serialises against every other read and
// write, no matter which key is involved. Two clients touching two completely
// unrelated keys still contend on the same lock.
//
// Phase 6 will replace this with a sharded design (many maps, each with its own
// lock) so unrelated keys can be mutated in parallel. Until then we keep the
// implementation intentionally simple and slow.
package db

import "sync"

// DB is a concurrency-safe map from string keys to opaque byte-slice values.
//
// Values are []byte rather than string because Redis values are "binary safe":
// they may contain NUL bytes or arbitrary encodings. The RESP bulk-string type
// that carries them on the wire is already a []byte, so storing []byte avoids a
// conversion (and the associated copy) on every read and write.
type DB struct {
	// mu guards data. Callers must never touch data without holding mu in the
	// appropriate mode: RLock for reads, Lock for writes.
	mu sync.RWMutex

	// data is the whole keyspace. One map, one lock — see the package comment.
	data map[string][]byte
}

// New returns an empty, ready-to-use DB. The map is allocated up front so that
// the zero-length store still accepts writes without a nil-map panic.
func New() *DB {
	return &DB{data: make(map[string][]byte)}
}

// Get returns the value stored at key and whether the key existed.
//
// It takes only a READ lock (RLock), so any number of concurrent Gets can run
// in parallel; they block only while some writer holds the exclusive lock. The
// returned slice aliases the value held inside the store, so callers must treat
// it as read-only — mutating it would corrupt the stored value without holding
// the lock. (Our only caller, the GET command, just hands it to the encoder,
// which reads it.)
func (db *DB) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	value, ok := db.data[key]
	return value, ok
}

// Set stores value under key, overwriting any existing value. It takes the
// exclusive WRITE lock (Lock), which blocks all other readers and writers for
// the duration of the update.
//
// We copy the incoming bytes before storing them so the store OWNS its data
// outright. Without the copy the stored value would alias the caller's buffer
// (ultimately the decoder's read buffer), and could change out from under us if
// that buffer were later reused or mutated. The copy is done before taking the
// lock so we hold the (global) write lock for as short a time as possible.
func (db *DB) Set(key string, value []byte) {
	stored := make([]byte, len(value))
	copy(stored, value)

	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = stored
}

// Del removes each of the given keys that is present and returns how many keys
// were actually deleted. Keys that do not exist are skipped and do not count, so
// the returned number is the count of real deletions (this matches Redis DEL,
// where deleting the same key twice in one call counts only once because the
// second occurrence is already gone). Write lock.
func (db *DB) Del(keys ...string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	removed := 0
	for _, key := range keys {
		if _, ok := db.data[key]; ok {
			delete(db.data, key)
			removed++
		}
	}
	return removed
}

// Exists returns how many of the given keys are present. A key listed more than
// once is counted once per occurrence — i.e. EXISTS k k returns 2 when k exists
// — which matches Redis EXISTS semantics. Read lock.
func (db *DB) Exists(keys ...string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	count := 0
	for _, key := range keys {
		if _, ok := db.data[key]; ok {
			count++
		}
	}
	return count
}
