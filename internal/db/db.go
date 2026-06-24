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

import (
	"errors"
	"sync"
)

// ErrWrongType is returned by an operation attempted against a key whose value
// is of the wrong type — e.g. GET on a list, or LPUSH on a string. The command
// layer turns it into the canonical RESP "WRONGTYPE ..." error reply; keeping
// the wire wording out of the store keeps the protocol concern in one place.
var ErrWrongType = errors.New("wrong kind of value")

// DB is a concurrency-safe map from string keys to typed values (Entry).
//
// Each key maps to an *Entry, which records both the value and its type so the
// store can reject type-mismatched operations (see ErrWrongType). String values
// are []byte because Redis values are "binary safe": they may contain NUL bytes
// or arbitrary encodings, and the RESP bulk-string type that carries them is
// already a []byte.
type DB struct {
	// mu guards data. Callers must never touch data without holding mu in the
	// appropriate mode: RLock for reads, Lock for writes.
	mu sync.RWMutex

	// data is the whole keyspace. One map, one lock — see the package comment.
	data map[string]*Entry
}

// New returns an empty, ready-to-use DB. The map is allocated up front so that
// the zero-length store still accepts writes without a nil-map panic.
func New() *DB {
	return &DB{data: make(map[string]*Entry)}
}

// cloneBytes returns an independent copy of b so the store owns its data outright
// rather than aliasing a caller's buffer (ultimately the decoder's read buffer),
// which could change out from under us if reused.
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// ---------------------------------------------------------------------------
// String type
// ---------------------------------------------------------------------------

// Get returns the string value stored at key and whether the key existed. It
// returns ErrWrongType if the key exists but holds a non-string value (e.g. a
// list), matching Redis: GET only operates on string keys.
//
// It takes only a READ lock, so any number of concurrent Gets run in parallel;
// they block only while some writer holds the exclusive lock. The returned slice
// aliases the value held inside the store, so callers must treat it as read-only.
func (db *DB) Get(key string) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	e, ok := db.data[key]
	if !ok {
		return nil, false, nil
	}
	if e.kind != kindString {
		return nil, false, ErrWrongType
	}
	return e.str, true, nil
}

// Set stores value as a string under key, overwriting any existing value AND its
// type — SET on a key that currently holds a list replaces it with the string,
// which is why it never returns a type error. Write lock.
//
// The bytes are copied before storing (see cloneBytes) so the store owns its
// data; the copy is done before taking the lock to minimise time under it.
func (db *DB) Set(key string, value []byte) {
	stored := cloneBytes(value)

	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = &Entry{kind: kindString, str: stored}
}

// ---------------------------------------------------------------------------
// Generic key commands (type-agnostic)
// ---------------------------------------------------------------------------

// Del removes each of the given keys that is present and returns how many keys
// were actually deleted, regardless of their type. Keys that do not exist are
// skipped and do not count (so deleting the same key twice in one call counts
// once, matching Redis DEL). Write lock.
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

// Exists returns how many of the given keys are present, regardless of type. A
// key listed more than once is counted once per occurrence — EXISTS k k returns
// 2 when k exists — matching Redis EXISTS semantics. Read lock.
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

// ---------------------------------------------------------------------------
// List type
//
// Lists are backed by a plain [][]byte slice (index 0 is the head). A slice
// keeps the implementation simple; the trade-off is that head operations
// (LPUSH/LPOP) are O(n) because they shift elements. That is fine at this stage
// and can be swapped for a doubly-linked list later without changing the API.
// ---------------------------------------------------------------------------

// listEntry returns the list Entry at key for mutation, creating an empty one if
// the key is absent and create is true. It returns ErrWrongType if the key holds
// a non-list value. Callers must hold the write lock.
func (db *DB) listEntry(key string, create bool) (*Entry, error) {
	e, ok := db.data[key]
	if !ok {
		if !create {
			return nil, nil
		}
		e = &Entry{kind: kindList}
		db.data[key] = e
		return e, nil
	}
	if e.kind != kindList {
		return nil, ErrWrongType
	}
	return e, nil
}

// LPush prepends each value to the head of the list at key, creating the list if
// the key does not exist, and returns the list's new length. Values are inserted
// one at a time, so "LPUSH k a b c" leaves the list as [c b a] — the new values
// end up in front, in reverse order. WRONGTYPE if key holds a non-list. Write lock.
func (db *DB) LPush(key string, values ...[]byte) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	e, err := db.listEntry(key, true)
	if err != nil {
		return 0, err
	}

	prefix := make([][]byte, 0, len(values))
	for i := len(values) - 1; i >= 0; i-- {
		prefix = append(prefix, cloneBytes(values[i]))
	}
	e.list = append(prefix, e.list...)
	return len(e.list), nil
}

// RPush appends each value to the tail of the list at key, creating the list if
// the key does not exist, and returns the list's new length. "RPUSH k a b c"
// leaves [a b c]. WRONGTYPE if key holds a non-list. Write lock.
func (db *DB) RPush(key string, values ...[]byte) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	e, err := db.listEntry(key, true)
	if err != nil {
		return 0, err
	}

	for _, v := range values {
		e.list = append(e.list, cloneBytes(v))
	}
	return len(e.list), nil
}

// LPop removes and returns the head element of the list at key. The second
// return is false when the key is absent or the list is empty. When the pop
// empties the list the key is deleted, matching Redis (empty lists do not
// linger). WRONGTYPE if key holds a non-list. Write lock.
func (db *DB) LPop(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	e, err := db.listEntry(key, false)
	if err != nil {
		return nil, false, err
	}
	if e == nil || len(e.list) == 0 {
		return nil, false, nil
	}

	v := e.list[0]
	e.list = e.list[1:]
	if len(e.list) == 0 {
		delete(db.data, key)
	}
	return v, true, nil
}

// RPop removes and returns the tail element of the list at key. Like LPop it
// reports false for a missing/empty list and deletes the key when the list
// becomes empty. WRONGTYPE if key holds a non-list. Write lock.
func (db *DB) RPop(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	e, err := db.listEntry(key, false)
	if err != nil {
		return nil, false, err
	}
	if e == nil || len(e.list) == 0 {
		return nil, false, nil
	}

	n := len(e.list)
	v := e.list[n-1]
	e.list = e.list[:n-1]
	if len(e.list) == 0 {
		delete(db.data, key)
	}
	return v, true, nil
}

// LLen returns the length of the list at key, or 0 if the key is absent.
// WRONGTYPE if key holds a non-list. Read lock.
func (db *DB) LLen(key string) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	e, ok := db.data[key]
	if !ok {
		return 0, nil
	}
	if e.kind != kindList {
		return 0, ErrWrongType
	}
	return len(e.list), nil
}

// LRange returns the elements of the list at key between start and stop,
// inclusive. Indices may be negative to count from the end (-1 is the last
// element). Out-of-range indices are clamped the way Redis clamps them, and a
// missing key yields an empty result. WRONGTYPE if key holds a non-list. Read lock.
//
// The returned slices alias the stored elements, so callers must treat them as
// read-only.
func (db *DB) LRange(key string, start, stop int) ([][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	e, ok := db.data[key]
	if !ok {
		return [][]byte{}, nil
	}
	if e.kind != kindList {
		return nil, ErrWrongType
	}

	lo, hi, ok := normalizeRange(start, stop, len(e.list))
	if !ok {
		return [][]byte{}, nil
	}

	out := make([][]byte, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, e.list[i])
	}
	return out, nil
}

// normalizeRange resolves Redis-style inclusive [start, stop] indices against a
// list of length n: negatives count from the end, the bounds are clamped into
// range, and ok is false when the range selects nothing (empty list or start
// past the end after clamping).
func normalizeRange(start, stop, n int) (lo, hi int, ok bool) {
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if n == 0 || start > stop || start >= n {
		return 0, 0, false
	}
	return start, stop, true
}
