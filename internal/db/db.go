// Package db is the in-memory key/value store at the heart of the server.
//
// The keyspace is SHARDED: instead of one map under one global RWMutex, it is
// split across shardCount independent maps, each with its own lock (see
// shard.go). A key is routed to a shard by hashing its name, so operations on
// keys that hash to different shards take different locks and run in parallel —
// two clients touching two unrelated keys no longer queue behind one global
// lock. Every keyed method here follows the same shape: resolve the key's shard
// with db.shardFor(key), then lock THAT shard (RLock to read, Lock to write).
//
// Whole-keyspace operations (Snapshot, active expiry) are the exception: they
// visit every shard, locking one at a time — see snapshot.go and expiry.go.
package db

import (
	"errors"
	"time"
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
	// shards is the keyspace, split into shardCount independent maps each under
	// its own lock (see shard.go). shardFor(key) picks the one a key belongs to.
	shards [shardCount]shard

	// pubsub is the process-wide message bus (PUBLISH/SUBSCRIBE). It is
	// independent of the keyspace above — see pubsub.go — and reached via PubSub().
	pubsub *Broker
}

// New returns an empty, ready-to-use DB. Each shard's map is allocated up front
// so the zero-length store still accepts writes without a nil-map panic.
func New() *DB {
	db := &DB{pubsub: NewBroker()}
	for i := range db.shards {
		db.shards[i].data = make(map[string]*Entry)
	}
	return db
}

// PubSub returns the database's pub/sub broker — the message bus that PUBLISH and
// SUBSCRIBE run against. It is exposed here so a command handler (which is only
// handed the DB) can reach the broker without a separate plumbing path.
func (db *DB) PubSub() *Broker { return db.pubsub }

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
// Get also drives LAZY expiry: if the key is found expired it is deleted and
// reported absent — the canonical "delete on access". The common, non-expired
// case stays on the READ lock so concurrent Gets still run in parallel; only an
// expired key pays a brief escalation to the write lock (via expireIfNeeded) to
// evict it. The returned slice aliases the stored value, so treat it as read-only.
func (db *DB) Get(key string) ([]byte, bool, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.RUnlock()
		return nil, false, nil
	}
	// An expired key is invisible regardless of its type, so check expiry before
	// the type check: GET on an expired list key is a miss, not a WRONGTYPE.
	if e.expired(time.Now()) {
		sh.mu.RUnlock()
		db.expireIfNeeded(key)
		return nil, false, nil
	}
	if e.kind != kindString {
		sh.mu.RUnlock()
		return nil, false, ErrWrongType
	}
	value := e.str
	sh.mu.RUnlock()
	return value, true, nil
}

// Set stores value as a string under key, overwriting any existing value AND its
// type — SET on a key that currently holds a list replaces it with the string,
// which is why it never returns a type error. Write lock.
//
// The bytes are copied before storing (see cloneBytes) so the store owns its
// data; the copy is done before taking the lock to minimise time under it.
func (db *DB) Set(key string, value []byte) {
	stored := cloneBytes(value)

	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	sh.data[key] = &Entry{kind: kindString, str: stored}
}

// ---------------------------------------------------------------------------
// Generic key commands (type-agnostic)
// ---------------------------------------------------------------------------

// Del removes each of the given keys that is present and returns how many keys
// were actually deleted, regardless of their type. Keys that do not exist are
// skipped and do not count (so deleting the same key twice in one call counts
// once, matching Redis DEL).
//
// Each key is locked in its own shard as it is processed rather than under one
// global lock, so DEL of keys in different shards is not one atomic step — but
// each key's delete is independent and the returned count is exact, which is all
// DEL promises.
func (db *DB) Del(keys ...string) int {
	removed := 0
	for _, key := range keys {
		sh := db.shardFor(key)
		sh.mu.Lock()
		// liveEntry drops an already-expired key and reports it absent, so DEL
		// neither counts nor double-frees a key the TTL has already retired.
		if _, ok := sh.liveEntry(key); ok {
			delete(sh.data, key)
			removed++
		}
		sh.mu.Unlock()
	}
	return removed
}

// Exists returns how many of the given keys are present, regardless of type. A
// key listed more than once is counted once per occurrence — EXISTS k k returns
// 2 when k exists — matching Redis EXISTS semantics. Read lock (per key's shard).
func (db *DB) Exists(keys ...string) int {
	count := 0
	for _, key := range keys {
		sh := db.shardFor(key)
		sh.mu.RLock()
		if _, ok := sh.peek(key); ok {
			count++
		}
		sh.mu.RUnlock()
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
// a non-list value. Callers must hold the shard's write lock.
func (sh *shard) listEntry(key string, create bool) (*Entry, error) {
	e, ok := sh.liveEntry(key)
	if !ok {
		if !create {
			return nil, nil
		}
		e = &Entry{kind: kindList}
		sh.data[key] = e
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
	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := sh.listEntry(key, true)
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
	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := sh.listEntry(key, true)
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
	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := sh.listEntry(key, false)
	if err != nil {
		return nil, false, err
	}
	if e == nil || len(e.list) == 0 {
		return nil, false, nil
	}

	v := e.list[0]
	e.list = e.list[1:]
	if len(e.list) == 0 {
		delete(sh.data, key)
	}
	return v, true, nil
}

// RPop removes and returns the tail element of the list at key. Like LPop it
// reports false for a missing/empty list and deletes the key when the list
// becomes empty. WRONGTYPE if key holds a non-list. Write lock.
func (db *DB) RPop(key string) ([]byte, bool, error) {
	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := sh.listEntry(key, false)
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
		delete(sh.data, key)
	}
	return v, true, nil
}

// LLen returns the length of the list at key, or 0 if the key is absent.
// WRONGTYPE if key holds a non-list. Read lock.
func (db *DB) LLen(key string) (int, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
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
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
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

// ---------------------------------------------------------------------------
// Hash type
//
// A hash is an unordered map from field name to value, backed by a Go
// map[string][]byte. Field names are the map keys; values are []byte and stay
// binary-safe exactly like top-level string values. Like an empty list, a hash
// that loses its last field is deleted so an empty hash never lingers. There is
// no field ordering — Go map iteration is randomised, which matches Redis, where
// hash field order is unspecified.
// ---------------------------------------------------------------------------

// hashEntry returns the hash Entry at key for mutation, creating an empty one if
// the key is absent and create is true. It returns ErrWrongType if the key holds
// a non-hash value. Callers must hold the shard's write lock.
func (sh *shard) hashEntry(key string, create bool) (*Entry, error) {
	e, ok := sh.liveEntry(key)
	if !ok {
		if !create {
			return nil, nil
		}
		e = &Entry{kind: kindHash, hash: make(map[string][]byte)}
		sh.data[key] = e
		return e, nil
	}
	if e.kind != kindHash {
		return nil, ErrWrongType
	}
	return e, nil
}

// HSet sets each field to its matching value in the hash at key (fields and
// values are parallel slices and must be the same length), creating the hash if
// the key does not exist. It returns the number of fields that were NEWLY added;
// fields that already existed are overwritten but not counted, matching Redis
// HSET. WRONGTYPE if key holds a non-hash. Write lock.
func (db *DB) HSet(key string, fields, values [][]byte) (int, error) {
	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := sh.hashEntry(key, true)
	if err != nil {
		return 0, err
	}

	added := 0
	for i, f := range fields {
		name := string(f)
		if _, exists := e.hash[name]; !exists {
			added++
		}
		e.hash[name] = cloneBytes(values[i])
	}
	return added, nil
}

// HGet returns the value of field in the hash at key and whether that field was
// present. A missing key and a missing field both report false. WRONGTYPE if key
// holds a non-hash. The returned slice aliases the stored value (read-only). Read lock.
func (db *DB) HGet(key, field string) ([]byte, bool, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
	if !ok {
		return nil, false, nil
	}
	if e.kind != kindHash {
		return nil, false, ErrWrongType
	}
	v, ok := e.hash[field]
	if !ok {
		return nil, false, nil
	}
	return v, true, nil
}

// HDel removes each of the given fields from the hash at key and returns how many
// were actually removed (absent fields are skipped). When the last field is
// removed the key is deleted. A missing key removes nothing. WRONGTYPE if key
// holds a non-hash. Write lock.
func (db *DB) HDel(key string, fields ...string) (int, error) {
	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := sh.hashEntry(key, false)
	if err != nil {
		return 0, err
	}
	if e == nil {
		return 0, nil
	}

	removed := 0
	for _, f := range fields {
		if _, ok := e.hash[f]; ok {
			delete(e.hash, f)
			removed++
		}
	}
	if len(e.hash) == 0 {
		delete(sh.data, key)
	}
	return removed, nil
}

// HGetAll returns the fields and values of the hash at key as two parallel slices
// (fields[i] holds values[i]). The order is unspecified. A missing key yields
// empty slices. WRONGTYPE if key holds a non-hash. Values alias the stored bytes
// (read-only). Read lock.
func (db *DB) HGetAll(key string) (fields, values [][]byte, err error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
	if !ok {
		return [][]byte{}, [][]byte{}, nil
	}
	if e.kind != kindHash {
		return nil, nil, ErrWrongType
	}

	fields = make([][]byte, 0, len(e.hash))
	values = make([][]byte, 0, len(e.hash))
	for f, v := range e.hash {
		fields = append(fields, []byte(f))
		values = append(values, v)
	}
	return fields, values, nil
}

// HKeys returns the field names of the hash at key (unspecified order); a missing
// key yields an empty slice. WRONGTYPE if key holds a non-hash. Read lock.
func (db *DB) HKeys(key string) ([][]byte, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
	if !ok {
		return [][]byte{}, nil
	}
	if e.kind != kindHash {
		return nil, ErrWrongType
	}
	keys := make([][]byte, 0, len(e.hash))
	for f := range e.hash {
		keys = append(keys, []byte(f))
	}
	return keys, nil
}

// HVals returns the values of the hash at key (unspecified order, but consistent
// with HKeys within a single call is NOT guaranteed); a missing key yields an
// empty slice. Values alias the stored bytes (read-only). WRONGTYPE if key holds
// a non-hash. Read lock.
func (db *DB) HVals(key string) ([][]byte, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
	if !ok {
		return [][]byte{}, nil
	}
	if e.kind != kindHash {
		return nil, ErrWrongType
	}
	vals := make([][]byte, 0, len(e.hash))
	for _, v := range e.hash {
		vals = append(vals, v)
	}
	return vals, nil
}

// HLen returns the number of fields in the hash at key, or 0 if the key is
// absent. WRONGTYPE if key holds a non-hash. Read lock.
func (db *DB) HLen(key string) (int, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
	if !ok {
		return 0, nil
	}
	if e.kind != kindHash {
		return 0, ErrWrongType
	}
	return len(e.hash), nil
}

// ---------------------------------------------------------------------------
// Set type
//
// A set is an unordered collection of distinct members, backed by a Go
// map[string]struct{} — the empty struct is Go's idiomatic value-less map,
// chosen because only membership matters, not any associated value. Members are
// the map keys, so duplicates collapse for free. Like empty lists and hashes, a
// set that loses its last member is deleted.
// ---------------------------------------------------------------------------

// setEntry returns the set Entry at key for mutation, creating an empty one if
// the key is absent and create is true. It returns ErrWrongType if the key holds
// a non-set value. Callers must hold the shard's write lock.
func (sh *shard) setEntry(key string, create bool) (*Entry, error) {
	e, ok := sh.liveEntry(key)
	if !ok {
		if !create {
			return nil, nil
		}
		e = &Entry{kind: kindSet, set: make(map[string]struct{})}
		sh.data[key] = e
		return e, nil
	}
	if e.kind != kindSet {
		return nil, ErrWrongType
	}
	return e, nil
}

// SAdd adds each member to the set at key, creating the set if the key does not
// exist, and returns how many members were NEWLY added (members already present
// are ignored). WRONGTYPE if key holds a non-set. Write lock.
func (db *DB) SAdd(key string, members ...[]byte) (int, error) {
	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := sh.setEntry(key, true)
	if err != nil {
		return 0, err
	}

	added := 0
	for _, m := range members {
		name := string(m)
		if _, exists := e.set[name]; !exists {
			e.set[name] = struct{}{}
			added++
		}
	}
	return added, nil
}

// SRem removes each member from the set at key and returns how many were actually
// removed (members not present are skipped). When the last member is removed the
// key is deleted. A missing key removes nothing. WRONGTYPE if key holds a
// non-set. Write lock.
func (db *DB) SRem(key string, members ...[]byte) (int, error) {
	sh := db.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := sh.setEntry(key, false)
	if err != nil {
		return 0, err
	}
	if e == nil {
		return 0, nil
	}

	removed := 0
	for _, m := range members {
		name := string(m)
		if _, ok := e.set[name]; ok {
			delete(e.set, name)
			removed++
		}
	}
	if len(e.set) == 0 {
		delete(sh.data, key)
	}
	return removed, nil
}

// SIsMember reports whether member is in the set at key. A missing key is an
// empty set, so it reports false. WRONGTYPE if key holds a non-set. Read lock.
func (db *DB) SIsMember(key string, member []byte) (bool, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
	if !ok {
		return false, nil
	}
	if e.kind != kindSet {
		return false, ErrWrongType
	}
	_, found := e.set[string(member)]
	return found, nil
}

// SMembers returns all members of the set at key (unspecified order). A missing
// key yields an empty slice. WRONGTYPE if key holds a non-set. Read lock.
func (db *DB) SMembers(key string) ([][]byte, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
	if !ok {
		return [][]byte{}, nil
	}
	if e.kind != kindSet {
		return nil, ErrWrongType
	}
	out := make([][]byte, 0, len(e.set))
	for m := range e.set {
		out = append(out, []byte(m))
	}
	return out, nil
}

// SCard returns the cardinality (number of members) of the set at key, or 0 if
// the key is absent. WRONGTYPE if key holds a non-set. Read lock.
func (db *DB) SCard(key string) (int, error) {
	sh := db.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.peek(key)
	if !ok {
		return 0, nil
	}
	if e.kind != kindSet {
		return 0, ErrWrongType
	}
	return len(e.set), nil
}
