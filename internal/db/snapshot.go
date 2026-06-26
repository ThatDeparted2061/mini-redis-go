package db

// This file exposes the keyspace as plain data so the persistence layer can
// COMPACT the append-only log (see internal/persistence/rewrite.go).
//
// The append-only log records every write command ever applied, so a key SET a
// million times leaves a million SET frames on disk even though only the last
// one matters. Compaction replaces that history with a SNAPSHOT: for each key it
// writes the single command that recreates the key's current value. To do that
// the persistence layer needs to read the current state of every key — but the
// store's internals (Entry, its kind tag, the typed fields) are unexported on
// purpose. Snapshot is the seam: it hands out a self-contained, type-tagged copy
// of the keyspace that any package can turn back into commands, without reaching
// into the store's guts.

import "time"

// SnapshotKind identifies which of a SnapshotEntry's typed fields is meaningful.
// It mirrors the store's internal kind, exported so a caller can pick the right
// recreation command (SET / RPUSH / HSET / SADD) for each key.
type SnapshotKind uint8

const (
	SnapshotString SnapshotKind = iota // value in Str
	SnapshotList                       // value in List (index 0 is the head)
	SnapshotHash                       // value in Fields/Values (parallel slices)
	SnapshotSet                        // value in Members
)

// SnapshotEntry is one key's complete state captured as plain data: the key, its
// type, the value (in whichever typed field the Kind selects), and its optional
// expiry. Every byte slice is an independent copy, so the snapshot stays valid
// and immutable even after the store is unlocked and mutated again — the caller
// can serialise it at leisure.
type SnapshotEntry struct {
	Key      string
	Kind     SnapshotKind
	Str      []byte    // Kind == SnapshotString
	List     [][]byte  // Kind == SnapshotList (index 0 is the head)
	Fields   [][]byte  // Kind == SnapshotHash, parallel to Values
	Values   [][]byte  // Kind == SnapshotHash, parallel to Fields
	Members  [][]byte  // Kind == SnapshotSet
	ExpireAt time.Time // zero == no TTL (the key is persistent)
}

// Snapshot returns a deep copy of every LIVE key's state, taken atomically under
// the read lock. Keys whose TTL has already elapsed are skipped — they are
// logically gone, so recreating them on replay would resurrect the dead.
//
// The whole keyspace is copied while the lock is held, so the cost is O(total
// data size) and peak memory briefly doubles. That is acceptable for the v1
// compactor, which runs rarely and pauses writes for its duration anyway.
//
// ponytail: deep copy under one RWMutex — O(n) and a transient 2x memory spike.
// A fork()/copy-on-write snapshot (how real Redis avoids both) is the upgrade
// path once datasets get large; unnecessary at this scale.
func (db *DB) Snapshot() []SnapshotEntry {
	db.mu.RLock()
	defer db.mu.RUnlock()

	now := time.Now()
	out := make([]SnapshotEntry, 0, len(db.data))
	for key, e := range db.data {
		if e.expired(now) {
			continue
		}

		se := SnapshotEntry{Key: key, ExpireAt: e.expireAt}
		switch e.kind {
		case kindString:
			se.Kind = SnapshotString
			se.Str = cloneBytes(e.str)
		case kindList:
			se.Kind = SnapshotList
			se.List = cloneByteSlices(e.list)
		case kindHash:
			se.Kind = SnapshotHash
			se.Fields = make([][]byte, 0, len(e.hash))
			se.Values = make([][]byte, 0, len(e.hash))
			for f, v := range e.hash {
				se.Fields = append(se.Fields, []byte(f))
				se.Values = append(se.Values, cloneBytes(v))
			}
		case kindSet:
			se.Kind = SnapshotSet
			se.Members = make([][]byte, 0, len(e.set))
			for m := range e.set {
				se.Members = append(se.Members, []byte(m))
			}
		}
		out = append(out, se)
	}
	return out
}

// cloneByteSlices deep-copies a slice of byte slices so the result shares no
// backing array with the store (see cloneBytes for the single-slice rationale).
func cloneByteSlices(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i, b := range in {
		out[i] = cloneBytes(b)
	}
	return out
}
