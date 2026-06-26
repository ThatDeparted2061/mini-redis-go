package persistence

// AOF rewrite (a.k.a. compaction).
//
// The append-only log grows forever: it records the COMMAND, not the resulting
// state, so every write ever applied stays on disk. A counter SET a million
// times leaves a million SET frames even though only the last value survives.
// Recovery still works, but the file and the replay time grow without bound.
//
// Compaction fixes that by rewriting the log as a SNAPSHOT of the current
// keyspace: one command per key that recreates its present value (SET / RPUSH /
// HSET / SADD), plus a PEXPIRE for any key that still carries a TTL. Replaying
// that snapshot from an empty store reproduces exactly the same state as
// replaying the full history — using only as many commands as there are keys.
//
// The new log is written to a sibling ".tmp" file and then os.Rename'd over the
// real path. Rename is atomic on POSIX: a concurrent reader (a crash-and-replay)
// always sees either the whole old file or the whole new one, never a torn mix.

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Rewrite compacts the log to the snapshot in records and atomically swaps it in
// for the live log.
//
// The caller is expected to hold whatever lock keeps the keyspace still while the
// snapshot is taken and swapped in (in this server, the write lock) so the new
// log can't miss a write that lands mid-rewrite — see the server's rewriteAOF.
// This method only owns the file mechanics, under a.mu so it can't race an
// Append or the everysec fsync ticker.
//
// On any failure the original log is left untouched (the half-written ".tmp" is
// removed), so a rewrite that errors never costs durability — it just doesn't
// compact this time.
func (a *AOF) Rewrite(path string, records []db.SnapshotEntry) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 1. Write the snapshot to a fresh temp file and make it durable. We fsync
	//    before trusting it as the new log: the rename below makes it THE file, so
	//    its bytes must already be on disk, not just in the page cache.
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	if err := writeSnapshot(w, records); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}

	// 2. Atomically replace the old log with the snapshot. After this the old
	//    inode is unlinked; our existing a.f still points at it, so it must not be
	//    used for further writes.
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}

	// 3. Reopen the path (now the snapshot) for appending, so future writes land
	//    after the snapshot rather than in the unlinked old file. Open the new
	//    handle BEFORE dropping the old one: if the open somehow fails we return
	//    with the old (still-open) handle intact rather than a closed, unusable
	//    AOF.
	nf, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("reopen after rewrite: %w", err)
	}
	a.w.Flush() // normally nothing buffered (Append flushes), but be safe
	a.f.Close()
	a.f = nf
	a.w = bufio.NewWriter(nf)

	// The compacted file is the new size baseline the rewrite trigger measures
	// growth against (see ShouldRewrite).
	if info, err := nf.Stat(); err == nil {
		a.baseSize = info.Size()
	}
	return nil
}

// writeSnapshot encodes records as the minimal command set that recreates them,
// in RESP wire format — the same frames a client would have sent, so replay runs
// them through the ordinary dispatcher with no special-casing.
func writeSnapshot(w *bufio.Writer, records []db.SnapshotEntry) error {
	for _, r := range records {
		if err := encodeRecord(w, r); err != nil {
			return err
		}
	}
	return nil
}

// encodeRecord writes the command(s) that recreate a single key: one
// value-restoring command, then a PEXPIRE if the key still has a TTL.
func encodeRecord(w *bufio.Writer, r db.SnapshotEntry) error {
	var value protocol.Value
	switch r.Kind {
	case db.SnapshotString:
		value = bulkCmd("SET", []byte(r.Key), r.Str)

	case db.SnapshotList:
		// RPUSH appends in argument order, and r.List is head-to-tail, so
		// "RPUSH key l0 l1 ..." rebuilds the list exactly.
		if len(r.List) == 0 {
			return nil // an emptied list is auto-deleted, so this shouldn't occur
		}
		value = bulkCmd("RPUSH", prependKey(r.Key, r.List)...)

	case db.SnapshotHash:
		// HSET key f0 v0 f1 v1 ... — interleave the parallel field/value slices.
		args := make([][]byte, 0, 1+2*len(r.Fields))
		args = append(args, []byte(r.Key))
		for i := range r.Fields {
			args = append(args, r.Fields[i], r.Values[i])
		}
		if len(args) == 1 {
			return nil // emptied hashes are auto-deleted
		}
		value = bulkCmd("HSET", args...)

	case db.SnapshotSet:
		if len(r.Members) == 0 {
			return nil // emptied sets are auto-deleted
		}
		value = bulkCmd("SADD", prependKey(r.Key, r.Members)...)

	default:
		return fmt.Errorf("rewrite: unknown snapshot kind %d for key %q", r.Kind, r.Key)
	}

	if err := protocol.Encode(w, value); err != nil {
		return err
	}
	return encodeExpire(w, r)
}

// encodeExpire writes a PEXPIRE for a key that still carries a TTL, and nothing
// for a persistent key.
//
// The TTL is written as the REMAINING milliseconds, which on replay are applied
// relative to replay time, not the original deadline. That matches the existing
// log's behaviour (it replays the verbatim EXPIRE/PEXPIRE, also relative to
// replay) and is the same imprecision real Redis avoids with PEXPIREAT (an
// absolute deadline) — a command this server doesn't implement yet.
func encodeExpire(w *bufio.Writer, r db.SnapshotEntry) error {
	if r.ExpireAt.IsZero() {
		return nil
	}
	ms := time.Until(r.ExpireAt).Milliseconds()
	if ms < 1 {
		// Snapshot already dropped expired keys, so the key is live; if it has
		// under a millisecond left, give it the smallest positive TTL rather than
		// PEXPIRE 0, which would delete it on replay.
		ms = 1
	}
	return protocol.Encode(w, bulkCmd("PEXPIRE", []byte(r.Key), []byte(strconv.FormatInt(ms, 10))))
}

// bulkCmd builds a RESP command array (all bulk strings) from a command name and
// its binary-safe arguments — the exact shape the server appends for a live
// write, so the snapshot is indistinguishable from ordinary logged commands.
func bulkCmd(name string, args ...[]byte) protocol.Value {
	arr := make([]protocol.Value, 0, len(args)+1)
	arr = append(arr, protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte(name)})
	for _, a := range args {
		arr = append(arr, protocol.Value{Type: protocol.TypeBulkString, Bulk: a})
	}
	return protocol.Value{Type: protocol.TypeArray, Array: arr}
}

// prependKey returns [key, elems...] — the argument list for a variadic write
// like RPUSH or SADD whose first argument is the key.
func prependKey(key string, elems [][]byte) [][]byte {
	out := make([][]byte, 0, len(elems)+1)
	out = append(out, []byte(key))
	return append(out, elems...)
}
