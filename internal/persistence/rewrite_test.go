package persistence

// White-box tests (package persistence) for AOF compaction: they reach into the
// AOF's unexported baseSize to drive the rewrite trigger, and verify that a
// rewrite swaps a compact snapshot in for a bloated history and that the file
// still replays cleanly afterwards.

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// decodeStrings turns a decoded command frame (an array of bulk strings) into a
// plain []string so tests can compare against literals. The test data is all
// ASCII, so the round trip through string is lossless.
func decodeStrings(v protocol.Value) []string {
	out := make([]string, len(v.Array))
	for i, e := range v.Array {
		out[i] = string(e.Bulk)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRewriteCompactsAndReplays is the core guarantee: a bloated log rewritten
// to a small snapshot (a) shrinks on disk, (b) leaves no temp file behind, (c)
// replays back to exactly the snapshot's commands, and (d) still accepts further
// appends through the swapped-in handle.
func TestRewriteCompactsAndReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultFilename)

	aof, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Bloat the log: the same key set 200 times, the history a rewrite collapses.
	for i := 0; i < 200; i++ {
		c := bulkCmd("SET", []byte("s"), []byte(strconv.Itoa(i)))
		if err := aof.Append(c); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	bloated, err := aof.Size()
	if err != nil {
		t.Fatalf("size before: %v", err)
	}

	// The snapshot we compact down to: one entry per type, plus a TTL key.
	records := []db.SnapshotEntry{
		{Key: "s", Kind: db.SnapshotString, Str: []byte("final")},
		{Key: "l", Kind: db.SnapshotList, List: [][]byte{[]byte("a"), []byte("b"), []byte("c")}},
		{Key: "h", Kind: db.SnapshotHash, Fields: [][]byte{[]byte("f")}, Values: [][]byte{[]byte("v")}},
		{Key: "set", Kind: db.SnapshotSet, Members: [][]byte{[]byte("m")}},
		{Key: "t", Kind: db.SnapshotString, Str: []byte("x"), ExpireAt: time.Now().Add(time.Hour)},
	}
	if err := aof.Rewrite(path, records); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	compacted, err := aof.Size()
	if err != nil {
		t.Fatalf("size after: %v", err)
	}
	if compacted >= bloated {
		t.Fatalf("rewrite did not shrink the log: %d -> %d bytes", bloated, compacted)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("rewrite left a temp file behind: %v", err)
	}

	// The reopened handle must still work: append one more command past the snapshot.
	if err := aof.Append(bulkCmd("SET", []byte("after"), []byte("1"))); err != nil {
		t.Fatalf("append after rewrite: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var got []protocol.Value
	n, err := Replay(path, func(c protocol.Value) error {
		got = append(got, c)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	// 5 records, but the TTL key emits two commands (SET then PEXPIRE), plus the
	// one post-rewrite append = 7 frames total, in snapshot-then-append order.
	want := [][]string{
		{"SET", "s", "final"},
		{"RPUSH", "l", "a", "b", "c"},
		{"HSET", "h", "f", "v"},
		{"SADD", "set", "m"},
		{"SET", "t", "x"},
		nil, // PEXPIRE t <ms> — checked separately, the ms is time-dependent
		{"SET", "after", "1"},
	}
	if n != len(want) {
		t.Fatalf("replayed %d commands; want %d: %v", n, len(want), got)
	}
	for i, w := range want {
		if w == nil {
			continue
		}
		if g := decodeStrings(got[i]); !equalStrings(g, w) {
			t.Fatalf("command %d = %v; want %v", i, g, w)
		}
	}

	// The PEXPIRE frame: key "t", and a positive TTL no larger than the hour we set.
	pexp := decodeStrings(got[5])
	if len(pexp) != 3 || pexp[0] != "PEXPIRE" || pexp[1] != "t" {
		t.Fatalf("PEXPIRE frame = %v; want [PEXPIRE t <ms>]", pexp)
	}
	ms, err := strconv.ParseInt(pexp[2], 10, 64)
	if err != nil || ms <= 0 || ms > time.Hour.Milliseconds() {
		t.Fatalf("PEXPIRE ms = %q (%v); want a positive value <= %d", pexp[2], err, time.Hour.Milliseconds())
	}
}

// TestShouldRewrite checks the trigger logic: a rewrite fires only once the log
// is both past the size floor AND has grown to twice its baseline.
func TestShouldRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultFilename)

	aof, err := Open(path, FsyncNo)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = aof.Close() }()

	// A tiny log is below the size floor, so no rewrite however much it "grew"
	// relative to its near-zero baseline.
	if err := aof.Append(bulkCmd("SET", []byte("k"), []byte("v"))); err != nil {
		t.Fatalf("append small: %v", err)
	}
	if aof.ShouldRewrite() {
		t.Fatalf("ShouldRewrite true for a sub-floor log")
	}

	// Push the log past the 64 KiB floor with one big value; baseline is still ~0,
	// so it is well over 2x and a rewrite is due.
	big := make([]byte, minRewriteSize+1024)
	if err := aof.Append(bulkCmd("SET", []byte("big"), big)); err != nil {
		t.Fatalf("append big: %v", err)
	}
	if !aof.ShouldRewrite() {
		t.Fatalf("ShouldRewrite false for a log past the floor with a ~0 baseline")
	}

	// Raise the baseline to the current size: now the log is over the floor but has
	// NOT grown 2x since the baseline, so no rewrite.
	size, err := aof.Size()
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	aof.baseSize = size
	if aof.ShouldRewrite() {
		t.Fatalf("ShouldRewrite true when the log has not doubled its baseline")
	}
}
