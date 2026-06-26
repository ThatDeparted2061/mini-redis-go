package persistence_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ThatDeparted2061/mini-redis-go/internal/persistence"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// command builds a RESP command array (e.g. ["SET","k","v"]) shaped exactly like
// one the server decodes off the wire, so it can be appended to and replayed from
// the log just as a real command would be.
func command(parts ...string) protocol.Value {
	arr := make([]protocol.Value, len(parts))
	for i, p := range parts {
		arr[i] = protocol.Value{Type: protocol.TypeBulkString, Bulk: []byte(p)}
	}
	return protocol.Value{Type: protocol.TypeArray, Array: arr}
}

// TestAppendThenReplayRoundTrips is the core guarantee: whatever is appended
// comes back out of Replay byte-for-byte and in order. That is what lets recovery
// re-run history exactly.
func TestAppendThenReplayRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), persistence.DefaultFilename)

	aof, err := persistence.Open(path, persistence.FsyncAlways)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	want := []protocol.Value{
		command("SET", "k", "v"),
		command("RPUSH", "l", "a", "b"),
		command("DEL", "k"),
	}
	for _, c := range want {
		if err := aof.Append(c); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var got []protocol.Value
	n, err := persistence.Replay(path, func(c protocol.Value) error {
		got = append(got, c)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if n != len(want) {
		t.Fatalf("Replay applied %d commands; want %d", n, len(want))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed commands = %v; want %v", got, want)
	}
}

// TestReplayMissingFileIsNoOp checks that a first-ever start — no log on disk —
// recovers nothing and reports no error, rather than failing to boot.
func TestReplayMissingFileIsNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.aof")

	n, err := persistence.Replay(path, func(protocol.Value) error {
		t.Fatal("apply must not be called for a missing log")
		return nil
	})
	if err != nil || n != 0 {
		t.Fatalf("Replay(missing) = %d, %v; want 0, nil", n, err)
	}
}

// TestReplayToleratesTruncatedTail simulates the file a kill -9 leaves behind:
// some whole commands followed by a half-written final frame. Replay must apply
// the intact commands and stop cleanly at the tear instead of erroring out.
func TestReplayToleratesTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), persistence.DefaultFilename)

	aof, err := persistence.Open(path, persistence.FsyncNo)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := aof.Append(command("SET", "a", "1")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := aof.Append(command("SET", "b", "2")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Append a command frame cut off partway through, as if the process died
	// mid-write.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := f.WriteString("*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1"); err != nil {
		t.Fatalf("write torn frame: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn: %v", err)
	}

	n, err := persistence.Replay(path, func(protocol.Value) error { return nil })
	if err != nil {
		t.Fatalf("Replay over truncated tail returned error: %v", err)
	}
	if n != 2 {
		t.Fatalf("Replay applied %d commands; want 2 (torn tail ignored)", n)
	}
}

// TestEverySecRoundTrips opens in the everysec mode (which spawns a background
// fsync goroutine), appends, and closes — Close must stop the goroutine cleanly
// (no hang) and the data must replay back. Run under -race to catch a fsync
// racing the close. The 1s ticker never fires in this fast test; Close's own
// fsync is what makes the data durable, which is the point.
func TestEverySecRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), persistence.DefaultFilename)

	aof, err := persistence.Open(path, persistence.FsyncEverySec)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := aof.Append(command("SET", "k", "v")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	n, err := persistence.Replay(path, func(protocol.Value) error { return nil })
	if err != nil || n != 1 {
		t.Fatalf("Replay = %d, %v; want 1, nil", n, err)
	}
}
