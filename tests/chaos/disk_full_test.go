package chaos

// Chaos test: the append-only log's disk fills up.
//
// We mount a tiny (10 MB) tmpfs, point the AOF at it, and pour in far more than
// it can hold. The design contract (server.applyRecord): an append that fails
// with ENOSPC is LOGGED and the client still gets its reply — the in-memory
// mutation already happened, so the server stays up and responsive; it just
// can't make that write durable. And the log must not be CORRUPTED: a restart
// replays cleanly (Replay tolerates a torn trailing frame), recovering every
// key that made it to disk before the wall.
//
// Requires Linux + root (mount) + the sch_netem-free bits are irrelevant here,
// just `mount`/`umount`. Elsewhere it SKIPS.

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDiskFullIsGracefulAndUncorrupted(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("disk-full test needs a Linux tmpfs mount")
	}
	if os.Geteuid() != 0 {
		t.Skipf("disk-full test needs root to mount tmpfs (euid=%d)", os.Geteuid())
	}

	mnt := filepath.Join(t.TempDir(), "tmpfs")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}
	// A 10 MB tmpfs. Unmount before the TempDir cleanup runs (LIFO) so RemoveAll
	// doesn't trip over a live mount.
	if out, err := exec.Command("mount", "-t", "tmpfs", "-o", "size=10m", "tmpfs", mnt).CombinedOutput(); err != nil {
		t.Skipf("could not mount tmpfs: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("umount", mnt).Run() })

	aof := filepath.Join(mnt, "appendonly.aof")

	// fsync=no keeps the fill fast; ENOSPC still surfaces at the buffered flush
	// inside Append regardless of fsync policy.
	sp := startServer(t, "--aof-path", aof, "--appendfsync", "no")
	sp.waitReady(t)

	// Pour ~32 MB of 4 KiB values into a 10 MB tmpfs: it WILL hit ENOSPC partway.
	// Every SET still returns OK (durability is silently dropped + logged), so we
	// can't detect "full" from the client — we detect it from the server log.
	ctx := context.Background()
	c := sp.client()
	val := strings.Repeat("x", 4096)
	const n = 8000
	for i := 0; i < n; i++ {
		if err := c.Set(ctx, dataKey("k", i), val, 0).Err(); err != nil {
			t.Fatalf("SET %s errored (server should absorb ENOSPC, not surface it): %v", dataKey("k", i), err)
		}
	}

	// Graceful contract #1: the ENOSPC was caught and logged, not fatal.
	if !strings.Contains(sp.stderr(), "aof append failed") {
		t.Fatalf("expected an 'aof append failed' log after filling the disk; server log:\n%s", sp.stderr())
	}
	// Graceful contract #2: the server is still alive and serving.
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("server unresponsive after disk filled: %v", err)
	}
	if got, err := c.Get(ctx, dataKey("k", 0)).Result(); err != nil || got != val {
		t.Fatalf("early key unreadable after disk full: got len=%d err=%v", len(got), err)
	}
	_ = c.Close()

	sp.kill() // crash it hard, mid-full-disk.

	// No-corruption contract: a restart replays the (possibly torn-tailed) log
	// without error, and the early keys that reached disk come back intact.
	sp2 := startServer(t, "--aof-path", aof, "--appendfsync", "no")
	sp2.waitReady(t) // becoming ready == Replay succeeded == no mid-file corruption.
	c2 := sp2.client()
	defer c2.Close()
	if got, err := c2.Get(ctx, dataKey("k", 0)).Result(); err != nil || got != val {
		t.Fatalf("after restart, early key k:0 corrupted/lost: got len=%d err=%v", len(got), err)
	}
}
