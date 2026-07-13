package chaos

// Chaos test: fsync=always durability under SIGKILL.
//
// The promise of --appendfsync always is "no acknowledged write is ever lost,
// even to a hard kill": each write is fsync'd to disk BEFORE the server replies,
// so an OK the client received means the bytes are already on the platter. This
// test makes that promise falsifiable — write 10k keys, wait for every ack,
// SIGKILL (-9, no graceful flush), restart from the log alone, and demand all
// 10k back.

import (
	"path/filepath"
	"testing"
)

func TestFsyncAlwaysDurabilityUnderKill(t *testing.T) {
	const n = 10000
	aof := filepath.Join(t.TempDir(), "appendonly.aof")

	// --- First run: fsync-every-write, load 10k, then get -9'd. ---
	sp := startServer(t, "--aof-path", aof, "--appendfsync", "always")
	sp.waitReady(t)

	c := sp.clientNoTimeout()
	// Every ack from writeBurst means that write's fsync already returned, because
	// FsyncAlways syncs inside Append before the reply is sent. So once this
	// returns, all n are durable — no sleep, no flush needed.
	writeBurst(t, c, "k", n)
	_ = c.Close()

	sp.kill() // SIGKILL: the process vanishes with no chance to flush anything.

	// --- Restart: recovery is purely from the on-disk log. ---
	sp2 := startServer(t, "--aof-path", aof, "--appendfsync", "always")
	sp2.waitReady(t)
	c2 := sp2.clientNoTimeout()
	defer c2.Close()

	assertAllPresent(t, c2, "k", n) // all 10k, or the durability promise is broken.
}
