package chaos

// Chaos test: a replica dies mid-stream and a fresh one reconnects.
//
// What this proves: after a replica is SIGKILL'd and a new replica process
// connects, the primary cleans up the dead feed, accepts the new one, and writes
// made AFTER the reconnect flow through and converge. That is the reconnect
// contract that actually holds in v1.
//
// What this deliberately does NOT assert: that the reconnected replica catches
// up on writes made while it was DOWN. mini-redis v1 has no snapshot bootstrap
// and no resync (see CLAUDE.md / replication_test.go) — a restarted replica
// starts EMPTY and only takes new writes. So we assert exactly that boundary:
// post-reconnect writes converge; pre-reconnect writes are gone. Full catch-up
// is the documented upgrade (stream a snapshot before the live feed).

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestReplicaReconnectResumesStream(t *testing.T) {
	primary := startServer(t, "--appendonly=false")
	primary.waitReady(t)

	// --- Replica 1 up; mirror a batch, then kill it. ---
	r1 := startServer(t, "--appendonly=false", "--replicaof", "127.0.0.1 "+primary.port)
	r1.waitReady(t)
	waitReplicaLive(t, primary, r1, "_live_a")

	pc := primary.client()
	defer pc.Close()
	writeSeq(t, pc, "a", 100)
	waitConverged(t, r1.client(), "a", 100, 10*time.Second) // r1 really has phase A.

	r1.kill() // SIGKILL the replica mid-stream.

	// --- Writes made while the replica is DOWN (lost to it forever in v1). ---
	writeSeq(t, pc, "b", 100)

	// --- A brand-new replica reconnects (starts empty, resumes the live feed). ---
	r2 := startServer(t, "--appendonly=false", "--replicaof", "127.0.0.1 "+primary.port)
	r2.waitReady(t)
	waitReplicaLive(t, primary, r2, "_live_c")

	// Post-reconnect writes must converge — the actual reconnect guarantee — and
	// well within the spec's 30s.
	writeSeq(t, pc, "c", 100)
	rc := r2.client()
	defer rc.Close()
	waitConverged(t, rc, "c", 100, 30*time.Second)

	// The no-bootstrap boundary: neither the pre-kill batch nor the during-downtime
	// batch is on the reconnected replica.
	ctx := context.Background()
	if v, err := rc.Get(ctx, dataKey("a", 0)).Result(); err != redis.Nil {
		t.Errorf("reconnected replica has pre-kill key a:0 = (%q, %v), want redis.Nil (no bootstrap)", v, err)
	}
	if v, err := rc.Get(ctx, dataKey("b", 0)).Result(); err != redis.Nil {
		t.Errorf("reconnected replica has during-downtime key b:0 = (%q, %v), want redis.Nil (no resync)", v, err)
	}
}
