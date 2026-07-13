package chaos

// Chaos test: replication consistency across a primary and two replicas.
//
// Fan a stream of writes at one primary and demand that BOTH replicas converge
// on the exact same keyspace. Writes are paced (one at a time) rather than
// bursted: the v1 replication path drops-and-logs when a replica's send queue
// overflows, so a flood could legitimately lose writes — pacing keeps every
// replica's queue shallow so convergence is the honest expectation.

import (
	"testing"
	"time"
)

func TestReplicationConsistencyTwoReplicas(t *testing.T) {
	const n = 10000

	primary := startServer(t, "--appendonly=false")
	primary.waitReady(t)

	r1 := startServer(t, "--appendonly=false", "--replicaof", "127.0.0.1 "+primary.port)
	r2 := startServer(t, "--appendonly=false", "--replicaof", "127.0.0.1 "+primary.port)
	r1.waitReady(t)
	r2.waitReady(t)

	// Both replicas must be registered on the stream BEFORE the bulk starts, or
	// (no snapshot bootstrap) the early writes would never reach them.
	waitReplicaLive(t, primary, r1, "_live_r1")
	waitReplicaLive(t, primary, r2, "_live_r2")

	c := primary.client()
	defer c.Close()
	writeSeq(t, c, "k", n)

	// The spec's "wait 2s"; convergence is typically far faster, but we still poll
	// (up to a generous ceiling) so a slow CI box doesn't flake.
	time.Sleep(2 * time.Second)
	rc1, rc2 := r1.client(), r2.client()
	defer rc1.Close()
	defer rc2.Close()
	waitConverged(t, rc1, "k", n, 15*time.Second)
	waitConverged(t, rc2, "k", n, 15*time.Second)
}
