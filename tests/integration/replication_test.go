package integration

// End-to-end check for the Day-14 replica handshake: a replica started with
// WithReplicaOf dials the primary, performs the REPLICAOF handshake, and then
// mirrors the primary's live writes. We assert the v1 contract exactly: writes
// made AFTER the handshake propagate; data written BEFORE it does not (no
// snapshot bootstrap).

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ThatDeparted2061/mini-redis-go/internal/server"
)

// startNamedServer brings up a server with the given options (no AOF, to keep the
// test hermetic) and returns its address plus a connected go-redis client.
func startNamedServer(t *testing.T, opts ...server.Option) (string, *redis.Client) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	srvErr := make(chan error, 1)
	srv := server.New(ln, opts...)
	go func() { srvErr <- srv.Serve(ctx) }()

	client := redis.NewClient(&redis.Options{
		Addr:            ln.Addr().String(),
		Protocol:        2,
		DisableIdentity: true,
	})

	t.Cleanup(func() {
		_ = client.Close()
		cancel()
		select {
		case <-srvErr:
		case <-time.After(2 * time.Second):
			t.Errorf("server did not shut down within 2s")
		}
	})

	return ln.Addr().String(), client
}

func TestReplicationLiveStream(t *testing.T) {
	primaryAddr, primary := startNamedServer(t)

	// A key written BEFORE any replica connects: v1 has no snapshot bootstrap, so
	// the replica must NOT see it.
	ctx := opCtx(t)
	if err := primary.Set(ctx, "before", "old", 0).Err(); err != nil {
		t.Fatalf("primary set before: %v", err)
	}

	_, replica := startNamedServer(t, server.WithReplicaOf(primaryAddr))

	// Give the replica a moment to complete the handshake before we write, so the
	// write below is part of the live stream it receives.
	waitReplicaReady(t, replica)

	if err := primary.Set(ctx, "after", "new", 0).Err(); err != nil {
		t.Fatalf("primary set after: %v", err)
	}

	// The post-handshake write must reach the replica.
	got := waitForReplica(t, replica, "after")
	if got != "new" {
		t.Errorf("replica GET after = %q, want %q", got, "new")
	}

	// The pre-handshake write must NOT be on the replica (no bootstrap).
	if v, err := replica.Get(ctx, "before").Result(); err != redis.Nil {
		t.Errorf("replica GET before = (%q, %v), want redis.Nil (no snapshot bootstrap)", v, err)
	}
}

// TestReplicaIsReadOnly is the Day-16 contract: a replica refuses client writes
// with a READONLY error, but still serves reads (values streamed from the primary).
func TestReplicaIsReadOnly(t *testing.T) {
	primaryAddr, primary := startNamedServer(t)
	_, replica := startNamedServer(t, server.WithReplicaOf(primaryAddr))
	waitReplicaReady(t, replica)

	ctx := opCtx(t)

	// A write issued by a client directly to the replica is refused.
	if err := replica.Set(ctx, "k", "v", 0).Err(); err == nil || !strings.Contains(err.Error(), "READONLY") {
		t.Fatalf("replica SET error = %v, want a READONLY error", err)
	}

	// Reads still work: a value written on the primary streams in and is readable.
	if err := primary.Set(ctx, "streamed", "yes", 0).Err(); err != nil {
		t.Fatalf("primary set: %v", err)
	}
	if got := waitForReplica(t, replica, "streamed"); got != "yes" {
		t.Errorf("replica GET streamed = %q, want %q", got, "yes")
	}
}

// waitReplicaReady blocks until the replica's own client connection is up, so the
// subsequent primary write is issued while the replica is live. Actual stream
// propagation is still asynchronous and is handled by waitForReplica's polling.
func waitReplicaReady(t *testing.T, replica *redis.Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := replica.Ping(context.Background()).Err(); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("replica never became reachable")
}

// waitForReplica polls the replica for key until it appears (replication is
// asynchronous) or a timeout fires.
func waitForReplica(t *testing.T, replica *redis.Client, key string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		v, err := replica.Get(context.Background(), key).Result()
		if err == nil {
			return v
		}
		if err != redis.Nil {
			t.Fatalf("replica GET %s: %v", key, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("key %q never replicated within timeout", key)
	return ""
}
