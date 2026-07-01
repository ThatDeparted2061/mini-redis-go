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
	"sync"
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

// TestReplicaRestartStartsEmpty is the Day-17 contract (initial-sync option 1: no
// bootstrap). A replica keeps NO prior state across a restart — it starts empty
// and only accepts writes the primary makes AFTER it (re)connects. This holds
// emergently: a read-only replica never logs to its own AOF (client writes are
// refused, and the primary's stream is applied via Dispatch, bypassing the AOF),
// so a restarted replica has nothing to load and comes up empty.
func TestReplicaRestartStartsEmpty(t *testing.T) {
	primaryAddr, primary := startNamedServer(t)

	// First replica connects and mirrors a live write, then "restarts" (stops).
	replica1, stop1 := startReplica(t, primaryAddr)
	waitReplicaReady(t, replica1)
	syncReplica(t, primary, replica1, "k1", "v1")
	stop1()

	// A brand-new replica connects. A write made after it connects arrives...
	replica2, _ := startReplica(t, primaryAddr)
	waitReplicaReady(t, replica2)
	syncReplica(t, primary, replica2, "k2", "v2")

	// ...but k1, written before replica2 existed, is NOT bootstrapped to it.
	if v, err := replica2.Get(opCtx(t), "k1").Result(); err != redis.Nil {
		t.Errorf("replica2 GET k1 = (%q, %v), want redis.Nil (no initial sync)", v, err)
	}
}

// startReplica brings up a replica of primaryAddr (no AOF) and returns a client
// plus a stop func that tears just this replica down — so a test can model a
// replica "restart" mid-run. t.Cleanup also calls stop, so an untouched stop leaks
// nothing.
func startReplica(t *testing.T, primaryAddr string) (*redis.Client, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	srvErr := make(chan error, 1)
	srv := server.New(ln, server.WithReplicaOf(primaryAddr))
	go func() { srvErr <- srv.Serve(ctx) }()

	client := redis.NewClient(&redis.Options{
		Addr:            ln.Addr().String(),
		Protocol:        2,
		DisableIdentity: true,
	})

	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = client.Close()
			cancel()
			select {
			case <-srvErr:
			case <-time.After(2 * time.Second):
				t.Errorf("replica did not shut down within 2s")
			}
		})
	}
	t.Cleanup(stop)
	return client, stop
}

// syncReplica blocks until a fresh primary write is observable on the replica,
// proving the replication feed is registered and live. It RE-SENDS on each poll
// because the handshake is asynchronous: a write issued in the sliver before the
// replica registers would be missed, so one send is not enough to prove liveness.
func syncReplica(t *testing.T, primary, replica *redis.Client, key, val string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := primary.Set(context.Background(), key, val, 0).Err(); err != nil {
			t.Fatalf("primary set %s: %v", key, err)
		}
		if v, err := replica.Get(context.Background(), key).Result(); err == nil && v == val {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("write %q never reached replica within timeout", key)
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
