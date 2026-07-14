package integration

// Unix domain socket smoke tests. Same RESP path as TCP — the only difference
// is net.Listen("unix", path) vs net.Listen("tcp", …). These exist so a UDS-only
// bind (used for quieter local benchmarks) is covered by the suite.

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ThatDeparted2061/mini-redis-go/internal/server"
)

// startUnixServer brings up a server on a temp-dir Unix socket and returns a
// go-redis client connected over UDS. Cleanup shuts the server and client down.
func startUnixServer(t *testing.T) *redis.Client {
	t.Helper()

	path := filepath.Join(t.TempDir(), "mini-redis.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	srvErr := make(chan error, 1)
	srv := server.New(ln)
	go func() { srvErr <- srv.Serve(ctx) }()

	client := redis.NewClient(&redis.Options{
		Network:         "unix",
		Addr:            path,
		Protocol:        2,
		DisableIdentity: true,
	})

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client close: %v", err)
		}
		cancel()
		select {
		case err := <-srvErr:
			if err != nil {
				t.Errorf("server.Serve returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("server did not shut down within 2s")
		}
	})

	return client
}

// TestUnixPing is the UDS smoke test: PING over a Unix domain socket.
func TestUnixPing(t *testing.T) {
	client := startUnixServer(t)
	ctx := opCtx(t)

	if got, err := client.Ping(ctx).Result(); err != nil || got != "PONG" {
		t.Fatalf("PING = %q, %v; want \"PONG\", nil", got, err)
	}
}

// TestUnixSetGet checks that string commands work over UDS the same as over TCP.
func TestUnixSetGet(t *testing.T) {
	client := startUnixServer(t)
	ctx := opCtx(t)

	if err := client.Set(ctx, "foo", "bar", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	got, err := client.Get(ctx, "foo").Result()
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if got != "bar" {
		t.Fatalf("GET foo = %q; want \"bar\"", got)
	}
}

// TestDualBindTCPAndUnix proves NewMulti serves one shared keyspace on both
// transports at once (the dual-bind mode used by --port N --unixsocket PATH).
func TestDualBindTCPAndUnix(t *testing.T) {
	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	path := filepath.Join(t.TempDir(), "mini-redis.sock")
	unixLn, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	srvErr := make(chan error, 1)
	srv := server.NewMulti([]net.Listener{tcpLn, unixLn})
	go func() { srvErr <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-srvErr:
			if err != nil {
				t.Errorf("server.Serve returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("server did not shut down within 2s")
		}
	})

	tcp := redis.NewClient(&redis.Options{
		Addr:            tcpLn.Addr().String(),
		Protocol:        2,
		DisableIdentity: true,
	})
	unix := redis.NewClient(&redis.Options{
		Network:         "unix",
		Addr:            path,
		Protocol:        2,
		DisableIdentity: true,
	})
	t.Cleanup(func() {
		_ = tcp.Close()
		_ = unix.Close()
	})

	op := opCtx(t)
	if err := tcp.Set(op, "shared", "via-tcp", 0).Err(); err != nil {
		t.Fatalf("SET over TCP: %v", err)
	}
	got, err := unix.Get(op, "shared").Result()
	if err != nil {
		t.Fatalf("GET over UDS: %v", err)
	}
	if got != "via-tcp" {
		t.Fatalf("GET over UDS = %q; want \"via-tcp\" (same keyspace)", got)
	}
}
