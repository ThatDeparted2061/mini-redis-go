package integration

// These tests exercise the server end-to-end over a real TCP socket using the
// upstream github.com/redis/go-redis/v9 client. Driving the server with a real
// Redis client (rather than our own encoder/decoder) is the whole point: it
// proves the server is wire-compatible with what actual Redis clients send and
// expect, not just self-consistent.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ThatDeparted2061/mini-redis-go/internal/server"
)

// startServer brings up a server instance on an OS-chosen port and returns a
// go-redis client connected to it. It registers cleanup that closes the client
// and shuts the server down, so each test gets an isolated, fresh database.
//
// Listening on port 0 lets the kernel pick any free port, so tests never fight
// over a fixed port and can run in parallel without collisions.
func startServer(t *testing.T) *redis.Client {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	// Serve runs until ctx is cancelled (which closes the listener and drains
	// connections). We run it in a goroutine and surface its return via a
	// channel so cleanup can wait for a clean shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	srvErr := make(chan error, 1)
	srv := server.New(ln)
	go func() { srvErr <- srv.Serve(ctx) }()

	client := redis.NewClient(&redis.Options{
		Addr: ln.Addr().String(),
		// This server speaks RESP2 and implements neither HELLO nor CLIENT
		// SETINFO. Pinning Protocol to 2 and disabling the identity handshake
		// stops go-redis from failing the connection on those unknown commands.
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

// opCtx returns a short-lived context so a hung server fails the test fast
// instead of blocking the whole suite.
func opCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestPing is the smoke test: if PING round-trips, the listen -> accept ->
// decode -> dispatch -> encode pipeline is wired up correctly.
func TestPing(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	if got, err := client.Ping(ctx).Result(); err != nil || got != "PONG" {
		t.Fatalf("PING = %q, %v; want \"PONG\", nil", got, err)
	}
}

// TestSetGet covers the core happy path: a value written with SET reads back
// identically with GET.
func TestSetGet(t *testing.T) {
	client := startServer(t)
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

// TestGetMissing checks that GET on an absent key reports "no such key". The
// server replies with a null bulk string, which go-redis surfaces as redis.Nil
// — distinct from a key that holds an empty value.
func TestGetMissing(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	_, err := client.Get(ctx, "nope").Result()
	if err != redis.Nil {
		t.Fatalf("GET missing key err = %v; want redis.Nil", err)
	}
}

// TestDel verifies DEL removes a key and reports how many keys it actually
// deleted, including that deleting an absent key counts as zero.
func TestDel(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}

	n, err := client.Del(ctx, "k").Result()
	if err != nil {
		t.Fatalf("DEL: %v", err)
	}
	if n != 1 {
		t.Fatalf("DEL k = %d; want 1", n)
	}

	// The key is gone, so a second DEL deletes nothing.
	if n, err := client.Del(ctx, "k").Result(); err != nil || n != 0 {
		t.Fatalf("DEL absent = %d, %v; want 0, nil", n, err)
	}

	// And GET now reports the key as missing.
	if _, err := client.Get(ctx, "k").Result(); err != redis.Nil {
		t.Fatalf("GET after DEL err = %v; want redis.Nil", err)
	}
}
