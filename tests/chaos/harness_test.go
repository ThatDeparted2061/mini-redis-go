// Package chaos holds the fault-injection suite: unlike the integration tests
// (which run the server in-process), these spawn the real server BINARY as an OS
// process via os/exec, because the faults we inject — SIGKILL, a tc-delayed
// network link, a full tmpfs — can only be applied to a real process, not a
// goroutine. This file is the shared harness: build the binary once, spawn/kill
// server processes, and drive them with a go-redis client.
package chaos

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// serverBin is the compiled server, built once in TestMain and reused by every
// test — building per-test would dominate the runtime.
var serverBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "chaos-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos: mktemp:", err)
		os.Exit(1)
	}
	serverBin = filepath.Join(dir, "mini-redis-server")
	build := exec.Command("go", "build", "-o", serverBin,
		"github.com/ThatDeparted2061/mini-redis-go/cmd/server")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "chaos: build server:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// lockedBuffer is a goroutine-safe sink for a child's stderr, so the wait
// goroutine and the test goroutine can write/read it without a data race.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// serverProc is a spawned server process plus everything a test needs to drive,
// observe, and tear it down.
type serverProc struct {
	cmd     *exec.Cmd
	addr    string // host:port a client dials
	port    string // listen port (for --replicaof strings)
	log     *lockedBuffer
	done    chan struct{} // closed when the process has exited
	waitErr error
}

// freePort asks the kernel for an unused TCP port and hands it back. There's a
// small window before the child claims it, but the ephemeral range is large so
// collisions are rare; a lost race just surfaces as a startup timeout.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}

// startServer spawns a server on the host with the given extra flags. Every
// chaos server disables the Prometheus endpoint (--metrics-addr "") so multiple
// processes don't collide on the default :9091.
func startServer(t *testing.T, args ...string) *serverProc {
	t.Helper()
	port := freePort(t)
	argv := append([]string{"--port", port, "--metrics-addr", ""}, args...)
	return launch(t, exec.Command(serverBin, argv...), "127.0.0.1:"+port, port)
}

// startServerNetns spawns a server inside network namespace ns (via `ip netns
// exec`), for the slow-replica test. dialHost is the address a host-side client
// uses to reach it (the namespace's veth IP).
func startServerNetns(t *testing.T, ns, dialHost string, args ...string) *serverProc {
	t.Helper()
	// The namespace has its own port space, so any number is free there; we still
	// use freePort just to pick a plausible one.
	port := freePort(t)
	argv := append([]string{"netns", "exec", ns, serverBin, "--port", port, "--metrics-addr", ""}, args...)
	return launch(t, exec.Command("ip", argv...), dialHost+":"+port, port)
}

func launch(t *testing.T, cmd *exec.Cmd, addr, port string) *serverProc {
	t.Helper()
	var buf lockedBuffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	sp := &serverProc{cmd: cmd, addr: addr, port: port, log: &buf, done: make(chan struct{})}
	go func() {
		sp.waitErr = cmd.Wait()
		close(sp.done)
	}()
	t.Cleanup(func() {
		if !sp.exited() {
			sp.stop()
		}
	})
	return sp
}

func (sp *serverProc) exited() bool {
	select {
	case <-sp.done:
		return true
	default:
		return false
	}
}

func (sp *serverProc) stderr() string { return sp.log.String() }

func (sp *serverProc) client() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: sp.addr, Protocol: 2, DisableIdentity: true})
}

// clientNoTimeout disables go-redis's per-batch read/write deadline. A deep
// pipeline under --appendfsync always makes the server fsync once per queued
// write before its replies land, which can exceed the default 3s batch deadline
// (F_FULLFSYNC on macOS is ~10ms each). Tests that flood an always-fsync server
// use this so a slow disk shows up as slowness, not a spurious i/o timeout.
func (sp *serverProc) clientNoTimeout() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: sp.addr, Protocol: 2, DisableIdentity: true,
		ReadTimeout: -1, WriteTimeout: -1,
	})
}

// kill is the SIGKILL the durability tests need: no graceful flush, the process
// just vanishes.
func (sp *serverProc) kill() {
	_ = sp.cmd.Process.Kill()
	<-sp.done
}

// stop asks for a graceful SIGTERM shutdown, escalating to SIGKILL if the server
// doesn't exit promptly.
func (sp *serverProc) stop() {
	_ = sp.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-sp.done:
	case <-time.After(5 * time.Second):
		_ = sp.cmd.Process.Kill()
		<-sp.done
	}
}

// waitReady blocks until the server answers PING, or fails the test. A PING is
// served in every mode (including a read-only replica), so it's a universal
// "are you up" probe.
func (sp *serverProc) waitReady(t *testing.T) {
	t.Helper()
	c := sp.client()
	defer c.Close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if sp.exited() {
			t.Fatalf("server exited during startup:\n%s", sp.stderr())
		}
		if err := c.Ping(context.Background()).Err(); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server never became ready within 15s:\n%s", sp.stderr())
}

// --- write / read helpers -------------------------------------------------

func dataKey(prefix string, i int) string { return prefix + ":" + strconv.Itoa(i) }

// writeSeq issues n SETs (prefix:i = i) one at a time on a single connection.
// Sequential writes are naturally paced by the round-trip, which keeps any
// attached replica's send queue shallow — so this never trips the drop-and-log
// path, and every write is meant to survive/replicate.
func writeSeq(t *testing.T, c *redis.Client, prefix string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if err := c.Set(ctx, dataKey(prefix, i), i, 0).Err(); err != nil {
			t.Fatalf("SET %s: %v", dataKey(prefix, i), err)
		}
	}
}

// writeBurst pipelines n SETs in chunks as fast as possible — used where we
// WANT to flood (durability: get all acked quickly; slow-replica: measure raw
// primary throughput).
func writeBurst(t *testing.T, c *redis.Client, prefix string, n int) {
	t.Helper()
	ctx := context.Background()
	const chunk = 1000
	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)
		if _, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i := start; i < end; i++ {
				p.Set(ctx, dataKey(prefix, i), i, 0)
			}
			return nil
		}); err != nil {
			t.Fatalf("pipelined SET [%d,%d): %v", start, end, err)
		}
	}
}

// assertAllPresent fails unless every prefix:i (i in [0,n)) reads back as i.
func assertAllPresent(t *testing.T, c *redis.Client, prefix string, n int) {
	t.Helper()
	ctx := context.Background()
	missing, wrong := 0, 0
	const chunk = 1000
	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)
		cmds := make([]*redis.StringCmd, 0, end-start)
		// Ignore the batch error: a redis.Nil from a missing key would abort it,
		// but we want to tally each key individually below.
		_, _ = c.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i := start; i < end; i++ {
				cmds = append(cmds, p.Get(ctx, dataKey(prefix, i)))
			}
			return nil
		})
		for idx, cmd := range cmds {
			i := start + idx
			v, err := cmd.Result()
			switch {
			case err == redis.Nil:
				missing++
			case err != nil:
				t.Fatalf("GET %s: %v", dataKey(prefix, i), err)
			case v != strconv.Itoa(i):
				wrong++
			}
		}
	}
	if missing > 0 || wrong > 0 {
		t.Fatalf("keyspace %q diverged: %d present ok, %d missing, %d wrong (of %d)",
			prefix, n-missing-wrong, missing, wrong, n)
	}
}

// waitConverged polls c until prefix:(n-1) — the LAST write, and since the
// replication stream is ordered its arrival implies every earlier write arrived
// — then asserts the whole range is present and correct.
func waitConverged(t *testing.T, c *redis.Client, prefix string, n int, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	last := dataKey(prefix, n-1)
	want := strconv.Itoa(n - 1)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, err := c.Get(ctx, last).Result(); err == nil && v == want {
			assertAllPresent(t, c, prefix, n)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replica never converged on %q within %s", prefix, timeout)
}

// waitReplicaLive proves a replica has finished its handshake and is registered
// on the primary's write stream: it re-sends a sentinel until the replica echoes
// it back, exactly like the integration suite's syncReplica. Doing this BEFORE a
// bulk write guarantees the bulk isn't started while the replica is still
// unregistered (which — with no snapshot bootstrap — would lose those writes).
func waitReplicaLive(t *testing.T, primary, replica *serverProc, sentinel string) {
	t.Helper()
	pc := primary.client()
	defer pc.Close()
	rc := replica.client()
	defer rc.Close()
	ctx := context.Background()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if err := pc.Set(ctx, sentinel, "live", 0).Err(); err != nil {
			t.Fatalf("primary SET %s: %v", sentinel, err)
		}
		if v, err := rc.Get(ctx, sentinel).Result(); err == nil && v == "live" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replica never became live for sentinel %q", sentinel)
}

// randSuffix is a short hex tag for uniquely naming namespaces / interfaces so a
// leftover from a crashed run can't collide.
func randSuffix() string { return strconv.FormatInt(rand.Int63(), 16)[:6] }
