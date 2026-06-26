package integration

// These tests exercise append-only persistence end to end: write through a real
// go-redis client, take the server down, bring a NEW server up against the same
// log file, and check the data is still there. The second server is the
// "restart"; its only source of truth is the log the first one left behind, so a
// value that survives proves the write was both recorded and correctly replayed.

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ThatDeparted2061/mini-redis-go/internal/server"
)

// startAOFServer brings up a server that persists to aofPath and returns a
// connected client plus a stop function. Unlike startServer it takes an explicit
// log path and hands back stop instead of registering cleanup, so a test can
// stop one instance and start another against the SAME file — the restart whose
// job is to replay the log.
//
// The listener is opened before the server starts serving, but the server does
// not Accept (and so does not process any command) until it has finished
// replaying and reopening the log. Client commands therefore queue in the socket
// until recovery is complete, which is what keeps these tests free of races
// between "client sends GET" and "server finished replaying".
func startAOFServer(t *testing.T, aofPath string) (*redis.Client, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	srvErr := make(chan error, 1)
	srv := server.New(ln, server.WithAOF(aofPath))
	go func() { srvErr <- srv.Serve(ctx) }()

	client := redis.NewClient(&redis.Options{
		Addr:            ln.Addr().String(),
		Protocol:        2,
		DisableIdentity: true,
	})

	stop := func() {
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
	}
	return client, stop
}

// TestAOFPersistsWritesAcrossRestart is the headline acceptance check: writes of
// every value type survive the server being taken down and brought back up, and a
// key deleted before the restart stays deleted afterwards (DEL is replayed too).
func TestAOFPersistsWritesAcrossRestart(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	ctx := opCtx(t)

	// --- First run: write a mix of types, then take the server down. ---
	c1, stop1 := startAOFServer(t, aofPath)

	if err := c1.Set(ctx, "str", "hello", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := c1.RPush(ctx, "list", "a", "b", "c").Err(); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	if err := c1.HSet(ctx, "hash", "f1", "v1", "f2", "v2").Err(); err != nil {
		t.Fatalf("HSET: %v", err)
	}
	if err := c1.SAdd(ctx, "set", "m1", "m2").Err(); err != nil {
		t.Fatalf("SADD: %v", err)
	}
	// A key written and then deleted: the log holds both the SET and the DEL, so
	// after replay it must be ABSENT, not resurrected.
	if err := c1.Set(ctx, "doomed", "x", 0).Err(); err != nil {
		t.Fatalf("SET doomed: %v", err)
	}
	if err := c1.Del(ctx, "doomed").Err(); err != nil {
		t.Fatalf("DEL doomed: %v", err)
	}

	stop1() // The server is gone; only its log remains.

	// --- Restart: a fresh server recovers purely by replaying the log. ---
	c2, stop2 := startAOFServer(t, aofPath)
	defer stop2()

	if got, err := c2.Get(ctx, "str").Result(); err != nil || got != "hello" {
		t.Fatalf("after restart GET str = %q, %v; want \"hello\", nil", got, err)
	}
	if got, err := c2.LRange(ctx, "list", 0, -1).Result(); err != nil ||
		!reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("after restart LRANGE list = %v, %v; want [a b c], nil", got, err)
	}
	if got, err := c2.HGetAll(ctx, "hash").Result(); err != nil ||
		!reflect.DeepEqual(got, map[string]string{"f1": "v1", "f2": "v2"}) {
		t.Fatalf("after restart HGETALL hash = %v, %v; want {f1:v1 f2:v2}, nil", got, err)
	}
	if got, err := c2.SMembers(ctx, "set").Result(); err != nil || !sameSet(got, []string{"m1", "m2"}) {
		t.Fatalf("after restart SMEMBERS set = %v, %v; want {m1 m2}, nil", got, err)
	}
	if _, err := c2.Get(ctx, "doomed").Result(); err != redis.Nil {
		t.Fatalf("after restart GET doomed err = %v; want redis.Nil (DEL must replay)", err)
	}
}

// TestAOFDoesNotPersistFailedWrite checks that a write rejected with an error
// (here a WRONGTYPE) is never logged: the command changed nothing, so replaying
// it on restart must not resurrect or corrupt anything. After the restart the key
// is still the original string.
func TestAOFDoesNotPersistFailedWrite(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	ctx := opCtx(t)

	c1, stop1 := startAOFServer(t, aofPath)
	if err := c1.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	// LPUSH against a string key is a WRONGTYPE error — a failed write.
	if err := c1.LPush(ctx, "k", "x").Err(); err == nil {
		t.Fatalf("LPUSH on a string key should have errored")
	}
	stop1()

	c2, stop2 := startAOFServer(t, aofPath)
	defer stop2()

	if got, err := c2.Get(ctx, "k").Result(); err != nil || got != "v" {
		t.Fatalf("after restart GET k = %q, %v; want \"v\", nil", got, err)
	}
}

// TestAOFConcurrentWritesReplayInAppliedOrder guards the ordering invariant: with
// many clients pushing to one list at once, the order the log records the pushes
// in must match the order the database actually applied them. RPUSH makes that
// observable — append order is position — so the list rebuilt by replay equals the
// list that was live only if every write was logged in apply order. If the
// apply-then-append step were not serialised, the two orders could drift and this
// test would catch it.
func TestAOFConcurrentWritesReplayInAppliedOrder(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	ctx := context.Background()

	c1, stop1 := startAOFServer(t, aofPath)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// go-redis pools connections, so these land on many sockets at once
			// and race in the server. Unique values make the order observable.
			if err := c1.RPush(ctx, "log", fmt.Sprintf("v%03d", i)).Err(); err != nil {
				t.Errorf("RPUSH: %v", err)
			}
		}(i)
	}
	wg.Wait()

	live, err := c1.LRange(ctx, "log", 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE live: %v", err)
	}
	stop1()

	c2, stop2 := startAOFServer(t, aofPath)
	defer stop2()

	replayed, err := c2.LRange(ctx, "log", 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE replayed: %v", err)
	}
	if len(replayed) != n {
		t.Fatalf("replayed %d elements; want %d", len(replayed), n)
	}
	if !reflect.DeepEqual(live, replayed) {
		t.Fatalf("replayed order diverged from applied order:\n live = %v\n got  = %v", live, replayed)
	}
}

// sameSet reports whether a and b hold the same elements, ignoring order — set
// replies have no defined order.
func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	return reflect.DeepEqual(as, bs)
}

// TestAOFRewriteCompactsAndSurvivesRestart exercises compaction end to end: bloat
// the log past the rewrite threshold, let the background compactor rewrite it
// down to a snapshot, and confirm the live state is untouched AND that a server
// restarted from the COMPACTED log alone recovers every key. That last part is
// what proves the snapshot commands (SET/RPUSH/HSET/SADD/EXPIRE) faithfully
// reproduce the keyspace.
func TestAOFRewriteCompactsAndSurvivesRestart(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	ctx := opCtx(t)

	c1, stop1 := startAOFServer(t, aofPath)

	// Bloat the log: set the same key thousands of times so the file blows well
	// past the 64 KiB rewrite floor, even though only the final value matters.
	const n = 4000
	if _, err := c1.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < n; i++ {
			pipe.Set(ctx, "counter", i, 0)
		}
		return nil
	}); err != nil {
		t.Fatalf("pipelined SET: %v", err)
	}

	// A spread of other types and a TTL key, so the snapshot has to recreate each.
	if err := c1.RPush(ctx, "list", "a", "b", "c").Err(); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	if err := c1.HSet(ctx, "hash", "f1", "v1", "f2", "v2").Err(); err != nil {
		t.Fatalf("HSET: %v", err)
	}
	if err := c1.SAdd(ctx, "set", "m1", "m2").Err(); err != nil {
		t.Fatalf("SADD: %v", err)
	}
	if err := c1.Set(ctx, "ttl", "live", 0).Err(); err != nil {
		t.Fatalf("SET ttl: %v", err)
	}
	if err := c1.Expire(ctx, "ttl", time.Hour).Err(); err != nil {
		t.Fatalf("EXPIRE ttl: %v", err)
	}

	bloated := fileSize(t, aofPath)
	if bloated < 64*1024 {
		t.Fatalf("log is only %d bytes; expected > 64 KiB to arm the rewrite", bloated)
	}

	// The background compactor checks once a second; wait for it to shrink the log.
	var compacted int64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		compacted = fileSize(t, aofPath)
		if compacted < bloated/2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if compacted >= bloated/2 {
		t.Fatalf("log was not compacted: still %d bytes (was %d)", compacted, bloated)
	}
	if _, err := os.Stat(aofPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("rewrite left a .tmp file behind: %v", err)
	}

	// The rewrite must not disturb the live keyspace.
	if got, err := c1.Get(ctx, "counter").Result(); err != nil || got != fmt.Sprint(n-1) {
		t.Fatalf("after rewrite GET counter = %q, %v; want %q", got, err, fmt.Sprint(n-1))
	}
	stop1()

	// Restart from the compacted log alone: every key must replay.
	c2, stop2 := startAOFServer(t, aofPath)
	defer stop2()

	if got, err := c2.Get(ctx, "counter").Result(); err != nil || got != fmt.Sprint(n-1) {
		t.Fatalf("after restart GET counter = %q, %v; want %q", got, err, fmt.Sprint(n-1))
	}
	if got, err := c2.LRange(ctx, "list", 0, -1).Result(); err != nil || !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("after restart LRANGE list = %v, %v; want [a b c], nil", got, err)
	}
	if got, err := c2.HGetAll(ctx, "hash").Result(); err != nil ||
		!reflect.DeepEqual(got, map[string]string{"f1": "v1", "f2": "v2"}) {
		t.Fatalf("after restart HGETALL hash = %v, %v; want {f1:v1 f2:v2}, nil", got, err)
	}
	if got, err := c2.SMembers(ctx, "set").Result(); err != nil || !sameSet(got, []string{"m1", "m2"}) {
		t.Fatalf("after restart SMEMBERS set = %v, %v; want {m1 m2}, nil", got, err)
	}
	if d, err := c2.TTL(ctx, "ttl").Result(); err != nil || d <= 0 || d > time.Hour {
		t.Fatalf("after restart TTL ttl = %v, %v; want a positive TTL <= 1h", d, err)
	}
}

// fileSize returns the size of the file at path, failing the test if it can't be
// stat'd.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}
