package persistence_test

import (
	"strconv"
	"testing"

	"github.com/ThatDeparted2061/mini-redis-go/internal/cmd"
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/persistence"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
	"path/filepath"
)

// BenchmarkReplay measures startup-recovery throughput: how fast the server can
// rebuild state from the log on boot. It mirrors the real recovery path exactly
// — Replay decodes each frame and re-runs it through cmd.Dispatch against a
// fresh, empty store — so the cmd/s it reports is the number quoted in the
// README, not just decode speed in isolation.
//
// Run with: go test ./internal/persistence/ -run=^$ -bench=BenchmarkReplay
func BenchmarkReplay(b *testing.B) {
	const cmds = 1_000_000

	// Build a log of `cmds` distinct SETs once, up front, so the timed loop
	// recovers a realistically-sized keyspace (one map entry per key).
	path := filepath.Join(b.TempDir(), persistence.DefaultFilename)
	aof, err := persistence.Open(path, persistence.FsyncNo)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	for i := 0; i < cmds; i++ {
		if err := aof.Append(command("SET", "key:"+strconv.Itoa(i), "v")); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
	if err := aof.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		database := db.New()
		n, err := persistence.Replay(path, func(c protocol.Value) error {
			cmd.Dispatch(database, c)
			return nil
		})
		if err != nil || n != cmds {
			b.Fatalf("replay = %d, %v; want %d, nil", n, err, cmds)
		}
	}
	b.ReportMetric(float64(cmds*b.N)/b.Elapsed().Seconds(), "cmd/s")
}
