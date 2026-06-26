// Package persistence makes the in-memory store durable through an append-only
// file (AOF): a log of every write command the server executed, recorded in the
// exact RESP wire format the client sent.
//
// The central idea is to log the COMMAND, not the resulting state. A "SET k v"
// is stored as the bytes "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n" — the same
// frame a client would put on the socket. Recovery then replays the log by
// feeding each frame back through the ordinary command dispatch path (see
// Replay), so a restart re-runs history through the very same code that ran it
// the first time. That is what makes recovery correct "for free": there is no
// second serialization format to keep in sync with the live data structures, and
// no way for the on-disk shape to drift from the in-memory one.
//
// Every Append flushes the buffered bytes out to the operating system, so a
// killed process (kill -9) always leaves a complete log behind — the kernel
// still owns the bytes and writes them back. Surviving a whole-machine POWER
// LOSS is stronger: it needs an fsync, which pushes the kernel's page cache to
// the physical disk. How often we pay that fsync is the FsyncMode policy below —
// the canonical durability/throughput trade-off (see the README).
package persistence

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// DefaultFilename is the conventional name for the log, matching Redis's own
// appendonly.aof so the file is recognisable.
const DefaultFilename = "appendonly.aof"

// FsyncMode decides how often the log is fsync'd to the physical disk — the
// trade-off between how much a crash can lose and how fast writes are.
type FsyncMode int

const (
	// FsyncEverySec fsyncs once a second from a background ticker. A crash loses
	// at most ~1s of writes; the write path itself never blocks on the disk.
	// This is the default (and Redis's), and the zero value so a Server with no
	// fsync option gets it for free.
	FsyncEverySec FsyncMode = iota
	// FsyncAlways fsyncs after every command. A crash loses at most the one
	// in-flight write, at the cost of a disk sync (~1ms on an SSD) per write.
	FsyncAlways
	// FsyncNo never explicitly fsyncs; it relies on the OS to flush its page
	// cache on its own schedule (~30s on Linux). Fastest, least durable.
	FsyncNo
)

func (m FsyncMode) String() string {
	switch m {
	case FsyncAlways:
		return "always"
	case FsyncEverySec:
		return "everysec"
	case FsyncNo:
		return "no"
	default:
		return "unknown"
	}
}

// ParseFsyncMode maps a CLI flag value ("always" | "everysec" | "no") to a
// FsyncMode, erroring on anything else.
func ParseFsyncMode(s string) (FsyncMode, error) {
	switch s {
	case "always":
		return FsyncAlways, nil
	case "everysec":
		return FsyncEverySec, nil
	case "no":
		return FsyncNo, nil
	default:
		return 0, fmt.Errorf("invalid appendfsync %q (want always, everysec, or no)", s)
	}
}

// AOF is a concurrency-safe append-only command log.
//
// Writes go through a bufio.Writer wrapped around the underlying *os.File. The
// buffer matters even though Append flushes every call: a single command encodes
// to roughly a dozen tiny writes (one per length header, payload, and CRLF), and
// the buffer coalesces them into ONE write syscall at flush time instead of a
// dozen. mu serialises Append/Flush/Close because a bufio.Writer is not safe for
// concurrent use and, just as importantly, because the bytes for two commands
// must never interleave in the file.
type AOF struct {
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	mode FsyncMode

	// baseSize is the log's size on disk right after the last rewrite (or, until
	// the first rewrite, its size when Open'd). The rewrite trigger compares the
	// current size against it to decide the log has grown enough to compact — see
	// ShouldRewrite. Guarded by mu.
	baseSize int64

	// stop/done drive the everysec ticker goroutine: Close closes stop to ask it
	// to exit and waits on done. Both are nil in the other modes (no goroutine).
	stop chan struct{}
	done chan struct{}
}

// Auto-rewrite thresholds, mirroring Redis's auto-aof-rewrite-* knobs.
const (
	// rewriteGrowthFactor triggers compaction once the log reaches this multiple
	// of its post-rewrite baseline — 2x, i.e. Redis's default
	// auto-aof-rewrite-percentage of 100 ("the log has doubled").
	rewriteGrowthFactor = 2

	// minRewriteSize is the smallest log worth compacting. Below it the savings
	// don't justify the rewrite, and it stops a tiny log (whose baseline is near
	// zero, so any growth is "2x") from rewriting on every check. Redis defaults
	// to 64MB; this is far smaller because mini-redis keyspaces are.
	minRewriteSize = 64 * 1024 // 64 KiB
)

// Open opens the AOF at path for appending, creating it if it does not exist.
//
// O_APPEND means every write lands at the current end of file regardless of any
// other writer, so reopening an existing log continues it rather than truncating
// it — exactly what we want across restarts: the prior history is preserved and
// new commands accumulate after it. The caller is expected to Replay the file
// first (to rebuild state) and only then Open it for appending.
func Open(path string, mode FsyncMode) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &AOF{f: f, w: bufio.NewWriter(f), mode: mode}
	// Seed the rewrite baseline with the size of the log we're continuing, so a
	// freshly-started server measures growth from what it recovered, not from
	// zero.
	if info, statErr := f.Stat(); statErr == nil {
		a.baseSize = info.Size()
	}
	if mode == FsyncEverySec {
		a.stop = make(chan struct{})
		a.done = make(chan struct{})
		go a.syncEverySec()
	}
	return a, nil
}

// syncEverySec fsyncs the file once a second until Close stops it. It runs only
// in FsyncEverySec mode. The fsync takes a.mu so it never races an Append's
// flush; the cost lands here on a background goroutine, not on the write path.
func (a *AOF) syncEverySec() {
	defer close(a.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.mu.Lock()
			err := a.f.Sync()
			a.mu.Unlock()
			if err != nil {
				log.Printf("aof everysec fsync: %v", err)
			}
		}
	}
}

// Append writes cmd — a RESP array such as ["SET","k","v"] — to the log and
// flushes it out to the operating system before returning.
//
// The flush puts the command in the OS page cache, so even an immediate kill -9
// of THIS process cannot lose it. Surviving a power loss is the FsyncMode's job:
// in FsyncAlways we fsync to disk here, before returning, so nothing acked is
// lost; in everysec/no the fsync is deferred (to the ticker, or to the OS).
func (a *AOF) Append(cmd protocol.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := protocol.Encode(a.w, cmd); err != nil {
		return err
	}
	if err := a.w.Flush(); err != nil {
		return err
	}
	if a.mode == FsyncAlways {
		return a.f.Sync()
	}
	return nil
}

// Flush pushes any buffered bytes out to the operating system. With the current
// flush-on-every-Append policy there is normally nothing buffered, but the method
// exists for the upcoming batched-flush mode and for an explicit drain on
// shutdown.
func (a *AOF) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.w.Flush()
}

// Size reports the log's current size on disk in bytes. Append flushes every
// call, so the stat reflects every command written so far.
func (a *AOF) Size() (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sizeLocked()
}

// sizeLocked is Size without taking the lock, for callers that already hold a.mu.
func (a *AOF) sizeLocked() (int64, error) {
	info, err := a.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// ShouldRewrite reports whether the log has grown enough to warrant compaction:
// it must be at least minRewriteSize on disk AND at least rewriteGrowthFactor
// times its size after the last rewrite. The size floor keeps a small log from
// rewriting constantly (its baseline is near zero, so the growth test alone
// would always pass). A stat error is reported as "no" — a failing rewrite check
// must never be louder than the failing write that would surface the real
// problem.
func (a *AOF) ShouldRewrite() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	size, err := a.sizeLocked()
	if err != nil {
		return false
	}
	return size >= minRewriteSize && size >= rewriteGrowthFactor*a.baseSize
}

// Close stops the everysec ticker (if any), flushes any buffered bytes, fsyncs,
// and closes the underlying file. The fsync runs in every mode because a clean
// shutdown is not a crash: a graceful stop should lose nothing, even in
// everysec/no where the per-command path skips it. Errors are reported
// flush-then-sync-then-close, since an earlier failure means more data lost.
func (a *AOF) Close() error {
	// Stop the ticker before touching the file so its fsync can't race the
	// flush/close below. Done outside the lock: the goroutine takes a.mu itself.
	if a.mode == FsyncEverySec {
		close(a.stop)
		<-a.done
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	flushErr := a.w.Flush()
	syncErr := a.f.Sync()
	closeErr := a.f.Close()
	switch {
	case flushErr != nil:
		return flushErr
	case syncErr != nil:
		return syncErr
	default:
		return closeErr
	}
}
