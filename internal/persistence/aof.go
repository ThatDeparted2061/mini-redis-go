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
// Durability today is "survive a process crash" (e.g. kill -9): every Append
// flushes the buffered bytes out to the operating system, so a killed process
// leaves a complete log behind. Surviving a power loss (an explicit fsync to the
// physical disk) and batching flushes for throughput are the next step and are
// deliberately not here yet.
package persistence

import (
	"bufio"
	"os"
	"sync"

	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// DefaultFilename is the conventional name for the log, matching Redis's own
// appendonly.aof so the file is recognisable.
const DefaultFilename = "appendonly.aof"

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
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// Open opens the AOF at path for appending, creating it if it does not exist.
//
// O_APPEND means every write lands at the current end of file regardless of any
// other writer, so reopening an existing log continues it rather than truncating
// it — exactly what we want across restarts: the prior history is preserved and
// new commands accumulate after it. The caller is expected to Replay the file
// first (to rebuild state) and only then Open it for appending.
func Open(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &AOF{f: f, w: bufio.NewWriter(f)}, nil
}

// Append writes cmd — a RESP array such as ["SET","k","v"] — to the log and
// flushes it out to the operating system before returning.
//
// Flushing on every call is the simplest durability policy: once Append returns,
// the command is in the OS page cache, so even an immediate kill -9 of THIS
// process cannot lose it (the kernel still owns the bytes and writes them back to
// disk). It does not yet protect against the whole machine losing power between
// the write and the kernel's writeback — that needs an fsync, which, along with a
// less aggressive flush cadence for throughput, is a later refinement.
func (a *AOF) Append(cmd protocol.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := protocol.Encode(a.w, cmd); err != nil {
		return err
	}
	return a.w.Flush()
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

// Close flushes any remaining buffered bytes and closes the underlying file. It
// reports the flush error in preference to the close error, since a failed flush
// means data was lost, whereas a close error on an already-flushed file is
// benign.
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	flushErr := a.w.Flush()
	closeErr := a.f.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
