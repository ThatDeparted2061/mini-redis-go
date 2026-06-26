// Package server ties the network layer to the command layer: it accepts TCP
// connections and runs the per-connection request/response loop, dispatching
// each decoded command against a single shared database.
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ThatDeparted2061/mini-redis-go/internal/cmd"
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/persistence"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Server owns the TCP listener and the one shared database used by every
// connection. There is exactly one DB instance for the whole process; all
// concurrency safety lives inside db.DB (its RWMutex), so the Server itself
// holds no locks of its own for the keyspace.
//
// When persistence is enabled (aofPath set) the Server also owns the append-only
// log: it replays the log on startup to rebuild state, appends every successful
// write to it while serving, and closes it on shutdown. writeMu serialises that
// append against the database mutation it records — see apply.
type Server struct {
	ln net.Listener
	db *db.DB

	// aofPath is the append-only log's path, or "" to run without persistence.
	// aof is the open log, set during Serve once aofPath has been replayed.
	// fsyncMode is the log's durability policy; its zero value is FsyncEverySec,
	// so a server with no WithFsync option gets the sensible default.
	aofPath   string
	fsyncMode persistence.FsyncMode
	aof       *persistence.AOF

	// writeMu serialises the apply-then-append of write commands so the order
	// commands are recorded in the AOF matches the order the db applied them.
	// See apply for why that invariant matters.
	writeMu sync.Mutex
}

// Option configures a Server at construction time. Options keep New backward
// compatible: callers that want the default (no persistence) pass none.
type Option func(*Server)

// WithAOF enables append-only persistence, using the log file at path. On Serve
// the server replays this file to recover prior state, then appends new writes to
// it. An empty path is treated as "persistence disabled".
func WithAOF(path string) Option {
	return func(s *Server) { s.aofPath = path }
}

// WithFsync sets the AOF's fsync policy (durability vs. throughput). It only
// matters alongside WithAOF; with no AOF there is nothing to fsync.
func WithFsync(mode persistence.FsyncMode) Option {
	return func(s *Server) { s.fsyncMode = mode }
}

// New builds a Server around an already-open listener and a fresh, empty
// database. The caller owns creating the listener (so it controls the address,
// and so listen errors surface in main before we start serving). Options enable
// optional features such as persistence (WithAOF).
func New(ln net.Listener, opts ...Option) *Server {
	s := &Server{
		ln: ln,
		db: db.New(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serve runs the accept loop until ctx is cancelled (or the listener is closed
// for any other reason), then waits for in-flight connections to drain before
// returning.
//
// Each accepted connection is handled in its own goroutine, so slow or idle
// clients never block others. The shared db.DB makes that safe: concurrent
// handlers serialise only through its lock.
func (s *Server) Serve(ctx context.Context) error {
	// Persistence comes first, before a single client is accepted: replay the
	// existing log to rebuild prior state, then open it for appending. Doing this
	// up front means recovery runs against a quiescent database (no connections,
	// no reaper yet) and the very first client write lands in an already-open log.
	if s.aofPath != "" {
		if err := s.loadAOF(); err != nil {
			return err
		}
		defer s.aof.Close()
	}

	// When the context is cancelled (e.g. on SIGINT) close the listener. That
	// makes the blocking Accept below return immediately with ErrClosed, which
	// is how we break out of the loop cleanly instead of polling.
	go func() {
		<-ctx.Done()
		log.Println("shutting down, closing listener")
		s.ln.Close()
	}()

	// Active expiry runs for the server's lifetime, reclaiming keys whose TTL
	// elapsed without being accessed (lazy expiry alone would leak those). It
	// stops when ctx is cancelled; we wait for it on the way out so no background
	// goroutine outlives Serve. This is the spec's `go db.RunActiveExpiry(ctx)`,
	// placed where the db and the lifecycle context actually live.
	expiryDone := make(chan struct{})
	go func() {
		defer close(expiryDone)
		s.db.RunActiveExpiry(ctx)
	}()

	// wg tracks live connection goroutines so we can wait them out on shutdown
	// rather than yanking the process out from under in-flight requests.
	var wg sync.WaitGroup
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			// A listener closed by the shutdown goroutine above surfaces here
			// as net.ErrClosed — the signal to stop accepting and drain.
			if errors.Is(err, net.ErrClosed) {
				break
			}
			// Other accept errors (e.g. transient "too many open files") are
			// not fatal: log and keep accepting.
			log.Printf("accept error: %v", err)
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handle(conn)
		}()
	}

	wg.Wait()
	<-expiryDone
	return nil
}

// loadAOF replays the append-only log at s.aofPath to rebuild state, then opens
// it for appending and stores it in s.aof, ready for the write path.
//
// Replaying re-executes each logged command through the ordinary dispatcher —
// the exact code that first ran it — so recovered state is correct by
// construction rather than by a separately-maintained loader. A command that now
// replies with an error is logged and skipped rather than aborting recovery: one
// unreadable frame should not strand every good write before it.
func (s *Server) loadAOF() error {
	start := time.Now()
	n, err := persistence.Replay(s.aofPath, func(c protocol.Value) error {
		if reply := cmd.Dispatch(s.db, c); reply.Type == protocol.TypeError {
			log.Printf("aof replay: %q replied %q (skipped)", commandName(c), reply.Str)
		}
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		return fmt.Errorf("aof replay %s: %w", s.aofPath, err)
	}
	if n > 0 {
		// Report throughput so a slow recovery is visible and measurable: this is
		// the number quoted in the README.
		log.Printf("aof: recovered %d command(s) from %s in %s (%.0f cmd/s)",
			n, s.aofPath, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
	}

	aof, err := persistence.Open(s.aofPath, s.fsyncMode)
	if err != nil {
		return fmt.Errorf("aof open %s: %w", s.aofPath, err)
	}
	s.aof = aof
	return nil
}

// apply runs request against the shared database and, when persistence is on and
// the command both writes and succeeds, appends it to the log.
//
// The write path holds writeMu across BOTH the dispatch and the append. That is
// the crux of correctness under concurrency: handlers run in many goroutines, and
// without this lock two writers could mutate the database in one order yet reach
// the log in the other — a replay would then rebuild a DIFFERENT state than the
// one that was live. Holding writeMu makes "apply then record" atomic per write,
// so the log's order always matches the database's. Reads never take writeMu, so
// they still run fully in parallel under the database's own read lock.
//
// Only non-error replies are logged: a command that failed (wrong arguments,
// WRONGTYPE, ...) changed nothing, so there is nothing to persist.
func (s *Server) apply(request protocol.Value) protocol.Value {
	if s.aof == nil || !cmd.IsWrite(commandName(request)) {
		return cmd.Dispatch(s.db, request)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	reply := cmd.Dispatch(s.db, request)
	if reply.Type != protocol.TypeError {
		if err := s.aof.Append(request); err != nil {
			// The mutation already happened in memory; we just failed to make it
			// durable. Surface it loudly — a later crash would lose this write —
			// but still return the reply the client earned.
			log.Printf("aof append failed for %q: %v", commandName(request), err)
		}
	}
	return reply
}

// commandName extracts the command name from a decoded request, or "" if the
// request is not a well-formed command array. IsWrite("") is false and Dispatch
// rejects the same malformed shapes, so a "" here simply routes to the normal
// (non-persisted) error path.
func commandName(request protocol.Value) string {
	if request.Type != protocol.TypeArray || len(request.Array) == 0 {
		return ""
	}
	return string(request.Array[0].Bulk)
}
