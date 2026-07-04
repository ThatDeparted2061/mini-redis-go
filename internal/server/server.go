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
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ThatDeparted2061/mini-redis-go/internal/cmd"
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/metrics"
	"github.com/ThatDeparted2061/mini-redis-go/internal/persistence"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
	"github.com/ThatDeparted2061/mini-redis-go/internal/replication"
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
	// See apply for why that invariant matters. The AOF compactor also holds it
	// for the duration of a rewrite, so no write slips into the gap between
	// snapshotting the keyspace and swapping in the compacted log — see
	// rewriteAOF. Replica propagation runs under it too, so replicas receive
	// writes in the same order the store applied them.
	writeMu sync.Mutex

	// replicas is the set of connected replicas this server streams its writes to
	// (its role as a PRIMARY). Always non-nil; empty until a replica connects.
	replicas *replication.Replicas

	// primaryAddr, when set (--replicaof), makes this server a REPLICA: Serve
	// launches a goroutine that streams the primary's writes into the local db.
	primaryAddr string

	// metricsAddr, when set (--metrics-addr), makes Serve expose a Prometheus
	// /metrics endpoint on that address. Empty leaves metrics recording off.
	metricsAddr string
}

// autoRewriteInterval is how often the background compactor checks whether the
// append-only log has grown past the rewrite threshold. The check is a single
// stat, so a modest cadence is plenty — the work only happens when the log has
// actually doubled.
const autoRewriteInterval = time.Second

// replicaHeartbeatInterval is how often a primary PINGs its replicas, and
// replicaAckTimeout is how long a replica may go without acking before the primary
// logs a warning. Matches the Day-16 spec (ping every 5s, warn after 30s of
// silence — i.e. six missed heartbeats).
const (
	replicaHeartbeatInterval = 5 * time.Second
	replicaAckTimeout        = 30 * time.Second
)

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

// WithReplicaOf makes this server a REPLICA of the primary at addr ("host:port").
// On Serve it dials the primary, performs the REPLICAOF handshake, and applies
// the primary's live write stream into the local store. An empty addr leaves the
// server as a standalone primary.
func WithReplicaOf(addr string) Option {
	return func(s *Server) { s.primaryAddr = addr }
}

// WithMetrics exposes a Prometheus /metrics endpoint on addr (e.g. ":9091") for
// the server's lifetime. An empty addr disables both the endpoint and the hot-path
// recording.
func WithMetrics(addr string) Option {
	return func(s *Server) { s.metricsAddr = addr }
}

// New builds a Server around an already-open listener and a fresh, empty
// database. The caller owns creating the listener (so it controls the address,
// and so listen errors surface in main before we start serving). Options enable
// optional features such as persistence (WithAOF).
func New(ln net.Listener, opts ...Option) *Server {
	s := &Server{
		ln:       ln,
		db:       db.New(),
		replicas: replication.NewReplicas(),
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
		defer func() {
			if err := s.aof.Close(); err != nil {
				log.Printf("aof: close on shutdown: %v", err)
			}
		}()
	}

	// When the context is cancelled (e.g. on SIGINT) close the listener. That
	// makes the blocking Accept below return immediately with ErrClosed, which
	// is how we break out of the loop cleanly instead of polling.
	go func() {
		<-ctx.Done()
		log.Println("shutting down, closing listener")
		_ = s.ln.Close()
	}()

	// Prometheus /metrics endpoint, when --metrics-addr is set. Init wires the
	// live-state gauges (keys, replication lag) to this server and flips recording
	// on; the HTTP server is shut down when ctx is cancelled. metricsDone stays
	// closed when metrics are off, so the drain below is a no-op there.
	metricsDone := make(chan struct{})
	if s.metricsAddr != "" {
		metrics.Init(
			func() float64 { return float64(s.db.KeyCount()) },
			s.replicas.LagSeconds,
		)
		mx := &http.Server{Addr: s.metricsAddr, Handler: metrics.Handler()}
		go func() {
			if err := mx.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("metrics server: %v", err)
			}
		}()
		go func() {
			defer close(metricsDone)
			<-ctx.Done()
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mx.Shutdown(shutCtx)
		}()
		log.Printf("metrics: serving /metrics on %s", s.metricsAddr)
	} else {
		close(metricsDone)
	}

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

	// When persistence is on, the append-only log grows with every write; this
	// compactor periodically rewrites it down to a snapshot of the live keyspace
	// once it has grown enough (see runAutoRewrite). Like the reaper it runs for
	// the server's lifetime and is waited out on shutdown. rewriteDone stays
	// closed when persistence is off, so the drain below is a no-op there.
	rewriteDone := make(chan struct{})
	if s.aof != nil {
		go func() {
			defer close(rewriteDone)
			s.runAutoRewrite(ctx)
		}()
	} else {
		close(rewriteDone)
	}

	// When --replicaof is set this server is a replica: stream the primary's live
	// writes into the local db for the server's lifetime. Like the goroutines
	// above it stops on ctx cancel and is waited out on shutdown. Streamed writes
	// go straight through Dispatch (not apply) — a replica mirrors, it does not
	// re-log or chain-propagate in v1.
	replicaDone := make(chan struct{})
	if s.primaryAddr != "" {
		go func() {
			defer close(replicaDone)
			replication.RunReplica(ctx, s.primaryAddr, func(c protocol.Value) {
				cmd.Dispatch(s.db, c)
			})
		}()
	} else {
		close(replicaDone)
	}

	// A primary probes its replicas with a PING every few seconds and warns about
	// any that stop acking — a liveness check on the replication stream. Runs for
	// the server's lifetime like the goroutines above; a no-op on a server that has
	// no replicas (an idle ticker), so it is safe to always run.
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		s.runReplicaHeartbeat(ctx)
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
	<-rewriteDone
	<-replicaDone
	<-heartbeatDone
	<-metricsDone
	return nil
}

// runReplicaHeartbeat pings connected replicas on a fixed interval and warns about
// any that have gone silent, until ctx is cancelled. See Replicas.Heartbeat.
func (s *Server) runReplicaHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(replicaHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.replicas.Heartbeat(replicaAckTimeout)
		}
	}
}

// runAutoRewrite periodically compacts the append-only log, until ctx is
// cancelled. Each tick it asks the log whether it has grown past the rewrite
// threshold and, if so, rewrites it. Errors are logged and shrugged off: a
// failed rewrite leaves the existing log intact, so the only cost is that
// compaction didn't happen this time.
func (s *Server) runAutoRewrite(ctx context.Context) {
	ticker := time.NewTicker(autoRewriteInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.aof.ShouldRewrite() {
				continue
			}
			if err := s.rewriteAOF(); err != nil {
				log.Printf("aof rewrite failed: %v", err)
			}
		}
	}
}

// rewriteAOF compacts the append-only log to a snapshot of the current keyspace.
//
// It holds writeMu for the whole operation, which is the simple, correct v1
// design: with the write path frozen, the snapshot can't miss a write that lands
// mid-rewrite, and no write can append to the old log after we've swapped in the
// new one. The cost is a latency hit — every client write blocks until the
// rewrite finishes (a keyspace copy plus a file write and fsync). That is the
// documented trade-off for v1.
//
// ponytail: global write freeze for the whole rewrite. Real Redis forks a child
// and buffers concurrent writes into the new log so the parent never pauses;
// that copy-on-write design is the upgrade path when the pause starts to hurt.
func (s *Server) rewriteAOF() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	before, _ := s.aof.Size()
	records := s.db.Snapshot()
	if err := s.aof.Rewrite(s.aofPath, records); err != nil {
		return err
	}
	after, _ := s.aof.Size()
	log.Printf("aof: rewrote log to %d key(s), %d -> %d bytes", len(records), before, after)
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

// apply runs request against the shared database and, when the command both
// writes and succeeds, records its effect downstream: appends it to the AOF (when
// persistence is on) and streams it to every connected replica (when there are
// any).
//
// The write path holds writeMu across the dispatch AND both downstream effects.
// That is the crux of correctness under concurrency: handlers run in many
// goroutines, and without this lock two writers could mutate the database in one
// order yet reach the log (or a replica) in the other — a replay or a replica
// would then rebuild a DIFFERENT state than the one that was live. Holding writeMu
// makes "apply then record then propagate" atomic per write, so the log's and the
// replicas' order always matches the database's. Reads never take writeMu, so they
// still run fully in parallel under the database's own read lock.
//
// Only non-error replies are recorded: a command that failed (wrong arguments,
// WRONGTYPE, ...) changed nothing, so there is nothing to persist or propagate.
func (s *Server) apply(request protocol.Value) protocol.Value {
	name := commandName(request)
	start := time.Now()
	reply := s.applyRecord(request, name)

	// Record the command for metrics. The label is the upper-cased name only for
	// registered commands; anything else collapses to "unknown" so a client
	// spraying garbage names cannot explode the metric's label cardinality.
	label := "unknown"
	if cmd.Known(name) {
		label = strings.ToUpper(name)
	}
	result := "ok"
	if reply.Type == protocol.TypeError {
		result = "error"
	}
	metrics.ObserveCommand(label, result, time.Since(start))
	return reply
}

// applyRecord runs the command against the store and, for successful writes,
// records it downstream (AOF + replicas). See apply for the metrics wrapper.
func (s *Server) applyRecord(request protocol.Value, name string) protocol.Value {
	// Fast lock-free path: read-only commands, and writes when there is neither an
	// AOF to append to nor a replica to stream to, need no ordering guarantee.
	if !cmd.IsWrite(name) || (s.aof == nil && !s.replicas.Any()) {
		return cmd.Dispatch(s.db, request)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	reply := cmd.Dispatch(s.db, request)
	if reply.Type != protocol.TypeError {
		if s.aof != nil {
			if err := s.aof.Append(request); err != nil {
				// The mutation already happened in memory; we just failed to make it
				// durable. Surface it loudly — a later crash would lose this write —
				// but still return the reply the client earned.
				log.Printf("aof append failed for %q: %v", commandName(request), err)
			}
		}
		// Mirror the same successful write to every connected replica, under the
		// same lock, so replicas receive writes in the store's order.
		s.replicas.Propagate(request)
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
