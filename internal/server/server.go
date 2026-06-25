// Package server ties the network layer to the command layer: it accepts TCP
// connections and runs the per-connection request/response loop, dispatching
// each decoded command against a single shared database.
package server

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
)

// Server owns the TCP listener and the one shared database used by every
// connection. There is exactly one DB instance for the whole process; all
// concurrency safety lives inside db.DB (its RWMutex), so the Server itself
// holds no locks and stays oblivious to keys.
type Server struct {
	ln net.Listener
	db *db.DB
}

// New builds a Server around an already-open listener and a fresh, empty
// database. The caller owns creating the listener (so it controls the address,
// and so listen errors surface in main before we start serving).
func New(ln net.Listener) *Server {
	return &Server{
		ln: ln,
		db: db.New(),
	}
}

// Serve runs the accept loop until ctx is cancelled (or the listener is closed
// for any other reason), then waits for in-flight connections to drain before
// returning.
//
// Each accepted connection is handled in its own goroutine, so slow or idle
// clients never block others. The shared db.DB makes that safe: concurrent
// handlers serialise only through its lock.
func (s *Server) Serve(ctx context.Context) error {
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
