package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ThatDeparted2061/mini-redis-go/internal/persistence"
	"github.com/ThatDeparted2061/mini-redis-go/internal/server"
)

func main() {
	port := flag.String("port", "6380", `TCP port to listen on ("0" asks the kernel for a free port)`)
	bind := flag.String("bind", "", `interface to bind (empty = all interfaces; use "127.0.0.1" to accept only local/tunneled connections)`)
	noTCP := flag.Bool("no-tcp", false, "do not bind TCP (requires --unixsocket)")
	unixSocket := flag.String("unixsocket", "", "Unix domain socket path (empty = disabled; combine with TCP for dual bind)")
	appendOnly := flag.Bool("appendonly", true, "persist writes to an append-only file and recover them on restart")
	aofPath := flag.String("aof-path", persistence.DefaultFilename, "path to the append-only file")
	appendFsync := flag.String("appendfsync", "everysec", "AOF fsync policy: always | everysec | no")
	replicaOf := flag.String("replicaof", "", `replicate from a primary, given as "host port" (e.g. --replicaof "127.0.0.1 6380")`)
	metricsAddr := flag.String("metrics-addr", ":9091", `address for the Prometheus /metrics endpoint (empty to disable)`)
	flag.Parse()

	fsyncMode, err := persistence.ParseFsyncMode(*appendFsync)
	if err != nil {
		log.Fatal(err)
	}

	// --replicaof "host port" makes this server a replica of that primary. Redis
	// takes the address as two space-separated fields, so we accept the same shape
	// and join them into a dial target.
	var primaryAddr string
	if *replicaOf != "" {
		fields := strings.Fields(*replicaOf)
		if len(fields) != 2 {
			log.Fatalf(`--replicaof must be "host port", got %q`, *replicaOf)
		}
		primaryAddr = net.JoinHostPort(fields[0], fields[1])
	}

	// Open listeners here (not inside the server) so a bad address or a
	// port-in-use error is reported before we claim to be "listening".
	// Bind modes: TCP alone (default --port 6380), UDS alone (--no-tcp --unixsocket),
	// or both (--unixsocket without --no-tcp). Same RESP protocol on every listener.
	lns, cleanup, err := openListeners(*bind, *port, *noTCP, *unixSocket)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	// ctx is cancelled on SIGINT/SIGTERM. server.Serve closes the listeners on
	// cancellation, which unblocks Accept and lets the accept loops drain and
	// return — giving us a graceful shutdown instead of an abrupt exit.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Persistence is opt-out: by default writes are logged to the append-only
	// file and recovered on the next start. WithAOF("") (i.e. --appendonly=false)
	// runs a purely in-memory server.
	var opts []server.Option
	if *appendOnly {
		opts = append(opts, server.WithAOF(*aofPath), server.WithFsync(fsyncMode))
		log.Printf("append-only persistence on: %s (appendfsync=%s)", *aofPath, fsyncMode)
	}
	if primaryAddr != "" {
		opts = append(opts, server.WithReplicaOf(primaryAddr))
		log.Printf("replica mode: streaming live writes from primary %s", primaryAddr)
	}
	if *metricsAddr != "" {
		opts = append(opts, server.WithMetrics(*metricsAddr))
		log.Printf("metrics: Prometheus /metrics on %s", *metricsAddr)
	}

	srv := server.NewMulti(lns, opts...)
	if err := srv.Serve(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
	log.Println("bye")
}

// openListeners builds the TCP and/or Unix domain socket listeners requested by
// the flags. At least one is required. cleanup removes a UDS path file after
// the server exits (stale socket files otherwise block the next bind).
//
// TCP is on by default (--port defaults to 6380). Pass noTCP to skip it
// (typically with a unixSocket). Port "0" is the usual kernel free-port request.
func openListeners(bind, port string, noTCP bool, unixSocket string) (lns []net.Listener, cleanup func(), err error) {
	cleanup = func() {}

	if !noTCP {
		addr := net.JoinHostPort(bind, port)
		ln, listenErr := net.Listen("tcp", addr)
		if listenErr != nil {
			return nil, cleanup, fmt.Errorf("listen tcp %s: %w", addr, listenErr)
		}
		log.Printf("listening on tcp %s", ln.Addr())
		lns = append(lns, ln)
	}

	if unixSocket != "" {
		// A leftover socket file from a previous crash makes Listen("unix") fail
		// with "address already in use". Remove it first when it is a socket
		// (not a regular file a user might care about).
		if removeErr := removeStaleUnixSocket(unixSocket); removeErr != nil {
			closeAll(lns)
			return nil, cleanup, removeErr
		}
		ln, listenErr := net.Listen("unix", unixSocket)
		if listenErr != nil {
			closeAll(lns)
			return nil, cleanup, fmt.Errorf("listen unix %s: %w", unixSocket, listenErr)
		}
		log.Printf("listening on unix %s", unixSocket)
		lns = append(lns, ln)
		path := unixSocket
		cleanup = func() {
			// Best-effort: only unlink our socket path after listeners are closed.
			_ = os.Remove(path)
		}
	}

	if len(lns) == 0 {
		return nil, cleanup, fmt.Errorf("nothing to listen on: omit --no-tcp and/or set --unixsocket")
	}
	return lns, cleanup, nil
}

// removeStaleUnixSocket deletes path if it is a Unix socket left behind by a
// previous run. Non-socket files are left alone so we never clobber a real file.
func removeStaleUnixSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	// On Unix, socket files report ModeSocket. If something else occupies the
	// path, fail loudly rather than deleting user data.
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("unix socket path exists and is not a socket: %s", path)
	}
	return os.Remove(path)
}

func closeAll(lns []net.Listener) {
	for _, ln := range lns {
		_ = ln.Close()
	}
}
