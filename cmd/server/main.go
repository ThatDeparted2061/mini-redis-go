package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os/signal"
	"syscall"

	"github.com/ThatDeparted2061/mini-redis-go/internal/persistence"
	"github.com/ThatDeparted2061/mini-redis-go/internal/server"
)

func main() {
	port := flag.String("port", "6380", "TCP port to listen on")
	appendOnly := flag.Bool("appendonly", true, "persist writes to an append-only file and recover them on restart")
	aofPath := flag.String("aof-path", persistence.DefaultFilename, "path to the append-only file")
	appendFsync := flag.String("appendfsync", "everysec", "AOF fsync policy: always | everysec | no")
	flag.Parse()

	fsyncMode, err := persistence.ParseFsyncMode(*appendFsync)
	if err != nil {
		log.Fatal(err)
	}

	// Open the listener here (not inside the server) so a bad address or a
	// port-in-use error is reported before we claim to be "listening".
	addr := ":" + *port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}
	log.Printf("listening on %s", addr)

	// ctx is cancelled on SIGINT/SIGTERM. server.Serve closes the listener on
	// cancellation, which unblocks Accept and lets the accept loop drain and
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

	srv := server.New(ln, opts...)
	if err := srv.Serve(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
	log.Println("bye")
}
