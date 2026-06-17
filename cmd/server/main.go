package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os/signal"
	"syscall"

	"github.com/ThatDeparted2061/mini-redis-go/internal/server"
)

func main() {
	port := flag.String("port", "6380", "TCP port to listen on")
	flag.Parse()

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

	srv := server.New(ln)
	if err := srv.Serve(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
	log.Println("bye")
}
