package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	port := flag.String("port", "6380", "TCP port to listen on")
	flag.Parse()

	addr := ":" + *port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}
	log.Printf("listening on %s", addr)

	// ctx is cancelled on SIGINT/SIGTERM. Closing the listener unblocks
	// Accept, which lets the accept loop return.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Println("shutting down, closing listener")
		ln.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			// A closed listener (from shutdown) surfaces here as ErrClosed.
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("accept error: %v", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle(conn)
		}()
	}

	// Wait for in-flight connections to finish before exiting.
	wg.Wait()
	log.Println("bye")
}

// handle reads bytes from the connection and prints them until the peer
// closes the connection. This is a placeholder until the RESP protocol and
// command dispatch are wired in.
func handle(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr()
	log.Printf("connection opened: %s", remote)
	defer log.Printf("connection closed: %s", remote)

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			log.Printf("recv from %s: %q", remote, buf[:n])
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("read error from %s: %v", remote, err)
			}
			return
		}
	}
}
