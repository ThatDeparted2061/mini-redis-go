package server

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// connState is the per-connection state the request loop and the pub/sub
// delivery goroutine share. Its key job is to SERIALISE writes to the socket:
// once a connection subscribes, two goroutines want to write to it — the request
// loop (replies to PING/SUBSCRIBE/...) and the delivery goroutine (pushed
// messages) — and their RESP frames must never interleave. writeMu guarantees
// one whole frame at a time.
//
// sub and channels are touched ONLY by the request-loop goroutine (subscribe,
// unsubscribe, teardown), never by the delivery goroutine, so they need no lock
// of their own. The delivery goroutine only ever reads from the mailbox and
// calls write.
type connState struct {
	writer  *bufio.Writer
	writeMu sync.Mutex
	remote  net.Addr

	// sub is this connection's mailbox into the pub/sub bus, created lazily on the
	// first SUBSCRIBE (nil before then). channels is the set of channels it is
	// currently subscribed to; while non-empty the connection is in "subscribe
	// mode" and may only run a restricted command set (see serve).
	sub      *db.Subscriber
	channels map[string]struct{}

	// isReplica records that this connection issued REPLICAOF and is now a replica
	// feed; replicaID is its handle in the server's replica registry, used to
	// unregister it on disconnect. Both are touched only by the request loop.
	isReplica bool
	replicaID uint64
}

// subscribed reports whether the connection is in subscribe mode.
func (cs *connState) subscribed() bool { return len(cs.channels) > 0 }

// write encodes v and flushes it to the client as one atomic frame, holding
// writeMu so a reply and a pushed message can't tear into each other. Both the
// request loop and the delivery goroutine go through here.
func (cs *connState) write(v protocol.Value) error {
	cs.writeMu.Lock()
	defer cs.writeMu.Unlock()

	if err := protocol.Encode(cs.writer, v); err != nil {
		log.Printf("write error to %s: %v", cs.remote, err)
		return err
	}
	if err := cs.writer.Flush(); err != nil {
		log.Printf("flush error to %s: %v", cs.remote, err)
		return err
	}
	return nil
}

// handle runs the request/response loop for a single connection. Each iteration:
//
//	decode one RESP frame  ->  serve runs it (normal dispatch, or pub/sub) -> reply
//
// The loop continues until the client closes cleanly (io.EOF at a frame
// boundary), sends QUIT, or an unrecoverable IO/protocol error occurs. On return
// the connection is closed and any pub/sub subscriptions are torn down.
func (s *Server) handle(conn net.Conn) {
	defer conn.Close()

	remote := conn.RemoteAddr()
	log.Printf("connection opened: %s", remote)
	defer log.Printf("connection closed: %s", remote)

	// Buffered IO: the reader lets the decoder pull a frame at a time without a
	// syscall per byte; the writer coalesces Encode's several small writes per
	// reply. write() Flushes so bytes actually reach the client.
	cs := &connState{writer: bufio.NewWriter(conn), remote: remote}
	defer s.unsubscribeAll(cs)
	defer s.removeReplica(cs)

	reader := bufio.NewReader(conn)
	for {
		// 1. DECODE one command frame from the client.
		request, err := protocol.Decode(reader)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				// Clean close: the client hung up at a frame boundary.
				return
			case errors.Is(err, io.ErrUnexpectedEOF):
				// The client vanished mid-frame; no one left to reply to.
				return
			default:
				// A genuine protocol error (garbage on the wire). Tell the client,
				// then close: after a framing error the byte stream is out of sync
				// and we can't reliably find the next frame.
				_ = cs.write(protocol.Value{Type: protocol.TypeError, Str: "ERR " + err.Error()})
				log.Printf("protocol error from %s: %v", remote, err)
				return
			}
		}

		// 2. SERVE the command and write its reply. serve returns false when the
		//    connection should close (QUIT, or a write failed).
		if !s.serve(cs, request) {
			return
		}
	}
}
