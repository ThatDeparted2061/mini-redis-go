package server

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"

	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// handle runs the request/response loop for a single connection. Each iteration:
//
//	decode one RESP frame  ->  cmd.Dispatch picks & runs the handler  ->  encode the reply
//
// The loop continues until the client closes the connection cleanly (io.EOF at a
// frame boundary) or an unrecoverable IO/protocol error occurs. The connection
// is always closed on return via the deferred Close.
func (s *Server) handle(conn net.Conn) {
	defer conn.Close()

	remote := conn.RemoteAddr()
	log.Printf("connection opened: %s", remote)
	defer log.Printf("connection closed: %s", remote)

	// Wrap the raw connection in buffered IO. The reader lets the decoder pull
	// bytes a frame at a time without a syscall per byte; the writer coalesces
	// the several small writes Encode makes per reply into fewer syscalls. We
	// must Flush the writer for the bytes to actually reach the client.
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// 1. DECODE one command frame from the client.
		request, err := protocol.Decode(reader)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				// Clean close: the client hung up at a frame boundary. Normal,
				// nothing to report.
				return
			case errors.Is(err, io.ErrUnexpectedEOF):
				// The client vanished mid-frame; there's no one left to reply
				// to, so just drop the connection.
				return
			default:
				// A genuine protocol error (garbage on the wire). Tell the
				// client, then close: after a framing error the byte stream is
				// out of sync and we can't reliably find the next frame.
				_ = protocol.Encode(writer, protocol.Value{
					Type: protocol.TypeError,
					Str:  "ERR " + err.Error(),
				})
				_ = writer.Flush()
				log.Printf("protocol error from %s: %v", remote, err)
				return
			}
		}

		// 2. DISPATCH: look up the command and run it, persisting it to the AOF
		//    first if it is a successful write (see apply). apply always returns a
		//    Value to send back — a normal reply or a RESP error — so command
		//    failures never break the loop.
		reply := s.apply(request)

		// 3. ENCODE the reply and flush it to the client. An error here means
		//    the connection is broken (peer gone, write timeout, ...), so we
		//    stop serving it.
		if err := protocol.Encode(writer, reply); err != nil {
			log.Printf("write error to %s: %v", remote, err)
			return
		}
		if err := writer.Flush(); err != nil {
			log.Printf("flush error to %s: %v", remote, err)
			return
		}
	}
}
