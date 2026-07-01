package replication

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// reconnectDelay is how long a replica waits before re-dialing the primary after
// the stream drops (or a dial fails). Fixed delay is plenty for a primary
// restart; the only cost of being wrong is reconnecting a second too early/late.
const reconnectDelay = time.Second

// RunReplica connects this server to its primary and applies the primary's live
// write stream into the local store, retrying until ctx is cancelled.
//
// The handshake is a single custom command: the replica dials the primary, sends
// REPLICAOF, and the primary replies +OK and then streams every subsequent write
// it makes down the same connection. For each streamed command RunReplica calls
// apply — the server passes a closure that dispatches it into the local db.
//
// v1 LIMITATION — no snapshot bootstrap. The replica only receives writes the
// primary makes AFTER the handshake; whatever data already lives on the primary
// is NOT transferred. A replica therefore mirrors the primary only for keys
// written after it connected. (The fix — streaming the primary's existing
// keyspace as a snapshot before the live stream — is the documented upgrade.)
func RunReplica(ctx context.Context, primaryAddr string, apply func(protocol.Value)) {
	for ctx.Err() == nil {
		if err := streamOnce(ctx, primaryAddr, apply); err != nil {
			log.Printf("replication: %v; retrying in %s", err, reconnectDelay)
		}
		// ponytail: fixed-delay reconnect; swap in exponential backoff if a
		// flapping primary ever makes the log noisy.
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

// streamOnce performs one full handshake-then-stream session against the primary,
// returning when the connection drops or ctx is cancelled.
func streamOnce(ctx context.Context, primaryAddr string, apply func(protocol.Value)) error {
	conn, err := net.Dial("tcp", primaryAddr)
	if err != nil {
		return fmt.Errorf("dial primary %s: %w", primaryAddr, err)
	}
	defer conn.Close()

	// Close the socket on shutdown so the blocking Decode below unblocks and this
	// goroutine can return — the same trick the accept loop uses on its listener.
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	r := bufio.NewReader(conn)

	// Handshake: send REPLICAOF, expect +OK before any streamed command.
	if err := protocol.Encode(conn, replicaofCommand()); err != nil {
		return fmt.Errorf("send REPLICAOF: %w", err)
	}
	ack, err := protocol.Decode(r)
	if err != nil {
		return fmt.Errorf("read handshake ack: %w", err)
	}
	if ack.Type != protocol.TypeSimpleString {
		return fmt.Errorf("unexpected handshake reply from primary: %+v", ack)
	}
	log.Printf("replication: connected to primary %s, streaming live writes", primaryAddr)

	// Stream: every subsequent frame is either the primary's heartbeat PING (which
	// we ack to prove liveness) or a write command to apply locally.
	for {
		cmd, err := protocol.Decode(r)
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown, not a failure
			}
			return fmt.Errorf("stream from primary %s: %w", primaryAddr, err)
		}
		if isHeartbeat(cmd) {
			// Ack so the primary's heartbeat sees us as alive. It is one-way — the
			// primary sends no reply to an ack — so writing here can't desync the
			// inbound stream we're decoding.
			if err := protocol.Encode(conn, replconfAck()); err != nil {
				return fmt.Errorf("ack heartbeat to %s: %w", primaryAddr, err)
			}
			continue
		}
		apply(cmd)
	}
}

// isHeartbeat reports whether a streamed frame is the primary's PING heartbeat
// (rather than a write command to apply).
func isHeartbeat(v protocol.Value) bool {
	return v.Type == protocol.TypeArray && len(v.Array) > 0 &&
		strings.EqualFold(string(v.Array[0].Bulk), "PING")
}

// replconfAck is the frame a replica sends back on each heartbeat so the primary
// can tell it is alive. v1 carries no offset (we track none); the primary only
// needs the ack's arrival time.
func replconfAck() protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
		{Type: protocol.TypeBulkString, Bulk: []byte("REPLCONF")},
		{Type: protocol.TypeBulkString, Bulk: []byte("ACK")},
		{Type: protocol.TypeBulkString, Bulk: []byte("0")},
	}}
}

// replicaofCommand is the handshake frame the replica sends to the primary. It
// carries no arguments: v1 just needs to say "stream your writes to me", and the
// primary already knows the connection's address for logging.
func replicaofCommand() protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
		{Type: protocol.TypeBulkString, Bulk: []byte("REPLICAOF")},
	}}
}
