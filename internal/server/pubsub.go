package server

import (
	"fmt"
	"strings"

	"github.com/ThatDeparted2061/mini-redis-go/internal/cmd"
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// allowedWhileSubscribed is the command set a connection in subscribe mode may
// run. A subscribed RESP2 connection is half-duplex — it is mostly a stream of
// pushed messages — so Redis restricts it to managing its subscriptions plus
// PING/QUIT. Anything else is refused (but the connection stays open).
var allowedWhileSubscribed = map[string]bool{
	"SUBSCRIBE":   true,
	"UNSUBSCRIBE": true,
	"PING":        true,
	"QUIT":        true,
}

// serve runs one decoded request for a connection and writes the reply. It
// returns false when the connection should close: a clean QUIT, or a write that
// failed (broken socket).
//
// SUBSCRIBE/UNSUBSCRIBE are handled here, not via cmd.Dispatch, because they act
// on the CONNECTION (registering its mailbox, entering/leaving subscribe mode) —
// state the stateless command handlers don't have. Everything else, PUBLISH
// included, goes through the normal apply path.
func (s *Server) serve(cs *connState, request protocol.Value) bool {
	name := strings.ToUpper(commandName(request))

	// In subscribe mode, refuse anything outside the restricted set but keep the
	// connection (and its live subscriptions) running.
	if cs.subscribed() && !allowedWhileSubscribed[name] {
		return cs.write(errorVal(fmt.Sprintf(
			"ERR Can't execute '%s': only (UN)SUBSCRIBE / PING / QUIT are allowed in subscribe context",
			strings.ToLower(name)))) == nil
	}

	switch name {
	case "SUBSCRIBE":
		return s.subscribe(cs, request.Array[1:])
	case "UNSUBSCRIBE":
		return s.unsubscribe(cs, request.Array[1:])
	case "REPLICAOF":
		// Handled here, not via cmd.Dispatch, because (like SUBSCRIBE) it acts on
		// the CONNECTION: it turns this socket into a replica feed.
		return s.replicaof(cs)
	case "REPLCONF":
		// A replica acks the primary's heartbeat with REPLCONF ACK. It is one-way:
		// record the ack and send NO reply, or the reply would land in the replica's
		// inbound stream and desync it. A REPLCONF from an ordinary client just gets OK.
		if cs.replica != nil {
			cs.replica.Acked()
			return true
		}
		return cs.write(okVal()) == nil
	case "QUIT":
		// QUIT acknowledges and closes, in any mode.
		_ = cs.write(okVal())
		return false
	default:
		// A read-only replica refuses writes from ordinary clients: its data comes
		// only from the primary's stream (which goes through Dispatch, not here).
		// Reads fall through to apply as usual — a replica is eventually consistent.
		if s.primaryAddr != "" && cmd.IsWrite(name) {
			return cs.write(errorVal("READONLY You can't write against a read only replica.")) == nil
		}
		return cs.write(s.apply(request)) == nil
	}
}

// subscribe registers the connection as a listener on each named channel and
// acknowledges each with a "subscribe" frame carrying the running subscription
// count. The first subscription lazily creates the connection's mailbox and
// starts its delivery goroutine.
//
// For each channel it registers in the broker BEFORE writing the acknowledgement,
// so that by the time the client sees the ack the subscription is already live —
// a publish the client issues next can't slip through unregistered.
func (s *Server) subscribe(cs *connState, channels []protocol.Value) bool {
	if len(channels) == 0 {
		return cs.write(errorVal("ERR wrong number of arguments for 'subscribe' command")) == nil
	}

	if cs.sub == nil {
		cs.sub = db.NewSubscriber()
		cs.channels = make(map[string]struct{})
		go s.deliver(cs)
	}

	for _, c := range channels {
		channel := string(c.Bulk)
		if _, already := cs.channels[channel]; !already {
			cs.channels[channel] = struct{}{}
			s.db.PubSub().Subscribe(channel, cs.sub)
		}
		if err := cs.write(subscribeReply("subscribe", channel, len(cs.channels))); err != nil {
			return false
		}
	}
	return true
}

// unsubscribe removes the connection from the named channels (or from ALL of them
// when none are named, matching Redis), acknowledging each. Leaving the last
// channel drops the connection out of subscribe mode.
func (s *Server) unsubscribe(cs *connState, channels []protocol.Value) bool {
	targets := make([]string, 0, len(channels))
	if len(channels) == 0 {
		for ch := range cs.channels {
			targets = append(targets, ch)
		}
	} else {
		for _, c := range channels {
			targets = append(targets, string(c.Bulk))
		}
	}

	// UNSUBSCRIBE while subscribed to nothing still gets a single acknowledgement
	// with a null channel and a zero count, as Redis does.
	if len(targets) == 0 {
		return cs.write(subscribeReplyNull("unsubscribe", 0)) == nil
	}

	for _, channel := range targets {
		if _, ok := cs.channels[channel]; ok {
			delete(cs.channels, channel)
			s.db.PubSub().Unsubscribe(channel, cs.sub)
		}
		if err := cs.write(subscribeReply("unsubscribe", channel, len(cs.channels))); err != nil {
			return false
		}
	}
	return true
}

// deliver is the per-connection delivery goroutine: it drains the mailbox and
// writes each message to the socket as a RESP "message" frame. It ends when the
// mailbox is closed (on teardown) or a write fails (broken socket).
func (s *Server) deliver(cs *connState) {
	for msg := range cs.sub.Mailbox() {
		if err := cs.write(messageReply(msg.Channel, msg.Payload)); err != nil {
			return
		}
	}
}

// unsubscribeAll tears down a connection's pub/sub state on disconnect: it
// removes the subscriber from every channel it held and then closes the mailbox,
// which stops the delivery goroutine. Removing from the broker BEFORE closing is
// what makes the close safe — once Unsubscribe (a broker writer) has returned for
// every channel, no in-flight Publish can still send into this mailbox.
func (s *Server) unsubscribeAll(cs *connState) {
	if cs.sub == nil {
		return
	}
	for channel := range cs.channels {
		s.db.PubSub().Unsubscribe(channel, cs.sub)
	}
	cs.sub.Close()
}

// ---------------------------------------------------------------------------
// RESP reply builders for the pub/sub frames. These mirror Redis's RESP2 shapes
// so unmodified clients (redis-cli, go-redis) understand them.
// ---------------------------------------------------------------------------

// subscribeReply builds the 3-element confirmation Redis sends for a
// (un)subscribe: [kind, channel, count] — e.g. ["subscribe", "news", 1].
func subscribeReply(kind, channel string, count int) protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
		{Type: protocol.TypeBulkString, Bulk: []byte(kind)},
		{Type: protocol.TypeBulkString, Bulk: []byte(channel)},
		{Type: protocol.TypeInteger, Int: int64(count)},
	}}
}

// subscribeReplyNull is the channel-less variant used when UNSUBSCRIBE has no
// channels to confirm: the channel field is a RESP null bulk string.
func subscribeReplyNull(kind string, count int) protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
		{Type: protocol.TypeBulkString, Bulk: []byte(kind)},
		{Type: protocol.TypeBulkString, Bulk: nil},
		{Type: protocol.TypeInteger, Int: int64(count)},
	}}
}

// messageReply builds the pushed-message frame: ["message", channel, payload].
func messageReply(channel string, payload []byte) protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
		{Type: protocol.TypeBulkString, Bulk: []byte("message")},
		{Type: protocol.TypeBulkString, Bulk: []byte(channel)},
		{Type: protocol.TypeBulkString, Bulk: payload},
	}}
}

// errorVal and okVal build the few non-pub/sub replies the connection loop emits
// directly (the cmd package's own reply constructors are unexported).
func errorVal(msg string) protocol.Value {
	return protocol.Value{Type: protocol.TypeError, Str: msg}
}

func okVal() protocol.Value {
	return protocol.Value{Type: protocol.TypeSimpleString, Str: "OK"}
}
