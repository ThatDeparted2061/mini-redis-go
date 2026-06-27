package db

// This file is the pub/sub MESSAGE BUS. It is orthogonal to the key/value
// keyspace in the rest of this package: publishing a message stores nothing and
// reads nothing — it just fans a payload out to whoever is listening right now.
// It lives beside the store only because both are process-wide shared state the
// server hands to its connections.
//
// The shape, following the classic design:
//
//	channel name ──► list of subscriber mailboxes (one per subscribed connection)
//
// A "mailbox" is a buffered Go channel. PUBLISH drops a message into every
// subscriber's mailbox; each connection has its own goroutine draining its
// mailbox out to the socket. The buffer absorbs short bursts; when it overflows
// (a subscriber too slow to keep up) PUBLISH DROPS the message rather than block
// — see Publish.

import (
	"log"
	"sync"
)

// subMailboxCap is how many undelivered messages a subscriber's mailbox holds
// before PUBLISH starts dropping for it. A few hundred absorbs normal bursts
// without letting a stuck consumer pin unbounded memory.
const subMailboxCap = 256

// Message is a published payload tagged with the channel it went out on. The
// channel name has to ride along with the payload because one subscriber can
// listen on many channels, and the RESP frame it forwards to the client names
// the originating channel. (This is why the mailbox carries Message, not a bare
// []byte.)
type Message struct {
	Channel string
	Payload []byte
}

// Subscriber is one connection's inbox into the bus. PUBLISH writes to mailbox;
// the connection's delivery goroutine reads from it. The same Subscriber is
// registered under every channel the connection has subscribed to.
type Subscriber struct {
	mailbox chan Message
}

// NewSubscriber creates a subscriber with an empty, buffered mailbox.
func NewSubscriber() *Subscriber {
	return &Subscriber{mailbox: make(chan Message, subMailboxCap)}
}

// Mailbox is the receive end the delivery goroutine ranges over.
func (s *Subscriber) Mailbox() <-chan Message { return s.mailbox }

// Close shuts the mailbox, which ends the delivery goroutine's range loop.
//
// The caller MUST have already removed this subscriber from the broker (via
// Unsubscribe for every channel it held) before calling Close. Otherwise a
// concurrent Publish could still try to send into the mailbox and panic on a
// closed channel. Broker's Lock/RLock make that ordering enforceable: once the
// final Unsubscribe (a writer) returns, no in-flight Publish (a reader) can still
// reference this subscriber.
func (s *Subscriber) Close() { close(s.mailbox) }

// Broker is the registry mapping each channel name to the subscribers currently
// listening on it. One Broker is shared by the whole process.
type Broker struct {
	mu   sync.RWMutex
	subs map[string][]*Subscriber
}

// NewBroker returns an empty broker ready to accept subscriptions.
func NewBroker() *Broker {
	return &Broker{subs: make(map[string][]*Subscriber)}
}

// Subscribe registers sub as a listener on channel. The caller is responsible
// for not registering the same subscriber twice on one channel (the connection
// tracks its own channel set and only calls this for newly-added channels), so
// no duplicate check is needed here.
func (b *Broker) Subscribe(channel string, sub *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[channel] = append(b.subs[channel], sub)
}

// Unsubscribe removes sub from channel, and drops the channel's entry entirely
// once its last listener leaves so the map doesn't accumulate empty slices.
func (b *Broker) Unsubscribe(channel string, sub *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	list := b.subs[channel]
	for i, s := range list {
		if s == sub {
			// Order among subscribers doesn't matter, so remove in O(1) by
			// swapping the last element into the gap.
			list[i] = list[len(list)-1]
			b.subs[channel] = list[:len(list)-1]
			break
		}
	}
	if len(b.subs[channel]) == 0 {
		delete(b.subs, channel)
	}
}

// Publish delivers payload to every subscriber on channel and returns how many
// received it.
//
// The send is NON-BLOCKING: if a subscriber's mailbox is full — a slow consumer
// not draining fast enough — its copy is DROPPED with a logged warning instead
// of blocking the publisher. Blocking would be far worse: PUBLISH runs on the
// publishing client's goroutine, so one stuck subscriber would stall the client
// that published and, if the lock were held, everyone else too. Real Redis
// instead disconnects a subscriber that overruns its output buffer; dropping is
// the simpler v1 choice (see the README's slow-subscriber discussion).
//
// The payload is copied ONCE here because the caller's slice is the decoder's
// read buffer, which is reused for the next command on that connection; the
// copied Message is then shared read-only across all subscribers.
func (b *Broker) Publish(channel string, payload []byte) int {
	msg := Message{Channel: channel, Payload: cloneBytes(payload)}

	b.mu.RLock()
	defer b.mu.RUnlock()

	delivered := 0
	for _, sub := range b.subs[channel] {
		select {
		case sub.mailbox <- msg:
			delivered++
		default:
			log.Printf("pubsub: mailbox full, dropping message to a slow subscriber on %q", channel)
		}
	}
	return delivered
}
