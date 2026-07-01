// Package replication wires a primary to its replicas: the primary streams every
// successful write command to each connected replica, and a replica applies that
// live stream into its own store. This file is the PRIMARY side — the registry of
// connected replicas to fan writes out to. The replica side is in replica.go.
//
// v1 has no snapshot bootstrap: a replica only ever sees writes the primary makes
// AFTER it connects (see RunReplica's doc for the consequence). The registry here
// is deliberately tiny — it is just "who is listening to my writes right now".
package replication

import (
	"log"
	"sync"

	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Replicas is the set of replica feeds a primary is currently streaming to. It is
// shared process-wide (one per server) and safe for concurrent use.
//
// A "feed" is just a function that writes one command frame to one replica's
// socket. The server supplies its per-connection write method (the same one that
// serialises pub/sub pushes against replies), so propagation reuses that
// machinery instead of touching sockets itself — and so a replica feed can never
// interleave with the +OK handshake ack on the same connection.
type Replicas struct {
	mu   sync.RWMutex
	next uint64
	feed map[uint64]func(protocol.Value) error
}

// NewReplicas returns an empty registry ready to accept replica feeds.
func NewReplicas() *Replicas {
	return &Replicas{feed: make(map[uint64]func(protocol.Value) error)}
}

// Add registers a replica feed and returns an id the caller uses to Remove it
// when the replica disconnects.
func (r *Replicas) Add(write func(protocol.Value) error) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.next
	r.next++
	r.feed[id] = write
	return id
}

// Remove drops a replica feed (on disconnect). Removing an unknown id is a no-op.
func (r *Replicas) Remove(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.feed, id)
}

// Any reports whether any replica is connected. The write path checks it to skip
// the propagation machinery entirely when there is nobody to stream to.
func (r *Replicas) Any() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.feed) > 0
}

// Propagate streams one write command to every connected replica. The caller
// invokes it under the server's writeMu, so commands are propagated one at a time
// in exactly the order the store applied them — which is what keeps a replica's
// state in step with the primary's.
//
// It snapshots the feeds under the lock and writes outside it, so a slow replica's
// socket does not block another replica registering or disconnecting. A failed
// write is only logged here: the broken replica's read loop notices the same
// break and Removes itself.
//
// ponytail: a blocking socket write means one slow replica stalls ALL writes
// (Propagate runs under writeMu). Upgrade path: a per-replica buffered output
// queue with disconnect-on-overflow, as real Redis does.
func (r *Replicas) Propagate(cmd protocol.Value) {
	r.mu.RLock()
	feeds := make([]func(protocol.Value) error, 0, len(r.feed))
	for _, w := range r.feed {
		feeds = append(feeds, w)
	}
	r.mu.RUnlock()

	for _, write := range feeds {
		if err := write(cmd); err != nil {
			log.Printf("replication: propagate to replica failed: %v", err)
		}
	}
}
