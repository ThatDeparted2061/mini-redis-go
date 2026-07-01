// Package replication wires a primary to its replicas: the primary streams every
// successful write command to each connected replica, and a replica applies that
// live stream into its own store. This file is the PRIMARY side — the registry of
// connected replicas to fan writes out to. The replica side is in replica.go.
//
// v1 has no snapshot bootstrap: a replica only ever sees writes the primary makes
// AFTER it connects (see RunReplica's doc for the consequence). The registry here
// is deliberately tiny — it is just "who is listening to my writes right now".
//
// Write log shipping (Day 15): propagation is DECOUPLED from the socket. Each
// replica owns a buffered queue of already-serialised RESP frames; Propagate does
// a NON-BLOCKING enqueue into every queue and returns immediately, and a
// per-replica delivery goroutine (server.deliverReplica) drains the queue to the
// socket. This is the same shape as the pub/sub bus (db.Broker): the buffer
// absorbs bursts, and a slow replica that overruns its queue can no longer stall
// the primary's write path.
package replication

import (
	"bytes"
	"log"
	"sync"

	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// replicaQueueCap is how many undelivered write frames a replica's queue holds
// before Propagate starts dropping for it. Matches the pub/sub mailbox cap: a few
// hundred absorbs normal bursts without letting a stuck replica pin unbounded
// memory.
const replicaQueueCap = 256

// Replica is one connected replica's outgoing feed: a buffered queue of serialised
// RESP write frames. Propagate writes into ch; the connection's delivery goroutine
// drains it to the socket.
type Replica struct {
	id   uint64
	addr string // for logging only
	ch   chan []byte
}

// Feed is the receive end the delivery goroutine ranges over.
func (rep *Replica) Feed() <-chan []byte { return rep.ch }

// Replicas is the set of replica feeds a primary is currently streaming to. It is
// shared process-wide (one per server) and safe for concurrent use.
type Replicas struct {
	mu   sync.RWMutex
	next uint64
	reps map[uint64]*Replica
}

// NewReplicas returns an empty registry ready to accept replica feeds.
func NewReplicas() *Replicas {
	return &Replicas{reps: make(map[uint64]*Replica)}
}

// Add registers a new replica (identified by addr for logging) and returns it. The
// caller starts a delivery goroutine draining rep.Feed() and passes the same
// *Replica to Remove on disconnect.
func (r *Replicas) Add(addr string) *Replica {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := &Replica{id: r.next, addr: addr, ch: make(chan []byte, replicaQueueCap)}
	r.next++
	r.reps[rep.id] = rep
	return rep
}

// Remove drops a replica (on disconnect) and closes its queue, which ends the
// delivery goroutine's range loop. Removing from the map (under the write lock)
// BEFORE closing is what makes the close safe: once Remove holds the lock no
// Propagate (a reader) is mid-send, and any later Propagate won't find rep — so
// nothing can send on the closed channel. Same ordering rule as pubsub teardown.
func (r *Replicas) Remove(rep *Replica) {
	r.mu.Lock()
	delete(r.reps, rep.id)
	r.mu.Unlock()
	close(rep.ch)
}

// Any reports whether any replica is connected. The write path checks it to skip
// the propagation machinery entirely when there is nobody to stream to.
func (r *Replicas) Any() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.reps) > 0
}

// Propagate serialises one write command ONCE and enqueues the resulting frame to
// every connected replica. The caller invokes it under the server's writeMu, so
// commands are enqueued one at a time in exactly the order the store applied them;
// each replica's queue is FIFO, so the delivery goroutine writes them out in that
// same order — which is what keeps a replica in step with the primary.
//
// The send is NON-BLOCKING and done under the read lock (safe because it never
// blocks): if a replica's queue is full it is DROPPED with a logged warning rather
// than stalling the write path. The single frame is shared read-only across all
// replicas — it is freshly allocated here and the delivery goroutines only read it.
//
// ponytail: DROP-and-log means a slow replica silently DRIFTS out of sync and,
// with no snapshot bootstrap, can't recover without a restart. Upgrade path (real
// Redis): disconnect the replica on overflow so it reconnects and re-syncs, or
// spill its backlog to disk. Kept as drop-and-log for v1, matching pub/sub.
func (r *Replicas) Propagate(cmd protocol.Value) {
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, cmd); err != nil {
		log.Printf("replication: encode for propagation failed: %v", err)
		return
	}
	frame := buf.Bytes()

	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, rep := range r.reps {
		select {
		case rep.ch <- frame:
		default:
			log.Printf("replication: replica %s queue full, dropping write — replica will drift and needs a resync", rep.addr)
		}
	}
}
