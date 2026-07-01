package server

import "log"

// replicaof turns this client connection into a replica feed: from now on the
// primary streams every successful write to it (see Server.apply -> replicas
// .Propagate). This is the primary side of the handshake the replica drives in
// internal/replication.
//
// Order matters: we write the +OK ack, then register the feed, then start the
// delivery goroutine that drains the feed to the socket. Because the ack is fully
// written before the feed exists and before anything drains it, no streamed
// command can reach the socket ahead of the ack. The flip side is a tiny window
// between ack and registration in which writes are not enqueued for this replica —
// acceptable under v1's "live stream from now on, no snapshot bootstrap" contract
// (a few writes around the handshake may be missed).
func (s *Server) replicaof(cs *connState) bool {
	if err := cs.write(okVal()); err != nil {
		return false
	}
	cs.replica = s.replicas.Add(cs.remote.String())
	go s.deliverReplica(cs)
	log.Printf("replication: %s registered as a replica", cs.remote)
	return true
}

// deliverReplica is the per-replica delivery goroutine: it drains the replica's
// queue of serialised write frames and ships each to the socket. It ends when the
// queue is closed (on disconnect, via removeReplica -> Replicas.Remove) or a write
// fails (broken socket) — the mirror of the pub/sub deliver goroutine.
func (s *Server) deliverReplica(cs *connState) {
	for frame := range cs.replica.Feed() {
		if err := cs.writeRaw(frame); err != nil {
			return
		}
	}
}

// removeReplica unregisters a connection's replica feed on disconnect, which also
// closes the feed and so stops deliverReplica. It is a no-op for ordinary
// (non-replica) connections, so handle can defer it unconditionally.
func (s *Server) removeReplica(cs *connState) {
	if cs.replica == nil {
		return
	}
	s.replicas.Remove(cs.replica)
	log.Printf("replication: %s replica disconnected", cs.remote)
}
