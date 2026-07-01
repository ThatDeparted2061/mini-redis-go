package server

import "log"

// replicaof turns this client connection into a replica feed: from now on the
// primary streams every successful write to it (see Server.apply -> replicas
// .Propagate). This is the primary side of the handshake the replica drives in
// internal/replication.
//
// Order matters: we write the +OK ack BEFORE registering the feed. If we
// registered first, a write on another connection could win cs.writeMu and push a
// command down this socket ahead of the ack, desyncing the replica's stream. The
// flip side is a tiny window between ack and registration in which writes are not
// propagated to this replica — acceptable under v1's "live stream from now on, no
// snapshot bootstrap" contract (a few writes around the handshake may be missed).
func (s *Server) replicaof(cs *connState) bool {
	if err := cs.write(okVal()); err != nil {
		return false
	}
	cs.replicaID = s.replicas.Add(cs.write)
	cs.isReplica = true
	log.Printf("replication: %s registered as a replica", cs.remote)
	return true
}

// removeReplica unregisters a connection's replica feed on disconnect. It is a
// no-op for ordinary (non-replica) connections, so handle can defer it
// unconditionally.
func (s *Server) removeReplica(cs *connState) {
	if !cs.isReplica {
		return
	}
	s.replicas.Remove(cs.replicaID)
	log.Printf("replication: %s replica disconnected", cs.remote)
}
