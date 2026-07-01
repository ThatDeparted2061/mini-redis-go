package replication

// White-box checks for the Day-15 write-log-shipping registry: Propagate enqueues
// a serialised frame per replica, never BLOCKS on a full queue (drop & log), and
// Remove closes the feed so the delivery goroutine can end.

import (
	"testing"
	"time"

	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

func setCmd() protocol.Value {
	return protocol.Value{Type: protocol.TypeArray, Array: []protocol.Value{
		{Type: protocol.TypeBulkString, Bulk: []byte("SET")},
		{Type: protocol.TypeBulkString, Bulk: []byte("k")},
		{Type: protocol.TypeBulkString, Bulk: []byte("v")},
	}}
}

func TestPropagateEnqueuesAndDrops(t *testing.T) {
	reps := NewReplicas()
	if reps.Any() {
		t.Fatal("empty registry should report no replicas")
	}

	rep := reps.Add("test")
	if !reps.Any() {
		t.Fatal("after Add, Any should be true")
	}

	// A propagated write lands as one frame in the replica's queue.
	reps.Propagate(setCmd())
	select {
	case frame := <-rep.Feed():
		if len(frame) == 0 {
			t.Fatal("propagated frame is empty")
		}
	default:
		t.Fatal("expected a queued frame after Propagate")
	}

	// Fill the queue to capacity, then Propagate must DROP rather than block.
	for i := 0; i < replicaQueueCap; i++ {
		rep.ch <- []byte("x")
	}
	done := make(chan struct{})
	go func() { reps.Propagate(setCmd()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Propagate blocked on a full replica queue instead of dropping")
	}

	// Remove closes the feed (ends the delivery goroutine) and empties the registry.
	reps.Remove(rep)
	if reps.Any() {
		t.Fatal("after Remove, Any should be false")
	}
	// draining the closed channel eventually yields a closed signal, never a panic
	for range rep.Feed() {
	}
}

func TestStaleReplicasReportsSilentReplicas(t *testing.T) {
	reps := NewReplicas()
	reps.Add("fresh")
	stale := reps.Add("stale")

	// A just-connected replica is never stale — Add seeds lastAck to connect time.
	if got := reps.StaleReplicas(30 * time.Second); len(got) != 0 {
		t.Fatalf("fresh replicas reported stale: %v", got)
	}

	// Age one replica past the threshold; only it should be reported.
	stale.lastAck.Store(time.Now().Add(-31 * time.Second).UnixNano())
	got := reps.StaleReplicas(30 * time.Second)
	if len(got) != 1 || got[0] != "stale" {
		t.Fatalf("StaleReplicas = %v, want [stale]", got)
	}

	// An ack refreshes it back to live.
	stale.Acked()
	if got := reps.StaleReplicas(30 * time.Second); len(got) != 0 {
		t.Fatalf("after Acked, replica still reported stale: %v", got)
	}
}
