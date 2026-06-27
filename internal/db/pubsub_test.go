package db

import "testing"

// TestBrokerPublishFanout: a published message reaches every subscriber on the
// channel, carries the channel name and payload, and the return count matches.
func TestBrokerPublishFanout(t *testing.T) {
	b := NewBroker()
	s1, s2 := NewSubscriber(), NewSubscriber()
	b.Subscribe("news", s1)
	b.Subscribe("news", s2)

	if got := b.Publish("news", []byte("hello")); got != 2 {
		t.Fatalf("Publish delivered to %d subscribers; want 2", got)
	}
	for i, s := range []*Subscriber{s1, s2} {
		select {
		case m := <-s.mailbox:
			if m.Channel != "news" || string(m.Payload) != "hello" {
				t.Fatalf("subscriber %d got %q on %q; want \"hello\" on \"news\"", i, m.Payload, m.Channel)
			}
		default:
			t.Fatalf("subscriber %d received no message", i)
		}
	}
}

// TestBrokerCopiesPayload: the broker must copy the caller's slice (the decoder's
// reused buffer), so mutating it after Publish doesn't corrupt a queued message.
func TestBrokerCopiesPayload(t *testing.T) {
	b := NewBroker()
	s := NewSubscriber()
	b.Subscribe("c", s)

	buf := []byte("orig")
	b.Publish("c", buf)
	copy(buf, "XXXX") // simulate the decoder reusing the buffer for the next frame

	m := <-s.mailbox
	if string(m.Payload) != "orig" {
		t.Fatalf("payload = %q; want \"orig\" (broker did not copy the buffer)", m.Payload)
	}
}

// TestBrokerDropsSlowSubscriber: a full mailbox makes Publish DROP (return 0),
// not block — the whole point of the non-blocking send.
func TestBrokerDropsSlowSubscriber(t *testing.T) {
	b := NewBroker()
	s := &Subscriber{mailbox: make(chan Message, 1)} // tiny mailbox, never drained
	b.Subscribe("c", s)

	if got := b.Publish("c", []byte("1")); got != 1 {
		t.Fatalf("first Publish delivered %d; want 1", got)
	}
	// Mailbox now full; the next send must be dropped rather than block forever.
	if got := b.Publish("c", []byte("2")); got != 0 {
		t.Fatalf("Publish to a full mailbox delivered %d; want 0 (dropped)", got)
	}
}

// TestBrokerUnsubscribeRemoves: after unsubscribing, the subscriber no longer
// receives, and the channel's entry is cleaned up.
func TestBrokerUnsubscribeRemoves(t *testing.T) {
	b := NewBroker()
	s := NewSubscriber()
	b.Subscribe("c", s)
	b.Unsubscribe("c", s)

	if got := b.Publish("c", []byte("x")); got != 0 {
		t.Fatalf("after Unsubscribe, Publish delivered %d; want 0", got)
	}
	if _, ok := b.subs["c"]; ok {
		t.Fatalf("channel entry lingered after its last subscriber left")
	}
}
