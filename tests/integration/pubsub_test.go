package integration

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestPubSubDeliversAcrossConnections is the acceptance test: a message PUBLISH'd
// on one connection reaches a SUBSCRIBE'r on another, with the right channel and
// payload.
func TestPubSubDeliversAcrossConnections(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	sub := client.Subscribe(ctx, "news")
	defer func() { _ = sub.Close() }()

	// Block for the subscribe confirmation so the subscription is registered
	// before we publish — otherwise the publish could race ahead of it.
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe confirmation: %v", err)
	}

	if n, err := client.Publish(ctx, "news", "hello").Result(); err != nil || n != 1 {
		t.Fatalf("PUBLISH = %d, %v; want 1, nil", n, err)
	}

	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("receive message: %v", err)
	}
	if msg.Channel != "news" || msg.Payload != "hello" {
		t.Fatalf("got %q on %q; want \"hello\" on \"news\"", msg.Payload, msg.Channel)
	}
}

// TestPubSubFanout: a single PUBLISH reaches every subscriber on the channel, and
// PUBLISH reports the subscriber count.
func TestPubSubFanout(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	subs := []*redis.PubSub{client.Subscribe(ctx, "room"), client.Subscribe(ctx, "room")}
	for _, sub := range subs {
		defer func() { _ = sub.Close() }()
		if _, err := sub.Receive(ctx); err != nil {
			t.Fatalf("subscribe confirmation: %v", err)
		}
	}

	if n, err := client.Publish(ctx, "room", "hi all").Result(); err != nil || n != 2 {
		t.Fatalf("PUBLISH = %d, %v; want 2, nil", n, err)
	}

	for i, sub := range subs {
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			t.Fatalf("subscriber %d receive: %v", i, err)
		}
		if msg.Payload != "hi all" {
			t.Fatalf("subscriber %d got %q; want \"hi all\"", i, msg.Payload)
		}
	}
}

// TestPublishNoSubscribers: publishing to a channel nobody listens on returns 0.
func TestPublishNoSubscribers(t *testing.T) {
	client := startServer(t)
	ctx := opCtx(t)

	if n, err := client.Publish(ctx, "void", "anybody?").Result(); err != nil || n != 0 {
		t.Fatalf("PUBLISH to no subscribers = %d, %v; want 0, nil", n, err)
	}
}
