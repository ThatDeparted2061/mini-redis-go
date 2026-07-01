package db

import (
	"fmt"
	"testing"
)

// TestShardIndexStableAndSpread checks the two properties the whole sharding
// scheme rests on: a key always maps to the SAME shard (or Get could never find
// what Set stored), and distinct keys SPREAD across shards (or sharding buys no
// parallelism). A bug that collapsed every key onto one shard would still pass
// every functional test — but it fails here.
func TestShardIndexStableAndSpread(t *testing.T) {
	// Deterministic: same key, same shard, every call.
	if shardIndex("hello") != shardIndex("hello") {
		t.Fatal("shardIndex is not deterministic")
	}

	// Spread: 1000 distinct keys should reach most shards, not pile into one.
	seen := make(map[uint32]struct{})
	for i := 0; i < 1000; i++ {
		seen[shardIndex(fmt.Sprintf("key:%d", i))] = struct{}{}
	}
	if len(seen) < shardCount/2 {
		t.Fatalf("1000 keys only reached %d of %d shards; distribution is skewed", len(seen), shardCount)
	}
}
