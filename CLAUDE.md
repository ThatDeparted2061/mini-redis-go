# mini-redis-go

We are building **mini-redis-go**: a small, from-scratch Redis-compatible server
in Go. The goal is a server that speaks the real Redis wire protocol (RESP2) so
that standard Redis clients — `redis-cli`, `github.com/redis/go-redis/v9` — talk
to it unmodified, backed by our own in-memory key/value store.

## Explaining code (IMPORTANT)

Whenever I ask you to explain code — a commit, a file, a function, anything —
assume I don't know anything and explain it from the ground up:

- Start from the concepts the code rests on (what the thing *is* and why it
  exists) before diving into specifics.
- Define every term the first time it appears; avoid unexplained jargon.
- Use plain language, concrete analogies, and step-by-step examples.
- Favor "what this means and why" over a line-by-line restatement of the code.

**Always give the explanation in this order:**

1. **Plain-terms first** — explain the idea with everyday language and a concrete
   analogy, as if to someone who has never seen the code.
2. **Then the technical terms, mapped to the analogy** — name the real terms
   (the function, the data structure, the protocol) and tie each one back to the
   plain-terms version, so the analogy and the actual code line up.
3. **Then a flowchart** — an ASCII diagram of what is happening (the flow of
   data/control), so the moving parts are visible at a glance.

This applies to every explanation request unless I explicitly ask for the short
or expert version.

## Commit policy (IMPORTANT — overrides global instructions)

These rules take precedence over any global/user instruction about commit
trailers or co-authors:

- **No "Claude" anywhere in commits.** Do not add a `Co-Authored-By: Claude`
  trailer, and do not mention Claude, Anthropic, or any AI assistant in the
  commit message, subject, or body.
- **Author is the user only.** Every commit is authored as
  `ThatDeparted2061 <harshraocodesup@gmail.com>` and nothing else.
- Before committing, double-check the message contains none of the above. If a
  commit already on a branch violates this, amend/rewrite it to comply.
- **Never describe or mention `CLAUDE.md` in a commit message.** When a commit
  touches this file (e.g. the status-summary update below), leave it out of the
  message entirely — the message describes the code change, not this file.

## Workflow: keep the status summary current

**After every commit**, review the "Project status" section below and update it
to reflect what now exists. Concretely:

1. After committing, check whether the change moved anything between
   "Implemented" and "Scaffolded (not yet implemented)", added a new command,
   or changed the architecture.
2. If so, edit this file to match reality, then include that update in the next
   commit (subject to the commit policy above).
3. If nothing meaningful changed, leave it as-is — don't churn the file.

Keep the summary truthful: an empty stub file is "scaffolded", not "done".

## Project status

_Last updated: 2026-07-01._

### Implemented
- **RESP2 protocol** (`internal/protocol`): decoder, encoder, value model, with
  parser tests. Binary-safe bulk strings.
- **Typed in-memory store** (`internal/db/`): `DB` guarded by an `RWMutex`,
  keyed to `*Entry` values (`entry.go`) that carry a type tag (string, list,
  hash, or set) plus an `expireAt` deadline. Operations are type-checked and
  return `ErrWrongType` on a mismatch. All key lookups funnel through two
  expiry-aware helpers — `peek` (read side) and `liveEntry` (write side) — so
  expired keys are uniformly invisible.
- **Command dispatch** (`internal/cmd/registry.go`): case-insensitive name
  lookup → handler → RESP reply. Registered commands:
  - Generic/string: `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`
  - Lists (`internal/cmd/list.go`): `LPUSH`, `RPUSH`, `LPOP`, `RPOP`,
    `LRANGE`, `LLEN` — slice-backed lists, negative-index `LRANGE`, empty-list
    keys auto-deleted, and `WRONGTYPE` enforced across types.
  - Hashes (`internal/cmd/hash.go`): `HSET`, `HGET`, `HDEL`, `HGETALL`,
    `HKEYS`, `HVALS`, `HLEN` — `map[string][]byte` per key, emptied hashes
    auto-deleted.
  - Sets (`internal/cmd/set.go`): `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`,
    `SCARD` — `map[string]struct{}` per key, emptied sets auto-deleted.
  - Expiry (`internal/cmd/expire.go`, `internal/db/expiry.go`): `EXPIRE`,
    `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`. Both Redis eviction strategies run:
    lazy (`GET` deletes an expired key on access, escalating the read lock) and
    active (a background reaper, `db.RunActiveExpiry`, wakes every 100ms, samples
    20 TTL-bearing keys, deletes the expired ones, and re-samples while >25% are
    expired).
  - Pub/Sub (`internal/cmd/pubsub.go` for `PUBLISH`; `internal/db/pubsub.go` for
    the bus): `PUBLISH` is a normal handler; `SUBSCRIBE`/`UNSUBSCRIBE` are
    intercepted in the connection loop (`internal/server/{connection,pubsub}.go`)
    because they act on the connection, not the keyspace.
- **Pub/Sub message bus** (`internal/db/pubsub.go`): a `Broker`
  (`map[channel] -> []*Subscriber`) reachable via `db.PubSub()`, independent of
  the keyspace. Each subscribed connection owns a buffered mailbox (`chan Message`,
  cap 256) and a delivery goroutine that drains it to the socket; the
  connection's `writeMu` serialises pushed messages against ordinary replies so
  RESP frames never interleave. `PUBLISH` fans out with a NON-BLOCKING send and
  DROPS to a slow subscriber (full mailbox) with a logged warning rather than
  blocking the publisher — the v1 trade-off (real Redis disconnects). Subscribed
  connections enter a restricted mode (only `SUBSCRIBE`/`UNSUBSCRIBE`/`PING`/
  `QUIT`). Teardown unregisters from the broker before closing the mailbox, so a
  concurrent `PUBLISH` can never send on a closed channel.
- **Server** (`internal/server`): TCP accept loop, one goroutine per
  connection, graceful shutdown on SIGINT/SIGTERM (context-cancel closes the
  listener and drains in-flight connections). Launches the active-expiry reaper
  and (when persistence is on) the AOF compactor for the server's lifetime and
  waits for them on shutdown. The per-connection loop routes pub/sub control
  commands and enforces subscribe-mode restrictions; `QUIT` is handled here too.
- **Persistence — append-only file** (`internal/persistence/{aof,replay}.go`):
  durability by logging the COMMAND, not the resulting state. On startup the
  server replays the log (`persistence.Replay`) by re-dispatching each recorded
  command through the normal `cmd.Dispatch` path, rebuilding state from history;
  while serving, every successful write (`cmd.IsWrite`) is appended in RESP wire
  format (`persistence.AOF`, a `bufio.Writer` over an `*os.File`) and flushed to
  the OS, so writes survive a `kill -9`. The apply-then-append of a write is
  serialised by the server's `writeMu` so the log's order matches the order the
  store applied them; reads stay lock-free of it. Replay tolerates a truncated
  trailing command (a torn tail from a crash) and a missing file. Failed writes
  (WRONGTYPE, bad args) are not logged. Surviving a power loss (not just a
  process crash) needs an `fsync` to the physical disk; how often that happens is
  the `FsyncMode` policy (`--appendfsync`): `always` (fsync inline per write,
  lose ≤1 command), `everysec` (default; a 1s `time.Ticker` goroutine fsyncs in
  the background, lose ≤1s) or `no` (never; OS flushes on its own). A clean
  shutdown fsyncs in every mode, so only an abrupt power loss costs writes.
- **Persistence — AOF rewrite / compaction**
  (`internal/persistence/rewrite.go`, `internal/db/snapshot.go`): the log grows
  with every write, so a key written a million times leaves a million frames.
  Compaction replaces that history with a SNAPSHOT — `db.Snapshot` hands out a
  deep copy of every live key, and the rewriter emits one value-restoring command
  per key (`SET`/`RPUSH`/`HSET`/`SADD`) plus a `PEXPIRE` for any TTL, writing them
  to `appendonly.aof.tmp` and `os.Rename`-ing it over the live log (atomic on
  POSIX). A background goroutine in the server checks once a second and triggers a
  rewrite once the log is past a 64 KiB floor AND has doubled since the last
  rewrite (`AOF.ShouldRewrite`, mirroring Redis's `auto-aof-rewrite-percentage`).
  The rewrite holds the server's `writeMu` for its whole duration — the simple v1
  design: writes pause so no command slips into the gap between snapshot and
  swap, at the cost of a write-latency hit (Redis avoids it with a fork/COW child;
  that's the upgrade path).
- **Replication — primary/replica live stream**
  (`internal/replication/{primary,replica}.go`, `internal/server/replication.go`):
  a primary streams every successful write to each connected replica, which
  applies it into its own store. A replica (`--replicaof "host port"`) dials the
  primary, sends a one-shot `REPLICAOF` handshake, gets `+OK`, then `Decode`s the
  live command stream and re-dispatches each frame through `cmd.Dispatch`
  (`RunReplica`, retrying every 1s until ctx cancel; the socket is closed on
  shutdown to unblock the read). On the primary, `REPLICAOF` is intercepted at the
  connection level (like `SUBSCRIBE`, not via `cmd.Dispatch`): `replicaof` writes
  the `+OK` ack, then registers the connection in the process-wide
  `replication.Replicas` registry — where each replica owns a buffered queue of
  serialised RESP frames (`chan []byte`, cap 256) — and starts a per-replica
  delivery goroutine (`deliverReplica`) that drains the queue to the socket.
  **Write log shipping (Day 15):** `apply` serialises each successful write once
  and `Propagate` does a NON-BLOCKING enqueue into every replica's queue, under the
  same `writeMu` as the dispatch and AOF append — so replicas receive writes in the
  store's order without any socket I/O on the write path; the fast lock-free path is
  taken only when there is neither an AOF nor any replica. A queue that overflows is
  DROPPED with a log (drop-and-log, same policy as a slow pub/sub subscriber).
  `Remove` deletes the replica under the write lock BEFORE closing its queue, so a
  concurrent `Propagate` can never send on a closed channel (mirrors pub/sub
  teardown); disconnect fires it via `removeReplica`, deferred in `handle`. v1
  trade-offs, both flagged in-code with upgrade paths: NO snapshot bootstrap (a
  replica only mirrors writes made AFTER it connects — pre-existing keys are not
  transferred), and DROP-and-log on overflow (a slow replica silently drifts and,
  with no bootstrap, can't resync without a restart; real Redis disconnects on
  overflow so the replica reconnects and re-syncs). Streamed writes go straight
  through `Dispatch`, so a replica does not re-log to its own AOF or chain-propagate.
- **Entrypoint** (`cmd/server/main.go`): `--port` (default `6380`),
  `--appendonly` (default `true`), `--aof-path` (default `appendonly.aof`),
  `--appendfsync` (default `everysec`) and `--replicaof` (default off; `"host port"`
  makes the server a replica of that primary).
- **Tests**: `internal/cmd` unit tests (dispatch, lists, hashes, sets, expiry,
  WRONGTYPE), `internal/db` white-box expiry tests (lazy/active eviction,
  resurrection of expired keys on write), `internal/persistence` unit tests
  (append/replay round-trip, missing file, truncated-tail tolerance, everysec
  fsync-goroutine lifecycle, and rewrite compaction + the `ShouldRewrite`
  trigger), and
  `tests/integration/` end-to-end coverage driven by the upstream `go-redis/v9`
  client (`basic_test.go`, `list_test.go`, `hash_test.go`, `set_test.go`,
  `expire_test.go`, and `aof_test.go` — cross-restart durability, failed writes
  not persisted, concurrent-write replay ordering, and rewrite-then-restart
  recovery), plus `internal/db` broker tests (fan-out, payload copy, slow-
  subscriber drop, unsubscribe cleanup) and `tests/integration/pubsub_test.go`
  (cross-connection delivery, fan-out, publish-to-nobody), plus
  `tests/integration/replication_test.go` (replica mirrors post-handshake writes;
  pre-handshake keys are NOT bootstrapped — asserts the v1 no-snapshot contract)
  and `internal/replication` white-box tests (`Propagate` enqueues per replica,
  drops without blocking on a full queue, `Remove` closes the feed safely).

### Scaffolded (not yet implemented — empty stub files)
- Store internals: `shard` (`internal/db/`).
- Metrics: `internal/metrics/metrics.go`.
- Tests: `tests/chaos/*`.
- Note: `internal/cmd/replication.go` is a vestigial stub — `REPLICAOF` is handled
  at the connection level (`internal/server/replication.go`), not as a `cmd`
  handler, so nothing lives in that file.

### Known gaps (intentionally unimplemented)
- **Sorted sets** (`ZADD`, `ZRANGE`, `ZSCORE`, …) are deliberately skipped.
  Doing them right needs a skip list (or balanced tree) to keep members ordered
  by score with O(log n) inserts and range queries — the plain map/slice
  backings the other types use can't provide that. Revisit once the store has a
  proper ordered structure.

> Note: `deploy/` (Dockerfile, compose, systemd, Grafana/Prometheus, docs)
> describes the intended production shape but runs ahead of the implemented
> server above.

## Architecture

Request flow for one client command:

```
TCP conn → protocol.Decode → server.serve ┬→ SUBSCRIBE/UNSUBSCRIBE/REPLICAOF (connection-level) ┐
                                           ├→ server.apply ┬→ cmd.Dispatch → handler            │
                                           │               ├→ (write & ok) AOF append           │
                                           │               └→ (write & ok) replicas.Propagate   │
                                           └→ QUIT / subscribe-mode guard                        │
                                                          protocol.Encode → TCP conn
```

Handlers never touch the socket or RESP wire format; they receive decoded
arguments and return a `protocol.Value`. There is exactly one shared `db.DB`
for the whole process — all keyspace concurrency safety lives inside its
`RWMutex`. The server adds one lock of its own, `writeMu`, held only on the
write path so a command's database mutation and its AOF append happen as a unit
and the log's order matches the store's; reads never take it.

Pub/Sub is the exception to "handlers never touch the socket": `SUBSCRIBE`/
`UNSUBSCRIBE` act on the connection (register a mailbox, enter/leave subscribe
mode, spawn a delivery goroutine), so `server.serve` handles them directly
instead of routing through `cmd.Dispatch`. `PUBLISH` stays an ordinary handler —
it only needs the broker, reachable via `db.PubSub()`. Each connection has a
`writeMu` (separate from the server-wide AOF `writeMu`) serialising socket writes
so a pushed message can't interleave with a reply.

`REPLICAOF` is the same kind of exception: it acts on the connection, turning the
socket into a replica feed registered in the `replication.Replicas` registry, so
`server.serve` handles it directly too. On the write path, `apply` propagates each
successful write to those feeds under the server-wide `writeMu` (alongside the AOF
append), so every replica sees writes in the store's order. A replica server
(`--replicaof`) runs the mirror side as a lifecycle goroutine (`RunReplica`) that
streams the primary's writes back through `cmd.Dispatch`.

## Build / test / run

```bash
go run ./cmd/server --port 6380     # start the server (a primary)
go run ./cmd/server --port 6381 --appendonly=false \
  --replicaof "127.0.0.1 6380"      # start a replica of that primary
redis-cli -p 6380                   # connect a real client
go test ./...                       # run everything
go test ./tests/integration/ -v     # integration tests only
```
