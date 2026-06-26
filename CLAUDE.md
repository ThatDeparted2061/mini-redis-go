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

_Last updated: 2026-06-26._

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
- **Server** (`internal/server`): TCP accept loop, one goroutine per
  connection, graceful shutdown on SIGINT/SIGTERM (context-cancel closes the
  listener and drains in-flight connections). Launches the active-expiry reaper
  for the server's lifetime and waits for it on shutdown.
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
- **Entrypoint** (`cmd/server/main.go`): `--port` (default `6380`),
  `--appendonly` (default `true`), `--aof-path` (default `appendonly.aof`) and
  `--appendfsync` (default `everysec`).
- **Tests**: `internal/cmd` unit tests (dispatch, lists, hashes, sets, expiry,
  WRONGTYPE), `internal/db` white-box expiry tests (lazy/active eviction,
  resurrection of expired keys on write), `internal/persistence` unit tests
  (append/replay round-trip, missing file, truncated-tail tolerance, everysec
  fsync-goroutine lifecycle), and
  `tests/integration/` end-to-end coverage driven by the upstream `go-redis/v9`
  client (`basic_test.go`, `list_test.go`, `hash_test.go`, `set_test.go`,
  `expire_test.go`, and `aof_test.go` — cross-restart durability, failed writes
  not persisted, and concurrent-write replay ordering).

### Scaffolded (not yet implemented — empty stub files)
- Commands: pub/sub, replication (`internal/cmd/{pubsub,replication}.go`).
- Store internals: `pubsub`, `shard` (`internal/db/`).
- AOF rewrite / compaction: `internal/persistence/rewrite.go`. The log only
  grows today (every write is appended forever); rewrite will compact it to the
  minimal command set that reproduces the current state.
- Replication: `internal/replication/{primary,replica}.go`.
- Metrics: `internal/metrics/metrics.go`.
- Tests: `tests/integration/replication_test.go`, `tests/chaos/*`.

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
TCP conn → protocol.Decode → server.apply ┬→ cmd.Dispatch → handler(db, args) ┐
                                           └→ (write & ok) AOF append           │
                                                          protocol.Encode → TCP conn
```

Handlers never touch the socket or RESP wire format; they receive decoded
arguments and return a `protocol.Value`. There is exactly one shared `db.DB`
for the whole process — all keyspace concurrency safety lives inside its
`RWMutex`. The server adds one lock of its own, `writeMu`, held only on the
write path so a command's database mutation and its AOF append happen as a unit
and the log's order matches the store's; reads never take it.

## Build / test / run

```bash
go run ./cmd/server --port 6380     # start the server
redis-cli -p 6380                   # connect a real client
go test ./...                       # run everything
go test ./tests/integration/ -v     # integration tests only
```
