# mini-redis-go

A small, from-scratch Redis-compatible server in Go. It speaks the real Redis
wire protocol (RESP2), so standard clients — `redis-cli` and
`github.com/redis/go-redis/v9` — talk to it unmodified, backed by our own
in-memory key/value store.

This is a learning project built in phases. It is intentionally simple where
production Redis is complex (one global lock, single-format AOF persistence), and
it calls those simplifications out rather than hiding them.

## Quick start

```bash
go run ./cmd/server --port 6380     # start the server
redis-cli -p 6380                   # connect a real client
go test ./...                       # run everything
go test -race ./...                 # run with the race detector
```

## Supported commands

| Type     | Commands                                                        |
| -------- | --------------------------------------------------------------- |
| Generic  | `PING` `ECHO` `DEL` `EXISTS`                                     |
| String   | `SET` `GET`                                                     |
| List     | `LPUSH` `RPUSH` `LPOP` `RPOP` `LRANGE` `LLEN`                   |
| Hash     | `HSET` `HGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HLEN`           |
| Set      | `SADD` `SREM` `SISMEMBER` `SMEMBERS` `SCARD`                    |
| Expiry   | `EXPIRE` `PEXPIRE` `TTL` `PTTL` `PERSIST`                       |

A key holds exactly one type; using a command on the wrong type returns the
canonical `WRONGTYPE` error, just like Redis.

## Architecture

One client command flows through four layers, each oblivious to the others:

```
TCP conn → protocol.Decode → cmd.Dispatch → handler(db, args) → protocol.Encode → TCP conn
```

Handlers never touch the socket or the RESP wire format; they receive decoded
arguments and return a `protocol.Value`. There is exactly one shared `db.DB` for
the whole process, and all concurrency safety lives inside its `RWMutex` — the
server holds no locks of its own. (That single global lock is a deliberate
phase-1 simplification; sharding it is future work.)

## Key expiry (TTL)

Expiry is the most interesting part of the design so far, because making keys
disappear "on time" without a single authoritative clock forces some real
trade-offs. mini-redis-go mirrors Redis and runs **two complementary eviction
strategies**.

### Data model

Each value is an `Entry` carrying a type tag and an `expireAt time.Time`. The
**zero value means "no expiry"** (the key is persistent), so a key with no TTL
costs no extra branches and no extra memory beyond the zero `Time`. Storing the
deadline inline on the entry is simpler than Redis's separate "expires"
dictionary; the cost shows up in active sampling (below).

A key is expired when `now >= expireAt`. That predicate (`Entry.expired`) is the
single source of truth; every read and write consults it.

### Why two strategies?

- **Lazy (passive) expiry** checks a key *when it is accessed* and deletes it if
  it has expired. It is essentially free — you were already looking the key up —
  but on its own it **leaks memory**: a key that is set with a TTL and then never
  touched again is never noticed, so it lives forever.
- **Active expiry** is a background loop that periodically samples the keyspace
  and evicts expired keys *even if no client touches them*. It bounds the leak
  that lazy expiry alone would allow.

Neither is sufficient alone; together they keep both the wire view and memory
correct. Redis makes the same choice for the same reason.

### Lazy expiry, and the lock-escalation problem

The subtle part: deleting a key is a **write**, but `GET` is a **read** and
deliberately takes only the read lock so many `GET`s run in parallel. If `GET`
discovers an expired key it cannot delete it while holding the read lock.

`GET` resolves this by **escalating** only on the (rare) expired path: it drops
the read lock and calls `expireIfNeeded`, which takes the write lock and
**re-checks** expiry before deleting. The re-check matters — between releasing
the read lock and acquiring the write lock another client may have `SET` the key
fresh or `PERSIST`ed it, and we must not delete a key that is no longer expired.
The common, non-expired path never escalates and stays fully concurrent.

All other lookups funnel through one of two helpers, so expiry is honored
uniformly instead of being re-implemented per command:

- **`peek`** (read side): returns a live entry or reports an expired key as
  absent, *without* deleting it. It runs under the caller's read lock, so it
  can't reclaim memory; that is left to `GET`, the write paths, and the active
  reaper. This keeps the typed read commands (`LLEN`, `HGET`, `SCARD`, …) on the
  cheap read lock.
- **`liveEntry`** (write side): returns a live entry or deletes an expired one,
  under the write lock. This is also what makes a write to an expired key
  **resurrect** it as fresh data — e.g. `RPUSH` onto an expired list starts a new
  list rather than appending to about-to-die contents.

### Active expiry, and the adaptive heuristic

`RunActiveExpiry(ctx)` runs for the server's lifetime and stops when the context
is cancelled. The server launches it where the database and lifecycle context
live:

```go
go db.RunActiveExpiry(ctx)
```

Every **100 ms** it runs a sampling *pass*: inspect up to **20** keys that carry
a TTL, delete the expired ones, and compute the expired fraction of the sample.
If **more than 25%** were expired, the keyspace is probably full of dead keys, so
it immediately runs another pass instead of waiting for the next tick — Redis's
own adaptive heuristic ("if it's dirty, drain harder"). This terminates: each hot
pass deletes the expired keys it sampled, so the population of expired keys
strictly shrinks.

The "random" sample is drawn by simply ranging the map: Go randomizes map
iteration order, so each pass inspects a different slice of the keyspace with no
extra index to maintain. Only keys *with* a TTL count toward the sample (the
meaningful denominator for the 25% ratio); persistent keys are skipped.

### Trade-offs vs. real Redis (the honest part)

- **Inline `expireAt` vs. an expires dict.** Storing the deadline on the entry is
  simple, but it means active sampling has to *scan past* persistent keys to find
  TTL-bearing ones. Redis keeps a dedicated dictionary of keys-with-expiry and
  samples directly from it. With a keyspace that is mostly persistent, our pass
  does more work per useful sample. Fixable later by maintaining a parallel index.
- **Read commands report-absent but don't delete.** Only `GET` does synchronous
  lazy deletion (the spec's canonical example); other reads honor expiry for
  their *result* and defer reclamation to the active cycle and write paths. This
  is a conscious choice to avoid escalating a read lock to a write lock on every
  typed read. The external behavior is identical; only the memory-reclamation
  timing differs, and it's bounded by the 100 ms active cycle.
- **Map-iteration sampling isn't uniformly random.** Go randomizes the *start* of
  iteration, which is good enough for this purpose but is not the uniform random
  sampling Redis does from its expires dict.

## Persistence (AOF) and the fsync trade-off

Writes are made durable with an **append-only file (AOF)**: the server logs the
*command* it just ran (e.g. `SET k v`), in the exact RESP bytes the client sent,
and on restart replays that log through the normal dispatcher to rebuild state.
Persistence is on by default (`--appendonly`, log at `--aof-path`).

The interesting knob is **when the log reaches the physical disk**. Writing bytes
with `write()` only hands them to the operating system's page cache; they survive
a crash of *our process* (`kill -9`) because the kernel still owns them, but a
**power loss** drops anything the kernel hasn't flushed yet. `fsync()` forces the
page cache to the disk platter/SSD — and it is slow (~1 ms on an SSD), so *how
often* we call it is the canonical durability-vs-throughput trade-off. The
`--appendfsync` flag picks the policy:

| Mode               | When it fsyncs              | Crash can lose     | Speed                       |
| ------------------ | --------------------------- | ------------------ | --------------------------- |
| `always`           | after every command         | at most 1 command  | slowest (~1 ms disk/write)  |
| `everysec` (default) | once a second, in the background | up to ~1 s of writes | fast (write path never waits on disk) |
| `no`               | never (OS flushes on its own, ~30 s) | up to ~30 s of writes | fastest, riskiest    |

- **`always`** fsyncs inline in the write path, so once a client gets its reply
  the command is on disk. Safest, but every write pays the sync.
- **`everysec`** is the sweet spot most deployments want (and Redis's default): a
  background `time.Ticker` fsyncs once a second while the write path only ever
  does the cheap `write()`. A crash loses at most the last second.
- **`no`** never explicitly fsyncs; durability is entirely at the OS's mercy.
  Fastest, but a power loss can roll you back tens of seconds.

A **clean shutdown is not a crash**: in every mode the log is fsync'd on graceful
stop, so a normal `Ctrl-C` loses nothing — the mode only governs what an abrupt
power loss costs.

## Known gaps / non-goals (for now)

- **Sorted sets** (`ZADD`, `ZRANGE`, …) are intentionally skipped: doing them
  right needs a skip list (or balanced tree) for O(log n) score-ordered ranges,
  which the plain map/slice backings here don't provide.
- **Replication** and **metrics** are scaffolded but not implemented. AOF
  persistence works (see above), but **AOF rewrite/compaction** does not — the
  log only grows.
- The store uses **one global lock**; unrelated keys still contend. Sharding is
  future work.

See `CLAUDE.md` for the detailed, always-current implementation status.
