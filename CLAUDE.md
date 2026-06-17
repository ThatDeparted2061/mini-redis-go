# mini-redis-go

We are building **mini-redis-go**: a small, from-scratch Redis-compatible server
in Go. The goal is a server that speaks the real Redis wire protocol (RESP2) so
that standard Redis clients — `redis-cli`, `github.com/redis/go-redis/v9` — talk
to it unmodified, backed by our own in-memory key/value store.

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

_Last updated: 2026-06-18._

### Implemented
- **RESP2 protocol** (`internal/protocol`): decoder, encoder, value model, with
  parser tests. Binary-safe bulk strings.
- **In-memory store** (`internal/db/db.go`): `DB` guarded by an `RWMutex`;
  `Get`/`Set`/`Del`/`Exists` over `[]byte` values.
- **Command dispatch** (`internal/cmd/registry.go`): case-insensitive name
  lookup → handler → RESP reply. Registered commands:
  `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`.
- **Server** (`internal/server`): TCP accept loop, one goroutine per
  connection, graceful shutdown on SIGINT/SIGTERM (context-cancel closes the
  listener and drains in-flight connections).
- **Entrypoint** (`cmd/server/main.go`): `--port` flag (default `6380`).
- **Integration tests** (`tests/integration/basic_test.go`): drive the server
  over TCP with the upstream `go-redis/v9` client (PING, SET/GET, missing-key
  `redis.Nil`, DEL).

### Scaffolded (not yet implemented — empty stub files)
- Commands: `EXPIRE`, hashes, lists, sets, pub/sub, replication
  (`internal/cmd/{expire,hash,list,set,pubsub,replication}.go`).
- Store internals: `entry`, `expiry`, `pubsub`, `shard`
  (`internal/db/`).
- Persistence / AOF: `internal/persistence/{aof,replay,rewrite}.go`.
- Replication: `internal/replication/{primary,replica}.go`.
- Metrics: `internal/metrics/metrics.go`.
- Tests: `tests/integration/{aof,replication}_test.go`, `tests/chaos/*`.

> Note: `deploy/` (Dockerfile, compose, systemd, Grafana/Prometheus, docs)
> describes the intended production shape but runs ahead of the implemented
> server above.

## Architecture

Request flow for one client command:

```
TCP conn → protocol.Decode → cmd.Dispatch → handler(db, args) → protocol.Encode → TCP conn
```

Handlers never touch the socket or RESP wire format; they receive decoded
arguments and return a `protocol.Value`. There is exactly one shared `db.DB`
for the whole process — all concurrency safety lives inside its `RWMutex`, so
the server holds no locks of its own.

## Build / test / run

```bash
go run ./cmd/server --port 6380     # start the server
redis-cli -p 6380                   # connect a real client
go test ./...                       # run everything
go test ./tests/integration/ -v     # integration tests only
```
