#!/usr/bin/env bash
#
# demo.sh — a scripted tour of mini-redis-go: start a primary, run a few
# commands with redis-cli, attach a read-only replica, and watch a write on the
# primary show up on the replica.
#
# Recorded with:  asciinema rec --command "bash demo/demo.sh" demo/demo.cast
# Play back with: asciinema play demo/demo.cast
#
# Requires redis-cli on PATH (any recent Redis client works — the server speaks
# real RESP2). Uses ports 6380 (primary) and 6381 (replica).

set -u
cd "$(dirname "$0")/.."

BIN=./bin/mini-redis
PRIMARY_PORT=6380
REPLICA_PORT=6381

say() { printf '\n\033[1;36m%s\033[0m\n' "$*"; }               # cyan headings
run() { printf '\033[1;32m$ %s\033[0m\n' "$*"; eval "$*"; sleep 0.5; }

cleanup() { kill "${PRIMARY:-}" "${REPLICA:-}" 2>/dev/null; }
trap cleanup EXIT

say "# build the server"
run "go build -o $BIN ./cmd/server"

say "# 1. start the primary on :$PRIMARY_PORT"
$BIN --port "$PRIMARY_PORT" --appendonly=false >/tmp/mini-redis-primary.log 2>&1 &
PRIMARY=$!
sleep 1
run "redis-cli -p $PRIMARY_PORT PING"

say "# 2. a few commands over redis-cli"
run "redis-cli -p $PRIMARY_PORT SET hello world"
run "redis-cli -p $PRIMARY_PORT GET hello"
run "redis-cli -p $PRIMARY_PORT RPUSH fruits apple banana cherry"
run "redis-cli -p $PRIMARY_PORT LRANGE fruits 0 -1"
run "redis-cli -p $PRIMARY_PORT HSET user:1 name harsh role dev"
run "redis-cli -p $PRIMARY_PORT HGETALL user:1"

say "# 3. attach a read-only replica on :$REPLICA_PORT (replicaof the primary)"
$BIN --port "$REPLICA_PORT" --appendonly=false --replicaof "127.0.0.1 $PRIMARY_PORT" \
  >/tmp/mini-redis-replica.log 2>&1 &
REPLICA=$!
printf '\033[2m  (waiting for the replica to connect and handshake...)\033[0m\n'
sleep 2

say "# 4. write on the PRIMARY, then read it back on the REPLICA"
run "redis-cli -p $PRIMARY_PORT SET streamed hello-from-primary"
sleep 1
run "redis-cli -p $REPLICA_PORT GET streamed"

say "# 5. the replica is read-only — client writes are refused"
run "redis-cli -p $REPLICA_PORT SET nope value"

say "# done — shutting both down"
