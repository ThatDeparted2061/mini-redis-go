# mini-redis-go — Capacity model

Quantitative sizing: how much one node does, where it breaks, and what a fleet
for a target load costs. Numbers are *measured* where marked and *derived* (with
stated assumptions) otherwise. See `ARCHITECTURE.md` for the design and `LLD.md`
for the per-key memory breakdown this reuses.

> **Measurement rig.** Apple M4, 10 cores, macOS, loopback, `redis-benchmark`.
> A dev laptop, not a cloud box — fsync latency in particular differs on a Linux
> NVMe/SSD. Treat these as an order-of-magnitude baseline, and re-measure on the
> target host (`/metrics` exposes the same latencies live).

---

## 1. Throughput (measured)

`redis-benchmark`, 50 connections, 300k–500k ops:

| Workload | No pipeline (`-P 1`) | Pipelined (`-P 16`) |
|---|---|---|
| SET, no AOF (`--appendonly=false`) | **~97k ops/s** | ~448k ops/s |
| GET, no AOF | **~97k ops/s** | ~445k ops/s |
| SET, AOF `everysec` | **~73k ops/s** | — |

Reading these:
- **~97k ops/s** is the honest single-node number for a normal
  request/response client (no pipelining). The 10 cores don't multiply it because
  a non-pipelined client is round-trip-latency bound, not CPU bound.
- **Pipelining ~4.5×'s it** (~450k) by amortizing syscalls/round-trips — the
  server's real ceiling when clients batch.
- **AOF `everysec` costs ~25%** (97k → 73k): every write does a `bufio` flush to
  the OS on the `writeMu`-serialised path; the fsync itself is off the hot path
  (background ticker, measured ~11 ms/fsync here).

"Per core": treat ~10k ops/s/core as the conservative planning figure for
non-pipelined traffic on this class of core, rising sharply with pipelining.

---

## 2. Network bandwidth

Redis is often network-bound before it's CPU-bound. At a 1 KB average payload:

```
200,000 QPS × 1 KB = 200 MB/s ≈ 1.6 Gbps   → saturates a 1 GbE link.
```

So a single node on 1 GbE tops out near ~120k QPS of 1 KB values *on the wire*
regardless of CPU. The fix is bigger values amortized by pipelining, 10 GbE
NICs, or — past one box — partitioning (§5). Small values (our benchmark used
tiny ones) stay CPU/syscall-bound instead, which is why §1 hit ~97k on loopback.

---

## 3. Memory

From `LLD.md`: fixed overhead is **~136 bytes/key** (96-byte `Entry` + ~40 bytes
of Go map slot/bucket), plus the key name and the value bytes.

```
per key ≈ 136 (fixed) + key_len + value_len
200 B values, 16 B keys : ~352 B/key   → 1M keys ≈ 350 MB   (measured-order in §1: keys_total × this)
500 B values, 16 B keys : ~652 B/key   → used for the fleet sizing in §4
```

The spec's "~270 B/entry for small workloads" assumes a slimmer entry; this
implementation's fat `Entry` (four inline type slots) lands higher. Provision for
the measured **~350 B/key at 200 B values**. With **no `maxmemory` eviction**,
this is a floor set by the working set — only TTLs bound it.

---

## 4. Sizing for 1M QPS / 50M keys / 500 B values

**Memory first.** 50M keys × ~652 B/key ≈ **32.6 GB** of live data. Go's GC wants
headroom on a write-heavy heap — budget ~2× → **~65 GB** total across the fleet.

**Throughput next.** Real traffic pipelines partially; assume a sustained
**~170k QPS/node** on a 16-core cloud box (between the measured 97k non-pipelined
and 450k fully-pipelined, left deliberately conservative for headroom):

```
nodes for throughput = 1,000,000 QPS ÷ 170k QPS/node ≈ 6 nodes
memory per node      = 65 GB ÷ 6 ≈ 11 GB   → fits comfortably in 32 GB
keys per node        = 50M ÷ 6 ≈ 8.3M keys
```

→ **~6 nodes × 16 vCPU × 32 GB RAM.** The binding constraint is **throughput, not
memory** (11 GB used of 32 GB) — the extra RAM is GC + burst headroom. Because
there's no cross-node clustering yet, "6 nodes" means **client-side partitioning**
(hash keys across 6 independent primaries), each optionally with a read replica.

**Network per node:** 170k QPS × 500 B ≈ 85 MB/s ≈ **0.7 Gbps** — inside 1 GbE
but with little margin, so spec 10 GbE.

---

## 5. Bottleneck progression

1. **AOF fsync / write durability (single node, first to bite).** `everysec`
   already costs ~25% (§1); `always` would cap writes at the disk's fsync rate.
   The `writeMu` that orders {dispatch, AOF append, replica propagate} serialises
   *all* writes through one lock — reads scale across 32 shards, writes do not.
   *Upgrade:* the documented per-write-enqueue → single log-writer goroutine
   (parallelises the store mutation from the append).
2. **Single-primary CPU / NIC.** Past the write-lock fix, one box is bounded by
   cores and the NIC (§2). Vertical scaling ends here.
3. **Partitioning (the next architectural shift).** Split the keyspace across N
   primaries so writes scale horizontally. Today that's client-side sharding; a
   real cluster (gossip, slot map, resharding) is the larger project. This is the
   step that takes the system past one machine's write ceiling.

---

## 6. Cost projection

At commodity-cloud pricing (Hetzner-class dedicated vCPU, ~$40/mo for a
16-core/32 GB box):

```
6 nodes × ~$40/mo ≈ $240/mo   (+ replicas / bandwidth as needed)
```

→ **~$240/mo** to serve 1M QPS / 50M keys on commodity hardware. A managed Redis
of the same size runs **5–10×** that — the cost argument for running your own,
and the reason the fsync/partitioning work above is worth doing.
