# Helix — Project Plan

> A single-binary, Linux-first, distributed commit-log / message queue written in C.
> Kafka-compatible *semantics* (topics, partitions, offsets, consumer groups, retention),
> with a design target of **lower tail latency and fewer moving parts** (no JVM, no ZooKeeper, no GC pauses).

---

## 0. Current Status (updated with each milestone)

| Phase | State | Evidence |
|-------|-------|----------|
| 0 Foundations | **Done** (CI, sanitizers, clang-tidy/format, fuzz harness) | `make ci` |
| 1 Storage engine | **Done**, minus the time index | 8 storage tests incl. 300-round torn-write/bit-flip recovery |
| 2 Network + broker | **Done** for epoll + Produce/Fetch/Ping; io_uring, sendfile, thread-per-core pending | 34-check smoke test, 5-round `kill -9` test, 219k fuzz execs clean |
| 3 Topics, partitions, groups | **Done** for a single node: Metadata, durable offsets, group coordinator (session expiry, eager rebalance, generation fencing), idempotent producer (in-memory, per broker lifetime), CreateTopic/DeleteTopic. Not done: cooperative rebalance, durable producer state, producer epochs | 53-check smoke test, offset `kill -9` test, 11 coordinator/producer/topic unit tests, 23-check group test (failover by `kill -9`), 36-check topic/idempotence test (dropped acks, delete + recreate) |
| 4 Replication & consensus | **Started**: Raft core is done and tested as a library (election with PreVote + CheckQuorum, log replication, commit rule, write-ahead storage interface, wire format) and the durable Raft WAL (CRC, torn-tail recovery, fail-closed corruption, compaction; simulator also run on real WAL files). and the TCP transport (3 real nodes over loopback elect, replicate and survive follower/leader restarts; hostile-peer and unreachable-peer tests). Snapshots / log compaction are done (host-driven, whole image in one message; simulator with a hash-chain state machine, 60k snapshots and 12k installs in the deep run). Not done: chunked snapshot transfer, membership change, TLS/auth on the Raft port, wiring into the broker (follower fetch, high-watermark, acks=quorum), broker-level partition/Jepsen test | `src/raft/`; 9 unit tests; deterministic simulator, 3005 random fault scenarios (~1.7M committed entries, 87k crashes, 28k partitions) with no safety violation; 9 deliberately injected Raft bugs all caught |
| 5-7 | Not started | |

**Measured, single node, 2-vCPU sandbox (client and server share the CPUs, so treat as a floor, not a result):**

| Workload (128 B records, pipeline depth 64) | Throughput | p99 latency |
|---|---|---|
| `--sync never` / `interval` | ~150-170k msg/s | ~1.0-1.4 ms |
| `--sync always`, per-record fsync (before group commit) | ~4.1k msg/s | 47 ms |
| `--sync always`, group commit | ~70k msg/s | 3.5 ms |

The v0.1 gate in section 3 (>= 300k msg/s, p99 < 5 ms) is **not yet demonstrated**: it needs a dedicated
multi-core machine, a separate load generator and the thread-per-core work in Phase 2/6. Nothing here is
a claim against Kafka; the comparison methodology is in `docs/SOLUTION.md` section 8.

---

## 1. Project Evaluation (read this first)

### 1.1 Is "Kafka but stronger" a realistic goal?

"Stronger" is not a measurable requirement. Kafka has 14+ years of production hardening. A senior-level
plan replaces the slogan with **explicit, falsifiable goals** and picks the dimensions where a C,
Linux-specific design can legitimately win:

| Dimension            | Kafka (JVM)                          | Helix target (C, Linux)                               |
|----------------------|--------------------------------------|-------------------------------------------------------|
| p99 produce latency  | GC pauses, page-cache jitter         | Thread-per-core, no GC, io_uring, deterministic memory|
| Memory footprint     | Multi-GB heap + page cache           | < 100 MB resident for broker core                     |
| Operational surface  | Broker + ZooKeeper/KRaft + tuning    | One static binary, one config file                    |
| Cold start           | Tens of seconds (log recovery + JVM) | < 1 s with checkpointed recovery                      |
| Ecosystem            | Huge                                 | **Will not win.** Mitigate via Kafka wire-protocol subset (Phase 7) |

**Honest verdict:** build it as a *learning-grade, production-shaped* system. Do not promise to beat
Kafka on throughput at scale; aim to match it per-core and beat it on tail latency and footprint on a
single node, then prove it with benchmarks.

### 1.2 Why this project is a good choice

- Touches every senior-level systems topic: storage engines, networking, concurrency, replication/consensus, crash consistency, observability.
- Scope is naturally **phaseable** — each phase ships a working artifact.
- Clear, benchmarkable outcomes (latency histograms, throughput, recovery time).

### 1.3 Terminology note: "cache" vs "message queue"

The request mentions a "caching system like Kafka". Kafka is a **durable commit log**, not a cache.
Helix is a durable log. A cache (e.g. Redis/Memcached) is a different system (evictable, in-memory).
Helix may optionally expose a **tiered in-memory hot cache** for recent segments (Phase 6), but durability is the primary contract.

---

## 2. Goals and Non-Goals

### Goals
1. Durable, ordered, append-only logs partitioned by key.
2. At-least-once delivery by default; idempotent producers → effectively-once per partition.
3. Consumer groups with offset commits and rebalancing.
4. Configurable retention (time/size) and log compaction.
5. Replication with leader election (Raft per partition group).
6. Observability: Prometheus metrics, structured logs, tracing hooks.
7. Linux-only optimizations: `io_uring`, `epoll`, `sendfile`/`splice`, `fallocate`, `O_DIRECT` (optional), CPU pinning, `SO_REUSEPORT`.

### Non-Goals (v1)
- Windows/macOS support.
- Full Kafka protocol compatibility (subset only, Phase 7).
- Exactly-once transactions across partitions.
- Stream processing (Kafka Streams equivalent).
- Multi-datacenter replication.

---

## 3. Success Metrics (Definition of Done per release)

| Metric                                   | v0.1 (single node) | v0.5 (replicated) | v1.0              |
|------------------------------------------|--------------------|-------------------|-------------------|
| Produce throughput (1 KB msgs, 1 node)   | ≥ 300k msg/s       | ≥ 200k msg/s (acks=quorum) | ≥ 500k msg/s |
| p99 produce latency (acks=leader)        | < 5 ms             | < 10 ms (quorum)  | < 3 ms            |
| Crash recovery (100 GB log)              | < 30 s             | < 30 s            | < 10 s            |
| Data loss after `kill -9` (fsync=always) | 0 acked messages   | 0                 | 0                 |
| Data loss after power-fail test          | 0 acked (fsync)    | 0                 | 0                 |
| Valgrind/ASan/TSan clean                 | Required           | Required          | Required + fuzzing|
| Jepsen-style partition test              | n/a                | No acked-write loss| Pass             |

---

## 4. Phased Roadmap

Estimates assume **one senior engineer, ~20 focused hours/week**. Adjust for team size.

### Phase 0 — Foundations (1–2 weeks)
- Repo layout, CMake/Make build, `-Wall -Wextra -Werror -fsanitize` profiles, clang-tidy, clang-format.
- CI: build matrix (gcc/clang), ASan/UBSan/TSan, unit tests (Unity/cmocka), coverage.
- Core utilities: arena allocator, intrusive lists, ring buffer, hash map, CRC32C (SSE4.2 + fallback), varint, logging, config parser, monotonic clock.
- **Exit criteria:** `make test` green under all sanitizers; CI enforced.

### Phase 1 — Storage Engine (3–4 weeks) ← *starter code included*
- Segmented append-only log, fixed-size preallocated segments (`fallocate`).
- Record batch format with CRC32C, length-prefix framing, magic/version byte.
- Sparse offset index + time index (mmap'd).
- Crash recovery: scan tail segment, truncate at first corrupt record.
- Fsync policies: `always | interval | never` + group commit.
- Retention by time/size; segment roll.
- **Exit criteria:** property-based test — random append/kill/recover sequences never lose acked data or serve corrupt data.

### Phase 2 — Network Layer & Broker Core (3–4 weeks)
- Binary protocol v1 (see ARCHITECTURE.md §6): Produce, Fetch, Metadata, OffsetCommit, Heartbeat.
- Event loop: thread-per-core, `SO_REUSEPORT`, `epoll` first; `io_uring` behind a backend interface.
- Zero-copy fetch via `sendfile` (plaintext) / `io_uring` fixed buffers.
- Backpressure, per-connection quotas, request pipelining, max-in-flight limits.
- **Exit criteria:** 100k concurrent idle connections; sustained 300k msg/s on one node.

### Phase 3 — Topics, Partitions, Consumer Groups (3 weeks)
- Topic/partition metadata store (single-node file, later Raft-backed).
- Partitioner (murmur3 key hash, sticky, round-robin).
- Consumer group coordinator, generation IDs, cooperative rebalance, `__consumer_offsets` as an internal log topic.
- Idempotent producer: producer ID + epoch + per-partition sequence numbers.
- **Exit criteria:** kill consumers randomly; no duplicates visible after commit, no gaps.

### Phase 4 — Replication & Consensus (6–8 weeks) ← *hardest phase*
- Raft implementation (leader election, log replication, snapshots, membership change).
- Decision (SOLUTION.md ADR-020, supersedes ADR-004): Kafka-style ISR replication with a Raft-backed controller. Replication core (`src/repl/`) and controller state machine (`src/ctrl/`, ADR-021) done; controller host on Raft (`src/ctrl/ctrl_host.c`, ADR-022) done; replica log on `hx_log` (ADR-023) done; replica transport over TCP (ADR-024) done; broker-controller channel and replica manager with a 6-process-style TCP cluster test (ADR-025) done; cluster mode in `helixd` with a static topic table (ADR-026) done; runtime topic admin, membership change and replicated groups/idempotence remain.
- Follower fetch, high-watermark, leader epoch, truncation on divergence.
- `acks=0|1|quorum`, `min.insync.replicas`.
- **Exit criteria:** deterministic simulation tests (network partition, clock skew, disk stall) + Jepsen-style linearizability check on committed offsets.

### Phase 5 — Operations & Observability (2–3 weeks)
- Prometheus endpoint, latency histograms (HdrHistogram), per-partition lag metrics.
- Admin API/CLI (`helixctl`): create topic, describe, reassign, dump segment.
- Config hot-reload, graceful shutdown, rolling upgrade (protocol versioning).
- TLS (OpenSSL/BoringSSL, kTLS where available), SASL/PLAIN + SCRAM, ACLs.
- **Exit criteria:** runbook exists; chaos test script included.

### Phase 6 — Performance & Tiering (3–4 weeks)
- Profiling loop: `perf`, `bpftrace`, flamegraphs, cache-miss analysis; NUMA awareness.
- Hot-segment in-memory cache (the legitimate "cache" feature), readahead tuning.
- Batch compression (LZ4, zstd) with per-batch codec.
- Optional tiered storage (cold segments → object store).
- **Exit criteria:** published benchmark report vs. Kafka on identical hardware (methodology in SOLUTION.md §8).

### Phase 7 — Compatibility & Hardening (4+ weeks)
- Kafka wire-protocol subset adapter (ApiVersions, Metadata, Produce, Fetch, OffsetCommit, JoinGroup...).
- Fuzzing (libFuzzer/AFL++) on every parser; protocol-level corpus.
- Security review, threat model, CVE-style disclosure process.
- **Exit criteria:** `kcat` and a stock Kafka client library can produce/consume.

**Total:** ~26–34 weeks solo to v1.0. A realistic **MVP (Phases 0–3)** is ~10–13 weeks.

---

## 5. Milestones & Releases

| Release | Contents                       | Gate                                  |
|---------|--------------------------------|---------------------------------------|
| v0.1    | Phases 0–2: single-node broker | Crash-safety suite + 300k msg/s       |
| v0.3    | + Phase 3: groups, idempotence | Consumer chaos test                   |
| v0.5    | + Phase 4: replication         | Partition/Jepsen test                 |
| v0.8    | + Phase 5: ops/security        | Runbook + TLS/ACL                     |
| v1.0    | + Phase 6–7                    | Published benchmarks, fuzz-clean      |

---

## 6. Risk Register

| # | Risk                                           | Likelihood | Impact | Mitigation |
|---|------------------------------------------------|-----------|--------|------------|
| R1| Memory-safety bugs in C (UAF, overflow)        | High      | High   | ASan/UBSan/TSan in CI, fuzzing, strict ownership conventions, arena allocators, no raw `malloc` in hot paths |
| R2| Consensus bugs (subtle, rare)                  | High      | Critical| Follow Raft paper strictly, TLA+ spec of core, deterministic simulation, Jepsen-style tests |
| R3| fsync semantics / filesystem differences       | Med       | Critical| Document supported FS (ext4/XFS), test with `dm-flakey`/`dm-log-writes`, fsync parent dir on create/rename |
| R4| Scope creep ("stronger than Kafka")            | High      | High   | Non-goals list, phase exit criteria, measurable targets |
| R5| io_uring kernel-version drift/CVEs             | Med       | Med    | epoll backend always available; feature-detect at startup |
| R6| Benchmark claims not reproducible              | Med       | Med    | Scripted benchmark harness, pinned hardware profile, publish raw data |
| R7| Solo bus-factor                                | Med       | Med    | ADRs for every major decision, docs-as-code |

---

## 7. Engineering Practices (non-negotiable)

- **ADRs** (Architecture Decision Records) for any decision that is hard to reverse (see SOLUTION.md).
- **Definition of Done** for every PR: tests, sanitizers clean, docs updated, benchmark delta noted for hot-path changes.
- **Error handling:** every syscall checked; `errno` mapped to a typed error enum; no silent `EINTR`/short-write bugs.
- **Memory:** ownership documented per struct; arena per request; pools for fixed-size objects; zero `malloc` on the produce fast path after warmup.
- **Concurrency:** shared-nothing partitions pinned to cores; cross-core communication via SPSC ring buffers; locks only in cold paths.
- **Style:** C11, `-std=c11 -D_GNU_SOURCE`, opaque handles in headers, `static` by default, `const` correctness.
- **Testing pyramid:** unit → property-based → crash/fault-injection → simulation → soak (72 h) → chaos.

---

## 8. Suggested Repository Layout

```
helix/
├── PLAN.md
├── docs/
│   ├── ARCHITECTURE.md
│   ├── SOLUTION.md
│   ├── PROTOCOL.md
│   └── adr/
├── src/
│   ├── util/        # arena, ring, crc32c, varint, log, config
│   ├── storage/     # segment, index, log, recovery, retention
│   ├── net/         # event loop, epoll/io_uring backends, conn
│   ├── proto/       # wire codec, request handlers
│   ├── broker/      # topics, partitions, groups, coordinator
│   ├── raft/        # consensus
│   └── main.c
├── tools/           # helixctl, segment dump, bench client
├── tests/           # unit, property, fault-injection, sim
├── bench/
└── CMakeLists.txt
```

---

## 9. Immediate Next Steps (first 2 weeks)

1. Set up repo + CI with sanitizers (Phase 0).
2. Take the included `src/storage` starter, add unit tests, extend with time index.
3. Write the crash-recovery property test **before** adding any networking.
4. Write ADR-001 … ADR-003 (language standard, storage format, threading model).
5. Build a throwaway benchmark: how fast can the log alone append with `fsync=interval`? That number is your performance ceiling and tells you if targets in §3 are plausible.
