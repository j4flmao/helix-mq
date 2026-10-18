# Helix — Architecture

Status: Draft v0.1 · Audience: engineers implementing or reviewing Helix · Language: C11, Linux ≥ 5.10 (io_uring optional ≥ 5.15)

---

## 1. Overview

Helix is a distributed, partitioned, replicated **commit log**. Producers append records to a partition;
consumers read from an offset. The log is the source of truth; everything else (indexes, caches, consumer
positions) is derived or is itself stored as a log.

```
                 ┌──────────────┐        ┌──────────────┐
   Producers ───▶│  Broker A    │◀──────▶│  Broker B    │◀── Consumers
                 │ (P0 leader)  │  Raft  │ (P0 follower)│
                 └──────┬───────┘        └──────┬───────┘
                        │                       │
                   ┌────▼────┐             ┌────▼────┐
                   │ Segments│             │ Segments│
                   │ on disk │             │ on disk │
                   └─────────┘             └─────────┘
```

### 1.1 Design principles

1. **The log is the database.** Append-only, immutable segments; deletes happen by whole-segment removal or compaction into new segments.
2. **Shared-nothing, thread-per-core.** Each partition is owned by exactly one worker thread. No locks on the data path.
3. **Sequential I/O only** on the write path; zero-copy on the read path.
4. **Crash-safe by construction.** Every on-disk structure can be validated and repaired from the log alone.
5. **Backpressure everywhere.** Bounded queues, bounded memory, explicit load shedding.
6. **Observable by default.** Every queue, syscall class and request type has a metric.

---

## 1.5 Implementation Status

| Area | State |
|------|-------|
| Segmented log, CRC32C, sparse index, tail recovery, retention | **Done** — `src/storage/` |
| Group commit, fail-stop on fsync error | **Done** |
| Binary protocol (APIs 0-14: Ping, Produce, Fetch, Metadata, OffsetCommit/Fetch, group, producer and topic APIs), bounds-checked codec | **Done** — `docs/PROTOCOL.md` |
| Durable consumer-group offsets (internal `__consumer_offsets` log, replayed at start) | **Done** |
| epoll server: pipelining, backpressure, conn limit, partial I/O | **Done** — `src/net/server.c` |
| `helixd` daemon, `helixctl` client + benchmark | **Done** |
| Static analysis, sanitizers, fuzzing, kill -9 crash test, CI | **Done** |
| io_uring backend, thread-per-core sharding, sendfile fetch | Planned — Phase 2/6 |
| Consumer-group coordinator: join / heartbeat / leave / describe, session expiry, eager rebalance, generation fencing | **Done** — `src/broker/groups.c`, ADR-014 |
| Idempotent producer (producer id + per-partition sequence, window of 8, TTL expiry) | **Done** — `src/broker/producers.c`, ADR-015 |
| Topic create/delete API (atomic rename, offset tombstones, crash-safe cleanup) | **Done** — ADR-015 |
| Raft consensus core: PreVote, CheckQuorum, replication, commit rule, write-ahead storage callbacks, wire format; fault-injection simulator | **Done** as a library — `src/raft/`, ADR-016 |
| Raft on-disk log (`raft_wal`): CRC-framed records, torn-tail recovery, fail-closed on mid-file corruption, compaction | **Done** — `src/raft/raft_wal.c`, ADR-017 |
| Raft TCP transport (`raft_net`): one outbound connection per peer, hello handshake, bounded queues, reconnect with backoff; 3-node cluster test over loopback | **Done** — `src/raft/raft_net.c`, ADR-018 |
| Raft snapshots and log compaction: host-driven `hx_raft_snapshot`, `SNAPSHOT` message for lagging followers, durable in the WAL | **Done** — ADR-019 |
| Partition replication core (`src/repl/`): leader epochs, follower fetch, ISR, high watermark, divergence truncation; simulator with fault injection and mutation tests | **Done as a library** — ADR-020; integrated through replica_mgr and cluster.c (ADR-026) |
| Controller state machine (`src/ctrl/`): sessions, elections from the ISR, ALTER_ISR validation, snapshot/restore; drives the replication simulator | **Done as a library** — ADR-021; hosted on Raft by `ctrl_host` (ADR-022) |
| Controller host (`src/ctrl/ctrl_host.c`): Raft + controller, failure detector, snapshots, ready-leader rule; simulated on 3 nodes with WAL, crashes, partitions and broker churn | **Done as a library** — ADR-022 |
| Replica log on the real engine (`src/repl/repl_log.c`, `hx_log_truncate`): epoch-prefixed records, durable crash-safe truncation; differential test against the simulation model | **Done as a library** — ADR-023 |
| Replica TCP transport (`src/repl/repl_net.c`) over the Raft transport with its own hello magic; 3 brokers, real logs, restart and leader change over loopback | **Done as a library** — ADR-024 |
| Broker-controller channel (`src/ctrl/ctrl_net.c`) and replica manager (`src/repl/replica_mgr.c`); 3 controllers + 3 brokers over TCP with leader kill, restart, controller failover | **Done and integrated into `helixd`** — ADR-025 |
| Cluster mode in `helixd` (`src/broker/cluster.c`): static topic table, quorum-acked Produce, HW-bounded leader Fetch, ClusterMetadata; 3 real processes with kill -9 | **Done** — ADR-026 |
| Runtime topic admin on replicated topics, membership change, follower reads, idempotent/groups on replicated topics | Planned — Phase 4 (remaining) |
| TLS/auth, metrics endpoint, compaction, time index | Planned — Phase 5+ |

Known limits of the current build: single event-loop thread; IPv4 only; no auth/TLS; one record per
Produce; Fetch copies through userspace; sparse index is rebuilt in memory at open (cost grows with
log size). Standalone topics support create/delete; replicated topics are static. See
[the current limitations](LIMITATIONS.md) for the operational contract.

## 2. Component Map

| Layer         | Component            | Responsibility                                                      |
|---------------|----------------------|---------------------------------------------------------------------|
| Network       | `net/loop`           | Event loop per core (epoll or io_uring), connection lifecycle       |
| Protocol      | `proto/codec`        | Frame parsing/serialization, versioning, bounds checking            |
| Dispatch      | `broker/router`      | Maps (topic, partition) → owning core via lock-free SPSC rings      |
| Partition     | `broker/partition`   | Append, fetch, high-watermark, leader epoch                         |
| Storage       | `storage/log`        | Segmented log, index, recovery, retention, compaction               |
| Replication   | `raft/*`             | Election, log replication, snapshotting, membership                 |
| Coordination  | `broker/group`       | Consumer groups, offsets, rebalance                                 |
| Control plane | `broker/controller`  | Topic/partition metadata, assignment (Raft-replicated)              |
| Ops           | `ops/metrics, admin` | Prometheus endpoint, admin CLI, health checks                       |

---

## 3. Threading Model

```
          ┌─────────────────────── listener (SO_REUSEPORT) ───────────────────────┐
          │                                                                         │
   ┌──────▼──────┐   ┌─────────────┐   ┌─────────────┐          ┌────────────────┐
   │  Core 0     │   │  Core 1     │   │  Core N     │          │ Background pool │
   │ event loop  │◀─▶│ event loop  │◀─▶│ event loop  │◀────────▶│ fsync/compact/  │
   │ partitions  │   │ partitions  │   │ partitions  │   SPSC   │ retention/tier  │
   │ {P0,P4,...} │   │ {P1,P5,...} │   │ {P2,P6,...} │   rings  │                 │
   └─────────────┘   └─────────────┘   └─────────────┘          └────────────────┘
```

- **Worker (core) threads**: pinned (`pthread_setaffinity_np`), each runs one event loop and owns a disjoint set of partitions.
- **Connection affinity**: a connection lands on a core via `SO_REUSEPORT`. If a request targets a partition owned by another core, the request is forwarded through an SPSC ring (no shared locks); the response returns the same way. This costs one cache-line transfer, not a mutex.
- **Background pool**: runs slow, blocking work (compaction, retention deletes, tiering, `fsync` when using a blocking backend). Results are posted back to the owning core.
- **Rule:** *a partition's data structures are touched by exactly one thread.* Violations are bugs; TSan enforces this in CI.

---

## 4. Storage Engine (Phase 1 — implemented as starter code)

### 4.1 Directory layout

```
data/
└── <topic>-<partition>/
    ├── 00000000000000000000.log      # segment: records starting at base offset 0
    ├── 00000000000000000000.index    # sparse offset → file position
    ├── 00000000000000000000.timeindex
    ├── 00000000000000052431.log      # next segment, base offset 52431
    ├── ...
    ├── leader-epoch-checkpoint
    └── recovery-point-checkpoint
```

Segment file name = zero-padded **base offset** of its first record. Lookup by offset is a binary search over
segment base offsets, then the sparse index, then a short linear scan.

### 4.2 On-disk record format (little-endian)

```
Record frame
┌────────────┬────────────┬──────────┬────────────┬───────────┬──────────┬─────────┐
│ crc32c u32 │ length u32 │ magic u8 │ attrs u8   │ offset u64│ ts i64   │ payload │
└────────────┴────────────┴──────────┴────────────┴───────────┴──────────┴─────────┘
 crc covers: everything after the crc field (length … payload)
 length = bytes after the length field
```

- `magic` identifies the format version (allows evolution without a flag day).
- `attrs`: bits 0–2 compression codec, bit 3 = tombstone, bit 4 = batch-start.
- In the production format, frames are **batches** (many records per frame) to amortize CRC, syscalls and index work; the starter code uses one record per frame for clarity.
- **CRC32C** (hardware accelerated via SSE4.2 `crc32` / ARMv8 CRC) detects torn writes and bit rot.

### 4.3 Write path

```
append(record):
  1. assign offset = next_offset++
  2. encode frame into the partition's write buffer
  3. if segment would exceed max size → roll (new segment, fsync dir)
  4. pwrite(fd, buf) at tail position          (sequential)
  5. every N bytes → add sparse index entry
  6. durability policy:
       always   → fdatasync before ack (group-commit: one fsync covers many records)
       interval → background fsync every T ms
       never    → rely on OS (testing/benchmarks only)
  7. advance high-watermark (when replicated per §5), ack producer
```

Group commit is the key throughput lever for `fsync=always`: producers whose frames land in the same
event-loop tick share one `fdatasync`. **Implemented** (`hx_log_flush`, `hx_broker_commit`, server
phases below); measured on the dev sandbox, 64-deep pipelined produce went from ~4.1k msg/s
(fsync per record) to ~70k msg/s with the same durability contract.

#### Event-batch processing (implemented in `src/net/server.c`)

```
epoll_wait -> n ready events
  phase 1  for each ready connection: read, decode, execute  (responses buffered, NOTHING sent)
  phase 2  hx_broker_commit(): one fdatasync per dirty partition      <- the only durability point
             failure => fail-stop: exit without acknowledging anything from this batch
  phase 3  for each ready connection: send buffered responses, re-arm epoll interest
```

Acks therefore can never overtake durability, and cost is amortised over every request that arrived
together. Fail-stop on fsync failure is ADR-011.

### 4.4 Read path

```
fetch(offset, max_bytes):
  segment = bsearch(segments, offset)
  pos     = index_lookup(segment, offset)      # greatest indexed offset ≤ target
  scan forward from pos to exact offset        # bounded by index interval
  send up to max_bytes via sendfile()/splice   # zero-copy, no userspace copy
```

Consumers only read up to the **high-watermark** (committed offset), never uncommitted data.

### 4.5 Crash recovery

On startup (per partition):
1. Read `recovery-point-checkpoint`; segments entirely before it are trusted (fast path).
2. For the tail segment(s): scan frames from the start, validating `length` bounds and CRC.
3. At the first invalid frame: `ftruncate` to the last valid frame boundary; discard index entries beyond it.
4. Rebuild index if missing/corrupt.
5. Set `next_offset` = last valid offset + 1.

Invariant: **after recovery, the log is a prefix of the sequence of acknowledged-or-in-flight appends.** No holes, no corrupt records.

### 4.6 Retention & compaction

- **Delete retention:** drop whole segments older than `retention.ms` or when total size exceeds `retention.bytes`. Never delete the active segment.
- **Compaction:** background thread rewrites cleanable segments keeping the latest record per key; output goes to a new segment and is atomically swapped (`rename` + dir `fsync`). Tombstones are retained for `delete.retention.ms`.

---

## 5. Replication & Consensus

### 5.1 Model

- Each partition has a **replica set** (default 3). One **leader**, rest **followers**.
- Leadership and metadata are decided by Raft (partition data uses ISR replication, see ADR-020).
- **High-watermark (HW)** = highest offset replicated to a quorum. Consumers read ≤ HW.
- **Leader epoch** increments on each leadership change; stored in every batch header and checkpointed. Followers use it to truncate divergent suffixes safely.

### 5.1.1 Raft core (implemented, `src/raft/`)

**Snapshots.** The state machine belongs to the host. After applying entries up to `i` it calls
`hx_raft_snapshot(i, image)`; Raft persists the image (`save_snapshot`), forgets entries `<= i` and remembers
`(base, base_term)` so the consistency check at the snapshot point still works. A leader whose follower needs
an entry below `base` sends one `SNAPSHOT` message with the whole image; the follower keeps its log suffix only
if its entry at the snapshot index has the same term, otherwise it drops the whole log, jumps its commit
index to the snapshot index and tells the host (`hx_raft_take_installed_snapshot`) to reload. An append whose
batch starts below `base` is trimmed, because those entries are committed. Limits: a snapshot travels in one
message (so it must fit the 8 MiB frame limit), it is resent on every heartbeat until acknowledged, and the
host decides when to snapshot (no automatic trigger yet).

**Transport (`raft_net.c`).** IPv4, non-blocking, own epoll instance, single owner thread. Each node keeps
one outbound connection per peer and writes only on those; accepted connections are read-only, so there is
no duplicate-connection tie-break. First frame is a hello (magic, sender id, cluster size); frames are
`u32 length | payload`, limited to 8 MiB. A bad hello, an oversized length or a mid-frame close drops the
connection and counts in `bad_peers`. Queues are bounded per peer (4 MiB) and a broken connection discards
its queue; Raft tolerates loss. Reconnect backoff 100 ms to 2 s. Limits: no TLS or authentication (any host
that can reach the port and knows the hello can inject messages), at most 64 inbound connections with no
idle timeout, so a client that connects and says nothing holds a slot.

**Durable log (`raft_wal.c`).** One file per Raft group: 8-byte header (`HXRW`, version), then records
`len | crc32c | type | payload`, type HARD (term, vote) or ENTRIES (first index, entries). Replaying an ENTRIES
record truncates the log to `first-1` and appends, which is exactly Raft's overwrite semantics, so the file
is append-only on disk. Recovery: an incomplete last record, a bad CRC at end of file, or a zero-filled tail
is a torn write and is cut off; a bad record followed by more non-zero data means real corruption and
`open` fails instead of guessing. A failed write or fsync poisons the handle (never retried). Compaction
rewrites live state to a temp file, renames and fsyncs the directory; `open` does it when the file is over
1 MiB and mostly dead. The whole log is still held in RAM.

A pure state machine (no sockets, threads, clock or globals) so a whole cluster can be driven
deterministically by a test. The host feeds `tick(now)` and `step(msg)`, sends what `take_msgs()` returns
and applies entries up to `commit_index`.

- **Election:** randomized timeouts; **PreVote** (a node only starts a real election if a majority would
  grant it, so a partitioned node cannot inflate terms and disrupt a healthy leader when it returns);
  **CheckQuorum** (a leader that has not heard from a majority within an election timeout steps down).
- **Replication:** `AppendEntries` with consistency check, fast backtracking through a conflict hint, batch
  limits, idempotent handling of duplicated or reordered messages (a late shorter copy never truncates).
- **Commit rule:** a leader counts replicas only for entries of its own term (Raft 5.4.2) and appends a no-op
  on election so earlier entries commit. A follower never rewrites a committed entry; if a message would,
  the node fails stop instead.
- **Durability:** all state goes through `hx_raft_storage` (`save_hard_state`, `append`, `sync`). `sync` runs
  before any call returns, and messages are handed out only after it, so a vote or an acknowledgement can
  never leave the node before the state it depends on is stable. A storage error makes the instance
  `failed` (no further output), as in ADR-011.
- **Limits of this version:** fixed membership of at most 32 nodes, whole log in memory, no snapshots, no
  read-index / lease reads, one `fsync` per call (no batching yet).
- **Verification:** `tests/test_raft.c` simulates 1-5 nodes with message loss, duplication, reordering,
  partitions, crashes that discard unsynced data, an adversary that power-cycles a node right after it
  votes, and tight timeouts that provoke split votes. After every event it checks election safety, state-machine
  safety, leader completeness and log matching, and at the end of each scenario that the healed cluster
  elects a leader and commits. `make raft-stress` runs thousands of scenarios. The checker itself was
  validated by mutation: removing vote persistence, `sync`, the up-to-date check, the current-term commit
  rule, the no-op, CheckQuorum, the conflict check and others each makes the suite fail.

### 5.1.2 Partition replication core (implemented, `src/repl/`)
Pure state machine, no I/O: the host supplies a log vtable and moves messages. `hx_repl_set_state` installs
the controller's (epoch, leader, ISR, version); the leader appends, fsyncs, answers FETCH with records and the
high watermark, and proposes ISR shrink/expand through ALTER_ISR. Followers fetch, verify the response against
(epoch, leader, offset), and truncate on DIVERGED. Storage failure or a protocol violation sets a fail-stop
flag. `tests/test_repl.c` simulates loss, duplication, reordering, partitions, zombie leaders, crashes that
drop unsynced writes and stale controller messages, and checks safety (no committed loss, leader completeness,
log matching, monotone HW) and liveness after healing. A fuzz harness covers the codec and state machine.

### 5.1.3 Controller (implemented as a library, `src/ctrl/`)
Commands CREATE, BROKER_UP, BROKER_DOWN and ALTER_ISR are applied in Raft-log order. BROKER_UP returns a
session id (the log index). Per partition the controller holds replicas, epoch, leader, ISR and ISR version;
`hx_ctrl_generation` tells the host when to push state again. See ADR-021 for the rules and for what the
host must still do (detect failures, propose, push, snapshot with `hx_raft_snapshot`).

### 5.1.4 Controller host (implemented as a library, `src/ctrl/ctrl_host.c`)
`hx_chost` runs `hx_raft` + `hx_ctrl` as one unit. Inputs: time, Raft messages, broker hello / ALTER_ISR
requests. Outputs: Raft messages via a callback, controller state via `hx_chost_ctrl()`. See ADR-022.

### 5.2 Produce with `acks`

| acks   | Ack sent when                    | Durability                         |
|--------|----------------------------------|------------------------------------|
| 0      | immediately                      | none                               |
| 1      | leader appended (+ fsync policy) | lost if leader dies before replicate|
| quorum | majority replicated              | survives minority failures         |

### 5.3 Failure handling

- **Follower slow/down:** leader keeps going if quorum is intact; follower catches up by fetching from its last offset (or installs a snapshot if too far behind).
- **Leader down:** election timeout → candidate with most up-to-date log wins (Raft safety).
- **Split brain:** prevented by quorum + epoch fencing; stale leaders cannot advance HW, and their produce requests with an old epoch are rejected.

---

## 6. Wire Protocol (v1)

Length-prefixed binary frames over TCP (optionally TLS).

```
Request:  | len u32 | api_key u16 | api_version u16 | correlation_id u32 | client_id (str) | body |
Response: | len u32 | correlation_id u32 | error_code u16 | body |
```

| API            | Purpose                                         |
|----------------|-------------------------------------------------|
| ApiVersions    | Capability negotiation                          |
| Metadata       | Topic/partition/leader discovery                |
| Produce        | Append batch to a partition                     |
| Fetch          | Read from offset (long-poll, `min_bytes`, `max_wait`) |
| OffsetCommit/Fetch | Consumer group positions                    |
| JoinGroup/SyncGroup/Heartbeat/LeaveGroup | Group membership      |
| CreateTopic/DeleteTopic | Admin                                  |

Rules: every length is bounds-checked before allocation; max frame size configurable (default 100 MB); unknown
`api_version` returns `UNSUPPORTED_VERSION` rather than guessing. Pipelining allowed; responses return in request order per connection.

---

## 7. Consumer Groups

**Implemented today:** offset storage only. Each OffsetCommit appends one record
`u16 glen | group | u16 tlen | topic | u32 partition | u64 offset` to the internal log
`__consumer_offsets-0` through the normal append + group-commit path. The broker keeps a hash table
(group, topic, partition) -> offset and rebuilds it at startup by replaying that log (later records win).
Startup **fails closed** if the log is unreadable or the table limit is too small for the stored state,
because serving missing offsets would silently make consumers re-read or skip data. Known limits: the
internal log is never compacted (replay time and disk grow with the number of commits; compaction is
Phase 5) and there is a single internal partition (one core owns it under ADR-003).

**Group coordinator (`src/broker/groups.c`).** One in-memory state machine per group, owned by the
event-loop thread, so it needs no locks.

- *States:* stable <-> rebalancing. A join of a new member, a subscription change, a leave or a session
  expiry starts a rebalance; the generation increments only when every member has rejoined.
- *Barrier without blocking:* JoinGroup always answers immediately (`stable = 0` until the barrier
  completes), so the single-threaded server never parks a connection. Clients poll.
- *Stragglers:* the rebalance has a deadline of `now + longest session_ms`; members that have not rejoined by
  then are evicted, which is what lets a group recover after a consumer is killed with SIGKILL.
- *Assignment:* per topic, partitions in order are dealt round-robin to the subscribed members sorted by id
  (deterministic, so it is unit-testable). The partition list comes from the same scan as Metadata.
- *Fencing:* GroupCommit validates (member, generation, assignment) before writing the offset.
- *Time:* a clock callback is injected (fake clock in unit tests). Expiry runs lazily, throttled to 100 ms, on
  every group request, and the server calls `hx_broker_tick` once per loop (epoll timeout 200 ms), so an idle
  broker still expires members.
- *Limits:* `max_groups`, `max_group_members`; exceeding them returns RESOURCE_LIMIT.
- *Not persisted:* membership is rebuilt by clients rejoining after a restart. Offsets are durable.

**Idempotent producer (`src/broker/producers.c`).** Hash table pid -> producer, each with up to 256 streams
`(topic, partition) -> {next_seq, ring of the last 8 offsets}`. `check()` classifies a sequence as
append / duplicate / out-of-order; the broker appends only on *append* and calls `record()` after the log
append succeeded, so a failed append never consumes a sequence. Ids are `(boot_ms << 20) | counter`.
Idle producers are removed by `hx_broker_tick` after the TTL. Deleting a topic drops its streams.

**Topic administration.** CreateTopic opens partitions `0..n-1` (rollback on failure) and tells the group
coordinator (`hx_groups_topic_changed`) so subscribed groups rebalance. DeleteTopic order: (1) write and
fsync a tombstone (`offset = UINT64_MAX`, impossible for a real commit) for every committed offset of the topic
into the internal log, (2) drop the in-memory entries, (3) close and rename each partition directory to
`.deleting.*`, (4) fsync the data directory, (5) remove the files, (6) drop producer streams, (7) rebalance
subscribed groups. A crash between 1 and 3 loses offsets of a topic that still exists (consumers re-read,
at-least-once); a crash after 3 is finished by the startup purge of `.deleting.*`. Offsets can therefore never
outlive their topic.

**Designed, not yet built:**

- Cooperative incremental rebalance (members keep partitions that do not move) instead of today's stop-the-world eager one.
- Coordinator placement for a cluster (one broker per group, chosen by hash of group id) and replicated group state.
- Offsets in a *compacted* internal log (today the log only grows).
- Durable idempotent-producer state (producer snapshots rebuilt from the log, epochs for fencing zombie producers) so exactly-once survives a broker restart.
- A persisted per-topic partition count, so Produce to a partition outside it can be refused.

---

## 8. Memory Management

- **Arena per request** — freed in O(1) after response.
- **Slab/pool allocators** for connections, buffers, index nodes; sizes fixed at startup where possible.
- **No `malloc` on the produce fast path** after warm-up (checked by a test that interposes `malloc`).
- **Page cache is the read cache** by default; optional hot-segment cache (Phase 6) uses explicit memory budgets and `madvise`.
- `mlock` option for index files to avoid page-fault latency spikes.

---

## 9. Error Handling & Invariants

- Every syscall result is checked; `EINTR` retried; short `pwrite`/`write` handled in a loop.
- Error type: `helix_err_t` enum, never raw `errno` across module boundaries.
- Disk-full (`ENOSPC`) puts the partition in read-only mode; process does **not** crash.
- Corruption detected on read → fail that partition, alert, continue serving others.
- Debug builds enable internal invariant checks (`HELIX_ASSERT`) that compile out in release.

**Key invariants (tested):**
1. Offsets in a partition are dense and strictly increasing.
2. A record acknowledged with `fsync=always` is present after any crash.
3. No reader ever observes an offset > high-watermark.
4. After recovery, no frame fails its CRC.

---

## 10. Observability

- **Metrics (Prometheus):** produce/fetch request rate & latency histograms, bytes in/out, fsync latency, segment count, consumer lag per group/partition, replication lag, queue depths, allocator stats.
- **Logs:** structured JSON lines with request/correlation IDs; levels configurable at runtime.
- **Tracing hooks:** optional USDT probes (`bpftrace`/`perf` friendly) on append, fsync, fetch.
- **Health:** `/healthz` (liveness) and `/readyz` (partitions recovered, quorum reachable).

---

## 11. Security

- TLS 1.3 (OpenSSL/BoringSSL); kTLS to preserve zero-copy where supported.
- Authentication: SASL/PLAIN, SCRAM-SHA-256; mTLS option.
- Authorization: ACLs per (principal, resource, operation).
- Hardening: fuzzed parsers, `-fstack-protector-strong`, `-D_FORTIFY_SOURCE=2`, full RELRO, PIE, seccomp-bpf allowlist, privilege drop after bind.
- Threat model: untrusted network clients; trusted disk; trusted peer brokers (authenticated).

---

## 12. Testing Strategy

| Layer              | Technique                                                          |
|--------------------|--------------------------------------------------------------------|
| Unit               | cmocka/Unity per module                                            |
| Property-based     | Random op sequences vs. an in-memory reference model              |
| Crash / fault      | `kill -9` loops, `dm-flakey`, `dm-log-writes`, injected short writes/EIO |
| Fuzzing            | libFuzzer on codec, frame decoder, config parser, index loader     |
| Simulation         | Deterministic network/disk/clock simulator for Raft               |
| Distributed        | Jepsen-style: partitions, pauses, clock skew; check linearizability of committed offsets |
| Performance        | Scripted bench harness; HdrHistogram; regression gates in CI      |
| Soak               | 72 h mixed workload with chaos                                     |

---

## 13. Capacity & Tuning Notes

- Filesystem: **XFS or ext4** (`noatime`); avoid NFS.
- Disk: NVMe preferred; one log dir per device for parallelism.
- Kernel: raise `vm.max_map_count`, `fs.file-max`, `net.core.somaxconn`; set `vm.dirty_*` for steady writeback; disable transparent hugepages for latency.
- Sizing: segment size 256 MB–1 GB; index interval 4 KB; partitions per core ~10–100.

---

## 14. Future Work

Tiered storage to object stores, geo-replication, tenant quotas/QoS, SQL-ish stream filters, WASM-based server-side transforms.
