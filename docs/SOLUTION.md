# Helix — Solution Design & Decision Records

This document explains **why** Helix is built the way it is. `ARCHITECTURE.md` says *what*; this says *why, and what we gave up*.

---

## 1. Problem Statement

Teams need an ordered, durable, replayable message log with high throughput and predictable latency. Existing options
(Kafka, Pulsar, Redpanda) either carry heavy runtime/operational cost (JVM, ZooKeeper/KRaft tuning) or are not
easily extensible/inspectable. Helix explores the design space of a **C, Linux-native log broker** with a small
operational footprint and tight tail latency.

### Requirements summary
- **Functional:** topics/partitions, produce/fetch by offset, consumer groups, retention, compaction, replication.
- **Non-functional:** p99 < 5 ms produce (single node), no acked-data loss on crash, < 100 MB broker memory, single static binary.
- **Constraints:** Linux only; C11; solo/small-team maintainable.

---

## 2. Alternatives Considered

| Option                         | Pros                                  | Cons                                          | Decision |
|--------------------------------|---------------------------------------|-----------------------------------------------|----------|
| Use Kafka as-is                | Mature, huge ecosystem                | JVM/GC, ops complexity, not a learning goal   | Baseline for benchmarks |
| Fork Redpanda (C++)            | Thread-per-core already               | Different language, licensing, huge codebase  | Rejected |
| Redis Streams                  | Simple, fast                          | Memory-bound, weak durability/replication     | Rejected |
| **New C log broker (Helix)**   | Full control, minimal footprint       | Memory-safety risk, long road to maturity     | **Chosen** |
| Rust implementation            | Memory safety                         | Not the requested language                    | Noted as future option |

---

## 3. Architecture Decision Records

### ADR-001: Language and standard — C11 on Linux
- **Context:** Requirement is C; goal is predictable latency and tiny footprint.
- **Decision:** C11, `-D_GNU_SOURCE`, Linux-only syscalls allowed.
- **Consequences:** + no GC, direct kernel access. − memory-safety bugs are our responsibility → mandatory ASan/UBSan/TSan CI, fuzzing, arena allocators, strict ownership docs.

### ADR-002: Append-only segmented log as the storage primitive
- **Context:** Need sequential writes, simple recovery, cheap retention.
- **Decision:** Fixed-max-size segments named by base offset; sparse index; immutable once rolled.
- **Alternatives:** LSM-tree (RocksDB-style) — unnecessary write amplification for a pure log; B-tree — random I/O.
- **Consequences:** + sequential I/O, trivial retention (delete a file), easy recovery by tail scan. − point lookups by key need compaction/secondary structures.

### ADR-003: Thread-per-core, shared-nothing partitions
- **Context:** Locks and cache-line bouncing dominate tail latency at high core counts.
- **Decision:** One event loop per core; each partition owned by one core; cross-core messages via SPSC rings.
- **Alternatives:** Thread pool + shared structures with mutexes (simpler, worse tails); one process per core (heavier IPC).
- **Consequences:** + deterministic latency, TSan-verifiable ownership. − hot partitions can't use more than one core; need rebalancing of partition→core mapping.

### ADR-004: Replication — Raft-per-partition-group vs. controller + ISR (superseded by ADR-020)
- **Context:** Need replicated, consistent logs with leader election.
- **Options:**
  - **A. Raft per partition** (like TiKV/Redpanda): uniform, well-specified, strong safety. Cost: per-partition election/heartbeat overhead; mitigate with multi-raft batching.
  - **B. Kafka ISR + Raft-backed controller:** cheaper per-partition, more moving parts, subtler failure modes.
- **Decision:** **A (multi-Raft with batched heartbeats)** for v1 — fewer special cases, easier to prove correct with simulation testing.
- **Revisit when:** partition counts exceed ~100k per cluster and heartbeat overhead becomes measurable.
- **Update:** revisited early, after the Raft core existed; see ADR-020. Raft stays for cluster metadata.

### ADR-005: Durability policy is explicit and per-topic
- **Decision:** `fsync = always | interval(ms) | never`, plus `acks = 0 | 1 | quorum`. Default: `interval=200ms`, `acks=quorum`.
- **Rationale:** `fsync` per record caps throughput at disk IOPS; group commit recovers most of it. Users choose the point on the durability/latency curve knowingly; defaults are safe via replication.
- **Consequence:** documented failure matrix (see §6) so nobody assumes more than they have.

### ADR-006: I/O backend abstraction — epoll default, io_uring optional
- **Context:** io_uring offers fewer syscalls and async file I/O but has a rapidly evolving kernel surface and past security issues.
- **Decision:** Define `io_backend` interface; ship `epoll` + blocking-thread file I/O as the reference backend; add `io_uring` behind runtime feature detection.
- **Consequences:** + portable across Linux kernels, safe fallback. − two code paths to test.

### ADR-007: Zero-copy fetch via `sendfile`/`splice`
- **Decision:** Plaintext consumers are served directly from page cache to socket.
- **Trade-off:** TLS defeats `sendfile` unless kTLS is available → documented, kTLS path in Phase 5.

### ADR-008: Record framing with CRC32C and length-prefix; batches in production format
- **Decision:** CRC32C over header+payload; torn-write detection by CRC and length bounds.
- **Why CRC32C not CRC32:** hardware instruction on x86 (SSE4.2) and ARMv8; much faster; same detection power for our use.
- **Consequence:** recovery = scan until first invalid frame, truncate.

### ADR-009: Metadata/control plane replicated with Raft (single group)
- **Decision:** Topic/partition/assignment state lives in a dedicated Raft group; data partitions follow assignments.
- **Alternative:** gossip (eventual) — rejected; assignments need linearizable agreement.

### ADR-010: Compatibility strategy — native protocol first, Kafka subset later
- **Decision:** Design native protocol for our invariants; later add an adapter for a Kafka API subset so existing clients work.
- **Risk:** adapter semantic mismatches; limit to documented, tested APIs.

### ADR-011: Fail-stop on fsync failure
- **Context:** On Linux, after `fsync`/`fdatasync` returns an error the kernel may mark the failed
  pages clean; a retry can then return success although the data never reached disk (the
  "fsyncgate" class of bugs). Continuing to serve would acknowledge records that are not durable.
- **Decision:** a failed group commit stops `helixd` (exit status 1) without sending any acknowledgement
  for the batch. On restart, tail recovery re-validates the log.
- **Alternatives:** retry the fsync (unsafe, see above); mark the partition read-only and keep serving
  others (viable later, needs per-partition fault state and operator tooling).
- **Consequence:** one bad disk takes the node down — correct for a durability-first system, and the
  right behaviour once replication exists (a replica takes over).

### ADR-012: Two-phase event batches for group commit
- **Decision:** execute all ready requests, commit once, then send all responses (see
  ARCHITECTURE §4.3). Durability is a property of the *batch boundary*, not of individual requests.
- **Why not fsync per request on a thread pool:** more threads, more latency variance, no gain over
  batching on the same core; batching also needs no locks (ADR-003).
- **Consequence:** under light load a request pays one full fsync (~0.3 ms here); under heavy load the
  cost per request approaches zero. Measured: 4.1k -> 70k msg/s at pipeline depth 64.

### ADR-013: Consumer offsets live in an internal log of the same engine
- **Context:** consumers need a durable position per (group, topic, partition).
- **Decision:** append every commit to the reserved topic `__consumer_offsets` using the storage engine,
  keep the latest value per key in an in-memory table, and rebuild the table by replaying the log at startup.
  User topics may not start with `__`, so clients cannot read or forge internal state.
- **Alternatives:** a separate key-value file (a second durability mechanism to get right, no replication
  story); one file per group rewritten with rename (fine for small state, poor for many groups, no batching).
- **Why this one:** commits inherit group commit, crash recovery, CRC and (later) Raft replication for free,
  and the same fault-injection tests already cover them.
- **Cost:** the log grows without bound until compaction exists; startup replay is O(commits). Both are
  tracked for Phase 5. Fail-closed startup trades availability for never serving wrong offsets.

### ADR-014: Eager, non-blocking rebalance with in-memory group state
- **Context:** several consumers must split partitions without overlap, survive a member dying, and never
  let a zombie overwrite a successor's offsets. The server is single-threaded (ADR-003), so a request
  that waits for other members would stall everything.
- **Decision:** (1) *eager* rebalance: every change stops the group; all members rejoin before the next
  generation; (2) the barrier is *polled*, JoinGroup returns immediately with `stable = 0`; (3) assignment is
  computed by the broker, round-robin per topic over id-sorted members; (4) a rebalance deadline of the longest
  session evicts members that never come back; (5) commits are fenced by (member, generation, assignment);
  (6) group state is memory-only, committed offsets stay durable (ADR-013).
- **Alternatives:** parked JoinGroup requests like Kafka (needs per-connection deferred responses and timers in
  the server loop, more code for no gain at this stage); client-side assignment with SyncGroup (flexible
  strategies, but two more APIs and a leader election among clients); cooperative incremental rebalance
  (fewer pauses, needs revocation rounds and per-partition ownership transfer, so it is deferred); persisting
  groups (needs replication to be worthwhile, and clients already know how to rejoin).
- **Consequences:** + simple, deterministic, fully unit-testable with a fake clock, no locks. − every
  membership change pauses the whole group for roughly one polling round; − polling adds up to a poll interval
  of latency to a rebalance; − after a broker restart all members must rejoin (they get `UNKNOWN_MEMBER`);
  − partitions created after a rebalance are unassigned until the next one; − delivery is at-least-once: a
  record fetched but not yet committed when a member dies is redelivered to its successor.

### ADR-015: Idempotent producer with in-memory sequence state; topic delete via rename + tombstones
- **Context:** a producer that retries after a lost acknowledgement must not duplicate records, and topics
  need an explicit lifecycle (create all partitions up front, delete including offsets).
- **Decision:** (1) producers get an id from InitProducer and number records per (topic, partition); the broker
  appends only the next sequence and answers a retry from a ring of the last 8 offsets; (2) the state is memory-only,
  and ids embed the boot time so a restart yields `UNKNOWN_PRODUCER` rather than silently accepting stale state;
  (3) DeleteTopic writes durable offset tombstones first, renames directories out of the namespace
  atomically, and finishes removal at startup if interrupted; (4) topic changes trigger group rebalances.
- **Alternatives:** persist producer state by writing (pid, seq) into every record (a format change on the hot
  path, and recovery has to rebuild the table from segments, which is the right long-term design and is
  tracked); a bigger window (more memory per stream for little gain at pipeline depth <= 8); a per-topic
  metadata file (needed to reject out-of-range partitions, deferred); `rm -rf` in place (not crash-safe).
- **Consequences:** + no duplicates from client retries inside one broker lifetime, cheap hot path (a hash
  lookup and a ring slot); + delete is crash-safe and offsets never outlive the topic. − exactly-once does not
  survive a broker restart (the producer must re-init and may re-send in-flight records); − at most 8 records
  can be in flight per stream and be retried; − no producer epochs, so two processes sharing an id are not fenced;
  − CreateTopic does not bound the partition count.

### ADR-016: Raft as a pure, simulation-tested library with a write-ahead storage interface
- **Context:** consensus bugs are subtle and rare (R2). A Raft entangled with sockets and threads can only be
  tested by sleeping and hoping.
- **Decision:** implement Raft as a deterministic state machine (`tick`, `step`, `propose`, `take_msgs`) that
  persists through callbacks and syncs before returning. Include PreVote and CheckQuorum from the start. Test
  it with a simulator that injects network and crash faults and asserts the Raft safety properties after every
  event, and validate the simulator by deliberately breaking the implementation (mutation checks).
- **Alternatives:** wire Raft straight into the epoll loop (not testable under faults); use an existing C
  library such as willemt/raft or NuRaft-style code (dependency, different storage model, and the project's
  purpose is to build this); Kafka-style ISR (ADR-004 chose Raft).
- **Consequences:** + safety properties are checked on millions of events per run; + the same core serves
  a single metadata group now and per-partition groups later; + fuzzable decoder. − one `fsync` per call until
  batching is added; − snapshots, membership change, on-disk log and transport are still to do, so there is no
  replicated broker yet; − the simulator proves the algorithm, not the final integration (that needs the
  broker-level partition test planned for the end of Phase 4).

### ADR-017: A dedicated write-ahead log for Raft instead of reusing the topic log
- **Context:** the topic log (ADR-001) is append-only, while Raft must discard an uncommitted suffix when a new
  leader overwrites it, and must persist term/vote atomically with the entries.
- **Decision:** a separate small WAL (`raft_wal`) whose records are idempotent to replay (an ENTRIES record
  truncates, then appends). Torn tails are repaired, mid-file damage fails closed, fsync failure is
  fail-stop. Verified by exhaustive byte-cut and bit-flip tests and by running the Raft simulator on real
  WAL files, where a crash cuts the file to its last synced size (sometimes keeping a torn prefix of the
  unsynced bytes or a zero-filled extension).
- **Alternatives:** reuse the segment log with truncate support (couples two formats and index files to a
  rarely used operation); per-entry files (slow, many inodes); LMDB/RocksDB (dependency).
- **Consequences:** + small, auditable, testable; − whole log in RAM, no snapshots yet so the file only
  shrinks through compaction of overwritten data; − fdatasync of the file but the tests skip the real syscall
  for speed, so true power-loss behaviour of the filesystem is assumed, not demonstrated.

### ADR-018: One-directional TCP connections for Raft traffic
- **Context:** two nodes that dial each other at the same time end up with two connections and need a
  tie-break rule, which is a classic source of races.
- **Decision:** a node only writes on connections it opened and only reads on connections it accepted.
  Messages are best effort with bounded queues; a dead connection drops its queue.
- **Alternatives:** one shared bidirectional connection with a node-id tie-break (fewer sockets, subtle
  races); UDP (no ordering or flow control, size limits for batches); gRPC-style library (dependency).
- **Consequences:** + simple, no shared state between directions; + loss handling is Raft's job; − twice
  the sockets; − no encryption or authentication yet, so the port must be on a trusted network.

### ADR-019: Host-driven, single-message Raft snapshots
- **Context:** the Raft log was held entirely in RAM and on disk forever. Only the state machine knows what a
  snapshot contains, and chunked InstallSnapshot adds a transfer state machine on both sides.
- **Decision:** Raft treats the image as opaque bytes. The host triggers compaction; a lagging follower gets
  the image in one message. The WAL stores the latest image as a record and applies the same
  "keep the suffix only if the term at the snapshot index matches" rule as Raft, so memory and disk cannot
  disagree. Verified by running the simulator with a hash-chain state machine: every applied entry and every
  installed or restored image must match the globally committed history; 11 deliberately injected bugs in
  the snapshot paths were all caught.
- **Alternatives:** chunked transfer with offsets (needed for multi-GB state, left for later); Raft deciding
  when to snapshot by log size (needs a size callback and policy); no snapshots, rely on log retention.
- **Consequences:** + small, testable; − snapshot size is capped by the frame limit; − a slow follower makes
  the leader resend the image every heartbeat; − the host must keep the image consistent with applied entries
  (Raft cannot check it).

### ADR-020: Kafka-style partition replication (ISR) with a Raft-backed controller
- **Context:** ADR-004 chose Raft per partition. Having built the Raft core, its costs are concrete: it keeps the
  retained log in RAM and copies entries between buffers, so it fits metadata, not high-throughput data.
- **Decision:** partition data is replicated by `src/repl/`: one leader per epoch, followers pull with FETCH,
  the in-sync set (ISR) decides the high watermark. The controller (to be built on the existing Raft) assigns
  (epoch, leader, ISR, isr_version). The leader fsyncs before counting itself; HW = min of log ends over
  ISR ∪ pending proposed ISR ∪ self; ISR changes are proposed by the leader (ALTER_ISR) and take effect only
  when the controller approves them. Followers truncate on divergence using leader-epoch end offsets
  (KIP-101/320 style) and fail-stop if truncation would go below the HW.
- **Rules found by simulation (not by reasoning):** ISR expansion requires follower end ≥ max(HW, epoch start),
  because HW is volatile after a restart; the leader forgets follower positions at each epoch; HW waits for a
  member of a pending ISR.
- **Contract:** the host must feed controller state only from a session opened after the replica instance was
  created; delayed messages from a previous life would otherwise look like a newer state.
- **Alternatives:** Raft per partition (ADR-004); chain replication.
- **Consequences:** + sequential I/O, one write per replica, f+1 replicas tolerate f failures; − subtler failure
  modes than Raft, mitigated by a fault-injecting simulator with invariants and 18 mutation tests; − the
  controller, replica transport and binding to `hx_log` are not built yet.

### ADR-021: Controller as a deterministic command state machine with broker sessions
- **Context:** ADR-020 needs someone to assign epochs, leaders and ISR changes. That decision must survive
  controller failover and must be testable without sockets or clocks.
- **Decision:** `src/ctrl/` is a pure state machine. Four commands (CREATE, BROKER_UP, BROKER_DOWN, ALTER_ISR)
  are replicated through Raft and applied in log order. The host on the Raft leader is the failure detector
  and proposes BROKER_DOWN; the state machine itself has no clock. BROKER_UP assigns the broker a session id
  equal to the Raft index, which makes ids unique and increasing without extra state. State is pushed to a
  broker tagged with its session; a broker accepts it only if it matches the session it registered in this
  process life. That implements the contract written in `repl.h`. Elections pick only from ISR ∩ alive and
  drop dead members from the ISR; with no candidate the partition stays leaderless (no unclean election).
  ALTER_ISR needs the leader's live session, the current epoch and the current ISR version.
- **Verification:** `tests/test_repl.c` now uses this module as its controller. Replicas register through a
  lossy network, heartbeat, get marked down by timeout and re-register; the live instance is periodically
  rebuilt from its snapshot and every command is also applied to a twin that must agree. 2007 scenarios, 4.5M
  committed records, no violation. Mutations: removing the session filter, the unclean-election guard, or
  both the epoch and version fences on ALTER_ISR each lose committed data. Removing only one of epoch or
  version, the session check on ALTER, or the dead-broker filter is not visible at cluster level (defence in
  depth; the controller's own unit tests catch each of them).
- **Alternatives:** wall-clock leases inside the state machine (not deterministic); leader election by the
  brokers themselves (no single authority, harder to reason about).
- **Consequences:** + replayable, snapshot-able, fuzzable; − failure detection quality depends on the host;
  − the Raft integration (proposing, applying, pushing state) and the controller's own failover under load
  are not built yet. A controller leader change loses heartbeat timestamps, so brokers get a fresh grace period.

### ADR-022: Controller host: Raft + controller + failure detector in one pure module
- **Context:** ADR-021's state machine needs a replicated log, a failure detector and a rule for when a
  node may answer brokers.
- **Decision:** `hx_chost` (src/ctrl/ctrl_host.c) owns one `hx_raft` and one `hx_ctrl`, applies committed
  entries in order, takes a Raft snapshot every N applied entries and rebuilds the controller when the
  leader installs one. It does no I/O: Raft messages leave through a `send` callback. Only a *ready* leader
  answers brokers (leader, applied everything committed, has committed an entry of its own term). Hello
  messages are heartbeats; silence for `down_ms` becomes a BROKER_DOWN proposal that names the session it
  refers to, so a late or duplicated proposal cannot kill a newer registration. A new ready leader restarts
  every broker's clock, so failover never marks a healthy broker down. Registration carries a per-process
  nonce; the controller returns a session only for the nonce it was asked about, and a duplicate UP with the
  same nonce is a no-op. The broker therefore never adopts a session created for a previous life.
- **Verification:** `tests/test_chost.c`: 3 controller nodes on real WAL files, 5 brokers, lossy and
  duplicating network, controller partitions and crashes, broker kills, brokers cut off, stale and invalid
  ALTER_ISR requests. 1500 scenarios, 589k commands, 80k leader changes, no violation. Checks: equal state at
  equal Raft index on all nodes, a single history per partition (isr_version -> epoch, leader, ISR), the
  nonce rule, convergence after healing, a permanently dead broker gets marked down, and a controller
  failover leaves healthy brokers' sessions unchanged. Mutants caught: snapshot not installed or taken at the
  wrong index, snapshot not restored at start-up, nonce ignored or not stored, DOWN without session, no
  failover grace period. Mutants that survive are safety-neutral by design and only cost availability or
  traffic: the "ready" guards (a stale node's answers are harmless because every command is validated at
  apply time) and the UP retry limit.
- **Consequences:** + one object to run per controller node; − not yet connected to sockets (the transport for
  brokers and for controller-to-controller traffic is the next step); − a broker's state push is still the
  host's job.

### ADR-023: Replica log on hx_log: epoch as a record prefix, durable truncation
- **Context:** hx_repl needs a log that remembers the leader epoch of every record and can drop a diverged
  suffix. hx_log was append-only and knew nothing about epochs.
- **Decision:** `hx_log_truncate` is added to the engine. It is durable when it returns and crash-safe:
  segments are unlinked newest first with a directory fsync after each, the boundary segment is cut with
  ftruncate + fsync, so a crash leaves a valid prefix at least as long as requested, never a gap. The
  adapter `hx_plog` (src/repl/repl_log.c) stores `u32 epoch | payload` in each record, so no frame-format
  change and no second file to keep consistent. Runs of equal epochs are indexed in memory.
- **Verification:** truncation at every cut point and with different record sizes afterwards (a stale
  sparse-index entry was a real hazard and has its own test); `tests/test_plog.c`; and a differential mode
  in the replication simulator: in one of six scenarios every log operation (append, truncate, read,
  epoch queries, sync) is mirrored on a real log and compared, and each simulated crash drops the unsynced
  suffix of the real log, reopens it and checks the recovered content. 2007 scenarios, no difference.
- **Limits:** opening a log reads every record once to rebuild the epoch runs (O(records); a checkpoint
  file would fix it); retention is refused on replicated logs (a log must start at offset 0) because a
  follower behind the leader's log start would need a snapshot-like transfer; sync() really fdatasyncs only
  outside the tests, where the crash model decides what survives, so the fdatasync call itself is not
  fault-tested.
- **Consequences:** + replicated and unreplicated partitions share the engine; − the 4-byte prefix also
  means consumers of a replicated partition must read through `hx_plog_read_payload`.

### ADR-024: Replica transport reuses the Raft TCP transport with a different hello magic
- **Context:** brokers need to exchange FETCH, FETCH_RESP and ALTER-free replication traffic for many
  partitions. A second transport implementation would duplicate connection handling, backoff, bounded queues
  and hostile-input defences that already exist and are tested.
- **Decision:** `hx_replnet` (src/repl/repl_net.c) wraps `hx_raftnet` and adds only framing: `u32 partition |
  hx_repl_msg`. The hello magic is configurable ("HXP1" for replication) so a replication port rejects Raft
  peers and vice versa. The sender id in the message must equal the id proven by the connection's hello,
  otherwise the frame is dropped. All partitions between a pair of brokers share one connection.
- **Verification:** `tests/test_replnet.c`: three brokers, two partitions, real `hx_log` replicas behind real
  sockets; replication and high watermark; a follower restarting on a new port and catching up; a leader
  change with an epoch bump; wrong-magic peers, junk frames and a spoofed sender are rejected and counted and
  the cluster keeps working. Mutants that drop frames, mislabel partitions, share the Raft magic or skip the
  sender check are caught.
- **Limits:** no TLS or authentication (trusted network only); one TCP stream per broker pair means a large
  fetch response delays small messages of other partitions (head-of-line blocking); messages carrying more
  than 1024 records are dropped by the receiver; loss is handled by the protocol, not the transport, so a
  stalled peer shows up as growing follower lag, not as an error.

---

## 4. Trade-off Summary

| Axis              | We chose                | We pay with                                   |
|-------------------|-------------------------|-----------------------------------------------|
| Latency           | Thread-per-core         | Partition→core imbalance, more complex routing|
| Safety            | Raft per partition      | Heartbeat overhead, more code                 |
| Portability       | Linux-only              | No macOS/Windows dev parity (use containers)  |
| Simplicity        | epoll baseline          | Not peak efficiency until io_uring backend    |
| Memory            | Pools/arenas            | Up-front sizing config, careful lifetimes     |
| Ecosystem         | Own protocol first      | Adoption friction until Kafka adapter exists  |

---

## 5. Data Integrity Argument (why acked data survives)

For `acks=quorum, fsync=always`:
1. Leader appends and `fdatasync`s; followers do the same before acknowledging replication.
2. Leader acks the producer only after a majority has durably persisted the record.
3. Any new leader must come from a majority intersecting the replicating majority (Raft election restriction) → it contains the record.
4. Recovery truncates only **unacknowledged** suffixes (beyond HW) and corrupt tails; acked prefix is never discarded.

Hence: acked ⇒ on ≥ ⌈(N+1)/2⌉ disks ⇒ survives any minority failure, including simultaneous power loss on a minority.

---

## 6. Failure Matrix

| Failure                          | `acks=1,fsync=interval` | `acks=quorum,fsync=interval` | `acks=quorum,fsync=always` |
|----------------------------------|-------------------------|------------------------------|-----------------------------|
| Broker process crash (kill -9)   | Safe (page cache survives) | Safe                       | Safe                        |
| Single node power loss           | May lose ≤ interval     | Safe (other replicas)        | Safe                        |
| Leader loss before replicate     | **Can lose acked data** | Safe                         | Safe                        |
| Majority simultaneous power loss | Loss possible           | May lose ≤ interval          | Safe                        |
| Disk bit-rot                     | Detected by CRC, repaired from replica | same           | same                        |
| Network partition (minority leader) | Stale leader may accept then discard | Rejected (no quorum) | Rejected             |

---

## 7. Scalability Plan

- **Vertical:** more cores ⇒ more partitions served in parallel; per-core targets in PLAN §3.
- **Horizontal:** add brokers, reassign partitions (Raft membership change + snapshot transfer).
- **Hot partition mitigation:** key-salting guidance, partition splitting (future), read-only follower fetch (future).
- **Back-pressure:** per-connection and per-partition in-flight byte limits; produce returns `THROTTLED` with a retry hint rather than buffering unbounded.

---

## 8. Benchmark Methodology (so claims are credible)

- **Hardware:** fixed profile (e.g., 16 cores, 64 GB RAM, NVMe, 25 GbE) documented in the report.
- **Baseline:** Kafka (latest stable) tuned per its docs (heap, `num.io.threads`, page cache), same disk/FS.
- **Workloads:** (a) 1 KB msgs, 1 producer/1 partition; (b) 100 producers/100 partitions; (c) mixed produce+consume; (d) `acks=quorum` 3-node.
- **Metrics:** throughput (msg/s, MB/s), latency p50/p99/p99.9/max via HdrHistogram with **coordinated-omission correction**, CPU, RSS, recovery time.
- **Rules:** warm-up 60 s, measure ≥ 10 min, ≥ 5 runs, report median + spread, publish raw data & scripts.
- **No claim** without a reproducible script in `bench/`.

---

## 9. Open Questions

1. Per-partition Raft vs. ISR — revisit after measuring heartbeat cost at scale (ADR-004).
2. Batch compression: per-batch codec only, or dictionary-trained per-topic?
3. Index structure: sparse + linear scan vs. small per-segment B+tree for faster seeks.
4. Is `O_DIRECT` + own cache worth it vs. page cache (jitter vs. complexity)?
5. Tiered storage object format — reuse segment bytes verbatim?

---

## 10. Glossary

- **Offset:** monotonically increasing per-partition record index.
- **High-watermark (HW):** highest offset durably replicated to a quorum.
- **ISR:** in-sync replica set (Kafka concept).
- **Group commit:** one `fsync` covering many appends.
- **Segment:** one file of the partition log.
- **Compaction:** retaining only latest record per key.

### ADR-025: Broker-controller channel and replica manager

Context: the replication core, controller and replica transport existed separately. Brokers need to register with the controller, learn leaders/ISR, and propose ISR changes; something must own the per-partition logs and drive `hx_repl`.

Decision:
- `ctrl_net` (poll()-based, `u32 len|u8 type|body`, 64 KiB max frame, 1 MiB out limit). The broker sends HELLO (id, nonce) periodically (it doubles as heartbeat) and ALTER_ISR; the controller leader answers SESSION and pushes STATE. Non-leaders answer NOT_READY with a leader hint.
- The server takes the broker identity from HELLO and closes the connection on an ALTER from a different broker. The client adopts a SESSION only for its own nonce and only if the number is greater, and passes STATE on only if it carries the adopted session. This is covered by `tests/test_ctlcli.c` (fake controller) and by `hostile_clients` in `tests/test_cluster.c`.
- `replica_mgr` owns one `hx_plog` per partition (`data_dir/p<part>`), the replica transport, and the controller client, and runs `hx_repl` for each partition. Committed-only reads (`hx_rmgr_read` returns NOT_FOUND at or above HW).
- `tests/test_cluster.c`: 3 controllers + 3 brokers over real TCP; leader kill, broker restart with a new nonce, controller leader kill.

Limits: no TLS or authentication; partitions can only be added before the first poll; the full state is pushed (cost grows with partitions); a deposed controller can push stale state, which `hx_repl` rejects through epoch checks; no per-partition handshake; not yet wired into `helixd`.

### ADR-026: Cluster mode inside `helixd` with a static topic table

Context: replication components existed as libraries. The broker core is synchronous (one request, one response) and has no notion of a response that arrives later.

Decision:
- `helixd --cluster-file F --node-id N`. The cluster file lists the nodes (client/raft/controller/replica ports) and the replicated topics with their partition counts. Every node is a Raft controller member and a replica of every partition; partition ids are assigned by file order, so all nodes agree without negotiation. Whichever node is the ready controller leader proposes the CREATE commands (windows of 16, repeated until the controller has them all).
- `src/broker/cluster.c` runs on the event-loop thread through `hx_server_hooks` (handle / resolve / drop / poll). `handle` sees each request first: declared topics are answered here, everything else falls through to the unchanged broker.
- Produce appends on the leader and parks the connection; no further frames of that connection are processed until the record is committed (HW passed it) or the wait ends (`REQUEST_TIMEOUT` after `--ack-timeout-ms`, or at once if leadership moved). Parking keeps response order per connection without a queue. Fetch is leader-only and bounded by the high watermark.
- Fail-stop: a failed controller host or replica, or an unresolvable parked request, stops the process.
- ClusterMetadata (API 15) exposes nodes, leader, epoch, ISR, HW and LEO per partition.
- `scripts/cluster_test.sh` runs three real processes: election, dense offsets, followers' HW, NOT_LEADER, kill -9 of the leader, rejoin and catch-up, kill -9 of all nodes, and "no acknowledgement while the followers are frozen". It also runs under ASan in `make asan-integration`.

Limits: topics are fixed by the file (no runtime CreateTopic, no growth, no change of node count); replication factor equals the node count; at most 8 nodes and 1024 partitions in total; ProduceIdempotent, consumer groups and committed offsets do not work on replicated topics; Fetch has no timestamps and no follower reads; one Produce per parked connection (no pipelining across a replicated write); no TLS or authentication; no retention on replicated logs; the loop wakes every millisecond in cluster mode; unacknowledged records on a deposed leader are truncated on rejoin, so a `REQUEST_TIMEOUT` may mean either outcome; parked-request lookup is linear.
