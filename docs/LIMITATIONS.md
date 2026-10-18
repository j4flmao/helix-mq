# Current cluster limitations (source audit, 2026-10-18)

This describes the current implementation, not the planned architecture. Docker CI validates the
existing test suite; it does not establish production readiness or the 300k msg/s throughput goal.

## Configuration and deployment

- Static membership and topics: no runtime replicated topic create/delete/expansion or node-count changes.
  Every partition is replicated to every node. Limits: 8 nodes, 64 topics, 1024 partitions in total
  (`src/broker/cluster.h`).
- Config accepts IPv4 literals only, not DNS names or IPv6. The same address is used for internal bind
  and peer connections; there is no separate advertised address. Each node needs client, Raft,
  controller and replication ports (`hx_cl_spec_parse`, `hx_cluster_create`).
- Partition IDs come from topic order in the file. There is no cluster-wide config fingerprint or
  persisted topic-name mapping check. Keep the file identical across nodes and preserve its topic
  order and partition counts when reusing data; reordering can associate names with the wrong logs.
- Topics absent from the config fall through to the local broker and can be implicitly created.
  A misspelled topic can therefore receive successful, unreplicated writes (`hx_cluster_handle`).
- The supplied Compose files start standalone brokers (`dev`, `staging`, `prod`) or a three-node
  cluster (`cluster`). The cluster topology is still one host and one Docker subnet with static
  IPv4 addresses, so it demonstrates replication but is not a multi-host or production topology;
  it also shares the single-threaded scheduling and durability limits listed below.

## Client behavior and availability

- Native Helix protocol only; existing Kafka clients cannot connect directly.
- ProduceIdempotent is unsupported on replicated topics. Groups and committed offsets remain local;
  they do not provide a replicated consumer coordinator. Even standalone producer sequence state and
  group membership are in memory and lost on restart.
- Metadata API 3 lists local topics only. Use ClusterMetadata API 15 for replicated partitions.
  It returns the answering node's local view, which can lag. Clients must discover the leader and
  refresh on NOT_LEADER; helixctl produce/fetch do not automatically reroute.
- Fetch is leader-only, returns committed records and timestamp 0. No follower reads.
- Minimum ISR is fixed to `floor(nodes / 2) + 1`; it is not a user option. Too few replicas prevents
  writes. A two-node deployment cannot continue writes with either node down. Ack waits for the
  current ISR, so lagging members can delay writes until ISR adjustment.
- REQUEST_TIMEOUT is an unknown outcome, including after leadership changes. Retrying can duplicate
  an already committed record because replicated idempotence is unavailable.

## Performance and operations

- Controller, replication, storage and client handling share one thread. Disk sync can stall the
  event loop. Cluster mode explicitly syncs replicated writes; the standalone --sync durability
  table should not be interpreted as the replicated write policy.
- One outstanding replicated Produce per connection: later requests on that connection wait for its
  completion. No client-side batch Produce API. Parked-request lookup and replica-manager partition
  lookup are linear; the loop wakes every millisecond. These are unmeasured scaling costs.
- A failed controller or replica can stop the entire daemon, not just the affected partition.
- No TLS/authentication or replicated-log retention/compaction. Restart scans all retained records
  to reconstruct epoch runs (`hx_plog_open`), so startup work grows with log size.
- Replica traffic shares one TCP stream per broker pair; large transfers can delay other partitions.
  Controller updates send full state rather than deltas.
- No cluster throughput measurement establishes the 300k msg/s target.

## Payload limits and resolved boundary defect

Cluster mode accepts `--max-record-bytes` from 1 to 1048576 bytes (1 MiB). Larger configured limits
are rejected at startup because the replication transport uses bounded frames/output queues.
Standalone mode can configure larger records independently.

The earlier four-byte epoch accounting bug is fixed: the replica storage limit reserves the epoch
in addition to the public payload limit. With a limit of 64 bytes, all payloads up to 64 bytes are
accepted and 65 bytes returns RECORD_TOO_LARGE without stopping the broker. Real TCP regressions
in `tests/test_protocol.c` verify the boundary, minimum/default limits and restart contents.
