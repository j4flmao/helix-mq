# Helix Wire Protocol v1

Binary, length-prefixed, request/response over TCP. **All integers are big-endian.**
The reference implementation is `src/proto/` (decoder) and `src/broker/broker.c` (handlers);
`tools/helixctl.c` is a complete client.

## Framing

```
Request : u32 len | u16 api | u16 version | u32 correlation_id | body
Response: u32 len | u32 correlation_id    | u16 error          | body
```

`len` counts the bytes **after** the length field. Limits (server-enforced, before any allocation):

| Rule | Value | On violation |
|------|-------|--------------|
| `len` >= 8 (request header) | fixed | connection closed |
| `len` <= `max_frame_bytes` | default 2 MiB, raised automatically to `max_record_bytes + 64 KiB` | connection closed |
| Version must equal 1 | fixed | `UNSUPPORTED_VERSION` |

Pipelining: a client may send many requests without waiting; responses return **in request order**
on a connection and echo `correlation_id`. The server stops reading from a connection whose unsent
output exceeds `max_out_pending` (8 MiB) until the peer drains it (backpressure).

## Error codes

| Code | Name | Meaning |
|-----:|------|---------|
| 0 | OK | success (body present) |
| 1 | BAD_REQUEST | truncated body, trailing bytes, or inconsistent lengths |
| 2 | UNKNOWN_API | unknown `api` |
| 3 | UNSUPPORTED_VERSION | `version` != 1 |
| 4 | INVALID_TOPIC | name empty / > 128 bytes / leading `.` / leading `__` (reserved) / chars outside `[A-Za-z0-9._-]` |
| 5 | OFFSET_OUT_OF_RANGE | fetch offset < first retained offset or > next offset |
| 6 | RECORD_TOO_LARGE | payload > `max_record_bytes` |
| 7 | STORAGE | I/O failure; details in the server log |
| 8 | UNKNOWN_PARTITION | partition > 1023, or Fetch on a (topic, partition) that does not exist |
| 9 | RESOURCE_LIMIT | `max_open_logs` or `max_offset_entries` reached |
| 10 | INVALID_GROUP | group name empty / > 128 bytes / leading `.` / chars outside `[A-Za-z0-9._-]` |
| 11 | UNKNOWN_MEMBER | group or member id not known (never joined, left, or expired) |
| 12 | REBALANCE_IN_PROGRESS | the group is rebalancing: rejoin with JoinGroup |
| 13 | ILLEGAL_GENERATION | the request carries a generation other than the group's current one |
| 14 | NOT_ASSIGNED | the member does not own that (topic, partition) in this generation |
| 15 | UNKNOWN_PRODUCER | producer id never issued, idle past the TTL, or issued before a broker restart |
| 16 | OUT_OF_ORDER_SEQUENCE | sequence is neither the next expected one nor one of the last 8 appended |
| 17 | TOPIC_EXISTS | CreateTopic on a topic that already has at least one partition |
| 18 | NOT_LEADER | replicated partition: this node is not its leader; ask ClusterMetadata (15) |
| 19 | NOT_ENOUGH_REPLICAS | replicated partition: the ISR is below the minimum (a majority); nothing was appended |
| 20 | REQUEST_TIMEOUT | replicated Produce: the commit was not seen in time or leadership moved; the record may or may not survive |
| 21 | UNSUPPORTED | idempotent produce, CreateTopic or DeleteTopic on a replicated topic |

Error responses carry no body. A malformed *header* (fewer than 8 bytes) closes the connection.

## APIs

### 0 — Ping
Request body: empty. Response body: empty.

### 1 — Produce
```
Request : u16 topic_len | topic | u32 partition | u32 payload_len | payload
Response: u64 offset
```
Appends one record and returns its offset. Offsets per partition are dense and start at 0.
Topics and partitions are created implicitly by Produce (never by Fetch).

**Durability contract.** The response is sent only after the record satisfies the server's `--sync` policy:

| `--sync` | When is the ack sent? | Survives |
|----------|-----------------------|----------|
| `always` | after `fdatasync` (shared by every request in the same event batch: *group commit*) | `kill -9`, power loss |
| `interval` (default) | immediately; fsync at most every `--sync-interval-ms` | `kill -9`; power loss may lose up to the interval |
| `never` | immediately | `kill -9` only |

If a group-commit `fdatasync` fails, `helixd` stops (fail-stop) without sending any acknowledgement
for that batch — a failed fsync cannot be retried safely on Linux, so continuing could acknowledge
data that never reached disk.

### 2 — Fetch
```
Request : u16 topic_len | topic | u32 partition | u64 offset | u32 max_bytes
Response: u32 count | count x ( u64 offset | i64 timestamp_ms | u32 len | payload )
```
Returns records starting at `offset`. `max_bytes` is capped by the server (default 4 MiB); at least one
record is always returned if any exists, so a consumer can always make progress. `offset == next_offset`
returns `count = 0` (not an error). With `--sync always`, a Fetch handled in the same event batch as a
Produce is also held until the batch's commit, so a consumer never observes a record before it is durable.

### 3 — Metadata
```
Request : (empty)
Response: u32 count | count x ( u16 topic_len | topic | u32 partition | u64 first_offset | u64 next_offset )
```
Lists every user partition on the broker, sorted by topic (bytewise) then partition. `first_offset` is the
oldest retained record, `next_offset` the offset the next Produce will get (so `next - first` records are
readable). Internal topics (`__*`) are never listed. Returns `RESOURCE_LIMIT` if the broker holds more
partitions than `max_open_logs`.

### 4 — OffsetCommit
```
Request : u16 group_len | group | u16 topic_len | topic | u32 partition | u64 offset
Response: (empty)
```
Stores the group's position for a partition. By convention `offset` is the **next offset to read** (last
processed + 1). Rules: the partition must exist (`UNKNOWN_PARTITION`), `offset <= next_offset`
(`OFFSET_OUT_OF_RANGE`); rewinding to an older offset is allowed. The commit is appended to the internal
`__consumer_offsets` log and follows the same durability contract and group commit as Produce, so an
acknowledged commit survives `kill -9` (and power loss with `--sync always`). The distinct
(group, topic, partition) keys are capped by `max_offset_entries` (default 100000); updating an existing key
is always allowed.

### 5 — OffsetFetch
```
Request : u16 group_len | group | u16 topic_len | topic | u32 partition
Response: i64 offset            (-1 = the group has no committed offset)
```
Works for any syntactically valid key, whether or not the partition exists.

**Scope.** API 4 is *unfenced*: it commits for anyone who names the group, so it is meant for admin and
manual consumers. Members of a managed group commit with API 10, which is fenced.

### Consumer groups (APIs 6-10)

Common types: `member` is `u16 len | id` (at most 63 bytes). Group state lives in broker memory only; after
a broker restart members get `UNKNOWN_MEMBER` and rejoin. Committed offsets are durable (API 4/10).

Protocol model (eager rebalance, "poll until stable"): any membership or subscription change starts a
rebalance. Members learn about it from `REBALANCE_IN_PROGRESS` on Heartbeat or GroupCommit and must call
JoinGroup again. JoinGroup never blocks: while some member has not rejoined it returns `stable = 0`
and the client polls. When all members have rejoined (or a deadline of `now + longest session` evicts
the stragglers) the generation increments, partitions are dealt round-robin per topic over members
sorted by id, and JoinGroup returns `stable = 1` with the member's assignment. Partitions created
after a rebalance are not assigned until the next rebalance.

### 6 — JoinGroup
```
Request : u16 glen | group | u16 mlen | member (empty = new member) | u32 session_ms |
          u16 ntopics | (u16 tlen | topic)*
Response: u16 idlen | member_id | u32 generation | u8 stable | u32 nassign |
          (u16 tlen | topic | u32 partition)*
```
`session_ms` is 1000..300000. A member that sends nothing for `session_ms` is expired. At most 64 topics.

### 7 — Heartbeat
```
Request : group | member | u32 generation        (no body in the response)
```
Refreshes the session. `REBALANCE_IN_PROGRESS` means rejoin, `ILLEGAL_GENERATION` / `UNKNOWN_MEMBER`
mean the membership was lost.

### 8 — LeaveGroup
```
Request : group | member                          (no body in the response)
```
Removes the member immediately and triggers a rebalance for the rest. An empty group is deleted.

### 9 — DescribeGroup
```
Request : group
Response: u8 rebalancing | u32 generation | u32 nmembers |
          (u16 idlen | id | u32 nassign | (u16 tlen | topic | u32 partition)*)*
```
Members are sorted by id. Unknown group: `UNKNOWN_MEMBER`.

### 10 — GroupCommit
```
Request : group | member | u32 generation | u16 tlen | topic | u32 partition | u64 offset
```
Fenced OffsetCommit. Checks, in order: group/member known (`UNKNOWN_MEMBER`), generation matches
(`ILLEGAL_GENERATION`), the member owns the partition (`NOT_ASSIGNED`). Only then is the offset written
through the same path as API 4, so it has the same durability. During a rebalance a member keeps its old
assignment and generation until the new generation is published, so it can still commit the progress it
made before rejoining; once the generation moves on, the old one is fenced.
A consumer that was kicked out or lost a partition can therefore never overwrite its successor's progress.

### Idempotent producer (APIs 11-12)

A producer calls InitProducer once, then numbers its records per (topic, partition) starting at 0 and sends
them with ProduceIdempotent. The broker appends a record only if its sequence is the next expected one.
A sequence among the last 8 appended is a **retry**: the broker answers with the original offset and appends
nothing. Any other sequence is refused, so retries cannot duplicate and a lost record shows up as a gap.
Retry with the same id and sequence after a dropped connection; do not renumber.

State is in memory: it survives reconnects but not a broker restart, and an idle producer is forgotten after
the TTL (default 15 min). Either way the next request gets `UNKNOWN_PRODUCER` and the producer must obtain a
new id. A record whose acknowledgement was lost *across that restart* may then be written twice, so the
guarantee is exactly-once per record within one broker lifetime, at-least-once across restarts.
Producer ids embed the boot time and are never reissued. A rejected record (error response) does not
consume its sequence and can be resent.

### 11 — InitProducer
```
Request : (empty)
Response: u64 producer_id
```

### 12 — ProduceIdempotent
```
Request : u64 producer_id | u32 sequence | u16 tlen | topic | u32 partition | u32 plen | payload
Response: u64 offset
```
Topic and size validation is the same as Produce (API 1). At most 256 (topic, partition) streams per producer.

### Topic administration (APIs 13-14)

Topics are still created implicitly by the first Produce; CreateTopic exists to create all partitions up
front, which makes the first rebalance of a consumer group see every partition. It does not stop a later
Produce to a higher partition number (up to 1023) from creating that partition too.

### 13 — CreateTopic
```
Request : u16 tlen | topic | u32 partition_count      (1..1024)
```
Creates empty partitions `0..count-1`. `TOPIC_EXISTS` if the topic has any partition, `RESOURCE_LIMIT` if the
partitions would exceed `max_open_logs`; a failed create leaves nothing behind. Groups subscribed to the
topic start a rebalance.

### 14 — DeleteTopic
```
Request : u16 tlen | topic
```
Removes every partition, all committed offsets of the topic (a tombstone per offset is made durable first)
and the producer state of the topic. `UNKNOWN_PARTITION` if the topic has no partitions. The directory is
renamed to `.deleting.<name>` atomically and then removed; an interrupted removal is finished at the next
start. Fetches that were in flight for the topic fail with `UNKNOWN_PARTITION`; groups subscribed to the
topic rebalance. Reserved (`__`) topics cannot be deleted.

### 15 — ClusterMetadata (cluster mode)
```
Request : (empty)
Response: u32 self | u32 nodes | nodes x ( u32 id | u16 host_len | host | u16 client_port )
          u32 partitions | partitions x ( u16 topic_len | topic | u32 partition | u32 leader (0xFFFFFFFF = none) |
                                          u32 epoch | u32 isr_bitmask | u64 high_watermark | u64 log_end | u8 role )
```
`role` is this node's own role for the partition (0 none, 1 leader, 2 follower). Everything is the answering
node's local view; a client should connect to `leader`, and on `NOT_LEADER` ask again.

### Replicated topics (cluster mode)
Topics declared in the cluster file are replicated to every node. For them, Produce (1) is acknowledged only
after the record is committed (replicated to the whole ISR, ISR >= majority). Fetch (2) is served by the leader
only and never returns records above the high watermark; timestamps are 0 (the replica log keeps no timestamps).
ProduceIdempotent, CreateTopic and DeleteTopic return `UNSUPPORTED`; Metadata (3) lists only node-local topics.
Consumer groups and offsets are node-local and not replicated. Topics not in the cluster file behave as before
and are NOT replicated.

## Compatibility

Native protocol only (ADR-010). A Kafka-protocol adapter is planned for Phase 7.
