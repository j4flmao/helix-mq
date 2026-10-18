/*
 * Raft consensus core (leader election with PreVote + CheckQuorum, log replication, commit index).
 *
 * Pure state machine: no sockets, no threads, no wall clock, no global state. The host
 *   - feeds time with hx_raft_tick(now_ms) and incoming messages with hx_raft_step(),
 *   - sends whatever hx_raft_take_msgs() returns,
 *   - applies committed entries (hx_raft_commit_index / hx_raft_entry_at) to its state machine.
 * That keeps it deterministic, so tests/test_raft.c drives whole clusters under injected faults
 * (loss, duplication, reordering, partitions, crashes) and checks the Raft safety properties.
 *
 * Durability (write-ahead): the raft module persists through the hx_raft_storage callbacks and
 * calls `sync` BEFORE a public call returns. Messages produced during a call are only handed out
 * afterwards, so no vote or acknowledgement can leave the node before the state it depends on is
 * durable. If any storage callback fails the instance becomes `failed`: it emits nothing more and
 * every call returns -1 (fail-stop, like ADR-011).
 *
 * Snapshots: the host owns the state machine. When it has applied entries up to `index` it calls
 * hx_raft_snapshot(index, bytes) and Raft forgets the log up to there. A follower that has fallen
 * behind the leader's snapshot receives it whole in one HX_RAFT_MSG_SNAPSHOT message (so a snapshot
 * must fit the transport's frame limit) and the host collects it with
 * hx_raft_take_installed_snapshot(). Without a save_snapshot callback snapshots are disabled.
 *
 * Scope of this version: fixed membership (nodes 0..n-1, n <= 32), the retained log is in memory,
 * no membership change. Those are listed in docs/ARCHITECTURE.md.
 * Threading: one owner thread per instance (ADR-003).
 */
#ifndef HELIX_RAFT_H
#define HELIX_RAFT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "util/buf.h"

#define HX_RAFT_MAX_NODES 32U
#define HX_RAFT_NONE      UINT32_MAX

typedef enum {
    HX_RAFT_FOLLOWER = 0,
    HX_RAFT_PRECANDIDATE,
    HX_RAFT_CANDIDATE,
    HX_RAFT_LEADER
} hx_raft_role;

typedef enum {
    HX_RAFT_MSG_PREVOTE = 1,
    HX_RAFT_MSG_PREVOTE_RESP = 2,
    HX_RAFT_MSG_VOTE = 3,
    HX_RAFT_MSG_VOTE_RESP = 4,
    HX_RAFT_MSG_APPEND = 5,
    HX_RAFT_MSG_APPEND_RESP = 6,
    HX_RAFT_MSG_SNAPSHOT = 7 /* leader -> lagging follower; answered with APPEND_RESP */
} hx_raft_msg_type;

/* One log entry. `data` is borrowed: valid until the next call into the instance (or the input
 * buffer). */
typedef struct {
    uint64_t term;
    uint32_t len;
    const uint8_t *data;
} hx_raft_entry;

typedef struct {
    hx_raft_msg_type type;
    uint32_t from;
    uint32_t to;
    uint64_t term;
    /* PREVOTE / VOTE: candidate's last log position. APPEND: previous entry. */
    uint64_t last_index; /* VOTE*: candidate last index; APPEND: prev index  */
    uint64_t last_term;  /* VOTE*: candidate last term;  APPEND: prev term   */
    uint64_t commit;     /* APPEND: leader commit index                       */
    uint64_t match; /* APPEND_RESP: highest replicated index (success) or retry hint (reject) */
    bool granted;   /* *_RESP: vote granted / append accepted             */
    const hx_raft_entry *entries; /* APPEND only */
    uint32_t nentries;
    const uint8_t *snap; /* SNAPSHOT only: state machine image as of (last_index, last_term) */
    uint32_t snap_len;
} hx_raft_msg;

/*
 * Stable storage. The instance calls these synchronously, in this order of intent:
 *   save_hard_state / append, ..., then sync once before returning to the host.
 * append(first_index, ...) means: discard every stored entry with index >= first_index, then store
 * these entries as first_index, first_index+1, ... Nothing counts as durable until sync() returns
 * 0. Every callback returns 0 on success.
 */
typedef struct {
    void *ctx;
    int (*save_hard_state)(void *ctx, uint64_t term, uint32_t voted_for);
    int (*append)(void *ctx, uint64_t first_index, const hx_raft_entry *entries, size_t n);
    int (*sync)(void *ctx);
    /*
     * Optional. Persist a snapshot of the state machine as of (index, term) and discard every
     * stored entry with index <= `index`; if no stored entry at `index` has this term, discard all
     * entries. Durable after the next sync().
     */
    int (*save_snapshot)(void *ctx, uint64_t index, uint64_t term, const uint8_t *data,
                         uint32_t len);
} hx_raft_storage;

typedef struct {
    uint32_t id;     /* this node, 0..nnodes-1 */
    uint32_t nnodes; /* cluster size, 1..HX_RAFT_MAX_NODES */
    uint32_t election_min_ms;
    uint32_t election_max_ms; /* timeout is uniform in [min, max) */
    uint32_t heartbeat_ms;    /* must be well below election_min_ms */
    uint32_t max_batch_entries;
    uint32_t max_batch_bytes;
    uint32_t max_entry_bytes; /* proposal size limit */
    uint64_t seed;            /* election-timeout jitter; use different seeds per node */
    hx_raft_storage storage;
} hx_raft_cfg;

/* State recovered from stable storage at start-up (all zero/empty for a fresh node). */
typedef struct {
    uint64_t term;
    uint32_t voted_for; /* HX_RAFT_NONE if none */
    const hx_raft_entry *entries;
    size_t nentries;     /* entry i has log index snap_index + i + 1 */
    uint64_t snap_index; /* 0 = no snapshot */
    uint64_t snap_term;
    const uint8_t *snap_data;
    uint32_t snap_len;
} hx_raft_restore;

typedef struct hx_raft hx_raft;

void hx_raft_cfg_default(hx_raft_cfg *cfg, uint32_t id, uint32_t nnodes);

/* Returns NULL on invalid config or out of memory. `restore` may be NULL. */
hx_raft *hx_raft_create(const hx_raft_cfg *cfg, const hx_raft_restore *restore, int64_t now_ms);
void hx_raft_destroy(hx_raft *r);

/* Advance time. Returns 0, or -1 if the instance has failed. */
int hx_raft_tick(hx_raft *r, int64_t now_ms);

/* Process one message (ignored if not addressed to this node). Returns 0 or -1 (failed). */
int hx_raft_step(hx_raft *r, int64_t now_ms, const hx_raft_msg *m);

typedef enum {
    HX_RAFT_OK = 0,
    HX_RAFT_NOT_LEADER = 1,
    HX_RAFT_TOO_LARGE = 2,
    HX_RAFT_FAILED = 3,
    HX_RAFT_NOMEM = 4
} hx_raft_status;

/* Leader only: append a command. On success the entry is identified by *index and *term. */
hx_raft_status hx_raft_propose(hx_raft *r, int64_t now_ms, const void *data, uint32_t len,
                               uint64_t *index, uint64_t *term);

/*
 * Messages produced by the LAST tick/step/propose call. Collect and send them after every such
 * call: the next call discards them. The array and everything it points to stay valid until the
 * next call.
 */
void hx_raft_take_msgs(hx_raft *r, const hx_raft_msg **msgs, size_t *n);

/*
 * Host-driven log compaction. Preconditions: save_snapshot is configured, snap_index(r) < index <=
 * commit_index(r), and `data` is the state machine image after applying entries 1..index. Returns
 * 0, or -1 if refused or failed (check hx_raft_failed). Like any mutating call it clears the
 * outbox.
 */
int hx_raft_snapshot(hx_raft *r, int64_t now_ms, uint64_t index, const void *data, uint32_t len);

/*
 * True once after a leader's snapshot replaced part of this node's log: the host must reset its
 * state machine to the image and continue applying from index + 1. Data stays valid until the next
 * mutating call.
 */
bool hx_raft_take_installed_snapshot(hx_raft *r, uint64_t *index, uint64_t *term,
                                     const uint8_t **data, uint32_t *len);
uint64_t hx_raft_snapshot_index(const hx_raft *r);
/* ------------------------------------------------------------ inspection */
hx_raft_role hx_raft_get_role(const hx_raft *r);
uint64_t hx_raft_term(const hx_raft *r);
uint32_t hx_raft_leader(const hx_raft *r); /* HX_RAFT_NONE if unknown */
uint32_t hx_raft_voted_for(const hx_raft *r);
uint64_t hx_raft_commit_index(const hx_raft *r);
uint64_t hx_raft_last_index(const hx_raft *r);
bool hx_raft_failed(const hx_raft *r);
/* Entry at 1-based index; false if out of range or already compacted into a snapshot. The data
 * pointer is valid until the next mutating call. */
bool hx_raft_entry_at(const hx_raft *r, uint64_t index, hx_raft_entry *out);

/* ------------------------------------------------------------ wire format */

/* Appends the encoded message (no length prefix) to `out`. 0 ok, -1 out of memory. */
int hx_raft_msg_encode(const hx_raft_msg *m, hx_buf *out);

/*
 * Decode into *m. Entry descriptors are written to `scratch` (capacity `cap`); their data points
 * into `p`. Returns 0, or -1 if the bytes are malformed, trailing, or carry more than `cap`
 * entries.
 */
int hx_raft_msg_decode(const uint8_t *p, size_t len, hx_raft_entry *scratch, size_t cap,
                       hx_raft_msg *m);

#endif /* HELIX_RAFT_H */
