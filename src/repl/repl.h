/*
 * Partition replication: one leader, followers that fetch, an in-sync replica set (ISR), a high
 * watermark (HW). This is the data-plane protocol; WHO leads and WHO is in the ISR is decided by a
 * controller (ADR-020: in the cluster it is the Raft group, in tests an oracle) and delivered with
 * hx_repl_set_state(). Pure state machine like hx_raft: no sockets, threads or clock, so a whole
 * cluster runs deterministically under injected faults (tests/test_repl.c).
 *
 * Protocol in one paragraph. The controller assigns (leader_epoch, leader, ISR, isr_version).
 * The leader appends client records tagged with its epoch and syncs them. A follower sends
 * FETCH(epoch, fetch_offset = its log end, last_fetched_epoch = epoch of the record before it).
 * If that (offset, epoch) is in the leader's log the leader replies with the next records and its
 * HW, and learns the follower's log end. Otherwise it replies DIVERGED(epoch, end_offset) and the
 * follower truncates to the common prefix (KIP-101/320 style) and fetches again. The leader's HW
 * is the minimum log end over the ISR (plus any ISR change it has proposed but not yet seen
 * approved); a record is committed once HW passes it. ISR changes are PROPOSED by the leader
 * (ALTER_ISR) and take effect only when the controller approves, which is what stops a deposed
 * leader from shrinking its way to acknowledging data the new leader will not have.
 *
 * Durability: every append and truncate is synced BEFORE the replica reports a log end to anyone
 * (a follower's next FETCH, the leader counting itself in the HW). A failing storage callback
 * makes the instance `failed` (fail-stop, ADR-011): it emits nothing more.
 *
 * Controller session. A replica cannot tell a stale controller message from a fresh one after it
 * restarts (its epoch is back to 0). The host must therefore feed hx_repl_set_state() only from a
 * controller session opened AFTER the instance was created (registration handshake / connection),
 * and discard anything from an earlier life. Within one session the epoch and ISR version make
 * reordered or duplicated input harmless.
 *
 * Not in this module: producer batching, retention, the controller, the network. Threading: one
 * owner thread per instance.
 */
#ifndef HELIX_REPL_H
#define HELIX_REPL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "util/buf.h"

#define HX_REPL_MAX_REPLICAS 16U
#define HX_REPL_NONE         UINT32_MAX
#define HX_REPL_CONTROLLER   (UINT32_MAX - 1U) /* destination of ALTER_ISR messages */

typedef struct {
    uint32_t epoch; /* leader epoch of the leader that appended it */
    uint32_t len;
    const uint8_t *data; /* borrowed */
} hx_repl_rec;

/*
 * The replica's local log. Offsets are dense from 0. The replica needs to know each record's
 * epoch, so the implementation stores it next to the record (the broker keeps an epoch checkpoint).
 * Every function returns 0 on success.
 */
typedef struct {
    void *ctx;
    uint64_t (*leo)(void *ctx); /* offset the next record will get */
    int (*epoch_at)(void *ctx, uint64_t offset, uint32_t *epoch); /* offset < leo */
    /* First offset whose record has epoch > `epoch`, or leo() if none. */
    uint64_t (*end_offset_for_epoch)(void *ctx, uint32_t epoch);
    int (*append)(void *ctx, const hx_repl_rec *recs, size_t n);
    int (*truncate)(void *ctx, uint64_t new_leo); /* drop every offset >= new_leo */
    /* Up to max_n records / max_bytes (at least one if any exist) from `offset`. The pointers must
     * stay valid until the next call into this log. */
    int (*read)(void *ctx, uint64_t offset, size_t max_n, size_t max_bytes, hx_repl_rec *out,
                size_t *n);
    int (*sync)(void *ctx); /* durable when it returns 0 */
} hx_repl_log;

typedef struct {
    uint32_t id;       /* this replica, 0..HX_REPL_MAX_REPLICAS-1 */
    uint32_t replicas; /* bitmask of the assigned replicas (includes id) */
    uint32_t min_isr;  /* appends are refused while the ISR is smaller than this */
    uint32_t lag_ms;   /* a follower that has not caught up for this long leaves the ISR */
    uint32_t fetch_ms; /* follower poll interval when idle */
    uint32_t alter_retry_ms;
    uint32_t max_fetch_records;
    uint32_t max_fetch_bytes;
    hx_repl_log log;
} hx_repl_cfg;

typedef enum { HX_REPL_ROLE_NONE = 0, HX_REPL_FOLLOWER, HX_REPL_LEADER } hx_repl_role;

typedef enum {
    HX_REPL_MSG_FETCH = 1,
    HX_REPL_MSG_FETCH_RESP = 2,
    HX_REPL_MSG_ALTER_ISR = 3 /* leader -> controller; the answer comes back as set_state */
} hx_repl_msg_type;

typedef enum {
    HX_REPL_RESP_OK = 0,
    HX_REPL_RESP_DIVERGED = 1,
    HX_REPL_RESP_NOT_LEADER = 2, /* responder is not the leader of the epoch asked about */
    HX_REPL_RESP_FENCED = 3      /* requester's epoch is older than the responder's */
} hx_repl_resp_status;

typedef struct {
    hx_repl_msg_type type;
    uint32_t from;
    uint32_t to;
    uint32_t epoch;
    /* FETCH */
    uint64_t fetch_offset;
    uint32_t
        last_fetched_epoch; /* epoch of the record at fetch_offset - 1, 0 if fetch_offset == 0 */
    /* FETCH_RESP (fetch_offset echoes the request so stale replies can be recognised) */
    hx_repl_resp_status status;
    uint64_t hw;
    uint32_t div_epoch; /* DIVERGED: largest epoch <= last_fetched_epoch the leader holds */
    uint64_t div_end;   /* DIVERGED: where that epoch ends in the leader's log */
    const hx_repl_rec *recs;
    uint32_t nrecs;
    /* ALTER_ISR */
    uint32_t isr;
    uint32_t isr_version;
} hx_repl_msg;

typedef struct hx_repl hx_repl;

void hx_repl_cfg_default(hx_repl_cfg *cfg, uint32_t id, uint32_t replicas);

/* NULL on invalid config or out of memory. Starts with no role: wait for set_state. */
hx_repl *hx_repl_create(const hx_repl_cfg *cfg, int64_t now_ms);
void hx_repl_destroy(hx_repl *r);

/*
 * The controller's current assignment. Stale (older epoch) or duplicate input is ignored; a newer
 * epoch switches role; the same epoch with a higher isr_version updates the ISR (this is also how
 * an ALTER_ISR answer arrives). Returns 0, or -1 if the instance has failed.
 */
int hx_repl_set_state(hx_repl *r, int64_t now_ms, uint32_t epoch, uint32_t leader, uint32_t isr,
                      uint32_t isr_version);

int hx_repl_tick(hx_repl *r, int64_t now_ms);
int hx_repl_step(hx_repl *r, int64_t now_ms, const hx_repl_msg *m);

typedef enum {
    HX_REPL_OK = 0,
    HX_REPL_NOT_LEADER = 1,
    HX_REPL_NOT_ENOUGH_REPLICAS = 2,
    HX_REPL_FAILED = 3,
    HX_REPL_BAD_ARG = 4
} hx_repl_status;

/* Leader only. The records get this replica's epoch; offset of the first one goes to *first. */
hx_repl_status hx_repl_append(hx_repl *r, int64_t now_ms, const hx_repl_rec *recs, size_t n,
                              uint64_t *first);

/* Messages produced by the LAST call; same contract as hx_raft_take_msgs. */
void hx_repl_take_msgs(hx_repl *r, const hx_repl_msg **msgs, size_t *n);

hx_repl_role hx_repl_get_role(const hx_repl *r);
uint32_t hx_repl_epoch(const hx_repl *r);
uint32_t hx_repl_leader(const hx_repl *r);
uint32_t hx_repl_isr(const hx_repl *r);
uint32_t hx_repl_isr_version(const hx_repl *r);
uint64_t hx_repl_hw(const hx_repl *r); /* committed prefix: records below are safe to expose */
uint64_t hx_repl_leo(const hx_repl *r);
bool hx_repl_failed(const hx_repl *r);

/* Wire format (no length prefix). Same contract as the Raft codec. */
int hx_repl_msg_encode(const hx_repl_msg *m, hx_buf *out);
int hx_repl_msg_decode(const uint8_t *p, size_t len, hx_repl_rec *scratch, size_t cap,
                       hx_repl_msg *m);

#endif /* HELIX_REPL_H */
