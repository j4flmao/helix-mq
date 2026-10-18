/*
 * Consumer-group coordinator: membership, rebalance barrier, session expiry,
 * partition assignment and commit fencing.
 *
 * Pure state machine: no I/O, no clock, no globals. The caller passes the
 * current time and a partition-listing callback with every call, so it is
 * unit-testable with a fake clock and runs on the broker's single owner thread
 * (ADR-003) without locks. Group state is deliberately in memory only
 * (committed offsets are durable elsewhere); after a restart members get
 * HXE_UNKNOWN_MEMBER and rejoin.
 *
 * Protocol model (eager, non-blocking, "poll until stable"):
 *   - Any membership or subscription change starts a REBALANCE: the group keeps
 *     its generation, every member is marked "not joined", and Heartbeat returns
 *     HXE_REBALANCE_IN_PROGRESS so members stop consuming and rejoin.
 *   - JoinGroup marks the caller joined. When every member has joined (or the
 *     rebalance deadline evicted those who did not), assignments are computed,
 *     the generation is incremented and the group is STABLE again.
 *   - JoinGroup answers stable=0 while others are still catching up; the client
 *     repeats it. Nothing ever blocks the event loop.
 *   - No member receives a new assignment before all others have given up the
 *     old one, and commits carry (member, generation), so a zombie is fenced.
 */
#ifndef HELIX_GROUPS_H
#define HELIX_GROUPS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "proto/protocol.h"
#include "util/buf.h"

/* One (topic, partition); also the element type of the partition listing. */
typedef struct {
    uint32_t partition;
    size_t tlen;
    char topic[HX_MAX_TOPIC_LEN];
} hx_tp;

/* Returns a malloc'ed array sorted by (topic bytes, partition) in *out (caller frees). 0 = ok. */
typedef int (*hx_list_fn)(void *ctx, hx_tp **out, size_t *n);

typedef struct {
    int64_t now_ms; /* monotonic */
    hx_list_fn list;
    void *list_ctx;
} hx_gctx;

typedef struct {
    const char *name;
    size_t len;
} hx_str;

typedef struct {
    char member_id[HX_MAX_MEMBER_ID + 1];
    size_t member_id_len;
    uint32_t generation;
    bool stable;         /* false: others have not rejoined yet, call again */
    const hx_tp *assign; /* valid until the next call into the coordinator   */
    size_t nassign;
} hx_join_result;

typedef struct hx_groups hx_groups;

/* id_prefix makes member ids unique across broker restarts (e.g. "m<boot time>"). */
hx_groups *hx_groups_create(uint32_t max_groups, uint32_t max_members, const char *id_prefix);
void hx_groups_destroy(hx_groups *gs);

/* Names must already be validated by the caller. `mid` empty = new member. */
hx_wire_err hx_groups_join(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name, size_t glen,
                           const uint8_t *mid, size_t mlen, uint32_t session_ms,
                           const hx_str *topics, size_t ntopics, hx_join_result *out);

hx_wire_err hx_groups_heartbeat(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name,
                                size_t glen, const uint8_t *mid, size_t mlen, uint32_t generation);

hx_wire_err hx_groups_leave(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name,
                            size_t glen, const uint8_t *mid, size_t mlen);

/* Fencing check for GroupCommit: member known, generation current, partition assigned. */
hx_wire_err hx_groups_check_commit(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name,
                                   size_t glen, const uint8_t *mid, size_t mlen,
                                   uint32_t generation, const uint8_t *topic, size_t tlen,
                                   uint32_t partition);

/* Expire silent members and overdue rebalances. Cheap when nothing is due. */
void hx_groups_tick(hx_groups *gs, const hx_gctx *cx);

/*
 * Body: u8 state(0 stable,1 rebalancing) | u32 generation | u32 nmembers |
 *       nmembers x ( u16 idlen | id | u32 nassign | nassign x (u16 tlen | topic | u32 partition) )
 * Members sorted by id. HXE_UNKNOWN_MEMBER if the group does not exist.
 */
hx_wire_err hx_groups_describe(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name,
                               size_t glen, hx_buf *out);

/*
 * A topic was created or deleted: every stable group with a member subscribed to it starts a
 * rebalance, so new partitions get owners and deleted ones lose them. Groups already rebalancing
 * pick the change up when the rebalance completes (assignment reads the live partition list).
 */
void hx_groups_topic_changed(hx_groups *gs, const hx_gctx *cx, const char *topic, size_t tlen);

uint64_t hx_groups_rebalance_count(const hx_groups *gs);
size_t hx_groups_group_count(const hx_groups *gs);

#endif /* HELIX_GROUPS_H */
