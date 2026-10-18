/*
 * TCP transport for Raft messages (IPv4, non-blocking, single owner thread).
 *
 * Model: every node listens; to send to peer P a node keeps ONE outbound connection to P and never
 * writes on connections it accepted. So there is no duplicate-connection tie-break, and each TCP
 * stream carries traffic in one direction only. The first frame on a connection is a hello
 * (magic, sender id, cluster size); inbound connections whose hello is wrong are closed. Frames are
 * `u32 length | payload`, length-limited.
 *
 * Delivery is best effort, which is all Raft needs: messages queued for an unreachable peer are
 * dropped once the per-peer queue is full, and a broken connection discards what it had queued.
 * Reconnects use exponential backoff (100 ms .. 2 s). Nothing here blocks: connect, accept, read
 * and write are all non-blocking, driven by hx_raftnet_poll().
 */
#ifndef HELIX_RAFT_NET_H
#define HELIX_RAFT_NET_H

#include <stddef.h>
#include <stdint.h>

#include "raft/raft.h"

typedef struct hx_raftnet hx_raftnet;

/* Called for each complete frame from peer `from` (hello already verified). `p` is valid only
 * during the call. Must not destroy the transport. */
typedef void (*hx_raftnet_on_frame)(void *ctx, uint32_t from, const uint8_t *p, size_t len);

typedef struct {
    uint32_t self;
    uint32_t nnodes;
    const char *bind_addr; /* dotted IPv4, e.g. "127.0.0.1" */
    uint16_t port;         /* 0 = ephemeral */
    uint32_t max_frame;    /* 0 = default 8 MiB */
    uint32_t max_queue;    /* bytes queued per peer before dropping, 0 = default 4 MiB */
    uint32_t magic;        /* hello magic: separates independent networks, 0 = "HXR1" */
    hx_raftnet_on_frame on_frame;
    void *ctx;
} hx_raftnet_cfg;

/* NULL on failure (bad config, cannot listen, out of memory). */
hx_raftnet *hx_raftnet_create(const hx_raftnet_cfg *cfg);
void hx_raftnet_destroy(hx_raftnet *n);

uint16_t hx_raftnet_port(const hx_raftnet *n); /* the bound port */

/* Where peer `id` listens. Can be called again to change it. 0 on success. */
int hx_raftnet_set_peer(hx_raftnet *n, uint32_t id, const char *addr, uint16_t port);

/* Queue one frame for a peer. Returns 0 if queued, -1 if dropped (queue full, unknown peer, or
 * out of memory). A drop is not an error for Raft. */
int hx_raftnet_send(hx_raftnet *n, uint32_t to, const void *payload, size_t len, int64_t now_ms);

/* Wait up to timeout_ms for I/O and process it. Returns the number of frames delivered. */
int hx_raftnet_poll(hx_raftnet *n, int timeout_ms, int64_t now_ms);

typedef struct {
    uint64_t frames_in;
    uint64_t frames_out;
    uint64_t dropped;   /* frames refused by a full queue */
    uint64_t bad_peers; /* inbound connections rejected (bad hello / oversized / garbage) */
    uint64_t connects;  /* successful outbound connections */
} hx_raftnet_stats;

hx_raftnet_stats hx_raftnet_get_stats(const hx_raftnet *n);

#endif /* HELIX_RAFT_NET_H */
