/*
 * TCP transport for partition replication messages between brokers, on top of hx_raftnet (same
 * connection model: one outbound connection per peer, hello handshake, bounded queues, reconnect
 * with backoff; separate hello magic so a replication port can never be mistaken for a Raft port).
 *
 * Each frame is `u32 partition | hx_repl_msg`. A frame that does not decode, or names a partition
 * nobody handles, is dropped and counted: replication tolerates loss, duplication and reordering
 * by design (see repl.h), so the transport promises nothing more than "best effort".
 *
 * There is no TLS or authentication: run it on a trusted network.
 */
#ifndef HELIX_REPL_NET_H
#define HELIX_REPL_NET_H

#include "raft/raft_net.h"
#include "repl/repl.h"

typedef struct hx_replnet hx_replnet;

/* A decoded message for `part`. m->recs points into transport memory and is valid only during the
 * call. Must not destroy the transport. */
typedef void (*hx_replnet_on_msg)(void *ctx, uint32_t part, const hx_repl_msg *m);

typedef struct {
    uint32_t self;
    uint32_t nbrokers;
    const char *bind_addr;
    uint16_t port; /* 0 = ephemeral */
    uint32_t max_frame;
    uint32_t max_queue;
    hx_replnet_on_msg on_msg;
    void *ctx;
} hx_replnet_cfg;

hx_replnet *hx_replnet_create(const hx_replnet_cfg *cfg);
void hx_replnet_destroy(hx_replnet *n);
uint16_t hx_replnet_port(const hx_replnet *n);
int hx_replnet_set_peer(hx_replnet *n, uint32_t id, const char *addr, uint16_t port);

/* Queue m (addressed by m->to) for partition `part`. 0 if queued, -1 if dropped (destination is
 * not a broker, queue full, ...). */
int hx_replnet_send(hx_replnet *n, uint32_t part, const hx_repl_msg *m, int64_t now_ms);

int hx_replnet_poll(hx_replnet *n, int timeout_ms, int64_t now_ms);

typedef struct {
    hx_raftnet_stats net;
    uint64_t undecodable; /* frames that were not a valid message */
    uint64_t wrong_dest;  /* valid frames addressed to another broker */
} hx_replnet_stats;

hx_replnet_stats hx_replnet_get_stats(const hx_replnet *n);

#endif
