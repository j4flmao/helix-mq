/*
 * Replica manager: everything a broker process needs to take part in replication, wired together.
 *
 *   controller (hx_ctlcli) --state--> hx_repl --FETCH/RESP--> peers (hx_replnet)
 *                                        |  ^
 *                                        v  |
 *                                    hx_plog (hx_log)
 *
 * One hx_repl + hx_plog per partition, one replica transport, one controller client. The manager
 * only moves messages between those pieces and drives time; all decisions stay in hx_repl and the
 * controller.
 *
 * Partitions must be added before the first hx_rmgr_poll(). Reason: hx_repl_set_state() may only be
 * fed from a controller session opened after the replica instance was created (repl.h); the session
 * is established by the first polls, so an instance created later could be handed state that was
 * sent before it existed. Adding partitions at run time needs a per-partition handshake and is not
 * implemented.
 */
#ifndef HELIX_REPLICA_MGR_H
#define HELIX_REPLICA_MGR_H

#include "ctrl/ctrl_net.h"
#include "repl/repl_log.h"
#include "repl/repl_net.h"

typedef struct hx_rmgr hx_rmgr;

typedef struct {
    uint32_t self;
    uint32_t nbrokers;
    const char *data_dir; /* partition p lives in data_dir/p<p> */
    const char *bind_addr;
    uint16_t port;  /* replica transport port, 0 = ephemeral */
    uint64_t nonce; /* process-life nonce for the controller session */
    uint32_t ncontrollers;
    const char *ctrl_addrs[HX_RAFT_MAX_NODES];
    uint16_t ctrl_ports[HX_RAFT_MAX_NODES];
    uint32_t min_isr;
    uint32_t lag_ms;
    uint32_t fetch_ms;
    hx_log_opts log_opts; /* used for every partition log */
    bool use_log_opts;
} hx_rmgr_cfg;

hx_rmgr *hx_rmgr_create(const hx_rmgr_cfg *cfg);
void hx_rmgr_destroy(hx_rmgr *m);
uint16_t hx_rmgr_port(const hx_rmgr *m);
int hx_rmgr_set_peer(hx_rmgr *m, uint32_t id, const char *addr, uint16_t port);

/* 0 on success; -1 after the first poll, on a duplicate, or if the log cannot be opened. */
int hx_rmgr_add_partition(hx_rmgr *m, uint32_t part, uint32_t replicas);

void hx_rmgr_poll(hx_rmgr *m, int timeout_ms, int64_t now_ms);

/* Leader-side produce (see hx_repl_append). */
hx_repl_status hx_rmgr_append(hx_rmgr *m, uint32_t part, int64_t now_ms, const hx_repl_rec *recs,
                              size_t n, uint64_t *first);

typedef struct {
    hx_repl_role role;
    uint32_t epoch;
    uint32_t leader;
    uint32_t isr;
    uint64_t hw;
    uint64_t leo;
    bool failed;
} hx_rmgr_part_info;

int hx_rmgr_info(const hx_rmgr *m, uint32_t part, hx_rmgr_part_info *out);

/* Read a committed record's payload (offset < high watermark). Same contract as hx_log_read:
 * HX_ERR_NOT_FOUND for an offset that is not committed (yet). */
hx_err hx_rmgr_read(hx_rmgr *m, uint32_t part, uint64_t offset, void *buf, size_t cap,
                    uint32_t *out_len);

uint64_t hx_rmgr_session(const hx_rmgr *m);

#endif
