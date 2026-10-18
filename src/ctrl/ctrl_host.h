/*
 * Controller host: runs one hx_ctrl on top of one hx_raft node.
 *
 * Still a pure state machine in the sense that it owns no sockets, threads or clock: the host
 * program feeds time, Raft messages and broker requests in, and gets Raft messages out through the
 * `send` callback. What this module adds on top of hx_raft and hx_ctrl:
 *   - applies committed Raft entries to the controller in order, takes Raft snapshots of it, and
 *     rebuilds it when the leader installs a snapshot;
 *   - on the leader, acts as the failure detector: broker hellos are heartbeats, and a broker that
 *     is silent for `down_ms` gets a BROKER_DOWN proposal;
 *   - serves broker requests (hello, alter_isr) only while it is a *ready* leader: it has committed
 *     an entry in its own term and applied everything committed, so its view is not stale.
 *
 * Heartbeat times are volatile and live only on the leader. A new leader starts every broker's
 * clock afresh, so a controller failover never marks brokers dead by itself.
 */
#ifndef HELIX_CTRL_HOST_H
#define HELIX_CTRL_HOST_H

#include "ctrl/ctrl.h"
#include "raft/raft.h"

typedef struct {
    hx_raft_cfg raft;
    void *send_ctx;
    /* Called right after each Raft call for every message it produced; msg is valid only during the
     * call. */
    void (*send)(void *ctx, const hx_raft_msg *msg);
    uint32_t down_ms;        /* silence after which a broker is declared down */
    uint32_t retry_ms;       /* minimum gap between repeated proposals for the same broker */
    uint32_t snapshot_every; /* applied entries between Raft snapshots, 0 = never */
} hx_chost_cfg;

typedef struct hx_chost hx_chost;

void hx_chost_cfg_default(hx_chost_cfg *cfg, uint32_t id, uint32_t nnodes);
hx_chost *hx_chost_create(const hx_chost_cfg *cfg, const hx_raft_restore *restore, int64_t now_ms);
void hx_chost_destroy(hx_chost *h);

int hx_chost_tick(hx_chost *h, int64_t now_ms);
int hx_chost_step(hx_chost *h, int64_t now_ms, const hx_raft_msg *m);

typedef enum {
    HX_CHOST_OK = 0,
    HX_CHOST_NOT_READY = 1, /* not a ready leader: try another controller node (see leader hint) */
    HX_CHOST_FAILED = 2     /* the host has failed and must be restarted */
} hx_chost_status;

/*
 * Broker heartbeat/registration. `nonce` identifies the broker's process life. On OK, *session is
 * the session registered under exactly this nonce, or 0 if registration is still in flight (the
 * broker keeps sending hellos). The broker may adopt only a non-zero session returned here.
 */
hx_chost_status hx_chost_hello(hx_chost *h, int64_t now_ms, uint32_t broker, uint64_t nonce,
                               uint64_t *session);

/* Leader's ISR proposal, forwarded verbatim. OK means "proposed"; the answer is the next state. */
hx_chost_status hx_chost_alter_isr(hx_chost *h, int64_t now_ms, uint32_t part, uint32_t broker,
                                   uint64_t session, uint32_t epoch, uint32_t isr,
                                   uint32_t isr_version);

/* Admin: create a partition (dense ids). */
hx_chost_status hx_chost_create_partition(hx_chost *h, int64_t now_ms, uint32_t part,
                                          uint32_t replicas);

bool hx_chost_ready(const hx_chost *h);
uint32_t hx_chost_leader_hint(const hx_chost *h); /* HX_RAFT_NONE if unknown */
const hx_ctrl *hx_chost_ctrl(const hx_chost *h);  /* state as of the last applied entry */
uint64_t hx_chost_applied(const hx_chost *h);
bool hx_chost_failed(const hx_chost *h);
const hx_raft *hx_chost_raft(const hx_chost *h);

#endif
