/*
 * Broker <-> controller channel over TCP.
 *
 * Controller side (hx_ctlsrv): listens for broker connections and serves them through an hx_chost.
 * Broker side (hx_ctlcli): keeps one connection to every controller node, says hello to all of them
 * periodically, and turns what it hears into validated partition state.
 *
 * Frames are `u32 length | u8 type | body`, big-endian, limited to 64 KiB:
 *   broker -> controller   HELLO  : u32 broker | u64 nonce
 *                          ALTER  : u32 part | u32 broker | u64 session | u32 epoch | u32 isr |
 *                                   u32 isr_version
 *   controller -> broker   SESSION: u64 nonce | u64 session
 *                          STATE  : u32 part | u64 session | u32 epoch | u32 leader | u32 isr |
 *                                   u32 isr_version
 *                          NOT_READY : u32 leader_hint   (answer to HELLO from a node that is not a
 *                                      ready leader)
 *
 * What makes this safe (see ADR-021/022):
 *   - the broker adopts a session only from a SESSION whose nonce is its own process life's nonce;
 *   - STATE reaches the replication layer only if it carries the adopted session, which is what
 *     hx_repl_set_state() requires. Old or duplicate STATE (even from a deposed controller leader)
 *     can still arrive; hx_repl ignores anything older than what it has;
 *   - only a ready controller leader answers hellos and pushes STATE;
 *   - the controller takes a broker's identity from its HELLO on that connection and refuses ALTER
 *     frames claiming another broker.
 * There is no TLS or authentication: run it on a trusted network.
 */
#ifndef HELIX_CTRL_NET_H
#define HELIX_CTRL_NET_H

#include "ctrl/ctrl_host.h"

/* ---------------------------------------------------------------- server */

typedef struct hx_ctlsrv hx_ctlsrv;

typedef struct {
    const char *bind_addr;
    uint16_t port;      /* 0 = ephemeral */
    uint32_t push_ms;   /* resend full state to every broker at least this often, default 500 */
    uint32_t max_conns; /* default 64 */
} hx_ctlsrv_cfg;

/* `host` must outlive the server. */
hx_ctlsrv *hx_ctlsrv_create(const hx_ctlsrv_cfg *cfg, hx_chost *host);
void hx_ctlsrv_destroy(hx_ctlsrv *s);
uint16_t hx_ctlsrv_port(const hx_ctlsrv *s);

/* I/O for up to timeout_ms, then push changed state to connected brokers. */
void hx_ctlsrv_poll(hx_ctlsrv *s, int timeout_ms, int64_t now_ms);

typedef struct {
    uint64_t hellos;
    uint64_t alters;
    uint64_t pushes;
    uint64_t bad_conns; /* closed for bad frames, bad identity, or overload */
} hx_ctlsrv_stats;

hx_ctlsrv_stats hx_ctlsrv_get_stats(const hx_ctlsrv *s);

/* ---------------------------------------------------------------- client */

typedef struct hx_ctlcli hx_ctlcli;

/* Validated state for one partition (session already checked). */
typedef void (*hx_ctlcli_on_state)(void *ctx, uint32_t part, const hx_ctrl_part *st);

typedef struct {
    uint32_t broker;
    uint64_t nonce; /* unique per process life: random or boot time + pid */
    uint32_t ncontrollers;
    const char *addrs[HX_RAFT_MAX_NODES]; /* dotted IPv4 */
    uint16_t ports[HX_RAFT_MAX_NODES];
    uint32_t hello_ms; /* default 100 */
    hx_ctlcli_on_state on_state;
    void *ctx;
} hx_ctlcli_cfg;

hx_ctlcli *hx_ctlcli_create(const hx_ctlcli_cfg *cfg);
void hx_ctlcli_destroy(hx_ctlcli *c);

/* Wait up to timeout_ms for I/O, send due hellos, deliver state. */
void hx_ctlcli_poll(hx_ctlcli *c, int timeout_ms, int64_t now_ms);

uint64_t hx_ctlcli_session(const hx_ctlcli *c); /* 0 until registered */

/* Forward a leader's ISR proposal to every connected controller node (only the ready leader acts).
 * Returns the number of nodes it was queued to. */
int hx_ctlcli_alter(hx_ctlcli *c, uint32_t part, uint32_t epoch, uint32_t isr,
                    uint32_t isr_version);

#endif
