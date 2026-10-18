/*
 * Cluster controller state machine: who leads each partition, in which epoch, with which ISR.
 *
 * Pure and deterministic like hx_raft and hx_repl: no I/O, no clock, no randomness. Commands are
 * replicated through Raft; every controller node applies the committed commands in log order and
 * so computes the same state. The host that runs the Raft leader also acts as the failure detector
 * (it proposes BROKER_DOWN when heartbeats stop) and distributes hx_ctrl_get() results to brokers.
 *
 * Sessions. BROKER_UP gives a broker a session id equal to the Raft index it was applied at, so ids
 * only grow. State sent to a broker is tagged with that broker's session, and the broker passes it
 * to hx_repl_set_state() only if it equals the session it registered in this process life. That is
 * the "session opened after the replica instance was created" contract of repl.h.
 *
 * Safety rules enforced here:
 *   - a leader is only ever chosen from the current ISR (no unclean election); if no ISR member is
 *     alive the partition has no leader until one registers;
 *   - an election removes dead brokers from the ISR, bumps the epoch and the ISR version;
 *   - ALTER_ISR is applied only if it comes from the current leader's live session, names the
 *     current epoch and ISR version, keeps the leader in the set, stays within the replica set
 *     and adds no dead broker.
 */
#ifndef HELIX_CTRL_H
#define HELIX_CTRL_H

#include <stddef.h>
#include <stdint.h>

#include "util/buf.h"

#define HX_CTRL_MAX_BROKERS    16U
#define HX_CTRL_MAX_PARTITIONS 4096U
#define HX_CTRL_NONE           UINT32_MAX

typedef enum {
    HX_CTRL_CREATE = 1,      /* part, replicas */
    HX_CTRL_BROKER_UP = 2,   /* broker, nonce */
    HX_CTRL_BROKER_DOWN = 3, /* broker, session */
    HX_CTRL_ALTER_ISR = 4    /* part, broker, session, epoch, isr, isr_version */
} hx_ctrl_cmd_type;

typedef struct {
    hx_ctrl_cmd_type type;
    uint32_t part;
    uint32_t broker;
    uint32_t replicas;
    uint64_t session;
    uint64_t nonce; /* BROKER_UP: random value of the broker's process life */
    uint32_t epoch;
    uint32_t isr;
    uint32_t isr_version;
} hx_ctrl_cmd;

typedef enum {
    HX_CTRL_OK = 0,       /* state changed */
    HX_CTRL_NOOP = 1,     /* valid but nothing to do (e.g. same ISR) */
    HX_CTRL_REJECTED = 2, /* well-formed but stale or not allowed in the current state */
    HX_CTRL_BAD = 3       /* malformed or out of range */
} hx_ctrl_result;

typedef struct {
    uint32_t replicas;
    uint32_t epoch;
    uint32_t leader; /* HX_CTRL_NONE when unavailable */
    uint32_t isr;
    uint32_t isr_version;
} hx_ctrl_part;

typedef struct hx_ctrl hx_ctrl;

hx_ctrl *hx_ctrl_create(void);
void hx_ctrl_destroy(hx_ctrl *c);

/* Apply one committed command. `index` is its Raft log index and must grow with every call. */
hx_ctrl_result hx_ctrl_apply(hx_ctrl *c, uint64_t index, const hx_ctrl_cmd *m);
hx_ctrl_result hx_ctrl_apply_bytes(hx_ctrl *c, uint64_t index, const uint8_t *p, size_t len);

int hx_ctrl_cmd_encode(const hx_ctrl_cmd *m, hx_buf *out);
int hx_ctrl_cmd_decode(const uint8_t *p, size_t len, hx_ctrl_cmd *m);

size_t hx_ctrl_nparts(const hx_ctrl *c);
int hx_ctrl_get(const hx_ctrl *c, uint32_t part, hx_ctrl_part *out);
uint32_t hx_ctrl_alive(const hx_ctrl *c);                    /* bit per registered broker */
uint64_t hx_ctrl_session(const hx_ctrl *c, uint32_t broker); /* 0 if never registered */
/* Nonce the broker presented when it registered. A broker may adopt a session only if this equals
 * the nonce of its own process life: otherwise the session was created for a previous life. */
uint64_t hx_ctrl_nonce(const hx_ctrl *c, uint32_t broker);
uint64_t hx_ctrl_generation(const hx_ctrl *c); /* bumps whenever any state visible above changes */

/* Self-check of the structural invariants; 0 if fine. Used by tests and after restore. */
int hx_ctrl_check(const hx_ctrl *c);

/* Serialise the whole state (for a Raft snapshot) and rebuild from it. restore() validates. */
int hx_ctrl_snapshot(const hx_ctrl *c, hx_buf *out);
int hx_ctrl_restore(hx_ctrl *c, const uint8_t *p, size_t len);

#endif
