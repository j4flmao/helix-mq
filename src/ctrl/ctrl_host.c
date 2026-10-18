#include "ctrl/ctrl_host.h"

#include <stdlib.h>
#include <string.h>

struct hx_chost {
    hx_chost_cfg cfg;
    hx_raft *raft;
    hx_ctrl *ctrl;
    uint64_t applied;
    bool failed;
    bool was_ready;
    uint64_t ready_term; /* last term in which this node, as leader, saw a commit of its own term */
    int64_t last_hb[HX_CTRL_MAX_BROKERS];
    int64_t down_sent[HX_CTRL_MAX_BROKERS];
    int64_t up_sent[HX_CTRL_MAX_BROKERS];
    uint64_t up_nonce[HX_CTRL_MAX_BROKERS];
};

void hx_chost_cfg_default(hx_chost_cfg *cfg, uint32_t id, uint32_t nnodes)
{
    memset(cfg, 0, sizeof *cfg);
    hx_raft_cfg_default(&cfg->raft, id, nnodes);
    cfg->down_ms = 1500;
    cfg->retry_ms = 300;
    cfg->snapshot_every = 256;
}

static void flush(hx_chost *h)
{
    const hx_raft_msg *msgs = NULL;
    size_t n = 0;
    hx_raft_take_msgs(h->raft, &msgs, &n);
    if (h->cfg.send != NULL) {
        for (size_t i = 0; i < n; i++) {
            h->cfg.send(h->cfg.send_ctx, &msgs[i]);
        }
    }
}

hx_chost *hx_chost_create(const hx_chost_cfg *cfg, const hx_raft_restore *restore, int64_t now_ms)
{
    hx_chost *h = calloc(1, sizeof *h);
    if (h == NULL) {
        return NULL;
    }
    h->cfg = *cfg;
    h->ctrl = hx_ctrl_create();
    h->raft = hx_raft_create(&cfg->raft, restore, now_ms);
    if (h->ctrl == NULL || h->raft == NULL) {
        hx_chost_destroy(h);
        return NULL;
    }
    if (restore != NULL && restore->snap_index != 0) {
        if (hx_ctrl_restore(h->ctrl, restore->snap_data, restore->snap_len) != 0) {
            hx_chost_destroy(h);
            return NULL;
        }
        h->applied = restore->snap_index;
    }
    return h;
}

void hx_chost_destroy(hx_chost *h)
{
    if (h != NULL) {
        hx_raft_destroy(h->raft);
        hx_ctrl_destroy(h->ctrl);
        free(h);
    }
}

static void fail(hx_chost *h)
{
    h->failed = true;
}

bool hx_chost_ready(const hx_chost *h)
{
    return !h->failed && hx_raft_get_role(h->raft) == HX_RAFT_LEADER &&
           h->ready_term == hx_raft_term(h->raft) && h->applied == hx_raft_commit_index(h->raft);
}

/* Bring the controller up to the Raft commit index, handle snapshots, track leadership. */
static void pump(hx_chost *h, int64_t now)
{
    uint64_t sidx = 0;
    uint64_t sterm = 0;
    const uint8_t *sdata = NULL;
    uint32_t slen = 0;
    if (hx_raft_take_installed_snapshot(h->raft, &sidx, &sterm, &sdata, &slen)) {
        if (hx_ctrl_restore(h->ctrl, sdata, slen) != 0) {
            fail(h);
            return;
        }
        h->applied = sidx;
    }
    const uint64_t commit = hx_raft_commit_index(h->raft);
    while (h->applied < commit) {
        hx_raft_entry e;
        if (!hx_raft_entry_at(h->raft, h->applied + 1U, &e)) {
            fail(h);
            return;
        }
        (void)hx_ctrl_apply_bytes(h->ctrl, ++h->applied, e.data, e.len);
    }
    if (hx_raft_get_role(h->raft) == HX_RAFT_LEADER && commit != 0) {
        hx_raft_entry e;
        if (hx_raft_entry_at(h->raft, commit, &e) && e.term == hx_raft_term(h->raft)) {
            h->ready_term = e.term; /* checked before a snapshot can compact the entry away */
        }
    }
    if (h->cfg.snapshot_every != 0 &&
        h->applied - hx_raft_snapshot_index(h->raft) >= h->cfg.snapshot_every) {
        hx_buf b;
        hx_buf_init(&b);
        if (hx_ctrl_snapshot(h->ctrl, &b) != 0 ||
            hx_raft_snapshot(h->raft, now, h->applied, b.data, (uint32_t)b.len) != 0) {
            fail(h);
        }
        hx_buf_free(&b);
        flush(h);
    }
    const bool ready = hx_chost_ready(h);
    if (ready && !h->was_ready) {
        for (uint32_t b = 0; b < HX_CTRL_MAX_BROKERS; b++) { /* fresh grace period */
            h->last_hb[b] = now;
            h->down_sent[b] = INT64_MIN / 2;
            h->up_sent[b] = INT64_MIN / 2;
        }
    }
    h->was_ready = ready;
}

static bool propose(hx_chost *h, int64_t now, const hx_ctrl_cmd *m)
{
    hx_buf b;
    hx_buf_init(&b);
    uint64_t idx = 0;
    uint64_t term = 0;
    bool ok = false;
    if (hx_ctrl_cmd_encode(m, &b) == 0) {
        ok = hx_raft_propose(h->raft, now, b.data, (uint32_t)b.len, &idx, &term) == HX_RAFT_OK;
    }
    hx_buf_free(&b);
    flush(h);
    if (hx_raft_failed(h->raft)) {
        fail(h);
    }
    return ok;
}

int hx_chost_tick(hx_chost *h, int64_t now)
{
    if (h->failed) {
        return -1;
    }
    if (hx_raft_tick(h->raft, now) != 0) {
        fail(h);
    }
    flush(h);
    pump(h, now);
    if (!h->failed && hx_chost_ready(h)) {
        const uint32_t alive = hx_ctrl_alive(h->ctrl);
        for (uint32_t b = 0; b < HX_CTRL_MAX_BROKERS; b++) {
            if ((alive & (UINT32_C(1) << b)) != 0 &&
                now - h->last_hb[b] >= (int64_t)h->cfg.down_ms &&
                now - h->down_sent[b] >= (int64_t)h->cfg.retry_ms) {
                hx_ctrl_cmd m;
                memset(&m, 0, sizeof m);
                m.type = HX_CTRL_BROKER_DOWN;
                m.broker = b;
                m.session = hx_ctrl_session(h->ctrl, b);
                h->down_sent[b] = now;
                (void)propose(h, now, &m);
            }
        }
    }
    return h->failed ? -1 : 0;
}

int hx_chost_step(hx_chost *h, int64_t now, const hx_raft_msg *m)
{
    if (h->failed) {
        return -1;
    }
    if (hx_raft_step(h->raft, now, m) != 0) {
        fail(h);
    }
    flush(h);
    pump(h, now);
    return h->failed ? -1 : 0;
}

hx_chost_status hx_chost_hello(hx_chost *h, int64_t now, uint32_t broker, uint64_t nonce,
                               uint64_t *session)
{
    *session = 0;
    if (h->failed) {
        return HX_CHOST_FAILED;
    }
    if (!hx_chost_ready(h)) {
        return HX_CHOST_NOT_READY;
    }
    if (broker >= HX_CTRL_MAX_BROKERS) {
        return HX_CHOST_OK;
    }
    const bool alive = (hx_ctrl_alive(h->ctrl) & (UINT32_C(1) << broker)) != 0;
    if (alive && hx_ctrl_nonce(h->ctrl, broker) == nonce) {
        h->last_hb[broker] = now;
        *session = hx_ctrl_session(h->ctrl, broker);
        return HX_CHOST_OK;
    }
    /* Unknown, or a different process life, or marked down: register (again). */
    if (h->up_nonce[broker] != nonce || now - h->up_sent[broker] >= (int64_t)h->cfg.retry_ms) {
        hx_ctrl_cmd m;
        memset(&m, 0, sizeof m);
        m.type = HX_CTRL_BROKER_UP;
        m.broker = broker;
        m.nonce = nonce;
        h->up_nonce[broker] = nonce;
        h->up_sent[broker] = now;
        h->last_hb[broker] = now;
        (void)propose(h, now, &m);
        if (h->failed) {
            return HX_CHOST_FAILED;
        }
    }
    return HX_CHOST_OK;
}

hx_chost_status hx_chost_alter_isr(hx_chost *h, int64_t now, uint32_t part, uint32_t broker,
                                   uint64_t session, uint32_t epoch, uint32_t isr,
                                   uint32_t isr_version)
{
    if (h->failed) {
        return HX_CHOST_FAILED;
    }
    if (!hx_chost_ready(h)) {
        return HX_CHOST_NOT_READY;
    }
    hx_ctrl_cmd m;
    memset(&m, 0, sizeof m);
    m.type = HX_CTRL_ALTER_ISR;
    m.part = part;
    m.broker = broker;
    m.session = session;
    m.epoch = epoch;
    m.isr = isr;
    m.isr_version = isr_version;
    (void)propose(h, now, &m);
    return h->failed ? HX_CHOST_FAILED : HX_CHOST_OK;
}

hx_chost_status hx_chost_create_partition(hx_chost *h, int64_t now, uint32_t part,
                                          uint32_t replicas)
{
    if (h->failed) {
        return HX_CHOST_FAILED;
    }
    if (!hx_chost_ready(h)) {
        return HX_CHOST_NOT_READY;
    }
    hx_ctrl_cmd m;
    memset(&m, 0, sizeof m);
    m.type = HX_CTRL_CREATE;
    m.part = part;
    m.replicas = replicas;
    (void)propose(h, now, &m);
    return h->failed ? HX_CHOST_FAILED : HX_CHOST_OK;
}

uint32_t hx_chost_leader_hint(const hx_chost *h)
{
    return hx_raft_leader(h->raft);
}

const hx_ctrl *hx_chost_ctrl(const hx_chost *h)
{
    return h->ctrl;
}

uint64_t hx_chost_applied(const hx_chost *h)
{
    return h->applied;
}

bool hx_chost_failed(const hx_chost *h)
{
    return h->failed;
}

const hx_raft *hx_chost_raft(const hx_chost *h)
{
    return h->raft;
}
