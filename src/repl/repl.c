#include "repl/repl.h"

#include <stdlib.h>
#include <string.h>

struct repl_block {
    struct repl_block *next;
    hx_repl_rec recs[];
};

struct hx_repl {
    hx_repl_cfg cfg;
    hx_repl_role role;
    uint32_t epoch;
    uint32_t leader;
    uint32_t isr;
    uint32_t isr_version;
    uint64_t hw;
    uint64_t
        epoch_start; /* leader: log end when this epoch began (everything committed is below it) */
    int64_t now;
    bool failed;

    /* leader-only knowledge about followers; reset on every new epoch */
    uint64_t f_leo[HX_REPL_MAX_REPLICAS]; /* log end the follower proved it has */
    uint64_t
        f_leo_seen[HX_REPL_MAX_REPLICAS]; /* leader's log end at the follower's previous fetch */
    int64_t f_last_fetch[HX_REPL_MAX_REPLICAS];
    int64_t f_caught_up[HX_REPL_MAX_REPLICAS];
    bool f_fetched[HX_REPL_MAX_REPLICAS];
    bool pending; /* an ISR change was proposed and not yet answered */
    uint32_t pending_isr;
    int64_t pending_at;

    /* follower-only */
    bool inflight;
    int64_t inflight_at;
    int64_t next_fetch_at;

    hx_repl_msg *out;
    size_t nout;
    size_t capout;
    struct repl_block *blocks; /* per-response record copies, freed by the next call */
    hx_repl_rec *tmp;          /* scratch for log reads */
};

static uint32_t popcount32(uint32_t v)
{
    uint32_t n = 0;
    while (v != 0) {
        v &= v - 1U;
        n++;
    }
    return n;
}

static uint32_t bit(uint32_t i)
{
    return 1U << i;
}

void hx_repl_cfg_default(hx_repl_cfg *cfg, uint32_t id, uint32_t replicas)
{
    memset(cfg, 0, sizeof *cfg);
    cfg->id = id;
    cfg->replicas = replicas;
    cfg->min_isr = 1;
    cfg->lag_ms = 500;
    cfg->fetch_ms = 20;
    cfg->alter_retry_ms = 100;
    cfg->max_fetch_records = 64;
    cfg->max_fetch_bytes = 1U << 20;
}

hx_repl *hx_repl_create(const hx_repl_cfg *cfg, int64_t now_ms)
{
    if (cfg == NULL || cfg->id >= HX_REPL_MAX_REPLICAS || (cfg->replicas & bit(cfg->id)) == 0 ||
        (cfg->replicas >> HX_REPL_MAX_REPLICAS) != 0 || cfg->min_isr == 0 || cfg->lag_ms == 0 ||
        cfg->fetch_ms == 0 || cfg->alter_retry_ms == 0 || cfg->max_fetch_records == 0 ||
        cfg->max_fetch_bytes == 0 || cfg->log.leo == NULL || cfg->log.epoch_at == NULL ||
        cfg->log.end_offset_for_epoch == NULL || cfg->log.append == NULL ||
        cfg->log.truncate == NULL || cfg->log.read == NULL || cfg->log.sync == NULL) {
        return NULL;
    }
    hx_repl *r = calloc(1, sizeof *r);
    if (r == NULL) {
        return NULL;
    }
    r->cfg = *cfg;
    r->leader = HX_REPL_NONE;
    r->now = now_ms;
    r->tmp = malloc((size_t)cfg->max_fetch_records * sizeof *r->tmp);
    if (r->tmp == NULL) {
        free(r);
        return NULL;
    }
    return r;
}

static void free_blocks(hx_repl *r)
{
    while (r->blocks != NULL) {
        struct repl_block *next = r->blocks->next;
        free(r->blocks);
        r->blocks = next;
    }
}

void hx_repl_destroy(hx_repl *r)
{
    if (r == NULL) {
        return;
    }
    free_blocks(r);
    free(r->out);
    free(r->tmp);
    free(r);
}

static uint64_t leo(const hx_repl *r)
{
    return r->cfg.log.leo(r->cfg.log.ctx);
}

static int begin(hx_repl *r, int64_t now_ms)
{
    free_blocks(r);
    r->nout = 0;
    if (r->failed) {
        return -1;
    }
    if (now_ms > r->now) {
        r->now = now_ms;
    }
    return 0;
}

static int finish(hx_repl *r)
{
    if (r->failed) {
        free_blocks(r);
        r->nout = 0;
        return -1;
    }
    return 0;
}

static hx_repl_msg *out_new(hx_repl *r, uint32_t to, hx_repl_msg_type type)
{
    if (r->nout == r->capout) {
        const size_t ncap = r->capout == 0 ? 8U : r->capout * 2U;
        hx_repl_msg *m = realloc(r->out, ncap * sizeof *m);
        if (m == NULL) {
            r->failed = true;
            return NULL;
        }
        r->out = m;
        r->capout = ncap;
    }
    hx_repl_msg *m = &r->out[r->nout++];
    memset(m, 0, sizeof *m);
    m->type = type;
    m->from = r->cfg.id;
    m->to = to;
    m->epoch = r->epoch;
    return m;
}

static void storage_fail(hx_repl *r, int rc)
{
    if (rc != 0) {
        r->failed = true;
    }
}

/* ------------------------------------------------------------- leader side */

static void recalc_hw(hx_repl *r)
{
    if (r->role != HX_REPL_LEADER) {
        return;
    }
    /* A pending ISR change counts as already in force for the HW: if the controller approved it and
     * the answer was lost, the new ISR must not contain a member the HW has run past. */
    const uint32_t members = r->isr | (r->pending ? r->pending_isr : 0U) | bit(r->cfg.id);
    uint64_t m = leo(r);
    for (uint32_t f = 0; f < HX_REPL_MAX_REPLICAS; f++) {
        if ((members & bit(f)) != 0 && f != r->cfg.id && r->f_leo[f] < m) {
            m = r->f_leo[f];
        }
    }
    if (m > r->hw) {
        r->hw = m;
    }
}

static void send_alter_isr(hx_repl *r)
{
    hx_repl_msg *m = out_new(r, HX_REPL_CONTROLLER, HX_REPL_MSG_ALTER_ISR);
    if (m != NULL) {
        m->isr = r->pending_isr;
        m->isr_version = r->isr_version;
    }
    r->pending_at = r->now;
}

static void maintain_isr(hx_repl *r)
{
    if (r->role != HX_REPL_LEADER) {
        return;
    }
    if (r->pending) {
        if (r->now - r->pending_at >= (int64_t)r->cfg.alter_retry_ms) {
            send_alter_isr(r);
        }
        return;
    }
    uint32_t shrink = 0;
    uint32_t expand = 0;
    for (uint32_t f = 0; f < HX_REPL_MAX_REPLICAS; f++) {
        if ((r->cfg.replicas & bit(f)) == 0 || f == r->cfg.id) {
            continue;
        }
        if ((r->isr & bit(f)) != 0) {
            if (r->now - r->f_caught_up[f] > (int64_t)r->cfg.lag_ms) {
                shrink |= bit(f);
            }
        } else if (r->f_fetched[f] && r->f_leo[f] >= r->hw && r->f_leo[f] >= r->epoch_start &&
                   r->now - r->f_last_fetch[f] <= (int64_t)r->cfg.lag_ms) {
            expand |= bit(f);
        }
    }
    const uint32_t next = (r->isr & ~shrink) | expand | bit(r->cfg.id);
    if (next != r->isr) {
        r->pending = true;
        r->pending_isr = next;
        send_alter_isr(r);
    }
}

static void become_leader(hx_repl *r)
{
    r->role = HX_REPL_LEADER;
    r->pending = false;
    r->inflight = false;
    for (uint32_t f = 0; f < HX_REPL_MAX_REPLICAS; f++) {
        r->f_leo[f] = 0; /* nothing proven about any follower in this epoch yet */
        r->f_leo_seen[f] = 0;
        r->f_last_fetch[f] = r->now;
        r->f_caught_up[f] = r->now; /* grace period before a silent follower is dropped */
        r->f_fetched[f] = false;
    }
    const uint64_t l = leo(r);
    if (r->hw > l) {
        r->hw = l;
    }
    /* The HW is volatile: after a restart it may be far below what was really committed. The
     * leader's own log end is not, so a follower joins the ISR only once it has everything the
     * leader had when this epoch began (Kafka's rule for the same reason). */
    r->epoch_start = l;
    recalc_hw(r); /* a single-member ISR commits at once */
}

static void on_fetch(hx_repl *r, const hx_repl_msg *m)
{
    hx_repl_msg *resp = out_new(r, m->from, HX_REPL_MSG_FETCH_RESP);
    if (resp == NULL) {
        return;
    }
    resp->fetch_offset = m->fetch_offset;
    if (r->role != HX_REPL_LEADER || m->from >= HX_REPL_MAX_REPLICAS ||
        (r->cfg.replicas & bit(m->from)) == 0 || m->epoch > r->epoch) {
        resp->status = HX_REPL_RESP_NOT_LEADER;
        return;
    }
    if (m->epoch < r->epoch) {
        resp->status = HX_REPL_RESP_FENCED;
        return;
    }
    const uint64_t l = leo(r);
    bool match = m->fetch_offset == 0;
    if (!match && m->fetch_offset <= l) {
        uint32_t e = 0;
        storage_fail(r, r->cfg.log.epoch_at(r->cfg.log.ctx, m->fetch_offset - 1U, &e));
        match = !r->failed && e == m->last_fetched_epoch;
    }
    if (r->failed) {
        return;
    }
    if (!match) {
        const uint64_t end = r->cfg.log.end_offset_for_epoch(r->cfg.log.ctx, m->last_fetched_epoch);
        uint32_t de = 0;
        if (end > 0) {
            storage_fail(r, r->cfg.log.epoch_at(r->cfg.log.ctx, end - 1U, &de));
        }
        resp->status = HX_REPL_RESP_DIVERGED;
        resp->div_epoch = de;
        resp->div_end = end;
        return;
    }
    const uint32_t f = m->from;
    if (m->fetch_offset > r->f_leo[f]) {
        r->f_leo[f] = m->fetch_offset;
    }
    r->f_fetched[f] = true;
    r->f_last_fetch[f] = r->now;
    if (m->fetch_offset >= l || m->fetch_offset >= r->f_leo_seen[f]) {
        r->f_caught_up[f] = r->now;
    }
    r->f_leo_seen[f] = l;
    recalc_hw(r);

    size_t n = 0;
    if (m->fetch_offset < l) {
        storage_fail(r, r->cfg.log.read(r->cfg.log.ctx, m->fetch_offset, r->cfg.max_fetch_records,
                                        r->cfg.max_fetch_bytes, r->tmp, &n));
        if (r->failed) {
            return;
        }
    }
    resp->status = HX_REPL_RESP_OK;
    resp->hw = r->hw;
    if (n != 0) {
        size_t bytes = n * sizeof(hx_repl_rec);
        for (size_t i = 0; i < n; i++) {
            bytes += r->tmp[i].len;
        }
        struct repl_block *blk = malloc(sizeof *blk + n * sizeof(hx_repl_rec) + bytes);
        if (blk == NULL) {
            r->failed = true;
            return;
        }
        blk->next = r->blocks;
        r->blocks = blk;
        hx_repl_rec *recs = blk->recs;
        uint8_t *dst = (uint8_t *)&recs[n];
        for (size_t i = 0; i < n; i++) {
            recs[i].epoch = r->tmp[i].epoch;
            recs[i].len = r->tmp[i].len;
            recs[i].data = dst;
            if (r->tmp[i].len != 0) {
                memcpy(dst, r->tmp[i].data, r->tmp[i].len);
            }
            dst += r->tmp[i].len;
        }
        resp->recs = recs;
        resp->nrecs = (uint32_t)n;
    }
    maintain_isr(r);
}

hx_repl_status hx_repl_append(hx_repl *r, int64_t now_ms, const hx_repl_rec *recs, size_t n,
                              uint64_t *first)
{
    if (begin(r, now_ms) != 0) {
        return HX_REPL_FAILED;
    }
    if (r->role != HX_REPL_LEADER) {
        return HX_REPL_NOT_LEADER;
    }
    if (n == 0 || recs == NULL) {
        return HX_REPL_BAD_ARG;
    }
    if (popcount32(r->isr) < r->cfg.min_isr) {
        return HX_REPL_NOT_ENOUGH_REPLICAS;
    }
    hx_repl_rec *t = malloc(n * sizeof *t);
    if (t == NULL) {
        r->failed = true;
        return HX_REPL_FAILED;
    }
    for (size_t i = 0; i < n; i++) {
        t[i].epoch = r->epoch;
        t[i].len = recs[i].len;
        t[i].data = recs[i].data;
    }
    const uint64_t at = leo(r);
    storage_fail(r, r->cfg.log.append(r->cfg.log.ctx, t, n));
    free(t);
    if (!r->failed) {
        storage_fail(
            r, r->cfg.log.sync(r->cfg.log.ctx)); /* the leader counts itself only when durable */
    }
    if (finish(r) != 0) {
        return HX_REPL_FAILED;
    }
    recalc_hw(r);
    if (first != NULL) {
        *first = at;
    }
    return HX_REPL_OK;
}

/* ----------------------------------------------------------- follower side */

static void send_fetch(hx_repl *r)
{
    if (r->role != HX_REPL_FOLLOWER || r->leader == HX_REPL_NONE) {
        return;
    }
    const uint64_t l = leo(r);
    uint32_t le = 0;
    if (l > 0) {
        storage_fail(r, r->cfg.log.epoch_at(r->cfg.log.ctx, l - 1U, &le));
        if (r->failed) {
            return;
        }
    }
    hx_repl_msg *m = out_new(r, r->leader, HX_REPL_MSG_FETCH);
    if (m != NULL) {
        m->fetch_offset = l;
        m->last_fetched_epoch = le;
    }
    r->inflight = true;
    r->inflight_at = r->now;
    r->next_fetch_at = r->now + (int64_t)r->cfg.fetch_ms;
}

static void on_fetch_resp(hx_repl *r, const hx_repl_msg *m)
{
    if (r->role != HX_REPL_FOLLOWER || m->from != r->leader || m->epoch != r->epoch ||
        m->fetch_offset != leo(r)) {
        return; /* stale, duplicated or from a deposed leader */
    }
    r->inflight = false;
    const uint64_t l = leo(r);
    switch (m->status) {
    case HX_REPL_RESP_OK: {
        uint64_t nl = l;
        if (m->nrecs != 0) {
            storage_fail(r, r->cfg.log.append(r->cfg.log.ctx, m->recs, m->nrecs));
            if (!r->failed) {
                storage_fail(r, r->cfg.log.sync(r->cfg.log.ctx));
            }
            if (r->failed) {
                return;
            }
            nl = l + m->nrecs;
        }
        const uint64_t h = m->hw < nl ? m->hw : nl;
        if (h > r->hw) {
            r->hw = h;
        }
        if (m->nrecs != 0) {
            send_fetch(r); /* more may be waiting: do not idle */
        } else {
            r->next_fetch_at = r->now + (int64_t)r->cfg.fetch_ms;
        }
        break;
    }
    case HX_REPL_RESP_DIVERGED: {
        const uint64_t fe = r->cfg.log.end_offset_for_epoch(r->cfg.log.ctx, m->div_epoch);
        uint64_t target = l < m->div_end ? l : m->div_end;
        if (fe < target) {
            target = fe;
        }
        if (target < r->hw) {
            r->failed = true; /* would drop data that was already committed: refuse to continue */
            return;
        }
        if (target < l) {
            storage_fail(r, r->cfg.log.truncate(r->cfg.log.ctx, target));
            if (!r->failed) {
                storage_fail(r, r->cfg.log.sync(r->cfg.log.ctx));
            }
            if (!r->failed) {
                send_fetch(r);
            }
        } else {
            r->next_fetch_at = r->now + (int64_t)r->cfg.fetch_ms; /* nothing to cut: do not spin */
        }
        break;
    }
    default:
        r->next_fetch_at = r->now + (int64_t)r->cfg.fetch_ms;
        break;
    }
}

/* -------------------------------------------------------- controller input */

int hx_repl_set_state(hx_repl *r, int64_t now_ms, uint32_t epoch, uint32_t leader, uint32_t isr,
                      uint32_t isr_version)
{
    if (begin(r, now_ms) != 0) {
        return -1;
    }
    if (epoch < r->epoch) {
        return finish(r);
    }
    if (epoch == r->epoch) {
        if (isr_version > r->isr_version && leader == r->leader) {
            r->isr = isr;
            r->isr_version = isr_version;
            r->pending = false; /* the answer to our proposal, or someone else's newer view */
            recalc_hw(r);
            maintain_isr(r);
        }
        return finish(r);
    }
    r->epoch = epoch;
    r->leader = leader;
    r->isr = isr;
    r->isr_version = isr_version;
    r->pending = false;
    r->inflight = false;
    const uint64_t l = leo(r);
    if (r->hw > l) {
        r->hw = l;
    }
    if ((r->cfg.replicas & bit(r->cfg.id)) == 0 || leader == HX_REPL_NONE) {
        r->role = HX_REPL_ROLE_NONE;
    } else if (leader == r->cfg.id) {
        become_leader(r);
    } else {
        r->role = HX_REPL_FOLLOWER;
        send_fetch(r);
    }
    return finish(r);
}

int hx_repl_tick(hx_repl *r, int64_t now_ms)
{
    if (begin(r, now_ms) != 0) {
        return -1;
    }
    if (r->role == HX_REPL_FOLLOWER) {
        if ((!r->inflight && r->now >= r->next_fetch_at) ||
            (r->inflight && r->now - r->inflight_at >= 4 * (int64_t)r->cfg.fetch_ms)) {
            send_fetch(r);
        }
    } else if (r->role == HX_REPL_LEADER) {
        maintain_isr(r);
    }
    return finish(r);
}

int hx_repl_step(hx_repl *r, int64_t now_ms, const hx_repl_msg *m)
{
    if (begin(r, now_ms) != 0) {
        return -1;
    }
    if (m->to == r->cfg.id) {
        if (m->type == HX_REPL_MSG_FETCH) {
            on_fetch(r, m);
        } else if (m->type == HX_REPL_MSG_FETCH_RESP) {
            on_fetch_resp(r, m);
        }
    }
    return finish(r);
}

void hx_repl_take_msgs(hx_repl *r, const hx_repl_msg **msgs, size_t *n)
{
    *msgs = r->out;
    *n = r->nout;
}

hx_repl_role hx_repl_get_role(const hx_repl *r)
{
    return r->role;
}

uint32_t hx_repl_epoch(const hx_repl *r)
{
    return r->epoch;
}

uint32_t hx_repl_leader(const hx_repl *r)
{
    return r->leader;
}

uint32_t hx_repl_isr(const hx_repl *r)
{
    return r->isr;
}

uint32_t hx_repl_isr_version(const hx_repl *r)
{
    return r->isr_version;
}

uint64_t hx_repl_hw(const hx_repl *r)
{
    return r->hw;
}

uint64_t hx_repl_leo(const hx_repl *r)
{
    return leo(r);
}

bool hx_repl_failed(const hx_repl *r)
{
    return r->failed;
}
