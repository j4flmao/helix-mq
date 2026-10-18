#include "ctrl/ctrl.h"

#include <stdlib.h>
#include <string.h>

#include "proto/codec.h"

struct hx_ctrl {
    hx_ctrl_part *parts;
    size_t nparts;
    size_t cap;
    uint32_t alive;
    uint64_t session[HX_CTRL_MAX_BROKERS];
    uint64_t nonce[HX_CTRL_MAX_BROKERS];
    uint64_t last_index;
    uint64_t generation;
};

static uint32_t bit(uint32_t i)
{
    return UINT32_C(1) << i;
}

static uint32_t all_brokers(void)
{
    return bit(HX_CTRL_MAX_BROKERS) - 1U;
}

hx_ctrl *hx_ctrl_create(void)
{
    return calloc(1, sizeof(hx_ctrl));
}

void hx_ctrl_destroy(hx_ctrl *c)
{
    if (c != NULL) {
        free(c->parts);
        free(c);
    }
}

size_t hx_ctrl_nparts(const hx_ctrl *c)
{
    return c->nparts;
}

int hx_ctrl_get(const hx_ctrl *c, uint32_t part, hx_ctrl_part *out)
{
    if (part >= c->nparts) {
        return -1;
    }
    *out = c->parts[part];
    return 0;
}

uint32_t hx_ctrl_alive(const hx_ctrl *c)
{
    return c->alive;
}

uint64_t hx_ctrl_session(const hx_ctrl *c, uint32_t broker)
{
    return broker < HX_CTRL_MAX_BROKERS ? c->session[broker] : 0;
}

uint64_t hx_ctrl_nonce(const hx_ctrl *c, uint32_t broker)
{
    return broker < HX_CTRL_MAX_BROKERS ? c->nonce[broker] : 0;
}

uint64_t hx_ctrl_generation(const hx_ctrl *c)
{
    return c->generation;
}

/* Pick a new leader from ISR ∩ alive. The pick depends only on the Raft index, so every node
 * computes the same one while load still spreads over the candidates. */
static void elect(hx_ctrl *c, hx_ctrl_part *p, uint64_t index)
{
    const uint32_t cands = p->isr & c->alive;
    uint32_t leader = HX_CTRL_NONE;
    if (cands != 0) {
        uint32_t pick = (uint32_t)(index % (uint64_t)__builtin_popcount(cands));
        for (uint32_t i = 0; i < HX_CTRL_MAX_BROKERS; i++) {
            if ((cands & bit(i)) != 0 && pick-- == 0) {
                leader = i;
                break;
            }
        }
        p->isr = cands; /* dead members leave the ISR together with the epoch change */
    }
    if (leader == HX_CTRL_NONE && p->leader == HX_CTRL_NONE) {
        return; /* still unavailable: nothing changed */
    }
    p->leader = leader;
    p->epoch++;
    p->isr_version++;
    c->generation++;
}

static hx_ctrl_result do_create(hx_ctrl *c, uint64_t index, const hx_ctrl_cmd *m)
{
    if (m->replicas == 0 || (m->replicas & ~all_brokers()) != 0) {
        return HX_CTRL_BAD;
    }
    if (m->part < c->nparts) {
        return HX_CTRL_REJECTED; /* already exists */
    }
    if (m->part != c->nparts || m->part >= HX_CTRL_MAX_PARTITIONS) {
        return HX_CTRL_BAD; /* ids are dense */
    }
    if (c->nparts == c->cap) {
        const size_t ncap = c->cap == 0 ? 16U : c->cap * 2U;
        hx_ctrl_part *np = realloc(c->parts, ncap * sizeof *np);
        if (np == NULL) {
            abort(); /* a controller that cannot apply a committed command must not continue */
        }
        c->parts = np;
        c->cap = ncap;
    }
    hx_ctrl_part *p = &c->parts[c->nparts++];
    memset(p, 0, sizeof *p);
    p->replicas = m->replicas;
    p->isr = m->replicas;
    p->leader = HX_CTRL_NONE;
    c->generation++;
    elect(c, p, index);
    return HX_CTRL_OK;
}

static hx_ctrl_result do_up(hx_ctrl *c, uint64_t index, const hx_ctrl_cmd *m)
{
    if (m->broker >= HX_CTRL_MAX_BROKERS) {
        return HX_CTRL_BAD;
    }
    if ((c->alive & bit(m->broker)) != 0 && c->nonce[m->broker] == m->nonce) {
        return HX_CTRL_NOOP; /* duplicate registration of a life that already has its session */
    }
    c->alive |= bit(m->broker);
    c->session[m->broker] = index;
    c->nonce[m->broker] = m->nonce;
    c->generation++;
    /* A re-registration of a live leader does not change leadership: its new process life
     * rejoins the same epoch, which hx_repl handles (epoch_start rule). */
    for (size_t i = 0; i < c->nparts; i++) {
        hx_ctrl_part *p = &c->parts[i];
        if (p->leader == HX_CTRL_NONE && (p->isr & bit(m->broker)) != 0) {
            elect(c, p, index);
        }
    }
    return HX_CTRL_OK;
}

static hx_ctrl_result do_down(hx_ctrl *c, uint64_t index, const hx_ctrl_cmd *m)
{
    if (m->broker >= HX_CTRL_MAX_BROKERS) {
        return HX_CTRL_BAD;
    }
    if ((c->alive & bit(m->broker)) == 0 || c->session[m->broker] != m->session) {
        return HX_CTRL_REJECTED; /* stale report about a previous session */
    }
    c->alive &= ~bit(m->broker);
    c->generation++;
    for (size_t i = 0; i < c->nparts; i++) {
        hx_ctrl_part *p = &c->parts[i];
        if (p->leader == m->broker) {
            elect(c, p, index);
        }
    }
    return HX_CTRL_OK;
}

static hx_ctrl_result do_alter(hx_ctrl *c, const hx_ctrl_cmd *m)
{
    if (m->part >= c->nparts || m->broker >= HX_CTRL_MAX_BROKERS) {
        return HX_CTRL_BAD;
    }
    hx_ctrl_part *p = &c->parts[m->part];
    if ((c->alive & bit(m->broker)) == 0 || c->session[m->broker] != m->session ||
        p->leader != m->broker || p->epoch != m->epoch || p->isr_version != m->isr_version) {
        return HX_CTRL_REJECTED;
    }
    const uint32_t added = m->isr & ~p->isr;
    if (m->isr == 0 || (m->isr & bit(m->broker)) == 0 || (m->isr & ~p->replicas) != 0 ||
        (added & ~c->alive) != 0) {
        return HX_CTRL_REJECTED;
    }
    if (m->isr == p->isr) {
        return HX_CTRL_NOOP;
    }
    p->isr = m->isr;
    p->isr_version++;
    c->generation++;
    return HX_CTRL_OK;
}

hx_ctrl_result hx_ctrl_apply(hx_ctrl *c, uint64_t index, const hx_ctrl_cmd *m)
{
    if (index <= c->last_index) {
        return HX_CTRL_BAD; /* indexes must grow; replaying a command would diverge replicas */
    }
    c->last_index = index;
    switch (m->type) {
    case HX_CTRL_CREATE:
        return do_create(c, index, m);
    case HX_CTRL_BROKER_UP:
        return do_up(c, index, m);
    case HX_CTRL_BROKER_DOWN:
        return do_down(c, index, m);
    case HX_CTRL_ALTER_ISR:
        return do_alter(c, m);
    default:
        return HX_CTRL_BAD;
    }
}

hx_ctrl_result hx_ctrl_apply_bytes(hx_ctrl *c, uint64_t index, const uint8_t *p, size_t len)
{
    hx_ctrl_cmd m;
    if (hx_ctrl_cmd_decode(p, len, &m) != 0) {
        if (index > c->last_index) {
            c->last_index = index;
        }
        return HX_CTRL_BAD;
    }
    return hx_ctrl_apply(c, index, &m);
}

/* ------------------------------------------------------------------ codec */

int hx_ctrl_cmd_encode(const hx_ctrl_cmd *m, hx_buf *out)
{
    int bad = hx_buf_put_u8(out, (uint8_t)m->type) != 0;
    switch (m->type) {
    case HX_CTRL_CREATE:
        bad = bad || hx_buf_put_u32(out, m->part) != 0 || hx_buf_put_u32(out, m->replicas) != 0;
        break;
    case HX_CTRL_BROKER_UP:
        bad = bad || hx_buf_put_u32(out, m->broker) != 0 || hx_buf_put_u64(out, m->nonce) != 0;
        break;
    case HX_CTRL_BROKER_DOWN:
        bad = bad || hx_buf_put_u32(out, m->broker) != 0 || hx_buf_put_u64(out, m->session) != 0;
        break;
    case HX_CTRL_ALTER_ISR:
        bad = bad || hx_buf_put_u32(out, m->part) != 0 || hx_buf_put_u32(out, m->broker) != 0 ||
              hx_buf_put_u64(out, m->session) != 0 || hx_buf_put_u32(out, m->epoch) != 0 ||
              hx_buf_put_u32(out, m->isr) != 0 || hx_buf_put_u32(out, m->isr_version) != 0;
        break;
    default:
        bad = 1;
        break;
    }
    return bad ? -1 : 0;
}

int hx_ctrl_cmd_decode(const uint8_t *p, size_t len, hx_ctrl_cmd *m)
{
    hx_rd rd;
    hx_rd_init(&rd, p, len);
    memset(m, 0, sizeof *m);
    const uint8_t type = hx_rd_u8(&rd);
    if (!rd.ok || type < HX_CTRL_CREATE || type > HX_CTRL_ALTER_ISR) {
        return -1;
    }
    m->type = (hx_ctrl_cmd_type)type;
    switch (m->type) {
    case HX_CTRL_CREATE:
        m->part = hx_rd_u32(&rd);
        m->replicas = hx_rd_u32(&rd);
        break;
    case HX_CTRL_BROKER_UP:
        m->broker = hx_rd_u32(&rd);
        m->nonce = hx_rd_u64(&rd);
        break;
    case HX_CTRL_BROKER_DOWN:
        m->broker = hx_rd_u32(&rd);
        m->session = hx_rd_u64(&rd);
        break;
    default:
        m->part = hx_rd_u32(&rd);
        m->broker = hx_rd_u32(&rd);
        m->session = hx_rd_u64(&rd);
        m->epoch = hx_rd_u32(&rd);
        m->isr = hx_rd_u32(&rd);
        m->isr_version = hx_rd_u32(&rd);
        break;
    }
    return rd.ok && hx_rd_left(&rd) == 0 ? 0 : -1;
}

/* --------------------------------------------------------------- invariants */

int hx_ctrl_check(const hx_ctrl *c)
{
    if ((c->alive & ~all_brokers()) != 0) {
        return -1;
    }
    for (uint32_t b = 0; b < HX_CTRL_MAX_BROKERS; b++) {
        if ((c->alive & bit(b)) != 0 && c->session[b] == 0) {
            return -2;
        }
    }
    for (size_t i = 0; i < c->nparts; i++) {
        const hx_ctrl_part *p = &c->parts[i];
        if (p->replicas == 0 || (p->replicas & ~all_brokers()) != 0 || p->isr == 0 ||
            (p->isr & ~p->replicas) != 0) {
            return -3;
        }
        if (p->leader != HX_CTRL_NONE &&
            (p->leader >= HX_CTRL_MAX_BROKERS || (p->isr & bit(p->leader)) == 0 ||
             (c->alive & bit(p->leader)) == 0)) {
            return -4;
        }
    }
    return 0;
}

/* ---------------------------------------------------------------- snapshot */

#define SNAP_MAGIC 0x48584354U /* "HXCT" */

int hx_ctrl_snapshot(const hx_ctrl *c, hx_buf *out)
{
    int bad = hx_buf_put_u32(out, SNAP_MAGIC) != 0 || hx_buf_put_u64(out, c->last_index) != 0 ||
              hx_buf_put_u64(out, c->generation) != 0 || hx_buf_put_u32(out, c->alive) != 0 ||
              hx_buf_put_u32(out, (uint32_t)c->nparts) != 0;
    for (uint32_t b = 0; b < HX_CTRL_MAX_BROKERS && !bad; b++) {
        bad = hx_buf_put_u64(out, c->session[b]) != 0 || hx_buf_put_u64(out, c->nonce[b]) != 0;
    }
    for (size_t i = 0; i < c->nparts && !bad; i++) {
        const hx_ctrl_part *p = &c->parts[i];
        bad = hx_buf_put_u32(out, p->replicas) != 0 || hx_buf_put_u32(out, p->epoch) != 0 ||
              hx_buf_put_u32(out, p->leader) != 0 || hx_buf_put_u32(out, p->isr) != 0 ||
              hx_buf_put_u32(out, p->isr_version) != 0;
    }
    return bad ? -1 : 0;
}

int hx_ctrl_restore(hx_ctrl *c, const uint8_t *p, size_t len)
{
    hx_rd rd;
    hx_rd_init(&rd, p, len);
    if (hx_rd_u32(&rd) != SNAP_MAGIC) {
        return -1;
    }
    hx_ctrl tmp;
    memset(&tmp, 0, sizeof tmp);
    tmp.last_index = hx_rd_u64(&rd);
    tmp.generation = hx_rd_u64(&rd);
    tmp.alive = hx_rd_u32(&rd);
    const uint32_t n = hx_rd_u32(&rd);
    for (uint32_t b = 0; b < HX_CTRL_MAX_BROKERS; b++) {
        tmp.session[b] = hx_rd_u64(&rd);
        tmp.nonce[b] = hx_rd_u64(&rd);
    }
    if (!rd.ok || n > HX_CTRL_MAX_PARTITIONS || hx_rd_left(&rd) != (size_t)n * 20U) {
        return -1;
    }
    tmp.parts = n == 0 ? NULL : calloc(n, sizeof *tmp.parts);
    if (n != 0 && tmp.parts == NULL) {
        return -1;
    }
    tmp.nparts = n;
    tmp.cap = n;
    for (uint32_t i = 0; i < n; i++) {
        tmp.parts[i].replicas = hx_rd_u32(&rd);
        tmp.parts[i].epoch = hx_rd_u32(&rd);
        tmp.parts[i].leader = hx_rd_u32(&rd);
        tmp.parts[i].isr = hx_rd_u32(&rd);
        tmp.parts[i].isr_version = hx_rd_u32(&rd);
    }
    if (!rd.ok || hx_ctrl_check(&tmp) != 0) {
        free(tmp.parts);
        return -1;
    }
    free(c->parts);
    *c = tmp;
    return 0;
}
