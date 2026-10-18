#include "repl/replica_mgr.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

typedef struct {
    uint32_t part;
    hx_plog *log;
    hx_repl *repl;
} rpart;

struct hx_rmgr {
    hx_rmgr_cfg cfg;
    hx_replnet *net;
    hx_ctlcli *cli;
    rpart *parts;
    size_t n;
    size_t cap;
    bool started;
    int64_t now; /* time of the poll or call in progress, for callbacks that carry none */
};

static rpart *find(hx_rmgr *m, uint32_t part)
{
    for (size_t i = 0; i < m->n; i++) {
        if (m->parts[i].part == part) {
            return &m->parts[i];
        }
    }
    return NULL;
}

static void flush_out(hx_rmgr *m, rpart *p)
{
    const hx_repl_msg *msgs = NULL;
    size_t n = 0;
    hx_repl_take_msgs(p->repl, &msgs, &n);
    for (size_t i = 0; i < n; i++) {
        if (msgs[i].to == HX_REPL_CONTROLLER) {
            if (msgs[i].type == HX_REPL_MSG_ALTER_ISR) {
                (void)hx_ctlcli_alter(m->cli, p->part, msgs[i].epoch, msgs[i].isr,
                                      msgs[i].isr_version);
            }
        } else {
            (void)hx_replnet_send(m->net, p->part, &msgs[i], m->now);
        }
    }
}

static void on_msg(void *ctx, uint32_t part, const hx_repl_msg *msg)
{
    hx_rmgr *m = ctx;
    rpart *p = find(m, part);
    if (p != NULL) {
        (void)hx_repl_step(p->repl, m->now, msg);
        flush_out(m, p);
    }
}

static void on_state(void *ctx, uint32_t part, const hx_ctrl_part *st)
{
    hx_rmgr *m = ctx;
    rpart *p = find(m, part);
    if (p != NULL) {
        (void)hx_repl_set_state(p->repl, m->now, st->epoch, st->leader, st->isr, st->isr_version);
        flush_out(m, p);
    }
}

hx_rmgr *hx_rmgr_create(const hx_rmgr_cfg *cfg)
{
    if (cfg == NULL || cfg->data_dir == NULL || cfg->bind_addr == NULL) {
        return NULL;
    }
    if (mkdir(cfg->data_dir, 0755) != 0 && errno != EEXIST) {
        return NULL;
    }
    hx_rmgr *m = calloc(1, sizeof *m);
    if (m == NULL) {
        return NULL;
    }
    m->cfg = *cfg;
    hx_replnet_cfg nc;
    memset(&nc, 0, sizeof nc);
    nc.self = cfg->self;
    nc.nbrokers = cfg->nbrokers;
    nc.bind_addr = cfg->bind_addr;
    nc.port = cfg->port;
    nc.on_msg = on_msg;
    nc.ctx = m;
    m->net = hx_replnet_create(&nc);
    hx_ctlcli_cfg cc;
    memset(&cc, 0, sizeof cc);
    cc.broker = cfg->self;
    cc.nonce = cfg->nonce;
    cc.ncontrollers = cfg->ncontrollers;
    for (uint32_t i = 0; i < cfg->ncontrollers; i++) {
        cc.addrs[i] = cfg->ctrl_addrs[i];
        cc.ports[i] = cfg->ctrl_ports[i];
    }
    cc.on_state = on_state;
    cc.ctx = m;
    m->cli = m->net != NULL ? hx_ctlcli_create(&cc) : NULL;
    if (m->net == NULL || m->cli == NULL || cfg->nonce == 0) {
        hx_rmgr_destroy(m);
        return NULL;
    }
    return m;
}

void hx_rmgr_destroy(hx_rmgr *m)
{
    if (m == NULL) {
        return;
    }
    for (size_t i = 0; i < m->n; i++) {
        hx_repl_destroy(m->parts[i].repl);
        hx_plog_close(m->parts[i].log);
    }
    hx_ctlcli_destroy(m->cli);
    hx_replnet_destroy(m->net);
    free(m->parts);
    free(m);
}

uint16_t hx_rmgr_port(const hx_rmgr *m)
{
    return hx_replnet_port(m->net);
}

int hx_rmgr_set_peer(hx_rmgr *m, uint32_t id, const char *addr, uint16_t port)
{
    return hx_replnet_set_peer(m->net, id, addr, port);
}

int hx_rmgr_add_partition(hx_rmgr *m, uint32_t part, uint32_t replicas)
{
    if (m->started || find(m, part) != NULL || (replicas & (UINT32_C(1) << m->cfg.self)) == 0) {
        return -1;
    }
    if (m->n == m->cap) {
        const size_t ncap = m->cap == 0 ? 8U : m->cap * 2U;
        rpart *np = realloc(m->parts, ncap * sizeof *np);
        if (np == NULL) {
            return -1;
        }
        m->parts = np;
        m->cap = ncap;
    }
    char dir[512];
    (void)snprintf(dir, sizeof dir, "%s/p%u", m->cfg.data_dir, part);
    rpart *p = &m->parts[m->n];
    memset(p, 0, sizeof *p);
    p->part = part;
    if (hx_plog_open(dir, m->cfg.use_log_opts ? &m->cfg.log_opts : NULL, &p->log) != HX_OK) {
        return -1;
    }
    hx_repl_cfg rc;
    hx_repl_cfg_default(&rc, m->cfg.self, replicas);
    if (m->cfg.min_isr != 0) {
        rc.min_isr = m->cfg.min_isr;
    }
    if (m->cfg.lag_ms != 0) {
        rc.lag_ms = m->cfg.lag_ms;
    }
    if (m->cfg.fetch_ms != 0) {
        rc.fetch_ms = m->cfg.fetch_ms;
    }
    rc.log = hx_plog_vtable(p->log);
    p->repl = hx_repl_create(&rc, 0);
    if (p->repl == NULL) {
        hx_plog_close(p->log);
        return -1;
    }
    m->n++;
    return 0;
}

void hx_rmgr_poll(hx_rmgr *m, int timeout_ms, int64_t now_ms)
{
    m->started = true;
    m->now = now_ms;
    hx_ctlcli_poll(m->cli, 0, now_ms);
    (void)hx_replnet_poll(m->net, timeout_ms, now_ms);
    for (size_t i = 0; i < m->n; i++) {
        (void)hx_repl_tick(m->parts[i].repl, now_ms);
        flush_out(m, &m->parts[i]);
    }
}

hx_repl_status hx_rmgr_append(hx_rmgr *m, uint32_t part, int64_t now_ms, const hx_repl_rec *recs,
                              size_t n, uint64_t *first)
{
    rpart *p = find(m, part);
    if (p == NULL) {
        return HX_REPL_BAD_ARG;
    }
    m->now = now_ms;
    const hx_repl_status st = hx_repl_append(p->repl, now_ms, recs, n, first);
    flush_out(m, p);
    return st;
}

int hx_rmgr_info(const hx_rmgr *m, uint32_t part, hx_rmgr_part_info *out)
{
    for (size_t i = 0; i < m->n; i++) {
        if (m->parts[i].part == part) {
            const hx_repl *r = m->parts[i].repl;
            out->role = hx_repl_get_role(r);
            out->epoch = hx_repl_epoch(r);
            out->leader = hx_repl_leader(r);
            out->isr = hx_repl_isr(r);
            out->hw = hx_repl_hw(r);
            out->leo = hx_repl_leo(r);
            out->failed = hx_repl_failed(r);
            return 0;
        }
    }
    return -1;
}

hx_err hx_rmgr_read(hx_rmgr *m, uint32_t part, uint64_t offset, void *buf, size_t cap,
                    uint32_t *out_len)
{
    rpart *p = find(m, part);
    if (p == NULL) {
        return HX_ERR_INVAL;
    }
    if (offset >= hx_repl_hw(p->repl)) {
        return HX_ERR_NOT_FOUND;
    }
    return hx_plog_read_payload(p->log, offset, buf, cap, out_len, NULL);
}

uint64_t hx_rmgr_session(const hx_rmgr *m)
{
    return hx_ctlcli_session(m->cli);
}
