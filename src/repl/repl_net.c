#include "repl/repl_net.h"

#include <stdlib.h>
#include <string.h>

#define MAGIC_REPL 0x48585031U /* "HXP1" */
#define MAX_RECS   1024U       /* records per message the receiver accepts */

struct hx_replnet {
    hx_replnet_cfg cfg;
    hx_raftnet *net;
    hx_repl_rec *scratch;
    uint64_t undecodable;
    uint64_t wrong_dest;
};

static void on_frame(void *ctx, uint32_t from, const uint8_t *p, size_t len)
{
    hx_replnet *n = ctx;
    if (len < 4U) {
        n->undecodable++;
        return;
    }
    const uint32_t part =
        ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
    hx_repl_msg m;
    if (hx_repl_msg_decode(p + 4, len - 4U, n->scratch, MAX_RECS, &m) != 0) {
        n->undecodable++;
        return;
    }
    if (m.from != from) { /* the hello says who is on the other end: do not trust the body */
        n->undecodable++;
        return;
    }
    if (m.to != n->cfg.self) {
        n->wrong_dest++;
        return;
    }
    n->cfg.on_msg(n->cfg.ctx, part, &m);
}

hx_replnet *hx_replnet_create(const hx_replnet_cfg *cfg)
{
    if (cfg == NULL || cfg->on_msg == NULL || cfg->nbrokers == 0 ||
        cfg->nbrokers > HX_REPL_MAX_REPLICAS) {
        return NULL;
    }
    hx_replnet *n = calloc(1, sizeof *n);
    if (n == NULL) {
        return NULL;
    }
    n->cfg = *cfg;
    n->scratch = malloc(MAX_RECS * sizeof *n->scratch);
    hx_raftnet_cfg rc;
    memset(&rc, 0, sizeof rc);
    rc.self = cfg->self;
    rc.nnodes = cfg->nbrokers;
    rc.bind_addr = cfg->bind_addr;
    rc.port = cfg->port;
    rc.max_frame = cfg->max_frame;
    rc.max_queue = cfg->max_queue;
    rc.magic = MAGIC_REPL;
    rc.on_frame = on_frame;
    rc.ctx = n;
    if (n->scratch != NULL) {
        n->net = hx_raftnet_create(&rc);
    }
    if (n->net == NULL) {
        hx_replnet_destroy(n);
        return NULL;
    }
    return n;
}

void hx_replnet_destroy(hx_replnet *n)
{
    if (n != NULL) {
        hx_raftnet_destroy(n->net);
        free(n->scratch);
        free(n);
    }
}

uint16_t hx_replnet_port(const hx_replnet *n)
{
    return hx_raftnet_port(n->net);
}

int hx_replnet_set_peer(hx_replnet *n, uint32_t id, const char *addr, uint16_t port)
{
    return hx_raftnet_set_peer(n->net, id, addr, port);
}

int hx_replnet_send(hx_replnet *n, uint32_t part, const hx_repl_msg *m, int64_t now_ms)
{
    if (m->to >= n->cfg.nbrokers || m->to == n->cfg.self) {
        return -1;
    }
    hx_buf b;
    hx_buf_init(&b);
    int rc = -1;
    if (hx_buf_put_u32(&b, part) == 0 && hx_repl_msg_encode(m, &b) == 0) {
        rc = hx_raftnet_send(n->net, m->to, b.data, b.len, now_ms);
    }
    hx_buf_free(&b);
    return rc;
}

int hx_replnet_poll(hx_replnet *n, int timeout_ms, int64_t now_ms)
{
    return hx_raftnet_poll(n->net, timeout_ms, now_ms);
}

hx_replnet_stats hx_replnet_get_stats(const hx_replnet *n)
{
    hx_replnet_stats s;
    s.net = hx_raftnet_get_stats(n->net);
    s.undecodable = n->undecodable;
    s.wrong_dest = n->wrong_dest;
    return s;
}
