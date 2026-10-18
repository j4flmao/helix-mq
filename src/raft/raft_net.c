#include "raft/raft_net.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "util/buf.h"

#define HELLO_MAGIC       0x48585231U /* "HXR1" */
#define HELLO_LEN         12U
#define DEFAULT_MAX_FRAME (8U << 20)
#define DEFAULT_MAX_QUEUE (4U << 20)
#define MAX_INBOUND       64U
#define BACKOFF_MIN_MS    100
#define BACKOFF_MAX_MS    2000

typedef enum { K_LISTEN, K_OUT, K_IN } kind;

typedef struct {
    kind kind;
    int fd;
    uint32_t peer; /* K_OUT: target; K_IN: sender after hello */
    bool connecting;
    bool hello_ok;
    bool want_out; /* EPOLLOUT currently registered */
    hx_buf in;
    hx_buf out;
} conn;

typedef struct {
    struct sockaddr_in addr;
    bool known;
    conn *c;          /* current outbound connection or NULL */
    int64_t retry_at; /* do not reconnect before this */
    int64_t backoff;
} peer;

struct hx_raftnet {
    hx_raftnet_cfg cfg;
    int ep;
    conn *lst;
    conn *inbound[MAX_INBOUND];
    peer peers[HX_RAFT_MAX_NODES];
    uint16_t port;
    hx_raftnet_stats st;
    int64_t now;
};

static void put32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static uint32_t get32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

static conn *conn_new(kind k, int fd)
{
    conn *c = calloc(1, sizeof *c);
    if (c == NULL) {
        return NULL;
    }
    c->kind = k;
    c->fd = fd;
    c->peer = HX_RAFT_NONE;
    hx_buf_init(&c->in);
    hx_buf_init(&c->out);
    return c;
}

static int ep_ctl(hx_raftnet *n, int op, conn *c, uint32_t events)
{
    struct epoll_event ev = {.events = events, .data = {.ptr = c}};
    return epoll_ctl(n->ep, op, c->fd, &ev);
}

static void set_want_out(hx_raftnet *n, conn *c, bool want)
{
    if (c->want_out != want) {
        c->want_out = want;
        (void)ep_ctl(n, EPOLL_CTL_MOD, c, EPOLLIN | (want ? EPOLLOUT : 0U));
    }
}

/* Closes and frees; detaches it from its slot. */
static void conn_close(hx_raftnet *n, conn *c)
{
    (void)epoll_ctl(n->ep, EPOLL_CTL_DEL, c->fd, NULL);
    (void)close(c->fd);
    if (c->kind == K_OUT && c->peer < HX_RAFT_MAX_NODES) {
        peer *p = &n->peers[c->peer];
        p->c = NULL;
        p->retry_at = n->now + p->backoff;
        p->backoff = p->backoff * 2 > BACKOFF_MAX_MS ? BACKOFF_MAX_MS : p->backoff * 2;
    } else if (c->kind == K_IN) {
        for (size_t i = 0; i < MAX_INBOUND; i++) {
            if (n->inbound[i] == c) {
                n->inbound[i] = NULL;
            }
        }
    }
    hx_buf_free(&c->in);
    hx_buf_free(&c->out);
    free(c);
}

/* Writes as much as the socket takes. -1 = connection is dead. */
static int flush(hx_raftnet *n, conn *c)
{
    while (c->out.len != 0) {
        const ssize_t w = send(c->fd, c->out.data, c->out.len, MSG_NOSIGNAL);
        if (w > 0) {
            hx_buf_consume(&c->out, (size_t)w);
        } else if (w < 0 && errno == EINTR) {
            continue;
        } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            set_want_out(n, c, true);
            return 0;
        } else {
            return -1;
        }
    }
    set_want_out(n, c, false);
    hx_buf_shrink(&c->out, 4096);
    return 0;
}

static int start_connect(hx_raftnet *n, uint32_t id)
{
    peer *p = &n->peers[id];
    const int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }
    const int one = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    conn *c = conn_new(K_OUT, fd);
    if (c == NULL) {
        (void)close(fd);
        return -1;
    }
    c->peer = id;
    const int rc = connect(fd, (const struct sockaddr *)&p->addr, sizeof p->addr);
    if (rc != 0 && errno != EINPROGRESS) {
        p->retry_at = n->now + p->backoff;
        p->backoff = p->backoff * 2 > BACKOFF_MAX_MS ? BACKOFF_MAX_MS : p->backoff * 2;
        (void)close(fd);
        free(c);
        return -1;
    }
    c->connecting = rc != 0;
    uint8_t hello[4U + HELLO_LEN]; /* framed like everything else */
    put32(hello, HELLO_LEN);
    put32(hello + 4, n->cfg.magic);
    put32(hello + 8, n->cfg.self);
    put32(hello + 12, n->cfg.nnodes);
    if (hx_buf_put(&c->out, hello, sizeof hello) != 0 ||
        ep_ctl(n, EPOLL_CTL_ADD, c, EPOLLIN | EPOLLOUT) != 0) {
        (void)close(fd);
        hx_buf_free(&c->out);
        free(c);
        return -1;
    }
    c->want_out = true;
    p->c = c;
    return 0;
}

hx_raftnet *hx_raftnet_create(const hx_raftnet_cfg *cfg)
{
    if (cfg == NULL || cfg->nnodes == 0 || cfg->nnodes > HX_RAFT_MAX_NODES ||
        cfg->self >= cfg->nnodes || cfg->on_frame == NULL || cfg->bind_addr == NULL) {
        return NULL;
    }
    hx_raftnet *n = calloc(1, sizeof *n);
    if (n == NULL) {
        return NULL;
    }
    n->cfg = *cfg;
    if (n->cfg.magic == 0) {
        n->cfg.magic = HELLO_MAGIC;
    }
    if (n->cfg.max_frame == 0) {
        n->cfg.max_frame = DEFAULT_MAX_FRAME;
    }
    if (n->cfg.max_queue == 0) {
        n->cfg.max_queue = DEFAULT_MAX_QUEUE;
    }
    n->ep = epoll_create1(EPOLL_CLOEXEC);
    for (uint32_t i = 0; i < HX_RAFT_MAX_NODES; i++) {
        n->peers[i].backoff = BACKOFF_MIN_MS;
    }
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons(cfg->port);
    const int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    socklen_t alen = sizeof a;
    const int one = 1;
    if (n->ep < 0 || fd < 0 || inet_pton(AF_INET, cfg->bind_addr, &a.sin_addr) != 1) {
        goto fail;
    }
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (bind(fd, (struct sockaddr *)&a, sizeof a) != 0 || listen(fd, 64) != 0 ||
        getsockname(fd, (struct sockaddr *)&a, &alen) != 0) {
        goto fail;
    }
    n->port = ntohs(a.sin_port);
    n->lst = conn_new(K_LISTEN, fd);
    if (n->lst == NULL || ep_ctl(n, EPOLL_CTL_ADD, n->lst, EPOLLIN) != 0) {
        goto fail;
    }
    return n;
fail:
    if (fd >= 0 && (n->lst == NULL)) {
        (void)close(fd);
    }
    if (n->ep >= 0) {
        (void)close(n->ep);
    }
    if (n->lst != NULL) {
        (void)close(n->lst->fd);
        free(n->lst);
    }
    free(n);
    return NULL;
}

void hx_raftnet_destroy(hx_raftnet *n)
{
    if (n == NULL) {
        return;
    }
    for (uint32_t i = 0; i < HX_RAFT_MAX_NODES; i++) {
        if (n->peers[i].c != NULL) {
            conn_close(n, n->peers[i].c);
        }
    }
    for (size_t i = 0; i < MAX_INBOUND; i++) {
        if (n->inbound[i] != NULL) {
            conn_close(n, n->inbound[i]);
        }
    }
    (void)close(n->lst->fd);
    free(n->lst);
    (void)close(n->ep);
    free(n);
}

uint16_t hx_raftnet_port(const hx_raftnet *n)
{
    return n->port;
}

hx_raftnet_stats hx_raftnet_get_stats(const hx_raftnet *n)
{
    return n->st;
}

int hx_raftnet_set_peer(hx_raftnet *n, uint32_t id, const char *addr, uint16_t port)
{
    if (id >= n->cfg.nnodes || id == n->cfg.self || addr == NULL) {
        return -1;
    }
    peer *p = &n->peers[id];
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    if (inet_pton(AF_INET, addr, &a.sin_addr) != 1) {
        return -1;
    }
    if (p->c != NULL) {
        conn_close(n, p->c);
    }
    p->addr = a;
    p->known = true;
    p->retry_at = 0;
    p->backoff = BACKOFF_MIN_MS;
    return 0;
}

int hx_raftnet_send(hx_raftnet *n, uint32_t to, const void *payload, size_t len, int64_t now_ms)
{
    n->now = now_ms;
    if (to >= n->cfg.nnodes || to == n->cfg.self || !n->peers[to].known || len > n->cfg.max_frame) {
        n->st.dropped++;
        return -1;
    }
    peer *p = &n->peers[to];
    if (p->c == NULL) {
        if (now_ms < p->retry_at || start_connect(n, to) != 0) {
            n->st.dropped++;
            return -1;
        }
    }
    conn *c = p->c;
    if (c->out.len + len + 4U > n->cfg.max_queue) {
        n->st.dropped++;
        return -1;
    }
    uint8_t h[4];
    put32(h, (uint32_t)len);
    if (hx_buf_put(&c->out, h, sizeof h) != 0 || hx_buf_put(&c->out, payload, len) != 0) {
        conn_close(n, c);
        n->st.dropped++;
        return -1;
    }
    n->st.frames_out++;
    if (!c->connecting && flush(n, c) != 0) {
        conn_close(n, c);
    }
    return 0;
}

/* Parses complete frames from an inbound connection. -1 = drop the connection. */
static int drain_inbound(hx_raftnet *n, conn *c, int *delivered)
{
    size_t off = 0;
    int rc = 0;
    while (c->in.len - off >= 4U) {
        const uint32_t flen = get32(c->in.data + off);
        if (!c->hello_ok ? flen != HELLO_LEN : flen > n->cfg.max_frame) {
            rc = -1;
            break;
        }
        if (c->in.len - off - 4U < flen) {
            break;
        }
        const uint8_t *body = c->in.data + off + 4U;
        if (!c->hello_ok) {
            if (get32(body) != n->cfg.magic || get32(body + 8) != n->cfg.nnodes ||
                get32(body + 4) >= n->cfg.nnodes || get32(body + 4) == n->cfg.self) {
                rc = -1;
                break;
            }
            c->peer = get32(body + 4);
            c->hello_ok = true;
        } else {
            n->st.frames_in++;
            (*delivered)++;
            n->cfg.on_frame(n->cfg.ctx, c->peer, body, flen);
        }
        off += 4U + flen;
    }
    hx_buf_consume(&c->in, off);
    return rc;
}

/* Returns true if the connection was closed (and freed). */
static bool on_readable(hx_raftnet *n, conn *c, int *delivered)
{
    uint8_t tmp[16384];
    for (;;) {
        const ssize_t r = recv(c->fd, tmp, sizeof tmp, 0);
        if (r > 0) {
            if (c->kind == K_OUT) { /* nobody sends on this direction: protocol violation */
                conn_close(n, c);
                return true;
            }
            if (hx_buf_put(&c->in, tmp, (size_t)r) != 0 || drain_inbound(n, c, delivered) != 0) {
                n->st.bad_peers++;
                conn_close(n, c);
                return true;
            }
            continue;
        }
        if (r < 0 && errno == EINTR) {
            continue;
        }
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return false;
        }
        if (c->kind == K_IN && c->in.len != 0) {
            n->st.bad_peers++; /* closed in the middle of a frame */
        }
        conn_close(n, c);
        return true;
    }
}

static void on_event(hx_raftnet *n, conn *c, uint32_t ev, int *delivered)
{
    if (c->kind == K_LISTEN) {
        for (;;) {
            const int fd = accept4(c->fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) {
                return;
            }
            size_t slot = MAX_INBOUND;
            for (size_t i = 0; i < MAX_INBOUND; i++) {
                if (n->inbound[i] == NULL) {
                    slot = i;
                    break;
                }
            }
            conn *ic = slot < MAX_INBOUND ? conn_new(K_IN, fd) : NULL;
            const int one = 1;
            (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            if (ic == NULL || ep_ctl(n, EPOLL_CTL_ADD, ic, EPOLLIN) != 0) {
                n->st.bad_peers++;
                (void)close(fd);
                free(ic);
                continue;
            }
            n->inbound[slot] = ic;
        }
    }
    if (c->kind == K_OUT && c->connecting && (ev & (EPOLLOUT | EPOLLERR | EPOLLHUP)) != 0) {
        int err = 0;
        socklen_t el = sizeof err;
        if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &el) != 0 || err != 0) {
            conn_close(n, c);
            return;
        }
        c->connecting = false;
        n->peers[c->peer].backoff = BACKOFF_MIN_MS;
        n->st.connects++;
    }
    if ((ev & (EPOLLERR | EPOLLHUP)) != 0 && (ev & EPOLLIN) == 0) {
        conn_close(n, c);
        return;
    }
    if ((ev & EPOLLIN) != 0) {
        if (on_readable(n, c, delivered) || (ev & EPOLLOUT) == 0 || c->kind == K_IN) {
            return;
        }
    }
    if (c->kind == K_OUT && !c->connecting && (ev & EPOLLOUT) != 0 && flush(n, c) != 0) {
        conn_close(n, c);
    }
}

int hx_raftnet_poll(hx_raftnet *n, int timeout_ms, int64_t now_ms)
{
    n->now = now_ms;
    struct epoll_event evs[32];
    const int cnt = epoll_wait(n->ep, evs, 32, timeout_ms);
    int delivered = 0;
    /* Collect the connections first: handling one may close another. */
    conn *batch[32];
    uint32_t bev[32];
    int m = 0;
    for (int i = 0; i < cnt; i++) {
        batch[m] = evs[i].data.ptr;
        bev[m++] = evs[i].events;
    }
    for (int i = 0; i < m; i++) {
        /* A conn closed earlier in this batch must not be touched: verify it is still registered.
         */
        bool alive = batch[i] == n->lst;
        for (size_t k = 0; !alive && k < MAX_INBOUND; k++) {
            alive = n->inbound[k] == batch[i];
        }
        for (uint32_t k = 0; !alive && k < n->cfg.nnodes; k++) {
            alive = n->peers[k].c == batch[i];
        }
        if (alive) {
            on_event(n, batch[i], bev[i], &delivered);
        }
    }
    return delivered;
}
