#include "server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "proto/codec.h"
#include "proto/protocol.h"
#include "util/buf.h"
#include "util/logger.h"

#define HX_READ_CHUNK        65536U
#define HX_KEEP_CAP          ((size_t)262144)
#define HX_COMPACT_THRESHOLD ((size_t)65536)
#define HX_MAX_EVENTS        256
#define HX_EPOLL_TIMEOUT_MS  200
#define HX_LISTEN_BACKLOG    1024

typedef struct conn conn;

/* One slot per fd. A struct (not conn **) keeps allocation arithmetic type-safe. */
typedef struct {
    conn *c;
} conn_slot;

struct conn {
    int fd;
    uint64_t id;
    bool parked; /* a request is waiting for the hooks to answer it */
    bool eof;    /* peer closed its write side */
    uint32_t events;
    hx_buf in;
    hx_buf out;
    size_t out_off; /* bytes of `out` already sent */
};

typedef struct {
    int ep;
    int lfd;
    conn_slot *tab; /* indexed by fd */
    size_t tab_cap;
    uint32_t nconn;
    uint64_t next_id;
    uint32_t nparked;
    const hx_server_cfg *cfg;
    hx_broker *broker;
    bool fatal; /* a group commit failed: fail-stop, never acknowledge */
} server;

void hx_server_cfg_default(hx_server_cfg *cfg)
{
    cfg->hooks = NULL;
    cfg->bind_addr = "127.0.0.1";
    cfg->port = 9400;
    cfg->max_frame_bytes = 2U << 20;
    cfg->max_conns = 10000;
    cfg->max_out_pending = 8U << 20;
    cfg->port_file = NULL;
    cfg->listen_backlog = HX_LISTEN_BACKLOG;
    cfg->tcp_nodelay = 1;
    cfg->recv_buffer_bytes = 0;
    cfg->send_buffer_bytes = 0;
}

static size_t pending(const conn *c)
{
    return c->out.len - c->out_off;
}

/* ---------------------------------------------------------- connections */

static int set_events(server *s, conn *c, uint32_t events)
{
    if (c->events == events) {
        return 0;
    }
    struct epoll_event ev = {.events = events, .data = {.fd = c->fd}};
    if (epoll_ctl(s->ep, EPOLL_CTL_MOD, c->fd, &ev) != 0) {
        return -1;
    }
    c->events = events;
    return 0;
}

static void conn_close(server *s, conn *c)
{
    if (c->parked) {
        s->nparked--;
    }
    if (s->cfg->hooks != NULL) {
        s->cfg->hooks->drop(s->cfg->hooks->ctx, c->id);
    }
    (void)epoll_ctl(s->ep, EPOLL_CTL_DEL, c->fd, NULL);
    (void)close(c->fd);
    s->tab[c->fd].c = NULL;
    s->nconn--;
    hx_buf_free(&c->in);
    hx_buf_free(&c->out);
    free(c);
}

static int tab_reserve(server *s, size_t fd)
{
    if (fd < s->tab_cap) {
        return 0;
    }
    size_t cap = s->tab_cap != 0 ? s->tab_cap : 256;
    while (cap <= fd) {
        cap *= 2;
    }
    conn_slot *t = realloc(s->tab, cap * sizeof *t);
    if (t == NULL) {
        return -1;
    }
    memset(t + s->tab_cap, 0, (cap - s->tab_cap) * sizeof *t);
    s->tab = t;
    s->tab_cap = cap;
    return 0;
}

static void conn_add(server *s, int fd)
{
    if (s->nconn >= s->cfg->max_conns || tab_reserve(s, (size_t)fd) != 0) {
        HX_WARN("refusing connection (conns=%u)", s->nconn);
        (void)close(fd);
        return;
    }
    conn *c = calloc(1, sizeof *c);
    if (c == NULL) {
        (void)close(fd);
        return;
    }
    c->fd = fd;
    c->id = ++s->next_id;
    c->events = EPOLLIN;
    hx_buf_init(&c->in);
    hx_buf_init(&c->out);

    struct epoll_event ev = {.events = c->events, .data = {.fd = fd}};
    if (epoll_ctl(s->ep, EPOLL_CTL_ADD, fd, &ev) != 0) {
        (void)close(fd);
        free(c);
        return;
    }
    s->tab[fd].c = c;
    s->nconn++;
}

static void accept_ready(server *s)
{
    for (;;) {
        int fd = accept4(s->lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            if (errno == EMFILE || errno == ENFILE) {
                HX_ERROR("accept: out of file descriptors");
                (void)usleep(10 * 1000); /* avoid a hot spin on the level-triggered listener */
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                HX_ERROR("accept failed (errno=%d)", errno);
            }
            return;
        }
        if (s->cfg->tcp_nodelay != 0) {
            int one = 1;
            (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        }
        conn_add(s, fd);
    }
}

/* ------------------------------------------------------------ I/O paths */

/* Returns 0, or -1 if the connection must be closed. */
static int flush_out(conn *c)
{
    while (pending(c) > 0) {
        ssize_t n = send(c->fd, c->out.data + c->out_off, pending(c), MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            return -1;
        }
        c->out_off += (size_t)n;
    }
    if (c->out_off == c->out.len) {
        c->out.len = 0;
        c->out_off = 0;
        hx_buf_shrink(&c->out, HX_KEEP_CAP);
    } else if (c->out_off >= HX_COMPACT_THRESHOLD) {
        hx_buf_consume(&c->out, c->out_off);
        c->out_off = 0;
    }
    return 0;
}

/* Handles complete buffered frames while output is below the high-water mark. */
static int process_frames(server *s, conn *c)
{
    size_t off = 0;
    while (!c->parked && c->in.len - off >= 4 && pending(c) < s->cfg->max_out_pending) {
        const uint32_t flen = hx_load_be32(c->in.data + off);
        if (flen < HX_REQ_HDR_LEN || flen > s->cfg->max_frame_bytes) {
            HX_WARN("fd=%d bad frame length %u, closing", c->fd, flen);
            return -1;
        }
        if (c->in.len - off - 4 < flen) {
            break; /* incomplete frame */
        }
        const hx_server_hooks *hk = s->cfg->hooks;
        int verdict = 0;
        if (hk != NULL) {
            verdict = hk->handle(hk->ctx, c->id, c->in.data + off + 4, flen, &c->out);
            if (verdict < 0) {
                return -1;
            }
        }
        if (verdict == 0 && hx_broker_handle(s->broker, c->in.data + off + 4, flen, &c->out) != 0) {
            return -1;
        }
        off += 4U + flen;
        if (verdict == 2) {
            c->parked = true;
            s->nparked++;
            break;
        }
    }
    hx_buf_consume(&c->in, off);
    return 0;
}

/*
 * Process input, commit, flush output; repeat until blocked. -1 => close connection.
 * Responses are only flushed AFTER hx_broker_commit(), so with --sync always an
 * acknowledgement is never sent before its record is on stable storage.
 */
static int pump(server *s, conn *c)
{
    for (;;) {
        const size_t before = c->in.len;
        if (process_frames(s, c) != 0) {
            return -1;
        }
        if (hx_broker_commit(s->broker) != 0) {
            s->fatal = true;
            return -1;
        }
        if (flush_out(c) != 0) {
            return -1;
        }
        const bool progressed = c->in.len != before;
        if (!progressed || pending(c) >= s->cfg->max_out_pending) {
            break;
        }
    }
    if (c->eof && pending(c) == 0 && !c->parked) {
        return -1; /* peer is done and everything has been answered */
    }
    hx_buf_shrink(&c->in, HX_KEEP_CAP);

    uint32_t events = 0;
    if (!c->eof && !c->parked && pending(c) < s->cfg->max_out_pending) {
        events |= EPOLLIN;
    }
    if (pending(c) > 0) {
        events |= EPOLLOUT;
    }
    return set_events(s, c, events);
}

static int read_some(conn *c)
{
    if (hx_buf_reserve(&c->in, HX_READ_CHUNK) != 0) {
        return -1;
    }
    for (;;) {
        ssize_t n = read(c->fd, c->in.data + c->in.len, HX_READ_CHUNK);
        if (n > 0) {
            c->in.len += (size_t)n;
            return 0;
        }
        if (n == 0) {
            c->eof = true;
            return 0;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        return -1;
    }
}

static conn *conn_of(const server *s, int fd)
{
    return (s->tab != NULL && fd >= 0 && (size_t)fd < s->tab_cap) ? s->tab[fd].c : NULL;
}

/*
 * Phase 1 of an event batch: read and execute requests, buffering the
 * responses but sending nothing. Requests from ALL ready connections are
 * executed before the single commit, so they share one fsync (group commit).
 */
static void conn_ingest(server *s, int fd, uint32_t events)
{
    conn *c = conn_of(s, fd);
    if (c == NULL) {
        return; /* closed earlier in this batch */
    }
    if ((events & (EPOLLERR | EPOLLHUP)) != 0 && (events & EPOLLIN) == 0) {
        conn_close(s, c);
        return;
    }
    if (((events & EPOLLIN) != 0 && read_some(c) != 0) || process_frames(s, c) != 0) {
        conn_close(s, c);
    }
}

/* 0, or -1 if the hooks failed fatally. */
static int resolve_parked(server *s)
{
    const hx_server_hooks *hk = s->cfg->hooks;
    for (size_t fd = 0; fd < s->tab_cap; fd++) {
        conn *c = s->tab[fd].c;
        if (c == NULL || !c->parked) {
            continue;
        }
        const int r = hk->resolve(hk->ctx, c->id, &c->out);
        if (r < 0) {
            HX_ERROR("cluster: cannot resolve a parked request; stopping (fail-stop)");
            return -1;
        }
        if (r == 1) {
            c->parked = false;
            s->nparked--;
            if (pump(s, c) != 0) {
                conn_close(s, c);
            }
        }
    }
    return 0;
}

/* ------------------------------------------------------------- listener */

static int set_buffer(int fd, int opt, uint32_t bytes)
{
    if (bytes == 0) {
        return 0;
    }
    int v = (int)bytes;
    return setsockopt(fd, SOL_SOCKET, opt, &v, sizeof v);
}

static int listen_on(const hx_server_cfg *cfg, uint16_t *bound_port)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg->port);
    if (inet_pton(AF_INET, cfg->bind_addr, &addr.sin_addr) != 1) {
        HX_ERROR("invalid bind address: %s", cfg->bind_addr);
        return -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }
    int one = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    /* Accepted connections inherit these on Linux; 0 keeps the kernel default,
     * which is also what the kernel's buffer autotuning wants. */
    (void)set_buffer(fd, SO_RCVBUF, cfg->recv_buffer_bytes);
    (void)set_buffer(fd, SO_SNDBUF, cfg->send_buffer_bytes);
    socklen_t alen = sizeof addr;
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) != 0 ||
        listen(fd, cfg->listen_backlog) != 0 ||
        getsockname(fd, (struct sockaddr *)&addr, &alen) != 0) {
        HX_ERROR("cannot listen on %s:%u (errno=%d)", cfg->bind_addr, cfg->port, errno);
        (void)close(fd);
        return -1;
    }
    *bound_port = ntohs(addr.sin_port);
    return fd;
}

/* Atomic publish: write a temp file, then rename. */
static int write_port_file(const char *path, uint16_t port)
{
    char tmp[4096];
    int n = snprintf(tmp, sizeof tmp, "%s.tmp", path);
    if (n < 0 || (size_t)n >= sizeof tmp) {
        return -1;
    }
    FILE *f = fopen(tmp, "w");
    if (f == NULL) {
        return -1;
    }
    int rc = fprintf(f, "%u\n", port) > 0 ? 0 : -1;
    if (fclose(f) != 0) {
        rc = -1;
    }
    if (rc == 0 && rename(tmp, path) != 0) {
        rc = -1;
    }
    return rc;
}

int hx_server_run(const hx_server_cfg *cfg, hx_broker *broker, const volatile sig_atomic_t *stop)
{
    server s;
    memset(&s, 0, sizeof s);
    s.cfg = cfg;
    s.broker = broker;
    s.ep = -1;

    uint16_t port = 0;
    s.lfd = listen_on(cfg, &port);
    if (s.lfd < 0) {
        return -1;
    }
    s.ep = epoll_create1(EPOLL_CLOEXEC);
    struct epoll_event ev = {.events = EPOLLIN, .data = {.fd = s.lfd}};
    if (s.ep < 0 || epoll_ctl(s.ep, EPOLL_CTL_ADD, s.lfd, &ev) != 0) {
        HX_ERROR("epoll setup failed (errno=%d)", errno);
        (void)close(s.lfd);
        if (s.ep >= 0) {
            (void)close(s.ep);
        }
        return -1;
    }
    if (cfg->port_file != NULL && write_port_file(cfg->port_file, port) != 0) {
        HX_ERROR("cannot write port file %s (errno=%d)", cfg->port_file, errno);
        (void)close(s.lfd);
        (void)close(s.ep);
        return -1;
    }
    HX_INFO("listening on %s:%u", cfg->bind_addr, port);

    int rc = 0;
    struct epoll_event events[HX_MAX_EVENTS];
    while (*stop == 0) {
        const hx_server_hooks *hk = cfg->hooks;
        int n =
            epoll_wait(s.ep, events, HX_MAX_EVENTS, hk != NULL ? hk->tick_ms : HX_EPOLL_TIMEOUT_MS);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            HX_ERROR("epoll_wait failed (errno=%d)", errno);
            rc = -1;
            break;
        }
        hx_broker_tick(broker); /* session expiry / rebalance deadlines */
        if (hk != NULL) {
            struct timespec ts;
            (void)clock_gettime(CLOCK_MONOTONIC, &ts);
            if (hk->poll(hk->ctx, (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000) != 0) {
                HX_ERROR("cluster component failed; stopping (fail-stop)");
                rc = -1;
                break;
            }
        }
        /* Phase 1: execute everything that is ready. */
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == s.lfd) {
                accept_ready(&s);
            } else {
                conn_ingest(&s, events[i].data.fd, events[i].events);
            }
        }
        /* Phase 2: one commit (fsync) for the whole batch, before any ack leaves. */
        if (hx_broker_commit(broker) != 0) {
            HX_ERROR("group commit failed; stopping without acknowledging (fail-stop)");
            rc = -1;
            break;
        }
        /* Parked requests whose answer is ready continue now. */
        if (s.nparked > 0 && resolve_parked(&s) != 0) {
            rc = -1;
            break;
        }
        /* Phase 3: send responses and re-arm epoll interest. */
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == s.lfd) {
                continue;
            }
            conn *c = conn_of(&s, events[i].data.fd);
            if (c != NULL && pump(&s, c) != 0) {
                conn_close(&s, c);
            }
        }
        if (s.fatal) {
            HX_ERROR("group commit failed; stopping without acknowledging (fail-stop)");
            rc = -1;
            break;
        }
    }

    for (size_t fd = 0; fd < s.tab_cap; fd++) {
        if (s.tab[fd].c != NULL) {
            conn_close(&s, s.tab[fd].c);
        }
    }
    free(s.tab);
    (void)close(s.lfd);
    (void)close(s.ep);
    return rc;
}
