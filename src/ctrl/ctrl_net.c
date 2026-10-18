#include "ctrl/ctrl_net.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "proto/codec.h"

#define MAX_FRAME   65536U
#define T_HELLO     1U
#define T_ALTER     2U
#define T_SESSION   3U
#define T_STATE     4U
#define T_NOT_READY 5U
#define OUT_LIMIT   (1U << 20)

/* ------------------------------------------------------------ shared bits */

typedef struct {
    int fd;
    hx_buf in;
    hx_buf out;
    bool connecting;
} chan;

static void chan_init(chan *c, int fd)
{
    memset(c, 0, sizeof *c);
    c->fd = fd;
    hx_buf_init(&c->in);
    hx_buf_init(&c->out);
}

static void chan_free(chan *c)
{
    if (c->fd >= 0) {
        (void)close(c->fd);
    }
    c->fd = -1;
    hx_buf_free(&c->in);
    hx_buf_free(&c->out);
    c->connecting = false;
}

/* Start a frame: reserves the length, writes the type. Returns the position of the length. */
static size_t frame_begin(chan *c, uint8_t type)
{
    const size_t pos = c->out.len;
    (void)hx_buf_put_u32(&c->out, 0);
    (void)hx_buf_put_u8(&c->out, type);
    return pos;
}

/* 0 ok, -1 if the output queue is over its limit (caller closes the channel). */
static int frame_end(chan *c, size_t pos)
{
    hx_buf_patch_u32(&c->out, pos, (uint32_t)(c->out.len - pos - 4U));
    return c->out.len > OUT_LIMIT ? -1 : 0;
}

/* Write what the socket takes. -1 = dead. */
static int chan_flush(chan *c)
{
    while (c->out.len != 0) {
        const ssize_t w = send(c->fd, c->out.data, c->out.len, MSG_NOSIGNAL);
        if (w > 0) {
            hx_buf_consume(&c->out, (size_t)w);
        } else if (w < 0 && errno == EINTR) {
            continue;
        } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 0;
        } else {
            return -1;
        }
    }
    hx_buf_shrink(&c->out, 4096);
    return 0;
}

/* Read everything available into c->in. -1 = closed or failed. */
static int chan_fill(chan *c)
{
    uint8_t tmp[4096];
    for (;;) {
        const ssize_t r = recv(c->fd, tmp, sizeof tmp, 0);
        if (r > 0) {
            if (hx_buf_put(&c->in, tmp, (size_t)r) != 0 || c->in.len > (size_t)4 * MAX_FRAME) {
                return -1;
            }
            continue;
        }
        if (r == 0) {
            return -1;
        }
        if (errno == EINTR) {
            continue;
        }
        return (errno == EAGAIN || errno == EWOULDBLOCK) ? 0 : -1;
    }
}

typedef struct {
    uint8_t type;
    hx_rd rd; /* positioned after the type */
} frame;

/* 1 = frame ready, 0 = need more bytes, -1 = protocol violation. The caller consumes with
 * hx_buf_consume(&c->in, consumed). */
static int frame_next(const chan *c, frame *f, size_t *consumed)
{
    if (c->in.len < 4U) {
        return 0;
    }
    const uint32_t len = hx_load_be32(c->in.data);
    if (len < 1U || len > MAX_FRAME) {
        return -1;
    }
    if (c->in.len < 4U + (size_t)len) {
        return 0;
    }
    f->type = c->in.data[4];
    hx_rd_init(&f->rd, c->in.data + 5, len - 1U);
    *consumed = 4U + (size_t)len;
    return 1;
}

static int set_nonblock_nodelay(int fd)
{
    const int one = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    const int fl = fcntl(fd, F_GETFL, 0);
    return fl < 0 ? -1 : fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/* ================================================================ server */

typedef struct {
    chan ch;
    uint32_t broker; /* UINT32_MAX until the first HELLO */
    uint64_t sent_gen;
    int64_t last_full;
} sconn;

struct hx_ctlsrv {
    hx_ctlsrv_cfg cfg;
    hx_chost *host;
    int lfd;
    uint16_t port;
    sconn *c;
    size_t n;
    hx_ctlsrv_stats st;
};

hx_ctlsrv *hx_ctlsrv_create(const hx_ctlsrv_cfg *cfg, hx_chost *host)
{
    if (cfg == NULL || host == NULL || cfg->bind_addr == NULL) {
        return NULL;
    }
    hx_ctlsrv *s = calloc(1, sizeof *s);
    if (s == NULL) {
        return NULL;
    }
    s->cfg = *cfg;
    s->host = host;
    s->lfd = -1;
    if (s->cfg.push_ms == 0) {
        s->cfg.push_ms = 500;
    }
    if (s->cfg.max_conns == 0) {
        s->cfg.max_conns = 64;
    }
    s->c = calloc(s->cfg.max_conns, sizeof *s->c);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons(cfg->port);
    socklen_t alen = sizeof a;
    const int one = 1;
    s->lfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (s->c == NULL || s->lfd < 0 || inet_pton(AF_INET, cfg->bind_addr, &a.sin_addr) != 1 ||
        setsockopt(s->lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) != 0 ||
        bind(s->lfd, (struct sockaddr *)&a, sizeof a) != 0 || listen(s->lfd, 64) != 0 ||
        getsockname(s->lfd, (struct sockaddr *)&a, &alen) != 0 ||
        set_nonblock_nodelay(s->lfd) != 0) {
        hx_ctlsrv_destroy(s);
        return NULL;
    }
    s->port = ntohs(a.sin_port);
    return s;
}

static void sconn_drop(hx_ctlsrv *s, size_t i, bool bad)
{
    chan_free(&s->c[i].ch);
    s->c[i] = s->c[--s->n];
    if (bad) {
        s->st.bad_conns++;
    }
}

void hx_ctlsrv_destroy(hx_ctlsrv *s)
{
    if (s == NULL) {
        return;
    }
    for (size_t i = 0; i < s->n; i++) {
        chan_free(&s->c[i].ch);
    }
    if (s->lfd >= 0) {
        (void)close(s->lfd);
    }
    free(s->c);
    free(s);
}

uint16_t hx_ctlsrv_port(const hx_ctlsrv *s)
{
    return s->port;
}

hx_ctlsrv_stats hx_ctlsrv_get_stats(const hx_ctlsrv *s)
{
    return s->st;
}

/* 0 ok, -1 drop the connection. */
static int srv_handle(hx_ctlsrv *s, sconn *c, frame *f, int64_t now)
{
    if (f->type == T_HELLO) {
        const uint32_t broker = hx_rd_u32(&f->rd);
        const uint64_t nonce = hx_rd_u64(&f->rd);
        if (!f->rd.ok || hx_rd_left(&f->rd) != 0 || broker >= HX_CTRL_MAX_BROKERS) {
            return -1;
        }
        s->st.hellos++;
        c->broker = broker;
        uint64_t session = 0;
        const hx_chost_status st = hx_chost_hello(s->host, now, broker, nonce, &session);
        if (st == HX_CHOST_FAILED) {
            return -1;
        }
        if (st == HX_CHOST_NOT_READY) {
            const size_t p = frame_begin(&c->ch, T_NOT_READY);
            (void)hx_buf_put_u32(&c->ch.out, hx_chost_leader_hint(s->host));
            return frame_end(&c->ch, p);
        }
        if (session != 0) {
            const size_t p = frame_begin(&c->ch, T_SESSION);
            (void)hx_buf_put_u64(&c->ch.out, nonce);
            (void)hx_buf_put_u64(&c->ch.out, session);
            c->sent_gen = 0; /* a fresh session gets the state right away */
            return frame_end(&c->ch, p);
        }
        return 0;
    }
    if (f->type == T_ALTER) {
        const uint32_t part = hx_rd_u32(&f->rd);
        const uint32_t broker = hx_rd_u32(&f->rd);
        const uint64_t session = hx_rd_u64(&f->rd);
        const uint32_t epoch = hx_rd_u32(&f->rd);
        const uint32_t isr = hx_rd_u32(&f->rd);
        const uint32_t ver = hx_rd_u32(&f->rd);
        if (!f->rd.ok || hx_rd_left(&f->rd) != 0 || broker != c->broker) {
            return -1; /* identity comes from the HELLO on this connection */
        }
        s->st.alters++;
        const hx_chost_status st =
            hx_chost_alter_isr(s->host, now, part, broker, session, epoch, isr, ver);
        return st == HX_CHOST_FAILED ? -1 : 0;
    }
    return -1;
}

static void srv_push(hx_ctlsrv *s, sconn *c, int64_t now)
{
    const hx_ctrl *ctl = hx_chost_ctrl(s->host);
    const uint64_t gen = hx_ctrl_generation(ctl);
    if (c->broker >= HX_CTRL_MAX_BROKERS || !hx_chost_ready(s->host) ||
        (hx_ctrl_alive(ctl) & (UINT32_C(1) << c->broker)) == 0) {
        return;
    }
    if (c->sent_gen == gen && now - c->last_full < (int64_t)s->cfg.push_ms) {
        return;
    }
    const uint64_t session = hx_ctrl_session(ctl, c->broker);
    for (uint32_t p = 0; p < hx_ctrl_nparts(ctl); p++) {
        hx_ctrl_part st;
        (void)hx_ctrl_get(ctl, p, &st);
        if ((st.replicas & (UINT32_C(1) << c->broker)) == 0) {
            continue;
        }
        const size_t pos = frame_begin(&c->ch, T_STATE);
        (void)hx_buf_put_u32(&c->ch.out, p);
        (void)hx_buf_put_u64(&c->ch.out, session);
        (void)hx_buf_put_u32(&c->ch.out, st.epoch);
        (void)hx_buf_put_u32(&c->ch.out, st.leader);
        (void)hx_buf_put_u32(&c->ch.out, st.isr);
        (void)hx_buf_put_u32(&c->ch.out, st.isr_version);
        if (frame_end(&c->ch, pos) != 0) {
            return; /* too slow a reader: the next flush failure or limit check drops it */
        }
        s->st.pushes++;
    }
    c->sent_gen = gen;
    c->last_full = now;
}

void hx_ctlsrv_poll(hx_ctlsrv *s, int timeout_ms, int64_t now_ms)
{
    struct pollfd fds[1U + 256U];
    const size_t cap = s->cfg.max_conns < 256U ? s->cfg.max_conns : 256U;
    size_t nf = 0;
    fds[nf].fd = s->lfd;
    fds[nf].events = POLLIN;
    nf++;
    for (size_t i = 0; i < s->n && i < cap; i++) {
        fds[nf].fd = s->c[i].ch.fd;
        fds[nf].events = (short)(POLLIN | (s->c[i].ch.out.len != 0 ? POLLOUT : 0));
        nf++;
    }
    const size_t polled = nf - 1U;
    (void)poll(fds, (nfds_t)nf, timeout_ms);
    if ((fds[0].revents & POLLIN) != 0) {
        for (;;) {
            const int fd = accept(s->lfd, NULL, NULL);
            if (fd < 0) {
                break;
            }
            if (s->n >= s->cfg.max_conns || set_nonblock_nodelay(fd) != 0) {
                (void)close(fd);
                s->st.bad_conns++;
                continue;
            }
            chan_init(&s->c[s->n].ch, fd);
            s->c[s->n].broker = UINT32_MAX;
            s->n++;
        }
    }
    /* Walk backwards so that dropping swaps in an element we have already handled. */
    for (size_t k = polled; k-- > 0;) {
        sconn *c = &s->c[k];
        const short re = fds[k + 1U].revents;
        bool bad = false;
        bool gone = false;
        if ((re & (POLLERR | POLLNVAL)) != 0) {
            gone = true;
        }
        if (!gone && (re & (POLLIN | POLLHUP)) != 0) {
            if (chan_fill(&c->ch) != 0) {
                gone = true;
            }
            frame f;
            size_t used = 0;
            int r;
            while (!bad && (r = frame_next(&c->ch, &f, &used)) == 1) {
                if (srv_handle(s, c, &f, now_ms) != 0) {
                    bad = true;
                    break;
                }
                hx_buf_consume(&c->ch.in, used);
            }
            if (r < 0) {
                bad = true;
            }
        }
        if (bad || gone) {
            sconn_drop(s, k, bad);
        }
    }
    for (size_t i = s->n; i-- > 0;) {
        srv_push(s, &s->c[i], now_ms);
        if (chan_flush(&s->c[i].ch) != 0 || s->c[i].ch.out.len > OUT_LIMIT) {
            sconn_drop(s, i, true);
        }
    }
}

/* ================================================================ client */

typedef struct {
    chan ch;
    struct sockaddr_in addr;
    int64_t retry_at;
    int64_t backoff;
} cconn;

struct hx_ctlcli {
    hx_ctlcli_cfg cfg;
    cconn c[HX_RAFT_MAX_NODES];
    uint64_t session;
    int64_t next_hello;
};

hx_ctlcli *hx_ctlcli_create(const hx_ctlcli_cfg *cfg)
{
    if (cfg == NULL || cfg->on_state == NULL || cfg->ncontrollers == 0 ||
        cfg->ncontrollers > HX_RAFT_MAX_NODES || cfg->broker >= HX_CTRL_MAX_BROKERS) {
        return NULL;
    }
    hx_ctlcli *c = calloc(1, sizeof *c);
    if (c == NULL) {
        return NULL;
    }
    c->cfg = *cfg;
    if (c->cfg.hello_ms == 0) {
        c->cfg.hello_ms = 100;
    }
    for (uint32_t i = 0; i < cfg->ncontrollers; i++) {
        c->c[i].ch.fd = -1;
        c->c[i].backoff = 100;
        c->c[i].addr.sin_family = AF_INET;
        c->c[i].addr.sin_port = htons(cfg->ports[i]);
        if (inet_pton(AF_INET, cfg->addrs[i], &c->c[i].addr.sin_addr) != 1) {
            free(c);
            return NULL;
        }
    }
    return c;
}

void hx_ctlcli_destroy(hx_ctlcli *c)
{
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->cfg.ncontrollers; i++) {
        chan_free(&c->c[i].ch);
    }
    free(c);
}

uint64_t hx_ctlcli_session(const hx_ctlcli *c)
{
    return c->session;
}

static void cli_down(hx_ctlcli *c, uint32_t i, int64_t now)
{
    cconn *k = &c->c[i];
    chan_free(&k->ch);
    k->ch.fd = -1;
    k->retry_at = now + k->backoff;
    k->backoff = k->backoff * 2 > 1000 ? 1000 : k->backoff * 2;
}

static void cli_connect(hx_ctlcli *c, uint32_t i, int64_t now)
{
    cconn *k = &c->c[i];
    const int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0 || set_nonblock_nodelay(fd) != 0) {
        if (fd >= 0) {
            (void)close(fd);
        }
        k->retry_at = now + k->backoff;
        return;
    }
    const int rc = connect(fd, (const struct sockaddr *)&k->addr, sizeof k->addr);
    if (rc != 0 && errno != EINPROGRESS) {
        (void)close(fd);
        k->retry_at = now + k->backoff;
        k->backoff = k->backoff * 2 > 1000 ? 1000 : k->backoff * 2;
        return;
    }
    chan_init(&k->ch, fd);
    k->ch.connecting = rc != 0;
}

static int cli_handle(hx_ctlcli *c, frame *f)
{
    if (f->type == T_SESSION) {
        const uint64_t nonce = hx_rd_u64(&f->rd);
        const uint64_t session = hx_rd_u64(&f->rd);
        if (!f->rd.ok || hx_rd_left(&f->rd) != 0) {
            return -1;
        }
        if (nonce == c->cfg.nonce && session > c->session) {
            c->session = session;
        }
        return 0;
    }
    if (f->type == T_STATE) {
        const uint32_t part = hx_rd_u32(&f->rd);
        const uint64_t session = hx_rd_u64(&f->rd);
        hx_ctrl_part st;
        memset(&st, 0, sizeof st);
        st.epoch = hx_rd_u32(&f->rd);
        st.leader = hx_rd_u32(&f->rd);
        st.isr = hx_rd_u32(&f->rd);
        st.isr_version = hx_rd_u32(&f->rd);
        if (!f->rd.ok || hx_rd_left(&f->rd) != 0) {
            return -1;
        }
        if (c->session != 0 && session == c->session) {
            c->cfg.on_state(c->cfg.ctx, part, &st);
        }
        return 0;
    }
    if (f->type == T_NOT_READY) {
        (void)hx_rd_u32(&f->rd);
        return f->rd.ok && hx_rd_left(&f->rd) == 0 ? 0 : -1;
    }
    return -1;
}

int hx_ctlcli_alter(hx_ctlcli *c, uint32_t part, uint32_t epoch, uint32_t isr, uint32_t isr_version)
{
    if (c->session == 0) {
        return 0;
    }
    int queued = 0;
    for (uint32_t i = 0; i < c->cfg.ncontrollers; i++) {
        cconn *k = &c->c[i];
        if (k->ch.fd < 0 || k->ch.connecting) {
            continue;
        }
        const size_t pos = frame_begin(&k->ch, T_ALTER);
        (void)hx_buf_put_u32(&k->ch.out, part);
        (void)hx_buf_put_u32(&k->ch.out, c->cfg.broker);
        (void)hx_buf_put_u64(&k->ch.out, c->session);
        (void)hx_buf_put_u32(&k->ch.out, epoch);
        (void)hx_buf_put_u32(&k->ch.out, isr);
        (void)hx_buf_put_u32(&k->ch.out, isr_version);
        if (frame_end(&k->ch, pos) == 0) {
            queued++;
        }
    }
    return queued;
}

void hx_ctlcli_poll(hx_ctlcli *c, int timeout_ms, int64_t now_ms)
{
    for (uint32_t i = 0; i < c->cfg.ncontrollers; i++) {
        if (c->c[i].ch.fd < 0 && now_ms >= c->c[i].retry_at) {
            cli_connect(c, i, now_ms);
        }
    }
    if (now_ms >= c->next_hello) {
        c->next_hello = now_ms + c->cfg.hello_ms;
        for (uint32_t i = 0; i < c->cfg.ncontrollers; i++) {
            cconn *k = &c->c[i];
            if (k->ch.fd >= 0 && !k->ch.connecting) {
                const size_t pos = frame_begin(&k->ch, T_HELLO);
                (void)hx_buf_put_u32(&k->ch.out, c->cfg.broker);
                (void)hx_buf_put_u64(&k->ch.out, c->cfg.nonce);
                (void)frame_end(&k->ch, pos);
            }
        }
    }
    struct pollfd fds[HX_RAFT_MAX_NODES];
    uint32_t idx[HX_RAFT_MAX_NODES];
    nfds_t nf = 0;
    for (uint32_t i = 0; i < c->cfg.ncontrollers; i++) {
        cconn *k = &c->c[i];
        if (k->ch.fd < 0) {
            continue;
        }
        fds[nf].fd = k->ch.fd;
        fds[nf].events = (short)(POLLIN | ((k->ch.out.len != 0 || k->ch.connecting) ? POLLOUT : 0));
        idx[nf++] = i;
    }
    if (nf == 0) {
        if (timeout_ms > 0) {
            (void)poll(NULL, 0, timeout_ms);
        }
        return;
    }
    (void)poll(fds, nf, timeout_ms);
    for (nfds_t j = 0; j < nf; j++) {
        const uint32_t i = idx[j];
        cconn *k = &c->c[i];
        const short re = fds[j].revents;
        bool dead = (re & (POLLERR | POLLNVAL)) != 0;
        if (!dead && k->ch.connecting && (re & (POLLOUT | POLLHUP)) != 0) {
            int err = 0;
            socklen_t el = sizeof err;
            if (getsockopt(k->ch.fd, SOL_SOCKET, SO_ERROR, &err, &el) != 0 || err != 0) {
                dead = true;
            } else {
                k->ch.connecting = false;
                k->backoff = 100;
            }
        }
        if (!dead && !k->ch.connecting && (re & (POLLIN | POLLHUP)) != 0) {
            if (chan_fill(&k->ch) != 0) {
                dead = true;
            }
            frame f;
            size_t used = 0;
            int r;
            while ((r = frame_next(&k->ch, &f, &used)) == 1) {
                if (cli_handle(c, &f) != 0) {
                    dead = true;
                    break;
                }
                hx_buf_consume(&k->ch.in, used);
            }
            if (r < 0) {
                dead = true;
            }
        }
        if (!dead && !k->ch.connecting && chan_flush(&k->ch) != 0) {
            dead = true;
        }
        if (dead) {
            cli_down(c, i, now_ms);
        }
    }
}
