/*
 * Partition replication over real TCP on loopback: three brokers, two partitions, each replica on a
 * real hx_log (hx_plog) behind its own transport, driven by one thread. Covers replication and the
 * high watermark, a follower restarting on a new port, a leader change, and hostile peers.
 * Timing is real, so every wait has a generous deadline.
 */
#define _XOPEN_SOURCE 700
#include <arpa/inet.h>
#include <ftw.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "repl/repl_log.h"
#include "repl/repl_net.h"

#define NB 3U
#define NP 2U

static int failures;

#define REQ(cond)                                                                                  \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

typedef struct {
    hx_plog *log;
    hx_repl *r;
} replica;

typedef struct {
    bool up;
    replica rep[NP];
    hx_replnet *net;
    uint16_t port;
    uint64_t unknown_part;
} broker;

typedef struct {
    char dir[256];
    broker b[NB];
    uint32_t epoch[NP];
    uint32_t leader[NP];
    uint32_t isr[NP];
    uint32_t ver[NP];
} cluster;

static int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int rm_cb(const char *p, const struct stat *sb, int t, struct FTW *f)
{
    (void)sb;
    (void)t;
    (void)f;
    return remove(p);
}

typedef struct {
    cluster *c;
    uint32_t id;
} bctx;

static bctx g_ctx[NB];

static void flush_out(broker *b, uint32_t part, int64_t now)
{
    const hx_repl_msg *msgs = NULL;
    size_t n = 0;
    hx_repl_take_msgs(b->rep[part].r, &msgs, &n);
    for (size_t i = 0; i < n; i++) {
        (void)hx_replnet_send(b->net, part, &msgs[i], now);
    }
}

static void on_msg(void *ctx, uint32_t part, const hx_repl_msg *m)
{
    bctx *bc = ctx;
    broker *b = &bc->c->b[bc->id];
    if (part >= NP || b->rep[part].r == NULL) {
        b->unknown_part++;
        return;
    }
    const int64_t now = now_ms();
    (void)hx_repl_step(b->rep[part].r, now, m);
    flush_out(b, part, now);
}

static void wire_peers(cluster *c)
{
    for (uint32_t i = 0; i < NB; i++) {
        if (!c->b[i].up) {
            continue;
        }
        for (uint32_t j = 0; j < NB; j++) {
            if (i != j && c->b[j].up) {
                (void)hx_replnet_set_peer(c->b[i].net, j, "127.0.0.1", c->b[j].port);
            }
        }
    }
}

static bool start_broker(cluster *c, uint32_t i)
{
    broker *b = &c->b[i];
    hx_replnet_cfg nc;
    memset(&nc, 0, sizeof nc);
    nc.self = i;
    nc.nbrokers = NB;
    nc.bind_addr = "127.0.0.1";
    nc.on_msg = on_msg;
    g_ctx[i].c = c;
    g_ctx[i].id = i;
    nc.ctx = &g_ctx[i];
    b->net = hx_replnet_create(&nc);
    REQ(b->net != NULL);
    b->port = hx_replnet_port(b->net);
    for (uint32_t p = 0; p < NP; p++) {
        char dir[300];
        (void)snprintf(dir, sizeof dir, "%s/b%u-p%u", c->dir, i, p);
        REQ(hx_plog_open(dir, NULL, &b->rep[p].log) == HX_OK);
        hx_repl_cfg rc;
        hx_repl_cfg_default(&rc, i, (1U << NB) - 1U);
        rc.fetch_ms = 10;
        rc.lag_ms = 2000;
        rc.log = hx_plog_vtable(b->rep[p].log);
        b->rep[p].r = hx_repl_create(&rc, now_ms());
        REQ(b->rep[p].r != NULL);
        if (c->epoch[p] != 0) {
            (void)hx_repl_set_state(b->rep[p].r, now_ms(), c->epoch[p], c->leader[p], c->isr[p],
                                    c->ver[p]);
            flush_out(b, p, now_ms());
        }
    }
    b->up = true;
    wire_peers(c);
    return true;
}

static void stop_broker(cluster *c, uint32_t i)
{
    broker *b = &c->b[i];
    for (uint32_t p = 0; p < NP; p++) {
        hx_repl_destroy(b->rep[p].r);
        hx_plog_close(b->rep[p].log);
        b->rep[p].r = NULL;
        b->rep[p].log = NULL;
    }
    hx_replnet_destroy(b->net);
    b->net = NULL;
    b->up = false;
}

/* Controller decision for one partition, delivered to every running broker. */
static void set_state(cluster *c, uint32_t p, uint32_t epoch, uint32_t leader, uint32_t isr)
{
    c->epoch[p] = epoch;
    c->leader[p] = leader;
    c->isr[p] = isr;
    c->ver[p]++;
    for (uint32_t i = 0; i < NB; i++) {
        if (c->b[i].up) {
            (void)hx_repl_set_state(c->b[i].rep[p].r, now_ms(), epoch, leader, isr, c->ver[p]);
            flush_out(&c->b[i], p, now_ms());
        }
    }
}

static void run_for(cluster *c, int ms)
{
    const int64_t end = now_ms() + ms;
    while (now_ms() < end) {
        for (uint32_t i = 0; i < NB; i++) {
            if (!c->b[i].up) {
                continue;
            }
            (void)hx_replnet_poll(c->b[i].net, 2, now_ms());
            for (uint32_t p = 0; p < NP; p++) {
                (void)hx_repl_tick(c->b[i].rep[p].r, now_ms());
                flush_out(&c->b[i], p, now_ms());
            }
        }
    }
}

static bool wait_hw(cluster *c, uint32_t p, uint64_t want, int timeout_ms)
{
    const int64_t end = now_ms() + timeout_ms;
    while (now_ms() < end) {
        run_for(c, 20);
        bool all = true;
        for (uint32_t i = 0; i < NB; i++) {
            if (c->b[i].up && hx_repl_hw(c->b[i].rep[p].r) < want) {
                all = false;
            }
        }
        if (all) {
            return true;
        }
    }
    return false;
}

static bool produce(cluster *c, uint32_t p, uint32_t count, uint32_t tag)
{
    broker *l = &c->b[c->leader[p]];
    for (uint32_t k = 0; k < count; k++) {
        char text[32];
        (void)snprintf(text, sizeof text, "p%u-t%u-%u", p, tag, k);
        hx_repl_rec r = {.epoch = 0, .len = (uint32_t)strlen(text), .data = (const uint8_t *)text};
        uint64_t first = 0;
        REQ(hx_repl_append(l->rep[p].r, now_ms(), &r, 1, &first) == HX_REPL_OK);
        flush_out(l, p, now_ms());
        if (k % 8U == 7U) {
            run_for(c, 5);
        }
    }
    return true;
}

/* Every running replica holds the same records up to the high watermark. */
static bool same_prefix(cluster *c, uint32_t p, uint64_t upto)
{
    for (uint64_t off = 0; off < upto; off++) {
        uint8_t ref[64];
        uint32_t reflen = 0;
        uint32_t refep = 0;
        bool have = false;
        for (uint32_t i = 0; i < NB; i++) {
            if (!c->b[i].up) {
                continue;
            }
            uint8_t buf[64];
            uint32_t len = 0;
            uint32_t ep = 0;
            REQ(hx_plog_read_payload(c->b[i].rep[p].log, off, buf, sizeof buf, &len, &ep) == HX_OK);
            if (!have) {
                memcpy(ref, buf, len);
                reflen = len;
                refep = ep;
                have = true;
            } else {
                REQ(len == reflen && ep == refep && memcmp(ref, buf, len) == 0);
            }
        }
    }
    return true;
}

static int raw_connect(uint16_t port)
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    (void)inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0) {
        (void)close(fd);
        return -1;
    }
    return fd;
}

static void put32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static bool hostile(cluster *c)
{
    const uint64_t before = hx_replnet_get_stats(c->b[0].net).undecodable;
    /* 1. Raft-style hello magic on the replication port: rejected as a bad peer. */
    int fd = raw_connect(c->b[0].port);
    REQ(fd >= 0);
    uint8_t hello[16];
    put32(hello, 12);
    put32(hello + 4, 0x48585231U); /* "HXR1" */
    put32(hello + 8, 1);
    put32(hello + 12, NB);
    REQ(write(fd, hello, sizeof hello) == (ssize_t)sizeof hello);
    run_for(c, 100);
    (void)close(fd);
    /* 2. A correct hello, then a frame that is not a message, then one for a wrong destination. */
    fd = raw_connect(c->b[0].port);
    REQ(fd >= 0);
    put32(hello + 4, 0x48585031U); /* "HXP1" */
    REQ(write(fd, hello, sizeof hello) == (ssize_t)sizeof hello);
    uint8_t junk[12];
    put32(junk, 8);
    memset(junk + 4, 0xAB, 8);
    REQ(write(fd, junk, sizeof junk) == (ssize_t)sizeof junk);
    /* A well-formed FETCH that claims to come from broker 2 on the connection that said it is 1. */
    hx_repl_msg spoof;
    memset(&spoof, 0, sizeof spoof);
    spoof.type = HX_REPL_MSG_FETCH;
    spoof.from = 2;
    spoof.to = 0;
    spoof.epoch = 1;
    hx_buf fr;
    hx_buf_init(&fr);
    REQ(hx_buf_put_u32(&fr, 0) == 0 && hx_buf_put_u32(&fr, 0) == 0 &&
        hx_repl_msg_encode(&spoof, &fr) == 0);
    hx_buf_patch_u32(&fr, 0, (uint32_t)(fr.len - 4U));
    REQ(write(fd, fr.data, fr.len) == (ssize_t)fr.len);
    hx_buf_free(&fr);
    run_for(c, 100);
    (void)close(fd);
    const hx_replnet_stats st = hx_replnet_get_stats(c->b[0].net);
    REQ(st.undecodable >= before + 2U); /* the junk frame and the spoofed sender */
    REQ(st.net.bad_peers >= 1);
    /* Everything still works afterwards. */
    REQ(produce(c, 0, 10, 99));
    REQ(wait_hw(c, 0, hx_repl_leo(c->b[c->leader[0]].rep[0].r), 5000));
    return true;
}

static bool scenario(cluster *c)
{
    for (uint32_t i = 0; i < NB; i++) {
        REQ(start_broker(c, i));
    }
    wire_peers(c);
    /* P0 led by broker 0, P1 by broker 1, everyone in the ISR. */
    set_state(c, 0, 1, 0, 7);
    set_state(c, 1, 1, 1, 7);

    REQ(produce(c, 0, 200, 1));
    REQ(produce(c, 1, 120, 1));
    REQ(wait_hw(c, 0, 200, 8000));
    REQ(wait_hw(c, 1, 120, 8000));
    REQ(same_prefix(c, 0, 200) && same_prefix(c, 1, 120));

    /* A follower restarts on a new port; data it already had survives, the rest is fetched. */
    stop_broker(c, 2);
    REQ(produce(c, 0, 60, 2));
    run_for(c, 100);
    REQ(start_broker(c, 2));
    wire_peers(c);
    REQ(produce(c, 0, 20, 3));
    REQ(wait_hw(c, 0, 280, 10000));
    REQ(same_prefix(c, 0, 280));

    /* Leader change for P0: broker 1 takes over in epoch 2 with ISR {1,2}. */
    set_state(c, 0, 2, 1, 6);
    REQ(produce(c, 0, 40, 4));
    REQ(wait_hw(c, 0, 320, 10000));
    REQ(same_prefix(c, 0, 320));
    uint32_t ep = 0;
    REQ(hx_plog_read_payload(c->b[1].rep[0].log, 319, (uint8_t[64]){0}, 64, &(uint32_t){0}, &ep) ==
        HX_OK);
    REQ(ep == 2);
    return hostile(c);
}

int main(void)
{
    cluster c;
    memset(&c, 0, sizeof c);
    const char *t = getenv("TMPDIR");
    (void)snprintf(c.dir, sizeof c.dir, "%s/helix-replnet-XXXXXX",
                   (t != NULL && *t != '\0') ? t : "/tmp");
    if (mkdtemp(c.dir) == NULL) {
        perror("mkdtemp");
        return 2;
    }
    const bool ok = scenario(&c);
    for (uint32_t i = 0; i < NB; i++) {
        if (c.b[i].up) {
            stop_broker(&c, i);
        }
    }
    (void)nftw(c.dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
    if (!ok || failures != 0) {
        (void)fprintf(stderr, "replication-over-TCP test failed\n");
        return 1;
    }
    (void)puts("all replication-over-TCP tests passed");
    return 0;
}
