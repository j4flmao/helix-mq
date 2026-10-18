/*
 * Raft over real TCP on loopback: three nodes, each with its own WAL file and transport, driven by
 * one thread. Covers election, replication, follower and leader restarts from the WAL, hostile
 * peers, and a peer that is not there. Timing is real, so every wait has a generous deadline.
 */
#define _XOPEN_SOURCE 700
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "raft/raft.h"
#include "raft/raft_net.h"
#include "raft/raft_wal.h"
#include "util/buf.h"

static int failures;

#define REQ(cond)                                                                                  \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define N 3U

typedef struct {
    uint32_t id;
    bool up;
    char path[320];
    hx_raft_wal *wal;
    hx_raft *r;
    hx_raftnet *net;
    uint16_t port;
    uint64_t bad_frames;
} tnode;

typedef struct {
    char dir[256];
    tnode n[N];
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

static void flush_out(tnode *t, int64_t now)
{
    const hx_raft_msg *msgs = NULL;
    size_t cnt = 0;
    hx_raft_take_msgs(t->r, &msgs, &cnt);
    for (size_t i = 0; i < cnt; i++) {
        hx_buf b;
        hx_buf_init(&b);
        if (hx_raft_msg_encode(&msgs[i], &b) == 0) {
            (void)hx_raftnet_send(t->net, msgs[i].to, b.data, b.len, now);
        }
        hx_buf_free(&b);
    }
}

static void on_frame(void *ctx, uint32_t from, const uint8_t *p, size_t len)
{
    tnode *t = ctx;
    hx_raft_entry scratch[256];
    hx_raft_msg m;
    if (hx_raft_msg_decode(p, len, scratch, 256, &m) != 0 || m.from != from || m.to != t->id) {
        t->bad_frames++;
        return;
    }
    const int64_t now = now_ms();
    (void)hx_raft_step(t->r, now, &m);
    flush_out(t, now);
}

static bool node_start(cluster *c, uint32_t id, uint16_t port)
{
    tnode *t = &c->n[id];
    t->id = id;
    REQ(hx_raft_wal_open(t->path, &t->wal) == HX_WAL_OK);
    hx_raft_wal_set_nosync_for_tests(t->wal, true);
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, id, N);
    cfg.seed = 99U + id;
    cfg.storage = hx_raft_wal_storage(t->wal);
    hx_raft_restore rs = hx_raft_wal_restore(t->wal);
    t->r = hx_raft_create(&cfg, &rs, now_ms());
    REQ(t->r != NULL);
    const hx_raftnet_cfg nc = {.self = id,
                               .nnodes = N,
                               .bind_addr = "127.0.0.1",
                               .port = port,
                               .on_frame = on_frame,
                               .ctx = t};
    t->net = hx_raftnet_create(&nc);
    REQ(t->net != NULL);
    t->port = hx_raftnet_port(t->net);
    t->up = true;
    return true;
}

static bool wire_peers(cluster *c)
{
    for (uint32_t i = 0; i < N; i++) {
        for (uint32_t j = 0; j < N; j++) {
            if (i != j && c->n[i].up && c->n[j].port != 0) {
                REQ(hx_raftnet_set_peer(c->n[i].net, j, "127.0.0.1", c->n[j].port) == 0);
            }
        }
    }
    return true;
}

static void node_stop(tnode *t)
{
    if (!t->up) {
        return;
    }
    hx_raftnet_destroy(t->net);
    hx_raft_destroy(t->r);
    hx_raft_wal_close(t->wal);
    t->up = false;
}

static void pump(cluster *c, int ms)
{
    const int64_t end = now_ms() + ms;
    do {
        for (uint32_t i = 0; i < N; i++) {
            tnode *t = &c->n[i];
            if (!t->up) {
                continue;
            }
            const int64_t now = now_ms();
            (void)hx_raft_tick(t->r, now);
            flush_out(t, now);
            (void)hx_raftnet_poll(t->net, 1, now_ms());
        }
    } while (now_ms() < end);
}

static uint32_t leader_of(const cluster *c)
{
    for (uint32_t i = 0; i < N; i++) {
        if (c->n[i].up && hx_raft_get_role(c->n[i].r) == HX_RAFT_LEADER) {
            return i;
        }
    }
    return HX_RAFT_NONE;
}

static bool await_leader(cluster *c, uint32_t *out)
{
    const int64_t end = now_ms() + 15000;
    while (now_ms() < end) {
        pump(c, 20);
        const uint32_t l = leader_of(c);
        if (l != HX_RAFT_NONE) {
            *out = l;
            return true;
        }
    }
    return false;
}

/* Waits until every running node has committed `idx`. */
static bool await_commit(cluster *c, uint64_t idx)
{
    const int64_t end = now_ms() + 15000;
    while (now_ms() < end) {
        pump(c, 20);
        bool all = true;
        for (uint32_t i = 0; i < N; i++) {
            all = all && (!c->n[i].up || hx_raft_commit_index(c->n[i].r) >= idx);
        }
        if (all) {
            return true;
        }
    }
    return false;
}

static bool propose_n(cluster *c, uint32_t count, uint64_t *last)
{
    for (uint32_t k = 0; k < count; k++) {
        uint32_t l = leader_of(c);
        REQ(l != HX_RAFT_NONE);
        char buf[48];
        const int len = snprintf(buf, sizeof buf, "cmd-%u-%llu", k, (unsigned long long)now_ms());
        uint64_t idx = 0;
        REQ(hx_raft_propose(c->n[l].r, now_ms(), buf, (uint32_t)len, &idx, NULL) == HX_RAFT_OK);
        flush_out(&c->n[l], now_ms());
        *last = idx;
        if (k % 10U == 9U) {
            pump(c, 5);
        }
    }
    return true;
}

static bool logs_equal(const cluster *c, uint64_t upto)
{
    uint64_t lo = 0; /* entries inside any node's snapshot are no longer comparable */
    for (uint32_t k = 0; k < N; k++) {
        if (c->n[k].up && hx_raft_snapshot_index(c->n[k].r) > lo) {
            lo = hx_raft_snapshot_index(c->n[k].r);
        }
    }
    for (uint64_t i = lo + 1U; i <= upto; i++) {
        hx_raft_entry ref;
        REQ(c->n[0].up ? hx_raft_entry_at(c->n[0].r, i, &ref)
                       : hx_raft_entry_at(c->n[1].r, i, &ref));
        for (uint32_t k = 0; k < N; k++) {
            if (!c->n[k].up) {
                continue;
            }
            hx_raft_entry e;
            REQ(hx_raft_entry_at(c->n[k].r, i, &e));
            REQ(e.term == ref.term && e.len == ref.len && memcmp(e.data, ref.data, e.len) == 0);
        }
    }
    return true;
}

static bool hostile_clients(cluster *c)
{
    const uint16_t port = c->n[0].port;
    static const uint8_t hello_ok[16] = {0, 0, 0, 12, 0x48, 0x58, 0x52, 0x31,
                                         0, 0, 0, 1,  0,    0,    0,    3};
    uint8_t cases[5][40];
    size_t lens[5];
    memset(cases, 0, sizeof cases);
    memset(cases[0], 0xAB, 40); /* garbage */
    lens[0] = 40;
    memcpy(cases[1], hello_ok, 16); /* valid hello, then an absurd frame length */
    memset(cases[1] + 16, 0xFF, 4);
    lens[1] = 20;
    memcpy(cases[2], hello_ok, 16); /* wrong cluster size */
    cases[2][15] = 7;
    lens[2] = 16;
    memcpy(cases[3], hello_ok, 16); /* claims to be node 0, the node it talks to */
    cases[3][11] = 0;
    lens[3] = 16;
    memcpy(cases[4], hello_ok, 16); /* hello then a truncated frame, then EOF */
    cases[4][16] = 0;
    cases[4][19] = 50;
    lens[4] = 24;

    const uint64_t before = hx_raftnet_get_stats(c->n[0].net).bad_peers;
    for (int k = 0; k < 5; k++) {
        const int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in a;
        memset(&a, 0, sizeof a);
        a.sin_family = AF_INET;
        a.sin_port = htons(port);
        (void)inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
        REQ(fd >= 0 && connect(fd, (struct sockaddr *)&a, sizeof a) == 0);
        REQ(send(fd, cases[k], lens[k], MSG_NOSIGNAL) == (ssize_t)lens[k]);
        if (k == 4) {
            (void)close(fd); /* EOF mid-frame */
            pump(c, 100);
            continue;
        }
        pump(c, 150);
        uint8_t b[8];
        const ssize_t r = recv(fd, b, sizeof b, MSG_DONTWAIT);
        REQ(r == 0); /* the node closed it */
        (void)close(fd);
    }
    REQ(hx_raftnet_get_stats(c->n[0].net).bad_peers >= before + 5U);
    return true;
}

static bool unreachable_peer(void)
{
    hx_raftnet_stats st;
    const hx_raftnet_cfg nc = {.self = 0,
                               .nnodes = 2,
                               .bind_addr = "127.0.0.1",
                               .on_frame = on_frame,
                               .max_queue = 1U << 16};
    hx_raftnet *n = hx_raftnet_create(&nc);
    REQ(n != NULL);
    REQ(hx_raftnet_set_peer(n, 1, "127.0.0.1", 1) == 0); /* nothing listens on port 1 */
    uint8_t payload[1024] = {0};
    const int64_t t0 = now_ms();
    int queued = 0;
    for (int i = 0; i < 100000; i++) {
        if (hx_raftnet_send(n, 1, payload, sizeof payload, now_ms()) == 0) {
            queued++;
        }
        if (i % 100 == 0) {
            (void)hx_raftnet_poll(n, 0, now_ms());
        }
    }
    st = hx_raftnet_get_stats(n);
    REQ(now_ms() - t0 < 3000); /* never blocked */
    REQ(st.dropped > 90000);
    REQ(queued < 1000);
    REQ(hx_raftnet_send(n, 0, payload, 1, now_ms()) != 0); /* to self */
    REQ(hx_raftnet_send(n, 5, payload, 1, now_ms()) != 0); /* unknown node */
    hx_raftnet_destroy(n);
    const hx_raftnet_cfg bad = {
        .self = 3, .nnodes = 2, .bind_addr = "127.0.0.1", .on_frame = on_frame};
    REQ(hx_raftnet_create(&bad) == NULL);
    return true;
}

static bool cluster_body(cluster *c)
{
    for (uint32_t i = 0; i < N; i++) {
        (void)snprintf(c->n[i].path, sizeof c->n[i].path, "%s/n%u.wal", c->dir, i);
        REQ(node_start(c, i, 0));
    }
    REQ(wire_peers(c));
    uint32_t l = 0;
    uint64_t last = 0;
    if (!await_leader(c, &l)) {
        for (uint32_t i = 0; i < N; i++) {
            const hx_raftnet_stats st = hx_raftnet_get_stats(c->n[i].net);
            (void)fprintf(
                stderr, "node %u role=%d term=%llu in=%llu out=%llu drop=%llu conn=%llu bad=%llu\n",
                i, (int)hx_raft_get_role(c->n[i].r), (unsigned long long)hx_raft_term(c->n[i].r),
                (unsigned long long)st.frames_in, (unsigned long long)st.frames_out,
                (unsigned long long)st.dropped, (unsigned long long)st.connects,
                (unsigned long long)st.bad_peers);
        }
        return false;
    }
    REQ(propose_n(c, 100, &last));
    REQ(await_commit(c, last));
    REQ(logs_equal(c, last));
    REQ(hostile_clients(c));

    /* A follower dies and comes back from its WAL on the same port. */
    const uint32_t f = (l + 1U) % N;
    const uint16_t fport = c->n[f].port;
    node_stop(&c->n[f]);
    REQ(propose_n(c, 50, &last));
    REQ(await_commit(c, last));
    /* The two live nodes compact their logs while the third is away: it can only catch up from a
     * snapshot, which has to travel over TCP. */
    uint8_t image[16] = {0};
    for (uint32_t i = 0; i < N; i++) {
        if (c->n[i].up) {
            REQ(hx_raft_snapshot(c->n[i].r, now_ms(), hx_raft_commit_index(c->n[i].r), image,
                                 sizeof image) == 0);
        }
    }
    REQ(node_start(c, f, fport));
    REQ(wire_peers(c));
    REQ(await_commit(c, last));
    REQ(logs_equal(c, last));
    {
        uint64_t si = 0;
        uint64_t st = 0;
        const uint8_t *data = NULL;
        uint32_t dl = 0;
        REQ(hx_raft_take_installed_snapshot(c->n[f].r, &si, &st, &data, &dl));
        REQ(si > 100 && dl == sizeof image && hx_raft_snapshot_index(c->n[f].r) == si);
    }

    /* The leader dies: the others elect a new one and keep committing. */
    l = leader_of(c);
    REQ(l != HX_RAFT_NONE);
    const uint16_t lport = c->n[l].port;
    const uint64_t term_before = hx_raft_term(c->n[l].r);
    node_stop(&c->n[l]);
    uint32_t l2 = 0;
    REQ(await_leader(c, &l2));
    REQ(l2 != l && hx_raft_term(c->n[l2].r) > term_before);
    REQ(propose_n(c, 30, &last));
    REQ(await_commit(c, last));
    REQ(node_start(c, l, lport));
    REQ(wire_peers(c));
    REQ(await_commit(c, last));
    REQ(logs_equal(c, last));
    for (uint32_t i = 0; i < N; i++) {
        REQ(c->n[i].bad_frames == 0);
    }
    (void)printf("raft over TCP: %llu entries replicated through follower and leader restarts, one "
                 "follower caught up from a snapshot\n",
                 (unsigned long long)last);
    return true;
}

int main(void)
{
    cluster *c = calloc(1, sizeof *c);
    const char *t = getenv("TMPDIR");
    (void)snprintf(c->dir, sizeof c->dir, "%s/helix-rnet-XXXXXX",
                   (t != NULL && *t != '\0') ? t : "/tmp");
    if (mkdtemp(c->dir) == NULL) {
        return 2;
    }
    if (!cluster_body(c)) {
        failures++;
    }
    for (uint32_t i = 0; i < N; i++) {
        node_stop(&c->n[i]);
    }
    (void)nftw(c->dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
    free(c);
    if (!unreachable_peer()) {
        failures++;
    }
    if (failures != 0) {
        (void)fputs("raft TCP tests FAILED\n", stderr);
        return 1;
    }
    (void)puts("all raft TCP tests passed");
    return 0;
}
