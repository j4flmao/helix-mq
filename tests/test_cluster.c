/*
 * A whole cluster in one process over real TCP on loopback: three controller nodes (Raft with WAL
 * files, controller host, broker-facing server) and three brokers (replica manager: controller
 * client, replica transport, one real log per partition). Nothing here is simulated except that all
 * nodes are driven by a single thread.
 *
 * Scenario: the cluster elects controller and partition leaders; data is produced and replicated;
 * a partition leader broker is killed -> the controller notices, elects another replica, the new
 * leader accepts writes, nothing committed is lost; the broker comes back with a new process life,
 * re-registers, catches up and rejoins; the controller leader is killed and restarted while
 * brokers keep their sessions. Every wait has a generous real-time deadline.
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

#include "ctrl/ctrl_net.h"
#include "raft/raft_net.h"
#include "raft/raft_wal.h"
#include "repl/replica_mgr.h"

#define NC 3U
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
    uint32_t id;
    bool up;
    char walpath[320];
    hx_raft_wal *wal;
    hx_chost *h;
    hx_raftnet *rn;
    hx_ctlsrv *srv;
    uint16_t rport;
    uint16_t cport;
} cnode;

typedef struct {
    uint32_t id;
    bool up;
    char dir[320];
    hx_rmgr *m;
    uint16_t port;
    uint64_t nonce;
} bnode;

typedef struct {
    char dir[256];
    cnode c[NC];
    bnode b[NB];
    uint64_t nonce_ctr;
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

/* ------------------------------------------------------------- controllers */

static void raft_send(void *ctx, const hx_raft_msg *m)
{
    cnode *n = ctx;
    hx_buf b;
    hx_buf_init(&b);
    if (hx_raft_msg_encode(m, &b) == 0) {
        (void)hx_raftnet_send(n->rn, m->to, b.data, b.len, now_ms());
    }
    hx_buf_free(&b);
}

static void raft_frame(void *ctx, uint32_t from, const uint8_t *p, size_t len)
{
    cnode *n = ctx;
    hx_raft_entry scratch[256];
    hx_raft_msg m;
    if (hx_raft_msg_decode(p, len, scratch, 256, &m) == 0 && m.from == from && m.to == n->id) {
        (void)hx_chost_step(n->h, now_ms(), &m);
    }
}

static void wire_controllers(cluster *c)
{
    for (uint32_t i = 0; i < NC; i++) {
        for (uint32_t j = 0; j < NC; j++) {
            if (i != j && c->c[i].up && c->c[j].rport != 0) {
                (void)hx_raftnet_set_peer(c->c[i].rn, j, "127.0.0.1", c->c[j].rport);
            }
        }
    }
}

static bool start_controller(cluster *c, uint32_t id)
{
    cnode *n = &c->c[id];
    n->id = id;
    REQ(hx_raft_wal_open(n->walpath, &n->wal) == HX_WAL_OK);
    hx_raft_wal_set_nosync_for_tests(n->wal, true);
    hx_chost_cfg cfg;
    hx_chost_cfg_default(&cfg, id, NC);
    cfg.raft.seed = 1234U + id;
    cfg.raft.storage = hx_raft_wal_storage(n->wal);
    cfg.send_ctx = n;
    cfg.send = raft_send;
    /* Match the daemon's failure detector. A 700 ms wall-clock budget can expire during
     * sanitizer/emulated CI scheduling pauses and legitimately re-register a live broker,
     * invalidating the session-preservation scenario rather than testing controller failover. */
    cfg.down_ms = 3000;
    cfg.retry_ms = 200;
    cfg.snapshot_every = 64;
    const hx_raft_restore rs = hx_raft_wal_restore(n->wal);
    n->h = hx_chost_create(&cfg, &rs, now_ms());
    REQ(n->h != NULL);
    const hx_raftnet_cfg rc = {.self = id,
                               .nnodes = NC,
                               .bind_addr = "127.0.0.1",
                               .port = 0,
                               .on_frame = raft_frame,
                               .ctx = n};
    n->rn = hx_raftnet_create(&rc);
    REQ(n->rn != NULL);
    n->rport = hx_raftnet_port(n->rn);
    const hx_ctlsrv_cfg sc = {.bind_addr = "127.0.0.1", .port = n->cport, .push_ms = 200};
    n->srv = hx_ctlsrv_create(&sc, n->h);
    REQ(n->srv != NULL);
    n->cport = hx_ctlsrv_port(n->srv);
    n->up = true;
    wire_controllers(c);
    return true;
}

static void stop_controller(cnode *n)
{
    if (!n->up) {
        return;
    }
    hx_ctlsrv_destroy(n->srv);
    hx_raftnet_destroy(n->rn);
    hx_chost_destroy(n->h);
    hx_raft_wal_close(n->wal);
    n->up = false;
}

/* ----------------------------------------------------------------- brokers */

static void wire_brokers(cluster *c)
{
    for (uint32_t i = 0; i < NB; i++) {
        for (uint32_t j = 0; j < NB; j++) {
            if (i != j && c->b[i].up && c->b[j].up) {
                (void)hx_rmgr_set_peer(c->b[i].m, j, "127.0.0.1", c->b[j].port);
            }
        }
    }
}

static bool start_broker(cluster *c, uint32_t id)
{
    bnode *b = &c->b[id];
    b->id = id;
    b->nonce = (++c->nonce_ctr << 20) ^ (uint64_t)now_ms();
    hx_rmgr_cfg cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.self = id;
    cfg.nbrokers = NB;
    cfg.data_dir = b->dir;
    cfg.bind_addr = "127.0.0.1";
    cfg.nonce = b->nonce;
    cfg.ncontrollers = NC;
    for (uint32_t i = 0; i < NC; i++) {
        cfg.ctrl_addrs[i] = "127.0.0.1";
        cfg.ctrl_ports[i] = c->c[i].cport;
    }
    cfg.min_isr = 2;
    cfg.lag_ms = 1500;
    cfg.fetch_ms = 10;
    b->m = hx_rmgr_create(&cfg);
    REQ(b->m != NULL);
    for (uint32_t p = 0; p < NP; p++) {
        REQ(hx_rmgr_add_partition(b->m, p, 7) == 0);
    }
    b->port = hx_rmgr_port(b->m);
    b->up = true;
    wire_brokers(c);
    return true;
}

static void stop_broker(bnode *b)
{
    if (b->up) {
        hx_rmgr_destroy(b->m);
        b->up = false;
    }
}

/* -------------------------------------------------------------------- loop */

static int ready_controller(const cluster *c)
{
    for (uint32_t i = 0; i < NC; i++) {
        if (c->c[i].up && hx_chost_ready(c->c[i].h)) {
            return (int)i;
        }
    }
    return -1;
}

static void run_for(cluster *c, int ms)
{
    const int64_t end = now_ms() + ms;
    while (now_ms() < end) {
        const int64_t now = now_ms();
        for (uint32_t i = 0; i < NC; i++) {
            if (c->c[i].up) {
                (void)hx_raftnet_poll(c->c[i].rn, 0, now);
                (void)hx_chost_tick(c->c[i].h, now);
                hx_ctlsrv_poll(c->c[i].srv, 0, now);
            }
        }
        const int rc = ready_controller(c);
        if (rc >= 0 && hx_ctrl_nparts(hx_chost_ctrl(c->c[rc].h)) < NP) {
            (void)hx_chost_create_partition(c->c[rc].h, now,
                                            (uint32_t)hx_ctrl_nparts(hx_chost_ctrl(c->c[rc].h)), 7);
        }
        for (uint32_t i = 0; i < NB; i++) {
            if (c->b[i].up) {
                hx_rmgr_poll(c->b[i].m, 0, now);
            }
        }
        (void)usleep(500);
    }
}

typedef bool (*cond_fn)(cluster *c, void *arg);

static bool wait_for(cluster *c, cond_fn f, void *arg, int timeout_ms)
{
    const int64_t end = now_ms() + timeout_ms;
    while (now_ms() < end) {
        run_for(c, 20);
        if (f(c, arg)) {
            return true;
        }
    }
    return false;
}

static int leader_of(cluster *c, uint32_t part)
{
    int best = -1;
    uint32_t best_epoch = 0;
    for (uint32_t i = 0; i < NB; i++) {
        hx_rmgr_part_info in;
        if (c->b[i].up && hx_rmgr_info(c->b[i].m, part, &in) == 0 && in.role == HX_REPL_LEADER &&
            in.epoch >= best_epoch) {
            best = (int)i;
            best_epoch = in.epoch;
        }
    }
    return best;
}

static bool all_registered_with_leaders(cluster *c, void *arg)
{
    (void)arg;
    for (uint32_t i = 0; i < NB; i++) {
        if (c->b[i].up && hx_rmgr_session(c->b[i].m) == 0) {
            return false;
        }
    }
    for (uint32_t p = 0; p < NP; p++) {
        if (leader_of(c, p) < 0) {
            return false;
        }
    }
    return true;
}

typedef struct {
    uint32_t part;
    uint64_t hw;
    uint32_t only_n;
    uint32_t only[NB];
} hw_arg;

static bool hw_reached(cluster *c, void *arg)
{
    const hw_arg *a = arg;
    for (uint32_t i = 0; i < NB; i++) {
        if (!c->b[i].up) {
            continue;
        }
        hx_rmgr_part_info in;
        if (hx_rmgr_info(c->b[i].m, a->part, &in) != 0 || in.hw < a->hw || in.failed) {
            return false;
        }
    }
    return true;
}

static bool produce(cluster *c, uint32_t part, uint32_t first_tag, uint32_t count)
{
    int l = leader_of(c, part);
    REQ(l >= 0);
    for (uint32_t k = 0; k < count; k++) {
        char text[40];
        (void)snprintf(text, sizeof text, "p%u-%u", part, first_tag + k);
        const hx_repl_rec r = {
            .epoch = 0, .len = (uint32_t)strlen(text), .data = (const uint8_t *)text};
        hx_repl_status st = HX_REPL_FAILED;
        for (int tries = 0; tries < 400; tries++) {
            l = leader_of(c, part);
            st = l >= 0 ? hx_rmgr_append(c->b[l].m, part, now_ms(), &r, 1, NULL)
                        : HX_REPL_NOT_LEADER;
            if (st == HX_REPL_OK) {
                break;
            }
            run_for(c, 10);
        }
        REQ(st == HX_REPL_OK);
        if (k % 8U == 7U) {
            run_for(c, 4);
        }
    }
    return true;
}

/* All running brokers hold byte-identical committed prefixes of length `n`, tags 0..n-1 in order.
 */
static bool verify_prefix(cluster *c, uint32_t part, uint64_t n)
{
    for (uint64_t off = 0; off < n; off++) {
        char want[40];
        (void)snprintf(want, sizeof want, "p%u-%llu", part, (unsigned long long)off);
        for (uint32_t i = 0; i < NB; i++) {
            if (!c->b[i].up) {
                continue;
            }
            char got[64];
            uint32_t len = 0;
            REQ(hx_rmgr_read(c->b[i].m, part, off, got, sizeof got, &len) == HX_OK);
            REQ(len == strlen(want) && memcmp(got, want, len) == 0);
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

static void be32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

/* Broker-facing port: forged identity, garbage and oversized frames end in a closed connection. */
static bool hostile_clients(cluster *c)
{
    const uint64_t before = hx_ctlsrv_get_stats(c->c[0].srv).bad_conns;
    uint8_t hello[4 + 1 + 4 + 8];
    be32(hello, 13);
    hello[4] = 1;       /* HELLO */
    be32(hello + 5, 0); /* broker 0 */
    memset(hello + 9, 0x11, 8);
    uint8_t alter[4 + 1 + 4 + 4 + 8 + 4 + 4 + 4];
    memset(alter, 0, sizeof alter);
    be32(alter, (uint32_t)sizeof alter - 4U);
    alter[4] = 2;       /* ALTER */
    be32(alter + 9, 1); /* ... claiming to be broker 1 */
    int fd = raw_connect(c->c[0].cport);
    REQ(fd >= 0);
    REQ(write(fd, hello, sizeof hello) == (ssize_t)sizeof hello);
    REQ(write(fd, alter, sizeof alter) == (ssize_t)sizeof alter);
    run_for(c, 150);
    (void)close(fd);
    fd = raw_connect(c->c[0].cport);
    REQ(fd >= 0);
    uint8_t huge[8];
    be32(huge, 0x7FFFFFFFU);
    huge[4] = 1;
    REQ(write(fd, huge, 5) == 5);
    run_for(c, 150);
    (void)close(fd);
    fd = raw_connect(c->c[0].cport);
    REQ(fd >= 0);
    uint8_t junk[9] = {0, 0, 0, 5, 99, 1, 2, 3, 4}; /* unknown frame type */
    REQ(write(fd, junk, sizeof junk) == (ssize_t)sizeof junk);
    run_for(c, 150);
    (void)close(fd);
    REQ(hx_ctlsrv_get_stats(c->c[0].srv).bad_conns >= before + 3U);
    return true;
}

static bool scenario(cluster *c)
{
    for (uint32_t i = 0; i < NC; i++) {
        (void)snprintf(c->c[i].walpath, sizeof c->c[i].walpath, "%s/ctl%u.wal", c->dir, i);
        REQ(start_controller(c, i));
    }
    for (uint32_t i = 0; i < NB; i++) {
        (void)snprintf(c->b[i].dir, sizeof c->b[i].dir, "%s/broker%u", c->dir, i);
        REQ(start_broker(c, i));
    }
    REQ(wait_for(c, all_registered_with_leaders, NULL, 15000));
    REQ(hostile_clients(c));

    /* 1. Replicate. Offsets and tags must line up, so produce per partition in order. */
    REQ(produce(c, 0, 0, 100));
    REQ(produce(c, 1, 0, 60));
    hw_arg a0 = {.part = 0, .hw = 100};
    hw_arg a1 = {.part = 1, .hw = 60};
    REQ(wait_for(c, hw_reached, &a0, 15000));
    REQ(wait_for(c, hw_reached, &a1, 15000));
    REQ(verify_prefix(c, 0, 100) && verify_prefix(c, 1, 60));

    /* 2. Kill the leader broker of partition 0. */
    const int old_leader = leader_of(c, 0);
    REQ(old_leader >= 0);
    hx_rmgr_part_info before;
    REQ(hx_rmgr_info(c->b[old_leader].m, 0, &before) == 0);
    stop_broker(&c->b[old_leader]);
    int64_t deadline = now_ms() + 20000;
    while (now_ms() < deadline) {
        run_for(c, 20);
        const int l = leader_of(c, 0);
        if (l >= 0 && l != old_leader) {
            break;
        }
    }
    const int new_leader = leader_of(c, 0);
    REQ(new_leader >= 0 && new_leader != old_leader);
    hx_rmgr_part_info after;
    REQ(hx_rmgr_info(c->b[new_leader].m, 0, &after) == 0 && after.epoch > before.epoch);
    REQ(produce(c, 0, 100, 50));
    a0.hw = 150;
    REQ(wait_for(c, hw_reached, &a0, 15000));
    REQ(verify_prefix(c, 0, 150));

    /* 3. The broker comes back as a new process life and catches up. */
    REQ(start_broker(c, (uint32_t)old_leader));
    REQ(wait_for(c, all_registered_with_leaders, NULL, 15000));
    REQ(produce(c, 0, 150, 20));
    a0.hw = 170;
    REQ(wait_for(c, hw_reached, &a0, 20000));
    REQ(verify_prefix(c, 0, 170));
    hx_rmgr_part_info back;
    REQ(hx_rmgr_info(c->b[old_leader].m, 0, &back) == 0 && back.leo >= 170 && !back.failed);

    /* 4. Kill the controller leader; brokers keep their sessions, data keeps flowing. */
    uint64_t sessions[NB];
    for (uint32_t i = 0; i < NB; i++) {
        sessions[i] = hx_rmgr_session(c->b[i].m);
    }
    const int cl = ready_controller(c);
    REQ(cl >= 0);
    stop_controller(&c->c[cl]);
    deadline = now_ms() + 15000;
    while (now_ms() < deadline && ready_controller(c) < 0) {
        run_for(c, 20);
    }
    REQ(ready_controller(c) >= 0);
    REQ(produce(c, 1, 60, 30));
    a1.hw = 90;
    REQ(wait_for(c, hw_reached, &a1, 15000));
    REQ(verify_prefix(c, 1, 90));
    for (uint32_t i = 0; i < NB; i++) {
        REQ(hx_rmgr_session(c->b[i].m) == sessions[i]);
    }

    /* 5. The controller node returns and catches up from its WAL and the leader. */
    REQ(start_controller(c, (uint32_t)cl));
    run_for(c, 2500);
    REQ(c->c[cl].up && !hx_chost_failed(c->c[cl].h));
    const int lead = ready_controller(c);
    REQ(lead >= 0);
    REQ(hx_chost_applied(c->c[cl].h) + 5U >= hx_chost_applied(c->c[lead].h));
    REQ(hx_ctrl_nparts(hx_chost_ctrl(c->c[cl].h)) == NP);
    return true;
}

int main(void)
{
    cluster c;
    memset(&c, 0, sizeof c);
    const char *t = getenv("TMPDIR");
    (void)snprintf(c.dir, sizeof c.dir, "%s/helix-cluster-XXXXXX",
                   (t != NULL && *t != '\0') ? t : "/tmp");
    if (mkdtemp(c.dir) == NULL) {
        perror("mkdtemp");
        return 2;
    }
    const bool ok = scenario(&c);
    for (uint32_t i = 0; i < NB; i++) {
        stop_broker(&c.b[i]);
    }
    for (uint32_t i = 0; i < NC; i++) {
        stop_controller(&c.c[i]);
    }
    (void)nftw(c.dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
    if (!ok || failures != 0) {
        (void)fprintf(stderr, "cluster test failed\n");
        return 1;
    }
    (void)puts("all cluster tests passed");
    return 0;
}
