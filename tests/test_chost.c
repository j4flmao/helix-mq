/*
 * Controller on Raft under fault injection.
 *
 * Three controller nodes (hx_chost = hx_raft + hx_ctrl, Raft log in real WAL files) and five
 * simulated brokers talk over a lossy, delaying, duplicating network. Faults: controller-node
 * partitions and crashes (restart from the WAL), brokers dying and restarting with a new nonce,
 * brokers cut off from the controllers, lost/duplicated/stale requests. After every few
 * milliseconds the checker asserts
 *   - every controller node that reaches Raft index i has the same controller state at i,
 *   - for each partition the history (isr_version -> epoch, leader, ISR) is one single line,
 *   - controller invariants hold on every node (leader in the ISR and alive, ...),
 *   - a broker only ever adopts a session created for its own process life (nonce check),
 * and after healing, within a bounded time, that a ready leader exists, every running broker is
 * registered with the session it adopted, every dead broker is marked down, and each partition
 * with a live ISR member has a live leader.
 *
 * HX_CHOST_SEEDS=n sets the number of scenarios (default 40).
 */
#define _XOPEN_SOURCE 700
#include <ftw.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "ctrl/ctrl_host.h"
#include "raft/raft_wal.h"

#define NC       3U /* controller nodes */
#define NB       5U /* brokers */
#define NPARTS   6U
#define HIST_MAX 4096U
#define UNSET    0xFFFFFFFEU

typedef enum { F_RAFT, F_HELLO, F_REPLY, F_ALTER } fkind;

typedef struct {
    int64_t at;
    fkind kind;
    uint32_t from;
    uint32_t to;
    hx_buf bytes;
    uint64_t v[6];
} flight;

typedef struct sim sim;

typedef struct {
    sim *s;
    uint32_t id;
} sendctx;

typedef struct {
    hx_chost *h;
    hx_raft_wal *wal;
    sendctx sc;
    bool up;
    int64_t restart_at;
    uint64_t checked_applied;
    char path[300];
} cnode;

typedef struct {
    bool up;
    int64_t restart_at;
    uint64_t nonce;
    uint64_t session; /* adopted, 0 if none */
    bool cut;
    uint32_t cached_epoch[NPARTS]; /* possibly stale view of a partition, used for stale ALTERs */
    uint32_t cached_ver[NPARTS];
} bnode;

typedef struct {
    uint32_t epoch;
    uint32_t leader;
    uint32_t isr;
} histent;

struct sim {
    char dir[200];
    cnode c[NC];
    bnode b[NB];
    int group[NC];
    int64_t now;
    uint64_t rng;
    uint64_t seed;
    uint64_t nonce_ctr;
    uint32_t loss_pm;
    uint32_t bc_loss_pm;
    uint32_t dup_pm;
    uint32_t crash_every;
    flight *net;
    size_t nnet;
    size_t capnet;
    histent hist[NPARTS][HIST_MAX];
    uint32_t replicas[NPARTS];
    bool bad;
    char why[300];
    uint64_t st_crashes;
    uint64_t st_kills;
    uint64_t st_partitions;
    uint64_t st_leaders; /* distinct (term) leaderships seen ready */
    uint64_t st_downs;
    uint64_t st_ups;
    uint64_t st_snaps;
    uint64_t last_ready_term;
};

static int rm_cb(const char *p, const struct stat *st, int t, struct FTW *f)
{
    (void)st;
    (void)t;
    (void)f;
    return remove(p);
}

static uint64_t rnd(sim *s)
{
    s->rng ^= s->rng >> 12;
    s->rng ^= s->rng << 25;
    s->rng ^= s->rng >> 27;
    return s->rng * 0x2545F4914F6CDD1DULL;
}

__attribute__((format(printf, 2, 3))) static void fail(sim *s, const char *fmt, ...)
{
    if (s->bad) {
        return;
    }
    s->bad = true;
    va_list ap;
    va_start(ap, fmt);
    // NOLINTNEXTLINE(clang-analyzer-valist.Uninitialized): analyzer false positive, va_start above
    (void)vsnprintf(s->why, sizeof s->why, fmt, ap);
    va_end(ap);
}

static void push(sim *s, fkind kind, uint32_t from, uint32_t to, const hx_buf *bytes,
                 const uint64_t *v, int64_t at)
{
    if (s->nnet == s->capnet) {
        const size_t ncap = s->capnet == 0 ? 256U : s->capnet * 2U;
        flight *f = realloc(s->net, ncap * sizeof *f);
        if (f == NULL) {
            fail(s, "out of memory");
            return;
        }
        s->net = f;
        s->capnet = ncap;
    }
    flight *f = &s->net[s->nnet++];
    memset(f, 0, sizeof *f);
    hx_buf_init(&f->bytes);
    if (bytes != NULL) {
        (void)hx_buf_put(&f->bytes, bytes->data, bytes->len);
    }
    f->kind = kind;
    f->from = from;
    f->to = to;
    f->at = at;
    if (v != NULL) {
        memcpy(f->v, v, sizeof f->v);
    }
}

static int64_t delay(sim *s)
{
    return s->now + 1 + (int64_t)(rnd(s) % 12U);
}

static void on_raft_send(void *ctx, const hx_raft_msg *m)
{
    sendctx *sc = ctx;
    sim *s = sc->s;
    if (m->to >= NC || s->group[m->from] != s->group[m->to] || rnd(s) % 1000U < s->loss_pm) {
        return;
    }
    hx_buf b;
    hx_buf_init(&b);
    if (hx_raft_msg_encode(m, &b) != 0) {
        fail(s, "raft encode failed");
    } else {
        push(s, F_RAFT, m->from, m->to, &b, NULL, delay(s));
        if (rnd(s) % 1000U < s->dup_pm) {
            push(s, F_RAFT, m->from, m->to, &b, NULL, delay(s));
        }
    }
    hx_buf_free(&b);
}

static bool cnode_start(sim *s, uint32_t i)
{
    cnode *n = &s->c[i];
    if (hx_raft_wal_open(n->path, &n->wal) != 0) {
        fail(s, "wal open failed for node %u", i);
        return false;
    }
    hx_raft_wal_set_nosync_for_tests(n->wal, true);
    hx_chost_cfg cfg;
    hx_chost_cfg_default(&cfg, i, NC);
    cfg.raft.seed = s->seed * 31U + i + s->now;
    cfg.raft.storage = hx_raft_wal_storage(n->wal);
    cfg.send_ctx = &n->sc;
    cfg.send = on_raft_send;
    cfg.down_ms = 400;
    cfg.retry_ms = 120;
    cfg.snapshot_every = 40;
    const hx_raft_restore rs = hx_raft_wal_restore(n->wal);
    n->h = hx_chost_create(&cfg, &rs, s->now);
    if (n->h == NULL) {
        fail(s, "controller node %u failed to start", i);
        return false;
    }
    n->up = true;
    n->checked_applied = 0;
    return true;
}

static void cnode_stop(cnode *n)
{
    hx_chost_destroy(n->h);
    n->h = NULL;
    hx_raft_wal_close(n->wal);
    n->wal = NULL;
    n->up = false;
}

static void broker_start(sim *s, uint32_t i)
{
    bnode *b = &s->b[i];
    b->up = true;
    b->nonce = (s->nonce_ctr++ * 0x9E3779B97F4A7C15ULL) ^ s->seed ^ 0x5555U;
    b->session = 0;
}

static bool sim_init(sim *s, uint64_t seed)
{
    memset(s, 0, sizeof *s);
    s->seed = seed;
    s->rng = seed * 0x9E3779B97F4A7C15ULL + 99U;
    s->loss_pm = 20 + (uint32_t)(seed % 5U) * 20U;
    s->bc_loss_pm = 50 + (uint32_t)(seed % 4U) * 60U;
    s->dup_pm = 30;
    s->crash_every = (seed & 1U) != 0 ? 1500U : 6000U;
    const char *t = getenv("TMPDIR");
    (void)snprintf(s->dir, sizeof s->dir, "%s/helix-chost-XXXXXX",
                   (t != NULL && *t != '\0') ? t : "/tmp");
    if (mkdtemp(s->dir) == NULL) {
        perror("mkdtemp");
        return false;
    }
    for (uint32_t i = 0; i < NPARTS; i++) {
        for (uint32_t k = 0; k < HIST_MAX; k++) {
            s->hist[i][k].epoch = UNSET;
        }
        uint32_t m = 0;
        while (__builtin_popcount(m) != 3) {
            m = (uint32_t)(rnd(s) % (1U << NB));
        }
        s->replicas[i] = m;
    }
    for (uint32_t i = 0; i < NC; i++) {
        s->c[i].sc.s = s;
        s->c[i].sc.id = i;
        (void)snprintf(s->c[i].path, sizeof s->c[i].path, "%s/node%u.wal", s->dir, i);
        if (!cnode_start(s, i)) {
            return false;
        }
    }
    for (uint32_t i = 0; i < NB; i++) {
        broker_start(s, i);
    }
    return true;
}

static void sim_free(sim *s)
{
    for (uint32_t i = 0; i < NC; i++) {
        if (s->c[i].up) {
            cnode_stop(&s->c[i]);
        }
    }
    for (size_t i = 0; i < s->nnet; i++) {
        hx_buf_free(&s->net[i].bytes);
    }
    free(s->net);
    (void)nftw(s->dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
}

/* ------------------------------------------------------------------ checks */

static uint64_t state_hash(const hx_ctrl *c)
{
    hx_buf b;
    hx_buf_init(&b);
    uint64_t h = 1469598103934665603ULL;
    if (hx_ctrl_snapshot(c, &b) == 0) {
        for (size_t i = 0; i < b.len; i++) {
            h = (h ^ b.data[i]) * 1099511628211ULL;
        }
    }
    hx_buf_free(&b);
    return h;
}

typedef struct {
    uint64_t hash;
    bool set;
} idxhash;

static idxhash *g_hash;
static size_t g_hash_cap;

static void check_nodes(sim *s)
{
    for (uint32_t i = 0; i < NC && !s->bad; i++) {
        cnode *n = &s->c[i];
        if (!n->up) {
            continue;
        }
        if (hx_chost_failed(n->h)) {
            fail(s, "controller node %u failed", i);
            return;
        }
        const hx_ctrl *c = hx_chost_ctrl(n->h);
        if (hx_ctrl_check(c) != 0) {
            fail(s, "controller invariant broken on node %u", i);
            return;
        }
        const uint64_t ap = hx_chost_applied(n->h);
        if (ap != n->checked_applied) {
            n->checked_applied = ap;
            if (ap < g_hash_cap) {
                const uint64_t h = state_hash(c);
                if (g_hash[ap].set && g_hash[ap].hash != h) {
                    fail(s,
                         "STATE MACHINE SAFETY: nodes disagree on controller state at index %llu",
                         (unsigned long long)ap);
                    return;
                }
                g_hash[ap].hash = h;
                g_hash[ap].set = true;
            }
        }
        for (uint32_t p = 0; p < hx_ctrl_nparts(c); p++) {
            hx_ctrl_part st;
            (void)hx_ctrl_get(c, p, &st);
            if (st.isr_version >= HIST_MAX) {
                continue;
            }
            histent *h = &s->hist[p][st.isr_version];
            if (h->epoch == UNSET) {
                h->epoch = st.epoch;
                h->leader = st.leader;
                h->isr = st.isr;
            } else if (h->epoch != st.epoch || h->leader != st.leader || h->isr != st.isr) {
                fail(s,
                     "HISTORY FORK: partition %u version %u seen as (e%u l%u isr%x) and (e%u l%u "
                     "isr%x)",
                     p, st.isr_version, h->epoch, h->leader, h->isr, st.epoch, st.leader, st.isr);
                return;
            }
        }
    }
}

static int ready_node(const sim *s)
{
    for (uint32_t i = 0; i < NC; i++) {
        if (s->c[i].up && hx_chost_ready(s->c[i].h)) {
            return (int)i;
        }
    }
    return -1;
}

/* ----------------------------------------------------------------- brokers */

static void broker_hello(sim *s, uint32_t i)
{
    const bnode *b = &s->b[i];
    if (b->cut) {
        return;
    }
    for (uint32_t c = 0; c < NC; c++) {
        if (rnd(s) % 1000U >= s->bc_loss_pm) {
            const uint64_t v[6] = {b->nonce, 0, 0, 0, 0, 0};
            push(s, F_HELLO, i, c, NULL, v, delay(s));
        }
    }
}

static void broker_alter(sim *s, uint32_t i)
{
    bnode *b = &s->b[i];
    const int rn = ready_node(s);
    if (b->cut || b->session == 0 || rn < 0) {
        return;
    }
    const hx_ctrl *ctl = hx_chost_ctrl(s->c[rn].h);
    for (uint32_t p = 0; p < hx_ctrl_nparts(ctl); p++) {
        hx_ctrl_part st;
        (void)hx_ctrl_get(ctl, p, &st);
        if (st.leader != i || rnd(s) % 3U != 0) {
            if (st.leader == i) {
                b->cached_epoch[p] = st.epoch;
                b->cached_ver[p] = st.isr_version;
            }
            continue;
        }
        const bool stale = rnd(s) % 5U == 0;
        const uint32_t pick = (uint32_t)(rnd(s) % 3U);
        uint32_t isr = st.isr; /* unchanged: no-op */
        if (pick == 1) {
            isr = (1U << i) | (st.isr & (uint32_t)rnd(s));
        } else if (pick == 2) {
            isr = (st.isr | st.replicas) & ((uint32_t)rnd(s) | (1U << i));
        }
        const uint64_t v[6] = {p,
                               b->session,
                               stale ? b->cached_epoch[p] : st.epoch,
                               isr,
                               stale ? b->cached_ver[p] : st.isr_version,
                               0};
        for (uint32_t c = 0; c < NC; c++) {
            if (rnd(s) % 1000U >= s->bc_loss_pm) {
                push(s, F_ALTER, i, c, NULL, v, delay(s));
            }
        }
        b->cached_epoch[p] = st.epoch;
        b->cached_ver[p] = st.isr_version;
    }
}

static void deliver(sim *s, const flight *f)
{
    if (f->kind == F_RAFT) {
        cnode *n = &s->c[f->to];
        if (!n->up || s->group[f->from] != s->group[f->to]) {
            return;
        }
        hx_raft_entry scratch[256];
        hx_raft_msg m;
        if (hx_raft_msg_decode(f->bytes.data, f->bytes.len, scratch, 256, &m) != 0) {
            fail(s, "own raft message failed to decode");
            return;
        }
        (void)hx_chost_step(n->h, s->now, &m);
        return;
    }
    if (f->kind == F_REPLY) {
        bnode *b = &s->b[f->to];
        /* v[0] = nonce echoed by the controller, v[1] = session. */
        if (b->up && !b->cut && f->v[0] == b->nonce && f->v[1] > b->session) {
            b->session = f->v[1];
        }
        return;
    }
    cnode *n = &s->c[f->to];
    if (!n->up) {
        return;
    }
    if (f->kind == F_HELLO) {
        uint64_t sess = 0;
        if (hx_chost_hello(n->h, s->now, f->from, f->v[0], &sess) == HX_CHOST_OK && sess != 0) {
            if (hx_ctrl_nonce(hx_chost_ctrl(n->h), f->from) != f->v[0]) {
                fail(s, "controller returned a session for a different nonce");
                return;
            }
            const uint64_t v[6] = {f->v[0], sess, 0, 0, 0, 0};
            if (rnd(s) % 1000U >= s->bc_loss_pm) {
                push(s, F_REPLY, f->to, f->from, NULL, v, delay(s));
            }
        }
        return;
    }
    (void)hx_chost_alter_isr(n->h, s->now, (uint32_t)f->v[0], f->from, f->v[1], (uint32_t)f->v[2],
                             (uint32_t)f->v[3], (uint32_t)f->v[4]);
}

/* -------------------------------------------------------------------- loop */

static void sim_ms(sim *s, bool chaos)
{
    s->now++;
    for (uint32_t i = 0; i < NC; i++) {
        if (!s->c[i].up && s->now >= s->c[i].restart_at) {
            (void)cnode_start(s, i);
        }
    }
    for (uint32_t i = 0; i < NB; i++) {
        if (!s->b[i].up && s->now >= s->b[i].restart_at) {
            broker_start(s, i);
        }
    }
    const uint64_t roll = rnd(s);
    if (chaos) {
        if (roll % 700U == 0) {
            const bool any = s->group[0] != 0 || s->group[1] != 0 || s->group[2] != 0;
            for (uint32_t i = 0; i < NC; i++) {
                s->group[i] = any ? 0 : (int)(rnd(s) & 1U);
            }
            if (!any) {
                s->st_partitions++;
            }
        }
        if (roll % s->crash_every == 1U) {
            const uint32_t v = (uint32_t)(rnd(s) % NC);
            if (s->c[v].up) {
                cnode_stop(&s->c[v]);
                s->c[v].restart_at = s->now + 30 + (int64_t)(rnd(s) % 400U);
                s->st_crashes++;
            }
        }
        if (roll % 900U == 2U) {
            const uint32_t v = (uint32_t)(rnd(s) % NB);
            if (s->b[v].up) {
                s->b[v].up = false; /* process dies: volatile session and nonce are gone */
                s->b[v].restart_at = s->now + 20 + (int64_t)(rnd(s) % 1500U);
                s->st_kills++;
            }
        }
        if (roll % 1100U == 3U) {
            s->b[rnd(s) % NB].cut ^= true;
        }
    }
    for (uint32_t i = 0; i < NB; i++) {
        if (s->b[i].up && s->now % 40 == (int64_t)(i * 7U) % 40) {
            broker_hello(s, i);
        }
        if (s->b[i].up && s->now % 60 == (int64_t)(i * 11U) % 60) {
            broker_alter(s, i);
        }
    }
    /* Admin: keep trying to create the partitions until a ready leader has them all. */
    if (s->now % 50 == 0) {
        const int rn = ready_node(s);
        if (rn >= 0) {
            const uint32_t have = (uint32_t)hx_ctrl_nparts(hx_chost_ctrl(s->c[rn].h));
            if (have < NPARTS) {
                (void)hx_chost_create_partition(s->c[rn].h, s->now, have, s->replicas[have]);
            }
        }
    }
    for (uint32_t i = 0; i < NC; i++) {
        if (s->c[i].up) {
            (void)hx_chost_tick(s->c[i].h, s->now);
        }
    }
    size_t k = 0;
    while (k < s->nnet && !s->bad) {
        if (s->net[k].at <= s->now) {
            flight f = s->net[k];
            s->net[k] = s->net[--s->nnet];
            deliver(s, &f);
            hx_buf_free(&f.bytes);
        } else {
            k++;
        }
    }
    if (s->now % 5 == 0 && !s->bad) {
        check_nodes(s);
    }
}

static void sim_heal(sim *s)
{
    for (uint32_t i = 0; i < NC; i++) {
        s->group[i] = 0;
        if (!s->c[i].up) {
            s->c[i].restart_at = s->now;
        }
    }
    for (uint32_t i = 0; i < NB; i++) {
        s->b[i].cut = false;
    }
    s->loss_pm = 0;
    s->bc_loss_pm = 0;
    s->dup_pm = 0;
    /* One broker stays dead for good: the controller must notice and mark it down. */
    for (uint32_t k = 0, v = (uint32_t)(rnd(s) % NB); k < NB; k++, v = (v + 1U) % NB) {
        if (s->b[v].up) {
            s->b[v].up = false;
            s->b[v].restart_at = INT64_MAX / 2;
            break;
        }
    }
}

/* Returns an empty string when converged, else a reason. */
static const char *converged(const sim *s)
{
    const int rn = ready_node(s);
    if (rn < 0) {
        return "no ready controller leader";
    }
    const hx_ctrl *c = hx_chost_ctrl(s->c[rn].h);
    if (hx_ctrl_nparts(c) != NPARTS) {
        return "partitions not created";
    }
    for (uint32_t i = 0; i < NB; i++) {
        const bool alive = (hx_ctrl_alive(c) & (1U << i)) != 0;
        if (s->b[i].up && (!alive || hx_ctrl_session(c, i) != s->b[i].session)) {
            return "a running broker is not registered with its adopted session";
        }
        if (!s->b[i].up && alive) {
            return "a dead broker is still marked alive";
        }
    }
    for (uint32_t p = 0; p < NPARTS; p++) {
        hx_ctrl_part st;
        (void)hx_ctrl_get(c, p, &st);
        const bool any = (st.isr & hx_ctrl_alive(c)) != 0;
        if (any != (st.leader != HX_CTRL_NONE)) {
            return "leader presence does not match a live ISR member";
        }
    }
    return "";
}

typedef struct {
    uint64_t faults;
    uint64_t snapped;
    uint64_t kills;
    uint64_t applied;
    uint64_t epochs;
    uint64_t isr_versions;
} totals;

static bool run_scenario(uint64_t seed, totals *tot)
{
    sim s;
    if (!sim_init(&s, seed)) {
        (void)fprintf(stderr, "FAIL seed=%llu: init: %s\n", (unsigned long long)seed, s.why);
        sim_free(&s);
        return false;
    }
    for (int64_t t = 0; t < 25000 && !s.bad; t++) {
        sim_ms(&s, true);
    }
    sim_heal(&s);
    const char *why = "";
    for (int64_t t = 0; t < 20000 && !s.bad; t++) {
        sim_ms(&s, false);
        if (t % 50 == 0) {
            why = converged(&s);
            if (*why == '\0') {
                break;
            }
        }
    }
    if (getenv("HX_DEBUG")) {
        for (uint32_t i = 0; i < NC; i++) {
            (void)fprintf(
                stderr,
                "node %u up=%d role=%d term=%llu commit=%llu last=%llu applied=%llu failed=%d\n", i,
                s.c[i].up, s.c[i].up ? (int)hx_raft_get_role(hx_chost_raft(s.c[i].h)) : -1,
                s.c[i].up ? (unsigned long long)hx_raft_term(hx_chost_raft(s.c[i].h)) : 0ULL,
                s.c[i].up ? (unsigned long long)hx_raft_commit_index(hx_chost_raft(s.c[i].h))
                          : 0ULL,
                s.c[i].up ? (unsigned long long)hx_raft_last_index(hx_chost_raft(s.c[i].h)) : 0ULL,
                s.c[i].up ? (unsigned long long)hx_chost_applied(s.c[i].h) : 0ULL,
                s.c[i].up ? (int)hx_chost_failed(s.c[i].h) : -1);
        }
    }
    if (!s.bad && *why != '\0') {
        fail(&s, "LIVENESS: %s", why);
    }
    /* Stability across a controller failover: the new leader must give every broker a fresh grace
     * period instead of declaring healthy brokers down because it has not heard them yet. */
    if (!s.bad) {
        uint64_t before[NB];
        bool was_up[NB];
        const int rn0 = ready_node(&s);
        for (uint32_t i = 0; i < NB; i++) {
            before[i] = hx_ctrl_session(hx_chost_ctrl(s.c[rn0].h), i);
            was_up[i] = s.b[i].up;
        }
        cnode_stop(&s.c[rn0]);
        s.c[rn0].restart_at = s.now + 100;
        for (int64_t t = 0; t < 4000 && !s.bad; t++) {
            sim_ms(&s, false);
        }
        why = converged(&s);
        if (*why != '\0') {
            fail(&s, "LIVENESS after controller failover: %s", why);
        }
        const int rn1 = ready_node(&s);
        for (uint32_t i = 0; rn1 >= 0 && i < NB && !s.bad; i++) {
            if (was_up[i] && s.b[i].up &&
                hx_ctrl_session(hx_chost_ctrl(s.c[rn1].h), i) != before[i]) {
                fail(&s, "broker %u was re-registered by a controller failover", i);
            }
        }
    }
    for (uint32_t i = 0; i < NC; i++) {
        if (s.c[i].up) {
            tot->snapped += hx_raft_snapshot_index(hx_chost_raft(s.c[i].h)) != 0;
        }
    }
    const int rn = ready_node(&s);
    if (rn >= 0) {
        const hx_ctrl *c = hx_chost_ctrl(s.c[rn].h);
        tot->applied += hx_chost_applied(s.c[rn].h);
        for (uint32_t p = 0; p < hx_ctrl_nparts(c); p++) {
            hx_ctrl_part st;
            (void)hx_ctrl_get(c, p, &st);
            tot->epochs += st.epoch;
            tot->isr_versions += st.isr_version;
        }
    }
    tot->faults += s.st_partitions + s.st_crashes;
    tot->kills += s.st_kills;
    const bool ok = !s.bad;
    if (!ok) {
        (void)fprintf(stderr, "FAIL seed=%llu t=%lld: %s\n", (unsigned long long)seed,
                      (long long)s.now, s.why);
    }
    sim_free(&s);
    return ok;
}

int main(void)
{
    const char *e = getenv("HX_CHOST_SEEDS");
    const uint64_t seeds = e != NULL ? strtoull(e, NULL, 10) : 40U;
    g_hash_cap = 1U << 16;
    g_hash = calloc(g_hash_cap, sizeof *g_hash);
    if (g_hash == NULL) {
        return 2;
    }
    totals tot;
    memset(&tot, 0, sizeof tot);
    bool ok = true;
    for (uint64_t seed = 1; seed <= seeds && ok; seed++) {
        memset(g_hash, 0, g_hash_cap * sizeof *g_hash); /* indexes mean different things per run */
        ok = run_scenario(seed, &tot);
    }
    free(g_hash);
    if (!ok) {
        return 1;
    }
    (void)printf(
        "controller-on-raft simulation: %llu scenarios, %llu controller crashes/partitions, "
        "%llu broker kills, %llu commands applied, %llu leader changes, %llu ISR changes, "
        "%llu nodes ended with a snapshot, no invariant violated\n",
        (unsigned long long)seeds, (unsigned long long)tot.faults, (unsigned long long)tot.kills,
        (unsigned long long)tot.applied, (unsigned long long)tot.epochs,
        (unsigned long long)tot.isr_versions, (unsigned long long)tot.snapped);
    return 0;
}
