/*
 * Partition replication under fault injection.
 *
 * A deterministic cluster of 3-5 replicas of ONE partition, a controller oracle that elects leaders
 * from the ISR and approves ISR changes, and random faults: message loss, duplication and
 * reordering; data-plane partitions; control-plane partitions (a leader cut off from the
 * controller keeps running as a zombie); crashes that drop every unsynced write. After every event
 * the checker asserts
 *   - committed data is identical on every replica and never lost (state-machine safety),
 *   - every leader of a later epoch holds all data committed before it (leader completeness),
 *   - replicas never disagree below a common (offset, epoch) (log matching),
 *   - a record acknowledged to a client is the committed record at that offset,
 *   - after healing, the partition commits again (liveness).
 * The checker itself is validated by mutation testing (see docs/SOLUTION.md ADR-020).
 *
 * HX_REPL_SEEDS=n sets the number of random scenarios (default 120).
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

#include "ctrl/ctrl.h"
#include "repl/repl.h"
#include "repl/repl_log.h"
#include "util/buf.h"

static int failures;

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

#define REQ(cond)                                                                                  \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define PAY         8U
#define MAX_RECORDS 60000U

/* ----------------------------------------------------------- simulated log */

typedef struct {
    uint32_t epoch;
    uint32_t len;
    uint8_t d[PAY];
} srec;

typedef struct {
    srec *w; /* written (may include unsynced data) */
    srec *d; /* what survived the last sync: all a crash leaves */
    size_t nw;
    size_t nd;
    size_t cap;
    size_t dirty; /* lowest index touched since the last sync, SIZE_MAX if none */
    bool fail_sync;
    bool fail_append;
    /* Differential mode: every operation is mirrored on a real hx_log behind hx_plog and the two
     * must agree. Truncation is durable in the real log, so the model makes it durable as well. */
    hx_plog *pl;
    hx_repl_log rv;
    unsigned nreads; /* reads seen, to compare only some of them */
    char dir[300];
} slog;

static char g_shadow_fail[200];

__attribute__((format(printf, 1, 2))) static void shadow_fail(const char *fmt, ...)
{
    if (g_shadow_fail[0] != '\0') {
        return;
    }
    va_list ap;
    va_start(ap, fmt);
    // NOLINTNEXTLINE(clang-analyzer-valist.Uninitialized): analyzer false positive, va_start above
    (void)vsnprintf(g_shadow_fail, sizeof g_shadow_fail, fmt, ap);
    va_end(ap);
}

static void slog_init(slog *l)
{
    memset(l, 0, sizeof *l);
    l->dirty = SIZE_MAX;
}

static void slog_free(slog *l)
{
    hx_plog_close(l->pl);
    free(l->w);
    free(l->d);
}

static int slog_grow(slog *l, size_t need)
{
    if (need <= l->cap) {
        return 0;
    }
    size_t nc = l->cap == 0 ? 512U : l->cap;
    while (nc < need) {
        nc *= 2U;
    }
    srec *w = realloc(l->w, nc * sizeof *w);
    if (w == NULL) {
        return -1;
    }
    l->w = w;
    srec *d = realloc(l->d, nc * sizeof *d);
    if (d == NULL) {
        return -1;
    }
    l->d = d;
    l->cap = nc;
    return 0;
}

static uint64_t sl_leo(void *c)
{
    const slog *l = c;
    if (l->pl != NULL && hx_plog_leo(l->pl) != l->nw) {
        shadow_fail("SHADOW: leo %llu vs real %llu", (unsigned long long)l->nw,
                    (unsigned long long)hx_plog_leo(l->pl));
    }
    return l->nw;
}

static int sl_epoch_at(void *c, uint64_t off, uint32_t *e)
{
    const slog *l = c;
    if (off >= l->nw) {
        return -1;
    }
    *e = l->w[off].epoch;
    if (l->pl != NULL) {
        uint32_t re = 0;
        if (l->rv.epoch_at(l->rv.ctx, off, &re) != 0 || re != *e) {
            shadow_fail("SHADOW: epoch_at(%llu) %u vs real %u", (unsigned long long)off, *e, re);
        }
    }
    return 0;
}

static uint64_t sl_end_for_epoch(void *c, uint32_t e)
{
    const slog *l = c;
    size_t i = l->nw;
    while (i > 0 && l->w[i - 1U].epoch > e) {
        i--;
    }
    if (l->pl != NULL && l->rv.end_offset_for_epoch(l->rv.ctx, e) != i) {
        shadow_fail("SHADOW: end_offset_for_epoch(%u) %zu vs real %llu", e, i,
                    (unsigned long long)l->rv.end_offset_for_epoch(l->rv.ctx, e));
    }
    return i;
}

static int sl_append(void *c, const hx_repl_rec *r, size_t n)
{
    slog *l = c;
    if (l->fail_append || slog_grow(l, l->nw + n) != 0) {
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        if (r[i].len > PAY) {
            return -1;
        }
        srec *s = &l->w[l->nw];
        memset(s, 0, sizeof *s); /* bytes past len must compare equal across replicas */
        s->epoch = r[i].epoch;
        s->len = r[i].len;
        if (r[i].len != 0) {
            memcpy(s->d, r[i].data, r[i].len);
        }
        l->nw++;
    }
    if (l->nw - n < l->dirty) {
        l->dirty = l->nw - n;
    }
    if (l->pl != NULL && l->rv.append(l->rv.ctx, r, n) != 0) {
        shadow_fail("SHADOW: real append failed");
    }
    return 0;
}

static int sl_truncate(void *c, uint64_t to)
{
    slog *l = c;
    if (to > l->nw) {
        return -1;
    }
    l->nw = (size_t)to;
    if (l->nw < l->dirty) {
        l->dirty = l->nw;
    }
    if (l->pl != NULL) {
        if (l->rv.truncate(l->rv.ctx, to) != 0) {
            shadow_fail("SHADOW: real truncate failed");
        }
        if (l->nd > l->nw) {
            l->nd = l->nw; /* the real cut is durable */
        }
    }
    return 0;
}

static int sl_read(void *c, uint64_t off, size_t max_n, size_t max_bytes, hx_repl_rec *out,
                   size_t *n)
{
    slog *l = c;
    size_t k = 0;
    size_t bytes = 0;
    while (off + k < l->nw && k < max_n) {
        const srec *s = &l->w[off + k];
        if (k > 0 && bytes + s->len > max_bytes) {
            break;
        }
        out[k].epoch = s->epoch;
        out[k].len = s->len;
        out[k].data = s->d;
        bytes += s->len;
        k++;
    }
    *n = k;
    if (l->pl != NULL && (++l->nreads % 4U) == 0) {
        hx_repl_rec rr[256];
        size_t rn = 0;
        const size_t want = max_n < 256U ? max_n : 256U;
        if (l->rv.read(l->rv.ctx, off, want, max_bytes, rr, &rn) != 0) {
            shadow_fail("SHADOW: real read failed");
        } else {
            const size_t cmp = k < rn ? k : rn;
            for (size_t i = 0; i < cmp; i++) {
                if (rr[i].epoch != out[i].epoch || rr[i].len != out[i].len ||
                    (out[i].len != 0 && memcmp(rr[i].data, out[i].data, out[i].len) != 0)) {
                    shadow_fail("SHADOW: record %zu (from %llu) differs on read", i,
                                (unsigned long long)off);
                }
            }
            if (rn != k && !(k > 256U)) {
                shadow_fail("SHADOW: read returned %zu records, real %zu", k, rn);
            }
        }
    }
    return 0;
}

static int sl_sync(void *c)
{
    slog *l = c;
    if (l->fail_sync) {
        return -1;
    }
    if (l->dirty != SIZE_MAX || l->nd != l->nw) {
        const size_t from = l->dirty < l->nd ? l->dirty : l->nd;
        const size_t start = from < l->nw ? from : l->nw;
        for (size_t i = start; i < l->nw; i++) {
            l->d[i] = l->w[i];
        }
        l->nd = l->nw;
        l->dirty = SIZE_MAX;
    }
    if (l->pl != NULL && l->rv.sync(l->rv.ctx) != 0) {
        shadow_fail("SHADOW: real sync failed");
    }
    return 0;
}

static void slog_crash(slog *l)
{
    for (size_t i = 0; i < l->nd; i++) {
        l->w[i] = l->d[i];
    }
    l->nw = l->nd;
    l->dirty = SIZE_MAX;
    if (l->pl != NULL) {
        /* Process death keeps what reached the page cache, so lose the unsynced suffix by hand,
         * then reopen: recovery must reproduce exactly the durable prefix. */
        hx_plog_close(l->pl);
        l->pl = NULL;
        hx_log_opts lo;
        hx_log_opts_default(&lo);
        lo.index_interval_bytes = 64;
        if (hx_plog_open(l->dir, &lo, &l->pl) != HX_OK) {
            shadow_fail("SHADOW: real log failed to reopen");
            return;
        }
        hx_plog_set_nosync_for_tests(l->pl, true);
        l->rv = hx_plog_vtable(l->pl);
        if (hx_plog_leo(l->pl) < l->nd) {
            shadow_fail("SHADOW: real log shorter (%llu) than the synced prefix (%zu)",
                        (unsigned long long)hx_plog_leo(l->pl), l->nd);
            return;
        }
        if (l->rv.truncate(l->rv.ctx, l->nd) != 0) {
            shadow_fail("SHADOW: truncate to the synced prefix failed");
            return;
        }
        for (size_t i = l->nd > 64U ? l->nd - 64U : 0; i < l->nd; i++) {
            hx_repl_rec rr[1];
            size_t rn = 0;
            if (l->rv.read(l->rv.ctx, i, 1, 1U << 20, rr, &rn) != 0 || rn != 1 ||
                rr[0].epoch != l->d[i].epoch || rr[0].len != l->d[i].len ||
                (rr[0].len != 0 && memcmp(rr[0].data, l->d[i].d, rr[0].len) != 0)) {
                shadow_fail("SHADOW: record %zu wrong after recovery", i);
                return;
            }
        }
    }
}

static void slog_cfg(hx_repl_cfg *cfg, slog *l)
{
    cfg->log.ctx = l;
    cfg->log.leo = sl_leo;
    cfg->log.epoch_at = sl_epoch_at;
    cfg->log.end_offset_for_epoch = sl_end_for_epoch;
    cfg->log.append = sl_append;
    cfg->log.truncate = sl_truncate;
    cfg->log.read = sl_read;
    cfg->log.sync = sl_sync;
}

/* -------------------------------------------------------------- simulator */

#define MAXN 5U

typedef struct {
    uint64_t off;
    uint32_t epoch;
    srec rec;
} pend;

typedef struct {
    hx_repl *r;
    slog lg;
    bool up;
    int64_t restart_at;
    uint32_t inc;     /* incarnation: bumped on every restart (models the TCP connection) */
    uint64_t session; /* controller session this process life registered, 0 if none */
    uint64_t seen_hw;
    uint32_t seen_epoch;
    uint32_t leader_checked_epoch;
    pend *pending;
    size_t npending;
    size_t cappending;
} node;

typedef enum { K_DATA, K_STATE, K_ALTER, K_HELLO, K_SESSION } fkind;

typedef struct {
    int64_t at;
    fkind kind;
    uint32_t from;
    uint32_t to;
    hx_buf bytes;
    uint32_t st[4]; /* K_STATE: epoch, leader, isr, version */
    uint32_t inc;   /* incarnation of the replica end of the session when the message was sent */
    uint64_t sess;  /* controller session the message is addressed to / sent under */
} flight;

typedef struct {
    hx_ctrl *c;
    hx_ctrl *twin; /* applies the same command log; must end byte-identical */
    uint64_t index;
    int64_t last_hb[16];
} ctl;

typedef struct {
    uint32_t n;
    uint32_t all;
    node nodes[MAXN];
    hx_repl_cfg cfg[MAXN];
    ctl c;
    int64_t now;
    uint64_t rng;
    uint64_t seed;
    char rdir[200]; /* root of the real logs in differential mode, empty otherwise */

    flight *net;
    size_t nnet;
    size_t capnet;
    int group[MAXN];
    bool cut[MAXN]; /* isolated from the controller */
    uint32_t loss_pm;
    uint32_t ctl_loss_pm;
    uint32_t dup_pm;
    uint32_t crash_every;
    uint32_t cut_every;
    uint32_t detect_ms;
    uint32_t min_delay;
    uint32_t max_delay;

    srec *committed;
    uint32_t *commit_epoch;
    uint64_t c_max;
    uint64_t payload_seq;

    uint64_t stat_commits;
    uint64_t stat_crashes;
    uint64_t stat_partitions;
    uint64_t stat_cuts;
    uint64_t stat_elections;
    uint64_t stat_isr_changes;
    uint64_t stat_acks;
    uint64_t stat_truncations;

    bool bad;
    char why[220];
} sim;

static hx_ctrl_part ctl_part(const sim *s);
static hx_ctrl_result ctl_apply(sim *s, const hx_ctrl_cmd *m);

static uint64_t srand_next(sim *s)
{
    uint64_t x = s->rng;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    s->rng = x;
    return x * 0x2545F4914F6CDD1DULL;
}

__attribute__((format(printf, 2, 3))) static void sim_fail(sim *s, const char *fmt, ...)
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

static uint32_t bitn(uint32_t i)
{
    return 1U << i;
}

static void node_start(sim *s, uint32_t i)
{
    node *nd = &s->nodes[i];
    nd->r = hx_repl_create(&s->cfg[i], s->now);
    nd->inc++;
    nd->session = 0;
    nd->up = nd->r != NULL;
    nd->seen_hw = 0;
    nd->seen_epoch = 0;
    nd->leader_checked_epoch = 0;
    nd->npending = 0;
    if (nd->r == NULL) {
        sim_fail(s, "replica %u could not start", i);
    }
}

static bool sim_init(sim *s, uint32_t n, uint64_t seed, unsigned flavor)
{
    memset(s, 0, sizeof *s);
    s->n = n;
    s->all = (1U << n) - 1U;
    s->seed = seed;
    s->rng = seed * 0x9E3779B97F4A7C15ULL + 4242U;
    s->min_delay = 1;
    s->max_delay = 15;
    s->crash_every = (flavor & 1U) != 0 ? 150U : 1200U;
    s->cut_every = (flavor & 2U) != 0 ? 400U : 2500U;
    s->detect_ms = 120 + (uint32_t)(seed % 4U) * 60U;
    g_shadow_fail[0] = '\0';
    if (seed % 6U == 1U) {
        const char *t = getenv("TMPDIR");
        (void)snprintf(s->rdir, sizeof s->rdir, "%s/helix-repl-XXXXXX",
                       (t != NULL && *t != '\0') ? t : "/tmp");
        if (mkdtemp(s->rdir) == NULL) {
            s->rdir[0] = '\0';
        }
    }
    s->committed = malloc(MAX_RECORDS * sizeof *s->committed);
    s->commit_epoch = malloc(MAX_RECORDS * sizeof *s->commit_epoch);
    if (s->committed == NULL || s->commit_epoch == NULL) {
        return false;
    }
    s->c.c = hx_ctrl_create();
    s->c.twin = hx_ctrl_create();
    if (s->c.c == NULL || s->c.twin == NULL) {
        return false;
    }
    for (uint32_t i = 0; i < n; i++) {
        hx_repl_cfg_default(&s->cfg[i], i, s->all);
        s->cfg[i].fetch_ms = 10;
        s->cfg[i].max_fetch_records = 16;
        s->cfg[i].max_fetch_bytes = 64;
        s->cfg[i].lag_ms = (flavor & 2U) != 0 ? 150U : 400U;
        s->cfg[i].min_isr = (flavor == 3U && n >= 3U) ? 2U : 1U;
        slog_init(&s->nodes[i].lg);
        if (s->rdir[0] != '\0') {
            slog *lg = &s->nodes[i].lg;
            (void)snprintf(lg->dir, sizeof lg->dir, "%s/n%u", s->rdir, i);
            hx_log_opts lo;
            hx_log_opts_default(&lo);
            lo.index_interval_bytes = 64; /* small records: keep reads from rescanning */
            if (hx_plog_open(lg->dir, &lo, &lg->pl) != HX_OK) {
                sim_fail(s, "real log failed to open");
                return false;
            }
            hx_plog_set_nosync_for_tests(lg->pl, true);
            lg->rv = hx_plog_vtable(lg->pl);
        }
        slog_cfg(&s->cfg[i], &s->nodes[i].lg);
        node_start(s, i);
    }
    hx_ctrl_cmd mk;
    memset(&mk, 0, sizeof mk);
    mk.type = HX_CTRL_CREATE;
    mk.replicas = s->all;
    (void)ctl_apply(s, &mk);
    return !s->bad;
}

static int rm_cb(const char *p, const struct stat *st, int t, struct FTW *f)
{
    (void)st;
    (void)t;
    (void)f;
    return remove(p);
}

static void sim_free(sim *s)
{
    hx_ctrl_destroy(s->c.c);
    hx_ctrl_destroy(s->c.twin);
    for (uint32_t i = 0; i < s->n; i++) {
        hx_repl_destroy(s->nodes[i].r);
        slog_free(&s->nodes[i].lg);
        free(s->nodes[i].pending);
    }
    for (size_t i = 0; i < s->nnet; i++) {
        hx_buf_free(&s->net[i].bytes);
    }
    free(s->net);
    free(s->committed);
    free(s->commit_epoch);
    if (s->rdir[0] != '\0') {
        (void)nftw(s->rdir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
    }
}

static bool same_rec(const srec *a, const srec *b)
{
    return a->epoch == b->epoch && a->len == b->len && memcmp(a->d, b->d, PAY) == 0;
}

/* ---------------------------------------------------------------- checker */

static void check_node(sim *s, uint32_t i)
{
    node *nd = &s->nodes[i];
    hx_repl *r = nd->r;
    if (!nd->up) {
        return;
    }
    if (hx_repl_failed(r)) {
        sim_fail(s, "replica %u entered the failed state", i);
        return;
    }
    const uint64_t hw = hx_repl_hw(r);
    const uint64_t leo = hx_repl_leo(r);
    const uint32_t ep = hx_repl_epoch(r);
    if (hw > leo) {
        sim_fail(s, "replica %u: hw %llu beyond leo %llu", i, (unsigned long long)hw,
                 (unsigned long long)leo);
        return;
    }
    if (ep < nd->seen_epoch || hw < nd->seen_hw) {
        sim_fail(s, "replica %u: epoch or hw went backwards (epoch %u->%u hw %llu->%llu)", i,
                 nd->seen_epoch, ep, (unsigned long long)nd->seen_hw, (unsigned long long)hw);
        return;
    }
    nd->seen_epoch = ep;
    if (hw >= MAX_RECORDS) {
        sim_fail(s, "simulator range exceeded");
        return;
    }
    /* Everything below hw is committed: it must be identical everywhere, forever. */
    const slog *lg = &nd->lg;
    for (uint64_t o = nd->seen_hw; o < hw; o++) {
        if (o < s->c_max) {
            if (!same_rec(&lg->w[o], &s->committed[o])) {
                sim_fail(s,
                         "STATE MACHINE SAFETY: offset %llu committed with two different values "
                         "(replica %u)",
                         (unsigned long long)o, i);
                return;
            }
        } else {
            if (o != s->c_max) {
                sim_fail(s, "commit gap at %llu", (unsigned long long)o);
                return;
            }
            s->committed[o] = lg->w[o];
            s->commit_epoch[o] = ep;
            s->c_max = o + 1U;
            s->stat_commits++;
        }
    }
    nd->seen_hw = hw;

    if (hx_repl_get_role(r) == HX_REPL_LEADER) {
        /* Leader completeness: a leader of epoch >= the epoch a record was committed in holds it.
         */
        if (nd->leader_checked_epoch != ep) {
            nd->leader_checked_epoch = ep;
            for (uint64_t o = 0; o < s->c_max; o++) {
                if (s->commit_epoch[o] <= ep &&
                    (o >= leo || !same_rec(&lg->w[o], &s->committed[o]))) {
                    sim_fail(s,
                             "LEADER COMPLETENESS: new leader %u (epoch %u) lacks committed offset "
                             "%llu (committed in epoch %u)",
                             i, ep, (unsigned long long)o, s->commit_epoch[o]);
                    return;
                }
            }
        }
        /* Acknowledgements: a record the client was told is committed must be THE committed one. */
        size_t keep = 0;
        for (size_t k = 0; k < nd->npending; k++) {
            const pend *p = &nd->pending[k];
            if (p->epoch != ep) {
                continue; /* leadership changed: the client never got an ack, nothing to check */
            }
            if (p->off < hw) {
                if (p->off >= s->c_max || !same_rec(&p->rec, &s->committed[p->off])) {
                    sim_fail(
                        s,
                        "ACKED DATA LOST: replica %u acknowledged offset %llu but the committed "
                        "record there is different",
                        i, (unsigned long long)p->off);
                    return;
                }
                s->stat_acks++;
            } else {
                nd->pending[keep++] = *p;
            }
        }
        nd->npending = keep;
    } else {
        nd->npending = 0;
    }
}

/* Log matching between every pair of live replicas. */
static void check_log_matching(sim *s)
{
    for (uint32_t a = 0; a < s->n; a++) {
        for (uint32_t b = a + 1U; b < s->n; b++) {
            if (!s->nodes[a].up || !s->nodes[b].up) {
                continue;
            }
            const slog *la = &s->nodes[a].lg;
            const slog *lb = &s->nodes[b].lg;
            size_t top = la->nw < lb->nw ? la->nw : lb->nw;
            while (top > 0 && la->w[top - 1U].epoch != lb->w[top - 1U].epoch) {
                top--;
            }
            for (size_t o = top; o > 0; o--) {
                if (!same_rec(&la->w[o - 1U], &lb->w[o - 1U])) {
                    sim_fail(s,
                             "LOG MATCHING: replicas %u and %u diverge at %zu below a common "
                             "(offset, epoch %u) [epochs there: %u vs %u; ctl epoch %u leader %u]",
                             a, b, o - 1U, la->w[top - 1U].epoch, la->w[o - 1U].epoch,
                             lb->w[o - 1U].epoch, ctl_part(s).epoch, ctl_part(s).leader);
                    return;
                }
            }
        }
    }
}

/* ---------------------------------------------------------------- network */

static void net_push(sim *s, fkind kind, uint32_t from, uint32_t to, const hx_buf *bytes,
                     const uint32_t *st, uint64_t sess, int64_t at)
{
    const uint32_t replica_end = (kind == K_STATE || kind == K_SESSION) ? to : from;
    if (s->nnet == s->capnet) {
        const size_t ncap = s->capnet == 0 ? 256U : s->capnet * 2U;
        flight *f = realloc(s->net, ncap * sizeof *f);
        if (f == NULL) {
            sim_fail(s, "out of memory");
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
    f->sess = sess;
    f->inc = kind == K_DATA ? 0U : s->nodes[replica_end].inc;
    if (st != NULL) {
        memcpy(f->st, st, sizeof f->st);
    }
}

static int64_t delay(sim *s)
{
    return s->now + s->min_delay + (int64_t)(srand_next(s) % (s->max_delay - s->min_delay + 1U));
}

static void send_data(sim *s, const hx_repl_msg *m)
{
    if (m->to >= s->n || s->group[m->from] != s->group[m->to] ||
        srand_next(s) % 1000U < s->loss_pm) {
        return;
    }
    hx_buf b;
    hx_buf_init(&b);
    if (hx_repl_msg_encode(m, &b) != 0) {
        sim_fail(s, "encode failed");
        hx_buf_free(&b);
        return;
    }
    net_push(s, K_DATA, m->from, m->to, &b, NULL, 0, delay(s));
    if (srand_next(s) % 1000U < s->dup_pm) {
        net_push(s, K_DATA, m->from, m->to, &b, NULL, 0, delay(s));
    }
    hx_buf_free(&b);
}

static hx_ctrl_part ctl_part(const sim *s)
{
    hx_ctrl_part p;
    memset(&p, 0, sizeof p);
    (void)hx_ctrl_get(s->c.c, 0, &p);
    return p;
}

static void send_state(sim *s, uint32_t to)
{
    if ((hx_ctrl_alive(s->c.c) & bitn(to)) == 0 || s->cut[to] ||
        srand_next(s) % 1000U < s->ctl_loss_pm) {
        return;
    }
    const hx_ctrl_part p = ctl_part(s);
    const uint32_t st[4] = {p.epoch, p.leader, p.isr, p.isr_version};
    const uint64_t sess = hx_ctrl_session(s->c.c, to);
    net_push(s, K_STATE, HX_REPL_CONTROLLER, to, NULL, st, sess, delay(s));
    if (srand_next(s) % 1000U < s->dup_pm) { /* a copy that wanders for up to 3 s */
        net_push(s, K_STATE, HX_REPL_CONTROLLER, to, NULL, st, sess,
                 s->now + 50 + (int64_t)(srand_next(s) % 3000U));
    }
}

static void send_alter(sim *s, const hx_repl_msg *m)
{
    const uint64_t sess = s->nodes[m->from].session;
    if (sess == 0 || s->cut[m->from] || srand_next(s) % 1000U < s->ctl_loss_pm) {
        return;
    }
    hx_buf b;
    hx_buf_init(&b);
    if (hx_repl_msg_encode(m, &b) != 0) {
        sim_fail(s, "encode failed");
    } else {
        net_push(s, K_ALTER, m->from, HX_REPL_CONTROLLER, &b, NULL, sess, delay(s));
        if (srand_next(s) % 1000U < s->dup_pm) {
            net_push(s, K_ALTER, m->from, HX_REPL_CONTROLLER, &b, NULL, sess,
                     s->now + 50 + (int64_t)(srand_next(s) % 3000U));
        }
    }
    hx_buf_free(&b);
}

/* Each live replica says hello to the controller now and then, carrying the session it holds. */
static void send_hello(sim *s, uint32_t i)
{
    if (s->cut[i] || srand_next(s) % 1000U < s->ctl_loss_pm) {
        return;
    }
    net_push(s, K_HELLO, i, HX_REPL_CONTROLLER, NULL, NULL, s->nodes[i].session, delay(s));
}

static void collect(sim *s, uint32_t i)
{
    const hx_repl_msg *msgs = NULL;
    size_t n = 0;
    hx_repl_take_msgs(s->nodes[i].r, &msgs, &n);
    for (size_t k = 0; k < n; k++) {
        if (msgs[k].to == HX_REPL_CONTROLLER) {
            send_alter(s, &msgs[k]);
        } else {
            send_data(s, &msgs[k]);
        }
    }
}

/* ------------------------------------------------------------- controller */

static void broadcast_state(sim *s)
{
    for (uint32_t i = 0; i < s->n; i++) {
        send_state(s, i);
    }
}

/* Every controller command goes through here: applied to the live instance and to a twin that
 * must stay identical, with the controller's own invariants checked after each one. Now and then
 * the live instance is rebuilt from a snapshot, as a restarted controller node would be. */
static hx_ctrl_result ctl_apply(sim *s, const hx_ctrl_cmd *m)
{
    const hx_ctrl_part before = ctl_part(s);
    hx_buf b;
    hx_buf_init(&b);
    if (hx_ctrl_cmd_encode(m, &b) != 0) {
        sim_fail(s, "controller command encode failed");
        hx_buf_free(&b);
        return HX_CTRL_BAD;
    }
    const uint64_t idx = ++s->c.index;
    const hx_ctrl_result r = hx_ctrl_apply_bytes(s->c.c, idx, b.data, b.len);
    const hx_ctrl_result r2 = hx_ctrl_apply_bytes(s->c.twin, idx, b.data, b.len);
    hx_buf_free(&b);
    if (r != r2 || hx_ctrl_check(s->c.c) != 0) {
        sim_fail(s, "controller diverged from its twin or broke an invariant");
        return r;
    }
    const hx_ctrl_part after = ctl_part(s);
    if (after.epoch < before.epoch || after.isr_version < before.isr_version) {
        sim_fail(s, "controller epoch or ISR version went backwards");
    }
    if (after.epoch != before.epoch && after.leader != HX_REPL_NONE) {
        s->stat_elections++;
    }
    if (m->type == HX_CTRL_ALTER_ISR && r == HX_CTRL_OK) {
        s->stat_isr_changes++;
    }
    if (idx % 61U == 0) {
        hx_buf snap;
        hx_buf_init(&snap);
        if (hx_ctrl_snapshot(s->c.c, &snap) != 0 ||
            hx_ctrl_restore(s->c.c, snap.data, snap.len) != 0) {
            sim_fail(s, "controller snapshot round trip failed");
        }
        hx_buf_free(&snap);
    }
    if (r == HX_CTRL_OK) {
        broadcast_state(s);
    }
    return r;
}

static void ctl_register(sim *s, uint32_t i, uint32_t node_inc)
{
    hx_ctrl_cmd m;
    memset(&m, 0, sizeof m);
    m.type = HX_CTRL_BROKER_UP;
    m.broker = i;
    m.nonce = node_inc;
    (void)ctl_apply(s, &m);
    s->c.last_hb[i] = s->now;
    net_push(s, K_SESSION, HX_REPL_CONTROLLER, i, NULL, NULL, hx_ctrl_session(s->c.c, i), delay(s));
    s->net[s->nnet - 1U].inc = node_inc; /* the reply travels back on the hello's connection */
}

static void ctl_on_hello(sim *s, const flight *f)
{
    const uint32_t i = f->from;
    const bool alive = (hx_ctrl_alive(s->c.c) & bitn(i)) != 0;
    if (alive && f->sess == hx_ctrl_session(s->c.c, i)) {
        s->c.last_hb[i] = s->now; /* heartbeat */
    } else {
        ctl_register(s, i, f->inc);
    }
}

static void ctl_tick(sim *s)
{
    for (uint32_t i = 0; i < s->n; i++) {
        if ((hx_ctrl_alive(s->c.c) & bitn(i)) != 0 &&
            s->now - s->c.last_hb[i] >= (int64_t)s->detect_ms) {
            hx_ctrl_cmd m;
            memset(&m, 0, sizeof m);
            m.type = HX_CTRL_BROKER_DOWN;
            m.broker = i;
            m.session = hx_ctrl_session(s->c.c, i);
            (void)ctl_apply(s, &m);
        }
    }
    if (s->now % 25 == 0) {
        broadcast_state(s);
    }
}

static void ctl_on_alter(sim *s, const flight *f, const hx_repl_msg *m)
{
    hx_ctrl_cmd c;
    memset(&c, 0, sizeof c);
    c.type = HX_CTRL_ALTER_ISR;
    c.part = 0;
    c.broker = f->from;
    c.session = f->sess;
    c.epoch = m->epoch;
    c.isr = m->isr;
    c.isr_version = m->isr_version;
    (void)ctl_apply(s, &c);
    send_state(s, f->from); /* always answer with the truth */
}

static void deliver(sim *s, const flight *f)
{
    if (f->kind == K_STATE) {
        node *nd = &s->nodes[f->to];
        /* The contract of hx_repl_set_state: only state sent under the session this process life
         * registered. Copies addressed to a previous life carry an older session and are dropped.
         */
        if (nd->up && !s->cut[f->to] && nd->session != 0 && f->sess == nd->session) {
            (void)hx_repl_set_state(nd->r, s->now, f->st[0], f->st[1], f->st[2], f->st[3]);
            collect(s, f->to);
            check_node(s, f->to);
        }
        return;
    }
    if (f->kind == K_SESSION) {
        node *nd = &s->nodes[f->to];
        if (nd->up && !s->cut[f->to] && f->inc == nd->inc && f->sess > nd->session) {
            nd->session = f->sess;
        }
        return;
    }
    if (f->kind == K_HELLO) {
        if (!s->cut[f->from]) {
            ctl_on_hello(s, f);
        }
        return;
    }
    hx_repl_rec scratch[128];
    hx_repl_msg m;
    if (hx_repl_msg_decode(f->bytes.data, f->bytes.len, scratch, 128, &m) != 0) {
        sim_fail(s, "own message failed to decode");
        return;
    }
    if (f->kind == K_ALTER) {
        if (!s->cut[f->from]) {
            ctl_on_alter(s, f, &m);
        }
        return;
    }
    node *nd = &s->nodes[f->to];
    if (!nd->up || s->group[f->from] != s->group[f->to]) {
        return;
    }
    const uint64_t before = hx_repl_leo(nd->r);
    (void)hx_repl_step(nd->r, s->now, &m);
    if (hx_repl_leo(nd->r) < before) {
        s->stat_truncations++;
    }
    collect(s, f->to);
    check_node(s, f->to);
}

/* ------------------------------------------------------------ fault model */

static void sim_crash(sim *s, uint32_t i, int64_t down_ms)
{
    node *nd = &s->nodes[i];
    if (!nd->up) {
        return;
    }
    hx_repl_destroy(nd->r);
    nd->r = NULL;
    nd->up = false;
    nd->restart_at = s->now + down_ms;
    nd->npending = 0;
    slog_crash(&nd->lg);
    s->stat_crashes++;
}

static void client_append(sim *s, uint32_t i, uint32_t count)
{
    node *nd = &s->nodes[i];
    hx_repl_rec recs[3];
    srec keep[3];
    for (uint32_t k = 0; k < count; k++) {
        const uint64_t v = ++s->payload_seq;
        keep[k].len = PAY;
        keep[k].epoch = 0;
        for (unsigned b = 0; b < PAY; b++) {
            keep[k].d[b] = (uint8_t)(v >> (8U * (PAY - 1U - b)));
        }
        recs[k].epoch = 0;
        recs[k].len = PAY;
        recs[k].data = keep[k].d;
    }
    uint64_t first = 0;
    const uint32_t ep = hx_repl_epoch(nd->r);
    if (hx_repl_append(nd->r, s->now, recs, count, &first) == HX_REPL_OK) {
        for (uint32_t k = 0; k < count; k++) {
            if (nd->npending == nd->cappending) {
                const size_t nc = nd->cappending == 0 ? 64U : nd->cappending * 2U;
                pend *p = realloc(nd->pending, nc * sizeof *p);
                if (p == NULL) {
                    sim_fail(s, "out of memory");
                    return;
                }
                nd->pending = p;
                nd->cappending = nc;
            }
            pend *p = &nd->pending[nd->npending++];
            p->off = first + k;
            p->epoch = ep;
            p->rec = keep[k];
            p->rec.epoch = ep;
        }
    }
    collect(s, i);
    check_node(s, i);
}

static void sim_ms(sim *s, bool chaos)
{
    s->now++;
    const uint32_t nn = s->n;
    if (nn == 0) {
        return;
    }
    for (uint32_t i = 0; i < s->n; i++) {
        node *nd = &s->nodes[i];
        if (!nd->up && s->now >= nd->restart_at) {
            node_start(s, i);
        }
    }
    if (chaos) {
        const uint64_t roll = srand_next(s);
        if (roll % 600U == 0) { /* data-plane partition toggles */
            bool any = false;
            for (uint32_t i = 0; i < s->n; i++) {
                any = any || s->group[i] != 0;
            }
            for (uint32_t i = 0; i < s->n; i++) {
                s->group[i] = any ? 0 : (int)(srand_next(s) & 1U);
            }
            if (!any) {
                s->group[0] = 1;
                s->group[s->n - 1U] = 0;
                s->stat_partitions++;
            }
        }
        if (roll % s->cut_every ==
            1) { /* control-plane partition: one replica loses the controller */
            const uint32_t v = (uint32_t)(srand_next(s) % nn);
            s->cut[v] = !s->cut[v];
            if (s->cut[v]) {
                s->stat_cuts++;
            }
        }
        if (roll % s->crash_every == 2) {
            sim_crash(s, (uint32_t)(srand_next(s) % nn), 20 + (int64_t)(srand_next(s) % 400U));
        }
        if (srand_next(s) % 4U == 0) {
            uint32_t pick = HX_REPL_NONE;
            for (uint32_t k = 0; k < s->n; k++) {
                const uint32_t c = (uint32_t)((k + srand_next(s)) % s->n);
                if (s->nodes[c].up && hx_repl_get_role(s->nodes[c].r) == HX_REPL_LEADER) {
                    pick = c;
                    break;
                }
            }
            if (pick != HX_REPL_NONE && s->nodes[pick].lg.nw < MAX_RECORDS - 400U) {
                client_append(s, pick, 1U + (uint32_t)(srand_next(s) % 3U));
            }
        }
    }
    for (uint32_t i = 0; i < s->n; i++) {
        if (s->nodes[i].up && s->now % 40 == (int64_t)(i * 7U) % 40) {
            send_hello(s, i);
        }
    }
    ctl_tick(s);
    for (uint32_t i = 0; i < s->n && !s->bad; i++) {
        if (s->nodes[i].up) {
            (void)hx_repl_tick(s->nodes[i].r, s->now);
            collect(s, i);
            check_node(s, i);
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
    if (g_shadow_fail[0] != '\0') {
        sim_fail(s, "%s", g_shadow_fail);
    }
    if (s->now % 10 == 0 && !s->bad) {
        check_log_matching(s);
    }
}

static void sim_run(sim *s, int64_t ms, bool chaos)
{
    for (int64_t t = 0; t < ms && !s->bad; t++) {
        sim_ms(s, chaos);
    }
}

static void sim_heal(sim *s)
{
    for (uint32_t i = 0; i < s->n; i++) {
        s->group[i] = 0;
        s->cut[i] = false;
        if (!s->nodes[i].up) {
            s->nodes[i].restart_at = s->now;
        }
    }
    s->loss_pm = 0;
    s->ctl_loss_pm = 0;
    s->dup_pm = 0;
}

static uint32_t sim_leader(const sim *s)
{
    for (uint32_t i = 0; i < s->n; i++) {
        if (s->nodes[i].up && hx_repl_get_role(s->nodes[i].r) == HX_REPL_LEADER &&
            hx_repl_epoch(s->nodes[i].r) == ctl_part(s).epoch) {
            return i;
        }
    }
    return HX_REPL_NONE;
}

/* ------------------------------------------------------------ unit tests */

static void test_codec_and_config(void)
{
    const uint8_t pay[3] = {1, 2, 3};
    const hx_repl_rec recs[2] = {{.epoch = 4, .len = 3, .data = pay},
                                 {.epoch = 5, .len = 0, .data = pay}};
    hx_repl_msg m = {.type = HX_REPL_MSG_FETCH_RESP,
                     .from = 1,
                     .to = 2,
                     .epoch = 9,
                     .fetch_offset = 77,
                     .status = HX_REPL_RESP_OK,
                     .hw = 70,
                     .div_epoch = 3,
                     .div_end = 66,
                     .recs = recs,
                     .nrecs = 2};
    hx_buf b;
    hx_buf_init(&b);
    CHECK(hx_repl_msg_encode(&m, &b) == 0);
    hx_repl_rec scratch[4];
    hx_repl_msg d;
    CHECK(hx_repl_msg_decode(b.data, b.len, scratch, 4, &d) == 0);
    CHECK(d.type == HX_REPL_MSG_FETCH_RESP && d.from == 1 && d.to == 2 && d.epoch == 9 &&
          d.fetch_offset == 77 && d.hw == 70 && d.div_epoch == 3 && d.div_end == 66 &&
          d.nrecs == 2);
    CHECK(d.recs[0].epoch == 4 && d.recs[0].len == 3 && memcmp(d.recs[0].data, pay, 3) == 0);
    for (size_t cut = 0; cut < b.len; cut++) {
        CHECK(hx_repl_msg_decode(b.data, cut, scratch, 4, &d) != 0);
    }
    CHECK(hx_buf_put_u8(&b, 0) == 0 && hx_repl_msg_decode(b.data, b.len, scratch, 4, &d) != 0);
    b.len--;
    CHECK(hx_repl_msg_decode(b.data, b.len, scratch, 1, &d) != 0); /* too many records */
    b.data[0] = 0;
    CHECK(hx_repl_msg_decode(b.data, b.len, scratch, 4, &d) != 0);
    hx_buf_free(&b);

    hx_buf_init(&b);
    hx_repl_msg a = {.type = HX_REPL_MSG_ALTER_ISR,
                     .from = 0,
                     .to = HX_REPL_CONTROLLER,
                     .epoch = 3,
                     .isr = 5,
                     .isr_version = 8};
    CHECK(hx_repl_msg_encode(&a, &b) == 0 &&
          hx_repl_msg_decode(b.data, b.len, scratch, 4, &d) == 0);
    CHECK(d.isr == 5 && d.isr_version == 8 && d.to == HX_REPL_CONTROLLER);
    hx_buf_free(&b);

    slog lg;
    slog_init(&lg);
    hx_repl_cfg cfg;
    hx_repl_cfg_default(&cfg, 0, 7);
    slog_cfg(&cfg, &lg);
    hx_repl *r = hx_repl_create(&cfg, 0);
    CHECK(r != NULL);
    hx_repl_destroy(r);
    cfg.id = 3; /* not a member of the replica set */
    CHECK(hx_repl_create(&cfg, 0) == NULL);
    cfg.id = 0;
    cfg.min_isr = 0;
    CHECK(hx_repl_create(&cfg, 0) == NULL);
    cfg.min_isr = 1;
    cfg.log.sync = NULL;
    CHECK(hx_repl_create(&cfg, 0) == NULL);
    slog_free(&lg);
}

/* The log-divergence case that a naive "truncate to the leader's end of my epoch" gets wrong:
 * follower [e1 x5, e2 x3], leader [e1 x7, e3 ...]. The leader has no epoch 2 and answers with the
 * end of epoch 1 (7); the follower must cut to the end of epoch 1 in ITS log (5), not to 7. */
static bool divergence_body(void)
{
    slog lg;
    slog_init(&lg);
    hx_repl_cfg cfg;
    hx_repl_cfg_default(&cfg, 1, 3U | 4U | 2U);
    slog_cfg(&cfg, &lg);
    hx_repl_rec rr[8];
    uint8_t byte = 0;
    for (int i = 0; i < 8; i++) {
        rr[i].epoch = i < 5 ? 1U : 2U;
        rr[i].len = 1;
        rr[i].data = &byte;
    }
    REQ(sl_append(&lg, rr, 8) == 0 && sl_sync(&lg) == 0);
    hx_repl *r = hx_repl_create(&cfg, 0);
    REQ(r != NULL);
    REQ(hx_repl_set_state(r, 1, 3, 0, 7, 1) == 0); /* epoch 3, leader 0, follower is replica 1 */
    const hx_repl_msg *out = NULL;
    size_t n = 0;
    hx_repl_take_msgs(r, &out, &n);
    REQ(n == 1 && out[0].type == HX_REPL_MSG_FETCH && out[0].fetch_offset == 8 &&
        out[0].last_fetched_epoch == 2 && out[0].to == 0);
    hx_repl_msg resp = {.type = HX_REPL_MSG_FETCH_RESP,
                        .from = 0,
                        .to = 1,
                        .epoch = 3,
                        .fetch_offset = 8,
                        .status = HX_REPL_RESP_DIVERGED,
                        .hw = 0,
                        .div_epoch = 1,
                        .div_end = 7};
    REQ(hx_repl_step(r, 2, &resp) == 0);
    REQ(hx_repl_leo(r) == 5 && lg.nd == 5); /* cut to 5, and durably */
    hx_repl_take_msgs(r, &out, &n);
    REQ(n == 1 && out[0].fetch_offset == 5 && out[0].last_fetched_epoch == 1); /* retries at once */
    /* A duplicate of the old answer is ignored: it is about an offset we no longer ask for. */
    REQ(hx_repl_step(r, 3, &resp) == 0);
    hx_repl_take_msgs(r, &out, &n);
    REQ(n == 0 && hx_repl_leo(r) == 5);
    /* A response from a deposed leader (older epoch) is ignored too. */
    hx_repl_rec rec = {.epoch = 2, .len = 1, .data = &byte};
    hx_repl_msg stale = {.type = HX_REPL_MSG_FETCH_RESP,
                         .from = 0,
                         .to = 1,
                         .epoch = 2,
                         .fetch_offset = 5,
                         .status = HX_REPL_RESP_OK,
                         .hw = 6,
                         .recs = &rec,
                         .nrecs = 1};
    REQ(hx_repl_step(r, 4, &stale) == 0 && hx_repl_leo(r) == 5);
    hx_repl_destroy(r);
    slog_free(&lg);
    return true;
}

static void test_divergence_truncates_to_common_prefix(void)
{
    if (!divergence_body()) {
        failures++;
    }
}

static bool never_truncate_committed_body(void)
{
    slog lg;
    slog_init(&lg);
    hx_repl_cfg cfg;
    hx_repl_cfg_default(&cfg, 1, 3);
    slog_cfg(&cfg, &lg);
    uint8_t byte = 0;
    hx_repl_rec rr[4];
    for (int i = 0; i < 4; i++) {
        rr[i] = (hx_repl_rec){.epoch = 1, .len = 1, .data = &byte};
    }
    REQ(sl_append(&lg, rr, 4) == 0 && sl_sync(&lg) == 0);
    hx_repl *r = hx_repl_create(&cfg, 0);
    REQ(r != NULL && hx_repl_set_state(r, 1, 2, 0, 3, 1) == 0);
    const hx_repl_msg *out = NULL;
    size_t n = 0;
    hx_repl_take_msgs(r, &out, &n);
    hx_repl_msg ok = {.type = HX_REPL_MSG_FETCH_RESP,
                      .from = 0,
                      .to = 1,
                      .epoch = 2,
                      .fetch_offset = 4,
                      .status = HX_REPL_RESP_OK,
                      .hw = 3};
    REQ(hx_repl_step(r, 2, &ok) == 0 && hx_repl_hw(r) == 3);
    /* A leader that now says our committed offset 2 conflicts is a protocol violation: stop. */
    hx_repl_msg bad = {.type = HX_REPL_MSG_FETCH_RESP,
                       .from = 0,
                       .to = 1,
                       .epoch = 2,
                       .fetch_offset = 4,
                       .status = HX_REPL_RESP_DIVERGED,
                       .div_epoch = 1,
                       .div_end = 2};
    REQ(hx_repl_step(r, 3, &bad) != 0 && hx_repl_failed(r));
    REQ(hx_repl_leo(r) == 4); /* nothing was cut */
    hx_repl_destroy(r);
    slog_free(&lg);
    return true;
}

static void test_never_truncates_committed_data(void)
{
    if (!never_truncate_committed_body()) {
        failures++;
    }
}

static bool storage_failure_body(void)
{
    slog lg;
    slog_init(&lg);
    hx_repl_cfg cfg;
    hx_repl_cfg_default(&cfg, 0, 3);
    slog_cfg(&cfg, &lg);
    hx_repl *r = hx_repl_create(&cfg, 0);
    REQ(r != NULL && hx_repl_set_state(r, 1, 1, 0, 1, 1) == 0); /* sole ISR member */
    uint8_t byte = 7;
    hx_repl_rec rec = {.len = 1, .data = &byte};
    uint64_t first = 99;
    REQ(hx_repl_append(r, 2, &rec, 1, &first) == HX_REPL_OK && first == 0);
    REQ(hx_repl_hw(r) == 1); /* a one-member ISR commits on its own fsync */
    lg.fail_sync = true;
    REQ(hx_repl_append(r, 3, &rec, 1, &first) == HX_REPL_FAILED && hx_repl_failed(r));
    REQ(hx_repl_hw(r) == 1); /* the unsynced record was never counted */
    REQ(hx_repl_tick(r, 4) != 0);
    hx_repl_destroy(r);
    slog_free(&lg);
    return true;
}

static void test_storage_failure_is_fail_stop(void)
{
    if (!storage_failure_body()) {
        failures++;
    }
}

static bool min_isr_body(void)
{
    slog lg;
    slog_init(&lg);
    hx_repl_cfg cfg;
    hx_repl_cfg_default(&cfg, 0, 7);
    cfg.min_isr = 2;
    slog_cfg(&cfg, &lg);
    hx_repl *r = hx_repl_create(&cfg, 0);
    uint8_t byte = 1;
    hx_repl_rec rec = {.len = 1, .data = &byte};
    uint64_t first = 0;
    REQ(r != NULL && hx_repl_append(r, 1, &rec, 1, &first) == HX_REPL_NOT_LEADER);
    REQ(hx_repl_set_state(r, 1, 1, 0, 1, 1) == 0); /* ISR = {0} only */
    REQ(hx_repl_append(r, 2, &rec, 1, &first) == HX_REPL_NOT_ENOUGH_REPLICAS && lg.nw == 0);
    REQ(hx_repl_set_state(r, 3, 1, 0, 3, 2) == 0); /* same epoch, newer ISR {0,1} */
    REQ(hx_repl_append(r, 4, &rec, 1, &first) == HX_REPL_OK);
    REQ(hx_repl_hw(r) == 0); /* replica 1 has not fetched: not committed */
    REQ(hx_repl_append(r, 4, NULL, 0, &first) == HX_REPL_BAD_ARG);
    hx_repl_destroy(r);
    slog_free(&lg);
    return true;
}

static void test_min_isr_and_leadership_checks(void)
{
    if (!min_isr_body()) {
        failures++;
    }
}

static hx_repl_msg fetch_msg(uint32_t from, uint32_t to, uint32_t epoch, uint64_t off, uint32_t lfe)
{
    hx_repl_msg m = {.type = HX_REPL_MSG_FETCH,
                     .from = from,
                     .to = to,
                     .epoch = epoch,
                     .fetch_offset = off,
                     .last_fetched_epoch = lfe};
    return m;
}

static bool append_n(hx_repl *r, int64_t now, size_t n)
{
    uint8_t byte = 1;
    hx_repl_rec rec = {.len = 1, .data = &byte};
    for (size_t i = 0; i < n; i++) {
        uint64_t first = 0;
        REQ(hx_repl_append(r, now, &rec, 1, &first) == HX_REPL_OK);
    }
    return true;
}

/* An ISR change the leader proposed counts as already in force for the HW: the controller may have
 * approved it while the answer was lost, and the HW must not run past the new member. */
static bool pending_isr_body(void)
{
    slog lg;
    slog_init(&lg);
    hx_repl_cfg cfg;
    hx_repl_cfg_default(&cfg, 0, 7);
    slog_cfg(&cfg, &lg);
    hx_repl *r = hx_repl_create(&cfg, 0);
    REQ(r != NULL && hx_repl_set_state(r, 1, 1, 0, 3, 1) == 0); /* ISR {0,1} */
    REQ(append_n(r, 2, 3));
    hx_repl_msg f1 = fetch_msg(1, 0, 1, 3, 1);
    REQ(hx_repl_step(r, 3, &f1) == 0 && hx_repl_hw(r) == 3);
    hx_repl_msg f2 =
        fetch_msg(2, 0, 1, 3, 1); /* replica 2 is caught up: the leader proposes adding it */
    REQ(hx_repl_step(r, 4, &f2) == 0);
    const hx_repl_msg *out = NULL;
    size_t n = 0;
    hx_repl_take_msgs(r, &out, &n);
    bool proposed = false;
    for (size_t i = 0; i < n; i++) {
        proposed = proposed || (out[i].type == HX_REPL_MSG_ALTER_ISR && out[i].isr == 7 &&
                                out[i].isr_version == 1 && out[i].to == HX_REPL_CONTROLLER);
    }
    REQ(proposed);
    REQ(append_n(r, 5, 2));
    hx_repl_msg f1b = fetch_msg(1, 0, 1, 5, 1);
    REQ(hx_repl_step(r, 6, &f1b) == 0);
    REQ(hx_repl_hw(r) ==
        3); /* replica 1 has 5, replica 2 only 3: the HW waits for the pending member */
    hx_repl_msg f2b = fetch_msg(2, 0, 1, 5, 1);
    REQ(hx_repl_step(r, 7, &f2b) == 0 && hx_repl_hw(r) == 5);
    hx_repl_destroy(r);
    slog_free(&lg);
    return true;
}

static void test_pending_isr_change_holds_back_the_hw(void)
{
    if (!pending_isr_body()) {
        failures++;
    }
}

/* What a leader learned about a follower in one epoch says nothing about the next. */
static bool stale_follower_state_body(void)
{
    slog lg;
    slog_init(&lg);
    hx_repl_cfg cfg;
    hx_repl_cfg_default(&cfg, 0, 7);
    slog_cfg(&cfg, &lg);
    hx_repl *r = hx_repl_create(&cfg, 0);
    REQ(r != NULL && hx_repl_set_state(r, 1, 1, 0, 7, 1) == 0); /* ISR {0,1,2} */
    REQ(append_n(r, 2, 9));
    hx_repl_msg f1 = fetch_msg(1, 0, 1, 9, 1);
    REQ(hx_repl_step(r, 3, &f1) == 0 &&
        hx_repl_hw(r) == 0);                       /* replica 2 has nothing: not committed */
    REQ(hx_repl_set_state(r, 4, 2, 2, 7, 2) == 0); /* replica 2 leads epoch 2 with an empty log */
    hx_repl_msg div = {.type = HX_REPL_MSG_FETCH_RESP,
                       .from = 2,
                       .to = 0,
                       .epoch = 2,
                       .fetch_offset = 9,
                       .status = HX_REPL_RESP_DIVERGED,
                       .div_epoch = 0,
                       .div_end = 0};
    REQ(hx_repl_step(r, 5, &div) == 0 && hx_repl_leo(r) == 0);
    REQ(hx_repl_set_state(r, 6, 3, 0, 3, 3) == 0); /* leader again, ISR {0,1} */
    REQ(append_n(r, 7, 1));
    REQ(hx_repl_hw(r) == 0); /* replica 1's 9 records belonged to a log that no longer exists */
    hx_repl_destroy(r);
    slog_free(&lg);
    return true;
}

static void test_leader_forgets_follower_state_between_epochs(void)
{
    if (!stale_follower_state_body()) {
        failures++;
    }
}

/* ---------------------------------------------------- randomized scenarios */

typedef struct {
    uint64_t scenarios;
    uint64_t commits;
    uint64_t crashes;
    uint64_t partitions;
    uint64_t cuts;
    uint64_t elections;
    uint64_t isr_changes;
    uint64_t acks;
    uint64_t truncations;
} totals;

static bool run_scenario(uint32_t n, uint64_t seed, totals *tot)
{
    sim s;
    const unsigned flavor = (unsigned)(seed % 4U);
    if (!sim_init(&s, n, seed, flavor)) {
        (void)fprintf(stderr, "FAIL n=%u seed=%llu: %s\n", n, (unsigned long long)seed,
                      s.bad ? s.why : "init");
        sim_free(&s);
        return false;
    }
    s.loss_pm = 30 + (uint32_t)(srand_next(&s) % 90U) + (flavor == 3U ? 100U : 0U);
    s.ctl_loss_pm = 50 + (uint32_t)(srand_next(&s) % 200U);
    s.dup_pm = 30;
    sim_run(&s, 12000, true);

    sim_heal(&s);
    sim_run(&s, 3000, false);
    uint32_t l = sim_leader(&s);
    if (!s.bad && l == HX_REPL_NONE) {
        sim_fail(&s, "LIVENESS: no leader 3 s after everything healed");
    }
    if (!s.bad) {
        uint64_t before = 0;
        uint8_t byte = 0x5A;
        hx_repl_rec rec = {.len = 1, .data = &byte};
        if (hx_repl_append(s.nodes[l].r, s.now, &rec, 1, &before) != HX_REPL_OK) {
            sim_fail(&s, "LIVENESS: the leader refused an append");
        } else {
            collect(&s, l);
            sim_run(&s, 10000, false); /* a replica may have thousands of records to catch up on */
            for (uint32_t i = 0; i < n && !s.bad; i++) {
                if (hx_repl_hw(s.nodes[i].r) <= before) {
                    sim_fail(&s, "LIVENESS: replica %u did not commit the marker (hw %llu <= %llu)",
                             i, (unsigned long long)hx_repl_hw(s.nodes[i].r),
                             (unsigned long long)before);
                }
            }
            if (!s.bad && hx_repl_isr(s.nodes[l].r) != s.all) {
                sim_fail(&s, "LIVENESS: the ISR did not recover to the full replica set (isr=%x)",
                         hx_repl_isr(s.nodes[l].r));
            }
        }
    }
    const bool ok = !s.bad;
    if (!ok) {
        (void)fprintf(stderr, "FAIL n=%u seed=%llu t=%lld: %s\n", n, (unsigned long long)seed,
                      (long long)s.now, s.why);
    }
    tot->scenarios++;
    tot->commits += s.stat_commits;
    tot->crashes += s.stat_crashes;
    tot->partitions += s.stat_partitions;
    tot->cuts += s.stat_cuts;
    tot->elections += s.stat_elections;
    tot->isr_changes += s.stat_isr_changes;
    tot->acks += s.stat_acks;
    tot->truncations += s.stat_truncations;
    sim_free(&s);
    return ok;
}

static void test_random_fault_injection(void)
{
    const char *env = getenv("HX_REPL_SEEDS");
    const uint64_t seeds3 = env != NULL ? strtoull(env, NULL, 10) : 120;
    totals tot;
    memset(&tot, 0, sizeof tot);
    for (uint64_t k = 1; k <= seeds3; k++) {
        if (!run_scenario(3, k, &tot)) {
            failures++;
            return;
        }
    }
    for (uint64_t k = 1; k <= seeds3 / 3U + 1U; k++) {
        if (!run_scenario(5, 1000U + k, &tot)) {
            failures++;
            return;
        }
    }
    for (uint64_t k = 1; k <= 6; k++) {
        if (!run_scenario(2, 2000U + k, &tot)) {
            failures++;
            return;
        }
    }
    (void)printf(
        "replication simulation: %llu scenarios, %llu records committed, %llu acknowledged, "
        "%llu elections, %llu ISR changes, %llu truncations after divergence, %llu crashes, "
        "%llu data partitions, %llu controller cuts, no invariant violated\n",
        (unsigned long long)tot.scenarios, (unsigned long long)tot.commits,
        (unsigned long long)tot.acks, (unsigned long long)tot.elections,
        (unsigned long long)tot.isr_changes, (unsigned long long)tot.truncations,
        (unsigned long long)tot.crashes, (unsigned long long)tot.partitions,
        (unsigned long long)tot.cuts);
    if (tot.commits < tot.scenarios * 40U || tot.acks == 0 || tot.crashes == 0 ||
        tot.partitions == 0 || tot.cuts == 0 || tot.isr_changes == 0 || tot.truncations == 0) {
        (void)fputs("FAIL: the simulation did not exercise enough faults to be meaningful\n",
                    stderr);
        failures++;
    }
}

int main(void)
{
    if (getenv("HX_REPL_ONLY_SIM") != NULL) { /* used by the mutation checks */
        test_random_fault_injection();
        return failures != 0 ? 1 : 0;
    }
    test_codec_and_config();
    test_divergence_truncates_to_common_prefix();
    test_never_truncates_committed_data();
    test_storage_failure_is_fail_stop();
    test_min_isr_and_leadership_checks();
    test_pending_isr_change_holds_back_the_hw();
    test_leader_forgets_follower_state_between_epochs();
    test_random_fault_injection();
    if (failures != 0) {
        (void)fprintf(stderr, "%d replication test(s) FAILED\n", failures);
        return 1;
    }
    (void)puts("all replication tests passed");
    return 0;
}
