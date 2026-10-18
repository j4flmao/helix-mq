/*
 * Raft tests. Besides unit tests of single nodes, this file contains a deterministic cluster
 * simulator: virtual time, a lossy / duplicating / reordering network with partitions, nodes that
 * crash and restart from their (simulated) stable storage, and global invariant checks after every
 * event:
 *
 *   election safety      at most one leader per term
 *   state-machine safety no two nodes ever commit different entries at the same index
 *   leader completeness  every new leader holds all entries committed so far
 *   log matching         equal (index, term) implies equal entry and equal prefix
 *   write-ahead          a crash loses everything not yet sync()ed, and the nodes must still agree
 *
 * HX_RAFT_SEEDS=n sets how many random scenarios run (default 80 for 3 nodes, half that for 5).
 */
#define _XOPEN_SOURCE 700
#include <fcntl.h>
#include <ftw.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "raft/raft.h"
#include "raft/raft_wal.h"
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

/* For helpers that own resources: report and return false so the caller can clean up. */
#define REQ(cond)                                                                                  \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define MAXN       5
#define PAYLOAD_SZ 8U
#define MAX_TERMS  200000U
#define MAX_COMMIT 200000U

/* ------------------------------------------------- simulated stable storage */

typedef struct {
    uint64_t term;
    uint32_t len;
    uint8_t data[PAYLOAD_SZ];
} sent;

typedef struct {
    uint64_t wterm;
    uint64_t dterm;
    uint32_t wvote;
    uint32_t dvote;
    sent *w; /* what has been written (may include unsynced data) */
    sent *d; /* what has survived a sync(): this is all a crash leaves behind */
    size_t nw;
    size_t nd;
    size_t cap;
    uint64_t dirty_from; /* lowest index written since the last sync, UINT64_MAX if none */
    bool fail_sync;
} store;

static void store_init(store *s)
{
    memset(s, 0, sizeof *s);
    s->wvote = HX_RAFT_NONE;
    s->dvote = HX_RAFT_NONE;
    s->dirty_from = UINT64_MAX;
}

static void store_free(store *s)
{
    free(s->w);
    free(s->d);
}

static int store_grow(store *s, size_t need)
{
    if (need <= s->cap) {
        return 0;
    }
    size_t ncap = s->cap == 0 ? 256U : s->cap;
    while (ncap < need) {
        ncap *= 2U;
    }
    sent *w = realloc(s->w, ncap * sizeof *w);
    if (w == NULL) {
        return -1;
    }
    s->w = w;
    sent *d = realloc(s->d, ncap * sizeof *d);
    if (d == NULL) {
        return -1;
    }
    s->d = d;
    s->cap = ncap;
    return 0;
}

static int st_save(void *ctx, uint64_t term, uint32_t voted_for)
{
    store *s = ctx;
    s->wterm = term;
    s->wvote = voted_for;
    return 0;
}

static int st_append(void *ctx, uint64_t first, const hx_raft_entry *e, size_t n)
{
    store *s = ctx;
    if (first == 0 || first - 1U > s->nw || store_grow(s, (size_t)(first - 1U) + n) != 0) {
        return -1;
    }
    s->nw = (size_t)(first - 1U);
    for (size_t i = 0; i < n; i++) {
        if (e[i].len > PAYLOAD_SZ) {
            return -1;
        }
        s->w[s->nw].term = e[i].term;
        s->w[s->nw].len = e[i].len;
        if (e[i].len != 0) {
            memcpy(s->w[s->nw].data, e[i].data, e[i].len);
        }
        s->nw++;
    }
    if (first < s->dirty_from) {
        s->dirty_from = first;
    }
    return 0;
}

static int st_sync(void *ctx)
{
    store *s = ctx;
    if (s->fail_sync) {
        return -1;
    }
    s->dterm = s->wterm;
    s->dvote = s->wvote;
    if (s->dirty_from != UINT64_MAX) {
        const size_t from = (size_t)(s->dirty_from - 1U);
        for (size_t i = from; i < s->nw; i++) {
            s->d[i] = s->w[i];
        }
        s->nd = s->nw;
        s->dirty_from = UINT64_MAX;
    }
    return 0;
}

/* Power loss: everything not synced disappears. */
static void store_crash(store *s)
{
    s->wterm = s->dterm;
    s->wvote = s->dvote;
    for (size_t i = 0; i < s->nd; i++) {
        s->w[i] = s->d[i];
    }
    s->nw = s->nd;
    s->dirty_from = UINT64_MAX;
}

static void store_cfg(hx_raft_cfg *cfg, store *s)
{
    cfg->storage.ctx = s;
    cfg->storage.save_hard_state = st_save;
    cfg->storage.append = st_append;
    cfg->storage.sync = st_sync;
}

/* Builds a raft instance from what survived in `s`. */
static hx_raft *raft_from_store(const hx_raft_cfg *cfg, store *s, int64_t now)
{
    hx_raft_entry *ents = s->nd != 0 ? calloc(s->nd, sizeof *ents) : NULL;
    for (size_t i = 0; i < s->nd; i++) {
        ents[i].term = s->d[i].term;
        ents[i].len = s->d[i].len;
        ents[i].data = s->d[i].data;
    }
    const hx_raft_restore rs = {
        .term = s->dterm, .voted_for = s->dvote, .entries = ents, .nentries = s->nd};
    hx_raft *r = hx_raft_create(cfg, &rs, now);
    free(ents);
    return r;
}

/* ------------------------------------------------------------- simulator */

typedef struct {
    hx_buf bytes;
    int64_t at;
    uint32_t from;
    uint32_t to;
} flight;

typedef struct {
    hx_raft *r;
    store st;
    hx_raft_wal *wal; /* WAL-backed mode: the real durable log instead of the in-memory store */
    char path[320];
    bool up;
    int64_t restart_at;
    uint64_t seen_commit;
    uint64_t leader_checked_term;
    uint64_t applied; /* state machine: entries applied (snapshot mode) */
    uint64_t sm_hash; /* chained hash of everything applied */
} node;

typedef struct {
    uint32_t n;
    node nodes[MAXN];
    hx_raft_cfg cfg[MAXN];
    int64_t now;
    uint64_t rng;
    uint64_t seed;

    flight *net;
    size_t nnet;
    size_t capnet;
    int group[MAXN];
    uint32_t loss_pm;
    uint32_t dup_pm;
    bool use_wal; /* nodes persist through hx_raft_wal; a crash cuts the file to its synced size */
    char dir[256];
    bool snapshots;      /* nodes apply entries to a hash state machine and compact their logs */
    uint32_t snap_every; /* compact once this many applied entries sit above the snapshot */
    uint64_t *chain;     /* chain[i] = state machine hash after applying entries 1..i */
    uint64_t stat_snaps;
    uint64_t stat_installs;
    bool crash_on_vote;   /* adversary: power-cycle a node right after it granted a vote */
    uint32_t crash_every; /* chaos: a node crashes about once per this many ms (0 = never) */
    uint32_t min_delay;
    uint32_t max_delay;

    uint32_t *leader_of_term;
    sent *committed; /* committed[i] belongs to index i + 1 */
    uint64_t c_max;
    uint64_t payload_seq;

    uint64_t stat_commits;
    uint64_t stat_crashes;
    uint64_t stat_partitions;
    uint64_t stat_leader_changes;

    bool bad;
    char why[200];
} sim;

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

#define FNV_PRIME 1099511628211ULL

static uint64_t ent_hash(uint64_t term, uint32_t len, const uint8_t *data)
{
    uint64_t h = 1469598103934665603ULL ^ term;
    h = (h ^ len) * FNV_PRIME;
    for (uint32_t k = 0; k < len; k++) {
        h = (h ^ data[k]) * FNV_PRIME;
    }
    return h;
}

static uint64_t be64(const uint8_t *p)
{
    uint64_t v = 0;
    for (unsigned k = 0; k < 8U; k++) {
        v = (v << 8) | p[k];
    }
    return v;
}

static void put_be64(uint8_t *p, uint64_t v)
{
    for (unsigned k = 0; k < 8U; k++) {
        p[k] = (uint8_t)(v >> (56U - 8U * k));
    }
}

static void node_start(sim *s, uint32_t i)
{
    node *nd = &s->nodes[i];
    if (s->use_wal) {
        if (hx_raft_wal_open(nd->path, &nd->wal) != HX_WAL_OK) {
            sim_fail(s, "node %u: the WAL did not reopen after a crash", i);
            return;
        }
        hx_raft_wal_set_nosync_for_tests(nd->wal, true);
        if (s->snapshots && srand_next(s) % 2U == 0 && hx_raft_wal_compact(nd->wal) != HX_WAL_OK) {
            sim_fail(s, "node %u: WAL compaction after reopen failed", i);
            return;
        }
        s->cfg[i].storage = hx_raft_wal_storage(nd->wal);
        hx_raft_restore rs = hx_raft_wal_restore(nd->wal);
        nd->applied = 0;
        nd->sm_hash = 0;
        if (s->snapshots && rs.snap_index != 0) { /* the state machine restarts from the snapshot */
            if (rs.snap_len != 16U || be64(rs.snap_data) != rs.snap_index ||
                rs.snap_index > s->c_max || be64(rs.snap_data + 8) != s->chain[rs.snap_index]) {
                sim_fail(s, "node %u: the snapshot read back from its WAL is wrong", i);
                return;
            }
            nd->applied = rs.snap_index;
            nd->sm_hash = be64(rs.snap_data + 8);
        }
        nd->r = hx_raft_create(&s->cfg[i], &rs, s->now);
    } else {
        nd->r = raft_from_store(&s->cfg[i], &nd->st, s->now);
    }
    nd->up = nd->r != NULL;
    nd->seen_commit = 0;
    nd->leader_checked_term = 0;
    if (nd->r == NULL) {
        sim_fail(s, "node %u could not start from its stable storage", i);
    }
}

static bool sim_init_flavor_wal(sim *s, uint32_t n, uint64_t seed, unsigned flavor, bool wal);

static bool sim_init_flavor(sim *s, uint32_t n, uint64_t seed, unsigned flavor)
{
    return sim_init_flavor_wal(s, n, seed, flavor, false);
}

static int rm_cb(const char *p, const struct stat *sb, int t, struct FTW *f)
{
    (void)sb;
    (void)t;
    (void)f;
    return remove(p);
}

static bool sim_init_flavor_wal(sim *s, uint32_t n, uint64_t seed, unsigned flavor, bool wal)
{
    memset(s, 0, sizeof *s);
    if (wal) {
        const char *t = getenv("TMPDIR");
        (void)snprintf(s->dir, sizeof s->dir, "%s/helix-rsim-XXXXXX",
                       (t != NULL && *t != '\0') ? t : "/tmp");
        if (mkdtemp(s->dir) == NULL) {
            return false;
        }
        s->use_wal = true;
    }
    s->snapshots = wal && (flavor & 4U) != 0;
    s->snap_every = 6U + (uint32_t)(seed % 19U);
    s->crash_every = (flavor & 1U) != 0 ? 120U : 900U;
    s->crash_on_vote = (flavor & 2U) != 0;
    s->n = n;
    s->seed = seed;
    s->rng = seed * 0x9E3779B97F4A7C15ULL + 12345U;
    s->min_delay = 1;
    s->max_delay = 20;
    s->leader_of_term = malloc(MAX_TERMS * sizeof *s->leader_of_term);
    s->committed = malloc(MAX_COMMIT * sizeof *s->committed);
    s->chain = calloc(MAX_COMMIT + 1U, sizeof *s->chain);
    if (s->leader_of_term == NULL || s->committed == NULL || s->chain == NULL) {
        return false;
    }
    for (size_t i = 0; i < MAX_TERMS; i++) {
        s->leader_of_term[i] = HX_RAFT_NONE;
    }
    for (uint32_t i = 0; i < n; i++) {
        hx_raft_cfg_default(&s->cfg[i], i, n);
        s->cfg[i].seed = seed * 131U + i;
        if ((flavor & 2U) != 0) { /* tight, nearly equal timeouts provoke split votes */
            s->cfg[i].election_min_ms = 100;
            s->cfg[i].election_max_ms = 112;
            s->cfg[i].heartbeat_ms = 30;
        }
        store_init(&s->nodes[i].st);
        store_cfg(&s->cfg[i], &s->nodes[i].st);
        (void)snprintf(s->nodes[i].path, sizeof s->nodes[i].path, "%s/node%u.wal", s->dir, i);
        node_start(s, i);
    }
    return !s->bad;
}

static bool sim_init(sim *s, uint32_t n, uint64_t seed)
{
    return sim_init_flavor(s, n, seed, 0);
}

static void sim_free(sim *s)
{
    for (uint32_t i = 0; i < s->n; i++) {
        hx_raft_destroy(s->nodes[i].r);
        if (s->nodes[i].wal != NULL) {
            hx_raft_wal_close(s->nodes[i].wal);
        }
        store_free(&s->nodes[i].st);
    }
    if (s->use_wal) {
        (void)nftw(s->dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
    }
    for (size_t i = 0; i < s->nnet; i++) {
        hx_buf_free(&s->net[i].bytes);
    }
    free(s->net);
    free(s->leader_of_term);
    free(s->committed);
    free(s->chain);
}

static bool same_entry(const hx_raft_entry *a, const sent *b)
{
    return a->term == b->term && a->len == b->len &&
           (a->len == 0 || memcmp(a->data, b->data, a->len) == 0);
}

/* Applies committed entries to the node's state machine, loads installed snapshots, and compacts.
 */
static void sm_apply(sim *s, uint32_t i)
{
    node *nd = &s->nodes[i];
    hx_raft *r = nd->r;
    uint64_t si = 0;
    uint64_t st = 0;
    const uint8_t *img = NULL;
    uint32_t il = 0;
    if (hx_raft_take_installed_snapshot(r, &si, &st, &img, &il)) {
        if (il != 16U || be64(img) != si || si > s->c_max || be64(img + 8) != s->chain[si]) {
            sim_fail(s, "node %u installed a snapshot whose content is wrong (index %llu)", i,
                     (unsigned long long)si);
            return;
        }
        nd->applied = si;
        nd->sm_hash = be64(img + 8);
        s->stat_installs++;
    }
    const uint64_t c = hx_raft_commit_index(r);
    while (nd->applied < c) {
        hx_raft_entry e;
        if (!hx_raft_entry_at(r, nd->applied + 1U, &e)) {
            sim_fail(s, "node %u: entry %llu was compacted before the state machine applied it", i,
                     (unsigned long long)nd->applied + 1U);
            return;
        }
        nd->sm_hash = (nd->sm_hash ^ ent_hash(e.term, e.len, e.data)) * FNV_PRIME;
        nd->applied++;
        if (nd->applied > s->c_max || nd->sm_hash != s->chain[nd->applied]) {
            sim_fail(s, "STATE MACHINE DIVERGED: node %u after applying %llu", i,
                     (unsigned long long)nd->applied);
            return;
        }
    }
    if (nd->applied - hx_raft_snapshot_index(r) >= s->snap_every) {
        uint8_t image[16];
        put_be64(image, nd->applied);
        put_be64(image + 8, nd->sm_hash);
        if (hx_raft_snapshot(r, s->now, nd->applied, image, sizeof image) != 0) {
            sim_fail(s, "node %u: taking a snapshot at %llu failed", i,
                     (unsigned long long)nd->applied);
            return;
        }
        s->stat_snaps++;
        /* Between raft calls everything is synced, so compacting the file is safe at any point
         * here. */
        if (s->use_wal && srand_next(s) % 4U == 0 && hx_raft_wal_compact(nd->wal) != HX_WAL_OK) {
            sim_fail(s, "node %u: WAL compaction failed", i);
        }
    }
}

/* Per-node invariants, run after every call into a node. */
static void check_node(sim *s, uint32_t i)
{
    node *nd = &s->nodes[i];
    hx_raft *r = nd->r;
    if (!nd->up) {
        return;
    }
    if (hx_raft_failed(r)) {
        sim_fail(s, "node %u entered the failed state", i);
        return;
    }
    if (hx_raft_get_role(r) == HX_RAFT_LEADER) {
        const uint64_t t = hx_raft_term(r);
        if (t >= MAX_TERMS) {
            sim_fail(s, "term %llu out of the simulator's range", (unsigned long long)t);
            return;
        }
        if (s->leader_of_term[t] == HX_RAFT_NONE) {
            s->leader_of_term[t] = i;
            s->stat_leader_changes++;
        } else if (s->leader_of_term[t] != i) {
            sim_fail(s, "ELECTION SAFETY: nodes %u and %u both lead term %llu",
                     s->leader_of_term[t], i, (unsigned long long)t);
            return;
        }
        if (nd->leader_checked_term != t) {
            nd->leader_checked_term = t;
            for (uint64_t idx = hx_raft_snapshot_index(r) + 1U; idx <= s->c_max; idx++) {
                hx_raft_entry e;
                if (!hx_raft_entry_at(r, idx, &e) || !same_entry(&e, &s->committed[idx - 1U])) {
                    sim_fail(
                        s,
                        "LEADER COMPLETENESS: new leader %u (term %llu) lacks committed index %llu",
                        i, (unsigned long long)t, (unsigned long long)idx);
                    return;
                }
            }
        }
    }
    const uint64_t c = hx_raft_commit_index(r);
    if (c < nd->seen_commit) {
        sim_fail(s, "node %u commit index went backwards (%llu -> %llu)", i,
                 (unsigned long long)nd->seen_commit, (unsigned long long)c);
        return;
    }
    if (c > hx_raft_last_index(r) || c >= MAX_COMMIT) {
        sim_fail(s, "node %u commit index %llu beyond its log", i, (unsigned long long)c);
        return;
    }
    const uint64_t cbase = hx_raft_snapshot_index(r);
    if (cbase > c) {
        sim_fail(s, "node %u: snapshot index %llu is beyond its commit index", i,
                 (unsigned long long)cbase);
        return;
    }
    if (nd->seen_commit < cbase) {
        nd->seen_commit =
            cbase; /* covered by the snapshot; the state machine hash vouches for it */
    }
    for (uint64_t idx = nd->seen_commit + 1U; idx <= c; idx++) {
        hx_raft_entry e;
        if (!hx_raft_entry_at(r, idx, &e) || e.len > PAYLOAD_SZ) {
            sim_fail(s, "node %u committed index %llu but cannot read it", i,
                     (unsigned long long)idx);
            return;
        }
        if (idx <= s->c_max) {
            if (!same_entry(&e, &s->committed[idx - 1U])) {
                sim_fail(s,
                         "STATE MACHINE SAFETY: index %llu committed with two different values "
                         "(node %u)",
                         (unsigned long long)idx, i);
                return;
            }
        } else {
            if (idx != s->c_max + 1U) {
                sim_fail(s, "commit gap at %llu", (unsigned long long)idx);
                return;
            }
            s->committed[idx - 1U].term = e.term;
            s->committed[idx - 1U].len = e.len;
            if (e.len != 0) {
                memcpy(s->committed[idx - 1U].data, e.data, e.len);
            }
            s->c_max = idx;
            s->chain[idx] = (s->chain[idx - 1U] ^ ent_hash(e.term, e.len, e.data)) * FNV_PRIME;
            s->stat_commits++;
        }
    }
    nd->seen_commit = c;
    if (s->snapshots) {
        sm_apply(s, i);
    }
}

/* Log matching between every pair of live nodes. */
static void check_log_matching(sim *s)
{
    for (uint32_t a = 0; a < s->n; a++) {
        for (uint32_t b = a + 1U; b < s->n; b++) {
            if (!s->nodes[a].up || !s->nodes[b].up) {
                continue;
            }
            const hx_raft *ra = s->nodes[a].r;
            const hx_raft *rb = s->nodes[b].r;
            uint64_t top = hx_raft_last_index(ra) < hx_raft_last_index(rb) ? hx_raft_last_index(ra)
                                                                           : hx_raft_last_index(rb);
            const uint64_t lo = hx_raft_snapshot_index(ra) > hx_raft_snapshot_index(rb)
                                    ? hx_raft_snapshot_index(ra)
                                    : hx_raft_snapshot_index(rb);
            hx_raft_entry ea;
            hx_raft_entry eb;
            while (top > lo) {
                (void)hx_raft_entry_at(ra, top, &ea);
                (void)hx_raft_entry_at(rb, top, &eb);
                if (ea.term == eb.term) {
                    break;
                }
                top--;
            }
            for (uint64_t idx = top; idx > lo; idx--) {
                (void)hx_raft_entry_at(ra, idx, &ea);
                (void)hx_raft_entry_at(rb, idx, &eb);
                if (ea.term != eb.term || ea.len != eb.len ||
                    (ea.len != 0 && memcmp(ea.data, eb.data, ea.len) != 0)) {
                    sim_fail(s,
                             "LOG MATCHING: nodes %u and %u diverge at %llu below a common (index, "
                             "term)",
                             a, b, (unsigned long long)idx);
                    return;
                }
            }
        }
    }
}

static void net_push(sim *s, uint32_t from, uint32_t to, const hx_buf *bytes, int64_t at)
{
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
    hx_buf_init(&f->bytes);
    (void)hx_buf_put(&f->bytes, bytes->data, bytes->len);
    f->at = at;
    f->from = from;
    f->to = to;
}

static void net_send(sim *s, const hx_raft_msg *m)
{
    if (s->group[m->from] != s->group[m->to] || srand_next(s) % 1000U < s->loss_pm) {
        return;
    }
    hx_buf b;
    hx_buf_init(&b);
    if (hx_raft_msg_encode(m, &b) != 0) {
        sim_fail(s, "encode failed");
        hx_buf_free(&b);
        return;
    }
    const uint32_t span = s->max_delay - s->min_delay + 1U;
    net_push(s, m->from, m->to, &b, s->now + s->min_delay + (int64_t)(srand_next(s) % span));
    if (srand_next(s) % 1000U < s->dup_pm) {
        net_push(s, m->from, m->to, &b, s->now + s->min_delay + (int64_t)(srand_next(s) % span));
    }
    hx_buf_free(&b);
}

static void sim_crash(sim *s, uint32_t i, int64_t down_ms);

static void collect(sim *s, uint32_t i)
{
    const hx_raft_msg *msgs = NULL;
    size_t n = 0;
    hx_raft_take_msgs(s->nodes[i].r, &msgs, &n);
    bool granted_vote = false;
    for (size_t k = 0; k < n; k++) {
        granted_vote = granted_vote || (msgs[k].type == HX_RAFT_MSG_VOTE_RESP && msgs[k].granted);
        net_send(s, &msgs[k]);
    }
    if (granted_vote && s->crash_on_vote && srand_next(s) % 3U == 0) {
        check_node(s, i);
        sim_crash(s, i, 10 + (int64_t)(srand_next(s) % 100U));
    }
}

static void deliver(sim *s, const flight *f)
{
    node *nd = &s->nodes[f->to];
    if (!nd->up || s->group[f->from] != s->group[f->to]) {
        return;
    }
    hx_raft_entry scratch[128];
    hx_raft_msg m;
    if (hx_raft_msg_decode(f->bytes.data, f->bytes.len, scratch, 128, &m) != 0) {
        sim_fail(s, "own message failed to decode");
        return;
    }
    (void)hx_raft_step(nd->r, s->now, &m);
    collect(s, f->to);
    check_node(s, f->to);
}

static void sim_crash(sim *s, uint32_t i, int64_t down_ms)
{
    node *nd = &s->nodes[i];
    if (!nd->up) {
        return;
    }
    hx_raft_destroy(nd->r);
    nd->r = NULL;
    nd->up = false;
    nd->restart_at = s->now + down_ms;
    if (s->use_wal) {
        /* Power loss: the file keeps everything that was synced, and an arbitrary part of the rest.
         */
        const uint64_t synced = hx_raft_wal_synced_size(nd->wal);
        const uint64_t written = hx_raft_wal_file_size(nd->wal);
        hx_raft_wal_close(nd->wal);
        nd->wal = NULL;
        const uint64_t how = srand_next(s) % 3U;
        uint64_t keep = synced;
        if (how == 1U && written > synced) {
            keep = synced + srand_next(s) % (written - synced + 1U); /* a torn prefix */
        }
        bool ok = truncate(nd->path, (off_t)keep) == 0;
        if (how == 2U) { /* extended with never-written zeros */
            const int fd = open(nd->path, O_WRONLY | O_APPEND);
            uint8_t z[200] = {0};
            ok = ok && fd >= 0 && write(fd, z, 1U + (size_t)(srand_next(s) % sizeof z)) > 0;
            if (fd >= 0) {
                (void)close(fd);
            }
        }
        if (!ok) {
            sim_fail(s, "node %u: could not simulate power loss on its WAL", i);
        }
    } else {
        store_crash(&nd->st);
    }
    s->stat_crashes++;
}

static hx_raft_status sim_propose(sim *s, uint32_t i, uint32_t len)
{
    uint8_t buf[PAYLOAD_SZ];
    const uint64_t v = ++s->payload_seq;
    for (unsigned k = 0; k < PAYLOAD_SZ; k++) {
        buf[k] = (uint8_t)(v >> (8U * (PAYLOAD_SZ - 1U - k)));
    }
    uint64_t idx = 0;
    uint64_t term = 0;
    const hx_raft_status st = hx_raft_propose(s->nodes[i].r, s->now, buf, len, &idx, &term);
    collect(s, i);
    check_node(s, i);
    return st;
}

/* Advance virtual time by one millisecond. `chaos` enables random faults and proposals. */
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
        if (roll % 500U == 0) {
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
        } else if (s->crash_every != 0 && roll % s->crash_every == 1) {
            sim_crash(s, (uint32_t)(srand_next(s) % nn), 20 + (int64_t)(srand_next(s) % 580U));
        }
        if (srand_next(s) % 6U == 0) {
            uint32_t pick = HX_RAFT_NONE;
            for (uint32_t i = 0; i < s->n; i++) {
                const uint32_t c = (uint32_t)((i + srand_next(s)) % nn);
                if (s->nodes[c].up && hx_raft_get_role(s->nodes[c].r) == HX_RAFT_LEADER) {
                    pick = c;
                    break;
                }
            }
            if (pick == HX_RAFT_NONE && srand_next(s) % 10U == 0) {
                pick = (uint32_t)(srand_next(s) % nn);
            }
            if (pick != HX_RAFT_NONE && s->nodes[pick].up) {
                static const uint32_t lens[] = {8, 8, 8, 0, 3};
                (void)sim_propose(s, pick, lens[srand_next(s) % 5U]);
            }
        }
    }
    for (uint32_t i = 0; i < s->n && !s->bad; i++) {
        if (s->nodes[i].up) {
            (void)hx_raft_tick(s->nodes[i].r, s->now);
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
    }
    s->loss_pm = 0;
    s->dup_pm = 0;
}

/* The unique live leader with the highest term, or HX_RAFT_NONE. */
static uint32_t sim_leader(const sim *s)
{
    uint32_t best = HX_RAFT_NONE;
    for (uint32_t i = 0; i < s->n; i++) {
        if (s->nodes[i].up && hx_raft_get_role(s->nodes[i].r) == HX_RAFT_LEADER &&
            (best == HX_RAFT_NONE ||
             hx_raft_term(s->nodes[i].r) > hx_raft_term(s->nodes[best].r))) {
            best = i;
        }
    }
    return best;
}

/* ------------------------------------------------------ scripted scenarios */

static bool test_no_faults_elects_and_replicates_body(sim *s)
{
    sim_run(s, 1500, false);
    REQ(!s->bad);
    const uint32_t l = sim_leader(s);
    REQ(l != HX_RAFT_NONE);
    for (uint32_t i = 0; i < 3; i++) {
        REQ(hx_raft_term(s->nodes[i].r) == hx_raft_term(s->nodes[l].r));
        REQ(hx_raft_leader(s->nodes[i].r) == l);
    }
    for (int k = 0; k < 100; k++) {
        REQ(sim_propose(s, l, 8) == HX_RAFT_OK);
        sim_run(s, 3, false);
    }
    REQ(sim_propose(s, (l + 1U) % 3U, 8) == HX_RAFT_NOT_LEADER);
    uint8_t big[2000];
    memset(big, 1, sizeof big);
    s->cfg[l].max_entry_bytes = 1U << 20;
    sim_run(s, 500, false);
    REQ(!s->bad);
    for (uint32_t i = 0; i < 3; i++) {
        REQ(hx_raft_commit_index(s->nodes[i].r) == 101); /* leader no-op + 100 commands */
        REQ(hx_raft_last_index(s->nodes[i].r) == 101);
    }
    return true;
}

static void test_no_faults_elects_and_replicates(void)
{
    sim s;
    if (!sim_init(&s, 3, 7)) {
        failures++;
        sim_free(&s);
        return;
    }
    if (!test_no_faults_elects_and_replicates_body(&s)) {
        failures++;
    }
    sim_free(&s);
}

static bool test_prevote_prevents_term_inflation_body(sim *s)
{
    sim_run(s, 1500, false);
    const uint32_t l = sim_leader(s);
    REQ(l != HX_RAFT_NONE);
    const uint64_t term0 = hx_raft_term(s->nodes[l].r);
    const uint32_t f = (l + 1U) % 3U;
    s->group[f] = 1; /* cut one follower off for a long time */
    sim_run(s, 6000, false);
    REQ(!s->bad);
    REQ(hx_raft_term(s->nodes[f].r) == term0); /* without PreVote it would have climbed to ~30 */
    REQ(hx_raft_term(s->nodes[l].r) == term0 && hx_raft_get_role(s->nodes[l].r) == HX_RAFT_LEADER);
    sim_heal(s);
    sim_run(s, 1500, false);
    REQ(!s->bad);
    REQ(sim_leader(s) == l &&
        hx_raft_term(s->nodes[l].r) == term0); /* rejoining did not disturb it */
    REQ(hx_raft_term(s->nodes[f].r) == term0);
    return true;
}

static void test_prevote_prevents_term_inflation(void)
{
    sim s;
    if (!sim_init(&s, 3, 11)) {
        failures++;
        sim_free(&s);
        return;
    }
    if (!test_prevote_prevents_term_inflation_body(&s)) {
        failures++;
    }
    sim_free(&s);
}

static bool test_isolated_leader_steps_down_and_loses_uncommitted_body(sim *s)
{
    sim_run(s, 1500, false);
    const uint32_t old = sim_leader(s);
    REQ(old != HX_RAFT_NONE);
    for (int k = 0; k < 5; k++) {
        REQ(sim_propose(s, old, 8) == HX_RAFT_OK);
        sim_run(s, 30, false);
    }
    const uint64_t committed_before = hx_raft_commit_index(s->nodes[old].r);

    s->group[old] = 1;
    REQ(sim_propose(s, old, 8) == HX_RAFT_OK); /* accepted locally, can never commit */
    REQ(sim_propose(s, old, 8) == HX_RAFT_OK);
    sim_run(s, 1200, false);
    REQ(!s->bad);
    REQ(hx_raft_get_role(s->nodes[old].r) != HX_RAFT_LEADER); /* CheckQuorum */
    const uint32_t fresh = sim_leader(s);
    REQ(fresh != HX_RAFT_NONE && fresh != old);
    REQ(hx_raft_term(s->nodes[fresh].r) > hx_raft_term(s->nodes[old].r));
    REQ(hx_raft_commit_index(s->nodes[old].r) == committed_before);

    for (int k = 0; k < 10; k++) {
        REQ(sim_propose(s, fresh, 8) == HX_RAFT_OK);
        sim_run(s, 10, false);
    }
    sim_heal(s);
    sim_run(s, 1500, false);
    REQ(!s->bad);
    const uint64_t c = hx_raft_commit_index(s->nodes[fresh].r);
    REQ(c > committed_before + 10);
    for (uint32_t i = 0; i < 3; i++) {
        REQ(hx_raft_commit_index(s->nodes[i].r) == c);
        REQ(hx_raft_last_index(s->nodes[i].r) ==
            hx_raft_last_index(s->nodes[fresh].r)); /* old suffix gone */
    }
    return true;
}

static void test_isolated_leader_steps_down_and_loses_uncommitted(void)
{
    sim s;
    if (!sim_init(&s, 3, 21)) {
        failures++;
        sim_free(&s);
        return;
    }
    if (!test_isolated_leader_steps_down_and_loses_uncommitted_body(&s)) {
        failures++;
    }
    sim_free(&s);
}

static bool test_single_node_cluster_body(sim *s)
{
    sim_run(s, 600, false);
    REQ(hx_raft_get_role(s->nodes[0].r) == HX_RAFT_LEADER);
    REQ(sim_propose(s, 0, 8) == HX_RAFT_OK);
    REQ(hx_raft_commit_index(s->nodes[0].r) == 2); /* no-op + command, no network needed */
    sim_crash(s, 0, 10);
    sim_run(s, 700, false);
    REQ(!s->bad);
    REQ(hx_raft_get_role(s->nodes[0].r) == HX_RAFT_LEADER);
    REQ(hx_raft_last_index(s->nodes[0].r) >= 3); /* both entries survived, plus a new no-op */
    return true;
}

static void test_single_node_cluster(void)
{
    sim s;
    if (!sim_init(&s, 1, 3)) {
        failures++;
        sim_free(&s);
        return;
    }
    if (!test_single_node_cluster_body(&s)) {
        failures++;
    }
    sim_free(&s);
}

/* ------------------------------------------------- single-node unit tests */

static hx_raft_msg vote_msg(uint32_t from, uint32_t to, uint64_t term, uint64_t li, uint64_t lt)
{
    hx_raft_msg m;
    memset(&m, 0, sizeof m);
    m.type = HX_RAFT_MSG_VOTE;
    m.from = from;
    m.to = to;
    m.term = term;
    m.last_index = li;
    m.last_term = lt;
    return m;
}

static bool granted_in(hx_raft *r)
{
    const hx_raft_msg *msgs = NULL;
    size_t n = 0;
    hx_raft_take_msgs(r, &msgs, &n);
    return n == 1 && msgs[0].type == HX_RAFT_MSG_VOTE_RESP && msgs[0].granted;
}

static void test_vote_is_durable_and_log_must_be_up_to_date(void)
{
    store st;
    store_init(&st);
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, 0, 3);
    store_cfg(&cfg, &st);
    hx_raft *r = hx_raft_create(&cfg, NULL, 0);
    CHECK(r != NULL);

    hx_raft_msg m = vote_msg(1, 0, 5, 0, 0);
    CHECK(hx_raft_step(r, 1, &m) == 0 && granted_in(r));
    CHECK(hx_raft_term(r) == 5 && hx_raft_voted_for(r) == 1);
    m = vote_msg(2, 0, 5, 0, 0);
    CHECK(hx_raft_step(r, 2, &m) == 0 && !granted_in(r)); /* one vote per term */

    /* Crash and restart: the vote must have been written ahead of the reply. */
    hx_raft_destroy(r);
    store_crash(&st);
    r = raft_from_store(&cfg, &st, 3);
    CHECK(r != NULL && hx_raft_term(r) == 5 && hx_raft_voted_for(r) == 1);
    m = vote_msg(2, 0, 5, 0, 0);
    CHECK(hx_raft_step(r, 4, &m) == 0 && !granted_in(r));
    m = vote_msg(1, 0, 5, 0, 0);
    CHECK(hx_raft_step(r, 5, &m) == 0 && granted_in(r)); /* the same candidate again is fine */
    hx_raft_destroy(r);
    store_free(&st);

    /* Election restriction: a candidate with a staler log is refused. */
    store_init(&st);
    const uint8_t d[1] = {9};
    const hx_raft_entry ents[2] = {{.term = 2, .len = 1, .data = d},
                                   {.term = 2, .len = 1, .data = d}};
    const hx_raft_restore rs = {
        .term = 2, .voted_for = HX_RAFT_NONE, .entries = ents, .nentries = 2};
    r = hx_raft_create(&cfg, &rs, 0);
    CHECK(r != NULL);
    m = vote_msg(1, 0, 3, 1, 2); /* same last term, shorter log */
    CHECK(hx_raft_step(r, 1, &m) == 0 && !granted_in(r));
    m = vote_msg(1, 0, 3, 5, 1); /* longer log but older last term */
    CHECK(hx_raft_step(r, 2, &m) == 0 && !granted_in(r));
    m = vote_msg(2, 0, 4, 2, 2); /* equal log */
    CHECK(hx_raft_step(r, 3, &m) == 0 && granted_in(r));
    hx_raft_destroy(r);
    store_free(&st);
}

static void test_storage_failure_is_fail_stop(void)
{
    store st;
    store_init(&st);
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, 0, 3);
    store_cfg(&cfg, &st);
    hx_raft *r = hx_raft_create(&cfg, NULL, 0);
    CHECK(r != NULL);
    st.fail_sync = true;
    hx_raft_msg m = vote_msg(1, 0, 5, 0, 0);
    CHECK(hx_raft_step(r, 1, &m) == -1);
    CHECK(hx_raft_failed(r));
    const hx_raft_msg *msgs = NULL;
    size_t n = 99;
    hx_raft_take_msgs(r, &msgs, &n);
    CHECK(n == 0); /* the vote reply must not leave a node that could not persist the vote */
    CHECK(hx_raft_tick(r, 1000) == -1);
    hx_raft_destroy(r);
    store_free(&st);
}

static void test_follower_append_semantics(void)
{
    store st;
    store_init(&st);
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, 1, 3);
    store_cfg(&cfg, &st);
    hx_raft *r = hx_raft_create(&cfg, NULL, 0);
    CHECK(r != NULL);

    const uint8_t d[3][1] = {{'a'}, {'b'}, {'c'}};
    hx_raft_entry e[3];
    for (int i = 0; i < 3; i++) {
        e[i].term = 2;
        e[i].len = 1;
        e[i].data = d[i];
    }
    hx_raft_msg a;
    memset(&a, 0, sizeof a);
    a.type = HX_RAFT_MSG_APPEND;
    a.from = 0;
    a.to = 1;
    a.term = 2;
    a.entries = e;
    a.nentries = 3;
    const hx_raft_msg *out = NULL;
    size_t n = 0;

    CHECK(hx_raft_step(r, 1, &a) == 0);
    hx_raft_take_msgs(r, &out, &n);
    CHECK(n == 1 && out[0].type == HX_RAFT_MSG_APPEND_RESP && out[0].granted && out[0].match == 3);
    CHECK(hx_raft_last_index(r) == 3 && hx_raft_commit_index(r) == 0 && hx_raft_leader(r) == 0);

    /* A delayed, shorter copy must neither truncate the log nor move commit beyond what it covers.
     */
    a.nentries = 2;
    a.commit = 2;
    CHECK(hx_raft_step(r, 2, &a) == 0);
    hx_raft_take_msgs(r, &out, &n);
    CHECK(n == 1 && out[0].granted && out[0].match == 2);
    CHECK(hx_raft_last_index(r) == 3 && hx_raft_commit_index(r) == 2);

    /* A gap in our log is refused with a hint. */
    hx_raft_msg gap = a;
    gap.last_index = 9;
    gap.last_term = 2;
    gap.nentries = 0;
    CHECK(hx_raft_step(r, 3, &gap) == 0);
    hx_raft_take_msgs(r, &out, &n);
    CHECK(n == 1 && !out[0].granted && out[0].match == 4);

    /* A new leader overwrites the uncommitted index 3. */
    hx_raft_entry x = {.term = 3, .len = 1, .data = d[0]};
    hx_raft_msg b = a;
    b.term = 3;
    b.last_index = 2;
    b.last_term = 2;
    b.entries = &x;
    b.nentries = 1;
    b.commit = 3;
    CHECK(hx_raft_step(r, 4, &b) == 0);
    hx_raft_take_msgs(r, &out, &n);
    CHECK(n == 1 && out[0].granted && out[0].match == 3);
    hx_raft_entry got;
    CHECK(hx_raft_entry_at(r, 3, &got) && got.term == 3 && hx_raft_commit_index(r) == 3);

    /* An append that would rewrite a committed entry is a protocol violation: fail-stop. */
    hx_raft_msg bad = a;
    bad.term = 4;
    bad.last_index = 0;
    bad.last_term = 0;
    hx_raft_entry y = {.term = 4, .len = 1, .data = d[1]};
    bad.entries = &y;
    bad.nentries = 1;
    bad.commit = 0;
    CHECK(hx_raft_step(r, 5, &bad) == -1 && hx_raft_failed(r));
    hx_raft_destroy(r);
    store_free(&st);
}

/* Raft 5.4.2 / Figure 8: replicas of an OLD-term entry must not make it committed by counting. */
static bool leader_commit_rule_body(hx_raft *r)
{
    /* Win an election at term 4: first the PreVote round, then the real one. */
    int64_t now = 1000;
    REQ(hx_raft_tick(r, now) == 0 && hx_raft_get_role(r) == HX_RAFT_PRECANDIDATE);
    for (uint32_t p = 1; p <= 2; p++) {
        hx_raft_msg pv;
        memset(&pv, 0, sizeof pv);
        pv.type = HX_RAFT_MSG_PREVOTE_RESP;
        pv.from = p;
        pv.to = 0;
        pv.term = 4;
        pv.granted = true;
        REQ(hx_raft_step(r, now, &pv) == 0);
    }
    REQ(hx_raft_get_role(r) == HX_RAFT_CANDIDATE && hx_raft_term(r) == 4);
    for (uint32_t p = 1; p <= 2; p++) {
        hx_raft_msg v;
        memset(&v, 0, sizeof v);
        v.type = HX_RAFT_MSG_VOTE_RESP;
        v.from = p;
        v.to = 0;
        v.term = 4;
        v.granted = true;
        REQ(hx_raft_step(r, now, &v) == 0);
    }
    REQ(hx_raft_get_role(r) == HX_RAFT_LEADER);
    REQ(hx_raft_last_index(r) == 3); /* the leader's no-op for term 4 */
    hx_raft_entry e3;
    REQ(hx_raft_entry_at(r, 3, &e3) && e3.term == 4 && e3.len == 0);

    /* Two followers acknowledge only index 2 (term 2): a majority holds it, but it must NOT commit.
     */
    for (uint32_t p = 1; p <= 2; p++) {
        hx_raft_msg a;
        memset(&a, 0, sizeof a);
        a.type = HX_RAFT_MSG_APPEND_RESP;
        a.from = p;
        a.to = 0;
        a.term = 4;
        a.granted = true;
        a.match = 2;
        REQ(hx_raft_step(r, now, &a) == 0);
    }
    REQ(hx_raft_commit_index(r) == 0);

    /* Once a majority holds a term-4 entry the rule applies, and everything before it commits too.
     */
    for (uint32_t p = 1; p <= 2; p++) {
        hx_raft_msg a;
        memset(&a, 0, sizeof a);
        a.type = HX_RAFT_MSG_APPEND_RESP;
        a.from = p;
        a.to = 0;
        a.term = 4;
        a.granted = true;
        a.match = 3;
        REQ(hx_raft_step(r, now, &a) == 0);
        REQ(hx_raft_commit_index(r) == (p == 1 ? 0U : 3U));
    }
    return true;
}

static void test_leader_commits_only_current_term(void)
{
    store st;
    store_init(&st);
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, 0, 5);
    store_cfg(&cfg, &st);
    const uint8_t d[1] = {7};
    const hx_raft_entry ents[2] = {{.term = 1, .len = 1, .data = d},
                                   {.term = 2, .len = 1, .data = d}};
    const hx_raft_restore rs = {
        .term = 3, .voted_for = HX_RAFT_NONE, .entries = ents, .nentries = 2};
    /* The simulated disk must hold what the node "remembers", or its later appends look like gaps.
     */
    if (st_save(&st, 3, HX_RAFT_NONE) != 0 || st_append(&st, 1, ents, 2) != 0 ||
        st_sync(&st) != 0) {
        failures++;
        store_free(&st);
        return;
    }
    hx_raft *r = hx_raft_create(&cfg, &rs, 0);
    CHECK(r != NULL);
    if (!leader_commit_rule_body(r)) {
        failures++;
    }
    hx_raft_destroy(r);
    store_free(&st);
}

static void test_config_validation(void)
{
    store st;
    store_init(&st);
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, 0, 3);
    store_cfg(&cfg, &st);
    hx_raft_cfg bad = cfg;
    bad.id = 3;
    CHECK(hx_raft_create(&bad, NULL, 0) == NULL);
    bad = cfg;
    bad.nnodes = 33;
    CHECK(hx_raft_create(&bad, NULL, 0) == NULL);
    bad = cfg;
    bad.heartbeat_ms = bad.election_min_ms;
    CHECK(hx_raft_create(&bad, NULL, 0) == NULL);
    bad = cfg;
    bad.election_max_ms = bad.election_min_ms;
    CHECK(hx_raft_create(&bad, NULL, 0) == NULL);
    bad = cfg;
    bad.storage.sync = NULL;
    CHECK(hx_raft_create(&bad, NULL, 0) == NULL);
    /* A restored log whose terms go backwards, or exceed the saved term, is rejected. */
    const uint8_t d[1] = {1};
    const hx_raft_entry ents[2] = {{.term = 3, .len = 1, .data = d},
                                   {.term = 2, .len = 1, .data = d}};
    hx_raft_restore rs = {.term = 3, .voted_for = HX_RAFT_NONE, .entries = ents, .nentries = 2};
    CHECK(hx_raft_create(&cfg, &rs, 0) == NULL);
    rs.term = 1;
    rs.nentries = 1;
    CHECK(hx_raft_create(&cfg, &rs, 0) == NULL);
    store_free(&st);
}

static void test_msg_codec(void)
{
    const uint8_t pay[3] = {1, 2, 3};
    const hx_raft_entry ents[2] = {{.term = 4, .len = 3, .data = pay},
                                   {.term = 5, .len = 0, .data = pay}};
    hx_raft_msg m;
    memset(&m, 0, sizeof m);
    m.type = HX_RAFT_MSG_APPEND;
    m.from = 2;
    m.to = 1;
    m.term = 9;
    m.last_index = 7;
    m.last_term = 4;
    m.commit = 6;
    m.entries = ents;
    m.nentries = 2;
    hx_buf b;
    hx_buf_init(&b);
    CHECK(hx_raft_msg_encode(&m, &b) == 0);
    hx_raft_entry scratch[4];
    hx_raft_msg d;
    CHECK(hx_raft_msg_decode(b.data, b.len, scratch, 4, &d) == 0);
    CHECK(d.type == HX_RAFT_MSG_APPEND && d.from == 2 && d.to == 1 && d.term == 9 &&
          d.last_index == 7 && d.last_term == 4 && d.commit == 6 && d.nentries == 2);
    CHECK(d.entries[0].term == 4 && d.entries[0].len == 3 &&
          memcmp(d.entries[0].data, pay, 3) == 0);
    CHECK(d.entries[1].term == 5 && d.entries[1].len == 0);
    /* Every strict prefix is malformed; so is trailing data, too many entries and an unknown type.
     */
    for (size_t cut = 0; cut < b.len; cut++) {
        CHECK(hx_raft_msg_decode(b.data, cut, scratch, 4, &d) != 0);
    }
    CHECK(hx_buf_put_u8(&b, 0) == 0 && hx_raft_msg_decode(b.data, b.len, scratch, 4, &d) != 0);
    b.len--;
    CHECK(hx_raft_msg_decode(b.data, b.len, scratch, 1, &d) != 0);
    b.data[0] = 0;
    CHECK(hx_raft_msg_decode(b.data, b.len, scratch, 4, &d) != 0);
    b.data[0] = 99;
    CHECK(hx_raft_msg_decode(b.data, b.len, scratch, 4, &d) != 0);
    hx_buf_free(&b);

    /* SNAPSHOT: round trip, and every strict prefix is rejected. */
    hx_buf sb;
    hx_buf_init(&sb);
    memset(&m, 0, sizeof m);
    m.type = HX_RAFT_MSG_SNAPSHOT;
    m.from = 0;
    m.to = 2;
    m.term = 5;
    m.last_index = 40;
    m.last_term = 4;
    m.snap = pay;
    m.snap_len = 3;
    CHECK(hx_raft_msg_encode(&m, &sb) == 0);
    CHECK(hx_raft_msg_decode(sb.data, sb.len, scratch, 4, &d) == 0);
    CHECK(d.type == HX_RAFT_MSG_SNAPSHOT && d.last_index == 40 && d.last_term == 4 &&
          d.snap_len == 3 && memcmp(d.snap, pay, 3) == 0);
    for (size_t cut = 0; cut < sb.len; cut++) {
        CHECK(hx_raft_msg_decode(sb.data, cut, scratch, 4, &d) != 0);
    }
    sb.data[sb.len - 4U] = 0xFF; /* length field claims more data than the message carries */
    CHECK(hx_raft_msg_decode(sb.data, sb.len, scratch, 4, &d) != 0);
    hx_buf_free(&sb);

    const hx_raft_msg_type simple[] = {HX_RAFT_MSG_PREVOTE, HX_RAFT_MSG_PREVOTE_RESP,
                                       HX_RAFT_MSG_VOTE, HX_RAFT_MSG_VOTE_RESP,
                                       HX_RAFT_MSG_APPEND_RESP};
    for (size_t i = 0; i < sizeof simple / sizeof simple[0]; i++) {
        hx_raft_msg s;
        memset(&s, 0, sizeof s);
        s.type = simple[i];
        s.from = 1;
        s.to = 0;
        s.term = 3;
        s.last_index = simple[i] == HX_RAFT_MSG_PREVOTE || simple[i] == HX_RAFT_MSG_VOTE ? 8 : 0;
        s.last_term = s.last_index != 0 ? 2 : 0;
        s.granted = simple[i] == HX_RAFT_MSG_PREVOTE_RESP || simple[i] == HX_RAFT_MSG_APPEND_RESP;
        s.match = simple[i] == HX_RAFT_MSG_APPEND_RESP ? 11 : 0;
        hx_buf_init(&b);
        CHECK(hx_raft_msg_encode(&s, &b) == 0);
        CHECK(hx_raft_msg_decode(b.data, b.len, scratch, 4, &d) == 0);
        CHECK(d.type == s.type && d.term == 3 && d.last_index == s.last_index &&
              d.granted == s.granted && d.match == s.match);
        hx_buf_free(&b);
    }
}

/* ------------------------------------------------- random fault injection */

typedef struct {
    uint64_t scenarios;
    uint64_t commits;
    uint64_t crashes;
    uint64_t partitions;
    uint64_t leaders;
    uint64_t snaps;
    uint64_t installs;
} totals;

static bool run_scenario(uint32_t n, uint64_t seed, bool wal, totals *tot)
{
    sim s;
    if (!sim_init_flavor_wal(&s, n, seed, (unsigned)(seed % 4U) | (wal ? 4U : 0U), wal)) {
        (void)fprintf(stderr, "FAIL n=%u seed=%llu: %s\n", n, (unsigned long long)seed,
                      s.bad ? s.why : "init");
        sim_free(&s);
        return false;
    }
    s.loss_pm = 40 + (uint32_t)(srand_next(&s) % 80U) + ((seed % 4U) == 3U ? 100U : 0U);
    s.dup_pm = 30;
    sim_run(&s, 9000, true);

    /* Recovery: with the network healed and every node back, the cluster must elect a leader and
     * commit. */
    sim_heal(&s);
    for (uint32_t i = 0; i < n; i++) {
        if (!s.nodes[i].up) {
            s.nodes[i].restart_at = s.now;
        }
    }
    sim_run(&s, 4000, false);
    uint32_t l = sim_leader(&s);
    if (!s.bad && l == HX_RAFT_NONE) {
        sim_fail(&s, "LIVENESS: no leader 4 s after the network healed");
    }
    if (!s.bad) {
        if (sim_propose(&s, l, 8) != HX_RAFT_OK) {
            sim_fail(&s, "LIVENESS: the leader refused a proposal");
        }
        const uint64_t marker = hx_raft_last_index(s.nodes[l].r);
        sim_run(&s, 2500, false);
        for (uint32_t i = 0; i < n && !s.bad; i++) {
            if (hx_raft_commit_index(s.nodes[i].r) < marker) {
                sim_fail(&s, "LIVENESS: node %u did not commit the marker entry (%llu < %llu)", i,
                         (unsigned long long)hx_raft_commit_index(s.nodes[i].r),
                         (unsigned long long)marker);
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
    tot->leaders += s.stat_leader_changes;
    tot->snaps += s.stat_snaps;
    tot->installs += s.stat_installs;
    sim_free(&s);
    return ok;
}

static bool snapshot_preconditions_body(sim *s)
{
    sim_run(s, 1500, false);
    const uint32_t l = sim_leader(s);
    REQ(l != HX_RAFT_NONE);
    for (int k = 0; k < 10; k++) {
        REQ(sim_propose(s, l, 8) == HX_RAFT_OK);
    }
    sim_run(s, 300, false);
    REQ(!s->bad);
    hx_raft *r = s->nodes[l].r;
    const uint64_t c = hx_raft_commit_index(r);
    REQ(c >= 11);
    REQ(sim_propose(s, l, 8) == HX_RAFT_OK); /* one entry that is not committed yet */
    const uint64_t last = hx_raft_last_index(r);
    uint8_t img[16] = {0};
    REQ(hx_raft_snapshot(r, s->now, last, img, sizeof img) != 0); /* beyond commit: refused */
    REQ(hx_raft_snapshot_index(r) == 0 && hx_raft_last_index(r) == last);
    REQ(hx_raft_snapshot(r, s->now, 0, img, sizeof img) != 0);
    REQ(hx_raft_snapshot(r, s->now, c + 50U, img, sizeof img) != 0);
    REQ(!hx_raft_failed(r));
    hx_raft_entry e;
    REQ(hx_raft_snapshot(r, s->now, 5, img, sizeof img) == 0);
    REQ(hx_raft_snapshot_index(r) == 5 && !hx_raft_entry_at(r, 5, &e) &&
        hx_raft_entry_at(r, 6, &e));
    REQ(hx_raft_snapshot(r, s->now, 5, img, sizeof img) != 0); /* not moving forward */
    REQ(hx_raft_snapshot(r, s->now, 3, img, sizeof img) != 0); /* going back */
    REQ(hx_raft_last_index(r) == last);
    return true;
}

static void test_snapshot_preconditions(void)
{
    sim s;
    /* flavor 4 = snapshots, but with a threshold the scripted test never reaches by itself */
    if (!sim_init_flavor_wal(&s, 3, 7, 4U, true)) {
        failures++;
        sim_free(&s);
        return;
    }
    s.snap_every = 1000000U;
    if (!snapshot_preconditions_body(&s) || s.bad) {
        (void)fprintf(stderr, "FAIL snapshot preconditions: %s\n", s.bad ? s.why : "check");
        failures++;
    }
    sim_free(&s);
}

static void test_random_fault_injection(void)
{
    const char *env = getenv("HX_RAFT_SEEDS");
    const uint64_t seeds3 = env != NULL ? strtoull(env, NULL, 10) : 80;
    const uint64_t seeds5 = seeds3 / 2U;
    totals tot;
    memset(&tot, 0, sizeof tot);
    for (uint64_t k = 1; k <= seeds3; k++) {
        if (!run_scenario(3, k, false, &tot)) {
            failures++;
            return;
        }
    }
    for (uint64_t k = 1; k <= seeds5; k++) {
        if (!run_scenario(5, 1000U + k, false, &tot)) {
            failures++;
            return;
        }
    }
    for (uint64_t k = 1; k <= 5; k++) {
        if (!run_scenario(1, 5000U + k, false, &tot)) {
            failures++;
            return;
        }
    }
    /* The same adversary against the real on-disk WAL: crashes cut the file back to its synced
     * size. */
    const uint64_t walseeds = seeds3 / 4U > 12U ? seeds3 / 4U : 12U;
    for (uint64_t k = 1; k <= walseeds; k++) {
        if (!run_scenario(3, 7000U + k, true, &tot)) {
            failures++;
            return;
        }
    }
    for (uint64_t k = 1; k <= 4; k++) {
        if (!run_scenario(5, 8000U + k, true, &tot)) {
            failures++;
            return;
        }
    }
    (void)printf("raft simulation: %llu scenarios, %llu entries committed, %llu leader elections, "
                 "%llu crashes, "
                 "%llu partitions, %llu snapshots taken, %llu installed on lagging nodes, no "
                 "invariant violated\n",
                 (unsigned long long)tot.scenarios, (unsigned long long)tot.commits,
                 (unsigned long long)tot.leaders, (unsigned long long)tot.crashes,
                 (unsigned long long)tot.partitions, (unsigned long long)tot.snaps,
                 (unsigned long long)tot.installs);
    if (tot.commits < tot.scenarios * 20U || tot.crashes == 0 || tot.partitions == 0 ||
        tot.snaps == 0 || tot.installs == 0) {
        (void)fputs("FAIL: the simulation did not exercise enough faults to be meaningful\n",
                    stderr);
        failures++;
    }
}

int main(void)
{
    if (getenv("HX_RAFT_ONLY_SIM") !=
        NULL) { /* used by the mutation checks: prove the simulator alone catches bugs */
        test_random_fault_injection();
        return failures != 0 ? 1 : 0;
    }
    test_msg_codec();
    test_config_validation();
    test_vote_is_durable_and_log_must_be_up_to_date();
    test_storage_failure_is_fail_stop();
    test_follower_append_semantics();
    test_leader_commits_only_current_term();
    test_single_node_cluster();
    test_no_faults_elects_and_replicates();
    test_prevote_prevents_term_inflation();
    test_isolated_leader_steps_down_and_loses_uncommitted();
    test_snapshot_preconditions();
    test_random_fault_injection();
    if (failures != 0) {
        (void)fprintf(stderr, "%d raft test(s) FAILED\n", failures);
        return 1;
    }
    (void)puts("all raft tests passed");
    return 0;
}
