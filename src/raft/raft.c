#include "raft/raft.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    uint64_t term;
    uint32_t len;
    uint8_t *data; /* owned */
} lentry;

struct hx_raft {
    hx_raft_cfg cfg;
    hx_raft_role role;
    uint64_t term;
    uint32_t voted_for;
    uint32_t leader;

    lentry *log; /* entry with index i lives at log[i - base - 1]; entries <= base are compacted */
    size_t nlog;
    size_t caplog;
    uint64_t commit;

    uint64_t base;      /* last index folded into the snapshot (0 = none) */
    uint64_t base_term; /* term of that entry */
    uint8_t *snap;      /* the snapshot image (owned) */
    uint32_t snap_len;
    bool snap_installed; /* a leader's snapshot arrived: the host has not collected it yet */

    int64_t now;
    int64_t election_deadline;
    int64_t heartbeat_deadline;
    int64_t quorum_deadline;
    int64_t last_leader_contact;
    bool leader_contact_valid;

    uint32_t votes; /* bitmask of nodes that granted (pre)vote */
    uint64_t next[HX_RAFT_MAX_NODES];
    uint64_t match[HX_RAFT_MAX_NODES];
    bool heard[HX_RAFT_MAX_NODES]; /* responded since the last quorum check */

    hx_raft_msg *out;
    size_t *out_eoff; /* per message: offset of its entries inside `arena` */
    size_t nout;
    size_t capout;
    hx_raft_entry *arena;
    size_t narena;
    size_t caparena;

    bool dirty;
    bool failed;
    uint64_t rng;
};

/* ------------------------------------------------------------- utilities */

void hx_raft_cfg_default(hx_raft_cfg *cfg, uint32_t id, uint32_t nnodes)
{
    memset(cfg, 0, sizeof *cfg);
    cfg->id = id;
    cfg->nnodes = nnodes;
    cfg->election_min_ms = 150;
    cfg->election_max_ms = 300;
    cfg->heartbeat_ms = 50;
    cfg->max_batch_entries = 64;
    cfg->max_batch_bytes = 1U << 20;
    cfg->max_entry_bytes = 1U << 20;
    cfg->seed = (uint64_t)id + 1U;
}

static uint64_t rng_next(hx_raft *r)
{
    uint64_t x = r->rng; /* xorshift64* */
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    r->rng = x;
    return x * 0x2545F4914F6CDD1DULL;
}

static uint32_t majority(const hx_raft *r)
{
    return r->cfg.nnodes / 2U + 1U;
}

static uint32_t popcount32(uint32_t v)
{
    uint32_t n = 0;
    while (v != 0) {
        v &= v - 1U;
        n++;
    }
    return n;
}

static uint64_t last_index(const hx_raft *r)
{
    return r->base + r->nlog;
}

static uint64_t last_term(const hx_raft *r)
{
    return r->nlog == 0 ? r->base_term : r->log[r->nlog - 1U].term;
}

/* The entry at a retained index: base < idx <= last_index. */
static lentry *entry(const hx_raft *r, uint64_t idx)
{
    return &r->log[idx - r->base - 1U];
}

/* False if idx is compacted away or beyond the log. Index `base` is the snapshot point. */
static bool term_at(const hx_raft *r, uint64_t idx, uint64_t *term)
{
    if (idx == r->base) {
        *term = r->base_term;
        return true;
    }
    if (idx < r->base || idx > last_index(r)) {
        return false;
    }
    *term = entry(r, idx)->term;
    return true;
}

/* Raft 5.4.1: is a candidate's log at least as up to date as ours? */
static bool log_up_to_date(const hx_raft *r, uint64_t cand_term, uint64_t cand_index)
{
    const uint64_t lt = last_term(r);
    return cand_term > lt || (cand_term == lt && cand_index >= last_index(r));
}

/* --------------------------------------------------------------- storage */

static void st_hard(hx_raft *r)
{
    if (r->cfg.storage.save_hard_state(r->cfg.storage.ctx, r->term, r->voted_for) != 0) {
        r->failed = true;
    }
    r->dirty = true;
}

/* Persist the log from `first` to the end (and drop anything stored beyond it). */
static void st_append(hx_raft *r, uint64_t first)
{
    const size_t n = first <= last_index(r) ? (size_t)(last_index(r) - first + 1U) : 0;
    hx_raft_entry *tmp = n != 0 ? malloc(n * sizeof *tmp) : NULL;
    if (n != 0 && tmp == NULL) {
        r->failed = true;
        return;
    }
    for (size_t i = 0; i < n; i++) {
        const lentry *e = entry(r, first + i);
        tmp[i].term = e->term;
        tmp[i].len = e->len;
        tmp[i].data = e->data;
    }
    if (r->cfg.storage.append(r->cfg.storage.ctx, first, tmp, n) != 0) {
        r->failed = true;
    }
    free(tmp);
    r->dirty = true;
}

/* ----------------------------------------------------------- log in RAM */

static int log_push(hx_raft *r, uint64_t term, const uint8_t *data, uint32_t len)
{
    if (r->nlog == r->caplog) {
        const size_t ncap = r->caplog == 0 ? 64U : r->caplog * 2U;
        lentry *grown = realloc(r->log, ncap * sizeof *grown);
        if (grown == NULL) {
            return -1;
        }
        r->log = grown;
        r->caplog = ncap;
    }
    uint8_t *copy = malloc(len != 0 ? len : 1U);
    if (copy == NULL) {
        return -1;
    }
    if (len != 0) {
        memcpy(copy, data, len);
    }
    r->log[r->nlog].term = term;
    r->log[r->nlog].len = len;
    r->log[r->nlog].data = copy;
    r->nlog++;
    return 0;
}

static void log_truncate(hx_raft *r, size_t keep)
{
    while (r->nlog > keep) {
        free(r->log[--r->nlog].data);
    }
}

/* Forget every entry up to and including `idx` (which becomes the snapshot point). */
static void drop_prefix(hx_raft *r, uint64_t idx, uint64_t term)
{
    size_t k = idx > last_index(r) ? r->nlog : (size_t)(idx - r->base);
    for (size_t i = 0; i < k; i++) {
        free(r->log[i].data);
    }
    memmove(r->log, r->log + k, (r->nlog - k) * sizeof *r->log);
    r->nlog -= k;
    r->base = idx;
    r->base_term = term;
}

/* Replace the stored snapshot image with a copy of data. False on out of memory. */
static bool set_snap(hx_raft *r, const uint8_t *data, uint32_t len)
{
    uint8_t *copy = malloc(len != 0 ? len : 1U);
    if (copy == NULL) {
        return false;
    }
    if (len != 0) {
        memcpy(copy, data, len);
    }
    free(r->snap);
    r->snap = copy;
    r->snap_len = len;
    return true;
}

/* ---------------------------------------------------------------- outbox */

/* Returns a fresh message slot, or NULL (and marks the instance failed) when out of memory. */
static hx_raft_msg *out_new(hx_raft *r, uint32_t to, hx_raft_msg_type type)
{
    if (r->nout == r->capout) {
        const size_t ncap = r->capout == 0 ? 16U : r->capout * 2U;
        hx_raft_msg *m = realloc(r->out, ncap * sizeof *m);
        if (m == NULL) {
            r->failed = true;
            return NULL;
        }
        r->out = m;
        size_t *o = realloc(r->out_eoff, ncap * sizeof *o);
        if (o == NULL) {
            r->failed = true;
            return NULL;
        }
        r->out_eoff = o;
        r->capout = ncap;
    }
    hx_raft_msg *m = &r->out[r->nout];
    memset(m, 0, sizeof *m);
    r->out_eoff[r->nout] = 0;
    r->nout++;
    m->type = type;
    m->from = r->cfg.id;
    m->to = to;
    m->term = r->term;
    return m;
}

static void send_vote_requests(hx_raft *r, hx_raft_msg_type type, uint64_t term)
{
    for (uint32_t p = 0; p < r->cfg.nnodes; p++) {
        if (p == r->cfg.id) {
            continue;
        }
        hx_raft_msg *m = out_new(r, p, type);
        if (m == NULL) {
            return;
        }
        m->term = term;
        m->last_index = last_index(r);
        m->last_term = last_term(r);
    }
}

static void send_snapshot(hx_raft *r, uint32_t p)
{
    hx_raft_msg *m = out_new(r, p, HX_RAFT_MSG_SNAPSHOT);
    if (m != NULL) {
        m->last_index = r->base;
        m->last_term = r->base_term;
        m->snap = r->snap;
        m->snap_len = r->snap_len;
    }
}

static void send_append(hx_raft *r, uint32_t p)
{
    const uint64_t prev = r->next[p] - 1U;
    uint64_t prev_term = 0;
    if (prev < r->base) { /* the entries it needs are gone: ship the snapshot instead */
        send_snapshot(r, p);
        return;
    }
    if (!term_at(r, prev, &prev_term)) {
        return; /* cannot happen: next <= last + 1 */
    }
    size_t n = 0;
    size_t bytes = 0;
    const uint64_t first = r->next[p];
    while (first + n <= last_index(r) && n < r->cfg.max_batch_entries) {
        const size_t len = entry(r, first + n)->len;
        if (n > 0 && bytes + len > r->cfg.max_batch_bytes) {
            break;
        }
        bytes += len;
        n++;
    }
    if (r->narena + n > r->caparena) {
        size_t ncap = r->caparena == 0 ? 64U : r->caparena;
        while (ncap < r->narena + n) {
            ncap *= 2U;
        }
        hx_raft_entry *a = realloc(r->arena, ncap * sizeof *a);
        if (a == NULL) {
            r->failed = true;
            return;
        }
        r->arena = a;
        r->caparena = ncap;
    }
    hx_raft_msg *m = out_new(r, p, HX_RAFT_MSG_APPEND);
    if (m == NULL) {
        return;
    }
    m->last_index = prev;
    m->last_term = prev_term;
    m->commit = r->commit;
    m->nentries = (uint32_t)n;
    r->out_eoff[r->nout - 1U] = r->narena;
    for (size_t i = 0; i < n; i++) {
        const lentry *e = entry(r, first + i);
        r->arena[r->narena + i].term = e->term;
        r->arena[r->narena + i].len = e->len;
        r->arena[r->narena + i].data = e->data;
    }
    r->narena += n;
}

static void broadcast_append(hx_raft *r)
{
    for (uint32_t p = 0; p < r->cfg.nnodes; p++) {
        if (p != r->cfg.id) {
            send_append(r, p);
        }
    }
}

/* ------------------------------------------------------------ transitions */

static void reset_election(hx_raft *r)
{
    const uint64_t span = (uint64_t)r->cfg.election_max_ms - r->cfg.election_min_ms;
    r->election_deadline = r->now + (int64_t)r->cfg.election_min_ms + (int64_t)(rng_next(r) % span);
}

static void become_follower(hx_raft *r, uint64_t term, uint32_t leader)
{
    if (term > r->term) {
        r->term = term;
        r->voted_for = HX_RAFT_NONE;
        st_hard(r);
    }
    r->role = HX_RAFT_FOLLOWER;
    r->leader = leader;
    r->votes = 0;
    reset_election(r);
}

static void advance_commit(hx_raft *r)
{
    uint64_t m[HX_RAFT_MAX_NODES];
    const uint32_t n = r->cfg.nnodes;
    for (uint32_t i = 0; i < n; i++) {
        m[i] = i == r->cfg.id ? last_index(r) : r->match[i];
    }
    for (uint32_t i = 1; i < n; i++) { /* insertion sort, descending; n <= 32 */
        const uint64_t v = m[i];
        uint32_t j = i;
        while (j > 0 && m[j - 1] < v) {
            m[j] = m[j - 1];
            j--;
        }
        m[j] = v;
    }
    const uint64_t q = m[majority(r) - 1U];
    uint64_t t = 0;
    /* Only entries of the current term are committed by counting replicas (Raft 5.4.2). */
    if (q > r->commit && term_at(r, q, &t) && t == r->term) {
        r->commit = q;
    }
}

static void become_leader(hx_raft *r)
{
    r->role = HX_RAFT_LEADER;
    r->leader = r->cfg.id;
    r->votes = 0;
    for (uint32_t p = 0; p < r->cfg.nnodes; p++) {
        r->next[p] = last_index(r) + 1U;
        r->match[p] = 0;
        r->heard[p] = false;
    }
    /* A no-op in the new term lets the leader commit entries of earlier terms. */
    if (log_push(r, r->term, NULL, 0) != 0) {
        r->failed = true;
        return;
    }
    st_append(r, last_index(r));
    advance_commit(r);
    broadcast_append(r);
    r->heartbeat_deadline = r->now + (int64_t)r->cfg.heartbeat_ms;
    r->quorum_deadline = r->now + (int64_t)r->cfg.election_max_ms;
}

static void start_election(hx_raft *r)
{
    r->term++;
    r->voted_for = r->cfg.id;
    st_hard(r);
    r->role = HX_RAFT_CANDIDATE;
    r->leader = HX_RAFT_NONE;
    r->votes = 1U << r->cfg.id;
    reset_election(r);
    if (popcount32(r->votes) >= majority(r)) {
        become_leader(r);
        return;
    }
    send_vote_requests(r, HX_RAFT_MSG_VOTE, r->term);
}

/* PreVote (Raft thesis 9.6): ask whether an election could succeed WITHOUT touching any term. */
static void start_prevote(hx_raft *r)
{
    r->role = HX_RAFT_PRECANDIDATE;
    r->leader = HX_RAFT_NONE;
    r->votes = 1U << r->cfg.id;
    reset_election(r);
    if (popcount32(r->votes) >= majority(r)) {
        start_election(r);
        return;
    }
    send_vote_requests(r, HX_RAFT_MSG_PREVOTE, r->term + 1U);
}

/* -------------------------------------------------------------- lifecycle */

hx_raft *hx_raft_create(const hx_raft_cfg *cfg, const hx_raft_restore *restore, int64_t now_ms)
{
    if (cfg == NULL || cfg->nnodes == 0 || cfg->nnodes > HX_RAFT_MAX_NODES ||
        cfg->id >= cfg->nnodes || cfg->election_min_ms == 0 ||
        cfg->election_max_ms <= cfg->election_min_ms || cfg->heartbeat_ms == 0 ||
        cfg->heartbeat_ms >= cfg->election_min_ms || cfg->max_batch_entries == 0 ||
        cfg->max_batch_bytes == 0 || cfg->max_entry_bytes == 0 ||
        cfg->storage.save_hard_state == NULL || cfg->storage.append == NULL ||
        cfg->storage.sync == NULL) {
        return NULL;
    }
    hx_raft *r = calloc(1, sizeof *r);
    if (r == NULL) {
        return NULL;
    }
    r->cfg = *cfg;
    r->role = HX_RAFT_FOLLOWER;
    r->voted_for = HX_RAFT_NONE;
    r->leader = HX_RAFT_NONE;
    r->now = now_ms;
    r->rng = (cfg->seed ^ ((uint64_t)cfg->id * 0x9E3779B97F4A7C15ULL)) | 1U;
    if (restore != NULL) {
        r->term = restore->term;
        r->voted_for = restore->voted_for;
        if (restore->snap_index != 0) {
            if (restore->snap_term > r->term ||
                !set_snap(r, restore->snap_data, restore->snap_len)) {
                hx_raft_destroy(r);
                return NULL;
            }
            r->base = restore->snap_index;
            r->base_term = restore->snap_term;
            r->commit = restore->snap_index; /* a snapshot only ever covers committed entries */
        }
        uint64_t prev_term = r->base_term;
        for (size_t i = 0; i < restore->nentries; i++) {
            const hx_raft_entry *e = &restore->entries[i];
            if (e->term < prev_term || e->term > r->term ||
                log_push(r, e->term, e->data, e->len) != 0) {
                hx_raft_destroy(r);
                return NULL;
            }
            prev_term = e->term;
        }
    }
    reset_election(r);
    return r;
}

void hx_raft_destroy(hx_raft *r)
{
    if (r == NULL) {
        return;
    }
    log_truncate(r, 0);
    free(r->log);
    free(r->snap);
    free(r->out);
    free(r->out_eoff);
    free(r->arena);
    free(r);
}

/*
 * Common prologue of every public mutating call. The outbox is rebuilt by every call, so a host
 * that did not collect it loses those messages (Raft tolerates loss) but can never see dangling
 * pointers.
 */
static int begin(hx_raft *r, int64_t now_ms)
{
    r->nout = 0;
    r->narena = 0;
    if (r->failed) {
        return -1;
    }
    if (now_ms > r->now) {
        r->now = now_ms;
    }
    return 0;
}

/* Common epilogue: make state durable BEFORE any message of this call can be handed out. */
static int finish(hx_raft *r)
{
    if (!r->failed && r->dirty && r->cfg.storage.sync(r->cfg.storage.ctx) != 0) {
        r->failed = true;
    }
    r->dirty = false;
    if (r->failed) {
        r->nout = 0;
        r->narena = 0;
        return -1;
    }
    return 0;
}

int hx_raft_tick(hx_raft *r, int64_t now_ms)
{
    if (begin(r, now_ms) != 0) {
        return -1;
    }
    if (r->role == HX_RAFT_LEADER) {
        if (r->now >= r->heartbeat_deadline) {
            broadcast_append(r);
            r->heartbeat_deadline = r->now + (int64_t)r->cfg.heartbeat_ms;
        }
        if (r->now >= r->quorum_deadline) {
            /* CheckQuorum: a leader cut off from the majority must stop acting as one. */
            uint32_t alive = 1;
            for (uint32_t p = 0; p < r->cfg.nnodes; p++) {
                if (p != r->cfg.id && r->heard[p]) {
                    alive++;
                }
                r->heard[p] = false;
            }
            if (alive < majority(r)) {
                become_follower(r, r->term, HX_RAFT_NONE);
            } else {
                r->quorum_deadline = r->now + (int64_t)r->cfg.election_max_ms;
            }
        }
    } else if (r->now >= r->election_deadline) {
        start_prevote(r);
    }
    return finish(r);
}

hx_raft_status hx_raft_propose(hx_raft *r, int64_t now_ms, const void *data, uint32_t len,
                               uint64_t *index, uint64_t *term)
{
    if (begin(r, now_ms) != 0) {
        return HX_RAFT_FAILED;
    }
    if (r->role != HX_RAFT_LEADER) {
        return HX_RAFT_NOT_LEADER;
    }
    if (len > r->cfg.max_entry_bytes) {
        return HX_RAFT_TOO_LARGE;
    }
    if (log_push(r, r->term, data, len) != 0) {
        return HX_RAFT_NOMEM;
    }
    st_append(r, last_index(r));
    advance_commit(r);
    broadcast_append(r);
    if (finish(r) != 0) {
        return HX_RAFT_FAILED;
    }
    if (index != NULL) {
        *index = last_index(r);
    }
    if (term != NULL) {
        *term = r->term;
    }
    return HX_RAFT_OK;
}

void hx_raft_take_msgs(hx_raft *r, const hx_raft_msg **msgs, size_t *n)
{
    for (size_t i = 0; i < r->nout; i++) {
        if (r->out[i].type == HX_RAFT_MSG_APPEND) {
            r->out[i].entries = r->arena + r->out_eoff[i];
        }
    }
    *msgs = r->out;
    *n = r->nout;
}

/* --------------------------------------------------------------- handlers */

static void on_prevote(hx_raft *r, const hx_raft_msg *m)
{
    const bool leader_recent = r->leader_contact_valid &&
                               r->now - r->last_leader_contact < (int64_t)r->cfg.election_min_ms;
    const bool grant = m->term > r->term && r->role != HX_RAFT_LEADER && !leader_recent &&
                       log_up_to_date(r, m->last_term, m->last_index);
    hx_raft_msg *o = out_new(r, m->from, HX_RAFT_MSG_PREVOTE_RESP);
    if (o != NULL) {
        o->term = grant ? m->term : r->term;
        o->granted = grant;
    }
}

static void on_prevote_resp(hx_raft *r, const hx_raft_msg *m)
{
    if (r->role != HX_RAFT_PRECANDIDATE) {
        return;
    }
    if (m->granted) {
        if (m->term == r->term + 1U) {
            r->votes |= 1U << m->from;
            if (popcount32(r->votes) >= majority(r)) {
                start_election(r);
            }
        }
    } else if (m->term > r->term) {
        become_follower(r, m->term, HX_RAFT_NONE);
    }
}

static void on_vote(hx_raft *r, const hx_raft_msg *m)
{
    const bool grant = (r->voted_for == HX_RAFT_NONE || r->voted_for == m->from) &&
                       log_up_to_date(r, m->last_term, m->last_index);
    if (grant) {
        r->voted_for = m->from;
        st_hard(r);
        reset_election(r);
    }
    hx_raft_msg *o = out_new(r, m->from, HX_RAFT_MSG_VOTE_RESP);
    if (o != NULL) {
        o->granted = grant;
    }
}

static void on_vote_resp(hx_raft *r, const hx_raft_msg *m)
{
    if (r->role == HX_RAFT_CANDIDATE && m->term == r->term && m->granted) {
        r->votes |= 1U << m->from;
        if (popcount32(r->votes) >= majority(r)) {
            become_leader(r);
        }
    }
}

static void reply_append(hx_raft *r, uint32_t to, bool ok, uint64_t match)
{
    hx_raft_msg *o = out_new(r, to, HX_RAFT_MSG_APPEND_RESP);
    if (o != NULL) {
        o->granted = ok;
        o->match = match;
    }
}

static void follower_contact(hx_raft *r, const hx_raft_msg *m)
{
    if (r->role != HX_RAFT_FOLLOWER) {
        become_follower(r, r->term, m->from);
    }
    r->leader = m->from;
    r->last_leader_contact = r->now;
    r->leader_contact_valid = true;
    reset_election(r);
}

static void on_append(hx_raft *r, const hx_raft_msg *m)
{
    if (r->role == HX_RAFT_LEADER) {
        return; /* two leaders in one term would violate election safety; never happens */
    }
    follower_contact(r, m);

    uint64_t prev = m->last_index;
    uint64_t prev_term = m->last_term;
    const hx_raft_entry *ents = m->entries;
    uint32_t cnt = m->nentries;
    if (prev < r->base) {
        /* The batch starts inside our snapshot: those entries are committed and already folded in.
         */
        const uint64_t skip = r->base - prev;
        if (skip >= cnt) {
            reply_append(r, m->from, true, r->base);
            return;
        }
        ents += skip;
        cnt -= (uint32_t)skip;
        prev = r->base;
        prev_term = r->base_term;
    }

    uint64_t t = 0;
    if (prev > last_index(r)) {
        reply_append(r, m->from, false, last_index(r) + 1U);
        return;
    }
    if (!term_at(r, prev, &t) || t != prev_term) {
        /* Conflict: hint the first index of our conflicting term so the leader skips it at once. */
        uint64_t hint = prev;
        while (hint > r->base + 1U && entry(r, hint - 1U)->term == t) {
            hint--;
        }
        reply_append(r, m->from, false, hint);
        return;
    }

    uint64_t first_changed = 0;
    for (uint32_t i = 0; i < cnt; i++) {
        const uint64_t idx = prev + 1U + i;
        const hx_raft_entry *e = &ents[i];
        if (first_changed == 0 && idx <= last_index(r)) {
            if (entry(r, idx)->term == e->term) {
                continue; /* already have it (duplicate or reordered message) */
            }
            if (idx <= r->commit) {
                r->failed = true; /* would rewrite a committed entry: refuse to continue */
                return;
            }
            log_truncate(r, (size_t)(idx - 1U - r->base));
        }
        if (first_changed == 0) {
            first_changed = idx;
        }
        if (log_push(r, e->term, e->data, e->len) != 0) {
            r->failed = true;
            return;
        }
    }
    if (first_changed != 0) {
        st_append(r, first_changed);
    }
    const uint64_t match = prev + cnt;
    const uint64_t newc = m->commit < match ? m->commit : match;
    if (newc > r->commit) {
        r->commit = newc;
    }
    reply_append(r, m->from, true, match);
}

static void on_snapshot(hx_raft *r, const hx_raft_msg *m)
{
    if (r->role == HX_RAFT_LEADER) {
        return;
    }
    follower_contact(r, m);
    if (r->cfg.storage.save_snapshot == NULL) {
        return; /* snapshots disabled on this node: the leader keeps retrying, an operator must act
                 */
    }
    const uint64_t idx = m->last_index;
    if (idx <= r->commit) {
        reply_append(r, m->from, true, idx); /* we already hold this committed prefix */
        return;
    }
    uint64_t t = 0;
    const bool keep_suffix = term_at(r, idx, &t) && t == m->last_term;
    if (r->cfg.storage.save_snapshot(r->cfg.storage.ctx, idx, m->last_term, m->snap, m->snap_len) !=
        0) {
        r->failed = true;
        return;
    }
    r->dirty = true;
    if (!set_snap(r, m->snap, m->snap_len)) {
        r->failed = true;
        return;
    }
    if (keep_suffix) {
        drop_prefix(r, idx, m->last_term);
    } else {
        log_truncate(r, 0);
        r->base = idx;
        r->base_term = m->last_term;
    }
    r->commit = idx;
    r->snap_installed = true;
    reply_append(r, m->from, true, idx);
}

static void on_append_resp(hx_raft *r, const hx_raft_msg *m)
{
    if (r->role != HX_RAFT_LEADER || m->term != r->term) {
        return;
    }
    const uint32_t p = m->from;
    r->heard[p] = true;
    if (m->granted) {
        const uint64_t mi = m->match < last_index(r) ? m->match : last_index(r);
        if (mi > r->match[p]) {
            r->match[p] = mi;
        }
        if (r->next[p] < r->match[p] + 1U) {
            r->next[p] = r->match[p] + 1U;
        }
        advance_commit(r);
        if (r->next[p] <= last_index(r)) {
            send_append(r, p);
        }
        return;
    }
    if (m->match < r->next[p]) {
        r->next[p] = m->match < 1U ? 1U : m->match;
    } else if (r->next[p] > 1U) {
        r->next[p]--;
    }
    if (r->next[p] < r->match[p] + 1U) {
        r->next[p] = r->match[p] + 1U; /* a stale reject must not undo known progress */
    }
    send_append(r, p);
}

int hx_raft_step(hx_raft *r, int64_t now_ms, const hx_raft_msg *m)
{
    if (begin(r, now_ms) != 0) {
        return -1;
    }
    if (m->to != r->cfg.id || m->from >= r->cfg.nnodes || m->from == r->cfg.id) {
        return finish(r);
    }
    switch (m->type) {
    case HX_RAFT_MSG_PREVOTE:
        on_prevote(r, m);
        return finish(r);
    case HX_RAFT_MSG_PREVOTE_RESP:
        on_prevote_resp(r, m);
        return finish(r);
    case HX_RAFT_MSG_VOTE:
    case HX_RAFT_MSG_VOTE_RESP:
    case HX_RAFT_MSG_APPEND:
    case HX_RAFT_MSG_APPEND_RESP:
    case HX_RAFT_MSG_SNAPSHOT:
        break;
    default:
        return finish(r);
    }

    if (m->term > r->term) {
        become_follower(r, m->term,
                        (m->type == HX_RAFT_MSG_APPEND || m->type == HX_RAFT_MSG_SNAPSHOT)
                            ? m->from
                            : HX_RAFT_NONE);
    } else if (m->term < r->term) {
        /* A stale peer: tell it our term so it steps down; ignore stale responses. */
        if (m->type == HX_RAFT_MSG_APPEND || m->type == HX_RAFT_MSG_SNAPSHOT) {
            reply_append(r, m->from, false, 0);
        } else if (m->type == HX_RAFT_MSG_VOTE) {
            hx_raft_msg *o = out_new(r, m->from, HX_RAFT_MSG_VOTE_RESP);
            if (o != NULL) {
                o->granted = false;
            }
        }
        return finish(r);
    }

    switch (m->type) {
    case HX_RAFT_MSG_VOTE:
        on_vote(r, m);
        break;
    case HX_RAFT_MSG_VOTE_RESP:
        on_vote_resp(r, m);
        break;
    case HX_RAFT_MSG_APPEND:
        on_append(r, m);
        break;
    case HX_RAFT_MSG_SNAPSHOT:
        on_snapshot(r, m);
        break;
    default:
        on_append_resp(r, m);
        break;
    }
    return finish(r);
}

/* ------------------------------------------------------------ inspection */

hx_raft_role hx_raft_get_role(const hx_raft *r)
{
    return r->role;
}

uint64_t hx_raft_term(const hx_raft *r)
{
    return r->term;
}

uint32_t hx_raft_leader(const hx_raft *r)
{
    return r->leader;
}

uint32_t hx_raft_voted_for(const hx_raft *r)
{
    return r->voted_for;
}

uint64_t hx_raft_commit_index(const hx_raft *r)
{
    return r->commit;
}

uint64_t hx_raft_last_index(const hx_raft *r)
{
    return last_index(r);
}

bool hx_raft_failed(const hx_raft *r)
{
    return r->failed;
}

bool hx_raft_entry_at(const hx_raft *r, uint64_t index, hx_raft_entry *out)
{
    if (index <= r->base || index > last_index(r)) {
        return false;
    }
    const lentry *e = entry(r, index);
    out->term = e->term;
    out->len = e->len;
    out->data = e->data;
    return true;
}

uint64_t hx_raft_snapshot_index(const hx_raft *r)
{
    return r->base;
}

int hx_raft_snapshot(hx_raft *r, int64_t now_ms, uint64_t index, const void *data, uint32_t len)
{
    if (begin(r, now_ms) != 0) {
        return -1;
    }
    uint64_t term = 0;
    if (r->cfg.storage.save_snapshot == NULL || index <= r->base || index > r->commit ||
        !term_at(r, index, &term)) {
        return -1;
    }
    if (r->cfg.storage.save_snapshot(r->cfg.storage.ctx, index, term, data, len) != 0) {
        r->failed = true;
        return finish(r);
    }
    r->dirty = true;
    if (!set_snap(r, data, len)) {
        r->failed = true;
        return finish(r);
    }
    drop_prefix(r, index, term);
    return finish(r);
}

bool hx_raft_take_installed_snapshot(hx_raft *r, uint64_t *index, uint64_t *term,
                                     const uint8_t **data, uint32_t *len)
{
    if (!r->snap_installed) {
        return false;
    }
    r->snap_installed = false;
    *index = r->base;
    *term = r->base_term;
    *data = r->snap;
    *len = r->snap_len;
    return true;
}
