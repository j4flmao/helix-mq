#include "groups.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/logger.h"

#define HX_GROUP_BUCKETS    128U
#define HX_TICK_INTERVAL_MS 100

typedef struct {
    size_t len;
    char name[HX_MAX_TOPIC_LEN];
} sub;

typedef struct member {
    struct member *next;
    char id[HX_MAX_MEMBER_ID + 1];
    size_t idlen;
    uint32_t session_ms;
    int64_t last_seen;
    bool joined; /* has (re)joined in the current rebalance */
    sub *subs;
    size_t nsubs;
    hx_tp *assign;
    size_t nassign;
    size_t cap_assign;
} member;

typedef struct group {
    struct group *next;
    char name[HX_MAX_GROUP_LEN];
    size_t nlen;
    uint32_t generation;
    bool rebalancing;
    int64_t deadline; /* members that have not rejoined by then are evicted */
    member *members;
    size_t nmembers;
} group;

struct hx_groups {
    group *buckets[HX_GROUP_BUCKETS];
    size_t ngroups;
    size_t nmembers;
    uint32_t max_groups;
    uint32_t max_members;
    char prefix[32];
    uint64_t counter;
    int64_t last_tick;
    bool ticked;
    uint64_t rebalances;
};

/* ------------------------------------------------------------- lifecycle */

hx_groups *hx_groups_create(uint32_t max_groups, uint32_t max_members, const char *id_prefix)
{
    if (max_groups == 0 || max_members == 0 || id_prefix == NULL) {
        return NULL;
    }
    hx_groups *gs = calloc(1, sizeof *gs);
    if (gs == NULL) {
        return NULL;
    }
    gs->max_groups = max_groups;
    gs->max_members = max_members;
    (void)snprintf(gs->prefix, sizeof gs->prefix, "%s", id_prefix);
    return gs;
}

static void remove_member(hx_groups *gs, group *g, member *m)
{
    for (member **pp = &g->members; *pp != NULL; pp = &(*pp)->next) {
        if (*pp == m) {
            *pp = m->next;
            break;
        }
    }
    free(m->subs);
    free(m->assign);
    free(m);
    g->nmembers--;
    gs->nmembers--;
}

static uint32_t group_hash(const uint8_t *name, size_t len)
{
    uint32_t h = 2166136261U; /* FNV-1a */
    for (size_t i = 0; i < len; i++) {
        h = (h ^ name[i]) * 16777619U;
    }
    return h % HX_GROUP_BUCKETS;
}

static void remove_group(hx_groups *gs, group *g)
{
    while (g->members != NULL) {
        remove_member(gs, g, g->members);
    }
    for (group **pp = &gs->buckets[group_hash((const uint8_t *)g->name, g->nlen)]; *pp != NULL;
         pp = &(*pp)->next) {
        if (*pp == g) {
            *pp = g->next;
            break;
        }
    }
    free(g);
    gs->ngroups--;
}

void hx_groups_destroy(hx_groups *gs)
{
    if (gs == NULL) {
        return;
    }
    for (size_t i = 0; i < HX_GROUP_BUCKETS; i++) {
        while (gs->buckets[i] != NULL) {
            remove_group(gs, gs->buckets[i]);
        }
    }
    free(gs);
}

uint64_t hx_groups_rebalance_count(const hx_groups *gs)
{
    return gs->rebalances;
}

size_t hx_groups_group_count(const hx_groups *gs)
{
    return gs->ngroups;
}

/* --------------------------------------------------------------- helpers */

static group *find_group(const hx_groups *gs, const uint8_t *name, size_t len)
{
    for (group *g = gs->buckets[group_hash(name, len)]; g != NULL; g = g->next) {
        if (g->nlen == len && memcmp(g->name, name, len) == 0) {
            return g;
        }
    }
    return NULL;
}

static member *find_member(const group *g, const uint8_t *id, size_t len)
{
    if (len == 0 || len > HX_MAX_MEMBER_ID) {
        return NULL;
    }
    for (member *m = g->members; m != NULL; m = m->next) {
        if (m->idlen == len && memcmp(m->id, id, len) == 0) {
            return m;
        }
    }
    return NULL;
}

static bool subscribed(const member *m, const char *topic, size_t tlen)
{
    for (size_t i = 0; i < m->nsubs; i++) {
        if (m->subs[i].len == tlen && memcmp(m->subs[i].name, topic, tlen) == 0) {
            return true;
        }
    }
    return false;
}

static bool in_request(const hx_str *topics, size_t n, const sub *s)
{
    for (size_t i = 0; i < n; i++) {
        if (topics[i].len == s->len && memcmp(topics[i].name, s->name, s->len) == 0) {
            return true;
        }
    }
    return false;
}

/* Same set of topics (order and duplicates do not matter). */
static bool subs_equal(const member *m, const hx_str *topics, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (!subscribed(m, topics[i].name, topics[i].len)) {
            return false;
        }
    }
    for (size_t i = 0; i < m->nsubs; i++) {
        if (!in_request(topics, n, &m->subs[i])) {
            return false;
        }
    }
    return true;
}

static hx_wire_err subs_set(member *m, const hx_str *topics, size_t n)
{
    sub *s = calloc(n != 0 ? n : 1, sizeof *s);
    if (s == NULL) {
        return HXE_STORAGE;
    }
    for (size_t i = 0; i < n; i++) {
        s[i].len = topics[i].len;
        memcpy(s[i].name, topics[i].name, topics[i].len);
    }
    free(m->subs);
    m->subs = s;
    m->nsubs = n;
    return HXE_OK;
}

static int assign_push(member *m, const hx_tp *tp)
{
    if (m->nassign == m->cap_assign) {
        const size_t nc = m->cap_assign != 0 ? m->cap_assign * 2 : 8;
        hx_tp *a = realloc(m->assign, nc * sizeof *a);
        if (a == NULL) {
            return -1;
        }
        m->assign = a;
        m->cap_assign = nc;
    }
    m->assign[m->nassign++] = *tp;
    return 0;
}

/* Wrapper so arrays of member pointers avoid multi-level pointer conversions. */
typedef struct {
    member *m;
} mref;

static int cmp_member(const void *pa, const void *pb)
{
    const mref *a = (const mref *)pa;
    const mref *b = (const mref *)pb;
    return strcmp(a->m->id, b->m->id);
}

static bool all_joined(const group *g)
{
    for (const member *m = g->members; m != NULL; m = m->next) {
        if (!m->joined) {
            return false;
        }
    }
    return true;
}

/* A membership or subscription change: everyone must rejoin before anyone gets a new assignment. */
static void begin_rebalance(group *g, int64_t now)
{
    uint32_t longest = 0;
    for (member *m = g->members; m != NULL; m = m->next) {
        m->joined = false;
        if (m->session_ms > longest) {
            longest = m->session_ms;
        }
    }
    g->rebalancing = true;
    g->deadline = now + (int64_t)longest;
}

/*
 * Every member has rejoined: assign partitions and publish the next generation.
 * Per topic, the partitions (in order) are dealt round-robin to the members
 * subscribed to it, members sorted by id, so the result is deterministic.
 */
static hx_wire_err complete_rebalance(hx_groups *gs, group *g, const hx_gctx *cx)
{
    hx_tp *parts = NULL;
    size_t np = 0;
    if (cx->list(cx->list_ctx, &parts, &np) != 0) {
        return HXE_STORAGE;
    }
    mref *ms = malloc(g->nmembers * sizeof *ms);
    mref *subs = malloc(g->nmembers * sizeof *subs);
    if (ms == NULL || subs == NULL) {
        free(ms);
        free(subs);
        free(parts);
        return HXE_STORAGE;
    }
    size_t n = 0;
    for (member *m = g->members; m != NULL; m = m->next) {
        ms[n++].m = m;
        m->nassign = 0;
    }
    qsort(ms, n, sizeof *ms, cmp_member);

    hx_wire_err err = HXE_OK;
    size_t i = 0;
    while (i < np && err == HXE_OK) {
        size_t j = i;
        while (j < np && parts[j].tlen == parts[i].tlen &&
               memcmp(parts[j].topic, parts[i].topic, parts[i].tlen) == 0) {
            j++;
        }
        size_t ns = 0;
        for (size_t k = 0; k < n; k++) {
            if (subscribed(ms[k].m, parts[i].topic, parts[i].tlen)) {
                subs[ns++] = ms[k];
            }
        }
        for (size_t k = i; ns > 0 && k < j; k++) {
            if (assign_push(subs[(k - i) % ns].m, &parts[k]) != 0) {
                err = HXE_STORAGE;
                break;
            }
        }
        i = j;
    }
    free(subs);
    free(ms);
    free(parts);
    if (err != HXE_OK) {
        for (member *m = g->members; m != NULL; m = m->next) {
            m->nassign = 0;
        }
        return err;
    }
    g->generation++;
    g->rebalancing = false;
    for (member *m = g->members; m != NULL; m = m->next) {
        m->joined = false;
    }
    gs->rebalances++;
    return HXE_OK;
}

static hx_wire_err maybe_complete(hx_groups *gs, group *g, const hx_gctx *cx)
{
    if (g->rebalancing && all_joined(g)) {
        return complete_rebalance(gs, g, cx);
    }
    return HXE_OK;
}

static void drop_if_empty(hx_groups *gs, group *g)
{
    if (g->nmembers == 0) {
        remove_group(gs, g);
    }
}

/* ------------------------------------------------------------------ tick */

static void tick_group(hx_groups *gs, group *g, const hx_gctx *cx)
{
    bool changed = false;
    member *m = g->members;
    while (m != NULL) {
        member *next = m->next;
        if (cx->now_ms - m->last_seen > (int64_t)m->session_ms) {
            HX_INFO("group member %s expired", m->id);
            remove_member(gs, g, m);
            changed = true;
        }
        m = next;
    }
    if (g->nmembers == 0) {
        remove_group(gs, g);
        return;
    }
    if (g->rebalancing && cx->now_ms >= g->deadline) {
        m = g->members;
        while (m != NULL) {
            member *next = m->next;
            if (!m->joined) {
                HX_INFO("group member %s evicted: did not rejoin before the rebalance deadline",
                        m->id);
                remove_member(gs, g, m);
                changed = true;
            }
            m = next;
        }
        if (g->nmembers == 0) {
            remove_group(gs, g);
            return;
        }
    }
    if (changed && !g->rebalancing) {
        begin_rebalance(g, cx->now_ms);
    }
    const hx_wire_err e = maybe_complete(gs, g, cx);
    if (e != HXE_OK) {
        HX_WARN("rebalance could not complete yet (error %u); will retry", (unsigned)e);
    }
}

void hx_groups_tick(hx_groups *gs, const hx_gctx *cx)
{
    gs->last_tick = cx->now_ms;
    gs->ticked = true;
    for (size_t b = 0; b < HX_GROUP_BUCKETS; b++) {
        group *g = gs->buckets[b];
        while (g != NULL) {
            group *next = g->next;
            tick_group(gs, g, cx);
            g = next;
        }
    }
}

void hx_groups_topic_changed(hx_groups *gs, const hx_gctx *cx, const char *topic, size_t tlen)
{
    for (size_t b = 0; b < HX_GROUP_BUCKETS; b++) {
        for (group *g = gs->buckets[b]; g != NULL; g = g->next) {
            if (g->rebalancing) {
                continue;
            }
            for (const member *m = g->members; m != NULL; m = m->next) {
                if (subscribed(m, topic, tlen)) {
                    begin_rebalance(g, cx->now_ms);
                    break;
                }
            }
        }
    }
}

/* Every request applies due expiries first, so correctness never depends on the caller's timer. */
static void maybe_tick(hx_groups *gs, const hx_gctx *cx)
{
    if (!gs->ticked || cx->now_ms < gs->last_tick ||
        cx->now_ms - gs->last_tick >= HX_TICK_INTERVAL_MS) {
        hx_groups_tick(gs, cx);
    }
}

/* ------------------------------------------------------------------- API */

static group *group_create(hx_groups *gs, const uint8_t *name, size_t len)
{
    group *g = calloc(1, sizeof *g);
    if (g == NULL) {
        return NULL;
    }
    g->nlen = len;
    memcpy(g->name, name, len);
    const uint32_t h = group_hash(name, len);
    g->next = gs->buckets[h];
    gs->buckets[h] = g;
    gs->ngroups++;
    return g;
}

static hx_wire_err member_create(hx_groups *gs, group *g, const hx_gctx *cx, uint32_t session_ms,
                                 const hx_str *topics, size_t ntopics, member **out)
{
    member *m = calloc(1, sizeof *m);
    if (m == NULL) {
        return HXE_STORAGE;
    }
    const hx_wire_err e = subs_set(m, topics, ntopics);
    if (e != HXE_OK) {
        free(m);
        return e;
    }
    const int n =
        snprintf(m->id, sizeof m->id, "%s-%llu", gs->prefix, (unsigned long long)++gs->counter);
    m->idlen = n > 0 ? (size_t)n : 0;
    m->session_ms = session_ms;
    m->last_seen = cx->now_ms;
    m->next = g->members;
    g->members = m;
    g->nmembers++;
    gs->nmembers++;
    *out = m;
    return HXE_OK;
}

hx_wire_err hx_groups_join(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name, size_t glen,
                           const uint8_t *mid, size_t mlen, uint32_t session_ms,
                           const hx_str *topics, size_t ntopics, hx_join_result *out)
{
    maybe_tick(gs, cx);
    group *g = find_group(gs, group_name, glen);
    member *m = NULL;

    if (mlen > 0) {
        if (g != NULL) {
            m = find_member(g, mid, mlen);
        }
        if (m == NULL) {
            return HXE_UNKNOWN_MEMBER;
        }
        m->last_seen = cx->now_ms;
        m->session_ms = session_ms;
        if (!subs_equal(m, topics, ntopics)) {
            const hx_wire_err e = subs_set(m, topics, ntopics);
            if (e != HXE_OK) {
                return e;
            }
            if (!g->rebalancing) {
                begin_rebalance(g, cx->now_ms);
            }
            m->joined = true;
        } else if (g->rebalancing) {
            m->joined = true;
        }
        /* else: stable group, nothing changed -> idempotent re-fetch of the assignment */
    } else {
        if (gs->nmembers >= gs->max_members) {
            return HXE_RESOURCE_LIMIT;
        }
        if (g == NULL) {
            if (gs->ngroups >= gs->max_groups) {
                return HXE_RESOURCE_LIMIT;
            }
            g = group_create(gs, group_name, glen);
            if (g == NULL) {
                return HXE_STORAGE;
            }
        }
        const hx_wire_err e = member_create(gs, g, cx, session_ms, topics, ntopics, &m);
        if (e != HXE_OK) {
            drop_if_empty(gs, g);
            return e;
        }
        if (!g->rebalancing) {
            begin_rebalance(g, cx->now_ms);
        }
        m->joined = true;
    }

    const hx_wire_err e = maybe_complete(gs, g, cx);
    if (e != HXE_OK) {
        return e;
    }
    memcpy(out->member_id, m->id, m->idlen + 1);
    out->member_id_len = m->idlen;
    out->generation = g->generation;
    out->stable = !g->rebalancing;
    out->assign = out->stable ? m->assign : NULL;
    out->nassign = out->stable ? m->nassign : 0;
    return HXE_OK;
}

hx_wire_err hx_groups_heartbeat(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name,
                                size_t glen, const uint8_t *mid, size_t mlen, uint32_t generation)
{
    maybe_tick(gs, cx);
    group *g = find_group(gs, group_name, glen);
    member *m = g != NULL ? find_member(g, mid, mlen) : NULL;
    if (m == NULL) {
        return HXE_UNKNOWN_MEMBER;
    }
    m->last_seen = cx->now_ms;
    if (g->rebalancing) {
        return HXE_REBALANCE_IN_PROGRESS;
    }
    return generation == g->generation ? HXE_OK : HXE_ILLEGAL_GENERATION;
}

hx_wire_err hx_groups_leave(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name,
                            size_t glen, const uint8_t *mid, size_t mlen)
{
    maybe_tick(gs, cx);
    group *g = find_group(gs, group_name, glen);
    member *m = g != NULL ? find_member(g, mid, mlen) : NULL;
    if (m == NULL) {
        return HXE_UNKNOWN_MEMBER;
    }
    remove_member(gs, g, m);
    if (g->nmembers == 0) {
        remove_group(gs, g);
        return HXE_OK;
    }
    if (!g->rebalancing) {
        begin_rebalance(g, cx->now_ms);
    }
    return maybe_complete(gs, g, cx);
}

hx_wire_err hx_groups_check_commit(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name,
                                   size_t glen, const uint8_t *mid, size_t mlen,
                                   uint32_t generation, const uint8_t *topic, size_t tlen,
                                   uint32_t partition)
{
    maybe_tick(gs, cx);
    group *g = find_group(gs, group_name, glen);
    member *m = g != NULL ? find_member(g, mid, mlen) : NULL;
    if (m == NULL) {
        return HXE_UNKNOWN_MEMBER;
    }
    m->last_seen = cx->now_ms;
    if (generation != g->generation) {
        return HXE_ILLEGAL_GENERATION;
    }
    for (size_t i = 0; i < m->nassign; i++) {
        if (m->assign[i].partition == partition && m->assign[i].tlen == tlen &&
            memcmp(m->assign[i].topic, topic, tlen) == 0) {
            return HXE_OK;
        }
    }
    return HXE_NOT_ASSIGNED;
}

hx_wire_err hx_groups_describe(hx_groups *gs, const hx_gctx *cx, const uint8_t *group_name,
                               size_t glen, hx_buf *out)
{
    maybe_tick(gs, cx);
    group *g = find_group(gs, group_name, glen);
    if (g == NULL) {
        return HXE_UNKNOWN_MEMBER;
    }
    mref *ms = malloc(g->nmembers * sizeof *ms);
    if (ms == NULL) {
        return HXE_STORAGE;
    }
    size_t n = 0;
    for (member *m = g->members; m != NULL; m = m->next) {
        ms[n++].m = m;
    }
    qsort(ms, n, sizeof *ms, cmp_member);

    int bad = hx_buf_put_u8(out, g->rebalancing ? 1 : 0) != 0 ||
              hx_buf_put_u32(out, g->generation) != 0 || hx_buf_put_u32(out, (uint32_t)n) != 0;
    for (size_t i = 0; i < n && !bad; i++) {
        const member *m = ms[i].m;
        bad = hx_buf_put_u16(out, (uint16_t)m->idlen) != 0 ||
              hx_buf_put(out, m->id, m->idlen) != 0 ||
              hx_buf_put_u32(out, (uint32_t)m->nassign) != 0;
        for (size_t k = 0; k < m->nassign && !bad; k++) {
            bad = hx_buf_put_u16(out, (uint16_t)m->assign[k].tlen) != 0 ||
                  hx_buf_put(out, m->assign[k].topic, m->assign[k].tlen) != 0 ||
                  hx_buf_put_u32(out, m->assign[k].partition) != 0;
        }
    }
    free(ms);
    return bad ? HXE_STORAGE : HXE_OK;
}
