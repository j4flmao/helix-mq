#include "broker.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "broker/groups.h"
#include "broker/producers.h"
#include "proto/codec.h"
#include "proto/protocol.h"
#include "util/logger.h"

#define HX_BUCKETS                 256U
#define HX_PATH_MAX                4096U
#define HX_FETCH_ENTRY_OVERHEAD    20U /* offset u64 + timestamp i64 + length u32 */
#define HX_DEFAULT_MAX_FETCH       (4U << 20)
#define HX_DEFAULT_MAX_LOGS        1024U
#define HX_DEFAULT_MAX_OFFSETS     100000U
#define HX_DEFAULT_MAX_GROUPS      1000U
#define HX_DEFAULT_MAX_MEMBERS     10000U
#define HX_DEFAULT_MAX_PRODUCERS   10000U
#define HX_DEFAULT_PRODUCER_TTL_MS (15U * 60U * 1000U)
#define HX_TOMBSTONE               UINT64_MAX /* offset value that deletes a committed offset */
#define HX_DELETING_PREFIX         ".deleting."
/* u16 glen + group + u16 tlen + topic + u32 partition + u64 offset */
#define HX_OFFSET_REC_MAX (2U + HX_MAX_GROUP_LEN + 2U + HX_MAX_TOPIC_LEN + 4U + 8U)

typedef struct part {
    struct part *next;
    struct part *dirty_next; /* intrusive list of partitions awaiting commit */
    bool in_dirty;
    hx_log *log;
    uint32_t partition;
    size_t topic_len;
    char topic[HX_MAX_TOPIC_LEN];
} part;

/* Committed consumer-group offset. key = group bytes followed by topic bytes. */
typedef struct oentry {
    struct oentry *next;
    uint64_t offset;
    uint32_t partition;
    uint16_t glen;
    uint16_t tlen;
    char key[];
} oentry;

struct hx_broker {
    hx_broker_cfg cfg;
    char *data_dir;
    part *buckets[HX_BUCKETS];
    part *dirty_head;
    oentry *offsets[HX_BUCKETS];
    size_t noffsets;
    hx_groups *groups;
    hx_producers *producers;
    size_t nparts;
    uint8_t *scratch; /* one record, sized max_record_bytes */
    hx_broker_stats stats;
};

/* --------------------------------------------------------------- helpers */

void hx_broker_cfg_default(hx_broker_cfg *cfg)
{
    cfg->data_dir = "./data";
    hx_log_opts_default(&cfg->log_opts);
    cfg->max_fetch_bytes = HX_DEFAULT_MAX_FETCH;
    cfg->max_open_logs = HX_DEFAULT_MAX_LOGS;
    cfg->max_offset_entries = HX_DEFAULT_MAX_OFFSETS;
    cfg->max_groups = HX_DEFAULT_MAX_GROUPS;
    cfg->max_group_members = HX_DEFAULT_MAX_MEMBERS;
    cfg->max_producers = HX_DEFAULT_MAX_PRODUCERS;
    cfg->producer_ttl_ms = HX_DEFAULT_PRODUCER_TTL_MS;
    cfg->clock_ms = NULL;
}

/* Shared charset for topic and group names: [A-Za-z0-9._-], 1..128 bytes, no leading '.'. */
static bool name_valid(const uint8_t *t, size_t len)
{
    if (t == NULL || len == 0 || len > HX_MAX_TOPIC_LEN || t[0] == '.') {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        const uint8_t c = t[i];
        const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        if (!ok) {
            return false;
        }
    }
    return true;
}

/* User topics may not use the reserved "__" prefix. */
static bool topic_valid(const uint8_t *t, size_t len)
{
    return name_valid(t, len) && !(len >= 2 && t[0] == '_' && t[1] == '_');
}

static bool group_valid(const uint8_t *g, size_t len)
{
    return len <= HX_MAX_GROUP_LEN && name_valid(g, len);
}

static size_t bucket_of(const uint8_t *topic, size_t len, uint32_t partition)
{
    uint32_t h = 2166136261U; /* FNV-1a */
    for (size_t i = 0; i < len; i++) {
        h = (h ^ topic[i]) * 16777619U;
    }
    for (unsigned shift = 0; shift < 32; shift += 8) {
        h = (h ^ ((partition >> shift) & 0xFFU)) * 16777619U;
    }
    return h % HX_BUCKETS;
}

static part *find_part(const hx_broker *b, const uint8_t *topic, size_t len, uint32_t partition)
{
    for (part *p = b->buckets[bucket_of(topic, len, partition)]; p != NULL; p = p->next) {
        if (p->partition == partition && p->topic_len == len && memcmp(p->topic, topic, len) == 0) {
            return p;
        }
    }
    return NULL;
}

/*
 * Look up (or open) the log for a partition. With create == false a partition
 * that does not exist on disk is NOT created, so Fetch on random names cannot
 * be used to fill the disk with directories.
 */
static part *get_part(hx_broker *b, const uint8_t *topic, size_t len, uint32_t partition,
                      bool create, hx_wire_err *err)
{
    part *existing = find_part(b, topic, len, partition);
    if (existing != NULL) {
        return existing;
    }

    char path[HX_PATH_MAX];
    int n = snprintf(path, sizeof path, "%s/%.*s-%u", b->data_dir, (int)len, (const char *)topic,
                     partition);
    if (n < 0 || (size_t)n >= sizeof path) {
        *err = HXE_INVALID_TOPIC;
        return NULL;
    }

    if (!create) {
        struct stat st;
        if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) {
            *err = HXE_UNKNOWN_PARTITION;
            return NULL;
        }
    }
    if (b->nparts >= b->cfg.max_open_logs) {
        *err = HXE_RESOURCE_LIMIT;
        return NULL;
    }

    part *p = calloc(1, sizeof *p);
    if (p == NULL) {
        *err = HXE_STORAGE;
        return NULL;
    }
    hx_err e = hx_log_open(path, &b->cfg.log_opts, &p->log);
    if (e != HX_OK) {
        HX_ERROR("open partition %s failed: %s (errno=%d)", path, hx_strerror(e), errno);
        free(p);
        *err = HXE_STORAGE;
        return NULL;
    }
    p->partition = partition;
    p->topic_len = len;
    memcpy(p->topic, topic, len);

    size_t bk = bucket_of(topic, len, partition);
    p->next = b->buckets[bk];
    b->buckets[bk] = p;
    b->nparts++;
    return p;
}

static void mark_dirty(hx_broker *b, part *p)
{
    if (hx_log_is_dirty(p->log) != 0 && !p->in_dirty) {
        p->in_dirty = true;
        p->dirty_next = b->dirty_head;
        b->dirty_head = p;
    }
}

/* ---------------------------------------------------- committed offsets */

static uint32_t okey_hash(const uint8_t *g, size_t gl, const uint8_t *t, size_t tl, uint32_t pidx)
{
    uint32_t h = 2166136261U; /* FNV-1a over group, separator, topic, partition */
    for (size_t i = 0; i < gl; i++) {
        h = (h ^ g[i]) * 16777619U;
    }
    h = (h ^ 0xFFU) * 16777619U;
    for (size_t i = 0; i < tl; i++) {
        h = (h ^ t[i]) * 16777619U;
    }
    for (unsigned shift = 0; shift < 32; shift += 8) {
        h = (h ^ ((pidx >> shift) & 0xFFU)) * 16777619U;
    }
    return h % HX_BUCKETS;
}

static oentry *find_oentry(const hx_broker *b, const uint8_t *g, size_t gl, const uint8_t *t,
                           size_t tl, uint32_t pidx)
{
    for (oentry *e = b->offsets[okey_hash(g, gl, t, tl, pidx)]; e != NULL; e = e->next) {
        if (e->partition == pidx && e->glen == gl && e->tlen == tl && memcmp(e->key, g, gl) == 0 &&
            memcmp(e->key + gl, t, tl) == 0) {
            return e;
        }
    }
    return NULL;
}

/* 0 = ok, -1 = out of memory, -2 = entry limit reached. */
static int offset_set(hx_broker *b, const uint8_t *g, size_t gl, const uint8_t *t, size_t tl,
                      uint32_t pidx, uint64_t offset)
{
    oentry *e = find_oentry(b, g, gl, t, tl, pidx);
    if (e != NULL) {
        e->offset = offset;
        return 0;
    }
    if (b->noffsets >= b->cfg.max_offset_entries) {
        return -2;
    }
    e = malloc(sizeof *e + gl + tl);
    if (e == NULL) {
        return -1;
    }
    e->offset = offset;
    e->partition = pidx;
    e->glen = (uint16_t)gl;
    e->tlen = (uint16_t)tl;
    memcpy(e->key, g, gl);
    memcpy(e->key + gl, t, tl);
    const uint32_t bk = okey_hash(g, gl, t, tl, pidx);
    e->next = b->offsets[bk];
    b->offsets[bk] = e;
    b->noffsets++;
    return 0;
}

static void offset_del(hx_broker *b, const uint8_t *g, size_t gl, const uint8_t *t, size_t tl,
                       uint32_t pidx)
{
    for (oentry **pp = &b->offsets[okey_hash(g, gl, t, tl, pidx)]; *pp != NULL; pp = &(*pp)->next) {
        oentry *e = *pp;
        if (e->partition == pidx && e->glen == gl && e->tlen == tl && memcmp(e->key, g, gl) == 0 &&
            memcmp(e->key + gl, t, tl) == 0) {
            *pp = e->next;
            free(e);
            b->noffsets--;
            return;
        }
    }
}

static size_t encode_offset_rec(uint8_t *dst, const uint8_t *g, size_t gl, const uint8_t *t,
                                size_t tl, uint32_t pidx, uint64_t offset)
{
    size_t n = 0;
    dst[n++] = (uint8_t)(gl >> 8);
    dst[n++] = (uint8_t)gl;
    memcpy(dst + n, g, gl);
    n += gl;
    dst[n++] = (uint8_t)(tl >> 8);
    dst[n++] = (uint8_t)tl;
    memcpy(dst + n, t, tl);
    n += tl;
    for (unsigned shift = 32; shift > 0; shift -= 8) {
        dst[n++] = (uint8_t)(pidx >> (shift - 8));
    }
    for (unsigned shift = 64; shift > 0; shift -= 8) {
        dst[n++] = (uint8_t)(offset >> (shift - 8));
    }
    return n;
}

/*
 * Rebuild the offset table from the internal log; later records win. Returns -1
 * if the log cannot be read: serving stale or missing offsets silently would make
 * consumers re-read or skip data, so startup fails instead.
 */
static int replay_offsets(hx_broker *b)
{
    hx_wire_err err = HXE_OK;
    part *p =
        get_part(b, (const uint8_t *)HX_INTERNAL_TOPIC, strlen(HX_INTERNAL_TOPIC), 0, false, &err);
    if (p == NULL) {
        return err == HXE_UNKNOWN_PARTITION ? 0 : -1; /* no commits yet is fine */
    }
    const uint64_t next = hx_log_next_offset(p->log);
    for (uint64_t o = hx_log_first_offset(p->log); o < next; o++) {
        uint32_t len = 0;
        if (hx_log_read(p->log, o, b->scratch, b->cfg.log_opts.max_record_bytes, &len, NULL) !=
            HX_OK) {
            HX_ERROR("cannot read internal offsets log at %llu", (unsigned long long)o);
            return -1;
        }
        hx_rd rd;
        hx_rd_init(&rd, b->scratch, len);
        const size_t gl = hx_rd_u16(&rd);
        const uint8_t *g = hx_rd_take(&rd, gl);
        const size_t tl = hx_rd_u16(&rd);
        const uint8_t *t = hx_rd_take(&rd, tl);
        const uint32_t pidx = hx_rd_u32(&rd);
        const uint64_t off = hx_rd_u64(&rd);
        if (!rd.ok || hx_rd_left(&rd) != 0 || !group_valid(g, gl) || !topic_valid(t, tl)) {
            HX_WARN("skipping malformed record %llu in internal offsets log",
                    (unsigned long long)o);
            continue;
        }
        if (off == HX_TOMBSTONE) {
            offset_del(b, g, gl, t, tl, pidx);
            continue;
        }
        if (offset_set(b, g, gl, t, tl, pidx, off) != 0) {
            HX_ERROR("offset table limit reached while replaying (raise max_offset_entries)");
            return -1;
        }
    }
    return 0;
}

static int64_t realtime_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return 0;
    }
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int64_t monotonic_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int64_t clock_now(const hx_broker *b)
{
    return b->cfg.clock_ms != NULL ? b->cfg.clock_ms() : monotonic_ms();
}

/* ------------------------------------------------- partition removal helpers */

/* Remove a flat directory of files (a partition). Best effort; returns 0 if it is gone. */
static int rmtree_flat(const char *path)
{
    DIR *d = opendir(path);
    if (d == NULL) {
        return errno == ENOENT ? 0 : -1;
    }
    const struct dirent *de = NULL;
    int rc = 0;
    while ((de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) {
            continue;
        }
        char child[HX_PATH_MAX];
        const int n = snprintf(child, sizeof child, "%s/%s", path, de->d_name);
        if (n < 0 || (size_t)n >= sizeof child || unlink(child) != 0) {
            rc = -1;
        }
    }
    (void)closedir(d);
    return rmdir(path) == 0 ? rc : -1;
}

/* Finish deletions that were interrupted by a crash (renamed but not yet removed). */
static void purge_deleting_dirs(const hx_broker *b)
{
    DIR *d = opendir(b->data_dir);
    if (d == NULL) {
        return;
    }
    const size_t plen = strlen(HX_DELETING_PREFIX);
    const struct dirent *de = NULL;
    while ((de = readdir(d)) != NULL) {
        if (strncmp(de->d_name, HX_DELETING_PREFIX, plen) != 0) {
            continue;
        }
        char path[HX_PATH_MAX];
        const int n = snprintf(path, sizeof path, "%s/%s", b->data_dir, de->d_name);
        if (n > 0 && (size_t)n < sizeof path && rmtree_flat(path) != 0) {
            HX_WARN("could not finish deleting %s (errno=%d)", path, errno);
        }
    }
    (void)closedir(d);
}

/* Close and forget an open partition (it may be on the dirty list). */
static void drop_part(hx_broker *b, part *victim)
{
    for (part **pp = &b->buckets[bucket_of((const uint8_t *)victim->topic, victim->topic_len,
                                           victim->partition)];
         *pp != NULL; pp = &(*pp)->next) {
        if (*pp == victim) {
            *pp = victim->next;
            break;
        }
    }
    for (part **pp = &b->dirty_head; *pp != NULL; pp = &(*pp)->dirty_next) {
        if (*pp == victim) {
            *pp = victim->dirty_next;
            break;
        }
    }
    hx_log_close(victim->log);
    free(victim);
    b->nparts--;
}

/*
 * Delete one partition: close it, atomically rename its directory out of the namespace (a crash
 * after the rename is finished by purge_deleting_dirs at the next start), then remove the files. 0
 * = gone (or never existed), -1 = failed.
 */
static int destroy_partition(hx_broker *b, const uint8_t *topic, size_t tlen, uint32_t pidx)
{
    part *open = find_part(b, topic, tlen, pidx);
    if (open != NULL) {
        drop_part(b, open);
    }
    char from[HX_PATH_MAX];
    char to[HX_PATH_MAX];
    int n = snprintf(from, sizeof from, "%s/%.*s-%u", b->data_dir, (int)tlen, (const char *)topic,
                     pidx);
    int m = snprintf(to, sizeof to, "%s/" HX_DELETING_PREFIX "%.*s-%u", b->data_dir, (int)tlen,
                     (const char *)topic, pidx);
    if (n < 0 || m < 0 || (size_t)n >= sizeof from || (size_t)m >= sizeof to) {
        return -1;
    }
    if (rename(from, to) != 0) {
        return errno == ENOENT ? 0 : -1;
    }
    return rmtree_flat(to);
}

/* ------------------------------------------------------------- lifecycle */

hx_broker *hx_broker_create(const hx_broker_cfg *cfg)
{
    if (cfg == NULL || cfg->data_dir == NULL || cfg->max_fetch_bytes == 0 ||
        cfg->max_open_logs == 0 || cfg->max_offset_entries == 0 || cfg->max_groups == 0 ||
        cfg->max_group_members == 0 || cfg->max_producers == 0 || cfg->producer_ttl_ms == 0) {
        return NULL;
    }
    if (mkdir(cfg->data_dir, 0755) != 0 && errno != EEXIST) {
        HX_ERROR("cannot create data dir %s (errno=%d)", cfg->data_dir, errno);
        return NULL;
    }
    hx_broker *b = calloc(1, sizeof *b);
    if (b == NULL) {
        return NULL;
    }
    b->cfg = *cfg;
    b->cfg.log_opts.group_commit = cfg->log_opts.sync == HX_SYNC_ALWAYS ? 1 : 0;
    b->data_dir = strdup(cfg->data_dir);
    b->scratch = malloc(cfg->log_opts.max_record_bytes);
    if (b->data_dir == NULL || b->scratch == NULL) {
        hx_broker_destroy(b);
        return NULL;
    }
    b->cfg.data_dir = b->data_dir;
    char prefix[32];
    (void)snprintf(prefix, sizeof prefix, "m%llx", (unsigned long long)realtime_ms());
    b->groups = hx_groups_create(cfg->max_groups, cfg->max_group_members, prefix);
    if (b->groups == NULL) {
        hx_broker_destroy(b);
        return NULL;
    }
    b->producers = hx_producers_create(cfg->max_producers, (int64_t)cfg->producer_ttl_ms,
                                       (uint64_t)realtime_ms());
    if (b->producers == NULL) {
        hx_broker_destroy(b);
        return NULL;
    }
    purge_deleting_dirs(b);
    if (replay_offsets(b) != 0) {
        hx_broker_destroy(b);
        return NULL;
    }
    return b;
}

void hx_broker_destroy(hx_broker *b)
{
    if (b == NULL) {
        return;
    }
    for (size_t i = 0; i < HX_BUCKETS; i++) {
        part *p = b->buckets[i];
        while (p != NULL) {
            part *next = p->next;
            hx_log_close(p->log);
            free(p);
            p = next;
        }
    }
    for (size_t i = 0; i < HX_BUCKETS; i++) {
        oentry *e = b->offsets[i];
        while (e != NULL) {
            oentry *next = e->next;
            free(e);
            e = next;
        }
    }
    hx_groups_destroy(b->groups);
    hx_producers_destroy(b->producers);
    free(b->scratch);
    free(b->data_dir);
    free(b);
}

int hx_broker_commit(hx_broker *b)
{
    int rc = 0;
    part *p = b->dirty_head;
    b->dirty_head = NULL;
    while (p != NULL) {
        part *next = p->dirty_next;
        p->in_dirty = false;
        p->dirty_next = NULL;
        const hx_err e = hx_log_flush(p->log);
        if (e != HX_OK) {
            HX_ERROR("group commit failed for %.*s-%u: %s (errno=%d)", (int)p->topic_len, p->topic,
                     p->partition, hx_strerror(e), errno);
            rc = -1;
        } else {
            b->stats.syncs++;
        }
        p = next;
    }
    return rc;
}

hx_broker_stats hx_broker_get_stats(const hx_broker *b)
{
    hx_broker_stats st = b->stats;
    st.group_rebalances = b->groups != NULL ? hx_groups_rebalance_count(b->groups) : 0;
    return st;
}

/* ------------------------------------------------------------- handlers */

/* Parses: u16 topic_len | topic | u32 partition. */
static bool read_topic_partition(hx_rd *rd, const uint8_t **topic, size_t *tlen,
                                 uint32_t *partition)
{
    *tlen = hx_rd_u16(rd);
    *topic = hx_rd_take(rd, *tlen);
    *partition = hx_rd_u32(rd);
    return rd->ok;
}

/* Validate and append one record. Shared by Produce and ProduceIdempotent. */
static hx_wire_err produce_core(hx_broker *b, const uint8_t *topic, size_t tlen, uint32_t partition,
                                const uint8_t *payload, uint32_t plen, uint64_t *offset)
{
    if (!topic_valid(topic, tlen)) {
        return HXE_INVALID_TOPIC;
    }
    if (partition > HX_MAX_PARTITION) {
        return HXE_UNKNOWN_PARTITION;
    }
    if (plen > b->cfg.log_opts.max_record_bytes) {
        return HXE_RECORD_TOO_LARGE;
    }
    hx_wire_err err = HXE_OK;
    part *p = get_part(b, topic, tlen, partition, true, &err);
    if (p == NULL) {
        return err;
    }
    const hx_err e = hx_log_append(p->log, payload, plen, realtime_ms(), offset);
    if (e == HX_ERR_TOO_LARGE) {
        return HXE_RECORD_TOO_LARGE;
    }
    if (e != HX_OK) {
        HX_ERROR("append failed: %s (errno=%d)", hx_strerror(e), errno);
        return HXE_STORAGE;
    }
    mark_dirty(b, p);
    b->stats.produced_records++;
    b->stats.produced_bytes += plen;
    return HXE_OK;
}

static hx_wire_err handle_produce(hx_broker *b, hx_rd *rd, hx_buf *out)
{
    const uint8_t *topic = NULL;
    size_t tlen = 0;
    uint32_t partition = 0;
    if (!read_topic_partition(rd, &topic, &tlen, &partition)) {
        return HXE_BAD_REQUEST;
    }
    const uint32_t plen = hx_rd_u32(rd);
    const uint8_t *payload = hx_rd_take(rd, plen);
    if (!rd->ok || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    uint64_t offset = 0;
    const hx_wire_err err = produce_core(b, topic, tlen, partition, payload, plen, &offset);
    if (err != HXE_OK) {
        return err;
    }
    return hx_buf_put_u64(out, offset) == 0 ? HXE_OK : HXE_STORAGE;
}

static hx_wire_err handle_fetch(hx_broker *b, hx_rd *rd, hx_buf *out)
{
    const uint8_t *topic = NULL;
    size_t tlen = 0;
    uint32_t partition = 0;
    if (!read_topic_partition(rd, &topic, &tlen, &partition)) {
        return HXE_BAD_REQUEST;
    }
    uint64_t offset = hx_rd_u64(rd);
    uint32_t max_bytes = hx_rd_u32(rd);
    if (!rd->ok || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!topic_valid(topic, tlen)) {
        return HXE_INVALID_TOPIC;
    }
    if (partition > HX_MAX_PARTITION) {
        return HXE_UNKNOWN_PARTITION;
    }
    if (max_bytes > b->cfg.max_fetch_bytes) {
        max_bytes = b->cfg.max_fetch_bytes;
    }

    hx_wire_err err = HXE_OK;
    part *p = get_part(b, topic, tlen, partition, false, &err);
    if (p == NULL) {
        return err;
    }
    hx_log *log = p->log;
    const uint64_t first = hx_log_first_offset(log);
    const uint64_t next = hx_log_next_offset(log);
    if (offset < first || offset > next) {
        return HXE_OFFSET_OUT_OF_RANGE;
    }

    const size_t count_pos = out->len;
    if (hx_buf_put_u32(out, 0) != 0) {
        return HXE_STORAGE;
    }
    uint32_t count = 0;
    uint64_t total = 0;
    while (offset < next) {
        uint32_t len = 0;
        int64_t ts = 0;
        hx_err e =
            hx_log_read(log, offset, b->scratch, b->cfg.log_opts.max_record_bytes, &len, &ts);
        if (e != HX_OK) {
            HX_ERROR("read at offset %llu failed: %s", (unsigned long long)offset, hx_strerror(e));
            return HXE_STORAGE;
        }
        const uint64_t entry = (uint64_t)len + HX_FETCH_ENTRY_OVERHEAD;
        /* Always return at least one record so a consumer can make progress. */
        if (count > 0 && total + entry > max_bytes) {
            break;
        }
        if (hx_buf_put_u64(out, offset) != 0 || hx_buf_put_u64(out, (uint64_t)ts) != 0 ||
            hx_buf_put_u32(out, len) != 0 || hx_buf_put(out, b->scratch, len) != 0) {
            return HXE_STORAGE;
        }
        count++;
        total += entry;
        offset++;
        if (total >= max_bytes) {
            break;
        }
    }
    hx_buf_patch_u32(out, count_pos, count);
    b->stats.fetched_records += count;
    b->stats.fetched_bytes += total;
    return HXE_OK;
}

/* Parses: u16 group_len | group. */
static const uint8_t *read_group(hx_rd *rd, size_t *glen)
{
    *glen = hx_rd_u16(rd);
    return hx_rd_take(rd, *glen);
}

/* Shared by OffsetCommit (unfenced) and GroupCommit (fenced): validate, append, remember. */
static hx_wire_err commit_offset_core(hx_broker *b, const uint8_t *group, size_t gl,
                                      const uint8_t *topic, size_t tl, uint32_t partition,
                                      uint64_t offset)
{
    hx_wire_err err = HXE_OK;
    part *target = get_part(b, topic, tl, partition, false, &err);
    if (target == NULL) {
        return err;
    }
    if (offset > hx_log_next_offset(target->log)) {
        return HXE_OFFSET_OUT_OF_RANGE;
    }
    if (find_oentry(b, group, gl, topic, tl, partition) == NULL &&
        b->noffsets >= b->cfg.max_offset_entries) {
        return HXE_RESOURCE_LIMIT; /* checked before the append so the log never gets ahead */
    }

    part *internal =
        get_part(b, (const uint8_t *)HX_INTERNAL_TOPIC, strlen(HX_INTERNAL_TOPIC), 0, true, &err);
    if (internal == NULL) {
        return err;
    }
    uint8_t rec[HX_OFFSET_REC_MAX];
    const size_t n = encode_offset_rec(rec, group, gl, topic, tl, partition, offset);
    const hx_err e = hx_log_append(internal->log, rec, (uint32_t)n, realtime_ms(), NULL);
    if (e != HX_OK) {
        HX_ERROR("offset commit append failed: %s (errno=%d)", hx_strerror(e), errno);
        return HXE_STORAGE;
    }
    mark_dirty(b, internal);
    if (offset_set(b, group, gl, topic, tl, partition, offset) != 0) {
        return HXE_STORAGE; /* durable in the log; the next restart restores it */
    }
    return HXE_OK;
}

static hx_wire_err handle_offset_commit(hx_broker *b, hx_rd *rd)
{
    size_t gl = 0;
    size_t tl = 0;
    uint32_t partition = 0;
    const uint8_t *topic = NULL;
    const uint8_t *group = read_group(rd, &gl);
    if (!read_topic_partition(rd, &topic, &tl, &partition)) {
        return HXE_BAD_REQUEST;
    }
    const uint64_t offset = hx_rd_u64(rd);
    if (!rd->ok || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!group_valid(group, gl)) {
        return HXE_INVALID_GROUP;
    }
    if (!topic_valid(topic, tl)) {
        return HXE_INVALID_TOPIC;
    }
    if (partition > HX_MAX_PARTITION) {
        return HXE_UNKNOWN_PARTITION;
    }

    return commit_offset_core(b, group, gl, topic, tl, partition, offset);
}

static hx_wire_err handle_offset_fetch(hx_broker *b, hx_rd *rd, hx_buf *out)
{
    size_t gl = 0;
    size_t tl = 0;
    uint32_t partition = 0;
    const uint8_t *topic = NULL;
    const uint8_t *group = read_group(rd, &gl);
    if (!read_topic_partition(rd, &topic, &tl, &partition) || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!group_valid(group, gl)) {
        return HXE_INVALID_GROUP;
    }
    if (!topic_valid(topic, tl)) {
        return HXE_INVALID_TOPIC;
    }
    if (partition > HX_MAX_PARTITION) {
        return HXE_UNKNOWN_PARTITION;
    }
    const oentry *e = find_oentry(b, group, gl, topic, tl, partition);
    const int64_t value = e != NULL ? (int64_t)e->offset : -1;
    return hx_buf_put_u64(out, (uint64_t)value) == 0 ? HXE_OK : HXE_STORAGE;
}

static int cmp_tp(const void *pa, const void *pb)
{
    const hx_tp *a = pa;
    const hx_tp *c = pb;
    const size_t n = a->tlen < c->tlen ? a->tlen : c->tlen;
    const int r = memcmp(a->topic, c->topic, n);
    if (r != 0) {
        return r;
    }
    if (a->tlen != c->tlen) {
        return a->tlen < c->tlen ? -1 : 1;
    }
    return (a->partition > c->partition) - (a->partition < c->partition);
}

/* "<topic>-<digits>" -> hx_tp; false if the name is not a user partition directory. */
static bool parse_partition_dir(const char *name, hx_tp *out)
{
    const char *dash = strrchr(name, '-');
    if (dash == NULL || dash == name || dash[1] == '\0') {
        return false;
    }
    uint64_t pnum = 0;
    for (const char *c = dash + 1; *c != '\0'; c++) {
        if (*c < '0' || *c > '9' || pnum > HX_MAX_PARTITION) {
            return false;
        }
        pnum = pnum * 10 + (uint64_t)(*c - '0');
    }
    const size_t tl = (size_t)(dash - name);
    if (pnum > HX_MAX_PARTITION || !topic_valid((const uint8_t *)name, tl)) {
        return false;
    }
    out->partition = (uint32_t)pnum;
    out->tlen = tl;
    memcpy(out->topic, name, tl);
    return true;
}

/* Sorted list of the user partitions found on disk. Caller frees *out. */
static hx_wire_err scan_partitions(hx_broker *b, hx_tp **out, size_t *n_out)
{
    DIR *d = opendir(b->data_dir);
    if (d == NULL) {
        return HXE_STORAGE;
    }
    const size_t cap = (size_t)b->cfg.max_open_logs + 1U; /* each listed partition gets opened */
    hx_tp *list = calloc(cap, sizeof *list);
    if (list == NULL) {
        (void)closedir(d);
        return HXE_STORAGE;
    }
    size_t n = 0;
    hx_wire_err err = HXE_OK;
    const struct dirent *de = NULL;
    while (err == HXE_OK && (de = readdir(d)) != NULL) {
        hx_tp m;
        if (!parse_partition_dir(de->d_name, &m)) {
            continue;
        }
        if (n >= cap - 1U) {
            err = HXE_RESOURCE_LIMIT;
        } else {
            list[n++] = m;
        }
    }
    (void)closedir(d);
    if (err != HXE_OK) {
        free(list);
        return err;
    }
    qsort(list, n, sizeof *list, cmp_tp);
    *out = list;
    *n_out = n;
    return HXE_OK;
}

static int list_partitions_cb(void *ctx, hx_tp **out, size_t *n)
{
    return scan_partitions(ctx, out, n) == HXE_OK ? 0 : -1;
}

static hx_wire_err handle_metadata(hx_broker *b, hx_rd *rd, hx_buf *out)
{
    if (hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    hx_tp *list = NULL;
    size_t n = 0;
    hx_wire_err err = scan_partitions(b, &list, &n);
    if (err != HXE_OK) {
        return err;
    }
    if (hx_buf_put_u32(out, (uint32_t)n) != 0) {
        err = HXE_STORAGE;
    }
    for (size_t i = 0; err == HXE_OK && i < n; i++) {
        const part *p = get_part(b, (const uint8_t *)list[i].topic, list[i].tlen, list[i].partition,
                                 false, &err);
        if (p == NULL) {
            break;
        }
        if (hx_buf_put_u16(out, (uint16_t)list[i].tlen) != 0 ||
            hx_buf_put(out, list[i].topic, list[i].tlen) != 0 ||
            hx_buf_put_u32(out, list[i].partition) != 0 ||
            hx_buf_put_u64(out, hx_log_first_offset(p->log)) != 0 ||
            hx_buf_put_u64(out, hx_log_next_offset(p->log)) != 0) {
            err = HXE_STORAGE;
        }
    }
    free(list);
    return err;
}

/* ------------------------------------------------------- group coordinator */

static hx_gctx make_gctx(hx_broker *b)
{
    const hx_gctx cx = {.now_ms = clock_now(b), .list = list_partitions_cb, .list_ctx = b};
    return cx;
}

void hx_broker_tick(hx_broker *b)
{
    const hx_gctx cx = make_gctx(b);
    hx_groups_tick(b->groups, &cx);
    hx_producers_expire(b->producers, cx.now_ms);
}

/* Parses: u16 group_len | group | u16 member_len | member. */
static bool read_group_member(hx_rd *rd, const uint8_t **group, size_t *gl, const uint8_t **mid,
                              size_t *ml)
{
    *group = read_group(rd, gl);
    *mid = read_group(rd, ml);
    return rd->ok;
}

static hx_wire_err handle_join_group(hx_broker *b, hx_rd *rd, hx_buf *out)
{
    size_t gl = 0;
    size_t ml = 0;
    const uint8_t *mid = NULL;
    const uint8_t *group = NULL;
    if (!read_group_member(rd, &group, &gl, &mid, &ml)) {
        return HXE_BAD_REQUEST;
    }
    const uint32_t session_ms = hx_rd_u32(rd);
    const size_t nt = hx_rd_u16(rd);
    if (!rd->ok || nt == 0 || nt > HX_MAX_SUBSCRIPTIONS) {
        return HXE_BAD_REQUEST;
    }
    hx_str topics[HX_MAX_SUBSCRIPTIONS];
    for (size_t i = 0; i < nt; i++) {
        size_t tl = 0;
        const uint8_t *t = read_group(rd, &tl);
        if (!rd->ok) {
            return HXE_BAD_REQUEST;
        }
        if (!topic_valid(t, tl)) {
            return HXE_INVALID_TOPIC;
        }
        topics[i].name = (const char *)t;
        topics[i].len = tl;
    }
    if (hx_rd_left(rd) != 0 || session_ms < HX_MIN_SESSION_MS || session_ms > HX_MAX_SESSION_MS) {
        return HXE_BAD_REQUEST;
    }
    if (!group_valid(group, gl)) {
        return HXE_INVALID_GROUP;
    }

    const hx_gctx cx = make_gctx(b);
    hx_join_result r;
    const hx_wire_err err =
        hx_groups_join(b->groups, &cx, group, gl, mid, ml, session_ms, topics, nt, &r);
    if (err != HXE_OK) {
        return err;
    }
    int bad = hx_buf_put_u16(out, (uint16_t)r.member_id_len) != 0 ||
              hx_buf_put(out, r.member_id, r.member_id_len) != 0 ||
              hx_buf_put_u32(out, r.generation) != 0 || hx_buf_put_u8(out, r.stable ? 1 : 0) != 0 ||
              hx_buf_put_u32(out, (uint32_t)r.nassign) != 0;
    for (size_t i = 0; i < r.nassign && !bad; i++) {
        bad = hx_buf_put_u16(out, (uint16_t)r.assign[i].tlen) != 0 ||
              hx_buf_put(out, r.assign[i].topic, r.assign[i].tlen) != 0 ||
              hx_buf_put_u32(out, r.assign[i].partition) != 0;
    }
    return bad ? HXE_STORAGE : HXE_OK;
}

static hx_wire_err handle_heartbeat(hx_broker *b, hx_rd *rd)
{
    size_t gl = 0;
    size_t ml = 0;
    const uint8_t *mid = NULL;
    const uint8_t *group = NULL;
    if (!read_group_member(rd, &group, &gl, &mid, &ml)) {
        return HXE_BAD_REQUEST;
    }
    const uint32_t generation = hx_rd_u32(rd);
    if (!rd->ok || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!group_valid(group, gl)) {
        return HXE_INVALID_GROUP;
    }
    const hx_gctx cx = make_gctx(b);
    return hx_groups_heartbeat(b->groups, &cx, group, gl, mid, ml, generation);
}

static hx_wire_err handle_leave_group(hx_broker *b, hx_rd *rd)
{
    size_t gl = 0;
    size_t ml = 0;
    const uint8_t *mid = NULL;
    const uint8_t *group = NULL;
    if (!read_group_member(rd, &group, &gl, &mid, &ml) || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!group_valid(group, gl)) {
        return HXE_INVALID_GROUP;
    }
    const hx_gctx cx = make_gctx(b);
    return hx_groups_leave(b->groups, &cx, group, gl, mid, ml);
}

static hx_wire_err handle_describe_group(hx_broker *b, hx_rd *rd, hx_buf *out)
{
    size_t gl = 0;
    const uint8_t *group = read_group(rd, &gl);
    if (!rd->ok || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!group_valid(group, gl)) {
        return HXE_INVALID_GROUP;
    }
    const hx_gctx cx = make_gctx(b);
    return hx_groups_describe(b->groups, &cx, group, gl, out);
}

/* OffsetCommit with fencing: only the current generation's owner of the partition may move the
 * offset. */
static hx_wire_err handle_group_commit(hx_broker *b, hx_rd *rd)
{
    size_t gl = 0;
    size_t ml = 0;
    size_t tl = 0;
    uint32_t partition = 0;
    const uint8_t *mid = NULL;
    const uint8_t *group = NULL;
    const uint8_t *topic = NULL;
    if (!read_group_member(rd, &group, &gl, &mid, &ml)) {
        return HXE_BAD_REQUEST;
    }
    const uint32_t generation = hx_rd_u32(rd);
    if (!read_topic_partition(rd, &topic, &tl, &partition)) {
        return HXE_BAD_REQUEST;
    }
    const uint64_t offset = hx_rd_u64(rd);
    if (!rd->ok || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!group_valid(group, gl)) {
        return HXE_INVALID_GROUP;
    }
    if (!topic_valid(topic, tl)) {
        return HXE_INVALID_TOPIC;
    }
    if (partition > HX_MAX_PARTITION) {
        return HXE_UNKNOWN_PARTITION;
    }
    const hx_gctx cx = make_gctx(b);
    const hx_wire_err fence = hx_groups_check_commit(b->groups, &cx, group, gl, mid, ml, generation,
                                                     topic, tl, partition);
    if (fence != HXE_OK) {
        return fence;
    }
    return commit_offset_core(b, group, gl, topic, tl, partition, offset);
}

/* ----------------------------------------------- idempotent producer, topics */

static hx_wire_err handle_init_producer(hx_broker *b, hx_rd *rd, hx_buf *out)
{
    if (hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    uint64_t pid = 0;
    const hx_wire_err err = hx_producers_init(b->producers, clock_now(b), &pid);
    if (err != HXE_OK) {
        return err;
    }
    return hx_buf_put_u64(out, pid) == 0 ? HXE_OK : HXE_STORAGE;
}

/* Request: u64 producer_id | u32 sequence | u16 tlen | topic | u32 partition | u32 plen | payload
 */
static hx_wire_err handle_produce_idempotent(hx_broker *b, hx_rd *rd, hx_buf *out)
{
    const uint64_t pid = hx_rd_u64(rd);
    const uint32_t seq = hx_rd_u32(rd);
    const uint8_t *topic = NULL;
    size_t tlen = 0;
    uint32_t partition = 0;
    if (!read_topic_partition(rd, &topic, &tlen, &partition)) {
        return HXE_BAD_REQUEST;
    }
    const uint32_t plen = hx_rd_u32(rd);
    const uint8_t *payload = hx_rd_take(rd, plen);
    if (!rd->ok || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!topic_valid(topic, tlen)) {
        return HXE_INVALID_TOPIC;
    }
    if (partition > HX_MAX_PARTITION) {
        return HXE_UNKNOWN_PARTITION;
    }

    hx_seq_verdict verdict = HX_SEQ_APPEND;
    uint64_t offset = 0;
    hx_stream *st = NULL;
    hx_wire_err err = hx_producers_check(b->producers, clock_now(b), pid, (const char *)topic, tlen,
                                         partition, seq, &verdict, &offset, &st);
    if (err != HXE_OK) {
        return err;
    }
    if (verdict == HX_SEQ_DUPLICATE) {
        b->stats.deduped_records++;
    } else {
        err = produce_core(b, topic, tlen, partition, payload, plen, &offset);
        if (err != HXE_OK) {
            return err; /* the sequence is not consumed: the producer may retry it */
        }
        hx_producers_record(st, offset);
    }
    return hx_buf_put_u64(out, offset) == 0 ? HXE_OK : HXE_STORAGE;
}

/* Partition numbers on disk for one topic, ascending. */
static hx_wire_err topic_partitions(const hx_broker *b, const uint8_t *topic, size_t tlen,
                                    uint32_t *parts, size_t *n_out)
{
    DIR *d = opendir(b->data_dir);
    if (d == NULL) {
        return HXE_STORAGE;
    }
    size_t n = 0;
    const struct dirent *de = NULL;
    while ((de = readdir(d)) != NULL) {
        hx_tp m;
        if (parse_partition_dir(de->d_name, &m) && m.tlen == tlen &&
            memcmp(m.topic, topic, tlen) == 0 && n <= HX_MAX_PARTITION) {
            parts[n++] = m.partition;
        }
    }
    (void)closedir(d);
    for (size_t i = 1; i < n; i++) { /* insertion sort: at most 1024 entries, rare admin path */
        const uint32_t v = parts[i];
        size_t j = i;
        while (j > 0 && parts[j - 1] > v) {
            parts[j] = parts[j - 1];
            j--;
        }
        parts[j] = v;
    }
    *n_out = n;
    return HXE_OK;
}

/* Request: u16 tlen | topic | u32 partition_count (1..1024). Creates empty partitions 0..count-1.
 */
static hx_wire_err handle_create_topic(hx_broker *b, hx_rd *rd)
{
    const size_t tlen = hx_rd_u16(rd);
    const uint8_t *topic = hx_rd_take(rd, tlen);
    const uint32_t count = hx_rd_u32(rd);
    if (!rd->ok || hx_rd_left(rd) != 0 || count == 0 || count > HX_MAX_PARTITION + 1U) {
        return HXE_BAD_REQUEST;
    }
    if (!topic_valid(topic, tlen)) {
        return HXE_INVALID_TOPIC;
    }
    uint32_t existing[HX_MAX_PARTITION + 1U];
    size_t nexisting = 0;
    hx_wire_err err = topic_partitions(b, topic, tlen, existing, &nexisting);
    if (err != HXE_OK) {
        return err;
    }
    if (nexisting != 0) {
        return HXE_TOPIC_EXISTS;
    }
    if (b->nparts + count > b->cfg.max_open_logs) {
        return HXE_RESOURCE_LIMIT;
    }
    for (uint32_t i = 0; i < count; i++) {
        if (get_part(b, topic, tlen, i, true, &err) == NULL) {
            for (uint32_t j = 0; j <= i; j++) { /* roll back so a failed create leaves nothing */
                (void)destroy_partition(b, topic, tlen, j);
            }
            return err;
        }
    }
    const hx_gctx cx = make_gctx(b);
    hx_groups_topic_changed(b->groups, &cx, (const char *)topic, tlen);
    return HXE_OK;
}

/*
 * Write a tombstone for every committed offset of `topic`, make them durable, then drop them from
 * the table. Done before the data is removed: a crash in between loses offsets of a topic that
 * still exists (consumers re-read, at-least-once) instead of leaving offsets that outlive their
 * topic.
 */
static hx_wire_err forget_topic_offsets(hx_broker *b, const uint8_t *topic, size_t tlen)
{
    hx_wire_err err = HXE_OK;
    part *internal = NULL;
    size_t found = 0;
    for (size_t i = 0; i < HX_BUCKETS && err == HXE_OK; i++) {
        for (const oentry *e = b->offsets[i]; e != NULL && err == HXE_OK; e = e->next) {
            if (e->tlen != tlen || memcmp(e->key + e->glen, topic, tlen) != 0) {
                continue;
            }
            if (internal == NULL) {
                internal = get_part(b, (const uint8_t *)HX_INTERNAL_TOPIC,
                                    strlen(HX_INTERNAL_TOPIC), 0, true, &err);
                if (internal == NULL) {
                    return err;
                }
            }
            uint8_t rec[HX_OFFSET_REC_MAX];
            const size_t n = encode_offset_rec(rec, (const uint8_t *)e->key, e->glen, topic, tlen,
                                               e->partition, HX_TOMBSTONE);
            if (hx_log_append(internal->log, rec, (uint32_t)n, realtime_ms(), NULL) != HX_OK) {
                err = HXE_STORAGE;
            }
            found++;
        }
    }
    if (err == HXE_OK && internal != NULL && hx_log_sync(internal->log) != HX_OK) {
        err = HXE_STORAGE;
    }
    if (err != HXE_OK) {
        if (internal != NULL) {
            mark_dirty(b, internal);
        }
        return err;
    }
    if (found == 0) {
        return HXE_OK;
    }
    for (size_t i = 0; i < HX_BUCKETS; i++) {
        oentry *e = b->offsets[i];
        while (e != NULL) {
            oentry *next = e->next;
            if (e->tlen == tlen && memcmp(e->key + e->glen, topic, tlen) == 0) {
                offset_del(b, (const uint8_t *)e->key, e->glen, topic, tlen, e->partition);
            }
            e = next;
        }
    }
    return HXE_OK;
}

/* Request: u16 tlen | topic. Removes every partition, its committed offsets and producer state. */
static hx_wire_err handle_delete_topic(hx_broker *b, hx_rd *rd)
{
    const size_t tlen = hx_rd_u16(rd);
    const uint8_t *topic = hx_rd_take(rd, tlen);
    if (!rd->ok || hx_rd_left(rd) != 0) {
        return HXE_BAD_REQUEST;
    }
    if (!topic_valid(topic, tlen)) {
        return HXE_INVALID_TOPIC;
    }
    uint32_t parts[HX_MAX_PARTITION + 1U];
    size_t n = 0;
    hx_wire_err err = topic_partitions(b, topic, tlen, parts, &n);
    if (err != HXE_OK) {
        return err;
    }
    if (n == 0) {
        return HXE_UNKNOWN_PARTITION;
    }
    err = forget_topic_offsets(b, topic, tlen);
    if (err != HXE_OK) {
        return err;
    }
    for (size_t i = 0; i < n; i++) {
        if (destroy_partition(b, topic, tlen, parts[i]) != 0) {
            HX_ERROR("delete topic: cannot remove partition %u (errno=%d)", parts[i], errno);
            err = HXE_STORAGE;
        }
    }
    int dfd = open(b->data_dir, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        if (fsync(dfd) != 0) {
            err = HXE_STORAGE;
        }
        (void)close(dfd);
    }
    hx_producers_drop_topic(b->producers, (const char *)topic, tlen);
    const hx_gctx cx = make_gctx(b);
    hx_groups_topic_changed(b->groups, &cx, (const char *)topic, tlen);
    return err;
}

int hx_broker_handle(hx_broker *b, const uint8_t *req, size_t len, hx_buf *out)
{
    hx_rd rd;
    hx_rd_init(&rd, req, len);
    const uint16_t api = hx_rd_u16(&rd);
    const uint16_t version = hx_rd_u16(&rd);
    const uint32_t corr = hx_rd_u32(&rd);
    if (!rd.ok) {
        return -1;
    }

    const size_t start = out->len;
    /* Header placeholder: length(4) + correlation id(4) + error(2). */
    if (hx_buf_put_u32(out, 0) != 0 || hx_buf_put_u32(out, corr) != 0 ||
        hx_buf_put_u16(out, 0) != 0) {
        out->len = start;
        return -1;
    }
    const size_t body_start = out->len;

    hx_wire_err err = HXE_OK;
    if (version != HX_PROTO_VERSION) {
        err = HXE_UNSUPPORTED_VERSION;
    } else {
        switch (api) {
        case HX_API_PING:
            err = hx_rd_left(&rd) == 0 ? HXE_OK : HXE_BAD_REQUEST;
            break;
        case HX_API_PRODUCE:
            err = handle_produce(b, &rd, out);
            break;
        case HX_API_FETCH:
            err = handle_fetch(b, &rd, out);
            break;
        case HX_API_METADATA:
            err = handle_metadata(b, &rd, out);
            break;
        case HX_API_OFFSET_COMMIT:
            err = handle_offset_commit(b, &rd);
            break;
        case HX_API_OFFSET_FETCH:
            err = handle_offset_fetch(b, &rd, out);
            break;
        case HX_API_JOIN_GROUP:
            err = handle_join_group(b, &rd, out);
            break;
        case HX_API_HEARTBEAT:
            err = handle_heartbeat(b, &rd);
            break;
        case HX_API_LEAVE_GROUP:
            err = handle_leave_group(b, &rd);
            break;
        case HX_API_DESCRIBE_GROUP:
            err = handle_describe_group(b, &rd, out);
            break;
        case HX_API_GROUP_COMMIT:
            err = handle_group_commit(b, &rd);
            break;
        case HX_API_INIT_PRODUCER:
            err = handle_init_producer(b, &rd, out);
            break;
        case HX_API_PRODUCE_IDEMPOTENT:
            err = handle_produce_idempotent(b, &rd, out);
            break;
        case HX_API_CREATE_TOPIC:
            err = handle_create_topic(b, &rd);
            break;
        case HX_API_DELETE_TOPIC:
            err = handle_delete_topic(b, &rd);
            break;
        default:
            err = HXE_UNKNOWN_API;
            break;
        }
    }

    b->stats.requests++;
    if (err != HXE_OK) {
        b->stats.errors++;
        out->len = body_start; /* errors carry no body */
    }
    hx_buf_patch_u16(out, start + 8, (uint16_t)err);
    hx_buf_patch_u32(out, start, (uint32_t)(out->len - start - 4));
    return 0;
}
