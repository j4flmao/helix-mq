#include "repl/repl_log.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    uint32_t epoch;
    uint64_t first; /* first offset written under this epoch */
} run;

struct hx_plog {
    hx_log *log;
    run *runs;
    size_t nruns;
    size_t capruns;
    uint32_t max_payload;
    uint8_t *wbuf; /* 4 + max_payload */
    uint8_t *arena;
    size_t arena_cap;
    bool nosync;
};

static uint32_t get_le32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void put_le32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static int add_run(hx_plog *p, uint32_t epoch, uint64_t first)
{
    if (p->nruns > 0 && p->runs[p->nruns - 1U].epoch == epoch) {
        return 0;
    }
    if (p->nruns > 0 && p->runs[p->nruns - 1U].epoch > epoch) {
        return -1;
    }
    if (p->nruns == p->capruns) {
        const size_t ncap = p->capruns == 0 ? 16U : p->capruns * 2U;
        run *r = realloc(p->runs, ncap * sizeof *r);
        if (r == NULL) {
            return -1;
        }
        p->runs = r;
        p->capruns = ncap;
    }
    p->runs[p->nruns].epoch = epoch;
    p->runs[p->nruns].first = first;
    p->nruns++;
    return 0;
}

void hx_plog_close(hx_plog *p)
{
    if (p != NULL) {
        hx_log_close(p->log);
        free(p->runs);
        free(p->wbuf);
        free(p->arena);
        free(p);
    }
}

hx_err hx_plog_open(const char *dir, const hx_log_opts *opts, hx_plog **out)
{
    hx_log_opts o;
    if (opts != NULL) {
        o = *opts;
    } else {
        hx_log_opts_default(&o);
    }
    if (o.max_record_bytes < 4U) {
        return HX_ERR_INVAL;
    }
    o.sync = HX_SYNC_NEVER; /* durability is explicit: hx_repl calls sync() */
    hx_plog *p = calloc(1, sizeof *p);
    if (p == NULL) {
        return HX_ERR_NOMEM;
    }
    p->max_payload = o.max_record_bytes - 4U;
    p->wbuf = malloc(o.max_record_bytes);
    hx_err e = p->wbuf == NULL ? HX_ERR_NOMEM : hx_log_open(dir, &o, &p->log);
    if (e == HX_OK && hx_log_first_offset(p->log) != 0) {
        e = HX_ERR_INVAL; /* retention on replicated logs is not supported yet */
    }
    uint8_t *tmp = e == HX_OK ? malloc(o.max_record_bytes) : NULL;
    if (e == HX_OK && tmp == NULL) {
        e = HX_ERR_NOMEM;
    }
    for (uint64_t off = 0; e == HX_OK && off < hx_log_next_offset(p->log); off++) {
        uint32_t len = 0;
        e = hx_log_read(p->log, off, tmp, o.max_record_bytes, &len, NULL);
        if (e == HX_OK && (len < 4U || add_run(p, get_le32(tmp), off) != 0)) {
            e = HX_ERR_CORRUPT; /* missing prefix or epochs going backwards */
        }
    }
    free(tmp);
    if (e != HX_OK) {
        hx_plog_close(p);
        return e;
    }
    *out = p;
    return HX_OK;
}

uint64_t hx_plog_leo(const hx_plog *p)
{
    return hx_log_next_offset(p->log);
}

hx_err hx_plog_read_payload(hx_plog *p, uint64_t offset, void *buf, size_t cap, uint32_t *out_len,
                            uint32_t *out_epoch)
{
    uint32_t len = 0;
    hx_err e = hx_log_read(p->log, offset, p->wbuf, p->max_payload + 4U, &len, NULL);
    if (e != HX_OK) {
        return e;
    }
    if (len < 4U) {
        return HX_ERR_CORRUPT;
    }
    *out_len = len - 4U;
    if (out_epoch != NULL) {
        *out_epoch = get_le32(p->wbuf);
    }
    if (cap < len - 4U) {
        return HX_ERR_BUF_SMALL;
    }
    if (len > 4U) {
        memcpy(buf, p->wbuf + 4, len - 4U);
    }
    return HX_OK;
}

/* ------------------------------------------------------------------ vtable */

static uint64_t v_leo(void *ctx)
{
    return hx_plog_leo(ctx);
}

static int v_epoch_at(void *ctx, uint64_t off, uint32_t *epoch)
{
    const hx_plog *p = ctx;
    if (off >= hx_plog_leo(p) || p->nruns == 0) {
        return -1;
    }
    size_t lo = 0;
    size_t hi = p->nruns; /* last run with first <= off */
    while (hi - lo > 1U) {
        const size_t mid = lo + (hi - lo) / 2U;
        if (p->runs[mid].first <= off) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    *epoch = p->runs[lo].epoch;
    return 0;
}

static uint64_t v_end_for_epoch(void *ctx, uint32_t epoch)
{
    const hx_plog *p = ctx;
    for (size_t i = 0; i < p->nruns; i++) {
        if (p->runs[i].epoch > epoch) {
            return p->runs[i].first;
        }
    }
    return hx_plog_leo(p);
}

static int v_append(void *ctx, const hx_repl_rec *recs, size_t n)
{
    hx_plog *p = ctx;
    for (size_t i = 0; i < n; i++) {
        if (recs[i].len > p->max_payload) {
            return -1;
        }
        const uint64_t off = hx_plog_leo(p);
        if (p->nruns > 0 && p->runs[p->nruns - 1U].epoch > recs[i].epoch) {
            return -1;
        }
        put_le32(p->wbuf, recs[i].epoch);
        if (recs[i].len != 0) {
            memcpy(p->wbuf + 4, recs[i].data, recs[i].len);
        }
        uint64_t got = 0;
        if (hx_log_append(p->log, p->wbuf, 4U + recs[i].len, 0, &got) != HX_OK || got != off) {
            return -1;
        }
        if (add_run(p, recs[i].epoch, off) != 0) {
            return -1;
        }
    }
    return 0;
}

static int v_truncate(void *ctx, uint64_t new_leo)
{
    hx_plog *p = ctx;
    if (hx_log_truncate(p->log, new_leo) != HX_OK) {
        return -1;
    }
    while (p->nruns > 0 && p->runs[p->nruns - 1U].first >= new_leo) {
        p->nruns--;
    }
    return 0;
}

static int v_read(void *ctx, uint64_t off, size_t max_n, size_t max_bytes, hx_repl_rec *out,
                  size_t *n)
{
    hx_plog *p = ctx;
    const uint64_t leo = hx_plog_leo(p);
    const size_t slot = (size_t)p->max_payload + 4U;
    size_t used = 0;
    size_t k = 0;
    size_t bytes = 0;
    /* Offsets into the arena first: it may move while it grows. */
    size_t offs[256];
    if (max_n > 256U) {
        max_n = 256U;
    }
    while (off + k < leo && k < max_n) {
        if (p->arena_cap - used < slot) {
            size_t ncap = p->arena_cap == 0 ? slot * 4U : p->arena_cap * 2U;
            while (ncap - used < slot) {
                ncap *= 2U;
            }
            uint8_t *a = realloc(p->arena, ncap);
            if (a == NULL) {
                return -1;
            }
            p->arena = a;
            p->arena_cap = ncap;
        }
        uint32_t len = 0;
        if (hx_log_read(p->log, off + k, p->arena + used, slot, &len, NULL) != HX_OK || len < 4U) {
            return -1;
        }
        if (k > 0 && bytes + (len - 4U) > max_bytes) {
            break;
        }
        out[k].epoch = get_le32(p->arena + used);
        out[k].len = len - 4U;
        offs[k] = used + 4U;
        bytes += len - 4U;
        used += len;
        k++;
    }
    for (size_t i = 0; i < k; i++) {
        out[i].data = p->arena + offs[i];
    }
    *n = k;
    return 0;
}

void hx_plog_set_nosync_for_tests(hx_plog *p, bool nosync)
{
    p->nosync = nosync;
}

static int v_sync(void *ctx)
{
    hx_plog *p = ctx;
    if (p->nosync) {
        return 0;
    }
    return hx_log_sync(p->log) == HX_OK ? 0 : -1;
}

hx_repl_log hx_plog_vtable(hx_plog *p)
{
    hx_repl_log v;
    memset(&v, 0, sizeof v);
    v.ctx = p;
    v.leo = v_leo;
    v.epoch_at = v_epoch_at;
    v.end_offset_for_epoch = v_end_for_epoch;
    v.append = v_append;
    v.truncate = v_truncate;
    v.read = v_read;
    v.sync = v_sync;
    return v;
}
