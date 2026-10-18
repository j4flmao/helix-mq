/*
 * Helix storage engine — see log.h and docs/ARCHITECTURE.md §4.
 *
 * Frame layout (little-endian), 26-byte header + payload:
 *   [0..4)   crc32c   over bytes [4 .. frame_len)
 *   [4..8)   length   bytes after this field = 18 + payload_len
 *   [8]      magic
 *   [9]      attrs    (reserved: codec / tombstone bits)
 *   [10..18) offset   u64
 *   [18..26) ts_ms    i64
 *   [26..)   payload
 */
#include "log.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define HX_MAGIC     0xA1u
#define HX_HDR       26u /* full header incl. crc+length */
#define HX_LEN_FIXED 18u /* bytes covered by `length` besides the payload */

/* read_frame() results */
enum { FR_OK = 0, FR_EOF, FR_TORN, FR_BAD, FR_TOOBIG, FR_IOERR };

typedef struct {
    uint64_t offset;
    uint64_t pos;
} idx_entry;

typedef struct {
    uint64_t base; /* offset of first record                        */
    uint64_t next; /* offset the next record in this segment gets   */
    uint64_t size; /* valid bytes                                   */
    uint64_t last_idx_pos;
    int fd;
    idx_entry *idx;
    size_t idx_n, idx_cap;
} segment;

struct hx_log {
    char *dir;
    int dir_fd;
    hx_log_opts opts;
    segment *seg;
    size_t nseg, capseg;
    uint64_t next_offset;
    uint8_t *scratch; /* HX_HDR + max_record_bytes */
    int64_t last_sync_ms;
    int dirty; /* group commit: appended but not yet fdatasync'ed */
};

/* ------------------------------------------------------------------ utils */

static void put_u32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++) {
        p[i] = (uint8_t)(v >> (8 * i));
    }
}
static void put_u64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) {
        p[i] = (uint8_t)(v >> (8 * i));
    }
}
static uint32_t get_u32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 3; i >= 0; i--) {
        v = (v << 8) | p[i];
    }
    return v;
}
static uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--) {
        v = (v << 8) | p[i];
    }
    return v;
}

static int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int pread_full(int fd, void *buf, size_t n, uint64_t pos)
{
    uint8_t *p = buf;
    while (n) {
        ssize_t r = pread(fd, p, n, (off_t)pos);
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (r == 0) {
            return -1; /* unexpected EOF */
        }
        p += r;
        n -= (size_t)r;
        pos += (uint64_t)r;
    }
    return 0;
}

static int pwrite_full(int fd, const void *buf, size_t n, uint64_t pos)
{
    const uint8_t *p = buf;
    while (n) {
        ssize_t w = pwrite(fd, p, n, (off_t)pos);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += w;
        n -= (size_t)w;
        pos += (uint64_t)w;
    }
    return 0;
}

static int fdatasync_retry(int fd)
{
    int r;
    do {
        r = fdatasync(fd);
    } while (r < 0 && errno == EINTR);
    return r;
}

static int fsync_retry(int fd)
{
    int r;
    do {
        r = fsync(fd);
    } while (r < 0 && errno == EINTR);
    return r;
}

static void seg_path(const hx_log *l, uint64_t base, char *out, size_t cap)
{
    snprintf(out, cap, "%s/%020llu.log", l->dir, (unsigned long long)base);
}

const char *hx_strerror(hx_err e)
{
    switch (e) {
    case HX_OK:
        return "ok";
    case HX_ERR_INVAL:
        return "invalid argument";
    case HX_ERR_IO:
        return "I/O error";
    case HX_ERR_NOMEM:
        return "out of memory";
    case HX_ERR_CORRUPT:
        return "corruption detected";
    case HX_ERR_NOT_FOUND:
        return "offset not found";
    case HX_ERR_TOO_LARGE:
        return "record too large";
    case HX_ERR_BUF_SMALL:
        return "buffer too small";
    }
    return "unknown error";
}

void hx_log_opts_default(hx_log_opts *o)
{
    o->segment_bytes = 64ull << 20;
    o->max_record_bytes = 1u << 20;
    o->index_interval_bytes = 4096;
    o->sync = HX_SYNC_INTERVAL;
    o->sync_interval_ms = 200;
    o->group_commit = 0;
}

/* ----------------------------------------------------------- frame reader */

/*
 * Read and validate the frame at `pos`. On FR_OK the whole frame is in
 * `scratch`; *frame_len is its size.
 * FR_EOF  : pos == size (clean end)
 * FR_TORN : incomplete frame at the end of the file (torn write)
 * FR_BAD  : bad length / magic / CRC
 * FR_TOOBIG: structurally complete frame larger than max_payload. This is NOT
 *            treated as a torn write: the configured limit was probably
 *            lowered after data was written, and truncating would destroy
 *            valid records. Callers fail with HX_ERR_CORRUPT instead.
 */
static int read_frame(int fd, uint64_t pos, uint64_t size, uint32_t max_payload, uint8_t *scratch,
                      uint32_t *frame_len)
{
    if (pos >= size) {
        return FR_EOF;
    }
    if (size - pos < 8) {
        return FR_TORN;
    }

    if (pread_full(fd, scratch, 8, pos) < 0) {
        return FR_IOERR;
    }
    uint32_t length = get_u32(scratch + 4);
    if (length < HX_LEN_FIXED) {
        return FR_BAD;
    }

    uint64_t flen = 8ull + length;
    if (size - pos < flen) {
        return FR_TORN;
    }
    if (length - HX_LEN_FIXED > max_payload) {
        return FR_TOOBIG;
    }

    if (pread_full(fd, scratch + 8, (size_t)flen - 8, pos + 8) < 0) {
        return FR_IOERR;
    }

    if (get_u32(scratch) != hx_crc32c(0, scratch + 4, (size_t)flen - 4)) {
        return FR_BAD;
    }
    if (scratch[8] != HX_MAGIC) {
        return FR_BAD;
    }

    *frame_len = (uint32_t)flen;
    return FR_OK;
}

/* ------------------------------------------------------------ segments */

static hx_err idx_add(segment *s, uint64_t offset, uint64_t pos)
{
    if (s->idx_n == s->idx_cap) {
        size_t nc = s->idx_cap ? s->idx_cap * 2 : 64;
        idx_entry *n = realloc(s->idx, nc * sizeof *n);
        if (!n) {
            return HX_ERR_NOMEM;
        }
        s->idx = n;
        s->idx_cap = nc;
    }
    s->idx[s->idx_n++] = (idx_entry){offset, pos};
    s->last_idx_pos = pos;
    return HX_OK;
}

static hx_err log_reserve_seg(hx_log *l)
{
    if (l->nseg < l->capseg) {
        return HX_OK;
    }
    size_t nc = l->capseg ? l->capseg * 2 : 8;
    segment *n = realloc(l->seg, nc * sizeof *n);
    if (!n) {
        return HX_ERR_NOMEM;
    }
    l->seg = n;
    l->capseg = nc;
    return HX_OK;
}

/* Open file for segment `base`, scan it, build the index, and (for the tail)
 * truncate any torn/corrupt suffix. Appends to l->seg on success. */
static hx_err load_segment(hx_log *l, uint64_t base, int create, int is_tail)
{
    hx_err e = log_reserve_seg(l);
    if (e) {
        return e;
    }

    char path[4096];
    seg_path(l, base, path, sizeof path);
    int flags = O_RDWR | O_CLOEXEC | (create ? (O_CREAT | O_EXCL) : 0);
    int fd = open(path, flags, 0644);
    if (fd < 0) {
        return HX_ERR_IO;
    }

    segment s = {.base = base, .next = base, .fd = fd};
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return HX_ERR_IO;
    }
    uint64_t fsize = (uint64_t)st.st_size, pos = 0;

    for (;;) {
        uint32_t flen = 0;
        int r = read_frame(fd, pos, fsize, l->opts.max_record_bytes, l->scratch, &flen);
        if (r == FR_OK) {
            if (get_u64(l->scratch + 10) != s.next) {
                r = FR_BAD; /* not dense */
            }
        }
        if (r == FR_OK) {
            if (s.idx_n == 0 || pos - s.last_idx_pos >= l->opts.index_interval_bytes) {
                e = idx_add(&s, s.next, pos);
                if (e != HX_OK) {
                    goto fail;
                }
            }
            pos += flen;
            s.next++;
            continue;
        }
        if (r == FR_EOF) {
            break;
        }
        if (r == FR_IOERR) {
            e = HX_ERR_IO;
            goto fail;
        }
        if (r == FR_TOOBIG) {
            e = HX_ERR_CORRUPT;
            goto fail;
        } /* never truncate */
        /* torn or corrupt frame at `pos` */
        if (!is_tail) {
            e = HX_ERR_CORRUPT;
            goto fail;
        }
        if (ftruncate(fd, (off_t)pos) < 0 || fsync_retry(fd) < 0) {
            e = HX_ERR_IO;
            goto fail;
        }
        break;
    }
    s.size = pos;
    l->seg[l->nseg++] = s;
    return HX_OK;

fail:
    free(s.idx);
    close(fd);
    return e;
}

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

static hx_err discover_bases(const char *dir, uint64_t **out, size_t *n_out)
{
    DIR *d = opendir(dir);
    if (!d) {
        return HX_ERR_IO;
    }
    uint64_t *v = NULL;
    size_t n = 0, cap = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        const char *nm = de->d_name;
        size_t len = strlen(nm);
        if (len != 24 || strcmp(nm + 20, ".log") != 0) {
            continue;
        }
        int ok = 1;
        for (int i = 0; i < 20; i++) {
            if (nm[i] < '0' || nm[i] > '9') {
                ok = 0;
                break;
            }
        }
        if (!ok) {
            continue;
        }
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 16;
            uint64_t *t = realloc(v, nc * sizeof *t);
            if (!t) {
                free(v);
                closedir(d);
                return HX_ERR_NOMEM;
            }
            v = t;
            cap = nc;
        }
        v[n++] = strtoull(nm, NULL, 10);
    }
    closedir(d);
    if (n) {
        qsort(v, n, sizeof *v, cmp_u64);
    }
    *out = v;
    *n_out = n;
    return HX_OK;
}

/* ------------------------------------------------------------- public API */

hx_err hx_log_open(const char *dir, const hx_log_opts *opts, hx_log **out)
{
    if (!dir || !out) {
        return HX_ERR_INVAL;
    }
    hx_log_opts o;
    if (opts) {
        o = *opts;
    } else {
        hx_log_opts_default(&o);
    }
    if (o.segment_bytes < HX_HDR + 1 || o.max_record_bytes == 0 || o.index_interval_bytes == 0) {
        return HX_ERR_INVAL;
    }

    if (mkdir(dir, 0755) < 0 && errno != EEXIST) {
        return HX_ERR_IO;
    }

    hx_log *l = calloc(1, sizeof *l);
    if (!l) {
        return HX_ERR_NOMEM;
    }
    l->opts = o;
    l->dir = strdup(dir);
    l->scratch = malloc((size_t)HX_HDR + o.max_record_bytes);
    l->dir_fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    l->last_sync_ms = now_ms();
    hx_err e = HX_OK;
    uint64_t *bases = NULL;
    size_t nb = 0;

    if (!l->dir || !l->scratch) {
        e = HX_ERR_NOMEM;
        goto fail;
    }
    if (l->dir_fd < 0) {
        e = HX_ERR_IO;
        goto fail;
    }
    e = discover_bases(dir, &bases, &nb);
    if (e != HX_OK) {
        goto fail;
    }

    for (size_t i = 0; i < nb; i++) {
        if (i > 0 && bases[i] != l->seg[i - 1].next) {
            e = HX_ERR_CORRUPT; /* gap or overlap between segments */
            goto fail;
        }
        e = load_segment(l, bases[i], 0, i + 1 == nb);
        if (e != HX_OK) {
            goto fail;
        }
    }
    if (nb == 0) {
        e = load_segment(l, 0, 1, 1);
        if (e != HX_OK) {
            goto fail;
        }
        if (fsync_retry(l->dir_fd) < 0) {
            e = HX_ERR_IO;
            goto fail;
        }
    }
    l->next_offset = l->seg[l->nseg - 1].next;
    free(bases);
    *out = l;
    return HX_OK;

fail:
    free(bases);
    hx_log_close(l);
    return e;
}

void hx_log_close(hx_log *l)
{
    if (!l) {
        return;
    }
    if (l->nseg && l->opts.sync != HX_SYNC_NEVER) {
        (void)fdatasync_retry(l->seg[l->nseg - 1].fd);
    }
    for (size_t i = 0; i < l->nseg; i++) {
        close(l->seg[i].fd);
        free(l->seg[i].idx);
    }
    if (l->dir_fd >= 0) {
        close(l->dir_fd);
    }
    free(l->seg);
    free(l->scratch);
    free(l->dir);
    free(l);
}

hx_err hx_log_sync(hx_log *l)
{
    if (!l) {
        return HX_ERR_INVAL;
    }
    if (fdatasync_retry(l->seg[l->nseg - 1].fd) < 0) {
        return HX_ERR_IO;
    }
    l->last_sync_ms = now_ms();
    l->dirty = 0;
    return HX_OK;
}

hx_err hx_log_flush(hx_log *l)
{
    if (!l) {
        return HX_ERR_INVAL;
    }
    return l->dirty != 0 ? hx_log_sync(l) : HX_OK;
}

int hx_log_is_dirty(const hx_log *l)
{
    return l->dirty;
}

static hx_err roll_segment(hx_log *l)
{
    /* Seal the old segment durably before the new one becomes visible. */
    if (fdatasync_retry(l->seg[l->nseg - 1].fd) < 0) {
        return HX_ERR_IO;
    }
    hx_err e = load_segment(l, l->next_offset, 1, 1);
    if (e) {
        return e;
    }
    if (fsync_retry(l->dir_fd) < 0) {
        return HX_ERR_IO; /* persist the new name */
    }
    return HX_OK;
}

hx_err hx_log_append(hx_log *l, const void *payload, uint32_t len, int64_t ts_ms,
                     uint64_t *out_offset)
{
    if (!l || (!payload && len)) {
        return HX_ERR_INVAL;
    }
    if (len > l->opts.max_record_bytes) {
        return HX_ERR_TOO_LARGE;
    }

    uint32_t flen = HX_HDR + len;
    segment *s = &l->seg[l->nseg - 1];
    if (s->size > 0 && s->size + flen > l->opts.segment_bytes) {
        hx_err e = roll_segment(l);
        if (e) {
            return e;
        }
        s = &l->seg[l->nseg - 1]; /* array may have moved */
    }

    uint8_t *f = l->scratch;
    put_u32(f + 4, HX_LEN_FIXED + len);
    f[8] = HX_MAGIC;
    f[9] = 0;
    put_u64(f + 10, l->next_offset);
    put_u64(f + 18, (uint64_t)ts_ms);
    if (len) {
        memcpy(f + HX_HDR, payload, len);
    }
    put_u32(f, hx_crc32c(0, f + 4, flen - 4));

    if (pwrite_full(s->fd, f, flen, s->size) < 0) {
        /* Best effort: drop a partial frame. If this also fails, recovery
         * truncates the torn tail on the next open. */
        if (ftruncate(s->fd, (off_t)s->size) < 0) {
            /* nothing more to do */
        }
        return HX_ERR_IO;
    }

    if (s->idx_n == 0 || s->size - s->last_idx_pos >= l->opts.index_interval_bytes) {
        /* A failed index insert only slows reads; data is already safe. */
        (void)idx_add(s, l->next_offset, s->size);
    }

    s->size += flen;
    s->next++;
    if (out_offset) {
        *out_offset = l->next_offset;
    }
    l->next_offset++;

    if (l->opts.sync == HX_SYNC_ALWAYS && l->opts.group_commit != 0) {
        l->dirty = 1;
        return HX_OK;
    }
    const int sync_now = l->opts.sync == HX_SYNC_ALWAYS ||
                         (l->opts.sync == HX_SYNC_INTERVAL &&
                          now_ms() - l->last_sync_ms >= (int64_t)l->opts.sync_interval_ms);
    return sync_now != 0 ? hx_log_sync(l) : HX_OK;
}

hx_err hx_log_read(hx_log *l, uint64_t offset, void *buf, size_t cap, uint32_t *out_len,
                   int64_t *out_ts_ms)
{
    if (!l || !out_len || (!buf && cap)) {
        return HX_ERR_INVAL;
    }
    if (offset < l->seg[0].base || offset >= l->next_offset) {
        return HX_ERR_NOT_FOUND;
    }

    /* last segment with base <= offset */
    size_t lo = 0, hi = l->nseg;
    while (hi - lo > 1) {
        size_t mid = lo + (hi - lo) / 2;
        if (l->seg[mid].base <= offset) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    segment *s = &l->seg[lo];

    /* greatest index entry with entry.offset <= offset */
    uint64_t pos = 0;
    size_t a = 0, b = s->idx_n;
    while (a < b) {
        size_t mid = a + (b - a) / 2;
        if (s->idx[mid].offset <= offset) {
            a = mid + 1;
        } else {
            b = mid;
        }
    }
    if (a > 0) {
        pos = s->idx[a - 1].pos;
    }

    for (;;) {
        uint32_t flen = 0;
        int r = read_frame(s->fd, pos, s->size, l->opts.max_record_bytes, l->scratch, &flen);
        if (r == FR_IOERR) {
            return HX_ERR_IO;
        }
        if (r != FR_OK) {
            return HX_ERR_CORRUPT;
        }
        uint64_t off = get_u64(l->scratch + 10);
        if (off == offset) {
            uint32_t plen = flen - HX_HDR;
            *out_len = plen;
            if (cap < plen) {
                return HX_ERR_BUF_SMALL;
            }
            if (plen) {
                memcpy(buf, l->scratch + HX_HDR, plen);
            }
            if (out_ts_ms) {
                *out_ts_ms = (int64_t)get_u64(l->scratch + 18);
            }
            return HX_OK;
        }
        if (off > offset) {
            return HX_ERR_CORRUPT;
        }
        pos += flen;
    }
}

/* Byte position of the frame holding `offset` inside segment s (offset < s->next). */
static hx_err seg_find_pos(hx_log *l, const segment *s, uint64_t offset, uint64_t *pos_out)
{
    uint64_t pos = 0;
    size_t a = 0;
    size_t b = s->idx_n;
    while (a < b) {
        size_t mid = a + (b - a) / 2;
        if (s->idx[mid].offset <= offset) {
            a = mid + 1;
        } else {
            b = mid;
        }
    }
    if (a > 0) {
        pos = s->idx[a - 1].pos;
    }
    for (;;) {
        uint32_t flen = 0;
        int r = read_frame(s->fd, pos, s->size, l->opts.max_record_bytes, l->scratch, &flen);
        if (r == FR_IOERR) {
            return HX_ERR_IO;
        }
        if (r != FR_OK) {
            return HX_ERR_CORRUPT;
        }
        const uint64_t off = get_u64(l->scratch + 10);
        if (off == offset) {
            *pos_out = pos;
            return HX_OK;
        }
        if (off > offset) {
            return HX_ERR_CORRUPT;
        }
        pos += flen;
    }
}

hx_err hx_log_truncate(hx_log *l, uint64_t new_next)
{
    if (!l || new_next < l->seg[0].base || new_next > l->next_offset) {
        return HX_ERR_INVAL;
    }
    if (new_next == l->next_offset) {
        return HX_OK;
    }
    /* Whole segments past the cut, newest first; every step is persisted before the next. */
    while (l->nseg > 1 && l->seg[l->nseg - 1].base >= new_next) {
        segment *s = &l->seg[l->nseg - 1];
        char path[4096];
        seg_path(l, s->base, path, sizeof path);
        if (unlink(path) < 0 || fsync_retry(l->dir_fd) < 0) {
            return HX_ERR_IO;
        }
        close(s->fd);
        free(s->idx);
        l->nseg--;
        l->next_offset = l->seg[l->nseg - 1].next;
    }
    segment *s = &l->seg[l->nseg - 1];
    if (new_next < s->next) {
        uint64_t pos = 0;
        hx_err e = seg_find_pos(l, s, new_next, &pos);
        if (e != HX_OK) {
            return e;
        }
        if (ftruncate(s->fd, (off_t)pos) < 0 || fsync_retry(s->fd) < 0) {
            return HX_ERR_IO;
        }
        s->size = pos;
        s->next = new_next;
        while (s->idx_n > 0 && s->idx[s->idx_n - 1].offset >= new_next) {
            s->idx_n--;
        }
        s->last_idx_pos = s->idx_n > 0 ? s->idx[s->idx_n - 1].pos : 0;
    }
    l->next_offset = new_next;
    l->dirty = 0;
    return HX_OK;
}

hx_err hx_log_retain_bytes(hx_log *l, uint64_t max_bytes)
{
    if (!l) {
        return HX_ERR_INVAL;
    }
    uint64_t total = 0;
    for (size_t i = 0; i < l->nseg; i++) {
        total += l->seg[i].size;
    }

    int removed = 0;
    while (l->nseg > 1 && total > max_bytes) {
        char path[4096];
        seg_path(l, l->seg[0].base, path, sizeof path);
        if (unlink(path) < 0) {
            return HX_ERR_IO;
        }
        total -= l->seg[0].size;
        close(l->seg[0].fd);
        free(l->seg[0].idx);
        memmove(&l->seg[0], &l->seg[1], (l->nseg - 1) * sizeof l->seg[0]);
        l->nseg--;
        removed = 1;
    }
    if (removed && fsync_retry(l->dir_fd) < 0) {
        return HX_ERR_IO;
    }
    return HX_OK;
}

uint64_t hx_log_first_offset(const hx_log *l)
{
    return l->seg[0].base;
}
uint64_t hx_log_next_offset(const hx_log *l)
{
    return l->next_offset;
}
size_t hx_log_segment_count(const hx_log *l)
{
    return l->nseg;
}
