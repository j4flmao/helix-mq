#include "raft/raft_wal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "storage/log.h" /* hx_crc32c */
#include "util/buf.h"

#define WAL_MAGIC        0x48585257U /* "HXRW" */
#define WAL_VERSION      1U
#define WAL_HEADER_LEN   8U
#define WAL_REC_HDR      8U /* u32 length + u32 crc */
#define WAL_MAX_RECORD   (64U << 20)
#define WAL_COMPACT_MIN  (1U << 20)
#define WAL_TYPE_HARD    1U
#define WAL_TYPE_ENTRIES 2U
#define WAL_TYPE_SNAP    3U
#define WAL_PATH_MAX     4096U

typedef struct {
    uint64_t term;
    uint32_t len;
    uint8_t *data;
} went;

struct hx_raft_wal {
    int fd;
    char *path;
    uint64_t size;   /* bytes in the file (== write offset) */
    uint64_t synced; /* prefix known durable */
    uint64_t live;   /* size a freshly compacted file would have */
    bool broken;     /* a write failed: refuse further use */
    bool nosync;     /* tests only: skip the fdatasync syscall */

    uint64_t term;
    uint32_t voted_for;
    went *ents; /* entry with index i lives at ents[i - base - 1] */
    size_t nents;
    size_t cap;
    uint64_t base; /* snapshot point: entries <= base are folded into `snap` */
    uint64_t base_term;
    uint8_t *snap;
    uint32_t snap_len;

    hx_raft_entry *view; /* what hx_raft_wal_restore() hands out */
};

/* -------------------------------------------------------------- helpers */

static void put32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static uint32_t get32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static uint64_t get64(const uint8_t *p)
{
    return ((uint64_t)get32(p) << 32) | get32(p + 4);
}

static int write_all(int fd, const uint8_t *p, size_t n, uint64_t off)
{
    while (n > 0) {
        const ssize_t w = pwrite(fd, p, n, (off_t)off);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += w;
        n -= (size_t)w;
        off += (uint64_t)w;
    }
    return 0;
}

static int read_all(int fd, uint8_t *p, size_t n, uint64_t off)
{
    while (n > 0) {
        const ssize_t r = pread(fd, p, n, (off_t)off);
        if (r < 0 && errno == EINTR) {
            continue;
        }
        if (r <= 0) {
            return -1;
        }
        p += r;
        n -= (size_t)r;
        off += (uint64_t)r;
    }
    return 0;
}

static int sync_dir_of(const char *path)
{
    char dir[WAL_PATH_MAX];
    if (snprintf(dir, sizeof dir, "%s", path) >= (int)sizeof dir) {
        return -1;
    }
    char *slash = strrchr(dir, '/');
    if (slash == NULL) {
        (void)snprintf(dir, sizeof dir, ".");
    } else if (slash == dir) {
        dir[1] = '\0';
    } else {
        *slash = '\0';
    }
    const int dfd = open(dir, O_RDONLY | O_DIRECTORY);
    if (dfd < 0) {
        return -1;
    }
    const int rc = fsync(dfd);
    (void)close(dfd);
    return rc;
}

/* Appends one framed record to `b`. */
static int frame(hx_buf *b, uint8_t type, const hx_buf *payload)
{
    uint8_t hdr[WAL_REC_HDR + 1];
    const uint32_t len = (uint32_t)payload->len + 1U;
    put32(hdr, len);
    hdr[8] = type;
    uint32_t crc = hx_crc32c(0, &type, 1);
    crc = hx_crc32c(crc, payload->data, payload->len);
    put32(hdr + 4, crc);
    return (hx_buf_put(b, hdr, sizeof hdr) == 0 && hx_buf_put(b, payload->data, payload->len) == 0)
               ? 0
               : -1;
}

static void free_entries_from(hx_raft_wal *w, size_t keep)
{
    while (w->nents > keep) {
        free(w->ents[--w->nents].data);
    }
}

static int push_entry(hx_raft_wal *w, uint64_t term, const uint8_t *data, uint32_t len)
{
    if (w->nents == w->cap) {
        const size_t ncap = w->cap == 0 ? 64U : w->cap * 2U;
        went *g = realloc(w->ents, ncap * sizeof *g);
        if (g == NULL) {
            return -1;
        }
        w->ents = g;
        w->cap = ncap;
    }
    uint8_t *copy = malloc(len != 0 ? len : 1U);
    if (copy == NULL) {
        return -1;
    }
    if (len != 0) {
        memcpy(copy, data, len);
    }
    w->ents[w->nents].term = term;
    w->ents[w->nents].len = len;
    w->ents[w->nents].data = copy;
    w->nents++;
    return 0;
}

static void drop_prefix_ents(hx_raft_wal *w, size_t k)
{
    for (size_t i = 0; i < k; i++) {
        free(w->ents[i].data);
    }
    memmove(w->ents, w->ents + k, (w->nents - k) * sizeof *w->ents);
    w->nents -= k;
}

/* Same rule for replay and for live saves. CORRUPT if the snapshot goes backwards. */
static hx_wal_status apply_snapshot(hx_raft_wal *w, uint64_t index, uint64_t term,
                                    const uint8_t *data, uint32_t len)
{
    if (index <= w->base) {
        return HX_WAL_CORRUPT;
    }
    uint8_t *copy = malloc(len != 0 ? len : 1U);
    if (copy == NULL) {
        return HX_WAL_NOMEM;
    }
    if (len != 0) {
        memcpy(copy, data, len);
    }
    const uint64_t k = index - w->base;
    if (k <= w->nents && w->ents[k - 1U].term == term) {
        drop_prefix_ents(w, (size_t)k);
    } else {
        free_entries_from(w, 0);
    }
    free(w->snap);
    w->snap = copy;
    w->snap_len = len;
    w->base = index;
    w->base_term = term;
    return HX_WAL_OK;
}

/* Size of a freshly compacted file holding the current state. */
static uint64_t live_size(const hx_raft_wal *w)
{
    uint64_t n = WAL_HEADER_LEN + WAL_REC_HDR + 1U + 12U; /* header + HARD record */
    if (w->base != 0) {
        n += WAL_REC_HDR + 1U + 20U + w->snap_len;
    }
    n += WAL_REC_HDR + 1U + 12U; /* ENTRIES record framing */
    for (size_t i = 0; i < w->nents; i++) {
        n += 12U + w->ents[i].len;
    }
    return n;
}

/* ------------------------------------------------------------------ replay */

/* True if every byte in [from, to) is zero: the signature of a file extended but never written. */
static bool region_is_zero(int fd, uint64_t from, uint64_t to)
{
    uint8_t buf[4096];
    while (from < to) {
        const size_t n = to - from < sizeof buf ? (size_t)(to - from) : sizeof buf;
        if (read_all(fd, buf, n, from) != 0) {
            return false;
        }
        for (size_t i = 0; i < n; i++) {
            if (buf[i] != 0) {
                return false;
            }
        }
        from += n;
    }
    return true;
}

static hx_wal_status apply_record(hx_raft_wal *w, uint8_t type, const uint8_t *p, size_t n)
{
    if (type == WAL_TYPE_HARD) {
        if (n != 12U) {
            return HX_WAL_CORRUPT;
        }
        w->term = get64(p);
        w->voted_for = get32(p + 8);
        return HX_WAL_OK;
    }
    if (type == WAL_TYPE_SNAP) {
        if (n < 20U || get32(p + 16) != n - 20U) {
            return HX_WAL_CORRUPT;
        }
        return apply_snapshot(w, get64(p), get64(p + 8), p + 20, get32(p + 16));
    }
    if (type != WAL_TYPE_ENTRIES || n < 12U) {
        return HX_WAL_CORRUPT;
    }
    const uint64_t first = get64(p);
    const uint32_t count = get32(p + 8);
    if (first == 0 || first - 1U < w->base || first - 1U - w->base > w->nents) {
        return HX_WAL_CORRUPT; /* would leave a hole in the log, or rewrite the snapshot */
    }
    free_entries_from(w, (size_t)(first - 1U - w->base));
    size_t pos = 12;
    for (uint32_t i = 0; i < count; i++) {
        if (n - pos < 12U) {
            return HX_WAL_CORRUPT;
        }
        const uint64_t term = get64(p + pos);
        const uint32_t len = get32(p + pos + 8);
        pos += 12U;
        if (len > n - pos) {
            return HX_WAL_CORRUPT;
        }
        if (push_entry(w, term, p + pos, len) != 0) {
            return HX_WAL_NOMEM;
        }
        pos += len;
    }
    return pos == n ? HX_WAL_OK : HX_WAL_CORRUPT;
}

static hx_wal_status replay(hx_raft_wal *w, uint64_t file_size, uint64_t *valid_end)
{
    uint64_t off = WAL_HEADER_LEN;
    uint8_t *buf = NULL;
    size_t cap = 0;
    hx_wal_status st = HX_WAL_OK;
    while (off < file_size && st == HX_WAL_OK) {
        uint8_t hdr[WAL_REC_HDR];
        if (file_size - off < sizeof hdr) {
            break; /* torn: not even a full record header */
        }
        if (read_all(w->fd, hdr, sizeof hdr, off) != 0) {
            st = HX_WAL_IO;
            break;
        }
        const uint32_t len = get32(hdr);
        const uint32_t crc = get32(hdr + 4);
        if (len == 0 || len > WAL_MAX_RECORD) {
            /* Not a valid record. It is a torn tail only if what remains is a stub or never-written
             * zeros. */
            st = (file_size - off <= WAL_REC_HDR + 64U || region_is_zero(w->fd, off, file_size))
                     ? HX_WAL_OK
                     : HX_WAL_CORRUPT;
            break;
        }
        const uint64_t end = off + WAL_REC_HDR + len;
        if (end > file_size) {
            break; /* torn: the body was cut short */
        }
        if (len > cap) {
            uint8_t *g = realloc(buf, len);
            if (g == NULL) {
                st = HX_WAL_NOMEM;
                break;
            }
            buf = g;
            cap = len;
        }
        if (read_all(w->fd, buf, len, off + WAL_REC_HDR) != 0) {
            st = HX_WAL_IO;
            break;
        }
        if (hx_crc32c(0, buf, len) != crc) {
            /* Torn tail (last record, or only never-written zeros after it) vs. real corruption. */
            st = (end == file_size || region_is_zero(w->fd, end, file_size)) ? HX_WAL_OK
                                                                             : HX_WAL_CORRUPT;
            break;
        }
        st = apply_record(w, buf[0], buf + 1, len - 1U);
        if (st != HX_WAL_OK) {
            break;
        }
        off = end;
    }
    free(buf);
    *valid_end = off;
    return st;
}

/* --------------------------------------------------------------- lifecycle */

static int write_header(int fd)
{
    uint8_t h[WAL_HEADER_LEN];
    put32(h, WAL_MAGIC);
    put32(h + 4, WAL_VERSION);
    return write_all(fd, h, sizeof h, 0);
}

hx_wal_status hx_raft_wal_open(const char *path, hx_raft_wal **out)
{
    if (path == NULL || out == NULL) {
        return HX_WAL_INVAL;
    }
    hx_raft_wal *w = calloc(1, sizeof *w);
    if (w == NULL) {
        return HX_WAL_NOMEM;
    }
    w->voted_for = HX_RAFT_NONE;
    w->path = strdup(path);
    w->fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (w->path == NULL || w->fd < 0) {
        const hx_wal_status st = w->path == NULL ? HX_WAL_NOMEM : HX_WAL_IO;
        hx_raft_wal_close(w);
        return st;
    }
    struct stat sb;
    if (fstat(w->fd, &sb) != 0) {
        hx_raft_wal_close(w);
        return HX_WAL_IO;
    }
    uint64_t fsize = (uint64_t)sb.st_size;
    hx_wal_status st = HX_WAL_OK;
    if (fsize < WAL_HEADER_LEN) {
        /* New, or a creation that crashed before the header was complete: nothing worth keeping. */
        if (ftruncate(w->fd, 0) != 0 || write_header(w->fd) != 0 || fsync(w->fd) != 0 ||
            sync_dir_of(path) != 0) {
            hx_raft_wal_close(w);
            return HX_WAL_IO;
        }
        fsize = WAL_HEADER_LEN;
    } else {
        uint8_t h[WAL_HEADER_LEN];
        if (read_all(w->fd, h, sizeof h, 0) != 0) {
            hx_raft_wal_close(w);
            return HX_WAL_IO;
        }
        if (get32(h) != WAL_MAGIC || get32(h + 4) != WAL_VERSION) {
            hx_raft_wal_close(w);
            return HX_WAL_CORRUPT;
        }
    }
    uint64_t valid_end = WAL_HEADER_LEN;
    st = replay(w, fsize, &valid_end);
    if (st != HX_WAL_OK) {
        hx_raft_wal_close(w);
        return st;
    }
    if (valid_end < fsize) { /* cut off the torn tail so new records follow valid data */
        if (ftruncate(w->fd, (off_t)valid_end) != 0 || fsync(w->fd) != 0) {
            hx_raft_wal_close(w);
            return HX_WAL_IO;
        }
    }
    w->size = valid_end;
    w->synced = valid_end;
    w->live = live_size(w);
    if (w->size > WAL_COMPACT_MIN && w->live * 2U < w->size) {
        st = hx_raft_wal_compact(w);
        if (st != HX_WAL_OK) {
            hx_raft_wal_close(w);
            return st;
        }
    }
    *out = w;
    return HX_WAL_OK;
}

void hx_raft_wal_close(hx_raft_wal *w)
{
    if (w == NULL) {
        return;
    }
    if (w->fd >= 0) {
        (void)close(w->fd);
    }
    free_entries_from(w, 0);
    free(w->ents);
    free(w->snap);
    free(w->view);
    free(w->path);
    free(w);
}

hx_raft_restore hx_raft_wal_restore(hx_raft_wal *w)
{
    free(w->view);
    w->view = w->nents != 0 ? malloc(w->nents * sizeof *w->view) : NULL;
    hx_raft_restore rs = {.term = w->term,
                          .voted_for = w->voted_for,
                          .entries = w->view,
                          .nentries = 0,
                          .snap_index = w->base,
                          .snap_term = w->base_term,
                          .snap_data = w->snap,
                          .snap_len = w->snap_len};
    if (w->view != NULL) {
        for (size_t i = 0; i < w->nents; i++) {
            w->view[i].term = w->ents[i].term;
            w->view[i].len = w->ents[i].len;
            w->view[i].data = w->ents[i].data;
        }
        rs.nentries = w->nents;
    }
    return rs;
}

/* ----------------------------------------------------------------- writing */

static int append_record(hx_raft_wal *w, uint8_t type, const hx_buf *payload)
{
    if (w->broken) {
        return -1;
    }
    hx_buf rec;
    hx_buf_init(&rec);
    int rc = frame(&rec, type, payload);
    if (rc == 0) {
        rc = write_all(w->fd, rec.data, rec.len, w->size);
    }
    if (rc != 0) {
        w->broken =
            true; /* the file may now hold a partial record: only a reopen can sort it out */
        hx_buf_free(&rec);
        return -1;
    }
    w->size += rec.len;
    hx_buf_free(&rec);
    return 0;
}

static int wal_save_hard(void *ctx, uint64_t term, uint32_t voted_for)
{
    hx_raft_wal *w = ctx;
    hx_buf p;
    hx_buf_init(&p);
    int rc = (hx_buf_put_u64(&p, term) == 0 && hx_buf_put_u32(&p, voted_for) == 0)
                 ? append_record(w, WAL_TYPE_HARD, &p)
                 : -1;
    hx_buf_free(&p);
    if (rc == 0) {
        w->term = term;
        w->voted_for = voted_for;
    }
    return rc;
}

static int wal_append(void *ctx, uint64_t first, const hx_raft_entry *e, size_t n)
{
    hx_raft_wal *w = ctx;
    if (first == 0 || first - 1U < w->base || first - 1U - w->base > w->nents || n > UINT32_MAX) {
        return -1;
    }
    hx_buf p;
    hx_buf_init(&p);
    int rc = (hx_buf_put_u64(&p, first) == 0 && hx_buf_put_u32(&p, (uint32_t)n) == 0) ? 0 : -1;
    for (size_t i = 0; i < n && rc == 0; i++) {
        rc = (hx_buf_put_u64(&p, e[i].term) == 0 && hx_buf_put_u32(&p, e[i].len) == 0 &&
              hx_buf_put(&p, e[i].data, e[i].len) == 0)
                 ? 0
                 : -1;
    }
    if (rc == 0 && p.len + 1U > WAL_MAX_RECORD) {
        rc = -1;
    }
    if (rc == 0) {
        rc = append_record(w, WAL_TYPE_ENTRIES, &p);
    }
    hx_buf_free(&p);
    if (rc != 0) {
        return -1;
    }
    free_entries_from(w, (size_t)(first - 1U - w->base));
    for (size_t i = 0; i < n; i++) {
        if (push_entry(w, e[i].term, e[i].data, e[i].len) != 0) {
            w->broken = true;
            return -1;
        }
    }
    return 0;
}

static int wal_save_snapshot(void *ctx, uint64_t index, uint64_t term, const uint8_t *data,
                             uint32_t len)
{
    hx_raft_wal *w = ctx;
    if (index <= w->base || (uint64_t)len + 21U > WAL_MAX_RECORD) {
        return -1;
    }
    hx_buf p;
    hx_buf_init(&p);
    int rc = (hx_buf_put_u64(&p, index) == 0 && hx_buf_put_u64(&p, term) == 0 &&
              hx_buf_put_u32(&p, len) == 0 && hx_buf_put(&p, data, len) == 0)
                 ? append_record(w, WAL_TYPE_SNAP, &p)
                 : -1;
    hx_buf_free(&p);
    if (rc != 0) {
        return -1;
    }
    if (apply_snapshot(w, index, term, data, len) != HX_WAL_OK) {
        w->broken =
            true; /* the file has the record but memory does not: only a reopen can fix it */
        return -1;
    }
    return 0;
}

static int wal_sync(void *ctx)
{
    hx_raft_wal *w = ctx;
    if (w->broken) {
        return -1;
    }
    if (w->synced == w->size) {
        return 0;
    }
    if (!w->nosync && fdatasync(w->fd) != 0) {
        w->broken = true; /* never retry a failed fsync: the kernel may have dropped the pages */
        return -1;
    }
    w->synced = w->size;
    return 0;
}

hx_raft_storage hx_raft_wal_storage(hx_raft_wal *w)
{
    const hx_raft_storage s = {.ctx = w,
                               .save_hard_state = wal_save_hard,
                               .append = wal_append,
                               .sync = wal_sync,
                               .save_snapshot = wal_save_snapshot};
    return s;
}

hx_wal_status hx_raft_wal_compact(hx_raft_wal *w)
{
    if (w->broken) {
        return HX_WAL_IO;
    }
    hx_buf all;
    hx_buf p;
    hx_buf_init(&all);
    hx_buf_init(&p);
    uint8_t hdr[WAL_HEADER_LEN];
    put32(hdr, WAL_MAGIC);
    put32(hdr + 4, WAL_VERSION);
    int rc = hx_buf_put(&all, hdr, sizeof hdr);
    if (rc == 0) {
        rc = (hx_buf_put_u64(&p, w->term) == 0 && hx_buf_put_u32(&p, w->voted_for) == 0)
                 ? frame(&all, WAL_TYPE_HARD, &p)
                 : -1;
    }
    p.len = 0;
    if (rc == 0 && w->base != 0) {
        rc = (hx_buf_put_u64(&p, w->base) == 0 && hx_buf_put_u64(&p, w->base_term) == 0 &&
              hx_buf_put_u32(&p, w->snap_len) == 0 && hx_buf_put(&p, w->snap, w->snap_len) == 0)
                 ? frame(&all, WAL_TYPE_SNAP, &p)
                 : -1;
        p.len = 0;
    }
    if (rc == 0) {
        rc = (hx_buf_put_u64(&p, w->base + 1U) == 0 && hx_buf_put_u32(&p, (uint32_t)w->nents) == 0)
                 ? 0
                 : -1;
    }
    for (size_t i = 0; i < w->nents && rc == 0; i++) {
        rc = (hx_buf_put_u64(&p, w->ents[i].term) == 0 && hx_buf_put_u32(&p, w->ents[i].len) == 0 &&
              hx_buf_put(&p, w->ents[i].data, w->ents[i].len) == 0)
                 ? 0
                 : -1;
    }
    if (rc == 0 && p.len + 1U > WAL_MAX_RECORD) {
        rc = -1; /* too large for one record: refuse rather than write a file open() would reject */
    }
    if (rc == 0) {
        rc = frame(&all, WAL_TYPE_ENTRIES, &p);
    }
    hx_buf_free(&p);
    if (rc != 0) {
        hx_buf_free(&all);
        return HX_WAL_NOMEM;
    }

    char tmp[WAL_PATH_MAX];
    if (snprintf(tmp, sizeof tmp, "%s.compact", w->path) >= (int)sizeof tmp) {
        hx_buf_free(&all);
        return HX_WAL_INVAL;
    }
    const int tfd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    hx_wal_status st = HX_WAL_OK;
    if (tfd < 0 || write_all(tfd, all.data, all.len, 0) != 0 || fsync(tfd) != 0) {
        st = HX_WAL_IO;
    }
    if (tfd >= 0) {
        (void)close(tfd);
    }
    if (st == HX_WAL_OK && (rename(tmp, w->path) != 0 || sync_dir_of(w->path) != 0)) {
        st = HX_WAL_IO;
    }
    if (st != HX_WAL_OK) {
        (void)unlink(tmp);
        hx_buf_free(&all);
        return st;
    }
    /* Switch to the new file; the old descriptor still points at the replaced inode. */
    const int nfd = open(w->path, O_RDWR | O_CLOEXEC);
    if (nfd < 0) {
        w->broken = true;
        hx_buf_free(&all);
        return HX_WAL_IO;
    }
    (void)close(w->fd);
    w->fd = nfd;
    w->size = all.len;
    w->synced = all.len;
    w->live = all.len;
    hx_buf_free(&all);
    return HX_WAL_OK;
}

void hx_raft_wal_set_nosync_for_tests(hx_raft_wal *w, bool nosync)
{
    w->nosync = nosync;
}

uint64_t hx_raft_wal_file_size(const hx_raft_wal *w)
{
    return w->size;
}

uint64_t hx_raft_wal_synced_size(const hx_raft_wal *w)
{
    return w->synced;
}

uint64_t hx_raft_wal_entry_count(const hx_raft_wal *w)
{
    return w->nents;
}

uint64_t hx_raft_wal_snapshot_index(const hx_raft_wal *w)
{
    return w->base;
}
