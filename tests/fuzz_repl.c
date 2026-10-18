/*
 * libFuzzer harness for the replication codec and state machine. The input is a stream of
 * (u16 length | bytes) records. A record starting with 0xF0 is a controller command
 * (set_state(epoch, leader, isr, version) from the next four bytes) or a tick; anything else is
 * decoded as a wire message and stepped into a 3-replica instance with a small in-memory log.
 * Decoding must never read out of bounds, and arbitrary well-formed input must never crash or
 * corrupt memory: protocol violations that the module detects end in the `failed` state.
 */
#include <stdint.h>
#include <string.h>

#include "repl/repl.h"

#define CAP 2048U

typedef struct {
    uint32_t epoch[CAP];
    uint8_t first[CAP];
    size_t n;
} flog;

static uint64_t f_leo(void *c)
{
    return ((flog *)c)->n;
}

static int f_epoch_at(void *c, uint64_t off, uint32_t *e)
{
    const flog *l = c;
    if (off >= l->n) {
        return -1;
    }
    *e = l->epoch[off];
    return 0;
}

static uint64_t f_end(void *c, uint32_t e)
{
    const flog *l = c;
    size_t i = l->n;
    while (i > 0 && l->epoch[i - 1U] > e) {
        i--;
    }
    return i;
}

static int f_append(void *c, const hx_repl_rec *r, size_t n)
{
    flog *l = c;
    if (l->n + n > CAP) {
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        l->epoch[l->n] = r[i].epoch;
        l->first[l->n] = r[i].len != 0 ? r[i].data[0] : 0;
        l->n++;
    }
    return 0;
}

static int f_truncate(void *c, uint64_t to)
{
    flog *l = c;
    if (to > l->n) {
        return -1;
    }
    l->n = (size_t)to;
    return 0;
}

static int f_read(void *c, uint64_t off, size_t max_n, size_t max_bytes, hx_repl_rec *out,
                  size_t *n)
{
    static const uint8_t zero = 0;
    const flog *l = c;
    (void)max_bytes;
    size_t k = 0;
    while (off + k < l->n && k < max_n) {
        out[k].epoch = l->epoch[off + k];
        out[k].len = 1;
        out[k].data = &zero;
        k++;
    }
    *n = k;
    return 0;
}

static int f_sync(void *c)
{
    (void)c;
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

// NOLINTNEXTLINE(readability-non-const-parameter): signature fixed by the libFuzzer ABI
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    flog log;
    memset(&log, 0, sizeof log);
    hx_repl_cfg cfg;
    hx_repl_cfg_default(&cfg, 1, 7);
    cfg.log.ctx = &log;
    cfg.log.leo = f_leo;
    cfg.log.epoch_at = f_epoch_at;
    cfg.log.end_offset_for_epoch = f_end;
    cfg.log.append = f_append;
    cfg.log.truncate = f_truncate;
    cfg.log.read = f_read;
    cfg.log.sync = f_sync;
    hx_repl *r = hx_repl_create(&cfg, 0);
    if (r == NULL) {
        return 0;
    }
    hx_repl_rec scratch[64];
    int64_t now = 0;
    size_t pos = 0;
    while (pos + 2U <= size) {
        const size_t len = ((size_t)data[pos] << 8) | data[pos + 1U];
        pos += 2U;
        if (len > size - pos) {
            break;
        }
        const uint8_t *p = data + pos;
        now += 7;
        if (len >= 5U && p[0] == 0xF0) {
            (void)hx_repl_set_state(r, now, p[1], p[2] % 4U == 3U ? HX_REPL_NONE : p[2] % 3U,
                                    p[3] & 7U, p[4]);
        } else if (len >= 1U && p[0] == 0xF1) {
            (void)hx_repl_tick(r, now + 100);
        } else {
            hx_repl_msg m;
            if (hx_repl_msg_decode(p, len, scratch, 64, &m) == 0) {
                (void)hx_repl_step(r, now, &m);
            }
        }
        const hx_repl_msg *out = NULL;
        size_t n = 0;
        hx_repl_take_msgs(r, &out, &n);
        pos += len;
    }
    hx_repl_destroy(r);
    return 0;
}
