/*
 * The replica log on the real storage engine: epoch bookkeeping, truncation, reopen, limits.
 * Behaviour under replication faults is covered by the differential mode of tests/test_repl.c.
 */
#define _XOPEN_SOURCE 700
#include <ftw.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "repl/repl_log.h"

static int failures;

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

static int rm_cb(const char *p, const struct stat *st, int t, struct FTW *f)
{
    (void)st;
    (void)t;
    (void)f;
    return remove(p);
}

static void make_tmp(char *out, size_t cap)
{
    const char *t = getenv("TMPDIR");
    (void)snprintf(out, cap, "%s/helix-plog-XXXXXX", (t != NULL && *t != '\0') ? t : "/tmp");
    if (mkdtemp(out) == NULL) {
        perror("mkdtemp");
        exit(2);
    }
}

static int add(hx_repl_log *v, uint32_t epoch, const char *text)
{
    const hx_repl_rec r = {
        .epoch = epoch, .len = (uint32_t)strlen(text), .data = (const uint8_t *)text};
    return v->append(v->ctx, &r, 1);
}

static void epochs_and_truncate(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_plog *p = NULL;
    CHECK(hx_plog_open(dir, NULL, &p) == HX_OK);
    hx_repl_log v = hx_plog_vtable(p);
    uint32_t e = 0;
    CHECK(v.leo(v.ctx) == 0);
    CHECK(v.epoch_at(v.ctx, 0, &e) != 0);
    CHECK(v.end_offset_for_epoch(v.ctx, 5) == 0);
    /* offsets: 0,1 epoch 1; 2,3,4 epoch 3; 5 epoch 4 */
    CHECK(add(&v, 1, "a") == 0 && add(&v, 1, "b") == 0);
    CHECK(add(&v, 3, "c") == 0 && add(&v, 3, "dd") == 0 && add(&v, 3, "") == 0);
    CHECK(add(&v, 4, "f") == 0);
    CHECK(v.leo(v.ctx) == 6);
    CHECK(v.epoch_at(v.ctx, 0, &e) == 0 && e == 1);
    CHECK(v.epoch_at(v.ctx, 1, &e) == 0 && e == 1);
    CHECK(v.epoch_at(v.ctx, 2, &e) == 0 && e == 3);
    CHECK(v.epoch_at(v.ctx, 5, &e) == 0 && e == 4);
    CHECK(v.epoch_at(v.ctx, 6, &e) != 0); /* leo itself is not a record */
    CHECK(v.end_offset_for_epoch(v.ctx, 0) == 0);
    CHECK(v.end_offset_for_epoch(v.ctx, 1) == 2);
    CHECK(v.end_offset_for_epoch(v.ctx, 2) == 2); /* no records of epoch 2 */
    CHECK(v.end_offset_for_epoch(v.ctx, 3) == 5);
    CHECK(v.end_offset_for_epoch(v.ctx, 4) == 6);
    CHECK(v.end_offset_for_epoch(v.ctx, 9) == 6);
    CHECK(add(&v, 2, "x") != 0); /* epochs never decrease */
    CHECK(v.leo(v.ctx) == 6);

    hx_repl_rec out[8];
    size_t n = 0;
    CHECK(v.read(v.ctx, 1, 8, 1000, out, &n) == 0 && n == 5);
    CHECK(out[0].epoch == 1 && out[0].len == 1 && out[0].data[0] == 'b');
    CHECK(out[2].epoch == 3 && out[2].len == 2 && memcmp(out[2].data, "dd", 2) == 0);
    CHECK(out[3].len == 0);
    CHECK(v.read(v.ctx, 0, 8, 1, out, &n) == 0 && n == 1); /* byte limit, but at least one record */
    CHECK(v.read(v.ctx, 0, 8, 2, out, &n) == 0 && n == 2);
    CHECK(v.read(v.ctx, 0, 3, 1000, out, &n) == 0 && n == 3);
    CHECK(v.read(v.ctx, 6, 8, 1000, out, &n) == 0 && n == 0);
    uint8_t buf[8];
    uint32_t len = 0;
    uint32_t ep = 0;
    CHECK(hx_plog_read_payload(p, 3, buf, sizeof buf, &len, &ep) == HX_OK && len == 2 && ep == 3);
    CHECK(hx_plog_read_payload(p, 3, buf, 1, &len, &ep) == HX_ERR_BUF_SMALL && len == 2);

    CHECK(v.sync(v.ctx) == 0);
    hx_plog_close(p);
    CHECK(hx_plog_open(dir, NULL, &p) == HX_OK); /* run index rebuilt from the records */
    v = hx_plog_vtable(p);
    CHECK(v.leo(v.ctx) == 6);
    CHECK(v.end_offset_for_epoch(v.ctx, 1) == 2 && v.end_offset_for_epoch(v.ctx, 3) == 5);
    CHECK(v.epoch_at(v.ctx, 4, &e) == 0 && e == 3);

    CHECK(v.truncate(v.ctx, 3) == 0); /* cut inside epoch 3 */
    CHECK(v.leo(v.ctx) == 3);
    CHECK(v.end_offset_for_epoch(v.ctx, 3) == 3 && v.end_offset_for_epoch(v.ctx, 4) == 3);
    CHECK(add(&v, 5, "g") == 0); /* a later epoch may follow */
    CHECK(v.epoch_at(v.ctx, 3, &e) == 0 && e == 5 && v.end_offset_for_epoch(v.ctx, 4) == 3);
    CHECK(v.truncate(v.ctx, 2) == 0); /* cut at an epoch boundary: epoch 3 disappears */
    CHECK(v.end_offset_for_epoch(v.ctx, 1) == 2 && v.end_offset_for_epoch(v.ctx, 3) == 2);
    CHECK(add(&v, 2, "y") == 0); /* now epoch 2 is legal again */
    CHECK(v.truncate(v.ctx, 0) == 0);
    CHECK(v.leo(v.ctx) == 0 && v.epoch_at(v.ctx, 0, &e) != 0);
    CHECK(v.truncate(v.ctx, 1) != 0); /* cannot grow */
    hx_plog_close(p);
    (void)nftw(dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
}

static void limits_and_bad_files(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.max_record_bytes = 64;
    hx_plog *p = NULL;
    CHECK(hx_plog_open(dir, &o, &p) == HX_OK);
    hx_repl_log v = hx_plog_vtable(p);
    uint8_t big[100];
    memset(big, 7, sizeof big);
    const hx_repl_rec ok = {.epoch = 1, .len = 60, .data = big};
    const hx_repl_rec bad = {.epoch = 1, .len = 61, .data = big};
    CHECK(v.append(v.ctx, &ok, 1) == 0);
    CHECK(v.append(v.ctx, &bad, 1) != 0); /* 4 bytes of the limit belong to the epoch */
    CHECK(v.leo(v.ctx) == 1);
    hx_plog_close(p);

    /* A log written without an epoch prefix, or with epochs going backwards, is refused. */
    char dir2[256];
    make_tmp(dir2, sizeof dir2);
    hx_log *raw = NULL;
    CHECK(hx_log_open(dir2, NULL, &raw) == HX_OK);
    CHECK(hx_log_append(raw, "ab", 2, 0, NULL) == HX_OK); /* 2 bytes: no room for an epoch */
    hx_log_close(raw);
    CHECK(hx_plog_open(dir2, NULL, &p) == HX_ERR_CORRUPT);

    char dir3[256];
    make_tmp(dir3, sizeof dir3);
    CHECK(hx_log_open(dir3, NULL, &raw) == HX_OK);
    const uint8_t e2[4] = {2, 0, 0, 0};
    const uint8_t e1[4] = {1, 0, 0, 0};
    CHECK(hx_log_append(raw, e2, 4, 0, NULL) == HX_OK &&
          hx_log_append(raw, e1, 4, 0, NULL) == HX_OK);
    hx_log_close(raw);
    CHECK(hx_plog_open(dir3, NULL, &p) == HX_ERR_CORRUPT);
    (void)nftw(dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
    (void)nftw(dir2, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
    (void)nftw(dir3, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
}

int main(void)
{
    epochs_and_truncate();
    limits_and_bad_files();
    if (failures != 0) {
        (void)fprintf(stderr, "%d replica-log test failure(s)\n", failures);
        return 1;
    }
    (void)puts("all replica-log tests passed");
    return 0;
}
