/*
 * Storage-engine tests: functional, crash-recovery and randomized
 * model-based fault injection (torn tails, bit flips).
 *
 *   ./test_log            run the test suite
 *   ./test_log --bench    also print append throughput numbers
 */
#define _XOPEN_SOURCE 700
#include <dirent.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../src/storage/log.h"

static int failures;

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                        \
            failures++;                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

#define CHECK_OK(expr)                                                                             \
    do {                                                                                           \
        hx_err e_ = (expr);                                                                        \
        if (e_ != HX_OK) {                                                                         \
            fprintf(stderr, "FAIL %s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, hx_strerror(e_)); \
            failures++;                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

/* ----- deterministic payloads: content is a pure function of the offset ---- */
static uint32_t plen_for(uint64_t off)
{
    return 1 + (uint32_t)((off * 7919u) % 300u);
}
static void fill(uint64_t off, uint8_t *b, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) {
        b[i] = (uint8_t)(off + (uint64_t)i * 31U);
    }
}

static hx_err append_rec(hx_log *l, uint64_t expect_off)
{
    uint8_t buf[512];
    uint32_t n = plen_for(expect_off);
    fill(expect_off, buf, n);
    uint64_t got = 0;
    hx_err e = hx_log_append(l, buf, n, (int64_t)expect_off, &got);
    if (e == HX_OK && got != expect_off) {
        return HX_ERR_CORRUPT;
    }
    return e;
}

/* returns 1 if record `off` reads back correctly */
static int verify_rec(hx_log *l, uint64_t off)
{
    uint8_t got[512], want[512];
    uint32_t n = 0;
    int64_t ts = 0;
    if (hx_log_read(l, off, got, sizeof got, &n, &ts) != HX_OK) {
        return 0;
    }
    if (n != plen_for(off) || ts != (int64_t)off) {
        return 0;
    }
    fill(off, want, n);
    return memcmp(got, want, n) == 0;
}

static int verify_range(hx_log *l, uint64_t from, uint64_t to)
{
    for (uint64_t o = from; o < to; o++) {
        if (!verify_rec(l, o)) {
            fprintf(stderr, "bad record %llu\n", (unsigned long long)o);
            return 0;
        }
    }
    return 1;
}

/* Deterministic xorshift64* PRNG: reproducible crash-test sequences, no libc rand(). */
static uint64_t g_rng = 0x9E3779B97F4A7C15ULL;
static uint32_t rng_next(void)
{
    g_rng ^= g_rng >> 12;
    g_rng ^= g_rng << 25;
    g_rng ^= g_rng >> 27;
    return (uint32_t)((g_rng * 0x2545F4914F6CDD1DULL) >> 33);
}

/* ----- filesystem helpers -------------------------------------------------- */
static int rm_cb(const char *p, const struct stat *s, int t, struct FTW *f)
{
    (void)s;
    (void)t;
    (void)f;
    return remove(p);
}
static void rm_rf(const char *p)
{
    nftw(p, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
}

static void make_tmp(char *out, size_t cap)
{
    const char *t = getenv("TMPDIR");
    snprintf(out, cap, "%s/helix-test-XXXXXX", t && *t ? t : "/tmp");
    if (!mkdtemp(out)) {
        perror("mkdtemp");
        exit(2);
    }
}

/* path of the highest-base .log file */
static int last_segment(const char *dir, char *out, size_t cap)
{
    DIR *d = opendir(dir);
    if (!d) {
        return -1;
    }
    char best[64] = "";
    struct dirent *de;
    while ((de = readdir(d))) {
        size_t n = strlen(de->d_name);
        if (n == 24 && !strcmp(de->d_name + 20, ".log") && strcmp(de->d_name, best) > 0) {
            memcpy(best, de->d_name, 25); /* n == 24 here; copies the NUL too */
        }
    }
    closedir(d);
    if (!best[0]) {
        return -1;
    }
    snprintf(out, cap, "%s/%s", dir, best);
    return 0;
}

/* ----- tests --------------------------------------------------------------- */

static void test_crc32c(void)
{
    /* Standard check value for CRC32C("123456789") */
    CHECK(hx_crc32c(0, "123456789", 9) == 0xE3069283u);
    /* chaining */
    uint32_t c = hx_crc32c(0, "12345", 5);
    CHECK(hx_crc32c(c, "6789", 4) == 0xE3069283u);

    /* The portable table must give the same value as any hardware path. */
    CHECK(hx_crc32c_sw(0, "123456789", 9) == 0xE3069283u);
    CHECK(hx_crc32c_hw() == 0 || hx_crc32c_hw() == 1);

    /* Hardware and software must agree for every length and every alignment:
     * a checksum that depended on the host would corrupt a log written on one
     * machine and verified on another. */
    uint8_t buf[256 + 8];
    for (size_t i = 0; i < sizeof buf; i++) {
        buf[i] = (uint8_t)(i * 31u + 7u);
    }
    for (size_t off = 0; off < 8; off++) {
        for (size_t n = 0; n <= 256; n++) {
            CHECK(hx_crc32c(0, buf + off, n) == hx_crc32c_sw(0, buf + off, n));
        }
    }
    /* Chaining is equivalent to one shot, on both paths. */
    CHECK(hx_crc32c(hx_crc32c(0, buf, 100), buf + 100, 50) == hx_crc32c(0, buf, 150));
    CHECK(hx_crc32c_sw(hx_crc32c_sw(0, buf, 100), buf + 100, 50) == hx_crc32c_sw(0, buf, 150));
}

static void test_append_read_reopen(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.segment_bytes = 8192; /* force many segments */
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    for (uint64_t i = 0; i < 1000; i++) {
        CHECK_OK(append_rec(l, i));
    }
    CHECK(hx_log_next_offset(l) == 1000);
    CHECK(hx_log_segment_count(l) > 5);
    CHECK(verify_range(l, 0, 1000));

    uint8_t tiny[1];
    uint32_t n = 0;
    CHECK(hx_log_read(l, 7, tiny, 1, &n, NULL) == HX_ERR_BUF_SMALL && n == plen_for(7));
    CHECK(hx_log_read(l, 1000, tiny, 1, &n, NULL) == HX_ERR_NOT_FOUND);
    hx_log_close(l);

    CHECK_OK(hx_log_open(dir, &o, &l));
    CHECK(hx_log_next_offset(l) == 1000);
    CHECK(verify_range(l, 0, 1000));
    CHECK_OK(append_rec(l, 1000));
    CHECK(verify_rec(l, 1000));
    hx_log_close(l);
    rm_rf(dir);
}

static void test_too_large(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.max_record_bytes = 100;
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    uint8_t big[101] = {0};
    CHECK(hx_log_append(l, big, sizeof big, 0, NULL) == HX_ERR_TOO_LARGE);
    CHECK(hx_log_next_offset(l) == 0);
    hx_log_close(l);
    rm_rf(dir);
}

static void test_torn_tail(void)
{
    char dir[256], seg[512];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    for (uint64_t i = 0; i < 100; i++) {
        CHECK_OK(append_rec(l, i));
    }
    hx_log_close(l);

    CHECK(last_segment(dir, seg, sizeof seg) == 0);
    struct stat st;
    CHECK(stat(seg, &st) == 0);
    CHECK(truncate(seg, st.st_size - 5) == 0); /* cut mid-record */

    CHECK_OK(hx_log_open(dir, &o, &l));
    CHECK(hx_log_next_offset(l) == 99); /* last record dropped */
    CHECK(verify_range(l, 0, 99));
    CHECK_OK(append_rec(l, 99)); /* log continues densely */
    CHECK(verify_range(l, 0, 100));
    hx_log_close(l);
    rm_rf(dir);
}

static void test_bit_flip(void)
{
    char dir[256], seg[512];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    for (uint64_t i = 0; i < 100; i++) {
        CHECK_OK(append_rec(l, i));
    }
    hx_log_close(l);

    CHECK(last_segment(dir, seg, sizeof seg) == 0);
    struct stat st;
    CHECK(stat(seg, &st) == 0);
    int fd = open(seg, O_RDWR);
    CHECK(fd >= 0);
    uint8_t b;
    CHECK(pread(fd, &b, 1, st.st_size - 1) == 1);
    b ^= 0x40;
    CHECK(pwrite(fd, &b, 1, st.st_size - 1) == 1);
    close(fd);

    CHECK_OK(hx_log_open(dir, &o, &l));
    CHECK(hx_log_next_offset(l) == 99); /* CRC caught the corruption */
    CHECK(verify_range(l, 0, 99));
    hx_log_close(l);
    rm_rf(dir);
}

static void test_corrupt_middle_segment_fails_open(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.segment_bytes = 4096;
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    for (uint64_t i = 0; i < 200; i++) {
        CHECK_OK(append_rec(l, i));
    }
    hx_log_close(l);

    char first[512];
    snprintf(first, sizeof first, "%s/%020d.log", dir, 0);
    int fd = open(first, O_RDWR);
    CHECK(fd >= 0);
    uint8_t b;
    CHECK(pread(fd, &b, 1, 40) == 1);
    b ^= 0xFF;
    CHECK(pwrite(fd, &b, 1, 40) == 1);
    close(fd);

    /* Corruption in a sealed segment is NOT silently repaired. */
    CHECK(hx_log_open(dir, &o, &l) == HX_ERR_CORRUPT);
    rm_rf(dir);
}

static void test_lowered_max_record_is_not_destructive(void)
{
    char dir[256], seg[512];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.max_record_bytes = 512;
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    for (uint64_t i = 0; i < 50; i++) {
        CHECK_OK(append_rec(l, i));
    }
    hx_log_close(l);

    CHECK(last_segment(dir, seg, sizeof seg) == 0);
    struct stat before;
    CHECK(stat(seg, &before) == 0);

    o.max_record_bytes = 20; /* operator lowers the limit by mistake */
    CHECK(hx_log_open(dir, &o, &l) == HX_ERR_CORRUPT);

    struct stat after;
    CHECK(stat(seg, &after) == 0);
    CHECK(after.st_size == before.st_size); /* data untouched */

    o.max_record_bytes = 512; /* restoring the limit recovers all */
    CHECK_OK(hx_log_open(dir, &o, &l));
    CHECK(hx_log_next_offset(l) == 50);
    CHECK(verify_range(l, 0, 50));
    hx_log_close(l);
    rm_rf(dir);
}

static void test_retention(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.segment_bytes = 4096;
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    for (uint64_t i = 0; i < 500; i++) {
        CHECK_OK(append_rec(l, i));
    }
    size_t before = hx_log_segment_count(l);
    CHECK_OK(hx_log_retain_bytes(l, 16384));
    CHECK(hx_log_segment_count(l) < before);
    uint64_t first = hx_log_first_offset(l);
    CHECK(first > 0);
    uint32_t n;
    uint8_t buf[512];
    CHECK(hx_log_read(l, 0, buf, sizeof buf, &n, NULL) == HX_ERR_NOT_FOUND);
    CHECK(verify_range(l, first, 500));
    hx_log_close(l);

    CHECK_OK(hx_log_open(dir, &o, &l)); /* survives reopen */
    CHECK(hx_log_first_offset(l) == first);
    CHECK(hx_log_next_offset(l) == 500);
    CHECK(verify_range(l, first, 500));
    hx_log_close(l);
    rm_rf(dir);
}

/* Records whose size depends on a generation number: after a cut, the re-appended records have
 * other sizes than the ones they replace, so a stale sparse-index entry would point into the middle
 * of a frame. */
static uint32_t glen(uint64_t off, uint32_t gen)
{
    return 1U + (uint32_t)((off * 13U + (uint64_t)gen * 71U) % 150U);
}

static hx_err gappend(hx_log *l, uint64_t off, uint32_t gen)
{
    uint8_t b[160];
    const uint32_t n = glen(off, gen);
    memset(b, (int)(off + gen), n);
    uint64_t got = 0;
    return hx_log_append(l, b, n, 0, &got);
}

static int gverify(hx_log *l, const uint32_t *gens, uint64_t upto)
{
    for (uint64_t o = 0; o < upto; o++) {
        uint8_t b[160];
        uint32_t n = 0;
        if (hx_log_read(l, o, b, sizeof b, &n, NULL) != HX_OK || n != glen(o, gens[o]) ||
            b[0] != (uint8_t)(o + gens[o])) {
            fprintf(stderr, "bad generation record %llu\n", (unsigned long long)o);
            return 0;
        }
    }
    return 1;
}

static void test_truncate_then_other_sizes(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.segment_bytes = 2048;
    o.index_interval_bytes = 64;
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    uint32_t gens[400];
    memset(gens, 0, sizeof gens);
    for (uint64_t i = 0; i < 300; i++) {
        CHECK_OK(gappend(l, i, 0));
    }
    for (uint32_t gen = 1; gen <= 12; gen++) {
        const uint64_t cut = (gen * 37U) % 250U;
        CHECK_OK(hx_log_truncate(l, cut));
        for (uint64_t i = cut; i < 300; i++) {
            gens[i] = gen;
            CHECK_OK(gappend(l, i, gen));
        }
        CHECK(gverify(l, gens, 300));
    }
    CHECK_OK(hx_log_sync(l));
    hx_log_close(l);
    CHECK_OK(hx_log_open(dir, &o, &l));
    CHECK(gverify(l, gens, 300));
    hx_log_close(l);
    rm_rf(dir);
}

static void test_truncate(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.segment_bytes = 4096;
    o.sync = HX_SYNC_NEVER;
    hx_log *l;
    CHECK_OK(hx_log_open(dir, &o, &l));
    for (uint64_t i = 0; i < 400; i++) {
        CHECK_OK(append_rec(l, i));
    }
    CHECK(hx_log_segment_count(l) > 4);
    CHECK(hx_log_truncate(l, 401) == HX_ERR_INVAL);
    CHECK_OK(hx_log_truncate(l, 400)); /* no-op */
    /* A cut in the middle of a segment, then in a segment boundary, then to empty. */
    const uint64_t cuts[] = {377, 250, 3, 0};
    for (size_t k = 0; k < sizeof cuts / sizeof cuts[0]; k++) {
        const uint64_t cut = cuts[k];
        CHECK_OK(hx_log_truncate(l, cut));
        CHECK(hx_log_next_offset(l) == cut);
        CHECK(verify_range(l, 0, cut));
        uint32_t n;
        uint8_t buf[512];
        CHECK(cut == 0 || hx_log_read(l, cut, buf, sizeof buf, &n, NULL) == HX_ERR_NOT_FOUND);
        /* Appendable again at the cut, across reopen as well. */
        for (uint64_t i = cut; i < cut + 40; i++) {
            CHECK_OK(append_rec(l, i));
        }
        CHECK(verify_range(l, 0, cut + 40));
        CHECK_OK(hx_log_sync(l));
        hx_log_close(l);
        CHECK_OK(hx_log_open(dir, &o, &l));
        CHECK(hx_log_next_offset(l) == cut + 40);
        CHECK(verify_range(l, 0, cut + 40));
    }
    /* Every possible cut point, which includes every segment base. */
    for (uint64_t i = hx_log_next_offset(l); i < 150; i++) {
        CHECK_OK(append_rec(l, i));
    }
    for (uint64_t cut = 150; cut-- > 0;) {
        CHECK_OK(hx_log_truncate(l, cut));
        CHECK(hx_log_next_offset(l) == cut && verify_range(l, 0, cut));
        for (uint64_t i = cut; i < 150; i++) {
            CHECK_OK(append_rec(l, i));
        }
    }
    CHECK(verify_range(l, 0, 150));
    hx_log_close(l);
    rm_rf(dir);
}

/*
 * Model-based crash test. After every simulated crash the recovered log must be
 * a dense PREFIX of what was written: no holes, no corrupt records, never
 * longer than before, and appendable again.
 */
static void test_random_crash_recovery(void)
{
    char dir[256], seg[512];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.segment_bytes = 16384;
    o.sync = HX_SYNC_NEVER;
    g_rng = 12345;

    uint64_t prev_next = 0;
    for (int iter = 0; iter < 300; iter++) {
        hx_log *l;
        CHECK_OK(hx_log_open(dir, &o, &l));
        uint64_t next = hx_log_next_offset(l);
        CHECK(hx_log_first_offset(l) == 0);
        CHECK(next <= prev_next);        /* never grows on recovery */
        CHECK(verify_range(l, 0, next)); /* every survivor is intact */

        const uint32_t count = rng_next() % 50U;
        for (uint32_t i = 0; i < count; i++) {
            CHECK_OK(append_rec(l, next + i));
        }
        prev_next = next + count;
        hx_log_close(l);

        /* inject a fault into the tail segment */
        CHECK(last_segment(dir, seg, sizeof seg) == 0);
        struct stat st;
        CHECK(stat(seg, &st) == 0);
        const uint32_t kind = rng_next() % 4U;
        if (kind <= 1 && st.st_size > 0) { /* torn write */
            off_t cut = (off_t)(rng_next() % (uint32_t)(st.st_size < 400 ? st.st_size + 1 : 401));
            CHECK(truncate(seg, st.st_size - cut) == 0);
        } else if (kind == 2 && st.st_size > 0) { /* bit rot near tail */
            off_t span = st.st_size < 300 ? st.st_size : 300;
            off_t at = st.st_size - 1 - (off_t)(rng_next() % (uint32_t)span);
            int fd = open(seg, O_RDWR);
            CHECK(fd >= 0);
            uint8_t b;
            CHECK(pread(fd, &b, 1, at) == 1);
            b ^= (uint8_t)(1U << (rng_next() % 8U));
            CHECK(pwrite(fd, &b, 1, at) == 1);
            close(fd);
        } /* kind 3: clean shutdown */
    }
    rm_rf(dir);
}

/* ----- benchmark ----------------------------------------------------------- */
static double secs(struct timespec a, struct timespec b)
{
    return (double)(b.tv_sec - a.tv_sec) + (double)(b.tv_nsec - a.tv_nsec) / 1e9;
}

static void bench(const char *label, hx_sync_policy pol, uint64_t n, uint32_t sz)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_log_opts o;
    hx_log_opts_default(&o);
    o.segment_bytes = 256ull << 20;
    o.sync = pol;
    o.sync_interval_ms = 200;
    hx_log *l;
    if (hx_log_open(dir, &o, &l) != HX_OK) {
        puts("bench open failed");
        return;
    }
    uint8_t *buf = calloc(1, sz);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < n; i++) {
        if (hx_log_append(l, buf, sz, 0, NULL) != HX_OK) {
            puts("bench append failed");
            break;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double s = secs(t0, t1);
    printf("bench %-22s %9llu msgs x %4u B : %9.0f msg/s  %7.1f MB/s\n", label,
           (unsigned long long)n, sz, (double)n / s, (double)n * (sz + 26) / s / 1e6);
    free(buf);
    hx_log_close(l);
    rm_rf(dir);
}

int main(int argc, char **argv)
{
    test_crc32c();
    test_append_read_reopen();
    test_too_large();
    test_torn_tail();
    test_bit_flip();
    test_corrupt_middle_segment_fails_open();
    test_lowered_max_record_is_not_destructive();
    test_retention();
    test_truncate();
    test_truncate_then_other_sizes();
    test_random_crash_recovery();

    if (failures) {
        fprintf(stderr, "%d test(s) FAILED\n", failures);
        return 1;
    }
    puts("all storage tests passed");

    if (argc > 1 && !strcmp(argv[1], "--bench")) {
        bench("sync=never", HX_SYNC_NEVER, 1000000, 128);
        bench("sync=interval(200ms)", HX_SYNC_INTERVAL, 1000000, 128);
        bench("sync=always", HX_SYNC_ALWAYS, 2000, 128);
    }
    return 0;
}
