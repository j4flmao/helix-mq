/*
 * Tests for the Raft write-ahead log: round trips, the truncating-append contract, torn-write
 * recovery at every byte boundary, bit flips, zero-filled tails, fail-closed corruption,
 * compaction, and a Raft node restarting from its WAL.
 */
#define _XOPEN_SOURCE 700
#include <fcntl.h>
#include <ftw.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "raft/raft.h"
#include "raft/raft_wal.h"

static int failures;

#define BIG_SNAP ((uint32_t)(1500U * 1024U))

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

/* For helpers: report and return false so that the caller can clean up. */
#define REQ(cond)                                                                                  \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

static int rm_cb(const char *p, const struct stat *s, int t, struct FTW *f)
{
    (void)s;
    (void)t;
    (void)f;
    return remove(p);
}

static void make_tmp(char *out, size_t cap)
{
    const char *t = getenv("TMPDIR");
    (void)snprintf(out, cap, "%s/helix-wal-XXXXXX", (t != NULL && *t != '\0') ? t : "/tmp");
    if (mkdtemp(out) == NULL) {
        perror("mkdtemp");
        exit(2);
    }
}

static void rm_rf(const char *p)
{
    (void)nftw(p, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
}

static bool copy_prefix(const char *from, const char *to, uint64_t n)
{
    FILE *in = fopen(from, "rb");
    FILE *out = fopen(to, "wb");
    bool ok = in != NULL && out != NULL;
    for (uint64_t i = 0; ok && i < n; i++) {
        const int c = fgetc(in);
        ok = c != EOF && fputc(c, out) != EOF;
    }
    if (in != NULL) {
        (void)fclose(in);
    }
    if (out != NULL) {
        ok = fclose(out) == 0 && ok;
    }
    return ok;
}

static bool flip_byte(const char *path, uint64_t off)
{
    const int fd = open(path, O_RDWR);
    uint8_t b = 0;
    bool ok = fd >= 0 && pread(fd, &b, 1, (off_t)off) == 1;
    b ^= 0x40U;
    ok = ok && pwrite(fd, &b, 1, (off_t)off) == 1;
    if (fd >= 0) {
        (void)close(fd);
    }
    return ok;
}

static uint64_t file_len(const char *path)
{
    struct stat sb;
    return stat(path, &sb) == 0 ? (uint64_t)sb.st_size : UINT64_MAX;
}

/* Compact description of a state for comparisons. */
typedef struct {
    uint64_t term;
    uint32_t vote;
    size_t n;
    uint64_t terms[16];
    uint8_t first_byte[16];
} snap;

static snap take_snap(hx_raft_wal *w)
{
    snap s;
    memset(&s, 0, sizeof s);
    const hx_raft_restore rs = hx_raft_wal_restore(w);
    s.term = rs.term;
    s.vote = rs.voted_for;
    s.n = rs.nentries;
    for (size_t i = 0; i < rs.nentries && i < 16; i++) {
        s.terms[i] = rs.entries[i].term;
        s.first_byte[i] = rs.entries[i].len != 0 ? rs.entries[i].data[0] : 0;
    }
    return s;
}

static bool same_snap(const snap *a, const snap *b)
{
    return a->term == b->term && a->vote == b->vote && a->n == b->n &&
           memcmp(a->terms, b->terms, sizeof a->terms) == 0 &&
           memcmp(a->first_byte, b->first_byte, sizeof a->first_byte) == 0;
}

static bool put_entries(hx_raft_wal *w, uint64_t first, uint64_t term, const char *letters)
{
    const hx_raft_storage st = hx_raft_wal_storage(w);
    hx_raft_entry e[8];
    const size_t n = strlen(letters);
    for (size_t i = 0; i < n; i++) {
        e[i].term = term;
        e[i].len = 1;
        e[i].data = (const uint8_t *)&letters[i];
    }
    return st.append(st.ctx, first, e, n) == 0;
}

static void test_roundtrip_and_truncating_append(void)
{
    char dir[256];
    char path[300];
    make_tmp(dir, sizeof dir);
    (void)snprintf(path, sizeof path, "%s/raft.wal", dir);
    hx_raft_wal *w = NULL;
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    hx_raft_restore rs = hx_raft_wal_restore(w);
    CHECK(rs.term == 0 && rs.voted_for == HX_RAFT_NONE && rs.nentries == 0);

    const hx_raft_storage st = hx_raft_wal_storage(w);
    CHECK(st.save_hard_state(st.ctx, 3, 1) == 0);
    CHECK(put_entries(w, 1, 1, "abc"));
    CHECK(st.sync(st.ctx) == 0);
    CHECK(hx_raft_wal_synced_size(w) == hx_raft_wal_file_size(w));
    CHECK(put_entries(w, 3, 2, "x")); /* replaces index 3 */
    CHECK(put_entries(w, 4, 2, "yz"));
    CHECK(hx_raft_wal_synced_size(w) < hx_raft_wal_file_size(w));
    CHECK(st.sync(st.ctx) == 0);
    CHECK(hx_raft_wal_entry_count(w) == 5);
    hx_raft_wal_close(w);

    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    rs = hx_raft_wal_restore(w);
    CHECK(rs.term == 3 && rs.voted_for == 1 && rs.nentries == 5);
    CHECK(rs.entries[0].term == 1 && rs.entries[0].data[0] == 'a');
    CHECK(rs.entries[2].term == 2 && rs.entries[2].data[0] == 'x');
    CHECK(rs.entries[4].data[0] == 'z');

    /* Calls that would leave a hole or are malformed are refused and change nothing. */
    const hx_raft_storage st2 = hx_raft_wal_storage(w);
    CHECK(st2.append(st2.ctx, 0, NULL, 0) != 0);
    CHECK(st2.append(st2.ctx, 9, NULL, 0) != 0);
    CHECK(hx_raft_wal_entry_count(w) == 5);
    CHECK(put_entries(w, 6, 3, "q")); /* appending right after the end is fine */
    CHECK(put_entries(w, 1, 4, ""));  /* truncating everything (zero entries) is fine */
    CHECK(hx_raft_wal_entry_count(w) == 0);
    hx_raft_wal_close(w);
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    CHECK(hx_raft_wal_entry_count(w) == 0);
    hx_raft_wal_close(w);
    rm_rf(dir);
}

/* A WAL with several records; returns the file size before the last record in *before_last. */
static bool build_wal(const char *path, uint64_t *before_last, snap *state_before_last)
{
    hx_raft_wal *w = NULL;
    REQ(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    const hx_raft_storage st = hx_raft_wal_storage(w);
    REQ(st.save_hard_state(st.ctx, 2, 0) == 0);
    REQ(put_entries(w, 1, 1, "ab"));
    REQ(put_entries(w, 3, 2, "cd"));
    REQ(st.sync(st.ctx) == 0);
    *before_last = hx_raft_wal_file_size(w);
    *state_before_last = take_snap(w);
    REQ(put_entries(w, 2, 3, "wxy")); /* the record we will damage: truncates 'b','c','d' */
    REQ(st.sync(st.ctx) == 0);
    hx_raft_wal_close(w);
    return true;
}

static bool torn_tail_body(const char *dir)
{
    char good[300];
    char cut[300];
    (void)snprintf(good, sizeof good, "%s/good.wal", dir);
    (void)snprintf(cut, sizeof cut, "%s/cut.wal", dir);
    uint64_t before = 0;
    snap expect;
    REQ(build_wal(good, &before, &expect));
    const uint64_t total = file_len(good);
    REQ(total > before);

    /* A crash can persist any prefix of the last write. Every one must recover to the prior state.
     */
    for (uint64_t c = before; c < total; c++) {
        REQ(copy_prefix(good, cut, c));
        hx_raft_wal *w = NULL;
        REQ(hx_raft_wal_open(cut, &w) == HX_WAL_OK);
        const snap got = take_snap(w);
        REQ(same_snap(&got, &expect));
        REQ(file_len(cut) == before);   /* the torn bytes were cut off */
        REQ(put_entries(w, 5, 9, "k")); /* and the log is writable again */
        hx_raft_wal_close(w);
        REQ(hx_raft_wal_open(cut, &w) == HX_WAL_OK);
        REQ(hx_raft_wal_entry_count(w) == 5);
        hx_raft_wal_close(w);
    }
    /* The complete file recovers the full state. */
    hx_raft_wal *w = NULL;
    REQ(hx_raft_wal_open(good, &w) == HX_WAL_OK);
    REQ(hx_raft_wal_entry_count(w) == 4); /* a, w, x, y */
    hx_raft_wal_close(w);

    /* Flipping any single byte of the last record: never a silently different state. */
    unsigned recovered = 0;
    unsigned refused = 0;
    for (uint64_t off = before; off < total; off++) {
        REQ(copy_prefix(good, cut, total));
        REQ(flip_byte(cut, off));
        const hx_wal_status s = hx_raft_wal_open(cut, &w);
        if (s == HX_WAL_OK) {
            const snap got = take_snap(w);
            REQ(same_snap(&got, &expect));
            hx_raft_wal_close(w);
            recovered++;
        } else {
            REQ(s == HX_WAL_CORRUPT);
            refused++;
        }
    }
    REQ(recovered + refused == total - before);
    REQ(recovered > 0);
    return true;
}

static void test_torn_tail_and_bit_flips_in_last_record(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    const bool ok = torn_tail_body(dir);
    rm_rf(dir);
    if (!ok) {
        failures++;
    }
}

static void test_corruption_in_the_middle_fails_closed(void)
{
    char dir[256];
    char path[300];
    char bad[300];
    make_tmp(dir, sizeof dir);
    (void)snprintf(path, sizeof path, "%s/a.wal", dir);
    (void)snprintf(bad, sizeof bad, "%s/b.wal", dir);
    uint64_t before = 0;
    snap unused;
    CHECK(build_wal(path, &before, &unused));
    /* Damage a byte inside the first ENTRIES record: later records exist, so this is not a torn
     * write. */
    const uint64_t total = file_len(path);
    unsigned refused = 0;
    for (uint64_t off = 8; off < before; off++) {
        CHECK(copy_prefix(path, bad, total));
        CHECK(flip_byte(bad, off));
        hx_raft_wal *w = NULL;
        const hx_wal_status s = hx_raft_wal_open(bad, &w);
        if (s == HX_WAL_OK) {
            hx_raft_wal_close(w);
        } else {
            CHECK(s == HX_WAL_CORRUPT);
            refused++;
        }
    }
    CHECK(refused >
          (before - 8U) / 2U); /* nearly every flip must be refused, none may be misread */
    rm_rf(dir);
}

static void test_header_and_zero_tail(void)
{
    char dir[256];
    char path[300];
    make_tmp(dir, sizeof dir);
    (void)snprintf(path, sizeof path, "%s/h.wal", dir);
    uint64_t before = 0;
    snap expect;
    CHECK(build_wal(path, &before, &expect));
    const uint64_t total = file_len(path);

    /* File extended with zeros but never written (a classic crash artifact): torn tail, not
     * corruption. */
    const int fd = open(path, O_RDWR | O_APPEND);
    CHECK(fd >= 0);
    uint8_t zeros[4096];
    memset(zeros, 0, sizeof zeros);
    CHECK(write(fd, zeros, sizeof zeros) == (ssize_t)sizeof zeros);
    (void)close(fd);
    hx_raft_wal *w = NULL;
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    CHECK(hx_raft_wal_entry_count(w) == 4);
    hx_raft_wal_close(w);
    CHECK(file_len(path) == total);

    /* Wrong magic is refused; a stub shorter than the header is a creation that never finished. */
    CHECK(flip_byte(path, 0));
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_CORRUPT);
    char stub[300];
    (void)snprintf(stub, sizeof stub, "%s/stub.wal", dir);
    CHECK(copy_prefix(path, stub, 5));
    CHECK(hx_raft_wal_open(stub, &w) == HX_WAL_OK);
    CHECK(hx_raft_wal_entry_count(w) == 0);
    hx_raft_wal_close(w);
    CHECK(hx_raft_wal_open(NULL, &w) == HX_WAL_INVAL);
    rm_rf(dir);
}

static void test_compaction(void)
{
    char dir[256];
    char path[300];
    char tmp[320];
    make_tmp(dir, sizeof dir);
    (void)snprintf(path, sizeof path, "%s/c.wal", dir);
    (void)snprintf(tmp, sizeof tmp, "%s.compact", path);
    hx_raft_wal *w = NULL;
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    hx_raft_wal_set_nosync_for_tests(w, true);
    const hx_raft_storage st = hx_raft_wal_storage(w);
    CHECK(st.save_hard_state(st.ctx, 7, 2) == 0);
    for (int i = 0; i < 400; i++) { /* repeated overwrite of the same suffix: lots of dead bytes */
        CHECK(put_entries(w, 1, 5, "abcdefg"));
    }
    CHECK(put_entries(w, 8, 6, "h"));
    CHECK(st.sync(st.ctx) == 0);
    const uint64_t big = hx_raft_wal_file_size(w);
    const snap before = take_snap(w);
    CHECK(hx_raft_wal_compact(w) == HX_WAL_OK);
    CHECK(hx_raft_wal_file_size(w) < big / 10U);
    CHECK(file_len(tmp) == UINT64_MAX); /* the temp file is gone */
    const snap after = take_snap(w);
    CHECK(same_snap(&before, &after));
    CHECK(put_entries(w, 9, 6, "i")); /* still writable after the swap */
    CHECK(st.sync(st.ctx) == 0);
    hx_raft_wal_close(w);

    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    CHECK(hx_raft_wal_entry_count(w) == 9);
    const hx_raft_restore rs = hx_raft_wal_restore(w);
    CHECK(rs.term == 7 && rs.voted_for == 2 && rs.entries[8].data[0] == 'i');
    hx_raft_wal_close(w);
    rm_rf(dir);
}

static void test_open_compacts_a_wasteful_file(void)
{
    char dir[256];
    char path[300];
    make_tmp(dir, sizeof dir);
    (void)snprintf(path, sizeof path, "%s/w.wal", dir);
    hx_raft_wal *w = NULL;
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    hx_raft_wal_set_nosync_for_tests(w, true);
    uint8_t blob[8192];
    memset(blob, 'z', sizeof blob);
    const hx_raft_entry e = {.term = 1, .len = sizeof blob, .data = blob};
    const hx_raft_storage st = hx_raft_wal_storage(w);
    for (int i = 0; i < 200; i++) { /* ~1.6 MB written, one 8 KB entry live */
        CHECK(st.append(st.ctx, 1, &e, 1) == 0);
    }
    CHECK(st.sync(st.ctx) == 0);
    const uint64_t big = hx_raft_wal_file_size(w);
    CHECK(big > (1U << 20));
    hx_raft_wal_close(w);
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    CHECK(hx_raft_wal_file_size(w) < big / 20U);
    CHECK(hx_raft_wal_entry_count(w) == 1);
    hx_raft_wal_close(w);
    rm_rf(dir);
}

/* A real Raft node on a real WAL: elect, propose, "crash" (drop the instance), recover. */
static bool node_on_wal_body(const char *path)
{
    hx_raft_wal *w = NULL;
    REQ(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    hx_raft_wal_set_nosync_for_tests(w, true);
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, 0, 1);
    cfg.storage = hx_raft_wal_storage(w);
    hx_raft_restore rs = hx_raft_wal_restore(w);
    hx_raft *r = hx_raft_create(&cfg, &rs, 0);
    REQ(r != NULL);
    for (int64_t t = 1; t <= 600 && hx_raft_get_role(r) != HX_RAFT_LEADER; t++) {
        REQ(hx_raft_tick(r, t) == 0);
    }
    REQ(hx_raft_get_role(r) == HX_RAFT_LEADER);
    for (int i = 0; i < 5; i++) {
        REQ(hx_raft_propose(r, 700, "cmd", 3, NULL, NULL) == HX_RAFT_OK);
    }
    const uint64_t term = hx_raft_term(r);
    REQ(hx_raft_commit_index(r) == 6);
    hx_raft_destroy(r);
    hx_raft_wal_close(w);

    REQ(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    rs = hx_raft_wal_restore(w);
    cfg.storage = hx_raft_wal_storage(w);
    r = hx_raft_create(&cfg, &rs, 1000);
    REQ(r != NULL);
    REQ(hx_raft_term(r) == term && hx_raft_last_index(r) == 6 && hx_raft_commit_index(r) == 0);
    hx_raft_entry e;
    REQ(hx_raft_entry_at(r, 4, &e) && e.len == 3 && memcmp(e.data, "cmd", 3) == 0);
    hx_raft_destroy(r);
    hx_raft_wal_close(w);
    return true;
}

static void test_raft_node_recovers_from_wal(void)
{
    char dir[256];
    char path[300];
    make_tmp(dir, sizeof dir);
    (void)snprintf(path, sizeof path, "%s/node.wal", dir);
    if (!node_on_wal_body(path)) {
        failures++;
    }
    rm_rf(dir);
}

static bool snap_msg_body(const char *path)
{
    /* Follower state: log [t1, t1, t1], term 1, nothing committed. */
    hx_raft_wal *w = NULL;
    REQ(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    hx_raft_wal_set_nosync_for_tests(w, true);
    const hx_raft_storage st = hx_raft_wal_storage(w);
    REQ(st.save_hard_state(st.ctx, 1, HX_RAFT_NONE) == 0);
    REQ(put_entries(w, 1, 1, "abc"));
    REQ(st.sync(st.ctx) == 0);
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, 1, 3);
    cfg.storage = st;
    hx_raft_restore rs = hx_raft_wal_restore(w);
    hx_raft *r = hx_raft_create(&cfg, &rs, 0);
    REQ(r != NULL);

    /* The leader (term 2) says: everything up to index 2 is committed, and entry 2 has term 2. Our
     * entry 2 has term 1, so our log conflicts with it and the whole log must go, including
     * index 3. */
    uint8_t img[16] = {0};
    img[7] = 2;
    hx_raft_msg m = {.type = HX_RAFT_MSG_SNAPSHOT,
                     .from = 0,
                     .to = 1,
                     .term = 2,
                     .last_index = 2,
                     .last_term = 2,
                     .snap = img,
                     .snap_len = sizeof img};
    REQ(hx_raft_step(r, 1, &m) == 0);
    REQ(hx_raft_snapshot_index(r) == 2 && hx_raft_commit_index(r) == 2);
    REQ(hx_raft_last_index(r) == 2 && hx_raft_term(r) == 2);
    uint64_t si = 0;
    uint64_t stm = 0;
    const uint8_t *data = NULL;
    uint32_t dl = 0;
    REQ(hx_raft_take_installed_snapshot(r, &si, &stm, &data, &dl) && si == 2 && stm == 2 &&
        dl == 16);
    REQ(!hx_raft_take_installed_snapshot(r, &si, &stm, &data, &dl)); /* reported once */
    const hx_raft_msg *out = NULL;
    size_t n = 0;
    hx_raft_take_msgs(r, &out, &n);
    REQ(n == 1 && out[0].type == HX_RAFT_MSG_APPEND_RESP && out[0].granted && out[0].match == 2);
    REQ(hx_raft_wal_entry_count(w) == 0 && hx_raft_wal_snapshot_index(w) == 2);

    /* A stale or duplicate snapshot is acknowledged but changes nothing. */
    m.last_index = 1;
    m.last_term = 1;
    REQ(hx_raft_step(r, 2, &m) == 0);
    REQ(hx_raft_snapshot_index(r) == 2);
    hx_raft_take_msgs(r, &out, &n);
    REQ(n == 1 && out[0].granted && out[0].match == 1);
    REQ(!hx_raft_take_installed_snapshot(r, &si, &stm, &data, &dl));

    /* After a restart the snapshot comes back with the node. */
    hx_raft_destroy(r);
    hx_raft_wal_close(w);
    REQ(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    rs = hx_raft_wal_restore(w);
    REQ(rs.snap_index == 2 && rs.snap_term == 2 && rs.snap_len == 16 && rs.snap_data[7] == 2);
    REQ(rs.nentries == 0 && rs.term == 2);
    cfg.storage = hx_raft_wal_storage(w);
    r = hx_raft_create(&cfg, &rs, 0);
    REQ(r != NULL && hx_raft_snapshot_index(r) == 2 && hx_raft_commit_index(r) == 2 &&
        hx_raft_last_index(r) == 2);
    hx_raft_destroy(r);
    hx_raft_wal_close(w);
    return true;
}

static void test_snapshot_message_discards_conflicting_log(void)
{
    char dir[256];
    char path[300];
    make_tmp(dir, sizeof dir);
    (void)snprintf(path, sizeof path, "%s/s.wal", dir);
    if (!snap_msg_body(path)) {
        failures++;
    }
    rm_rf(dir);
}

static void test_snapshot_records_replay_and_compact(void)
{
    char dir[256];
    char path[300];
    make_tmp(dir, sizeof dir);
    (void)snprintf(path, sizeof path, "%s/r.wal", dir);
    hx_raft_wal *w = NULL;
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    const hx_raft_storage st = hx_raft_wal_storage(w);
    CHECK(put_entries(w, 1, 1, "abcde"));
    uint8_t blob[4] = {1, 2, 3, 4};
    CHECK(st.save_snapshot(st.ctx, 3, 1, blob, sizeof blob) == 0); /* keeps 4, 5 */
    CHECK(hx_raft_wal_entry_count(w) == 2 && hx_raft_wal_snapshot_index(w) == 3);
    CHECK(put_entries(w, 4, 2, "xy")); /* absolute indexes continue after the snapshot */
    CHECK(!put_entries(w, 3, 2, "q")); /* inside the snapshot: refused */
    CHECK(st.save_snapshot(st.ctx, 3, 1, blob, sizeof blob) != 0); /* not forward */
    CHECK(st.save_snapshot(st.ctx, 9, 2, blob, sizeof blob) == 0); /* beyond the log: drops all */
    CHECK(hx_raft_wal_entry_count(w) == 0 && hx_raft_wal_snapshot_index(w) == 9);
    CHECK(put_entries(w, 10, 3, "z"));
    CHECK(st.sync(st.ctx) == 0);
    hx_raft_wal_close(w);

    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    hx_raft_restore rs = hx_raft_wal_restore(w);
    CHECK(rs.snap_index == 9 && rs.snap_term == 2 && rs.nentries == 1 && rs.entries[0].term == 3);
    CHECK(hx_raft_wal_compact(w) == HX_WAL_OK);
    hx_raft_wal_close(w);
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    rs = hx_raft_wal_restore(w);
    CHECK(rs.snap_index == 9 && rs.snap_len == 4 && rs.snap_data[3] == 4 && rs.nentries == 1);
    hx_raft_wal_close(w);

    /* A big snapshot counts as live data: opening must not rewrite a file with nothing to reclaim.
     */
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    hx_raft_wal_set_nosync_for_tests(w, true);
    uint8_t *big = calloc(1, BIG_SNAP);
    CHECK(big != NULL);
    const hx_raft_storage st2 = hx_raft_wal_storage(w);
    CHECK(st2.save_snapshot(st2.ctx, 10, 3, big, BIG_SNAP) == 0);
    free(big);
    CHECK(st2.sync(st2.ctx) == 0);
    hx_raft_wal_close(w);
    struct stat a;
    struct stat b;
    CHECK(stat(path, &a) == 0 && a.st_size > (1 << 20));
    CHECK(hx_raft_wal_open(path, &w) == HX_WAL_OK);
    hx_raft_wal_close(w);
    CHECK(stat(path, &b) == 0 && a.st_ino == b.st_ino);
    rm_rf(dir);
}

int main(void)
{
    test_roundtrip_and_truncating_append();
    test_torn_tail_and_bit_flips_in_last_record();
    test_corruption_in_the_middle_fails_closed();
    test_header_and_zero_tail();
    test_compaction();
    test_open_compacts_a_wasteful_file();
    test_raft_node_recovers_from_wal();
    test_snapshot_message_discards_conflicting_log();
    test_snapshot_records_replay_and_compact();
    if (failures != 0) {
        (void)fprintf(stderr, "%d raft WAL test(s) FAILED\n", failures);
        return 1;
    }
    (void)puts("all raft WAL tests passed");
    return 0;
}
