/*
 * Broker + codec tests. They drive hx_broker_handle() directly with raw
 * frames, so the full request-decoding and storage path is covered without any
 * sockets (network behaviour is covered by scripts/smoke.sh).
 */
#define _XOPEN_SOURCE 700
#include <dirent.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "broker/broker.h"
#include "proto/codec.h"
#include "proto/protocol.h"
#include "util/buf.h"
#include "util/logger.h"

static int failures;

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

#define CORR 0xC0FFEEU

/* ------------------------------------------------------------------ utils */

static int rm_cb(const char *p, const struct stat *s, int t, struct FTW *f)
{
    (void)s;
    (void)t;
    (void)f;
    return remove(p);
}

static void rm_rf(const char *p)
{
    (void)nftw(p, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
}

static void make_tmp(char *out, size_t cap)
{
    const char *t = getenv("TMPDIR");
    (void)snprintf(out, cap, "%s/helix-broker-XXXXXX", (t != NULL && *t != '\0') ? t : "/tmp");
    if (mkdtemp(out) == NULL) {
        perror("mkdtemp");
        exit(2);
    }
}

static hx_broker *new_broker(const char *dir, uint32_t max_record, uint32_t max_logs)
{
    hx_broker_cfg cfg;
    hx_broker_cfg_default(&cfg);
    cfg.data_dir = dir;
    cfg.log_opts.sync = HX_SYNC_NEVER;
    cfg.log_opts.max_record_bytes = max_record;
    cfg.max_open_logs = max_logs;
    return hx_broker_create(&cfg);
}

/* Build a request (without the length prefix) and run it through the broker. */
static int call(hx_broker *b, uint16_t api, uint16_t ver, const hx_buf *body, uint16_t *err,
                hx_buf *rbody)
{
    hx_buf req;
    hx_buf out;
    hx_buf_init(&req);
    hx_buf_init(&out);
    (void)hx_buf_put_u16(&req, api);
    (void)hx_buf_put_u16(&req, ver);
    (void)hx_buf_put_u32(&req, CORR);
    if (body != NULL) {
        (void)hx_buf_put(&req, body->data, body->len);
    }
    int rc = hx_broker_handle(b, req.data, req.len, &out);
    if (rc == 0) {
        hx_rd rd;
        hx_rd_init(&rd, out.data, out.len);
        uint32_t len = hx_rd_u32(&rd);
        uint32_t corr = hx_rd_u32(&rd);
        *err = hx_rd_u16(&rd);
        if (!rd.ok || len != out.len - 4 || corr != CORR) {
            rc = -2; /* malformed response frame */
        } else if (rbody != NULL) {
            rbody->len = 0;
            (void)hx_buf_put(rbody, out.data + rd.pos, hx_rd_left(&rd));
        }
    }
    hx_buf_free(&req);
    hx_buf_free(&out);
    return rc;
}

static void topic_part(hx_buf *b, const char *topic, size_t tlen, uint32_t part)
{
    (void)hx_buf_put_u16(b, (uint16_t)tlen);
    (void)hx_buf_put(b, topic, tlen);
    (void)hx_buf_put_u32(b, part);
}

static uint16_t produce(hx_broker *b, const char *topic, uint32_t part, const void *payload,
                        uint32_t len, uint64_t *offset)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    topic_part(&body, topic, strlen(topic), part);
    (void)hx_buf_put_u32(&body, len);
    (void)hx_buf_put(&body, payload, len);
    uint16_t err = 0xFFFF;
    int rc = call(b, HX_API_PRODUCE, 1, &body, &err, &resp);
    if (rc == 0 && err == HXE_OK && offset != NULL) {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        *offset = hx_rd_u64(&rd);
        if (!rd.ok) {
            err = 0xFFFE;
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc == 0 ? err : 0xFFFD;
}

static uint16_t fetch(hx_broker *b, const char *topic, uint32_t part, uint64_t offset,
                      uint32_t max_bytes, hx_buf *resp)
{
    hx_buf body;
    hx_buf_init(&body);
    topic_part(&body, topic, strlen(topic), part);
    (void)hx_buf_put_u64(&body, offset);
    (void)hx_buf_put_u32(&body, max_bytes);
    uint16_t err = 0xFFFF;
    int rc = call(b, HX_API_FETCH, 1, &body, &err, resp);
    hx_buf_free(&body);
    return rc == 0 ? err : 0xFFFD;
}

/* Parse a fetch body; returns the record count or -1 if malformed. */
typedef struct {
    uint64_t offset;
    uint32_t len;
    const uint8_t *payload;
} rec;

static long parse_fetch(const hx_buf *resp, rec *out, size_t cap)
{
    hx_rd rd;
    hx_rd_init(&rd, resp->data, resp->len);
    uint32_t count = hx_rd_u32(&rd);
    for (uint32_t i = 0; i < count; i++) {
        rec r;
        r.offset = hx_rd_u64(&rd);
        (void)hx_rd_i64(&rd);
        r.len = hx_rd_u32(&rd);
        r.payload = hx_rd_take(&rd, r.len);
        if (!rd.ok) {
            return -1;
        }
        if (i < cap) {
            out[i] = r;
        }
    }
    return (rd.ok && hx_rd_left(&rd) == 0) ? (long)count : -1;
}

/* ------------------------------------------------------------------ tests */

static void test_header_handling(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);
    uint16_t err = 0xFFFF;

    CHECK(call(b, HX_API_PING, 1, NULL, &err, NULL) == 0 && err == HXE_OK);
    CHECK(call(b, HX_API_PING, 2, NULL, &err, NULL) == 0 && err == HXE_UNSUPPORTED_VERSION);
    CHECK(call(b, 99, 1, NULL, &err, NULL) == 0 && err == HXE_UNKNOWN_API);

    hx_buf junk;
    hx_buf_init(&junk);
    (void)hx_buf_put_u8(&junk, 0);
    CHECK(call(b, HX_API_PING, 1, &junk, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    hx_buf_free(&junk);

    /* A request too short for a header is rejected and leaves `out` untouched. */
    hx_buf out;
    hx_buf_init(&out);
    (void)hx_buf_put_u32(&out, 0xDEADBEEFU);
    const uint8_t shortreq[7] = {0};
    CHECK(hx_broker_handle(b, shortreq, sizeof shortreq, &out) == -1);
    CHECK(out.len == 4);
    hx_buf_free(&out);

    hx_broker_stats st = hx_broker_get_stats(b);
    CHECK(st.requests == 4 && st.errors == 3);
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_produce_fetch_roundtrip(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);

    for (uint64_t i = 0; i < 100; i++) {
        char msg[32];
        int n = snprintf(msg, sizeof msg, "rec-%llu", (unsigned long long)i);
        uint64_t off = 999;
        CHECK(produce(b, "orders", 0, msg, (uint32_t)n, &off) == HXE_OK);
        CHECK(off == i);
    }

    hx_buf resp;
    hx_buf_init(&resp);
    rec recs[128];
    CHECK(fetch(b, "orders", 0, 0, 1U << 20, &resp) == HXE_OK);
    CHECK(parse_fetch(&resp, recs, 128) == 100);
    for (uint64_t i = 0; i < 100; i++) {
        char want[32];
        int n = snprintf(want, sizeof want, "rec-%llu", (unsigned long long)i);
        CHECK(recs[i].offset == i && recs[i].len == (uint32_t)n &&
              memcmp(recs[i].payload, want, recs[i].len) == 0);
    }

    CHECK(fetch(b, "orders", 0, 100, 1024, &resp) == HXE_OK); /* at the tail: empty, not an error */
    CHECK(parse_fetch(&resp, recs, 128) == 0);
    CHECK(fetch(b, "orders", 0, 101, 1024, &resp) == HXE_OFFSET_OUT_OF_RANGE);

    /* max_bytes=1 still returns exactly one record per fetch (guaranteed progress). */
    uint64_t next = 0;
    int rounds = 0;
    while (next < 100 && rounds < 200) {
        CHECK(fetch(b, "orders", 0, next, 1, &resp) == HXE_OK);
        CHECK(parse_fetch(&resp, recs, 128) == 1 && recs[0].offset == next);
        next++;
        rounds++;
    }
    CHECK(next == 100);

    /* A small budget returns a bounded prefix. */
    CHECK(fetch(b, "orders", 0, 0, 100, &resp) == HXE_OK);
    long got = parse_fetch(&resp, recs, 128);
    CHECK(got > 1 && got < 100);

    /* Partitions are independent logs. */
    uint64_t off = 999;
    CHECK(produce(b, "orders", 1, "x", 1, &off) == HXE_OK && off == 0);

    hx_buf_free(&resp);
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_validation(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);

    static const char *const bad[] = {"",
                                      "..",
                                      ".",
                                      ".hidden",
                                      "a/b",
                                      "../x",
                                      "a b",
                                      "a\\b",
                                      "a:b",
                                      "caf\xC3\xA9",
                                      "a\nb",
                                      "__x",
                                      "__consumer_offsets"};
    for (size_t i = 0; i < sizeof bad / sizeof bad[0]; i++) {
        CHECK(produce(b, bad[i], 0, "x", 1, NULL) == HXE_INVALID_TOPIC);
        hx_buf resp;
        hx_buf_init(&resp);
        CHECK(fetch(b, bad[i], 0, 0, 10, &resp) == HXE_INVALID_TOPIC);
        hx_buf_free(&resp);
    }

    char longname[HX_MAX_TOPIC_LEN + 2];
    memset(longname, 'a', sizeof longname);
    longname[HX_MAX_TOPIC_LEN + 1] = '\0';
    CHECK(produce(b, longname, 0, "x", 1, NULL) == HXE_INVALID_TOPIC);
    longname[HX_MAX_TOPIC_LEN] = '\0'; /* exactly the maximum is fine */
    CHECK(produce(b, longname, 0, "x", 1, NULL) == HXE_OK);

    CHECK(produce(b, "ok-topic_1.v2", HX_MAX_PARTITION + 1, "x", 1, NULL) == HXE_UNKNOWN_PARTITION);

    uint8_t big[1025] = {0};
    CHECK(produce(b, "big", 0, big, 1025, NULL) == HXE_RECORD_TOO_LARGE);
    CHECK(produce(b, "big", 0, big, 1024, NULL) == HXE_OK);

    /* Fetch must never create a partition (no directory-creation DoS). */
    hx_buf resp;
    hx_buf_init(&resp);
    CHECK(fetch(b, "ghost", 0, 0, 10, &resp) == HXE_UNKNOWN_PARTITION);
    char path[300];
    (void)snprintf(path, sizeof path, "%s/ghost-0", dir);
    struct stat st;
    CHECK(stat(path, &st) != 0);
    hx_buf_free(&resp);

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_malformed_requests(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);

    hx_buf prod;
    hx_buf fet;
    hx_buf_init(&prod);
    hx_buf_init(&fet);
    topic_part(&prod, "t", 1, 0);
    (void)hx_buf_put_u32(&prod, 3);
    (void)hx_buf_put(&prod, "abc", 3);
    topic_part(&fet, "t", 1, 0);
    (void)hx_buf_put_u64(&fet, 0);
    (void)hx_buf_put_u32(&fet, 100);

    /* Every strict prefix of a valid body must be rejected as BAD_REQUEST. */
    for (size_t n = 0; n < prod.len; n++) {
        hx_buf cut = {.data = prod.data, .len = n, .cap = n};
        uint16_t err = 0;
        CHECK(call(b, HX_API_PRODUCE, 1, &cut, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    }
    for (size_t n = 0; n < fet.len; n++) {
        hx_buf cut = {.data = fet.data, .len = n, .cap = n};
        uint16_t err = 0;
        CHECK(call(b, HX_API_FETCH, 1, &cut, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    }

    /* Trailing garbage is rejected too. */
    (void)hx_buf_put_u8(&prod, 0);
    (void)hx_buf_put_u8(&fet, 0);
    uint16_t err = 0;
    CHECK(call(b, HX_API_PRODUCE, 1, &prod, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    CHECK(call(b, HX_API_FETCH, 1, &fet, &err, NULL) == 0 && err == HXE_BAD_REQUEST);

    /* A payload length that lies about the remaining bytes. */
    hx_buf liar;
    hx_buf_init(&liar);
    topic_part(&liar, "t", 1, 0);
    (void)hx_buf_put_u32(&liar, 0xFFFFFFFFU);
    (void)hx_buf_put(&liar, "abc", 3);
    CHECK(call(b, HX_API_PRODUCE, 1, &liar, &err, NULL) == 0 && err == HXE_BAD_REQUEST);

    hx_buf_free(&prod);
    hx_buf_free(&fet);
    hx_buf_free(&liar);
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_persistence(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);
    uint64_t off = 0;
    CHECK(produce(b, "events", 3, "first", 5, &off) == HXE_OK && off == 0);
    CHECK(produce(b, "events", 3, "second", 6, &off) == HXE_OK && off == 1);
    hx_broker_destroy(b);

    b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);
    hx_buf resp;
    hx_buf_init(&resp);
    rec recs[4];
    CHECK(fetch(b, "events", 3, 0, 4096, &resp) == HXE_OK);
    CHECK(parse_fetch(&resp, recs, 4) == 2);
    CHECK(recs[1].len == 6 && memcmp(recs[1].payload, "second", 6) == 0);
    CHECK(produce(b, "events", 3, "third", 5, &off) == HXE_OK && off == 2);
    hx_buf_free(&resp);
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_open_log_limit(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 3);
    CHECK(b != NULL);
    for (uint32_t p = 0; p < 3; p++) {
        CHECK(produce(b, "lim", p, "x", 1, NULL) == HXE_OK);
    }
    CHECK(produce(b, "lim", 3, "x", 1, NULL) == HXE_RESOURCE_LIMIT);
    CHECK(produce(b, "lim", 1, "y", 1, NULL) == HXE_OK); /* already-open partitions keep working */
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_group_commit_batches_syncs(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker_cfg cfg;
    hx_broker_cfg_default(&cfg);
    cfg.data_dir = dir;
    cfg.log_opts.sync = HX_SYNC_ALWAYS;
    hx_broker *b = hx_broker_create(&cfg);
    CHECK(b != NULL);

    CHECK(hx_broker_commit(b) == 0); /* nothing pending: no-op */
    CHECK(hx_broker_get_stats(b).syncs == 0);

    for (int i = 0; i < 10; i++) {
        CHECK(produce(b, "gc", 0, "x", 1, NULL) == HXE_OK);
    }
    for (int i = 0; i < 3; i++) {
        CHECK(produce(b, "gc", 1, "y", 1, NULL) == HXE_OK);
    }
    CHECK(hx_broker_get_stats(b).syncs == 0); /* appends alone never sync */

    CHECK(hx_broker_commit(b) == 0);
    CHECK(hx_broker_get_stats(b).syncs == 2); /* 13 appends, 2 partitions => 2 fdatasyncs */
    CHECK(hx_broker_commit(b) == 0);
    CHECK(hx_broker_get_stats(b).syncs == 2); /* clean again: no extra sync */

    CHECK(produce(b, "gc", 0, "z", 1, NULL) == HXE_OK);
    CHECK(hx_broker_commit(b) == 0);
    CHECK(hx_broker_get_stats(b).syncs == 3);

    hx_buf resp;
    hx_buf_init(&resp);
    rec recs[16];
    CHECK(fetch(b, "gc", 0, 0, 4096, &resp) == HXE_OK);
    CHECK(parse_fetch(&resp, recs, 16) == 11);
    hx_buf_free(&resp);
    hx_broker_destroy(b);
    rm_rf(dir);
}

/* ------------------------------------------------- consumer-group offsets */

static void put_group(hx_buf *b, const char *group)
{
    const size_t gl = strlen(group);
    (void)hx_buf_put_u16(b, (uint16_t)gl);
    (void)hx_buf_put(b, group, gl);
}

static void commit_body(hx_buf *body, const char *group, const char *topic, uint32_t part,
                        uint64_t offset)
{
    put_group(body, group);
    topic_part(body, topic, strlen(topic), part);
    (void)hx_buf_put_u64(body, offset);
}

static uint16_t ocommit(hx_broker *b, const char *group, const char *topic, uint32_t part,
                        uint64_t offset)
{
    hx_buf body;
    hx_buf_init(&body);
    commit_body(&body, group, topic, part, offset);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_OFFSET_COMMIT, 1, &body, &err, NULL);
    hx_buf_free(&body);
    return rc == 0 ? err : 0xFFFD;
}

/* Returns the wire error; *value is the committed offset or -1 when none exists. */
static uint16_t ofetch(hx_broker *b, const char *group, const char *topic, uint32_t part,
                       int64_t *value)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    put_group(&body, group);
    topic_part(&body, topic, strlen(topic), part);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_OFFSET_FETCH, 1, &body, &err, &resp);
    if (rc == 0 && err == HXE_OK) {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        *value = hx_rd_i64(&rd);
        if (!rd.ok || hx_rd_left(&rd) != 0) {
            err = 0xFFFE;
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc == 0 ? err : 0xFFFD;
}

static void test_offsets_roundtrip_and_validation(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);
    for (int i = 0; i < 5; i++) {
        CHECK(produce(b, "t", 0, "x", 1, NULL) == HXE_OK);
    }

    int64_t v = 12345;
    CHECK(ofetch(b, "g1", "t", 0, &v) == HXE_OK && v == -1); /* nothing committed yet */
    CHECK(ocommit(b, "g1", "t", 0, 3) == HXE_OK);
    CHECK(ofetch(b, "g1", "t", 0, &v) == HXE_OK && v == 3);
    CHECK(ocommit(b, "g1", "t", 0, 5) == HXE_OK); /* == next offset: "consumed everything" */
    CHECK(ofetch(b, "g1", "t", 0, &v) == HXE_OK && v == 5);
    CHECK(ocommit(b, "g1", "t", 0, 6) == HXE_OFFSET_OUT_OF_RANGE); /* beyond the log end */
    CHECK(ofetch(b, "g1", "t", 0, &v) == HXE_OK && v == 5); /* rejected commit changed nothing */
    CHECK(ocommit(b, "g1", "t", 0, 1) == HXE_OK);           /* rewinding is allowed */
    CHECK(ofetch(b, "g1", "t", 0, &v) == HXE_OK && v == 1);

    CHECK(ofetch(b, "g2", "t", 0, &v) == HXE_OK && v == -1); /* groups are independent */
    CHECK(produce(b, "t", 1, "y", 1, NULL) == HXE_OK);
    CHECK(ocommit(b, "g1", "t", 1, 1) == HXE_OK);
    CHECK(ofetch(b, "g1", "t", 1, &v) == HXE_OK && v == 1); /* so are partitions */
    CHECK(ofetch(b, "g1", "t", 0, &v) == HXE_OK && v == 1);

    char longgroup[HX_MAX_GROUP_LEN + 2];
    memset(longgroup, 'g', sizeof longgroup);
    longgroup[sizeof longgroup - 1] = '\0';
    CHECK(ocommit(b, "", "t", 0, 0) == HXE_INVALID_GROUP);
    CHECK(ocommit(b, "a/b", "t", 0, 0) == HXE_INVALID_GROUP);
    CHECK(ocommit(b, ".g", "t", 0, 0) == HXE_INVALID_GROUP);
    CHECK(ocommit(b, longgroup, "t", 0, 0) == HXE_INVALID_GROUP);
    CHECK(ofetch(b, "a b", "t", 0, &v) == HXE_INVALID_GROUP);
    CHECK(ocommit(b, "g1", "../t", 0, 0) == HXE_INVALID_TOPIC);
    CHECK(ocommit(b, "g1", HX_INTERNAL_TOPIC, 0, 0) == HXE_INVALID_TOPIC);
    CHECK(ofetch(b, "g1", HX_INTERNAL_TOPIC, 0, &v) == HXE_INVALID_TOPIC);
    CHECK(ocommit(b, "g1", "t", HX_MAX_PARTITION + 1, 0) == HXE_UNKNOWN_PARTITION);
    CHECK(ocommit(b, "g1", "missing", 0, 0) == HXE_UNKNOWN_PARTITION);
    CHECK(ocommit(b, "g1", "t", 7, 0) == HXE_UNKNOWN_PARTITION);

    /* The internal topic is unreachable through the public data APIs. */
    CHECK(produce(b, HX_INTERNAL_TOPIC, 0, "evil", 4, NULL) == HXE_INVALID_TOPIC);
    hx_buf resp;
    hx_buf_init(&resp);
    CHECK(fetch(b, HX_INTERNAL_TOPIC, 0, 0, 1024, &resp) == HXE_INVALID_TOPIC);
    hx_buf_free(&resp);

    /* Every strict prefix of a valid commit/fetch body is BAD_REQUEST; so is trailing garbage. */
    hx_buf cbody;
    hx_buf_init(&cbody);
    commit_body(&cbody, "g1", "t", 0, 1);
    for (size_t n = 0; n < cbody.len; n++) {
        hx_buf cut = {.data = cbody.data, .len = n, .cap = n};
        uint16_t err = 0;
        CHECK(call(b, HX_API_OFFSET_COMMIT, 1, &cut, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    }
    (void)hx_buf_put_u8(&cbody, 0);
    uint16_t err = 0;
    CHECK(call(b, HX_API_OFFSET_COMMIT, 1, &cbody, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    hx_buf_free(&cbody);

    hx_buf fbody;
    hx_buf_init(&fbody);
    put_group(&fbody, "g1");
    topic_part(&fbody, "t", 1, 0);
    for (size_t n = 0; n < fbody.len; n++) {
        hx_buf cut = {.data = fbody.data, .len = n, .cap = n};
        CHECK(call(b, HX_API_OFFSET_FETCH, 1, &cut, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    }
    hx_buf_free(&fbody);

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_offsets_persist_across_restart(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker_cfg cfg;
    hx_broker_cfg_default(&cfg);
    cfg.data_dir = dir;
    cfg.log_opts.sync = HX_SYNC_ALWAYS; /* offsets must use the same group-commit path */
    hx_broker *b = hx_broker_create(&cfg);
    CHECK(b != NULL);
    for (int i = 0; i < 10; i++) {
        CHECK(produce(b, "orders", 0, "x", 1, NULL) == HXE_OK);
    }
    CHECK(hx_broker_commit(b) == 0);
    const uint64_t syncs_before = hx_broker_get_stats(b).syncs;

    CHECK(ocommit(b, "billing", "orders", 0, 2) == HXE_OK);
    CHECK(ocommit(b, "billing", "orders", 0, 7) == HXE_OK); /* later record must win on replay */
    CHECK(ocommit(b, "audit", "orders", 0, 4) == HXE_OK);
    CHECK(hx_broker_get_stats(b).syncs == syncs_before); /* commits alone never sync */
    CHECK(hx_broker_commit(b) == 0);
    CHECK(hx_broker_get_stats(b).syncs == syncs_before + 1); /* 3 commits, 1 internal partition */
    hx_broker_destroy(b);

    b = hx_broker_create(&cfg);
    CHECK(b != NULL);
    int64_t v = 0;
    CHECK(ofetch(b, "billing", "orders", 0, &v) == HXE_OK && v == 7);
    CHECK(ofetch(b, "audit", "orders", 0, &v) == HXE_OK && v == 4);
    CHECK(ofetch(b, "nobody", "orders", 0, &v) == HXE_OK && v == -1);
    CHECK(ocommit(b, "billing", "orders", 0, 9) == HXE_OK);
    CHECK(hx_broker_commit(b) == 0);
    hx_broker_destroy(b);

    b = hx_broker_create(&cfg);
    CHECK(b != NULL);
    CHECK(ofetch(b, "billing", "orders", 0, &v) == HXE_OK && v == 9);
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_offsets_entry_limit(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker_cfg cfg;
    hx_broker_cfg_default(&cfg);
    cfg.data_dir = dir;
    cfg.log_opts.sync = HX_SYNC_NEVER;
    cfg.max_offset_entries = 2;
    hx_broker *b = hx_broker_create(&cfg);
    CHECK(b != NULL);
    CHECK(produce(b, "t", 0, "x", 1, NULL) == HXE_OK);

    CHECK(ocommit(b, "g1", "t", 0, 0) == HXE_OK);
    CHECK(ocommit(b, "g2", "t", 0, 0) == HXE_OK);
    CHECK(ocommit(b, "g3", "t", 0, 0) == HXE_RESOURCE_LIMIT); /* a third distinct key is refused */
    CHECK(ocommit(b, "g2", "t", 0, 1) == HXE_OK);             /* updating an existing key is not */
    int64_t v = 0;
    CHECK(ofetch(b, "g3", "t", 0, &v) == HXE_OK && v == -1);
    hx_broker_destroy(b);

    /* A refused commit must not have reached the log: replay with the same limit still works. */
    b = hx_broker_create(&cfg);
    CHECK(b != NULL);
    CHECK(ofetch(b, "g2", "t", 0, &v) == HXE_OK && v == 1);
    hx_broker_destroy(b);

    /* Starting with a smaller limit than the stored state fails closed instead of dropping offsets.
     */
    cfg.max_offset_entries = 1;
    CHECK(hx_broker_create(&cfg) == NULL);
    rm_rf(dir);
}

/* ------------------------------------------------------------- metadata */

typedef struct {
    char topic[HX_MAX_TOPIC_LEN + 1];
    uint32_t partition;
    uint64_t first;
    uint64_t next;
} meta;

static long parse_metadata(const hx_buf *resp, meta *out, size_t cap)
{
    hx_rd rd;
    hx_rd_init(&rd, resp->data, resp->len);
    const uint32_t count = hx_rd_u32(&rd);
    for (uint32_t i = 0; i < count; i++) {
        const size_t tl = hx_rd_u16(&rd);
        const uint8_t *t = hx_rd_take(&rd, tl);
        meta m;
        memset(&m, 0, sizeof m);
        m.partition = hx_rd_u32(&rd);
        m.first = hx_rd_u64(&rd);
        m.next = hx_rd_u64(&rd);
        if (!rd.ok || tl > HX_MAX_TOPIC_LEN) {
            return -1;
        }
        memcpy(m.topic, t, tl);
        if (i < cap) {
            out[i] = m;
        }
    }
    return (rd.ok && hx_rd_left(&rd) == 0) ? (long)count : -1;
}

static void test_metadata(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);

    hx_buf resp;
    hx_buf_init(&resp);
    meta m[16];
    uint16_t err = 0;
    CHECK(call(b, HX_API_METADATA, 1, NULL, &err, &resp) == 0 && err == HXE_OK);
    CHECK(parse_metadata(&resp, m, 16) == 0); /* empty broker */

    CHECK(produce(b, "b", 1, "x", 1, NULL) == HXE_OK);
    CHECK(produce(b, "a", 10, "x", 1, NULL) == HXE_OK);
    CHECK(produce(b, "a", 0, "x", 1, NULL) == HXE_OK);
    CHECK(produce(b, "a", 0, "y", 1, NULL) == HXE_OK);
    CHECK(produce(b, "x-y", 2, "x", 1, NULL) == HXE_OK); /* dash inside the topic name */
    CHECK(ocommit(b, "g", "a", 0, 1) == HXE_OK);         /* creates the internal topic */

    /* Directories that are not user partitions must be ignored. */
    char path[300];
    (void)snprintf(path, sizeof path, "%s/junk", dir);
    CHECK(mkdir(path, 0755) == 0);
    (void)snprintf(path, sizeof path, "%s/foo-bar", dir);
    CHECK(mkdir(path, 0755) == 0);
    (void)snprintf(path, sizeof path, "%s/big-99999", dir);
    CHECK(mkdir(path, 0755) == 0);

    CHECK(call(b, HX_API_METADATA, 1, NULL, &err, &resp) == 0 && err == HXE_OK);
    CHECK(parse_metadata(&resp, m, 16) == 4);
    CHECK(strcmp(m[0].topic, "a") == 0 && m[0].partition == 0 && m[0].first == 0 && m[0].next == 2);
    CHECK(strcmp(m[1].topic, "a") == 0 && m[1].partition == 10 && m[1].next == 1);
    CHECK(strcmp(m[2].topic, "b") == 0 && m[2].partition == 1 && m[2].next == 1);
    CHECK(strcmp(m[3].topic, "x-y") == 0 && m[3].partition == 2 && m[3].next == 1);

    hx_buf junk;
    hx_buf_init(&junk);
    (void)hx_buf_put_u8(&junk, 0);
    CHECK(call(b, HX_API_METADATA, 1, &junk, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    hx_buf_free(&junk);
    hx_buf_free(&resp);
    hx_broker_destroy(b);
    rm_rf(dir);
}

/* ------------------------------------------------- group coordinator (fake clock) */

static int64_t g_now;

static int64_t fake_clock(void)
{
    return g_now;
}

static hx_broker *new_group_broker(const char *dir, uint32_t max_groups, uint32_t max_members)
{
    hx_broker_cfg cfg;
    hx_broker_cfg_default(&cfg);
    cfg.data_dir = dir;
    cfg.log_opts.sync = HX_SYNC_NEVER;
    cfg.clock_ms = fake_clock;
    cfg.max_groups = max_groups;
    cfg.max_group_members = max_members;
    return hx_broker_create(&cfg);
}

typedef struct {
    char member[HX_MAX_MEMBER_ID + 1];
    uint32_t gen;
    bool stable;
    uint32_t n;
    struct {
        char topic[HX_MAX_TOPIC_LEN + 1];
        uint32_t part;
    } a[32];
} jres;

static void put_str16(hx_buf *b, const char *s)
{
    const size_t n = strlen(s);
    (void)hx_buf_put_u16(b, (uint16_t)n);
    (void)hx_buf_put(b, s, n);
}

static void join_body(hx_buf *body, const char *group, const char *member, uint32_t session_ms,
                      const char *const *topics, size_t nt)
{
    put_str16(body, group);
    put_str16(body, member);
    (void)hx_buf_put_u32(body, session_ms);
    (void)hx_buf_put_u16(body, (uint16_t)nt);
    for (size_t i = 0; i < nt; i++) {
        put_str16(body, topics[i]);
    }
}

static uint16_t gjoin_n(hx_broker *b, const char *group, const char *member, uint32_t session_ms,
                        const char *const *topics, size_t nt, jres *out)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    join_body(&body, group, member, session_ms, topics, nt);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_JOIN_GROUP, 1, &body, &err, &resp);
    if (rc == 0 && err == HXE_OK && out != NULL) {
        memset(out, 0, sizeof *out);
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        const size_t ml = hx_rd_u16(&rd);
        const uint8_t *m = hx_rd_take(&rd, ml);
        out->gen = hx_rd_u32(&rd);
        out->stable = hx_rd_u8(&rd) != 0;
        out->n = hx_rd_u32(&rd);
        if (rd.ok && ml <= HX_MAX_MEMBER_ID) {
            memcpy(out->member, m, ml);
        }
        for (uint32_t i = 0; i < out->n && rd.ok; i++) {
            const size_t tl = hx_rd_u16(&rd);
            const uint8_t *t = hx_rd_take(&rd, tl);
            const uint32_t part = hx_rd_u32(&rd);
            if (rd.ok && i < 32 && tl <= HX_MAX_TOPIC_LEN) {
                memcpy(out->a[i].topic, t, tl);
                out->a[i].part = part;
            }
        }
        if (!rd.ok || hx_rd_left(&rd) != 0) {
            err = 0xFFFE;
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc == 0 ? err : 0xFFFD;
}

static uint16_t gjoin(hx_broker *b, const char *group, const char *member, const char *topic,
                      jres *out)
{
    return gjoin_n(b, group, member, 10000, &topic, 1, out);
}

static uint16_t gsimple(hx_broker *b, uint16_t api, const char *group, const char *member,
                        int64_t gen)
{
    hx_buf body;
    hx_buf_init(&body);
    put_str16(&body, group);
    put_str16(&body, member);
    if (gen >= 0) {
        (void)hx_buf_put_u32(&body, (uint32_t)gen);
    }
    uint16_t err = 0xFFFF;
    const int rc = call(b, api, 1, &body, &err, NULL);
    hx_buf_free(&body);
    return rc == 0 ? err : 0xFFFD;
}

static uint16_t ghb(hx_broker *b, const char *group, const char *member, uint32_t gen)
{
    return gsimple(b, HX_API_HEARTBEAT, group, member, gen);
}

static uint16_t gleave(hx_broker *b, const char *group, const char *member)
{
    return gsimple(b, HX_API_LEAVE_GROUP, group, member, -1);
}

static uint16_t gcommit(hx_broker *b, const char *group, const char *member, uint32_t gen,
                        const char *topic, uint32_t part, uint64_t offset)
{
    hx_buf body;
    hx_buf_init(&body);
    put_str16(&body, group);
    put_str16(&body, member);
    (void)hx_buf_put_u32(&body, gen);
    topic_part(&body, topic, strlen(topic), part);
    (void)hx_buf_put_u64(&body, offset);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_GROUP_COMMIT, 1, &body, &err, NULL);
    hx_buf_free(&body);
    return rc == 0 ? err : 0xFFFD;
}

/* Returns the wire error; on success fills phase (0 stable, 1 rebalancing), generation, member
 * count. */
static uint16_t gdescribe(hx_broker *b, const char *group, int *phase, uint32_t *gen, uint32_t *nm)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    put_str16(&body, group);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_DESCRIBE_GROUP, 1, &body, &err, &resp);
    if (rc == 0 && err == HXE_OK) {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        *phase = hx_rd_u8(&rd);
        *gen = hx_rd_u32(&rd);
        *nm = hx_rd_u32(&rd);
        if (!rd.ok) {
            err = 0xFFFE;
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc == 0 ? err : 0xFFFD;
}

static bool has_partitions(const jres *r, const char *topic, const uint32_t *parts, size_t n)
{
    if (r->n != n) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (strcmp(r->a[i].topic, topic) != 0 || r->a[i].part != parts[i]) {
            return false;
        }
    }
    return true;
}

static void make_jobs(hx_broker *b, const char *topic, uint32_t nparts)
{
    for (uint32_t p = 0; p < nparts; p++) {
        (void)produce(b, topic, p, "x", 1, NULL);
    }
}

static void test_group_single_member(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker *b = new_group_broker(dir, 10, 10);
    CHECK(b != NULL);
    make_jobs(b, "jobs", 4);

    jres j;
    const uint32_t all[] = {0, 1, 2, 3};
    CHECK(gjoin(b, "g1", "", "jobs", &j) == HXE_OK);
    CHECK(j.stable && j.gen == 1 && j.member[0] != '\0' && has_partitions(&j, "jobs", all, 4));
    char m1[HX_MAX_MEMBER_ID + 1];
    memcpy(m1, j.member, sizeof m1);

    CHECK(ghb(b, "g1", m1, 1) == HXE_OK);
    CHECK(ghb(b, "g1", m1, 2) == HXE_ILLEGAL_GENERATION);
    CHECK(ghb(b, "g1", "nobody", 1) == HXE_UNKNOWN_MEMBER);
    CHECK(ghb(b, "nogroup", m1, 1) == HXE_UNKNOWN_MEMBER);

    /* Re-joining with an unchanged subscription is an idempotent re-fetch, not a rebalance. */
    CHECK(gjoin(b, "g1", m1, "jobs", &j) == HXE_OK);
    CHECK(j.stable && j.gen == 1 && strcmp(j.member, m1) == 0 &&
          has_partitions(&j, "jobs", all, 4));
    CHECK(hx_broker_get_stats(b).group_rebalances == 1);

    /* A subscription to a topic with no partitions is stable with an empty assignment. */
    CHECK(gjoin(b, "g2", "", "ghost", &j) == HXE_OK);
    CHECK(j.stable && j.gen == 1 && j.n == 0);

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_group_rebalance_barrier_and_fencing(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker *b = new_group_broker(dir, 10, 10);
    CHECK(b != NULL);
    make_jobs(b, "jobs", 4);

    jres j1;
    jres j2;
    CHECK(gjoin(b, "g1", "", "jobs", &j1) == HXE_OK && j1.stable && j1.gen == 1);
    char m1[HX_MAX_MEMBER_ID + 1];
    memcpy(m1, j1.member, sizeof m1);

    /* A second member joins: m1 has not rejoined yet, so nothing is assigned. */
    CHECK(gjoin(b, "g1", "", "jobs", &j2) == HXE_OK);
    CHECK(!j2.stable && j2.n == 0 && j2.gen == 1 && j2.member[0] != '\0');
    char m2[HX_MAX_MEMBER_ID + 1];
    memcpy(m2, j2.member, sizeof m2);
    CHECK(ghb(b, "g1", m1, 1) == HXE_REBALANCE_IN_PROGRESS);
    CHECK(ghb(b, "g1", m2, 1) == HXE_REBALANCE_IN_PROGRESS);
    int phase = -1;
    uint32_t gen = 0;
    uint32_t nm = 0;
    CHECK(gdescribe(b, "g1", &phase, &gen, &nm) == HXE_OK && phase == 1 && gen == 1 && nm == 2);

    /* While rebalancing, the old owner can still commit with the old generation (stop-the-world
     * commit). */
    CHECK(gcommit(b, "g1", m1, 1, "jobs", 0, 1) == HXE_OK);

    /* m1 rejoins: everyone is in, assignments are dealt, generation 2 is published. */
    CHECK(gjoin(b, "g1", m1, "jobs", &j1) == HXE_OK);
    const uint32_t evens[] = {0, 2};
    const uint32_t odds[] = {1, 3};
    CHECK(j1.stable && j1.gen == 2 && has_partitions(&j1, "jobs", evens, 2));
    CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK);
    CHECK(j2.stable && j2.gen == 2 && has_partitions(&j2, "jobs", odds, 2));
    CHECK(gdescribe(b, "g1", &phase, &gen, &nm) == HXE_OK && phase == 0 && gen == 2 && nm == 2);

    /* Fencing. */
    CHECK(ghb(b, "g1", m1, 2) == HXE_OK);
    CHECK(ghb(b, "g1", m1, 1) == HXE_ILLEGAL_GENERATION);
    CHECK(gcommit(b, "g1", m1, 1, "jobs", 0, 1) == HXE_ILLEGAL_GENERATION); /* zombie generation */
    CHECK(gcommit(b, "g1", m1, 2, "jobs", 1, 1) == HXE_NOT_ASSIGNED);       /* m2's partition */
    CHECK(gcommit(b, "g1", "stranger", 2, "jobs", 0, 1) == HXE_UNKNOWN_MEMBER);
    CHECK(gcommit(b, "nogroup", m1, 2, "jobs", 0, 1) == HXE_UNKNOWN_MEMBER);
    CHECK(gcommit(b, "g1", m1, 2, "jobs", 0, 99) == HXE_OFFSET_OUT_OF_RANGE);
    CHECK(gcommit(b, "g1", m1, 2, "jobs", 0, 1) == HXE_OK);
    CHECK(gcommit(b, "g1", m2, 2, "jobs", 1, 1) == HXE_OK);
    int64_t v = 0;
    CHECK(ofetch(b, "g1", "jobs", 0, &v) == HXE_OK && v == 1);
    CHECK(ofetch(b, "g1", "jobs", 1, &v) == HXE_OK && v == 1);

    CHECK(hx_broker_get_stats(b).group_rebalances == 2);
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_group_session_expiry(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker *b = new_group_broker(dir, 10, 10);
    CHECK(b != NULL);
    make_jobs(b, "jobs", 4);

    jres j1;
    jres j2;
    CHECK(gjoin(b, "g1", "", "jobs", &j1) == HXE_OK);
    char m1[HX_MAX_MEMBER_ID + 1];
    memcpy(m1, j1.member, sizeof m1);
    CHECK(gjoin(b, "g1", "", "jobs", &j2) == HXE_OK);
    char m2[HX_MAX_MEMBER_ID + 1];
    memcpy(m2, j2.member, sizeof m2);
    CHECK(gjoin(b, "g1", m1, "jobs", &j1) == HXE_OK && j1.stable && j1.gen == 2);
    CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK && j2.stable);

    g_now += 4000;
    CHECK(ghb(b, "g1", m1, 2) == HXE_OK); /* m1 keeps its session alive, m2 stays silent */
    g_now += 4000;
    CHECK(ghb(b, "g1", m1, 2) == HXE_OK); /* 8s since m2 spoke: still within its 10s session */
    g_now += 3000;                        /* 11s: m2 is overdue */
    CHECK(ghb(b, "g1", m1, 2) ==
          HXE_REBALANCE_IN_PROGRESS); /* the request itself applied the expiry */

    int phase = -1;
    uint32_t gen = 0;
    uint32_t nm = 0;
    CHECK(gdescribe(b, "g1", &phase, &gen, &nm) == HXE_OK && phase == 1 && nm == 1);
    const uint32_t all[] = {0, 1, 2, 3};
    CHECK(gjoin(b, "g1", m1, "jobs", &j1) == HXE_OK);
    CHECK(j1.stable && j1.gen == 3 &&
          has_partitions(&j1, "jobs", all, 4)); /* m1 inherits everything */

    CHECK(ghb(b, "g1", m2, 2) ==
          HXE_UNKNOWN_MEMBER); /* the evicted member must rejoin from scratch */
    CHECK(gcommit(b, "g1", m2, 2, "jobs", 1, 1) ==
          HXE_UNKNOWN_MEMBER); /* ...and is fenced meanwhile */

    /* hx_broker_tick() alone (no request) also applies expiry, and an empty group disappears. */
    g_now += 20000;
    hx_broker_tick(b);
    CHECK(gdescribe(b, "g1", &phase, &gen, &nm) == HXE_UNKNOWN_MEMBER);

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_group_rebalance_deadline_evicts_stragglers(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker *b = new_group_broker(dir, 10, 10);
    CHECK(b != NULL);
    make_jobs(b, "jobs", 4);

    jres j1;
    jres j2;
    CHECK(gjoin(b, "g1", "", "jobs", &j1) == HXE_OK);
    char m1[HX_MAX_MEMBER_ID + 1];
    memcpy(m1, j1.member, sizeof m1);
    CHECK(gjoin(b, "g1", "", "jobs", &j2) == HXE_OK &&
          !j2.stable); /* rebalance starts, deadline = +10s */
    char m2[HX_MAX_MEMBER_ID + 1];
    memcpy(m2, j2.member, sizeof m2);

    /* m1 stays alive (heartbeats) but never rejoins: a buggy or stuck client must not block the
     * group. */
    for (int i = 0; i < 3; i++) {
        g_now += 3000;
        CHECK(ghb(b, "g1", m1, 1) == HXE_REBALANCE_IN_PROGRESS);
        CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK && !j2.stable); /* m2 polls and waits */
    }
    g_now += 1500;                                    /* 10.5s after the rebalance started */
    CHECK(ghb(b, "g1", m1, 1) == HXE_UNKNOWN_MEMBER); /* evicted */
    const uint32_t all[] = {0, 1, 2, 3};
    CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK);
    CHECK(j2.stable && j2.gen == 2 && has_partitions(&j2, "jobs", all, 4));

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_group_leave(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker *b = new_group_broker(dir, 10, 10);
    CHECK(b != NULL);
    make_jobs(b, "jobs", 4);

    jres j1;
    jres j2;
    jres j3;
    CHECK(gjoin(b, "g1", "", "jobs", &j1) == HXE_OK);
    char m1[HX_MAX_MEMBER_ID + 1];
    memcpy(m1, j1.member, sizeof m1);
    CHECK(gjoin(b, "g1", "", "jobs", &j2) == HXE_OK);
    char m2[HX_MAX_MEMBER_ID + 1];
    memcpy(m2, j2.member, sizeof m2);
    CHECK(gjoin(b, "g1", m1, "jobs", &j1) == HXE_OK && j1.stable);
    CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK && j2.stable && j2.gen == 2);

    /* A leaving member triggers a rebalance for the one that stays. */
    CHECK(gleave(b, "g1", m1) == HXE_OK);
    CHECK(gleave(b, "g1", m1) == HXE_UNKNOWN_MEMBER);
    CHECK(ghb(b, "g1", m2, 2) == HXE_REBALANCE_IN_PROGRESS);
    const uint32_t all[] = {0, 1, 2, 3};
    CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK);
    CHECK(j2.stable && j2.gen == 3 && has_partitions(&j2, "jobs", all, 4));

    /* Leaving during a rebalance can be what completes it. */
    CHECK(gjoin(b, "g1", "", "jobs", &j3) == HXE_OK && !j3.stable); /* m3 joins, m2 must rejoin */
    char m3[HX_MAX_MEMBER_ID + 1];
    memcpy(m3, j3.member, sizeof m3);
    CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK && j2.stable); /* both in -> gen 4 */
    CHECK(gjoin(b, "g1", "", "jobs", &j1) == HXE_OK &&
          !j1.stable); /* m4 joins, rebalancing again */
    CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK && !j2.stable); /* m2 in, m3 still out */
    CHECK(gleave(b, "g1", m3) == HXE_OK);                           /* the straggler leaves */
    CHECK(gjoin(b, "g1", m2, "jobs", &j2) == HXE_OK);
    CHECK(j2.stable); /* m2 and m4 were the only members left and both had joined */

    /* The last member leaving deletes the group. */
    CHECK(gleave(b, "g1", j1.member) == HXE_OK);
    CHECK(gleave(b, "g1", m2) == HXE_OK);
    int phase = 0;
    uint32_t gen = 0;
    uint32_t nm = 0;
    CHECK(gdescribe(b, "g1", &phase, &gen, &nm) == HXE_UNKNOWN_MEMBER);
    (void)m3;
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_group_subscription_changes(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker *b = new_group_broker(dir, 10, 10);
    CHECK(b != NULL);
    make_jobs(b, "jobs", 4);
    make_jobs(b, "other", 1);

    jres j;
    CHECK(gjoin(b, "g1", "", "jobs", &j) == HXE_OK && j.stable && j.gen == 1 && j.n == 4);
    char m1[HX_MAX_MEMBER_ID + 1];
    memcpy(m1, j.member, sizeof m1);

    const char *both[] = {"jobs", "other"};
    CHECK(gjoin_n(b, "g1", m1, 10000, both, 2, &j) == HXE_OK);
    CHECK(j.stable && j.gen == 2 && j.n == 5); /* a changed subscription is a rebalance */

    const char *reversed[] = {"other", "jobs", "jobs"};
    CHECK(gjoin_n(b, "g1", m1, 10000, reversed, 3, &j) == HXE_OK);
    CHECK(j.stable && j.gen == 2 &&
          j.n == 5); /* same topic set, different order/duplicates: no rebalance */

    /* Members with different subscriptions only receive what they asked for. */
    jres a;
    jres c;
    CHECK(gjoin(b, "g2", "", "jobs", &a) == HXE_OK);
    char ma[HX_MAX_MEMBER_ID + 1];
    memcpy(ma, a.member, sizeof ma);
    CHECK(gjoin(b, "g2", "", "other", &c) == HXE_OK && !c.stable);
    char mc[HX_MAX_MEMBER_ID + 1];
    memcpy(mc, c.member, sizeof mc);
    CHECK(gjoin(b, "g2", ma, "jobs", &a) == HXE_OK && a.stable);
    CHECK(gjoin(b, "g2", mc, "other", &c) == HXE_OK && c.stable);
    const uint32_t jobs_all[] = {0, 1, 2, 3};
    const uint32_t other_all[] = {0};
    CHECK(has_partitions(&a, "jobs", jobs_all, 4));
    CHECK(has_partitions(&c, "other", other_all, 1));

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_group_validation_and_limits(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker *b = new_group_broker(dir, 10, 10);
    CHECK(b != NULL);
    make_jobs(b, "jobs", 2);

    const char *jobs[] = {"jobs"};
    const char *badtopic[] = {"../x"};
    const char *internal[] = {HX_INTERNAL_TOPIC};
    jres j;
    CHECK(gjoin(b, "", "", "jobs", &j) == HXE_INVALID_GROUP);
    CHECK(gjoin(b, "a/b", "", "jobs", &j) == HXE_INVALID_GROUP);
    CHECK(gjoin_n(b, "g", "", 10000, badtopic, 1, &j) == HXE_INVALID_TOPIC);
    CHECK(gjoin_n(b, "g", "", 10000, internal, 1, &j) == HXE_INVALID_TOPIC);
    CHECK(gjoin_n(b, "g", "", 999, jobs, 1, &j) == HXE_BAD_REQUEST); /* session below the minimum */
    CHECK(gjoin_n(b, "g", "", 300001, jobs, 1, &j) == HXE_BAD_REQUEST); /* ...above the maximum */
    CHECK(gjoin_n(b, "g", "", 10000, jobs, 0, &j) == HXE_BAD_REQUEST);  /* no topics */
    const char *many[HX_MAX_SUBSCRIPTIONS + 1];
    for (size_t i = 0; i < HX_MAX_SUBSCRIPTIONS + 1; i++) {
        many[i] = "jobs";
    }
    CHECK(gjoin_n(b, "g", "", 10000, many, HX_MAX_SUBSCRIPTIONS + 1, &j) == HXE_BAD_REQUEST);
    CHECK(gjoin_n(b, "g", "", 10000, many, HX_MAX_SUBSCRIPTIONS, &j) == HXE_OK);

    char longid[HX_MAX_MEMBER_ID + 3];
    memset(longid, 'm', sizeof longid);
    longid[sizeof longid - 1] = '\0';
    CHECK(gjoin(b, "g", longid, "jobs", &j) == HXE_UNKNOWN_MEMBER);
    CHECK(gjoin(b, "g", "m-unknown", "jobs", &j) == HXE_UNKNOWN_MEMBER);
    CHECK(ghb(b, "", "x", 1) == HXE_INVALID_GROUP);

    /* Every strict prefix of a valid body is rejected; so is trailing garbage. */
    hx_buf body;
    hx_buf_init(&body);
    join_body(&body, "g", "", 10000, jobs, 1);
    uint16_t err = 0;
    for (size_t n = 0; n < body.len; n++) {
        hx_buf cut = {.data = body.data, .len = n, .cap = n};
        CHECK(call(b, HX_API_JOIN_GROUP, 1, &cut, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    }
    (void)hx_buf_put_u8(&body, 0);
    CHECK(call(b, HX_API_JOIN_GROUP, 1, &body, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    hx_buf_free(&body);

    hx_buf gc;
    hx_buf_init(&gc);
    put_str16(&gc, "g");
    put_str16(&gc, "m");
    (void)hx_buf_put_u32(&gc, 1);
    topic_part(&gc, "jobs", 4, 0);
    (void)hx_buf_put_u64(&gc, 0);
    for (size_t n = 0; n < gc.len; n++) {
        hx_buf cut = {.data = gc.data, .len = n, .cap = n};
        CHECK(call(b, HX_API_GROUP_COMMIT, 1, &cut, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    }
    hx_buf_free(&gc);
    hx_broker_destroy(b);

    /* Limits are enforced and failed joins leak neither groups nor members. */
    b = new_group_broker(dir, 1, 1);
    CHECK(b != NULL);
    CHECK(gjoin(b, "gA", "", "jobs", &j) == HXE_OK);
    char ma[HX_MAX_MEMBER_ID + 1];
    memcpy(ma, j.member, sizeof ma);
    CHECK(gjoin(b, "gB", "", "jobs", &j) == HXE_RESOURCE_LIMIT); /* member limit */
    CHECK(gjoin(b, "gA", "", "jobs", &j) == HXE_RESOURCE_LIMIT);
    CHECK(gleave(b, "gA", ma) == HXE_OK);
    CHECK(gjoin(b, "gB", "", "jobs", &j) == HXE_OK); /* both limits are free again */
    hx_broker_destroy(b);

    b = new_group_broker(dir, 2, 100);
    CHECK(b != NULL);
    CHECK(gjoin(b, "g1", "", "jobs", &j) == HXE_OK);
    CHECK(gjoin(b, "g2", "", "jobs", &j) == HXE_OK);
    CHECK(gjoin(b, "g3", "", "jobs", &j) == HXE_RESOURCE_LIMIT); /* group limit */
    int phase = 0;
    uint32_t gen = 0;
    uint32_t nm = 0;
    CHECK(gdescribe(b, "g3", &phase, &gen, &nm) == HXE_UNKNOWN_MEMBER);
    CHECK(gjoin(b, "g1", "", "jobs", &j) ==
          HXE_OK); /* extra members of existing groups still fit */
    hx_broker_destroy(b);
    rm_rf(dir);
}

/* ------------------------------------- idempotent producer and topic admin */

static uint16_t init_producer(hx_broker *b, uint64_t *pid)
{
    hx_buf resp;
    hx_buf_init(&resp);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_INIT_PRODUCER, 1, NULL, &err, &resp);
    if (rc == 0 && err == HXE_OK) {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        *pid = hx_rd_u64(&rd);
        if (!rd.ok || hx_rd_left(&rd) != 0) {
            err = 0xFFFE;
        }
    }
    hx_buf_free(&resp);
    return rc == 0 ? err : 0xFFFD;
}

static uint16_t iproduce(hx_broker *b, uint64_t pid, uint32_t seq, const char *topic, uint32_t part,
                         const void *payload, uint32_t len, uint64_t *offset)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    (void)hx_buf_put_u64(&body, pid);
    (void)hx_buf_put_u32(&body, seq);
    topic_part(&body, topic, strlen(topic), part);
    (void)hx_buf_put_u32(&body, len);
    (void)hx_buf_put(&body, payload, len);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_PRODUCE_IDEMPOTENT, 1, &body, &err, &resp);
    if (rc == 0 && err == HXE_OK && offset != NULL) {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        *offset = hx_rd_u64(&rd);
        if (!rd.ok || hx_rd_left(&rd) != 0) {
            err = 0xFFFE;
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc == 0 ? err : 0xFFFD;
}

static uint16_t create_topic(hx_broker *b, const char *topic, uint32_t count)
{
    hx_buf body;
    hx_buf_init(&body);
    put_str16(&body, topic);
    (void)hx_buf_put_u32(&body, count);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_CREATE_TOPIC, 1, &body, &err, NULL);
    hx_buf_free(&body);
    return rc == 0 ? err : 0xFFFD;
}

static uint16_t delete_topic(hx_broker *b, const char *topic)
{
    hx_buf body;
    hx_buf_init(&body);
    put_str16(&body, topic);
    uint16_t err = 0xFFFF;
    const int rc = call(b, HX_API_DELETE_TOPIC, 1, &body, &err, NULL);
    hx_buf_free(&body);
    return rc == 0 ? err : 0xFFFD;
}

/* Next offset of a partition via Metadata, or -1 if it does not exist. */
static int64_t next_offset_of(hx_broker *b, const char *topic, uint32_t part)
{
    hx_buf resp;
    hx_buf_init(&resp);
    meta m[64];
    uint16_t err = 0;
    int64_t result = -1;
    if (call(b, HX_API_METADATA, 1, NULL, &err, &resp) == 0 && err == HXE_OK) {
        const long n = parse_metadata(&resp, m, 64);
        for (long i = 0; i < n && i < 64; i++) {
            if (strcmp(m[i].topic, topic) == 0 && m[i].partition == part) {
                result = (int64_t)m[i].next;
            }
        }
    }
    hx_buf_free(&resp);
    return result;
}

static void test_idempotent_producer(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 16);
    CHECK(b != NULL);

    uint64_t pid = 0;
    uint64_t pid2 = 0;
    uint64_t off = 99;
    CHECK(init_producer(b, &pid) == HXE_OK && pid != 0);
    CHECK(init_producer(b, &pid2) == HXE_OK && pid2 != pid);

    CHECK(iproduce(b, pid, 0, "t", 0, "a", 1, &off) == HXE_OK && off == 0);
    CHECK(iproduce(b, pid, 1, "t", 0, "b", 1, &off) == HXE_OK && off == 1);
    /* A retry of the last record is answered with the original offset and appends nothing. */
    CHECK(iproduce(b, pid, 1, "t", 0, "b", 1, &off) == HXE_OK && off == 1);
    CHECK(iproduce(b, pid, 0, "t", 0, "a", 1, &off) == HXE_OK && off == 0);
    CHECK(next_offset_of(b, "t", 0) == 2);
    CHECK(hx_broker_get_stats(b).deduped_records == 2);

    /* A gap is a lost record: refuse it. */
    CHECK(iproduce(b, pid, 3, "t", 0, "d", 1, &off) == HXE_OUT_OF_ORDER_SEQUENCE);
    CHECK(iproduce(b, pid, 2, "t", 0, "c", 1, &off) == HXE_OK && off == 2);

    /* Another producer interleaving does not disturb this one's sequence; offsets are shared. */
    CHECK(iproduce(b, pid2, 0, "t", 0, "x", 1, &off) == HXE_OK && off == 3);
    CHECK(iproduce(b, pid, 1, "t", 0, "b", 1, &off) == HXE_OK && off == 1); /* still deduped */

    /* Streams are per partition and must start at 0. */
    CHECK(iproduce(b, pid, 5, "t", 1, "z", 1, &off) == HXE_OUT_OF_ORDER_SEQUENCE);
    CHECK(iproduce(b, pid, 0, "t", 1, "z", 1, &off) == HXE_OK && off == 0);

    /* The dedup window is bounded: after 10 more appends sequence 3 is too old. */
    for (uint32_t q = 3; q < 13; q++) {
        CHECK(iproduce(b, pid, q, "t", 0, "w", 1, &off) == HXE_OK);
    }
    CHECK(iproduce(b, pid, 4, "t", 0, "w", 1, &off) == HXE_OUT_OF_ORDER_SEQUENCE);
    CHECK(iproduce(b, pid, 12, "t", 0, "w", 1, &off) == HXE_OK); /* the newest is still a retry */

    /* Unknown producers and bad input; a rejected record does not consume its sequence. */
    CHECK(iproduce(b, pid + 1000, 0, "t", 0, "a", 1, &off) == HXE_UNKNOWN_PRODUCER);
    CHECK(iproduce(b, pid2, 1, "__x", 0, "a", 1, &off) == HXE_INVALID_TOPIC);
    CHECK(iproduce(b, pid2, 1, "t", 5000, "a", 1, &off) == HXE_UNKNOWN_PARTITION);
    uint8_t big[2000];
    memset(big, 'B', sizeof big);
    CHECK(iproduce(b, pid2, 1, "t", 0, big, sizeof big, &off) == HXE_RECORD_TOO_LARGE);
    CHECK(iproduce(b, pid2, 1, "t", 0, "ok", 2, &off) == HXE_OK);

    /* Malformed bodies. */
    hx_buf junk;
    hx_buf_init(&junk);
    (void)hx_buf_put_u8(&junk, 1);
    uint16_t err = 0;
    CHECK(call(b, HX_API_PRODUCE_IDEMPOTENT, 1, &junk, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    CHECK(call(b, HX_API_INIT_PRODUCER, 1, &junk, &err, NULL) == 0 && err == HXE_BAD_REQUEST);
    hx_buf_free(&junk);

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_producer_expiry_limits_and_restart(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker_cfg cfg;
    hx_broker_cfg_default(&cfg);
    cfg.data_dir = dir;
    cfg.log_opts.sync = HX_SYNC_NEVER;
    cfg.clock_ms = fake_clock;
    cfg.max_producers = 2;
    cfg.producer_ttl_ms = 5000;
    hx_broker *b = hx_broker_create(&cfg);
    CHECK(b != NULL);

    uint64_t p1 = 0;
    uint64_t p2 = 0;
    uint64_t p3 = 0;
    CHECK(init_producer(b, &p1) == HXE_OK && init_producer(b, &p2) == HXE_OK);
    CHECK(init_producer(b, &p3) == HXE_RESOURCE_LIMIT);

    /* p1 stays active, p2 goes silent past the TTL and is forgotten. */
    g_now += 3000;
    CHECK(iproduce(b, p1, 0, "t", 0, "a", 1, NULL) == HXE_OK);
    g_now += 3000;
    hx_broker_tick(b);
    CHECK(iproduce(b, p2, 0, "t", 0, "a", 1, NULL) == HXE_UNKNOWN_PRODUCER);
    CHECK(iproduce(b, p1, 1, "t", 0, "b", 1, NULL) == HXE_OK);
    CHECK(init_producer(b, &p3) == HXE_OK); /* space freed */

    /* After a restart every old id is unknown and a new id never collides with an old one. */
    hx_broker_destroy(b);
    (void)usleep(3000); /* ids embed the boot time in ms */
    b = hx_broker_create(&cfg);
    CHECK(b != NULL);
    CHECK(iproduce(b, p1, 2, "t", 0, "c", 1, NULL) == HXE_UNKNOWN_PRODUCER);
    uint64_t fresh = 0;
    CHECK(init_producer(b, &fresh) == HXE_OK && fresh != p1 && fresh != p2 && fresh != p3);

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_topic_create_delete(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    hx_broker *b = new_broker(dir, 1024, 6);
    CHECK(b != NULL);

    CHECK(create_topic(b, "t", 3) == HXE_OK);
    CHECK(next_offset_of(b, "t", 0) == 0 && next_offset_of(b, "t", 2) == 0 &&
          next_offset_of(b, "t", 3) == -1);
    CHECK(create_topic(b, "t", 3) == HXE_TOPIC_EXISTS);
    CHECK(create_topic(b, "__x", 1) == HXE_INVALID_TOPIC);
    CHECK(create_topic(b, "bad name", 1) == HXE_INVALID_TOPIC);
    CHECK(create_topic(b, "z", 0) == HXE_BAD_REQUEST);
    CHECK(create_topic(b, "z", 1025) == HXE_BAD_REQUEST);

    /* Not enough room (6 logs max, 3 used): nothing may be left behind. */
    CHECK(create_topic(b, "big", 4) == HXE_RESOURCE_LIMIT);
    CHECK(next_offset_of(b, "big", 0) == -1);
    CHECK(create_topic(b, "u", 2) == HXE_OK);
    /* Auto-creation on first produce still works for topics that were never created. */
    CHECK(delete_topic(b, "u") == HXE_OK);

    /* Data, offsets and producer state of the topic, plus an unrelated topic. */
    uint64_t off = 0;
    uint64_t pid = 0;
    CHECK(produce(b, "t", 0, "a", 1, &off) == HXE_OK && off == 0);
    CHECK(produce(b, "t", 0, "b", 1, &off) == HXE_OK && off == 1);
    CHECK(produce(b, "keep", 0, "k", 1, &off) == HXE_OK);
    CHECK(init_producer(b, &pid) == HXE_OK);
    CHECK(iproduce(b, pid, 0, "t", 0, "p", 1, &off) == HXE_OK && off == 2);
    CHECK(ocommit(b, "g", "t", 0, 2) == HXE_OK && ocommit(b, "g", "t", 1, 0) == HXE_OK);
    CHECK(ocommit(b, "g", "keep", 0, 1) == HXE_OK);

    CHECK(delete_topic(b, "t") == HXE_OK);
    CHECK(next_offset_of(b, "t", 0) == -1);
    CHECK(delete_topic(b, "t") == HXE_UNKNOWN_PARTITION);
    CHECK(delete_topic(b, "__consumer_offsets") == HXE_INVALID_TOPIC);
    hx_buf resp;
    hx_buf_init(&resp);
    CHECK(fetch(b, "t", 0, 0, 1024, &resp) == HXE_UNKNOWN_PARTITION);
    hx_buf_free(&resp);
    int64_t v = 0;
    CHECK(ofetch(b, "g", "t", 0, &v) == HXE_OK && v == -1);
    CHECK(ofetch(b, "g", "keep", 0, &v) == HXE_OK && v == 1);

    /* A recreated topic starts clean: offset 0, and the old producer stream is gone. */
    CHECK(create_topic(b, "t", 1) == HXE_OK);
    CHECK(iproduce(b, pid, 0, "t", 0, "n", 1, &off) == HXE_OK && off == 0);

    /* The tombstones survive a restart, so deleted offsets do not come back. */
    hx_broker_destroy(b);
    b = new_broker(dir, 1024, 6);
    CHECK(b != NULL);
    CHECK(ofetch(b, "g", "t", 0, &v) == HXE_OK && v == -1);
    CHECK(ofetch(b, "g", "t", 1, &v) == HXE_OK && v == -1);
    CHECK(ofetch(b, "g", "keep", 0, &v) == HXE_OK && v == 1);
    hx_broker_destroy(b);

    /* A delete interrupted by a crash (renamed, not yet removed) is finished at startup. */
    char path[300];
    (void)snprintf(path, sizeof path, "%s/.deleting.old-0", dir);
    CHECK(mkdir(path, 0755) == 0);
    (void)snprintf(path, sizeof path, "%s/.deleting.old-0/00000000000000000000.log", dir);
    FILE *f = fopen(path, "w");
    CHECK(f != NULL);
    (void)fputs("junk", f);
    (void)fclose(f);
    b = new_broker(dir, 1024, 6);
    CHECK(b != NULL);
    (void)snprintf(path, sizeof path, "%s/.deleting.old-0", dir);
    struct stat st;
    CHECK(stat(path, &st) != 0);
    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_topic_changes_trigger_rebalance(void)
{
    char dir[256];
    make_tmp(dir, sizeof dir);
    g_now = 1000;
    hx_broker *b = new_group_broker(dir, 10, 10);
    CHECK(b != NULL);

    /* Subscribing to a topic that does not exist yet is allowed and gives an empty assignment. */
    jres j;
    CHECK(gjoin(b, "g", "", "jobs", &j) == HXE_OK && j.stable && j.n == 0);
    char m1[HX_MAX_MEMBER_ID + 1];
    memcpy(m1, j.member, sizeof m1);
    const uint32_t gen1 = j.gen;
    CHECK(ghb(b, "g", m1, gen1) == HXE_OK);

    CHECK(create_topic(b, "jobs", 2) == HXE_OK);
    CHECK(ghb(b, "g", m1, gen1) == HXE_REBALANCE_IN_PROGRESS);
    const uint32_t both[] = {0, 1};
    CHECK(gjoin(b, "g", m1, "jobs", &j) == HXE_OK && j.stable && j.gen == gen1 + 1 &&
          has_partitions(&j, "jobs", both, 2));
    CHECK(ghb(b, "g", m1, j.gen) == HXE_OK);

    /* Deleting it takes the partitions away and fences the old assignment. */
    const uint32_t gen2 = j.gen;
    CHECK(delete_topic(b, "jobs") == HXE_OK);
    CHECK(ghb(b, "g", m1, gen2) == HXE_REBALANCE_IN_PROGRESS);
    /* The old assignment stays valid until the new generation, but the data is gone. */
    CHECK(gcommit(b, "g", m1, gen2, "jobs", 0, 0) == HXE_UNKNOWN_PARTITION);
    CHECK(gjoin(b, "g", m1, "jobs", &j) == HXE_OK && j.stable && j.n == 0);

    /* A topic nobody subscribes to does not disturb the group. */
    CHECK(create_topic(b, "other", 1) == HXE_OK);
    CHECK(ghb(b, "g", m1, j.gen) == HXE_OK);

    hx_broker_destroy(b);
    rm_rf(dir);
}

static void test_reader_underflow_is_sticky(void)
{
    const uint8_t data[3] = {1, 2, 3};
    hx_rd rd;
    hx_rd_init(&rd, data, sizeof data);
    CHECK(hx_rd_u16(&rd) == 0x0102 && rd.ok);
    CHECK(hx_rd_u32(&rd) == 0 && !rd.ok); /* underflow */
    CHECK(hx_rd_u8(&rd) == 0 && !rd.ok);  /* stays failed even though 1 byte remains */
    CHECK(hx_rd_take(&rd, 0) == NULL);
}

int main(void)
{
    hxl_set_level(HXL_ERROR);
    test_reader_underflow_is_sticky();
    test_header_handling();
    test_produce_fetch_roundtrip();
    test_validation();
    test_malformed_requests();
    test_persistence();
    test_open_log_limit();
    test_group_commit_batches_syncs();
    test_offsets_roundtrip_and_validation();
    test_offsets_persist_across_restart();
    test_offsets_entry_limit();
    test_metadata();
    test_group_single_member();
    test_group_rebalance_barrier_and_fencing();
    test_group_session_expiry();
    test_group_rebalance_deadline_evicts_stragglers();
    test_group_leave();
    test_group_subscription_changes();
    test_group_validation_and_limits();
    test_idempotent_producer();
    test_producer_expiry_limits_and_restart();
    test_topic_create_delete();
    test_topic_changes_trigger_rebalance();

    if (failures != 0) {
        (void)fprintf(stderr, "%d broker test(s) FAILED\n", failures);
        return 1;
    }
    (void)puts("all broker tests passed");
    return 0;
}
