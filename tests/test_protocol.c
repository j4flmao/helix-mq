/*
 * Wire-protocol regression tests against a real broker over real TCP.
 *
 * This is the C regression suite for the wire protocol (its predecessor was a
 * Python client, now gone so the gate needs nothing but a C toolchain): it
 * spawns <HELIX_BIN_DIR>/helixd (an in-process cluster of
 * one node, so the replicated code paths are the ones under test), speaks the
 * framed protocol by hand and checks the answers, then keeps going after every
 * malformed frame to prove the broker survived.
 *
 * Cases:
 *   1. record-size boundary, a restart, and the reloaded log
 *   2. pipelining keeps response order
 *   3. malformed requests fail the request, not the broker
 *   4. unsupported operations on a replicated topic and local-topic routing
 *   5. the smallest accepted record limit
 *   6. the default (1 MiB) record limit
 *   7. an unsafe transport limit is refused at startup
 *   8. invalid cluster files fail cleanly (no sanitizer output)
 *
 *   HELIX_BIN_DIR=build ./test_protocol
 */
#define _XOPEN_SOURCE 700
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int failures;
static char helixd[512];

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
        }                                                                                          \
    } while (0)

/* ------------------------------------------------------------- small utili */

static void section(const char *name)
{
    (void)printf("== %s\n", name);
    (void)fflush(stdout);
}

static int64_t now_ms(void)
{
    struct timespec ts;
    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void sleep_ms(long ms)
{
    struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L};
    (void)nanosleep(&ts, NULL);
}

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

static void put_u16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

static void put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static void put_u64(uint8_t *p, uint64_t v)
{
    put_u32(p, (uint32_t)(v >> 32));
    put_u32(p + 4, (uint32_t)v);
}

static uint16_t get_u16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0] << 8) | (uint16_t)p[1]);
}

static uint32_t get_u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static uint64_t get_u64(const uint8_t *p)
{
    return ((uint64_t)get_u32(p) << 32) | (uint64_t)get_u32(p + 4);
}

/* ----------------------------------------------------------- big-endian rd */

typedef struct {
    const uint8_t *p;
    size_t len;
    size_t pos;
    bool ok;
} rd;

static void rd_init(rd *r, const uint8_t *p, size_t len)
{
    r->p = p;
    r->len = len;
    r->pos = 0;
    r->ok = true;
}

static size_t rd_left(const rd *r)
{
    return r->ok ? r->len - r->pos : 0;
}

static const uint8_t *rd_take(rd *r, size_t n)
{
    if (!r->ok || r->pos + n > r->len) {
        r->ok = false;
        return NULL;
    }
    const uint8_t *p = r->p + r->pos;
    r->pos += n;
    return p;
}

static uint32_t rd_u32(rd *r)
{
    const uint8_t *p = rd_take(r, 4);
    return p != NULL ? get_u32(p) : 0;
}

static uint64_t rd_u64(rd *r)
{
    const uint8_t *p = rd_take(r, 8);
    return p != NULL ? get_u64(p) : 0;
}

static int64_t rd_i64(rd *r)
{
    const uint8_t *p = rd_take(r, 8);
    return p != NULL ? (int64_t)get_u64(p) : 0;
}

/* ------------------------------------------------------------- one-shot run */

/*
 * Run a program to completion with stdout+stderr captured into `out`.
 * Returns its exit status (128 + signal for a signal), or -1 on failure to
 * fork or wait. The output goes through a temporary file, so a child that
 * writes a lot cannot deadlock the test.
 */
static int run_once(char *const argv[], char *out, size_t outcap)
{
    char tmpl[] = "/tmp/helix-run-XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) {
        return -1;
    }
    (void)unlink(tmpl);
    (void)fflush(stdout);
    pid_t pid = fork();
    if (pid < 0) {
        (void)close(fd);
        return -1;
    }
    if (pid == 0) {
        (void)dup2(fd, STDOUT_FILENO);
        (void)dup2(fd, STDERR_FILENO);
        (void)close(fd);
        execv(argv[0], argv);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        (void)close(fd);
        return -1;
    }
    if (out != NULL && outcap > 0) {
        (void)lseek(fd, 0, SEEK_SET);
        size_t n = 0;
        ssize_t got;
        while (n + 1 < outcap && (got = read(fd, out + n, outcap - n - 1)) > 0) {
            n += (size_t)got;
        }
        out[n] = '\0';
    }
    (void)close(fd);
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return WIFSIGNALED(status) ? 128 + WTERMSIG(status) : -1;
}

/* ------------------------------------------------------------------ frame */

#define REPLY_MAX (64u * 1024u)

typedef struct {
    uint32_t corr;
    uint16_t error;
    size_t blen;
    uint8_t body[REPLY_MAX];
} reply;

static size_t frame(uint8_t *out, uint16_t api, const uint8_t *body, size_t blen, uint32_t corr,
                    uint16_t version)
{
    put_u32(out, (uint32_t)(8 + blen));
    put_u16(out + 4, api);
    put_u16(out + 6, version);
    put_u32(out + 8, corr);
    if (blen > 0) {
        memcpy(out + 12, body, blen);
    }
    return 12 + blen;
}

static int write_all(int fd, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    while (n > 0) {
        ssize_t w = send(fd, p, n, 0);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += (size_t)w;
        n -= (size_t)w;
    }
    return 0;
}

static int read_all(int fd, void *buf, size_t n)
{
    uint8_t *p = buf;
    while (n > 0) {
        ssize_t r = recv(fd, p, n, 0);
        if (r == 0) {
            return -1;
        }
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += (size_t)r;
        n -= (size_t)r;
    }
    return 0;
}

static int response_on(int fd, reply *r)
{
    static uint8_t buf[6 + REPLY_MAX];
    uint8_t hdr[4];
    if (read_all(fd, hdr, 4) != 0) {
        return -1;
    }
    const uint32_t len = get_u32(hdr);
    if (len < 6 || len > REPLY_MAX + 6) {
        return -1;
    }
    if (read_all(fd, buf, len) != 0) {
        return -1;
    }
    r->corr = get_u32(buf);
    r->error = get_u16(buf + 4);
    r->blen = len - 6;
    memcpy(r->body, buf + 6, r->blen);
    return 0;
}

static int request_on(int fd, uint16_t api, const uint8_t *body, size_t blen, uint32_t corr,
                      uint16_t version, reply *r)
{
    uint8_t *buf = malloc(12 + blen);
    if (buf == NULL) {
        return -1;
    }
    const size_t n = frame(buf, api, body, blen, corr, version);
    const int rc = write_all(fd, buf, n);
    free(buf);
    if (rc != 0) {
        return -1;
    }
    return response_on(fd, r);
}

/* ----------------------------------------------------------------- fixture */

typedef struct {
    char dir[64];    /* mkdtemp() directory                                  */
    char data[80];   /* <dir>/data                                           */
    char config[96]; /* <dir>/cluster.conf                                   */
    char log[96];    /* <dir>/broker.log                                     */
    uint16_t ports[4];
    pid_t pid;
} fixture;

static bool write_file(const char *path, const char *body)
{
    FILE *f = fopen(path, "wb");
    if (f == NULL) {
        return false;
    }
    const size_t n = strlen(body);
    bool ok = fwrite(body, 1, n, f) == n;
    if (fclose(f) != 0) {
        ok = false;
    }
    return ok;
}

static int connect_client(const fixture *f)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons(f->ports[0]);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0) {
        (void)close(fd);
        return -1;
    }
    return fd;
}

static int request(const fixture *f, uint16_t api, const uint8_t *body, size_t blen, reply *r)
{
    const int fd = connect_client(f);
    if (fd < 0) {
        return -1;
    }
    const int rc = request_on(fd, api, body, blen, 1, 1, r);
    (void)close(fd);
    return rc;
}

/* A length-prefixed field: u16 length, then the bytes. Takes `void *` on
 * purpose -- these are binary frames, not strings, so no terminator belongs
 * after the payload (and clang-tidy's string heuristic stays out of it). */
static size_t bytes_field(uint8_t *out, const void *data, size_t n)
{
    put_u16(out, (uint16_t)n);
    memcpy(out + 2, data, n);
    return 2 + n;
}

static size_t produce_body(uint8_t *out, const char *topic, uint32_t partition, const void *payload,
                           size_t plen)
{
    size_t o = bytes_field(out, topic, strlen(topic));
    put_u32(out + o, partition);
    put_u32(out + o + 4, (uint32_t)plen);
    o += 8;
    if (plen > 0) {
        memcpy(out + o, payload, plen);
        o += plen;
    }
    return o;
}

static size_t fetch_body(uint8_t *out, const char *topic, uint32_t partition, uint64_t offset,
                         uint32_t max_bytes)
{
    size_t o = bytes_field(out, topic, strlen(topic));
    put_u32(out + o, partition);
    put_u64(out + o + 4, offset);
    put_u32(out + o + 12, max_bytes);
    return o + 16;
}

static int produce(const fixture *f, const char *topic, uint32_t partition, const void *payload,
                   size_t plen, reply *r)
{
    uint8_t *body = malloc(2 + strlen(topic) + 8 + plen);
    if (body == NULL) {
        return -1;
    }
    const size_t n = produce_body(body, topic, partition, payload, plen);
    const int rc = request(f, 1, body, n, r);
    free(body);
    return rc;
}

static bool fx_setup(fixture *f)
{
    (void)snprintf(f->dir, sizeof f->dir, "/tmp/helix-protocol-XXXXXX");
    if (mkdtemp(f->dir) == NULL) {
        return false;
    }
    (void)snprintf(f->data, sizeof f->data, "%s/data", f->dir);
    (void)snprintf(f->config, sizeof f->config, "%s/cluster.conf", f->dir);
    (void)snprintf(f->log, sizeof f->log, "%s/broker.log", f->dir);
    f->pid = 0;

    /* Reserve four distinct ephemeral ports, then write the cluster file. */
    int socks[4];
    for (int i = 0; i < 4; i++) {
        socks[i] = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in a;
        memset(&a, 0, sizeof a);
        a.sin_family = AF_INET;
        a.sin_port = 0;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t al = sizeof a;
        if (socks[i] < 0 || bind(socks[i], (struct sockaddr *)&a, sizeof a) != 0 ||
            getsockname(socks[i], (struct sockaddr *)&a, &al) != 0) {
            return false;
        }
        f->ports[i] = ntohs(a.sin_port);
    }
    char conf[256];
    (void)snprintf(conf, sizeof conf, "node 0 127.0.0.1 %u %u %u %u\ntopic events 1\n",
                   (unsigned)f->ports[0], (unsigned)f->ports[1], (unsigned)f->ports[2],
                   (unsigned)f->ports[3]);
    for (int i = 0; i < 4; i++) {
        (void)close(socks[i]);
    }
    return write_file(f->config, conf);
}

/* Start helixd on the fixture; a second call appends to the same log. */
static bool fx_start(fixture *f, const char *limit)
{
    char lim[32];
    (void)snprintf(lim, sizeof lim, "%s", limit);
    const int lfd = open(f->log, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (lfd < 0) {
        return false;
    }
    (void)fflush(stdout);
    const pid_t pid = fork();
    if (pid < 0) {
        (void)close(lfd);
        return false;
    }
    if (pid == 0) {
        (void)dup2(lfd, STDOUT_FILENO);
        (void)dup2(lfd, STDERR_FILENO);
        (void)close(lfd);
        char *argv[] = {
            helixd,      (char *)"--cluster-file",   f->config,      (char *)"--node-id",
            (char *)"0", (char *)"--data-dir",       f->data,        (char *)"--max-record-bytes",
            lim,         (char *)"--ack-timeout-ms", (char *)"1000", NULL};
        execv(helixd, argv);
        _exit(127);
    }
    (void)close(lfd);
    f->pid = pid;

    /* Ready when a Fetch on the (empty) partition is answered. */
    const int64_t deadline = now_ms() + 15000;
    while (now_ms() < deadline) {
        int status = 0;
        if (waitpid(pid, &status, WNOHANG) == pid) {
            f->pid = 0;
            return false;
        }
        uint8_t body[32];
        const size_t n = fetch_body(body, "events", 0, 0, 1024);
        reply r = {0};
        if (request(f, 2, body, n, &r) == 0 && r.error == 0) {
            return true;
        }
        sleep_ms(50);
    }
    return false;
}

/* Returns the exit status, or -1 if it never exited. */
static int fx_stop(fixture *f)
{
    if (f->pid == 0) {
        return 0;
    }
    (void)kill(f->pid, SIGTERM);
    int status = 0;
    int rc = -1;
    if (waitpid(f->pid, &status, 0) >= 0) {
        rc = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }
    f->pid = 0;
    return rc;
}

/* A fresh broker with an empty data directory. */
static bool fx_reset(fixture *f)
{
    if (fx_stop(f) != 0) {
        return false;
    }
    rm_rf(f->data);
    return fx_start(f, "64");
}

static void fx_cleanup(fixture *f)
{
    if (f->pid != 0) {
        (void)kill(f->pid, SIGKILL);
        (void)waitpid(f->pid, NULL, 0);
        f->pid = 0;
    }
    const char *art = getenv("TEST_ARTIFACT_DIR");
    if (art != NULL) {
        char dest[600];
        (void)snprintf(dest, sizeof dest, "%s/protocol.log", art);
        FILE *in = fopen(f->log, "rb");
        FILE *out = in != NULL ? fopen(dest, "wb") : NULL;
        if (in != NULL && out != NULL) {
            char buf[4096];
            size_t n;
            while ((n = fread(buf, 1, sizeof buf, in)) > 0) {
                (void)fwrite(buf, 1, n, out);
            }
        }
        if (out != NULL) {
            (void)fclose(out);
        }
        if (in != NULL) {
            (void)fclose(in);
        }
    }
    rm_rf(f->dir);
}

/* ------------------------------------------------------------------ tests */

static void test_payload_boundary(fixture *f)
{
    section("record-size boundary, restart, reloaded log");
    static const size_t sizes[] = {0, 60, 61, 62, 63, 64};
    uint8_t payload[4096];
    memset(payload, 'x', sizeof payload);

    for (size_t i = 0; i < sizeof sizes / sizeof sizes[0]; i++) {
        reply r = {0};
        CHECK(produce(f, "events", 0, payload, sizes[i], &r) == 0);
        CHECK(r.error == 0);
        CHECK(r.blen == 8);
        CHECK(r.blen == 8 && get_u64(r.body) == i);
    }
    static const size_t too_big[] = {65, 128, 4096};
    for (size_t i = 0; i < sizeof too_big / sizeof too_big[0]; i++) {
        reply r = {0};
        CHECK(produce(f, "events", 0, payload, too_big[i], &r) == 0);
        CHECK(r.error == 6); /* HXE_RECORD_TOO_LARGE */
    }
    CHECK(f->pid != 0); /* the broker survived the rejected records */

    CHECK(fx_stop(f) == 0);
    CHECK(fx_start(f, "64"));

    uint8_t body[32];
    const size_t n = fetch_body(body, "events", 0, 0, 4096);
    reply r = {0};
    CHECK(request(f, 2, body, n, &r) == 0);
    CHECK(r.error == 0);
    rd cur;
    rd_init(&cur, r.body, r.blen);
    CHECK(rd_u32(&cur) == 6);
    for (size_t i = 0; i < 6; i++) {
        const uint64_t off = rd_u64(&cur);
        const int64_t ts = rd_i64(&cur);
        const uint32_t len = rd_u32(&cur);
        const uint8_t *pl = rd_take(&cur, len);
        CHECK(off == i);
        CHECK(ts == 0);
        CHECK(len == sizes[i]);
        bool same = pl != NULL;
        for (uint32_t k = 0; k < len && same; k++) {
            same = pl[k] == 'x';
        }
        CHECK(same);
    }
    CHECK(rd_left(&cur) == 0);

    CHECK(produce(f, "events", 0, (const uint8_t *)"after-restart", 13, &r) == 0);
    CHECK(r.error == 0 && r.blen == 8 && get_u64(r.body) == 6);
}

static void test_pipelining(fixture *f)
{
    section("pipelining keeps response order");
    const int fd = connect_client(f);
    CHECK(fd >= 0);
    if (fd < 0) {
        return;
    }
    uint8_t b1[64];
    uint8_t b2[64];
    const size_t n1 = produce_body(b1, "events", 0, "one", 3);
    const size_t n2 = produce_body(b2, "events", 0, "two", 3);
    uint8_t out[256];
    size_t o = 0;
    o += frame(out + o, 1, b1, n1, 41, 1);
    o += frame(out + o, 1, b2, n2, 42, 1);
    o += frame(out + o, 0, NULL, 0, 43, 1);
    CHECK(write_all(fd, out, o) == 0);

    reply r = {0};
    CHECK(response_on(fd, &r) == 0);
    CHECK(r.corr == 41 && r.error == 0 && r.blen == 8 && get_u64(r.body) == 0);
    CHECK(response_on(fd, &r) == 0);
    CHECK(r.corr == 42 && r.error == 0 && r.blen == 8 && get_u64(r.body) == 1);
    CHECK(response_on(fd, &r) == 0);
    CHECK(r.corr == 43 && r.error == 0);
    (void)close(fd);
}

static void test_malformed(fixture *f)
{
    section("malformed requests fail the request, not the broker");
    uint8_t good[64];
    const size_t gn = produce_body(good, "events", 0, "x", 1);
    reply r = {0};
    CHECK(request(f, 1, good, gn - 1, &r) == 0 && r.error == 1); /* truncated body   */
    uint8_t extra[80];
    memcpy(extra, good, gn);
    extra[gn] = 'e';
    CHECK(request(f, 1, extra, gn + 1, &r) == 0 && r.error == 1); /* trailing byte    */

    uint8_t body[64];
    const size_t pn = produce_body(body, "events", 1, "x", 1);
    CHECK(request(f, 1, body, pn, &r) == 0 && r.error == 8); /* no such partition  */

    const size_t fn2 = fetch_body(body, "events", 0, 1, 100);
    CHECK(request(f, 2, body, fn2, &r) == 0 && r.error == 5); /* offset past the end */

    int fd = connect_client(f);
    if (fd >= 0) {
        uint8_t out[16];
        const size_t o = frame(out, 0, NULL, 0, 1, 99); /* wrong version */
        CHECK(write_all(fd, out, o) == 0);
        CHECK(response_on(fd, &r) == 0 && r.error == 3);
        (void)close(fd);
    }
    /* A length prefix no valid frame could have closes the connection. */
    fd = connect_client(f);
    if (fd >= 0) {
        const uint8_t len[4] = {0xFF, 0xFF, 0xFF, 0xFF};
        CHECK(write_all(fd, len, 4) == 0);
        uint8_t c = 0;
        const ssize_t got = recv(fd, &c, 1, 0);
        CHECK(got == 0 || got < 0);
        (void)close(fd);
    }
    CHECK(request(f, 0, NULL, 0, &r) == 0 && r.error == 0); /* still alive */
}

static void test_unsupported_and_local_routing(fixture *f)
{
    section("replicated-topic admin is unsupported, local topics route");
    reply r = {0};
    uint8_t body[128];
    size_t n = bytes_field(body, "events", 6);
    put_u32(body + n, 1);
    CHECK(request(f, 13, body, n + 4, &r) == 0 && r.error == 21); /* CreateTopic   */
    n = bytes_field(body, "events", 6);
    CHECK(request(f, 14, body, n, &r) == 0 && r.error == 21); /* DeleteTopic       */

    put_u64(body, 1);
    put_u32(body + 8, 0);
    const size_t pn = produce_body(body + 12, "events", 0, "x", 1);
    CHECK(request(f, 12, body, 12 + pn, &r) == 0 && r.error == 21); /* idempotent   */

    /* A topic outside the cluster file is served by the local broker. */
    const size_t ln = produce_body(body, "local", 0, "x", 1);
    CHECK(request(f, 1, body, ln, &r) == 0 && r.error == 0);

    CHECK(request(f, 15, NULL, 0, &r) == 0 && r.error == 0); /* ClusterMetadata    */
    CHECK(request(f, 15, (const uint8_t *)"trailing", 8, &r) == 0 && r.error == 1);
}

static void test_minimum_payload_limit(fixture *f)
{
    section("smallest accepted record limit");
    CHECK(fx_stop(f) == 0);
    CHECK(fx_start(f, "1"));
    reply r = {0};
    CHECK(produce(f, "events", 0, "a", 1, &r) == 0 && r.error == 0);
    CHECK(produce(f, "events", 0, "ab", 2, &r) == 0 && r.error == 6);
    CHECK(request(f, 0, NULL, 0, &r) == 0 && r.error == 0);
}

static void test_default_payload_limit(fixture *f)
{
    section("default (1 MiB) record limit");
    CHECK(fx_stop(f) == 0);
    CHECK(fx_start(f, "1048576"));
    const size_t big = (size_t)1 << 20; /* the default 1 MiB payload limit */
    uint8_t *payload = malloc(big + 1);
    CHECK(payload != NULL);
    if (payload == NULL) {
        return;
    }
    memset(payload, 'x', big + 1);
    reply r = {0};
    CHECK(produce(f, "events", 0, payload, big, &r) == 0 && r.error == 0);
    CHECK(produce(f, "events", 0, payload, big + 1, &r) == 0 && r.error == 6);
    free(payload);
}

static void test_unsafe_transport_limit(fixture *f)
{
    section("unsafe transport limit refused at startup");
    CHECK(fx_stop(f) == 0);
    char other[96];
    (void)snprintf(other, sizeof other, "%s/other", f->dir);
    char *argv[] = {helixd,
                    (char *)"--cluster-file",
                    f->config,
                    (char *)"--node-id",
                    (char *)"0",
                    (char *)"--data-dir",
                    other,
                    (char *)"--max-record-bytes",
                    (char *)"1048577",
                    NULL};
    char out[4096];
    const int rc = run_once(argv, out, sizeof out);
    CHECK(rc > 0);
    CHECK(strstr(out, "max-record-bytes must be between") != NULL);
}

static void test_invalid_cluster_files(void)
{
    section("invalid cluster files fail cleanly");
    char dir[64];
    (void)snprintf(dir, sizeof dir, "/tmp/helix-config-XXXXXX");
    CHECK(mkdtemp(dir) != NULL);
    char cfg[96];
    char data[96];
    (void)snprintf(cfg, sizeof cfg, "%s/cluster.conf", dir);
    (void)snprintf(data, sizeof data, "%s/data", dir);
    const char *node = "node 0 127.0.0.1 9400 9401 9402 9403\n";

    static char many_topics[65 * 32];
    size_t mt = 0;
    for (int i = 0; i < 65; i++) {
        mt += (size_t)snprintf(many_topics + mt, sizeof many_topics - mt, "topic t%d 1\n", i);
    }
    static char many_nodes[9 * 64];
    size_t mn = 0;
    for (int i = 0; i < 9; i++) {
        mn += (size_t)snprintf(many_nodes + mn, sizeof many_nodes - mn,
                               "node %d 127.0.0.1 %d %d %d %d\n", i, 9400 + i * 4, 9401 + i * 4,
                               9402 + i * 4, 9403 + i * 4);
    }
    static char oversized[65538];
    oversized[0] = '#';
    memset(oversized + 1, 'x', 65536);
    oversized[65537] = '\0';

    char dup[128];
    (void)snprintf(dup, sizeof dup, "%stopic a 1\ntopic a 1\n", node);
    char zero[128];
    (void)snprintf(zero, sizeof zero, "%stopic a 0\n", node);
    char many_parts[128];
    (void)snprintf(many_parts, sizeof many_parts, "%stopic a 1025\n", node);
    char reserved[128];
    (void)snprintf(reserved, sizeof reserved, "%stopic __internal 1\n", node);

    const struct {
        const char *name;
        const char *body;
    } cases[] = {
        {"dns", "node 0 localhost 9400 9401 9402 9403\ntopic a 1\n"},
        {"ipv6", "node 0 ::1 9400 9401 9402 9403\ntopic a 1\n"},
        {"zero_partitions", zero},
        {"too_many_partitions", many_parts},
        {"too_many_topics", many_topics},
        {"duplicate_topic", dup},
        {"reserved_topic", reserved},
        {"oversized_file", oversized},
        {"too_many_nodes", many_nodes},
    };

    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
        if (!write_file(cfg, cases[i].body)) {
            CHECK(false);
            continue;
        }
        char *argv[] = {helixd,      (char *)"--cluster-file", cfg,  (char *)"--node-id",
                        (char *)"0", (char *)"--data-dir",     data, NULL};
        char out[8192];
        const int rc = run_once(argv, out, sizeof out);
        if (rc <= 0) {
            (void)fprintf(stderr, "FAIL case '%s': exit %d\n", cases[i].name, rc);
            failures++;
        }
        if (strstr(out, "AddressSanitizer") != NULL || strstr(out, "runtime error:") != NULL) {
            (void)fprintf(stderr, "FAIL case '%s': sanitizer output\n", cases[i].name);
            failures++;
        }
        CHECK(strstr(out, "cluster") != NULL || strstr(out, "helixd") != NULL);
    }
    rm_rf(dir);
}

int main(void)
{
    const char *bindir = getenv("HELIX_BIN_DIR");
    if (bindir == NULL || *bindir == '\0') {
        bindir = "build";
    }
    (void)snprintf(helixd, sizeof helixd, "%s/helixd", bindir);
    if (access(helixd, X_OK) != 0) {
        (void)fprintf(stderr, "%s is not executable: run 'make all' first\n", helixd);
        return 2;
    }
    (void)alarm(300); /* a hung broker must fail the test, not the CI job */

    fixture f;
    if (!fx_setup(&f)) {
        (void)fprintf(stderr, "could not set up the fixture\n");
        return 1;
    }
    CHECK(fx_start(&f, "64"));

    test_payload_boundary(&f);
    CHECK(fx_reset(&f));
    test_pipelining(&f);
    CHECK(fx_reset(&f));
    test_malformed(&f);
    CHECK(fx_reset(&f));
    test_unsupported_and_local_routing(&f);
    test_minimum_payload_limit(&f);
    test_default_payload_limit(&f);
    test_unsafe_transport_limit(&f);
    fx_cleanup(&f);

    test_invalid_cluster_files();

    if (failures != 0) {
        (void)fprintf(stderr, "%d protocol test(s) failed\n", failures);
        return 1;
    }
    (void)puts("all protocol tests passed");
    return 0;
}
