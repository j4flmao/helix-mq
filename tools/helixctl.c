/*
 * helixctl — command-line client for helixd.
 *
 *   helixctl [-H host] [-P port | -F port-file] ping
 *   helixctl ... produce <topic> <partition> [message]   (no message: one record per stdin line)
 *   helixctl ... fetch   <topic> <partition> <offset> [max_records]
 *   helixctl ... bench   <topic> <partition> <count> <size> [depth]
 *   helixctl ... metadata                                  (topic, partition, first, next)
 *   helixctl ... commit    <group> <topic> <partition> <offset>
 *   helixctl ... committed <group> <topic> <partition>     (offset or NONE)
 *   helixctl ... consume   <group> <topic> <partition> [max_records]
 *   helixctl ... join      <group> <topic> <session_ms> [member]    (one JoinGroup call)
 *   helixctl ... heartbeat <group> <member> <generation>
 *   helixctl ... leave     <group> <member>
 *   helixctl ... describe  <group>
 *   helixctl ... gcommit   <group> <member> <generation> <topic> <partition> <offset>
 *   helixctl ... consume-group <group> <topic> <session_ms> <idle_exit_ms> [max_records]
 *   helixctl ... init-producer                                       (prints a producer id)
 *   helixctl ... iproduce  <pid> <seq> <topic> <partition> <message>   (one idempotent produce)
 *   helixctl ... produce-idem <topic> <partition> [message]          (retries without duplicates)
 *   helixctl ... create-topic <topic> <partitions>
 *   helixctl ... delete-topic <topic>
 *
 * consume reads from the group's committed offset (0 if none), prints the records, flushes stdout,
 * and only then commits the next offset: at-least-once delivery, no records skipped on a crash.
 *
 * produce prints "ACK <offset>" per acknowledged record (flushed immediately,
 * so scripts can tell exactly which records were acknowledged).
 * fetch prints "<offset>\t<payload>" per record, reading until the end of log.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "proto/codec.h"
#include "proto/protocol.h"
#include "util/buf.h"

#define MAX_RESPONSE         67108864U
#define FETCH_BYTES          1048576U
#define BENCH_MAX_DEPTH      128U
#define BENCH_MAX_SIZE       65536U
#define FETCH_PARTITION_GONE 4

typedef struct {
    int fd;
    uint32_t next_corr;
    const char *host; /* kept so produce-idem can reconnect and retry */
    const char *port;
} client;

/* ----------------------------------------------------------- socket I/O */

static int send_all(int fd, const uint8_t *p, size_t n)
{
    while (n > 0) {
        ssize_t w = send(fd, p, n, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

static int recv_all(int fd, uint8_t *p, size_t n)
{
    while (n > 0) {
        ssize_t r = recv(fd, p, n, 0);
        if (r < 0 && errno == EINTR) {
            continue;
        }
        if (r <= 0) {
            return -1;
        }
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

static int dial(const char *host, const char *port)
{
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = NULL;
    if (getaddrinfo(host, port, &hints, &res) != 0) {
        return -1;
    }
    int fd = -1;
    for (const struct addrinfo *ai = res; ai != NULL; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
            break;
        }
        (void)close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd >= 0) {
        int one = 1;
        (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    }
    return fd;
}

/* ------------------------------------------------------------- requests */

/* Build a full request frame in `b` (cleared first). */
static int build_request(hx_buf *b, hx_api api, uint32_t corr, const hx_buf *body)
{
    b->len = 0;
    if (hx_buf_put_u32(b, 0) != 0 || hx_buf_put_u16(b, (uint16_t)api) != 0 ||
        hx_buf_put_u16(b, (uint16_t)HX_PROTO_VERSION) != 0 || hx_buf_put_u32(b, corr) != 0) {
        return -1;
    }
    if (body != NULL && hx_buf_put(b, body->data, body->len) != 0) {
        return -1;
    }
    hx_buf_patch_u32(b, 0, (uint32_t)(b->len - 4));
    return 0;
}

/* Reads one response. On success *err is the wire error and `body` holds the body. */
static int read_response(client *c, uint32_t *corr, uint16_t *err, hx_buf *body)
{
    uint8_t lenb[4];
    if (recv_all(c->fd, lenb, sizeof lenb) != 0) {
        return -1;
    }
    uint32_t len = hx_load_be32(lenb);
    if (len < HX_RESP_HDR_LEN || len > MAX_RESPONSE) {
        return -1;
    }
    body->len = 0;
    if (hx_buf_reserve(body, len) != 0 || recv_all(c->fd, body->data, len) != 0) {
        return -1;
    }
    body->len = len;
    hx_rd rd;
    hx_rd_init(&rd, body->data, body->len);
    *corr = hx_rd_u32(&rd);
    *err = hx_rd_u16(&rd);
    hx_buf_consume(body, HX_RESP_HDR_LEN);
    return rd.ok ? 0 : -1;
}

static int roundtrip(client *c, hx_api api, const hx_buf *body, uint16_t *err, hx_buf *resp)
{
    hx_buf req;
    hx_buf_init(&req);
    uint32_t corr = c->next_corr++;
    int rc = build_request(&req, api, corr, body) == 0 ? send_all(c->fd, req.data, req.len) : -1;
    hx_buf_free(&req);
    if (rc != 0) {
        return -1;
    }
    uint32_t got = 0;
    if (read_response(c, &got, err, resp) != 0 || got != corr) {
        return -1;
    }
    return 0;
}

static int put_topic_partition(hx_buf *b, const char *topic, uint32_t partition)
{
    size_t tl = strlen(topic);
    if (tl > UINT16_MAX) {
        return -1;
    }
    return (hx_buf_put_u16(b, (uint16_t)tl) == 0 && hx_buf_put(b, topic, tl) == 0 &&
            hx_buf_put_u32(b, partition) == 0)
               ? 0
               : -1;
}

static int build_produce_body(hx_buf *body, const char *topic, uint32_t partition, const void *msg,
                              size_t mlen)
{
    body->len = 0;
    if (mlen > UINT32_MAX || put_topic_partition(body, topic, partition) != 0 ||
        hx_buf_put_u32(body, (uint32_t)mlen) != 0 || hx_buf_put(body, msg, mlen) != 0) {
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------- commands */

static int parse_uint(const char *s, uint64_t max, uint64_t *out)
{
    if (s == NULL || *s == '\0' || *s == '-') {
        return -1;
    }
    errno = 0;
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno != 0 || *end != '\0' || v > max) {
        return -1;
    }
    *out = v;
    return 0;
}

static int report_error(uint16_t err)
{
    (void)fprintf(stderr, "error: %s (code %u)\n", hx_wire_strerror(err), err);
    return 1;
}

static int cmd_ping(client *c)
{
    uint16_t err = 0;
    hx_buf resp;
    hx_buf_init(&resp);
    int rc = roundtrip(c, HX_API_PING, NULL, &err, &resp);
    hx_buf_free(&resp);
    if (rc != 0) {
        (void)fputs("error: connection failed\n", stderr);
        return 1;
    }
    if (err != HXE_OK) {
        return report_error(err);
    }
    (void)puts("PONG");
    return 0;
}

static int produce_one(client *c, const char *topic, uint32_t partition, const void *msg,
                       size_t mlen, hx_buf *body, hx_buf *resp)
{
    if (build_produce_body(body, topic, partition, msg, mlen) != 0) {
        return 1;
    }
    uint16_t err = 0;
    if (roundtrip(c, HX_API_PRODUCE, body, &err, resp) != 0) {
        (void)fputs("error: connection lost\n", stderr);
        return 1;
    }
    if (err != HXE_OK) {
        return report_error(err);
    }
    hx_rd rd;
    hx_rd_init(&rd, resp->data, resp->len);
    uint64_t off = hx_rd_u64(&rd);
    if (!rd.ok) {
        (void)fputs("error: malformed response\n", stderr);
        return 1;
    }
    (void)printf("ACK %llu\n", (unsigned long long)off);
    (void)fflush(stdout);
    return 0;
}

static int cmd_produce(client *c, const char *topic, uint32_t partition, const char *msg)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    int rc = 0;
    if (msg != NULL) {
        rc = produce_one(c, topic, partition, msg, strlen(msg), &body, &resp);
    } else {
        char *line = NULL;
        size_t cap = 0;
        ssize_t n = 0;
        while (rc == 0 && (n = getline(&line, &cap, stdin)) >= 0) {
            size_t len = (size_t)n;
            if (len > 0 && line[len - 1] == '\n') {
                len--;
            }
            rc = produce_one(c, topic, partition, line, len, &body, &resp);
        }
        free(line);
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc;
}

static int cmd_fetch(client *c, const char *topic, uint32_t partition, uint64_t offset,
                     uint64_t limit, uint64_t *next_out, bool show_partition)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    int rc = 0;
    uint64_t printed = 0;
    while (rc == 0 && printed < limit) {
        body.len = 0;
        uint16_t err = 0;
        if (put_topic_partition(&body, topic, partition) != 0 ||
            hx_buf_put_u64(&body, offset) != 0 || hx_buf_put_u32(&body, FETCH_BYTES) != 0) {
            rc = 1;
            break;
        }
        if (roundtrip(c, HX_API_FETCH, &body, &err, &resp) != 0) {
            (void)fputs("error: connection lost\n", stderr);
            rc = 1;
            break;
        }
        if (err == HXE_UNKNOWN_PARTITION && show_partition) {
            rc = FETCH_PARTITION_GONE; /* group mode: the topic was deleted, caller rejoins */
            break;
        }
        if (err != HXE_OK) {
            rc = report_error(err);
            break;
        }
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        uint32_t count = hx_rd_u32(&rd);
        if (!rd.ok) {
            rc = 1;
            break;
        }
        if (count == 0) {
            break; /* end of log */
        }
        for (uint32_t i = 0; i < count && printed < limit; i++) {
            uint64_t off = hx_rd_u64(&rd);
            (void)hx_rd_i64(&rd); /* timestamp */
            uint32_t len = hx_rd_u32(&rd);
            const uint8_t *p = hx_rd_take(&rd, len);
            if (!rd.ok) {
                (void)fputs("error: malformed response\n", stderr);
                rc = 1;
                break;
            }
            if (show_partition) {
                (void)printf("%u\t", partition);
            }
            (void)printf("%llu\t", (unsigned long long)off);
            (void)fwrite(p, 1, len, stdout);
            (void)fputc('\n', stdout);
            offset = off + 1;
            printed++;
        }
    }
    if (next_out != NULL) {
        *next_out = offset;
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc;
}

/* ------------------------------------------------------ groups / metadata */

static int put_group(hx_buf *b, const char *group)
{
    const size_t gl = strlen(group);
    if (gl > UINT16_MAX) {
        return -1;
    }
    return (hx_buf_put_u16(b, (uint16_t)gl) == 0 && hx_buf_put(b, group, gl) == 0) ? 0 : -1;
}

static int commit_offset(client *c, const char *group, const char *topic, uint32_t partition,
                         uint64_t offset)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    uint16_t err = 0;
    int rc = 1;
    if (put_group(&body, group) == 0 && put_topic_partition(&body, topic, partition) == 0 &&
        hx_buf_put_u64(&body, offset) == 0) {
        if (roundtrip(c, HX_API_OFFSET_COMMIT, &body, &err, &resp) != 0) {
            (void)fputs("error: connection lost\n", stderr);
        } else {
            rc = err == HXE_OK ? 0 : report_error(err);
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc;
}

/* *value = committed offset, or -1 if the group has none. */
static int get_committed(client *c, const char *group, const char *topic, uint32_t partition,
                         int64_t *value)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    uint16_t err = 0;
    int rc = 1;
    if (put_group(&body, group) == 0 && put_topic_partition(&body, topic, partition) == 0) {
        if (roundtrip(c, HX_API_OFFSET_FETCH, &body, &err, &resp) != 0) {
            (void)fputs("error: connection lost\n", stderr);
        } else if (err != HXE_OK) {
            rc = report_error(err);
        } else {
            hx_rd rd;
            hx_rd_init(&rd, resp.data, resp.len);
            *value = hx_rd_i64(&rd);
            if (rd.ok && hx_rd_left(&rd) == 0) {
                rc = 0;
            } else {
                (void)fputs("error: malformed response\n", stderr);
            }
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc;
}

static int cmd_commit(client *c, const char *group, const char *topic, uint32_t partition,
                      uint64_t offset)
{
    const int rc = commit_offset(c, group, topic, partition, offset);
    if (rc == 0) {
        (void)puts("OK");
    }
    return rc;
}

static int cmd_committed(client *c, const char *group, const char *topic, uint32_t partition)
{
    int64_t v = -1;
    const int rc = get_committed(c, group, topic, partition, &v);
    if (rc == 0) {
        if (v < 0) {
            (void)puts("NONE");
        } else {
            (void)printf("%lld\n", (long long)v);
        }
    }
    return rc;
}

static int cmd_consume(client *c, const char *group, const char *topic, uint32_t partition,
                       uint64_t limit)
{
    int64_t committed = -1;
    int rc = get_committed(c, group, topic, partition, &committed);
    if (rc != 0) {
        return rc;
    }
    const uint64_t start = committed < 0 ? 0 : (uint64_t)committed;
    uint64_t next = start;
    rc = cmd_fetch(c, topic, partition, start, limit, &next, false);
    if (rc != 0 || next == start) {
        return rc;
    }
    /* Deliver first, commit second: a crash in between re-delivers instead of losing records. */
    if (fflush(stdout) != 0) {
        (void)fputs("error: cannot write to stdout; offset not committed\n", stderr);
        return 1;
    }
    return commit_offset(c, group, topic, partition, next);
}

static int cmd_metadata(client *c)
{
    hx_buf resp;
    hx_buf_init(&resp);
    uint16_t err = 0;
    int rc = 1;
    if (roundtrip(c, HX_API_METADATA, NULL, &err, &resp) != 0) {
        (void)fputs("error: connection lost\n", stderr);
    } else if (err != HXE_OK) {
        rc = report_error(err);
    } else {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        const uint32_t count = hx_rd_u32(&rd);
        rc = 0;
        for (uint32_t i = 0; i < count && rd.ok; i++) {
            const size_t tl = hx_rd_u16(&rd);
            const uint8_t *t = hx_rd_take(&rd, tl);
            const uint32_t p = hx_rd_u32(&rd);
            const uint64_t first = hx_rd_u64(&rd);
            const uint64_t next = hx_rd_u64(&rd);
            if (!rd.ok) {
                break;
            }
            (void)printf("%.*s\t%u\t%llu\t%llu\n", (int)tl, (const char *)t, p,
                         (unsigned long long)first, (unsigned long long)next);
        }
        if (!rd.ok || hx_rd_left(&rd) != 0) {
            (void)fputs("error: malformed response\n", stderr);
            rc = 1;
        }
    }
    hx_buf_free(&resp);
    return rc;
}

/* Replicated-cluster view of one node: who the nodes are and, per partition, what this node knows.
 */
static int cmd_cluster_metadata(client *c)
{
    hx_buf resp;
    hx_buf_init(&resp);
    uint16_t err = 0;
    int rc = 1;
    if (roundtrip(c, HX_API_CLUSTER_METADATA, NULL, &err, &resp) != 0) {
        (void)fputs("error: connection lost\n", stderr);
    } else if (err != HXE_OK) {
        rc = report_error(err);
    } else {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        (void)printf("self\t%u\n", hx_rd_u32(&rd));
        const uint32_t nn = hx_rd_u32(&rd);
        for (uint32_t i = 0; i < nn && rd.ok; i++) {
            const uint32_t id = hx_rd_u32(&rd);
            const size_t hl = hx_rd_u16(&rd);
            const uint8_t *h = hx_rd_take(&rd, hl);
            const uint16_t port = hx_rd_u16(&rd);
            if (rd.ok) {
                (void)printf("node\t%u\t%.*s\t%u\n", id, (int)hl, (const char *)h, port);
            }
        }
        const uint32_t np = hx_rd_u32(&rd);
        for (uint32_t i = 0; i < np && rd.ok; i++) {
            const size_t tl = hx_rd_u16(&rd);
            const uint8_t *t = hx_rd_take(&rd, tl);
            const uint32_t p = hx_rd_u32(&rd);
            const uint32_t leader = hx_rd_u32(&rd);
            const uint32_t epoch = hx_rd_u32(&rd);
            const uint32_t isr = hx_rd_u32(&rd);
            const uint64_t hw = hx_rd_u64(&rd);
            const uint64_t leo = hx_rd_u64(&rd);
            const uint8_t role = hx_rd_u8(&rd);
            if (!rd.ok) {
                break;
            }
            if (leader == UINT32_MAX) {
                (void)printf("part\t%.*s\t%u\t-\t%u\t%u\t%llu\t%llu\t%u\n", (int)tl,
                             (const char *)t, p, epoch, isr, (unsigned long long)hw,
                             (unsigned long long)leo, role);
            } else {
                (void)printf("part\t%.*s\t%u\t%u\t%u\t%u\t%llu\t%llu\t%u\n", (int)tl,
                             (const char *)t, p, leader, epoch, isr, (unsigned long long)hw,
                             (unsigned long long)leo, role);
            }
        }
        rc = 0;
        if (!rd.ok || hx_rd_left(&rd) != 0) {
            (void)fputs("error: malformed response\n", stderr);
            rc = 1;
        }
    }
    hx_buf_free(&resp);
    return rc;
}

/* ----------------------------------------------------- consumer-group client */

#define MAX_ASSIGN       1024U
#define GROUP_FETCH_PASS 100U
#define POLL_MS          50U

typedef struct {
    char topic[HX_MAX_TOPIC_LEN + 1];
    uint32_t partition;
    uint64_t position;
} assignment;

typedef struct {
    char id[HX_MAX_MEMBER_ID + 1];
    uint32_t generation;
    bool stable;
    size_t n;
    assignment a[MAX_ASSIGN];
} membership;

static uint64_t now_ns(void)
{
    struct timespec ts;
    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static volatile sig_atomic_t g_stop = 0;

static void on_term(int sig)
{
    (void)sig;
    g_stop = 1;
}

static void sleep_ms(unsigned ms)
{
    struct timespec ts = {.tv_sec = (time_t)(ms / 1000U), .tv_nsec = (long)(ms % 1000U) * 1000000L};
    (void)nanosleep(&ts, NULL);
}

static int put_member(hx_buf *b, const char *group, const char *member)
{
    const size_t ml = strlen(member);
    if (put_group(b, group) != 0 || ml > UINT16_MAX) {
        return -1;
    }
    return (hx_buf_put_u16(b, (uint16_t)ml) == 0 && hx_buf_put(b, member, ml) == 0) ? 0 : -1;
}

/* One request/response with an empty-bodied success. Returns the wire error, or -1 on I/O failure.
 */
static int simple_call(client *c, hx_api api, const hx_buf *body)
{
    hx_buf resp;
    hx_buf_init(&resp);
    uint16_t err = 0;
    int rc = roundtrip(c, api, body, &err, &resp) != 0 ? -1 : (int)err;
    hx_buf_free(&resp);
    return rc;
}

static int finish_simple(int r)
{
    if (r < 0) {
        (void)fputs("error: connection lost\n", stderr);
        return 1;
    }
    if (r != HXE_OK) {
        return report_error((uint16_t)r);
    }
    (void)puts("OK");
    return 0;
}

/* Reads (u32 n | (u16 tlen topic u32 partition)*) into m->a. */
static bool read_assignments(hx_rd *rd, membership *m)
{
    const uint32_t n = hx_rd_u32(rd);
    if (!rd->ok || n > MAX_ASSIGN) {
        return false;
    }
    m->n = 0;
    for (uint32_t i = 0; i < n; i++) {
        const size_t tl = hx_rd_u16(rd);
        const uint8_t *t = hx_rd_take(rd, tl);
        const uint32_t p = hx_rd_u32(rd);
        if (!rd->ok || tl > HX_MAX_TOPIC_LEN) {
            return false;
        }
        memcpy(m->a[m->n].topic, t, tl);
        m->a[m->n].topic[tl] = '\0';
        m->a[m->n].partition = p;
        m->a[m->n].position = 0;
        m->n++;
    }
    return true;
}

/* One JoinGroup call. Returns 0 on success (m filled), the wire error, or -1 on I/O failure. */
static int join_once(client *c, const char *group, const char *topic, uint32_t session_ms,
                     membership *m)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    int rc = -1;
    const size_t tl = strlen(topic);
    if (put_member(&body, group, m->id) == 0 && hx_buf_put_u32(&body, session_ms) == 0 &&
        hx_buf_put_u16(&body, 1) == 0 && tl <= UINT16_MAX &&
        hx_buf_put_u16(&body, (uint16_t)tl) == 0 && hx_buf_put(&body, topic, tl) == 0) {
        uint16_t err = 0;
        if (roundtrip(c, HX_API_JOIN_GROUP, &body, &err, &resp) == 0) {
            rc = err;
            if (err == HXE_OK) {
                hx_rd rd;
                hx_rd_init(&rd, resp.data, resp.len);
                const size_t il = hx_rd_u16(&rd);
                const uint8_t *id = hx_rd_take(&rd, il);
                m->generation = hx_rd_u32(&rd);
                m->stable = hx_rd_u8(&rd) != 0;
                if (!rd.ok || il > HX_MAX_MEMBER_ID) {
                    rc = -1;
                } else {
                    memcpy(m->id, id, il);
                    m->id[il] = '\0';
                    if (!read_assignments(&rd, m) || hx_rd_left(&rd) != 0) {
                        rc = -1;
                    }
                }
            }
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc;
}

static int cmd_join(client *c, const char *group, const char *topic, uint32_t session_ms,
                    const char *member)
{
    static membership m;
    memset(&m, 0, sizeof m);
    if (member != NULL && snprintf(m.id, sizeof m.id, "%s", member) >= (int)sizeof m.id) {
        return 2;
    }
    const int r = join_once(c, group, topic, session_ms, &m);
    if (r != 0) {
        return r < 0 ? finish_simple(r) : report_error((uint16_t)r);
    }
    (void)printf("member=%s generation=%u stable=%d\n", m.id, m.generation, m.stable ? 1 : 0);
    for (size_t i = 0; i < m.n; i++) {
        (void)printf("%s\t%u\n", m.a[i].topic, m.a[i].partition);
    }
    return 0;
}

static int cmd_heartbeat(client *c, const char *group, const char *member, uint32_t generation)
{
    hx_buf body;
    hx_buf_init(&body);
    int rc = 1;
    if (put_member(&body, group, member) == 0 && hx_buf_put_u32(&body, generation) == 0) {
        rc = finish_simple(simple_call(c, HX_API_HEARTBEAT, &body));
    }
    hx_buf_free(&body);
    return rc;
}

static int cmd_leave(client *c, const char *group, const char *member)
{
    hx_buf body;
    hx_buf_init(&body);
    int rc = 1;
    if (put_member(&body, group, member) == 0) {
        rc = finish_simple(simple_call(c, HX_API_LEAVE_GROUP, &body));
    }
    hx_buf_free(&body);
    return rc;
}

static int group_commit(client *c, const char *group, const membership *m, const assignment *as,
                        uint64_t offset)
{
    hx_buf body;
    hx_buf_init(&body);
    int rc = -1;
    if (put_member(&body, group, m->id) == 0 && hx_buf_put_u32(&body, m->generation) == 0 &&
        put_topic_partition(&body, as->topic, as->partition) == 0 &&
        hx_buf_put_u64(&body, offset) == 0) {
        rc = simple_call(c, HX_API_GROUP_COMMIT, &body);
    }
    hx_buf_free(&body);
    return rc;
}

static int cmd_gcommit(client *c, const char *group, const char *member, uint32_t generation,
                       const char *topic, uint32_t partition, uint64_t offset)
{
    membership *m = calloc(1, sizeof *m);
    if (m == NULL || snprintf(m->id, sizeof m->id, "%s", member) >= (int)sizeof m->id) {
        free(m);
        return 2;
    }
    m->generation = generation;
    assignment as;
    memset(&as, 0, sizeof as);
    if (snprintf(as.topic, sizeof as.topic, "%s", topic) >= (int)sizeof as.topic) {
        free(m);
        return 2;
    }
    as.partition = partition;
    const int rc = finish_simple(group_commit(c, group, m, &as, offset));
    free(m);
    return rc;
}

static int cmd_describe(client *c, const char *group)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    uint16_t err = 0;
    int rc = 1;
    if (put_group(&body, group) == 0) {
        if (roundtrip(c, HX_API_DESCRIBE_GROUP, &body, &err, &resp) != 0) {
            (void)fputs("error: connection lost\n", stderr);
        } else if (err != HXE_OK) {
            rc = report_error(err);
        } else {
            hx_rd rd;
            hx_rd_init(&rd, resp.data, resp.len);
            const uint8_t rebalancing = hx_rd_u8(&rd);
            const uint32_t gen = hx_rd_u32(&rd);
            const uint32_t n = hx_rd_u32(&rd);
            (void)printf("state=%s generation=%u members=%u\n",
                         rebalancing ? "rebalancing" : "stable", gen, n);
            for (uint32_t i = 0; i < n && rd.ok; i++) {
                const size_t il = hx_rd_u16(&rd);
                const uint8_t *id = hx_rd_take(&rd, il);
                const uint32_t na = hx_rd_u32(&rd);
                if (!rd.ok) {
                    break;
                }
                (void)printf("member\t%.*s\t", (int)il, (const char *)id);
                for (uint32_t k = 0; k < na && rd.ok; k++) {
                    const size_t tl = hx_rd_u16(&rd);
                    const uint8_t *t = hx_rd_take(&rd, tl);
                    const uint32_t p = hx_rd_u32(&rd);
                    if (rd.ok) {
                        (void)printf("%s%.*s:%u", k == 0 ? "" : ",", (int)tl, (const char *)t, p);
                    }
                }
                (void)putchar('\n');
            }
            if (rd.ok && hx_rd_left(&rd) == 0) {
                rc = 0;
            } else {
                (void)fputs("error: malformed response\n", stderr);
            }
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc;
}

/* A wire error that means "my membership is stale: rejoin" rather than "give up". */
static bool is_membership_error(int r)
{
    return r == HXE_REBALANCE_IN_PROGRESS || r == HXE_ILLEGAL_GENERATION ||
           r == HXE_UNKNOWN_MEMBER || r == HXE_NOT_ASSIGNED;
}

/* Join and poll until the group is stable. Returns 0 ok, 1 stopped by signal, -1 fatal. */
static int join_until_stable(client *c, const char *group, const char *topic, uint32_t session_ms,
                             membership *m)
{
    while (g_stop == 0) {
        const int r = join_once(c, group, topic, session_ms, m);
        if (r == HXE_UNKNOWN_MEMBER) {
            m->id[0] = '\0'; /* our id was evicted: start over as a new member */
            continue;
        }
        if (r != 0) {
            (void)fprintf(stderr, "error: join failed (%d)\n", r);
            return -1;
        }
        if (m->stable) {
            return 0;
        }
        sleep_ms(POLL_MS);
    }
    return 1;
}

/*
 * Group consumer loop: join, heartbeat, fetch assigned partitions from the group's committed
 * offset, print "partition<TAB>offset<TAB>payload", flush, then commit through the fenced
 * GroupCommit. Exits after idle_exit_ms without new records (0 = run until SIGTERM) or after
 * max_records. SIGTERM/SIGINT leave the group cleanly. A kill -9 leaves it to session expiry.
 */
static int cmd_consume_group(client *c, const char *group, const char *topic, uint32_t session_ms,
                             uint64_t idle_exit_ms, uint64_t max_records)
{
    membership *m = calloc(1, sizeof *m);
    if (m == NULL) {
        return 1;
    }
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_term;
    (void)sigemptyset(&sa.sa_mask);
    (void)sigaction(SIGTERM, &sa, NULL);
    (void)sigaction(SIGINT, &sa, NULL);

    const uint64_t hb_every_ns = (uint64_t)session_ms * 1000000ULL / 3U;
    uint64_t delivered = 0;
    uint64_t last_hb = 0;
    uint64_t last_progress = now_ns();
    bool joined = false;
    int rc = 0;

    while (g_stop == 0 && rc == 0 && delivered < max_records) {
        if (!joined) {
            const int jr = join_until_stable(c, group, topic, session_ms, m);
            if (jr != 0) {
                rc = jr < 0 ? 1 : 0;
                break;
            }
            bool ok = true;
            for (size_t i = 0; i < m->n && ok; i++) {
                int64_t committed = -1;
                ok = get_committed(c, group, m->a[i].topic, m->a[i].partition, &committed) == 0;
                m->a[i].position = committed < 0 ? 0 : (uint64_t)committed;
            }
            if (!ok) {
                rc = 1;
                break;
            }
            (void)fprintf(stderr, "joined %s generation=%u partitions=%zu\n", m->id, m->generation,
                          m->n);
            joined = true;
            last_hb = now_ns();
        }

        if (now_ns() - last_hb >= hb_every_ns) {
            hx_buf body;
            hx_buf_init(&body);
            int r = -1;
            if (put_member(&body, group, m->id) == 0 && hx_buf_put_u32(&body, m->generation) == 0) {
                r = simple_call(c, HX_API_HEARTBEAT, &body);
            }
            hx_buf_free(&body);
            last_hb = now_ns();
            if (is_membership_error(r)) {
                joined = false;
                continue;
            }
            if (r != HXE_OK) {
                rc = 1;
                break;
            }
        }

        bool progressed = false;
        for (size_t i = 0; i < m->n && joined && delivered < max_records; i++) {
            assignment *as = &m->a[i];
            uint64_t next = as->position;
            const uint64_t want = max_records - delivered < GROUP_FETCH_PASS
                                      ? max_records - delivered
                                      : GROUP_FETCH_PASS;
            const int fr = cmd_fetch(c, as->topic, as->partition, as->position, want, &next, true);
            if (fr == FETCH_PARTITION_GONE) {
                joined = false; /* the topic changed under us; a rebalance is already running */
                sleep_ms(POLL_MS);
                break;
            }
            if (fr != 0) {
                rc = 1;
                break;
            }
            if (next == as->position) {
                continue;
            }
            /* Deliver first, commit second: a lost commit re-delivers, never skips. */
            if (fflush(stdout) != 0) {
                rc = 1;
                break;
            }
            const int r = group_commit(c, group, m, as, next);
            delivered += next - as->position;
            progressed = true;
            if (r == HXE_OK) {
                as->position = next;
            } else if (is_membership_error(r)) {
                joined = false; /* fenced: another member may now own this partition */
            } else {
                (void)fprintf(stderr, "error: commit failed (%d)\n", r);
                rc = 1;
            }
        }
        if (progressed) {
            last_progress = now_ns();
        } else if (joined) {
            if (idle_exit_ms != 0 && (now_ns() - last_progress) / 1000000ULL >= idle_exit_ms) {
                break;
            }
            sleep_ms(POLL_MS);
        }
    }

    if (m->id[0] != '\0') {
        hx_buf body;
        hx_buf_init(&body);
        if (put_member(&body, group, m->id) == 0) {
            (void)simple_call(c, HX_API_LEAVE_GROUP, &body);
        }
        hx_buf_free(&body);
    }
    (void)fflush(stdout);
    free(m);
    return rc;
}

/* ------------------------------------------- idempotent producer, topic admin */

#define IDEM_MAX_ATTEMPTS 5

static int cmd_init_producer(client *c)
{
    hx_buf resp;
    hx_buf_init(&resp);
    uint16_t err = 0;
    int rc = 1;
    if (roundtrip(c, HX_API_INIT_PRODUCER, NULL, &err, &resp) != 0) {
        (void)fputs("error: connection lost\n", stderr);
    } else if (err != HXE_OK) {
        rc = report_error(err);
    } else {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        const uint64_t pid = hx_rd_u64(&rd);
        if (rd.ok && hx_rd_left(&rd) == 0) {
            (void)printf("%llu\n", (unsigned long long)pid);
            rc = 0;
        } else {
            (void)fputs("error: malformed response\n", stderr);
        }
    }
    hx_buf_free(&resp);
    return rc;
}

static int build_idem_body(hx_buf *body, uint64_t pid, uint32_t seq, const char *topic,
                           uint32_t partition, const void *msg, size_t mlen)
{
    body->len = 0;
    if (mlen > UINT32_MAX || hx_buf_put_u64(body, pid) != 0 || hx_buf_put_u32(body, seq) != 0 ||
        put_topic_partition(body, topic, partition) != 0 ||
        hx_buf_put_u32(body, (uint32_t)mlen) != 0 || hx_buf_put(body, msg, mlen) != 0) {
        return -1;
    }
    return 0;
}

/* Single idempotent produce with explicit pid/sequence (for tests and manual retries). */
static int cmd_iproduce(client *c, uint64_t pid, uint32_t seq, const char *topic,
                        uint32_t partition, const char *msg)
{
    hx_buf body;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&resp);
    int rc = 1;
    uint16_t err = 0;
    if (build_idem_body(&body, pid, seq, topic, partition, msg, strlen(msg)) == 0) {
        if (roundtrip(c, HX_API_PRODUCE_IDEMPOTENT, &body, &err, &resp) != 0) {
            (void)fputs("error: connection lost\n", stderr);
        } else if (err != HXE_OK) {
            rc = report_error(err);
        } else {
            hx_rd rd;
            hx_rd_init(&rd, resp.data, resp.len);
            const uint64_t off = hx_rd_u64(&rd);
            if (rd.ok) {
                (void)printf("ACK %llu\n", (unsigned long long)off);
                rc = 0;
            }
        }
    }
    hx_buf_free(&body);
    hx_buf_free(&resp);
    return rc;
}

static int redial(client *c)
{
    (void)close(c->fd);
    c->fd = dial(c->host, c->port);
    return c->fd < 0 ? -1 : 0;
}

/*
 * Exactly-once-per-record producer: obtains a producer id, numbers every record, and when the
 * connection breaks it reconnects and resends the SAME sequence, so a record whose acknowledgement
 * was lost is not written twice. HELIXCTL_CHAOS_DROP_EVERY=n makes the client drop the connection
 * right after sending every n-th record (before reading the answer) to exercise exactly that path.
 */
static int cmd_produce_idem(client *c, const char *topic, uint32_t partition, const char *msg)
{
    const char *chaos_env = getenv("HELIXCTL_CHAOS_DROP_EVERY");
    uint64_t chaos = 0;
    if (chaos_env != NULL && parse_uint(chaos_env, 1000000ULL, &chaos) != 0) {
        chaos = 0;
    }
    hx_buf resp;
    hx_buf body;
    hx_buf_init(&resp);
    hx_buf_init(&body);
    uint16_t err = 0;
    uint64_t pid = 0;
    int rc = 1;
    if (roundtrip(c, HX_API_INIT_PRODUCER, NULL, &err, &resp) != 0 || err != HXE_OK) {
        (void)fputs("error: cannot obtain a producer id\n", stderr);
        goto done;
    }
    {
        hx_rd rd;
        hx_rd_init(&rd, resp.data, resp.len);
        pid = hx_rd_u64(&rd);
        if (!rd.ok) {
            goto done;
        }
    }

    char *line = NULL;
    size_t cap = 0;
    uint32_t seq = 0;
    rc = 0;
    while (rc == 0) {
        const char *payload = msg;
        size_t plen = msg != NULL ? strlen(msg) : 0;
        if (msg == NULL) {
            const ssize_t n = getline(&line, &cap, stdin);
            if (n < 0) {
                break;
            }
            plen = (size_t)n;
            if (plen > 0 && line[plen - 1] == '\n') {
                plen--;
            }
            payload = line;
        } else if (seq > 0) {
            break; /* a command-line message is sent once */
        }
        bool acked = false;
        for (int attempt = 0; attempt < IDEM_MAX_ATTEMPTS && !acked && rc == 0; attempt++) {
            if (build_idem_body(&body, pid, seq, topic, partition, payload, plen) != 0) {
                rc = 1;
                break;
            }
            hx_buf req;
            hx_buf_init(&req);
            uint32_t corr = c->next_corr++;
            const bool sent = build_request(&req, HX_API_PRODUCE_IDEMPOTENT, corr, &body) == 0 &&
                              send_all(c->fd, req.data, req.len) == 0;
            hx_buf_free(&req);
            const bool drop = chaos != 0 && attempt == 0 && (seq + 1U) % chaos == 0;
            uint32_t got = 0;
            if (!sent || drop || read_response(c, &got, &err, &resp) != 0 || got != corr) {
                (void)fprintf(stderr, "retry seq=%u attempt=%d\n", seq, attempt + 2);
                if (redial(c) != 0) {
                    (void)fputs("error: cannot reconnect\n", stderr);
                    rc = 1;
                }
                continue; /* same pid, same sequence */
            }
            if (err != HXE_OK) {
                rc = report_error(err);
                break;
            }
            hx_rd rd;
            hx_rd_init(&rd, resp.data, resp.len);
            const uint64_t off = hx_rd_u64(&rd);
            if (!rd.ok) {
                rc = 1;
                break;
            }
            (void)printf("ACK %llu\n", (unsigned long long)off);
            (void)fflush(stdout);
            acked = true;
        }
        if (!acked && rc == 0) {
            (void)fputs("error: gave up after repeated connection failures\n", stderr);
            rc = 1;
        }
        seq++;
    }
    free(line);
done:
    hx_buf_free(&resp);
    hx_buf_free(&body);
    return rc;
}

static int cmd_create_topic(client *c, const char *topic, uint32_t count)
{
    hx_buf body;
    hx_buf_init(&body);
    int rc = 1;
    const size_t tl = strlen(topic);
    if (tl <= UINT16_MAX && hx_buf_put_u16(&body, (uint16_t)tl) == 0 &&
        hx_buf_put(&body, topic, tl) == 0 && hx_buf_put_u32(&body, count) == 0) {
        rc = finish_simple(simple_call(c, HX_API_CREATE_TOPIC, &body));
    }
    hx_buf_free(&body);
    return rc;
}

static int cmd_delete_topic(client *c, const char *topic)
{
    hx_buf body;
    hx_buf_init(&body);
    int rc = 1;
    const size_t tl = strlen(topic);
    if (tl <= UINT16_MAX && hx_buf_put_u16(&body, (uint16_t)tl) == 0 &&
        hx_buf_put(&body, topic, tl) == 0) {
        rc = finish_simple(simple_call(c, HX_API_DELETE_TOPIC, &body));
    }
    hx_buf_free(&body);
    return rc;
}

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

/* Pipelined produce benchmark with per-request latency percentiles. */
static int cmd_bench(client *c, const char *topic, uint32_t partition, uint64_t count,
                     uint32_t size, uint32_t depth)
{
    if (count == 0) {
        return 1;
    }
    uint8_t *msg = calloc(1, size != 0 ? size : 1);
    uint32_t *lat = malloc((size_t)count * sizeof *lat);
    uint64_t *sent_at = calloc(depth, sizeof *sent_at);
    if (msg == NULL || lat == NULL || sent_at == NULL) {
        free(msg);
        free(lat);
        free(sent_at);
        return 1;
    }
    hx_buf body;
    hx_buf req;
    hx_buf resp;
    hx_buf_init(&body);
    hx_buf_init(&req);
    hx_buf_init(&resp);

    int rc = 0;
    uint64_t sent = 0;
    uint64_t done = 0;
    const uint64_t t0 = now_ns();
    while (done < count && rc == 0) {
        while (sent < count && sent - done < depth) {
            uint32_t corr = (uint32_t)sent;
            if (build_produce_body(&body, topic, partition, msg, size) != 0 ||
                build_request(&req, HX_API_PRODUCE, corr, &body) != 0 ||
                send_all(c->fd, req.data, req.len) != 0) {
                rc = 1;
                break;
            }
            sent_at[sent % depth] = now_ns();
            sent++;
        }
        if (rc != 0) {
            break;
        }
        uint32_t corr = 0;
        uint16_t err = 0;
        if (read_response(c, &corr, &err, &resp) != 0 || corr != (uint32_t)done) {
            (void)fputs("error: bad or missing response\n", stderr);
            rc = 1;
            break;
        }
        if (err != HXE_OK) {
            rc = report_error(err);
            break;
        }
        uint64_t us = (now_ns() - sent_at[done % depth]) / 1000U;
        lat[done] = us > UINT32_MAX ? UINT32_MAX : (uint32_t)us;
        done++;
    }
    const double secs = (double)(now_ns() - t0) / 1e9;
    if (rc == 0 && done > 0) {
        qsort(lat, (size_t)done, sizeof *lat, cmp_u32);
        (void)printf("bench: %llu records x %u B, depth %u: %.0f msg/s, %.1f MB/s\n",
                     (unsigned long long)done, size, depth, (double)done / secs,
                     (double)done * size / secs / 1e6);
        (void)printf("latency us: p50=%u p99=%u p99.9=%u max=%u\n", lat[done / 2],
                     lat[(done * 99) / 100], lat[(done * 999) / 1000], lat[done - 1]);
    }
    hx_buf_free(&body);
    hx_buf_free(&req);
    hx_buf_free(&resp);
    free(msg);
    free(lat);
    free(sent_at);
    return rc;
}

/* ----------------------------------------------------------------- main */

static void usage(void)
{
    (void)fputs("usage: helixctl [-H host] [-P port | -F port-file] <command>\n"
                "  ping\n"
                "  produce <topic> <partition> [message]   (stdin lines if no message)\n"
                "  fetch   <topic> <partition> <offset> [max_records]\n"
                "  bench   <topic> <partition> <count> <size> [depth]\n"
                "  metadata\n"
                "  cluster-metadata\n"
                "  commit    <group> <topic> <partition> <offset>\n"
                "  committed <group> <topic> <partition>\n"
                "  consume   <group> <topic> <partition> [max_records]\n"
                "  join      <group> <topic> <session_ms> [member]\n"
                "  heartbeat <group> <member> <generation>\n"
                "  leave     <group> <member>\n"
                "  describe  <group>\n"
                "  gcommit   <group> <member> <generation> <topic> <partition> <offset>\n"
                "  consume-group <group> <topic> <session_ms> <idle_exit_ms> [max_records]\n"
                "  init-producer\n"
                "  iproduce  <pid> <seq> <topic> <partition> <message>\n"
                "  produce-idem <topic> <partition> [message]   (stdin lines if no message)\n"
                "  create-topic <topic> <partitions>\n"
                "  delete-topic <topic>\n",
                stderr);
}

static int read_port_file(const char *path, char *out, size_t cap)
{
    FILE *f = fopen(path, "r");
    if (f == NULL) {
        return -1;
    }
    int ok = fgets(out, (int)cap, f) != NULL;
    (void)fclose(f);
    if (!ok) {
        return -1;
    }
    out[strcspn(out, "\r\n")] = '\0';
    return 0;
}

int main(int argc, char **argv)
{
    const char *host = "127.0.0.1";
    char port[16] = "9400";
    int opt = 0;
    while ((opt = getopt(argc, argv, "+H:P:F:h")) != -1) {
        switch (opt) {
        case 'H':
            host = optarg;
            break;
        case 'P':
            if (snprintf(port, sizeof port, "%s", optarg) >= (int)sizeof port) {
                usage();
                return 2;
            }
            break;
        case 'F':
            if (read_port_file(optarg, port, sizeof port) != 0) {
                (void)fprintf(stderr, "error: cannot read port file %s\n", optarg);
                return 2;
            }
            break;
        default:
            usage();
            return opt == 'h' ? 0 : 2;
        }
    }
    if (optind >= argc) {
        usage();
        return 2;
    }
    const char *cmd = argv[optind];
    char **args = argv + optind + 1;
    int nargs = argc - optind - 1;

    uint64_t part = 0;
    uint64_t a = 0;
    uint64_t b = 0;
    uint64_t d = 32;
    bool ok = false;
    if (strcmp(cmd, "ping") == 0 || strcmp(cmd, "metadata") == 0 ||
        strcmp(cmd, "cluster-metadata") == 0 || strcmp(cmd, "init-producer") == 0) {
        ok = nargs == 0;
    } else if (strcmp(cmd, "produce") == 0 || strcmp(cmd, "produce-idem") == 0) {
        ok = (nargs == 2 || nargs == 3) && parse_uint(args[1], HX_MAX_PARTITION, &part) == 0;
    } else if (strcmp(cmd, "fetch") == 0) {
        a = UINT64_MAX;
        ok = (nargs == 3 || nargs == 4) && parse_uint(args[1], HX_MAX_PARTITION, &part) == 0 &&
             parse_uint(args[2], UINT64_MAX, &b) == 0 &&
             (nargs == 3 || parse_uint(args[3], UINT64_MAX, &a) == 0);
    } else if (strcmp(cmd, "commit") == 0) {
        ok = nargs == 4 && parse_uint(args[2], HX_MAX_PARTITION, &part) == 0 &&
             parse_uint(args[3], UINT64_MAX, &a) == 0;
    } else if (strcmp(cmd, "committed") == 0) {
        ok = nargs == 3 && parse_uint(args[2], HX_MAX_PARTITION, &part) == 0;
    } else if (strcmp(cmd, "consume") == 0) {
        a = UINT64_MAX;
        ok = (nargs == 3 || nargs == 4) && parse_uint(args[2], HX_MAX_PARTITION, &part) == 0 &&
             (nargs == 3 || parse_uint(args[3], UINT64_MAX, &a) == 0);
    } else if (strcmp(cmd, "join") == 0) {
        ok = (nargs == 3 || nargs == 4) && parse_uint(args[2], HX_MAX_SESSION_MS, &a) == 0;
    } else if (strcmp(cmd, "heartbeat") == 0) {
        ok = nargs == 3 && parse_uint(args[2], UINT32_MAX, &a) == 0;
    } else if (strcmp(cmd, "leave") == 0) {
        ok = nargs == 2;
    } else if (strcmp(cmd, "describe") == 0 || strcmp(cmd, "delete-topic") == 0) {
        ok = nargs == 1;
    } else if (strcmp(cmd, "gcommit") == 0) {
        ok = nargs == 6 && parse_uint(args[2], UINT32_MAX, &a) == 0 &&
             parse_uint(args[4], HX_MAX_PARTITION, &part) == 0 &&
             parse_uint(args[5], UINT64_MAX, &b) == 0;
    } else if (strcmp(cmd, "consume-group") == 0) {
        d = UINT64_MAX;
        ok = (nargs == 4 || nargs == 5) && parse_uint(args[2], HX_MAX_SESSION_MS, &a) == 0 &&
             parse_uint(args[3], 86400000ULL, &b) == 0 &&
             (nargs == 4 || parse_uint(args[4], UINT64_MAX, &d) == 0);
    } else if (strcmp(cmd, "iproduce") == 0) {
        ok = nargs == 5 && parse_uint(args[0], UINT64_MAX, &b) == 0 &&
             parse_uint(args[1], UINT32_MAX, &a) == 0 &&
             parse_uint(args[3], HX_MAX_PARTITION, &part) == 0;
    } else if (strcmp(cmd, "create-topic") == 0) {
        ok = nargs == 2 && parse_uint(args[1], HX_MAX_PARTITION + 1U, &a) == 0 && a > 0;
    } else if (strcmp(cmd, "bench") == 0) {
        ok = (nargs == 4 || nargs == 5) && parse_uint(args[1], HX_MAX_PARTITION, &part) == 0 &&
             parse_uint(args[2], 100000000ULL, &a) == 0 && a > 0 &&
             parse_uint(args[3], BENCH_MAX_SIZE, &b) == 0 &&
             (nargs == 4 || parse_uint(args[4], BENCH_MAX_DEPTH, &d) == 0) && d > 0;
    }
    if (!ok) {
        usage();
        return 2;
    }

    client c = {.fd = dial(host, port), .next_corr = 1, .host = host, .port = port};
    if (c.fd < 0) {
        (void)fprintf(stderr, "error: cannot connect to %s:%s\n", host, port);
        return 1;
    }

    int rc = 2;
    if (strcmp(cmd, "ping") == 0) {
        rc = cmd_ping(&c);
    } else if (strcmp(cmd, "produce") == 0) {
        rc = cmd_produce(&c, args[0], (uint32_t)part, nargs == 3 ? args[2] : NULL);
    } else if (strcmp(cmd, "fetch") == 0) {
        rc = cmd_fetch(&c, args[0], (uint32_t)part, b, a, NULL, false);
    } else if (strcmp(cmd, "metadata") == 0) {
        rc = cmd_metadata(&c);
    } else if (strcmp(cmd, "cluster-metadata") == 0) {
        rc = cmd_cluster_metadata(&c);
    } else if (strcmp(cmd, "commit") == 0) {
        rc = cmd_commit(&c, args[0], args[1], (uint32_t)part, a);
    } else if (strcmp(cmd, "committed") == 0) {
        rc = cmd_committed(&c, args[0], args[1], (uint32_t)part);
    } else if (strcmp(cmd, "consume") == 0) {
        rc = cmd_consume(&c, args[0], args[1], (uint32_t)part, a);
    } else if (strcmp(cmd, "join") == 0) {
        rc = cmd_join(&c, args[0], args[1], (uint32_t)a, nargs == 4 ? args[3] : NULL);
    } else if (strcmp(cmd, "heartbeat") == 0) {
        rc = cmd_heartbeat(&c, args[0], args[1], (uint32_t)a);
    } else if (strcmp(cmd, "leave") == 0) {
        rc = cmd_leave(&c, args[0], args[1]);
    } else if (strcmp(cmd, "describe") == 0) {
        rc = cmd_describe(&c, args[0]);
    } else if (strcmp(cmd, "gcommit") == 0) {
        rc = cmd_gcommit(&c, args[0], args[1], (uint32_t)a, args[3], (uint32_t)part, b);
    } else if (strcmp(cmd, "consume-group") == 0) {
        rc = cmd_consume_group(&c, args[0], args[1], (uint32_t)a, b, d);
    } else if (strcmp(cmd, "init-producer") == 0) {
        rc = cmd_init_producer(&c);
    } else if (strcmp(cmd, "iproduce") == 0) {
        rc = cmd_iproduce(&c, b, (uint32_t)a, args[2], (uint32_t)part, args[4]);
    } else if (strcmp(cmd, "produce-idem") == 0) {
        rc = cmd_produce_idem(&c, args[0], (uint32_t)part, nargs == 3 ? args[2] : NULL);
    } else if (strcmp(cmd, "create-topic") == 0) {
        rc = cmd_create_topic(&c, args[0], (uint32_t)a);
    } else if (strcmp(cmd, "delete-topic") == 0) {
        rc = cmd_delete_topic(&c, args[0]);
    } else if (strcmp(cmd, "bench") == 0) {
        rc = cmd_bench(&c, args[0], (uint32_t)part, a, (uint32_t)b, (uint32_t)d);
    }
    (void)close(c.fd);
    return rc;
}
