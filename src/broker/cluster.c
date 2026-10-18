#include "broker/cluster.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "ctrl/ctrl_net.h"
#include "proto/codec.h"
#include "raft/raft_net.h"
#include "raft/raft_wal.h"
#include "repl/replica_mgr.h"
#include "util/logger.h"

#define FETCH_ENTRY_OVERHEAD 20U
#define NO_NODE              UINT32_MAX
#define MAX_PARKED           65536U
#define SCRATCH_ENTRIES      256U
#define CREATE_WINDOW        16U
#define CREATE_EVERY_MS      100
#define HEALTH_EVERY_MS      100

/* ================================================================ cluster file */

static void set_err(char *err, size_t cap, int line, const char *what)
{
    if (err != NULL && cap != 0) {
        (void)snprintf(err, cap, "line %d: %s", line, what);
    }
}

static int parse_port(const char *s, uint16_t *out)
{
    char *end = NULL;
    errno = 0;
    const unsigned long v = strtoul(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v == 0 || v > 65535UL) {
        return -1;
    }
    *out = (uint16_t)v;
    return 0;
}

static int parse_uint32(const char *s, uint32_t max, uint32_t *out)
{
    char *end = NULL;
    errno = 0;
    const unsigned long v = strtoul(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v > max) {
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

static int topic_name_ok(const char *s, size_t n)
{
    if (n == 0 || n > HX_MAX_TOPIC_LEN || (n >= 2 && s[0] == '_' && s[1] == '_')) {
        return 0;
    }
    for (size_t i = 0; i < n; i++) {
        const char ch = s[i];
        const int ok = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                       (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-';
        if (!ok) {
            return 0;
        }
    }
    return 1;
}

int hx_cl_spec_parse(const char *text, hx_cl_spec *spec, char *err, size_t errcap)
{
    memset(spec, 0, sizeof *spec);
    char *copy = strdup(text);
    if (copy == NULL) {
        set_err(err, errcap, 0, "out of memory");
        return -1;
    }
    int line = 0;
    int rc = -1;
    char *save_line = NULL;
    for (char *ln = strtok_r(copy, "\n", &save_line); ln != NULL;
         ln = strtok_r(NULL, "\n", &save_line)) {
        line++;
        char *hash = strchr(ln, '#');
        if (hash != NULL) {
            *hash = '\0';
        }
        char *tok[8];
        size_t nt = 0;
        char *save_tok = NULL;
        for (char *t = strtok_r(ln, " \t\r", &save_tok); t != NULL;
             t = strtok_r(NULL, " \t\r", &save_tok)) {
            if (nt == 8) {
                set_err(err, errcap, line, "too many fields");
                goto out;
            }
            tok[nt++] = t;
        }
        if (nt == 0) {
            continue;
        }
        if (strcmp(tok[0], "node") == 0) {
            uint32_t id = 0;
            struct in_addr ia;
            hx_cl_node *n = &spec->nodes[spec->nnodes < HX_CL_MAX_NODES ? spec->nnodes : 0];
            if (nt != 7 || spec->nnodes >= HX_CL_MAX_NODES ||
                parse_uint32(tok[1], HX_CL_MAX_NODES, &id) != 0 || id != spec->nnodes ||
                strlen(tok[2]) >= sizeof n->host || inet_pton(AF_INET, tok[2], &ia) != 1 ||
                parse_port(tok[3], &n->client_port) != 0 ||
                parse_port(tok[4], &n->raft_port) != 0 || parse_port(tok[5], &n->ctl_port) != 0 ||
                parse_port(tok[6], &n->repl_port) != 0) {
                set_err(err, errcap, line,
                        "node <id> <ipv4> <client> <raft> <ctl> <repl>, ids 0..n-1 in order");
                goto out;
            }
            (void)snprintf(n->host, sizeof n->host, "%s", tok[2]);
            spec->nnodes++;
        } else if (strcmp(tok[0], "topic") == 0) {
            uint32_t np = 0;
            if (nt != 3 || spec->ntopics >= HX_CL_MAX_TOPICS ||
                !topic_name_ok(tok[1], strlen(tok[1])) ||
                parse_uint32(tok[2], HX_CL_MAX_PARTS, &np) != 0 || np == 0 ||
                spec->nparts + np > HX_CL_MAX_PARTS) {
                set_err(
                    err, errcap, line,
                    "topic <name> <partitions>, valid name, 1.. partitions, at most 1024 in total");
                goto out;
            }
            for (uint32_t i = 0; i < spec->ntopics; i++) {
                if (strcmp(spec->topics[i].name, tok[1]) == 0) {
                    set_err(err, errcap, line, "duplicate topic");
                    goto out;
                }
            }
            hx_cl_topic *t = &spec->topics[spec->ntopics++];
            (void)snprintf(t->name, sizeof t->name, "%s", tok[1]);
            t->nlen = strlen(t->name);
            t->nparts = np;
            t->base = spec->nparts;
            spec->nparts += np;
        } else {
            set_err(err, errcap, line, "unknown directive");
            goto out;
        }
    }
    if (spec->nnodes == 0) {
        set_err(err, errcap, line, "no node declared");
        goto out;
    }
    rc = 0;
out:
    free(copy);
    return rc;
}

int hx_cl_spec_load(const char *path, hx_cl_spec *spec, char *err, size_t errcap)
{
    FILE *f = fopen(path, "r");
    if (f == NULL) {
        set_err(err, errcap, 0, "cannot open cluster file");
        return -1;
    }
    char *text = malloc(65536);
    if (text == NULL) {
        (void)fclose(f);
        return -1;
    }
    const size_t n = fread(text, 1, 65535, f);
    const int bad = ferror(f) || !feof(f);
    (void)fclose(f);
    text[n] = '\0';
    int rc = -1;
    if (bad) {
        set_err(err, errcap, 0, "cluster file unreadable or larger than 64 KiB");
    } else {
        rc = hx_cl_spec_parse(text, spec, err, errcap);
    }
    free(text);
    return rc;
}

/* ===================================================================== cluster */

typedef struct {
    uint64_t conn;
    uint32_t corr;
    uint32_t part;
    uint32_t epoch;
    uint64_t target; /* committed once hw >= target */
    uint64_t first;
    int64_t deadline;
} parked;

struct hx_cluster {
    hx_cluster_cfg cfg;
    const hx_cl_spec *spec;
    uint32_t self;
    int64_t now;
    int64_t next_create;
    int64_t next_health;
    bool failed;

    hx_raft_wal *wal;
    hx_chost *host;
    hx_raftnet *rnet;
    hx_ctlsrv *csrv;
    hx_rmgr *rmgr;
    hx_raft_entry *scratch;
    uint8_t *rec_buf;
    uint32_t max_record;

    parked *pk;
    size_t npk;
    size_t cappk;
};

static void raft_send(void *ctx, const hx_raft_msg *m)
{
    hx_cluster *c = ctx;
    hx_buf b;
    hx_buf_init(&b);
    if (hx_raft_msg_encode(m, &b) == 0) {
        (void)hx_raftnet_send(c->rnet, m->to, b.data, b.len, c->now);
    }
    hx_buf_free(&b);
}

static void raft_frame(void *ctx, uint32_t from, const uint8_t *p, size_t len)
{
    hx_cluster *c = ctx;
    hx_raft_msg m;
    if (hx_raft_msg_decode(p, len, c->scratch, SCRATCH_ENTRIES, &m) == 0 && m.from == from &&
        m.to == c->self) {
        (void)hx_chost_step(c->host, c->now, &m);
    }
}

static int64_t mono_ms(void)
{
    struct timespec ts;
    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* A fresh value per process life: the controller refuses sessions created for an older life. */
static uint64_t make_nonce(void)
{
    uint64_t v = 0;
    const int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        if (read(fd, &v, sizeof v) != (ssize_t)sizeof v) {
            v = 0;
        }
        (void)close(fd);
    }
    if (v == 0) {
        v = ((uint64_t)mono_ms() << 20) ^ (uint64_t)getpid() ^ 0x9E3779B97F4A7C15ULL;
    }
    return v != 0 ? v : 1;
}

static int mkdir_p1(const char *path)
{
    return mkdir(path, 0755) == 0 || errno == EEXIST ? 0 : -1;
}

hx_cluster *hx_cluster_create(const hx_cluster_cfg *cfg)
{
    const hx_cl_spec *sp = cfg->spec;
    if (sp == NULL || cfg->self >= sp->nnodes || sp->nparts == 0 ||
        cfg->log_opts.max_record_bytes == 0 || cfg->log_opts.max_record_bytes > (1U << 20)) {
        HX_ERROR("cluster: max-record-bytes must be between 1 and 1048576");
        return NULL;
    }
    hx_cluster *c = calloc(1, sizeof *c);
    if (c == NULL) {
        return NULL;
    }
    c->cfg = *cfg;
    c->spec = sp;
    c->self = cfg->self;
    c->now = mono_ms();
    c->next_create = c->now;
    c->max_record = cfg->log_opts.max_record_bytes;
    c->scratch = calloc(SCRATCH_ENTRIES, sizeof *c->scratch);
    c->rec_buf = malloc(c->max_record);
    if (c->scratch == NULL || c->rec_buf == NULL) {
        hx_cluster_destroy(c);
        return NULL;
    }

    char dir[1024];
    char wal[1100];
    char parts[1100];
    (void)snprintf(dir, sizeof dir, "%s/cluster", cfg->data_dir);
    (void)snprintf(wal, sizeof wal, "%s/raft.wal", dir);
    (void)snprintf(parts, sizeof parts, "%s/partitions", dir);
    if (mkdir_p1(cfg->data_dir) != 0 || mkdir_p1(dir) != 0 || mkdir_p1(parts) != 0) {
        HX_ERROR("cluster: cannot create %s (errno=%d)", dir, errno);
        hx_cluster_destroy(c);
        return NULL;
    }
    if (hx_raft_wal_open(wal, &c->wal) != HX_WAL_OK) {
        HX_ERROR("cluster: cannot open the controller log %s", wal);
        hx_cluster_destroy(c);
        return NULL;
    }

    const uint32_t n = sp->nnodes;
    hx_chost_cfg hc;
    hx_chost_cfg_default(&hc, c->self, n);
    hc.raft.seed = (uint32_t)(make_nonce() & 0x7fffffffU);
    hc.raft.storage = hx_raft_wal_storage(c->wal);
    hc.send_ctx = c;
    hc.send = raft_send;
    hc.down_ms = 3000;
    hc.retry_ms = 500;
    hc.snapshot_every = 1000;
    const hx_raft_restore rs = hx_raft_wal_restore(c->wal);
    c->host = hx_chost_create(&hc, &rs, c->now);
    const hx_raftnet_cfg rc = {.self = c->self,
                               .nnodes = n,
                               .bind_addr = sp->nodes[c->self].host,
                               .port = sp->nodes[c->self].raft_port,
                               .on_frame = raft_frame,
                               .ctx = c};
    c->rnet = c->host != NULL ? hx_raftnet_create(&rc) : NULL;
    const hx_ctlsrv_cfg sc = {
        .bind_addr = sp->nodes[c->self].host, .port = sp->nodes[c->self].ctl_port, .push_ms = 500};
    c->csrv = c->rnet != NULL ? hx_ctlsrv_create(&sc, c->host) : NULL;
    if (c->csrv == NULL) {
        HX_ERROR("cluster: cannot start the controller (ports in use?)");
        hx_cluster_destroy(c);
        return NULL;
    }

    hx_rmgr_cfg mc;
    memset(&mc, 0, sizeof mc);
    mc.self = c->self;
    mc.nbrokers = n;
    mc.data_dir = parts;
    mc.bind_addr = sp->nodes[c->self].host;
    mc.port = sp->nodes[c->self].repl_port;
    mc.nonce = make_nonce();
    mc.ncontrollers = n;
    for (uint32_t i = 0; i < n; i++) {
        mc.ctrl_addrs[i] = sp->nodes[i].host;
        mc.ctrl_ports[i] = sp->nodes[i].ctl_port;
    }
    mc.min_isr = n / 2 + 1;
    mc.lag_ms = 3000;
    mc.fetch_ms = 5;
    mc.log_opts = cfg->log_opts;
    /* The public limit is payload bytes; reserve the on-disk epoch separately. */
    mc.log_opts.max_record_bytes += 4U;
    mc.use_log_opts = true;
    c->rmgr = hx_rmgr_create(&mc);
    if (c->rmgr == NULL) {
        HX_ERROR("cluster: cannot start the replica manager");
        hx_cluster_destroy(c);
        return NULL;
    }
    const uint32_t all = n >= 32 ? UINT32_MAX : (UINT32_C(1) << n) - 1U;
    for (uint32_t p = 0; p < sp->nparts; p++) {
        if (hx_rmgr_add_partition(c->rmgr, p, all) != 0) {
            HX_ERROR("cluster: cannot open the log of partition %u", p);
            hx_cluster_destroy(c);
            return NULL;
        }
    }
    for (uint32_t i = 0; i < n; i++) {
        if (i != c->self) {
            (void)hx_raftnet_set_peer(c->rnet, i, sp->nodes[i].host, sp->nodes[i].raft_port);
            (void)hx_rmgr_set_peer(c->rmgr, i, sp->nodes[i].host, sp->nodes[i].repl_port);
        }
    }
    HX_INFO("cluster: node %u of %u, %u replicated partitions in %u topics", c->self, n, sp->nparts,
            sp->ntopics);
    return c;
}

void hx_cluster_destroy(hx_cluster *c)
{
    if (c == NULL) {
        return;
    }
    hx_rmgr_destroy(c->rmgr);
    hx_ctlsrv_destroy(c->csrv);
    hx_raftnet_destroy(c->rnet);
    hx_chost_destroy(c->host);
    if (c->wal != NULL) {
        hx_raft_wal_close(c->wal);
    }
    free(c->pk);
    free(c->scratch);
    free(c->rec_buf);
    free(c);
}

uint16_t hx_cluster_client_port(const hx_cluster *c)
{
    return c->spec->nodes[c->self].client_port;
}

/* Part 0..nparts-1 of the controller are created in order by whichever node is the ready leader. */
static void bootstrap_partitions(hx_cluster *c)
{
    if (c->now < c->next_create || !hx_chost_ready(c->host)) {
        return;
    }
    c->next_create = c->now + CREATE_EVERY_MS;
    const uint32_t have = (uint32_t)hx_ctrl_nparts(hx_chost_ctrl(c->host));
    const uint32_t n = c->spec->nnodes;
    const uint32_t all = n >= 32 ? UINT32_MAX : (UINT32_C(1) << n) - 1U;
    for (uint32_t p = have; p < c->spec->nparts && p < have + CREATE_WINDOW; p++) {
        (void)hx_chost_create_partition(c->host, c->now, p, all);
    }
}

static bool healthy(hx_cluster *c)
{
    if (hx_chost_failed(c->host)) {
        HX_ERROR("cluster: the controller host failed");
        return false;
    }
    for (uint32_t p = 0; p < c->spec->nparts; p++) {
        hx_rmgr_part_info in;
        if (hx_rmgr_info(c->rmgr, p, &in) == 0 && in.failed) {
            HX_ERROR("cluster: replica of partition %u failed (storage error or violated "
                     "invariant)",
                     p);
            return false;
        }
    }
    return true;
}

int hx_cluster_poll(hx_cluster *c, int64_t now_ms)
{
    if (c->failed) {
        return -1;
    }
    c->now = now_ms;
    (void)hx_raftnet_poll(c->rnet, 0, now_ms);
    (void)hx_chost_tick(c->host, now_ms);
    hx_ctlsrv_poll(c->csrv, 0, now_ms);
    hx_rmgr_poll(c->rmgr, 0, now_ms);
    bootstrap_partitions(c);
    if (now_ms >= c->next_health) {
        c->next_health = now_ms + HEALTH_EVERY_MS;
        if (!healthy(c)) {
            c->failed = true;
            return -1;
        }
    }
    return 0;
}

/* ================================================================== requests */

static size_t resp_begin(hx_buf *out, uint32_t corr, uint16_t err)
{
    const size_t start = out->len;
    (void)hx_buf_put_u32(out, 0);
    (void)hx_buf_put_u32(out, corr);
    (void)hx_buf_put_u16(out, err);
    return start;
}

static void resp_end(hx_buf *out, size_t start)
{
    hx_buf_patch_u32(out, start, (uint32_t)(out->len - start - 4U));
}

/* Error response; returns HX_CL_DONE, or FATAL if memory ran out (out is then restored). */
static hx_cl_verdict resp_err(hx_buf *out, uint32_t corr, uint16_t err)
{
    const size_t start = resp_begin(out, corr, err);
    resp_end(out, start);
    return HX_CL_DONE;
}

static const hx_cl_topic *find_topic(const hx_cluster *c, const uint8_t *name, size_t n)
{
    for (uint32_t i = 0; i < c->spec->ntopics; i++) {
        const hx_cl_topic *t = &c->spec->topics[i];
        if (t->nlen == n && memcmp(t->name, name, n) == 0) {
            return t;
        }
    }
    return NULL;
}

static hx_cl_verdict produce(hx_cluster *c, uint64_t conn, uint32_t corr, const hx_cl_topic *t,
                             uint32_t partition, const uint8_t *payload, uint32_t plen, hx_buf *out)
{
    if (partition >= t->nparts) {
        return resp_err(out, corr, HXE_UNKNOWN_PARTITION);
    }
    if (plen > c->max_record) {
        return resp_err(out, corr, HXE_RECORD_TOO_LARGE);
    }
    if (c->npk >= MAX_PARKED) {
        return resp_err(out, corr, HXE_RESOURCE_LIMIT);
    }
    const uint32_t part = t->base + partition;
    const hx_repl_rec rec = {.epoch = 0, .len = plen, .data = payload};
    uint64_t first = 0;
    const hx_repl_status st = hx_rmgr_append(c->rmgr, part, c->now, &rec, 1, &first);
    if (st == HX_REPL_NOT_LEADER) {
        return resp_err(out, corr, HXE_NOT_LEADER);
    }
    if (st == HX_REPL_NOT_ENOUGH_REPLICAS) {
        return resp_err(out, corr, HXE_NOT_ENOUGH_REPLICAS);
    }
    if (st != HX_REPL_OK) {
        c->failed = true; /* FAILED or BAD_ARG: this replica can no longer be trusted */
        return HX_CL_FATAL;
    }
    hx_rmgr_part_info in;
    if (hx_rmgr_info(c->rmgr, part, &in) != 0) {
        c->failed = true;
        return HX_CL_FATAL;
    }
    if (c->npk == c->cappk) {
        const size_t ncap = c->cappk == 0 ? 64U : c->cappk * 2U;
        parked *np = realloc(c->pk, ncap * sizeof *np);
        if (np == NULL) {
            c->failed = true; /* the record is appended but cannot be tracked: stop */
            return HX_CL_FATAL;
        }
        c->pk = np;
        c->cappk = ncap;
    }
    c->pk[c->npk++] = (parked){.conn = conn,
                               .corr = corr,
                               .part = part,
                               .epoch = in.epoch,
                               .target = first + 1U,
                               .first = first,
                               .deadline = c->now + (int64_t)c->cfg.ack_timeout_ms};
    return HX_CL_PARK;
}

static hx_cl_verdict fetch(hx_cluster *c, uint32_t corr, const hx_cl_topic *t, uint32_t partition,
                           uint64_t offset, uint32_t max_bytes, hx_buf *out)
{
    if (partition >= t->nparts) {
        return resp_err(out, corr, HXE_UNKNOWN_PARTITION);
    }
    if (max_bytes > c->cfg.max_fetch_bytes) {
        max_bytes = c->cfg.max_fetch_bytes;
    }
    const uint32_t part = t->base + partition;
    hx_rmgr_part_info in;
    if (hx_rmgr_info(c->rmgr, part, &in) != 0 || in.role != HX_REPL_LEADER) {
        return resp_err(out, corr, HXE_NOT_LEADER);
    }
    if (offset > in.hw) {
        return resp_err(out, corr, HXE_OFFSET_OUT_OF_RANGE);
    }
    const size_t start = resp_begin(out, corr, HXE_OK);
    const size_t count_pos = out->len;
    (void)hx_buf_put_u32(out, 0);
    uint32_t count = 0;
    uint64_t total = 0;
    while (offset < in.hw) {
        uint32_t len = 0;
        if (hx_rmgr_read(c->rmgr, part, offset, c->rec_buf, c->max_record, &len) != HX_OK) {
            out->len = start;
            return resp_err(out, corr, HXE_STORAGE);
        }
        const uint64_t entry = (uint64_t)len + FETCH_ENTRY_OVERHEAD;
        if (count > 0 && total + entry > max_bytes) {
            break;
        }
        /* Replicated records carry no timestamp (the replica log stores epoch + payload only). */
        if (hx_buf_put_u64(out, offset) != 0 || hx_buf_put_u64(out, 0) != 0 ||
            hx_buf_put_u32(out, len) != 0 || hx_buf_put(out, c->rec_buf, len) != 0) {
            out->len = start;
            return HX_CL_FATAL;
        }
        count++;
        total += entry;
        offset++;
        if (total >= max_bytes) {
            break;
        }
    }
    hx_buf_patch_u32(out, count_pos, count);
    resp_end(out, start);
    return HX_CL_DONE;
}

static hx_cl_verdict cluster_metadata(hx_cluster *c, uint32_t corr, hx_buf *out)
{
    const hx_cl_spec *sp = c->spec;
    const size_t start = resp_begin(out, corr, HXE_OK);
    bool bad = hx_buf_put_u32(out, c->self) != 0 || hx_buf_put_u32(out, sp->nnodes) != 0;
    for (uint32_t i = 0; i < sp->nnodes; i++) {
        const size_t hl = strlen(sp->nodes[i].host);
        bad = bad || hx_buf_put_u32(out, i) != 0 || hx_buf_put_u16(out, (uint16_t)hl) != 0 ||
              hx_buf_put(out, sp->nodes[i].host, hl) != 0 ||
              hx_buf_put_u16(out, sp->nodes[i].client_port) != 0;
    }
    bad = bad || hx_buf_put_u32(out, sp->nparts) != 0;
    for (uint32_t ti = 0; ti < sp->ntopics; ti++) {
        const hx_cl_topic *t = &sp->topics[ti];
        for (uint32_t p = 0; p < t->nparts; p++) {
            hx_rmgr_part_info in;
            memset(&in, 0, sizeof in);
            (void)hx_rmgr_info(c->rmgr, t->base + p, &in);
            bad = bad || hx_buf_put_u16(out, (uint16_t)t->nlen) != 0 ||
                  hx_buf_put(out, t->name, t->nlen) != 0 || hx_buf_put_u32(out, p) != 0 ||
                  hx_buf_put_u32(out, in.leader) != 0 || hx_buf_put_u32(out, in.epoch) != 0 ||
                  hx_buf_put_u32(out, in.isr) != 0 || hx_buf_put_u64(out, in.hw) != 0 ||
                  hx_buf_put_u64(out, in.leo) != 0 ||
                  hx_buf_put_u8(out, in.role == HX_REPL_LEADER     ? 1
                                     : in.role == HX_REPL_FOLLOWER ? 2
                                                                   : 0) != 0;
        }
    }
    if (bad) {
        out->len = start;
        return HX_CL_FATAL;
    }
    resp_end(out, start);
    return HX_CL_DONE;
}

hx_cl_verdict hx_cluster_handle(hx_cluster *c, uint64_t conn, const uint8_t *req, size_t len,
                                hx_buf *out)
{
    hx_rd rd;
    hx_rd_init(&rd, req, len);
    const uint16_t api = hx_rd_u16(&rd);
    const uint16_t version = hx_rd_u16(&rd);
    const uint32_t corr = hx_rd_u32(&rd);
    if (!rd.ok || version != HX_PROTO_VERSION) {
        return HX_CL_PASS; /* the local broker reports header problems */
    }
    if (api == HX_API_CLUSTER_METADATA) {
        return hx_rd_left(&rd) == 0 ? cluster_metadata(c, corr, out)
                                    : resp_err(out, corr, HXE_BAD_REQUEST);
    }
    if (api == HX_API_PRODUCE_IDEMPOTENT) {
        (void)hx_rd_u64(&rd);
        (void)hx_rd_u32(&rd);
    } else if (api != HX_API_PRODUCE && api != HX_API_FETCH && api != HX_API_CREATE_TOPIC &&
               api != HX_API_DELETE_TOPIC) {
        return HX_CL_PASS;
    }
    const size_t tl = hx_rd_u16(&rd);
    const uint8_t *topic = hx_rd_take(&rd, tl);
    if (!rd.ok) {
        return HX_CL_PASS;
    }
    const hx_cl_topic *t = find_topic(c, topic, tl);
    if (t == NULL) {
        return HX_CL_PASS;
    }
    switch (api) {
    case HX_API_PRODUCE: {
        const uint32_t partition = hx_rd_u32(&rd);
        const uint32_t plen = hx_rd_u32(&rd);
        const uint8_t *payload = hx_rd_take(&rd, plen);
        if (!rd.ok || hx_rd_left(&rd) != 0) {
            return resp_err(out, corr, HXE_BAD_REQUEST);
        }
        return produce(c, conn, corr, t, partition, payload, plen, out);
    }
    case HX_API_FETCH: {
        const uint32_t partition = hx_rd_u32(&rd);
        const uint64_t offset = hx_rd_u64(&rd);
        const uint32_t max_bytes = hx_rd_u32(&rd);
        if (!rd.ok || hx_rd_left(&rd) != 0) {
            return resp_err(out, corr, HXE_BAD_REQUEST);
        }
        return fetch(c, corr, t, partition, offset, max_bytes, out);
    }
    default:
        return resp_err(out, corr, HXE_UNSUPPORTED);
    }
}

static size_t find_parked(const hx_cluster *c, uint64_t conn)
{
    for (size_t i = 0; i < c->npk; i++) {
        if (c->pk[i].conn == conn) {
            return i;
        }
    }
    return SIZE_MAX;
}

int hx_cluster_resolve(hx_cluster *c, uint64_t conn, hx_buf *out)
{
    const size_t i = find_parked(c, conn);
    if (i == SIZE_MAX) {
        return -1;
    }
    const parked k = c->pk[i];
    hx_rmgr_part_info in;
    if (hx_rmgr_info(c->rmgr, k.part, &in) != 0 || in.failed) {
        return -1;
    }
    uint16_t err = 0;
    bool done = true;
    if (in.hw >= k.target && in.epoch == k.epoch) {
        err = HXE_OK;
    } else if (in.role != HX_REPL_LEADER || in.epoch != k.epoch || c->now >= k.deadline) {
        /* Leadership moved (the record's fate belongs to the new leader) or the wait ran out. */
        err = HXE_REQUEST_TIMEOUT;
    } else {
        done = false;
    }
    if (!done) {
        return 0;
    }
    const size_t start = resp_begin(out, k.corr, err);
    if (err == HXE_OK) {
        (void)hx_buf_put_u64(out, k.first);
    }
    resp_end(out, start);
    c->pk[i] = c->pk[--c->npk];
    return 1;
}

void hx_cluster_drop(hx_cluster *c, uint64_t conn)
{
    const size_t i = find_parked(c, conn);
    if (i != SIZE_MAX) {
        c->pk[i] = c->pk[--c->npk];
    }
}
