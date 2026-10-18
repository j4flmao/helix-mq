/*
 * helixd — the Helix broker daemon.
 *
 *   helixd [options]                        standalone broker
 *   helixd --config FILE [options]          options from a file, command line wins
 *   helixd --cluster-file FILE --node-id N  replicated cluster member
 *
 * Options are layered, and later layers win: built-in defaults, then every
 * --config file in the order it was given, then the command line. A file is
 * read in either format: "key = value" lines (util/conf.c) or the XML subset
 * (util/confxml.c); the first non-space byte picks the parser. Both accept
 * '#'/<!-- --> comments and ${NAME} environment expansion; an unknown key, an
 * invalid value or an undefined variable refuses the start with the file and
 * line rather than silently falling back to a default.
 */
#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "broker/broker.h"
#include "broker/cluster.h"
#include "net/server.h"
#include "util/conf.h"
#include "util/logger.h"

#ifndef HELIX_VERSION
#define HELIX_VERSION "dev"
#endif

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int signo)
{
    (void)signo;
    g_stop = 1;
}

static void usage(FILE *out)
{
    (void)fputs(
        "usage: helixd [options]\n"
        "  --config FILE            read options from a .conf or .xml file; command line wins\n"
        "  --listen ADDR            IPv4 bind address        (default 127.0.0.1)\n"
        "  --port N                 TCP port, 0 = ephemeral  (default 9400)\n"
        "  --data-dir DIR           log directory            (default ./data)\n"
        "  --sync always|interval|never   fsync policy       (default interval)\n"
        "  --sync-interval-ms N     interval for 'interval'  (default 200)\n"
        "  --segment-bytes N        segment roll size        (default 64 MiB)\n"
        "  --max-record-bytes N     max record payload       (default 1 MiB)\n"
        "  --max-conns N            max client connections   (default 10000)\n"
        "  --port-file FILE         write the bound port here\n"
        "  --log-level LEVEL        debug|info|warn|error    (default info)\n"
        "  --cluster-file FILE      replicated topics + all nodes (cluster mode)\n"
        "  --node-id N              this node in the cluster file (cluster mode)\n"
        "  --ack-timeout-ms N       replicated Produce wait     (default 5000)\n"
        "  --version, --help\n",
        out);
}

static int parse_u64(const char *s, uint64_t min, uint64_t max, uint64_t *out)
{
    if (s == NULL || *s == '\0' || *s == '-') {
        return -1;
    }
    errno = 0;
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno != 0 || *end != '\0' || v < min || v > max) {
        return -1;
    }
    *out = v;
    return 0;
}

enum {
    OPT_LISTEN = 256,
    OPT_PORT,
    OPT_DATA_DIR,
    OPT_SYNC,
    OPT_SYNC_INTERVAL,
    OPT_SEGMENT,
    OPT_MAX_RECORD,
    OPT_MAX_CONNS,
    OPT_PORT_FILE,
    OPT_LOG_LEVEL,
    OPT_CLUSTER_FILE,
    OPT_NODE_ID,
    OPT_ACK_TIMEOUT,
    OPT_VERSION,
    OPT_HELP
};

static const struct option k_opts[] = {
    {"listen", required_argument, NULL, OPT_LISTEN},
    {"port", required_argument, NULL, OPT_PORT},
    {"data-dir", required_argument, NULL, OPT_DATA_DIR},
    {"sync", required_argument, NULL, OPT_SYNC},
    {"sync-interval-ms", required_argument, NULL, OPT_SYNC_INTERVAL},
    {"segment-bytes", required_argument, NULL, OPT_SEGMENT},
    {"max-record-bytes", required_argument, NULL, OPT_MAX_RECORD},
    {"max-conns", required_argument, NULL, OPT_MAX_CONNS},
    {"port-file", required_argument, NULL, OPT_PORT_FILE},
    {"log-level", required_argument, NULL, OPT_LOG_LEVEL},
    {"cluster-file", required_argument, NULL, OPT_CLUSTER_FILE},
    {"node-id", required_argument, NULL, OPT_NODE_ID},
    {"ack-timeout-ms", required_argument, NULL, OPT_ACK_TIMEOUT},
    {"version", no_argument, NULL, OPT_VERSION},
    {"help", no_argument, NULL, OPT_HELP},
    {NULL, 0, NULL, 0}};

typedef struct {
    const char *cluster_file;
    uint32_t node_id;
    bool have_node_id;
    uint32_t ack_timeout_ms;
} cluster_opts;

/*
 * Options must outlive parsing: the configuration file hands out values from a
 * scratch buffer, so every string is copied into a store owned by main().
 */
typedef struct {
    char *owned[16];
    size_t nowned;
} str_store;

static const char *keep(str_store *ss, const char *s)
{
    if (ss->nowned >= sizeof ss->owned / sizeof ss->owned[0]) {
        return NULL;
    }
    char *copy = strdup(s);
    if (copy == NULL) {
        return NULL;
    }
    ss->owned[ss->nowned++] = copy;
    return copy;
}

static void keep_free(str_store *ss)
{
    for (size_t i = 0; i < ss->nowned; i++) {
        free(ss->owned[i]);
    }
    ss->nowned = 0;
}

enum { OPT_APPLIED = 0, OPT_INVALID = -1, OPT_UNKNOWN = -2 };

/* `reason` is filled without the "helixd: " prefix so callers can add a location. */
static int opt_fail(char *reason, size_t rlen, const char *text)
{
    (void)snprintf(reason, rlen, "%s", text);
    return OPT_INVALID;
}

/*
 * One option, wherever it came from: a command-line flag or a --config file.
 * Command-line names are reported as "--port" (tests and muscle memory depend
 * on it); configuration-only names as "value for 'max-groups'".
 */
static int apply_option(hx_broker_cfg *bc, hx_server_cfg *sc, cluster_opts *co, str_store *ss,
                        const char *key, const char *val, char *reason, size_t rlen)
{
    uint64_t v = 0;
    if (strcmp(key, "listen") == 0) {
        const char *addr = keep(ss, val);
        if (addr == NULL) {
            return opt_fail(reason, rlen, "out of memory");
        }
        sc->bind_addr = addr;
    } else if (strcmp(key, "port") == 0) {
        if (parse_u64(val, 0, UINT16_MAX, &v) != 0) {
            return opt_fail(reason, rlen, "invalid --port");
        }
        sc->port = (uint16_t)v;
    } else if (strcmp(key, "data-dir") == 0) {
        const char *dir = keep(ss, val);
        if (dir == NULL) {
            return opt_fail(reason, rlen, "out of memory");
        }
        bc->data_dir = dir;
    } else if (strcmp(key, "sync") == 0) {
        if (strcmp(val, "always") == 0) {
            bc->log_opts.sync = HX_SYNC_ALWAYS;
        } else if (strcmp(val, "interval") == 0) {
            bc->log_opts.sync = HX_SYNC_INTERVAL;
        } else if (strcmp(val, "never") == 0) {
            bc->log_opts.sync = HX_SYNC_NEVER;
        } else {
            return opt_fail(reason, rlen, "invalid --sync");
        }
    } else if (strcmp(key, "sync-interval-ms") == 0) {
        if (parse_u64(val, 1, 3600000, &v) != 0) {
            return opt_fail(reason, rlen, "invalid --sync-interval-ms");
        }
        bc->log_opts.sync_interval_ms = (uint32_t)v;
    } else if (strcmp(key, "segment-bytes") == 0) {
        if (parse_u64(val, 4096, (uint64_t)1 << 40, &v) != 0) {
            return opt_fail(reason, rlen, "invalid --segment-bytes (min 4096)");
        }
        bc->log_opts.segment_bytes = v;
    } else if (strcmp(key, "max-record-bytes") == 0) {
        if (parse_u64(val, 1, (uint64_t)64 << 20, &v) != 0) {
            return opt_fail(reason, rlen, "invalid --max-record-bytes");
        }
        bc->log_opts.max_record_bytes = (uint32_t)v;
    } else if (strcmp(key, "max-conns") == 0) {
        if (parse_u64(val, 1, 1000000, &v) != 0) {
            return opt_fail(reason, rlen, "invalid --max-conns");
        }
        sc->max_conns = (uint32_t)v;
    } else if (strcmp(key, "port-file") == 0) {
        const char *file = keep(ss, val);
        if (file == NULL) {
            return opt_fail(reason, rlen, "out of memory");
        }
        sc->port_file = file;
    } else if (strcmp(key, "log-level") == 0) {
        hxl_level lvl = HXL_INFO;
        if (hxl_parse_level(val, &lvl) != 0) {
            return opt_fail(reason, rlen, "invalid --log-level");
        }
        hxl_set_level(lvl);
    } else if (strcmp(key, "cluster-file") == 0) {
        const char *file = keep(ss, val);
        if (file == NULL) {
            return opt_fail(reason, rlen, "out of memory");
        }
        co->cluster_file = file;
    } else if (strcmp(key, "node-id") == 0) {
        if (parse_u64(val, 0, 7, &v) != 0) {
            return opt_fail(reason, rlen, "invalid --node-id");
        }
        co->node_id = (uint32_t)v;
        co->have_node_id = true;
    } else if (strcmp(key, "ack-timeout-ms") == 0) {
        if (parse_u64(val, 1, 600000, &v) != 0) {
            return opt_fail(reason, rlen, "invalid --ack-timeout-ms");
        }
        co->ack_timeout_ms = (uint32_t)v;
    } else if (strcmp(key, "max-fetch-bytes") == 0) {
        if (parse_u64(val, 4096, (uint64_t)1 << 30, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'max-fetch-bytes' out of range (min 4096)");
        }
        bc->max_fetch_bytes = (uint32_t)v;
    } else if (strcmp(key, "max-open-logs") == 0) {
        if (parse_u64(val, 1, (uint64_t)1 << 20, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'max-open-logs' out of range (min 1)");
        }
        bc->max_open_logs = (uint32_t)v;
    } else if (strcmp(key, "max-offset-entries") == 0) {
        if (parse_u64(val, 1, (uint64_t)1 << 24, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'max-offset-entries' out of range (min 1)");
        }
        bc->max_offset_entries = (uint32_t)v;
    } else if (strcmp(key, "max-groups") == 0) {
        if (parse_u64(val, 1, (uint64_t)1 << 20, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'max-groups' out of range (min 1)");
        }
        bc->max_groups = (uint32_t)v;
    } else if (strcmp(key, "max-group-members") == 0) {
        if (parse_u64(val, 1, (uint64_t)1 << 20, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'max-group-members' out of range (min 1)");
        }
        bc->max_group_members = (uint32_t)v;
    } else if (strcmp(key, "max-producers") == 0) {
        if (parse_u64(val, 1, (uint64_t)1 << 20, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'max-producers' out of range (min 1)");
        }
        bc->max_producers = (uint32_t)v;
    } else if (strcmp(key, "producer-ttl-ms") == 0) {
        if (parse_u64(val, 1000, 86400000, &v) != 0) {
            return opt_fail(reason, rlen,
                            "value for 'producer-ttl-ms' out of range (1000..86400000)");
        }
        bc->producer_ttl_ms = (uint32_t)v;
    } else if (strcmp(key, "max-out-pending") == 0) {
        if (parse_u64(val, 65536, (uint64_t)1 << 32, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'max-out-pending' out of range (min 65536)");
        }
        sc->max_out_pending = (size_t)v;
    } else if (strcmp(key, "max-frame-bytes") == 0) {
        if (parse_u64(val, 4096, (uint64_t)1 << 30, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'max-frame-bytes' out of range (min 4096)");
        }
        sc->max_frame_bytes = (uint32_t)v;
    } else if (strcmp(key, "listen-backlog") == 0) {
        if (parse_u64(val, 1, 65535, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'listen-backlog' out of range (1..65535)");
        }
        sc->listen_backlog = (int)v;
    } else if (strcmp(key, "tcp-nodelay") == 0) {
        if (parse_u64(val, 0, 1, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'tcp-nodelay' must be 0 or 1");
        }
        sc->tcp_nodelay = (uint8_t)v;
    } else if (strcmp(key, "recv-buffer-bytes") == 0) {
        if (parse_u64(val, 0, (uint64_t)1 << 30, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'recv-buffer-bytes' out of range (max 1 GiB)");
        }
        sc->recv_buffer_bytes = (uint32_t)v;
    } else if (strcmp(key, "send-buffer-bytes") == 0) {
        if (parse_u64(val, 0, (uint64_t)1 << 30, &v) != 0) {
            return opt_fail(reason, rlen, "value for 'send-buffer-bytes' out of range (max 1 GiB)");
        }
        sc->send_buffer_bytes = (uint32_t)v;
    } else {
        return OPT_UNKNOWN;
    }
    return OPT_APPLIED;
}

/* Command-line option id -> configuration key (NULL for flags that set no option). */
static const char *opt_key(int opt)
{
    switch (opt) {
    case OPT_LISTEN:
        return "listen";
    case OPT_PORT:
        return "port";
    case OPT_DATA_DIR:
        return "data-dir";
    case OPT_SYNC:
        return "sync";
    case OPT_SYNC_INTERVAL:
        return "sync-interval-ms";
    case OPT_SEGMENT:
        return "segment-bytes";
    case OPT_MAX_RECORD:
        return "max-record-bytes";
    case OPT_MAX_CONNS:
        return "max-conns";
    case OPT_PORT_FILE:
        return "port-file";
    case OPT_LOG_LEVEL:
        return "log-level";
    case OPT_CLUSTER_FILE:
        return "cluster-file";
    case OPT_NODE_ID:
        return "node-id";
    case OPT_ACK_TIMEOUT:
        return "ack-timeout-ms";
    default:
        return NULL;
    }
}

static int parse_args(int argc, char **argv, hx_broker_cfg *bc, hx_server_cfg *sc, cluster_opts *co,
                      str_store *ss)
{
    int opt = 0;
    while ((opt = getopt_long(argc, argv, "", k_opts, NULL)) != -1) {
        const char *key = opt_key(opt);
        if (key != NULL) {
            char reason[200];
            int rc = apply_option(bc, sc, co, ss, key, optarg, reason, sizeof reason);
            if (rc == OPT_UNKNOWN) {
                usage(stderr);
                return -1;
            }
            if (rc != OPT_APPLIED) {
                (void)fprintf(stderr, "helixd: %s\n", reason);
                return -1;
            }
            continue;
        }
        switch (opt) {
        case OPT_VERSION:
            (void)printf("helixd %s\n", HELIX_VERSION);
            exit(0);
        case OPT_HELP:
            usage(stdout);
            exit(0);
        default:
            usage(stderr);
            return -1;
        }
    }
    if (optind != argc) {
        usage(stderr);
        return -1;
    }
    return 0;
}

/* ------------------------------------------------- configuration files */

typedef struct {
    hx_broker_cfg *bc;
    hx_server_cfg *sc;
    cluster_opts *co;
    str_store *ss;
} conf_ctx;

/* The parser prefixes "<file>:<line>: " and, for a setting inside an
 * <include>, names the included file rather than the one that pulled it in. */
static int conf_apply(void *ctx, const char *key, const char *val, size_t line, char *err,
                      size_t errlen)
{
    (void)line;
    conf_ctx *cc = ctx;
    int rc = apply_option(cc->bc, cc->sc, cc->co, cc->ss, key, val, err, errlen);
    if (rc == OPT_UNKNOWN) {
        (void)snprintf(err, errlen, "unknown option '%s'", key);
        return -1;
    }
    return rc == OPT_APPLIED ? 0 : -1;
}

static int load_config(const char *path, hx_broker_cfg *bc, hx_server_cfg *sc, cluster_opts *co,
                       str_store *ss)
{
    conf_ctx cc = {.bc = bc, .sc = sc, .co = co, .ss = ss};
    char err[512];
    if (hx_config_load(path, conf_apply, &cc, err, sizeof err) != 0) {
        (void)fprintf(stderr, "helixd: %s\n", err);
        return -1;
    }
    return 0;
}

/*
 * Collect every --config/--config=FILE from argv (removing it, so getopt never
 * sees the flag) and load those files first: defaults < config file < flags.
 * Repeatable, so a wrapper can layer a base file under an environment file.
 */
static int load_config_files(int *pargc, char **argv, hx_broker_cfg *bc, hx_server_cfg *sc,
                             cluster_opts *co, str_store *ss)
{
    int rc = 0;
    int out = 1; /* rewritten argv write position; argv[0] is the program name */
    for (int i = 1; i < *pargc; i++) {
        const char *path = NULL;
        if (strcmp(argv[i], "--config") == 0) {
            if (i + 1 >= *pargc) {
                (void)fputs("helixd: --config needs a file\n", stderr);
                rc = -1;
                break;
            }
            path = argv[++i];
        } else if (strncmp(argv[i], "--config=", 9) == 0) {
            path = argv[i] + 9;
        }
        if (path == NULL) {
            argv[out++] = argv[i];
            continue;
        }
        if (load_config(path, bc, sc, co, ss) != 0) {
            rc = -1;
            break;
        }
    }
    if (rc != 0) {
        return rc;
    }
    for (int i = out; i < *pargc; i++) {
        argv[i] = NULL;
    }
    *pargc = out;
    return 0;
}

static int hook_handle(void *ctx, uint64_t conn, const uint8_t *req, size_t len, hx_buf *out)
{
    return (int)hx_cluster_handle(ctx, conn, req, len, out);
}

static int hook_resolve(void *ctx, uint64_t conn, hx_buf *out)
{
    return hx_cluster_resolve(ctx, conn, out);
}

static void hook_drop(void *ctx, uint64_t conn)
{
    hx_cluster_drop(ctx, conn);
}

static int hook_poll(void *ctx, int64_t now_ms)
{
    return hx_cluster_poll(ctx, now_ms);
}

static int install_signals(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; /* no SA_RESTART: epoll_wait must return EINTR */
    if (sigaction(SIGINT, &sa, NULL) != 0 || sigaction(SIGTERM, &sa, NULL) != 0) {
        return -1;
    }
    struct sigaction ign;
    memset(&ign, 0, sizeof ign);
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    return sigaction(SIGPIPE, &ign, NULL);
}

int main(int argc, char **argv)
{
    hx_broker_cfg bc;
    hx_server_cfg sc;
    hx_broker_cfg_default(&bc);
    hx_server_cfg_default(&sc);

    str_store store = {0};
    cluster_opts co = {.ack_timeout_ms = 5000};
    /* defaults < configuration file(s) < command line */
    if (load_config_files(&argc, argv, &bc, &sc, &co, &store) != 0) {
        keep_free(&store);
        return 2;
    }
    if (parse_args(argc, argv, &bc, &sc, &co, &store) != 0) {
        keep_free(&store);
        return 2;
    }
    if ((co.cluster_file == NULL) != !co.have_node_id) {
        (void)fputs("helixd: --cluster-file and --node-id go together\n", stderr);
        keep_free(&store);
        return 2;
    }
    static hx_cl_spec spec;
    if (co.cluster_file != NULL) {
        char err[200];
        if (hx_cl_spec_load(co.cluster_file, &spec, err, sizeof err) != 0) {
            (void)fprintf(stderr, "helixd: %s: %s\n", co.cluster_file, err);
            keep_free(&store);
            return 2;
        }
        if (co.node_id >= spec.nnodes) {
            (void)fputs("helixd: --node-id is not in the cluster file\n", stderr);
            keep_free(&store);
            return 2;
        }
        sc.bind_addr = spec.nodes[co.node_id].host;
        sc.port = spec.nodes[co.node_id].client_port;
    }
    /* A record must fit in one request frame (frame = header + topic + body). */
    if ((uint64_t)bc.log_opts.max_record_bytes + 65536U > sc.max_frame_bytes) {
        sc.max_frame_bytes = bc.log_opts.max_record_bytes + 65536U;
    }
    if (install_signals() != 0) {
        HX_ERROR("cannot install signal handlers (errno=%d)", errno);
        keep_free(&store);
        return 1;
    }

    hx_broker *broker = hx_broker_create(&bc);
    if (broker == NULL) {
        HX_ERROR("cannot initialise broker (data dir %s)", bc.data_dir);
        keep_free(&store);
        return 1;
    }
    HX_INFO("helixd %s starting: data_dir=%s sync=%d segment_bytes=%llu", HELIX_VERSION,
            bc.data_dir, (int)bc.log_opts.sync, (unsigned long long)bc.log_opts.segment_bytes);

    hx_cluster *cluster = NULL;
    hx_server_hooks hooks;
    if (co.cluster_file != NULL) {
        const hx_cluster_cfg cc = {.spec = &spec,
                                   .self = co.node_id,
                                   .data_dir = bc.data_dir,
                                   .log_opts = bc.log_opts,
                                   .max_fetch_bytes = bc.max_fetch_bytes,
                                   .ack_timeout_ms = co.ack_timeout_ms};
        cluster = hx_cluster_create(&cc);
        if (cluster == NULL) {
            hx_broker_destroy(broker);
            keep_free(&store);
            return 1;
        }
        hooks = (hx_server_hooks){.ctx = cluster,
                                  .handle = hook_handle,
                                  .resolve = hook_resolve,
                                  .drop = hook_drop,
                                  .poll = hook_poll,
                                  .tick_ms = 1};
        sc.hooks = &hooks;
    }

    int rc = hx_server_run(&sc, broker, &g_stop);
    hx_cluster_destroy(cluster);

    hx_broker_stats st = hx_broker_get_stats(broker);
    HX_INFO("shutting down: requests=%llu errors=%llu produced=%llu fetched=%llu",
            (unsigned long long)st.requests, (unsigned long long)st.errors,
            (unsigned long long)st.produced_records, (unsigned long long)st.fetched_records);
    hx_broker_destroy(broker);
    keep_free(&store);
    return rc == 0 ? 0 : 1;
}
