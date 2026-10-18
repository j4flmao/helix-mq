/*
 * Cluster mode of helixd: replicated topics.
 *
 * Every node of the cluster runs everything in this one process and thread: a Raft-backed
 * controller (hx_chost + hx_ctlsrv), a replica manager (hx_rmgr: controller client, replica
 * transport, one replicated log per partition) and, via the hooks below, the client-facing
 * protocol for the topics that the cluster file declares.
 *
 * Cluster file (the same on every node), one directive per line, '#' starts a comment:
 *     node  <id> <ipv4> <client_port> <raft_port> <ctl_port> <repl_port>   ids are 0..n-1, in order
 *     topic <name> <partitions>
 * Every partition of a declared topic is replicated to every node. A Produce is acknowledged when
 * the record is committed (high watermark passed it), i.e. replicated to the whole ISR and the ISR
 * has at least a majority of nodes. Fetch is served by the leader only, and only up to the
 * high watermark. Requests for topics that are not declared keep going to the local broker, whose
 * topics are NOT replicated.
 */
#ifndef HELIX_CLUSTER_H
#define HELIX_CLUSTER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "proto/protocol.h"
#include "storage/log.h"
#include "util/buf.h"

#define HX_CL_MAX_NODES  8U
#define HX_CL_MAX_TOPICS 64U
#define HX_CL_MAX_PARTS  1024U

typedef struct {
    char host[16];
    uint16_t client_port;
    uint16_t raft_port;
    uint16_t ctl_port;
    uint16_t repl_port;
} hx_cl_node;

typedef struct {
    char name[HX_MAX_TOPIC_LEN + 1];
    size_t nlen;
    uint32_t nparts;
    uint32_t base; /* cluster-wide id of partition 0 */
} hx_cl_topic;

typedef struct {
    uint32_t nnodes;
    hx_cl_node nodes[HX_CL_MAX_NODES];
    uint32_t ntopics;
    hx_cl_topic topics[HX_CL_MAX_TOPICS];
    uint32_t nparts; /* total */
} hx_cl_spec;

/* 0 on success; -1 with a message in err. */
int hx_cl_spec_parse(const char *text, hx_cl_spec *spec, char *err, size_t errcap);
int hx_cl_spec_load(const char *path, hx_cl_spec *spec, char *err, size_t errcap);

typedef struct {
    const hx_cl_spec *spec; /* must outlive the cluster */
    uint32_t self;
    const char *data_dir;
    hx_log_opts log_opts;
    uint32_t max_fetch_bytes;
    uint32_t ack_timeout_ms; /* Produce gives up waiting for the commit after this long */
} hx_cluster_cfg;

typedef struct hx_cluster hx_cluster;

hx_cluster *hx_cluster_create(const hx_cluster_cfg *cfg);
void hx_cluster_destroy(hx_cluster *c);

/* Drive controller, replication and parked requests. 0, or -1 on a fatal condition (a component
 * has failed; the process must stop). */
int hx_cluster_poll(hx_cluster *c, int64_t now_ms);

typedef enum {
    HX_CL_PASS = 0,  /* not a cluster request: give it to the local broker */
    HX_CL_DONE = 1,  /* response appended to out */
    HX_CL_PARK = 2,  /* answer later: no more frames from this connection until resolved */
    HX_CL_FATAL = -1 /* close the connection (malformed) */
} hx_cl_verdict;

hx_cl_verdict hx_cluster_handle(hx_cluster *c, uint64_t conn, const uint8_t *req, size_t len,
                                hx_buf *out);
/* 1 = the parked request of this connection finished and its response was appended to out,
 * 0 = still waiting, -1 = fatal failure. */
int hx_cluster_resolve(hx_cluster *c, uint64_t conn, hx_buf *out);
void hx_cluster_drop(hx_cluster *c, uint64_t conn);

uint16_t hx_cluster_client_port(const hx_cluster *c);

#endif
