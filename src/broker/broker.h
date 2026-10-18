/*
 * Broker core: owns the (topic, partition) -> hx_log registry and turns one
 * decoded request frame into one response frame. It performs no network I/O,
 * so it can be unit-tested and fuzzed directly.
 *
 * Threading: single-owner (ADR-003). One broker instance is used by exactly
 * one event-loop thread.
 */
#ifndef HELIX_BROKER_H
#define HELIX_BROKER_H

#include <stddef.h>
#include <stdint.h>

#include "storage/log.h"
#include "util/buf.h"

typedef struct {
    const char *data_dir;
    hx_log_opts log_opts;
    uint32_t max_fetch_bytes;    /* upper bound for one Fetch response body     */
    uint32_t max_open_logs;      /* cap on simultaneously open partitions       */
    uint32_t max_offset_entries; /* cap on committed (group, topic, partition) offsets */
    uint32_t max_groups;         /* cap on live consumer groups                  */
    uint32_t max_group_members;  /* cap on live group members, all groups together */
    uint32_t max_producers;      /* cap on live idempotent producers              */
    uint32_t producer_ttl_ms;    /* a producer silent this long is forgotten      */
    int64_t (*clock_ms)(void);   /* monotonic ms; NULL = CLOCK_MONOTONIC (tests inject a fake) */
} hx_broker_cfg;

typedef struct {
    uint64_t requests;
    uint64_t errors;
    uint64_t produced_records;
    uint64_t produced_bytes;
    uint64_t fetched_records;
    uint64_t fetched_bytes;
    uint64_t syncs; /* fdatasync calls issued by hx_broker_commit */
    uint64_t group_rebalances;
    uint64_t deduped_records; /* idempotent retries answered without appending */
} hx_broker_stats;

typedef struct hx_broker hx_broker;

void hx_broker_cfg_default(hx_broker_cfg *cfg);

/* Returns NULL on failure (bad config, out of memory, data_dir unusable). */
hx_broker *hx_broker_create(const hx_broker_cfg *cfg);
void hx_broker_destroy(hx_broker *b);

/*
 * Handle one request. `req` points at the bytes AFTER the 4-byte length
 * prefix (api, version, correlation id, body). On success a complete response
 * frame (including its length prefix) is appended to `out` and 0 is returned.
 * Returns -1 if the request is too short to carry a header or on allocation
 * failure; the caller must then close the connection. `out` is left exactly as
 * it was in that case.
 */
int hx_broker_handle(hx_broker *b, const uint8_t *req, size_t len, hx_buf *out);

/*
 * Group commit. With cfg.log_opts.sync == HX_SYNC_ALWAYS the broker does NOT
 * fsync inside hx_broker_handle(); it remembers which partitions were written.
 * The caller must call hx_broker_commit() after handling a batch of requests
 * and BEFORE sending any of their responses. One fdatasync per dirty partition
 * then covers every append in the batch. Returns 0 on success and -1 if any
 * sync failed; the responses of that batch must then NOT be sent (and, because
 * a failed fsync cannot be retried safely on Linux, the process should stop).
 * With other sync policies this is a cheap no-op.
 */
int hx_broker_commit(hx_broker *b);

/*
 * Expire silent group members and overdue rebalances. Call regularly (the server does so every loop
 * iteration); group requests also apply due expiries themselves, so a missed tick only delays
 * eviction of members nobody is talking to.
 */
void hx_broker_tick(hx_broker *b);

hx_broker_stats hx_broker_get_stats(const hx_broker *b);

#endif /* HELIX_BROKER_H */
