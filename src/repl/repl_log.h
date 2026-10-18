/*
 * Binds hx_repl to the real storage engine: a partition replica's log on top of hx_log.
 *
 * Each record is stored as `u32 epoch (little endian) | payload`, so the leader epoch travels with
 * the record and survives crashes without a separate file. An in-memory run-length index
 * (epoch -> first offset) answers epoch_at() and end_offset_for_epoch(). It is rebuilt when the log
 * is opened by reading every record's prefix, so opening costs O(records): a persistent epoch
 * checkpoint is future work.
 *
 * Contract with hx_repl:
 *   - append() only writes; sync() is the only durability point (the log is opened with
 *     HX_SYNC_NEVER and sync() calls hx_log_sync);
 *   - truncate() is durable when it returns (hx_log_truncate), a crash can never bring the cut
 *     records back after later appends;
 *   - epochs never decrease along the log; an append that would break this fails.
 * Retention is not supported on a replicated log yet: the log must start at offset 0.
 */
#ifndef HELIX_REPL_LOG_H
#define HELIX_REPL_LOG_H

#include "repl/repl.h"
#include "storage/log.h"

typedef struct hx_plog hx_plog;

hx_err hx_plog_open(const char *dir, const hx_log_opts *opts, hx_plog **out);
void hx_plog_close(hx_plog *p);

/* The vtable to put into hx_repl_cfg.log. */
hx_repl_log hx_plog_vtable(hx_plog *p);

uint64_t hx_plog_leo(const hx_plog *p);

/* Tests only: make sync() a no-op (the simulated crash model decides what survives). */
void hx_plog_set_nosync_for_tests(hx_plog *p, bool nosync);

/* Consumer-side read: payload without the epoch prefix. Same contract as hx_log_read. */
hx_err hx_plog_read_payload(hx_plog *p, uint64_t offset, void *buf, size_t cap, uint32_t *out_len,
                            uint32_t *out_epoch);

#endif
