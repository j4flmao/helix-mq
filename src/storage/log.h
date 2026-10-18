/*
 * Helix storage engine — segmented, append-only, crash-safe commit log.
 *
 * Starter implementation of ARCHITECTURE.md §4 (Phase 1).
 *
 * Threading: an hx_log is owned by exactly ONE thread (shared-nothing model,
 * ADR-003). No internal locking. Do not share a handle across threads.
 *
 * Simplifications vs. the target design (tracked in PLAN.md Phase 1):
 *   - one record per frame (production format uses batches)
 *   - sparse index is rebuilt in memory on open (no .index/.timeindex files yet)
 *   - CRC32C on SSE4.2/ARMv8 when the CPU has it, slice-by-8 otherwise
 *   - interval fsync is checked on append (no background thread yet)
 */
#ifndef HELIX_LOG_H
#define HELIX_LOG_H

#include <stddef.h>
#include <stdint.h>

typedef enum {
    HX_OK = 0,
    HX_ERR_INVAL = -1, /* bad argument                                   */
    HX_ERR_IO = -2,    /* syscall failed (see errno)                     */
    HX_ERR_NOMEM = -3,
    HX_ERR_CORRUPT = -4,   /* corruption outside the recoverable tail        */
    HX_ERR_NOT_FOUND = -5, /* offset outside [first_offset, next_offset)     */
    HX_ERR_TOO_LARGE = -6, /* payload exceeds max_record_bytes               */
    HX_ERR_BUF_SMALL = -7  /* caller buffer too small; *out_len has the size */
} hx_err;

typedef enum {
    HX_SYNC_NEVER = 0,    /* rely on the OS page cache (benchmarks only)     */
    HX_SYNC_INTERVAL = 1, /* fdatasync when sync_interval_ms has elapsed     */
    HX_SYNC_ALWAYS = 2    /* fdatasync after every append                    */
} hx_sync_policy;

typedef struct {
    uint64_t segment_bytes;        /* roll threshold, default 64 MiB         */
    uint32_t max_record_bytes;     /* max payload, default 1 MiB             */
    uint32_t index_interval_bytes; /* sparse index density, default 4 KiB    */
    hx_sync_policy sync;           /* default HX_SYNC_INTERVAL               */
    uint32_t sync_interval_ms;     /* default 200                            */
    uint8_t group_commit;          /* with HX_SYNC_ALWAYS: append only marks the
                                      log dirty; the caller MUST hx_log_flush()
                                      before acknowledging (one fsync, many appends) */
} hx_log_opts;

typedef struct hx_log hx_log;

void hx_log_opts_default(hx_log_opts *o);

/* Open (creating if needed) the log in `dir`; recovers the tail segment. */
hx_err hx_log_open(const char *dir, const hx_log_opts *opts, hx_log **out);
void hx_log_close(hx_log *l);

/* Append one record; *out_offset (optional) receives its offset. */
hx_err hx_log_append(hx_log *l, const void *payload, uint32_t len, int64_t ts_ms,
                     uint64_t *out_offset);

/*
 * Read the record at `offset` into buf. *out_len is always set to the payload
 * length (even on HX_ERR_BUF_SMALL). out_ts_ms is optional.
 */
hx_err hx_log_read(hx_log *l, uint64_t offset, void *buf, size_t cap, uint32_t *out_len,
                   int64_t *out_ts_ms);

/* Force the active segment to stable storage. */
hx_err hx_log_sync(hx_log *l);

/* fdatasync only if appends are pending since the last sync (group commit). */
hx_err hx_log_flush(hx_log *l);
int hx_log_is_dirty(const hx_log *l);

/*
 * Drop every record with offset >= new_next (first_offset <= new_next <= next_offset). Durable when
 * it returns HX_OK: the cut is fsynced, and segments are removed newest first with a directory
 * fsync after each, so a crash at any point leaves a valid prefix of the log that is at least
 * new_next long, never a gap. The next append gets offset new_next.
 */
hx_err hx_log_truncate(hx_log *l, uint64_t new_next);

/* Delete oldest whole segments while total size > max_bytes (keeps the
 * active segment always). */
hx_err hx_log_retain_bytes(hx_log *l, uint64_t max_bytes);

uint64_t hx_log_first_offset(const hx_log *l); /* oldest retained offset     */
uint64_t hx_log_next_offset(const hx_log *l);  /* offset the next append gets */
size_t hx_log_segment_count(const hx_log *l);

const char *hx_strerror(hx_err e);

/*
 * CRC32C (Castagnoli). Chainable: crc = hx_crc32c(crc, buf, n). Start at 0.
 *
 * hx_crc32c() uses the widest instruction this CPU has (SSE4.2 on x86-64, the
 * CRC extension on ARMv8) and a portable slice-by-8 table otherwise. Both paths
 * must agree byte for byte -- tests/test_log.c checks every length and
 * alignment -- and hx_crc32c_hw() reports which one this host selected.
 */
uint32_t hx_crc32c(uint32_t crc, const void *data, size_t n);
uint32_t hx_crc32c_sw(uint32_t crc, const void *data, size_t n);
int hx_crc32c_hw(void);

#endif /* HELIX_LOG_H */
