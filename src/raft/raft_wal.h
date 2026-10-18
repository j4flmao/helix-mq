/*
 * Write-ahead log for Raft: the durable home of (term, voted_for) and the Raft log.
 *
 * Why not hx_log: Raft must be able to discard a conflicting suffix of its log, which the
 * append-only storage engine cannot do. This is a small dedicated file instead.
 *
 * File: 8-byte header (magic "HXRW", u32 version) followed by records
 *     u32 length | u32 crc32c(type + payload) | u8 type | payload          (length counts type +
 * payload) type 1 HARD   : u64 term | u32 voted_for type 2 ENTRIES: u64 first_index | u32 count |
 * count x (u64 term | u32 len | data) Replaying the records in order rebuilds the state:
 * ENTRIES(first, ...) discards every entry with index >= first and then stores the new ones,
 * exactly the hx_raft_storage `append` contract. type 3 SNAP: u64 index | u64 term | u32 len | data
 * folds every entry up to `index` into the snapshot (entries after it are kept only if the entry at
 * `index` has the same term), exactly the `save_snapshot` contract. ENTRIES indexes stay absolute.
 *
 * Crash safety: records are appended with write(2) and made durable by sync() (fdatasync). A crash
 * can leave a torn final record. On open, a record that is incomplete, or whose CRC fails AND which
 * ends exactly at end-of-file, is a torn tail and is cut off. A bad record that is followed by more
 * bytes cannot be a torn write: that is corruption, and open FAILS rather than silently dropping
 * later, possibly acknowledged data. Creating the file also fsyncs the directory.
 *
 * The file only grows; hx_raft_wal_compact() rewrites it as one HARD + (SNAP) + one ENTRIES record
 * (temp file + rename + directory fsync). open() compacts by itself when more than half of a
 * non-trivial file is dead.
 *
 * Threading: single owner thread.
 */
#ifndef HELIX_RAFT_WAL_H
#define HELIX_RAFT_WAL_H

#include <stdbool.h>
#include <stdint.h>

#include "raft/raft.h"

typedef struct hx_raft_wal hx_raft_wal;

typedef enum {
    HX_WAL_OK = 0,
    HX_WAL_IO = -1,      /* syscall failed (errno) */
    HX_WAL_CORRUPT = -2, /* damage that is not a torn tail, or an impossible record sequence */
    HX_WAL_NOMEM = -3,
    HX_WAL_INVAL = -4
} hx_wal_status;

/* Opens (creating if absent) the WAL at `path` and replays it. */
hx_wal_status hx_raft_wal_open(const char *path, hx_raft_wal **out);
void hx_raft_wal_close(hx_raft_wal *w);

/* State recovered by open(); the entries stay valid until the WAL is modified or closed. */
hx_raft_restore hx_raft_wal_restore(hx_raft_wal *w);

/* Plug into hx_raft_cfg.storage. */
hx_raft_storage hx_raft_wal_storage(hx_raft_wal *w);

hx_wal_status hx_raft_wal_compact(hx_raft_wal *w);

/*
 * TEST ONLY: skip the real fdatasync() but keep the bookkeeping, so simulations with hundreds of
 * thousands of syncs finish quickly. Never use it in a server: it voids the durability guarantee.
 */
void hx_raft_wal_set_nosync_for_tests(hx_raft_wal *w, bool nosync);

/* Introspection (tests, metrics). */
uint64_t hx_raft_wal_file_size(const hx_raft_wal *w);
uint64_t hx_raft_wal_synced_size(const hx_raft_wal *w);    /* bytes known durable */
uint64_t hx_raft_wal_entry_count(const hx_raft_wal *w);    /* retained entries only */
uint64_t hx_raft_wal_snapshot_index(const hx_raft_wal *w); /* 0 = no snapshot */

#endif /* HELIX_RAFT_WAL_H */
