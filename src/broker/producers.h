/*
 * Idempotent-producer state: producer ids and per-(producer, topic, partition) sequence tracking.
 *
 * Contract (like Kafka's idempotent producer, with a smaller window): a producer obtains an id,
 * then numbers its records 0, 1, 2, ... per (topic, partition). The broker appends a record only if
 * its sequence is the next expected one. A sequence among the last HX_SEQ_WINDOW appended ones is a
 * retry and is answered with the original offset without appending again. Anything else is
 * rejected. Retries therefore cannot create duplicates, and gaps (lost records) are detected.
 *
 * Pure state machine, no I/O, injected clock, single owner thread. State is in memory only: after a
 * broker restart ids are unknown (HXE_UNKNOWN_PRODUCER) and the producer must obtain a new one; a
 * record whose acknowledgement was lost across that restart may then be written twice. Ids embed
 * the boot time so an id can never be re-issued after a restart.
 */
#ifndef HELIX_PRODUCERS_H
#define HELIX_PRODUCERS_H

#include <stddef.h>
#include <stdint.h>

#include "proto/protocol.h"

#define HX_SEQ_WINDOW           8U
#define HX_MAX_PRODUCER_STREAMS 256U

typedef struct hx_producers hx_producers;
typedef struct hx_stream hx_stream; /* opaque; valid until the next call into hx_producers */

typedef enum {
    HX_SEQ_APPEND,   /* next expected sequence: append, then call hx_producers_record() */
    HX_SEQ_DUPLICATE /* retry of a recent record: *dup_offset is where it was written */
} hx_seq_verdict;

/*
 * id_base: boot time in ms (ids are (id_base << 20) | counter, so a restart never reuses one).
 * ttl_ms: a producer silent for longer is forgotten.
 */
hx_producers *hx_producers_create(uint32_t max_producers, int64_t ttl_ms, uint64_t id_base);
void hx_producers_destroy(hx_producers *ps);

hx_wire_err hx_producers_init(hx_producers *ps, int64_t now_ms, uint64_t *pid);

/* Classifies `seq`. On HXE_OK and HX_SEQ_APPEND, *st must be passed to hx_producers_record(). */
hx_wire_err hx_producers_check(hx_producers *ps, int64_t now_ms, uint64_t pid, const char *topic,
                               size_t tlen, uint32_t partition, uint32_t seq,
                               hx_seq_verdict *verdict, uint64_t *dup_offset, hx_stream **st);

/* The append of `seq` succeeded at `offset`: advance the stream. */
void hx_producers_record(hx_stream *st, uint64_t offset);

/* Forget all streams of a topic (it was deleted; a recreated topic starts at offset 0). */
void hx_producers_drop_topic(hx_producers *ps, const char *topic, size_t tlen);

/* Remove producers idle for more than the TTL. Cheap when nothing is due (self-throttled to 1 s).
 */
void hx_producers_expire(hx_producers *ps, int64_t now_ms);

size_t hx_producers_count(const hx_producers *ps);

#endif /* HELIX_PRODUCERS_H */
