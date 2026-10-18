#include "producers.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define HX_PRODUCER_BUCKETS 256U
#define HX_ID_COUNTER_BITS  20U
#define HX_EXPIRE_EVERY_MS  1000

struct hx_stream {
    size_t tlen;
    uint32_t partition;
    uint32_t next_seq;
    uint32_t recorded; /* how many appends this stream has seen, saturating at the window */
    uint64_t offsets[HX_SEQ_WINDOW];
    char topic[HX_MAX_TOPIC_LEN];
};

typedef struct producer {
    struct producer *next;
    uint64_t pid;
    int64_t last_used;
    hx_stream *streams;
    size_t nstreams;
    size_t cap;
} producer;

struct hx_producers {
    producer *buckets[HX_PRODUCER_BUCKETS];
    size_t count;
    uint32_t max_producers;
    int64_t ttl_ms;
    uint64_t id_base;
    uint64_t counter;
    int64_t last_expire;
    bool expired_once;
};

hx_producers *hx_producers_create(uint32_t max_producers, int64_t ttl_ms, uint64_t id_base)
{
    if (max_producers == 0 || ttl_ms <= 0) {
        return NULL;
    }
    hx_producers *ps = calloc(1, sizeof *ps);
    if (ps == NULL) {
        return NULL;
    }
    ps->max_producers = max_producers;
    ps->ttl_ms = ttl_ms;
    ps->id_base = id_base;
    return ps;
}

static size_t bucket_of_pid(uint64_t pid)
{
    return (size_t)((pid * 0x9E3779B97F4A7C15ULL) >> 56) % HX_PRODUCER_BUCKETS;
}

static void free_producer(hx_producers *ps, producer *p)
{
    free(p->streams);
    free(p);
    ps->count--;
}

void hx_producers_destroy(hx_producers *ps)
{
    if (ps == NULL) {
        return;
    }
    for (size_t i = 0; i < HX_PRODUCER_BUCKETS; i++) {
        while (ps->buckets[i] != NULL) {
            producer *p = ps->buckets[i];
            ps->buckets[i] = p->next;
            free_producer(ps, p);
        }
    }
    free(ps);
}

size_t hx_producers_count(const hx_producers *ps)
{
    return ps->count;
}

static producer *find_producer(const hx_producers *ps, uint64_t pid)
{
    for (producer *p = ps->buckets[bucket_of_pid(pid)]; p != NULL; p = p->next) {
        if (p->pid == pid) {
            return p;
        }
    }
    return NULL;
}

hx_wire_err hx_producers_init(hx_producers *ps, int64_t now_ms, uint64_t *pid)
{
    hx_producers_expire(ps, now_ms);
    if (ps->count >= ps->max_producers || ps->counter >= (1ULL << HX_ID_COUNTER_BITS) - 1U) {
        return HXE_RESOURCE_LIMIT;
    }
    producer *p = calloc(1, sizeof *p);
    if (p == NULL) {
        return HXE_STORAGE;
    }
    p->pid = (ps->id_base << HX_ID_COUNTER_BITS) | ++ps->counter;
    p->last_used = now_ms;
    const size_t bk = bucket_of_pid(p->pid);
    p->next = ps->buckets[bk];
    ps->buckets[bk] = p;
    ps->count++;
    *pid = p->pid;
    return HXE_OK;
}

static hx_stream *find_stream(producer *p, const char *topic, size_t tlen, uint32_t partition)
{
    for (size_t i = 0; i < p->nstreams; i++) {
        hx_stream *s = &p->streams[i];
        if (s->partition == partition && s->tlen == tlen && memcmp(s->topic, topic, tlen) == 0) {
            return s;
        }
    }
    return NULL;
}

hx_wire_err hx_producers_check(hx_producers *ps, int64_t now_ms, uint64_t pid, const char *topic,
                               size_t tlen, uint32_t partition, uint32_t seq,
                               hx_seq_verdict *verdict, uint64_t *dup_offset, hx_stream **st)
{
    producer *p = find_producer(ps, pid);
    if (p == NULL) {
        return HXE_UNKNOWN_PRODUCER;
    }
    p->last_used = now_ms;
    hx_stream *s = find_stream(p, topic, tlen, partition);
    if (s == NULL) {
        if (seq != 0) {
            return HXE_OUT_OF_ORDER_SEQUENCE; /* a fresh stream must start at 0 */
        }
        if (p->nstreams >= HX_MAX_PRODUCER_STREAMS) {
            return HXE_RESOURCE_LIMIT;
        }
        if (p->nstreams == p->cap) {
            const size_t ncap = p->cap == 0 ? 4U : p->cap * 2U;
            hx_stream *grown = realloc(p->streams, ncap * sizeof *grown);
            if (grown == NULL) {
                return HXE_STORAGE;
            }
            p->streams = grown;
            p->cap = ncap;
        }
        s = &p->streams[p->nstreams++];
        memset(s, 0, sizeof *s);
        s->tlen = tlen;
        s->partition = partition;
        memcpy(s->topic, topic, tlen);
    }
    const uint32_t behind = s->next_seq - seq; /* wraps correctly modulo 2^32 */
    if (behind == 0) {
        *verdict = HX_SEQ_APPEND;
        *st = s;
        return HXE_OK;
    }
    if (behind <= s->recorded) {
        *verdict = HX_SEQ_DUPLICATE;
        *dup_offset = s->offsets[seq % HX_SEQ_WINDOW];
        return HXE_OK;
    }
    return HXE_OUT_OF_ORDER_SEQUENCE;
}

void hx_producers_record(hx_stream *st, uint64_t offset)
{
    st->offsets[st->next_seq % HX_SEQ_WINDOW] = offset;
    st->next_seq++;
    if (st->recorded < HX_SEQ_WINDOW) {
        st->recorded++;
    }
}

void hx_producers_drop_topic(hx_producers *ps, const char *topic, size_t tlen)
{
    for (size_t i = 0; i < HX_PRODUCER_BUCKETS; i++) {
        for (producer *p = ps->buckets[i]; p != NULL; p = p->next) {
            size_t w = 0;
            for (size_t r = 0; r < p->nstreams; r++) {
                if (p->streams[r].tlen == tlen && memcmp(p->streams[r].topic, topic, tlen) == 0) {
                    continue;
                }
                if (w != r) {
                    p->streams[w] = p->streams[r];
                }
                w++;
            }
            p->nstreams = w;
        }
    }
}

void hx_producers_expire(hx_producers *ps, int64_t now_ms)
{
    if (ps->expired_once && now_ms - ps->last_expire < HX_EXPIRE_EVERY_MS) {
        return;
    }
    ps->expired_once = true;
    ps->last_expire = now_ms;
    for (size_t i = 0; i < HX_PRODUCER_BUCKETS; i++) {
        producer **pp = &ps->buckets[i];
        while (*pp != NULL) {
            producer *p = *pp;
            if (now_ms - p->last_used > ps->ttl_ms) {
                *pp = p->next;
                free_producer(ps, p);
            } else {
                pp = &p->next;
            }
        }
    }
}
