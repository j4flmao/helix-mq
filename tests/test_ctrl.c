/*
 * Controller state machine: scenario tests, codec bounds, determinism and snapshot round-trip under
 * a random command storm. The cluster-level behaviour (the controller driving real replicas through
 * crashes and partitions) is covered by tests/test_repl.c, which uses this module as its
 * controller.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ctrl/ctrl.h"

static int failures;

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
            return;                                                                                \
        }                                                                                          \
    } while (0)

static uint64_t g_index;

static hx_ctrl_result ap(hx_ctrl *c, hx_ctrl_cmd m)
{
    return hx_ctrl_apply(c, ++g_index, &m);
}

static hx_ctrl_cmd create(uint32_t part, uint32_t replicas)
{
    hx_ctrl_cmd m;
    memset(&m, 0, sizeof m);
    m.type = HX_CTRL_CREATE;
    m.part = part;
    m.replicas = replicas;
    return m;
}

static hx_ctrl_cmd up(uint32_t b)
{
    hx_ctrl_cmd m;
    memset(&m, 0, sizeof m);
    static uint64_t life;
    m.type = HX_CTRL_BROKER_UP;
    m.broker = b;
    m.nonce = ++life; /* every call is a new process life */
    return m;
}

static hx_ctrl_cmd down(uint32_t b, uint64_t session)
{
    hx_ctrl_cmd m;
    memset(&m, 0, sizeof m);
    m.type = HX_CTRL_BROKER_DOWN;
    m.broker = b;
    m.session = session;
    return m;
}

static hx_ctrl_cmd alter(const hx_ctrl *c, uint32_t part, uint32_t from, uint32_t isr)
{
    hx_ctrl_part p;
    hx_ctrl_cmd m;
    memset(&m, 0, sizeof m);
    m.type = HX_CTRL_ALTER_ISR;
    m.part = part;
    m.broker = from;
    m.session = hx_ctrl_session(c, from);
    m.isr = isr;
    if (hx_ctrl_get(c, part, &p) == 0) {
        m.epoch = p.epoch;
        m.isr_version = p.isr_version;
    }
    return m;
}

static void election_basics(void)
{
    hx_ctrl *c = hx_ctrl_create();
    hx_ctrl_part p;
    CHECK(c != NULL);
    CHECK(ap(c, create(0, 0x7)) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == HX_CTRL_NONE && p.isr == 0x7); /* nobody up */
    CHECK(ap(c, create(0, 0x7)) == HX_CTRL_REJECTED);                              /* exists */
    CHECK(ap(c, create(5, 0x7)) == HX_CTRL_BAD);                                   /* not dense */
    CHECK(ap(c, create(1, 0)) == HX_CTRL_BAD);
    CHECK(ap(c, up(1)) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == 1 && p.isr == 0x2 && p.epoch == 1);
    CHECK(hx_ctrl_session(c, 1) == g_index);
    const uint64_t s1 = hx_ctrl_session(c, 1);
    CHECK(ap(c, up(0)) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == 1 && p.epoch == 1); /* no needless change */
    /* Broker 0 is alive but not in the ISR (it was dropped at the election): it can never lead. */
    CHECK(ap(c, down(1, s1)) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == HX_CTRL_NONE && p.epoch == 2);
    CHECK(ap(c, up(2)) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == HX_CTRL_NONE); /* 2 is not in the ISR either */
    CHECK(ap(c, up(1)) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == 1 && p.epoch == 3);
    CHECK(hx_ctrl_check(c) == 0);
    hx_ctrl_destroy(c);
}

static void stale_down_and_reregister(void)
{
    hx_ctrl *c = hx_ctrl_create();
    hx_ctrl_part p;
    CHECK(ap(c, create(0, 0x3)) == HX_CTRL_OK);
    CHECK(ap(c, up(0)) == HX_CTRL_OK);
    CHECK(ap(c, up(1)) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == 0 && p.isr == 0x1);
    const uint64_t old = hx_ctrl_session(c, 0);
    CHECK(ap(c, up(0)) == HX_CTRL_OK); /* leader restarted before the failure detector fired */
    CHECK(hx_ctrl_session(c, 0) > old);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == 0 && p.epoch == 1); /* same epoch, new life */
    CHECK(ap(c, down(0, old)) == HX_CTRL_REJECTED); /* report about the old life */
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == 0);
    CHECK(ap(c, down(0, hx_ctrl_session(c, 0))) == HX_CTRL_OK);
    CHECK(ap(c, down(0, hx_ctrl_session(c, 0))) == HX_CTRL_REJECTED); /* already down */
    hx_ctrl_destroy(c);
}

static void alter_validation(void)
{
    hx_ctrl *c = hx_ctrl_create();
    hx_ctrl_part p;
    CHECK(ap(c, create(0, 0x7)) == HX_CTRL_OK);
    CHECK(ap(c, up(0)) == HX_CTRL_OK && ap(c, up(1)) == HX_CTRL_OK && ap(c, up(2)) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.leader == 0 && p.isr == 0x1);
    /* The election dropped 1 and 2 (they registered later); the leader may add them back. */
    hx_ctrl_cmd m = alter(c, 0, 0, 0x3);
    CHECK(ap(c, m) == HX_CTRL_OK);
    CHECK(hx_ctrl_get(c, 0, &p) == 0 && p.isr == 0x3 && p.isr_version == 2);
    CHECK(ap(c, m) == HX_CTRL_REJECTED); /* same version again: replay is refused */
    m = alter(c, 0, 1, 0x3);             /* not the leader */
    CHECK(ap(c, m) == HX_CTRL_REJECTED);
    m = alter(c, 0, 0, 0x2); /* leader not in set */
    CHECK(ap(c, m) == HX_CTRL_REJECTED);
    m = alter(c, 0, 0, 0xB); /* 3 is not a replica */
    CHECK(ap(c, m) == HX_CTRL_REJECTED);
    m = alter(c, 0, 0, 0x3);
    CHECK(ap(c, m) == HX_CTRL_NOOP);
    m = alter(c, 0, 0, 0x3);
    m.epoch++; /* wrong epoch */
    CHECK(ap(c, m) == HX_CTRL_REJECTED);
    m = alter(c, 0, 0, 0x3);
    m.session--; /* wrong session */
    CHECK(ap(c, m) == HX_CTRL_REJECTED);
    m = alter(c, 0, 0, 0x1); /* shrink is fine */
    CHECK(ap(c, m) == HX_CTRL_OK);
    CHECK(ap(c, down(2, hx_ctrl_session(c, 2))) == HX_CTRL_OK);
    m = alter(c, 0, 0, 0x5); /* adding a dead broker is refused */
    CHECK(ap(c, m) == HX_CTRL_REJECTED);
    m = alter(c, 9, 0, 0x1);
    CHECK(ap(c, m) == HX_CTRL_BAD);
    CHECK(hx_ctrl_check(c) == 0);
    hx_ctrl_destroy(c);
}

static void duplicate_registration(void)
{
    hx_ctrl *c = hx_ctrl_create();
    CHECK(ap(c, create(0, 0x3)) == HX_CTRL_OK);
    hx_ctrl_cmd m = up(0);
    CHECK(ap(c, m) == HX_CTRL_OK);
    const uint64_t s0 = hx_ctrl_session(c, 0);
    CHECK(hx_ctrl_nonce(c, 0) == m.nonce);
    CHECK(ap(c, m) == HX_CTRL_NOOP); /* same life again: keeps its session */
    CHECK(hx_ctrl_session(c, 0) == s0);
    CHECK(ap(c, down(0, s0)) == HX_CTRL_OK);
    CHECK(ap(c, m) == HX_CTRL_OK); /* same life after being declared down gets a new session */
    CHECK(hx_ctrl_session(c, 0) > s0);
    hx_ctrl_destroy(c);
}

static void index_must_grow(void)
{
    hx_ctrl *c = hx_ctrl_create();
    hx_ctrl_cmd m = create(0, 1);
    CHECK(hx_ctrl_apply(c, 10, &m) == HX_CTRL_OK);
    m = create(1, 1);
    CHECK(hx_ctrl_apply(c, 10, &m) == HX_CTRL_BAD);
    CHECK(hx_ctrl_apply(c, 9, &m) == HX_CTRL_BAD);
    CHECK(hx_ctrl_nparts(c) == 1);
    hx_ctrl_destroy(c);
}

static void codec(void)
{
    hx_ctrl_cmd in[4];
    in[0] = create(3, 0x15);
    in[1] = up(7);
    in[1].nonce = 0xABCDEF0123456789ULL;
    in[2] = down(7, 0x1122334455667788ULL);
    memset(&in[3], 0, sizeof in[3]);
    in[3].type = HX_CTRL_ALTER_ISR;
    in[3].part = 9;
    in[3].broker = 2;
    in[3].session = 77;
    in[3].epoch = 5;
    in[3].isr = 0x6;
    in[3].isr_version = 8;
    for (size_t i = 0; i < 4; i++) {
        hx_buf b;
        hx_buf_init(&b);
        hx_ctrl_cmd out;
        CHECK(hx_ctrl_cmd_encode(&in[i], &b) == 0);
        CHECK(hx_ctrl_cmd_decode(b.data, b.len, &out) == 0);
        CHECK(in[i].type == out.type && in[i].part == out.part && in[i].broker == out.broker &&
              in[i].replicas == out.replicas && in[i].session == out.session &&
              in[i].nonce == out.nonce && in[i].epoch == out.epoch && in[i].isr == out.isr &&
              in[i].isr_version == out.isr_version);
        for (size_t cut = 0; cut < b.len; cut++) {
            CHECK(hx_ctrl_cmd_decode(b.data, cut, &out) != 0);
        }
        CHECK(hx_buf_put_u8(&b, 0) == 0 && hx_ctrl_cmd_decode(b.data, b.len, &out) != 0);
        hx_buf_free(&b);
    }
    const uint8_t bad[] = {0, 1, 2};
    hx_ctrl_cmd o;
    CHECK(hx_ctrl_cmd_decode(bad, sizeof bad, &o) != 0);
    CHECK(hx_ctrl_cmd_decode(bad, 0, &o) != 0);
}

static uint64_t rng_state = 0x9E3779B97F4A7C15ULL;

static uint64_t rnd(void)
{
    rng_state ^= rng_state >> 12;
    rng_state ^= rng_state << 25;
    rng_state ^= rng_state >> 27;
    return rng_state * 0x2545F4914F6CDD1DULL;
}

static bool same_state(const hx_ctrl *a, const hx_ctrl *b)
{
    hx_buf x;
    hx_buf y;
    hx_buf_init(&x);
    hx_buf_init(&y);
    const bool ok = hx_ctrl_snapshot(a, &x) == 0 && hx_ctrl_snapshot(b, &y) == 0 &&
                    x.len == y.len && memcmp(x.data, y.data, x.len) == 0;
    hx_buf_free(&x);
    hx_buf_free(&y);
    return ok;
}

/* Random commands, valid and garbage: invariants hold after every step, epochs and ISR versions
 * never go backwards, a replica with the same command log reaches the same bytes, and a snapshot
 * round-trips at any point. */
static void command_storm(uint64_t seed)
{
    rng_state ^= seed * 0xD6E8FEB86659FD93ULL;
    hx_ctrl *c = hx_ctrl_create();
    hx_ctrl *twin = hx_ctrl_create();
    hx_ctrl_part last[8];
    memset(last, 0, sizeof last);
    uint32_t nparts = 0;
    for (uint64_t idx = 1; idx <= 400; idx++) {
        hx_ctrl_cmd m;
        memset(&m, 0, sizeof m);
        const uint64_t k = rnd() % 10U;
        if (k == 0 && nparts < 8U) {
            m = create(nparts, (uint32_t)(rnd() % 0x3FU) | 1U);
        } else if (k <= 3U) {
            m = up((uint32_t)(rnd() % 6U));
        } else if (k <= 5U) {
            const uint32_t b = (uint32_t)(rnd() % 6U);
            m = down(b, rnd() % 4U == 0 ? rnd() % 50U : hx_ctrl_session(c, b));
        } else if (nparts > 0) {
            const uint32_t pt = (uint32_t)(rnd() % nparts);
            hx_ctrl_part p;
            CHECK(hx_ctrl_get(c, pt, &p) == 0);
            m = alter(c, pt, p.leader == HX_CTRL_NONE ? 0 : p.leader, (uint32_t)(rnd() % 0x40U));
            if (rnd() % 4U == 0) {
                m.isr_version += (uint32_t)(rnd() % 3U);
            }
        } else {
            m = up(0);
        }
        hx_buf b;
        hx_buf_init(&b);
        CHECK(hx_ctrl_cmd_encode(&m, &b) == 0);
        const hx_ctrl_result r1 = hx_ctrl_apply_bytes(c, idx, b.data, b.len);
        const hx_ctrl_result r2 = hx_ctrl_apply_bytes(twin, idx, b.data, b.len);
        hx_buf_free(&b);
        CHECK(r1 == r2);
        if (r1 == HX_CTRL_OK && m.type == HX_CTRL_CREATE) {
            nparts++;
        }
        CHECK(hx_ctrl_check(c) == 0);
        for (uint32_t i = 0; i < nparts; i++) {
            hx_ctrl_part p;
            CHECK(hx_ctrl_get(c, i, &p) == 0);
            CHECK(p.epoch >= last[i].epoch && p.isr_version >= last[i].isr_version);
            last[i] = p;
        }
        if (idx % 37U == 0) {
            hx_buf s;
            hx_buf_init(&s);
            CHECK(hx_ctrl_snapshot(c, &s) == 0);
            hx_ctrl *r = hx_ctrl_create();
            CHECK(hx_ctrl_restore(r, s.data, s.len) == 0);
            CHECK(same_state(c, r));
            CHECK(hx_ctrl_restore(r, s.data, s.len - 1U) != 0); /* truncated */
            CHECK(same_state(c, r));                            /* failed restore changes nothing */
            hx_ctrl_destroy(r);
            hx_buf_free(&s);
        }
    }
    CHECK(same_state(c, twin));
    hx_ctrl_destroy(c);
    hx_ctrl_destroy(twin);
}

static void restore_rejects_garbage(void)
{
    hx_ctrl *c = hx_ctrl_create();
    CHECK(ap(c, create(0, 0x3)) == HX_CTRL_OK && ap(c, up(0)) == HX_CTRL_OK);
    hx_buf s;
    hx_buf_init(&s);
    CHECK(hx_ctrl_snapshot(c, &s) == 0);
    hx_ctrl *r = hx_ctrl_create();
    for (size_t i = 0; i < s.len; i++) { /* every single-byte flip: rejected or still consistent */
        uint8_t copy[512];
        CHECK(s.len <= sizeof copy);
        memcpy(copy, s.data, s.len);
        copy[i] ^= 0xFFU;
        if (hx_ctrl_restore(r, copy, s.len) == 0) {
            CHECK(hx_ctrl_check(r) == 0);
        }
    }
    hx_buf_free(&s);
    hx_ctrl_destroy(r);
    hx_ctrl_destroy(c);
}

int main(void)
{
    election_basics();
    stale_down_and_reregister();
    alter_validation();
    duplicate_registration();
    index_must_grow();
    codec();
    restore_rejects_garbage();
    const char *e = getenv("HX_CTRL_SEEDS");
    const uint64_t seeds = e != NULL ? strtoull(e, NULL, 10) : 300U;
    for (uint64_t s = 1; s <= seeds; s++) {
        command_storm(s);
    }
    if (failures != 0) {
        (void)fprintf(stderr, "%d controller test failure(s)\n", failures);
        return 1;
    }
    (void)puts("all controller tests passed");
    return 0;
}
