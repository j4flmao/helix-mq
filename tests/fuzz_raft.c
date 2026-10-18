/*
 * libFuzzer harness for the Raft message decoder and state machine. The input is a stream of
 * (u16 length | message bytes) records fed to a 3-node follower; decoding must never read out of
 * bounds and stepping arbitrary well-formed messages must never crash or corrupt memory. Safety
 * violations that Raft itself detects (rewriting a committed entry) end in the `failed` state, not
 * in undefined behaviour.
 */
#include <stdint.h>
#include <string.h>

#include "raft/raft.h"

static int nop_hard(void *ctx, uint64_t term, uint32_t voted_for)
{
    (void)ctx;
    (void)term;
    (void)voted_for;
    return 0;
}

static int nop_append(void *ctx, uint64_t first, const hx_raft_entry *e, size_t n)
{
    (void)ctx;
    (void)first;
    (void)e;
    (void)n;
    return 0;
}

static int nop_sync(void *ctx)
{
    (void)ctx;
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

// NOLINTNEXTLINE(readability-non-const-parameter): signature fixed by the libFuzzer ABI
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    hx_raft_cfg cfg;
    hx_raft_cfg_default(&cfg, 1, 3);
    cfg.storage.save_hard_state = nop_hard;
    cfg.storage.append = nop_append;
    cfg.storage.sync = nop_sync;
    hx_raft *r = hx_raft_create(&cfg, NULL, 0);
    if (r == NULL) {
        return 0;
    }
    hx_raft_entry scratch[64];
    int64_t now = 0;
    size_t pos = 0;
    while (pos + 2U <= size) {
        const size_t len = ((size_t)data[pos] << 8) | data[pos + 1U];
        pos += 2U;
        if (len > size - pos) {
            break;
        }
        hx_raft_msg m;
        if (hx_raft_msg_decode(data + pos, len, scratch, 64, &m) == 0) {
            now += 10;
            (void)hx_raft_step(r, now, &m);
            const hx_raft_msg *out = NULL;
            size_t n = 0;
            hx_raft_take_msgs(r, &out, &n);
            (void)hx_raft_tick(r, now + 400);
        }
        pos += len;
    }
    hx_raft_destroy(r);
    return 0;
}
