/*
 * libFuzzer harness for the controller: the first byte selects the target. Mode 0 feeds a stream of
 * (u16 length | command bytes) to a controller, checking its invariants after every command; any
 * other mode hands the remaining bytes to hx_ctrl_restore(), which must reject or accept
 * consistently and never read out of bounds. A snapshot that restores must re-encode and restore
 * again to the same bytes.
 */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "ctrl/ctrl.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

// NOLINTNEXTLINE(readability-non-const-parameter): signature fixed by the libFuzzer ABI
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size == 0) {
        return 0;
    }
    hx_ctrl *c = hx_ctrl_create();
    if (c == NULL) {
        return 0;
    }
    if (data[0] == 0) {
        size_t pos = 1;
        uint64_t idx = 0;
        while (pos + 2U <= size) {
            const size_t len = ((size_t)data[pos] << 8) | data[pos + 1U];
            pos += 2U;
            if (len > size - pos) {
                break;
            }
            (void)hx_ctrl_apply_bytes(c, ++idx, data + pos, len);
            if (hx_ctrl_check(c) != 0) {
                abort();
            }
            pos += len;
        }
    } else if (hx_ctrl_restore(c, data + 1, size - 1U) == 0) {
        hx_buf a;
        hx_buf_init(&a);
        hx_ctrl *d = hx_ctrl_create();
        if (hx_ctrl_check(c) != 0 || hx_ctrl_snapshot(c, &a) != 0 || d == NULL ||
            hx_ctrl_restore(d, a.data, a.len) != 0) {
            abort();
        }
        hx_buf b;
        hx_buf_init(&b);
        if (hx_ctrl_snapshot(d, &b) != 0 || a.len != b.len || memcmp(a.data, b.data, a.len) != 0) {
            abort();
        }
        hx_buf_free(&a);
        hx_buf_free(&b);
        hx_ctrl_destroy(d);
    }
    hx_ctrl_destroy(c);
    return 0;
}
