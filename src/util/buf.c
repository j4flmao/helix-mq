#include "buf.h"

#include <stdlib.h>
#include <string.h>

#define HX_BUF_MIN_CAP 256U

void hx_buf_init(hx_buf *b)
{
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
}

void hx_buf_free(hx_buf *b)
{
    free(b->data);
    hx_buf_init(b);
}

int hx_buf_reserve(hx_buf *b, size_t extra)
{
    if (extra > SIZE_MAX - b->len) {
        return -1;
    }
    size_t need = b->len + extra;
    if (need <= b->cap) {
        return 0;
    }
    size_t cap = b->cap != 0 ? b->cap : HX_BUF_MIN_CAP;
    while (cap < need) {
        if (cap > SIZE_MAX / 2) {
            cap = need;
            break;
        }
        cap *= 2;
    }
    uint8_t *p = realloc(b->data, cap);
    if (p == NULL) {
        return -1;
    }
    b->data = p;
    b->cap = cap;
    return 0;
}

int hx_buf_put(hx_buf *b, const void *src, size_t n)
{
    if (n == 0) {
        return 0;
    }
    if (hx_buf_reserve(b, n) != 0) {
        return -1;
    }
    memcpy(b->data + b->len, src, n);
    b->len += n;
    return 0;
}

int hx_buf_put_u8(hx_buf *b, uint8_t v)
{
    return hx_buf_put(b, &v, 1);
}

int hx_buf_put_u16(hx_buf *b, uint16_t v)
{
    const uint8_t t[2] = {(uint8_t)(v >> 8), (uint8_t)v};
    return hx_buf_put(b, t, sizeof t);
}

int hx_buf_put_u32(hx_buf *b, uint32_t v)
{
    const uint8_t t[4] = {(uint8_t)(v >> 24), (uint8_t)(v >> 16), (uint8_t)(v >> 8), (uint8_t)v};
    return hx_buf_put(b, t, sizeof t);
}

int hx_buf_put_u64(hx_buf *b, uint64_t v)
{
    uint8_t t[8];
    for (size_t i = 0; i < sizeof t; i++) {
        t[i] = (uint8_t)(v >> (8U * (7U - i)));
    }
    return hx_buf_put(b, t, sizeof t);
}

void hx_buf_patch_u16(hx_buf *b, size_t pos, uint16_t v)
{
    if (pos > b->len || b->len - pos < 2) {
        return;
    }
    b->data[pos] = (uint8_t)(v >> 8);
    b->data[pos + 1] = (uint8_t)v;
}

void hx_buf_patch_u32(hx_buf *b, size_t pos, uint32_t v)
{
    if (pos > b->len || b->len - pos < 4) {
        return;
    }
    b->data[pos] = (uint8_t)(v >> 24);
    b->data[pos + 1] = (uint8_t)(v >> 16);
    b->data[pos + 2] = (uint8_t)(v >> 8);
    b->data[pos + 3] = (uint8_t)v;
}

void hx_buf_consume(hx_buf *b, size_t n)
{
    if (n >= b->len) {
        b->len = 0;
        return;
    }
    if (n > 0) {
        memmove(b->data, b->data + n, b->len - n);
        b->len -= n;
    }
}

void hx_buf_shrink(hx_buf *b, size_t keep_cap)
{
    if (b->len == 0 && b->cap > keep_cap) {
        hx_buf_free(b);
    }
}
