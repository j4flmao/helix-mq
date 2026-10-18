/*
 * Bounds-checked big-endian reader. Every accessor is safe on truncated input:
 * after the first underflow `ok` becomes false, all further reads return 0 /
 * NULL, and the caller checks rd->ok once at the end of parsing. This is the
 * only way request bytes are decoded, so it is the primary fuzz target.
 */
#ifndef HELIX_CODEC_H
#define HELIX_CODEC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    const uint8_t *p;
    size_t len;
    size_t pos;
    bool ok;
} hx_rd;

void hx_rd_init(hx_rd *r, const uint8_t *p, size_t len);
size_t hx_rd_left(const hx_rd *r);

uint8_t hx_rd_u8(hx_rd *r);
uint16_t hx_rd_u16(hx_rd *r);
uint32_t hx_rd_u32(hx_rd *r);
uint64_t hx_rd_u64(hx_rd *r);
int64_t hx_rd_i64(hx_rd *r);

/* Returns a pointer to the next n bytes (and advances), or NULL on underflow. */
const uint8_t *hx_rd_take(hx_rd *r, size_t n);

/* Unaligned big-endian load of 4 bytes; caller guarantees availability. */
static inline uint32_t hx_load_be32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

#endif /* HELIX_CODEC_H */
