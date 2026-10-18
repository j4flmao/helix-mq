/*
 * Growable byte buffer with big-endian (network order) append helpers.
 * All mutating functions return 0 on success, -1 on allocation failure or
 * size overflow; on failure the buffer is left valid and unchanged.
 */
#ifndef HELIX_BUF_H
#define HELIX_BUF_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
    uint8_t *data;
    size_t len;
    size_t cap;
} hx_buf;

void hx_buf_init(hx_buf *b);
void hx_buf_free(hx_buf *b);

/* Ensure room for `extra` more bytes. */
int hx_buf_reserve(hx_buf *b, size_t extra);

int hx_buf_put(hx_buf *b, const void *src, size_t n);
int hx_buf_put_u8(hx_buf *b, uint8_t v);
int hx_buf_put_u16(hx_buf *b, uint16_t v);
int hx_buf_put_u32(hx_buf *b, uint32_t v);
int hx_buf_put_u64(hx_buf *b, uint64_t v);

/* Overwrite 2/4 bytes at an already-written position (length back-patching). */
void hx_buf_patch_u16(hx_buf *b, size_t pos, uint16_t v);
void hx_buf_patch_u32(hx_buf *b, size_t pos, uint32_t v);

/* Drop the first n bytes (n >= len empties the buffer). */
void hx_buf_consume(hx_buf *b, size_t n);

/* Free the backing store if the buffer is empty and larger than keep_cap. */
void hx_buf_shrink(hx_buf *b, size_t keep_cap);

#endif /* HELIX_BUF_H */
