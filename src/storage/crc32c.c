/*
 * CRC32C (Castagnoli), reflected polynomial 0x82F63B78.
 *
 * Three implementations of the same checksum, picked once at start-up:
 *
 *   - SSE4.2 `crc32` (x86-64)     one 8-byte word per instruction
 *   - ARMv8 CRC32 (aarch64)       `crc32cx`, the same shape
 *   - portable slice-by-8         8 bytes per iteration, the fallback
 *
 * The hardware paths are compiled in conditionally and selected by a runtime
 * CPU check, because a release archive is built for a generic target and must
 * still run on hosts that lack the instruction. hx_crc32c_sw() always exposes
 * the portable version so the unit test can compare the two on every machine:
 * a checksum that changed with the host would silently corrupt a log written
 * on one machine and verified on another.
 */
#include <stdbool.h>
#include <string.h>

#include "log.h"

#if defined(__x86_64__) || defined(__i386__)
#include <nmmintrin.h>
#define HX_CRC32C_X86 1
#elif defined(__aarch64__) && defined(__linux__)
#include <sys/auxv.h>
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1u << 7) /* uapi/asm-generic/hwcap.h: the CRC extension */
#endif
#define HX_CRC32C_ARM 1
#endif

#define POLY 0x82F63B78u

/* tables[0] is the byte table; tables[k] folds in one more byte (slice-by-8). */
static uint32_t tables[8][256];

__attribute__((constructor)) static void crc32c_init(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) {
            c = ((c & 1u) != 0u) ? (c >> 1) ^ POLY : (c >> 1);
        }
        tables[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = tables[0][i];
        for (int t = 1; t < 8; t++) {
            c = tables[0][c & 0xFFu] ^ (c >> 8);
            tables[t][i] = c;
        }
    }
}

static uint32_t load32(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, sizeof v);
    return v;
}

#if defined(HX_CRC32C_X86) || defined(HX_CRC32C_ARM)
static uint64_t load64(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, sizeof v);
    return v;
}
#endif

/* The portable implementation, always compiled and always correct. */
static uint32_t crc_sw(uint32_t crc, const uint8_t *p, size_t n)
{
    crc = ~crc;
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    while (n >= 8) {
        uint32_t lo = load32(p);
        uint32_t hi = load32(p + 4);
        crc ^= lo;
        crc = tables[7][crc & 0xFFu] ^ tables[6][(crc >> 8) & 0xFFu] ^
              tables[5][(crc >> 16) & 0xFFu] ^ tables[4][(crc >> 24) & 0xFFu] ^
              tables[3][hi & 0xFFu] ^ tables[2][(hi >> 8) & 0xFFu] ^ tables[1][(hi >> 16) & 0xFFu] ^
              tables[0][(hi >> 24) & 0xFFu];
        p += 8;
        n -= 8;
    }
#endif
    while (n-- > 0) {
        crc = tables[0][(crc ^ *p++) & 0xFFu] ^ (crc >> 8);
    }
    return ~crc;
}

#if defined(HX_CRC32C_X86)
/*
 * Compiled for SSE4.2 while the rest of the program is not; the caller checks
 * the CPU first. Each step is a single instruction on bytes already in a
 * register, which is why this is the fast path worth the #if.
 */
__attribute__((target("sse4.2"))) static uint32_t crc_x86(uint32_t crc, const uint8_t *p, size_t n)
{
    crc = ~crc;
#if defined(__x86_64__)
    while (n >= 8) {
        crc = (uint32_t)_mm_crc32_u64(crc, load64(p));
        p += 8;
        n -= 8;
    }
#endif
    while (n >= 4) {
        crc = _mm_crc32_u32(crc, load32(p));
        p += 4;
        n -= 4;
    }
    while (n-- > 0) {
        crc = _mm_crc32_u8(crc, *p++);
    }
    return ~crc;
}
#endif

#if defined(HX_CRC32C_ARM)
/*
 * Inline assembly rather than <arm_acle.h>: `.arch_extension crc` inside the
 * asm enables the extension for that instruction only, so this assembles on a
 * toolchain whose baseline -march is plain armv8-a. The HWCAP check in
 * crc32c_select() is what keeps the instruction off CPUs that lack it.
 *
 * The operands name their register class explicitly: the 64-bit step reads an
 * X register, the other two a W register.
 */
static uint32_t crc_arm(uint32_t crc, const uint8_t *p, size_t n)
{
    crc = ~crc;
    while (n >= 8) {
        uint64_t v = load64(p);
        __asm__(".arch_extension crc\n\tcrc32cx %w0, %w0, %x1" : "+r"(crc) : "r"(v));
        p += 8;
        n -= 8;
    }
    while (n >= 4) {
        uint32_t v = load32(p);
        __asm__(".arch_extension crc\n\tcrc32cw %w0, %w0, %w1" : "+r"(crc) : "r"(v));
        p += 4;
        n -= 4;
    }
    while (n-- > 0) {
        uint32_t v = *p++;
        __asm__(".arch_extension crc\n\tcrc32cb %w0, %w0, %w1" : "+r"(crc) : "r"(v));
    }
    return ~crc;
}
#endif

static uint32_t (*crc_best)(uint32_t, const uint8_t *, size_t) = crc_sw;
static bool crc_hw_in_use;

__attribute__((constructor)) static void crc32c_select(void)
{
#if defined(HX_CRC32C_X86)
    __builtin_cpu_init();
    if (__builtin_cpu_supports("sse4.2") != 0) {
        crc_best = crc_x86;
        crc_hw_in_use = true;
    }
#elif defined(HX_CRC32C_ARM)
    if ((getauxval(AT_HWCAP) & HWCAP_CRC32) != 0u) {
        crc_best = crc_arm;
        crc_hw_in_use = true;
    }
#endif
}

uint32_t hx_crc32c(uint32_t crc, const void *data, size_t n)
{
    return crc_best(crc, (const uint8_t *)data, n);
}

uint32_t hx_crc32c_sw(uint32_t crc, const void *data, size_t n)
{
    return crc_sw(crc, (const uint8_t *)data, n);
}

int hx_crc32c_hw(void)
{
    return crc_hw_in_use ? 1 : 0;
}
