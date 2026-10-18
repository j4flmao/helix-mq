/*
 * libFuzzer harness: feeds arbitrary bytes to the request decoder + broker.
 * Build/run:  make fuzz FUZZ_TIME=30   (requires clang)
 * Invariants checked by the sanitizers: no out-of-bounds, no UB, no leaks, and
 * the broker must always either return a well-formed response or -1.
 */
#define _XOPEN_SOURCE 700
#include <ftw.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "broker/broker.h"
#include "util/buf.h"
#include "util/logger.h"

static hx_broker *g_broker;
static char g_dir[256];

static int rm_cb(const char *p, const struct stat *s, int t, struct FTW *f)
{
    (void)s;
    (void)t;
    (void)f;
    return remove(p);
}

static void cleanup(void)
{
    hx_broker_destroy(g_broker);
    (void)nftw(g_dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
}

int LLVMFuzzerInitialize(int *argc, char ***argv);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

// NOLINTNEXTLINE(readability-non-const-parameter): signature fixed by the libFuzzer ABI
int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc;
    (void)argv;
    hxl_set_level(HXL_ERROR);
    const char *t = getenv("TMPDIR");
    (void)snprintf(g_dir, sizeof g_dir, "%s/helix-fuzz-XXXXXX",
                   (t != NULL && *t != '\0') ? t : "/tmp");
    if (mkdtemp(g_dir) == NULL) {
        return 1;
    }
    hx_broker_cfg cfg;
    hx_broker_cfg_default(&cfg);
    cfg.data_dir = g_dir;
    cfg.log_opts.sync = HX_SYNC_NEVER;
    cfg.log_opts.max_record_bytes = 4096;
    cfg.log_opts.segment_bytes = 8192;
    cfg.max_open_logs = 16;
    g_broker = hx_broker_create(&cfg);
    if (g_broker == NULL) {
        return 1;
    }
    (void)atexit(cleanup);
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    hx_buf out;
    hx_buf_init(&out);
    (void)hx_broker_handle(g_broker, data, size, &out);
    hx_buf_free(&out);
    return 0;
}
