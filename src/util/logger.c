#include "logger.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define HXL_LINE_MAX 1024

static hxl_level g_level = HXL_INFO;

static const char *const k_names[] = {"debug", "info", "warn", "error"};

void hxl_set_level(hxl_level lvl)
{
    g_level = lvl;
}

hxl_level hxl_get_level(void)
{
    return g_level;
}

int hxl_parse_level(const char *name, hxl_level *out)
{
    if (name == NULL || out == NULL) {
        return -1;
    }
    for (int i = 0; i < 4; i++) {
        if (strcmp(name, k_names[i]) == 0) {
            *out = (hxl_level)i;
            return 0;
        }
    }
    return -1;
}

void hxl_log(hxl_level lvl, const char *fmt, ...)
{
    if (lvl < g_level) {
        return;
    }

    struct timespec now;
    struct tm tm_utc;
    char ts[64] = "unknown";
    if (clock_gettime(CLOCK_REALTIME, &now) == 0 && gmtime_r(&now.tv_sec, &tm_utc) != NULL) {
        char date[32];
        if (strftime(date, sizeof date, "%Y-%m-%dT%H:%M:%S", &tm_utc) > 0) {
            (void)snprintf(ts, sizeof ts, "%s.%03dZ", date, (int)(now.tv_nsec / 1000000L));
        }
    }

    char msg[HXL_LINE_MAX];
    va_list ap;
    va_start(ap, fmt);
    // va_start() on the previous line initialises `ap`; clang-tidy 18 mis-reports it as
    // uninitialised. NOLINTNEXTLINE(clang-analyzer-valist.Uninitialized)
    const int written = vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    if (written < 0) {
        msg[0] = '\0';
    }

    /* One fprintf call per line keeps lines intact when several writers share stderr. */
    (void)fprintf(stderr, "ts=%s level=%s msg=\"%s\"\n", ts, k_names[lvl], msg);
}
