/*
 * Minimal leveled logger. Writes logfmt-style lines to stderr:
 *   ts=2026-10-13T09:00:00.123Z level=info msg="..."
 * Not thread-safe configuration: set the level once at startup.
 */
#ifndef HELIX_LOGGER_H
#define HELIX_LOGGER_H

typedef enum { HXL_DEBUG = 0, HXL_INFO = 1, HXL_WARN = 2, HXL_ERROR = 3 } hxl_level;

void hxl_set_level(hxl_level lvl);
hxl_level hxl_get_level(void);

/* Parses "debug|info|warn|error". Returns 0 on success, -1 otherwise. */
int hxl_parse_level(const char *name, hxl_level *out);

void hxl_log(hxl_level lvl, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

#define HX_DEBUG(...) hxl_log(HXL_DEBUG, __VA_ARGS__)
#define HX_INFO(...)  hxl_log(HXL_INFO, __VA_ARGS__)
#define HX_WARN(...)  hxl_log(HXL_WARN, __VA_ARGS__)
#define HX_ERROR(...) hxl_log(HXL_ERROR, __VA_ARGS__)

#endif /* HELIX_LOGGER_H */
