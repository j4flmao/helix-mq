/*
 * Configuration files for `helixd --config`.
 *
 * Two formats, chosen by the first non-space byte of the file: '<' selects the
 * XML subset in util/confxml.h, anything else the flat "key = value" format
 * documented here. Both share this one callback, so the option table lives with
 * the options whatever the file looks like.
 *
 * Flat grammar: one setting per line, `key = value` (the spaces and the '=' are
 * optional), '#' starts a comment at the beginning of a line or after
 * whitespace inside a value, a value may be quoted with ' or ", and ${NAME}
 * is substituted from the environment. Keys are [A-Za-z0-9._-].
 *
 * The parser only splits lines and performs substitution: it never interprets
 * a key. Every key is handed to an hx_conf_fn that decides whether it exists
 * and validates its value, so the option table lives with the options.
 */
#ifndef HELIX_CONF_H
#define HELIX_CONF_H

#include <stddef.h>

#define HX_CONF_MAX_LINE 4096 /* longest line or value, newline included  */
#define HX_CONF_MAX_KEY  64   /* longest key, excluding the terminator     */

/*
 * Called once per setting. Returns 0 to accept it, a negative value to reject
 * it; on rejection it must write the *reason* into err, without a "name:line:"
 * prefix -- the parser adds the name and line of the setting, so a message
 * about a setting inside an <include> names the included file. The key/value
 * pointers are only valid for the call.
 */
typedef int (*hx_conf_fn)(void *ctx, const char *key, const char *val, size_t line, char *err,
                          size_t errlen);

/*
 * Parse an in-memory configuration. `name` labels error messages only.
 * Returns 0 when every setting was accepted, -1 otherwise (err is filled).
 * An empty document is valid.
 */
int hx_conf_parse(const char *name, const char *text, hx_conf_fn fn, void *ctx, char *err,
                  size_t errlen);

/*
 * Read `path` and parse it. A missing or unreadable file is an error, as is
 * a line longer than HX_CONF_MAX_LINE - 1 bytes.
 */
int hx_conf_load(const char *path, hx_conf_fn fn, void *ctx, char *err, size_t errlen);

/*
 * Read `path` and parse it in whichever format it is written in. Used by the
 * daemon so a deployment can keep one .conf for the simple environments and
 * move to XML without changing the command line.
 */
int hx_config_load(const char *path, hx_conf_fn fn, void *ctx, char *err, size_t errlen);

/*
 * Format "<name>:<line>: <reason>" into err and return -1. Shared with the XML
 * parser so one error message looks the same whoever rejected the setting.
 */
int hx_conf_set_err(char *err, size_t errlen, const char *name, size_t line, const char *reason);

/*
 * Substitute ${NAME} in `in` from the environment. An undefined variable is an
 * error: silently substituting "" is how a broker ends up with an empty data
 * dir. `name`/`line` only label the error.
 */
int hx_conf_expand(const char *name, size_t line, const char *in, char *out, size_t outlen,
                   char *err, size_t errlen);

#endif /* HELIX_CONF_H */
