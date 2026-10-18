/*
 * Line-oriented key = value parser for `helixd --config`, plus hx_config_load()
 * which picks this parser or the XML one by sniffing the file.
 *
 * The parser is deliberately text-only: no includes, no interpolation beyond
 * ${NAME}, no sections. It is shared by the daemon and unit-tested on its own
 * (tests/test_conf.c), because "the broker read the wrong value" is a class of
 * production incident that unit tests catch cheapest.
 */
#include "util/conf.h"

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/confxml.h"

/* Every failure is reported as "<name>:<line>: <reason>". */
int hx_conf_set_err(char *err, size_t errlen, const char *name, size_t line, const char *reason)
{
    (void)snprintf(err, errlen, "%s:%zu: %s", name, line, reason);
    return -1;
}

/*
 * Replace ${NAME} with the environment. An undefined variable is an error:
 * silently substituting "" is how a broker ends up with an empty data dir.
 */
int hx_conf_expand(const char *name, size_t line_no, const char *in, char *out, size_t outlen,
                   char *err, size_t errlen)
{
    size_t o = 0;
    for (const char *p = in; *p != '\0'; p++) {
        if (p[0] != '$' || p[1] != '{') {
            if (o + 1 >= outlen) {
                return hx_conf_set_err(err, errlen, name, line_no,
                                       "value too long after expansion");
            }
            out[o++] = *p;
            continue;
        }
        const char *start = p + 2;
        const char *stop = start;
        while (*stop != '\0' && *stop != '}') {
            stop++;
        }
        if (*stop != '}') {
            char reason[160];
            (void)snprintf(reason, sizeof reason, "unterminated ${ in '%s'", in);
            return hx_conf_set_err(err, errlen, name, line_no, reason);
        }
        size_t nlen = (size_t)(stop - start);
        if (nlen == 0 || nlen > 127) {
            return hx_conf_set_err(err, errlen, name, line_no, "bad variable name in ${...}");
        }
        if (!isalpha((unsigned char)start[0]) && start[0] != '_') {
            return hx_conf_set_err(err, errlen, name, line_no, "bad variable name in ${...}");
        }
        for (size_t i = 1; i < nlen; i++) {
            unsigned char c = (unsigned char)start[i];
            if (!isalnum(c) && c != '_') {
                return hx_conf_set_err(err, errlen, name, line_no, "bad variable name in ${...}");
            }
        }
        char vname[128];
        memcpy(vname, start, nlen);
        vname[nlen] = '\0';
        const char *value = getenv(vname);
        if (value == NULL) {
            char reason[192];
            (void)snprintf(reason, sizeof reason, "undefined variable '%s'", vname);
            return hx_conf_set_err(err, errlen, name, line_no, reason);
        }
        size_t vlen = strlen(value);
        if (o + vlen + 1 > outlen) {
            return hx_conf_set_err(err, errlen, name, line_no, "value too long after expansion");
        }
        memcpy(out + o, value, vlen);
        o += vlen;
        p = stop; /* the loop's p++ steps over the closing brace */
    }
    out[o] = '\0';
    return 0;
}

/*
 * Validate the key, split it from the value, drop the comment and the quotes,
 * expand ${NAME}, then hand the pair to the caller.
 */
static int handle_line(const char *name, size_t line_no, char *line, hx_conf_fn fn, void *ctx,
                       char *err, size_t errlen)
{
    char *s = line;
    if (line_no == 1 && strncmp(s, "\xEF\xBB\xBF", 3) == 0) {
        s += 3; /* editors like adding a UTF-8 BOM */
    }
    while (isspace((unsigned char)*s)) {
        s++;
    }
    /* Trim the end of the line here so a file's trailing newline cannot change
     * the meaning of a line: "broken" and "broken\n" must report the same error. */
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) {
        end--;
    }
    *end = '\0';
    if (*s == '\0' || *s == '#') {
        return 0;
    }

    char *sep = s;
    while (*sep != '\0' && *sep != '=' && !isspace((unsigned char)*sep)) {
        sep++;
    }
    size_t klen = (size_t)(sep - s);
    if (klen == 0) {
        return hx_conf_set_err(err, errlen, name, line_no, "missing key");
    }
    if (klen > HX_CONF_MAX_KEY) {
        char reason[64];
        (void)snprintf(reason, sizeof reason, "key longer than %d bytes", HX_CONF_MAX_KEY);
        return hx_conf_set_err(err, errlen, name, line_no, reason);
    }
    char key[HX_CONF_MAX_KEY + 1];
    memcpy(key, s, klen);
    key[klen] = '\0';
    for (size_t i = 0; i < klen; i++) {
        unsigned char c = (unsigned char)key[i];
        if (!isalnum(c) && c != '-' && c != '_' && c != '.') {
            char reason[HX_CONF_MAX_KEY + 32];
            (void)snprintf(reason, sizeof reason, "invalid key '%s'", key);
            return hx_conf_set_err(err, errlen, name, line_no, reason);
        }
    }
    if (*sep == '\0') {
        char reason[HX_CONF_MAX_KEY + 32];
        (void)snprintf(reason, sizeof reason, "missing value for '%s'", key);
        return hx_conf_set_err(err, errlen, name, line_no, reason);
    }

    char *v = sep;
    if (*v != '=') {
        while (isspace((unsigned char)*v)) {
            v++;
        }
    }
    if (*v == '=') {
        v++;
    }
    while (isspace((unsigned char)*v)) {
        v++;
    }

    /* Strip an inline comment, but only outside quotes and after whitespace. */
    bool quoted = false;
    char quote = '\0';
    for (char *q = v; *q != '\0'; q++) {
        if (quoted) {
            if (*q == quote) {
                quoted = false;
            }
        } else if (*q == '"' || *q == '\'') {
            quoted = true;
            quote = *q;
        } else if (*q == '#' && (q == v || isspace((unsigned char)q[-1]))) {
            *q = '\0';
            break;
        }
    }
    char *stop = v + strlen(v);
    while (stop > v && isspace((unsigned char)stop[-1])) {
        stop--;
    }
    *stop = '\0';
    if (*v == '\0') {
        char reason[HX_CONF_MAX_KEY + 32];
        (void)snprintf(reason, sizeof reason, "empty value for '%s'", key);
        return hx_conf_set_err(err, errlen, name, line_no, reason);
    }

    size_t vlen = strlen(v);
    if (v[0] == '"' || v[0] == '\'') {
        quote = v[0];
        if (vlen < 2 || v[vlen - 1] != quote) {
            char reason[HX_CONF_MAX_KEY + 40];
            (void)snprintf(reason, sizeof reason, "unterminated quote for '%s'", key);
            return hx_conf_set_err(err, errlen, name, line_no, reason);
        }
        memmove(v, v + 1, vlen - 2);
        v[vlen - 2] = '\0';
    }

    char value[HX_CONF_MAX_LINE];
    if (hx_conf_expand(name, line_no, v, value, sizeof value, err, errlen) != 0) {
        return -1;
    }
    /* The callback writes the reason; the parser owns the "name:line:" prefix
     * so both formats report the setting the same way. */
    char reason[HX_CONF_MAX_LINE];
    reason[0] = '\0';
    if (fn(ctx, key, value, line_no, reason, sizeof reason) != 0) {
        return hx_conf_set_err(err, errlen, name, line_no, reason);
    }
    return 0;
}

int hx_conf_parse(const char *name, const char *text, hx_conf_fn fn, void *ctx, char *err,
                  size_t errlen)
{
    char line[HX_CONF_MAX_LINE];
    size_t len = 0;
    size_t line_no = 0;
    const char *p = text;
    for (;;) {
        char c = *p;
        if (c != '\n' && c != '\0') {
            if (len + 1 >= sizeof line) {
                char reason[64];
                (void)snprintf(reason, sizeof reason, "line longer than %d bytes",
                               HX_CONF_MAX_LINE - 1);
                return hx_conf_set_err(err, errlen, name, line_no + 1, reason);
            }
            line[len++] = c;
            p++;
            continue;
        }
        if (len > 0 || c == '\n') {
            line[len] = '\0';
            line_no++;
            if (handle_line(name, line_no, line, fn, ctx, err, errlen) != 0) {
                return -1;
            }
            len = 0;
        }
        if (c == '\0') {
            return 0;
        }
        p++;
    }
}

int hx_conf_load(const char *path, hx_conf_fn fn, void *ctx, char *err, size_t errlen)
{
    FILE *f = fopen(path, "rb");
    if (f == NULL) {
        (void)snprintf(err, errlen, "%s: %s", path, strerror(errno));
        return -1;
    }
    char line[HX_CONF_MAX_LINE];
    size_t line_no = 0;
    int rc = 0;
    while (fgets(line, (int)sizeof line, f) != NULL) {
        line_no++;
        size_t len = strlen(line);
        if (len == 0 || line[len - 1] != '\n') {
            /* No newline: either the last line or a line that filled the buffer. */
            int c = fgetc(f);
            if (c != EOF && c != '\n') {
                char reason[64];
                (void)snprintf(reason, sizeof reason, "line longer than %d bytes",
                               HX_CONF_MAX_LINE - 1);
                rc = hx_conf_set_err(err, errlen, path, line_no, reason);
                break;
            }
        }
        if (handle_line(path, line_no, line, fn, ctx, err, errlen) != 0) {
            rc = -1;
            break;
        }
    }
    if (rc == 0 && ferror(f) != 0) {
        (void)snprintf(err, errlen, "%s: read error", path);
        rc = -1;
    }
    (void)fclose(f);
    return rc;
}

/*
 * Sniff the format and hand the file to the right parser. A UTF-8 BOM and
 * leading whitespace/comments are skipped; '<' means XML. The file is read
 * twice at most, which costs nothing next to the daemon's startup.
 */
int hx_config_load(const char *path, hx_conf_fn fn, void *ctx, char *err, size_t errlen)
{
    FILE *f = fopen(path, "rb");
    if (f == NULL) {
        (void)snprintf(err, errlen, "%s: %s", path, strerror(errno));
        return -1;
    }
    int c = fgetc(f);
    if (c == 0xEF) {
        /* Either a UTF-8 BOM or (in a latin-1 file) some other byte; only the
         * BOM is three bytes long, and only '<' matters for the choice. */
        int c2 = fgetc(f);
        int c3 = fgetc(f);
        c = (c2 == 0xBB && c3 == 0xBF) ? fgetc(f) : c2;
    }
    while (c != EOF && isspace(c)) {
        c = fgetc(f);
    }
    (void)fclose(f);
    if (c == '<') {
        return hx_confxml_load(path, fn, ctx, err, errlen);
    }
    return hx_conf_load(path, fn, ctx, err, errlen);
}
