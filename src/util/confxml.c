/*
 * XML configuration files: a hand-written parser for the subset documented in
 * util/confxml.h.
 *
 * Recursive descent over an in-memory document: no allocation while parsing,
 * no external dependency, and errors shaped exactly like the flat parser's
 * ("file:line: reason") because both are reported through the same callback.
 * Every rule here exists to make a wrong configuration file fail at startup
 * with the offending line instead of quietly changing a default.
 */
#include "util/confxml.h"

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_ATTRS     32
#define MAX_DOC_BYTES (1u << 20) /* a config file larger than 1 MiB is a mistake */
#define MAX_PATH      4096
#define MAX_MSG       512

typedef struct {
    char name[HX_CONFXML_MAX_NAME + 1];
    char value[HX_CONFXML_MAX_VALUE];
    size_t line;
} xml_attr;

typedef struct xml_parser xml_parser;

struct xml_parser {
    const char *name; /* path, labels every error */
    const char *p;    /* cursor */
    const char *end;
    size_t line;
    hx_conf_fn fn;
    void *ctx;
    char *err;
    size_t errlen;
    /* Non-NULL when parsing a file: <include> is resolved through this hook. */
    int (*include)(void *ictx, const char *file, size_t line, char *err, size_t errlen);
    void *ictx;
};

/* ---------------------------------------------------------------- cursor */

static void xadv(xml_parser *x, size_t n)
{
    while (n-- > 0 && x->p < x->end) {
        if (*x->p == '\n') {
            x->line++;
        }
        x->p++;
    }
}

static bool xrest(const xml_parser *x, char c)
{
    return x->p < x->end && *x->p == c;
}

static bool xstarts(const xml_parser *x, const char *lit)
{
    size_t n = strlen(lit);
    return (size_t)(x->end - x->p) >= n && memcmp(x->p, lit, n) == 0;
}

static int xerr(xml_parser *x, const char *reason)
{
    return hx_conf_set_err(x->err, x->errlen, x->name, x->line, reason);
}

static int xerrf(xml_parser *x, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

static int xerrf(xml_parser *x, const char *fmt, ...)
{
    char reason[MAX_MSG];
    va_list ap;
    va_start(ap, fmt);
    // va_start() on the previous line initialises `ap`; clang-tidy 18 mis-reports it as
    // uninitialised. NOLINTNEXTLINE(clang-analyzer-valist.Uninitialized)
    (void)vsnprintf(reason, sizeof reason, fmt, ap);
    va_end(ap);
    return hx_conf_set_err(x->err, x->errlen, x->name, x->line, reason);
}

/* ------------------------------------------------------- insignificant bits */

static int skip_comment(xml_parser *x)
{
    xadv(x, 4); /* "<!--" */
    while (!xstarts(x, "-->")) {
        if (x->p >= x->end) {
            return xerr(x, "unterminated comment");
        }
        xadv(x, 1);
    }
    xadv(x, 3);
    return 0;
}

static int skip_pi(xml_parser *x)
{
    xadv(x, 2); /* "<?" */
    while (!xstarts(x, "?>")) {
        if (x->p >= x->end) {
            return xerr(x, "unterminated processing instruction");
        }
        xadv(x, 1);
    }
    xadv(x, 2);
    return 0;
}

/*
 * Skip whitespace, comments and (outside element content) processing
 * instructions. Inside content whitespace is data, so only comments and PIs
 * are handled there -- and a PI between elements of a group is refused.
 */
static int skip_misc(xml_parser *x, bool in_content)
{
    for (;;) {
        while (x->p < x->end && isspace((unsigned char)*x->p)) {
            xadv(x, 1);
        }
        if (xstarts(x, "<!--")) {
            if (skip_comment(x) != 0) {
                return -1;
            }
            continue;
        }
        if (xstarts(x, "<?")) {
            if (in_content) {
                return xerr(x, "a processing instruction is not allowed inside an element");
            }
            if (skip_pi(x) != 0) {
                return -1;
            }
            continue;
        }
        return 0;
    }
}

/* --------------------------------------------------------------- scanning */

/* [A-Za-z_][A-Za-z0-9._-]*; ':' is refused by the caller as a namespace. */
static int read_name(xml_parser *x, char *out, size_t cap)
{
    if (x->p >= x->end) {
        return xerr(x, "expected a name");
    }
    unsigned char c = (unsigned char)*x->p;
    if (!isalpha(c) && c != '_') {
        return xerrf(x, "expected a name, found '%c'", (char)c);
    }
    size_t n = 0;
    while (x->p < x->end) {
        c = (unsigned char)*x->p;
        if (!isalnum(c) && c != '_' && c != '-' && c != '.') {
            break;
        }
        if (n + 1 >= cap) {
            return xerrf(x, "name longer than %zu bytes", cap - 1);
        }
        out[n++] = (char)c;
        xadv(x, 1);
    }
    out[n] = '\0';
    return 0;
}

/* Read the element name after '<' without consuming it (for the root check). */
static int peek_name(xml_parser *x, char *out, size_t cap)
{
    const char *p = x->p;
    if (p >= x->end || *p != '<') {
        return -1;
    }
    p++;
    size_t n = 0;
    while (p < x->end) {
        unsigned char c = (unsigned char)*p;
        if (!isalnum(c) && c != '_' && c != '-' && c != '.') {
            break;
        }
        if (n + 1 >= cap) {
            return -1;
        }
        out[n++] = (char)c;
        p++;
    }
    out[n] = '\0';
    if (n == 0) {
        return -1;
    }
    unsigned char first = (unsigned char)out[0];
    return (isalpha(first) || first == '_') ? 0 : -1;
}

static int read_quoted(xml_parser *x, char *out, size_t cap)
{
    if (!xrest(x, '"') && !xrest(x, '\'')) {
        return xerr(x, "attribute value must be quoted with \" or '");
    }
    char quote = *x->p;
    xadv(x, 1);
    size_t n = 0;
    while (x->p < x->end && *x->p != quote) {
        if (n + 1 >= cap) {
            return xerrf(x, "value longer than %zu bytes", cap - 1);
        }
        out[n++] = *x->p;
        xadv(x, 1);
    }
    if (x->p >= x->end) {
        return xerr(x, "unterminated attribute value");
    }
    xadv(x, 1);
    out[n] = '\0';
    return 0;
}

/* ---------------------------------------------------------------- values */

static bool only_space(const char *s)
{
    for (; *s != '\0'; s++) {
        if (!isspace((unsigned char)*s)) {
            return false;
        }
    }
    return true;
}

static char *trim(char *s)
{
    while (*s != '\0' && isspace((unsigned char)*s)) {
        s++;
    }
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) {
        end--;
    }
    *end = '\0';
    return s;
}

static int put_utf8(char *out, size_t cap, size_t *o, unsigned long cp)
{
    if (*o + 4 >= cap) {
        return -1;
    }
    if (cp < 0x80UL) {
        out[(*o)++] = (char)cp;
    } else if (cp < 0x800UL) {
        out[(*o)++] = (char)(0xC0U | (cp >> 6));
        out[(*o)++] = (char)(0x80U | (cp & 0x3FU));
    } else if (cp < 0x10000UL) {
        out[(*o)++] = (char)(0xE0U | (cp >> 12));
        out[(*o)++] = (char)(0x80U | ((cp >> 6) & 0x3FU));
        out[(*o)++] = (char)(0x80U | (cp & 0x3FU));
    } else {
        out[(*o)++] = (char)(0xF0U | (cp >> 18));
        out[(*o)++] = (char)(0x80U | ((cp >> 12) & 0x3FU));
        out[(*o)++] = (char)(0x80U | ((cp >> 6) & 0x3FU));
        out[(*o)++] = (char)(0x80U | (cp & 0x3FU));
    }
    return 0;
}

/*
 * Decode the five XML entities plus decimal and hexadecimal character
 * references. A bare '&' is an error: guessing is how "&amp;" in a password
 * becomes "&" and nobody notices.
 */
static int decode_entities(xml_parser *x, const char *in, char *out, size_t cap)
{
    size_t o = 0;
    for (const char *p = in; *p != '\0'; p++) {
        if (*p != '&') {
            if (o + 2 > cap) {
                return xerrf(x, "value longer than %zu bytes", cap - 1);
            }
            out[o++] = *p;
            continue;
        }
        const char *stop = strchr(p, ';');
        if (stop == NULL) {
            return xerr(x, "bare '&' must be written &amp;");
        }
        size_t n = (size_t)(stop - p) - 1;
        char ent[16];
        if (n == 0 || n >= sizeof ent) {
            return xerr(x, "unknown entity in value");
        }
        memcpy(ent, p + 1, n);
        ent[n] = '\0';

        unsigned long cp = 0;
        bool ok = true;
        if (strcmp(ent, "amp") == 0) {
            cp = '&';
        } else if (strcmp(ent, "lt") == 0) {
            cp = '<';
        } else if (strcmp(ent, "gt") == 0) {
            cp = '>';
        } else if (strcmp(ent, "quot") == 0) {
            cp = '"';
        } else if (strcmp(ent, "apos") == 0) {
            cp = '\'';
        } else if (ent[0] == '#') {
            const bool hex = ent[1] == 'x' || ent[1] == 'X';
            char *stopc = NULL;
            errno = 0;
            cp = strtoul(ent + (hex ? 2 : 1), &stopc, hex ? 16 : 10);
            ok = errno == 0 && stopc != NULL && *stopc == '\0' && cp > 0 && cp <= 0x10FFFFUL;
        } else {
            ok = false;
        }
        if (!ok) {
            char reason[64];
            (void)snprintf(reason, sizeof reason, "unknown entity '&%s;'", ent);
            return xerr(x, reason);
        }
        if (put_utf8(out, cap, &o, cp) != 0) {
            return xerrf(x, "value longer than %zu bytes", cap - 1);
        }
        p = stop; /* the for-loop's p++ steps over the ';' */
    }
    out[o] = '\0';
    return 0;
}

/* Entity-decode then ${NAME}-expand one value. */
static int decode_value(xml_parser *x, const char *raw, size_t line, char *out, size_t cap)
{
    char decoded[HX_CONFXML_MAX_VALUE];
    x->line = line;
    if (decode_entities(x, raw, decoded, sizeof decoded) != 0) {
        return -1;
    }
    return hx_conf_expand(x->name, line, decoded, out, cap, x->err, x->errlen);
}

/* ---------------------------------------------------------------- elements */

/*
 * Group names are vocabulary the parser owns: they are structure, never
 * settings. They must not collide with option names (apply_option in
 * src/helixd.c), which tests/test_confxml.c pins down.
 */
static const char *const k_groups[] = {
    "helix",  "broker",  "server",   "network",         "io",       "storage",   "log",
    "limits", "cluster", "consumer", "consumer-groups", "producer", "producers", NULL};

static bool is_group(const char *name)
{
    for (size_t i = 0; k_groups[i] != NULL; i++) {
        if (strcmp(k_groups[i], name) == 0) {
            return true;
        }
    }
    return false;
}

static int apply_option(xml_parser *x, const char *key, const char *raw, size_t line)
{
    char value[HX_CONFXML_MAX_VALUE];
    if (decode_value(x, raw, line, value, sizeof value) != 0) {
        return -1;
    }
    /* The callback writes the reason; the parser prefixes the file it is
     * parsing, so a failure inside an <include> names the included file. */
    char reason[MAX_MSG];
    reason[0] = '\0';
    x->line = line;
    if (x->fn(x->ctx, key, value, line, reason, sizeof reason) == 0) {
        return 0;
    }
    return hx_conf_set_err(x->err, x->errlen, x->name, line, reason);
}

static int apply_element(xml_parser *x, const char *name, const xml_attr *attrs, size_t nattrs,
                         char *text, bool has_child, size_t line)
{
    const bool has_text = !only_space(text);
    if (has_text && has_child) {
        return xerrf(x, "mixed content in <%s>: text next to elements", name);
    }

    if (strcmp(name, "include") == 0) {
        if (has_child || has_text) {
            return xerr(x, "<include> must be empty, e.g. <include file=\"base.xml\"/>");
        }
        if (x->include == NULL) {
            return xerr(x, "<include> is only allowed when loading a file");
        }
        if (nattrs != 1 || strcmp(attrs[0].name, "file") != 0) {
            return xerr(x, "<include> needs exactly one file=\"...\" attribute");
        }
        char value[HX_CONFXML_MAX_VALUE];
        if (decode_value(x, attrs[0].value, attrs[0].line, value, sizeof value) != 0) {
            return -1;
        }
        return x->include(x->ictx, value, line, x->err, x->errlen);
    }

    if (is_group(name)) {
        if (has_text) {
            return xerrf(x, "group <%s> must not contain text", name);
        }
        return 0;
    }

    /* An option: its text is the value, and nothing else is allowed. */
    if (nattrs > 0) {
        return xerrf(x, "option <%s> must not have attributes", name);
    }
    if (has_child) {
        return xerrf(x, "option <%s> must not contain elements", name);
    }
    if (!has_text) {
        return xerrf(x, "option <%s> needs a value, e.g. <%s>9400</%s>", name, name, name);
    }
    return apply_option(x, name, trim(text), line);
}

static int parse_element(xml_parser *x, unsigned depth)
{
    if (depth > HX_CONFXML_MAX_DEPTH) {
        return xerr(x, "elements nested too deeply");
    }
    const size_t elem_line = x->line;
    if (!xrest(x, '<')) {
        return xerr(x, "expected '<'");
    }
    xadv(x, 1);

    char name[HX_CONFXML_MAX_NAME + 1];
    if (read_name(x, name, sizeof name) != 0) {
        return -1;
    }
    if (xrest(x, ':')) {
        return xerr(x, "namespaces are not supported");
    }

    xml_attr attrs[MAX_ATTRS];
    size_t nattrs = 0;
    bool self_close = false;
    for (;;) {
        if (skip_misc(x, false) != 0) {
            return -1;
        }
        if (xrest(x, '>')) {
            xadv(x, 1);
            break;
        }
        if (xstarts(x, "/>")) {
            xadv(x, 2);
            self_close = true;
            break;
        }
        if (x->p >= x->end) {
            return xerrf(x, "unclosed element <%s>", name);
        }
        if (nattrs >= MAX_ATTRS) {
            return xerrf(x, "<%s> has more than %d attributes", name, MAX_ATTRS);
        }
        xml_attr *a = &attrs[nattrs];
        a->line = x->line;
        if (read_name(x, a->name, sizeof a->name) != 0) {
            return -1;
        }
        if (skip_misc(x, false) != 0) {
            return -1;
        }
        if (!xrest(x, '=')) {
            return xerrf(x, "attribute '%s' needs =\"value\"", a->name);
        }
        xadv(x, 1);
        if (skip_misc(x, false) != 0) {
            return -1;
        }
        if (read_quoted(x, a->value, sizeof a->value) != 0) {
            return -1;
        }
        nattrs++;
    }

    /* A group's attributes are settings; apply them where the start tag
     * stands, so the file is applied in document order (a later duplicate
     * overrides an earlier one, exactly like the flat format). */
    if (is_group(name)) {
        for (size_t i = 0; i < nattrs; i++) {
            if (apply_option(x, attrs[i].name, attrs[i].value, attrs[i].line) != 0) {
                return -1;
            }
        }
    }

    char text[HX_CONFXML_MAX_VALUE];
    size_t tlen = 0;
    bool has_child = false;
    if (!self_close) {
        for (;;) {
            if (x->p >= x->end) {
                return xerrf(x, "unclosed element <%s>", name);
            }
            if (xstarts(x, "<!--")) {
                if (skip_comment(x) != 0) {
                    return -1;
                }
                continue;
            }
            if (xstarts(x, "<?")) {
                return xerr(x, "a processing instruction is not allowed inside an element");
            }
            if (xstarts(x, "<![CDATA[")) {
                xadv(x, 9);
                for (;;) {
                    if (x->p >= x->end) {
                        return xerrf(x, "unterminated CDATA section in <%s>", name);
                    }
                    if (xstarts(x, "]]>")) {
                        xadv(x, 3);
                        break;
                    }
                    /* CDATA is literal: escape '&' so the decode pass below
                     * leaves it alone instead of reading an entity. */
                    if (*x->p == '&') {
                        if (tlen + 5 + 1 >= sizeof text) {
                            return xerrf(x, "text in <%s> longer than %d bytes", name,
                                         HX_CONFXML_MAX_VALUE - 1);
                        }
                        memcpy(text + tlen, "&amp;", 5);
                        tlen += 5;
                    } else {
                        if (tlen + 1 + 1 >= sizeof text) {
                            return xerrf(x, "text in <%s> longer than %d bytes", name,
                                         HX_CONFXML_MAX_VALUE - 1);
                        }
                        text[tlen++] = *x->p;
                    }
                    xadv(x, 1);
                }
                continue;
            }
            if (xstarts(x, "</")) {
                xadv(x, 2);
                char close[HX_CONFXML_MAX_NAME + 1];
                if (read_name(x, close, sizeof close) != 0) {
                    return -1;
                }
                if (strcmp(close, name) != 0) {
                    return xerrf(x, "</%s> closes <%s>", close, name);
                }
                if (skip_misc(x, false) != 0) {
                    return -1;
                }
                if (!xrest(x, '>')) {
                    return xerr(x, "expected '>' at the end of the closing tag");
                }
                xadv(x, 1);
                break;
            }
            if (xrest(x, '<')) {
                has_child = true;
                if (parse_element(x, depth + 1) != 0) {
                    return -1;
                }
                continue;
            }
            if (tlen + 1 + 1 >= sizeof text) {
                return xerrf(x, "text in <%s> longer than %d bytes", name,
                             HX_CONFXML_MAX_VALUE - 1);
            }
            text[tlen++] = *x->p;
            xadv(x, 1);
        }
    }
    text[tlen] = '\0';
    return apply_element(x, name, attrs, nattrs, text, has_child, elem_line);
}

static int parse_document(xml_parser *x)
{
    if (x->end - x->p >= 3 && memcmp(x->p, "\xEF\xBB\xBF", 3) == 0) {
        xadv(x, 3); /* editors like adding a UTF-8 BOM */
    }
    if (skip_misc(x, false) != 0) {
        return -1;
    }
    if (x->p >= x->end) {
        return 0; /* an empty document configures nothing, like an empty .conf */
    }
    if (xstarts(x, "<!")) {
        return xerr(x, "DOCTYPE, ENTITY and other <! declarations are not supported");
    }
    if (!xrest(x, '<')) {
        return xerr(x, "expected an element");
    }
    char root[HX_CONFXML_MAX_NAME + 1];
    if (peek_name(x, root, sizeof root) != 0) {
        return xerr(x, "expected an element name");
    }
    if (strcmp(root, "helix") != 0) {
        return xerrf(x, "the root element must be <helix>, not <%s>", root);
    }
    if (parse_element(x, 0) != 0) {
        return -1;
    }
    if (skip_misc(x, false) != 0) {
        return -1;
    }
    if (x->p < x->end) {
        return xerr(x, "unexpected content after the root element");
    }
    return 0;
}

int hx_confxml_parse(const char *name, const char *text, hx_conf_fn fn, void *ctx, char *err,
                     size_t errlen)
{
    xml_parser x = {.name = name,
                    .p = text,
                    .end = text + strlen(text),
                    .line = 1,
                    .fn = fn,
                    .ctx = ctx,
                    .err = err,
                    .errlen = errlen,
                    .include = NULL,
                    .ictx = NULL};
    return parse_document(&x);
}

/* ----------------------------------------------------------------- files */

typedef struct {
    hx_conf_fn fn;
    void *ctx;
    char paths[HX_CONFXML_MAX_DEPTH][MAX_PATH]; /* include stack, for cycles */
    unsigned npaths;
} loader;

static char *read_file(const char *path, size_t *out_len, char *err, size_t errlen)
{
    FILE *f = fopen(path, "rb");
    if (f == NULL) {
        (void)snprintf(err, errlen, "%s: %s", path, strerror(errno));
        return NULL;
    }
    size_t cap = 8192;
    size_t len = 0;
    char *buf = malloc(cap);
    if (buf == NULL) {
        (void)snprintf(err, errlen, "%s: out of memory", path);
        (void)fclose(f);
        return NULL;
    }
    for (;;) {
        if (len + 1 >= cap) {
            if (cap >= MAX_DOC_BYTES) {
                (void)snprintf(err, errlen, "%s: larger than %u bytes", path, MAX_DOC_BYTES);
                free(buf);
                (void)fclose(f);
                return NULL;
            }
            cap *= 2;
            char *grown = realloc(buf, cap);
            if (grown == NULL) {
                (void)snprintf(err, errlen, "%s: out of memory", path);
                free(buf);
                (void)fclose(f);
                return NULL;
            }
            buf = grown;
        }
        size_t got = fread(buf + len, 1, cap - len - 1, f);
        len += got;
        if (got == 0) {
            break;
        }
    }
    if (ferror(f) != 0) {
        (void)snprintf(err, errlen, "%s: read error", path);
        free(buf);
        (void)fclose(f);
        return NULL;
    }
    (void)fclose(f);
    buf[len] = '\0';
    *out_len = len;
    return buf;
}

static int loader_include(void *ictx, const char *file, size_t line, char *err, size_t errlen);

static int load_file(loader *lc, const char *path, unsigned depth, char *err, size_t errlen)
{
    if (depth >= HX_CONFXML_MAX_DEPTH) {
        (void)snprintf(err, errlen, "%s: includes nested deeper than %d", path,
                       HX_CONFXML_MAX_DEPTH);
        return -1;
    }
    /* Resolving first gives canonical paths (so symlinks cannot hide a cycle)
     * and a proper message for a missing file. */
    char resolved[MAX_PATH];
    if (realpath(path, resolved) == NULL) {
        (void)snprintf(err, errlen, "%s: %s", path, strerror(errno));
        return -1;
    }
    for (unsigned i = 0; i < lc->npaths; i++) {
        if (strcmp(lc->paths[i], resolved) == 0) {
            (void)snprintf(err, errlen, "%s: include cycle", path);
            return -1;
        }
    }
    if (strlen(resolved) >= sizeof lc->paths[0]) {
        (void)snprintf(err, errlen, "%s: path too long", path);
        return -1;
    }

    size_t len = 0;
    char *text = read_file(resolved, &len, err, errlen);
    if (text == NULL) {
        return -1;
    }
    (void)snprintf(lc->paths[lc->npaths], sizeof lc->paths[0], "%s", resolved);
    lc->npaths++;

    xml_parser x = {.name = resolved,
                    .p = text,
                    .end = text + len,
                    .line = 1,
                    .fn = lc->fn,
                    .ctx = lc->ctx,
                    .err = err,
                    .errlen = errlen,
                    .include = NULL, /* set below, to close over the loader */
                    .ictx = lc};
    x.include = loader_include;
    int rc = parse_document(&x);

    lc->npaths--;
    free(text);
    return rc;
}

static int loader_include(void *ictx, const char *file, size_t line, char *err, size_t errlen)
{
    (void)line; /* the child's own errors carry its path and line */
    loader *lc = ictx;
    if (file[0] == '\0') {
        (void)snprintf(err, errlen, "include file=\"\" names no file");
        return -1;
    }
    char path[MAX_PATH];
    if (file[0] == '/') {
        if (strlen(file) >= sizeof path) {
            (void)snprintf(err, errlen, "include path too long");
            return -1;
        }
        (void)snprintf(path, sizeof path, "%s", file);
    } else {
        /* Relative to the file that names it, like C's #include "...". */
        const char *base = lc->npaths > 0 ? lc->paths[lc->npaths - 1] : ".";
        const char *slash = strrchr(base, '/');
        size_t dlen = slash != NULL ? (size_t)(slash - base) + 1 : 0;
        if (dlen + strlen(file) >= sizeof path) {
            (void)snprintf(err, errlen, "include path too long");
            return -1;
        }
        memcpy(path, base, dlen);
        (void)snprintf(path + dlen, sizeof path - dlen, "%s", file);
    }
    return load_file(lc, path, lc->npaths, err, errlen);
}

int hx_confxml_load(const char *path, hx_conf_fn fn, void *ctx, char *err, size_t errlen)
{
    loader lc = {.fn = fn, .ctx = ctx, .npaths = 0};
    return load_file(&lc, path, 0, err, errlen);
}
