/*
 * Configuration parser tests: what it accepts and, more importantly, what it
 * refuses (a silent "" or a stale value is how a broker loses data).
 *
 *   ./test_conf
 */
#define _XOPEN_SOURCE 700
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "util/conf.h"

static int failures;

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
        }                                                                                          \
    } while (0)

#define CHECK_HAS(text, needle)                                                                    \
    do {                                                                                           \
        const char *t_ = (text);                                                                   \
        const char *n_ = (needle);                                                                 \
        if (t_ == NULL || strstr(t_, n_) == NULL) {                                                \
            (void)fprintf(stderr, "FAIL %s:%d: [%s] lacks [%s]\n", __FILE__, __LINE__,             \
                          t_ == NULL ? "(null)" : t_, n_);                                         \
            failures++;                                                                            \
        }                                                                                          \
    } while (0)

#define MAX_PAIRS 16

typedef struct {
    char keys[MAX_PAIRS][HX_CONF_MAX_KEY + 1];
    char vals[MAX_PAIRS][256];
    size_t n;
    const char *reject; /* reject this key through the callback */
} record;

static int collect(void *ctx, const char *key, const char *val, size_t line, char *err,
                   size_t errlen)
{
    record *r = ctx;
    (void)line; /* the parser prefixes file and line itself */
    if (r->reject != NULL && strcmp(key, r->reject) == 0) {
        (void)snprintf(err, errlen, "rejected '%s'", key);
        return -1;
    }
    if (r->n >= MAX_PAIRS) {
        (void)snprintf(err, errlen, "too many pairs");
        return -1;
    }
    (void)snprintf(r->keys[r->n], sizeof r->keys[0], "%s", key);
    (void)snprintf(r->vals[r->n], sizeof r->vals[0], "%s", val);
    r->n++;
    return 0;
}

/* Returns the parse result and leaves the captured pairs in r; err is filled on failure. */
static int parse(const char *text, record *r, char *err, size_t errlen)
{
    memset(r, 0, sizeof *r);
    err[0] = '\0';
    return hx_conf_parse("doc", text, collect, r, err, errlen);
}

static void test_good(void)
{
    record r;
    char err[512];
    const char *good = "# full-line comment\n"
                       "   # indented comment\n"
                       "\n"
                       "listen = 127.0.0.1\n"
                       "port 9400\n"
                       "port-bytes=65536\n"
                       "data-dir   =   /var/lib/helix/data   # trailing comment\n"
                       "sync = 'always'\n"
                       "log-level = \"debug\"\n"
                       "with.eq = a=b\n"
                       "hash = a#b\n"
                       "dollar = $5 and $$ stay literal"; /* no trailing newline */
    CHECK(parse(good, &r, err, sizeof err) == 0);
    CHECK(r.n == 9);
    if (r.n == 9) {
        CHECK(strcmp(r.keys[0], "listen") == 0 && strcmp(r.vals[0], "127.0.0.1") == 0);
        CHECK(strcmp(r.keys[1], "port") == 0 && strcmp(r.vals[1], "9400") == 0);
        CHECK(strcmp(r.keys[2], "port-bytes") == 0 && strcmp(r.vals[2], "65536") == 0);
        CHECK(strcmp(r.keys[3], "data-dir") == 0 && strcmp(r.vals[3], "/var/lib/helix/data") == 0);
        CHECK(strcmp(r.keys[4], "sync") == 0 && strcmp(r.vals[4], "always") == 0);
        CHECK(strcmp(r.keys[5], "log-level") == 0 && strcmp(r.vals[5], "debug") == 0);
        CHECK(strcmp(r.keys[6], "with.eq") == 0 && strcmp(r.vals[6], "a=b") == 0);
        CHECK(strcmp(r.keys[7], "hash") == 0 && strcmp(r.vals[7], "a#b") == 0);
        /* '$' is literal unless it opens a ${...} variable reference */
        CHECK(strcmp(r.keys[8], "dollar") == 0 && strcmp(r.vals[8], "$5 and $$ stay literal") == 0);
    }

    CHECK(parse("", &r, err, sizeof err) == 0 && r.n == 0); /* empty document */
    CHECK(parse("\n\n# only comments\n", &r, err, sizeof err) == 0 && r.n == 0);
    CHECK(parse("a = 1\r\nb = 2\r\n", &r, err, sizeof err) == 0 && r.n == 2); /* CRLF */
    CHECK(r.n == 2 && strcmp(r.vals[1], "2") == 0);
    CHECK(parse("\xEF\xBB\xBF"
                "bom = yes",
                &r, err, sizeof err) == 0 &&
          r.n == 1); /* BOM */
    CHECK(r.n == 1 && strcmp(r.vals[0], "yes") == 0);
}

static void test_rejections(void)
{
    record r;
    char err[512];

    CHECK(parse("port\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: missing value for 'port'");
    CHECK(parse("port =\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: empty value for 'port'");
    CHECK(parse("port = # nothing\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "empty value for 'port'");
    CHECK(parse("= 5\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: missing key");
    CHECK(parse("p@rt = 5\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: invalid key 'p@rt'");
    CHECK(parse("port \n", &r, err, sizeof err) != 0); /* whitespace, then nothing */
    CHECK_HAS(err, "missing value for 'port'");
    CHECK(parse("port = \"unterminated\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: unterminated quote for 'port'");
    CHECK(parse("port = 'x\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "unterminated quote for 'port'");
    CHECK(parse("123456789012345678901234567890123456789012345678901234567890123456789 = 1\n", &r,
                err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: key longer than 64 bytes");
    CHECK(parse("ok = 1\nok = 2\nbroken\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:3: missing value"); /* line numbers are real */
    CHECK(r.n == 2);

    /* callback rejection surfaces with the parser's name:line prefix */
    memset(&r, 0, sizeof r);
    r.reject = "sync";
    err[0] = '\0';
    CHECK(hx_conf_parse("doc", "sync = oops\n", collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: rejected 'sync'");
    CHECK(r.n == 0);

    /* over-long line: rejected with its line number instead of truncated */
    static char big[HX_CONF_MAX_LINE + 64];
    memset(big, 'x', sizeof big - 1);
    big[sizeof big - 1] = '\0';
    memcpy(big, "k = ", 4);
    big[HX_CONF_MAX_LINE + 10] = '\n';
    CHECK(parse(big, &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: line longer than 4095 bytes");

    /* 4095 bytes of content is exactly the limit and must be accepted */
    static char exact[HX_CONF_MAX_LINE + 1];
    memset(exact, 'v', HX_CONF_MAX_LINE - 1);
    exact[HX_CONF_MAX_LINE - 1] = '\n';
    exact[HX_CONF_MAX_LINE] = '\0';
    memcpy(exact, "k=", 2);
    CHECK(parse(exact, &r, err, sizeof err) == 0);
    CHECK(r.n == 1);
}

static void test_expansion(void)
{
    record r;
    char err[512];

    (void)setenv("HX_TEST_DIR", "/srv/helix", 1);
    (void)setenv("HX_TEST_EMPTY", "", 1);

    CHECK(parse("data-dir = ${HX_TEST_DIR}/data\n", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "/srv/helix/data") == 0);
    CHECK(parse("a=\"${HX_TEST_DIR}\"\n", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "/srv/helix") == 0);
    CHECK(parse("b = ${HX_TEST_EMPTY}\n", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "") == 0); /* empty stays the caller's problem */

    CHECK(parse("c = ${HX_TEST_MISSING}\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: undefined variable 'HX_TEST_MISSING'");
    CHECK(parse("d = ${HX_TEST_DIR\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "doc:1: unterminated ${");
    CHECK(parse("e = ${}\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "bad variable name");
    CHECK(parse("f = ${9bad}\n", &r, err, sizeof err) != 0);
    CHECK_HAS(err, "bad variable name");
    CHECK(parse("g = ${HX_TEST_DIR} tail\n", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "/srv/helix tail") == 0);
}

static void test_load(void)
{
    record r;
    char err[512];
    char path[] = "/tmp/helix-conf-XXXXXX";
    int fd = mkstemp(path);
    CHECK(fd >= 0);
    if (fd < 0) {
        return;
    }
    (void)close(fd);

    FILE *f = fopen(path, "wb");
    CHECK(f != NULL);
    if (f != NULL) {
        const char *body = "# loaded from disk\nlisten = 0.0.0.0\nport = 9410";
        CHECK(fwrite(body, 1, strlen(body), f) == strlen(body));
        CHECK(fclose(f) == 0);
    }
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_conf_load(path, collect, &r, err, sizeof err) == 0);
    CHECK(r.n == 2);
    CHECK(r.n == 2 && strcmp(r.keys[1], "port") == 0 && strcmp(r.vals[1], "9410") == 0);

    /* a callback failure is prefixed with the file the setting came from */
    memset(&r, 0, sizeof r);
    r.reject = "port";
    err[0] = '\0';
    CHECK(hx_conf_load(path, collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, path);
    CHECK_HAS(err, ":3: rejected 'port'");

    CHECK(unlink(path) == 0);

    /* missing file: reported, not silently ignored */
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_conf_load(path, collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, path);
    CHECK(r.n == 0);
}

int main(void)
{
    test_good();
    test_rejections();
    test_expansion();
    test_load();

    if (failures != 0) {
        (void)fprintf(stderr, "%d config test(s) failed\n", failures);
        return 1;
    }
    (void)puts("all config tests passed");
    return 0;
}
