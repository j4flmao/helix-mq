/*
 * XML configuration parser tests: the grammar in util/confxml.h, what it
 * accepts and, mostly, what it refuses. A configuration file that is half
 * understood is worse than one that is refused, so every rule has a case.
 *
 *   ./test_confxml
 */
#define _XOPEN_SOURCE 700
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "util/conf.h"
#include "util/confxml.h"

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

#define MAX_PAIRS 24

/* Only these keys exist, mirroring the daemon's option table. */
static const char *const known[] = {"listen",
                                    "port",
                                    "data-dir",
                                    "sync",
                                    "log-level",
                                    "max-conns",
                                    "max-record-bytes",
                                    "max-frame-bytes",
                                    "node-id",
                                    "max-fetch-bytes",
                                    NULL};

typedef struct {
    char keys[MAX_PAIRS][HX_CONFXML_MAX_NAME + 1];
    char vals[MAX_PAIRS][HX_CONFXML_MAX_VALUE];
    size_t n;
} record;

static int collect(void *ctx, const char *key, const char *val, size_t line, char *err,
                   size_t errlen)
{
    record *r = ctx;
    (void)line; /* the parser prefixes the file and line itself */
    bool ok = false;
    for (size_t i = 0; known[i] != NULL; i++) {
        if (strcmp(known[i], key) == 0) {
            ok = true;
            break;
        }
    }
    if (!ok) {
        (void)snprintf(err, errlen, "unknown option '%s'", key);
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

static int parse(const char *text, record *r, char *err, size_t errlen)
{
    memset(r, 0, sizeof *r);
    err[0] = '\0';
    return hx_confxml_parse("doc", text, collect, r, err, errlen);
}

/* A bad document is one that is refused, with the expected reason. */
static void reject(const char *text, const char *want)
{
    record r;
    char err[512];
    if (parse(text, &r, err, sizeof err) == 0) {
        (void)fprintf(stderr, "FAIL %s: accepted [%s]\n", __FILE__, text);
        failures++;
        return;
    }
    CHECK_HAS(err, want);
}

static void test_good(void)
{
    record r;
    char err[512];
    const char *doc =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<!-- a comment before the root -->\n"
        "<helix max-frame-bytes=\"8388608\">\n"
        "  <server>\n"
        "    <listen>127.0.0.1</listen>\n"
        "    <port>9400</port>\n"
        "  </server>\n"
        "  <limits max-record-bytes=\"4194304\" max-conns=\"10000\"/>\n"
        "  <storage>\n"
        "    <data-dir>\n"
        "      /var/lib/helix/data\n"
        "    </data-dir>\n"
        "    <sync>always</sync>\n"
        "  </storage>\n"
        "  <log-level>debug</log-level>\n"
        "  <data-dir><![CDATA[/run/a & b<c>.pid]]></data-dir>\n"
        "  <data-dir>entities: &amp; &lt; &gt; &quot; &apos; &#65; &#x42;</data-dir>\n"
        "</helix>\n";
    CHECK(parse(doc, &r, err, sizeof err) == 0);
    CHECK(r.n == 10);
    if (r.n == 10) {
        CHECK(strcmp(r.keys[0], "max-frame-bytes") == 0 && strcmp(r.vals[0], "8388608") == 0);
        CHECK(strcmp(r.keys[1], "listen") == 0 && strcmp(r.vals[1], "127.0.0.1") == 0);
        CHECK(strcmp(r.keys[2], "port") == 0 && strcmp(r.vals[2], "9400") == 0);
        CHECK(strcmp(r.keys[3], "max-record-bytes") == 0 && strcmp(r.vals[3], "4194304") == 0);
        CHECK(strcmp(r.keys[4], "max-conns") == 0 && strcmp(r.vals[4], "10000") == 0);
        /* text spanning lines is trimmed, like a .conf value */
        CHECK(strcmp(r.keys[5], "data-dir") == 0 && strcmp(r.vals[5], "/var/lib/helix/data") == 0);
        CHECK(strcmp(r.keys[6], "sync") == 0 && strcmp(r.vals[6], "always") == 0);
        CHECK(strcmp(r.keys[7], "log-level") == 0 && strcmp(r.vals[7], "debug") == 0);
        /* CDATA is literal: '&' and '<' keep their meaning */
        CHECK(strcmp(r.keys[8], "data-dir") == 0 && strcmp(r.vals[8], "/run/a & b<c>.pid") == 0);
        CHECK(strcmp(r.keys[9], "data-dir") == 0 &&
              strcmp(r.vals[9], "entities: & < > \" ' A B") == 0);
    }

    /* Document order is kept; a repeated key is the caller's business. */
    CHECK(parse("<helix><port>1</port><port>2</port></helix>", &r, err, sizeof err) == 0);
    CHECK(r.n == 2 && strcmp(r.vals[0], "1") == 0 && strcmp(r.vals[1], "2") == 0);

    /* An empty document (or one with only a prolog and comments) is valid. */
    CHECK(parse("", &r, err, sizeof err) == 0 && r.n == 0);
    CHECK(parse("<?xml version=\"1.0\"?>\n<!-- nothing here -->\n", &r, err, sizeof err) == 0);
    CHECK(r.n == 0);
    CHECK(parse("\xEF\xBB\xBF<helix><port>1</port></helix>", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "1") == 0); /* UTF-8 BOM */
    /* Single-quoted attributes and CRLF line endings. */
    CHECK(parse("<helix>\r\n<limits max-conns='7'/>\r\n</helix>\r\n", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "7") == 0);

    /* Groups nest. A group's attributes are applied where its start tag stands,
     * so document order -- and with it "the last one wins" -- survives the
     * nesting, exactly as in a flat file. examples/config/standalone.xml uses
     * <limits> inside <broker>. */
    CHECK(parse("<helix><broker><limits max-conns=\"5\"/></broker></helix>", &r, err, sizeof err) ==
          0);
    CHECK(r.n == 1 && strcmp(r.keys[0], "max-conns") == 0 && strcmp(r.vals[0], "5") == 0);

    CHECK(parse("<helix><broker max-conns=\"1\"><limits max-conns=\"2\"/></broker></helix>", &r,
                err, sizeof err) == 0);
    CHECK(r.n == 2 && strcmp(r.vals[0], "1") == 0 && strcmp(r.vals[1], "2") == 0);

    CHECK(parse("<helix><broker><limits><max-conns>3</max-conns></limits></broker></helix>", &r,
                err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "3") == 0);

    /* An option element is not a group even inside one. */
    CHECK(parse("<helix><storage><sync>always</sync></storage></helix>", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.keys[0], "sync") == 0 && strcmp(r.vals[0], "always") == 0);
}

static void test_rejections(void)
{
    reject("<configuration><port>1</port></configuration>", "root element must be <helix>");
    reject("<helix><portt>1</portt></helix>", "doc:1: unknown option 'portt'");
    reject("<helix>\n<server>hello</server>\n</helix>",
           "doc:2: group <server> must not contain text");
    reject("<helix><server>oops<port>1</port></server></helix>", "mixed content in <server>");
    reject("<helix><port x=\"1\">9400</port></helix>", "option <port> must not have attributes");
    reject("<helix><port/></helix>", "option <port> needs a value");
    reject("<helix><port>   </port></helix>", "option <port> needs a value");
    reject("<helix><port><listen>1.1.1.1</listen></port></helix>",
           "option <port> must not contain elements");
    reject("<helix><port>1</portt></helix>", "</portt> closes <port>");
    reject("<helix><port>1</helix>", "</helix> closes <port>");
    reject("<helix><port>1", "unclosed element <port>");
    reject("<helix><port>1</port>", "unclosed element <helix>");
    reject("<helix/>trailing", "unexpected content after the root element");
    reject("<!DOCTYPE helix SYSTEM \"x\"><helix/>", "<! declarations are not supported");
    reject("<helix><ns:port>1</ns:port></helix>", "namespaces are not supported");
    reject("<helix><port>&nope;</port></helix>", "unknown entity '&nope;'");
    reject("<helix><port>a&b</port></helix>", "bare '&' must be written &amp;");
    reject("<helix><port>&#0;</port></helix>", "unknown entity '&#0;'");
    reject("<helix><!-- unclosed", "unterminated comment");
    reject("<helix><?php ?><port>1</port></helix>",
           "processing instruction is not allowed inside an element");
    reject("<helix><port>x</port", "expected '>' at the end of the closing tag");
    reject("<helix><port x=1>9400</port></helix>", "attribute value must be quoted");
    reject("<helix><port x\"1\">9400</port></helix>", "attribute 'x' needs =\"value\"");
    reject("<helix><port><![CDATA[x</port></helix>", "unterminated CDATA section in <port>");
    reject("<helix><include file=\"x.xml\"/></helix>",
           "<include> is only allowed when loading a file");
    reject("<helix><ports>1</ports><port>2</port></helix>", "unknown option 'ports'");

    /* A name longer than the bound is refused instead of truncated. */
    static char longname[160];
    memset(longname, 'a', sizeof longname);
    memcpy(longname, "<helix><", 8);
    memcpy(longname + 8 + 70, "/></helix>", 10);
    longname[8 + 70 + 10] = '\0';
    reject(longname, "name longer than 64 bytes");

    /* Text longer than a configuration line is refused, not truncated. */
    static char longtext[HX_CONFXML_MAX_VALUE + 128];
    memcpy(longtext, "<helix><port>", 13);
    memset(longtext + 13, 'v', HX_CONFXML_MAX_VALUE + 16);
    memcpy(longtext + 13 + HX_CONFXML_MAX_VALUE + 16, "</port></helix>", 15);
    longtext[13 + HX_CONFXML_MAX_VALUE + 16 + 15] = '\0';
    reject(longtext, "longer than 4095 bytes");
}

static void test_expansion(void)
{
    record r;
    char err[512];

    (void)setenv("HX_XML_DIR", "/srv/helix", 1);

    CHECK(parse("<helix><data-dir>${HX_XML_DIR}/data</data-dir></helix>", &r, err, sizeof err) ==
          0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "/srv/helix/data") == 0);
    CHECK(parse("<helix><listen attr=\"x\">${HX_XML_DIR}</listen></helix>", &r, err, sizeof err) !=
          0);
    CHECK(parse("<helix><limits max-conns=\"${HX_XML_DIR}\"/></helix>", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "/srv/helix") == 0); /* attribute value */
    /* Entities are decoded first, so a character reference can start a name. */
    CHECK(parse("<helix><data-dir>&#36;{HX_XML_DIR}</data-dir></helix>", &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "/srv/helix") == 0);

    reject("<helix><data-dir>${HX_XML_MISSING}</data-dir></helix>",
           "doc:1: undefined variable 'HX_XML_MISSING'");
    reject("<helix><data-dir>${HX_XML_DIR</data-dir></helix>", "doc:1: unterminated ${");
}

/* ------------------------------------------------------------------ files */

/* Rewrite a file in place: mkstemp() hands out a fresh name every call, and
 * the include tests need two files that point at each other. */
static int write_path(const char *path, const char *body)
{
    FILE *f = fopen(path, "wb");
    if (f == NULL) {
        return -1;
    }
    size_t n = strlen(body);
    int rc = fwrite(body, 1, n, f) == n ? 0 : -1;
    if (fclose(f) != 0) {
        rc = -1;
    }
    return rc;
}

static int write_temp(char *path, size_t cap, const char *body)
{
    (void)snprintf(path, cap, "/tmp/helix-xml-XXXXXX");
    int fd = mkstemp(path);
    if (fd < 0) {
        return -1;
    }
    (void)close(fd);
    return write_path(path, body);
}

static const char *base_name(const char *path)
{
    const char *slash = strrchr(path, '/');
    return slash != NULL ? slash + 1 : path;
}

static void test_load(void)
{
    record r;
    char err[512];
    char body[256];

    /* A relative <include> resolves against the directory of the including
     * file, and its settings are applied where the element stands. */
    char base[64];
    CHECK(write_temp(base, sizeof base, "<helix><port>9402</port></helix>") == 0);
    char mainfile[64];
    (void)snprintf(body, sizeof body,
                   "<helix>\n  <data-dir>/tmp/d</data-dir>\n"
                   "  <include file=\"%s\"/>\n</helix>\n",
                   base_name(base));
    CHECK(write_temp(mainfile, sizeof mainfile, body) == 0);

    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_confxml_load(mainfile, collect, &r, err, sizeof err) == 0);
    CHECK(r.n == 2);
    CHECK(r.n == 2 && strcmp(r.keys[0], "data-dir") == 0);
    CHECK(r.n == 2 && strcmp(r.keys[1], "port") == 0 && strcmp(r.vals[1], "9402") == 0);

    /* An error inside an included file names that file, not the includer. */
    CHECK(write_path(base, "<helix><portt>1</portt></helix>") == 0);
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_confxml_load(mainfile, collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, base);
    CHECK_HAS(err, ":1: unknown option 'portt'");

    /* A cycle is refused instead of recursing until the stack runs out. */
    char a[64];
    char b[64];
    CHECK(write_temp(a, sizeof a, "<helix/>") == 0);
    CHECK(write_temp(b, sizeof b, "<helix/>") == 0);
    (void)snprintf(body, sizeof body, "<helix><include file=\"%s\"/></helix>", base_name(b));
    CHECK(write_path(a, body) == 0);
    (void)snprintf(body, sizeof body, "<helix><include file=\"%s\"/></helix>", base_name(a));
    CHECK(write_path(b, body) == 0);
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_confxml_load(a, collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, "include cycle");

    /* <include> wants exactly one file attribute; a missing file is reported. */
    char inc[64];
    CHECK(write_temp(inc, sizeof inc, "<helix><include/></helix>") == 0);
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_confxml_load(inc, collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, "<include> needs exactly one file=\"...\" attribute");
    CHECK(write_temp(inc, sizeof inc, "<helix><include file=\"nope.xml\" other=\"1\"/></helix>") ==
          0);
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_confxml_load(inc, collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, "file=\"...\"");
    CHECK(write_temp(inc, sizeof inc, "<helix><include file=\"nope.xml\"/></helix>") == 0);
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_confxml_load(inc, collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, "nope.xml");

    /* Includes nest, but only HX_CONFXML_MAX_DEPTH deep. */
    char chain[9][64];
    for (int i = 0; i < 9; i++) {
        CHECK(write_temp(chain[i], sizeof chain[i], "<helix/>") == 0);
    }
    for (int i = 7; i >= 0; i--) {
        (void)snprintf(body, sizeof body, "<helix><include file=\"%s\"/></helix>",
                       base_name(chain[i + 1]));
        CHECK(write_path(chain[i], body) == 0);
    }
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_confxml_load(chain[0], collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, "nested deeper than 8");

    /* A missing file is reported with its path. */
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_confxml_load("/tmp/helix-xml-absent", collect, &r, err, sizeof err) != 0);
    CHECK_HAS(err, "/tmp/helix-xml-absent");
    CHECK(r.n == 0);

    /* hx_config_load picks the parser from the content, not from the name. */
    char any[64];
    CHECK(write_temp(any, sizeof any, "port = 9403\ndata-dir = /tmp/conf\n") == 0);
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_config_load(any, collect, &r, err, sizeof err) == 0);
    CHECK(r.n == 2 && strcmp(r.vals[0], "9403") == 0);
    CHECK(write_temp(any, sizeof any, "<helix><port>9404</port></helix>") == 0);
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_config_load(any, collect, &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "9404") == 0);
    CHECK(write_temp(any, sizeof any, "\xEF\xBB\xBF\n<helix><port>9405</port></helix>") == 0);
    memset(&r, 0, sizeof r);
    err[0] = '\0';
    CHECK(hx_config_load(any, collect, &r, err, sizeof err) == 0);
    CHECK(r.n == 1 && strcmp(r.vals[0], "9405") == 0);

    CHECK(unlink(any) == 0);
    CHECK(unlink(mainfile) == 0);
    CHECK(unlink(base) == 0);
    CHECK(unlink(a) == 0);
    CHECK(unlink(b) == 0);
    for (int i = 0; i < 9; i++) {
        CHECK(unlink(chain[i]) == 0);
    }
}

int main(void)
{
    test_good();
    test_rejections();
    test_expansion();
    test_load();

    if (failures != 0) {
        (void)fprintf(stderr, "%d XML configuration test(s) failed\n", failures);
        return 1;
    }
    (void)puts("all XML configuration tests passed");
    return 0;
}
