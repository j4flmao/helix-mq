/* The cluster file parser: what it accepts and, more importantly, what it refuses. */
#include <stdio.h>
#include <string.h>

#include "broker/cluster.h"

static int failures;

#define CHECK(cond)                                                                                \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
        }                                                                                          \
    } while (0)

static int parse(const char *text, hx_cl_spec *s)
{
    char err[100];
    return hx_cl_spec_parse(text, s, err, sizeof err);
}

int main(void)
{
    static hx_cl_spec s;
    const char *good = "# a cluster\n"
                       "node 0 127.0.0.1 9400 9401 9402 9403   # first\n"
                       "node 1 10.0.0.2 9400 9401 9402 9403\n"
                       "\n"
                       "topic events 4\n"
                       "topic orders.v2 2\n";
    CHECK(parse(good, &s) == 0);
    CHECK(s.nnodes == 2 && s.ntopics == 2 && s.nparts == 6);
    CHECK(s.topics[0].base == 0 && s.topics[1].base == 4 && s.topics[1].nlen == 9);
    CHECK(s.nodes[1].repl_port == 9403 && strcmp(s.nodes[1].host, "10.0.0.2") == 0);

    CHECK(parse("", &s) != 0);                               /* no node */
    CHECK(parse("node 1 127.0.0.1 1 2 3 4\n", &s) != 0);     /* ids start at 0 */
    CHECK(parse("node 0 127.0.0.1 1 2 3\n", &s) != 0);       /* missing port */
    CHECK(parse("node 0 127.0.0.1 1 2 3 0\n", &s) != 0);     /* port 0 */
    CHECK(parse("node 0 127.0.0.1 1 2 3 70000\n", &s) != 0); /* port too big */
    CHECK(parse("node 0 localhost 1 2 3 4\n", &s) != 0);     /* not an IPv4 literal */
    CHECK(parse("node 0 127.0.0.1 1 2 3 4\nnode 0 127.0.0.1 1 2 3 4\n", &s) != 0);
    CHECK(parse("node 0 127.0.0.1 1 2 3 4\ntopic a 0\n", &s) != 0); /* zero partitions */
    CHECK(parse("node 0 127.0.0.1 1 2 3 4\ntopic a 1\ntopic a 1\n", &s) != 0);
    CHECK(parse("node 0 127.0.0.1 1 2 3 4\ntopic __x 1\n", &s) != 0); /* reserved prefix */
    CHECK(parse("node 0 127.0.0.1 1 2 3 4\ntopic a/b 1\n", &s) != 0); /* bad character */
    CHECK(parse("node 0 127.0.0.1 1 2 3 4\ntopic a 1000\ntopic b 100\n", &s) != 0); /* > 1024 */
    CHECK(parse("node 0 127.0.0.1 1 2 3 4\nbogus\n", &s) != 0);
    CHECK(parse("node 0 127.0.0.1 1 2 3 4 5 6\n", &s) != 0); /* too many fields */
    CHECK(parse("node 0 127.0.0.1 1 2 3 4\ntopic a 1\n", &s) == 0);

    if (failures != 0) {
        (void)fprintf(stderr, "%d cluster-file test(s) failed\n", failures);
        return 1;
    }
    (void)puts("all cluster-file tests passed");
    return 0;
}
