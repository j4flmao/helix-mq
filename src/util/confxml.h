/*
 * XML configuration files, a documented subset of XML 1.0 for `helixd --config`.
 *
 * The flat format (util/conf.h) covers one setting per line; this one adds the
 * structure a deep deployment file wants: groups, attributes, comments and
 * includes. It is a hand-written parser for a *subset*, with no external
 * dependency and no allocation while parsing:
 *
 *   <?xml version="1.0"?>          optional prolog (accepted, not interpreted)
 *   <!-- comments -->              allowed between elements and inside groups
 *   <helix>                        the required root element
 *     <include file="base.xml"/>   merge another file, relative to this one
 *     <server>                     a group: structure, no text of its own
 *       <port>9400</port>          an option: its text is the value
 *     </server>
 *     <limits max-record-bytes="4194304" max-conns="10000"/>   group attributes
 *   </helix>
 *
 * Groups ({helix, broker, server, network, io, storage, log, limits, cluster,
 * consumer, consumer-groups, producer, producers}) only nest settings; their
 * attributes are settings too, so a group with many small values reads like a
 * table. Every other element name is an option and is validated by the
 * hx_conf_fn, so a typo fails with "file:line: unknown option 'x'" instead of
 * being dropped.
 *
 * Reported as errors rather than ignored: DTDs and entity declarations,
 * namespaces ('ns:tag'), mixed content (text next to elements), text inside a
 * group, attributes on an option, empty options, and anything that is not an
 * element, comment, prolog or CDATA section. CDATA is literal, as in XML.
 * Values are entity-decoded (&amp;, &lt;, &#65;, &#x41;, ...) and then
 * ${NAME}-expanded from the environment.
 *
 * The grammar above is the whole specification; tests/test_confxml.c is its
 * executable form.
 */
#ifndef HELIX_CONFXML_H
#define HELIX_CONFXML_H

#include <stddef.h>

#include "util/conf.h"

/* Names and values are bounded like a .conf line, so both formats agree. */
#define HX_CONFXML_MAX_NAME  HX_CONF_MAX_KEY
#define HX_CONFXML_MAX_VALUE HX_CONF_MAX_LINE
#define HX_CONFXML_MAX_DEPTH 8 /* how deep <include> may nest              */

/*
 * Parse an in-memory document. `name` labels error messages only. Returns 0
 * when every setting was accepted, -1 otherwise (err is filled). An empty
 * document (or one holding only a prolog and comments) is valid.
 *
 * <include> is rejected here: there is no file to resolve it against.
 */
int hx_confxml_parse(const char *name, const char *text, hx_conf_fn fn, void *ctx, char *err,
                     size_t errlen);

/*
 * Read `path` and parse it, following <include file="..."/> elements (a path is
 * relative to the file that names it; an absolute path is used as is). A missing
 * file, a cycle, or an include nested deeper than HX_CONFXML_MAX_DEPTH is an
 * error.
 */
int hx_confxml_load(const char *path, hx_conf_fn fn, void *ctx, char *err, size_t errlen);

#endif /* HELIX_CONFXML_H */
