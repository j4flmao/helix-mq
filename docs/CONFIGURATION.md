# Configuring helixd

Everything `helixd` can be told is a named setting. The command line and a
configuration file set the same names; a few performance keys exist only in a
file because nobody wants them on a command line. This document is the reference
for getting those settings right, format by format.

`scripts/config_test.sh` is the executable form of this page: it starts daemons
from real files, checks precedence and proves that bad files are refused.

- [Where a setting comes from](#where-a-setting-comes-from)
- [Flat format (`key = value`)](#flat-format-key--value)
- [XML format](#xml-format)
- [Includes](#includes)
- [`${NAME}` expansion](#name-expansion)
- [Full key reference](#full-key-reference)
- [Recipes](#recipes)
- [The cluster file](#the-cluster-file)
- [Failure behaviour](#failure-behaviour)
- [Tuning cheat-sheet](#tuning-cheat-sheet)
- [Verifying a configuration](#verifying-a-configuration)

## Where a setting comes from

`--config` is read *before* the command line, and may be repeated:

```sh
helixd --config /etc/helix/helixd.conf \
       --config /etc/helix/host.xml \
       --data-dir /var/lib/helix \
       --port 9401
```

Precedence, smallest to largest:

1. built-in defaults;
2. every `--config` file, in the order given;
3. the command line, in the order given.

Within one file, later settings win over earlier ones (the last assignment
wins), which is what makes a small override file possible. A setting that
appears in no layer keeps its default, listed in the [key
reference](#full-key-reference).

The first non-space byte of a file decides its format: `<` selects the XML
parser, anything else the flat one. The extension is irrelevant, so a
`.conf` file that starts with `<` is read as XML.

## Flat format (`key = value`)

```
# /etc/helix/helixd.conf
listen           = 0.0.0.0
port             = 9400
data-dir         = ${HELIX_DATA_DIR}
sync             = always
sync-interval-ms = 200
segment-bytes    = 268435456
max-record-bytes = 4194304
log-level        = info

tcp-nodelay      = 1
recv-buffer-bytes = 262144
```

Rules, exactly:

| Rule | Detail |
| --- | --- |
| One setting per line | `key`, optional spaces, `=`, optional spaces, `value`. The `=` may be omitted when spaces separate the two. |
| Key characters | `A-Z a-z 0-9 . _ -`, up to 64 bytes. |
| Values | Up to 4095 bytes. Surrounding whitespace is trimmed; `'` or `"` may quote a value, in which case the quotes are removed, so a value may keep leading/trailing spaces or a `#`. |
| Comments | `#` at the start of a line, or after whitespace, is a comment to the end of the line. `#` inside a word (e.g. `a#b`) is data. |
| Blank lines | Ignored. A UTF-8 BOM on the first line is tolerated. |
| Continuation | None. One line is one setting. |
| Encoding | Bytes. Keep it ASCII; non-ASCII only survives inside a quoted value. |

A value that is missing (`port =`) or a line that is neither blank, a comment
nor a setting is an error with its line number, not something to skip.

## XML format

For a deployment file that wants structure, the XML parser reads a documented
subset of XML 1.0. The root element must be `<helix>`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!-- Everything the broker needs, in one file. -->
<helix>
  <include file="network.xml"/>

  <broker>
    <data-dir>${HELIX_DATA_DIR}</data-dir>
    <limits max-record-bytes="4194304" max-conns="10000"/>
  </broker>

  <storage segment-bytes="268435456" sync="always" sync-interval-ms="200"/>

  <network listen="0.0.0.0" port="9400" tcp-nodelay="1"
           recv-buffer-bytes="262144" send-buffer-bytes="262144"/>

  <log-level>info</log-level>
</helix>
```

Two kinds of element:

- A **group** — one of `helix`, `broker`, `server`, `network`, `io`, `storage`,
  `log`, `limits`, `cluster`, `consumer`, `consumer-groups`, `producer`,
  `producers` — is pure structure. It has no text of its own, and **each of its
  attributes is a setting**, so a group with many small values reads like a
  table. Groups may nest.
- Every **other** element is a setting: its name is the key and its text is the
  value. `<port>9400</port>` sets `port`. An option may not have attributes or
  child elements, and must have non-empty text.

The element name is the key wherever it appears: `<port>` inside `<storage>` is
the same setting as `<port>` at the top level. Groups only shape the file for a
human reader. Settings are applied in document order, and a group's attributes
are applied where its start tag stands, so a later duplicate overrides an
earlier one exactly as in the flat format.

Also understood:

| Construct | Meaning |
| --- | --- |
| `<?xml version="1.0" ...?>` | Accepted and ignored; only at the top of the file. |
| `<!-- ... -->` | Comment; anywhere between elements or inside a group. |
| `<![CDATA[ ... ]]>` | Literal text; no entity decoding inside. |
| `&amp; &lt; &gt; &quot; &apos;` | Standard entities. |
| `&#65;` / `&#x41;` | Decimal and hexadecimal character references. |
| `<include file="..."/>` | Merge another file (see below). |
| Whitespace | Only whitespace may sit between elements; a group may not contain text. |

Refused as errors, on purpose, rather than guessed at:

- `<!DOCTYPE ... >` and any other `<!...>` declaration;
- namespaces (`<ns:port>`);
- mixed content (text next to child elements), text inside a group;
- an attribute or a child element on an option, an empty option;
- a processing instruction inside an element;
- a bare `&` that is not `&amp;`, an unknown entity, `&#0;`;
- any mismatch between start and end tags, an unclosed element, content after
  the root element.

Limits: a document up to 1 MiB; element and attribute names up to 64 bytes;
values and text up to 4095 bytes.

## Includes

`<include file="network.xml"/>` reads another file and applies its settings at
that point, as if its content stood there. It is available only when loading a
file (`hx_confxml_load`), not from an in-memory document.

- A relative path is resolved against the directory of the **including** file,
  so `network.xml` sits next to the file that names it. An absolute path is used
  as is.
- Includes may nest up to 8 deep.
- A cycle (`a.xml` includes `b.xml` includes `a.xml`) is an error; the include
  stack is tracked by resolved path.
- Missing file, a non-existent directory or a broken symlink is an error.
- The included file has its own `<helix>` root and may itself use every feature
  here, including further includes.

A typical split: one file per concern, a top file that includes them in a
deliberate order, and the environment supplying the paths.

## `${NAME}` expansion

Every value in either format is expanded after format-level decoding (`&#36;`
in XML becomes `$` before expansion, as in XML's own rules):

```
data-dir   = ${HELIX_DATA_DIR}
port-file  = ${HELIX_RUN}/helix.port
```

- A name is `[A-Za-z_][A-Za-z0-9_]*`, up to 127 bytes.
- The value comes from the daemon's environment.
- An **undefined variable is an error**, not an empty string: silently turning
  `${HELIX_DATA_DIR}` into `""` is how a broker ends up writing to the current
  directory. The message names the file, the line and the variable.
- An unterminated `${` is an error.
- Expansion is not recursive: the substituted text is not scanned again.
- The total value must still fit in 4095 bytes.
- The command line is not expanded by helixd; your shell does that.

Compose deployments lean on this: `${NAME}` in a mounted config file reads the
per-environment values from `.env.<env>` through the container environment.

## Full key reference

Names are the long option names without `--`. Every key can be set from a
configuration file; keys marked *(file only)* have no command-line equivalent.

### Network / server

| Key | Type | Default | Valid range | Notes |
| --- | --- | --- | --- | --- |
| `listen` | IPv4 literal | `127.0.0.1` | dotted quad | Only IPv4 literals; no host names. |
| `port` | u16 | `9400` | 0..65535 | `0` binds an ephemeral port. |
| `port-file` | path | — | — | Written atomically after a successful bind. |
| `max-conns` | u32 | `10000` | 1..1000000 | Connections beyond this are refused. |
| `max-out-pending` *(file only)* | bytes | `8388608` | 65536..4294967296 | Per-connection unsent output before reads stop (backpressure). |
| `max-frame-bytes` *(file only)* | bytes | `2097152` | 4096..1073741824 | Largest request frame after its length prefix. |
| `listen-backlog` *(file only)* | int | `1024` | 1..65535 | `listen(2)` backlog. |
| `tcp-nodelay` *(file only)* | 0/1 | `1` | 0..1 | `0` re-enables Nagle; keep `1` for request/response latency. |
| `recv-buffer-bytes` *(file only)* | bytes | `0` | 0..1073741824 | `SO_RCVBUF` cap; `0` keeps the OS default and its autotuning. |
| `send-buffer-bytes` *(file only)* | bytes | `0` | 0..1073741824 | `SO_SNDBUF` cap; `0` keeps the OS default. |

### Durability / storage

| Key | Type | Default | Valid range | Notes |
| --- | --- | --- | --- | --- |
| `data-dir` | path | `./data` | — | One directory per daemon; never share it. |
| `sync` | enum | `interval` | `always`/`interval`/`never` | Durability policy, see below. |
| `sync-interval-ms` | u32 | `200` | 1..3600000 | Interval for `sync = interval`. |
| `segment-bytes` | u64 | `67108864` | 4096..1099511627776 | Segment roll threshold (64 MiB). |
| `max-record-bytes` | u32 | `1048576` | 1..67108864 | Largest record payload (1 MiB). Also caps what replication accepts. |

| `sync` | Acknowledgement | Power-loss behaviour |
| --- | --- | --- |
| `always` | after `fdatasync` (group commit batches concurrent appends) | acknowledged data is on disk |
| `interval` | before the periodic sync | recent acknowledged data may be lost |
| `never` | before any sync | no power-loss guarantee; benchmarks only |

### Broker limits

| Key | Type | Default | Valid range | Notes |
| --- | --- | --- | --- | --- |
| `max-fetch-bytes` *(file only)* | u32 | `4194304` | 4096..1073741824 | Caps one Fetch response. A larger request is clamped, not refused. |
| `max-open-logs` *(file only)* | u32 | `1024` | 1..1048576 | Partition logs open at once; also bounds topic creation. |
| `max-offset-entries` *(file only)* | u32 | `100000` | 1..16777216 | Committed-offset table entries. |
| `max-groups` *(file only)* | u32 | `1000` | 1..1048576 | Consumer groups per broker. |
| `max-group-members` *(file only)* | u32 | `10000` | 1..1048576 | Members per group. |
| `max-producers` *(file only)* | u32 | `10000` | 1..1048576 | Idempotent producer ids tracked. |
| `producer-ttl-ms` *(file only)* | u32 | `900000` | 1000..86400000 | Idle producer id eviction (15 min). |

### Logging

| Key | Type | Default | Valid range |
| --- | --- | --- | --- |
| `log-level` | enum | `info` | `debug`/`info`/`warn`/`error` |

### Cluster

| Key | Type | Default | Valid range | Notes |
| --- | --- | --- | --- | --- |
| `cluster-file` | path | — | — | Topics and nodes; present means clustered. |
| `node-id` | u32 | — | 0..7 | This node's line in the cluster file. Required with `cluster-file`. |
| `ack-timeout-ms` | u32 | `5000` | 1..600000 | How long a replicated Produce waits for the ISR. |

Replicated topics cap `max-record-bytes` at 1 MiB, and a larger value is refused
at startup rather than accepted for standalone use and failing once a client
writes a big record. `max-frame-bytes` does not need hand-tuning for that: if it
is too small to fit one request frame, helixd raises it to
`max-record-bytes + 64 KiB`. In cluster mode `listen` and `port` are ignored —
both are taken from this node's line in the cluster file.

## Recipes

**Durability first (the default for anything you care about).** Every
acknowledgement is on stable storage before the client sees it:

```ini
sync             = always
segment-bytes    = 67108864
log-level        = info
```

**Throughput / benchmarking on a trusted host.** Let the page cache absorb
writes and batch large records:

```ini
sync             = never
segment-bytes    = 268435456
max-record-bytes = 4194304
max-fetch-bytes  = 16777216
max-out-pending  = 16777216
send-buffer-bytes = 1048576
```

**Latency-sensitive ingest (many small records, many connections).**

```ini
sync             = interval
sync-interval-ms = 20
tcp-nodelay      = 1
listen-backlog   = 4096
recv-buffer-bytes = 262144
max-conns        = 20000
```

**Many small topics.** Raise the open-log ceiling if you keep hundreds of
partitions hot at once:

```ini
max-open-logs          = 4096
max-offset-entries     = 500000
max-groups             = 5000
max-group-members      = 50000
```

**Development sandbox.** Ephemeral port, per-check directories, chatty log:

```ini
listen    = 127.0.0.1
port      = 0
port-file = /tmp/helix.port
data-dir  = /tmp/helix-data
sync      = interval
log-level = debug
```

**Container.** Keep the environment out of the image and name things:

```ini
listen    = 0.0.0.0
data-dir  = ${HELIX_DATA_DIR}
sync      = ${HELIX_SYNC}
log-level = ${HELIX_LOG_LEVEL}
```

**Cluster node** (three nodes share one cluster file, differ only by `node-id`):

```ini
data-dir       = /var/lib/helix/data
sync           = always
cluster-file   = /etc/helix/cluster.conf
node-id        = 0
ack-timeout-ms = 5000
```

See `deploy/config/{dev,staging,prod,cluster}/` for files that are used by the
shipped compose environments, and `examples/config/` for the same standalone
broker written as a flat file and as XML.

## The cluster file

`cluster-file` is not a settings file; it is a topology list. One record per
line; `part` and `node` are literal prefixes from the format:

```
# node <id> <ip> <client-port> <repl-port> <raft-port> <ctrl-port>
node 0 10.77.0.10 9400 9401 9402 9403
node 1 10.77.0.11 9410 9411 9412 9413
node 2 10.77.0.12 9420 9421 9422 9423

# topic <name> <partitions>
topic events 4
topic metrics 1
```

Each node needs its own four ports, so the numbers step by ten per node in the
example above; `deploy/config/cluster/cluster.conf` is the file the shipped
compose cluster uses.

- Up to 8 nodes, 64 replicated topics, 1024 partitions total.
- Addresses are IPv4 **literals**; host names are rejected.
- All nodes must load the same file in the same order. A topic that is absent
  falls through to the node-local broker, so a misspelled name silently stops
  being replicated — which `scripts/config_test.sh` and the cluster walkthrough
  both exercise.
- Replication factor equals the node count, and the minimum in-sync replicas is
  a majority (`n / 2 + 1`).

## Failure behaviour

A configuration mistake refuses the start; it never degrades silently. When
something is wrong, helixd prints one line to stderr and exits non-zero:

```
helixd: /etc/helix/helixd.conf:12: invalid --max-conns
helixd: /etc/helix/rate.xml:4: unknown option 'recv-buffer'
helixd: /etc/helix/base.xml:9: undefined variable 'HELIX_DATA_DIR'
```

The `file:line:` prefix names the file the setting came from — for a setting
inside an `<include>`, it names the **included** file, not the includer. Nothing
is half-applied: the process stops before it opens a log or binds a port.

Values are validated against the ranges above, not just parsed: `max-conns =
99999999`, `sync = sometimes`, `listen = localhost`, `tcp-nodelay = yes` and
`port = -1` are all refused.

## Tuning cheat-sheet

| Symptom | Key to turn |
| --- | --- |
| Recent acknowledgements lost on power loss | `sync = always` |
| fsync latency dominates throughput | `sync = interval` and lower `sync-interval-ms` |
| A single fetch is huge and stalls | lower `max-fetch-bytes` |
| Memory grows with slow consumers | lower `max-out-pending` |
| "too many open partitions" under many topics | raise `max-open-logs` |
| Connection refused under a burst of reconnects | raise `listen-backlog` and `max-conns` |
| Small writes feel delayed | keep `tcp-nodelay = 1` |
| Throughput limited by syscall overhead | raise `recv-buffer-bytes`/`send-buffer-bytes` |
| Consumer-offset writes hit a cap | raise `max-offset-entries` |
| Idempotent state grows without bound | lower `producer-ttl-ms`, raise `max-producers` |
| Replicated Produce times out | raise `ack-timeout-ms`, check follower health |
| A record is refused as too large | raise `max-record-bytes` everywhere, cluster included |

## Verifying a configuration

Start the daemon once with the file and confirm the settings in effect, rather
than trusting a review:

```sh
build/helixd --config /etc/helix/helixd.conf --port-file /tmp/helix.port &
port="$(cat /tmp/helix.port)"
build/helixctl -P "$port" ping
build/helixctl -P "$port" produce smoke 0 hello
build/helixctl -P "$port" fetch smoke 0 0
ls -l "${HELIX_DATA_DIR}"               # data-dir is where the file says
kill -TERM %1
```

For a repeatable check, run the shipped suite:

```sh
make config          # scripts/config_test.sh: precedence, refusals, a 3-node file cluster
make demo            # examples/demo/05_config_formats.sh: the same broker as .conf and XML
```

Both start real daemons and fail on a setting that did not take effect.
