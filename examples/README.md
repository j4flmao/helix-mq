# Helix examples

Runnable examples for a real broker over its native TCP protocol. Everything
here talks to `helixd` through `helixctl`; nothing is mocked.

## Standalone walkthrough

```bash
make                                # build helixd + helixctl into build/
examples/demo/run.sh                # starts a broker from a config file and runs everything
```

`run.sh` generates a `helixd.conf`, starts a daemon with `--config` (ephemeral
port, its own data directory), runs five steps against it and stops it again:

| Step | Script | What it demonstrates |
| --- | --- | --- |
| 1 | `01_produce_fetch.sh` | durable log, produce by argument and stdin, fetch by offset, metadata, on-disk layout |
| 2 | `02_consumer_groups.sh` | group join, delivery, offset commits, resume after restart |
| 3 | `03_idempotent.sh` | producer ids, sequence numbers, retried sequences collapse into one record |
| 4 | `04_topics.sh` | create/delete topics, the error surface, oversized records refused |
| 5 | `05_config_formats.sh` | the same broker as a flat `.conf` and as XML with an `<include>`, both loaded with `--config` |

To point the same scripts at a broker that is already running:

```bash
HELIX_HOST=127.0.0.1 HELIX_PORT=9400 examples/demo/01_produce_fetch.sh
```

Every step deletes and recreates the topics it needs, so running them again against
a warm broker is safe and produces the same result; nothing here depends on a
fresh data directory.

## Configuration examples

`examples/config/` holds one broker written twice -- `standalone.conf` (flat) and
`standalone.xml` (groups, attributes, an `<include>`) -- plus the fragment that
the XML file includes. Both use `${HELIX_DEMO_WORK}` for their writable
directory, so they can be started anywhere:

```bash
HELIX_DEMO_WORK=/tmp/helix-demo build/helixd --config examples/config/standalone.xml
```

`examples/demo/05_config_formats.sh` starts each file and checks the settings
took effect (the 64 KiB record limit comes from the file, not a default).

## Cluster walkthrough

Start the three-node local cluster (see `examples/cluster-local.conf`), then:

```bash
HELIX_NODES=127.0.0.1:9400,127.0.0.1:9410,127.0.0.1:9420 examples/demo/cluster.sh
```

The script checks that every node agrees on the partition leader, produces to
the leader, verifies that a follower refuses writes with `NOT_LEADER`, and
compares high watermarks across the cluster.

## Inside Docker

The images ship with these examples; compose runs them for you:

```bash
scripts/compose.sh dev up -d --build      # broker + the standalone walkthrough
scripts/compose.sh cluster up -d --build  # three nodes + the cluster walkthrough
scripts/compose.sh dev demo               # re-run the walkthrough by hand
scripts/compose.sh dev test               # unit tests + config tests in a container
```

See `deploy/config/` for the per-environment configuration files the compose
files mount into the daemons.
