# Helix MQ

[![CI](https://github.com/j4flmao/helix-mq/actions/workflows/ci.yml/badge.svg)](https://github.com/j4flmao/helix-mq/actions/workflows/ci.yml)
[![CodeQL](https://github.com/j4flmao/helix-mq/actions/workflows/codeql.yml/badge.svg)](https://github.com/j4flmao/helix-mq/actions/workflows/codeql.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A Linux-only commit-log message broker written in C11, with a native TCP protocol, durable local
storage and an optional replicated cluster. `helixd` runs the broker; `helixctl` is the command-line
client. This is pre-1.0 software. The 300k msg/s cluster throughput goal has not been demonstrated.

## What works

| Capability | Standalone topics | Replicated topics |
| --- | --- | --- |
| Produce / fetch by offset | Yes | Leader only; fetch reads committed records |
| Durability | Configurable sync policy | Explicit sync and commit across the ISR |
| Topic creation / deletion | Runtime and implicit creation | Static config only |
| Consumer groups / committed offsets | Node-local | No replicated coordinator or offsets |
| Idempotent producer | Within one broker lifetime | Unsupported |
| Failover | No | Raft controller, leader epochs, ISR and replica catch-up |
| TLS / authentication | No | No |

Helix is not Kafka wire-compatible. Kafka clients cannot connect directly.
Read [current limitations](docs/LIMITATIONS.md) before choosing a deployment.

## Run a release binary

Release automation produces static Linux **amd64** and **arm64** archives containing both programs.
They do not require an installed libc or compiler. Windows/macOS users need Docker, WSL2 or a Linux VM.
Use the [Releases page](https://github.com/j4flmao/helix-mq/releases) once a release is published;
the existence of this workflow does not mean a release is already available.

Example for amd64 version 0.2.0, after downloading its archive and checksum:

```sh
sha256sum -c helix-mq-0.2.0-linux-amd64.tar.gz.sha256
tar -xzf helix-mq-0.2.0-linux-amd64.tar.gz
cd helix-mq-0.2.0-linux-amd64
./bin/helixd --version
./bin/helixd --data-dir ./data --sync always
```

In a second terminal, from that directory:

```sh
./bin/helixctl ping                       # PONG
./bin/helixctl produce orders 0 hello     # ACK 0
./bin/helixctl fetch orders 0 0            # 0<TAB>hello
```

Stop with SIGTERM or Ctrl+C for a clean shutdown. Data remains in the chosen directory.
Never run two daemons against the same data directory.

## Configuration file

`helixd --config FILE` reads the same options as the command line, so a deployment can keep its
settings in a file and still override one option at a time. Files may be layered and **the command
line always wins**:

```sh
helixd --config /etc/helix/helixd.conf --config /etc/helix/local.xml --port 9401
```

Two formats are accepted; the first non-space byte picks the parser:

- `key = value`, one per line, `#` starts a comment, values may be quoted.
- a documented **XML subset** when the file starts with `<`: root `<helix>`, group elements, group
  attributes, comments, `<![CDATA[ ]]>`, entities and `<include file="..."/>`.

Both support `${NAME}` environment expansion. Keys are the long option names without `--` (`listen`,
`port`, `data-dir`, `sync`, `max-record-bytes`, `cluster-file`, ...) plus keys the command line does
not expose: `max-fetch-bytes`, `max-open-logs`, `max-offset-entries`, `max-groups`,
`max-group-members`, `max-producers`, `producer-ttl-ms`, `max-out-pending`, `max-frame-bytes`,
`listen-backlog`, `tcp-nodelay`, `recv-buffer-bytes`, `send-buffer-bytes`. `build/helixd --help`
lists the command-line names; [docs/CONFIGURATION.md](docs/CONFIGURATION.md) is the full reference.

An unknown key, a malformed line, an invalid value or an undefined `${NAME}` refuses the start with
the file, line and reason instead of silently falling back to a default. Precedence is
defaults < config files in order < command line.

```ini
# /etc/helix/helixd.conf
listen            = 0.0.0.0
port              = 9400
data-dir          = ${HELIX_DATA_DIR}
sync              = always
sync-interval-ms  = 200
log-level         = info
segment-bytes     = 268435456
max-conns         = 10000
```

The same broker in XML, with a shared network file pulled in and settings as attributes:

```xml
<helix>
  <include file="network.xml"/>
  <broker>
    <data-dir>${HELIX_DATA_DIR}</data-dir>
    <limits max-record-bytes="1048576"/>
  </broker>
  <storage sync="always" segment-bytes="268435456"/>
</helix>
```

`examples/config/` ships both spellings, and `examples/demo/05_config_formats.sh` starts a broker
from each to prove they configure the same thing.

## Docker

```sh
docker compose up --build -d
docker compose exec helixd helixctl ping
docker compose exec helixd helixctl produce orders 0 hello
docker compose exec helixd helixctl fetch orders 0 0
docker compose down
```

Compose starts one **standalone** broker, exposes port 9400 on localhost, uses a named data volume
and checks health with `helixctl ping`. The image runs as non-root UID 10001. `docker compose down`
preserves data; adding `--volumes` deletes it. The broker has no authentication: configure network
access deliberately before changing the localhost port binding.

### Environments

`scripts/compose.sh` selects the compose files and the environment file per deployment:

```sh
scripts/compose.sh dev up -d --build        # debug logging; the example walkthrough runs once
scripts/compose.sh staging up -d            # fsync on every acknowledgement, no demo
scripts/compose.sh prod up -d --build       # restart=always, read-only rootfs, resource limits
scripts/compose.sh cluster up -d --build    # three replicated nodes + the cluster walkthrough
scripts/compose.sh dev logs -f helixd
scripts/compose.sh dev demo                 # re-run the walkthrough on demand
scripts/compose.sh dev test                 # unit + configuration tests in a container
scripts/compose.sh cluster down
```

Each environment is a compose override (`docker-compose.dev.yml`, `.staging.yml`, `.prod.yml`) plus
an `.env.<env>` file, and each daemon is configured through a real file:
`deploy/config/<env>/helixd.conf` is mounted read-only at `/etc/helix/<env>/helixd.conf` and started
with `--config`. `${NAME}` substitution pulls the per-environment values (`HELIX_SYNC`,
`HELIX_LOG_LEVEL`, `HELIX_DATA_DIR`) from `.env.<env>`, so an unset variable fails the start rather
than falling back. The cluster topology lives in `deploy/config/cluster/` and uses static IPv4
addresses on an internal subnet, because the cluster file accepts literals only; `--node-id` on the
command line demonstrates a deployment wrapper overriding the shared node file.

```sh
docker build -t helix:latest .                         # runtime image + unit tests
docker build --target ci -t helix-ci .                 # full quality gate
docker build --target release --output type=local,dest=dist .  # tested static archive
```

The local release build targets the Docker engine's architecture. GitHub CI builds amd64 and arm64
on native runners. Release archives are placed in `dist/` with SHA256 checksums.

## Build from source

Requires Linux, a C11 compiler, GNU Make and Bash. The tests are C; no interpreter is needed.

```sh
git clone https://github.com/j4flmao/helix-mq.git
cd helix-mq
chmod +x scripts/*.sh
make all test
build/helixd --data-dir ./data --sync always
```

The normal build links against the host libc; use the Docker release target for portable static binaries.
`build/helixd --help` lists resource, durability and cluster options.

## Three-node cluster on one Linux host

Run each command in a separate terminal. Every daemon needs its own directory and four distinct ports:

```sh
mkdir -p ./data
build/helixd --cluster-file examples/cluster-local.conf --node-id 0 --data-dir ./data/node0
build/helixd --cluster-file examples/cluster-local.conf --node-id 1 --data-dir ./data/node1
build/helixd --cluster-file examples/cluster-local.conf --node-id 2 --data-dir ./data/node2
```

For an extracted release archive, use `bin/helixd` and `bin/helixctl` instead of `build/`.

```sh
build/helixctl -P 9400 cluster-metadata
```

Find the leader of `events` partition 0. Connect to that node's client port
(node 0: 9400, node 1: 9410, node 2: 9420):

```sh
build/helixctl -P 9400 produce events 0 hello  # replace 9400 with the leader's client port
build/helixctl -P 9400 fetch events 0 0
```

On NOT_LEADER, refresh metadata and reconnect. `helixctl` does not automatically route or retry
replicated requests. REQUEST_TIMEOUT means **outcome unknown**; retrying can produce a duplicate.

All nodes must use the same config, including topic order. For multiple hosts, use reachable IPv4
literals and permit the four configured ports on a private network. Do not reorder topics or change
partition counts when reusing data. Topics absent from the config remain node-local, including
accidentally misspelled names. Maximums: 8 nodes, 64 replicated topics, 1024 total partitions, 1 MiB
payload. Replication factor equals node count; minimum ISR is a majority. No replicated-log retention,
TLS or dynamic membership. Full details: [LIMITATIONS.md](docs/LIMITATIONS.md).

## Standalone groups and idempotence

```sh
build/helixctl create-topic orders 4
seq 1 1000 | build/helixctl produce-idem orders 0
build/helixctl consume-group workers orders 5000 0
build/helixctl describe workers
build/helixctl committed workers orders 0
```

Group consumption is at-least-once. Committed offsets persist; membership and producer sequence state
are in memory. Restart resets idempotence state. This does not provide durable exactly-once processing
or replicated consumer groups.

## Durability

Standalone behavior:

| `--sync` | Acknowledgement | Power-loss behavior |
| --- | --- | --- |
| `always` | After fdatasync; event-batch group commit | Acknowledged data is synced |
| `interval` (default, 200 ms) | Before periodic sync | Recent acknowledged data may be lost |
| `never` | Before sync | No power-loss durability guarantee |

The OS page cache survives a killed process, which differs from power loss. Replication explicitly
syncs records and acknowledges after commit across the current ISR; the table above is not its ack
policy. Hardware/filesystem guarantees still matter. A failed sync or replica invariant stops the
daemon instead of continuing to acknowledge writes.

## Examples

`examples/demo/` exercises the broker like a real system: every check goes over TCP through
`helixctl`, nothing is mocked.

```sh
make demo                                       # builds, starts a daemon from a generated config
                                                # file, runs all five steps, stops it again
scripts/compose.sh dev up -d --build            # the same walkthrough inside Docker
HELIX_NODES=127.0.0.1:9400,127.0.0.1:9410,127.0.0.1:9420 examples/demo/cluster.sh
```

| Script | What it demonstrates |
| --- | --- |
| `01_produce_fetch.sh` | durable log, produce by argument and by stdin, fetch by offset, metadata, on-disk layout |
| `02_consumer_groups.sh` | group join, delivery, offset commit, resume after a restart |
| `03_idempotent.sh` | producer ids, sequence numbers, a retried sequence collapsing into one record |
| `04_topics.sh` | create/delete topics, the error surface, oversized records refused without a crash |
| `05_config_formats.sh` | the same broker written as a `.conf` and as XML, loaded with `--config` |
| `cluster.sh` | leader agreement, produce on the leader, NOT_LEADER on a follower, high watermarks |

See [examples/README.md](examples/README.md) for details and environment variables.

## Tests and automation

```sh
make test                 # storage, broker, Raft, replication, controller and transports
make protocol             # TCP cluster boundary/malformed/restart regressions
make config demo          # configuration files and the end-to-end example walkthrough
make smoke groups crash   # standalone integration and kill -9 recovery
make asan                 # unit tests with ASan + UBSan
make asan-integration     # sanitizer smoke/crash/groups/cluster/protocol tests
make format-check tidy    # C formatting and static analysis
make fuzz FUZZ_TIME=60    # four libFuzzer targets
make raft-stress repl-stress chost-stress
make ci                   # full gate used by Docker and GitHub Actions
```

Full CI needs Clang 18 tooling, `libclang-rt-18-dev`, Valgrind and ShellCheck on Ubuntu 24.04.
PRs run the quality gate, the runtime Docker image, a portability matrix (Ubuntu 22.04/24.04,
Debian 12, Alpine 3.20) and both static architecture builds. CodeQL performs security analysis.
A weekly/manual workflow runs longer randomized stress and fuzz tests. Dependabot updates action/image
dependencies. Actions are pinned to commit SHAs; PR labeling never executes pull-request code.

Pushing a `vX.Y.Z` tag matching `VERSION` runs the full gate and creates a **draft release** with tested
archives, checksums and provenance attestations. See [release and repository setup](docs/RELEASING.md).
Branch protection and private vulnerability reporting need to be enabled in GitHub settings.

## Documentation and contributing

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/): run
`npm install` once to install the hooks (commitlint checks the message, the pre-commit hook keeps
staged C files clang-formatted, the pre-push hook runs the unit tests). Details in
[CONTRIBUTING.md](CONTRIBUTING.md).

- [Protocol](docs/PROTOCOL.md), [architecture](docs/ARCHITECTURE.md), [design decisions](docs/SOLUTION.md)
- [Configuring helixd](docs/CONFIGURATION.md): both file formats, every key and its range
- [Current limitations](docs/LIMITATIONS.md) and [roadmap](PLAN.md)
- [Contributing](CONTRIBUTING.md), [security](SECURITY.md), [community conduct](CODE_OF_CONDUCT.md)
- [Changelog](CHANGELOG.md) and [release process](docs/RELEASING.md)

Licensed under [Apache-2.0](LICENSE).
