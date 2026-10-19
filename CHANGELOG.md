# Changelog

## Unreleased

### Fixed
- Replicated writes reserve epoch storage separately from the configured payload limit. Valid
  boundary-sized records no longer trigger a daemon fail-stop.
- Cluster startup rejects payload limits above 1 MiB rather than accepting records too large for
  the bounded replication transport.
- Docker CI includes the Clang sanitizer/fuzzer runtime and normalizes shell scripts from Windows.
- Example walkthroughs and the configuration-file test drive real daemons, so the release build is
  now exercised end to end (not only the sanitizer integration targets).
- The Docker CI walkthroughs detect a failed example run by the container's real exit code
  (`docker wait`) instead of `docker compose wait`, which reported a bogus status on one runner.
- The aarch64 CRC32C backend assembles again: the 64-bit step used a mnemonic that does not exist
  and the other two passed an X register where the instruction takes a W register, so the arm64
  runner could not build the release archive. The quality job now cross-compiles the tree for
  aarch64 on every push, because no amd64 job ever compiles that code.

### Added
- `helixd --config FILE`: layered configuration files (`key = value`, `#` comments, quoted values,
  `${NAME}` environment expansion, fail-fast validation with file and line) whose precedence is
  defaults < files in order < command line. Unknown keys and undefined variables refuse the start.
- XML configuration format: a documented subset (root `<helix>`, group elements and group
  attributes, comments, prolog, CDATA, entities, `<include file="..."/>` resolved relative to the
  including file with cycle detection) selected when the file starts with `<`. Both formats set the
  same keys and share one validation table, so a setting error names the file, the line and the key
  whether it came from a plain file, an included fragment or XML.
- `examples/config/`: the same standalone broker written as a flat `.conf` and as XML with an
  `<include>`, plus `examples/demo/05_config_formats.sh`, which starts a daemon from each file and
  proves the settings took effect.
- Socket and storage tuning keys reachable only from a configuration file: `listen-backlog`,
  `tcp-nodelay`, `recv-buffer-bytes`, `send-buffer-bytes`, `max-out-pending` and `max-frame-bytes`,
  validated with the same file/line errors as the rest.
- CRC32C now uses the SSE4.2 `crc32` instruction on x86-64 and the ARMv8 CRC extension on aarch64
  when the CPU has it, with runtime detection and a portable slice-by-8 table otherwise. The unit
  tests compare both backends for every length and alignment, because a checksum that depended on
  the host would silently corrupt logs written on one machine and verified on another.
- `docs/CONFIGURATION.md`: the full reference for both formats, includes, environment expansion,
  every key with its type, default and valid range, per-role recipes, the cluster file format, the
  failure behaviour and a tuning cheat-sheet.
- Configuration-file integration test (`make config`, `scripts/config_test.sh`), including a
  three-node cluster launched entirely from per-node files, plus unit tests for the parser
  (`tests/test_conf.c`).
- Unit tests for the XML parser (`tests/test_confxml.c`), including nested groups.
- Runnable examples (`examples/demo/`): a standalone walkthrough covering the commit log, consumer
  groups, idempotent retries, the error surface and configuration files, and a cluster walkthrough
  covering leader agreement, NOT_LEADER on a follower and high watermarks (`make demo`).
- Multi-environment Docker Compose deployments (`dev`, `staging`, `prod` layers over the base file,
  `cluster` for three replicated nodes on a static subnet) with per-environment config files under
  `deploy/config/`, environment files, and `scripts/compose.sh` as the entry point. The `dev` and
  `cluster` topologies run the example walkthrough once the brokers are healthy.
- A portability CI matrix that builds, tests, runs the wire-protocol suite, the configuration test
  and the example walkthrough on Ubuntu 22.04/24.04, Debian 12 and Alpine 3.20.
- Commit-message tooling: Husky hooks (commitlint on `commit-msg`, clang-format on staged C files,
  unit tests before push) configured through `commitlint.config.cjs` and `package.json`.
- TCP regressions for payload boundaries, malformed requests, pipelined response ordering,
  unsupported APIs, local routing, restart and transport-size validation.
- Static Linux amd64/arm64 archive builds with checksums, extracted-package tests and draft release
  automation with provenance attestations.
- CI quality/runtime/portable jobs, weekly stress/fuzzing, CodeQL, Dependabot and PR labels.
- Apache-2.0 license, contributor/security/conduct documentation and issue/PR templates.

### Changed
- The wire-protocol regression suite is plain C (`tests/test_protocol.c`, `make protocol`); it
  spawns the real `helixd` and speaks the protocol over TCP. Python is no longer used anywhere in
  the project, so a build and its tests need nothing but a C toolchain.
- Static Linux amd64/arm64 archive builds with checksums, extracted-package tests and draft release
  automation with provenance attestations.
- CI quality/runtime/portable jobs, weekly stress/fuzzing, CodeQL, Dependabot and PR labels.
- Apache-2.0 license, contributor/security/conduct documentation and issue/PR templates.

`VERSION` is currently 0.2.0. This file does not imply that a GitHub release has been published.
