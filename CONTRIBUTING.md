# Contributing to Helix MQ

Helix MQ is a Linux-only C11 project. Contributions are licensed under Apache-2.0.
Open a focused issue before changing replication semantics, the disk format or the wire protocol.
For vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Development

Fork https://github.com/j4flmao/helix-mq, create a branch, and keep changes focused.
On Ubuntu 24.04 install `build-essential clang clang-format clang-tidy libclang-rt-18-dev
valgrind shellcheck`. Run `chmod +x scripts/*.sh` after extracting a source archive.
Windows and macOS development uses Linux containers; native binaries are Linux-only.

```sh
make all test
make protocol             # real TCP cluster regressions
make format-check tidy
make ci                   # release + sanitizer + crash/failover + fuzz
docker build --target ci -t helix-ci .
```

The example walkthrough (`make demo`, `examples/demo/run.sh`) and the configuration-file test
(`make config`, `scripts/config_test.sh`) start real daemons and are the quickest way to see a
change behave end to end. Adding a key means adding a row to the table in
[docs/CONFIGURATION.md](docs/CONFIGURATION.md) and a check to `scripts/config_test.sh`.

Add a regression test that fails before a bug fix. Check both success and rejection paths,
restart durability, resource limits, and behavior under leadership changes when applicable.
Use reproducible simulator seeds for consensus changes. Do not weaken assertions or skip a
failing sanitizer to make CI green. Performance claims need reproducible measurements, hardware,
payload size, durability policy, concurrency, percentiles and cluster topology.

## Commit messages

Commits follow [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): subject`,
for example `fix(broker): reject empty topic names`. Types are the conventional set (`feat`, `fix`,
`docs`, `test`, `build`, `ci`, `perf`, `refactor`, `chore`, `revert`); scopes mirror the source tree
(`storage`, `broker`, `proto`, `net`, `ctl`, `raft`, `repl`, `cluster`, `util`, `tools`, `deploy`,
`docker`, `docs`, `deps`, ...) and an unusual scope is reported as a warning only. Breaking changes
are marked with `!` after the type or scope, or with a `BREAKING CHANGE:` footer. Keep the header
within 100 characters.

The rules are enforced locally by [Husky](https://typicode.github.io/husky/) hooks, configured in
`commitlint.config.cjs`:

| Hook | Check |
| --- | --- |
| `commit-msg` | `commitlint` rejects a message that does not follow the convention |
| `pre-commit` | staged `.c`/`.h` files must already be clang-formatted (skipped without clang-format) |
| `pre-push` | `make test` on Linux (skipped on hosts without `make`) |

Run `npm install` once after cloning to install the hooks; Node 18 or newer is only needed for this
tooling, nothing in the product depends on it. Bypass with `git commit --no-verify` only when you
are certain the check does not apply.

## Pull requests

- Describe the problem, observable behavior after the change, and validation performed.
- Include migration/compatibility notes for storage, config or protocol changes.
- Update docs and the Unreleased changelog for user-facing changes.
- Keep commit messages conventional; `npm install` installs the hooks that check them.
- Use descriptive titles; the PR title should read like a conventional commit header (see above).
- Wait for quality, runtime, portable architecture and CodeQL checks, then request review.
- Keep generated binaries, data directories, credentials and local test logs out of commits.

The maintainer decides whether to merge; CI never merges PRs automatically. There is no CLA or
sign-off requirement. Keep reviews respectful and follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Releases

See [docs/RELEASING.md](docs/RELEASING.md). Only maintainers should push version tags or publish
draft releases. `VERSION` is the source of truth for the daemon version and archive names.
