# Maintainer setup and releases

## Repository settings (not configured by files alone)

1. Enable GitHub Actions and private vulnerability reporting. CodeQL requires code scanning support
   for the repository's visibility and plan.
2. Protect the default branch: require a PR, an approving review, resolved discussions, and the CI
   checks `Quality gate`, `Runtime image`, `Portable amd64`, `Portable arm64` plus CodeQL. Select the
   actual check names after their first run. Disallow force pushes and branch deletion.
3. Restrict `v*` tag creation/deletion to maintainers. Keep workflow permissions read-only by default;
   individual jobs request the small write scopes they need.
4. If repository/organization policy blocks `pull_request_target`, explicitly allow the metadata-only
   PR labels workflow after review. It does not checkout or execute PR code. Do not add PR scripts to it.
5. Enable Dependabot. CODEOWNERS currently routes review to `@j4flmao`; update it as maintainers join.
   Require code-owner approval in the branch rules if that matches the project's review policy.

## Build locally

```sh
docker build --target release --output type=local,dest=dist .
```

This compiles with musl, runs unit tests and a three-process cluster test, creates the archive, verifies
its checksum, then tests the extracted binaries with smoke and TCP regression tests. The resulting
`dist/helix-mq-VERSION-linux-ARCH.tar.gz` includes `bin/helixd`, `bin/helixctl`, license, documentation
and an example config. Static binaries require Linux but no separately installed libc. Windows/macOS
use Docker or a Linux VM/WSL2. The Linux arm64 workflow uses a native `ubuntu-24.04-arm` runner.

## Publish

1. Update `VERSION`, move Unreleased changelog entries into the version section, and merge the PR after
   checks and review. Verify that changing the storage/protocol does not require a migration notice.
2. Create and push an annotated tag matching `VERSION`, for example:

   ```sh
   git tag -a "v$(cat VERSION)" -m "Helix MQ $(cat VERSION)"
   git push origin "v$(cat VERSION)"
   ```

3. The Release workflow validates the tag, invokes the full CI workflow, downloads both tested
   architecture artifacts, verifies checksums and creates provenance attestations. It creates a
   **draft** GitHub Release with the exact CI artifacts, individual checksums and `SHA256SUMS`.
4. Review the generated release notes and compatibility limits, then publish the draft in GitHub.
   Nothing deploys to a running cluster automatically, and containers are not pushed to a registry.

Do not move a published tag. Fix a bad release with a new version. If a workflow is rerun after a draft
already exists, inspect that draft before deleting/recreating it; publication deliberately does not
overwrite existing release assets silently.

## Verify a download

Download the archive and its `.sha256` file from the same release, then run:

```sh
sha256sum -c helix-mq-0.2.0-linux-amd64.tar.gz.sha256
gh attestation verify helix-mq-0.2.0-linux-amd64.tar.gz --repo j4flmao/helix-mq
```

Checksums detect corruption; attestations establish which GitHub workflow built the artifact.
See [GitHub's attestation documentation](https://docs.github.com/en/actions/concepts/security/artifact-attestations)
and [pull_request_target security guidance](https://docs.github.com/en/actions/reference/security/securely-using-pull_request_target).
