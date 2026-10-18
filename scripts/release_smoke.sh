#!/usr/bin/env bash
# Verify the deliverable, not just the build directory.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p build
stage="$(mktemp -d build/release-smoke-XXXXXX)"
trap 'rm -rf "$stage"' EXIT
for checksum in dist/*.tar.gz.sha256; do
    (cd dist && sha256sum -c "$(basename "$checksum")")
done
for archive in dist/*.tar.gz; do
    tar -xzf "$archive" -C "$stage" --no-same-owner
done
bins=("$stage"/*/bin)
test "${#bins[@]}" = 1
export HELIX_BIN_DIR="${bins[0]}"
bash scripts/smoke.sh
make --no-print-directory BUILD=build build/test_protocol
build/test_protocol
echo 'RELEASE PACKAGE CHECKS PASSED'
