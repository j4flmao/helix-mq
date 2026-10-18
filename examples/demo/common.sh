#!/usr/bin/env bash
# Shared helpers for the example scripts. Source this file; do not execute it.
#
#   HELIX_HOST / HELIX_PORT   broker to talk to (default 127.0.0.1:9400)
#   HELIXCTL                  helixctl binary (default: build tree, then PATH)
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DEMO_DIR/../.." && pwd)"

: "${HELIX_HOST:=127.0.0.1}"
: "${HELIX_PORT:=9400}"
: "${PASSED:=0}"

if [[ -z ${HELIXCTL:-} ]]; then
    # Honour a non-default `make BUILD=...` through HELIX_BIN_DIR (scripts/lib.sh).
    for cand in "$REPO_ROOT/${HELIX_BIN_DIR:-build}/helixctl" "$REPO_ROOT/dist/helixctl"; do
        [[ -x $cand ]] && { HELIXCTL=$cand; break; }
    done
fi
: "${HELIXCTL:=helixctl}"
if ! command -v "$HELIXCTL" >/dev/null 2>&1; then
    echo "helixctl not found: run 'make' or set HELIXCTL" >&2
    exit 2
fi

ctl() { "$HELIXCTL" -H "$HELIX_HOST" -P "$HELIX_PORT" "$@"; }

say() { printf '\n=== %s\n' "$*"; }
note() { printf '      %s\n' "$*"; }
ok() {
    PASSED=$((PASSED + 1))
    printf '  ok   %s\n' "$*"
}
die() {
    printf '  FAIL %s\n' "$*" >&2
    exit 1
}
expect_eq() { # expect_eq <want> <got> <description>
    [[ $1 == "$2" ]] || die "$3: want [$1], got [$2]"
    ok "$3"
}
expect_contains() { # expect_contains <haystack> <needle> <description>
    [[ $1 == *"$2"* ]] || die "$3: [$1] lacks [$2]"
    ok "$3"
}
count_lines() { awk 'NF {n++} END {print n + 0}'; }
summary() { printf '\n%s: %d checks passed\n' "${0##*/}" "$PASSED"; }
