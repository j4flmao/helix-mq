#!/usr/bin/env bash
# Shared helpers for the integration scripts. Source this file; do not execute it.
#
# Environment:
#   HELIX_BIN_DIR   directory holding helixd and helixctl (default: build)
#   TMPDIR          where the throw-away work directory is created

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

HELIX_BIN_DIR="${HELIX_BIN_DIR:-build}"
HELIXD="$REPO_ROOT/$HELIX_BIN_DIR/helixd"
HELIXCTL="$REPO_ROOT/$HELIX_BIN_DIR/helixctl"
[[ -x $HELIXD && -x $HELIXCTL ]] || {
    echo "missing binaries in $HELIX_BIN_DIR (run 'make' first)" >&2
    exit 2
}

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/helix-it-XXXXXX")"
DATA_DIR="$WORKDIR/data"
PORT_FILE="$WORKDIR/port"
BROKER_PID=""

cleanup() {
    local rc=$?
    if [[ -n $BROKER_PID ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
        kill -KILL "$BROKER_PID" 2>/dev/null || true
        wait "$BROKER_PID" 2>/dev/null || true
    fi
    jobs -p | xargs -r kill -KILL 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $rc -ne 0 && -n ${TEST_ARTIFACT_DIR:-} ]]; then
        mkdir -p "$TEST_ARTIFACT_DIR"
        cp -a "$WORKDIR" "$TEST_ARTIFACT_DIR/"
    fi
    rm -rf "$WORKDIR"
    return "$rc"
}
trap cleanup EXIT

die() {
    echo "FAIL: $*" >&2
    for log in "$WORKDIR"/*.log; do
        if [[ -s $log ]]; then
            echo "---- $(basename "$log") (tail) ----" >&2
            tail -n 20 "$log" >&2
        fi
    done
    exit 1
}

# start_broker [helixd args...]  — starts on an ephemeral port, waits until it is listening.
start_broker() {
    rm -f "$PORT_FILE"
    "$HELIXD" --port 0 --port-file "$PORT_FILE" --data-dir "$DATA_DIR" --log-level warn "$@" \
        2>>"$WORKDIR/helixd.log" &
    BROKER_PID=$!
    local i
    for ((i = 0; i < 200; i++)); do
        [[ -s $PORT_FILE ]] && return 0
        kill -0 "$BROKER_PID" 2>/dev/null || die "helixd exited during startup"
        sleep 0.05
    done
    die "helixd did not start listening within 10s"
}

port() { cat "$PORT_FILE"; }

# ctl <helixctl args...>
ctl() { "$HELIXCTL" -F "$PORT_FILE" "$@"; }

# Graceful stop: SIGTERM, require exit status 0.
stop_broker() {
    local rc=0
    kill -TERM "$BROKER_PID"
    wait "$BROKER_PID" || rc=$?
    BROKER_PID=""
    [[ $rc -eq 0 ]] || die "helixd exited with status $rc after SIGTERM (expected 0)"
}

# Abrupt stop: SIGKILL, no chance to flush or clean up.
kill9_broker() {
    kill -KILL "$BROKER_PID"
    wait "$BROKER_PID" 2>/dev/null || true
    BROKER_PID=""
}

PASSED=0
pass() {
    PASSED=$((PASSED + 1))
    echo "  ok   $*"
}

assert_eq() { # assert_eq <expected> <actual> <description>
    [[ $1 == "$2" ]] || die "$3: expected [$1], got [$2]"
    pass "$3"
}

# assert_fails <stderr-substring> <description> -- <command...>
assert_fails() {
    local needle=$1 desc=$2
    shift 3
    local err rc=0
    err="$("$@" 2>&1 >/dev/null)" || rc=$?
    [[ $rc -ne 0 ]] || die "$desc: command unexpectedly succeeded"
    [[ $err == *"$needle"* ]] || die "$desc: stderr [$err] lacks [$needle]"
    pass "$desc"
}
