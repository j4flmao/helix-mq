#!/usr/bin/env bash
# Helix end-to-end walkthrough: data, consumer groups, idempotent producers and
# the error surface, run against a real broker over TCP.
#
#   examples/demo/run.sh                 start a local broker from a config file, run everything
#   HELIX_HOST=helixd HELIX_PORT=9400 examples/demo/run.sh   use an already-running broker
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DEMO_DIR/../.." && pwd)"

HELIXD="${HELIXD:-}"
if [[ -z $HELIXD ]]; then
    # Honour a non-default `make BUILD=...` through HELIX_BIN_DIR.
    for cand in "$REPO_ROOT/${HELIX_BIN_DIR:-build}/helixd" "$REPO_ROOT/dist/helixd"; do
        [[ -x $cand ]] && { HELIXD=$cand; break; }
    done
fi
: "${HELIXD:=helixd}"
export HELIXD

WORK=""
DAEMON_PID=""
# Run from an EXIT trap: keep every branch explicit. With 'set -e' a trap whose
# last command fails makes bash report a failure even after 'exit 0'.
cleanup() {
    if [[ -n $DAEMON_PID ]]; then
        kill -TERM "$DAEMON_PID" 2>/dev/null || true
    fi
    if [[ -n $WORK ]]; then
        rm -rf "$WORK" || true
    fi
}
trap cleanup EXIT

if [[ -z ${HELIX_PORT:-} ]]; then
    command -v "$HELIXD" >/dev/null 2>&1 || {
        echo "helixd not found: run 'make' or set HELIXD/HELIX_PORT" >&2
        exit 2
    }
    WORK="$(mktemp -d)"
    # The demo daemon is configured the way a deployed one is: through a file.
    cat >"$WORK/helixd.conf" <<EOF
listen           = 127.0.0.1
port             = 0
port-file        = $WORK/port
data-dir         = $WORK/data
sync             = ${DEMO_SYNC:-interval}
sync-interval-ms = 50
log-level        = ${DEMO_LOG_LEVEL:-warn}
max-conns        = 256
EOF
    "$HELIXD" --config "$WORK/helixd.conf" 2>>"$WORK/helixd.log" &
    DAEMON_PID=$!
    for ((i = 0; i < 200; i++)); do
        [[ -s $WORK/port ]] && break
        kill -0 "$DAEMON_PID" 2>/dev/null || {
            echo "helixd exited during startup:" >&2
            cat "$WORK/helixd.log" >&2
            exit 1
        }
        sleep 0.05
    done
    [[ -s $WORK/port ]] || { echo "helixd never wrote its port file" >&2; exit 1; }
    export HELIX_HOST=127.0.0.1
    export HELIX_PORT
    HELIX_PORT="$(cat "$WORK/port")"
    export HELIX_DATA_DIR="$WORK/data"
    echo "demo broker: pid $DAEMON_PID, client port $HELIX_PORT (config: $WORK/helixd.conf)"
else
    echo "demo broker: external, $HELIX_HOST:$HELIX_PORT"
fi

rc=0
for script in 01_produce_fetch.sh 02_consumer_groups.sh 03_idempotent.sh 04_topics.sh \
    05_config_formats.sh; do
    if ! bash "$DEMO_DIR/$script"; then
        rc=1
    fi
done

if [[ $rc -eq 0 ]]; then
    echo
    echo "demo finished: all steps passed"
else
    echo
    echo "demo finished: FAILURES above" >&2
fi
exit $rc
