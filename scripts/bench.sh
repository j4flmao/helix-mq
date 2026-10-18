#!/usr/bin/env bash
# Network benchmark: pipelined produce against a local helixd, per fsync policy.
# Numbers are only meaningful on an idle machine with the data dir on the disk you care about
# (set TMPDIR). Methodology and caveats: docs/SOLUTION.md section 8.
#
#   HELIX_BIN_DIR=build scripts/bench.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

run() { # run <label> <count> <size> <depth> <helixd args...>
    local label=$1 count=$2 size=$3 depth=$4
    shift 4
    start_broker "$@"
    echo "-- $label"
    ctl bench bench 0 "$count" "$size" "$depth"
    stop_broker
    rm -rf "$DATA_DIR"
}

echo "== helix network benchmark ($HELIX_BIN_DIR, data dir on $(df --output=fstype "$WORKDIR" | tail -1 | tr -d ' ')) =="
run "sync=never,    128 B, depth 64"  500000 128  64  --sync never
run "sync=interval, 128 B, depth 64"  500000 128  64  --sync interval
run "sync=interval, 1 KiB, depth 64"  300000 1024 64  --sync interval
run "sync=always,   128 B, depth 64"  5000   128  64  --sync always
run "sync=always,   128 B, depth 1"   2000   128  1   --sync always
