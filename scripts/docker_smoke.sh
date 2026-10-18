#!/usr/bin/env bash
set -euo pipefail
image="${1:-helix:latest}"
docker run --rm --network none --entrypoint /bin/sh "$image" -ec '
test "$(id -u)" = 10001
helixd --data-dir /var/lib/helix/data --sync always >/tmp/helix.log 2>&1 &
pid=$!
trap '\''kill "$pid" 2>/dev/null || true'\'' EXIT
for i in $(seq 1 50); do helixctl ping && break; sleep 0.1; done
helixctl produce smoke 0 persisted
helixctl fetch smoke 0 0 | grep persisted
kill -TERM "$pid"
wait "$pid"
helixd --data-dir /var/lib/helix/data --sync always >>/tmp/helix.log 2>&1 &
pid=$!
for i in $(seq 1 50); do helixctl ping && break; sleep 0.1; done
helixctl fetch smoke 0 0 | grep persisted
kill -TERM "$pid"
wait "$pid"
trap - EXIT
echo RUNTIME_SMOKE_PASSED
'
