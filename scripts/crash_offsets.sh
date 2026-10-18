#!/usr/bin/env bash
# Crash test for consumer-group offsets: SIGKILL the broker while a client is committing
# increasing offsets with --sync always, restart, and verify that
#   1. every commit the client saw acknowledged is still there (committed >= last acked);
#   2. nothing from the future appeared (committed <= last acked + 2: one commit may have been
#      in flight, one may have been acked just before the client recorded it);
#   3. groups committed in earlier rounds are untouched by later crashes.
#
#   ROUNDS=6 HELIX_BIN_DIR=build scripts/crash_offsets.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ROUNDS="${ROUNDS:-5}"
RECORDS=3000
declare -A LAST_ACKED

echo "== offset crash test: $ROUNDS rounds of kill -9 ($HELIX_BIN_DIR) =="

verify_all() { # verify_all <upto_round> <label>
    local g got low high
    for g in $(seq 1 "$1"); do
        got="$(ctl committed "g$g" crash 0)"
        [[ $got == NONE ]] && got=0
        low=${LAST_ACKED[$g]:-0}
        high=$((low + 2))
        ((got >= low && got <= high)) ||
            die "$2: group g$g has offset $got, expected between $low (last acked) and $high"
    done
}

for round in $(seq 1 "$ROUNDS"); do
    start_broker --sync always
    if ((round == 1)); then
        ctl bench crash 0 "$RECORDS" 16 64 >/dev/null || die "could not preload $RECORDS records"
    fi

    marker="$WORKDIR/last.$round"
    (
        n=1
        while ((n <= RECORDS)); do
            ctl commit "g$round" crash 0 "$n" >/dev/null 2>&1 || exit 0
            echo "$n" >"$marker"
            n=$((n + 1))
        done
    ) &
    committer=$!

    sleep "0.$((RANDOM % 6 + 3))"
    kill9_broker
    wait "$committer" 2>/dev/null || true

    LAST_ACKED[$round]=$(cat "$marker" 2>/dev/null || echo 0)
    ((LAST_ACKED[$round] > 0)) || die "round $round: no commit was acknowledged before the crash"

    start_broker --sync always # recovery: replays the internal offsets log
    verify_all "$round" "round $round after recovery"
    echo "  round $round: ${LAST_ACKED[$round]} commits acked before the crash; all groups verified"
    stop_broker
done

pass "acknowledged offset commits survived $ROUNDS crashes"
echo "== offset crash test passed =="
