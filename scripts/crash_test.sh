#!/usr/bin/env bash
# Crash-recovery test: SIGKILL the broker in the middle of a produce stream,
# restart it, and verify the durability contract of --sync always:
#
#   1. every record the client saw acknowledged is present, byte-identical, at
#      the offset it was acknowledged with;
#   2. offsets are dense (no holes, no duplicates);
#   3. nothing phantom: the log holds at most ONE record that was never acked
#      per crash (the single request that was in flight when the process died).
#
# Note: this exercises process crashes (page cache survives). Power-loss
# behaviour needs dm-flakey / dm-log-writes and is tracked in PLAN.md Phase 1.
#
#   ROUNDS=10 HELIX_BIN_DIR=build scripts/crash_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ROUNDS="${ROUNDS:-6}"
EXPECTED="$WORKDIR/expected.tsv"
: >"$EXPECTED"

echo "== crash-recovery test: $ROUNDS rounds of kill -9 ($HELIX_BIN_DIR) =="

for round in $(seq 1 "$ROUNDS"); do
    start_broker --sync always --segment-bytes 65536 # tiny segments: crashes also hit segment rolls

    acks="$WORKDIR/acks.$round"
    (seq 1 1000000 | sed "s/^/r${round}-/" | ctl produce crash 0 >"$acks" 2>/dev/null) &
    producer=$!

    # Let the producer run for a random 0.3-0.9s, then pull the plug.
    sleep "0.$((RANDOM % 7 + 3))"
    kill9_broker
    wait "$producer" 2>/dev/null || true

    n_acked=$(wc -l <"$acks")
    [[ $n_acked -gt 0 ]] || die "round $round: no records were acknowledged before the crash"
    awk -v r="$round" '{ printf "%s\tr%d-%d\n", $2, r, NR }' "$acks" >>"$EXPECTED"

    # Restart on the same data directory: recovery must run and the log must be intact.
    start_broker --sync always --segment-bytes 65536
    recovered="$WORKDIR/recovered.tsv"
    ctl fetch crash 0 0 >"$recovered" || die "round $round: fetch after recovery failed"

    awk -F'\t' '
        NR == FNR { have[$1] = $2; if ($1 + 0 != n++) { print "hole/duplicate at line " n; bad = 1 } ; next }
        { total++; if (!($1 in have)) { missing++; print "MISSING offset " $1 } else if (have[$1] != $2) { wrong++; print "CORRUPT offset " $1 ": " have[$1] " != " $2 } }
        END { exit (bad || missing || wrong) ? 1 : 0 }
    ' "$recovered" "$EXPECTED" || die "round $round: acknowledged data lost or corrupted after kill -9"

    total_acked=$(wc -l <"$EXPECTED")
    total_logged=$(wc -l <"$recovered")
    extra=$((total_logged - total_acked))
    ((extra >= 0 && extra <= round)) ||
        die "round $round: $extra unacknowledged records in the log (at most $round allowed)"

    echo "  round $round: acked $n_acked this round, $total_acked total, log holds $total_logged (+$extra in-flight)"
    stop_broker
done

pass "all acknowledged records survived $ROUNDS crashes"
echo "== crash-recovery test passed =="
