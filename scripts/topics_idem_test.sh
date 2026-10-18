#!/usr/bin/env bash
# Integration test for topic administration and the idempotent producer, with real processes:
#   1. CreateTopic pre-creates partitions, refuses duplicates, survives a restart;
#   2. duplicate / out-of-order / unknown-producer handling on the wire;
#   3. produce-idem under connection drops (lost acknowledgements): exactly one copy of every record;
#   4. a running consumer group follows a topic that is created, deleted and recreated;
#   5. DeleteTopic removes data and committed offsets for good (also across kill -9).
#
#   HELIX_BIN_DIR=build scripts/topics_idem_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "== topic admin + idempotent producer test ($HELIX_BIN_DIR) =="

wait_for() { # wait_for <description> <command...>
    local desc=$1 i
    shift
    for ((i = 0; i < 200; i++)); do
        if "$@" >/dev/null 2>&1; then
            pass "$desc"
            return 0
        fi
        sleep 0.1
    done
    die "timed out waiting for: $desc"
}

start_broker --sync interval

# --- 1. topic creation --------------------------------------------------------------------------
assert_eq "OK" "$(ctl create-topic orders 4)" "create-topic orders (4 partitions)"
assert_eq "4" "$(ctl metadata | awk '$1 == "orders"' | wc -l | tr -d ' ')" "all 4 partitions exist, empty"
assert_eq "0" "$(ctl metadata | awk '$1 == "orders" {s += $4} END {print s + 0}')" "partitions start empty"
assert_fails "topic already exists" "creating twice is refused" -- ctl create-topic orders 4
assert_fails "invalid topic" "reserved names are refused" -- ctl create-topic __internal 1
assert_fails "usage" "zero partitions is rejected by the client" -- ctl create-topic empty 0

# --- 2. wire-level idempotence --------------------------------------------------------------------
PID="$(ctl init-producer)"
[[ $PID =~ ^[0-9]+$ ]] || die "init-producer printed [$PID]"
pass "init-producer returns a numeric id"
assert_eq "ACK 0" "$(ctl iproduce "$PID" 0 orders 0 first)" "sequence 0 appended at offset 0"
assert_eq "ACK 0" "$(ctl iproduce "$PID" 0 orders 0 first)" "retry of sequence 0 returns the same offset"
assert_eq "ACK 1" "$(ctl iproduce "$PID" 1 orders 0 second)" "sequence 1 appended at offset 1"
assert_eq "2" "$(ctl fetch orders 0 0 | wc -l | tr -d ' ')" "retries wrote nothing"
assert_fails "out-of-order producer sequence" "a gap is refused" -- ctl iproduce "$PID" 5 orders 0 gap
assert_fails "unknown producer id" "unknown producer is refused" -- ctl iproduce 12345 0 orders 0 x

# --- 3. lost acknowledgements ---------------------------------------------------------------------
seq 1 500 | HELIXCTL_CHAOS_DROP_EVERY=7 "$HELIXCTL" -F "$PORT_FILE" produce-idem orders 1 \
    >"$WORKDIR/idem.out" 2>"$WORKDIR/idem.err" || die "produce-idem failed"
assert_eq "500" "$(grep -c '^ACK ' "$WORKDIR/idem.out")" "500 acknowledgements received"
retries="$(grep -c '^retry seq=' "$WORKDIR/idem.err" || true)"
((retries >= 70)) || die "chaos did not trigger enough retries ($retries)"
pass "connection was dropped and the same sequence resent ($retries retries)"
ctl fetch orders 1 0 | cut -f2 >"$WORKDIR/idem.log"
assert_eq "500" "$(wc -l <"$WORKDIR/idem.log" | tr -d ' ')" "exactly 500 records stored despite retries"
assert_eq "$(seq 1 500 | md5sum)" "$(md5sum <"$WORKDIR/idem.log")" "records are complete and in order"

# --- 4. a consumer group follows topic changes -----------------------------------------------------
: >"$WORKDIR/C.out"
"$HELIXCTL" -F "$PORT_FILE" consume-group follow events 2000 0 >"$WORKDIR/C.out" 2>"$WORKDIR/C.err" &
PID_C=$!
wait_for "group exists before its topic does" bash -c "'$HELIXCTL' -F '$PORT_FILE' describe follow | grep -q 'members=1'"
ctl create-topic events 2 >/dev/null
wait_for "consumer is assigned the new partitions" bash -c "'$HELIXCTL' -F '$PORT_FILE' describe follow | grep -q 'state=stable.*' && '$HELIXCTL' -F '$PORT_FILE' describe follow | grep -q 'events:0,events:1'"
for p in 0 1; do seq 1 10 | sed "s/^/old-$p-/" | ctl produce events "$p" >/dev/null; done
wait_for "20 records delivered" bash -c "(( \$(grep -c 'old-' '$WORKDIR/C.out') >= 20 ))"

ctl delete-topic events >/dev/null
wait_for "assignment emptied after the topic was deleted" bash -c "'$HELIXCTL' -F '$PORT_FILE' describe follow | grep -Eq 'state=stable.*members=1' && ! '$HELIXCTL' -F '$PORT_FILE' describe follow | grep -q 'events:'"
kill -0 "$PID_C" 2>/dev/null || die "consumer died when its topic was deleted"
pass "consumer survived the deletion"
assert_eq "NONE" "$(ctl committed follow events 0)" "committed offsets of the deleted topic are gone"

ctl create-topic events 2 >/dev/null
wait_for "consumer re-assigned after recreation" bash -c "'$HELIXCTL' -F '$PORT_FILE' describe follow | grep -q 'events:0,events:1'"
for p in 0 1; do seq 1 5 | sed "s/^/new-$p-/" | ctl produce events "$p" >/dev/null; done
wait_for "recreated topic is consumed from offset 0" bash -c "(( \$(grep -c 'new-' '$WORKDIR/C.out') >= 10 ))"
first_new="$(grep 'new-0-1$' "$WORKDIR/C.out" | head -1 | cut -f2)"
assert_eq "0" "$first_new" "first record of the recreated partition has offset 0"
kill -TERM "$PID_C"
rc=0
wait "$PID_C" || rc=$?
assert_eq 0 "$rc" "consumer exits 0 on SIGTERM"

# --- 5. delete is durable ---------------------------------------------------------------------------
ctl commit gone orders 0 2 >/dev/null
assert_eq "2" "$(ctl committed gone orders 0)" "offset committed before the delete"
assert_eq "OK" "$(ctl delete-topic orders)" "delete-topic orders"
assert_eq "NONE" "$(ctl committed gone orders 0)" "its offset is gone"
assert_eq "0" "$(ctl metadata | awk '$1 == "orders"' | wc -l | tr -d ' ')" "its partitions are gone"
assert_fails "unknown topic or partition" "deleting twice is refused" -- ctl delete-topic orders
kill9_broker
start_broker --sync interval
assert_eq "NONE" "$(ctl committed gone orders 0)" "offset stays deleted after kill -9"
assert_eq "0" "$(ctl metadata | awk '$1 == "orders"' | wc -l | tr -d ' ')" "topic stays deleted after kill -9"
assert_eq "2" "$(ctl metadata | awk '$1 == "events"' | wc -l | tr -d ' ')" "recreated topic survived the restart"
leftover="$(find "$DATA_DIR" -maxdepth 1 -name '.deleting.*' | wc -l | tr -d ' ')"
assert_eq "0" "$leftover" "no half-deleted directories left"

stop_broker
echo "topics/idempotence test: $PASSED checks passed"
