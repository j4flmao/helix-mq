#!/usr/bin/env bash
# Consumer-group integration test with real processes:
#   1. two consumers split 4 partitions 2/2 (rebalance barrier);
#   2. kill -9 one consumer: after session expiry the survivor takes over all 4 partitions;
#   3. every produced record is delivered at least once (duplicates are allowed, loss is not);
#   4. stale or unknown members are fenced on commit and heartbeat;
#   5. SIGTERM makes the last consumer leave, which removes the group.
#
#   HELIX_BIN_DIR=build scripts/group_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TOPIC=jobs
GROUP=workers
SESSION_MS=2000
PER_PART=20
echo "== consumer group test ($HELIX_BIN_DIR) =="

# wait_for <description> <command...>: poll up to 20 s until the command succeeds.
wait_for() {
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

describe_has() { # describe_has <regex>
    ctl describe "$GROUP" 2>/dev/null | tr '\n' ' ' | grep -Eq "$1"
}

unique_delivered() { cat "$WORKDIR/A.out" "$WORKDIR/B.out" 2>/dev/null | cut -f1,2 | sort -u | wc -l; }
delivered_at_least() { (($(unique_delivered) >= $1)); }

produce_all() { # produce_all <first> <last>
    local p n
    for p in 0 1 2 3; do
        for ((n = $1; n <= $2; n++)); do echo "p$p-$n"; done | ctl produce "$TOPIC" "$p" >/dev/null
    done
}

start_broker --sync interval
: >"$WORKDIR/A.out"
: >"$WORKDIR/B.out"

produce_all 1 "$PER_PART"
assert_eq "$((4 * PER_PART))" "$(ctl metadata | awk '{s += $4 - $3} END {print s}')" "preloaded records"

# --- 1. two consumers split the partitions -------------------------------------------------
"$HELIXCTL" -F "$PORT_FILE" consume-group "$GROUP" "$TOPIC" "$SESSION_MS" 0 >"$WORKDIR/A.out" 2>"$WORKDIR/A.err" &
PID_A=$!
wait_for "A alone owns 4 partitions" describe_has 'state=stable generation=[0-9]+ members=1 .*(jobs:[0-3],?){4}'

"$HELIXCTL" -F "$PORT_FILE" consume-group "$GROUP" "$TOPIC" "$SESSION_MS" 0 >"$WORKDIR/B.out" 2>"$WORKDIR/B.err" &
PID_B=$!
wait_for "group of 2 is stable" describe_has 'state=stable generation=[0-9]+ members=2 '
split="$(ctl describe "$GROUP" | awk -F'\t' '$1 == "member" {n = split($3, a, ","); print n}' | sort | tr '\n' ' ')"
assert_eq "2 2 " "$split" "partitions split 2/2"
GEN2="$(ctl describe "$GROUP" | head -1 | sed -E 's/.*generation=([0-9]+).*/\1/')"

wait_for "all preloaded records delivered" delivered_at_least $((4 * PER_PART))

# --- 2. kill -9 B: A takes over after the session expires -----------------------------------
member_b_logged() { grep -q '^joined m' "$WORKDIR/B.err"; }
wait_for "B logged its member id" member_b_logged
MEMBER_B="$(sed -n 's/^joined \(m[^ ]*\) .*/\1/p' "$WORKDIR/B.err" | tail -1)"
[[ -n $MEMBER_B ]] || die "could not read B's member id"
kill -KILL "$PID_B"
wait "$PID_B" 2>/dev/null || true
produce_all $((PER_PART + 1)) $((2 * PER_PART))
wait_for "A owns all 4 partitions again" describe_has 'state=stable generation=[0-9]+ members=1 .*(jobs:[0-3],?){4}'
GEN3="$(ctl describe "$GROUP" | head -1 | sed -E 's/.*generation=([0-9]+).*/\1/')"
((GEN3 > GEN2)) || die "generation did not advance after failover ($GEN2 -> $GEN3)"
pass "generation advanced after failover ($GEN2 -> $GEN3)"

# --- 3. nothing lost -----------------------------------------------------------------------
wait_for "all records delivered after failover" delivered_at_least $((8 * PER_PART))
for p in 0 1 2 3; do
    have="$(cat "$WORKDIR/A.out" "$WORKDIR/B.out" | awk -F'\t' -v p="$p" '$1 == p {print $2}' | sort -un | tr '\n' ' ')"
    want="$(seq 0 $((2 * PER_PART - 1)) | tr '\n' ' ')"
    assert_eq "$want" "$have" "partition $p fully delivered"
done
payload_ok="$(cat "$WORKDIR/A.out" "$WORKDIR/B.out" | awk -F'\t' '$3 != "p" $1 "-" ($2 + 1) {bad++} END {print bad + 0}')"
assert_eq 0 "$payload_ok" "payloads match their offsets"

# --- 4. fencing ----------------------------------------------------------------------------
MEMBER_A="$(sed -n 's/^joined \(m[^ ]*\) .*/\1/p' "$WORKDIR/A.err" | tail -1)"
assert_fails "unknown group or member" "dead member cannot commit" -- \
    ctl gcommit "$GROUP" "$MEMBER_B" "$GEN3" "$TOPIC" 0 5
assert_fails "unknown group or member" "dead member cannot heartbeat" -- \
    ctl heartbeat "$GROUP" "$MEMBER_B" "$GEN3"
assert_fails "stale group generation" "stale generation is fenced" -- \
    ctl gcommit "$GROUP" "$MEMBER_A" "$GEN2" "$TOPIC" 0 5
assert_fails "unknown group or member" "unknown group has no members" -- \
    ctl heartbeat nosuchgroup "$MEMBER_A" 1
committed="$(ctl committed "$GROUP" "$TOPIC" 0)"
((committed == 2 * PER_PART)) || die "fenced commits moved the offset (committed=$committed)"
pass "fenced commits left the offset untouched"

# --- 5. clean leave removes the group -------------------------------------------------------
kill -TERM "$PID_A"
rc=0
wait "$PID_A" || rc=$?
assert_eq 0 "$rc" "consumer exits 0 on SIGTERM"
assert_fails "unknown group or member" "group is gone after the last member left" -- ctl describe "$GROUP"

# --- 6. single-shot commands behave --------------------------------------------------------
out="$(ctl join solo "$TOPIC" 1000)"
first="$(head -1 <<<"$out")"
[[ $first == member=m*" generation="*" stable="* ]] || die "unexpected join output: $first"
pass "join prints member/generation/stable"
SOLO="$(sed -E 's/^member=([^ ]+) .*/\1/' <<<"$first")"
ctl leave solo "$SOLO" >/dev/null
pass "leave succeeds"
assert_fails "malformed request" "join rejects a too-short session" -- ctl join solo "$TOPIC" 10

stop_broker
echo "group test: $PASSED checks passed"
