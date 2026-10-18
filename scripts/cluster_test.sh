#!/usr/bin/env bash
# Replicated topics with three real helixd processes (one node = controller + broker):
#   1. the cluster elects partition leaders by itself;
#   2. a Produce is acknowledged only when committed; every node's high watermark follows;
#   3. a non-leader refuses Produce and Fetch (NOT_LEADER);
#   4. kill -9 the leader of partition 0: another node takes over, no acknowledged record is lost,
#      and writes continue;
#   5. the killed node restarts, catches up and rejoins the ISR;
#   6. kill -9 every node, start them again: the committed prefix is intact.
#
#   HELIX_BIN_DIR=build scripts/cluster_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "== cluster test ($HELIX_BIN_DIR) =="
# Stay below Linux's default ephemeral client-port range (32768..60999). Repeated
# tests otherwise collide with TIME_WAIT sockets left by their own helixctl clients.
BASE=$((12000 + ($$ % 900) * 16))
declare -a CPORT PIDS
CONF="$WORKDIR/cluster.conf"
: >"$CONF"
for n in 0 1 2; do
    b=$((BASE + n * 4 + 100))
    CPORT[n]=$b
    echo "node $n 127.0.0.1 $b $((b + 1)) $((b + 2)) $((b + 3))" >>"$CONF"
done
echo "topic events 2" >>"$CONF"

start_node() {
    "$HELIXD" --cluster-file "$CONF" --node-id "$1" --data-dir "$WORKDIR/n$1" --log-level warn \
        --ack-timeout-ms 3000 2>>"$WORKDIR/n$1.log" &
    PIDS[$1]=$!
}
kill_node() {
    kill -KILL "${PIDS[$1]}" 2>/dev/null || true
    wait "${PIDS[$1]}" 2>/dev/null || true
    PIDS[$1]=""
}
nctl() { # nctl <node> <helixctl args...>
    local n=$1
    shift
    "$HELIXCTL" -P "${CPORT[n]}" "$@"
}
live_nodes() {
    local n
    for n in 0 1 2; do [[ -n ${PIDS[n]:-} ]] && echo "$n"; done
    return 0
}
# leader_of <partition>: the leader that every live node agrees on, else fails.
leader_of() {
    local p=$1 n l="" cur
    for n in $(live_nodes); do
        cur="$(nctl "$n" cluster-metadata 2>/dev/null | awk -F'\t' -v p="$p" '$1=="part" && $3==p {print $4}')"
        [[ -n $cur && $cur != "-" ]] || return 1
        [[ -z $l || $l == "$cur" ]] || return 1
        l=$cur
    done
    [[ -n $l ]] && echo "$l"
}
# is_leader <node> <partition>: that node itself reports being the leader.
is_leader() {
    nctl "$1" cluster-metadata 2>/dev/null | awk -F'\t' -v p="$2" '$1=="part" && $3==p {print $9}' | grep -qx 1
}
wait_for() { # wait_for <description> <seconds> <command...>
    local desc=$1 secs=$2 i
    shift 2
    for ((i = 0; i < secs * 10; i++)); do
        if "$@" >/dev/null 2>&1; then
            pass "$desc"
            return 0
        fi
        sleep 0.1
    done
    die "timed out waiting for: $desc"
}
stable_leader() {
    local l
    l="$(leader_of "$1")" && is_leader "$l" "$1"
}
# col <node> <partition> <column>: one column of the partition line a node reports.
col() { nctl "$1" cluster-metadata | awk -F'\t' -v p="$2" -v c="$3" '$1=="part" && $3==p {print $c}'; }
# produce_retry <partition> <message>: find the leader, retry for up to 30 s. Prints the offset.
produce_retry() {
    local p=$1 msg=$2 i l out
    for ((i = 0; i < 300; i++)); do
        l="$(leader_of "$p" 2>/dev/null)" || l=""
        if [[ -n $l ]] && out="$(nctl "$l" produce events "$p" "$msg" 2>/dev/null)"; then
            echo "${out#ACK }"
            return 0
        fi
        sleep 0.1
    done
    die "could not produce $msg to partition $p"
}
expect_records() { # expect_records <node> <partition> <count>: records 0.. are msg-<i>
    local out want k
    out="$(nctl "$1" fetch events "$2" 0 2>/dev/null)" || return 1
    want="$(for ((k = 0; k < $3; k++)); do printf '%d\tmsg-%d\n' "$k" "$k"; done)"
    [[ $out == "$want" ]]
}
col_is() { [[ "$(col "$1" "$2" "$3")" == "$4" ]]; }
all_hw_is() { # all_hw_is <partition> <value>
    local n
    for n in $(live_nodes); do col_is "$n" "$1" 7 "$2" || return 1; done
}

for n in 0 1 2; do start_node "$n"; done
wait_for "partition 0 gets a leader that all nodes agree on" 30 stable_leader 0
wait_for "partition 1 gets a leader that all nodes agree on" 30 stable_leader 1

L0="$(leader_of 0)"
for ((i = 0; i < 50; i++)); do
    off="$(produce_retry 0 "msg-$i")"
    [[ $off == "$i" ]] || die "offset of record $i is [$off]"
done
pass "50 records acknowledged with dense offsets"
expect_records "$L0" 0 50 || die "leader does not return the 50 records"
pass "the leader serves exactly the acknowledged records"
wait_for "every node's high watermark reaches 50" 20 all_hw_is 0 50

FOLLOWER=$(((L0 + 1) % 3))
assert_fails "not the leader" "a follower refuses Produce" -- nctl "$FOLLOWER" produce events 0 nope
assert_fails "not the leader" "a follower refuses Fetch" -- nctl "$FOLLOWER" fetch events 0 0
assert_fails "unknown topic or partition" "partition out of range" -- nctl "$L0" produce events 9 x

# --- leader failure ---------------------------------------------------------------------------
kill_node "$L0"
pass "kill -9 the leader of partition 0 (node $L0)"
L1=""
for ((i = 0; i < 400; i++)); do
    L1="$(leader_of 0 2>/dev/null)" || L1=""
    if [[ -n $L1 && $L1 != "$L0" ]] && is_leader "$L1" 0; then break; fi
    L1=""
    sleep 0.1
done
[[ -n $L1 ]] || die "no new leader for partition 0"
pass "new leader of partition 0 is node $L1"
expect_records "$L1" 0 50 || die "the new leader lost acknowledged records"
pass "no acknowledged record lost"
for ((i = 50; i < 100; i++)); do
    off="$(produce_retry 0 "msg-$i")"
    [[ $off == "$i" ]] || die "offset of record $i after failover is [$off]"
done
pass "50 more records acknowledged after failover"

# --- the node comes back ----------------------------------------------------------------------
start_node "$L0"
wait_for "restarted node catches up (log end offset 100)" 60 col_is "$L0" 0 8 100
wait_for "restarted node is back in the ISR (all three nodes)" 60 col_is "$L1" 0 6 7
expect_records "$(leader_of 0)" 0 100 || die "records differ after rejoin"
pass "all 100 records intact after rejoin"

# --- everything down --------------------------------------------------------------------------
for n in 0 1 2; do kill_node "$n"; done
pass "kill -9 all nodes"
for n in 0 1 2; do start_node "$n"; done
wait_for "partition 0 elects a leader again" 60 stable_leader 0
wait_for "the committed prefix is intact after a full restart" 30 bash -c \
    "$(declare -f nctl expect_records); CPORT=(${CPORT[*]}); HELIXCTL=$HELIXCTL; expect_records $(leader_of 0) 0 100"
# --- no acknowledgement without a quorum ----------------------------------------------------------
# Freeze both followers of partition 1: the leader may append, but nothing can commit, so Produce
# must fail (timeout or too few in-sync replicas) instead of being acknowledged.
L2="$(leader_of 1)"
for n in 0 1 2; do
    [[ $n == "$L2" ]] || kill -STOP "${PIDS[n]}"
done
if nctl "$L2" produce events 1 never-acked >/dev/null 2>&1; then
    die "Produce was acknowledged while no follower could confirm it"
fi
pass "Produce is not acknowledged while the followers are frozen"
for n in 0 1 2; do
    [[ $n == "$L2" ]] || kill -CONT "${PIDS[n]}"
done
# The maximum public payload must fit storage AND the follower transport. This catches
# limits that pass a one-node test but fail to replicate to a real quorum.
head -c 1048576 /dev/zero | tr '\000' 'x' >"$WORKDIR/max-payload"
printf '\n' >>"$WORKDIR/max-payload"
# Metadata is a local view and leadership may still change after the frozen nodes resume.
# Retry discovery with the operation; do not assume a previous readiness check pins the leader.
# A timed-out attempt may commit too, so validate the returned offset rather than a record count.
produce_max_payload() {
    local i leader reply
    for ((i = 0; i < 300; i++)); do
        if leader="$(leader_of 1 2>/dev/null)" &&
            reply="$(nctl "$leader" produce events 1 <"$WORKDIR/max-payload" 2>/dev/null)"; then
            echo "$reply"
            return 0
        fi
        sleep 0.1
    done
    die "maximum payload was never acknowledged after followers resumed"
}
ack="$(produce_max_payload)"
off="${ack#ACK }"
[[ $off =~ ^[0-9]+$ ]] || die "maximum payload was not acknowledged: $ack"
wait_for "all replicas commit the 1 MiB record" 30 all_hw_is 1 "$((off + 1))"
{ printf '%s\t' "$off"; cat "$WORKDIR/max-payload"; } >"$WORKDIR/max-expected"
max_payload_matches() {
    local leader
    leader="$(leader_of 1)" || return 1
    nctl "$leader" fetch events 1 "$off" 1 >"$WORKDIR/max-fetched" || return 1
    cmp -s "$WORKDIR/max-expected" "$WORKDIR/max-fetched"
}
wait_for "1 MiB payload round-trips through a three-node quorum" 30 max_payload_matches

echo "cluster test passed ($PASSED checks)"
