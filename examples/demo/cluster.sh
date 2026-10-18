#!/usr/bin/env bash
# Cluster walkthrough: three real nodes, replication, leader routing and
# fail-fast on a follower.
#
#   HELIX_NODES=127.0.0.1:9400,127.0.0.1:9410,127.0.0.1:9420 \
#       examples/demo/cluster.sh
#
# The nodes must run with a --cluster-file (see examples/cluster-local.conf).
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

: "${HELIX_NODES:=127.0.0.1:9400,127.0.0.1:9410,127.0.0.1:9420}"
: "${CLUSTER_TOPIC:=events}"

IFS=',' read -r -a NODES <<<"$HELIX_NODES"
[[ ${#NODES[@]} -ge 2 ]] || die "need at least two nodes in HELIX_NODES"

node_ctl() { # node_ctl <index> <helixctl args...>
    local idx=$1 host port
    shift
    host="${NODES[idx]%:*}"
    port="${NODES[idx]#*:}"
    "$HELIXCTL" -H "$host" -P "$port" "$@"
}

say "cluster demo: ${#NODES[@]} nodes, topic '$CLUSTER_TOPIC'"

for i in "${!NODES[@]}"; do
    expect_eq "PONG" "$(node_ctl "$i" ping)" "node $i (${NODES[i]}) answers ping"
done

# Every node must agree on who leads partition 0.
leader_of() { # leader_of <partition>: agreed leader, or fail
    local p=$1 n l="" cur
    for n in "${!NODES[@]}"; do
        cur="$(node_ctl "$n" cluster-metadata 2>/dev/null |
            awk -F'\t' -v p="$p" '$1 == "part" && $3 == p {print $4}')"
        [[ -n $cur && $cur != "-" ]] || return 1
        [[ -z $l || $l == "$cur" ]] || return 1
        l="$cur"
    done
    [[ -n $l ]] && echo "$l"
}

for ((i = 0; i < 300; i++)); do
    L="$(leader_of 0 || true)"
    [[ -n $L ]] && break
    sleep 0.1
done
[[ -n $L ]] || die "the cluster never elected a leader for partition 0"
ok "all nodes agree that node $L leads partition 0"

base="$(node_ctl "$L" cluster-metadata 2>/dev/null |
    awk -F'\t' '$1 == "part" && $3 == 0 {print $8}')"
[[ $base =~ ^[0-9]+$ ]] || die "cannot read the log end offset"

for i in 0 1 2; do
    ack="$(node_ctl "$L" produce "$CLUSTER_TOPIC" 0 "cluster-record-$((base + i))")" ||
        die "produce to the leader failed"
    [[ $ack == "ACK $((base + i))" ]] || die "unexpected ack: $ack"
done
ok "the leader acknowledged three records with dense offsets"

fetch="$(node_ctl "$L" fetch "$CLUSTER_TOPIC" 0 "$base")"
expect_contains "$fetch" "$(printf '%s\t%s' "$base" "cluster-record-$base")" \
    "the leader serves the committed records"

# A follower refuses writes instead of accepting and losing them.
follower=""
for i in "${!NODES[@]}"; do
    [[ $i != "$L" ]] && { follower=$i; break; }
done
err="$(node_ctl "$follower" produce "$CLUSTER_TOPIC" 0 'wrong-node' 2>&1 >/dev/null || true)"
expect_contains "$err" "not the leader" "a follower refuses Produce with NOT_LEADER"

hw_of() { # hw_of <node index>: high watermark reported for partition 0
    node_ctl "$1" cluster-metadata 2>/dev/null |
        awk -F'\t' '$1 == "part" && $3 == 0 {print $7}'
}
# A follower learns the high watermark from the leader asynchronously, so wait
# for it to catch up instead of sampling once.
expect_hw() { # expect_hw <node index> <want> <description>
    local got i
    for i in $(seq 1 100); do
        if got="$(hw_of "$1")" && [[ $got == "$2" ]]; then
            ok "$3"
            return 0
        fi
        sleep 0.05
    done
    die "$3: want [$2], got [$got]"
}

want=$((base + 3))
for i in "${!NODES[@]}"; do
    expect_hw "$i" "$want" "node $i committed all three records (high watermark)"
done

summary
