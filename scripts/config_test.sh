#!/usr/bin/env bash
# helixd --config, end to end:
#   1. a plain configuration file starts a working broker (port-file, data-dir,
#      limits and log level all come from the file);
#   2. command-line flags win over the file;
#   3. a broken file is refused with its file:line, never half-applied;
#   4. three daemons configured only by files form a real cluster.
#
#   HELIX_BIN_DIR=build scripts/config_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "== config test ($HELIX_BIN_DIR) =="

wait_file() { # wait_file <path>: print the contents once the file is non-empty
    local f=$1 i
    for ((i = 0; i < 200; i++)); do
        if [[ -s $f ]]; then
            cat "$f"
            return 0
        fi
        sleep 0.05
    done
    return 1
}

stop_pid() { # stop_pid <pid>: SIGTERM and require a clean exit
    local pid=$1 rc=0
    kill -TERM "$pid"
    wait "$pid" || rc=$?
    [[ $rc -eq 0 ]] || die "helixd exited with status $rc after SIGTERM (expected 0)"
}

# The generic lib.sh `ctl` always dials -F; this daemon is addressed by port.
on_port() { # on_port <port> <helixctl args...>
    local p=$1
    shift
    "$HELIXCTL" -P "$p" "$@"
}

# ---- 1. a configuration file on its own ---------------------------------------------------
CONF="$WORKDIR/helixd.conf"
DATA_CFG="$WORKDIR/data-from-config"
PORT_CFG="$WORKDIR/port-from-config"
cat >"$CONF" <<EOF
# Every key below must reach the running daemon.
listen          = 127.0.0.1
port            = 0
port-file       = $PORT_CFG
data-dir        = $DATA_CFG
sync            = interval
sync-interval-ms = 50
max-conns       = 128
max-groups      = 64
log-level       = warn
# Socket tuning keys are configuration-only: no flag sets them.
listen-backlog     = 128
tcp-nodelay        = 1
recv-buffer-bytes  = 262144
send-buffer-bytes  = 262144
max-out-pending    = 131072
EOF

"$HELIXD" --config "$CONF" 2>>"$WORKDIR/config.log" &
CFG_PID=$!
port="$(wait_file "$PORT_CFG")" || die "the port file from the configuration was never written"
pass "port-file written from the configuration file"

assert_eq "PONG" "$(on_port "$port" ping)" "daemon starts from the configuration file"
pass "socket tuning keys (backlog, nodelay, buffers, out-pending) are accepted"
assert_eq "ACK 0" "$(on_port "$port" produce orders 0 hello)" "produce on a config-driven daemon"
assert_eq $'0\thello' "$(on_port "$port" fetch orders 0 0)" "fetch returns the produced record"
[[ -d $DATA_CFG/orders-0 ]] || die "data-dir from the configuration was ignored"
pass "data-dir from the configuration file holds the partition"
stop_pid "$CFG_PID"
CFG_PID=""

# ---- 2. the command line overrides the file ------------------------------------------------
CONF_OVR="$WORKDIR/override.conf"
DATA_FILE="$WORKDIR/data-file-must-stay-empty"
PORT_FILE_CFG="$WORKDIR/port-file-must-stay-empty"
DATA_CLI="$WORKDIR/data-from-cli"
PORT_CLI="$WORKDIR/port-from-cli"
cat >"$CONF_OVR" <<EOF
port      = 0
port-file = $PORT_FILE_CFG
data-dir  = $DATA_FILE
log-level = warn
EOF

"$HELIXD" --config "$CONF_OVR" --data-dir "$DATA_CLI" --port-file "$PORT_CLI" \
    2>>"$WORKDIR/config.log" &
CFG_PID=$!
port="$(wait_file "$PORT_CLI")" || die "--port-file did not override the configuration file"
[[ ! -e $PORT_FILE_CFG ]] || die "the port file from the configuration was written anyway"
pass "--port-file on the command line wins over the file"
assert_eq "PONG" "$(on_port "$port" ping)" "daemon runs with the overridden options"
assert_eq "ACK 0" "$(on_port "$port" produce orders 0 hello)" "produce with overridden data-dir"
[[ -d $DATA_CLI/orders-0 ]] || die "--data-dir did not override the configuration file"
[[ ! -e $DATA_FILE ]] || die "the data-dir from the configuration was used anyway"
pass "--data-dir on the command line wins over the file"
stop_pid "$CFG_PID"
CFG_PID=""

# ---- 3. broken files are refused, with file and line ---------------------------------------
BAD="$WORKDIR/bad.conf"
printf 'listen = 127.0.0.1\nbogus = 1\nport = 0\n' >"$BAD"
assert_fails "$BAD:2: unknown option 'bogus'" "unknown option names its line" -- \
    "$HELIXD" --config "$BAD"

printf 'port = 0\nmax-conns = 99999999\n' >"$BAD"
assert_fails "$BAD:2: invalid --max-conns" "out-of-range value names its line" -- \
    "$HELIXD" --config "$BAD"

printf 'max-groups = 0\n' >"$BAD"
assert_fails "value for 'max-groups' out of range" "broker limit range is enforced" -- \
    "$HELIXD" --config "$BAD"

printf 'listen-backlog = 0\n' >"$BAD"
assert_fails "value for 'listen-backlog' out of range" "socket tuning range is enforced" -- \
    "$HELIXD" --config "$BAD"

printf 'tcp-nodelay = yes\n' >"$BAD"
assert_fails "value for 'tcp-nodelay' must be 0 or 1" "tcp-nodelay is a 0/1 flag" -- \
    "$HELIXD" --config "$BAD"

printf 'data-dir = ${HELIX_TEST_UNDEFINED}\n' >"$BAD"
assert_fails "undefined variable 'HELIX_TEST_UNDEFINED'" "undefined \${VAR} is an error, not empty" -- \
    "$HELIXD" --config "$BAD"

printf 'sync = always\nbroken\n' >"$BAD"
assert_fails "$BAD:2: missing value" "a setting without a value names its line" -- \
    "$HELIXD" --config "$BAD"

assert_fails "--config needs a file" "--config without an argument is rejected" -- \
    "$HELIXD" --config
assert_fails "No such file or directory" "a missing configuration file is rejected" -- \
    "$HELIXD" --config "$WORKDIR/does-not-exist.conf"
assert_fails "invalid --port" "command-line validation keeps its message" -- \
    "$HELIXD" --port 99999999

# ---- 4. a three-node cluster configured only by files ---------------------------------------
BASE=$((24000 + ($$ % 800) * 10))
CDIR="$WORKDIR/cluster-conf"
mkdir -p "$CDIR"
CONF_CL="$CDIR/cluster.conf"
: >"$CONF_CL"
declare -a CPORT CPID
for n in 0 1 2; do
    b=$((BASE + n * 4))
    CPORT[n]=$b
    echo "node $n 127.0.0.1 $b $((b + 1)) $((b + 2)) $((b + 3))" >>"$CONF_CL"
done
echo "topic events 2" >>"$CONF_CL"

for n in 0 1 2; do
    cat >"$CDIR/node$n.conf" <<EOF
# Node $n: identity and membership come from files, not from flags.
cluster-file   = $CONF_CL
node-id        = $n
data-dir       = $WORKDIR/cn$n
sync           = interval
ack-timeout-ms = 3000
log-level      = warn
EOF
    "$HELIXD" --config "$CDIR/node$n.conf" 2>>"$WORKDIR/cluster$n.log" &
    CPID[n]=$!
done
for n in 0 1 2; do
    for ((i = 0; i < 200; i++)); do
        if "$HELIXCTL" -P "${CPORT[n]}" ping >/dev/null 2>&1; then
            break
        fi
        kill -0 "${CPID[n]}" 2>/dev/null || die "node $n exited during startup"
        sleep 0.05
    done
    "$HELIXCTL" -P "${CPORT[n]}" ping >/dev/null 2>&1 || die "node $n never answered ping"
done
pass "three file-configured daemons are up"

nctl() { # nctl <node> <helixctl args...>
    local n=$1
    shift
    "$HELIXCTL" -P "${CPORT[n]}" "$@"
}
live_nodes() {
    local n
    for n in 0 1 2; do
        [[ -n ${CPID[n]:-} ]] && echo "$n"
    done
    return 0
}
leader_of() { # leader_of <partition>: the leader every live node agrees on, else fails
    local p=$1 n l="" cur
    for n in $(live_nodes); do
        cur="$(nctl "$n" cluster-metadata 2>/dev/null | awk -F'\t' -v p="$p" \
            '$1=="part" && $3==p {print $4}')"
        [[ -n $cur && $cur != "-" ]] || return 1
        [[ -z $l || $l == "$cur" ]] || return 1
        l="$cur"
    done
    [[ -n $l ]] && echo "$l"
}
hw_is() { # hw_is <partition> <value>: every live node reports that high watermark
    local p=$1 want=$2 n
    for n in $(live_nodes); do
        nctl "$n" cluster-metadata 2>/dev/null | awk -F'\t' -v p="$p" '$1=="part" && $3==p {print $7}' \
            | grep -qx "$want" || return 1
    done
    return 0
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

wait_for "file-configured cluster elects a leader for partition 0" 30 leader_of 0
L="$(leader_of 0)"
out=""
for i in 0 1 2; do
    ack="$(nctl "$L" produce events 0 "cfg-$i")" || die "produce to leader node $L failed"
    out+="${ack#ACK }"$'\n'
done
assert_eq $'0\n1\n2' "${out%$'\n'}" "records are acknowledged with dense offsets"
assert_eq "$(printf '0\tcfg-0\n1\tcfg-1\n2\tcfg-2')" \
    "$(nctl "$L" fetch events 0 0)" "the leader serves the acknowledged records"
wait_for "every node commits the three records" 30 hw_is 0 3

# Teardown: a losing quorum may deliberately stop its daemons, so this section
# tears the cluster down abruptly (the graceful path is checked above).
for n in 0 1 2; do
    kill -KILL "${CPID[n]}" 2>/dev/null || true
    wait "${CPID[n]}" 2>/dev/null || true
    CPID[n]=""
done
pass "file-configured cluster torn down"

echo "config test passed ($PASSED checks)"
