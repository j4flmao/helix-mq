#!/usr/bin/env bash
# 5/5: one broker, two configuration formats.
#
# The same standalone broker is written twice -- once as a flat .conf and once
# as XML with groups, attributes and an <include> -- and both files are fed to
# `helixd --config`. The step proves the settings were read: the 64 KiB record
# limit that only the file sets is enforced, data-dir is honoured, and a broken
# file stops the daemon at startup instead of being ignored.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

say "5/5 - configuration files: flat .conf and XML with an <include>"

DEMO_CONFIG="$REPO_ROOT/examples/config"

# The daemon these checks configure themselves. The walkthrough's broker is not
# reused, because the point is the file, not the running server.
HELIXD_05="${HELIXD:-}"
if [[ -z $HELIXD_05 ]]; then
    for cand in "$REPO_ROOT/${HELIX_BIN_DIR:-build}/helixd" "$REPO_ROOT/dist/helixd"; do
        [[ -x $cand ]] && { HELIXD_05=$cand; break; }
    done
fi
: "${HELIXD_05:=helixd}"
command -v "$HELIXD_05" >/dev/null 2>&1 || die "helixd not found: run 'make' or set HELIXD"

WORK="$(mktemp -d)"
PID_05=""
# Run from an EXIT trap: keep every branch explicit, never let the trap's last
# command fail (with 'set -e' that would replace a successful exit).
cleanup_05() {
    if [[ -n $PID_05 ]]; then
        kill -TERM "$PID_05" 2>/dev/null || true
        wait "$PID_05" 2>/dev/null || true
    fi
    rm -rf "$WORK" || true
}
trap cleanup_05 EXIT

# check_format <label> <config file>
# Start one instance from that file and assert the file's settings are in effect.
check_format() {
    local label=$1 file=$2
    local work="$WORK/$label"
    local portfile="$work/standalone.port"
    local log="$work/helixd.log"
    mkdir -p "$work"

    HELIX_DEMO_WORK="$work" "$HELIXD_05" --config "$file" 2>>"$log" &
    PID_05=$!
    local i
    for ((i = 0; i < 200; i++)); do
        [[ -s $portfile ]] && break
        kill -0 "$PID_05" 2>/dev/null || {
            cat "$log" >&2
            die "$label: helixd exited during startup"
        }
        sleep 0.05
    done
    [[ -s $portfile ]] || {
        cat "$log" >&2
        die "$label: helixd never wrote $portfile"
    }

    export HELIX_HOST=127.0.0.1
    export HELIX_PORT
    HELIX_PORT="$(cat "$portfile")"
    expect_eq "PONG" "$(ctl ping)" "$label: broker answers on the configured port"

    [[ -d $work/standalone-data ]] || die "$label: data-dir was not created"
    ok "$label: data-dir is the one named in the file"

    expect_eq "OK" "$(ctl create-topic records 1)" "$label: create a topic"
    head -c 65536 /dev/zero | tr '\000' 'x' >"$work/exact"
    expect_eq "ACK 0" "$(ctl produce records 0 <"$work/exact")" \
        "$label: a 64 KiB record is accepted (the limit set by the file)"
    head -c 65537 /dev/zero | tr '\000' 'x' >"$work/over"
    local err
    err="$(ctl produce records 0 <"$work/over" 2>&1 >/dev/null || true)"
    expect_contains "$err" "record too large" "$label: one byte over the limit is refused"

    kill -TERM "$PID_05" 2>/dev/null || true
    wait "$PID_05" 2>/dev/null || true
    PID_05=""
}

check_format flat "$DEMO_CONFIG/standalone.conf"
check_format xml "$DEMO_CONFIG/standalone.xml"

# A typo in a configuration file stops the daemon and names the file, the line
# and the option, so the failure is a one-line fix rather than a bisect.
refuse() { # refuse <description> <file> <needle>
    local description=$1 file=$2 needle=$3 err
    if err="$("$HELIXD_05" --config "$file" 2>&1 >/dev/null)"; then
        die "$description: a broken configuration was accepted"
    fi
    expect_contains "$err" "$needle" "$description"
}

printf 'port = nine thousand\n' >"$WORK/bad.conf"
refuse "flat .conf: the bad option is reported" "$WORK/bad.conf" "invalid --port"
refuse "flat .conf: the file is named" "$WORK/bad.conf" "bad.conf"
printf '<helix><portt>9400</portt></helix>\n' >"$WORK/bad.xml"
refuse "XML: an unknown element is an option typo" "$WORK/bad.xml" "unknown option 'portt'"
refuse "XML: the file and line are named" "$WORK/bad.xml" "bad.xml:1:"

summary
