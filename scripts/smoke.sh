#!/usr/bin/env bash
# End-to-end smoke test: real helixd process, real TCP, real helixctl client.
#   HELIX_BIN_DIR=build scripts/smoke.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "== smoke test ($HELIX_BIN_DIR) =="
start_broker --sync interval

# ---- basic protocol ---------------------------------------------------------
assert_eq "PONG" "$(ctl ping)" "ping"

out="$( { ctl produce orders 0 hello; ctl produce orders 0 world; ctl produce orders 0 "with spaces"; } )"
assert_eq $'ACK 0\nACK 1\nACK 2' "$out" "produce returns dense offsets"
assert_eq $'0\thello\n1\tworld\n2\twith spaces' "$(ctl fetch orders 0 0)" "fetch returns what was produced"
assert_eq $'1\tworld' "$(ctl fetch orders 0 1 1)" "fetch honours offset and record limit"
assert_eq "" "$(ctl fetch orders 0 3)" "fetch at the tail is empty, not an error"

# ---- stdin streaming + partitions ------------------------------------------
seq 1 1000 | ctl produce events 1 >/dev/null
assert_eq "1000" "$(ctl fetch events 1 0 | wc -l)" "1000 records streamed over one connection"
assert_eq $'0\t1' "$(ctl fetch events 1 0 1)" "first streamed record"
assert_eq $'999\t1000' "$(ctl fetch events 1 999)" "last streamed record"
assert_fails "unknown topic or partition" "partitions are independent" -- ctl fetch events 0 0

# ---- error handling ---------------------------------------------------------
assert_fails "unknown topic or partition" "fetch of a missing topic fails" -- ctl fetch ghost 0 0
[[ ! -e $DATA_DIR/ghost-0 ]] || die "fetch created a partition directory"
pass "fetch does not create partitions"
assert_fails "invalid topic name" "path traversal rejected" -- ctl produce ../etc 0 x
assert_fails "invalid topic name" "hidden topic rejected" -- ctl produce .hidden 0 x
assert_fails "offset out of range" "offset beyond the log end" -- ctl fetch orders 0 99999
[[ -z $(ls "$DATA_DIR" | grep -E '^\.\.|etc' || true) ]] || die "traversal produced files"

# 2,000,000 B is under the frame limit but over max_record_bytes; 3,000,000 B exceeds the frame limit.
assert_fails "record too large" "oversized record rejected" -- \
    bash -c "head -c 2000000 /dev/zero | tr '\\0' a | \"$HELIXCTL\" -F \"$PORT_FILE\" produce big 0"
assert_fails "connection" "oversized frame closes the connection" -- \
    bash -c "head -c 3000000 /dev/zero | tr '\\0' a | \"$HELIXCTL\" -F \"$PORT_FILE\" produce big 0"
assert_eq "PONG" "$(ctl ping)" "broker still healthy after the abuse above"

# ---- hostile / partial wire input ------------------------------------------
exec 3<>"/dev/tcp/127.0.0.1/$(port)"
printf '\xff\xff\xff\xff' >&3
if timeout 3 cat <&3 >/dev/null; then pass "absurd frame length: server drops the connection"; else die "server kept a bad connection open"; fi
exec 3<&- 3>&-

exec 3<>"/dev/tcp/127.0.0.1/$(port)"
printf '\x00\x00\x00\x03abc' >&3 # shorter than a request header
if timeout 3 cat <&3 >/dev/null; then pass "header-less frame: server drops the connection"; else die "server kept a bad connection open"; fi
exec 3<&- 3>&-

# A valid ping frame delivered one byte at a time must still be answered correctly.
exec 3<>"/dev/tcp/127.0.0.1/$(port)"
for b in '\x00' '\x00' '\x00' '\x08' '\x00' '\x00' '\x00' '\x01' '\x00' '\x00' '\x00' '\x07'; do
    printf "$b" >&3
    sleep 0.01
done
resp="$(timeout 3 head -c 10 <&3 | od -An -tx1 | tr -d ' \n')"
exec 3<&- 3>&-
assert_eq "00000006000000070000" "$resp" "byte-at-a-time request gets a correct response"

# ---- concurrency ------------------------------------------------------------
producers=()
for i in $(seq 1 10); do
    (seq 1 500 | sed "s/^/c$i-/" | ctl produce stress 0 >/dev/null) &
    producers+=("$!")
done
# Wait for the producers only: a bare `wait` would also wait for helixd and hang.
for p in "${producers[@]}"; do
    wait "$p" || die "a concurrent producer failed"
done
assert_eq "5000" "$(ctl fetch stress 0 0 | wc -l)" "10 concurrent producers: 5000 records"
dense="$(ctl fetch stress 0 0 | cut -f1 | awk 'BEGIN{e=0} $1!=e{bad=1} {e++} END{print bad?"gaps":"dense"}')"
assert_eq "dense" "$dense" "offsets are dense and ordered under concurrency"
per_producer="$(ctl fetch stress 0 0 | cut -f2 | sed 's/-.*//' | sort | uniq -c | awk '$1!=500' | wc -l)"
assert_eq "0" "$per_producer" "every producer got all 500 records in"

# ---- large fetch through the output buffer / backpressure path --------------
ctl bench big-fetch 0 4000 1024 64 >/dev/null
assert_eq "4000" "$(ctl fetch big-fetch 0 0 | wc -l)" "4 MB fetched across multiple responses"
bench_out="$(ctl bench pipeline 0 20000 256 128)"
[[ $bench_out == bench:* ]] || die "pipelined bench failed: $bench_out"
pass "pipelined produce (depth 128) completes"

# ---- consumer groups and metadata -------------------------------------------
assert_eq "NONE" "$(ctl committed g1 orders 0)" "no committed offset yet"
assert_eq $'0\thello\n1\tworld' "$(ctl consume g1 orders 0 2)" "consume starts at the beginning"
assert_eq "2" "$(ctl committed g1 orders 0)" "consume committed the next offset"
assert_eq $'2\twith spaces' "$(ctl consume g1 orders 0)" "next consume resumes where the group stopped"
assert_eq "" "$(ctl consume g1 orders 0)" "nothing new: empty output"
assert_eq "3" "$(ctl committed g1 orders 0)" "offset unchanged when nothing was consumed"
assert_eq $'0\thello' "$(ctl consume g2 orders 0 1)" "a second group has its own position"
assert_eq "OK" "$(ctl commit g1 orders 0 1)" "explicit commit (rewind)"
assert_eq $'1\tworld\n2\twith spaces' "$(ctl consume g1 orders 0)" "rewound group re-reads from the new offset"
assert_eq "3" "$(ctl committed g1 orders 0)" "group g1 caught up again"
assert_fails "offset out of range" "commit beyond the log end is rejected" -- ctl commit g1 orders 0 99
assert_fails "unknown topic or partition" "commit for a missing partition is rejected" -- ctl commit g1 ghost 0 0
assert_fails "invalid group name" "invalid group name is rejected" -- ctl commit bad/group orders 0 0
assert_fails "invalid topic name" "internal topic is not reachable" -- ctl fetch __consumer_offsets 0 0
assert_fails "invalid topic name" "internal topic cannot be written" -- ctl produce __consumer_offsets 0 x

expected_meta=$'big-fetch\t0\t0\t4000\nevents\t1\t0\t1000\norders\t0\t0\t3\npipeline\t0\t0\t20000\nstress\t0\t0\t5000'
assert_eq "$expected_meta" "$(ctl metadata)" "metadata lists user partitions, sorted, hides internals"

# ---- durability across a graceful restart ----------------------------------
stop_broker
pass "SIGTERM shuts down cleanly with exit status 0"
start_broker --sync interval
assert_eq $'0\thello\n1\tworld\n2\twith spaces' "$(ctl fetch orders 0 0)" "data survives restart"
assert_eq "3" "$(ctl committed g1 orders 0)" "committed offsets survive restart (g1)"
assert_eq "1" "$(ctl committed g2 orders 0)" "committed offsets survive restart (g2)"
assert_eq "ACK 3" "$(ctl produce orders 0 again)" "offsets continue after restart"
assert_eq $'3	again' "$(ctl consume g1 orders 0)" "group resumes after restart and sees the new record"
assert_eq "5000" "$(ctl fetch stress 0 0 | wc -l)" "large partition survives restart"
stop_broker

# ---- connection limit -------------------------------------------------------
start_broker --max-conns 2
exec 3<>"/dev/tcp/127.0.0.1/$(port)"
exec 4<>"/dev/tcp/127.0.0.1/$(port)"
sleep 0.2
assert_fails "" "third connection is refused at --max-conns 2" -- ctl ping
exec 3<&- 3>&- 4<&- 4>&-
sleep 0.2
assert_eq "PONG" "$(ctl ping)" "capacity frees up when connections close"
stop_broker

# ---- CLI argument validation ------------------------------------------------
assert_fails "usage" "helixd rejects unknown options" -- "$HELIXD" --bogus
assert_fails "invalid --port" "helixd validates numeric options" -- "$HELIXD" --port 99999999
assert_fails "usage" "helixctl rejects bad arguments" -- "$HELIXCTL" fetch only-one-arg

echo "== smoke test passed ($PASSED checks) =="
