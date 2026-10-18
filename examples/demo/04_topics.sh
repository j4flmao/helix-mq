#!/usr/bin/env bash
# 4/5: topic lifecycle and the errors a real client must handle.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

say "4/5 - topics, retention and the error surface"

ctl delete-topic scratch >/dev/null 2>&1 || true
expect_eq "OK" "$(ctl create-topic scratch 2)" "create a topic with 2 partitions"
expect_contains "$(ctl metadata)" $'scratch\t0' "metadata lists partition 0"
expect_contains "$(ctl metadata)" $'scratch\t1' "metadata lists partition 1"

# Creating it twice is refused, not silently accepted.
err="$(ctl create-topic scratch 2 2>&1 >/dev/null || true)"
expect_contains "$err" "topic already exists" "creating the topic twice is refused"

err="$(ctl fetch scratch 99 0 2>&1 >/dev/null || true)"
expect_contains "$err" "unknown topic or partition" "fetching an unknown partition is refused"

expect_eq "OK" "$(ctl delete-topic scratch)" "delete the topic"
err="$(ctl fetch scratch 0 0 2>&1 >/dev/null || true)"
expect_contains "$err" "unknown topic or partition" "fetching a deleted topic is refused"

# The broker refuses records above max-record-bytes (1 MiB) without dying. A record
# just over the limit still fits the frame limit, so it is answered, not dropped.
ctl create-topic tiny >/dev/null 2>&1 || true
err="$({ head -c 1101004 /dev/zero | tr '\0' 'x'; echo; } | ctl produce tiny 0 2>&1 >/dev/null || true)"
expect_contains "$err" "record too large" "a 1.05 MiB record is refused (max-record-bytes is 1 MiB)"
expect_eq "PONG" "$(ctl ping)" "the broker survived the oversized record"

summary
