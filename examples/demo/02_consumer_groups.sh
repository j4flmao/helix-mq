#!/usr/bin/env bash
# 2/5: consumer groups: join a group, deliver records, commit offsets, resume.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

say "2/5 - consumer groups: deliver, commit, resume"

ctl delete-topic orders >/dev/null 2>&1 || true
expect_eq "OK" "$(ctl create-topic orders 4)" "topic 'orders' exists"

produce_n() { # produce_n <partition> <prefix> <count>
    local i
    for ((i = 0; i < $3; i++)); do
        echo "$2-$i"
    done | ctl produce orders "$1" >/dev/null
}
produce_n 0 order 15 || die "produce failed"

expect_eq "NONE" "$(ctl committed workers orders 0)" "a new group has no committed offset"

# One member takes every partition, fetches from the committed offsets and
# commits after delivery (deliver first, commit second: crash = redelivery).
out="$(ctl consume-group workers orders 3000 1500 100 2>/dev/null)"
expect_eq "15" "$(printf '%s\n' "$out" | count_lines)" "the group consumed all 15 records"
expect_eq "14" "$(printf '%s\n' "$out" | awk -F'\t' 'END {print $2}')" \
    "records arrive in offset order, last one is 14"
expect_eq "15" "$(ctl committed workers orders 0)" "the group committed offset 15"

# Five more records; the same group name resumes instead of replaying.
produce_n 0 rest 5 || die "produce failed"
out="$(ctl consume-group workers orders 3000 1500 100 2>/dev/null)"
expect_eq "5" "$(printf '%s\n' "$out" | count_lines)" \
    "a restarted consumer resumes at the committed offset"
expect_eq $'0\t15\trest-0' "$(printf '%s\n' "$out" | head -n 1)" \
    "the first redelivered record is offset 15"
# The group expires once its members are gone; keep one member alive to inspect it.
ctl join workers orders 30000 >/dev/null
expect_contains "$(ctl describe workers | head -n 1)" "generation=" "the group can be described"

summary
