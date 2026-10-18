#!/usr/bin/env bash
# 3/5: idempotent producers: a retried sequence number is acknowledged, not stored twice.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

say "3/5 - idempotent producer: retries collapse into one record"

ctl delete-topic payments >/dev/null 2>&1 || true
expect_eq "OK" "$(ctl create-topic payments 1)" "topic 'payments' exists"

pid="$(ctl init-producer)"
[[ $pid =~ ^[0-9]+$ ]] || die "init-producer printed no producer id: [$pid]"
ok "init-producer returned producer id $pid"

expect_eq "ACK 0" "$(ctl iproduce "$pid" 0 payments 0 'payment #1')" "sequence 0 lands at offset 0"
expect_eq "ACK 1" "$(ctl iproduce "$pid" 1 payments 0 'payment #2')" "sequence 1 lands at offset 1"
# The classic failure mode: the client timed out and repeats sequence 1.
expect_eq "ACK 1" "$(ctl iproduce "$pid" 1 payments 0 'payment #2')" \
    "re-sending sequence 1 is acknowledged but not stored twice"
expect_eq $'0\tpayment #1\n1\tpayment #2' "$(ctl fetch payments 0 0)" \
    "the log holds each record exactly once"

# A whole stream in one call; helixctl opens its own producer and numbers the records.
printf 'a\nb\nc\n' | ctl produce-idem payments 0 >/dev/null
expect_eq $'2\ta\n3\tb\n4\tc' "$(ctl fetch payments 0 2)" \
    "produce-idem appends the stream after the deduplicated records"

summary
