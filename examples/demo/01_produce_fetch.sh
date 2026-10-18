#!/usr/bin/env bash
# 1/5: produce records into a durable log and read them back by offset.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

say "1/5 - produce and fetch: the commit log"

expect_eq "PONG" "$(ctl ping)" "broker answers ping"

# A fresh topic keeps the run repeatable even against a persistent broker.
ctl delete-topic orders >/dev/null 2>&1 || true
expect_eq "OK" "$(ctl create-topic orders 4)" "create topic 'orders' with 4 partitions"

# One record per command-line argument, then several from stdin in one call.
expect_eq "ACK 0" "$(ctl produce orders 0 'first order')" "first record lands at offset 0"
printf 'second order\nthird order\n' | ctl produce orders 0 >/dev/null
ok "two more records produced from stdin"

expect_eq $'0\tfirst order\n1\tsecond order\n2\tthird order' \
    "$(ctl fetch orders 0 0)" "fetch by offset returns the records in order"

expect_contains "$(ctl metadata)" $'orders\t0\t0\t3' \
    "metadata reports first=0 next=3 for partition 0"

if [[ -n ${HELIX_DATA_DIR:-} && -d $HELIX_DATA_DIR/orders-0 ]]; then
    note "on-disk layout of partition orders-0:"
    ls -1 "$HELIX_DATA_DIR/orders-0" | sed 's/^/        /'
fi

summary
