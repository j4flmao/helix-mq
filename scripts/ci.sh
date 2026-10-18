#!/usr/bin/env bash
# Full quality gate. Same script locally and in CI:  make ci   (or scripts/ci.sh)
#
#   SKIP_FUZZ=1      skip the fuzz step
#   FUZZ_TIME=30     fuzz seconds (default 10)
#   CRASH_ROUNDS=6   kill -9 rounds
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

: "${FUZZ_TIME:=10}"
: "${CRASH_ROUNDS:=6}"
export TMPDIR="${TMPDIR:-/tmp}"

STEP=0
step() {
    STEP=$((STEP + 1))
    echo
    echo "################ [$STEP] $* ################"
    STEP_START=$SECONDS
}
done_step() { echo "---- done in $((SECONDS - STEP_START))s"; }

need() { command -v "$1" >/dev/null || { echo "missing required tool: $1" >&2; exit 2; }; }
need gcc
need make
need clang-format
need clang-tidy
need shellcheck

step "shell syntax and correctness"
for script in scripts/*.sh examples/demo/*.sh; do bash -n "$script"; done
shellcheck -S error scripts/*.sh examples/demo/*.sh
done_step

step "format check (clang-format)"
make --no-print-directory format-check
done_step

step "static analysis (clang-tidy, warnings are errors)"
make --no-print-directory tidy
done_step

step "release build + unit tests (gcc)"
make --no-print-directory clean
make --no-print-directory all test
done_step

step "unit tests under AddressSanitizer + UBSan (gcc)"
make --no-print-directory asan
done_step

if command -v clang >/dev/null; then
    step "build + unit tests (clang)"
    make --no-print-directory CC=clang BUILD=build/clang test
    done_step
fi

step "end-to-end smoke test (release binaries)"
HELIX_BIN_DIR=build scripts/smoke.sh
done_step

step "kill -9 crash-recovery test (release binaries)"
ROUNDS="$CRASH_ROUNDS" HELIX_BIN_DIR=build scripts/crash_test.sh
ROUNDS="$CRASH_ROUNDS" HELIX_BIN_DIR=build scripts/crash_offsets.sh
done_step

step "consumer-group, topic-admin and idempotent-producer tests (release binaries)"
HELIX_BIN_DIR=build scripts/group_test.sh
HELIX_BIN_DIR=build scripts/topics_idem_test.sh
HELIX_BIN_DIR=build scripts/cluster_test.sh
done_step

step "wire-protocol regressions over real TCP (spawns the release helixd)"
make --no-print-directory protocol
done_step

step "configuration-file integration test (helixd --config)"
HELIX_BIN_DIR=build bash scripts/config_test.sh
done_step

step "end-to-end example walkthrough (examples/demo/run.sh)"
HELIX_BIN_DIR=build bash examples/demo/run.sh
done_step

step "smoke + crash + group tests against the sanitizer build"
make --no-print-directory BUILD=build asan-integration
done_step

if [[ -z ${SKIP_FUZZ:-} ]] && command -v clang >/dev/null; then
    step "fuzz the request decoder (${FUZZ_TIME}s)"
    make --no-print-directory fuzz FUZZ_TIME="$FUZZ_TIME"
    done_step
fi

echo
echo "ALL CHECKS PASSED"
