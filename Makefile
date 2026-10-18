# Helix build system. Linux only.
#
#   make                 release build of helixd + helixctl
#   make test            unit tests (storage, codec, broker)
#   make asan            unit tests under AddressSanitizer + UBSan
#   make smoke           end-to-end network smoke test (needs build)
#   make crash           kill -9 crash-recovery test (needs build)
#   make groups          consumer-group, topic-admin and idempotent-producer tests (needs build)
#   make asan-integration smoke + crash tests against the sanitizer build
#   make tidy            clang-tidy (warnings are errors)
#   make format          apply clang-format in place
#   make format-check    fail if any file is not formatted
#   make fuzz            libFuzzer on the request decoder (clang), FUZZ_TIME seconds
#   make bench           append + network benchmarks
#   make ci              everything CI runs

CC        ?= gcc
VERSION   := $(shell cat VERSION)
BUILD     ?= build
SAN       ?=
OPT       ?= -O2
FUZZ_TIME ?= 20

CSTD  := -std=c11 -D_GNU_SOURCE
WARN  := -Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion -Wno-sign-conversion \
         -Wformat=2 -Wstrict-prototypes -Wmissing-prototypes -Wcast-align -Wwrite-strings \
         -Wvla -Wundef -Wpointer-arith
HARDEN := -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIE
LDHARD := -pie -Wl,-z,relro,-z,now
CFLAGS_ALL := $(CSTD) $(WARN) $(HARDEN) $(OPT) -g $(SAN) -Isrc -DHELIX_VERSION='"$(VERSION)"' $(CFLAGS)
LDFLAGS_ALL := $(LDHARD) $(SAN) $(LDFLAGS)

OBJ := $(BUILD)/obj

SRC_STORAGE := src/storage/log.c src/storage/crc32c.c
SRC_UTIL    := src/util/buf.c src/util/logger.c src/util/conf.c src/util/confxml.c
SRC_PROTO   := src/proto/codec.c
SRC_BROKER  := src/broker/broker.c src/broker/groups.c src/broker/producers.c
SRC_REPL    := src/repl/repl.c src/repl/repl_msg.c
SRC_PLOG    := src/repl/repl_log.c
SRC_RNET    := src/repl/repl_net.c
SRC_RMGR    := src/repl/replica_mgr.c
SRC_CTRL    := src/ctrl/ctrl.c
SRC_CHOST   := src/ctrl/ctrl_host.c
SRC_CNET    := src/ctrl/ctrl_net.c
SRC_RAFT    := src/raft/raft.c src/raft/raft_msg.c src/raft/raft_wal.c src/raft/raft_net.c
SRC_NET     := src/net/server.c
SRC_CLUSTER := src/broker/cluster.c
SRC_LIB     := $(SRC_UTIL) $(SRC_STORAGE) $(SRC_PROTO) $(SRC_BROKER) $(SRC_NET)
SRC_TOOLS   := src/helixd.c tools/helixctl.c
SRC_TESTS   := tests/test_log.c tests/test_broker.c tests/test_raft.c tests/test_raft_wal.c tests/test_raft_net.c tests/test_repl.c tests/test_plog.c tests/test_replnet.c tests/test_cluster.c tests/test_ctlcli.c tests/test_clspec.c tests/test_ctrl.c tests/test_chost.c tests/test_conf.c tests/test_confxml.c tests/test_protocol.c tests/fuzz_broker.c tests/fuzz_raft.c tests/fuzz_repl.c tests/fuzz_ctrl.c
ALL_C       := $(SRC_LIB) $(SRC_CLUSTER) $(SRC_REPL) $(SRC_PLOG) $(SRC_RNET) $(SRC_RMGR) $(SRC_CTRL) $(SRC_CHOST) $(SRC_CNET) $(SRC_RAFT) $(SRC_TOOLS) $(SRC_TESTS)
ALL_H       := $(wildcard src/*/*.h)

O = $(patsubst %.c,$(OBJ)/%.o,$(1))

all: $(BUILD)/helixd $(BUILD)/helixctl

$(OBJ)/%.o: %.c VERSION
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS_ALL) -MMD -MP -c $< -o $@

$(BUILD)/helixd: $(call O,$(SRC_LIB) $(SRC_CLUSTER) $(SRC_REPL) $(SRC_PLOG) $(SRC_RNET) $(SRC_RMGR) $(SRC_CTRL) $(SRC_CHOST) $(SRC_CNET) $(SRC_RAFT) src/helixd.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

# helixctl only needs the wire codec and buffers.
$(BUILD)/helixctl: $(call O,tools/helixctl.c src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_log: $(call O,tests/test_log.c $(SRC_STORAGE))
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_conf: $(call O,tests/test_conf.c src/util/conf.c src/util/confxml.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_confxml: $(call O,tests/test_confxml.c src/util/conf.c src/util/confxml.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

# Spawns the real daemon over TCP, so it needs no project objects of its own.
$(BUILD)/test_protocol: $(call O,tests/test_protocol.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_broker: $(call O,tests/test_broker.c $(filter-out $(SRC_NET),$(SRC_LIB)))
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_raft: $(call O,tests/test_raft.c $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_raft_wal: $(call O,tests/test_raft_wal.c $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_raft_net: $(call O,tests/test_raft_net.c $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_repl: $(call O,tests/test_repl.c $(SRC_REPL) $(SRC_PLOG) $(SRC_STORAGE) $(SRC_CTRL) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_ctrl: $(call O,tests/test_ctrl.c $(SRC_CTRL) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_chost: $(call O,tests/test_chost.c $(SRC_CTRL) $(SRC_CHOST) $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_plog: $(call O,tests/test_plog.c $(SRC_PLOG) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_replnet: $(call O,tests/test_replnet.c $(SRC_REPL) $(SRC_PLOG) $(SRC_RNET) $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_cluster: $(call O,tests/test_cluster.c $(SRC_REPL) $(SRC_PLOG) $(SRC_RNET) $(SRC_RMGR) $(SRC_CTRL) $(SRC_CHOST) $(SRC_CNET) $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_ctlcli: $(call O,tests/test_ctlcli.c $(SRC_CTRL) $(SRC_CHOST) $(SRC_CNET) $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

$(BUILD)/test_clspec: $(call O,tests/test_clspec.c $(SRC_CLUSTER) $(SRC_REPL) $(SRC_PLOG) $(SRC_RNET) $(SRC_RMGR) $(SRC_CTRL) $(SRC_CHOST) $(SRC_CNET) $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/util/logger.c src/proto/codec.c)
	$(CC) -o $@ $^ $(LDFLAGS_ALL)

test: $(BUILD)/test_clspec $(BUILD)/test_ctlcli $(BUILD)/test_cluster $(BUILD)/test_replnet $(BUILD)/test_plog $(BUILD)/test_chost $(BUILD)/test_ctrl $(BUILD)/test_log $(BUILD)/test_conf $(BUILD)/test_confxml $(BUILD)/test_broker $(BUILD)/test_raft $(BUILD)/test_raft_wal $(BUILD)/test_raft_net $(BUILD)/test_repl
	$(BUILD)/test_log
	$(BUILD)/test_conf
	$(BUILD)/test_confxml
	$(BUILD)/test_broker
	$(BUILD)/test_raft
	$(BUILD)/test_raft_wal
	$(BUILD)/test_raft_net
	$(BUILD)/test_repl
	$(BUILD)/test_ctrl
	$(BUILD)/test_chost
	$(BUILD)/test_plog
	$(BUILD)/test_replnet
	$(BUILD)/test_cluster
	$(BUILD)/test_ctlcli
	$(BUILD)/test_clspec

# Many more random fault-injection scenarios than the default `make test`.
repl-stress: $(BUILD)/test_repl
	HX_REPL_SEEDS=$${HX_REPL_SEEDS:-3000} $(BUILD)/test_repl

chost-stress: $(BUILD)/test_chost
	HX_CHOST_SEEDS=$${HX_CHOST_SEEDS:-1500} $(BUILD)/test_chost

raft-stress: $(BUILD)/test_raft
	HX_RAFT_SEEDS=$${HX_RAFT_SEEDS:-3000} $(BUILD)/test_raft

SAN_FLAGS := -fsanitize=address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer

asan:
	$(MAKE) BUILD=$(BUILD)/asan SAN="$(SAN_FLAGS)" OPT=-O1 test

asan-integration:
	$(MAKE) BUILD=$(BUILD)/asan SAN="$(SAN_FLAGS)" OPT=-O1 all $(BUILD)/asan/test_protocol
	HELIX_BIN_DIR=$(BUILD)/asan scripts/smoke.sh
	ROUNDS=$${ROUNDS:-4} HELIX_BIN_DIR=$(BUILD)/asan scripts/crash_test.sh
	HELIX_BIN_DIR=$(BUILD)/asan scripts/cluster_test.sh
	ROUNDS=$${ROUNDS:-3} HELIX_BIN_DIR=$(BUILD)/asan scripts/crash_offsets.sh
	HELIX_BIN_DIR=$(BUILD)/asan scripts/group_test.sh
	HELIX_BIN_DIR=$(BUILD)/asan scripts/topics_idem_test.sh
	HELIX_BIN_DIR=$(BUILD)/asan $(BUILD)/asan/test_protocol

protocol: all $(BUILD)/test_protocol
	HELIX_BIN_DIR=$(BUILD) $(BUILD)/test_protocol

# Configuration-file integration test (scripts/config_test.sh needs a real daemon).
config: all
	HELIX_BIN_DIR=$(BUILD) bash scripts/config_test.sh

# End-to-end examples against a local daemon (examples/demo/run.sh).
demo: all
	HELIX_BIN_DIR=$(BUILD) bash examples/demo/run.sh

dist: all
	BUILD=$(BUILD) bash scripts/package.sh

.PHONY: protocol dist config demo

smoke: all
	HELIX_BIN_DIR=$(BUILD) scripts/smoke.sh

crash: all
	HELIX_BIN_DIR=$(BUILD) scripts/crash_test.sh
	HELIX_BIN_DIR=$(BUILD) scripts/crash_offsets.sh

groups: all
	HELIX_BIN_DIR=$(BUILD) scripts/group_test.sh
	HELIX_BIN_DIR=$(BUILD) scripts/topics_idem_test.sh

bench: all $(BUILD)/test_log
	$(BUILD)/test_log --bench
	HELIX_BIN_DIR=$(BUILD) scripts/bench.sh

tidy:
	clang-tidy --quiet $(ALL_C) -- $(CSTD) -Isrc

format:
	clang-format -i $(ALL_C) $(ALL_H)

format-check:
	clang-format --dry-run -Werror $(ALL_C) $(ALL_H)

# libFuzzer needs clang. Seeds are valid requests so the fuzzer starts deep in the parser.
fuzz:
	@command -v clang >/dev/null || { echo "clang not found; skipping fuzz"; exit 0; }
	@mkdir -p $(BUILD)/fuzz $(BUILD)/fuzz_corpus
	@printf '\000\000\000\001\000\000\000\001' > $(BUILD)/fuzz_corpus/ping.bin
	@printf '\000\001\000\001\000\000\000\001\000\004test\000\000\000\000\000\000\000\005hello' > $(BUILD)/fuzz_corpus/produce.bin
	@printf '\000\002\000\001\000\000\000\001\000\004test\000\000\000\000\000\000\000\000\000\000\000\000\000\000\001\000' > $(BUILD)/fuzz_corpus/fetch.bin
	@printf '\000\003\000\001\000\000\000\001' > $(BUILD)/fuzz_corpus/metadata.bin
	@printf '\000\004\000\001\000\000\000\001\000\002g1\000\004test\000\000\000\000\000\000\000\000\000\000\000\000' > $(BUILD)/fuzz_corpus/ocommit.bin
	@printf '\000\005\000\001\000\000\000\001\000\002g1\000\004test\000\000\000\000' > $(BUILD)/fuzz_corpus/ofetch.bin
	@printf '\000\006\000\001\000\000\000\001\000\002g1\000\000\000\000\007\320\000\001\000\004test' > $(BUILD)/fuzz_corpus/join.bin
	@printf '\000\007\000\001\000\000\000\001\000\002g1\000\003m1x\000\000\000\001' > $(BUILD)/fuzz_corpus/hb.bin
	@printf '\000\011\000\001\000\000\000\001\000\002g1' > $(BUILD)/fuzz_corpus/describe.bin
	@printf '\000\012\000\001\000\000\000\001\000\002g1\000\003m1x\000\000\000\001\000\004test\000\000\000\000\000\000\000\000\000\000\000\005' > $(BUILD)/fuzz_corpus/gcommit.bin
	@printf '\000\013\000\001\000\000\000\001' > $(BUILD)/fuzz_corpus/initprod.bin
	@printf '\000\014\000\001\000\000\000\001\000\000\000\000\000\020\000\001\000\000\000\000\000\004test\000\000\000\000\000\000\000\002hi' > $(BUILD)/fuzz_corpus/iproduce.bin
	@printf '\000\015\000\001\000\000\000\001\000\004test\000\000\000\003' > $(BUILD)/fuzz_corpus/createtopic.bin
	@printf '\000\016\000\001\000\000\000\001\000\004test' > $(BUILD)/fuzz_corpus/deletetopic.bin
	clang $(CSTD) -g -O1 -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=undefined \
		-Isrc -o $(BUILD)/fuzz/fuzz_broker tests/fuzz_broker.c \
		$(filter-out $(SRC_NET),$(SRC_LIB))
	$(BUILD)/fuzz/fuzz_broker -max_total_time=$(FUZZ_TIME) -max_len=4096 $(BUILD)/fuzz_corpus
	@mkdir -p $(BUILD)/fuzz_raft_corpus
	@{ printf '\000\041\001\000\000\000\002\000\000\000\001\000\000\000\000\000\000\000\005'; head -c 16 /dev/zero; } > $(BUILD)/fuzz_raft_corpus/prevote.bin
	@{ printf '\000\102\005\000\000\000\000\000\000\000\001\000\000\000\000\000\000\000\002'; head -c 24 /dev/zero; printf '\000\000\000\001\000\000\000\000\000\000\000\002\000\000\000\001a'; } > $(BUILD)/fuzz_raft_corpus/append.bin
	clang $(CSTD) -g -O1 -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=undefined \
		-Isrc -o $(BUILD)/fuzz/fuzz_raft tests/fuzz_raft.c $(SRC_RAFT) $(SRC_STORAGE) src/util/buf.c src/proto/codec.c
	$(BUILD)/fuzz/fuzz_raft -max_total_time=$(FUZZ_TIME) -max_len=4096 $(BUILD)/fuzz_raft_corpus
	@mkdir -p $(BUILD)/fuzz_repl_corpus
	@{ printf '\000\006\360\002\000\007\001'; printf '\000\040\001\000\000\000\000\000\000\000\002\000\000\000\001\000\000\000\000\000\000\000\005\000\000\000\001'; } > $(BUILD)/fuzz_repl_corpus/fetch.bin
	clang $(CSTD) -g -O1 -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=undefined \
		-Isrc -o $(BUILD)/fuzz/fuzz_repl tests/fuzz_repl.c $(SRC_REPL) src/util/buf.c src/proto/codec.c
	$(BUILD)/fuzz/fuzz_repl -max_total_time=$(FUZZ_TIME) -max_len=4096 $(BUILD)/fuzz_repl_corpus
	@mkdir -p $(BUILD)/fuzz_ctrl_corpus
	@printf '\000\000\005\001\000\000\000\000\000\000\006\002\000\000\000\000' > $(BUILD)/fuzz_ctrl_corpus/cmds.bin
	clang $(CSTD) -g -O1 -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=undefined \
		-Isrc -o $(BUILD)/fuzz/fuzz_ctrl tests/fuzz_ctrl.c $(SRC_CTRL) src/util/buf.c src/proto/codec.c
	$(BUILD)/fuzz/fuzz_ctrl -max_total_time=$(FUZZ_TIME) -max_len=4096 $(BUILD)/fuzz_ctrl_corpus

ci:
	scripts/ci.sh

docker:
	docker build -t helix .

docker-ci:
	docker build --target ci -t helix-ci .

docker-dev:
	docker build --target dev -t helix-dev .
	docker run --rm -it -v $(CURDIR):/src helix-dev bash

clean:
	rm -rf build

-include $(shell find $(BUILD) -name '*.d' 2>/dev/null)

.PHONY: chost-stress repl-stress raft-stress all test asan asan-integration smoke crash groups bench tidy format format-check fuzz ci docker docker-ci docker-dev clean
