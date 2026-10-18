# syntax=docker/dockerfile:1
#
#   docker build -t helix .                      runtime image (helixd + helixctl)
#   docker build --target ci -t helix-ci .       full quality gate (format, tidy, sanitizers, fuzz)
#   docker build --target dev -t helix-dev .     toolchain shell for development
ARG BASE=ubuntu:24.04

FROM ${BASE} AS dev
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential clang clang-tidy clang-format \
        libclang-rt-18-dev valgrind shellcheck ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src

FROM dev AS build
COPY . /src
RUN sed -i 's/\r$//' scripts/*.sh && chmod +x scripts/*.sh
RUN make clean && make all test

FROM build AS ci
ENV FUZZ_TIME=30 CRASH_ROUNDS=8
RUN make ci

# Portable Linux binaries: musl is linked statically, no installed libc required.
FROM dev AS release-build
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools file \
 && rm -rf /var/lib/apt/lists/*
COPY . /src
RUN sed -i 's/\r$//' scripts/*.sh && chmod +x scripts/*.sh
RUN make CC=musl-gcc BUILD=build/static LDHARD='-static -Wl,-z,relro,-z,now' all test \
 && HELIX_BIN_DIR=build/static bash scripts/cluster_test.sh \
 && BUILD=build/static REQUIRE_STATIC=1 bash scripts/package.sh \
 && bash scripts/release_smoke.sh

FROM scratch AS release
COPY --from=release-build /src/dist/ /

FROM ${BASE} AS runtime
RUN useradd --system --uid 10001 --home-dir /var/lib/helix --create-home helix \
 && mkdir -p /var/lib/helix/data && chown -R helix:helix /var/lib/helix
COPY --from=build /src/build/helixd /src/build/helixctl /usr/local/bin/
COPY --from=build /src/deploy/config /etc/helix
COPY --from=build /src/examples /opt/helix/examples
USER helix
VOLUME /var/lib/helix/data
EXPOSE 9400
ENTRYPOINT ["helixd"]
CMD ["--listen", "0.0.0.0", "--port", "9400", "--data-dir", "/var/lib/helix/data"]
