# syntax=docker/dockerfile:1
# ──────────────────────────────────────────────────────────────────────────────
# rune-broker — multi-arch image (linux/amd64 + linux/arm64).
# Stage 1: builder  — gcc:13 (Debian bookworm) + conan; builds the rune_broker target.
# Stage 2: runtime  — debian:bookworm-slim (SAME glibc as gcc:13) + the binary.
# The matching Debian-bookworm glibc across both stages avoids GLIBC version skew;
# only the newer libstdc++ (gcc-13) is copied into the slim runtime.
# ──────────────────────────────────────────────────────────────────────────────

# ── BUILDER ───────────────────────────────────────────────────────────────────
FROM gcc:13 AS toolchain

ARG CMAKE_VERSION=3.31.5

# gcc:13 already provides gcc-13/g++-13 + build-essential + make. Add the rest:
#   python3-pip           — conan
#   ninja-build/pkg-config— build backend
#   default-jdk-headless  — aeron's C++ build runs an SBE Java codegen step
#   uuid-dev/zlib1g-dev   — aeron media-driver C dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv git curl ninja-build pkg-config \
        default-jdk-headless uuid-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir --break-system-packages "conan~=2.0"

# aeron's SBE codegen locates the JDK via JAVA_HOME.
ENV JAVA_HOME=/usr/lib/jvm/default-java

# The project requires CMake >= 3.31; bookworm ships older.  Install the pinned
# official Kitware release into /usr/local.  Auto-detect the host arch so the
# image builds natively on both linux/amd64 and linux/arm64 (Docker Desktop on
# Apple Silicon runs linux/arm64 natively — no Rosetta emulation needed).
ARG TARGETARCH
RUN case "${TARGETARCH}" in \
        arm64) CMAKE_ARCH=aarch64 ;; \
        *)     CMAKE_ARCH=x86_64  ;; \
    esac \
    && curl -LO https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz \
    && tar -xzf cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz --strip-components=1 -C /usr/local \
    && rm cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz \
    && cmake --version

RUN conan profile detect --force

# ── Broker build ──────────────────────────────────────────────────────────────
FROM toolchain AS builder
WORKDIR /build

# ── Conan install (public ConanCenter only — the private `common` dep was dropped)
# Copy ONLY the conan metadata first so this expensive layer (aeron builds from
# source) is cached independently of application source edits.
# --build=missing builds aeron/spdlog (+ gtest/benchmark) from source when no
# prebuilt Linux binary matches the detected gcc-13 profile.
COPY conanfile.py conandata.yml VERSION.txt ./
RUN conan install . --output-folder=conan-release --build=missing -s build_type=Release

# ── rune-protocol sibling headers (Phase-1 sibling-include, not yet packaged) ─
# libs/storage includes <rune/protocol/RecordBatch.h>. CMake resolves
# RUNE_PROTOCOL_INCLUDE to <src>/../protocol/include → /rune-protocol/include
# in this image. Supplied via a BuildKit named build context:
#   docker build --build-context rune-protocol=../protocol ...
COPY --from=rune-protocol include /rune-protocol/include
# GSO-1 / A7: rune_gso + rune_driver_wake compile rune-protocol C sources (src/net).
COPY --from=rune-protocol src     /rune-protocol/src

# ── flight-recorder flight recorder sibling (compiled: src + include) ─────────────────
# The root CMakeLists wires add_library(flight_recorder ...) from ${SRC}/../flight-recorder, which
# from WORKDIR /build resolves to /flight-recorder.  Unlike rune-protocol (headers only),
# flight-recorder is COMPILED, so copy BOTH its sources and headers.  Supplied via the
# BuildKit named build context `flight-recorder` (see docker-compose additional_contexts).
COPY --from=flight-recorder src     /flight-recorder/src
COPY --from=flight-recorder include /flight-recorder/include

# ── rune-codec (0a) sibling (compiled: src + include) ─────────────────────────
# The root CMakeLists wires add_library(rune_codec ...) from ${RUNE_CODEC_ROOT}
# (default ${SRC}/../codec → /rune-codec here) and links it into rune_node.
# rune-codec is COMPILED (Serialize/Record/Validator/Generator), so copy BOTH its
# sources and headers.  Supplied via the BuildKit named build context `rune-codec`
# (see docker-compose additional_contexts).  The CMake configure below passes
# -DRUNE_CODEC_ROOT=/rune-codec so the sibling path resolves inside the image.
COPY --from=rune-codec src     /rune-codec/src
COPY --from=rune-codec include /rune-codec/include

# ── rune-crypto (client<->broker TLS/AEAD, Task 7) sibling (compiled: src+include)
# rune_crypto (Caps/Aead/TlsSession) is COMPILED into the broker: the root
# CMakeLists wires add_library(rune_crypto ...) from ${RUNE_CRYPTO_ROOT} and links
# it PUBLIC on rune_gateway → transitively into rune_node/rune_broker.  Supplied via
# the BuildKit named build context `rune-crypto`:
#   docker build --build-context rune-crypto=../crypto ...
# The configure below passes -DRUNE_CRYPTO_ROOT=/rune-crypto; OpenSSL comes from conan.
COPY --from=rune-crypto src     /rune-crypto/src
COPY --from=rune-crypto include /rune-crypto/include

# ── Application sources (copied last so edits don't bust the conan cache) ─────
COPY . .

# Optional cap on the build's parallelism (`--build-arg CMAKE_BUILD_PARALLEL_LEVEL=4`, or the same
# variable exported before `docker compose build`).  Unset = ninja's default of cores+2 jobs, which
# OOM-kills cc1plus (each big translation unit needs ~1.5 GB) in a small Docker VM.
ARG CMAKE_BUILD_PARALLEL_LEVEL

# ── Configure + build (rune_broker target only; no tests, no benchmarks) ──────
RUN cmake -S . -B build -G Ninja \
        -DCMAKE_TOOLCHAIN_FILE=conan-release/build/Release/generators/conan_toolchain.cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTING=OFF \
        -DBUILD_BENCHMARKS=OFF \
        -DBUILD_INTEGRATION_TESTING=OFF \
        -DRUNE_PROTOCOL_INCLUDE=/rune-protocol/include \
        -DRUNE_CODEC_ROOT=/rune-codec \
        -DRUNE_CRYPTO_ROOT=/rune-crypto \
        -DFLIGHT_RECORDER_DIR=/flight-recorder \
        -DRUNE_TLS=ON \
    && cmake --build build --target rune_broker rune_role

# gcc-13's libstdc++ is newer than bookworm-slim's; stage it for the runtime.
RUN cp -L /usr/local/lib64/libstdc++.so.6 /build/libstdc++.so.6

# ── RUNTIME ───────────────────────────────────────────────────────────────────
# debian:bookworm-slim shares the gcc:13 builder's glibc (2.36). Only the binary,
# the newer libstdc++, and the entrypoint land here — no toolchain, no conan cache.
FROM debian:bookworm-slim AS base

# Runtime shared libs: libgcc/atomic + libuuid1 (the binary dynamically links
# libuuid for aeron's embedded driver) + procps for the healthcheck. libstdc++6
# provides the base symlink that ldconfig repoints to the copied 6.0.32 below.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libstdc++6 \
        libgcc-s1 \
        libatomic1 \
        libuuid1 \
        procps \
    && rm -rf /var/lib/apt/lists/*

# The broker binary + the newer libstdc++ from the builder (ldconfig repoints
# the libstdc++.so.6 symlink to the higher 6.0.32 version).
COPY --from=builder /build/build/rune_broker /usr/local/bin/rune_broker
# rune_role: standalone Aeron role-counter reader — the dr-e2e harness runs it
# via `docker exec` to detect leadership from counters (not log scraping).
COPY --from=builder /build/build/rune_role /usr/local/bin/rune_role
# Multi-arch: the multiarch lib dir is x86_64-linux-gnu or aarch64-linux-gnu (= uname -m).
COPY --from=builder /build/libstdc++.so.6 /tmp/libstdc++.so.6.0.32
RUN mv /tmp/libstdc++.so.6.0.32 /usr/lib/$(uname -m)-linux-gnu/libstdc++.so.6.0.32 && ldconfig

# Entrypoint (defaults RUNE_DATA_DIR, exec's the binary as PID 1 → clean SIGTERM).
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ── Default data directory ────────────────────────────────────────────────────
RUN mkdir -p /var/lib/rune

# ── Aeron shared-memory note ──────────────────────────────────────────────────
# The embedded Aeron media driver maps CnC + log buffers into /dev/shm.
# The default container /dev/shm (64 MB) is too small.
# Always start with --shm-size=256m (or --ipc=host for trusted environments).
VOLUME ["/var/lib/rune"]

# ── Health check ─────────────────────────────────────────────────────────────
# Simple liveness probe: verify the process is alive.
# Consensus-readiness (knows-leader) is the richer probe in BR-7.
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD pgrep -x rune_broker > /dev/null || exit 1

# ── Metadata ─────────────────────────────────────────────────────────────────
LABEL org.opencontainers.image.title="rune" \
      org.opencontainers.image.description="Rune broker — durable log and database; embedded Aeron media driver, single node or cluster member" \
      org.opencontainers.image.source="https://github.com/arcanumruna/rune"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/local/bin/rune_broker"]

# ── rune CLI (release image only) ─────────────────────────────────────────────
# The published image also ships the `rune` CLI (cluster key, admin bootstrap, Rune QL shell), the
# way a database image ships its client. Built only for `--target release` (broker/build_docker.sh),
# so the default target below needs none of these extra build contexts:
#   --build-context rune-client=../client-cpp --build-context rune-ql=../ql
FROM toolchain AS cli-builder
WORKDIR /cli
COPY --from=rune-client conanfile.py conandata.yml VERSION.txt ./
RUN conan install . --output-folder=conan-release --build=missing -s build_type=Release
COPY --from=rune-protocol include /rune-protocol/include
COPY --from=rune-protocol src     /rune-protocol/src
COPY --from=rune-codec src     /rune-codec/src
COPY --from=rune-codec include /rune-codec/include
COPY --from=rune-codec tools   /rune-codec/tools
COPY --from=rune-ql src     /rune-ql/src
COPY --from=rune-ql include /rune-ql/include
COPY --from=flight-recorder src     /flight-recorder/src
COPY --from=flight-recorder include /flight-recorder/include
COPY --from=rune-crypto src     /rune-crypto/src
COPY --from=rune-crypto include /rune-crypto/include
COPY --from=rune-client . .
ARG CMAKE_BUILD_PARALLEL_LEVEL
RUN cmake -S . -B build -G Ninja \
        -DCMAKE_TOOLCHAIN_FILE=conan-release/build/Release/generators/conan_toolchain.cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTING=OFF \
        -DBUILD_BENCHMARKS=OFF \
        -DBUILD_EXAMPLES=OFF \
        -DRUNE_PROTOCOL_INCLUDE=/rune-protocol/include \
        -DRUNE_CODEC_ROOT=/rune-codec \
        -DRUNE_QL_ROOT=/rune-ql \
        -DFLIGHT_RECORDER_DIR=/flight-recorder \
        -DRUNE_CRYPTO_ROOT=/rune-crypto \
    && cmake --build build --target rune

FROM base AS release
COPY --from=cli-builder /cli/build/rune /usr/local/bin/rune

# ── Default target: the broker alone (compose files and the e2e suites build this) ──
FROM base AS runtime
