# syntax=docker/dockerfile:1
# ──────────────────────────────────────────────────────────────────────────────
# rune-broker — multi-stage image (linux/amd64)
# Stage 1: builder  — conan install + cmake build (rune_broker target only)
# Stage 2: runtime  — slim debian image + binary + entrypoint
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# BUILDER
# Base: ubuntu:24.04 with gcc-13 + cmake + conan 2.x installed via pip.
# The profile targets: gcc/13, arch=x86_64, cppstd=gnu17, libstdc++11, Linux.
# All deps come from public ConanCenter (aeron, spdlog) — no private remote.
# ──────────────────────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# ── System packages ───────────────────────────────────────────────────────────
# ubuntu:24.04 (Noble) ships gcc-13 in its main archive; no PPA needed.
# cmake 3.28+ is available from the Noble toolchain.
# build-essential+make+ninja: the build backend CMake needs to probe the compiler
#   and build aeron/spdlog from source.
# default-jdk-headless: aeron's C++ build runs an SBE Java codegen step.
# uuid-dev/zlib1g-dev/libbsd-dev: aeron media-driver C dependencies.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc-13 g++-13 \
        build-essential make ninja-build pkg-config \
        cmake \
        python3 python3-pip \
        default-jdk-headless \
        uuid-dev zlib1g-dev libbsd-dev \
        git \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100 \
    && update-alternatives --install /usr/bin/cc  cc  /usr/bin/gcc-13 100 \
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++-13 100

# aeron's SBE codegen locates the JDK via JAVA_HOME.
ENV JAVA_HOME=/usr/lib/jvm/default-java

# ── Conan 2.x via pip ────────────────────────────────────────────────────────
# ubuntu:24.04 marks the system Python PEP-668 "externally managed"; the builder
# is a throwaway stage, so install into system site-packages directly.
RUN pip3 install --no-cache-dir --break-system-packages "conan>=2.0"

# ── Create a conan profile (gcc-13 / x86_64 / gnu17 / libstdc++11) ───────────
RUN conan profile detect --force
RUN conan profile path default && \
    printf '[settings]\nos=Linux\narch=x86_64\ncompiler=gcc\ncompiler.version=13\ncompiler.cppstd=gnu17\ncompiler.libcxx=libstdc++11\nbuild_type=Release\n' \
        > "$(conan profile path default)"

# ── Conan remote (public ConanCenter only) ───────────────────────────────────
# No private remote / credentials needed: all deps are on ConanCenter.
RUN conan remote add conancenter https://center2.conan.io --force || true

WORKDIR /build

# ── Conan install (resolve + download all deps; build from source if needed) ─
# Copy ONLY the conan metadata first so this expensive layer (aeron builds from
# source) is cached independently of application source edits.
# --build=missing: builds aeron/spdlog from source if no prebuilt Linux binary
# exists on ConanCenter (aeron pulls a JDK build-requirement transiently).
COPY conanfile.py conandata.yml VERSION.txt ./
RUN conan install . \
        --build=missing \
        -s os=Linux \
        -s arch=x86_64 \
        -s compiler=gcc \
        -s compiler.version=13 \
        -s compiler.cppstd=gnu17 \
        -s compiler.libcxx=libstdc++11 \
        -s build_type=Release

# ── Upgrade CMake for the project configure ──────────────────────────────────
# ubuntu:24.04 ships cmake 3.28; the project requires >= 3.31. Install a recent
# cmake via pip in its own layer (AFTER conan install) so the dependency-build
# cache above is preserved on rebuilds. This cmake shadows /usr/bin/cmake.
RUN pip3 install --no-cache-dir --break-system-packages "cmake>=3.31"

# ── rune-protocol sibling headers (Phase-1 sibling-include, not yet packaged) ─
# libs/storage includes <rune/protocol/RecordBatch.h>. CMake resolves
# RUNE_PROTOCOL_INCLUDE to <src>/../rune-protocol/include → /rune-protocol/include
# in this image. Supplied via a BuildKit named build context:
#   docker build --build-context rune-protocol=../rune-protocol ...
COPY --from=rune-protocol include /rune-protocol/include

# ── Application sources (copied last so edits don't bust the conan cache) ─────
COPY . .

# ── CMake configure + build (rune_broker target only; no tests, no benchmarks) ─
RUN cmake . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
        -DCMAKE_PREFIX_PATH="/build/build/Release/generators" \
        -DBUILD_TESTING=OFF \
        -DBUILD_BENCHMARKS=OFF && \
    cmake --build build --target rune_broker -j2

# ──────────────────────────────────────────────────────────────────────────────
# RUNTIME
# Base MUST match the builder's glibc/libstdc++ ABI. The builder is ubuntu:24.04
# (GLIBC 2.38 / GLIBCXX 3.4.32); an older base (e.g. debian:bookworm-slim) lacks
# those symbol versions and the binary fails to start. Only the binary +
# entrypoint land here — no toolchain, no conan cache.
# ──────────────────────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS runtime

# Runtime shared libs: C++ runtime + libuuid1 (the binary dynamically links
# libuuid for aeron's embedded driver) + procps for the healthcheck.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libstdc++6 \
        libgcc-s1 \
        libatomic1 \
        libuuid1 \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Copy the broker binary.
COPY --from=builder /build/build/rune_broker /usr/local/bin/rune_broker

# Copy the entrypoint.
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
LABEL org.opencontainers.image.title="rune-broker" \
      org.opencontainers.image.description="Rune single-node broker — embedded Aeron media driver, self-elect" \
      org.opencontainers.image.source="https://github.com/vradu/rune-broker"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/local/bin/rune_broker"]
