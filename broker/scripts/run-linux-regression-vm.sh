#!/usr/bin/env bash
# The Linux-only stage of the regression checkpoint (review B-I1, DR-bugs fix round 1).
#
#   scripts/run-linux-regression-vm.sh [jobs] [build-dir]
#     jobs       default 3 (the VM has 4 cores, shared with k3s)
#     build-dir  default build-fast (its CMakeCache names the sibling trees)
#
# The Mac cannot build the Linux-only tests (UDP GSO / GRO, netns sysctls), so a checkpoint is green only with
# this stage: the exact trees this build compiles are copied to the VM (k3s-node-01) and, inside the broker
# Dockerfile's builder stage, the `linux-only` ctest label runs (rune_udp_gso_tests: every UdpGsoBindings test,
# incl. the DR-BUG-MTU regressions; container with CAP_NET_ADMIN and RUNE_LINUX_GATE=1), then the bind-order
# proof (BindOrderRegression.FixedPortsBindBeforeAnyEphemeralPublicationRegression) in a container whose netns
# ephemeral range is narrowed to 40 ports (RUNE_BIND_ORDER_GATE=1).
#
#   - Refuses (exit 2) if any tree has uncommitted tracked changes, unless RUNE_ALLOW_DIRTY=1: a verdict must be
#     about committed code.
#   - Everything on the VM runs under `flock /root/vm.lock`, in RUNE_LINUX_GATE_DIR (default /root/rune-linux-gate).
#   - Never prunes; removes only its own image tag.  Exit code = the stage's verdict.
# The sanitizer checkpoint (build-asan / build-tsan, Mac) does not need this stage: GSO is not sanitizer-relevant
# on the Mac (stub bindings).
set -u
J="${1:-3}"; B="${2:-build-fast}"
VM="${RUNE_LINUX_GATE_HOST:-root@178.104.16.102}"
GD="${RUNE_LINUX_GATE_DIR:-/root/rune-linux-gate}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
CACHE="$HERE/$B/CMakeCache.txt"
[ -f "$CACHE" ] || { echo "run-linux-regression-vm: no $CACHE" >&2; exit 2; }
val() { grep -E "^$1:" "$CACHE" | head -1 | cut -d= -f2-; }
PROTO="$(cd "$(val RUNE_PROTOCOL_INCLUDE)/.." && pwd)"
declare -a NAMES=(rune-broker rune-protocol rune-client-cpp rune-codec rune-fr rune-ql rune-crypto)
declare -a TREES=("$HERE" "$PROTO" "$(val RUNE_CLIENT_ROOT)" "$(val RUNE_CODEC_ROOT)" "$(val RUNE_FR_DIR)" \
                  "$(val RUNE_QL_ROOT)" "$(val RUNE_CRYPTO_ROOT)")
for i in "${!TREES[@]}"; do
    t="${TREES[$i]}"
    [ -d "$t" ] || { echo "run-linux-regression-vm: ${NAMES[$i]} tree '$t' missing" >&2; exit 2; }
    if git -C "$t" rev-parse --git-dir >/dev/null 2>&1; then
        if [ -n "$(git -C "$t" status --porcelain --untracked-files=no)" ] && [ "${RUNE_ALLOW_DIRTY:-0}" != "1" ]; then
            echo "run-linux-regression-vm: ${NAMES[$i]} at $t has uncommitted changes — commit/stash them or set" \
                 "RUNE_ALLOW_DIRTY=1 (the verdict would not be about committed code)" >&2
            exit 2
        fi
    fi
done
SHA="$(git -C "$HERE" rev-parse --short HEAD)"
echo "== linux stage: broker $SHA, protocol $(git -C "$PROTO" rev-parse --short HEAD 2>/dev/null) -> $VM:$GD"
ssh "$VM" "mkdir -p '$GD'" || exit 2
for i in "${!TREES[@]}"; do
    t="${TREES[$i]}"; n="${NAMES[$i]}"
    rsync -a --delete --exclude 'build*' --exclude '.git' --exclude '.cache' --exclude '_build' "$t/" "$VM:$GD/$n/" || exit 2
    rev="$(git -C "$t" rev-parse HEAD 2>/dev/null || echo unknown)"
    ssh "$VM" "echo '$rev' > '$GD/$n/REVISION'" || exit 2
done
ssh "$VM" "flock -w 7200 /root/vm.lock bash -s" <<REMOTE
set -u
cd "$GD/rune-broker"
IMG="rune-linux-gate:$SHA"
DOCKER_BUILDKIT=1 docker build --platform linux/amd64 -f docker/Dockerfile --target builder \
    --build-context rune-protocol=../rune-protocol --build-context rune-fr=../rune-fr \
    --build-context rune-codec=../rune-codec --build-context rune-crypto=../rune-crypto \
    --build-arg CMAKE_BUILD_PARALLEL_LEVEL=$J -t "\$IMG" . > "$GD/builder.log" 2>&1 || { echo "builder stage failed: $GD/builder.log"; exit 2; }
CFG='cmake -S /src/rune-broker -B /src/_build -G Ninja -DCMAKE_TOOLCHAIN_FILE=/build/conan-release/build/Release/generators/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DBUILD_BENCHMARKS=OFF -DBUILD_INTEGRATION_TESTING=OFF \
  -DRUNE_PROTOCOL_INCLUDE=/src/rune-protocol/include -DRUNE_CODEC_ROOT=/src/rune-codec -DRUNE_CRYPTO_ROOT=/src/rune-crypto \
  -DRUNE_FR_DIR=/src/rune-fr -DRUNE_CLIENT_ROOT=/src/rune-client-cpp -DRUNE_QL_ROOT=/src/rune-ql -DRUNE_TLS=ON > /src/cmake.log 2>&1'
docker run --rm --name "rune-linux-gate-build-$SHA" -v "$GD:/src" "\$IMG" bash -c "\$CFG && cmake --build /src/_build -j$J --target rune_udp_gso_tests rune_catalog_replication_tests > /src/ninja.log 2>&1" \
    || { echo "build failed: $GD/cmake.log / ninja.log"; tail -20 "$GD/ninja.log"; docker rmi "\$IMG" >/dev/null 2>&1; exit 2; }
echo "== linux-only (CAP_NET_ADMIN, RUNE_LINUX_GATE=1)"
docker run --rm --name "rune-linux-gate-run-$SHA" --cap-add NET_ADMIN --shm-size=1g -e RUNE_LINUX_GATE=1 -v "$GD:/src" "\$IMG" \
    bash -c "ctest --test-dir /src/_build -L linux-only --output-on-failure"; rc1=\$?
echo "== bind order (netns ip_local_port_range 45000-45039, RUNE_BIND_ORDER_GATE=1)"
docker run --rm --name "rune-linux-gate-bind-$SHA" --sysctl net.ipv4.ip_local_port_range="45000 45039" --shm-size=1g \
    -e RUNE_BIND_ORDER_GATE=1 -v "$GD:/src" "\$IMG" \
    bash -c "/src/_build/libs/node/rune_catalog_replication_tests --gtest_filter='BindOrderRegression.FixedPortsBindBeforeAnyEphemeralPublicationRegression'"; rc2=\$?
docker rmi "\$IMG" >/dev/null 2>&1
echo "== linux stage: linux-only rc \$rc1, bind-order rc \$rc2"
[ "\$rc1" -eq 0 ] && [ "\$rc2" -eq 0 ]
REMOTE
