#!/usr/bin/env bash
# The Linux-only stage of the regression checkpoint (review B-I1, DR-bugs fix round 1; isolation: re-check I3).
#
#   scripts/run-linux-regression-vm.sh [jobs] [build-dir] [ctest-filter]
#     jobs       default 3 (the VM has 4 cores, shared with k3s)
#     build-dir  default $RUNE_BUILD_DIR, else build-fast under broker/, else build/root at the monorepo root.
#                Either a broker-only build (cmake -S broker) or the monorepo root build (cmake -S . -B build/root):
#                both cache the same RUNE_*_ROOT / FLIGHT_RECORDER_DIR variables that name the trees to copy.  A
#                relative path is tried against the cwd, broker/ and the monorepo root, in that order.
#     ctest-filter  default empty.  ISR v3 Step 10 (CP2): a ctest -R regex.  When set, the stage also configures
#                BUILD_INTEGRATION_TESTING=ON, builds rune_consensus_candidacy_tests, rune_client_broker_tests and
#                rune_node_tests (exit 2, naming the target, when one does not exist) and runs
#                `ctest -R <filter>` in a third container; its verdict joins the stage's.
#
# The Mac cannot build the Linux-only tests (UDP GSO / GRO, netns sysctls), so a checkpoint is green only with
# this stage: the exact trees this build compiles are copied to the VM (k3s-node-01) and, inside the broker
# Dockerfile's builder stage, the `linux-only` ctest label runs (rune_udp_gso_tests: every UdpGsoBindings test,
# incl. the DR-BUG-MTU regressions; container with CAP_NET_ADMIN and RUNE_LINUX_GATE=1), then the bind-order
# proof (BindOrderRegression.FixedPortsBindBeforeAnyEphemeralPublicationRegression) in a container whose netns
# ephemeral range is narrowed to 40 ports (RUNE_BIND_ORDER_GATE=1).
#
# Isolation (re-check I3) — a verdict is computed only from files THIS invocation copied, compiled into objects
# THIS invocation created:
#   - The trees are snapshotted locally at invocation, then streamed (one tar) into a PRIVATE run directory, $RUNE_LINUX_GATE_DIR/run-<sha>-<time>-<pid>,
#     inside the same `flock -w 7200 /root/vm.lock` session that builds and runs: copy, build and run all hold
#     the lock, and no other invocation ever reads or writes that directory.
#   - The build goes to <run>/_build (always fresh); the image tag and container names carry the run id.
#   - Never prunes; on every exit path (trap) it removes only its own run directory and its own image tag.
#   - Refuses (exit 2) if any tree has uncommitted tracked changes, unless RUNE_ALLOW_DIRTY=1: a verdict must be
#     about committed code.
#   - RUNE_LINUX_GATE_SELFTEST=1 additionally runs scripts/linux-stage-c.sh (the Linux-host stage c, re-check M5)
#     in a --privileged container (must be green) and in an unprivileged one (must exit 3, NOT GREEN).
#   - RUNE_LINUX_GATE_EXTRA=<local script> runs that script last in the same locked session (RUN, IMG, RID, J
#     exported; e.g. a RED proof that mutates the copy in \$RUN and rebuilds); its exit code joins the verdict.
# Exit code = the stage's verdict.  The sanitizer checkpoint (build-asan / build-tsan, Mac) does not need this
# stage: GSO is not sanitizer-relevant on the Mac (stub bindings).
set -u
J="${1:-3}"; B="${2:-${RUNE_BUILD_DIR:-}}"; F="${3:-}"
ITEG=OFF; XT=""
if [ -n "$F" ]; then ITEG=ON; XT="rune_consensus_candidacy_tests rune_client_broker_tests rune_node_tests"; fi
# The Linux VM to gate on, as user@host.  No default: set RUNE_LINUX_GATE_HOST (or just RUNE_VM_HOST).
VM="${RUNE_LINUX_GATE_HOST:-${RUNE_VM_HOST:+root@${RUNE_VM_HOST}}}"
[ -n "$VM" ] || { echo "run-linux-regression-vm: set RUNE_VM_HOST (VM address) or RUNE_LINUX_GATE_HOST (user@host)" >&2; exit 2; }
GD="${RUNE_LINUX_GATE_DIR:-/root/rune-linux-gate}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
CACHE=""
if [ -n "$B" ]; then cands=("$B" "$PWD/$B" "$HERE/$B" "$ROOT/$B"); else cands=("$HERE/build-fast" "$ROOT/build/root"); fi
for c in "${cands[@]}"; do
    [ -f "$c/CMakeCache.txt" ] && { CACHE="$c/CMakeCache.txt"; break; }
done
[ -n "$CACHE" ] || { echo "run-linux-regression-vm: no CMakeCache.txt in any of: ${cands[*]} (pass the build dir or set RUNE_BUILD_DIR)" >&2; exit 2; }
val() { grep -E "^$1:" "$CACHE" | head -1 | cut -d= -f2-; }
PROTO="$(cd "$(val RUNE_PROTOCOL_INCLUDE)/.." && pwd)"
declare -a NAMES=(broker protocol client-cpp codec flight-recorder ql crypto)
declare -a TREES=("$HERE" "$PROTO" "$(val RUNE_CLIENT_ROOT)" "$(val RUNE_CODEC_ROOT)" "$(val FLIGHT_RECORDER_DIR)" \
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
SHA="$(git -C "$HERE" rev-parse --short HEAD 2>/dev/null || echo nogit)"
RID="$SHA-$(date +%s)-$$"
RUN="$GD/run-$RID"
IMG="rune-linux-gate:$RID"
STAGE="$(mktemp -d "${TMPDIR:-/tmp}/rune-linux-gate.XXXXXX")" || exit 2
trap 'rm -rf "$STAGE"' EXIT
echo "== linux stage: broker $SHA, protocol $(git -C "$PROTO" rev-parse --short HEAD 2>/dev/null) -> $VM:$RUN"

# The REVISION files and the remote body travel in the same stream as the trees (nothing is written on the VM
# outside the locked session).
mkdir -p "$STAGE/meta"
for i in "${!TREES[@]}"; do
    echo "${NAMES[$i]} $(git -C "${TREES[$i]}" rev-parse HEAD 2>/dev/null || echo unknown)" >> "$STAGE/meta/REVISIONS"
done
cat > "$STAGE/meta/gate.sh" <<REMOTE
set -u
cd "$RUN/broker" || exit 2
DOCKER_BUILDKIT=1 docker build --platform linux/amd64 -f docker/Dockerfile --target builder \\
    --build-context rune-protocol=../protocol --build-context flight-recorder=../flight-recorder \\
    --build-context rune-codec=../codec --build-context rune-crypto=../crypto \\
    --build-arg CMAKE_BUILD_PARALLEL_LEVEL=$J -t "$IMG" . > "$RUN/builder.log" 2>&1 \\
    || { echo "builder stage failed:"; tail -30 "$RUN/builder.log"; exit 2; }
CFG='cmake -S /src/broker -B /src/_build -G Ninja -DCMAKE_TOOLCHAIN_FILE=/build/conan-release/build/Release/generators/conan_toolchain.cmake \\
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DBUILD_BENCHMARKS=OFF -DBUILD_INTEGRATION_TESTING=$ITEG \\
  -DRUNE_PROTOCOL_INCLUDE=/src/protocol/include -DRUNE_CODEC_ROOT=/src/codec -DRUNE_CRYPTO_ROOT=/src/crypto \\
  -DFLIGHT_RECORDER_DIR=/src/flight-recorder -DRUNE_CLIENT_ROOT=/src/client-cpp -DRUNE_QL_ROOT=/src/ql -DRUNE_TLS=ON > /src/cmake.log 2>&1'
docker run --rm --name "rune-linux-gate-build-$RID" -v "$RUN:/src" "$IMG" bash -c "\$CFG && for t in $XT; do ninja -C /src/_build -t targets all | grep -q \"^\\\$t:\" || { echo \"missing target \\\$t (the ctest filter needs it)\" >&2; exit 2; }; done && cmake --build /src/_build -j$J --target rune_udp_gso_tests rune_catalog_replication_tests $XT > /src/ninja.log 2>&1" \\
    || { echo "build failed:"; tail -20 "$RUN/cmake.log" "$RUN/ninja.log"; exit 2; }
echo "== linux-only (CAP_NET_ADMIN, RUNE_LINUX_GATE=1)"
docker run --rm --name "rune-linux-gate-run-$RID" --cap-add NET_ADMIN --shm-size=1g -e RUNE_LINUX_GATE=1 -v "$RUN:/src" "$IMG" \\
    bash -c "ctest --test-dir /src/_build -L linux-only --output-on-failure"; rc1=\$?
echo "== bind order (netns ip_local_port_range 45000-45039, RUNE_BIND_ORDER_GATE=1)"
docker run --rm --name "rune-linux-gate-bind-$RID" --sysctl net.ipv4.ip_local_port_range="45000 45039" --shm-size=1g \\
    -e RUNE_BIND_ORDER_GATE=1 -v "$RUN:/src" "$IMG" \\
    bash -c "/src/_build/libs/node/rune_catalog_replication_tests --gtest_filter='BindOrderRegression.FixedPortsBindBeforeAnyEphemeralPublicationRegression'"; rc2=\$?
rcF=0
if [ -n "$F" ]; then
    echo "== ctest -R '$F' (--shm-size=1g)"
    docker run --rm --name "rune-linux-gate-filter-$RID" --shm-size=1g -v "$RUN:/src" "$IMG" \\
        bash -c "ctest --test-dir /src/_build -R '$F' --output-on-failure"; rcF=\$?
fi
rc3=0
if [ "${RUNE_LINUX_GATE_SELFTEST:-0}" = "1" ]; then
    echo "== selftest: linux-stage-c.sh, --privileged (expect 0)"
    docker run --rm --name "rune-linux-gate-m5p-$RID" --privileged --shm-size=1g -v "$RUN:/src" "$IMG" \\
        bash /src/broker/scripts/linux-stage-c.sh /src/_build; p=\$?
    echo "== selftest: linux-stage-c.sh, unprivileged (expect 3)"
    docker run --rm --name "rune-linux-gate-m5u-$RID" --shm-size=1g -v "$RUN:/src" "$IMG" \\
        bash /src/broker/scripts/linux-stage-c.sh /src/_build; u=\$?
    echo "== selftest: privileged rc \$p, unprivileged rc \$u"
    [ "\$p" -eq 0 ] && [ "\$u" -eq 3 ] || rc3=1
fi
echo "== linux stage ($RID): linux-only rc \$rc1, bind-order rc \$rc2\$([ -n "$F" ] && echo ", filter rc \$rcF")\$([ "${RUNE_LINUX_GATE_SELFTEST:-0}" = 1 ] && echo ", selftest rc \$rc3")"
rc4=0
if [ -f "$RUN/meta/extra.sh" ]; then
    echo "== extra: RUNE_LINUX_GATE_EXTRA (RUN, IMG, RID, J exported)"
    RUN="$RUN" IMG="$IMG" RID="$RID" J="$J" bash "$RUN/meta/extra.sh"; rc4=\$?
    echo "== extra rc \$rc4"
fi
[ "\$rc1" -eq 0 ] && [ "\$rc2" -eq 0 ] && [ "\${rcF:-0}" -eq 0 ] && [ "\$rc3" -eq 0 ] && [ "\$rc4" -eq 0 ]
REMOTE
if [ -n "${RUNE_LINUX_GATE_EXTRA:-}" ]; then
    cp "$RUNE_LINUX_GATE_EXTRA" "$STAGE/meta/extra.sh" || exit 2
fi

# One ssh session: take the lock, create the private run dir, unpack this invocation's trees into it, run,
# and (trap) remove the run dir and the image tag on every exit path.  Concatenated tars: --ignore-zeros.
REMOTE_CMD="flock -w 7200 /root/vm.lock bash -c '
    RUN=$RUN; IMG=$IMG
    trap \"rm -rf \\\"\$RUN\\\"; docker rmi \\\"\$IMG\\\" >/dev/null 2>&1\" EXIT
    mkdir -p \"\$RUN\" || exit 2
    gzip -dc | tar -x --ignore-zeros -C \"\$RUN\" || { echo \"unpack failed\"; exit 2; }
    df -h / | tail -1 | sed \"s/^/== disk (start): /\"
    RUNE_LINUX_GATE_SELFTEST=${RUNE_LINUX_GATE_SELFTEST:-0} bash \"\$RUN/meta/gate.sh\"; rc=\$?
    df -h / | tail -1 | sed \"s/^/== disk (end, before cleanup): /\"
    exit \$rc'"
# Snapshot NOW, locally: the lock may be held for an hour by another run, and a lazily read stream would pick
# up edits made meanwhile.  The verdict is about the trees as they were at invocation.
{
    COPYFILE_DISABLE=1 tar --no-xattrs -cf - -C "$STAGE" meta
    for i in "${!TREES[@]}"; do
        COPYFILE_DISABLE=1 tar --no-xattrs -cf - --exclude 'build*' --exclude '.git' --exclude '.cache' \
            --exclude '_build' -s "|^\\.|${NAMES[$i]}|" -C "${TREES[$i]}" .
    done
} | gzip -1 > "$STAGE/trees.tgz" || { echo "run-linux-regression-vm: snapshot failed" >&2; exit 2; }
echo "== snapshot $(wc -c < "$STAGE/trees.tgz" | tr -d ' ') bytes; waiting for /root/vm.lock"
ssh "$VM" "$REMOTE_CMD" < "$STAGE/trees.tgz"
