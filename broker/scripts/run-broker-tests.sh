#!/usr/bin/env bash
# Two-tier broker test runner.
#
#   scripts/run-broker-tests.sh [build-dir] [extra ctest args...]
#
# Tier 1 (no lock, wide -j): the ~3000 fast unit / in-process tests.
# Tier 2 (under /tmp/rune-heavy.lock, -j2): the slow multi-node / real-Aeron /
#   compaction suites. Only these are timing-sensitive enough to false-fail when
#   several suites share the machine, so only these queue behind the shared lock.
#
# The heavy set = a fixed list of known multi-node suites PLUS every suite that
# took >= 0.5 s in the last run (ctest's own CTestCostData.txt), so a new slow
# suite is picked up automatically after its first run.
set -u
B="${1:-build-fast}"; shift || true
COST="$B/Testing/Temporary/CTestCostData.txt"
HEAVY_RE='ShardMode|UdpCluster|PerWorkerReplication|GR[0-9]+_Integration|GatewayReplicationWiring|TX2b_GatewayDriven|NodeGatewayFixture|BrokerNode|ThreeNode|CompactionRetentionSmoke|CompactionStress|WorkerPool|UdpMessageSink|RetentionCleanerAgent|BackPressure|BrokerProcessSignal'
if [ -f "$COST" ]; then
  dyn=$(awk 'NF==3 && $3>=0.5 {split($1,a,"."); print a[1]}' "$COST" | sed 's/[][\\.^$*+?(){}|]/\\&/g' | sort -u | paste -sd'|' -)
  [ -n "$dyn" ] && HEAVY_RE="$HEAVY_RE|$dyn"
fi
CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
LIGHT_J=$(( CORES > 2 ? CORES / 2 : 1 ))

echo "== tier 1: fast tests (-j$LIGHT_J, no lock)"
ctest --test-dir "$B" -E "$HEAVY_RE" -j "$LIGHT_J" --timeout 300 "$@"; rc1=$?

echo "== tier 2: heavy multi-node tests (-j2, waiting for /tmp/rune-heavy.lock)"
LOCK=/tmp/rune-heavy.lock
# Owned lock: our pid lives in $LOCK/pid.  A lock whose recorded owner is dead
# (SIGKILL / OOM / crash) is stale and reclaimed; we only ever remove a lock
# that records OUR pid.  (Legacy holders using plain mkdir/rmdir write no pid
# file and are simply waited for.)
acquire_lock() {
  while :; do
    if mkdir "$LOCK" 2>/dev/null; then echo $$ > "$LOCK/pid"; return 0; fi
    owner=$(cat "$LOCK/pid" 2>/dev/null || true)
    if [ -n "$owner" ] && ! kill -0 "$owner" 2>/dev/null; then
      echo "reclaiming stale $LOCK (owner pid $owner is gone)" >&2
      rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null || true
      continue
    fi
    sleep 5
  done
}
release_lock() {
  [ "$(cat "$LOCK/pid" 2>/dev/null)" = "$$" ] || return 0
  rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null || true
}
acquire_lock
trap 'release_lock' EXIT
trap 'release_lock; exit 130' INT
trap 'release_lock; exit 143' TERM
ctest --test-dir "$B" -R "$HEAVY_RE" -j2 --timeout 300 "$@"; rc2=$?
release_lock

[ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ]
