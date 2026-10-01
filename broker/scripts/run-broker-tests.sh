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
# Owned lock = a SYMLINK whose target is the owner pid, created with symlink(2)
# directly (perl): it fails with EEXIST on ANY existing path — including a
# legacy holder's mkdir'd directory, which plain `ln -s` would silently write
# INTO.  Create + record-owner is one atomic step.  A lock whose owner process is
# gone is reclaimed under a separate $LOCK.reclaim mutex that re-reads the owner
# AND re-checks it is still dead before removing.  A reclaim mutex left behind by
# a killed reclaimer is broken after RECLAIM_STALE_S seconds (logged).
RECLAIM_STALE_S=60
mklock() { perl -e 'symlink($ARGV[0], $ARGV[1]) or exit 1' "$1" "$LOCK"; }
# ps -p works on macOS + procps; containers with a separate PID namespace sharing
# /tmp would see live owners as dead — the heavy lock is host-local by design.
owner_alive() { ps -p "$1" >/dev/null 2>&1; }
mtime() { stat -f %m "$1" 2>/dev/null || stat -c %Y "$1" 2>/dev/null || echo 0; }
acquire_lock() {
  local owner warned=0
  while :; do
    if mklock "$$" 2>/dev/null; then return 0; fi
    owner=$(readlink "$LOCK" 2>/dev/null || true)
    if [ -n "$owner" ] && ! owner_alive "$owner"; then
      if mkdir "$LOCK.reclaim" 2>/dev/null; then
        if [ "$(readlink "$LOCK" 2>/dev/null)" = "$owner" ] && ! owner_alive "$owner" && rm -f "$LOCK"; then
          echo "reclaimed stale $LOCK (owner pid $owner is gone)" >&2
        fi
        rmdir "$LOCK.reclaim" 2>/dev/null || true
        continue
      fi
      age=$(( $(date +%s) - $(mtime "$LOCK.reclaim") ))
      if [ "$age" -gt "$RECLAIM_STALE_S" ]; then
        echo "breaking stale $LOCK.reclaim (${age}s old; a reclaimer was killed)" >&2
        rmdir "$LOCK.reclaim" 2>/dev/null || true
        continue
      fi
    fi
    if [ -z "$owner" ] && [ "$warned" = 0 ]; then
      echo "waiting on $LOCK held by a legacy (pid-less, mkdir) holder" >&2; warned=1
    fi
    sleep 5
  done
}
release_lock() {
  [ "$(readlink "$LOCK" 2>/dev/null)" = "$$" ] && rm -f "$LOCK"
  return 0
}
acquire_lock
trap 'release_lock' EXIT
trap 'release_lock; exit 130' INT
trap 'release_lock; exit 143' TERM
ctest --test-dir "$B" -R "$HEAVY_RE" -j2 --timeout 300 "$@"; rc2=$?
release_lock

[ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ]
