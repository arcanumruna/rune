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
until mkdir /tmp/rune-heavy.lock 2>/dev/null; do sleep 5; done
trap 'rmdir /tmp/rune-heavy.lock 2>/dev/null' EXIT INT TERM
ctest --test-dir "$B" -R "$HEAVY_RE" -j2 --timeout 300 "$@"; rc2=$?
rmdir /tmp/rune-heavy.lock 2>/dev/null; trap - EXIT INT TERM

[ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ]
