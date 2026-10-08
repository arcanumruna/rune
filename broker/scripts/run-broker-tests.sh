#!/usr/bin/env bash
# Two-tier broker test runner.
#
#   scripts/run-broker-tests.sh [build-dir] [extra ctest args...]
#
# Tier 1 (no lock, wide -j): the ~3000 fast unit / in-process tests.
# Tier 2 (under the heavy-test lock, -j2): the slow multi-node / real-Aeron /
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

echo "== tier 2: heavy multi-node tests (-j2, waiting for the heavy-test lock)"
# Mutual exclusion, in two layers:
#  1. A kernel flock(2) on /tmp/rune-heavy.flock, opened by a perl wrapper with
#     close-on-exec CLEARED so bash, ctest and every test binary inherit it: the
#     lock lives exactly as long as ANY process of this tier, and the kernel drops
#     it when the last one dies (SIGKILL/OOM/crash) — no stale-lock logic among
#     new-style runners.
#  2. Under that flock, the legacy directory lock /tmp/rune-heavy.lock (still used
#     by older agents' plain mkdir/rmdir) is taken too, with our pid in a marker
#     file.  Holding the flock means no process of any earlier new-style run is
#     alive, so a dir WITH an owner.pid marker is stale by construction and is
#     reclaimed (race-free: one new-style runner at a time).  A stale symlink from
#     the previous script version is reclaimed if its pid is dead.  A pid-less dir
#     (legacy mkdir holder, or a holder killed between mkdir and the marker write)
#     is waited for, with a message.
command -v perl >/dev/null || { echo "perl is required for the heavy-test lock" >&2; exit 2; }
: "${RUNE_HEAVY_FLOCK:=/tmp/rune-heavy.flock}" "${RUNE_HEAVY_LOCK_DIR:=/tmp/rune-heavy.lock}"
export RUNE_HEAVY_FLOCK RUNE_HEAVY_LOCK_DIR
HEAVY_RE="$HEAVY_RE" B="$B" perl -e '
  use Fcntl qw(:flock F_SETFD);
  open(my $fh, ">>", $ENV{RUNE_HEAVY_FLOCK}) or die "open flock: $!";
  fcntl($fh, F_SETFD, 0) or die "fcntl: $!";   # children inherit the lock
  flock($fh, LOCK_EX) or die "flock: $!";
  exit(system(@ARGV) == 0 ? 0 : 1);
' -- bash -c '
  D="$RUNE_HEAVY_LOCK_DIR"
  # EPERM-safe, ps-free liveness (works in minimal containers).
  alive() { perl -e '"'"'exit((kill(0, $ARGV[0]) || $!{EPERM}) ? 0 : 1)'"'"' "$1"; }
  warned=0
  while ! mkdir "$D" 2>/dev/null; do
    if [ -L "$D" ]; then                              # previous script version
      p=$(readlink "$D" 2>/dev/null || true)
      if [ -n "$p" ] && ! alive "$p"; then
        echo "reclaiming stale symlink lock $D (pid $p is gone)" >&2; rm -f "$D"; continue
      fi
    elif [ -f "$D/owner.pid" ]; then                  # stale: we hold the flock
      echo "reclaiming $D left by a dead runner (pid $(cat "$D/owner.pid" 2>/dev/null))" >&2
      rm -f "$D/owner.pid"; rmdir "$D" 2>/dev/null && continue
    fi
    [ "$warned" = 0 ] && { echo "waiting on $D (legacy or pid-less holder)" >&2; warned=1; }
    sleep 5
  done
  echo $$ > "$D/owner.pid"
  release() { [ "$(cat "$D/owner.pid" 2>/dev/null)" = "$$" ] && { rm -f "$D/owner.pid"; rmdir "$D" 2>/dev/null; }; return 0; }
  trap release EXIT; trap "release; exit 130" INT; trap "release; exit 143" TERM
  ctest --test-dir "$B" -R "$HEAVY_RE" -j2 --timeout 300 "$@"
' run-heavy "$@"
rc2=$?

[ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ]
