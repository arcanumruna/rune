#!/usr/bin/env bash
# replbench.sh — 3-node REPLICATION throughput + flight-recorder latency-breakdown
# benchmark for the acks=all (majority / all-in-sync) path. Purpose: measure the
# full "leader receives message -> last replica appends it -> commit -> client ack"
# pipeline with per-stage rune-fr histograms, so we can drive Rune's replication
# throughput to Kafka parity.
#
# Optimal-config by DEFAULT (override via env): 2 workers, 4 partitions, batching
# ON (linger>0 + 1 MiB batch), large in-flight window, large Aeron term buffer +
# worker rings. Flight recorder is armed on all 3 brokers and dumped via SIGUSR1
# after the run (rune_stop_cluster uses kill -9, which would skip the graceful dump).
#
# Usage: replbench.sh [parts] [recs] [window]
set -uo pipefail
cd /root/rune-e5/rune-broker/bench/kafka-compare
set +u; source env.sh; source run-rune.sh; source cluster.sh

# ── verified-idle box (spec §1a.10 / §5): refuse to measure on a busy box ────────
load1="$(cut -d' ' -f1 /proc/loadavg)"
if [ "${REPL_ALLOW_BUSY:-0}" != "1" ] && awk -v l="$load1" 'BEGIN{exit !(l >= 1.0)}'; then
  echo "BUSY-BOX: load1=$load1 >= 1.0 — kill stray CPU processes first (REPL_ALLOW_BUSY=1 overrides)"; exit 2
fi
echo "=== box: load1=$load1 cores=$(nproc) shard=${RUNE_SHARD:-0} placement=${RUNE_RUNTIME_PLACEMENT:-fused} ==="

# ── tunables (env-overridable) ───────────────────────────────────────────────
PARTS="${1:-${REPL_PARTS:-4}}"
RECS="${2:-${REPL_RECS:-1000000}}"
WIN="${3:-${REPL_WINDOW:-8000}}"
WORKERS="${REPL_WORKERS:-2}"
VBYTES="${REPL_VBYTES:-256}"
LINGER_US="${REPL_LINGER_US:-200}"     # batching: >0 so records coalesce into batches
BATCH_BYTES="${REPL_BATCH_BYTES:-1048576}"   # 1 MiB batch cap
WARMUP_N="${REPL_WARMUP:-50000}"
# large buffers (override cluster.sh defaults of 32 MiB)
export RUNE_AERON_TERM_BUFFER_BYTES="${RUNE_AERON_TERM_BUFFER_BYTES:-67108864}"    # 64 MiB
export RUNE_WORKER_INGRESS_RING_BYTES="${RUNE_WORKER_INGRESS_RING_BYTES:-67108864}"
export RUNE_WORKER_RETURN_RING_BYTES="${RUNE_WORKER_RETURN_RING_BYTES:-67108864}"
export RUNE_FS_LOG=0
if [ "${RUNE_SHARD:-0}" = "1" ]; then
  export RUNE_SHARD RUNE_RUNTIME_PLACEMENT="${RUNE_RUNTIME_PLACEMENT:-fused}"
else
  export RUNE_WORKER_IDLE=backoff RUNE_RUNTIME_IDLE=backoff
fi
export RUNE_BROKER_BIN=/root/rune-e5/rune-broker/build-linux/rune_broker
export RUNE_ROLE_BIN=/root/rune-e5/rune-broker/build-linux/rune_role
export CELL_CORES="${CELL_CORES:-0}"

# ── flight recorder (opt-in via REPL_FR=1) ───────────────────────────────────
FR="${REPL_FR:-1}"
FROUT="${RUNE_FR_OUT:-/tmp/frout}"
if [ "$FR" = "1" ]; then
  rm -rf "$FROUT"; mkdir -p "$FROUT"
  export RUNE_FR_ENABLE=1 RUNE_FR_OUT="$FROUT"
  export RUNE_FR_RING_BYTES="${RUNE_FR_RING_BYTES:-33554432}"   # 32 MiB/thread (shared-VM safe)
fi

echo "=== replbench: parts=$PARTS workers=$WORKERS recs=$RECS win=$WIN vbytes=$VBYTES linger=${LINGER_US}us batch=${BATCH_BYTES} FR=$FR ==="
pkill -9 -x rune_broker 2>/dev/null; sleep 1
rm -rf /dev/shm/kbench/rune-c* /root/kbench-disk/rune-log* /tmp/rune_bn_egress_* /dev/shm/aeron* 2>/dev/null

LEADER="$(rune_start_cluster "$WORKERS" "bench:${PARTS}:tier=${REPL_TIER:-majority:durability=all-in-sync}" uring nvme)"
[ -z "$LEADER" ] && { echo "NO-LEADER"; exit 1; }
echo "leader=node$LEADER workers=$WORKERS"
sleep 6   # let startup churn settle to ISR=3

port=$((40200+LEADER)); d=/dev/shm/kbench/cP; rm -rf "$d"; mkdir -p "$d"
echo -n "RESULT: "
env RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:${port}" RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:40900" \
  RUNE_DATA_DIR="$d" RUNE_TOPICS="bench:0:${PARTS}" RUNE_BENCH_RECORDS="$RECS" RUNE_BENCH_VALUE_BYTES="$VBYTES" \
  RUNE_BENCH_WARMUP="$WARMUP_N" RUNE_BENCH_MODE=acked RUNE_BENCH_ACKS=${REPL_ACKS:-all} \
  RUNE_BENCH_WINDOW="$WIN" RUNE_BENCH_MAX_IN_FLIGHT="$WIN" \
  RUNE_LINGER_US="$LINGER_US" RUNE_BATCH_BYTES="$BATCH_BYTES" RUNE_IDLE_MODE=backoff \
  timeout 120 "$RUNE_PROD_BIN" 2>&1 | grep -E "^RESULT" | \
  sed -E "s/.*records_per_sec=([0-9.]*).*MB_per_sec=([0-9.]*).*p50_us=([0-9.]*).*p99_us=([0-9.e+]*).*/rps=\1 MBps=\2 p50=\3us p99=\4us/" \
  || echo "(no RESULT / timed out)"

# ── flight-recorder dump (SIGUSR1 → graceful report; stop uses kill -9) ───────
if [ "$FR" = "1" ]; then
  echo "=== dumping flight recorder (SIGUSR1) ==="
  # NB: rune_start_cluster ran under $(...) command substitution (subshell), so the
  # RUNE_CLUSTER_PIDS array is EMPTY in this scope — signal by process name instead.
  pkill -USR1 -x rune_broker 2>/dev/null
  sleep "${REPL_FR_DUMP_WAIT:-12}"   # let exportChromeTrace finish (large JSON) before kill -9
  echo "--- FR reports in $FROUT ---"; ls -la "$FROUT" 2>/dev/null | grep fr-hist
fi
echo "=== per-thread CPU (broker threads) ==="
for p in $(pgrep -x rune_broker); do ps -L -o pid,tid,psr,pcpu,comm -p "$p"; done
rune_stop_cluster
if [ "$FR" = "1" ] && command -v python3 >/dev/null 2>&1; then
  echo "=== REPLICATION LATENCY BREAKDOWN (frwaterfall.py) ==="
  python3 "$(dirname "$0")/frwaterfall.py" "$FROUT/fr-trace-n*.json" 2>/dev/null || true
fi
echo "=== DONE ==="
