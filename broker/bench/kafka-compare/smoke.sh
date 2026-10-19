#!/usr/bin/env bash
# Tiny end-to-end validation of the whole harness on ANY box (prototype).
# Proves both tools start, run every scenario, and emit clean JSON. NOT a
# measurement run: record counts are tiny and cores are unpinned by default.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"; source "$HERE/lib.sh"
source "$HERE/run-rune.sh"; source "$HERE/run-kafka.sh"

# tiny knobs (overridable)
WARMUP="${WARMUP:-1000}"
REC="${REC:-30000}"
LATREC="${LATREC:-3000}"
PARTS="${PARTS:-8}"
WK="${WK:-2}"
export WARMUP CELL_CORES=0
: >"$RESULTS_RAW"

TIERS=(D0 D2)   # leader + strict (strict exercises the tier= parse + fsync path)

echo "======== RUNE ========"
for tier in "${TIERS[@]}"; do
  spec="$(rune_topics_spec "$tier" "$PARTS")"
  if rune_start_broker "$WK" "$spec" uring nvme; then
    rune_produce write   "$tier" "$REC"    256 "$PARTS" "$WK" 100000 throughput nvme 1 || log "produce(write) FAILED $tier"
    rune_consume         "$tier" "$REC"    256 "$PARTS" "$WK" 1              nvme 1 || log "consume FAILED $tier"
    rune_produce latency "$tier" "$LATREC" 256 "$PARTS" "$WK" 1 lowlatency   nvme 1 || log "produce(lat,busyspin) FAILED $tier"
    rune_produce latency "$tier" "$LATREC" 256 "$PARTS" "$WK" 1 throughput   nvme 1 || log "produce(lat,backoff) FAILED $tier"
    rune_stop_broker
  else
    log "rune broker failed to start for $tier"
  fi
done
# persistence toggle: same write cell on tmpfs storage
if rune_start_broker "$WK" "$(rune_topics_spec D0 "$PARTS")" uring tmpfs; then
  rune_produce write D0 "$REC" 256 "$PARTS" "$WK" 100000 throughput tmpfs 1 || log "produce(tmpfs) FAILED"
  rune_stop_broker
fi

echo "======== KAFKA ========"
if [ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
  if kafka_start nvme; then
    for tier in "${TIERS[@]}"; do
      kafka_produce write "$tier" "$REC" 256 "$PARTS" nvme 1 || log "kafka produce FAILED $tier"
      kafka_consume       "$tier" "$REC" 256 "$PARTS" nvme 1 || log "kafka consume FAILED $tier"
      kafka_latency       "$tier"        256 "$PARTS" nvme 1 || log "kafka latency FAILED $tier"
    done
    kafka_stop
  else
    log "kafka failed to start"
  fi
  # persistence toggle: kafka write on tmpfs storage
  if kafka_start tmpfs; then
    kafka_produce write D0 "$REC" 256 "$PARTS" tmpfs 1 || log "kafka produce(tmpfs) FAILED"
    kafka_stop
  fi
else
  log "KAFKA not installed ($KAFKA_HOME) — skipping kafka smoke. Run install-kafka.sh first."
fi

echo "======== COLLECT ========"
python3 "$HERE/collect.py" "$RESULTS_RAW" "$RESULTS_DIR/results.json"
echo "---- raw jsonl ----"; cat "$RESULTS_RAW"
echo "---- results.json ----"; cat "$RESULTS_DIR/results.json"
echo "SMOKE DONE"
