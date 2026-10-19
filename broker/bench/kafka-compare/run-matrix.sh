#!/usr/bin/env bash
# Full-grid driver. Loops the validated cell primitives across every dimension.
# All sweeps + record counts are env-overridable; smoke-matrix.sh shrinks them.
#
#   PHASE toggles: DO_HEADLINE DO_WRITESCALE DO_WORKERSCALE DO_READ DO_PERSIST DO_LATENCY
#   sweeps (space-separated): TIERS HEAD_REC PART_SWEEP REC_SWEEP WK_SWEEP CORE_SWEEP CONS_SWEEP
#   sizes: REC LATREC WARMUP PARTS_MAIN WK_MAIN
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"; source "$HERE/lib.sh"
source "$HERE/run-rune.sh"; source "$HERE/run-kafka.sh"

: "${TIERS:=D0 D1 D2}"
: "${HEAD_REC:=256 1024}"
: "${PART_SWEEP:=1 2 4 8}"
: "${REC_SWEEP:=100 256 1024 4096}"
: "${WK_SWEEP:=1 2 4 8}"
: "${CORE_SWEEP:=1 2 4 8}"
: "${CONS_SWEEP:=1 2 4}"
: "${PARTS_MAIN:=8}"
: "${WK_MAIN:=8}"
: "${REC:=2000000}"
: "${LATREC:=100000}"
: "${WARMUP:=100000}"
: "${DO_HEADLINE:=1}"; : "${DO_WRITESCALE:=1}"; : "${DO_WORKERSCALE:=1}"
: "${DO_READ:=1}"; : "${DO_PERSIST:=1}"; : "${DO_LATENCY:=1}"
: "${FRESH:=1}"   # truncate results.jsonl at start
export WARMUP
[ "$FRESH" = 1 ] && : >"$RESULTS_RAW"

banner() { echo; echo "######## $* ########"; }

# ---------------------------------------------------------------------------
banner "PHASE 1: HEADLINE (parts=$PARTS_MAIN workers=$WK_MAIN)"
if [ "$DO_HEADLINE" = 1 ]; then
  export CELL_CORES="${CORES_BROKER:+8}"; : "${CELL_CORES:=0}"
  for tier in $TIERS; do
    if rune_start_broker "$WK_MAIN" "$(rune_topics_spec "$tier" "$PARTS_MAIN")" uring nvme; then
      for rec in $HEAD_REC; do
        rune_produce write "$tier" "$REC" "$rec" "$PARTS_MAIN" "$WK_MAIN" 100000 throughput nvme 1
        rune_consume       "$tier" "$REC" "$rec" "$PARTS_MAIN" "$WK_MAIN" 1       nvme 1
      done
      rune_stop_broker
    fi
  done
  if kafka_start nvme; then
    for tier in $TIERS; do for rec in $HEAD_REC; do
      kafka_produce write "$tier" "$REC" "$rec" "$PARTS_MAIN" nvme 1
      kafka_consume       "$tier" "$REC" "$rec" "$PARTS_MAIN" nvme 1
    done; done
    kafka_stop
  fi
fi

# ---------------------------------------------------------------------------
banner "PHASE 2: WRITE SCALING (partitions x record)"
if [ "$DO_WRITESCALE" = 1 ]; then
  for tier in $TIERS; do
    for parts in $PART_SWEEP; do
      if rune_start_broker "$WK_MAIN" "$(rune_topics_spec "$tier" "$parts")" uring nvme; then
        for rec in $REC_SWEEP; do
          rune_produce write "$tier" "$REC" "$rec" "$parts" "$WK_MAIN" 100000 throughput nvme 1
        done
        rune_stop_broker
      fi
    done
  done
  if kafka_start nvme; then
    for tier in $TIERS; do for parts in $PART_SWEEP; do for rec in $REC_SWEEP; do
      kafka_produce write "$tier" "$REC" "$rec" "$parts" nvme 1
    done; done; done
    kafka_stop
  fi
fi

# ---------------------------------------------------------------------------
banner "PHASE 3: SCALING (rune workers | kafka core budget) parts=$PARTS_MAIN tier=D1"
if [ "$DO_WORKERSCALE" = 1 ]; then
  for wk in $WK_SWEEP; do
    export CELL_CORES="$wk"
    if rune_start_broker "$wk" "$(rune_topics_spec D1 "$PARTS_MAIN")" uring nvme; then
      rune_produce scaling D1 "$REC" 256 "$PARTS_MAIN" "$wk" 100000 throughput nvme 1
      rune_stop_broker
    fi
  done
  # kafka: give the broker N cores via taskset (only meaningful when the box has cores to pin)
  local_save_cores="$CORES_BROKER"
  for n in $CORE_SWEEP; do
    export CORES_BROKER="0-$((n-1))" CELL_CORES="$n"
    if kafka_start nvme; then
      kafka_produce scaling D1 "$REC" 256 "$PARTS_MAIN" nvme 1
      kafka_stop
    fi
  done
  export CORES_BROKER="$local_save_cores"
fi

# ---------------------------------------------------------------------------
banner "PHASE 4: READ / consumer sweep + read-vs-write (tier=D1 parts=$PARTS_MAIN)"
if [ "$DO_READ" = 1 ]; then
  export CELL_CORES=0
  if rune_start_broker "$WK_MAIN" "$(rune_topics_spec D1 "$PARTS_MAIN")" uring nvme; then
    # produce once, then sweep consumer counts
    rune_produce write D1 "$REC" 256 "$PARTS_MAIN" "$WK_MAIN" 100000 throughput nvme 1
    for nc in $CONS_SWEEP; do
      rune_consume D1 "$REC" 256 "$PARTS_MAIN" "$WK_MAIN" "$nc" nvme 1
    done
    rune_stop_broker
  fi
  if kafka_start nvme; then
    kafka_produce write D1 "$REC" 256 "$PARTS_MAIN" nvme 1
    kafka_consume       D1 "$REC" 256 "$PARTS_MAIN" nvme 1
    kafka_stop
  fi
fi

# ---------------------------------------------------------------------------
banner "PHASE 5: PERSISTENCE (nvme vs tmpfs) tier=D1 parts=$PARTS_MAIN rec=256"
if [ "$DO_PERSIST" = 1 ]; then
  export CELL_CORES=0
  for pers in nvme tmpfs; do
    if rune_start_broker "$WK_MAIN" "$(rune_topics_spec D1 "$PARTS_MAIN")" uring "$pers"; then
      rune_produce write D1 "$REC" 256 "$PARTS_MAIN" "$WK_MAIN" 100000 throughput "$pers" 1
      rune_stop_broker
    fi
    if kafka_start "$pers"; then
      kafka_produce write D1 "$REC" 256 "$PARTS_MAIN" "$pers" 1
      kafka_stop
    fi
  done
fi

# ---------------------------------------------------------------------------
banner "PHASE 6: LATENCY (round-trip window=1) parts=$PARTS_MAIN"
if [ "$DO_LATENCY" = 1 ]; then
  export CELL_CORES=0
  for tier in $TIERS; do
    if rune_start_broker "$WK_MAIN" "$(rune_topics_spec "$tier" "$PARTS_MAIN")" uring nvme; then
      rune_produce latency "$tier" "$LATREC" 256 "$PARTS_MAIN" "$WK_MAIN" 1 lowlatency nvme 1
      rune_produce latency "$tier" "$LATREC" 256 "$PARTS_MAIN" "$WK_MAIN" 1 throughput nvme 1
      rune_stop_broker
    fi
  done
  if kafka_start nvme; then
    for tier in $TIERS; do
      kafka_latency "$tier" 256 "$PARTS_MAIN" nvme 1
    done
    kafka_stop
  fi
fi

banner "COLLECT"
python3 "$HERE/collect.py" "$RESULTS_RAW" "$RESULTS_DIR/results.json"
echo "MATRIX DONE — rows=$(grep -c '^{' "$RESULTS_RAW")"
