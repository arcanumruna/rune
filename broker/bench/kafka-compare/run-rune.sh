#!/usr/bin/env bash
# Rune-side cell runners. Source env.sh + lib.sh first, or run standalone
# (it sources them itself). Each function assumes the broker is already up
# (start it with rune_start_broker for the tier/workers under test).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"; source "$HERE/lib.sh"

# tier -> (topic tier token, client acks)
_rune_tier_topic() { case "$1" in D0) echo "tier=leader";; D1) echo "tier=majority";; D2) echo "tier=strict";; esac; }
_rune_tier_acks()  { case "$1" in D0) echo "leader";; D1|D2) echo "all";; esac; }

# Build broker topics spec: "bench:<parts>:<tier>".  Storage location (nvme vs
# tmpfs) is chosen by rune_start_broker's persistence arg, NOT here — the log
# stays disk-backed either way (tmpfs = disk-format log on a RAM filesystem),
# matching Kafka's log.dirs-on-tmpfs so the comparison is apples-to-apples.
rune_topics_spec() {
  local tier="$1" parts="$2"
  echo "bench:${parts}:$(_rune_tier_topic "$tier")"
}

# rune_produce <scenario> <tier> <records> <vbytes> <parts> <workers> <window> <idle> <persistence> <nodes>
rune_produce() {
  local scenario="$1" tier="$2" records="$3" vbytes="$4" parts="$5" workers="$6" window="$7" idle="$8" persistence="$9" nodes="${10}"
  local acks; acks="$(_rune_tier_acks "$tier")"
  local cdir="$WORK/rune/cP"; rm -rf "$cdir"; mkdir -p "$cdir"
  local batch=1048576; [ "$window" -le 1 ] && batch=512
  local idlemode=backoff; [ "$idle" = "lowlatency" ] && idlemode=busyspin
  log "rune produce: $scenario $tier rec=$records vb=$vbytes parts=$parts wk=$workers win=$window idle=$idle"
  local out
  # shellcheck disable=SC2086
  out="$(env RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:${RUNE_CLIENT_PORT}" \
      RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:${RUNE_EGRESS_PORT}" \
      RUNE_DATA_DIR="$cdir" "${RUNE_CLIENT_TOPICS_ENV}=bench:0:${parts}" \
      RUNE_BENCH_RECORDS="$records" RUNE_BENCH_VALUE_BYTES="$vbytes" \
      RUNE_BENCH_WARMUP="$WARMUP" RUNE_BENCH_MODE=acked RUNE_BENCH_ACKS="$acks" \
      RUNE_BENCH_WINDOW="$window" RUNE_BENCH_MAX_IN_FLIGHT="$window" \
      RUNE_LINGER_US=0 RUNE_BATCH_BYTES="$batch" RUNE_IDLE_MODE="$idlemode" \
      $(pin "$CORES_CLIENT") "$RUNE_PROD_BIN" 2>/dev/null | grep '^RESULT')"
  [ -z "$out" ] && { log "rune produce: NO RESULT"; return 1; }
  echo "  $out" >&2
  local rps mbps p50 p99 p999
  rps=$(sed -n 's/.*records_per_sec=\([0-9.]*\).*/\1/p' <<<"$out")
  mbps=$(sed -n 's/.*MB_per_sec=\([0-9.]*\).*/\1/p' <<<"$out")
  p50=$(sed -n 's/.*p50_us=\([0-9.]*\).*/\1/p' <<<"$out")
  p99=$(sed -n 's/.*p99_us=\([0-9.]*\).*/\1/p' <<<"$out")
  p999=$(sed -n 's/.*p999_us=\([0-9.]*\).*/\1/p' <<<"$out")
  emit_result tool=rune scenario="$scenario" tier="$tier" record_bytes="$vbytes" \
    partitions="$parts" cores="${CELL_CORES:-0}" workers="$workers" consumers=0 \
    persistence="$persistence" nodes="$nodes" idle="$idle" \
    throughput_msgs_per_sec="${rps:-0}" throughput_mb_per_sec="${mbps:-0}" \
    p50_us="${p50:-0}" p99_us="${p99:-0}" p999_us="${p999:-0}" status=ok
}

# rune_consume <tier> <records> <vbytes> <parts> <workers> <consumers> <persistence> <nodes>
rune_consume() {
  local tier="$1" records="$2" vbytes="$3" parts="$4" workers="$5" ncons="$6" persistence="$7" nodes="$8"
  log "rune consume: $tier rec=$records parts=$parts consumers=$ncons"
  local pids=() total=0 c
  for c in $(seq 1 "$ncons"); do
    local port=$((RUNE_EGRESS_PORT + 1 + c)) dd="$WORK/rune/rc$c"; mkdir -p "$dd"
    # shellcheck disable=SC2086
    env RUNE_TARGET="aeron:udp?endpoint=127.0.0.1:${RUNE_CLIENT_PORT}" \
        RUNE_EGRESS="aeron:udp?endpoint=127.0.0.1:${port}" \
        RUNE_DATA_DIR="$dd" "${RUNE_CLIENT_TOPICS_ENV}=bench:0:${parts}" RUNE_BENCH_MODE=catchup \
        RUNE_BENCH_FROM=earliest RUNE_BENCH_RECORDS="$records" RUNE_BENCH_IDLE_MS="${RUNE_CONS_IDLE_MS:-10000}" \
        $(pin "$CORES_CLIENT") "$RUNE_CONS_BIN" >"$WORK/rune/rc$c.log" 2>&1 &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; done
  local drained=0
  for c in $(seq 1 "$ncons"); do
    local r rec; r=$(sed -n 's/.*records_per_sec=\([0-9]*\).*/\1/p' "$WORK/rune/rc$c.log" | head -1)
    rec=$(sed -n 's/.*records=\([0-9]*\).*/\1/p' "$WORK/rune/rc$c.log" | head -1)
    echo "  consumer$c: $(grep '^RESULT' "$WORK/rune/rc$c.log" | head -1)" >&2
    total=$((total + ${r:-0})); drained=$((drained + ${rec:-0}))
  done
  local st=ok; [ "$drained" -lt "$records" ] && st=short_drain
  [ "$st" = short_drain ] && log "WARN rune read under-drained: $drained/$records ($st)"
  emit_result tool=rune scenario=read tier="$tier" record_bytes="$vbytes" \
    partitions="$parts" cores="${CELL_CORES:-0}" workers="$workers" consumers="$ncons" \
    persistence="$persistence" nodes="$nodes" idle=throughput \
    throughput_msgs_per_sec="$total" records_consumed="$drained" records_expected=$((records*ncons)) status="$st"
}
