#!/usr/bin/env bash
# Kafka-side cell runners (official perf tools). Source env.sh + lib.sh first.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"; source "$HERE/lib.sh"

# tier -> producer acks + extra topic config for the fsync tier.
_kafka_tier_acks() { case "$1" in D0) echo "1";; D1|D2) echo "all";; esac; }
# D2 = synchronous disk: flush every message.
_kafka_tier_topiccfg() { case "$1" in D2) echo "--config flush.messages=1 --config flush.ms=0";; *) echo "";; esac; }

KTOPIC=bench

# kafka_produce <scenario> <tier> <records> <vbytes> <parts> <persistence> <nodes>
kafka_produce() {
  local scenario="$1" tier="$2" records="$3" vbytes="$4" parts="$5" persistence="$6" nodes="$7"
  local acks; acks="$(_kafka_tier_acks "$tier")"
  "$KAFKA_HOME"/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$KTOPIC" >/dev/null 2>&1 || true
  sleep 0.5
  # shellcheck disable=SC2086
  kafka_topic "$KTOPIC" "$parts" $(_kafka_tier_topiccfg "$tier")
  log "kafka produce: $scenario $tier rec=$records vb=$vbytes parts=$parts acks=$acks"
  local out
  # shellcheck disable=SC2086
  out="$(env KAFKA_HEAP_OPTS="$KAFKA_HEAP" $(pin "$CORES_CLIENT") \
      "$KAFKA_HOME"/bin/kafka-producer-perf-test.sh --topic "$KTOPIC" \
      --num-records "$records" --record-size "$vbytes" --throughput -1 \
      --producer-props bootstrap.servers="$KAFKA_BOOTSTRAP" acks="$acks" \
      linger.ms=0 batch.size=131072 2>/dev/null)"
  echo "$out" | tail -1 >&2
  local kv; kv="$(echo "$out" | python3 "$HERE/parse_kafka.py" producer)"
  eval "$kv"
  emit_result tool=kafka scenario="$scenario" tier="$tier" record_bytes="$vbytes" \
    partitions="$parts" cores="${CELL_CORES:-0}" workers=0 consumers=0 \
    persistence="$persistence" nodes="$nodes" idle=na \
    throughput_msgs_per_sec="${rps:-0}" throughput_mb_per_sec="${mbps:-0}" \
    p50_us="${p50:-0}" p99_us="${p99:-0}" p999_us="${p999:-0}" status=ok
}

# kafka_consume <tier> <records> <vbytes> <parts> <persistence> <nodes>
kafka_consume() {
  local tier="$1" records="$2" vbytes="$3" parts="$4" persistence="$5" nodes="$6"
  log "kafka consume: $tier rec=$records parts=$parts"
  local out
  # shellcheck disable=SC2086
  out="$(env KAFKA_HEAP_OPTS="$KAFKA_HEAP" $(pin "$CORES_CLIENT") \
      "$KAFKA_HOME"/bin/kafka-consumer-perf-test.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
      --topic "$KTOPIC" --messages "$records" --timeout 60000 2>/dev/null)"
  echo "$out" | tail -1 >&2
  local kv; kv="$(echo "$out" | python3 "$HERE/parse_kafka.py" consumer)"
  eval "$kv"
  emit_result tool=kafka scenario=read tier="$tier" record_bytes="$vbytes" \
    partitions="$parts" cores="${CELL_CORES:-0}" workers=0 consumers=1 \
    persistence="$persistence" nodes="$nodes" idle=na \
    throughput_msgs_per_sec="${rps:-0}" throughput_mb_per_sec="${mbps:-0}" status=ok
}

# kafka_latency <tier> <vbytes> <parts> <persistence> <nodes>  (round-trip)
kafka_latency() {
  local tier="$1" vbytes="$2" parts="$3" persistence="$4" nodes="$5"
  local acks; acks="$(_kafka_tier_acks "$tier")"
  "$KAFKA_HOME"/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$KTOPIC" >/dev/null 2>&1 || true
  sleep 0.5
  # shellcheck disable=SC2086
  kafka_topic "$KTOPIC" "$parts" $(_kafka_tier_topiccfg "$tier")
  log "kafka latency: $tier vb=$vbytes acks=$acks"
  local out
  # EndToEndLatency <bootstrap> <topic> <num_msgs> <acks> <msg_size>
  out="$(env KAFKA_HEAP_OPTS="$KAFKA_HEAP" $(pin "$CORES_CLIENT") \
      "$KAFKA_HOME"/bin/kafka-e2e-latency.sh "$KAFKA_BOOTSTRAP" "$KTOPIC" 10000 "$acks" "$vbytes" 2>/dev/null || true)"
  echo "$out" | tail -2 >&2
  local kv; kv="$(echo "$out" | python3 "$HERE/parse_kafka.py" e2e)"
  eval "$kv"
  emit_result tool=kafka scenario=latency tier="$tier" record_bytes="$vbytes" \
    partitions="$parts" cores="${CELL_CORES:-0}" workers=0 consumers=0 \
    persistence="$persistence" nodes="$nodes" idle=na \
    p50_us="${p50:-0}" p99_us="${p99:-0}" p999_us="${p999:-0}" status=ok
}
