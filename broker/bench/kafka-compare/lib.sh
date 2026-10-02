#!/usr/bin/env bash
# Shared helpers: logging, broker/kafka lifecycle, JSON emission.
# Requires env.sh already sourced.

log()  { echo "[$(date +%H:%M:%S)] $*" >&2; }
die()  { echo "FATAL: $*" >&2; exit 1; }

# --------------------------------------------------------------------------
# JSON emission: append one result object (jsonl) to $RESULTS_RAW.
# Usage: emit_result <k=v> ... where values are already correct types.
# String keys we always quote; numeric keys are emitted raw.
# We build the object with python for correctness.
# --------------------------------------------------------------------------
emit_result() {
  python3 - "$@" <<'PY' >>"$RESULTS_RAW"
import json, sys
STRINGS = {"tool","scenario","tier","persistence","idle","notes","status"}
obj = {}
for kv in sys.argv[1:]:
    k, _, v = kv.partition("=")
    if k in STRINGS:
        obj[k] = v
    else:
        try:
            obj[k] = int(v)
        except ValueError:
            try: obj[k] = float(v)
            except ValueError: obj[k] = v
print(json.dumps(obj))
PY
}

# --------------------------------------------------------------------------
# Rune broker lifecycle
# --------------------------------------------------------------------------
# rune_start_broker <workers> <topics_spec> [io_backend] [persistence]
#   topics_spec e.g. "bench:8:tier=strict"; persistence nvme|tmpfs selects the
#   LOG storage root (aeron IPC always lives on tmpfs scratch).
RUNE_BPID=""
rune_start_broker() {
  local workers="$1" topics="$2" io="${3:-uring}" persistence="${4:-nvme}"
  local logdir; logdir="$(data_root "$persistence" rune-log)"
  local d="$WORK/rune"; rm -rf "$d" "$logdir"; mkdir -p "$d/a" "$logdir"
  pkill -9 -f "$RUNE_BROKER_BIN" 2>/dev/null; sleep 0.4
  log "rune broker: workers=$workers topics='$topics' io=$io"
  # shellcheck disable=SC2086
  env RUNE_NODE_ID=1 RUNE_PEERS="1@127.0.0.1:${RUNE_CONSENSUS_PORT}:${RUNE_CLIENT_PORT}" \
      RUNE_CLIENT_ENDPOINT="0.0.0.0:${RUNE_CLIENT_PORT}" \
      RUNE_DATA_DIR="$logdir" RUNE_AERON_DIR="$d/a" RUNE_WORKERS="$workers" \
      RUNE_PER_WORKER_INGRESS=true RUNE_PER_WORKER_EGRESS=true RUNE_TOPICS="$topics" \
      RUNE_IO_BACKEND="$io" RUNE_AERON_TERM_BUFFER_BYTES=33554432 \
      RUNE_WORKER_INGRESS_RING_BYTES=33554432 RUNE_WORKER_RETURN_RING_BYTES=33554432 \
      RUNE_WORKER_EGRESS_CONTROL_BASE_PORT="$RUNE_WORKER_EGRESS_BASE" \
      RUNE_WORKER_IDLE="${RUNE_WORKER_IDLE:-backoff}" RUNE_RUNTIME_IDLE="${RUNE_RUNTIME_IDLE:-backoff}" \
      RUNE_WORKER_MODE="${RUNE_WORKER_MODE:-legacy}" RUNE_RUNTIME_PLACEMENT="${RUNE_RUNTIME_PLACEMENT:-thread}" \
      RUNE_WORKER_CORE_BASE="${RUNE_WORKER_CORE_BASE:--1}" RUNE_LOG_LEVEL=info \
      $(pin "$CORES_BROKER") "$RUNE_BROKER_BIN" >"$d/broker.log" 2>&1 &
  RUNE_BPID=$!
  local i
  for i in $(seq 1 200); do
    # Ready when the workerpool is built (info log) OR the client endpoint is bound.
    if grep -qiE "WorkerPool built|is LEADER" "$d/broker.log" 2>/dev/null \
       || ss -lun 2>/dev/null | grep -q ":${RUNE_CLIENT_PORT} "; then
      sleep 1; log "rune broker ready"; return 0
    fi
    kill -0 "$RUNE_BPID" 2>/dev/null || { log "BROKER DIED"; tail -20 "$d/broker.log" >&2; return 1; }
    sleep 0.25
  done
  log "BROKER READY TIMEOUT"; tail -20 "$d/broker.log" >&2; return 1
}
rune_stop_broker() { [ -n "$RUNE_BPID" ] && kill -9 "$RUNE_BPID" 2>/dev/null; RUNE_BPID=""; pkill -9 -f "$RUNE_BROKER_BIN" 2>/dev/null; sleep 0.3; true; }

# --------------------------------------------------------------------------
# Kafka (KRaft single-node) lifecycle
# --------------------------------------------------------------------------
KAFKA_PID=""
kafka_start() {
  local persistence="${1:-nvme}"; local cores="${2:-$CORES_BROKER}"
  local logdir; logdir="$(data_root "$persistence" kafka-logs)"
  rm -rf "$logdir"; mkdir -p "$logdir"
  local cfg="$WORK/kraft.properties"
  local cid
  cid="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)"
  cat >"$cfg" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
advertised.listeners=PLAINTEXT://127.0.0.1:9092
log.dirs=$logdir
num.network.threads=8
num.io.threads=16
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF
  "$KAFKA_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$cfg" >/dev/null 2>&1 || die "kafka-storage format failed"
  log "kafka start: logdir=$logdir"
  # shellcheck disable=SC2086
  env KAFKA_HEAP_OPTS="$KAFKA_HEAP" $(pin "$cores") \
      "$KAFKA_HOME"/bin/kafka-server-start.sh "$cfg" >"$WORK/kafka.log" 2>&1 &
  KAFKA_PID=$!
  local i
  for i in $(seq 1 120); do
    "$KAFKA_HOME"/bin/kafka-broker-api-versions.sh --bootstrap-server "$KAFKA_BOOTSTRAP" >/dev/null 2>&1 && { log "kafka ready"; return 0; }
    kill -0 "$KAFKA_PID" 2>/dev/null || { log "KAFKA DIED"; tail -30 "$WORK/kafka.log" >&2; return 1; }
    sleep 1
  done
  log "KAFKA READY TIMEOUT"; tail -30 "$WORK/kafka.log" >&2; return 1
}
kafka_stop() {
  "$KAFKA_HOME"/bin/kafka-server-stop.sh >/dev/null 2>&1
  [ -n "$KAFKA_PID" ] && { for _ in $(seq 1 30); do kill -0 "$KAFKA_PID" 2>/dev/null || break; sleep 0.5; done; kill -9 "$KAFKA_PID" 2>/dev/null; }
  KAFKA_PID=""; sleep 0.5; true
}
kafka_topic() {
  local name="$1" parts="$2"; shift 2
  "$KAFKA_HOME"/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --create \
    --topic "$name" --partitions "$parts" --replication-factor 1 "$@" >/dev/null 2>&1 || true
}

# median of stdin numbers (one per line)
median() { python3 -c 'import sys;v=sorted(float(x) for x in sys.stdin if x.strip());print(0 if not v else v[len(v)//2])'; }
