#!/usr/bin/env bash
# 3-node co-located clusters (Rune + Kafka) + DR/failover helpers.
# Source env.sh + lib.sh first. Co-located on ONE box (loopback) — measures
# replication-protocol cost + failover, NOT cross-host network latency.
: "${RUNE_ROLE_BIN:=/root/rune-e5/rune-broker/build/Release/build/Release/rune_role}"

# ---- Rune 3-node -----------------------------------------------------------
# node i (0,1,2): consensus=40110+i client=40200+i repl=40220+i wker-egress-base=40400+i*20
RUNE_CLUSTER_PIDS=()
rune_cluster_peers() {
  echo "0@127.0.0.1:40110:40200:40220,1@127.0.0.1:40111:40201:40221,2@127.0.0.1:40112:40202:40222"
}
# The embedded Aeron driver places its CnC file under RUNE_DATA_DIR/aeron (NOT
# RUNE_AERON_DIR), so rune_role must read there.
CLUSTER_PERSISTENCE="nvme"
_rune_node_aeron() { echo "$(data_root "${CLUSTER_PERSISTENCE:-nvme}" rune-log-$1)/aeron"; }

# rune_start_cluster <workers> <topics_spec> [io] [persistence]
rune_start_cluster() {
  local workers="$1" topics="$2" io="${3:-uring}" persistence="${4:-nvme}"
  CLUSTER_PERSISTENCE="$persistence"
  # Memory-heavy Aeron/ring sizes are env-overridable so a small (4-core) VM can
  # run the 8MB small-VM recipe without OOM; default to the 32MB fat-VM profile.
  local termb="${RUNE_AERON_TERM_BUFFER_BYTES:-33554432}"
  local iring="${RUNE_WORKER_INGRESS_RING_BYTES:-33554432}"
  local rring="${RUNE_WORKER_RETURN_RING_BYTES:-33554432}"
  local peers; peers="$(rune_cluster_peers)"
  pkill -9 -x rune_broker 2>/dev/null; sleep 0.5
  RUNE_CLUSTER_PIDS=()
  local i
  for i in 0 1 2; do
    local cport=$((40200+i)) egbase=$((40400+i*20))
    local logdir; logdir="$(data_root "$persistence" rune-log-$i)"
    local d="$WORK/rune-c$i"; rm -rf "$d" "$logdir"; mkdir -p "$d/a" "$logdir"
    # shellcheck disable=SC2086
    env RUNE_NODE_ID=$i RUNE_PEERS="$peers" RUNE_CLIENT_ENDPOINT="0.0.0.0:$cport" \
        RUNE_DATA_DIR="$logdir" RUNE_AERON_DIR="$d/a" RUNE_WORKERS="$workers" \
        RUNE_PER_WORKER_INGRESS=true RUNE_PER_WORKER_EGRESS=true RUNE_TOPICS="$topics" \
        RUNE_IO_BACKEND="$io" RUNE_AERON_TERM_BUFFER_BYTES="$termb" \
        RUNE_WORKER_INGRESS_RING_BYTES="$iring" RUNE_WORKER_RETURN_RING_BYTES="$rring" \
        RUNE_WORKER_EGRESS_CONTROL_BASE_PORT=$egbase \
        RUNE_ELECTION_TIMEOUT_MS="${RUNE_ELECTION_TIMEOUT_MS:-300}" \
        RUNE_ELECTION_STATUS_INTERVAL_MS="${RUNE_ELECTION_STATUS_INTERVAL_MS:-50}" \
        RUNE_STARTUP_CANVASS_TIMEOUT_MS="${RUNE_STARTUP_CANVASS_TIMEOUT_MS:-200}" \
        RUNE_WORKER_IDLE="${RUNE_WORKER_IDLE:-backoff}" \
        RUNE_RUNTIME_IDLE="${RUNE_RUNTIME_IDLE:-backoff}" \
        RUNE_AERON_DRIVER_IDLE="${RUNE_AERON_DRIVER_IDLE:-sleep-ns}" \
        RUNE_LOG_LEVEL="${RUNE_LOG_LEVEL:-info}" \
        $(pin "$CORES_BROKER") "$RUNE_BROKER_BIN" >"$d/broker.log" 2>&1 &
    RUNE_CLUSTER_PIDS+=($!)
  done
  # Wait for a leader via rune_role (reliable — reads the Aeron role counter).
  local leader; leader="$(rune_wait_leader 60)"
  [ -z "$leader" ] && { log "CLUSTER: no leader elected"; for i in 0 1 2; do tail -5 "$WORK/rune-c$i/broker.log"; done; return 1; }
  log "rune cluster ready: leader=node$leader (client port $((40200+leader)))"
  echo "$leader"
}

# rune_node_role <i> -> LEADER|FOLLOWER|CANDIDATE|NONE|UNKNOWN
rune_node_role() {
  local out; out="$("$RUNE_ROLE_BIN" "$(_rune_node_aeron "$1")" 2>/dev/null | grep -oE "role=[A-Z]+" | head -1)"
  echo "${out#role=}"
}
# rune_leader_node -> node index of current leader, or empty
rune_leader_node() {
  local i r
  for i in 0 1 2; do
    kill -0 "${RUNE_CLUSTER_PIDS[$i]:-0}" 2>/dev/null || continue
    r="$(rune_node_role "$i")"; [ "$r" = "LEADER" ] && { echo "$i"; return 0; }
  done
  echo ""
}
# rune_wait_leader <deadline_s> -> leader node index (blocks until elected or timeout)
rune_wait_leader() {
  local secs="${1:-30}" i l
  for _ in $(seq 1 $((secs*4))); do
    l="$(rune_leader_node)"; [ -n "$l" ] && { echo "$l"; return 0; }
    sleep 0.25
  done
  echo ""
}
rune_stop_cluster() {
  local p; for p in "${RUNE_CLUSTER_PIDS[@]:-}"; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done
  RUNE_CLUSTER_PIDS=(); pkill -9 -x rune_broker 2>/dev/null; sleep 0.3; true
}

# ---- Kafka 3-node (KRaft combined broker+controller) -----------------------
KAFKA_CLUSTER_PIDS=()
kafka_start_cluster() {
  local persistence="${1:-nvme}"
  pkill -9 -f kafka.Kafka 2>/dev/null; sleep 1
  local voters="1@127.0.0.1:9093,2@127.0.0.1:9095,3@127.0.0.1:9097"
  local cid; cid="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)"
  KAFKA_CLUSTER_PIDS=()
  local n
  for n in 1 2 3; do
    local bport=$((9092 + (n-1)*2)) cport=$((9093 + (n-1)*2))
    local logdir; logdir="$(data_root "$persistence" kafka-logs-$n)"; rm -rf "$logdir"; mkdir -p "$logdir"
    local cfg="$WORK/kraft-$n.properties"
    cat >"$cfg" <<EOF
process.roles=broker,controller
node.id=$n
controller.quorum.voters=$voters
listeners=PLAINTEXT://127.0.0.1:$bport,CONTROLLER://127.0.0.1:$cport
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
advertised.listeners=PLAINTEXT://127.0.0.1:$bport
log.dirs=$logdir
num.network.threads=4
num.io.threads=8
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
EOF
    "$KAFKA_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$cfg" >/dev/null 2>&1 || { log "kafka-storage format node$n failed"; return 1; }
    # shellcheck disable=SC2086
    env KAFKA_HEAP_OPTS="$KAFKA_HEAP" $(pin "$CORES_BROKER") \
        "$KAFKA_HOME"/bin/kafka-server-start.sh "$cfg" >"$WORK/kafka-$n.log" 2>&1 &
    KAFKA_CLUSTER_PIDS+=($!)
  done
  local i
  for i in $(seq 1 120); do
    "$KAFKA_HOME"/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1 && { log "kafka 3-node ready"; return 0; }
    sleep 1
  done
  log "KAFKA CLUSTER READY TIMEOUT"; tail -20 "$WORK/kafka-1.log" >&2; return 1
}
KAFKA_CLUSTER_BOOTSTRAP="127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096"
kafka_stop_cluster() {
  "$KAFKA_HOME"/bin/kafka-server-stop.sh >/dev/null 2>&1
  local p; for p in "${KAFKA_CLUSTER_PIDS[@]:-}"; do [ -n "$p" ] && { for _ in $(seq 1 20); do kill -0 "$p" 2>/dev/null || break; sleep 0.5; done; kill -9 "$p" 2>/dev/null; }; done
  KAFKA_CLUSTER_PIDS=(); pkill -9 -f kafka.Kafka 2>/dev/null; sleep 1; true
}
