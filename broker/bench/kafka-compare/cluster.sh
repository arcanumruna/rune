#!/usr/bin/env bash
# 3-node co-located clusters (Rune + Kafka) + DR/failover helpers.
# Source env.sh + lib.sh first. Co-located on ONE box (loopback) — measures
# replication-protocol cost + failover, NOT cross-host network latency.
: "${RUNE_ROLE_BIN:=/root/rune-e5/rune-broker/build/Release/build/Release/rune_role}"

# ---- Rune 3-node -----------------------------------------------------------
# node i (0,1,2): consensus=40110+i client=40200+i repl=40220+i wker-egress-base=40400+i*20
RUNE_CLUSTER_PIDS=()
# Shard mode (RUNE_SHARD=1, spec §3.3): node i gets explicit, distinct per-worker
# ranges — ingress 40500+20i, replication 40600+20i — advertised in RUNE_PEERS
# (id@host:consensus:client:repl:replBase:ingressBase:egressControlBase).  <= 20
# workers per node.  RUNE_SHARD unset: plain peers — the broker DERIVES the shard
# ports and the core layout (the default since 2026-10-02).
rune_cluster_peers() {
  if [ "${RUNE_SHARD:-0}" = "1" ]; then
    echo "0@127.0.0.1:40110:40200:40220:40600:40500:40400,1@127.0.0.1:40111:40201:40221:40620:40520:40420,2@127.0.0.1:40112:40202:40222:40640:40540:40440"
  else
    echo "0@127.0.0.1:40110:40200:40220,1@127.0.0.1:40111:40201:40221,2@127.0.0.1:40112:40202:40222"
  fi
}
# rune_shard_env <i> <workers> -> extra env for node i (empty unless RUNE_SHARD=1).
# Node i's W busy-spinning shard workers are pinned to cores CORE0+i*W .. CORE0+i*W+W-1
# (WorkerPool pins worker w to RUNE_WORKER_CORE_BASE+w), so nodes never share a core.
rune_shard_env() {
  [ "${RUNE_SHARD:-0}" = "1" ] || return 0
  local w="${2:-1}"
  echo "RUNE_WORKER_MODE=shard RUNE_RUNTIME_PLACEMENT=${RUNE_RUNTIME_PLACEMENT:-fused} RUNE_WORKER_INGRESS_BASE_PORT=$((40500+20*$1)) RUNE_WORKER_REPL_BASE_PORT=$((40600+20*$1)) RUNE_WORKER_EGRESS_CONTROL_BASE_PORT=$((40400+20*$1)) RUNE_WORKER_CORE_BASE=$(( ${RUNE_SHARD_CORE0:-1} + $1 * w ))"
}
# rune_shard_layout_check <workers> [cores] -> non-zero (with a message) when the 3
# nodes' pinned busy-spin workers do not fit on distinct cores: overlapping or
# out-of-range pins silently distort every shard-mode number.
rune_shard_layout_check() {
  [ "${RUNE_SHARD:-0}" = "1" ] || return 0
  local w="$1" cores="${2:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}" core0="${RUNE_SHARD_CORE0:-1}"
  local need=$(( core0 + 3 * w ))
  if [ "$need" -gt "$cores" ]; then
    echo "refusing shard layout: 3 nodes x $w workers from core $core0 needs cores ${core0}..$((need-1)) but the box has $cores" >&2
    return 1
  fi
  local i; for i in 0 1 2; do
    echo "node $i -> cores $(( core0 + i*w ))..$(( core0 + i*w + w - 1 ))" >&2
  done
}
# The broker honours RUNE_AERON_DIR (set to $WORK/rune-c$i/a below, on tmpfs):
# Aeron's CnC + term buffers must NOT live on the disk-backed data dir, and
# rune_role reads the role counter from that same dir.
CLUSTER_PERSISTENCE="nvme"
_rune_node_aeron() { echo "$WORK/rune-c$1/a"; }

# rune_start_cluster <workers> <topics_spec> [io] [persistence]
#   topics_spec: see rune_create_topics (created by DDL once the cluster has a leader).
# 2026-10-02: the broker's DEFAULTS are the measured 110k config (shard + fused,
# 16 MiB non-sparse terms, 32 MiB rings, derived MTU / socket buffer / window,
# uring, derived shard ports + core layout).  This harness therefore passes a
# sizing / io / worker knob ONLY when the caller set it (env or a non-empty arg):
# an unset knob means "the broker default".  workers="" / io="" = default.
rune_start_cluster() {
  local workers="$1" topics="$2" io="${3-}" persistence="${4:-nvme}"
  CLUSTER_PERSISTENCE="$persistence"
  local peers; peers="$(rune_cluster_peers)"
  pkill -9 -x rune_broker 2>/dev/null; sleep 0.5
  RUNE_CLUSTER_PIDS=()
  [ -n "$workers" ] && { rune_shard_layout_check "$workers" || return 1; }
  # Explicit knobs only (KEY=VALUE words; empty when unset).
  local k knobs=""
  for k in RUNE_AERON_TERM_BUFFER_BYTES RUNE_WORKER_INGRESS_RING_BYTES RUNE_WORKER_RETURN_RING_BYTES \
           RUNE_REPL_OUTBOUND_RING_BYTES RUNE_AERON_MTU RUNE_AERON_RCV_WINDOW RUNE_AERON_SOCKET_BUFFER_BYTES \
           RUNE_AERON_TERM_SPARSE RUNE_WORKER_MODE RUNE_RUNTIME_PLACEMENT RUNE_WORKER_IDLE RUNE_RUNTIME_IDLE \
           RUNE_AERON_DRIVER_IDLE RUNE_PER_WORKER_INGRESS RUNE_PER_WORKER_EGRESS RUNE_MIN_ISR; do
    [ -n "${!k:-}" ] && knobs="$knobs $k=${!k}"
  done
  [ -n "$io" ] && knobs="$knobs RUNE_IO_BACKEND=$io"
  [ -n "$workers" ] && knobs="$knobs RUNE_WORKERS=$workers"
  log "rune cluster knobs (explicit; everything else = broker default):${knobs:- none}"
  local i
  for i in 0 1 2; do
    local cport=$((40200+i))
    local logdir; logdir="$(data_root "$persistence" rune-log-$i)"
    local d="$WORK/rune-c$i"; rm -rf "$d" "$logdir"; mkdir -p "$d/a" "$logdir"
    local pinprefix; pinprefix="$(pin "$CORES_BROKER")"
    [ "${RUNE_SHARD:-0}" = "1" ] && pinprefix=""
    # Legacy (RUNE_WORKER_MODE=legacy) keeps its pre-2026-10-02 harness wiring:
    # per-worker ingress/egress planes on, a fixed egress-control base per node.
    # Shard mode derives the base per node id unless RUNE_SHARD=1 sets the layout.
    local eg=""
    if [ "${RUNE_WORKER_MODE:-}" = "legacy" ]; then
      eg="RUNE_WORKER_EGRESS_CONTROL_BASE_PORT=$((40400+i*20))"
      [ -z "${RUNE_PER_WORKER_INGRESS:-}" ] && eg="$eg RUNE_PER_WORKER_INGRESS=true"
      [ -z "${RUNE_PER_WORKER_EGRESS:-}" ] && eg="$eg RUNE_PER_WORKER_EGRESS=true"
    fi
    # A taskset prefix owns placement: never let the broker auto-pin past it.
    [ -n "$pinprefix" ] && [ -z "${RUNE_WORKER_CORE_BASE:-}" ] && eg="$eg RUNE_WORKER_CORE_BASE=-1"
    # shellcheck disable=SC2086
    # Co-located loopback cluster: bind the client endpoint on 127.0.0.1 (a
    # wildcard bind accepts remote clients, so the broker would derive the NIC MTU
    # instead of the 64 KiB loopback MTU).
    env RUNE_NODE_ID=$i RUNE_PEERS="$peers" RUNE_CLIENT_ENDPOINT="${RUNE_CLIENT_BIND:-127.0.0.1}:$cport" \
        RUNE_DATA_DIR="$logdir" RUNE_AERON_DIR="$d/a" \
        $knobs $eg \
        $(rune_shard_env "$i" "${workers:-1}") \
        RUNE_ELECTION_TIMEOUT_MS="${RUNE_ELECTION_TIMEOUT_MS:-300}" \
        RUNE_ELECTION_STATUS_INTERVAL_MS="${RUNE_ELECTION_STATUS_INTERVAL_MS:-50}" \
        RUNE_STARTUP_CANVASS_TIMEOUT_MS="${RUNE_STARTUP_CANVASS_TIMEOUT_MS:-200}" \
        RUNE_LOG_LEVEL="${RUNE_LOG_LEVEL:-info}" RUNE_FS_LOG="${RUNE_FS_LOG:-0}" \
        $pinprefix "$RUNE_BROKER_BIN" >"$d/broker.log" 2>&1 &
    RUNE_CLUSTER_PIDS+=($!)
  done
  # Wait for a leader via rune_role (reliable — reads the Aeron role counter).
  local leader; leader="$(rune_wait_leader 60)"
  [ -z "$leader" ] && { log "CLUSTER: no leader elected"; for i in 0 1 2; do tail -5 "$WORK/rune-c$i/broker.log"; done; return 1; }
  # The brokers boot with an empty catalog: create the topics by DDL (retries while the catalog leader forms).
  rune_create_topics "$((40200+leader))" "$topics" || return 1
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
