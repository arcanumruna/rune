#!/usr/bin/env bash
# Shared configuration for the Rune-vs-Kafka comparison harness.
# Every path/knob is overridable via the environment so the same scripts run on
# build-agent-01 (prototype) and on the rented ccx43 (final numbers).
#
# Source this from every driver:  source "$(dirname "$0")/env.sh"

# ---- binaries -------------------------------------------------------------
: "${RUNE_BROKER_BIN:=/root/rune-e5/rune-broker/build/Release/build/Release/rune_broker}"
: "${RUNE_PROD_BIN:=/root/rune-e5/rune-client-cpp/build/Release/build/Release/rune_produce_throughput}"
: "${RUNE_CONS_BIN:=/root/rune-e5/rune-client-cpp/build/Release/build/Release/rune_consume_throughput}"
# The rune CLI creates the bench topics by DDL once the broker is up (the brokers take no topic list).
: "${RUNE_CLI:=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)/rune-client-cpp/build/rune}"   # ../rune-client-cpp/build/rune
: "${KAFKA_HOME:=/root/kafka}"
# The env var of the rune_produce/consume_throughput CLIENT binaries that carries THEIR topic table
# "name:topicId:partitions" (a client setting, unrelated to the brokers).
RUNE_CLIENT_TOPICS_ENV="RUNE_TOPICS"

# ---- scratch + results ----------------------------------------------------
: "${WORK:=/dev/shm/kbench}"                 # tmpfs scratch: broker/kafka LOGS, client data
: "${RESULTS_DIR:=$HOME/kbench-results}"
: "${RESULTS_RAW:=$RESULTS_DIR/results.jsonl}"
: "${SPECS_JSON:=$RESULTS_DIR/specs.json}"

# ---- data-storage roots (persistence dimension) ---------------------------
# The broker LOG / kafka log.dirs go under one of these, chosen per cell by the
# 'persistence' param: nvme -> real disk, tmpfs -> /dev/shm.
: "${DISK_ROOT:=/root/kbench-disk}"          # a path on the real NVMe/SSD
: "${TMPFS_ROOT:=/dev/shm/kbench-data}"      # RAM-backed
# data_root <nvme|tmpfs> <subdir> -> echoes the absolute dir (created).
data_root() {
  local base; case "$1" in tmpfs) base="$TMPFS_ROOT";; *) base="$DISK_ROOT";; esac
  echo "$base/$2"
}

# ---- core pinning (empty = no taskset; set on ccx43) ----------------------
: "${CORES_BROKER:=}"                        # e.g. "0-7"
: "${CORES_CLIENT:=}"                        # e.g. "8-11"

# ---- rune ports -----------------------------------------------------------
: "${RUNE_CONSENSUS_PORT:=40100}"
: "${RUNE_CLIENT_PORT:=40200}"
: "${RUNE_EGRESS_PORT:=40400}"
: "${RUNE_WORKER_EGRESS_BASE:=40300}"

# ---- kafka ----------------------------------------------------------------
: "${KAFKA_BOOTSTRAP:=127.0.0.1:9092}"
: "${KAFKA_VERSION:=4.0.0}"
: "${KAFKA_SCALA:=2.13}"
: "${KAFKA_HEAP:=-Xmx2g -Xms2g}"

# ---- default record/repeat knobs (smoke overrides these to tiny) ----------
: "${REPEATS:=3}"
: "${WARMUP:=5000}"

export RUNE_BROKER_BIN RUNE_PROD_BIN RUNE_CONS_BIN RUNE_CLI KAFKA_HOME
export WORK RESULTS_DIR RESULTS_RAW SPECS_JSON CORES_BROKER CORES_CLIENT DISK_ROOT TMPFS_ROOT
export RUNE_CONSENSUS_PORT RUNE_CLIENT_PORT RUNE_EGRESS_PORT RUNE_WORKER_EGRESS_BASE
export KAFKA_BOOTSTRAP KAFKA_VERSION KAFKA_SCALA KAFKA_HEAP REPEATS WARMUP

mkdir -p "$RESULTS_DIR" "$WORK" "$DISK_ROOT" "$TMPFS_ROOT"

# taskset prefix helper: pin "$1"=corelist -> echoes "taskset -c <list>" or "".
pin() { [ -n "$1" ] && echo "taskset -c $1" || true; }
