#!/usr/bin/env bash
# scripts/t15b-gates.sh — T15b gates 2-5 on k3s-node-01 against a REAL 3-node
# Rune cluster (rune-broker bench/kafka-compare/cluster.sh), client = shard_gate.
#
#   t15b-gates.sh gate2          default-config e2e: legacy + shard broker, shard-agent AUTO and N=1
#   t15b-gates.sh gate3          N=2 UDP egress (base+i) vs the shard broker: per-shard acks on own port
#   t15b-gates.sh gate4          SIGSTOP the client process > 15 s, SIGCONT: Live -> Reconnecting -> Live
#   t15b-gates.sh gate5 <n>      interleaved ABAB x n: client-thread vs shard-agent, client-invoker vs shard-invoker
#
# Every run starts from a passing vm-clean.sh (kills by exact name, waits load1<1).
# Dirs: G=/root/t15b-gates (own), WORK=/dev/shm/t15b, DISK_ROOT=$G/disk.
# Never pkill -f / pgrep -f: processes are killed by exact name or pid.
set -u
G="${G:-/root/t15b-gates}"
K="$G/rune-broker/bench/kafka-compare"
SG="$G/rune-client-cpp/build-rel/shard_gate"
VMCLEAN=/root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh
export RUNE_BROKER_BIN="$G/rune-broker/build-rel/rune_broker" RUNE_ROLE_BIN="$G/rune-broker/build-rel/rune_role"
export WORK=/dev/shm/t15b DISK_ROOT="$G/disk" TMPFS_ROOT=/dev/shm/t15b-data RESULTS_DIR="$G/results"
# 4-core box: 16 MiB terms/rings (64 MiB thrashes it); F28: an explicit receive
# window (the 128 KiB default starves replication), window <= SO buffers <= sysctl, <= term/2.
export RUNE_AERON_TERM_BUFFER_BYTES=16777216 RUNE_WORKER_INGRESS_RING_BYTES=16777216
export RUNE_WORKER_RETURN_RING_BYTES=16777216 RUNE_REPL_OUTBOUND_RING_BYTES=16777216
export RUNE_AERON_SOCKET_BUFFER_BYTES=16777216 RUNE_AERON_RCV_WINDOW=4194304
TOPIC_SPEC="bench:8:tier=majority:durability=all-in-sync"
OUT="$G/results"; mkdir -p "$OUT" "$DISK_ROOT"
cd "$K" || exit 1
set +u; source env.sh; source lib.sh; source cluster.sh; set -u

# Never clean (kill) while another session's Rune run is live: wait — bounded
# (WAIT_MAX_S, default 3 h) — until none of the FOREIGN processes (exact names)
# has existed for 3 checks 10 s apart; then vm-clean.sh.  (No load1 wait here:
# the box is dedicated and vm-clean.sh has its own quiet check.)
FOREIGN="rune_broker rune_broker_benchmark rune_produce_throughput rune_consume_throughput aeronmd dr_client"
wait_free() {
    local quiet=0 waited=0 p busy
    while [ "$quiet" -lt 3 ]; do
        busy=""
        for p in $FOREIGN; do pgrep -x "$p" >/dev/null 2>&1 && busy="$busy $p"; done
        if [ -z "$busy" ]; then quiet=$((quiet + 1)); else quiet=0; fi
        [ "$quiet" -ge 3 ] && break
        [ "$waited" -ge "${WAIT_MAX_S:-10800}" ] && { echo "BOX-BUSY: gave up after ${waited}s (busy:$busy)"; exit 8; }
        [ "$quiet" -eq 0 ] && [ $((waited % 300)) -eq 0 ] && echo "waiting for a free box (busy:$busy) $(date +%T)"
        sleep 10; waited=$((waited + 10))
    done
}

clean() {
    pkill -9 -x shard_gate 2>/dev/null
    wait_free
    rune_stop_cluster >/dev/null 2>&1
    bash "$VMCLEAN" || { echo "VM-CLEAN FAILED"; exit 9; }
    rm -rf "$WORK" "$TMPFS_ROOT" "$DISK_ROOT"/rune-log-* /tmp/shard_gate_* 2>/dev/null
    mkdir -p "$WORK" "$TMPFS_ROOT" "$DISK_ROOT"
}
# start <shard 0|1> -> sets LEADER (the seed's node)
start() {
    export RUNE_SHARD="$1"
    local workers=2; [ "$1" = "1" ] && workers=1
    LEADER="$(rune_start_cluster "$workers" "$TOPIC_SPEC" uring nvme)"
    [ -z "$LEADER" ] && { echo "NO-LEADER"; return 1; }
    sleep 6  # startup churn -> ISR=3
    GW="127.0.0.1:$((40200 + LEADER))"
}
stop() { rune_stop_cluster >/dev/null 2>&1; }
sg() { "$SG" --gateway "$GW" --topic bench --topic-id 0 --partitions 8 "$@"; }

# one produce + read-back verify; prints RESULT and VERIFY lines
produce_verify() {  # <tag> <shard_gate args...>
    local tag="$1"; shift
    local r v n
    r="$(sg "$@" 2>"$OUT/$tag.err" | grep -E '^RESULT')"
    n="$(echo "$r" | grep -oE 'accepted=[0-9]+' | cut -d= -f2)"
    v="$(sg --arm verify --expect "${n:-0}" --egress 127.0.0.1:41900 2>>"$OUT/$tag.err" | grep -E '^VERIFY')"
    echo "$tag $r"; echo "$tag $v"
}

gate2() {  # GATE2_MODES="0 1" (0 legacy, 1 shard broker) — resume a partial run
    local mode
    for mode in ${GATE2_MODES:-0 1}; do
        local kind=legacy; [ "$mode" = "1" ] && kind=shard
        for shards in 0 1; do
            clean; start "$mode" || continue
            produce_verify "gate2-$kind-shards$shards" --arm shard-agent --shards "$shards" --records 100000 \
                --value-bytes 1024 --acks all --egress 127.0.0.1:41000
            stop
        done
    done
}

gate3() {
    clean; start 1 || return 1
    produce_verify "gate3-udp-n2" --arm shard-agent --shards 2 --records 200000 --value-bytes 1024 --acks all \
        --egress 127.0.0.1:41000
    stop
}

gate4() {
    clean; start 1 || return 1
    local fifo="$OUT/gate4.in" log="$OUT/gate4.out"
    rm -f "$fifo" "$log"; mkfifo "$fifo"
    sg --arm shard-agent --shards 2 --records 3000000 --rate 5000 --value-bytes 1024 --acks all \
        --egress 127.0.0.1:41000 --sigstop <"$fifo" >"$log" 2>"$OUT/gate4.err" &
    local pid=$!
    exec 7>"$fifo"  # hold the fifo open (the client reads 'q' from it)
    local i
    for i in $(seq 1 120); do
        [ "$(grep -c '^EVENT [01] Live' "$log")" -ge 2 ] && break; sleep 0.5
    done
    echo "gate4: both shards Live: $(grep -c '^EVENT [01] Live' "$log") live events"
    sleep 5
    kill -STOP "$pid"; echo "gate4: SIGSTOP $(date +%T)"; sleep 16
    local mark; mark="$(wc -l <"$log")"
    kill -CONT "$pid"; echo "gate4: SIGCONT $(date +%T)"
    for i in $(seq 1 120); do
        [ "$(tail -n +"$((mark + 1))" "$log" | grep -c '^EVENT [01] Live')" -ge 2 ] && break; sleep 0.5
    done
    sleep 10
    echo q >&7; exec 7>&-
    for i in $(seq 1 240); do grep -q '^RESULT' "$log" && break; sleep 0.5; done
    wait "$pid" 2>/dev/null
    echo "gate4 events:"; grep '^EVENT' "$log"
    local r n; r="$(grep '^RESULT' "$log")"; echo "gate4 $r"
    n="$(echo "$r" | grep -oE 'accepted=[0-9]+' | cut -d= -f2)"
    sg --arm verify --expect "${n:-0}" --egress 127.0.0.1:41900 2>>"$OUT/gate4.err" | grep -E '^VERIFY' | sed 's/^/gate4 /'
    stop
}

gate5() {
    local n="${1:-15}" rate="${RATE:-100000}" secs="${SECS:-10}"
    local recs=$((rate * secs + 1000))
    local i
    for i in $(seq 1 "$n"); do
        # A/B: today's Thread-mode Client vs an Agent shard (N=1, D1, BusySpin, pinned core 0)
        for arm in client-thread shard-agent; do
            clean; start 1 || continue
            sg --arm "$arm" --shards 1 --busy-spin --core 0 --rate "$rate" --seconds "$secs" --records "$recs" \
                --value-bytes 1024 --acks all --egress 127.0.0.1:41000 2>>"$OUT/gate5.err" | grep '^RESULT' | sed "s/^/gate5 i=$i /"
            stop
        done
        # A/B: Invoker Client vs Invoker shard, owner on core 0
        for arm in client-invoker shard-invoker; do
            clean; start 1 || continue
            sg --arm "$arm" --core 0 --rate "$rate" --seconds "$secs" --records "$recs" \
                --value-bytes 1024 --acks all --egress 127.0.0.1:41000 2>>"$OUT/gate5.err" | grep '^RESULT' | sed "s/^/gate5 i=$i /"
            stop
        done
    done
}

case "${1:-}" in
    gate2) gate2 ;;
    gate3) gate3 ;;
    gate4) gate4 ;;
    gate5) shift; gate5 "${1:-15}" ;;
    *) echo "usage: $0 gate2|gate3|gate4|gate5 [n]"; exit 2 ;;
esac
clean >/dev/null 2>&1
