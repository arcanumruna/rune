#!/usr/bin/env bash
# vm-clean.sh — run on k3s-node-01 BEFORE every benchmark / DR e2e run.
# Kills leftover Rune/Kafka/bench processes by EXACT name (never `pkill -f`
# patterns that can match the calling ssh command line), removes Rune DR
# containers, then refuses to proceed unless the box is quiet and has room.
# Exit 0 = clean and ready; non-zero = do not benchmark.
set -u
PROCS=" rune_broker rune_produce_th rune_produce_throughput rune_consume_throughput aeronmd rune_broker_benchmark rune_vm_bench.sh rune_vm_bench_bo.sh bench.sh dr_client "

# Our own ancestry (this script, its caller script, the ssh session) is never a target.
declare -A SELF=()
a=$$
while [ -n "$a" ] && [ "$a" -gt 1 ]; do
  SELF[$a]=1
  a=$(awk '{print $4}' /proc/$a/stat 2>/dev/null)
done

# Full names, not /proc/comm (truncated to 15 chars): the executable's basename,
# or for interpreters (bash/sh) the script argument's basename.
proc_name() {
  local pid=$1 exe a0 a1
  exe=$(basename -- "$(readlink /proc/$pid/exe 2>/dev/null)" 2>/dev/null)
  case "$exe" in
    bash|sh|dash)
      a1=$(tr '\0' '\n' 2>/dev/null < /proc/$pid/cmdline | sed -n 2p)
      [ -n "$a1" ] && basename -- "$a1" || echo "$exe" ;;
    *)
      a0=$(tr '\0' '\n' 2>/dev/null < /proc/$pid/cmdline | sed -n 1p)
      basename -- "${a0:-$exe}" 2>/dev/null ;;
  esac
}
targets() {
  local d pid n
  for d in /proc/[0-9]*; do
    pid=${d#/proc/}
    [ -n "${SELF[$pid]:-}" ] && continue
    n=$(proc_name "$pid")
    case "$PROCS" in *" $n "*) echo "$pid $n" ;; esac
  done
}
targets | while read -r pid n; do kill -KILL "$pid" 2>/dev/null; done
# Kafka (java) by its main class.
for pid in $(pgrep -x java 2>/dev/null); do
  tr '\0' ' ' 2>/dev/null < /proc/$pid/cmdline | grep -q 'kafka\.Kafka' && kill -KILL "$pid" 2>/dev/null
done
if command -v docker >/dev/null 2>&1; then
  ids=$(docker ps -aq --filter 'name=^rune-' 2>/dev/null); [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
fi
# flight-recorder traces (default FLIGHT_RECORDER_OUT=/tmp/frout*) + broker egress scratch fill the root disk.
rm -rf /tmp/frout* /tmp/rune_bn_egress_* 2>/dev/null

# Strays: re-check a few times (SIGKILLed processes take a moment to be reaped).
left=""
for _ in 1 2 3 4 5; do left=$(targets); [ -z "$left" ] && break; sleep 0.2; done
[ -z "$left" ] || { echo "STRAY: $left" >&2; exit 2; }

# Wait (bounded) for the box to go quiet: load1 < 1.
for _ in $(seq 1 60); do
  l=$(cut -d' ' -f1 /proc/loadavg); awk -v l="$l" 'BEGIN{exit !(l<1.0)}' && break; sleep 5
done
l=$(cut -d' ' -f1 /proc/loadavg)
awk -v l="$l" 'BEGIN{exit !(l<1.0)}' || { echo "BUSY: load1=$l" >&2; exit 3; }

root_free_kb=$(df -Pk / | awk 'NR==2{print $4}')
shm_free_kb=$(df -Pk /dev/shm | awk 'NR==2{print $4}')
[ "$root_free_kb" -ge $((3*1024*1024)) ] || { echo "LOW DISK: / has $((root_free_kb/1024)) MiB free (<3 GiB)" >&2; exit 4; }
[ "$shm_free_kb" -ge $((1024*1024)) ] || { echo "LOW SHM: /dev/shm has $((shm_free_kb/1024)) MiB free (<1 GiB)" >&2; exit 5; }
echo "vm-clean: OK (load1=$l, root free $((root_free_kb/1024)) MiB, shm free $((shm_free_kb/1024)) MiB)"
