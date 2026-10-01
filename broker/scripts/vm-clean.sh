#!/usr/bin/env bash
# vm-clean.sh — run on k3s-node-01 BEFORE every benchmark / DR e2e run.
# Kills leftover Rune/Kafka/bench processes by EXACT name (never `pkill -f`
# patterns that can match the calling ssh command line), removes Rune DR
# containers, then refuses to proceed unless the box is quiet and has room.
# Exit 0 = clean and ready; non-zero = do not benchmark.
set -u
PROCS=(rune_broker rune_produce_th rune_produce_throughput rune_consume_throughput
       aeronmd rune_broker_benchmark rune_vm_bench.sh rune_vm_bench_bo.sh bench.sh dr_client)
for p in "${PROCS[@]}"; do pkill -KILL -x "$p" 2>/dev/null || true; done
# Kafka (java) by its main class, anchored to the java binary's argv[0].
for pid in $(pgrep -x java 2>/dev/null); do
  tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -q 'kafka\.Kafka' && kill -KILL "$pid" 2>/dev/null
done
if command -v docker >/dev/null 2>&1; then
  ids=$(docker ps -aq --filter 'name=^rune-' 2>/dev/null); [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
fi
rm -rf /tmp/fr* 2>/dev/null   # rune-fr traces fill the root disk (see memory: disk-full masquerade)

strays=0
for p in "${PROCS[@]}"; do pgrep -x "$p" >/dev/null 2>&1 && { echo "STRAY: $p still running" >&2; strays=1; }; done
[ "$strays" = 0 ] || exit 2

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
