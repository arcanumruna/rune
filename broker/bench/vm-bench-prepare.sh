#!/usr/bin/env bash
# vm-bench-prepare.sh - put the Linux benchmark VM into the state every Rune benchmark number assumes.
#
#   flock -w 7200 /root/vm.lock env RUNE_VM_LOCK_HELD=1 broker/bench/vm-bench-prepare.sh
#   ... run the benchmark ...
#   flock -w 7200 /root/vm.lock env RUNE_VM_LOCK_HELD=1 broker/bench/vm-bench-restore.sh
#
# Steps (BENCHMARKING.md, "Box preparation"), in this order:
#   1. require the host lock                       5. loopback fast path (NOTRACK + ACCEPT on lo)
#   2. kill every benchmark process, clean shm     6. socket-buffer sysctls
#   3. stop k3s (never reboots)                    7. pre-flight checks
#   4. flush iptables / ip6tables
# Everything it changes is saved first under the run directory, so vm-bench-restore.sh can undo it, also after
# a partial prepare.  If a step or a pre-flight check fails, prepare restores what it already changed and exits non-zero.
#
# Environment (no defaults point at a host; this script only ever acts on the machine it runs on):
#   RUNE_VM_LOCK_HELD=1        required: the caller took the host lock (flock on RUNE_VM_LOCK_FILE)
#   RUNE_VM_LOCK_FILE          default /root/vm.lock
#   RUNE_BENCH_STATE_ROOT      default /root/rune-bench-runs; the run dir is <root>/run-<utc time>-<pid>
#   RUNE_BENCH_RUN_DIR         use this run dir instead of creating one
#   RUNE_BENCH_PORTS           space-separated ports or LOW-HIGH ranges that must be free (default "40100-40700 8200-8600")
#   RUNE_BENCH_SHM_GLOBS       /dev/shm globs removed as leftovers (default "/dev/shm/kbench* /dev/shm/aeron-*")
#   RUNE_BENCH_MAX_CPU_PCT     refuse if another process uses more CPU than this (default 5.0)
#   RUNE_BENCH_MIN_SHM_KB / RUNE_BENCH_MIN_ROOT_KB   free-space floors (default 1 GiB / 3 GiB)
#   K3S_KILLALL                default /usr/local/bin/k3s-killall.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCK_FILE="${RUNE_VM_LOCK_FILE:-/root/vm.lock}"
STATE_ROOT="${RUNE_BENCH_STATE_ROOT:-/root/rune-bench-runs}"
PORTS="${RUNE_BENCH_PORTS:-40100-40700 8200-8600}"
SHM_GLOBS="${RUNE_BENCH_SHM_GLOBS:-/dev/shm/kbench* /dev/shm/aeron-*}"
MAX_CPU="${RUNE_BENCH_MAX_CPU_PCT:-5.0}"
MIN_SHM_KB="${RUNE_BENCH_MIN_SHM_KB:-1048576}"
MIN_ROOT_KB="${RUNE_BENCH_MIN_ROOT_KB:-3145728}"
K3S_KILLALL="${K3S_KILLALL:-/usr/local/bin/k3s-killall.sh}"

log() { echo "[vm-bench-prepare] $*" >&2; }
die() { log "FAIL: $*"; exit 1; }

# ---- 1. the host lock -------------------------------------------------------------------------------------
[ "${RUNE_VM_LOCK_HELD:-0}" = "1" ] || die "run under the host lock: flock -w 7200 ${LOCK_FILE} env RUNE_VM_LOCK_HELD=1 $0"
[ "$(id -u)" -eq 0 ] || die "must run as root"
# The wrapper claims it holds the lock; make sure somebody does: a fresh non-blocking flock must FAIL.
if exec 8>>"${LOCK_FILE}" && flock -n 8; then
    die "${LOCK_FILE} is not locked: RUNE_VM_LOCK_HELD=1 was set without holding the lock"
fi
exec 8>&-

RUN_DIR="${RUNE_BENCH_RUN_DIR:-${STATE_ROOT}/run-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
mkdir -p "${RUN_DIR}"
ln -sfn "${RUN_DIR}" "${STATE_ROOT}/latest"
exec > >(tee -a "${RUN_DIR}/prepare.log") 2>&1
log "run dir ${RUN_DIR}"
: >"${RUN_DIR}/workdirs"   # work dirs the benchmark registers here; restore removes them

PREPARED=0
on_exit() {
    local rc=$?
    if [ "${PREPARED}" -ne 1 ]; then
        log "prepare failed (exit ${rc}): restoring what was changed"
        RUNE_BENCH_RUN_DIR="${RUN_DIR}" bash "${HERE}/vm-bench-restore.sh" || log "restore also failed: run it by hand"
    fi
}
trap on_exit EXIT

{
    echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "kernel=$(uname -r)"
    echo "vcpus=$(nproc)"
    echo "mem_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
} >"${RUN_DIR}/box.txt"

# ---- 2. kill every benchmark-relevant process --------------------------------------------------------------
# vm-clean.sh SIGKILLs by exact name (rune_broker, bench clients, aeronmd, Kafka, dr_client), removes rune-* containers
# and /tmp/frout*, waits for load1 < 1 and checks 3 GiB on / and 1 GiB on /dev/shm.  It kills on a shared box, so it
# runs only here, inside the lock.
log "step 2: vm-clean.sh"
bash "${HERE}/../scripts/vm-clean.sh"
log "removing leftover shm/work dirs: ${SHM_GLOBS}"
for g in ${SHM_GLOBS}; do
    for f in ${g}; do
        [ -e "${f}" ] || continue
        log "  rm -rf ${f}"
        rm -rf -- "${f}"
    done
done
log "top CPU consumers:"
ps -eo pid,comm,%cpu --sort=-%cpu | head -n 8 | tee "${RUN_DIR}/top-cpu.txt"

# ---- 3. stop k3s ---------------------------------------------------------------------------------------------
log "step 3: stop k3s"
K3S_WAS="$(systemctl is-active k3s 2>/dev/null || true)"
echo "${K3S_WAS:-unknown}" >"${RUN_DIR}/k3s.was"
if [ "${K3S_WAS}" = "active" ]; then
    systemctl stop k3s
    [ -x "${K3S_KILLALL}" ] && "${K3S_KILLALL}"
fi

# ---- 4. iptables ---------------------------------------------------------------------------------------------
log "step 4: save and flush iptables"
iptables-save >"${RUN_DIR}/iptables.save"
command -v ip6tables-save >/dev/null 2>&1 && ip6tables-save >"${RUN_DIR}/ip6tables.save"
flush_family() {
    local ipt="$1" t
    command -v "${ipt}" >/dev/null 2>&1 || return 0
    for t in filter nat mangle raw; do
        "${ipt}" -t "${t}" -F 2>/dev/null || true
        "${ipt}" -t "${t}" -X 2>/dev/null || true
    done
    for c in INPUT FORWARD OUTPUT; do "${ipt}" -P "${c}" ACCEPT; done
}
touch "${RUN_DIR}/iptables.flushed"   # from here restore re-applies the saved rules, also after a partial flush
flush_family iptables
flush_family ip6tables

# ---- 5. loopback fast path ------------------------------------------------------------------------------------
# Skips connection tracking and the rule walk for loopback: small-send syscall p50 30 -> 19.5 us, p99 110 -> 67 us.
log "step 5: loopback NOTRACK + ACCEPT"
ensure() {   # ensure <table-args...> -- <rule...>: insert unless present
    local tbl=() rule=()
    while [ "$1" != "--" ]; do tbl+=("$1"); shift; done
    shift
    rule=("$@")
    iptables "${tbl[@]}" -C "${rule[@]}" 2>/dev/null || iptables "${tbl[@]}" -I "${rule[@]}"
}
ensure -t raw -- PREROUTING -i lo -j NOTRACK
ensure -t raw -- OUTPUT -o lo -j NOTRACK
ensure -- INPUT -i lo -j ACCEPT
ensure -- OUTPUT -o lo -j ACCEPT

# ---- 6. sysctls (old values saved first) -----------------------------------------------------------------------
# Constraints: rcv window <= socket buffer <= rmem_max/wmem_max, and window <= term/2.
log "step 6: sysctls"
SYSCTLS="net.core.rmem_max=16777216 net.core.wmem_max=16777216 net.core.rmem_default=16777216 net.core.wmem_default=16777216 net.core.netdev_max_backlog=100000"
: >"${RUN_DIR}/sysctl.save"
for kv in ${SYSCTLS}; do
    key="${kv%%=*}"
    echo "${key}=$(sysctl -n "${key}")" >>"${RUN_DIR}/sysctl.save"
done
for kv in ${SYSCTLS}; do sysctl -q -w "${kv}"; done

# ---- 7. pre-flight ---------------------------------------------------------------------------------------------
log "step 7: pre-flight checks"
load1="$(cut -d' ' -f1 /proc/loadavg)"
awk -v l="${load1}" 'BEGIN{exit !(l<1.0)}' || die "load1=${load1} >= 1 after cleanup"
shm_kb="$(df -Pk /dev/shm | awk 'NR==2{print $4}')"
root_kb="$(df -Pk / | awk 'NR==2{print $4}')"
[ "${shm_kb}" -ge "${MIN_SHM_KB}" ] || die "/dev/shm has ${shm_kb} KiB free (< ${MIN_SHM_KB})"
[ "${root_kb}" -ge "${MIN_ROOT_KB}" ] || die "/ has ${root_kb} KiB free (< ${MIN_ROOT_KB})"
# Another CPU consumer would sit on a worker core: refuse (this shell, ps and tee are not counted).
busy="$(ps -eo pid,comm,%cpu --sort=-%cpu | awk -v m="${MAX_CPU}" 'NR>1 && $3+0>m && $2!="ps" && $2!="tee" && $2!="awk" && $2!="bash" {print}')"
[ -z "${busy}" ] || die "processes above ${MAX_CPU}% CPU: ${busy}"
# nothing listening on the bench ports
listening="$(ss -H -lunt 2>/dev/null | awk '{n=split($5,a,":"); print a[n]}' | sort -un)"
for spec in ${PORTS}; do
    lo="${spec%-*}"; hi="${spec#*-}"
    for p in ${listening}; do
        if [ "${p}" -ge "${lo}" ] && [ "${p}" -le "${hi}" ]; then die "port ${p} is already in use (${spec})"; fi
    done
done
# UDP segmentation offload (UDP_SEGMENT) needs Linux 4.18 or newer
kver="$(uname -r | sed 's/[^0-9.].*//')"
[ "$(printf '%s\n%s\n' 4.18 "${kver}" | sort -V | head -n1)" = "4.18" ] || die "kernel ${kver} < 4.18: no UDP GSO"
if command -v conntrack >/dev/null 2>&1; then
    log "conntrack entries now: $(conntrack -C 2>/dev/null || echo '?') (must stay near 0 during a run)"
fi

PREPARED=1
echo "RUNE_BENCH_RUN_DIR=${RUN_DIR}" >"${RUN_DIR}/env"
log "ready. run dir ${RUN_DIR}; register work dirs in ${RUN_DIR}/workdirs; run vm-bench-restore.sh afterwards"
echo "RUNE_BENCH_RUN_DIR=${RUN_DIR}"
