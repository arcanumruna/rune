#!/usr/bin/env bash
# vm-bench-restore.sh - undo vm-bench-prepare.sh: sysctls, iptables, k3s, and the run's work dirs.
#
#   flock -w 7200 /root/vm.lock env RUNE_VM_LOCK_HELD=1 broker/bench/vm-bench-restore.sh [run-dir]
#
# Idempotent: every step leaves a marker in the run dir and is skipped when done, so it is safe to run twice and
# after a partial prepare (steps whose saved state does not exist are skipped).  The run dir comes from the argument,
# RUNE_BENCH_RUN_DIR, or <RUNE_BENCH_STATE_ROOT>/latest.  Reverses steps 6 -> 3 of the preparation.
#   RUNE_VM_LOCK_HELD=1     required (set by the locking wrapper); RUNE_VM_LOCK_FILE as in prepare
#   RUNE_BENCH_K3S_WAIT_S   seconds to wait for the node to be Ready (default 300)
set -euo pipefail

LOCK_FILE="${RUNE_VM_LOCK_FILE:-/root/vm.lock}"
STATE_ROOT="${RUNE_BENCH_STATE_ROOT:-/root/rune-bench-runs}"
WAIT_S="${RUNE_BENCH_K3S_WAIT_S:-300}"

log() { echo "[vm-bench-restore] $*" >&2; }
die() { log "FAIL: $*"; exit 1; }

[ "${RUNE_VM_LOCK_HELD:-0}" = "1" ] || die "run under the host lock: flock -w 7200 ${LOCK_FILE} env RUNE_VM_LOCK_HELD=1 $0"
[ "$(id -u)" -eq 0 ] || die "must run as root"

RUN_DIR="${1:-${RUNE_BENCH_RUN_DIR:-${STATE_ROOT}/latest}}"
[ -d "${RUN_DIR}" ] || die "no run dir at ${RUN_DIR} (nothing was prepared)"
RUN_DIR="$(cd "${RUN_DIR}" && pwd -P)"
exec > >(tee -a "${RUN_DIR}/restore.log") 2>&1
log "run dir ${RUN_DIR}"

# ---- 6. sysctls ----------------------------------------------------------------------------------------------
if [ -f "${RUN_DIR}/sysctl.save" ] && [ ! -f "${RUN_DIR}/sysctl.restored" ]; then
    log "restoring sysctls"
    while IFS= read -r kv; do
        [ -n "${kv}" ] && sysctl -q -w "${kv}"
    done <"${RUN_DIR}/sysctl.save"
    touch "${RUN_DIR}/sysctl.restored"
fi

# ---- 5/4. iptables (the saved rules replace the NOTRACK/ACCEPT fast path too) -------------------------------------
if [ -f "${RUN_DIR}/iptables.flushed" ] && [ ! -f "${RUN_DIR}/iptables.restored" ]; then
    log "restoring iptables"
    [ -f "${RUN_DIR}/iptables.save" ] && iptables-restore <"${RUN_DIR}/iptables.save"
    [ -f "${RUN_DIR}/ip6tables.save" ] && ip6tables-restore <"${RUN_DIR}/ip6tables.save"
    touch "${RUN_DIR}/iptables.restored"
fi

# ---- 3. k3s ---------------------------------------------------------------------------------------------------
if [ -f "${RUN_DIR}/k3s.was" ] && [ ! -f "${RUN_DIR}/k3s.restored" ]; then
    if [ "$(cat "${RUN_DIR}/k3s.was")" = "active" ]; then
        if [ "$(systemctl is-active k3s 2>/dev/null || true)" != "active" ]; then
            log "starting k3s"
            systemctl start k3s
        fi
        if command -v kubectl >/dev/null 2>&1; then KUBECTL=(kubectl); else KUBECTL=(k3s kubectl); fi
        log "waiting up to ${WAIT_S}s for the node to be Ready"
        deadline=$(( $(date +%s) + WAIT_S ))
        until "${KUBECTL[@]}" get nodes 2>/dev/null | awk 'NR>1 && $2=="Ready" {ok=1} END{exit !ok}'; do
            [ "$(date +%s)" -lt "${deadline}" ] || die "node not Ready after ${WAIT_S}s: check systemctl status k3s (never reboot)"
            sleep 5
        done
    fi
    touch "${RUN_DIR}/k3s.restored"
fi

# ---- the run's work dirs -------------------------------------------------------------------------------------------
if [ -f "${RUN_DIR}/workdirs" ]; then
    while IFS= read -r d; do
        [ -n "${d}" ] && [ -e "${d}" ] || continue
        case "${d}" in /|/root|/dev|/dev/shm|/tmp) log "refusing to remove ${d}"; continue ;; esac
        log "removing work dir ${d}"
        rm -rf -- "${d}"
    done <"${RUN_DIR}/workdirs"
fi

log "restored (state and logs kept in ${RUN_DIR})"
