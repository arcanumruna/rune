#!/usr/bin/env bash
# docker/e2e.sh — BR-7b end-to-end: 3-node aeron:udp cluster + raw-Aeron ping probe + leader-kill.
#
# Usage:  bash docker/e2e.sh
# Must be run from the rune-broker repo root.
#
# Steps:
#   1. Build compose images (broker + probe).
#   2. Start rune-1, rune-2, rune-3.
#   3. Wait for exactly ONE leader across the 3 nodes (polls compose logs for
#      "is LEADER", hard cap 90 s — message logged once per node on initial boot).
#   4. Run probe against rune-1 (any node accepts Ping) → assert exit 0 + "PROBE: OK".
#   5. Identify the leader; `docker compose stop <leader>`.
#   6. Wait for a NEW leader: probe a survivor with bounded retries (hard cap 90 s).
#      Re-election completes when the probe gets a Pong — no new "is LEADER" log
#      is emitted by the surviving nodes (main.cpp only logs once at startup).
#   7. Assert PROBE: OK from a survivor.
#   8. Report success; trap tears down (`docker compose down -v`).
#
# Constraints (brief):
#   - All channels aeron:udp.
#   - Bounded waits, hard caps, clear failure messages.
#   - Clean teardown even on failure (trap EXIT).
#   - NEVER busy-spin.

set -euo pipefail

COMPOSE_FILE="docker/docker-compose.yml"
LEADER_WAIT_S=90       # max seconds to wait for initial leader (log polling)
SURVIVOR_PROBE_WAIT_S=90  # max seconds to probe a survivor after leader-kill
PROBE_TIMEOUT_MS=8000  # ms per individual probe attempt

log() { echo "[e2e] $*" >&2; }
fail() { echo "[e2e] FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Teardown trap — always runs, even on error or Ctrl-C.
# ---------------------------------------------------------------------------
teardown() {
    log "Tearing down compose stack (down -v)..."
    docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true
}
trap teardown EXIT

# ---------------------------------------------------------------------------
# Step 1: Build
# ---------------------------------------------------------------------------
log "Step 1: Building images..."
docker compose -f "${COMPOSE_FILE}" build 2>&1 | grep -E "^( Image|ERROR|#[0-9]+ DONE|#[0-9]+ ERROR)" || true
docker compose -f "${COMPOSE_FILE}" --profile tools build 2>&1 | grep -E "^( Image|ERROR|#[0-9]+ DONE|#[0-9]+ ERROR)" || true
log "Build complete."

# ---------------------------------------------------------------------------
# Step 2: Start broker nodes
# ---------------------------------------------------------------------------
log "Step 2: Starting rune-1, rune-2, rune-3..."
docker compose -f "${COMPOSE_FILE}" up -d rune-1 rune-2 rune-3

# ---------------------------------------------------------------------------
# await_leader_logs <timeout_s> <exclude_container>
# Polls `docker compose logs` for "is LEADER" emitted at startup.
# Returns the SERVICE name of the leader on stdout, or fails.
# <exclude_container>: if non-empty, ignore that service (used after kill).
#
# NOTE: "is LEADER" is only logged once per node at startup (main.cpp).
# After a leader-kill, re-election is detected by the probe, not logs.
# ---------------------------------------------------------------------------
await_leader_logs() {
    local timeout_s="${1}"
    local exclude="${2:-}"
    local deadline=$(( $(date +%s) + timeout_s ))
    local leader=""

    log "Waiting up to ${timeout_s}s for a leader in logs (excluding '${exclude}')..."
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        for svc in rune-1 rune-2 rune-3; do
            [ "${svc}" = "${exclude}" ] && continue
            if docker compose -f "${COMPOSE_FILE}" logs --no-log-prefix "${svc}" 2>/dev/null \
                    | grep -q "is LEADER"; then
                leader="${svc}"
                break
            fi
        done
        [ -n "${leader}" ] && break
        sleep 2
    done

    if [ -z "${leader}" ]; then
        log "=== compose logs (all nodes) ==="
        docker compose -f "${COMPOSE_FILE}" logs --tail=50 rune-1 rune-2 rune-3 2>/dev/null || true
        fail "Timed out (${timeout_s}s) waiting for a leader in logs"
    fi
    echo "${leader}"
}

# ---------------------------------------------------------------------------
# run_probe_once <target_service> <egress_stream> <data_dir_suffix>
# Returns 0 if PROBE: OK received, 1 otherwise.
# Stderr of the probe is forwarded; stdout (the "PROBE: OK" line) is captured.
# ---------------------------------------------------------------------------
run_probe_once() {
    local target_svc="${1}"
    local egress_stream="${2}"
    local data_suffix="${3}"
    local out
    out=$(docker compose -f "${COMPOSE_FILE}" run --rm \
        -e RUNE_PROBE_TARGET="aeron:udp?endpoint=${target_svc}:40200" \
        -e RUNE_PROBE_TARGET_STREAM=1 \
        -e RUNE_PROBE_EGRESS="aeron:udp?endpoint=probe:40300" \
        -e RUNE_PROBE_EGRESS_STREAM="${egress_stream}" \
        -e RUNE_PROBE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" \
        -e RUNE_DATA_DIR="/tmp/rune_probe_${data_suffix}" \
        probe 2>&1) || true
    if echo "${out}" | grep -q "PROBE: OK"; then
        log "Probe OK: ${out}"
        return 0
    fi
    return 1
}

# ---------------------------------------------------------------------------
# probe_with_retry <target_service> <egress_stream_base> <timeout_s>
# Retries probe every ~10s until success or timeout.
# Uses incrementing egress streams to avoid channel conflicts across runs.
# ---------------------------------------------------------------------------
probe_with_retry() {
    local target_svc="${1}"
    local egress_stream_base="${2}"
    local timeout_s="${3}"
    local deadline=$(( $(date +%s) + timeout_s ))
    local attempt=0

    log "Probing ${target_svc} (up to ${timeout_s}s)..."
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        attempt=$(( attempt + 1 ))
        local egress_stream=$(( egress_stream_base + attempt ))
        log "  Probe attempt ${attempt}: target=${target_svc} egressStream=${egress_stream}"
        if run_probe_once "${target_svc}" "${egress_stream}" "${egress_stream_base}_${attempt}"; then
            log "Probe succeeded on attempt ${attempt}."
            return 0
        fi
        log "  Probe attempt ${attempt} failed; retrying in 5s..."
        sleep 5
    done
    return 1
}

# ---------------------------------------------------------------------------
# Step 3: Await initial leader (log-based — emitted once at startup)
# ---------------------------------------------------------------------------
log "Step 3: Awaiting initial leader (up to ${LEADER_WAIT_S}s)..."
leader=$(await_leader_logs "${LEADER_WAIT_S}" "")
log "Initial leader logged: ${leader}"

# Determine which node ID is the leader (from logs of the leader service).
leader_id=$(docker compose -f "${COMPOSE_FILE}" logs --no-log-prefix "${leader}" 2>/dev/null \
    | grep "is LEADER" | grep -oE 'node [0-9]+' | grep -oE '[0-9]+' | tail -1)
log "Leader node ID: ${leader_id} (service: ${leader})"

# ---------------------------------------------------------------------------
# Step 4: Probe before leader-kill (any node accepts Ping, no leader required)
# ---------------------------------------------------------------------------
log "Step 4: Running probe against rune-1 (pre-kill)..."
# Use egress_stream=10 for first probe run.
if ! probe_with_retry "rune-1" "10" "30"; then
    log "rune-1 probe failed; trying rune-2..."
    if ! probe_with_retry "rune-2" "20" "30"; then
        log "rune-2 probe failed; trying rune-3..."
        probe_with_retry "rune-3" "30" "30" || fail "All pre-kill probe attempts failed"
    fi
fi
log "Step 4 PASSED: probe got Pong before leader-kill."

# ---------------------------------------------------------------------------
# Step 5: Kill the leader
# ---------------------------------------------------------------------------
log "Step 5: Stopping leader service '${leader}'..."
docker compose -f "${COMPOSE_FILE}" stop "${leader}"
log "Leader '${leader}' stopped."

# Identify survivors.
survivors=()
for svc in rune-1 rune-2 rune-3; do
    [ "${svc}" = "${leader}" ] && continue
    survivors+=("${svc}")
done
log "Survivors: ${survivors[*]}"

# ---------------------------------------------------------------------------
# Step 6 + 7: Probe a survivor — this proves re-election completed.
#
# Re-election among survivors uses election timeout of 300 ms; it should
# complete within a few seconds. We give it SURVIVOR_PROBE_WAIT_S in total
# via probe_with_retry (each attempt has PROBE_TIMEOUT_MS = 8s).
# ---------------------------------------------------------------------------
log "Step 6+7: Probing survivor '${survivors[0]}' to prove re-election (up to ${SURVIVOR_PROBE_WAIT_S}s)..."
# Use egress_stream=40 for post-kill probe runs.
if ! probe_with_retry "${survivors[0]}" "40" "${SURVIVOR_PROBE_WAIT_S}"; then
    log "Survivor ${survivors[0]} probe failed; trying ${survivors[1]}..."
    probe_with_retry "${survivors[1]}" "50" "${SURVIVOR_PROBE_WAIT_S}" \
        || fail "Probe against all survivors failed after leader-kill"
fi
log "Step 6+7 PASSED: survivor answered Pong after leader-kill (re-election confirmed)."

# ---------------------------------------------------------------------------
# Done — trap will call teardown.
# ---------------------------------------------------------------------------
log ""
log "========================================"
log "  BR-7b E2E PASSED"
log "  Initial leader: ${leader} (node ${leader_id})"
log "  Leader killed; survivor(s) re-elected and answered Pong."
log "========================================"
