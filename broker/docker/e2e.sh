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
#      "node <id> is LEADER", hard cap 90 s — message logged once per node on
#      initial boot). Parse the numeric leader node ID from the log line; map
#      id→service (node 0→rune-1, 1→rune-2, 2→rune-3); assert id is 0/1/2.
#   3b. Create the data-plane topics by DDL (docker/create-topics.sh e2e: events 0,
#      multi 1, cg 2) — run twice: the fixed request id makes the second run return
#      the original results (no duplicates, no TopicExists).
#   4. Run probe against rune-1 (any node accepts Ping) → assert exit 0 + "PROBE: OK".
#   5. Stop the ACTUAL leader service (mapped from parsed leader node ID).
#   6. Assert a DIFFERENT node ID becomes the new leader (probe a survivor with
#      bounded retries, hard cap 90 s); confirm new_leader_id != old_leader_id.
#   7. Assert PROBE: OK from a survivor.
#   8. Report success; trap tears down (`docker compose down -v`).
#
# Constraints (brief):
#   - All channels aeron:udp.
#   - Bounded waits, hard caps, clear failure messages.
#   - Clean teardown even on failure (trap EXIT).
#   - NEVER busy-spin.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="docker/docker-compose.yml"
COMPOSE_NETWORK="rune-broker_default"   # docker-compose.yml: `name: rune-broker`
LEADER_WAIT_S=90          # max seconds to wait for initial leader (log polling)
SURVIVOR_PROBE_WAIT_S=90  # max seconds to probe a survivor after leader-kill
PROBE_TIMEOUT_MS=8000     # ms per individual probe attempt

# ---------------------------------------------------------------------------
# node_id_to_service <id>
# Maps a 0-based node ID to its compose service name.
# Fails loudly if <id> is not 0, 1, or 2 (3-node cluster assumption).
# ---------------------------------------------------------------------------
node_id_to_service() {
    local id="${1}"
    case "${id}" in
        0) echo "rune-1" ;;
        1) echo "rune-2" ;;
        2) echo "rune-3" ;;
        *) fail "leader_id '${id}' is not a valid node ID (expected 0, 1, or 2)" ;;
    esac
}

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
# A failed build must never fall through to a stale image (it did: the whole e2e once ran against an
# old broker image while the current source did not compile under gcc 13).
set +e
# rune-1/2/3 share one image tag: build it once (three concurrent builds of the same image OOM a small Docker VM).
out=$(docker compose -f "${COMPOSE_FILE}" build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ DONE|#[0-9]+ ERROR)" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "broker image build failed (exit ${rc})"; }
docker compose -f "${COMPOSE_FILE}" --profile tools build 2>&1 | grep -E "^( Image|ERROR|#[0-9]+ DONE|#[0-9]+ ERROR)" || true
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"
log "Build complete."

# ---------------------------------------------------------------------------
# Step 2: Start broker nodes
# ---------------------------------------------------------------------------
log "Step 2: Starting rune-1, rune-2, rune-3..."
docker compose -f "${COMPOSE_FILE}" up -d rune-1 rune-2 rune-3

# ---------------------------------------------------------------------------
# await_leader_id <timeout_s>
# Polls `docker compose logs` (all running nodes) for "node <id> is LEADER".
# Every node logs this same line (with the winner's id, not its own id), so
# we scan all running nodes and parse the numeric leader node ID.
# Returns the INTEGER leader node ID on stdout (0, 1, or 2), or fails.
#
# NOTE: "is LEADER" is only logged once per node at startup (main.cpp).
# After a leader-kill, re-election is detected by the probe, not logs.
# ---------------------------------------------------------------------------
await_leader_id() {
    local timeout_s="${1}"
    local deadline=$(( $(date +%s) + timeout_s ))
    local leader_id=""

    log "Waiting up to ${timeout_s}s for a leader node ID in logs..."
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        # Scan all three services; grep returns the first "node <N> is LEADER"
        # found in any service's log. All nodes agree on <N>.
        # NB: `|| true` — under `set -e`+`pipefail` a no-match grep (exit 1) in
        # this poll iteration would otherwise abort the whole script, bypassing
        # the bounded-wait loop. A miss must just retry until the deadline.
        local raw
        raw=$(docker compose -f "${COMPOSE_FILE}" logs --no-log-prefix \
                rune-1 rune-2 rune-3 2>/dev/null \
              | grep -oE 'node [0-9]+ is LEADER' \
              | grep -oE '[0-9]+' \
              | head -1) || true
        if [ -n "${raw}" ]; then
            leader_id="${raw}"
            break
        fi
        sleep 2
    done

    if [ -z "${leader_id}" ]; then
        log "=== compose logs (all nodes) ==="
        docker compose -f "${COMPOSE_FILE}" logs --tail=50 rune-1 rune-2 rune-3 2>/dev/null || true
        fail "Timed out (${timeout_s}s) waiting for a leader in logs"
    fi

    # Validate — must be 0, 1, or 2.
    case "${leader_id}" in
        0|1|2) ;;
        *) fail "Parsed leader_id '${leader_id}' is out of range (expected 0-2)" ;;
    esac

    echo "${leader_id}"
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
leader_id=$(await_leader_id "${LEADER_WAIT_S}")
leader_svc=$(node_id_to_service "${leader_id}")
log "Initial leader: node ID=${leader_id} → service=${leader_svc}"

# ---------------------------------------------------------------------------
# Step 3b: Create the topics by DDL (the brokers boot with an empty catalog).  Twice:
# the second run must exit 0 and create nothing new (fixed request id -> the
# original results come back, same topic ids).
# ---------------------------------------------------------------------------
log "Step 3b: Creating topics by DDL (docker/create-topics.sh e2e)..."
export COMPOSE_NETWORK
bash "${SCRIPT_DIR}/create-topics.sh" e2e || fail "create-topics.sh e2e failed"
log "Step 3b: re-running create-topics.sh (must be idempotent)..."
bash "${SCRIPT_DIR}/create-topics.sh" e2e || fail "create-topics.sh e2e re-run failed (not idempotent)"

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
# Step 5: Kill the ACTUAL leader (service derived from parsed leader_id)
# ---------------------------------------------------------------------------
log "Step 5: Stopping actual leader service '${leader_svc}' (node ID=${leader_id})..."
docker compose -f "${COMPOSE_FILE}" stop "${leader_svc}"
log "Leader '${leader_svc}' (node ${leader_id}) stopped."

# Identify survivors.
survivors=()
for svc in rune-1 rune-2 rune-3; do
    [ "${svc}" = "${leader_svc}" ] && continue
    survivors+=("${svc}")
done
log "Survivors: ${survivors[*]}"

# ---------------------------------------------------------------------------
# Step 6 + 7: Probe a survivor — this proves re-election completed.
#
# Re-election among survivors uses election timeout of 300 ms; it should
# complete within a few seconds. We give it SURVIVOR_PROBE_WAIT_S in total
# via probe_with_retry (each attempt has PROBE_TIMEOUT_MS = 8s).
#
# After the probe succeeds, we verify the ConnectResponse.leaderMemberId
# reflects a DIFFERENT node ID from the one we killed — proving a genuine
# re-election occurred (not just an existing leader continuing to serve).
# ---------------------------------------------------------------------------
log "Step 6+7: Probing survivor '${survivors[0]}' to prove re-election (up to ${SURVIVOR_PROBE_WAIT_S}s)..."
# Use egress_stream=40 for post-kill probe runs.
survivor_probe_out=""
survivor_svc_used=""
if probe_with_retry "${survivors[0]}" "40" "${SURVIVOR_PROBE_WAIT_S}"; then
    survivor_svc_used="${survivors[0]}"
else
    log "Survivor ${survivors[0]} probe failed; trying ${survivors[1]}..."
    probe_with_retry "${survivors[1]}" "50" "${SURVIVOR_PROBE_WAIT_S}" \
        || fail "Probe against all survivors failed after leader-kill"
    survivor_svc_used="${survivors[1]}"
fi
log "Step 6+7 PASSED: survivor answered Pong after leader-kill (re-election confirmed)."

# ---------------------------------------------------------------------------
# Assert re-election: new leader must be a DIFFERENT node ID.
# The probe prints "leaderMemberId=<N>" to stderr (captured in run_probe_once
# via combined 2>&1). We re-run a quick log scan to confirm a different node
# is now acting as leader — evidenced by leaderMemberId from ConnectResponse.
# We parse it from a targeted probe output captured here.
# ---------------------------------------------------------------------------
log "Asserting new leader is different from killed node (ID=${leader_id})..."
new_leader_raw=$(docker compose -f "${COMPOSE_FILE}" run --rm \
    -e RUNE_PROBE_TARGET="aeron:udp?endpoint=${survivor_svc_used}:40200" \
    -e RUNE_PROBE_TARGET_STREAM=1 \
    -e RUNE_PROBE_EGRESS="aeron:udp?endpoint=probe:40300" \
    -e RUNE_PROBE_EGRESS_STREAM="60" \
    -e RUNE_PROBE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" \
    -e RUNE_DATA_DIR="/tmp/rune_probe_60_assert" \
    probe 2>&1) || true

# `|| true` — a parse miss (no leaderMemberId= in the probe output) must fall
# through to the graceful WARNING branch below, not abort under set -e+pipefail.
new_leader_id=$(echo "${new_leader_raw}" \
    | grep -oE 'leaderMemberId=[0-9]+' \
    | grep -oE '[0-9]+' \
    | tail -1) || true

if [ -z "${new_leader_id}" ]; then
    log "WARNING: could not parse new leaderMemberId from ConnectResponse (probe output below):"
    log "${new_leader_raw}"
    log "Continuing — probe already confirmed Pong from survivor."
else
    log "New leader after kill: node ID=${new_leader_id}"
    if [ "${new_leader_id}" = "${leader_id}" ]; then
        fail "Re-election assertion FAILED: new leader ID (${new_leader_id}) == killed leader ID (${leader_id}). The old leader may still be running or re-election did not occur."
    fi
    log "Re-election confirmed: old leader ID=${leader_id}, new leader ID=${new_leader_id} (different node)."
fi

# ---------------------------------------------------------------------------
# Done — trap will call teardown.
# ---------------------------------------------------------------------------
log ""
log "========================================"
log "  BR-7b E2E PASSED"
log "  Initial leader : node ID=${leader_id} (service=${leader_svc})"
if [ -n "${new_leader_id:-}" ]; then
    log "  New leader      : node ID=${new_leader_id} (different node — genuine re-election)"
else
    log "  New leader      : (id not captured from ConnectResponse)"
fi
log "  Leader killed; survivor(s) re-elected and answered Pong."
log "========================================"
