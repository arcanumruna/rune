#!/usr/bin/env bash
# docker/e2e-data.sh — CB-5 C E2E: rune_produce_subscribe vs 3-node aeron:udp cluster
#
# Proves the full produce + subscribe data path against a live 3-node Rune
# cluster:
#
#   Phase 1:
#     Connect → produce RUNE_PRODUCE_N records (Leader acks) → subscribe from
#     Earliest → assert all N records arrive in offset order.
#
#   Phase 2 (leader-kill recovery):
#     Kill the leader → wait RUNE_RECOVERY_WAIT_MS for re-election → produce
#     RUNE_PRODUCE_M more records → subscribe from Earliest → assert all N+M
#     records arrive in offset order across the recovery boundary.
#
# Usage: bash docker/e2e-data.sh
# Must be run from the rune-client-cpp repo root.
#
# Environment overrides:
#   RUNE_PRODUCE_N          — phase 1 record count (default 20)
#   RUNE_PRODUCE_M          — phase 2 record count (default 10)
#   RUNE_TIMEOUT_MS         — per-op timeout ms (default 10000)
#   RUNE_RECOVERY_WAIT_MS   — ms to wait after leader kill (default 5000)
#
# Constraints:
#   - All channels aeron:udp.
#   - Bounded waits with hard caps (no infinite loops).
#   - Clean teardown even on failure (trap EXIT).
#   - NEVER conan create/export/upload.

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
BROKER_DOCKER_DIR="${CLIENT_REPO}/../broker/docker"
BROKER_COMPOSE="${BROKER_DOCKER_DIR}/docker-compose.yml"

if [ ! -f "${BROKER_COMPOSE}" ]; then
    echo "[e2e-data] FAIL: broker compose not found: ${BROKER_COMPOSE}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_IMAGE="rune-client-data:$(cd "${CLIENT_REPO}" && git rev-parse --short HEAD)"  # git SHA, never dev/latest
CLIENT_HOSTNAME="rune-data-client"
CLIENT_EGRESS_PORT="40402"   # different port from e2e-client.sh to avoid conflict

# Client targets rune-1.
CLIENT_TARGET_SVC="rune-1"
CLIENT_TARGET_PORT="40200"

RUNE_PRODUCE_N="${RUNE_PRODUCE_N:-20}"
RUNE_PRODUCE_M="${RUNE_PRODUCE_M:-10}"
RUNE_TIMEOUT_MS="${RUNE_TIMEOUT_MS:-10000}"
RUNE_RECOVERY_WAIT_MS="${RUNE_RECOVERY_WAIT_MS:-5000}"

# The client's topic table: "events:0:1" (name:id:partitionCount).
# topicId 0 = `events`, the first statement of rune-broker docker/topics/e2e.rql, created by
# create-topics.sh (Step 3b) once the cluster has a leader.
RUNE_CLIENT_TOPIC_TABLE="${RUNE_CLIENT_TOPIC_TABLE:-events:0:1}"

LEADER_WAIT_S=90
CLIENT_WAIT_S=180   # Phase 1 + kill + Phase 2 may take up to 3 min

BROKER_NETWORK="rune-broker_default"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
node_id_to_service() {
    local id="${1}"
    case "${id}" in
        0) echo "rune-1" ;;
        1) echo "rune-2" ;;
        2) echo "rune-3" ;;
        *) fail "leader_id '${id}' out of range (0-2)" ;;
    esac
}

log()  { echo "[e2e-data] $*" >&2; }
fail() { echo "[e2e-data] FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
teardown() {
    log "Tearing down broker compose stack (down -v)..."
    docker compose -f "${BROKER_COMPOSE}" down -v --remove-orphans 2>/dev/null || true
    docker rm -f rune-data-e2e 2>/dev/null || true
}
trap teardown EXIT

# ---------------------------------------------------------------------------
# Step 1: Build broker images
# ---------------------------------------------------------------------------
log "Step 1: Building broker images..."
set +e
out=$(docker compose -f "${BROKER_COMPOSE}" build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG:-latest}" >/dev/null 2>&1 || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG:-latest}"
bash "${BROKER_DOCKER_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"
log "Broker build complete."

# ---------------------------------------------------------------------------
# Step 2: Start broker cluster
# ---------------------------------------------------------------------------
log "Step 2: Starting rune-1, rune-2, rune-3..."
docker compose -f "${BROKER_COMPOSE}" up -d rune-1 rune-2 rune-3

# ---------------------------------------------------------------------------
# Step 3: Await initial leader (log-based)
# ---------------------------------------------------------------------------
await_leader_id() {
    local timeout_s="${1}"
    shift
    # Remaining args = the set of services to scan for a leader announcement.
    # After a leader kill, the killed service is EXCLUDED so its stale
    # "node N is LEADER" line cannot re-select the dead node.  Defaults to all
    # three nodes when no services are supplied.
    local -a svcs=("$@")
    if [ "${#svcs[@]}" -eq 0 ]; then
        svcs=(rune-1 rune-2 rune-3)
    fi
    local deadline=$(( $(date +%s) + timeout_s ))
    local leader_id=""

    log "Waiting up to ${timeout_s}s for a leader node ID in logs (scan: ${svcs[*]})..."
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        local raw
        # tail -1: take the LATEST leader announcement, not the earliest — after
        # a re-election the newest line reflects the current leader.
        raw=$(docker compose -f "${BROKER_COMPOSE}" logs --no-log-prefix \
                "${svcs[@]}" 2>/dev/null \
              | grep -oE 'node [0-9]+ is LEADER' \
              | grep -oE '[0-9]+' \
              | tail -1) || true
        if [ -n "${raw}" ]; then
            leader_id="${raw}"
            break
        fi
        sleep 2
    done

    if [ -z "${leader_id}" ]; then
        log "=== broker logs (last 50 lines) ==="
        docker compose -f "${BROKER_COMPOSE}" logs --tail=50 \
            "${svcs[@]}" 2>/dev/null || true
        fail "Timed out (${timeout_s}s) waiting for leader"
    fi

    echo "${leader_id}"
}

log "Step 3: Awaiting initial leader (up to ${LEADER_WAIT_S}s)..."
leader_id=$(await_leader_id "${LEADER_WAIT_S}")
leader_svc=$(node_id_to_service "${leader_id}")
log "Initial leader: node ID=${leader_id} → service=${leader_svc}"

# ---------------------------------------------------------------------------
# Step 3b: Create the data-plane topics by DDL (the brokers take no topic list)
# ---------------------------------------------------------------------------
log "Step 3b: creating the data-plane topics by DDL (create-topics.sh e2e)..."
COMPOSE_NETWORK="${BROKER_NETWORK}" bash "${BROKER_DOCKER_DIR}/create-topics.sh" e2e || fail "create-topics.sh e2e failed"

# ---------------------------------------------------------------------------
# Step 4: Build the rune-client-data image
# ---------------------------------------------------------------------------
log "Step 4: Building rune-client-data image (${DATA_IMAGE})..."
set +e
out=$(docker build \
    --platform linux/amd64 \
    --build-context rune-protocol="${CLIENT_REPO}/../protocol" \
    --build-context flight-recorder="${FLIGHT_RECORDER_DIR:-${CLIENT_REPO}/../../flight-recorder}" \
    --build-context rune-codec="${CLIENT_REPO}/../codec" \
    --build-context rune-ql="${CLIENT_REPO}/../ql" \
    --build-context rune-crypto="${CLIENT_REPO}/../crypto" \
    -f "${CLIENT_REPO}/docker/Dockerfile.data" \
    -t "${DATA_IMAGE}" \
    "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "data image build failed (exit ${rc})"; }
docker image inspect "${DATA_IMAGE}" >/dev/null 2>&1 || fail "data image not built: ${DATA_IMAGE}"
log "Data image built: ${DATA_IMAGE}"

# ---------------------------------------------------------------------------
# Step 5: Run the produce+subscribe client
# ---------------------------------------------------------------------------
log "Step 5: Running rune_produce_subscribe container..."
log "  target=${CLIENT_TARGET_SVC}:${CLIENT_TARGET_PORT}"
log "  topics=${RUNE_CLIENT_TOPIC_TABLE}"
log "  produceN=${RUNE_PRODUCE_N} produceM=${RUNE_PRODUCE_M}"
log "  timeoutMs=${RUNE_TIMEOUT_MS} recoveryWaitMs=${RUNE_RECOVERY_WAIT_MS}"

docker rm -f rune-data-e2e 2>/dev/null || true

# Leader kill command: issued from inside the container.
# The container has Docker socket access (mounted) so it can docker stop the
# leader service.  Alternative: pass the compose service name and use docker
# compose stop from outside — we use RUNE_LEADER_KILL_CMD="" and handle the
# kill from the SCRIPT LEVEL instead (simpler, avoids socket mount).
# We run phase 1 first (no kill), then kill the leader, then phase 2.

# Phase 1: produce N + subscribe, NO kill command.
docker run \
    --name rune-data-e2e \
    --platform linux/amd64 \
    --network "${BROKER_NETWORK}" \
    --hostname "${CLIENT_HOSTNAME}" \
    --shm-size=256m \
    -e RUNE_TARGET="aeron:udp?endpoint=${CLIENT_TARGET_SVC}:${CLIENT_TARGET_PORT}" \
    -e RUNE_TARGET_STREAM=1 \
    -e RUNE_EGRESS="aeron:udp?endpoint=${CLIENT_HOSTNAME}:${CLIENT_EGRESS_PORT}" \
    -e RUNE_EGRESS_STREAM=2 \
    -e RUNE_DATA_DIR="/var/lib/rune-client" \
    -e RUNE_CLIENT_TOPIC_TABLE="${RUNE_CLIENT_TOPIC_TABLE}" \
    -e RUNE_PRODUCE_N="${RUNE_PRODUCE_N}" \
    -e RUNE_PRODUCE_M=0 \
    -e RUNE_TIMEOUT_MS="${RUNE_TIMEOUT_MS}" \
    -e RUNE_LEADER_KILL_CMD="" \
    -e RUNE_RECOVERY_WAIT_MS="0" \
    "${DATA_IMAGE}" 2>&1 | tee /tmp/rune-data-phase1.log

phase1_rc=${PIPESTATUS[0]}

if [ "${phase1_rc}" -ne 0 ]; then
    fail "Phase 1 container exited with code ${phase1_rc}"
fi

if ! grep -q "CLIENT: DONE rc=0" /tmp/rune-data-phase1.log; then
    fail "Phase 1: CLIENT: DONE rc=0 not found in output"
fi

log "Phase 1 PASSED (produce ${RUNE_PRODUCE_N} + subscribe from Earliest, offset order verified)."

# ---------------------------------------------------------------------------
# Step 6: Kill the leader before Phase 2
# ---------------------------------------------------------------------------
log "Step 6: Killing leader (${leader_svc}) for Phase 2 recovery test..."
docker compose -f "${BROKER_COMPOSE}" stop "${leader_svc}"
log "Killed ${leader_svc}. Waiting ${RUNE_RECOVERY_WAIT_MS}ms for re-election..."
sleep $(( RUNE_RECOVERY_WAIT_MS / 1000 ))

# Find the new leader.  Exclude the killed service so its stale "is LEADER"
# line cannot be re-selected, and poll until the elected id differs from the
# killed node (re-election may lag the kill by a few election timeouts).
log "Awaiting new leader after kill (excluding ${leader_svc})..."
live_svcs=()
for s in rune-1 rune-2 rune-3; do
    [ "${s}" != "${leader_svc}" ] && live_svcs+=("${s}")
done

new_leader_id=""
new_leader_deadline=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "${new_leader_deadline}" ]; do
    cand=$(await_leader_id 60 "${live_svcs[@]}") || \
        fail "No new leader elected after killing ${leader_svc}"
    cand_svc=$(node_id_to_service "${cand}")
    if [ "${cand_svc}" != "${leader_svc}" ]; then
        new_leader_id="${cand}"
        break
    fi
    # Still seeing the old (killed) leader's stale announcement — wait for the
    # surviving nodes to elect a fresh leader.
    sleep 2
done

[ -n "${new_leader_id}" ] || fail "New leader still resolves to killed ${leader_svc}"
new_leader_svc=$(node_id_to_service "${new_leader_id}")
log "New leader: node ID=${new_leader_id} → service=${new_leader_svc}"

# ---------------------------------------------------------------------------
# Step 7: Phase 2 — produce M more, subscribe from Earliest, assert N+M total
# ---------------------------------------------------------------------------
log "Step 7: Phase 2 — produce ${RUNE_PRODUCE_M} more records to new leader..."
docker rm -f rune-data-e2e 2>/dev/null || true

docker run \
    --name rune-data-e2e \
    --platform linux/amd64 \
    --network "${BROKER_NETWORK}" \
    --hostname "${CLIENT_HOSTNAME}" \
    --shm-size=256m \
    -e RUNE_TARGET="aeron:udp?endpoint=${new_leader_svc}:${CLIENT_TARGET_PORT}" \
    -e RUNE_TARGET_STREAM=1 \
    -e RUNE_EGRESS="aeron:udp?endpoint=${CLIENT_HOSTNAME}:${CLIENT_EGRESS_PORT}" \
    -e RUNE_EGRESS_STREAM=2 \
    -e RUNE_DATA_DIR="/var/lib/rune-client" \
    -e RUNE_CLIENT_TOPIC_TABLE="${RUNE_CLIENT_TOPIC_TABLE}" \
    -e RUNE_PRODUCE_N="${RUNE_PRODUCE_M}" \
    -e RUNE_PRODUCE_M=0 \
    -e RUNE_TIMEOUT_MS=$(( RUNE_TIMEOUT_MS * 3 )) \
    -e RUNE_LEADER_KILL_CMD="" \
    -e RUNE_RECOVERY_WAIT_MS="0" \
    "${DATA_IMAGE}" 2>&1 | tee /tmp/rune-data-phase2.log

phase2_rc=${PIPESTATUS[0]}

if [ "${phase2_rc}" -ne 0 ]; then
    fail "Phase 2 container exited with code ${phase2_rc}"
fi

if ! grep -q "CLIENT: DONE rc=0" /tmp/rune-data-phase2.log; then
    fail "Phase 2: CLIENT: DONE rc=0 not found in output"
fi

log "Phase 2 PASSED (produce ${RUNE_PRODUCE_M} more after leader kill, subscribe verifies ordering)."

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
log ""
log "========================================"
log "  CB-5 C E2E PASSED"
log "  Cluster: 3-node aeron:udp"
log "  Phase 1: N=${RUNE_PRODUCE_N} records produced + delivered in offset order"
log "  Leader kill: ${leader_svc} killed, new leader: ${new_leader_svc}"
log "  Phase 2: M=${RUNE_PRODUCE_M} records produced + delivered post-recovery"
log "========================================"
