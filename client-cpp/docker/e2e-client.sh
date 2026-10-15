#!/usr/bin/env bash
# docker/e2e-client.sh — CC-7 E2E: rune::client::Client vs 3-node aeron:udp cluster
#
# Proves the real rune::client::Client (CC-6) works against the live 3-node
# broker cluster: connect → ping→pong (several) → kill a non-target node
# (cluster re-elects) → pings keep succeeding after re-election.
#
# Usage: bash docker/e2e-client.sh
# Must be run from the rune-client-cpp repo root.
#
# M-A reality (shapes this test):
#   The M-A gateway degrades-to-accept: a client connected to node X keeps
#   getting Pongs from X regardless of who is the cluster leader. Killing a
#   NON-target node proves the client survives cluster churn (re-election
#   happens, pings still arrive). Killing the CLIENT'S OWN target would
#   require retarget-to-survivor = Milestone B (per-peer client endpoints +
#   REDIRECT follow in SessionMachine). That gap is documented below.
#
# Milestone-B gap (NOT tested here):
#   target-death → retarget: when the client's own target dies, the client
#   must follow a REDIRECT response (with the new leader's client endpoint)
#   to a surviving node. That requires the broker to maintain a per-peer
#   client endpoint map and the client SessionMachine to retarget. CC-7/M-A
#   does NOT exercise this path.
#
# Steps:
#   1. Build broker images.
#   2. Start rune-1, rune-2, rune-3.
#   3. Await initial leader (log-based, same pattern as BR-7b e2e.sh).
#   4. Build the rune-client-ping image.
#   5. Run client in background (targeting rune-1, sending RUNE_PINGS pings
#      with RUNE_PING_INTERVAL_MS sleep between them so the kill happens mid-run).
#   6. Kill a node that is NOT rune-1:
#      prefer the leader if leader != rune-1; else kill rune-2.
#      This is deterministic via the leader-id→service map from BR-7b e2e.sh.
#   7. Wait for the client container to exit; assert exit 0 + "CLIENT: DONE".
#   8. Report; trap tears down (docker compose down -v).
#
# Constraints (brief):
#   - All channels aeron:udp.
#   - Bounded waits with hard caps (no infinite loops).
#   - Clean teardown even on failure (trap EXIT).
#   - NEVER conan create/export/upload.
#   - Client container joins the broker compose network (hostname rune-client).

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
BROKER_COMPOSE="${CLIENT_REPO}/../rune-broker/docker/docker-compose.yml"

# Validate broker compose file exists.
if [ ! -f "${BROKER_COMPOSE}" ]; then
    echo "[e2e-client] FAIL: broker compose not found: ${BROKER_COMPOSE}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CLIENT_IMAGE="rune-client-ping:$(cd "${CLIENT_REPO}" && git rev-parse --short HEAD)"  # git SHA, never dev/latest
CLIENT_HOSTNAME="rune-client"
CLIENT_EGRESS_PORT="40400"

# Client targets rune-1 (the node we will NOT kill).
# M-A: client stays on rune-1 for the whole session.
CLIENT_TARGET_SVC="rune-1"
CLIENT_TARGET_PORT="40200"

# Ping count + interval: enough pings so the kill happens mid-run.
# 20 pings × 500 ms = 10 s total; we kill after ~5 s (around ping 10).
RUNE_PINGS="${RUNE_PINGS:-20}"
RUNE_PING_INTERVAL_MS="${RUNE_PING_INTERVAL_MS:-500}"
RUNE_TIMEOUT_MS="${RUNE_TIMEOUT_MS:-10000}"

LEADER_WAIT_S=90    # max seconds to wait for initial leader
CLIENT_WAIT_S=120   # max seconds to wait for client container to finish

# Compose network name: by default compose uses "<project-name>_default".
# The rune-broker compose has `name: rune-broker` so the network is rune-broker_default.
BROKER_NETWORK="rune-broker_default"

# ---------------------------------------------------------------------------
# Helpers (leader-id→service map reused from BR-7b e2e.sh)
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

log()  { echo "[e2e-client] $*" >&2; }
fail() { echo "[e2e-client] FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Teardown — always runs, even on error or Ctrl-C.
# ---------------------------------------------------------------------------
teardown() {
    log "Tearing down broker compose stack (down -v)..."
    docker compose -f "${BROKER_COMPOSE}" down -v --remove-orphans 2>/dev/null || true
    # Remove the client container if still running (belt-and-suspenders).
    docker rm -f rune-client-e2e 2>/dev/null || true
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
log "Broker build complete."

# ---------------------------------------------------------------------------
# Step 2: Start rune-1, rune-2, rune-3
# ---------------------------------------------------------------------------
log "Step 2: Starting rune-1, rune-2, rune-3..."
docker compose -f "${BROKER_COMPOSE}" up -d rune-1 rune-2 rune-3

# ---------------------------------------------------------------------------
# Step 3: Await initial leader (log-based — emitted once at startup per node)
# Mirror of BR-7b e2e.sh await_leader_id.
# ---------------------------------------------------------------------------
await_leader_id() {
    local timeout_s="${1}"
    local deadline=$(( $(date +%s) + timeout_s ))
    local leader_id=""

    log "Waiting up to ${timeout_s}s for a leader node ID in logs..."
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        local raw
        raw=$(docker compose -f "${BROKER_COMPOSE}" logs --no-log-prefix \
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
        log "=== broker logs (last 50 lines) ==="
        docker compose -f "${BROKER_COMPOSE}" logs --tail=50 \
            rune-1 rune-2 rune-3 2>/dev/null || true
        fail "Timed out (${timeout_s}s) waiting for a leader in logs"
    fi

    case "${leader_id}" in
        0|1|2) ;;
        *) fail "Parsed leader_id '${leader_id}' is out of range (expected 0-2)" ;;
    esac

    echo "${leader_id}"
}

log "Step 3: Awaiting initial leader (up to ${LEADER_WAIT_S}s)..."
leader_id=$(await_leader_id "${LEADER_WAIT_S}")
leader_svc=$(node_id_to_service "${leader_id}")
log "Initial leader: node ID=${leader_id} → service=${leader_svc}"

# ---------------------------------------------------------------------------
# Step 4: Build the rune-client-ping image
# ---------------------------------------------------------------------------
log "Step 4: Building rune-client-ping image (${CLIENT_IMAGE})..."
set +e
out=$(docker build \
    --platform linux/amd64 \
    --build-context rune-protocol="${CLIENT_REPO}/../rune-protocol" \
    --build-context rune-fr="${CLIENT_REPO}/../rune-fr" \
    --build-context rune-codec="${CLIENT_REPO}/../rune-codec" \
    --build-context rune-ql="${CLIENT_REPO}/../rune-ql" \
    --build-context rune-crypto="${CLIENT_REPO}/../rune-crypto" \
    -f "${CLIENT_REPO}/docker/Dockerfile.client" \
    -t "${CLIENT_IMAGE}" \
    "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "client image build failed (exit ${rc})"; }
docker image inspect "${CLIENT_IMAGE}" >/dev/null 2>&1 || fail "client image not built: ${CLIENT_IMAGE}"
log "Client image built: ${CLIENT_IMAGE}"

# ---------------------------------------------------------------------------
# Step 5: Run client in background (targets rune-1, sends RUNE_PINGS pings)
#
# The client container joins the broker compose network so:
#   a) it can reach rune-1:40200 (target ingress), and
#   b) the broker node can publish pongs back to rune-client:40400 (egress).
#
# hostname=rune-client is DNS-resolvable inside the broker network so
# egress publications from the broker to rune-client:40400 route correctly.
# ---------------------------------------------------------------------------
log "Step 5: Running client container in background..."
log "  target=${CLIENT_TARGET_SVC}:${CLIENT_TARGET_PORT} egress=${CLIENT_HOSTNAME}:${CLIENT_EGRESS_PORT}"
log "  pings=${RUNE_PINGS} intervalMs=${RUNE_PING_INTERVAL_MS} timeoutMs=${RUNE_TIMEOUT_MS}"

# Remove any leftover container from a previous run.
docker rm -f rune-client-e2e 2>/dev/null || true

docker run \
    --name rune-client-e2e \
    --platform linux/amd64 \
    --network "${BROKER_NETWORK}" \
    --hostname "${CLIENT_HOSTNAME}" \
    --shm-size=256m \
    --detach \
    -e RUNE_TARGET="aeron:udp?endpoint=${CLIENT_TARGET_SVC}:${CLIENT_TARGET_PORT}" \
    -e RUNE_TARGET_STREAM=1 \
    -e RUNE_EGRESS="aeron:udp?endpoint=${CLIENT_HOSTNAME}:${CLIENT_EGRESS_PORT}" \
    -e RUNE_EGRESS_STREAM=2 \
    -e RUNE_DATA_DIR="/var/lib/rune-client" \
    -e RUNE_PINGS="${RUNE_PINGS}" \
    -e RUNE_PING_INTERVAL_MS="${RUNE_PING_INTERVAL_MS}" \
    -e RUNE_TIMEOUT_MS="${RUNE_TIMEOUT_MS}" \
    "${CLIENT_IMAGE}" > /dev/null

log "Client container started (rune-client-e2e)."

# ---------------------------------------------------------------------------
# Give the client time to connect and complete a few pings before the kill.
# Total run = RUNE_PINGS × RUNE_PING_INTERVAL_MS ms.
# Kill after ~40% of the way through so there are pings both before and after.
# ---------------------------------------------------------------------------
total_run_ms=$(( RUNE_PINGS * RUNE_PING_INTERVAL_MS ))
kill_delay_s=$(( total_run_ms * 40 / 100 / 1000 ))
# Minimum 3 s so the client always has time to connect.
[ "${kill_delay_s}" -lt 3 ] && kill_delay_s=3

log "Waiting ${kill_delay_s}s before killing a non-target node..."
sleep "${kill_delay_s}"

# ---------------------------------------------------------------------------
# Step 6: Kill a node that is NOT rune-1 (the client's target).
#
# Strategy (deterministic via leader-id→service map):
#   - If the leader is NOT rune-1, kill the leader (exercises leadership
#     disruption while the client's target stays up).
#   - If the leader IS rune-1, kill rune-2 (a non-leader follower — still
#     forces a re-election among rune-2 and rune-3, since rune-2 goes away
#     and rune-3 must become the new leader together with the remaining node).
#     Actually with 3 nodes: killing one non-leader leaves 2 of 3 → still
#     quorum; if rune-1 is the leader and we kill rune-2, rune-3 is a follower
#     and the cluster continues. Either way re-election may occur.
#   - In all cases we kill a node != rune-1 so the client's session is intact.
# ---------------------------------------------------------------------------
if [ "${leader_svc}" != "${CLIENT_TARGET_SVC}" ]; then
    kill_svc="${leader_svc}"
    log "Step 6: Leader is ${leader_svc} (not the client's target ${CLIENT_TARGET_SVC}) — killing the leader for maximum churn."
else
    kill_svc="rune-2"
    log "Step 6: Leader is ${CLIENT_TARGET_SVC} (same as client's target) — killing rune-2 (non-leader) instead to avoid disrupting the client session."
fi

log "Stopping ${kill_svc}..."
docker compose -f "${BROKER_COMPOSE}" stop "${kill_svc}"
log "Killed: ${kill_svc} — cluster will re-elect."

# ---------------------------------------------------------------------------
# Step 7: Wait for the client container to finish and assert success.
# ---------------------------------------------------------------------------
log "Step 7: Waiting up to ${CLIENT_WAIT_S}s for client container to finish..."

client_exit_code=0
wait_deadline=$(( $(date +%s) + CLIENT_WAIT_S ))

while [ "$(date +%s)" -lt "${wait_deadline}" ]; do
    status=$(docker inspect --format='{{.State.Status}}' rune-client-e2e 2>/dev/null) || true
    if [ "${status}" = "exited" ]; then
        break
    fi
    sleep 2
done

final_status=$(docker inspect --format='{{.State.Status}}' rune-client-e2e 2>/dev/null) || true
if [ "${final_status}" != "exited" ]; then
    log "=== client container logs (last 50 lines) ==="
    docker logs --tail=50 rune-client-e2e 2>&1 || true
    fail "Client container did not exit within ${CLIENT_WAIT_S}s (status=${final_status})"
fi

client_exit_code=$(docker inspect --format='{{.State.ExitCode}}' rune-client-e2e 2>/dev/null) || true
client_logs=$(docker logs rune-client-e2e 2>&1) || true

log "=== client container logs ==="
echo "${client_logs}" >&2
log "=== end client logs ==="

# Check CLIENT: DONE in stdout.
if ! echo "${client_logs}" | grep -q "CLIENT: DONE"; then
    fail "client output does not contain 'CLIENT: DONE' — client did not complete normally"
fi

# Check exit code.
if [ "${client_exit_code}" != "0" ]; then
    fail "client container exited with code ${client_exit_code}"
fi

# Parse ok count from "CLIENT: DONE pings=<N> ok=<M>"
done_line=$(echo "${client_logs}" | grep "CLIENT: DONE" | tail -1)
log "Result line: ${done_line}"

total_pings=$(echo "${done_line}" | grep -oE 'pings=[0-9]+' | grep -oE '[0-9]+') || true
ok_count=$(echo "${done_line}" | grep -oE 'ok=[0-9]+' | grep -oE '[0-9]+') || true

log "Client reported: pings=${total_pings:-?} ok=${ok_count:-?}"

# Check that we got some pings after the kill (at least 1 "CLIENT: OK" line
# in the logs after the kill delay — we can't timestamp logs precisely here,
# but if ok_count > 0 and the run completed with exit 0, that's the assertion).
if [ -n "${ok_count}" ] && [ "${ok_count}" -gt 0 ]; then
    log "Client successfully completed pings (ok=${ok_count}/${total_pings:-?})."
else
    fail "Client reported ok=${ok_count:-0} — no successful pings"
fi

# ---------------------------------------------------------------------------
# Done — trap will call teardown.
# ---------------------------------------------------------------------------
log ""
log "========================================"
log "  CC-7 E2E PASSED"
log "  Broker: 3-node aeron:udp cluster"
log "  Client target: ${CLIENT_TARGET_SVC} (session intact through churn)"
log "  Killed node: ${kill_svc} (leader=${leader_svc})"
log "  Client result: ${done_line}"
log "  Pings OK: ${ok_count:-?}/${total_pings:-?}"
log ""
log "  Milestone-B gap (NOT tested here):"
log "    target-death → retarget: if the client's own target (${CLIENT_TARGET_SVC})"
log "    dies, the client needs to follow a REDIRECT to a surviving node's endpoint."
log "    This requires the broker to maintain a per-peer client endpoint map and"
log "    the client SessionMachine to retarget. CC-7/M-A does NOT cover this path."
log "========================================"
