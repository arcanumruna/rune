#!/usr/bin/env bash
# docker/schema-e2e.sh — Task 13 HEADLINE typed round-trip (#11 real-e2e gate).
#
# SINGLE-NODE real-Aeron typed round-trip against a REAL broker (default config):
#   1. Build the single-node broker image (0b protocol + rune-codec wired) and
#      the rune-client-cpp rune_schema_probe image.
#   2. Start ONE broker node (self-elects).
#   3. Wait (bounded) for the node to become LEADER.
#   4. Run the typed probe: connect → defineTopic("characters", validate=on) →
#      openTopic<Characters> (exact-fingerprint gate) → send N typed records →
#      subscribe/consume → decode via Characters::decode → assert round-trip →
#      produce a MALFORMED raw record → assert SchemaValidationFailed reject.
#   5. Assert the probe printed "PROBE: OK" and exited 0.
#   6. Tear down (trap EXIT).
#
# Usage:  bash docker/schema-e2e.sh
# Run from the rune-broker repo root.  Requires the sibling repos:
#   ../protocol, ../codec, ../flight-recorder, ../ql, ../crypto, ../client-cpp
#
# Constraints (engineering philosophy #11/#16):
#   - Real client↔broker over real Aeron (NOT in-process).
#   - Bounded waits, hard caps, clean teardown even on failure.
#   - NEVER a fixed sleep as a correctness gate — poll to a deadline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"   # authentication is ON: cluster key, bootstrap, e2e user (Task 20)
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
# flight-recorder is its own repo (github.com/arcanumruna/flight-recorder), checked out next to the monorepo.
FLIGHT_RECORDER_SRC="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
CLIENT_REPO="${CLIENT_REPO:-${PROJECTS}/client-cpp}"

COMPOSE_FILE="${BROKER_REPO}/docker/docker-compose.single.yml"
PROBE_DOCKERFILE="${CLIENT_REPO}/docker/Dockerfile.schema-probe"

LEADER_WAIT_S=120        # max seconds to wait for the single node to self-elect
PROBE_TIMEOUT_MS=15000   # ms per probe operation (relaxed for amd64 emulation)
PROBE_RECORDS=5          # typed records to round-trip

BROKER_NETWORK="rune-broker-single_default"
PROBE_IMAGE="rune-schema-probe:$(cd "${CLIENT_REPO}" && git rev-parse --short HEAD)"
PROBE_HOSTNAME="rune-schema-probe"
PROBE_EGRESS_PORT="40500"

export RUNE_IMAGE_TAG="$(cd "${BROKER_REPO}" && git rev-parse --short HEAD)"

log()  { echo "[schema-e2e] $*" >&2; }
fail() { echo "[schema-e2e] FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Preflight: sibling repos present.
# ---------------------------------------------------------------------------
# Sibling repos live next to the broker repo (../<repo>).  NB: the compose
# additional_contexts use ../../<repo> because they resolve relative to the
# compose file's dir (docker/), not the repo root.
[ -d "${FLIGHT_RECORDER_SRC}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
for d in protocol codec ql crypto; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done
[ -d "${CLIENT_REPO}" ]      || fail "missing ${CLIENT_REPO}"
[ -f "${PROBE_DOCKERFILE}" ] || fail "missing ${PROBE_DOCKERFILE}"

# ---------------------------------------------------------------------------
# Teardown — always runs.
# ---------------------------------------------------------------------------
teardown() {
    log "Tearing down (compose down -v + probe rm)..."
    docker rm -f "${PROBE_HOSTNAME}" 2>/dev/null || true
    docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true
    rune_auth_cleanup
}
rune_auth_init   # RUNE_SECRETS_DIR (compose mount of the cluster key) + RUNE_AUTH_WORK_DIR (passwords)
trap teardown EXIT

# ---------------------------------------------------------------------------
# Step 1: Build broker image (0b protocol + rune-codec) + probe image.
# ---------------------------------------------------------------------------
log "Step 1a: Building single-node broker image (tag=${RUNE_IMAGE_TAG})..."
# A failed build must never fall through to a stale image.
set +e
out=$(docker compose -f "${COMPOSE_FILE}" build 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Step 1b: Building probe image (${PROBE_IMAGE})..."
set +e
out=$(docker build \
    --platform linux/amd64 \
    -f "${PROBE_DOCKERFILE}" \
    --build-context rune-protocol="${PROJECTS}/protocol" \
    --build-context rune-codec="${PROJECTS}/codec" \
    --build-context flight-recorder="${FLIGHT_RECORDER_SRC}" \
    --build-context rune-ql="${PROJECTS}/ql" \
    --build-context rune-crypto="${PROJECTS}/crypto" \
    -t "${PROBE_IMAGE}" \
    "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "probe image build failed (exit ${rc})"; }
docker image inspect "${PROBE_IMAGE}" >/dev/null 2>&1 || fail "probe image not built: ${PROBE_IMAGE}"
log "Build complete."

# ---------------------------------------------------------------------------
# Step 2: Start the single broker node.
# ---------------------------------------------------------------------------
log "Step 2: Starting single broker node rune-1..."
log "Step 1c: building the rune CLI image + generating the cluster key..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"

docker compose -f "${COMPOSE_FILE}" up -d rune-1

# ---------------------------------------------------------------------------
# Step 3: Await leader (bounded log poll — "node 0 is LEADER" at startup).
# ---------------------------------------------------------------------------
log "Step 3: Awaiting leader (up to ${LEADER_WAIT_S}s)..."
deadline=$(( $(date +%s) + LEADER_WAIT_S ))
leader=""
while [ "$(date +%s)" -lt "${deadline}" ]; do
    raw=$(docker compose -f "${COMPOSE_FILE}" logs --no-log-prefix rune-1 2>/dev/null \
          | grep -oE 'node [0-9]+ is LEADER' | head -1) || true
    if [ -n "${raw}" ]; then leader="${raw}"; break; fi
    sleep 2
done
if [ -z "${leader}" ]; then
    log "=== broker logs (tail) ==="
    docker compose -f "${COMPOSE_FILE}" logs --tail=60 rune-1 2>/dev/null || true
    fail "Timed out (${LEADER_WAIT_S}s) waiting for leader"
fi
log "Leader elected: ${leader}"

log "Step 3a: bootstrapping the cluster + the e2e user..."
rune_auth_prepare "${BROKER_NETWORK}" "rune-1:40200" || fail "bootstrap / e2e user"
read -r -a AUTH_ARGS <<< "$(rune_auth_probe_args)"

# ---------------------------------------------------------------------------
# Step 4: Run the typed probe on the broker network.
# The probe hostname must be DNS-resolvable inside the broker network so the
# node can publish egress (records/responses) back to rune-schema-probe:40500.
# ---------------------------------------------------------------------------
log "Step 4: Running typed probe (records=${PROBE_RECORDS}, timeoutMs=${PROBE_TIMEOUT_MS})..."
docker rm -f "${PROBE_HOSTNAME}" 2>/dev/null || true
set +e
probe_out=$(docker run --rm \
    --name "${PROBE_HOSTNAME}" \
    --hostname "${PROBE_HOSTNAME}" \
    --network "${BROKER_NETWORK}" \
    --platform linux/amd64 \
    --shm-size=256m \
    --security-opt seccomp=unconfined \
    -e RUNE_TARGET="aeron:udp?endpoint=rune-1:40200" \
    -e RUNE_TARGET_STREAM=1 \
    -e RUNE_EGRESS="aeron:udp?endpoint=${PROBE_HOSTNAME}:${PROBE_EGRESS_PORT}" \
    -e RUNE_EGRESS_STREAM=2 \
    -e RUNE_DATA_DIR="/tmp/rune_schema_probe" \
    -e RUNE_BLOB_PATH="/etc/rune/characters.blob" \
    -e RUNE_RECORDS="${PROBE_RECORDS}" \
    -e RUNE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" \
    "${AUTH_ARGS[@]}" \
    "${PROBE_IMAGE}" 2>&1)
probe_rc=$?
set -e

echo "----- probe output -----" >&2
echo "${probe_out}" >&2
echo "------------------------" >&2

# ---------------------------------------------------------------------------
# Step 5: Assert PROBE: OK + exit 0.
# ---------------------------------------------------------------------------
if [ "${probe_rc}" -ne 0 ]; then
    fail "probe exited non-zero (${probe_rc})"
fi
if ! echo "${probe_out}" | grep -q "PROBE: OK"; then
    fail "probe did not print 'PROBE: OK'"
fi

log ""
log "========================================"
log "  TASK 13 TYPED E2E PASSED"
log "  Single node self-elected: ${leader}"
log "  Probe: define→openTopic<Characters>→send×${PROBE_RECORDS}→consume→decode"
log "         + malformed record REJECTED (SchemaValidationFailed)"
log "  PROBE: OK, exit 0."
log "========================================"
