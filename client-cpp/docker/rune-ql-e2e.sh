#!/usr/bin/env bash
# docker/rune-ql-e2e.sh — Task 10 HEADLINE 0c CLI-driven typed round-trip (#11 gate).
#
# SINGLE-NODE real-Aeron typed round-trip through the `rune` CLI against a REAL
# broker (default config), exercising a `string(N)` FixedStr field end to end:
#   1. Build the single-node broker image (broker CODE unchanged; compiled against
#      rune-codec so its validator/readField handle FixedStr) + the client
#      rune-ql image (the `rune` CLI + client libs + retained toolchain).
#   2. Start ONE broker node (self-elects).
#   3. Wait (bounded) for the node to become LEADER.
#   4. Run the client container which, INSIDE itself, against the live broker:
#        a. rune schema apply    tests/e2e/characters.rql --broker rune-1:40200 --validate
#        b. rune schema generate characters --out <gen> --broker rune-1:40200
#        c. cmake --build ... --target rune_ql_probe   (compile probe vs the header)
#        d. ./rune_ql_probe   → openTopic<Characters> → produce/consume typed →
#           decode → assert id/userId/code[8]/name round-trip → assert a malformed
#           record is rejected (SchemaValidationFailed).
#   5. Assert the probe printed "PROBE: OK" and exited 0.
#   6. Tear down (trap EXIT).
#
# Usage:  bash docker/rune-ql-e2e.sh
# Run from the rune-client-cpp-0c repo root.  Requires the sibling repos:
#   ../broker  ../protocol  ../codec  ../ql  ../flight-recorder
#
# Constraints (engineering philosophy #11/#16):
#   - Real client↔broker over real Aeron (NOT in-process); the DEFAULT path.
#   - Bounded waits, hard caps, clean teardown even on failure.
#   - NEVER a fixed sleep as a correctness gate — poll to a deadline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${CLIENT_REPO}/.." && pwd)"

COMPOSE_FILE="${CLIENT_REPO}/docker/docker-compose.rune-ql.yml"
CLIENT_DOCKERFILE="${CLIENT_REPO}/docker/Dockerfile.rune-ql-probe"

LEADER_WAIT_S=180        # max seconds to wait for the single node to self-elect
PROBE_TIMEOUT_MS=20000   # ms per probe operation (relaxed for amd64 emulation)
PROBE_RECORDS=5          # typed records to round-trip

BROKER_NETWORK="rune-ql-e2e_default"
CLIENT_IMAGE="rune-ql-probe:$(cd "${CLIENT_REPO}" && git rev-parse --short HEAD)"
CLIENT_HOSTNAME="rune-ql-probe"
CLIENT_EGRESS_PORT="40500"

export RUNE_IMAGE_TAG="$(cd "${PROJECTS}/broker" && git rev-parse --short HEAD)-0c"

log()  { echo "[rune-ql-e2e] $*" >&2; }
fail() { echo "[rune-ql-e2e] FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Preflight: sibling repos present.
# ---------------------------------------------------------------------------
[ -d "${PROJECTS}/broker" ]    || fail "missing ../broker"
[ -d "${PROJECTS}/protocol" ]  || fail "missing ../protocol"
[ -d "${PROJECTS}/codec" ]     || fail "missing ../codec"
[ -d "${PROJECTS}/crypto" ]    || fail "missing ../crypto"
[ -d "${PROJECTS}/ql" ]        || fail "missing ../ql"
[ -d "${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}" ]        || fail "missing ../flight-recorder"
[ -f "${CLIENT_DOCKERFILE}" ]       || fail "missing ${CLIENT_DOCKERFILE}"
[ -f "${CLIENT_REPO}/tests/e2e/characters.rql" ] || fail "missing tests/e2e/characters.rql"

# ---------------------------------------------------------------------------
# Teardown — always runs.
# ---------------------------------------------------------------------------
teardown() {
    log "Tearing down (compose down -v + client rm)..."
    docker rm -f "${CLIENT_HOSTNAME}" 2>/dev/null || true
    docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true
}
trap teardown EXIT

# ---------------------------------------------------------------------------
# Step 1: Build broker image (rune-codec) + client rune-ql image.
# ---------------------------------------------------------------------------
log "Step 1a: Building single-node broker image against rune-codec (tag=${RUNE_IMAGE_TAG})..."
set +e
out=$(docker compose -f "${COMPOSE_FILE}" build 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-0c-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built: rune-broker-0c-node:${RUNE_IMAGE_TAG}"

log "Step 1b: Building client rune-ql image (${CLIENT_IMAGE})..."
set +e
out=$(docker build \
    --platform linux/amd64 \
    -f "${CLIENT_DOCKERFILE}" \
    --build-context rune-protocol="${PROJECTS}/protocol" \
    --build-context rune-codec="${PROJECTS}/codec" \
    --build-context rune-ql="${PROJECTS}/ql" \
    --build-context flight-recorder="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}" \
    --build-context rune-crypto="${PROJECTS}/crypto" \
    -t "${CLIENT_IMAGE}" \
    "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "client image build failed (exit ${rc})"; }
docker image inspect "${CLIENT_IMAGE}" >/dev/null 2>&1 || fail "client image not built: ${CLIENT_IMAGE}"
log "Build complete."

# ---------------------------------------------------------------------------
# Step 2: Start the single broker node.
# ---------------------------------------------------------------------------
log "Step 2: Starting single broker node rune-1..."
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

# ---------------------------------------------------------------------------
# Step 4: Run the CLI-driven client container on the broker network.
# The client hostname must be DNS-resolvable inside the broker network so the
# node can publish egress (records/responses) back to rune-ql-probe:40500.
# ---------------------------------------------------------------------------
BROKER_TARGET="aeron:udp?endpoint=rune-1:40200"
EGRESS_CH="aeron:udp?endpoint=${CLIENT_HOSTNAME}:${CLIENT_EGRESS_PORT}"

# The in-container e2e: apply -> generate -> compile probe -> run probe.
# --cli-data-dir / RUNE_DATA_DIR give the CLI + probe their own Aeron driver dir.
# The CLI uses --broker <channel>; hidden --cli-data-dir routes its embedded
# driver.  RUNE_TARGET/RUNE_EGRESS/RUNE_DATA_DIR drive the compiled probe.
read -r -d '' IN_CONTAINER <<'EOS' || true
set -euo pipefail
GEN_DIR="${RUNE_GEN_DIR:-/build/build/e2e-gen}"
mkdir -p "${GEN_DIR}"

echo "[in] rune schema apply (validate=on) ..." >&2
rune schema apply tests/e2e/characters.rql \
    --broker "${RUNE_TARGET}" \
    --validate \
    --cli-data-dir /tmp/rune_cli_apply

echo "[in] rune schema generate characters -> ${GEN_DIR} ..." >&2
rune schema generate characters \
    --out "${GEN_DIR}" \
    --broker "${RUNE_TARGET}" \
    --cli-data-dir /tmp/rune_cli_gen
test -f "${GEN_DIR}/characters.gen.h" || { echo "[in] FAIL: characters.gen.h not generated" >&2; exit 30; }
echo "[in] generated header:" >&2
grep -E 'kFingerprint|std::array<char' "${GEN_DIR}/characters.gen.h" >&2 || true

echo "[in] compiling rune_ql_probe against generated header ..." >&2
cmake --build /build/build --target rune_ql_probe

echo "[in] running rune_ql_probe ..." >&2
exec /build/build/rune_ql_probe
EOS

log "Step 4: Running CLI-driven client (records=${PROBE_RECORDS}, timeoutMs=${PROBE_TIMEOUT_MS})..."
docker rm -f "${CLIENT_HOSTNAME}" 2>/dev/null || true
set +e
client_out=$(docker run --rm \
    --name "${CLIENT_HOSTNAME}" \
    --hostname "${CLIENT_HOSTNAME}" \
    --network "${BROKER_NETWORK}" \
    --platform linux/amd64 \
    --shm-size=256m \
    --security-opt seccomp=unconfined \
    -e RUNE_TARGET="${BROKER_TARGET}" \
    -e RUNE_TARGET_STREAM=1 \
    -e RUNE_EGRESS="${EGRESS_CH}" \
    -e RUNE_EGRESS_STREAM=2 \
    -e RUNE_DATA_DIR="/tmp/rune_ql_probe" \
    -e RUNE_RECORDS="${PROBE_RECORDS}" \
    -e RUNE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" \
    "${CLIENT_IMAGE}" \
    bash -c "${IN_CONTAINER}" 2>&1)
client_rc=$?
set -e

echo "----- client output -----" >&2
echo "${client_out}" >&2
echo "-------------------------" >&2

# ---------------------------------------------------------------------------
# Step 5: Assert PROBE: OK + exit 0.
# ---------------------------------------------------------------------------
if [ "${client_rc}" -ne 0 ]; then
    fail "client exited non-zero (${client_rc})"
fi
if ! echo "${client_out}" | grep -q "PROBE: OK"; then
    fail "client did not print 'PROBE: OK'"
fi

log ""
log "========================================"
log "  TASK 10 (0c #11 GATE) CLI-DRIVEN E2E PASSED"
log "  Single node self-elected: ${leader}"
log "  Broker compiled against rune-codec (FixedStr)"
log "  rune schema apply --validate -> generate -> compile probe -> run"
log "  Probe: openTopic<Characters> -> send×${PROBE_RECORDS} -> consume -> decode"
log "         round-trip incl. code[8] FixedStr + var name"
log "         + malformed record REJECTED (SchemaValidationFailed)"
log "  PROBE: OK, exit 0."
log "========================================"
