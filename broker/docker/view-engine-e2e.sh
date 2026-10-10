#!/usr/bin/env bash
# docker/view-engine-e2e.sh — Task 21 real-Aeron view-engine e2e.
#
# SINGLE-NODE, DEFAULT-CONFIG, real-Aeron Phase 1 view-engine round-trip
# against a REAL rune-broker (NOT in-process). Modeled EXACTLY on
# docker/schema-evolution-e2e.sh: reuses docker-compose.single.yml for the
# broker and a dedicated client probe image (rune-client-cpp's
# docker/Dockerfile.view-probe, built from examples/view_probe.cpp).
#
# What it proves, against ONE broker, across TWO client container runs (a
# `docker run --rm` container cannot itself survive `docker compose restart
# rune-1` on the broker side — the restart leg necessarily spans two runs):
#
#   RUN 1 (RUNE_MODE=setup):
#     1. defineTopic "character" {id: Int64 key, hp: Int64}
#     2. createView "char_persisted" (persistence=0) and "char_transient"
#        (persistence=1), both a {id,hp} Replace-projection over "character"
#        keyed by "id" — Phase 1 has NO open-by-name wire, so the probe
#        remembers {viewTopicId, outputFingerprint} from each CreateView
#        response for the rest of its life (and hands them to RUN 2 below).
#     3. produce one "character" record (id=4242, hp=100)
#     4. getViewRow on BOTH views -> OK, decoded row == {id=4242, hp=100}
#     Prints VIEW_* KEY=VALUE handoff lines + "VIEW-PROBE-SETUP: OK".
#
#   RESTART: `docker compose restart rune-1` (same named volume — the
#   persisted view's checkpoint / the source topic's log survive; a
#   TRANSIENT view's in-memory table does not). Await a FRESH leader
#   election (bounded log poll, counting LEADER lines so a restart's new
#   election is distinguished from the pre-restart one still in the log).
#
#   RUN 2 (RUNE_MODE=verify_restart, fed the RUN 1 handoff as env vars):
#     (a) char_persisted -> OK with the ORIGINAL row (the readiness gate
#         replayed it across the restart — no reproduce needed).
#     (b) char_transient -> AbsentKey (no checkpoint survives a transient
#         view's restart).
#     (c) re-produce the SAME key with a NEW hp value -> char_transient
#         eventually answers OK with the NEW value (it folds live off the
#         source log, which IS persisted).
#     Prints "VIEW-ENGINE-E2E: OK" on success.
#
# Usage:  bash docker/view-engine-e2e.sh
# Run from the rune-broker repo root (this worktree). Requires the sibling
# worktrees: ../rune-protocol ../rune-codec ../rune-crypto ../rune-fr ../rune-ql
# ../rune-client-cpp (feat/view-engine-phase1 on both the broker and the
# client repo).
#
# Constraints (engineering philosophy #5/#7):
#   - Real client<->broker over real Aeron (NOT in-process); default config.
#   - Bounded waits, hard caps, clean teardown even on failure.
#   - NEVER a fixed sleep as a correctness gate — poll to a deadline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
CLIENT_REPO="${PROJECTS}/rune-client-cpp"

COMPOSE_FILE="${BROKER_REPO}/docker/docker-compose.single.yml"
CLIENT_DOCKERFILE="${CLIENT_REPO}/docker/Dockerfile.view-probe"

LEADER_WAIT_S=180         # max seconds to wait for the single node to self-elect
PROBE_TIMEOUT_MS=20000    # ms per client op (relaxed for amd64 emulation)
WAIT_BOUND_MS="${WAIT_BOUND_MS:-60000}"  # ms the probe itself polls a view read to a deadline

# docker-compose.single.yml hardcodes `name: rune-broker-single` (also used,
# unmodified, by docker/schema-e2e.sh / docker/schema-evolution-e2e.sh). `-p`
# isolates this e2e's container/network/volume under its own namespace so a
# concurrent run of another e2e script (or a second run of THIS one) can never
# collide with / tear down this one's live broker.
COMPOSE_PROJECT="rune-view-engine21"
BROKER_NETWORK="${COMPOSE_PROJECT}_default"
CLIENT_IMAGE="rune-view-probe:$(cd "${CLIENT_REPO}" && git rev-parse --short HEAD)"
CLIENT_HOSTNAME="rune-view-probe"
CLIENT_EGRESS_PORT="40500"

# Tag is <broker-commit-sha>-view-engine21: ties the image tag to Task 21
# specifically, on top of the commit sha, so it cannot collide with another
# concurrent worktree/task scenario's "rune-broker-node:*" image even if two
# happen to share a commit sha.
export RUNE_IMAGE_TAG="$(cd "${BROKER_REPO}" && git rev-parse --short HEAD)-view-engine21"

log()  { echo "[view-engine-e2e] $*" >&2; }
fail() { echo "[view-engine-e2e] FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Preflight: sibling repos + fixtures present.
# ---------------------------------------------------------------------------
[ -d "${PROJECTS}/rune-protocol" ] || fail "missing ../rune-protocol"
[ -d "${PROJECTS}/rune-codec" ]    || fail "missing ../rune-codec"
[ -d "${PROJECTS}/rune-crypto" ]   || fail "missing ../rune-crypto"
[ -d "${PROJECTS}/rune-fr" ]       || fail "missing ../rune-fr (symlink it to the canonical rune-fr checkout if not worktreed)"
[ -d "${PROJECTS}/rune-ql" ]       || fail "missing ../rune-ql"
[ -d "${CLIENT_REPO}" ]            || fail "missing ../rune-client-cpp"
[ -f "${CLIENT_DOCKERFILE}" ]      || fail "missing ${CLIENT_DOCKERFILE}"
[ -f "${CLIENT_REPO}/examples/view_probe.cpp" ] || fail "missing ${CLIENT_REPO}/examples/view_probe.cpp"

# ---------------------------------------------------------------------------
# Teardown — always runs.
# ---------------------------------------------------------------------------
teardown() {
    log "Tearing down (compose down -v + client rm)..."
    docker rm -f "${CLIENT_HOSTNAME}" 2>/dev/null || true
    docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true
}
trap teardown EXIT

# ---------------------------------------------------------------------------
# Step 1: Build broker image (docker-compose.single.yml) + client image
# (Dockerfile.view-probe: no runtime codegen — view_probe builds its own
# ViewPlan client-side and decodes rowBytes positionally).
# ---------------------------------------------------------------------------
log "Step 1a: Building single-node broker image (tag=${RUNE_IMAGE_TAG})..."
# IMPORTANT: capture `docker compose build`'s OWN exit status directly via
# command substitution (NOT through a `| grep ... || true` pipe under
# `set -o pipefail`, which would unconditionally report success regardless of
# whether the build itself failed — a genuinely broken build could then
# silently fall through to `compose up` reusing a STALE already-tagged image
# from an earlier successful run under the same commit-sha tag, and the e2e
# would test nothing of this run's actual source). Filter for display only
# AFTER the real exit code is captured and checked.
set +e
broker_build_out=$(docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" build 2>&1)
broker_build_rc=$?
set -e
echo "${broker_build_out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
if [ "${broker_build_rc}" -ne 0 ]; then
    echo "${broker_build_out}" >&2
    fail "broker image build failed (exit ${broker_build_rc}) — see full output above"
fi
# Defense in depth: confirm the expected-for-THIS-run tag actually exists
# (catches the image having been produced under a different/unexpected tag
# even if the build reported success).
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 \
    || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Step 1b: Building client image (${CLIENT_IMAGE})..."
# IMPORTANT: capture `docker build`'s OWN exit status directly (NOT through a
# `| grep ... || true` pipe under `set -o pipefail`, which reports the grep's
# exit and would unconditionally mask a genuinely broken build — the exact bug
# the broker-build block above documents and guards). Filter for display only
# AFTER the real rc is captured and checked, BEFORE the docker image inspect
# defense-in-depth check.
set +e
client_build_out=$(docker build \
    --platform linux/amd64 \
    -f "${CLIENT_DOCKERFILE}" \
    --build-context rune-protocol="${PROJECTS}/rune-protocol" \
    --build-context rune-codec="${PROJECTS}/rune-codec" \
    --build-context rune-crypto="${PROJECTS}/rune-crypto" \
    --build-context rune-fr="${PROJECTS}/rune-fr" \
    --build-context rune-ql="${PROJECTS}/rune-ql" \
    -t "${CLIENT_IMAGE}" \
    "${CLIENT_REPO}" 2>&1)
client_build_rc=$?
set -e
echo "${client_build_out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
if [ "${client_build_rc}" -ne 0 ]; then
    echo "${client_build_out}" >&2
    fail "client image build failed (exit ${client_build_rc}) — see full output above"
fi
docker image inspect "${CLIENT_IMAGE}" >/dev/null 2>&1 || fail "client image not built: ${CLIENT_IMAGE}"
log "Build complete."

# ---------------------------------------------------------------------------
# Step 2: Start the single broker node.
# ---------------------------------------------------------------------------
log "Step 2: Starting single broker node rune-1..."
docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" up -d rune-1

# ---------------------------------------------------------------------------
# Leader-wait helper: bounded log poll, counting LEADER lines so a restart's
# fresh election is distinguishable from an earlier (pre-restart) one still
# sitting in the container's (restarted-process, but volume-persisted-log)
# compose logs output.
# ---------------------------------------------------------------------------
leader_line_count() {
    docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" logs --no-log-prefix rune-1 2>/dev/null \
        | grep -cE 'node [0-9]+ is LEADER' || true
}

await_leader() {
    local min_count="$1" deadline leader_count=0
    deadline=$(( $(date +%s) + LEADER_WAIT_S ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        leader_count="$(leader_line_count)"
        if [ "${leader_count}" -gt "${min_count}" ]; then
            docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" logs --no-log-prefix rune-1 2>/dev/null \
                | grep -oE 'node [0-9]+ is LEADER' | tail -1
            return 0
        fi
        sleep 2
    done
    log "=== broker logs (tail) ==="
    docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" logs --tail=60 rune-1 2>/dev/null || true
    return 1
}

# ---------------------------------------------------------------------------
# Step 3: Await leader (bounded log poll).
# ---------------------------------------------------------------------------
log "Step 3: Awaiting leader (up to ${LEADER_WAIT_S}s)..."
leader1="$(await_leader 0)" || fail "Timed out (${LEADER_WAIT_S}s) waiting for initial leader"
log "Leader elected: ${leader1}"

# ---------------------------------------------------------------------------
# Step 4: RUN 1 (RUNE_MODE=setup) — define/createView x2/produce/getViewRow.
# ---------------------------------------------------------------------------
BROKER_TARGET="aeron:udp?endpoint=rune-1:40200"
EGRESS_CH="aeron:udp?endpoint=${CLIENT_HOSTNAME}:${CLIENT_EGRESS_PORT}"

run_client() {
    local mode="$1"; shift
    docker rm -f "${CLIENT_HOSTNAME}" 2>/dev/null || true
    docker run --rm \
        --name "${CLIENT_HOSTNAME}" \
        --hostname "${CLIENT_HOSTNAME}" \
        --network "${BROKER_NETWORK}" \
        --platform linux/amd64 \
        --shm-size=256m \
        --security-opt seccomp=unconfined \
        -e RUNE_MODE="${mode}" \
        -e RUNE_TARGET="${BROKER_TARGET}" \
        -e RUNE_TARGET_STREAM=1 \
        -e RUNE_EGRESS="${EGRESS_CH}" \
        -e RUNE_EGRESS_STREAM=2 \
        -e RUNE_DATA_DIR="/tmp/rune_view_probe" \
        -e RUNE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" \
        -e RUNE_WAIT_BOUND_MS="${WAIT_BOUND_MS}" \
        "$@" \
        "${CLIENT_IMAGE}"
}

log "Step 4: RUN 1 (setup) — define/createView x2/produce/getViewRow..."
set +e
setup_out=$(run_client setup 2>&1)
setup_rc=$?
set -e

echo "----- setup output -----" >&2
echo "${setup_out}" >&2
echo "-------------------------" >&2

if [ "${setup_rc}" -ne 0 ]; then
    fail "setup client exited non-zero (${setup_rc})"
fi
if ! echo "${setup_out}" | grep -q "VIEW-PROBE-SETUP: OK"; then
    fail "setup client did not print the 'VIEW-PROBE-SETUP: OK' sentinel"
fi

extract() {
    echo "${setup_out}" | grep -oE "^$1=.*" | tail -1 | cut -d= -f2-
}
VIEW_SRC_TOPIC_ID="$(extract VIEW_SRC_TOPIC_ID)"
VIEW_TOPIC_ID_PERSISTED="$(extract VIEW_TOPIC_ID_PERSISTED)"
VIEW_FP_PERSISTED="$(extract VIEW_FP_PERSISTED)"
VIEW_TOPIC_ID_TRANSIENT="$(extract VIEW_TOPIC_ID_TRANSIENT)"
VIEW_FP_TRANSIENT="$(extract VIEW_FP_TRANSIENT)"
VIEW_KEY_ID="$(extract VIEW_KEY_ID)"
VIEW_HP_INITIAL="$(extract VIEW_HP_INITIAL)"

for v in VIEW_SRC_TOPIC_ID VIEW_TOPIC_ID_PERSISTED VIEW_FP_PERSISTED \
         VIEW_TOPIC_ID_TRANSIENT VIEW_FP_TRANSIENT VIEW_KEY_ID VIEW_HP_INITIAL; do
    [ -n "${!v:-}" ] || fail "setup output did not carry ${v}= (handoff parse failed)"
done
log "Setup OK: srcTopicId=${VIEW_SRC_TOPIC_ID}" \
    "viewPersisted={id=${VIEW_TOPIC_ID_PERSISTED},fp=${VIEW_FP_PERSISTED}}" \
    "viewTransient={id=${VIEW_TOPIC_ID_TRANSIENT},fp=${VIEW_FP_TRANSIENT}}" \
    "key=${VIEW_KEY_ID} hp0=${VIEW_HP_INITIAL}"

# ---------------------------------------------------------------------------
# Step 5: Restart the broker (same named volume — persisted state survives).
# ---------------------------------------------------------------------------
pre_restart_leader_count="$(leader_line_count)"
log "Step 5: Restarting rune-1 (docker compose restart)..."
docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" restart rune-1

log "Step 5b: Awaiting a FRESH leader election post-restart (up to ${LEADER_WAIT_S}s)..."
leader2="$(await_leader "${pre_restart_leader_count}")" \
    || fail "Timed out (${LEADER_WAIT_S}s) waiting for leader after restart"
log "Leader re-elected: ${leader2}"

# ---------------------------------------------------------------------------
# Step 6: RUN 2 (RUNE_MODE=verify_restart) — persisted recovers, transient is
# empty then refilled.
# ---------------------------------------------------------------------------
log "Step 6: RUN 2 (verify_restart) — persisted recovery + transient empty-then-refilled..."
set +e
restart_out=$(run_client verify_restart \
    -e RUNE_VIEW_SRC_TOPIC_ID="${VIEW_SRC_TOPIC_ID}" \
    -e RUNE_VIEW_TOPIC_ID_PERSISTED="${VIEW_TOPIC_ID_PERSISTED}" \
    -e RUNE_VIEW_FP_PERSISTED="${VIEW_FP_PERSISTED}" \
    -e RUNE_VIEW_TOPIC_ID_TRANSIENT="${VIEW_TOPIC_ID_TRANSIENT}" \
    -e RUNE_VIEW_FP_TRANSIENT="${VIEW_FP_TRANSIENT}" \
    -e RUNE_VIEW_KEY_ID="${VIEW_KEY_ID}" \
    -e RUNE_VIEW_HP_INITIAL="${VIEW_HP_INITIAL}" \
    2>&1)
restart_rc=$?
set -e

echo "----- verify_restart output -----" >&2
echo "${restart_out}" >&2
echo "----------------------------------" >&2

# ---------------------------------------------------------------------------
# Step 7: Assert exit 0 AND the final sentinel.
# ---------------------------------------------------------------------------
if [ "${restart_rc}" -ne 0 ]; then
    fail "verify_restart client exited non-zero (${restart_rc})"
fi
if ! echo "${restart_out}" | grep -q "VIEW-ENGINE-E2E: OK"; then
    fail "verify_restart client did not print the final 'VIEW-ENGINE-E2E: OK' sentinel"
fi

log ""
log "========================================"
log "  TASK 21 VIEW-ENGINE E2E PASSED"
log "  Single node self-elected: ${leader1}"
log "  define(character) -> createView(char_persisted, char_transient)"
log "  -> produce -> getViewRow x2 OK"
log "  -> restart -> re-elected: ${leader2}"
log "  -> char_persisted recovered OK (readiness gate replay)"
log "  -> char_transient AbsentKey (empty) -> reproduce -> refilled OK"
log "  VIEW-ENGINE-E2E: OK, exit 0."
log "========================================"
