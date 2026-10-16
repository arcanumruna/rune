#!/usr/bin/env bash
# docker/schema-evolution-e2e.sh — Task 21 real-Aeron schema-evolution e2e.
#
# SINGLE-NODE, DEFAULT-CONFIG, real-Aeron schema-evolution round-trip against a
# REAL rune-broker (NOT in-process). Modeled on docker/schema-e2e.sh (single-
# node typed round-trip) and the client's docker/rune-ql-e2e.sh (CLI-driven
# apply/generate/probe flow); reuses docker-compose.single.yml for the broker
# and the client's Dockerfile.rune-ql-probe (full-toolchain, runtime-compiled
# probe) pattern for the client image.
#
# What it proves, all against ONE broker, via the `rune` CLI + 4 generated
# "evo_probe_vN" typed probes (../rune-client-cpp/tests/e2e/evo_probe_v{1..4}.cpp):
#
#   1. rune schema apply   v1.rql                  -> v1; produce/consume/decode OK.
#   2. rune schema alter   v2_add.rql    (ADD)      -> v2; produce v2; consume v1+v2,
#                                                      each decoded per its own version.
#   3. rune schema alter   v3_drop.rql   (DROP)     -> v3 (default BACKWARD succeeds);
#                                                      old v1/v2 records still decode
#                                                      the dropped field; v3 omits it.
#   4. rune schema alter   v4_rename.rql (RENAME)   -> v4; a v3 record decodes
#                                                      byte-identical under the new
#                                                      name (power == old level).
#   5. rune schema alter   v5_set_compat_forward.rql -> v5 (SET COMPATIBILITY forward);
#      rune schema alter   v6_drop_reject.rql        -> REJECTED (SchemaIncompatible).
#   6. The v1-bound probe binary (compiled ONCE, in step 1, never recompiled)
#      still opens the topic after it has evolved to v5 (fingerprint-selecting
#      GetTopicSchema binds it to its own v1, not latest).
#   7. Two concurrent `rune schema alter` calls with the SAME --expected-version
#      -> exactly one succeeds, the other returns SchemaConflict.
#   8. (GC7) multi-producer-different-versions + long-lineage reopen: AFTER the
#      full v1..v5 lineage, evo_probe_multi drives a v1-bound AND a v3-bound
#      producer against the SAME evolved topic; both writes land; the mixed log
#      is re-read and each sentinel record decodes per its OWN writer version
#      (v1 keeps the original tag field, v3 reflects the drop/rename lineage) with
#      no cross-version read; both producers fingerprint-select their OWN version
#      (1 and 3), not latest (v5).
#
# Usage:  bash docker/schema-evolution-e2e.sh
# Run from the rune-broker repo root (this worktree).  Requires the sibling
# worktrees: ../rune-protocol ../rune-codec ../rune-client-cpp ../rune-ql,
# plus the (unworktreed, feature-unrelated) ../rune-fr flight recorder.
#
# Constraints (engineering philosophy #11/#16):
#   - Real client<->broker over real Aeron (NOT in-process); default config.
#   - Bounded waits, hard caps, clean teardown even on failure.
#   - NEVER a fixed sleep as a correctness gate — poll to a deadline.

set -euo pipefail
# The Dockerfiles use BuildKit cache mounts (ccache/conan/build dir): BuildKit is mandatory.
export DOCKER_BUILDKIT=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
CLIENT_REPO="${PROJECTS}/rune-client-cpp"

COMPOSE_FILE="${BROKER_REPO}/docker/docker-compose.single.yml"
CLIENT_DOCKERFILE="${CLIENT_REPO}/docker/Dockerfile.rune-ql-probe"

LEADER_WAIT_S=180        # max seconds to wait for the single node to self-elect
PROBE_TIMEOUT_MS=20000   # ms per probe operation (relaxed for amd64 emulation)
RECORDS_V1="${RECORDS_V1:-5}"
RECORDS_V2="${RECORDS_V2:-5}"
RECORDS_V3="${RECORDS_V3:-5}"
RECORDS_V4="${RECORDS_V4:-5}"
RECORDS_MULTI="${RECORDS_MULTI:-3}"   # GC7 phase 8: records PER producer (v1 + v3)

# docker-compose.single.yml hardcodes `name: rune-broker-single` (also used,
# unmodified, by docker/schema-e2e.sh).  Running under that SHARED project
# name would collide with a concurrent schema-e2e.sh run (or a second run of
# THIS script) on the same container/network/volume names — one script's
# `compose down -v` would tear down the other's live broker mid-test.  `-p`
# overrides the project name per-invocation (no compose-file edit needed),
# isolating this e2e's container/network/volume under its own namespace.
COMPOSE_PROJECT="rune-schema-evo21"
BROKER_NETWORK="${COMPOSE_PROJECT}_default"
CLIENT_IMAGE="rune-schema-evo-probe:$(cd "${CLIENT_REPO}" && git rev-parse --short HEAD)"
CLIENT_HOSTNAME="rune-schema-evo-probe"
CLIENT_EGRESS_PORT="40500"

# Tag is <broker-commit-sha>-schema-evo21: the docker daemon on this host is
# shared across many concurrent worktree/task scenarios (dr-e2e, schema-e2e,
# rune-ql-e2e, other feature worktrees, ...) all building "rune-broker-node:*"
# images — a generic suffix risks a same-name collision with some OTHER
# scenario's image/container.  "schema-evo21" ties the tag to Task 21
# specifically, on top of the commit sha, so it cannot collide with another
# scenario's build even if two happen to share a commit sha.
export RUNE_IMAGE_TAG="$(cd "${BROKER_REPO}" && git rev-parse --short HEAD)-schema-evo21"

log()  { echo "[schema-evolution-e2e] $*" >&2; }
fail() { echo "[schema-evolution-e2e] FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Preflight: sibling repos + fixtures present.
# ---------------------------------------------------------------------------
[ -d "${PROJECTS}/rune-protocol" ] || fail "missing ../rune-protocol"
[ -d "${PROJECTS}/rune-codec" ]    || fail "missing ../rune-codec"
[ -d "${PROJECTS}/rune-crypto" ]   || fail "missing ../rune-crypto"
[ -d "${PROJECTS}/rune-ql" ]       || fail "missing ../rune-ql"
[ -d "${PROJECTS}/rune-fr" ]       || fail "missing ../rune-fr (symlink it to the canonical rune-fr checkout if not worktreed)"
[ -d "${CLIENT_REPO}" ]            || fail "missing ../rune-client-cpp"
[ -f "${CLIENT_DOCKERFILE}" ]      || fail "missing ${CLIENT_DOCKERFILE}"
for f in v1.rql v2_add.rql v3_drop.rql v4_rename.rql v5_set_compat_forward.rql \
         v6_drop_reject.rql race_a.rql race_b.rql; do
    [ -f "${CLIENT_REPO}/tests/e2e/schema_evolution/${f}" ] || fail "missing tests/e2e/schema_evolution/${f}"
done
for f in evo_probe_v1.cpp evo_probe_v2.cpp evo_probe_v3.cpp evo_probe_v4.cpp evo_probe_multi.cpp; do
    [ -f "${CLIENT_REPO}/tests/e2e/${f}" ] || fail "missing tests/e2e/${f}"
done

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
# (Dockerfile.rune-ql-probe: full toolchain retained, `rune` CLI on PATH,
# evo_probe_vN compiled at RUNTIME once their generated headers exist).
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
# Defense in depth, symmetric to the client image check below: confirm the
# expected-for-THIS-run tag actually exists (catches the image having been
# produced under a different/unexpected tag even if the build reported success).
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 \
    || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Step 1b: Building client image (${CLIENT_IMAGE})..."
# IMPORTANT: capture `docker build`'s OWN exit status directly (NOT through a
# `| grep ... || true` pipe under `set -o pipefail`, which reports the grep's
# exit and would unconditionally mask a genuinely broken build — the exact bug
# the broker-build block above documents and guards). Filter for display only
# AFTER the real rc is captured and checked, BEFORE the docker image inspect
# defense-in-depth check. (GC7 fix-on-sight, philosophy #5/#16 test-masking.)
set +e
client_build_out=$(docker build \
    --platform linux/amd64 \
    -f "${CLIENT_DOCKERFILE}" \
    --build-context rune-protocol="${PROJECTS}/rune-protocol" \
    --build-context rune-codec="${PROJECTS}/rune-codec" \
    --build-context rune-ql="${PROJECTS}/rune-ql" \
    --build-context rune-fr="${PROJECTS}/rune-fr" \
    --build-context rune-crypto="${PROJECTS}/rune-crypto" \
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
# Step 3: Await leader (bounded log poll).
# ---------------------------------------------------------------------------
log "Step 3: Awaiting leader (up to ${LEADER_WAIT_S}s)..."
deadline=$(( $(date +%s) + LEADER_WAIT_S ))
leader=""
while [ "$(date +%s)" -lt "${deadline}" ]; do
    raw=$(docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" logs --no-log-prefix rune-1 2>/dev/null \
          | grep -oE 'node [0-9]+ is LEADER' | head -1) || true
    if [ -n "${raw}" ]; then leader="${raw}"; break; fi
    sleep 2
done
if [ -z "${leader}" ]; then
    log "=== broker logs (tail) ==="
    docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" logs --tail=60 rune-1 2>/dev/null || true
    fail "Timed out (${LEADER_WAIT_S}s) waiting for leader"
fi
log "Leader elected: ${leader}"

# ---------------------------------------------------------------------------
# Step 4: Run the CLI-driven, multi-phase client container on the broker
# network.  One container, one long IN_CONTAINER script covering all 7 phases
# (sequential CLI apply/alter/generate + compiled evo_probe_vN runs, plus the
# two concurrent-alter / reject sub-checks) so the schema-evolution sequence
# is exercised against a single live broker end to end.
# ---------------------------------------------------------------------------
BROKER_TARGET="aeron:udp?endpoint=rune-1:40200"
EGRESS_CH="aeron:udp?endpoint=${CLIENT_HOSTNAME}:${CLIENT_EGRESS_PORT}"

read -r -d '' IN_CONTAINER <<'EOS' || true
set -euo pipefail
RQL=tests/e2e/schema_evolution
EVO_GEN=/build/build/evo-gen
mkdir -p /tmp/gen-v1 /tmp/gen-v2 /tmp/gen-v3 /tmp/gen-v4 "${EVO_GEN}"

rename_ns() {
    # Rename the generated struct's inner namespace "gen" -> "gen_vN" so
    # multiple per-version snapshots of the SAME struct name (CharsEvo) can
    # coexist, qualified, in one translation unit.
    local file="$1" ns="$2"
    sed -i \
        -e "s/^namespace gen {/namespace ${ns} {/" \
        -e "s#^} // namespace gen\$#} // namespace ${ns}#" \
        "${file}"
}

echo "[in] ===== PHASE 1: apply v1 (id,userId,name) ====="
rune schema apply "${RQL}/v1.rql" --broker "${RUNE_TARGET}" --validate \
    --cli-data-dir /tmp/cli_apply_v1

echo "[in] generate + stage v1 header..."
rune schema generate chars_evo --out /tmp/gen-v1 --broker "${RUNE_TARGET}" \
    --cli-data-dir /tmp/cli_gen_v1
cp /tmp/gen-v1/chars_evo.gen.h "${EVO_GEN}/chars_evo_v1.gen.h"
rename_ns "${EVO_GEN}/chars_evo_v1.gen.h" gen_v1

echo "[in] compile + run evo_probe_v1 (produce_consume)..."
cmake --build /build/build --target evo_probe_v1
RUNE_MODE=produce_consume RUNE_RECORDS_V1="${RECORDS_V1}" /build/build/evo_probe_v1

echo "[in] ===== PHASE 2: alter ADD COLUMN level int32 (v1 -> v2) ====="
rune schema alter "${RQL}/v2_add.rql" --broker "${RUNE_TARGET}" --expected-version 1 \
    --cli-data-dir /tmp/cli_alter_v2

rune schema generate chars_evo --out /tmp/gen-v2 --broker "${RUNE_TARGET}" \
    --cli-data-dir /tmp/cli_gen_v2
cp /tmp/gen-v2/chars_evo.gen.h "${EVO_GEN}/chars_evo_v2.gen.h"
rename_ns "${EVO_GEN}/chars_evo_v2.gen.h" gen_v2

cmake --build /build/build --target evo_probe_v2
RUNE_RECORDS_V1="${RECORDS_V1}" RUNE_RECORDS_V2="${RECORDS_V2}" /build/build/evo_probe_v2

echo "[in] ===== PHASE 3: alter DROP COLUMN name (v2 -> v3, default BACKWARD) ====="
rune schema alter "${RQL}/v3_drop.rql" --broker "${RUNE_TARGET}" --expected-version 2 \
    --cli-data-dir /tmp/cli_alter_v3

rune schema generate chars_evo --out /tmp/gen-v3 --broker "${RUNE_TARGET}" \
    --cli-data-dir /tmp/cli_gen_v3
cp /tmp/gen-v3/chars_evo.gen.h "${EVO_GEN}/chars_evo_v3.gen.h"
rename_ns "${EVO_GEN}/chars_evo_v3.gen.h" gen_v3

cmake --build /build/build --target evo_probe_v3
RUNE_RECORDS_V1="${RECORDS_V1}" RUNE_RECORDS_V2="${RECORDS_V2}" RUNE_RECORDS_V3="${RECORDS_V3}" \
    /build/build/evo_probe_v3

echo "[in] ===== PHASE 4: alter RENAME COLUMN level TO power (v3 -> v4) ====="
rune schema alter "${RQL}/v4_rename.rql" --broker "${RUNE_TARGET}" --expected-version 3 \
    --cli-data-dir /tmp/cli_alter_v4

rune schema generate chars_evo --out /tmp/gen-v4 --broker "${RUNE_TARGET}" \
    --cli-data-dir /tmp/cli_gen_v4
cp /tmp/gen-v4/chars_evo.gen.h "${EVO_GEN}/chars_evo_v4.gen.h"
rename_ns "${EVO_GEN}/chars_evo_v4.gen.h" gen_v4

cmake --build /build/build --target evo_probe_v4
RUNE_RECORDS_V1="${RECORDS_V1}" RUNE_RECORDS_V2="${RECORDS_V2}" RUNE_RECORDS_V3="${RECORDS_V3}" \
    RUNE_RECORDS_V4="${RECORDS_V4}" /build/build/evo_probe_v4

echo "[in] ===== PHASE 5: SET COMPATIBILITY forward, then DROP -> must be REJECTED ====="
rune schema alter "${RQL}/v5_set_compat_forward.rql" --broker "${RUNE_TARGET}" --expected-version 4 \
    --cli-data-dir /tmp/cli_alter_v5

set +e
reject_out=$(rune schema alter "${RQL}/v6_drop_reject.rql" --broker "${RUNE_TARGET}" \
    --expected-version 5 --cli-data-dir /tmp/cli_alter_v6_reject 2>&1)
reject_rc=$?
set -e
echo "${reject_out}"
if [ "${reject_rc}" -eq 0 ]; then
    echo "[in] FAIL: drop-under-forward was ACCEPTED (expected a SchemaIncompatible reject)" >&2
    exit 40
fi
# Tighten beyond "any nonzero exit": the CLI's cmdSchemaAlter returns the
# broker's wire errorCode as its process exit code (rune::cli reportAlterResult
# -> `return static_cast<int>(r.errorCode)`), and
# rune::protocol::client::ErrorCode::SchemaIncompatible == 30 (Protocol.h) —
# distinct from SchemaConflict (31, phase 7) or any other failure mode (e.g. a
# CLI usage error, a connect/timeout failure, or a different broker rejection).
# Require BOTH the exact code AND the message token so this phase cannot be
# satisfied by some other, unrelated rejection.
RUNE_SCHEMA_INCOMPATIBLE_CODE=30
if [ "${reject_rc}" -ne "${RUNE_SCHEMA_INCOMPATIBLE_CODE}" ]; then
    echo "[in] FAIL: drop-under-forward was rejected, but with exit=${reject_rc}" \
         "(want ${RUNE_SCHEMA_INCOMPATIBLE_CODE} = SchemaIncompatible)" >&2
    exit 41
fi
if ! echo "${reject_out}" | grep -qi "incompatible"; then
    echo "[in] FAIL: drop-under-forward exited ${reject_rc} but without an 'incompatible' message" >&2
    exit 42
fi
echo "[in] drop-under-forward correctly REJECTED (SchemaIncompatible, errorCode=${reject_rc})"

echo "[in] ===== PHASE 6: the v1-bound probe (compiled ONCE, in phase 1) still opens at v5 ====="
RUNE_MODE=open_only /build/build/evo_probe_v1

echo "[in] ===== PHASE 7: concurrent ALTER, same --expected-version -> SchemaConflict ====="
set +e
rune schema alter "${RQL}/race_a.rql" --broker "${RUNE_TARGET}" --expected-version 5 \
    --cli-egress "aeron:udp?endpoint=${RUNE_EGRESS_HOST}:40501" \
    --cli-data-dir /tmp/cli_race_a > /tmp/race_a.out 2>&1 &
PID_A=$!
rune schema alter "${RQL}/race_b.rql" --broker "${RUNE_TARGET}" --expected-version 5 \
    --cli-egress "aeron:udp?endpoint=${RUNE_EGRESS_HOST}:40502" \
    --cli-data-dir /tmp/cli_race_b > /tmp/race_b.out 2>&1 &
PID_B=$!
wait "${PID_A}"; RC_A=$?
wait "${PID_B}"; RC_B=$?
set -e

echo "[in] race_a (rc=${RC_A}):"; cat /tmp/race_a.out
echo "[in] race_b (rc=${RC_B}):"; cat /tmp/race_b.out

successes=0
conflicts=0
if [ "${RC_A}" -eq 0 ]; then successes=$((successes + 1));
elif grep -qi "stale\|conflict" /tmp/race_a.out; then conflicts=$((conflicts + 1)); fi
if [ "${RC_B}" -eq 0 ]; then successes=$((successes + 1));
elif grep -qi "stale\|conflict" /tmp/race_b.out; then conflicts=$((conflicts + 1)); fi

if [ "${successes}" -ne 1 ] || [ "${conflicts}" -ne 1 ]; then
    echo "[in] FAIL: expected exactly one success + one SchemaConflict; got successes=${successes} conflicts=${conflicts}" >&2
    exit 50
fi
echo "[in] concurrent-alter race OK: exactly one success, one SchemaConflict"

echo "[in] ===== PHASE 8 (GC7): multi-producer-different-versions + long-lineage reopen ====="
# The topic is now at v5 (phase 5), the full v1->v2->v3->v4->v5 lineage behind
# it.  evo_probe_multi links BOTH the v1 and v3 generated snapshots already
# staged (phases 1 and 3) and, in ONE run against the evolved topic:
#   (a) opens two producers bound to DIFFERENT versions (v1 and v3) and both
#       write chars_evo;
#   (b) re-reads the mixed log and decodes EACH of its sentinel records per its
#       OWN writer version (v1 carries the original `name` tag; v3 reflects the
#       drop/rename lineage) — no record cross-reads another version's layout;
#   (c) asserts the v1-bound producer binds v1 and the v3-bound producer binds v3
#       (fingerprint-select binds each to its OWN version, NOT latest v5).
# Non-zero exit on ANY mismatch (handled by the container's set -e).
cmake --build /build/build --target evo_probe_multi
RUNE_RECORDS_MULTI="${RECORDS_MULTI}" /build/build/evo_probe_multi

echo "SCHEMA-EVOLUTION-E2E: OK"
EOS

log "Step 4: Running CLI-driven multi-phase client (records v1..v4=${RECORDS_V1}/${RECORDS_V2}/${RECORDS_V3}/${RECORDS_V4})..."
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
    -e RUNE_EGRESS_HOST="${CLIENT_HOSTNAME}" \
    -e RUNE_DATA_DIR="/tmp/rune_evo_probe" \
    -e RUNE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" \
    -e RECORDS_V1="${RECORDS_V1}" \
    -e RECORDS_V2="${RECORDS_V2}" \
    -e RECORDS_V3="${RECORDS_V3}" \
    -e RECORDS_V4="${RECORDS_V4}" \
    -e RECORDS_MULTI="${RECORDS_MULTI}" \
    -e RUNE_RECORDS_MULTI="${RECORDS_MULTI}" \
    "${CLIENT_IMAGE}" \
    bash -c "${IN_CONTAINER}" 2>&1)
client_rc=$?
set -e

echo "----- client output -----" >&2
echo "${client_out}" >&2
echo "--------------------------" >&2

# ---------------------------------------------------------------------------
# Step 5: Assert exit 0 AND the final sentinel.
# ---------------------------------------------------------------------------
if [ "${client_rc}" -ne 0 ]; then
    fail "client exited non-zero (${client_rc})"
fi
if ! echo "${client_out}" | grep -q "SCHEMA-EVOLUTION-E2E: OK"; then
    fail "client did not print the final 'SCHEMA-EVOLUTION-E2E: OK' sentinel"
fi

log ""
log "========================================"
log "  TASK 21 SCHEMA-EVOLUTION E2E PASSED"
log "  Single node self-elected: ${leader}"
log "  v1 apply -> v2 add -> v3 drop (BACKWARD) -> v4 rename -> v5 set-forward"
log "  -> drop REJECTED (SchemaIncompatible) -> v1-bound reopen at v5 OK"
log "  -> concurrent same-version alter: 1 success + 1 SchemaConflict"
log "  -> GC7: multi-producer v1+v3 both write evolved topic; mixed-log"
log "         decode per writer version; v1/v3 reopen bind own version"
log "  SCHEMA-EVOLUTION-E2E: OK, exit 0."
log "========================================"
