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
#     5. Phase 2a: defineTopic "order" {orderId key, userId, amount}; createView
#        "user_totals" (persistence=0; GROUP BY userId: Count(*), Sum, Avg);
#        produce o1(u100,10) o2(u100,30) o3(u200,5), update o1->50, move
#        o3->(u100,5) -> u100 = {3, 85, 85/3}, u200 AbsentKey (emptied group).
#     6. Phase 2b C: typed_route (8 partitions) routed by key.
#     7. Phase 2b E/E2: transient topic presence_t defined at runtime; produce
#        acks None/Leader/All; subscribe earliest/offset/latest; read_committed
#        over a committed + an aborted transaction.
#     Prints VIEW_* KEY=VALUE handoff lines + "VIEW-PROBE-SETUP: OK".
#
#   RESTART: `docker compose restart rune-1` (same named volume — the
#   persisted view's checkpoint / the source topic's log survive; a
#   TRANSIENT view's in-memory table does not). Await a FRESH leader
#   election (bounded log poll, counting LEADER lines so a restart's new
#   election is distinguished from the pre-restart one still in the log).
#
#   RUN 2 (RUNE_MODE=verify_restart, fed the RUN 1 handoff as env vars):
#     (a) char_persisted -> OK with the ORIGINAL row (checkpoint + tail replay
#         behind the readiness gate — no reproduce needed).
#     (b) char_transient -> OK with the ORIGINAL row too: it has no checkpoint,
#         so the readiness gate rebuilds it by re-folding the retained source
#         log (spec §9.7) — the source topic itself survives the restart via
#         runtime-topic recovery.
#     (c) re-produce the SAME key with a NEW hp value -> BOTH views converge to
#         the NEW value (live folding resumes after the restart).
#     (d) user_totals recovered from its v2 checkpoint: u100 = {3, 85, 85/3},
#         u200 AbsentKey; then o3 5->25 (a PRE-restart contributor) ->
#         u100 = {3, 105, 35} — proves the contributor map survived (a lost
#         map would give n 4, total 110).
#     (e) presence_t exists after the restart (TopicExists, same id) and is
#         empty (first produce at offset 0; subscribe + read_committed see 1).
#     Prints "VIEW-ENGINE-E2E: OK" on success.
#
#   DDL conformance legs: setup also asserts ALTER on a view answers 38
#   (ViewNotAlterable); two race containers (RUNE_MODE=ddl_race) fire colliding
#   DDL released by one barrier file — exactly one wins per leg; and a
#   RUNE_MODE=catalog_dump snapshot taken before the restart must equal the
#   one taken after it.
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

# Image-tag source for a repo: its short HEAD sha when it is a git checkout;
# otherwise (e.g. a `git archive` staging copy, which has no .git) a content
# hash of its files, so a staged tree needs no `git init` and two different
# staged trees never share a tag.
sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then
        git -C "${d}" rev-parse --short HEAD
        return
    fi
    (cd "${d}" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
CLIENT_IMAGE="rune-view-probe:$(repo_tag "${CLIENT_REPO}")"
CLIENT_HOSTNAME="rune-view-probe"
CLIENT_EGRESS_PORT="40500"

# Tag is <broker-commit-sha>-view-engine21: ties the image tag to Task 21
# specifically, on top of the commit sha, so it cannot collide with another
# concurrent worktree/task scenario's "rune-broker-node:*" image even if two
# happen to share a commit sha.
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-view-engine21"

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
    docker rm -f rune-ddl-race-a rune-ddl-race-b 2>/dev/null || true
    [ -n "${RACE_DIR:-}" ] && rm -rf "${RACE_DIR}" || true
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
# Capture stdout (the machine-readable handoff: VIEW_*=... lines + the
# VIEW-PROBE-SETUP sentinel) SEPARATELY from stderr (the human PROBE: log).
# Merging them with 2>&1 let an unbuffered stderr PROBE: line race onto the
# start of a stdout handoff line ("PROBE: VIEW_SRC_TOPIC_ID=0"), which broke
# the ^-anchored extract below — a racy correctness gate.  Keep streams split.
set +e
setup_err="$(mktemp)"
setup_out=$(run_client setup 2>"${setup_err}")
setup_rc=$?
set -e
setup_err_text="$(cat "${setup_err}" 2>/dev/null || true)"; rm -f "${setup_err}"

echo "----- setup stdout (handoff) -----" >&2
echo "${setup_out}" >&2
echo "----- setup stderr (PROBE log) ---" >&2
echo "${setup_err_text}" >&2
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
VIEW_ORDER_TOPIC_ID="$(extract VIEW_ORDER_TOPIC_ID)"
VIEW_TOPIC_ID_AGG="$(extract VIEW_TOPIC_ID_AGG)"
VIEW_FP_AGG="$(extract VIEW_FP_AGG)"
VIEW_TT_TOPIC_ID="$(extract VIEW_TT_TOPIC_ID)"

for v in VIEW_SRC_TOPIC_ID VIEW_TOPIC_ID_PERSISTED VIEW_FP_PERSISTED \
         VIEW_TOPIC_ID_TRANSIENT VIEW_FP_TRANSIENT VIEW_KEY_ID VIEW_HP_INITIAL \
         VIEW_ORDER_TOPIC_ID VIEW_TOPIC_ID_AGG VIEW_FP_AGG VIEW_TT_TOPIC_ID; do
    [ -n "${!v:-}" ] || fail "setup output did not carry ${v}= (handoff parse failed)"
done
log "Setup OK: srcTopicId=${VIEW_SRC_TOPIC_ID}" \
    "viewPersisted={id=${VIEW_TOPIC_ID_PERSISTED},fp=${VIEW_FP_PERSISTED}}" \
    "viewTransient={id=${VIEW_TOPIC_ID_TRANSIENT},fp=${VIEW_FP_TRANSIENT}}" \
    "key=${VIEW_KEY_ID} hp0=${VIEW_HP_INITIAL}" \
    "orderTopicId=${VIEW_ORDER_TOPIC_ID} viewAgg={id=${VIEW_TOPIC_ID_AGG},fp=${VIEW_FP_AGG}}" \
    "transientTopicId=${VIEW_TT_TOPIC_ID}"

# Phase 2b Part C: typed_route (8 partitions) — typed records routed by key.
echo "${setup_out}" | grep -q "^TYPED-ROUTING: OK" \
    || fail "setup client did not print 'TYPED-ROUTING: OK' (typed topics must route by key)"
log "Typed routing OK: $(echo "${setup_out}" | grep -E '^TYPED-ROUTING: OK' | tail -1)"

# ---------------------------------------------------------------------------
# The CREATE path above (define -> createView x2 -> produce -> getViewRow x2 OK)
# has passed.  The restart -> recover leg below runs by default; set
# RUN_RESTART_LEG=0 for a quick create-path-only run.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Step 4b (DDL conformance): ALTER on a view's output answers ViewNotAlterable.
# ---------------------------------------------------------------------------
echo "${setup_out}" | grep -q "^ALTER-VIEW: OK code=38" \
    || fail "setup client did not print 'ALTER-VIEW: OK code=38' (alter topic <view> must be 38)"
log "ALTER on a view: 38 (ViewNotAlterable) OK"

# ---------------------------------------------------------------------------
# Step 4c (DDL conformance): two clients fire colliding DDL concurrently.  Both
# containers connect and print READY, then block on a shared barrier file; the
# script releases them together.  Per leg exactly one wins (0) and the other gets
# the collision code: topic/topic 25, view/view 32, topic/view 25.
# ---------------------------------------------------------------------------
RACE_DIR="$(mktemp -d "${BROKER_REPO}/.ddl-race.XXXXXX")"
RACE_A="rune-ddl-race-a"
RACE_B="rune-ddl-race-b"

start_race_client() {
    local role="$1" host="$2"
    docker rm -f "${host}" >/dev/null 2>&1 || true
    docker run -d \
        --name "${host}" --hostname "${host}" --network "${BROKER_NETWORK}" \
        --platform linux/amd64 --shm-size=256m --security-opt seccomp=unconfined \
        -v "${RACE_DIR}:/barrier" \
        -e RUNE_MODE=ddl_race -e RUNE_RACE_ROLE="${role}" -e RUNE_BARRIER_DIR=/barrier \
        -e RUNE_TARGET="${BROKER_TARGET}" -e RUNE_TARGET_STREAM=1 \
        -e RUNE_EGRESS="aeron:udp?endpoint=${host}:${CLIENT_EGRESS_PORT}" -e RUNE_EGRESS_STREAM=2 \
        -e RUNE_DATA_DIR="/tmp/rune_view_probe" \
        -e RUNE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" -e RUNE_WAIT_BOUND_MS="${WAIT_BOUND_MS}" \
        "${CLIENT_IMAGE}" >/dev/null
}
await_race_ready() {
    local host="$1" deadline
    deadline=$(( $(date +%s) + 120 ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        docker logs "${host}" 2>/dev/null | grep -q "^DDL-RACE: READY" && return 0
        sleep 1
    done
    return 1
}
race_code() {   # $1 = stdout of one race client, $2 = leg
    echo "$1" | grep -oE "^DDL_RACE leg=$2 role=[ab] code=[0-9]+" | tail -1 | sed -E 's/.*code=//'
}
check_race_leg() {   # $1 = leg, $2 = the loser's collision code
    local leg="$1" loser="$2" a b
    a="$(race_code "${race_out_a}" "${leg}")"
    b="$(race_code "${race_out_b}" "${leg}")"
    [ -n "${a}" ] && [ -n "${b}" ] || fail "DDL race leg ${leg}: missing result (a='${a}' b='${b}')"
    if { [ "${a}" = 0 ] && [ "${b}" = "${loser}" ]; } || { [ "${b}" = 0 ] && [ "${a}" = "${loser}" ]; }; then
        log "DDL race leg ${leg}: exactly one winner (a=${a} b=${b})"
    else
        fail "DDL race leg ${leg}: want exactly one 0 and one ${loser}, got a=${a} b=${b}"
    fi
}

log "Step 4c: DDL race — two clients, colliding DDL, released together..."
start_race_client a "${RACE_A}"
start_race_client b "${RACE_B}"
await_race_ready "${RACE_A}" || fail "race client a never printed READY"
await_race_ready "${RACE_B}" || fail "race client b never printed READY"
touch "${RACE_DIR}/go"
race_rc_a="$(docker wait "${RACE_A}")"
race_rc_b="$(docker wait "${RACE_B}")"
race_out_a="$(docker logs "${RACE_A}" 2>/dev/null)"
race_out_b="$(docker logs "${RACE_B}" 2>/dev/null)"
echo "----- race a -----" >&2; echo "${race_out_a}" >&2
echo "----- race b -----" >&2; echo "${race_out_b}" >&2
[ "${race_rc_a}" = 0 ] && [ "${race_rc_b}" = 0 ] || fail "race clients exited ${race_rc_a}/${race_rc_b}"
check_race_leg topic 25
check_race_leg view 32
check_race_leg mixed 25
docker rm -f "${RACE_A}" "${RACE_B}" >/dev/null 2>&1 || true

CATALOG_NAMES="character,char_persisted,char_transient,order,user_totals,typed_route,race_topic,race_view,race_mixed,presence_t"
dump_catalog() {
    local out
    out="$(run_client catalog_dump -e RUNE_CATALOG_NAMES="${CATALOG_NAMES}" 2>/dev/null)" || return 1
    echo "${out}" | grep -q "^CATALOG-DUMP: OK" || return 1
    echo "${out}" | grep -E "^CATALOG name="
}

if [ "${RUN_RESTART_LEG:-1}" != "1" ]; then
    log ""
    log "========================================"
    log "  VIEW-ENGINE E2E (create path only) PASSED"
    log "  Single node self-elected: ${leader1}"
    log "  define(character) -> createView(char_persisted, char_transient)"
    log "  -> produce -> getViewRow x2 OK"
    log "  restart->recover leg SKIPPED (RUN_RESTART_LEG=0)."
    log "  VIEW-ENGINE-E2E: OK, exit 0."
    log "========================================"
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 4d (DDL conformance): snapshot the catalog before the restart.
# ---------------------------------------------------------------------------
catalog_before="$(dump_catalog)" || fail "catalog dump (pre-restart) failed"
echo "${catalog_before}" | grep -q " code=[1-9]" && fail "a pre-restart catalog entry did not resolve: ${catalog_before}"
log "Catalog before restart:"; echo "${catalog_before}" >&2

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
    -e RUNE_VIEW_ORDER_TOPIC_ID="${VIEW_ORDER_TOPIC_ID}" \
    -e RUNE_VIEW_TOPIC_ID_AGG="${VIEW_TOPIC_ID_AGG}" \
    -e RUNE_VIEW_FP_AGG="${VIEW_FP_AGG}" \
    -e RUNE_VIEW_TT_TOPIC_ID="${VIEW_TT_TOPIC_ID}" \
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

# ---------------------------------------------------------------------------
# Step 7b (DDL conformance): the catalog is identical after the restart.
# ---------------------------------------------------------------------------
catalog_after="$(dump_catalog)" || fail "catalog dump (post-restart) failed"
if [ "${catalog_before}" != "${catalog_after}" ]; then
    diff <(echo "${catalog_before}") <(echo "${catalog_after}") >&2 || true
    fail "the DDL catalog differs after the restart"
fi
log "Catalog identical after restart ($(echo "${catalog_after}" | wc -l | tr -d ' ') entries)"

# ---------------------------------------------------------------------------
# Step 8: prove user_totals was restored from its v2 CHECKPOINT (not merely
# re-folded from the retained source, which would also yield correct rows).
# The broker logs ONE "VIEW-RECOVERED view=<id> partition=<p> O=<O>
# records=<n> format=<v1|v2>" line per successful recovery replay; require one
# for the aggregate view with format=v2 and O > 0.
# ---------------------------------------------------------------------------
log "Step 8: Checking broker logs for user_totals' v2 checkpoint recovery..."
recovered_lines="$(docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" logs --no-log-prefix rune-1 2>/dev/null \
    | grep -oE "VIEW-RECOVERED view=${VIEW_TOPIC_ID_AGG} partition=[0-9]+ O=-?[0-9]+ records=[0-9]+ format=v[12]" || true)"
echo "----- VIEW-RECOVERED (user_totals) -----" >&2
echo "${recovered_lines:-<none>}" >&2
echo "----------------------------------------" >&2
[ -n "${recovered_lines}" ] \
    || fail "no VIEW-RECOVERED line for user_totals (view=${VIEW_TOPIC_ID_AGG}) — not recovered from a checkpoint"
echo "${recovered_lines}" | grep -qE " O=[1-9][0-9]* records=[1-9][0-9]* format=v2$" \
    || fail "user_totals VIEW-RECOVERED line lacks format=v2 with O>0: ${recovered_lines}"
log "user_totals recovered from a v2 checkpoint: $(echo "${recovered_lines}" | tail -1)"

log ""
log "========================================"
log "  VIEW-ENGINE E2E (full, incl. restart) PASSED"
log "  Single node self-elected: ${leader1}"
log "  define(character) -> createView(char_persisted, char_transient)"
log "  -> produce -> getViewRow x2 OK"
log "  -> restart -> re-elected: ${leader2}"
log "  -> char_persisted recovered OK (readiness gate replay)"
log "  -> char_transient re-folded from the retained source OK"
log "  -> reproduce -> both views fold the new value live OK"
log "  -> user_totals (Count(*)/Sum/Avg) recovered from its v2 checkpoint (VIEW-RECOVERED format=v2, O>0)"
log "     + post-restart update retracts OK"
log "  -> typed_route: 8-partition typed topic routed by key (Phase 2b C)"
log "  -> DDL conformance: alter on a view = 38; two racing clients, exactly one"
log "     winner per leg (topic 25, view 32, mixed 25); catalog identical across restart"
log "  -> transient topic presence_t: produce (None/Leader/All) + subscribe (earliest/offset/latest) + read_committed (commit+abort); restart -> exists (TopicExists, same id) and empty (first offset 0)"
log "  VIEW-ENGINE-E2E: OK, exit 0."
log "========================================"
