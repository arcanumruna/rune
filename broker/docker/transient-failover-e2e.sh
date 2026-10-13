#!/usr/bin/env bash
# docker/transient-failover-e2e.sh — Phase 2b E2 Task 14 (Task 13 re-review N4):
# a REAL client against a REAL 3-node cluster moves its transient-topic
# subscriptions off a demoted leader.
#
# The single-node view-engine e2e (docker/view-engine-e2e.sh) cannot demote a
# leader; this leg runs on the 3-node compose (docker/docker-compose.yml — the
# same cluster docker/e2e.sh drives: aeron:udp, default shard worker mode).
#
#   1. Build the broker image and the client probe image (rune-client-cpp's
#      docker/Dockerfile.view-probe, examples/view_probe.cpp).
#   2. Start rune-1..3, await a consensus leader (bounded log poll).
#   3. Start the probe detached (RUNE_MODE=transient_failover).  On the STATIC
#      transient topic "presence_failover" (docker-compose.transient-failover.yml;
#      runtime DDL is single-node today, see that file) it produces offsets 0..4
#      (acks=all), binds a read_committed subscription from earliest (delivers
#      0..4) and a Latest read_committed subscription (bind-time position 5),
#      then flushes an OPEN transaction (offsets 5..9: replicated, past the HW,
#      delivered to neither), prints FAILOVER_LEADER=<partition leader node>
#      and "FAILOVER: READY".
#   4. Controlled stop of that leader's container (`docker compose stop`:
#      SIGTERM -> controlled shutdown hands partition leadership off; the
#      demoted node answers both subscriptions a position note + NotLeader),
#      (RESTART_DURING_MOVE=1: start it again at once; default: after the
#      verdict, so the move goes to a DIFFERENT node).
#   5. The probe, on its own: once the Latest subscription moved it commits the
#      transaction on the new leader, produces 200 records (acks=all) and asserts
#        (a) both subscriptions moved to the new leader and kept receiving
#            committed data (bounded waits, loud STALL failures);
#        (b) the Latest subscription lost no record from its bind-time position
#            on — in particular the transaction committed after the move
#            (re-resolving Latest = the HW on the new leader would skip it:
#            the position note is used);
#        (c) no record was delivered twice.
#      Exit 0 + "TRANSIENT-FAILOVER-E2E: OK".
#
# Variants (all by default; FAILOVER_VARIANTS selects): each on a fresh cluster.
# full-restart-group (E2 final fix wave C1): a consumer group commits, every node
# restarts — staggered, the partition's old leader stopped and started LAST
# (residual N1) — and the group must deliver every refilled record exactly once.
# The partition leader is deterministic (StaticBalancer: node 2 = rune-3):
#   seed-follows-redirect  the probe's seed is rune-1: its Subscribe is
#                          redirected to rune-3 and the seed session follows;
#   seed-is-leader         the probe's seed is rune-3 itself.
# Each variant also asserts: the leader is node 2; the seed did / did not
# redirect; the demoted node logged the moved-away summary and answered both
# subscriptions Redirect; the probe saw acked=200 inDoubt=0 and a new leader.
#
# Usage:  bash docker/transient-failover-e2e.sh   (from the rune-broker repo
# root; sibling layout ../rune-protocol ../rune-codec ../rune-crypto ../rune-fr
# ../rune-ql ../rune-client-cpp, as docker/view-engine-e2e.sh).
#
# Bounded waits everywhere, clean teardown (compose down -v) even on failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
CLIENT_REPO="${PROJECTS}/rune-client-cpp"

COMPOSE_FILE="${BROKER_REPO}/docker/docker-compose.yml"
COMPOSE_OVERRIDE="${BROKER_REPO}/docker/docker-compose.transient-failover.yml"
TOPIC_ID=3   # presence_failover: the 4th RUNE_TOPICS entry of the override
CLIENT_DOCKERFILE="${CLIENT_REPO}/docker/Dockerfile.view-probe"

LEADER_WAIT_S=180          # consensus leader after `up`
READY_WAIT_S=420           # probe: define + produce + both subscriptions bound
PROBE_DONE_WAIT_S=600      # probe: the move + the burst + its own bounded checks
PROBE_TIMEOUT_MS=20000     # ms per client op (amd64 emulation)
WAIT_BOUND_MS="${WAIT_BOUND_MS:-90000}"
MOVE_BOUND_MS="${MOVE_BOUND_MS:-180000}"
STOP_TIMEOUT_S=30          # SIGTERM grace (the broker's controlled-shutdown budget is 5 s)

# Own compose project (docker-compose.yml hardcodes `name: rune-broker`, which
# docker/e2e.sh uses): never collides with another e2e's cluster.
COMPOSE_PROJECT="rune-transient-failover-e2"
BROKER_NETWORK="${COMPOSE_PROJECT}_default"
PROBE_NAME="rune-tt-failover-probe"
PROBE_EGRESS_PORT="40500"

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
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-transient-failover"

# The variant checks read the gateway's debug lines (Subscribe redirect,
# moved-away Redirect): debug by default for this leg.
export RUNE_LOG_LEVEL=debug

log()  { echo "[transient-failover-e2e] $*" >&2; }
fail() { echo "[transient-failover-e2e] FAIL: $*" >&2; exit 1; }
dc()   { docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" -f "${COMPOSE_OVERRIDE}" "$@"; }

[ -d "${PROJECTS}/rune-protocol" ] || fail "missing ../rune-protocol"
[ -d "${PROJECTS}/rune-codec" ]    || fail "missing ../rune-codec"
[ -d "${PROJECTS}/rune-crypto" ]   || fail "missing ../rune-crypto"
[ -d "${PROJECTS}/rune-fr" ]       || fail "missing ../rune-fr"
[ -d "${PROJECTS}/rune-ql" ]       || fail "missing ../rune-ql"
[ -f "${CLIENT_DOCKERFILE}" ]      || fail "missing ${CLIENT_DOCKERFILE}"

teardown() {
    log "Tearing down (probe rm + compose down -v)..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then   # keep the evidence (broker + probe logs)
        local d="${E2E_LOG_DIR}/${VARIANT:-none}"
        mkdir -p "${d}"
        dc logs --no-color --timestamps rune-1 rune-2 rune-3 > "${d}/brokers.log" 2>&1 || true
        docker logs --timestamps "${PROBE_NAME}" > "${d}/probe.log" 2>&1 || true
    fi
    docker rm -f "${PROBE_NAME}" >/dev/null 2>&1 || true
    dc down -v --remove-orphans >/dev/null 2>&1 || true
}
trap teardown EXIT

node_id_to_service() {
    case "$1" in
        0) echo "rune-1" ;;
        1) echo "rune-2" ;;
        2) echo "rune-3" ;;
        *) fail "leader node id '$1' is not 0, 1 or 2" ;;
    esac
}

# ---------------------------------------------------------------------------
# Step 1: build (own exit codes captured — a failed build must never fall
# through to a stale image; see docker/view-engine-e2e.sh).
# ---------------------------------------------------------------------------
log "Step 1a: Building the broker image (tag=${RUNE_IMAGE_TAG})..."
set +e
out=$(dc build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 \
    || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Step 1b: Building the client probe image (${CLIENT_IMAGE})..."
set +e
out=$(docker build \
    --platform linux/amd64 \
    -f "${CLIENT_DOCKERFILE}" \
    --build-context rune-protocol="${PROJECTS}/rune-protocol" \
    --build-context rune-codec="${PROJECTS}/rune-codec" \
    --build-context rune-crypto="${PROJECTS}/rune-crypto" \
    --build-context rune-fr="${PROJECTS}/rune-fr" \
    --build-context rune-ql="${PROJECTS}/rune-ql" \
    -t "${CLIENT_IMAGE}" \
    "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" >&2; fail "client image build failed (exit ${rc})"; }
docker image inspect "${CLIENT_IMAGE}" >/dev/null 2>&1 || fail "client image not built: ${CLIENT_IMAGE}"

# ---------------------------------------------------------------------------
# One variant = a fresh cluster (Steps 2-5), torn down afterwards.
#   $1 label   $2 the probe's seed service
# The partition leader is DETERMINISTIC: StaticBalancer stripes partition
# leaders over the nodes in RUNE_TOPICS declaration order (events 1 + multi 3 +
# cg 4 partitions before it) -> presence_failover:0 -> node (8 % 3) = 2 =
# rune-3 (the dr-e2e harness relies on the same rule).  The seed choice gives
# the two client paths:
#   seed-is-leader         seed rune-3: plain subscriptions bind on the seed
#                          session directly;
#   seed-follows-redirect  seed rune-1: the Subscribe is redirected to rune-3
#                          and the seed session follows it there.
# ---------------------------------------------------------------------------
EXPECTED_LEADER=2
probe_running() { [ "$(docker inspect -f '{{.State.Running}}' "${PROBE_NAME}" 2>/dev/null)" = "true" ]; }
dump_probe() { echo "----- probe output -----" >&2; docker logs "${PROBE_NAME}" >&2 2>&1 || true; echo "------------------------" >&2; }

run_variant() {
    local label="$1" seed_svc="$2"
    VARIANT="${label}"
    log "======== variant ${label} (probe seed ${seed_svc}) ========"

    # ---- Step 2: the 3-node cluster + a consensus leader.
    log "Step 2: Starting rune-1, rune-2, rune-3..."
    dc up -d rune-1 rune-2 rune-3
    local deadline consensus_leader=""
    deadline=$(( $(date +%s) + LEADER_WAIT_S ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        consensus_leader=$(dc logs --no-log-prefix rune-1 rune-2 rune-3 2>/dev/null \
            | grep -oE 'node [0-9]+ is LEADER' | grep -oE '[0-9]+' | head -1) || true
        [ -n "${consensus_leader}" ] && break
        sleep 2
    done
    if [ -z "${consensus_leader}" ]; then
        dc logs --tail=60 rune-1 rune-2 rune-3 >&2 2>/dev/null || true
        fail "[${label}] no consensus leader within ${LEADER_WAIT_S}s"
    fi
    log "Consensus leader: node ${consensus_leader}"

    # ---- Step 3: the probe — bound subscriptions, an open txn, then READY.
    log "Step 3: Starting the probe (RUNE_MODE=transient_failover, seed ${seed_svc})..."
    docker rm -f "${PROBE_NAME}" >/dev/null 2>&1 || true
    docker run -d \
        --name "${PROBE_NAME}" --hostname "${PROBE_NAME}" --network "${BROKER_NETWORK}" \
        --platform linux/amd64 --shm-size=256m --security-opt seccomp=unconfined \
        -e RUNE_MODE=transient_failover -e RUNE_FAILOVER_TOPIC_ID="${TOPIC_ID}" \
        -e RUNE_TARGET="aeron:udp?endpoint=${seed_svc}:40200" -e RUNE_TARGET_STREAM=1 \
        -e RUNE_EGRESS="aeron:udp?endpoint=${PROBE_NAME}:${PROBE_EGRESS_PORT}" -e RUNE_EGRESS_STREAM=2 \
        -e RUNE_DATA_DIR="/tmp/rune_tt_failover" \
        -e RUNE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" \
        -e RUNE_WAIT_BOUND_MS="${WAIT_BOUND_MS}" \
        -e RUNE_MOVE_BOUND_MS="${MOVE_BOUND_MS}" \
        -e RUNE_EXPECT_LEADER_CHANGE="$([ "${RESTART_DURING_MOVE:-0}" = 1 ] && echo 0 || echo 1)" \
        "${CLIENT_IMAGE}" >/dev/null

    local ready=0
    deadline=$(( $(date +%s) + READY_WAIT_S ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        if docker logs "${PROBE_NAME}" 2>/dev/null | grep -q "^FAILOVER: READY"; then ready=1; break; fi
        probe_running || { dump_probe; fail "[${label}] the probe exited before READY"; }
        sleep 1
    done
    [ "${ready}" = 1 ] || { dump_probe; fail "[${label}] the probe did not print READY within ${READY_WAIT_S}s"; }
    local leader_id leader_svc
    leader_id=$(docker logs "${PROBE_NAME}" 2>/dev/null | grep -oE '^FAILOVER_LEADER=-?[0-9]+' | tail -1 | cut -d= -f2) || true
    [ -n "${leader_id}" ] || { dump_probe; fail "[${label}] the probe did not report the partition leader"; }
    [ "${leader_id}" = "${EXPECTED_LEADER}" ] \
        || { dump_probe; fail "[${label}] partition leader is node ${leader_id}, not the deterministic node ${EXPECTED_LEADER}"; }
    leader_svc=$(node_id_to_service "${leader_id}")
    log "presence_failover partition 0 is led by node ${leader_id} (${leader_svc}); consensus leader node ${consensus_leader}"
    # The variant's client path really happened (gateway debug log of the seed).
    local seed_log
    seed_log="$(dc logs --no-log-prefix "${seed_svc}" 2>/dev/null || true)"
    if [ "${seed_svc}" = "${leader_svc}" ]; then
        if echo "${seed_log}" | grep -q "Subscribe per-partition redirect (topicId=${TOPIC_ID} part=0"; then
            fail "[${label}] the seed is the leader but redirected the Subscribe"
        fi
    else
        echo "${seed_log}" | grep -q "Subscribe per-partition redirect (topicId=${TOPIC_ID} part=0 leader=${EXPECTED_LEADER}" \
            || fail "[${label}] the seed ${seed_svc} never redirected the Subscribe to node ${EXPECTED_LEADER}"
        log "  seed ${seed_svc} redirected the Subscribe to node ${EXPECTED_LEADER} (the seed session followed it)"
    fi

    # ---- Step 4: controlled stop (SIGTERM -> controlled shutdown).
    log "Step 4: Controlled stop of ${leader_svc} (SIGTERM, grace ${STOP_TIMEOUT_S}s)..."
    local t_stop stopped_log shutdown_line moved_line redirected
    t_stop=$(date +%s)
    dc stop -t "${STOP_TIMEOUT_S}" "${leader_svc}"
    log "${leader_svc} stopped after $(( $(date +%s) - t_stop ))s"
    stopped_log="$(dc logs --no-log-prefix "${leader_svc}" 2>/dev/null || true)"
    echo "${stopped_log}" | grep -q "controlled shutdown — handing partition leadership off" \
        || { echo "${stopped_log}" | tail -60 >&2; fail "[${label}] ${leader_svc} did not run a controlled shutdown"; }
    shutdown_line="$(echo "${stopped_log}" | grep -E "controlled shutdown (complete|budget)" | tail -1 || true)"
    log "  ${shutdown_line:-<no completion line>}"
    # The demoted node told the client: the moved-away summary, and (v6, a known
    # new leader) one Redirect per subscription (gateway debug line).
    moved_line="$(echo "${stopped_log}" | grep -E "subscription\(s\) this node can no longer serve" | tail -1 || true)"
    [ -n "${moved_line}" ] || fail "[${label}] ${leader_svc} logged no moved-away summary before exiting"
    log "  ${moved_line}"
    redirected="$(echo "${stopped_log}" | grep -cE "topicId=${TOPIC_ID} partition=0 .*demoted / partition moved away\): Redirect" || true)"
    [ "${redirected}" -ge 2 ] \
        || fail "[${label}] ${leader_svc} redirected ${redirected} subscription(s) to the new leader (want 2)"
    log "  ${leader_svc} answered ${redirected} subscription(s) Redirect to the new leader"
    if [ "${RESTART_DURING_MOVE:-0}" = 1 ]; then
        log "Restarting ${leader_svc} now (RESTART_DURING_MOVE=1)..."
        dc start "${leader_svc}"
    fi

    # ---- Step 5: the probe's verdict.
    deadline=$(( $(date +%s) + PROBE_DONE_WAIT_S ))
    while probe_running && [ "$(date +%s)" -lt "${deadline}" ]; do sleep 2; done
    if probe_running; then
        dump_probe
        fail "[${label}] STALL: the probe was still running ${PROBE_DONE_WAIT_S}s after the stop"
    fi
    local probe_rc probe_out ok_line new_leader
    probe_rc="$(docker inspect -f '{{.State.ExitCode}}' "${PROBE_NAME}")"
    probe_out="$(docker logs "${PROBE_NAME}" 2>&1 || true)"
    dump_probe
    [ "${probe_rc}" = 0 ] || fail "[${label}] the probe exited ${probe_rc}"
    ok_line="$(echo "${probe_out}" | grep -E '^TRANSIENT-FAILOVER-E2E: OK' | tail -1 || true)"
    [ -n "${ok_line}" ] || fail "[${label}] the probe did not print TRANSIENT-FAILOVER-E2E: OK"
    # Every record around the move acked, none in doubt (the probe asserts it too).
    echo "${ok_line}" | grep -qE "acked=${AROUND} inDoubt=0$" \
        || fail "[${label}] want acked=${AROUND} inDoubt=0, got: ${ok_line}"
    new_leader="$(echo "${probe_out}" | grep -oE '^FAILOVER_NEW_LEADER=-?[0-9]+' | tail -1 | cut -d= -f2 || true)"
    if [ "${RESTART_DURING_MOVE:-0}" != 1 ]; then
        [ -n "${new_leader}" ] && [ "${new_leader}" != "${leader_id}" ] && [ "${new_leader}" -ge 0 ] \
            || fail "[${label}] the leader did not move off node ${leader_id} (after: '${new_leader}')"
        # The stopped node comes back after the verdict and rejoins as a follower.
        log "Restarting ${leader_svc} (it rejoins as a follower)..."
        dc start "${leader_svc}"
    fi
    log "[${label}] PASSED: leader node ${leader_id} -> node ${new_leader}; ${ok_line}"
    RESULTS+=("${label}: leader ${leader_id} -> ${new_leader}; ${ok_line}")
    teardown
}

# ---------------------------------------------------------------------------
# Variant full-restart-group (E2 final fix wave C1 multi-node, option b):
# GroupConsume x FULL 3-node restart, asserted where the client sees it.  A
# consumer group commits presence_failover's end (20) on a fresh cluster; every
# node restarts (the transient ring is lost everywhere; metadata survives); the
# SAME group must then deliver every refilled record exactly once — its old
# committed offset belongs to data that did not survive (a new lineage is
# minted) and must never be resumed (exit 155 = the silent loss).
# ---------------------------------------------------------------------------
leader_lines() { dc logs --no-log-prefix rune-1 rune-2 rune-3 2>/dev/null | grep -cE 'node [0-9]+ is LEADER' || true; }
run_group_probe() {
    local mode="$1"; shift
    docker rm -f "${PROBE_NAME}" >/dev/null 2>&1 || true
    docker run --rm \
        --name "${PROBE_NAME}" --hostname "${PROBE_NAME}" --network "${BROKER_NETWORK}" \
        --platform linux/amd64 --shm-size=256m --security-opt seccomp=unconfined \
        -e RUNE_MODE="${mode}" -e RUNE_GROUP_TOPIC=presence_failover -e RUNE_VIEW_TT_TOPIC_ID="${TOPIC_ID}" \
        -e RUNE_TARGET="aeron:udp?endpoint=rune-1:40200" -e RUNE_TARGET_STREAM=1 \
        -e RUNE_EGRESS="aeron:udp?endpoint=${PROBE_NAME}:${PROBE_EGRESS_PORT}" -e RUNE_EGRESS_STREAM=2 \
        -e RUNE_DATA_DIR="/tmp/rune_tt_group" \
        -e RUNE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" -e RUNE_WAIT_BOUND_MS="${WAIT_BOUND_MS}" \
        "$@" "${CLIENT_IMAGE}" 2>&1
}
run_full_restart_group() {
    local label="full-restart-group"
    VARIANT="${label}"
    log "======== variant ${label} (GroupConsume x full 3-node restart) ========"
    dc up -d rune-1 rune-2 rune-3
    local deadline n0
    deadline=$(( $(date +%s) + LEADER_WAIT_S ))
    until [ "$(leader_lines)" -ge 1 ]; do
        [ "$(date +%s)" -lt "${deadline}" ] || fail "[${label}] no consensus leader within ${LEADER_WAIT_S}s"
        sleep 2
    done
    log "Step G1: a group commits presence_failover's end (20 records produced first)..."
    local out rc
    set +e
    out="$(run_group_probe group_commit -e RUNE_GROUP_PRODUCE=20 -e RUNE_VIEW_TT_END=20)"; rc=$?
    set -e
    echo "${out}" >&2
    [ "${rc}" -eq 0 ] || fail "[${label}] group_commit exited ${rc}"
    echo "${out}" | grep -q "^GROUP-COMMIT: OK committed=20" || fail "[${label}] the group did not commit 20"
    log "Step G2: FULL restart, STAGGERED: the partition's old leader (rune-3) stops last and starts last..."
    n0="$(leader_lines)"
    dc stop -t "${STOP_TIMEOUT_S}" rune-1 rune-2
    dc stop -t "${STOP_TIMEOUT_S}" rune-3
    dc start rune-1 rune-2
    deadline=$(( $(date +%s) + LEADER_WAIT_S ))
    until [ "$(leader_lines)" -gt "${n0}" ]; do
        [ "$(date +%s)" -lt "${deadline}" ] || fail "[${label}] no fresh consensus leader after the restart"
        sleep 2
    done
    # The old leader comes back last (the coordinator ran without it meanwhile).
    sleep "${STAGGER_S:-8}"
    dc start rune-3
    log "Step G3: the same group must deliver every post-restart record (2) exactly once..."
    set +e
    out="$(run_group_probe group_restart -e RUNE_GROUP_PRE=0)"; rc=$?
    set -e
    echo "${out}" >&2
    [ "${rc}" -eq 0 ] || fail "[${label}] group_restart exited ${rc} (155 = SILENT LOSS: resumed at the old offset)"
    echo "${out}" | grep -q "^GROUP-RESTART: OK" || fail "[${label}] the group did not deliver the post-restart records"
    local minted
    minted="$(dc logs --no-log-prefix rune-1 rune-2 rune-3 2>/dev/null | grep -cE "topicId=${TOPIC_ID} partition=0 .*data lineage MINTED" || true)"
    [ "${minted}" -ge 2 ] || fail "[${label}] want a lineage minted before and after the restart, saw ${minted} MINTED line(s)"
    log "[${label}] PASSED: committed 20 before the restart; after it the group delivered the refilled records exactly once (${minted} lineage mints)"
    RESULTS+=("${label}: committed 20; after a full 3-node restart every refilled record delivered exactly once")
    teardown
}

AROUND=200                 # the probe's kAround
RESULTS=()
VARIANTS="${FAILOVER_VARIANTS:-seed-follows-redirect seed-is-leader full-restart-group}"
for v in ${VARIANTS}; do
    case "${v}" in
        seed-follows-redirect) run_variant "${v}" rune-1 ;;
        seed-is-leader)        run_variant "${v}" rune-3 ;;
        full-restart-group)    run_full_restart_group ;;
        *) fail "unknown variant '${v}'" ;;
    esac
done

log ""
log "========================================"
log "  TRANSIENT FAILOVER E2E PASSED (3 nodes, shard mode)"
for r in "${RESULTS[@]}"; do log "  ${r}"; done
log "  -> read_committed + Latest (nothing delivered) subscriptions moved to the new leader"
log "  -> the transaction committed after the move reached the Latest subscription (it resumed at"
log "     its position note, not the new leader's HW); nothing lost, nothing re-delivered"
log "  TRANSIENT-FAILOVER-E2E: OK, exit 0."
log "========================================"
