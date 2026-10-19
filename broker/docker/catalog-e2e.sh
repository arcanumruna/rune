#!/usr/bin/env bash
# docker/catalog-e2e.sh — catalog history, Task 24: 3-node docker e2e (spec §10).
#
#   1. DDL through each broker (a follower answers Redirect; the CLI follows it);
#   2. the `__catalog` leader killed mid-DDL (same --request-id re-run: exactly one CreateTopic);
#   3. `rune catalog export`, then `replay` into a fresh second cluster;
#   4. `rune catalog diff` of the two clusters reports no differences.
#
# Two 3-node clusters (docker/docker-compose.catalog.yml, projects catalog-a / catalog-b) and ONE
# long-lived client container (the rune-ql-probe image: the `rune` CLI + the retained toolchain, in
# which view_probe and rune_produce_subscribe are built once).  Every client command is a
# `docker exec` into it; it is attached to both cluster networks and addresses the brokers by their
# `a-rune-N` / `b-rune-N` names.
#
# Usage:  bash docker/catalog-e2e.sh   (sibling layout ../rune-protocol ../rune-codec ../rune-crypto
#         ../rune-fr ../rune-ql ../rune-client-cpp, as docker/transient-failover-e2e.sh)
#
# House style: bounded waits (never a fixed sleep as a gate), trap teardown, every failure names its step.
# Environment:
#   E2E_LOG_DIR   when set, the brokers' and the client's logs are kept there at teardown
#   E2E_WORK_DIR  scratch dir shared with the client container as /work (default <repo>/.catalog-e2e-work)

# Authentication (Task 20): both clusters run with authentication ON.  One cluster key is generated before the first
# `up`; each cluster is bootstrapped (first admin + the e2e user) once it has a leader; the client container logs in as
# the e2e user (RUNE_USER / RUNE_PASSWORD_FILE in its environment, so every `rune ...`, view_probe and
# rune_produce_subscribe run authenticated).  The e2e role grants CREATE/ALTER/DROP/READ/WRITE on everything and
# DESCRIBE ON CLUSTER (catalog history, export, diff and replay need it).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
CLIENT_REPO="${PROJECTS}/rune-client-cpp"
COMPOSE_FILE="${BROKER_REPO}/docker/docker-compose.catalog.yml"
WORK_DIR="${E2E_WORK_DIR:-${BROKER_REPO}/.catalog-e2e-work}"

LEADER_WAIT_S=180      # consensus leader per cluster + `show history` answering
OP_TIMEOUT_S=150       # one client command (the CLI has its own 15 s DDL budget inside)
CONVERGE_WAIT_S=180    # restarted node's catalog == node 1's
NEWLEADER_WAIT_S=90    # a new __catalog leader after the kill
TOOLS="catalog-e2e-tools"
EGRESS_PORT=40500
BROKER_PORT=40200
MIDKILL_RID="00000000000000c1000000000000006d"

# Both clusters build (and run) the one image; ninja -j2 inside the broker build.
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"

log()  { echo "[catalog-e2e] $*" >&2; }
fail() { echo "[catalog-e2e] FAIL: $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.catalog-e2e-work/*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
export RUNE_AUTH_WORK_DIR="${WORK_DIR}"   # the client container's /work is also where the passwords live
export RUNE_AUTH_TOOLS="${TOOLS}"   # the CLI runs inside the long-lived client container
rune_auth_init
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-catalog-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"

dc() { local c="$1"; shift; CLUSTER="${c}" docker compose -p "catalog-${c}" -f "${COMPOSE_FILE}" "$@"; }
node_id()      { echo $(( $1 - 1 )); }                 # node N (1-based) -> consensus node id
tx()           { docker exec "${TOOLS}" "$@"; }        # a command in the client container
txt()          { local t="$1"; shift; docker exec "${TOOLS}" timeout "${t}" "$@"; }
rune_ql()      { txt "${OP_TIMEOUT_S}" rune ql "$@"; }

for d in rune-protocol rune-codec rune-crypto rune-fr rune-ql rune-client-cpp; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done
[ -f "${COMPOSE_FILE}" ] || fail "missing ${COMPOSE_FILE}"

teardown() {
    local rc=$?
    log "Tearing down (exit ${rc})..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then
        mkdir -p "${E2E_LOG_DIR}"
        for c in a b; do
            for n in 1 2 3; do docker logs --timestamps "${c}-rune-${n}" > "${E2E_LOG_DIR}/${c}-rune-${n}.log" 2>&1 || true; done
        done
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.rcat "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
    dc a down -v --remove-orphans >/dev/null 2>&1 || true
    dc b down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "${WORK_DIR}" 2>/dev/null || true
    rune_auth_cleanup
    return "${rc}"
}
trap teardown EXIT

# wait_until <deadline_s> <description> <command...> : polls the command (0 = done) every 2 s.
wait_until() {
    local budget="$1" what="$2"; shift 2
    local deadline=$(( $(date +%s) + budget ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 2
    done
    return 1
}

# await_cluster <a|b> : every node logs the same consensus leader, then `show history` answers via node 1.
cluster_has_leader() {
    local c="$1" ids n id
    ids=""
    for n in 1 2 3; do
        id=$(docker logs "${c}-rune-${n}" 2>&1 | grep -oE 'node [0-9]+ is LEADER' | tail -1 | grep -oE '[0-9]+') || return 1
        [ -n "${id}" ] || return 1
        ids="${ids} ${id}"
    done
    [ "$(echo "${ids}" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')" = "1" ]
}
history_answers() { txt 60 rune ql -e "show history;" --broker "$1-rune-1:${BROKER_PORT}"; }
await_cluster() {
    local c="$1"
    wait_until "${LEADER_WAIT_S}" "consensus leader of cluster ${c}" cluster_has_leader "${c}" \
        || { for n in 1 2 3; do docker logs --tail 40 "${c}-rune-${n}" >&2 || true; done; fail "cluster ${c}: no common consensus leader within ${LEADER_WAIT_S}s"; }
    log "cluster ${c}: bootstrapping the first admin + the e2e user..."
    rune_auth_prepare "catalog-${c}_default" "${c}-rune-1:${BROKER_PORT}" || fail "cluster ${c}: bootstrap / e2e user"
    wait_until "${LEADER_WAIT_S}" "show history on cluster ${c}" history_answers "${c}" \
        || fail "cluster ${c}: 'show history;' never succeeded against ${c}-rune-1 within ${LEADER_WAIT_S}s"
    log "cluster ${c}: consensus leader elected, catalog answering"
}

# catalog_leader : prints "<node 1-based> <epoch>" of the highest-epoch `__catalog role -> LeaderServing` line.
catalog_leader() {
    local best_n="" best_e=-1 n e
    for n in 1 2 3; do
        e=$(docker logs "a-rune-${n}" 2>&1 | grep -oE '__catalog role -> LeaderServing \(epoch [0-9]+\)' | tail -1 | grep -oE '[0-9]+\)' | tr -d ')') || true
        [ -n "${e}" ] || continue
        if [ "${e}" -gt "${best_e}" ]; then best_e="${e}"; best_n="${n}"; fi
    done
    [ -n "${best_n}" ] || return 1
    echo "${best_n} ${best_e}"
}

# The client container is multi-homed (default bridge + the cluster networks): a hostname egress would
# bind the bridge address, which the brokers cannot reach.  Egress channels name the container's IP on
# cluster a's network (the `rune` CLI derives its own from the route to the broker).
tools_ip() { docker inspect -f "{{(index .NetworkSettings.Networks \"catalog-$1_default\").IPAddress}}" "${TOOLS}"; }

mkdir -p "${WORK_DIR}"

# ---------------------------------------------------------------------------
# Step 1: build the broker image and the rune CLI image (exit status checked directly).
# ---------------------------------------------------------------------------
log "Step 1a: building the broker image (tag ${RUNE_IMAGE_TAG}; the three nodes share it)..."
set +e
out=$(dc a build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Step 1b: building the rune CLI image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"

log "Step 1c: starting the client container; building view_probe + rune_produce_subscribe in it..."
docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
docker run -d --name "${TOOLS}" --hostname "${TOOLS}" --platform linux/amd64 --shm-size=256m \
    --security-opt seccomp=unconfined \
    -v "${WORK_DIR}:/work" -v "${SCRIPT_DIR}/topics:/topics:ro" -v "${RUNE_SECRETS_DIR}:/run/secrets:ro" \
    -e RUNE_USER="${RUNE_AUTH_E2E_USER}" -e RUNE_PASSWORD_FILE=/work/e2e.pw \
    "${QL_IMAGE}" sleep infinity >/dev/null || fail "could not start the client container"
set +e
out=$(tx cmake --build /build/build --target view_probe rune_produce_subscribe -j2 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "building the client probes failed (exit ${rc})"; }
tx test -x /build/build/view_probe && tx test -x /build/build/rune_produce_subscribe || fail "client probes missing after build"

# ---------------------------------------------------------------------------
# Step 2: cluster a up.
# ---------------------------------------------------------------------------
log "Step 2: generating the cluster key; cluster a up..."
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"
dc a up -d --no-build rune-1 rune-2 rune-3
docker network connect catalog-a_default "${TOOLS}" || fail "could not attach the client container to catalog-a_default"
TOOLS_IP_A="$(tools_ip a)"; [ -n "${TOOLS_IP_A}" ] || fail "no IP for ${TOOLS} on catalog-a_default"
await_cluster a

# ---------------------------------------------------------------------------
# Step 3: DDL through each broker.
# ---------------------------------------------------------------------------
log "Step 3: DDL through each broker..."
rune_ql -f /topics/catalog-e2e.rql --broker "a-rune-1:${BROKER_PORT}" || fail "step 3: catalog-e2e.rql through a-rune-1"
rune_ql -e "alter topic players add column mana int32;" --broker "a-rune-2:${BROKER_PORT}" || fail "step 3: alter topic players through a-rune-2"
rune_ql -e "alter topic events set retention.ms = 600000;" --broker "a-rune-3:${BROKER_PORT}" || fail "step 3: alter topic events through a-rune-3"
txt "${OP_TIMEOUT_S}" env RUNE_TARGET="aeron:udp?endpoint=a-rune-2:${BROKER_PORT}" \
    RUNE_EGRESS="aeron:udp?endpoint=${TOOLS_IP_A}:${EGRESS_PORT}" RUNE_DATA_DIR=/tmp/vp-create \
    RUNE_TIMEOUT_MS=30000 RUNE_VIEW_SOURCE=players RUNE_VIEW_NAME=players_hp \
    /build/build/view_probe --create-only || fail "step 3: view_probe --create-only through a-rune-2"
log "Step 3 OK: players/events/presence, alter players, alter events, view players_hp"

# ---------------------------------------------------------------------------
# Step 4: the __catalog leader killed mid-DDL.
# ---------------------------------------------------------------------------
log "Step 4: finding the __catalog leader..."
wait_until 60 "a __catalog leader line" catalog_leader || fail "step 4: no 'AdminWorker: __catalog role -> LeaderServing' line within 60s"
read -r killed killed_epoch < <(catalog_leader)
log "Step 4: __catalog leader = a-rune-${killed} (epoch ${killed_epoch}); starting 'topic midkill' there and killing it 50 ms later..."
( set +e   # a failing CLI must still record its exit status
  txt "${OP_TIMEOUT_S}" rune ql -e "topic midkill partitions 2;" --request-id "${MIDKILL_RID}" \
      --broker "a-rune-${killed}:${BROKER_PORT}" --verbose > "${WORK_DIR}/midkill1.out" 2>&1
  echo $? > "${WORK_DIR}/midkill1.rc" ) &
bg=$!
sleep 0.05   # the kill must land while the DDL is in flight; this is the stimulus, not a gate
docker kill "a-rune-${killed}" >/dev/null || fail "step 4: docker kill a-rune-${killed}"
wait "${bg}" || true
mk_rc=$(cat "${WORK_DIR}/midkill1.rc" 2>/dev/null || echo missing)
log "Step 4: the killed attempt exited ${mk_rc}: $(tr '\n' ' ' < "${WORK_DIR}/midkill1.out" | cut -c1-300)"
case "${mk_rc}" in
    0) ;;                                                                  # retried on the new leader, deduplicated
    1) grep -qE "NotLeader" "${WORK_DIR}/midkill1.out" \
         || grep -qE "connect to .* timed out" "${WORK_DIR}/midkill1.out" \
         || fail "step 4: the killed attempt exited 1 without NotLeader (nor a connect timeout to the killed node)" ;;
    *) fail "step 4: the killed attempt exited ${mk_rc} (want 0, or 1 with NotLeader)" ;;
esac

survivor=1; [ "${killed}" = "1" ] && survivor=2
new_leader_elected() {
    local r n e
    r=$(catalog_leader_excluding) || return 1
    echo "${r}"
}
# A new LeaderServing with a higher epoch than the killed leader's, on a survivor.
catalog_leader_excluding() {
    local n e best=""
    for n in 1 2 3; do
        [ "${n}" = "${killed}" ] && continue
        e=$(docker logs "a-rune-${n}" 2>&1 | grep -oE '__catalog role -> LeaderServing \(epoch [0-9]+\)' | tail -1 | grep -oE '[0-9]+\)' | tr -d ')') || true
        [ -n "${e}" ] && [ "${e}" -gt "${killed_epoch}" ] && best="${n}"
    done
    [ -n "${best}" ] && echo "${best}"
}
wait_until "${NEWLEADER_WAIT_S}" "a new __catalog leader" new_leader_elected \
    || fail "step 4: no new __catalog leader (epoch > ${killed_epoch}) within ${NEWLEADER_WAIT_S}s"
log "Step 4: new __catalog leader = a-rune-$(new_leader_elected)"

log "Step 4: re-running the SAME statement with the SAME --request-id through a-rune-${survivor}..."
rune_ql -e "topic midkill partitions 2;" --request-id "${MIDKILL_RID}" --broker "a-rune-${survivor}:${BROKER_PORT}" --verbose \
    || fail "step 4: the --request-id re-run did not exit 0 (F17: the dedupe must return the original result, never TopicExists)"
hist=$(rune_ql -e "show history topic midkill;" --broker "a-rune-${survivor}:${BROKER_PORT}") || fail "step 4: show history topic midkill"
creates=$(echo "${hist}" | grep -c "CreateTopic" || true)
[ "${creates}" = "1" ] || { echo "${hist}" >&2; fail "step 4: 'show history topic midkill;' lists ${creates} CreateTopic events (want exactly 1)"; }
log "Step 4: exactly one CreateTopic for midkill"

log "Step 4: restarting a-rune-${killed}; waiting for the catalogs to converge..."
docker start "a-rune-${killed}" >/dev/null || fail "step 4: docker start a-rune-${killed}"
ref=1; [ "${killed}" = "1" ] && ref=2
converged() { txt 90 rune catalog diff --from "a-rune-${ref}:${BROKER_PORT}" --to "a-rune-${killed}:${BROKER_PORT}"; }
wait_until "${CONVERGE_WAIT_S}" "catalog convergence" converged \
    || { converged >&2 || true; fail "step 4: a-rune-${killed} did not converge to a-rune-${ref} within ${CONVERGE_WAIT_S}s"; }
log "Step 4 OK: all brokers converged"

# ---------------------------------------------------------------------------
# Step 5: produce + consume through every broker on `events` (topic id 1, 1 partition).
# ---------------------------------------------------------------------------
log "Step 5: produce + consume on events through every broker..."
for n in 1 2 3; do
    set +e
    out=$(txt "${OP_TIMEOUT_S}" env RUNE_TARGET="aeron:udp?endpoint=a-rune-${n}:${BROKER_PORT}" \
        RUNE_EGRESS="aeron:udp?endpoint=${TOOLS_IP_A}:${EGRESS_PORT}" RUNE_DATA_DIR="/tmp/ps-${n}" \
        RUNE_CLIENT_TOPIC_TABLE="events:1:1" RUNE_PRODUCE_N=10 RUNE_PRODUCE_M=0 RUNE_TIMEOUT_MS=20000 \
        /build/build/rune_produce_subscribe 2>&1); rc=$?
    set -e
    echo "${out}" > "${WORK_DIR}/produce-${n}.out"
    [ "${rc}" -eq 0 ] || { echo "${out}" | tail -30 >&2; fail "step 5: rune_produce_subscribe via a-rune-${n} exited ${rc}"; }
    echo "${out}" | grep -q "CLIENT: DONE rc=0" || fail "step 5: via a-rune-${n}: 'CLIENT: DONE rc=0' not printed"
done
log "Step 5 OK"

# ---------------------------------------------------------------------------
# Step 6: export -> replay into a fresh cluster -> diff.
# ---------------------------------------------------------------------------
log "Step 6: export a's catalog..."
tx sh -c "rune catalog export --broker a-rune-1:${BROKER_PORT} > /work/a.rcat" || fail "step 6: rune catalog export"
[ -s "${WORK_DIR}/a.rcat" ] || fail "step 6: /work/a.rcat is empty"
src_hist=$(rune_ql -e "show history;" --broker "a-rune-1:${BROKER_PORT}") || fail "step 6: show history on a"
src_rows=$(( $(echo "${src_hist}" | wc -l | tr -d ' ') - 1 ))

log "Step 6: cluster b up (fresh, empty)..."
dc b up -d --no-build rune-1 rune-2 rune-3
docker network connect catalog-b_default "${TOOLS}" || fail "could not attach the client container to catalog-b_default"
await_cluster b

log "Step 6: replay a.rcat into b (--cluster-id a)..."
txt 600 rune catalog replay --from /work/a.rcat --into "b-rune-1:${BROKER_PORT}" --cluster-id a || fail "step 6: rune catalog replay"

set +e
diff_out=$(txt 120 rune catalog diff --from "a-rune-1:${BROKER_PORT}" --to "b-rune-1:${BROKER_PORT}" 2>"${WORK_DIR}/diff.err"); diff_rc=$?
set -e
[ "${diff_rc}" -eq 0 ] || { echo "${diff_out}" >&2; cat "${WORK_DIR}/diff.err" >&2; fail "step 6: 'rune catalog diff' a vs b exited ${diff_rc} (want 0)"; }
[ -z "${diff_out}" ] || { echo "${diff_out}" >&2; fail "step 6: 'rune catalog diff' printed differences although it exited 0"; }

b_hist=$(rune_ql -e "show history;" --broker "b-rune-2:${BROKER_PORT}") || fail "step 6: show history on b-rune-2"
b_rows=$(( $(echo "${b_hist}" | wc -l | tr -d ' ') - 1 ))
replayed_rows=$(echo "${b_hist}" | tail -n +2 | grep -cE '(^|[[:space:]])a@[0-9]+([[:space:]]|$)' || true)
[ "${b_rows}" -ge 1 ] || fail "step 6: b's history is empty after the replay"
[ "${b_rows}" = "${src_rows}" ] || { echo "${b_hist}" >&2; fail "step 6: b has ${b_rows} history events, a has ${src_rows}"; }
[ "${replayed_rows}" = "${b_rows}" ] || { echo "${b_hist}" >&2; fail "step 6: only ${replayed_rows} of ${b_rows} events on b show 'replayedFrom a@<offset>'"; }
log "Step 6 OK: ${b_rows} events replayed, diff clean, every event carries replayedFrom a@<offset>"

echo "CATALOG-E2E: OK"
