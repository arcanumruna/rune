#!/usr/bin/env bash
# docker/auth-e2e.sh — authentication Task 20: the 3-node docker e2e of spec §11 (steps 1-5) plus the cluster-key rotation.
#
#   1. build + up a 3-node authenticated cluster (docker-compose.yml + docker-compose.auth.yml); a consensus leader;
#   2. BOOTSTRAP: `rune security bootstrap` -> 0; a second run -> 1 "already bootstrapped"; ALTER USER admin SET PASSWORD
#      from a generated file -> 0; the one-time password again -> 1 AuthenticationFailed;
#   3. ROLE, USER, GRANTS: docker/topics/auth-e2e.rql as the admin; `show grants for user ws1;` lists the four grants
#      via role:world_server;
#   4. WORLD SERVER: the rune_produce_subscribe example as ws1 produces 10 000 records to world_zone1 through rune-3 and
#      consumes them in group world-1 (exit 0, every record); the same client writing player_events -> exit 1
#      NotAuthorized; a client with a wrong password -> exit 1 AuthenticationFailed;
#   5. REVOKE -> EXPLICIT FAILURE: a live consumer exits within 2 s printing "subscription ended: NotAuthorized" after
#      REVOKE READ; a live producer's next send fails NotAuthorized after REVOKE WRITE;
#   6. ROTATE THE CLUSTER KEY across a rolling restart (spec §8's three steps) while a ws1 producer / consumer pair runs:
#      every acked record is received;
#   7. print AUTH-E2E: OK; teardown.
#
# One cluster (compose project rune-auth-e2e) and ONE long-lived client container (the rune-ql-probe image: the `rune`
# CLI + the retained toolchain, in which rune_produce_subscribe is built once).  Every client command is a `docker exec`.
# Topic names are identifiers (letters, digits, underscore): the spec's "world." family is world_*; group names are free
# text, so the groups stay world-*.
#
# Usage:  bash docker/auth-e2e.sh   (sibling layout ../protocol ../codec ../crypto ../flight-recorder ../ql
#         ../client-cpp, as docker/catalog-e2e.sh)
#
# House style: bounded waits (never a fixed sleep as a gate), trap teardown, every failure names its step.
# Environment:
#   E2E_LOG_DIR   when set, the brokers' and the clients' logs are kept there at teardown
#   E2E_WORK_DIR  scratch dir shared with the client container as /work (default <repo>/.auth-e2e-work)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
# flight-recorder is its own repo, checked out next to the monorepo (compose additional_contexts read FLIGHT_RECORDER_DIR).
export FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
CLIENT_REPO="${PROJECTS}/client-cpp"
COMPOSE_PROJECT="rune-auth-e2e"
NETWORK="${COMPOSE_PROJECT}_default"
WORK_DIR="${E2E_WORK_DIR:-${BROKER_REPO}/.auth-e2e-work}"

LEADER_WAIT_S=180      # a common consensus leader after `up`
OP_TIMEOUT_S=120       # one rune CLI command (the CLI has its own 15 s DDL budget inside)
CLIENT_RUN_S=600       # the 10 000-record produce + consume client
READY_WAIT_S=180       # a background client connected (and its group assigned)
REVOKE_END_MS=2000     # a live consumer must end within this after the REVOKE returned
LINK_WAIT_S=60         # both peer links of a restarted node established
RESTART_WAIT_S=180     # a recreated node serving again
TOOLS="auth-e2e-tools"
BROKER_PORT=40200
EGRESS_BASE=40500
TOPIC_TABLE="world_zone1:0:1,player_events:1:1,world_rot:2:1"   # creation order in docker/topics/auth-e2e.rql
RECORDS=10000
ROT_INTERVAL_US=10000  # 10 ms between acked records of the rotation producer
ROT_ACKS="${AUTH_E2E_ROT_ACKS:-all}"   # all (the test); AUTH_E2E_ROT_ACKS=leader is a DIAGNOSTIC that shows why acked must mean durable

# The broker image is shared by the three nodes; ninja -j2 inside the broker build.
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export RUNE_SECRETS_DIR="${SCRIPT_DIR}/secrets"        # docker/secrets/cluster.key (the compose mount)
export RUNE_AUTH_WORK_DIR="${WORK_DIR}"                # the client container's /work is also where the passwords live
export RUNE_AUTH_TOOLS="${TOOLS}"
export RUNE_AUTH_NETWORK="${NETWORK}"

log()  { echo "[auth-e2e] $*" >&2; }
fail() { echo "[auth-e2e] FAIL: $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.auth-e2e-work/*' -not -path './docker/secrets/*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-auth-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"

dc() { docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" -f "${SCRIPT_DIR}/docker-compose.auth.yml" "$@"; }
tx()   { docker exec "${TOOLS}" "$@"; }
txt()  { local t="$1"; shift; docker exec "${TOOLS}" timeout "${t}" "$@"; }
# ql_as <user> <password-file> <args...> : `rune ql` as that principal (output and exit status are the CLI's).
ql_as() { local u="$1" p="$2"; shift 2; txt "${OP_TIMEOUT_S}" rune ql --user "${u}" --password-file "${p}" "$@"; }
admin_ql() { ql_as admin /work/admin.pw "$@"; }
now_ms() { echo $(( $(date +%s%N) / 1000000 )); }

[ -d "${FLIGHT_RECORDER_DIR}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
for d in protocol codec crypto ql client-cpp; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done
[ -f "${SCRIPT_DIR}/topics/auth-e2e.rql" ] || fail "missing docker/topics/auth-e2e.rql"

teardown() {
    local rc=$?
    log "Tearing down (exit ${rc})..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then
        mkdir -p "${E2E_LOG_DIR}"
        for n in 1 2 3; do docker logs --timestamps "$(cname "${n}")" > "${E2E_LOG_DIR}/rune-${n}.log" 2>&1 || true; done
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.err "${WORK_DIR}"/*.acked "${WORK_DIR}"/*.received "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
    dc down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "${WORK_DIR}" 2>/dev/null || true
    rune_auth_cleanup        # docker/secrets/cluster.key and cluster.next.key
    return "${rc}"
}
trap teardown EXIT

# wait_until <budget_s> <description> <command...> : polls the command (0 = done) every 1 s.
wait_until() {
    local budget="$1" what="$2"; shift 2
    local deadline=$(( $(date +%s) + budget ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

# The container id of node <n> (1-3) under the compose project (survives --force-recreate: looked up each time).
cname() { dc ps -a -q "rune-$1" | head -1; }

cluster_has_leader() {
    local ids="" n id
    for n in 1 2 3; do
        id=$(docker logs "$(cname "${n}")" 2>&1 | grep -oE 'node [0-9]+ is LEADER' | tail -1 | grep -oE '[0-9]+') || return 1
        [ -n "${id}" ] || return 1
        ids="${ids} ${id}"
    done
    [ "$(echo "${ids}" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')" = "1" ]
}

# ---- background / foreground world-server clients (rune_produce_subscribe, RUNE_MODE=ws) ---------------------------------
EGRESS_N=0
# ws_env <tag> <egress port> <seed node 1-3> <user> <password-file> [KEY=VALUE ...] : prints the `docker exec -e ...` arguments.
ws_env() {
    local tag="$1" port="$2" seed="$3" user="$4" pw="$5"; shift 5
    printf -- '-e\nRUNE_MODE=ws\n-e\nRUNE_USER=%s\n-e\nRUNE_PASSWORD_FILE=%s\n' "${user}" "${pw}"
    printf -- '-e\nRUNE_TARGET=aeron:udp?endpoint=rune-%s:%s\n-e\nRUNE_EGRESS=aeron:udp?endpoint=%s:%s\n' "${seed}" "${BROKER_PORT}" "${TOOLS}" "${port}"
    printf -- '-e\nRUNE_DATA_DIR=/tmp/ws-%s\n-e\nRUNE_CLIENT_TOPIC_TABLE=%s\n-e\nRUNE_TIMEOUT_MS=20000\n' "${tag}" "${TOPIC_TABLE}"
    local kv
    for kv in "$@"; do printf -- '-e\n%s\n' "${kv}"; done
}
# ws_run <tag> <seed> <user> <pw> [KEY=VALUE ...] : foreground; sets WS_RC and WS_OUT (stdout+stderr), files /work/<tag>.out|.err.
WS_RC=0; WS_OUT=""
ws_run() {
    local tag="$1"; shift
    local -a envargs
    EGRESS_N=$(( EGRESS_N + 1 ))   # (not inside the process substitution: it would not survive)
    mapfile -t envargs < <(ws_env "${tag}" $(( EGRESS_BASE + EGRESS_N )) "$@")
    set +e
    docker exec "${envargs[@]}" "${TOOLS}" timeout "${CLIENT_RUN_S}" /build/build/rune_produce_subscribe \
        > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
    WS_RC=$?
    set -e
    WS_OUT="$(cat "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err" 2>/dev/null)"
}
# ws_start <tag> <seed> <user> <pw> [KEY=VALUE ...] : background (the exit status lands in /work/<tag>.rc).
ws_start() {
    local tag="$1"; shift
    local -a envargs
    EGRESS_N=$(( EGRESS_N + 1 ))
    mapfile -t envargs < <(ws_env "${tag}" $(( EGRESS_BASE + EGRESS_N )) "$@")
    rm -f "${WORK_DIR}/${tag}.rc" "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err"
    docker exec -d "${envargs[@]}" "${TOOLS}" sh -c \
        "timeout ${CLIENT_RUN_S} /build/build/rune_produce_subscribe > /work/${tag}.out 2> /work/${tag}.err; echo \$? > /work/${tag}.rc"
}
bg_out()   { cat "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null || true; }
bg_ready() { grep -q "^WS: READY" "${WORK_DIR}/$1.out" 2>/dev/null; }
bg_done()  { [ -s "${WORK_DIR}/$1.rc" ]; }
bg_rc()    { cat "${WORK_DIR}/$1.rc"; }
bg_acks()  { grep -c "^CLIENT: OK produce ws=" "${WORK_DIR}/$1.out" 2>/dev/null || true; }

# ---------------------------------------------------------------------------
# Step 1: build the broker + CLI images; the cluster key; the cluster up; a client container.
# ---------------------------------------------------------------------------
mkdir -p "${WORK_DIR}"
rm -f "${RUNE_SECRETS_DIR}/cluster.key" "${RUNE_SECRETS_DIR}/cluster.next.key" "${RUNE_SECRETS_DIR}/tls.pem" \
      "${RUNE_SECRETS_DIR}/tls.key" "${RUNE_SECRETS_DIR}/ca.pem"
rune_auth_init   # RUNE_SECRETS_DIR (docker/secrets) is pre-set: the key lives where the brief says

log "Step 1a: building the broker image (tag ${RUNE_IMAGE_TAG}; the three nodes share it)..."
set +e
out=$(dc build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Step 1b: building the rune CLI image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"

log "Step 1c: rune security gen-cluster-key --out docker/secrets/cluster.key; the 3-node cluster up..."
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"
mode=$(stat -c %a "${RUNE_SECRETS_DIR}/cluster.key" 2>/dev/null || stat -f %Lp "${RUNE_SECRETS_DIR}/cluster.key")
[ "${mode}" = "600" ] || fail "cluster.key has mode ${mode} (want 600)"
dc up -d --no-build rune-1 rune-2 rune-3

log "Step 1d: the client container; building rune_produce_subscribe in it..."
docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
docker run -d --name "${TOOLS}" --hostname "${TOOLS}" --network "${NETWORK}" --platform linux/amd64 --shm-size=512m \
    --security-opt seccomp=unconfined \
    -v "${WORK_DIR}:/work" -v "${SCRIPT_DIR}/topics:/topics:ro" -v "${RUNE_SECRETS_DIR}:/run/secrets:ro" \
    -e RUNE_TLS_CA_FILE=/run/secrets/ca.pem \
    "${QL_IMAGE}" sleep infinity >/dev/null || fail "could not start the client container"
set +e
out=$(tx cmake --build /build/build --target rune_produce_subscribe -j2 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "building rune_produce_subscribe failed (exit ${rc})"; }
tx test -x /build/build/rune_produce_subscribe || fail "rune_produce_subscribe missing after build"

log "Step 1e: waiting for a common consensus leader (up to ${LEADER_WAIT_S}s)..."
wait_until "${LEADER_WAIT_S}" "a common consensus leader" cluster_has_leader \
    || { for n in 1 2 3; do docker logs --tail 40 "$(cname "${n}")" >&2 || true; done; fail "no common consensus leader within ${LEADER_WAIT_S}s"; }
log "Step 1 OK: a consensus leader is elected"

# ---------------------------------------------------------------------------
# Step 2: bootstrap.
# ---------------------------------------------------------------------------
log "Step 2: bootstrap..."
rune_auth_gen_password "${WORK_DIR}/otp" || fail "step 2: could not generate the one-time password"
rune_auth_gen_password "${WORK_DIR}/admin.pw" || fail "step 2: could not generate the admin password"

boot_out=""
boot_deadline=$(( $(date +%s) + 150 ))
while :; do
    set +e
    boot_out=$(txt "${OP_TIMEOUT_S}" rune security bootstrap --admin admin --password-file /work/otp \
                  --cluster-key-file /run/secrets/cluster.key --broker "rune-1:${BROKER_PORT}" 2>&1); rc=$?
    set -e
    [ "${rc}" -ne 0 ] || break
    if [ "$(date +%s)" -ge "${boot_deadline}" ] || ! echo "${boot_out}" | grep -qiE 'NotLeader|Redirect|timed out|connection refused|not connected|unreachable|not ready'; then
        echo "${boot_out}" >&2
        fail "step 2: the first bootstrap exited ${rc} (want 0)"
    fi
    log "step 2: the cluster is not ready for the bootstrap yet ($(echo "${boot_out}" | tail -1)); retrying"
    sleep 2
done
log "step 2: first bootstrap exit 0: $(echo "${boot_out}" | tail -1)"

set +e
boot2_out=$(txt "${OP_TIMEOUT_S}" rune security bootstrap --admin admin --password-file /work/otp \
               --cluster-key-file /run/secrets/cluster.key --broker "rune-1:${BROKER_PORT}" 2>&1); boot2_rc=$?
set -e
[ "${boot2_rc}" -eq 1 ] || { echo "${boot2_out}" >&2; fail "step 2: the second bootstrap exited ${boot2_rc} (want 1)"; }
echo "${boot2_out}" | grep -q "already bootstrapped" || { echo "${boot2_out}" >&2; fail "step 2: the second bootstrap's stderr lacks 'already bootstrapped'"; }
log "step 2: second bootstrap exit 1: $(echo "${boot2_out}" | tail -1)"

( umask 077; printf "ALTER USER admin SET PASSWORD '%s';\n" "$(cat "${WORK_DIR}/admin.pw")" > "${WORK_DIR}/alter-admin.rql" )
ql_as admin /work/otp -f /work/alter-admin.rql --broker "rune-1:${BROKER_PORT}" \
    || fail "step 2: ALTER USER admin SET PASSWORD (as the one-time-password login) did not exit 0"
rm -f "${WORK_DIR}/alter-admin.rql"

set +e
otp_out=$(ql_as admin /work/otp -e "show users;" --broker "rune-1:${BROKER_PORT}" 2>&1); otp_rc=$?
set -e
[ "${otp_rc}" -eq 1 ] || { echo "${otp_out}" >&2; fail "step 2: the one-time password still logs in (exit ${otp_rc}, want 1)"; }
echo "${otp_out}" | grep -q "AuthenticationFailed" || { echo "${otp_out}" >&2; fail "step 2: the one-time password was refused without 'AuthenticationFailed'"; }
admin_ql -e "show users;" --broker "rune-1:${BROKER_PORT}" >/dev/null || fail "step 2: the admin's new password does not log in"
log "Step 2 OK: bootstrap 0; again 1 'already bootstrapped'; ALTER USER 0; the one-time password -> 1 AuthenticationFailed"

# ---------------------------------------------------------------------------
# Step 3: topics, role, user, grants.
# ---------------------------------------------------------------------------
log "Step 3: role, user, grants..."
rune_auth_gen_password "${WORK_DIR}/ws1.pw" || fail "step 3: could not generate ws1's password"
( umask 077; sed "s/__WS1_PASSWORD__/$(cat "${WORK_DIR}/ws1.pw")/" "${SCRIPT_DIR}/topics/auth-e2e.rql" > "${WORK_DIR}/auth-e2e.rql" )
admin_ql -f /work/auth-e2e.rql --broker "rune-2:${BROKER_PORT}" || fail "step 3: auth-e2e.rql through rune-2 failed"
rm -f "${WORK_DIR}/auth-e2e.rql"
grants=$(admin_ql -e "show grants for user ws1;" --broker "rune-1:${BROKER_PORT}") || fail "step 3: show grants for user ws1"
echo "${grants}" >&2
[ "$(echo "${grants}" | grep -c "role:world_server")" = "4" ] || fail "step 3: 'show grants' lists $(echo "${grants}" | grep -c "role:world_server") role:world_server grants (want 4)"
echo "${grants}" | grep -iE "role:world_server +read +topic +'player_events'"        >/dev/null || fail "step 3: grant READ topic 'player_events' missing"
echo "${grants}" | grep -iE "role:world_server +write +topic +PREFIX 'world_'"       >/dev/null || fail "step 3: grant WRITE topic PREFIX 'world_' missing"
echo "${grants}" | grep -iE "role:world_server +read +topic +PREFIX 'world_'"        >/dev/null || fail "step 3: grant READ topic PREFIX 'world_' missing"
echo "${grants}" | grep -iE "role:world_server +read +group +PREFIX 'world-'"        >/dev/null || fail "step 3: grant READ group PREFIX 'world-' missing"
log "Step 3 OK: ws1 holds the four grants via role:world_server"

# ---------------------------------------------------------------------------
# Step 4: the world server.
# ---------------------------------------------------------------------------
log "Step 4a: ws1 produces ${RECORDS} records to world_zone1 through rune-3 and consumes them in group world-1..."
t0=$(now_ms)
ws_run ws-main 3 ws1 /work/ws1.pw RUNE_WS_ROLE=both RUNE_GROUP_ID=world-1 RUNE_PRODUCE_N="${RECORDS}" RUNE_QUIET=1 \
    RUNE_PRODUCE_TOPIC=world_zone1 RUNE_CONSUME_TOPIC=world_zone1
[ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -40 >&2; fail "step 4a: the world-server client exited ${WS_RC} (want 0)"; }
echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=${RECORDS} failed=0" || { echo "${WS_OUT}" | tail -20 >&2; fail "step 4a: not all ${RECORDS} records were acked"; }
echo "${WS_OUT}" | grep -q "WS-CONSUMER: distinct=${RECORDS} " || { echo "${WS_OUT}" | tail -20 >&2; fail "step 4a: not every record was consumed in group world-1"; }
log "Step 4a OK: ${RECORDS} produced and consumed in $(( $(now_ms) - t0 )) ms"

log "Step 4b: ws1 writes player_events (no WRITE grant) -> exit 1 NotAuthorized..."
ws_run ws-denied 3 ws1 /work/ws1.pw RUNE_WS_ROLE=produce RUNE_PRODUCE_TOPIC=player_events RUNE_PRODUCE_N=5
[ "${WS_RC}" -eq 1 ] || { echo "${WS_OUT}" | tail -20 >&2; fail "step 4b: writing player_events exited ${WS_RC} (want 1)"; }
echo "${WS_OUT}" | grep -q "NotAuthorized" || { echo "${WS_OUT}" | tail -20 >&2; fail "step 4b: the refusal does not name NotAuthorized"; }

log "Step 4c: a client with a wrong password -> exit 1 AuthenticationFailed..."
( umask 077; printf 'not-the-password-of-ws1-0000\n' > "${WORK_DIR}/wrong.pw" )
ws_run ws-wrongpw 3 ws1 /work/wrong.pw RUNE_WS_ROLE=produce RUNE_PRODUCE_TOPIC=world_zone1 RUNE_PRODUCE_N=5
[ "${WS_RC}" -eq 1 ] || { echo "${WS_OUT}" | tail -20 >&2; fail "step 4c: the wrong password exited ${WS_RC} (want 1)"; }
echo "${WS_OUT}" | grep -q "AuthenticationFailed" || { echo "${WS_OUT}" | tail -20 >&2; fail "step 4c: the refusal does not name AuthenticationFailed"; }
log "Step 4 OK: produce+consume 0; player_events 1 NotAuthorized; wrong password 1 AuthenticationFailed"

# ---------------------------------------------------------------------------
# Step 5: revoke -> explicit failure.
# ---------------------------------------------------------------------------
log "Step 5a: a live consumer; REVOKE READ -> it ends within ${REVOKE_END_MS} ms with 'subscription ended: NotAuthorized'..."
ws_start live-consumer 1 ws1 /work/ws1.pw RUNE_WS_ROLE=consume RUNE_GROUP_ID=world-live RUNE_CONSUME_TOPIC=world_zone1 \
    RUNE_STOP_FILE=/work/live-consumer.stop
wait_until "${READY_WAIT_S}" "the live consumer READY" bg_ready live-consumer \
    || { bg_out live-consumer >&2; fail "step 5a: the live consumer never became ready"; }
# (The group starts at the log start: it may still be draining the 10 000 records; either way its READ is revoked live.)
admin_ql -e "REVOKE READ ON TOPIC PREFIX 'world_' FROM ROLE world_server;" --broker "rune-1:${BROKER_PORT}" \
    || fail "step 5a: REVOKE READ failed"
t_revoked=$(now_ms)
deadline_ms=$(( t_revoked + REVOKE_END_MS ))
while ! bg_done live-consumer; do
    [ "$(now_ms)" -lt "${deadline_ms}" ] || { bg_out live-consumer | tail -20 >&2; fail "step 5a: the consumer did not end within ${REVOKE_END_MS} ms of the REVOKE (a hang or a silent stop)"; }
    sleep 0.05
done
ended_in=$(( $(now_ms) - t_revoked ))
[ "$(bg_rc live-consumer)" = "1" ] || { bg_out live-consumer | tail -20 >&2; fail "step 5a: the consumer exited $(bg_rc live-consumer) (want 1)"; }
bg_out live-consumer | grep -q "subscription ended: NotAuthorized" || { bg_out live-consumer | tail -20 >&2; fail "step 5a: the consumer did not print 'subscription ended: NotAuthorized'"; }
log "Step 5a OK: the consumer ended ${ended_in} ms after the REVOKE: subscription ended: NotAuthorized"

log "Step 5b: restore READ; a live producer; REVOKE WRITE -> its next send fails NotAuthorized..."
admin_ql -e "GRANT READ ON TOPIC PREFIX 'world_' TO ROLE world_server;" --broker "rune-1:${BROKER_PORT}" || fail "step 5b: re-GRANT READ failed"
ws_start live-producer 2 ws1 /work/ws1.pw RUNE_WS_ROLE=produce RUNE_PRODUCE_TOPIC=world_zone1 RUNE_PRODUCE_N=0 \
    RUNE_PRODUCE_INTERVAL_US=20000 RUNE_STOP_FILE=/work/live-producer.stop
wait_until "${READY_WAIT_S}" "the live producer READY" bg_ready live-producer \
    || { bg_out live-producer >&2; fail "step 5b: the live producer never became ready"; }
acks_ready() { [ "$(bg_acks live-producer)" -ge 5 ]; }
wait_until 60 "the live producer acking" acks_ready || { bg_out live-producer | tail -20 >&2; fail "step 5b: the live producer is not acking"; }
admin_ql -e "REVOKE WRITE ON TOPIC PREFIX 'world_' FROM ROLE world_server;" --broker "rune-1:${BROKER_PORT}" \
    || fail "step 5b: REVOKE WRITE failed"
t_revoked=$(now_ms)
deadline_ms=$(( t_revoked + 5000 ))
while ! bg_done live-producer; do
    [ "$(now_ms)" -lt "${deadline_ms}" ] || { bg_out live-producer | tail -20 >&2; fail "step 5b: the producer kept sending 5 s after REVOKE WRITE"; }
    sleep 0.05
done
[ "$(bg_rc live-producer)" = "1" ] || { bg_out live-producer | tail -20 >&2; fail "step 5b: the producer exited $(bg_rc live-producer) (want 1)"; }
bg_out live-producer | grep -q "produce refused: NotAuthorized" || { bg_out live-producer | tail -20 >&2; fail "step 5b: the producer did not report 'produce refused: NotAuthorized'"; }
log "Step 5b OK: the producer's send failed NotAuthorized $(( $(now_ms) - t_revoked )) ms after the REVOKE"
admin_ql -e "GRANT WRITE ON TOPIC PREFIX 'world_' TO ROLE world_server;" --broker "rune-1:${BROKER_PORT}" || fail "step 5: re-GRANT WRITE failed"
log "Step 5 OK (grants restored)"

# ---------------------------------------------------------------------------
# Step 6: rotate the cluster key across a rolling restart (spec §8).
# ---------------------------------------------------------------------------
log "Step 6: cluster-key rotation across a rolling restart, under a running ws1 producer / consumer pair..."
ws_start rot-consumer 1 ws1 /work/ws1.pw RUNE_WS_ROLE=consume RUNE_GROUP_ID=world-rot RUNE_CONSUME_TOPIC=world_rot \
    RUNE_QUIET=1 RUNE_EXPECT_FILE=/work/rot.acked RUNE_RECEIVED_FILE=/work/rot.received
ws_start rot-producer 2 ws1 /work/ws1.pw RUNE_WS_ROLE=produce RUNE_PRODUCE_TOPIC=world_rot RUNE_PRODUCE_N=0 \
    RUNE_PRODUCE_INTERVAL_US="${ROT_INTERVAL_US}" RUNE_STOP_FILE=/work/rot.stop RUNE_ACKED_FILE=/work/rot.acked \
    RUNE_ACKS="${ROT_ACKS}"
# "Acked" must mean durable across the restarts below: the producer must run with acks=all (acks=leader loses a record
# acked by a leader that is restarted before it replicated - the first version of this step lost 7 of 7229 that way).
wait_until "${READY_WAIT_S}" "the rotation producer to report its ack level" grep -q "^WS: producer acks=${ROT_ACKS}" "${WORK_DIR}/rot-producer.out" \
    || { bg_out rot-producer | head -5 >&2; fail "step 6: the rotation producer is not running with acks=${ROT_ACKS}"; }
wait_until "${READY_WAIT_S}" "the rotation consumer READY" bg_ready rot-consumer || { bg_out rot-consumer >&2; fail "step 6: the rotation consumer never became ready"; }
wait_until "${READY_WAIT_S}" "the rotation producer READY" bg_ready rot-producer || { bg_out rot-producer >&2; fail "step 6: the rotation producer never became ready"; }

rune_gen_cluster_key "${RUNE_SECRETS_DIR}" cluster.next.key || fail "step 6: could not generate cluster.next.key"

peer_links_up() {   # peer_links_up <n> : the restarted node n logs "peer link with node X established" for both other nodes
    local n="$1" id
    for id in 0 1 2; do
        [ "${id}" = "$(( n - 1 ))" ] && continue
        docker logs "$(cname "${n}")" 2>&1 | grep -q "peer link with node ${id} established" || return 1
    done
}
serving() { docker logs "$(cname "$1")" 2>&1 | grep -q "is LEADER"; }
# restart_node <n> <current key path> <next key path or ""> : recreate the node with that key pair, wait for both links.
restart_node() {
    local n="$1" cur="$2" next="$3"
    log "  restarting rune-${n}: current=${cur} next=${next:-<none>}"
    env "RUNE_KEY_CUR_${n}=${cur}" "RUNE_KEY_NEXT_${n}=${next}" \
        docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" -f "${SCRIPT_DIR}/docker-compose.auth.yml" \
        up -d --no-deps --no-build --force-recreate "rune-${n}" >/dev/null 2>&1 || fail "step 6: recreating rune-${n} failed"
    wait_until "${LINK_WAIT_S}" "rune-${n}'s peer links" peer_links_up "${n}" \
        || { docker logs --tail 60 "$(cname "${n}")" >&2; fail "step 6: rune-${n} did not log 'peer link with node N established' for both peers within ${LINK_WAIT_S}s"; }
}
OLD=/run/keys/cluster.key
NEW=/run/keys/cluster.next.key
# The producer must keep getting acks through every phase (the cluster serves throughout the roll).
ACKS_SEEN=$(bg_acks rot-producer)
phase_acks_grew() {   # phase_acks_grew <label> : at least one more ack since the last phase ended
    local cur; cur=$(bg_acks rot-producer)
    [ "${cur}" -gt "${ACKS_SEEN}" ] || fail "step 6: no record was acked during ${1} (${cur} acks in total)"
    log "  ${1}: $(( cur - ACKS_SEEN )) records acked during the phase"
    ACKS_SEEN="${cur}"
}
log "step 6.1: every node restarted with the NEW key mounted as _NEXT (accepts both)..."
for n in 1 2 3; do restart_node "${n}" "${OLD}" "${NEW}"; done
phase_acks_grew "step 6.1"
log "step 6.2: every node restarted with current = NEW, the old key as _NEXT..."
for n in 1 2 3; do restart_node "${n}" "${NEW}" "${OLD}"; done
phase_acks_grew "step 6.2"
log "step 6.3: every node restarted with the NEW key only..."
for n in 1 2 3; do restart_node "${n}" "${NEW}" ""; done
phase_acks_grew "step 6.3"
for n in 1 2 3; do
    docker exec "$(cname "${n}")" sh -c 'echo "$RUNE_CLUSTER_KEY_FILE|${RUNE_CLUSTER_KEY_FILE_NEXT:-}"' | grep -q "^${NEW}|$" \
        || fail "step 6: rune-${n} is not running on the new key only"
done

log "step 6: stopping the producer; the consumer must hold every acked record..."
touch "${WORK_DIR}/rot.stop"
wait_until 120 "the rotation producer to stop" bg_done rot-producer || { bg_out rot-producer | tail -20 >&2; fail "step 6: the producer did not stop"; }
[ "$(bg_rc rot-producer)" = "0" ] || { bg_out rot-producer | tail -30 >&2; fail "step 6: the producer exited $(bg_rc rot-producer) (want 0)"; }
wait_until 240 "the rotation consumer to hold every acked record" bg_done rot-consumer || { bg_out rot-consumer | tail -20 >&2; fail "step 6: the consumer did not finish"; }
[ "$(bg_rc rot-consumer)" = "0" ] || { bg_out rot-consumer | tail -30 >&2; fail "step 6: the consumer exited $(bg_rc rot-consumer) (want 0)"; }
acked=$(wc -l < "${WORK_DIR}/rot.acked" | tr -d ' ')
received=$(wc -l < "${WORK_DIR}/rot.received" | tr -d ' ')
[ "${acked}" -ge 50 ] || fail "step 6: only ${acked} records were acked across the rotation (the producer was not running through it)"
missing=$(comm -23 <(sort "${WORK_DIR}/rot.acked") <(sort "${WORK_DIR}/rot.received") | wc -l | tr -d ' ')
if [ "${missing}" != "0" ]; then
    log "step 6: acked but never received (record numbers, of ${acked} acked): $(comm -23 <(sort "${WORK_DIR}/rot.acked") <(sort "${WORK_DIR}/rot.received") | sort -n | head -40 | tr '\n' ' ')"
    fail "step 6: ${missing} acked record(s) were never received (acks=${ROT_ACKS})"
fi
log "Step 6 OK: ${acked} records acked across the rolling rotation, ${received} distinct received, none of the acked missing"

echo "AUTH-E2E: OK"
