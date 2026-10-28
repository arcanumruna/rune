#!/usr/bin/env bash
# docker/namespace-e2e.sh — namespaces Task 15: the 3-node docker e2e on the DEFAULT config (spec §8, §10).
#
#   1. build + up a 3-node authenticated cluster (docker-compose.yml + docker-compose.auth.yml); BOOTSTRAP the cluster admin;
#   2. `rune namespace create a` as the cluster admin;
#   3. the cluster admin creates user `a/admin` and grants it ADMIN ON NAMESPACE a (QL);
#   4. `a/admin` (session namespace a: bare names) creates user alice, topic t and READ + WRITE on t for alice;
#   5. alice (RUNE_USER=a/alice) produces N=1000 records to t and consumes them all (rune_produce_subscribe, ws mode,
#      a plain subscription; groups and transactions work in every namespace);
#   6. a `default` user (granted READ on every topic prefix "") cannot see `t`: `show topic t;` answers exactly like
#      `show topic <never created>;` (same exit status, same text modulo the name);
#   7. `rune namespace list` as a/admin shows exactly `a`;
#   8. the __catalog leader is killed; once a new one serves, steps 5-7 repeat (on a survivor seed);
#   9. the whole cluster is stopped and started again (volumes kept); steps 5-7 repeat;
#  10. `rune namespace limits a --set max-topics=1`: the read-back shows it AND a topic create over it is refused with
#      NamespaceLimitExceeded (52); the limit is then raised (Phase 2: limits are enforced);
#  11. `rune namespace suspend a --read-only`: alice's produce is refused, a plain consume is served; resume; produce served;
#  12. `rune namespace suspend a --reject-all`: a live session of a is closed, a login with the right password gets
#      NamespaceSuspended and with a wrong one AuthenticationFailed; resume; the login works again;
#  13. `produce-bytes-per-sec` over the limit: alice's client is told Throttled, backs off and every record still lands;
#  14. the whole cluster is restarted (volumes kept): the max-topics limit and a ReadOnly suspend still hold;
#  15. `rune namespace delete a --yes` with data of every kind that this script writes (topic records, a consumer group's
#      committed offsets, users); the erase is awaited through `rune namespace list`; `a` is created again and is EMPTY
#      (no topic, no user, no old id), and a/alice cannot log in;
#  16. the whole cluster is restarted once more: the recreated `a` is still empty and the deleted id is still gone.
#   then print NAMESPACE-E2E: OK and tear down.
#
# Not exercised here (covered by the in-process Phase 2 tests, which drive the same code on real brokers): views and
# transactions as delete data (NamespaceDeleteRegression, NamespaceReaperRegression, TxnAbortOnSuspendRegression) and the
# 336-cell lifecycle matrix.  The VM run of THIS script is pending (it needs the shared Linux test VM).
#
# Client: the rune_produce_subscribe example logs in as the qualified name (RUNE_USER=a/alice) and takes the topic by
# its client table entry t:0:1 (t is the first and only topic of the fresh cluster, so its id is 0).  Because the
# example numbers its records from 0 on every run, repeat runs re-send the same sequence numbers: the checks assert
# acked=N failed=0 and distinct>=N, i.e. every acked record of THIS run came back.
#
# One cluster (compose project rune-ns-e2e), ONE client container (ns-e2e-tools; the rune-ql-probe image).
# House style: bounded waits (never a fixed sleep as a gate), trap teardown that removes only this project's
# containers, network, volumes and the tools container, every failure names its step.  `df -h .` at start and end.
#
# Usage:  bash docker/namespace-e2e.sh   (sibling layout as docker/auth-e2e.sh; hold the shared VM lock)
# Environment:
#   E2E_LOG_DIR   when set, the brokers' and the clients' logs are kept there at teardown
#   E2E_WORK_DIR  scratch dir shared with the client container as /work (default <repo>/.ns-e2e-work)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
export FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
export ARCANUM_METRICS_DIR="${ARCANUM_METRICS_DIR:-${PROJECTS}/../arcanum-metrics}"
CLIENT_REPO="${PROJECTS}/client-cpp"
COMPOSE_PROJECT="rune-ns-e2e"
NETWORK="${COMPOSE_PROJECT}_default"
WORK_DIR="${E2E_WORK_DIR:-${BROKER_REPO}/.ns-e2e-work}"

LEADER_WAIT_S=180      # a common consensus leader after `up`
OP_TIMEOUT_S=120       # one rune CLI command
CLIENT_RUN_S=300       # one produce + consume client run
NEWLEADER_WAIT_S=120   # a new __catalog leader after the kill
SERVING_WAIT_S=240     # the admin's login answering after the full restart
TOOLS="ns-e2e-tools"
BROKER_PORT=40200      # inside the compose network; the project has its own network
EGRESS_BASE=40700
TOPIC_TABLE="t:0:1"
RECORDS=1000

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export RUNE_SECRETS_DIR="${SCRIPT_DIR}/secrets"
export RUNE_AUTH_WORK_DIR="${WORK_DIR}"
export RUNE_AUTH_TOOLS="${TOOLS}"
export RUNE_AUTH_NETWORK="${NETWORK}"

log()  { echo "[namespace-e2e] $*" >&2; }
fail() { echo "[namespace-e2e] FAIL: $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.ns-e2e-work/*' -not -path './docker/secrets/*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-ns-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"

dc() { docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" -f "${SCRIPT_DIR}/docker-compose.auth.yml" "$@"; }
tx()   { docker exec "${TOOLS}" "$@"; }
txt()  { local t="$1"; shift; docker exec "${TOOLS}" timeout "${t}" "$@"; }
# ql_as <user> <password-file> <args...> : `rune ql` as that principal.
ql_as() { local u="$1" p="$2"; shift 2; txt "${OP_TIMEOUT_S}" rune ql --user "${u}" --password-file "${p}" "$@"; }
# ns_as <user> <password-file> <args...> : `rune namespace ...` as that principal.
ns_as() { local u="$1" p="$2"; shift 2; txt "${OP_TIMEOUT_S}" rune namespace "$@" --user "${u}" --password-file "${p}"; }

[ -d "${FLIGHT_RECORDER_DIR}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
[ -d "${ARCANUM_METRICS_DIR}" ] || fail "missing arcanum-metrics checkout (set ARCANUM_METRICS_DIR)"
for d in protocol codec crypto ql client-cpp; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done

teardown() {
    local rc=$?
    log "Tearing down (exit ${rc})..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then
        mkdir -p "${E2E_LOG_DIR}"
        for n in 1 2 3; do docker logs --timestamps "$(cname "${n}")" > "${E2E_LOG_DIR}/rune-${n}.log" 2>&1 || true; done
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.err "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
    dc down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "${WORK_DIR}" 2>/dev/null || true
    rune_auth_cleanup
    log "disk after:"; df -h . >&2 || true
    return "${rc}"
}
trap teardown EXIT

wait_until() {
    local budget="$1" what="$2"; shift 2
    local deadline=$(( $(date +%s) + budget ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

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

# catalog_leader : prints "<node 1-based> <epoch>" of the highest-epoch `__catalog role -> LeaderServing` line.
catalog_leader() {
    local best_n="" best_e=-1 n e
    for n in 1 2 3; do
        e=$(docker logs "$(cname "${n}")" 2>&1 | grep -oE '__catalog role -> LeaderServing \(epoch [0-9]+\)' | tail -1 | grep -oE '[0-9]+\)' | tr -d ')') || true
        [ -n "${e}" ] || continue
        if [ "${e}" -gt "${best_e}" ]; then best_e="${e}"; best_n="${n}"; fi
    done
    [ -n "${best_n}" ] || return 1
    echo "${best_n} ${best_e}"
}

# ---- the alice client (rune_produce_subscribe, RUNE_MODE=ws, a plain subscription) --------------------------------------
EGRESS_N=0
WS_RC=0; WS_OUT=""
# ws_run_ex <tag> <seed node 1-3> <user> <password-file> <role> <records> [VAR=value ...] : foreground; sets WS_RC and
# WS_OUT.  role = both | produce | consume; extra VAR=value pairs are passed to the client's environment.
ws_run_ex() {
    local tag="$1" seed="$2" user="$3" pw="$4" role="$5" n="$6"; shift 6
    local extra=() kv
    for kv in "$@"; do extra+=(-e "${kv}"); done
    EGRESS_N=$(( EGRESS_N + 1 ))
    set +e
    docker exec -e RUNE_MODE=ws -e "RUNE_WS_ROLE=${role}" -e "RUNE_USER=${user}" -e "RUNE_PASSWORD_FILE=${pw}" \
        -e "RUNE_TARGET=aeron:udp?endpoint=rune-${seed}:${BROKER_PORT}" \
        -e "RUNE_EGRESS=aeron:udp?endpoint=${TOOLS}:$(( EGRESS_BASE + EGRESS_N ))" \
        -e "RUNE_DATA_DIR=/tmp/ws-${tag}" -e "RUNE_CLIENT_TOPIC_TABLE=${TOPIC_TABLE}" -e RUNE_TIMEOUT_MS=20000 \
        -e RUNE_PRODUCE_TOPIC=t -e RUNE_CONSUME_TOPIC=t -e "RUNE_PRODUCE_N=${n}" -e RUNE_QUIET=1 "${extra[@]}" \
        "${TOOLS}" timeout "${CLIENT_RUN_S}" /build/build/rune_produce_subscribe \
        > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
    WS_RC=$?
    set -e
    WS_OUT="$(cat "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err" 2>/dev/null)"
}
# ws_run <tag> <seed node 1-3> <user> <password-file> : the step-5 run (produce and consume ${RECORDS}).
ws_run() { ws_run_ex "$1" "$2" "$3" "$4" both "${RECORDS}"; }

# checks <label> <seed node> : steps 5, 6 and 7.
checks() {
    local label="$1" seed="$2" n
    log "step 5 [${label}]: alice produces and consumes ${RECORDS} records of t through rune-${seed}..."
    ws_run "alice-${label}" "${seed}" a/alice /work/alice.pw
    [ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -30 >&2; fail "step 5 [${label}]: alice's client exited ${WS_RC} (want 0)"; }
    echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=${RECORDS} failed=0" || { echo "${WS_OUT}" | tail -20 >&2; fail "step 5 [${label}]: not all ${RECORDS} records were acked"; }
    n=$(echo "${WS_OUT}" | grep -oE 'WS-CONSUMER: distinct=[0-9]+' | tail -1 | grep -oE '[0-9]+$') || true
    [ -n "${n}" ] && [ "${n}" -ge "${RECORDS}" ] || { echo "${WS_OUT}" | tail -20 >&2; fail "step 5 [${label}]: alice consumed distinct=${n:-none} (want >= ${RECORDS})"; }

    log "step 6 [${label}]: a default user cannot see t (same answer as a never-created topic)..."
    local rc_t rc_n out_t out_n
    set +e
    # GetTopicSchema by name: the lookup a hidden topic must answer exactly like a missing one (`u` never exists).
    out_t=$(txt "${OP_TIMEOUT_S}" rune schema get t --user du --password-file /work/du.pw --broker "rune-${seed}:${BROKER_PORT}" 2>&1); rc_t=$?
    out_n=$(txt "${OP_TIMEOUT_S}" rune schema get u --user du --password-file /work/du.pw --broker "rune-${seed}:${BROKER_PORT}" 2>&1); rc_n=$?
    set -e
    out_n=$(echo "${out_n}" | sed "s/'u'/'t'/g")
    [ "${rc_t}" -ne 0 ] || { echo "t: ${out_t}" >&2; fail "step 6 [${label}]: a default user resolved a/t as its own t"; }
    [ "${rc_t}" = "${rc_n}" ] || { echo "t: ${out_t}" >&2; echo "never: ${out_n}" >&2; fail "step 6 [${label}]: schema get t exited ${rc_t}, a never-created topic ${rc_n}"; }
    [ "${out_t}" = "${out_n}" ] || { echo "t: ${out_t}" >&2; echo "never: ${out_n}" >&2; fail "step 6 [${label}]: a default user's answer for t differs from a never-created topic's"; }

    log "step 7 [${label}]: rune namespace list as a/admin shows exactly a..."
    local listing names
    listing=$(ns_as a/admin /work/aadmin.pw list --broker "rune-${seed}:${BROKER_PORT}") || fail "step 7 [${label}]: rune namespace list as a/admin failed"
    names=$(echo "${listing}" | tail -n +2 | awk 'NF {print $2}' | tr '\n' ' ')
    [ "${names}" = "a " ] || { echo "${listing}" >&2; fail "step 7 [${label}]: a/admin lists '${names}' (want exactly 'a')"; }
    log "steps 5-7 [${label}] OK"
}

log "disk before:"; df -h . >&2 || true

# ---------------------------------------------------------------------------
# Step 1: images, key, cluster, client container, bootstrap.
# ---------------------------------------------------------------------------
mkdir -p "${WORK_DIR}"
rm -f "${RUNE_SECRETS_DIR}/cluster.key" "${RUNE_SECRETS_DIR}/cluster.next.key" "${RUNE_SECRETS_DIR}/tls.pem" \
      "${RUNE_SECRETS_DIR}/tls.key" "${RUNE_SECRETS_DIR}/ca.pem"
rune_auth_init

log "step 1: building the broker image (${RUNE_IMAGE_TAG})..."
set +e
out=$(dc build rune-1 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "step 1: broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "step 1: broker image not built"
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "step 1: could not build the rune CLI image"
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "step 1: could not generate the cluster key"
dc up -d --no-build rune-1 rune-2 rune-3

docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
docker run -d --name "${TOOLS}" --hostname "${TOOLS}" --network "${NETWORK}" --platform linux/amd64 --shm-size=512m \
    --security-opt seccomp=unconfined \
    -v "${WORK_DIR}:/work" -v "${RUNE_SECRETS_DIR}:/run/secrets:ro" -e RUNE_TLS_CA_FILE=/run/secrets/ca.pem \
    "${QL_IMAGE}" sleep infinity >/dev/null || fail "step 1: could not start the client container"
set +e
out=$(tx cmake --build /build/build --target rune_produce_subscribe -j2 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "step 1: building rune_produce_subscribe failed (exit ${rc})"; }
wait_until "${LEADER_WAIT_S}" "a common consensus leader" cluster_has_leader \
    || { for n in 1 2 3; do docker logs --tail 40 "$(cname "${n}")" >&2 || true; done; fail "step 1: no common consensus leader within ${LEADER_WAIT_S}s"; }

log "step 1: bootstrap..."
rune_auth_gen_password "${WORK_DIR}/otp" || fail "step 1: could not generate the one-time password"
rune_auth_gen_password "${WORK_DIR}/admin.pw" || fail "step 1: could not generate the admin password"
boot_deadline=$(( $(date +%s) + 150 ))
while :; do
    set +e
    boot_out=$(txt "${OP_TIMEOUT_S}" rune security bootstrap --admin admin --password-file /work/otp \
                  --cluster-key-file /run/secrets/cluster.key --broker "rune-1:${BROKER_PORT}" 2>&1); rc=$?
    set -e
    [ "${rc}" -ne 0 ] || break
    if [ "$(date +%s)" -ge "${boot_deadline}" ] || ! echo "${boot_out}" | grep -qiE 'NotLeader|Redirect|timed out|connection refused|not connected|unreachable|not ready'; then
        echo "${boot_out}" >&2
        fail "step 1: bootstrap exited ${rc} (want 0)"
    fi
    sleep 2
done
( umask 077; printf "ALTER USER admin SET PASSWORD '%s';\n" "$(cat "${WORK_DIR}/admin.pw")" > "${WORK_DIR}/alter-admin.rql" )
ql_as admin /work/otp -f /work/alter-admin.rql --broker "rune-1:${BROKER_PORT}" || fail "step 1: ALTER USER admin SET PASSWORD failed"
rm -f "${WORK_DIR}/alter-admin.rql"
ql_as admin /work/admin.pw -e "show users;" --broker "rune-1:${BROKER_PORT}" >/dev/null || fail "step 1: the admin's new password does not log in"
log "step 1 OK: bootstrapped"

# ---------------------------------------------------------------------------
# Step 2: the namespace.
# ---------------------------------------------------------------------------
log "step 2: rune namespace create a..."
ns_as admin /work/admin.pw create a --broker "rune-1:${BROKER_PORT}" || fail "step 2: rune namespace create a failed"

# ---------------------------------------------------------------------------
# Step 3: a/admin, Namespace Admin on a (and the default-namespace user of step 6, made here by the same admin).
# ---------------------------------------------------------------------------
log "step 3: the cluster admin creates a/admin with ADMIN ON NAMESPACE a (and the default user du)..."
for u in aadmin alice du; do rune_auth_gen_password "${WORK_DIR}/${u}.pw" || fail "step 3: could not generate ${u}'s password"; done
( umask 077
  {
      printf "CREATE USER 'a/admin' WITH PASSWORD '%s';\n" "$(cat "${WORK_DIR}/aadmin.pw")"
      printf "GRANT ADMIN ON NAMESPACE a TO USER 'a/admin';\n"
      printf "CREATE USER du WITH PASSWORD '%s';\n" "$(cat "${WORK_DIR}/du.pw")"
      printf "GRANT READ ON TOPIC PREFIX '' TO USER du;\n"
  } > "${WORK_DIR}/step3.rql" )
ql_as admin /work/admin.pw -f /work/step3.rql --broker "rune-2:${BROKER_PORT}" || fail "step 3: the cluster admin's user/grant script failed"
rm -f "${WORK_DIR}/step3.rql"

# ---------------------------------------------------------------------------
# Step 4: a/admin creates alice, topic t and the grants (bare names resolve in the session's namespace a).
# ---------------------------------------------------------------------------
log "step 4: a/admin creates alice, topic t and READ + WRITE on t..."
( umask 077
  {
      printf "CREATE USER alice WITH PASSWORD '%s';\n" "$(cat "${WORK_DIR}/alice.pw")"
      printf "topic t partitions 1 replicas 3;\n"
      printf "GRANT READ ON TOPIC t TO USER alice;\n"
      printf "GRANT WRITE ON TOPIC t TO USER alice;\n"
  } > "${WORK_DIR}/step4.rql" )
ql_as a/admin /work/aadmin.pw -f /work/step4.rql --broker "rune-3:${BROKER_PORT}" || fail "step 4: a/admin's script failed"
rm -f "${WORK_DIR}/step4.rql"

# ---------------------------------------------------------------------------
# Steps 5-7.
# ---------------------------------------------------------------------------
checks "initial" 1

# ---------------------------------------------------------------------------
# Step 8: kill the __catalog leader, wait for a new one, repeat 5-7 on a survivor.
# ---------------------------------------------------------------------------
log "step 8: finding the __catalog leader..."
wait_until 60 "a __catalog leader line" catalog_leader || fail "step 8: no '__catalog role -> LeaderServing' line within 60s"
read -r killed killed_epoch < <(catalog_leader)
log "step 8: killing the __catalog leader rune-${killed} (epoch ${killed_epoch})..."
docker kill "$(cname "${killed}")" >/dev/null || fail "step 8: docker kill rune-${killed}"
survivor=1; [ "${killed}" = "1" ] && survivor=2
new_leader_elected() {
    local n e best=""
    for n in 1 2 3; do
        [ "${n}" = "${killed}" ] && continue
        e=$(docker logs "$(cname "${n}")" 2>&1 | grep -oE '__catalog role -> LeaderServing \(epoch [0-9]+\)' | tail -1 | grep -oE '[0-9]+\)' | tr -d ')') || true
        [ -n "${e}" ] && [ "${e}" -gt "${killed_epoch}" ] && best="${n}"
    done
    [ -n "${best}" ]
}
wait_until "${NEWLEADER_WAIT_S}" "a new __catalog leader" new_leader_elected \
    || fail "step 8: no new __catalog leader (epoch > ${killed_epoch}) within ${NEWLEADER_WAIT_S}s"
checks "after-leader-kill" "${survivor}"

# ---------------------------------------------------------------------------
# restart_cluster <label> : stop and start all three nodes (volumes kept) and wait for the admin to be served again.
# ---------------------------------------------------------------------------
admin_answers() { ql_as admin /work/admin.pw -e "show users;" --broker "rune-1:${BROKER_PORT}"; }
restart_cluster() {
    local label="$1"
    log "${label}: stopping and starting all three nodes (volumes kept)..."
    dc stop >/dev/null 2>&1 || fail "${label}: dc stop failed"
    dc start >/dev/null 2>&1 || fail "${label}: dc start failed"
    wait_until "${SERVING_WAIT_S}" "the admin answering after the restart" admin_answers \
        || { for n in 1 2 3; do docker logs --tail 40 "$(cname "${n}")" >&2 || true; done; fail "${label}: the cluster does not serve within ${SERVING_WAIT_S}s of the restart"; }
}

# ns_row <name> : the `rune namespace list` row of <name>'s ACTIVE or suspended entry ("<nsId> <name> <state>"), or empty.
ns_row() {
    ns_as admin /work/admin.pw list --broker "rune-1:${BROKER_PORT}" | awk -v n="$1" 'NR > 1 && $2 == n && $3 != "deleting" {print $1, $2, $3}' | head -1
}
# ns_listed <nsId> : succeeds while the id is still listed (in any state, DELETING included).
ns_listed() {
    ns_as admin /work/admin.pw list --broker "rune-1:${BROKER_PORT}" | awk -v id="$1" 'NR > 1 && $1 == id {f = 1} END {exit f ? 0 : 1}'
}
# expect_refusal <label> <needle> <command...> : the command must FAIL and its output (stdout + stderr) must name <needle>.
expect_refusal() {
    local label="$1" needle="$2" out rc; shift 2
    set +e
    out=$("$@" 2>&1); rc=$?
    set -e
    [ "${rc}" -ne 0 ] || { echo "${out}" >&2; fail "${label}: succeeded (want a refusal naming ${needle})"; }
    echo "${out}" | grep -q "${needle}" || { echo "${out}" >&2; fail "${label}: refused, but not with ${needle}"; }
}
a_admin_topic() { ql_as a/admin /work/aadmin.pw -e "topic $1 partitions 1 replicas 3;" --broker "rune-3:${BROKER_PORT}"; }

# ---------------------------------------------------------------------------
# Step 9: stop and start the whole cluster (volumes kept), repeat 5-7.
# ---------------------------------------------------------------------------
restart_cluster "step 9"
checks "after-restart" 1

# ---------------------------------------------------------------------------
# Step 10: limits are ENFORCED.  max-topics=1 with t existing: a second topic is refused with NamespaceLimitExceeded (52).
# ---------------------------------------------------------------------------
log "step 10: rune namespace limits a --set max-topics=1: stored, read back, and enforced..."
ns_as admin /work/admin.pw limits a --set max-topics=1 --broker "rune-1:${BROKER_PORT}" || fail "step 10: limits --set failed"
lim=$(ns_as admin /work/admin.pw limits a --broker "rune-1:${BROKER_PORT}") || fail "step 10: limits read-back failed"
echo "${lim}" | grep -qE '^max-topics +1$' || { echo "${lim}" >&2; fail "step 10: the read-back does not show max-topics 1"; }
expect_refusal "step 10: a second topic over max-topics=1" "NamespaceLimitExceeded" a_admin_topic t2
usage=$(ns_as admin /work/admin.pw usage a --broker "rune-1:${BROKER_PORT}") || fail "step 10: rune namespace usage a failed"
echo "${usage}" | grep -qE '^max-topics +1 +1' || { echo "${usage}" >&2; fail "step 10: usage does not show max-topics value 1 with usage 1"; }
log "step 10 OK: max-topics=1 refused a second topic (usage 1 of 1); the limit stays for the restart check of step 14"

# ---------------------------------------------------------------------------
# Step 11: suspend a ReadOnly: produce refused, fetch served; resume: produce served.
# ---------------------------------------------------------------------------
log "step 11: rune namespace suspend a --read-only..."
ns_as admin /work/admin.pw suspend a --read-only --broker "rune-1:${BROKER_PORT}" || fail "step 11: suspend --read-only failed"
ws_run_ex "ro-produce" 2 a/alice /work/alice.pw produce 5
if [ "${WS_RC}" -eq 0 ] && echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=5 failed=0"; then
    echo "${WS_OUT}" | tail -20 >&2; fail "step 11: alice's produce was served in a ReadOnly namespace"
fi
# Not just "any failure": the client output must name the suspension (NamespaceSuspended, code 51).
echo "${WS_OUT}" | grep -qE 'NamespaceSuspended|errorCode=51|\(51\)' \
    || { echo "${WS_OUT}" | tail -20 >&2; fail "step 11: alice's refused produce does not report NamespaceSuspended (51)"; }
ws_run_ex "ro-consume" 2 a/alice /work/alice.pw consume 0 "RUNE_CONSUME_EXPECT=${RECORDS}"
[ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -20 >&2; fail "step 11: alice's fetch was not served in a ReadOnly namespace (exit ${WS_RC})"; }
ns_as admin /work/admin.pw resume a --broker "rune-1:${BROKER_PORT}" || fail "step 11: resume failed"
ws_run_ex "ro-resumed" 2 a/alice /work/alice.pw produce "${RECORDS}"
[ "${WS_RC}" -eq 0 ] && echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=${RECORDS} failed=0" \
    || { echo "${WS_OUT}" | tail -20 >&2; fail "step 11: produce is not served after resume"; }
log "step 11 OK"

# ---------------------------------------------------------------------------
# Step 12: suspend a RejectAll: a live session is closed, logins get NamespaceSuspended (right password) or
# AuthenticationFailed (wrong one); resume: the login works.
# ---------------------------------------------------------------------------
log "step 12: rune namespace suspend a --reject-all..."
( set +e   # the client must exit non-zero here (RejectAll ends its session): record that status, never abort on it
  docker exec -e RUNE_MODE=ws -e RUNE_WS_ROLE=produce -e RUNE_USER=a/alice -e RUNE_PASSWORD_FILE=/work/alice.pw \
      -e "RUNE_TARGET=aeron:udp?endpoint=rune-1:${BROKER_PORT}" -e "RUNE_EGRESS=aeron:udp?endpoint=${TOOLS}:$(( EGRESS_BASE + 90 ))" \
      -e RUNE_DATA_DIR=/tmp/ws-live -e "RUNE_CLIENT_TOPIC_TABLE=${TOPIC_TABLE}" -e RUNE_TIMEOUT_MS=20000 \
      -e RUNE_PRODUCE_TOPIC=t -e RUNE_PRODUCE_N=0 -e RUNE_PRODUCE_INTERVAL_US=20000 -e RUNE_STOP_FILE=/work/never-exists -e RUNE_QUIET=1 \
      "${TOOLS}" timeout 150 /build/build/rune_produce_subscribe > "${WORK_DIR}/live.out" 2> "${WORK_DIR}/live.err"; echo $? > "${WORK_DIR}/live.rc" ) &
live_pid=$!
wait_until 60 "alice's live producer running" grep -q 'WS: READY' "${WORK_DIR}/live.out" || fail "step 12: the live producer never became ready"
ns_as admin /work/admin.pw suspend a --reject-all --broker "rune-1:${BROKER_PORT}" || fail "step 12: suspend --reject-all failed"
live_ended() { [ -s "${WORK_DIR}/live.rc" ]; }
wait_until 90 "alice's live session to be closed" live_ended || {
    # Which phase hangs: the client's own lines (DONE = the produce loop ended, CLOSED = close() returned) and its threads.
    echo "--- live.out:" >&2; cat "${WORK_DIR}/live.out" >&2 || true
    echo "--- live.err (tail):" >&2; tail -20 "${WORK_DIR}/live.err" >&2 || true
    echo "--- client threads:" >&2
    docker exec "${TOOLS}" sh -c 'for p in $(pgrep -f rune_produce_subscribe); do for t in /proc/$p/task/*; do echo "$t $(cat $t/comm) $(cat $t/wchan) $(cut -d" " -f3 $t/stat)"; done; done' >&2 || true
    kill "${live_pid}" 2>/dev/null || true
    fail "step 12: the live session of a was not closed by RejectAll"
}
[ "$(cat "${WORK_DIR}/live.rc")" != "0" ] || fail "step 12: the live producer ended cleanly; RejectAll must end its session with an error"
wait "${live_pid}" 2>/dev/null || true
expect_refusal "step 12: alice's login (right password) to a RejectAll namespace" "NamespaceSuspended" \
    ql_as a/alice /work/alice.pw -e "show grants for user alice;" --broker "rune-2:${BROKER_PORT}"
printf 'not the password' > "${WORK_DIR}/wrong.pw"
expect_refusal "step 12: a wrong password to a RejectAll namespace" "AuthenticationFailed" \
    ql_as a/alice /work/wrong.pw -e "show grants for user alice;" --broker "rune-2:${BROKER_PORT}"
ns_as admin /work/admin.pw resume a --broker "rune-1:${BROKER_PORT}" || fail "step 12: resume failed"
ws_run_ex "ra-resumed" 2 a/alice /work/alice.pw produce 1
[ "${WS_RC}" -eq 0 ] && echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=1 failed=0" \
    || { echo "${WS_OUT}" | tail -20 >&2; fail "step 12: alice cannot log in and produce after resume (exit ${WS_RC})"; }
log "step 12 OK"

# ---------------------------------------------------------------------------
# Step 13: a produce rate over the limit is Throttled; the client backs off and every record lands.
# ---------------------------------------------------------------------------
log "step 13: produce-bytes-per-sec=20000: alice is throttled, backs off, and succeeds..."
ns_as admin /work/admin.pw limits a --set produce-bytes-per-sec=20000 --broker "rune-1:${BROKER_PORT}" || fail "step 13: limits --set failed"
# The control run: the same 3000 records with no rate limit (limit 0 = unlimited).  Neither the example client nor the broker
# exposes a Throttled counter to this script (the client backs off silently; the broker's count is a test seam only), so the
# throttle is proven by contrast with this control, not by a bare "took >= 2".
ns_as admin /work/admin.pw limits a --set produce-bytes-per-sec=0 --broker "rune-1:${BROKER_PORT}" || fail "step 13: clearing the rate failed"
t0=${SECONDS}
ws_run_ex "unthrottled" 1 a/alice /work/alice.pw produce 3000
base=$(( SECONDS - t0 ))
[ "${WS_RC}" -eq 0 ] && echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=3000 failed=0" \
    || { echo "${WS_OUT}" | tail -20 >&2; fail "step 13: the unthrottled control run did not get every record acked (exit ${WS_RC})"; }
ns_as admin /work/admin.pw limits a --set produce-bytes-per-sec=20000 --broker "rune-1:${BROKER_PORT}" || fail "step 13: limits --set failed"
t0=${SECONDS}
ws_run_ex "throttled" 1 a/alice /work/alice.pw produce 3000
took=$(( SECONDS - t0 ))
[ "${WS_RC}" -eq 0 ] && echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=3000 failed=0" \
    || { echo "${WS_OUT}" | tail -20 >&2; fail "step 13: the throttled client did not get every record acked (exit ${WS_RC})"; }
[ "${took}" -ge 2 ] && [ "${took}" -ge $(( base * 2 )) ] && [ "${took}" -gt "${base}" ] \
    || fail "step 13: 3000 records at 20000 B/s took ${took}s against ${base}s unthrottled; the throttle never made the client wait"
# A failed record would hide a refused (not retried) Throttled: the throttled run must also report no failure at all.
echo "${WS_OUT}" | grep -q "failed=0" || fail "step 13: the throttled run reports failed records"
ns_as admin /work/admin.pw limits a --set produce-bytes-per-sec=0 --broker "rune-1:${BROKER_PORT}" || fail "step 13: clearing the rate failed"
log "step 13 OK (${took}s throttled, ${base}s unthrottled)"

# ---------------------------------------------------------------------------
# Step 14: restart the cluster: the max-topics limit and a ReadOnly suspend survive.
# ---------------------------------------------------------------------------
log "step 14: suspend a read-only, restart every broker, the limit and the gate still hold..."
ns_as admin /work/admin.pw suspend a --read-only --broker "rune-1:${BROKER_PORT}" || fail "step 14: suspend --read-only failed"
restart_cluster "step 14"
row=$(ns_row a); echo "${row}" | grep -q 'suspended-read-only' || { echo "${row}" >&2; fail "step 14: after the restart a is not suspended-read-only"; }
ws_run_ex "ro-restart" 1 a/alice /work/alice.pw produce 5
if [ "${WS_RC}" -eq 0 ] && echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=5 failed=0"; then
    echo "${WS_OUT}" | tail -20 >&2; fail "step 14: a ReadOnly namespace served a produce after the restart"
fi
echo "${WS_OUT}" | grep -qE 'NamespaceSuspended|errorCode=51|\(51\)' \
    || { echo "${WS_OUT}" | tail -20 >&2; fail "step 14: the refused produce after the restart does not report NamespaceSuspended (51)"; }
ns_as admin /work/admin.pw resume a --broker "rune-1:${BROKER_PORT}" || fail "step 14: resume failed"
expect_refusal "step 14: a second topic over max-topics=1 after the restart" "NamespaceLimitExceeded" a_admin_topic t2
log "step 14 OK"

# ---------------------------------------------------------------------------
# Step 15: delete a (topic records, committed group offsets, users), wait for the erase, create it again: empty.
# ---------------------------------------------------------------------------
log "step 15: a consumer group commits offsets on t; then rune namespace delete a --yes..."
ws_run_ex "group" 1 a/alice /work/alice.pw consume 0 RUNE_GROUP_ID=g "RUNE_CONSUME_EXPECT=${RECORDS}"
[ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -20 >&2; fail "step 15: the group consumer failed (exit ${WS_RC})"; }
old_row=$(ns_row a); old_id=$(echo "${old_row}" | awk '{print $1}')
[ -n "${old_id}" ] || fail "step 15: no nsId for a"
ns_as admin /work/admin.pw delete a --yes --broker "rune-1:${BROKER_PORT}" || fail "step 15: delete a failed"
still_listed() { ns_listed "${old_id}"; }
gone() { ! still_listed; }
wait_until "${SERVING_WAIT_S}" "the deleted namespace ${old_id} to leave the listing" gone || fail "step 15: ns ${old_id} is still listed ${SERVING_WAIT_S}s after the delete"
# The data is gone from every node's volume: no ns-<old_id> directory (nor ns-<old_id>.deleting) is left.
ns_dir_gone() {
    local n out
    for n in 1 2 3; do
        out=$(docker exec "$(cname "${n}")" find /var/lib/rune -maxdepth 3 \( -name "ns-${old_id}" -o -name "ns-${old_id}.deleting" \) 2>&1) || return 1
        [ -z "${out}" ] || return 1
    done
}
wait_until "${SERVING_WAIT_S}" "ns-${old_id} to vanish from every node's data volume" ns_dir_gone \
    || { for n in 1 2 3; do docker exec "$(cname "${n}")" find /var/lib/rune -maxdepth 3 -name "ns-${old_id}*" >&2 || true; done; fail "step 15: ns-${old_id} is still on a node's data volume"; }
expect_refusal "step 15: a/alice logging in to the deleted namespace" "AuthenticationFailed" \
    ql_as a/alice /work/alice.pw -e "show grants for user alice;" --broker "rune-2:${BROKER_PORT}"
ns_as admin /work/admin.pw create a --broker "rune-1:${BROKER_PORT}" || fail "step 15: creating a again failed"
new_row=$(ns_row a); new_id=$(echo "${new_row}" | awk '{print $1}')
[ -n "${new_id}" ] && [ "${new_id}" != "${old_id}" ] || fail "step 15: the re-created a has id '${new_id}' (old ${old_id}): ids are never reused"
echo "${new_row}" | grep -q 'active' || fail "step 15: the re-created a is not active: ${new_row}"
expect_refusal "step 15: the old topic a/t in the re-created namespace" "UnknownTopic" \
    txt "${OP_TIMEOUT_S}" rune schema get a/t --user admin --password-file /work/admin.pw --broker "rune-1:${BROKER_PORT}"
expect_refusal "step 15: the old user a/alice in the re-created namespace" "AuthenticationFailed" \
    ql_as a/alice /work/alice.pw -e "show grants for user alice;" --broker "rune-3:${BROKER_PORT}"
lim2=$(ns_as admin /work/admin.pw limits a --broker "rune-1:${BROKER_PORT}") || fail "step 15: limits of the re-created a"
echo "${lim2}" | grep -qE '^max-topics +1$' && fail "step 15: the re-created a inherited max-topics=1 of the old namespace"
log "step 15 OK: ${old_id} erased, a re-created as ${new_id}, empty"

# ---------------------------------------------------------------------------
# Step 16: restart once more: the recreated a is still empty and the deleted id is still gone.
# ---------------------------------------------------------------------------
restart_cluster "step 16"
gone || fail "step 16: the deleted namespace ${old_id} is listed again after the restart"
row=$(ns_row a); [ "$(echo "${row}" | awk '{print $1}')" = "${new_id}" ] || { echo "${row}" >&2; fail "step 16: after the restart a is not ${new_id}"; }
expect_refusal "step 16: the old topic a/t after the restart" "UnknownTopic" \
    txt "${OP_TIMEOUT_S}" rune schema get a/t --user admin --password-file /work/admin.pw --broker "rune-2:${BROKER_PORT}"
expect_refusal "step 16: the old user a/alice after the restart" "AuthenticationFailed" \
    ql_as a/alice /work/alice.pw -e "show grants for user alice;" --broker "rune-3:${BROKER_PORT}"
log "step 16 OK"

echo "NAMESPACE-E2E: OK"
