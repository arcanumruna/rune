#!/usr/bin/env bash
# docker/eos-e2e.sh — Phase 2b F Task 15: the exactly-once pipeline on 3 authenticated brokers.
#
#   1. build the broker + CLI images, generate the cluster key, `up` 3 nodes, bootstrap the first admin + the e2e user
#      (docker/lib/auth.sh: the e2e role holds WRITE TRANSACTIONAL_ID PREFIX '', READ GROUP PREFIX '', topic
#      READ / WRITE / CREATE), create eos_in (plain) and eos_out (typed) with docker/topics/eos-e2e.rql, rf 3, 8 partitions;
#      build eos_probe in the client container against the header `rune schema generate eos_out` writes;
#   2. FILL:     a plain acked producer writes N records {id, v} to eos_in;
#   3. PIPELINE: a group consumer + a transactional producer (transactionalId eos-worker-1) copy eos_in to eos_out as
#                v' = 2 * v in transactions of <= 500 records, the produce path rotating per transaction (send / batch /
#                tryClaim / claimBatch / typed claim), every 7th transaction aborted on purpose, rewound with
#                seekToCommitted and redone;
#      ~30 %:    the transaction coordinator (the consensus leader) is `docker kill`ed; it restarts once the pipeline
#                has made progress again without it;
#      ~60 %:    an eos_out partition leader is `docker kill`ed; restarted after progress resumes;
#      ~80 %:    ZOMBIE: a second pipeline with the same transactionalId starts: the first reports Fenced (exit 4) and the
#                second finishes (exit 0, within 600 s of the first start);
#   4. VERIFY:   a read_committed reader of eos_out prints `EOS-VERIFY: OK count=<N>` (every id exactly once, v' = 2 * v);
#   5. the cluster is torn down, then dr-e2e's test_tx_fencing.py runs (it brings up its own cluster);
#   6. print EOS-E2E: OK.
#
# One cluster (compose project rune-eos-e2e) and ONE long-lived client container (the rune-ql-probe image: the `rune`
# CLI + the retained toolchain, in which eos_probe is built once).  Every client command is a `docker exec`.
#
# Usage:  bash broker/docker/eos-e2e.sh      (from any directory; the monorepo is found from this file's location)
# Environment (all optional):
#   FLIGHT_RECORDER_DIR   the flight-recorder checkout (default: <monorepo>/../flight-recorder)
#   EOS_E2E_N             records (default 200000)
#   E2E_LOG_DIR           when set, the brokers' and the probes' logs are kept there at teardown
#   E2E_WORK_DIR          scratch dir shared with the client container as /work (default: a mktemp dir under <broker>, removed at teardown; yours is kept)
#   EOS_E2E_PAUSE_MS      pause after every pipeline commit (default 100: keeps the run long enough to inject the faults)
#   EOS_E2E_PYTHON        python3 for the dr-e2e step (default python3; a venv with requirements.txt is created when pytest is missing)
#
# House style: bounded waits (never a fixed sleep as a gate), trap teardown, every failure names its step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
# flight-recorder is its own repo, checked out next to the monorepo (compose additional_contexts read FLIGHT_RECORDER_DIR).
export FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
CLIENT_REPO="${PROJECTS}/client-cpp"
COMPOSE_PROJECT="rune-eos-e2e"
NETWORK="${COMPOSE_PROJECT}_default"
# A work dir this script made (mktemp) is removed at teardown; a caller-supplied E2E_WORK_DIR never is.
if [ -n "${E2E_WORK_DIR:-}" ]; then WORK_DIR="${E2E_WORK_DIR}"; OWN_WORK=""
else WORK_DIR="$(mktemp -d "${BROKER_REPO}/.eos-e2e-work.XXXXXX")"; OWN_WORK=1; fi

N="${EOS_E2E_N:-200000}"
PAUSE_MS="${EOS_E2E_PAUSE_MS:-100}"
LEADER_WAIT_S=180      # a common consensus leader after `up`
OP_TIMEOUT_S=150       # one rune CLI command
FILL_S=600             # the fill client
PIPELINE_S=600         # the pipeline must exit 0 within this, from its first start
PROGRESS_WAIT_S=300    # a progress threshold reached
RESUME_WAIT_S=180      # progress resumes after a kill
RESUME_RECORDS=2500    # "resumed" = this many more committed input records since the kill
LINK_WAIT_S=120        # a restarted node serving again
FENCE_WAIT_S=120       # the first pipeline reports Fenced after the second started
VERIFY_S=600           # the verify client
TOOLS="eos-e2e-tools"
BROKER_PORT=40200
EGRESS_BASE=40500
TOPIC_TABLE="eos_in:0:8,eos_out:1:8"   # creation order in docker/topics/eos-e2e.rql

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export RUNE_AUTH_WORK_DIR="${WORK_DIR}"   # the client container's /work is also where the passwords live
export RUNE_AUTH_TOOLS="${TOOLS}"         # the CLI runs inside the long-lived client container

log()  { echo "[eos-e2e] $*" >&2; }
fail() { echo "[eos-e2e] FAIL: $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.eos-e2e-work*' -not -path './.auth.*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
mkdir -p "${WORK_DIR}"
rune_auth_init   # RUNE_AUTH_DIR (mode 700) holds the cluster key; RUNE_SECRETS_DIR is exported for the compose files
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-eos-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"

dc() { docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" -f "${SCRIPT_DIR}/docker-compose.auth.yml" "$@"; }
tx()   { docker exec "${TOOLS}" "$@"; }
txt()  { local t="$1"; shift; docker exec "${TOOLS}" timeout "${t}" "$@"; }
now_s() { date +%s; }

[ -d "${FLIGHT_RECORDER_DIR}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
for d in protocol codec crypto ql client-cpp; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done
[ -f "${SCRIPT_DIR}/topics/eos-e2e.rql" ] || fail "missing docker/topics/eos-e2e.rql"
[ -f "${BROKER_REPO}/dr-e2e/tests/test_tx_fencing.py" ] || fail "missing dr-e2e/tests/test_tx_fencing.py"

cname() { dc ps -a -q "rune-$1" | head -1; }   # the container id of node <n> (1-3)

cluster_down() {
    docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
    dc down -v --remove-orphans >/dev/null 2>&1 || true
}
teardown() {
    local rc=$?
    log "Tearing down (exit ${rc})..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then
        mkdir -p "${E2E_LOG_DIR}"
        for n in 1 2 3; do docker logs --timestamps "$(cname "${n}")" > "${E2E_LOG_DIR}/rune-${n}.log" 2>&1 || true; done
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.err "${WORK_DIR}"/*.progress "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    cluster_down
    if [ -n "${OWN_WORK}" ]; then rm -rf "${WORK_DIR}" 2>/dev/null || true; fi
    rune_auth_cleanup        # the cluster key and the generated passwords
    return "${rc}"
}
trap teardown EXIT

# wait_until <budget_s> <description> <command...> : polls the command (0 = done) every 1 s.
wait_until() {
    local budget="$1" what="$2"; shift 2
    local deadline=$(( $(now_s) + budget ))
    while [ "$(now_s)" -lt "${deadline}" ]; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

# The consensus leader every node agrees on (node id 0-2, printed); the transaction coordinator is the leader.
consensus_leader() {
    local ids="" n id
    for n in 1 2 3; do
        id=$(docker logs "$(cname "${n}")" 2>&1 | grep -oE 'node [0-9]+ is LEADER' | tail -1 | grep -oE '[0-9]+') || return 1
        [ -n "${id}" ] || return 1
        ids="${ids} ${id}"
    done
    [ "$(echo "${ids}" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')" = "1" ] || return 1
    echo "${ids}" | awk '{print $1}'
}
cluster_has_leader() { consensus_leader >/dev/null; }

# The leader node (0-2) of <topic id> partition <p> in the brokers' logs: the becomeLeader line with the highest epoch.
partition_leader() {
    local topic="$1" part="$2" n best_e=-1 best_n="" line e node
    for n in 1 2 3; do
        line=$(docker logs "$(cname "${n}")" 2>&1 | grep -E "PartitionReplica\[node=[0-9]+\] becomeLeader topic=${topic} part=${part} epoch=[0-9]+" | tail -1) || true
        [ -n "${line}" ] || continue
        e=$(echo "${line}" | sed -E 's/.*epoch=([0-9]+).*/\1/')
        node=$(echo "${line}" | sed -E 's/.*node=([0-9]+)\].*/\1/')
        if [ "${e}" -ge "${best_e}" ]; then best_e="${e}"; best_n="${node}"; fi
    done
    [ -n "${best_n}" ] || return 1
    echo "${best_n}"
}

# kill_node <node id 0-2> / restart_node <node id 0-2>: docker kill, and docker start + both peer links back (logs since the start).
kill_node() { docker kill "$(cname $(( $1 + 1 )))" >/dev/null || fail "docker kill of rune-$(( $1 + 1 )) failed"; }
SINCE=""
links_since() {
    local n="$1" id
    for id in 0 1 2; do
        [ "${id}" = "$(( n - 1 ))" ] && continue
        docker logs --since "${SINCE}" "$(cname "${n}")" 2>&1 | grep -q "peer link with node ${id} established" || return 1
    done
}
restart_node() {
    local n=$(( $1 + 1 ))
    SINCE="$(date +%s)"   # epoch seconds: no timezone ambiguity
    docker start "$(cname "${n}")" >/dev/null || fail "docker start of rune-${n} failed"
    wait_until "${LINK_WAIT_S}" "rune-${n}'s peer links" links_since "${n}" \
        || { docker logs --tail 60 "$(cname "${n}")" >&2; fail "rune-${n} did not re-establish both peer links within ${LINK_WAIT_S}s"; }
}

# ---- probe runs (eos_probe in the client container) --------------------------------------------------------------------
EGRESS_N=0
# probe_env <tag> <egress port> <seed node 1-3> [KEY=VALUE ...] : the `docker exec -e ...` arguments (one per line).
probe_env() {
    local tag="$1" port="$2" seed="$3"; shift 3
    printf -- '-e\nRUNE_TARGET=aeron:udp?endpoint=rune-%s:%s\n-e\nRUNE_EGRESS=aeron:udp?endpoint=%s:%s\n' "${seed}" "${BROKER_PORT}" "${TOOLS}" "${port}"
    printf -- '-e\nRUNE_DATA_DIR=/tmp/eos-%s\n-e\nRUNE_CLIENT_TOPIC_TABLE=%s\n-e\nRUNE_TIMEOUT_MS=20000\n-e\nRUNE_EOS_N=%s\n' "${tag}" "${TOPIC_TABLE}" "${N}"
    printf -- '-e\nRUNE_COUNTS_FILE=/work/eos-counts\n'
    local kv
    for kv in "$@"; do printf -- '-e\n%s\n' "${kv}"; done
}
# probe_run <tag> <mode> <timeout_s> <seed> [KEY=VALUE ...] : foreground; sets PROBE_RC and PROBE_OUT; files /work/<tag>.out|.err.
PROBE_RC=0; PROBE_OUT=""
probe_run() {
    local tag="$1" mode="$2" t="$3" seed="$4"; shift 4
    local -a envargs
    EGRESS_N=$(( EGRESS_N + 1 ))   # (not inside the process substitution: it would not survive)
    mapfile -t envargs < <(probe_env "${tag}" $(( EGRESS_BASE + EGRESS_N )) "${seed}" "RUNE_MODE=${mode}" "$@")
    set +e
    docker exec "${envargs[@]}" "${TOOLS}" timeout "${t}" /build/build/eos_probe > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
    PROBE_RC=$?
    set -e
    PROBE_OUT="$(cat "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err" 2>/dev/null)"
}
# probe_start <tag> <mode> <timeout_s> <seed> [KEY=VALUE ...] : background (the exit status lands in /work/<tag>.rc).
probe_start() {
    local tag="$1" mode="$2" t="$3" seed="$4"; shift 4
    local -a envargs
    EGRESS_N=$(( EGRESS_N + 1 ))
    mapfile -t envargs < <(probe_env "${tag}" $(( EGRESS_BASE + EGRESS_N )) "${seed}" "RUNE_MODE=${mode}" "$@")
    rm -f "${WORK_DIR}/${tag}.rc" "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err" "${WORK_DIR}/${tag}.progress"
    docker exec -d "${envargs[@]}" "${TOOLS}" sh -c \
        "timeout ${t} /build/build/eos_probe > /work/${tag}.out 2> /work/${tag}.err; echo \$? > /work/${tag}.rc"
}
bg_out()   { cat "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null || true; }
bg_ready() { grep -q "^EOS-PIPELINE: READY" "${WORK_DIR}/$1.out" 2>/dev/null; }
bg_done()  { [ -s "${WORK_DIR}/$1.rc" ]; }
bg_rc()    { cat "${WORK_DIR}/$1.rc"; }
progress() { local p; p=$(cat "${WORK_DIR}/$1.progress" 2>/dev/null) || p=0; echo "${p:-0}"; }

# wait_progress <tag> <records> <description> : the pipeline's committed input records reach <records> (it must still run).
wait_progress() {
    local tag="$1" want="$2" what="$3" deadline=$(( $(now_s) + PROGRESS_WAIT_S ))
    while [ "$(progress "${tag}")" -lt "${want}" ]; do
        if bg_done "${tag}"; then bg_out "${tag}" | tail -20 >&2; fail "${what}: ${tag} exited $(bg_rc "${tag}") at $(progress "${tag}") of ${N} records, before ${want}"; fi
        [ "$(now_s)" -lt "${deadline}" ] || { bg_out "${tag}" | tail -20 >&2; fail "${what}: ${tag} stuck at $(progress "${tag}") records (want ${want}) for ${PROGRESS_WAIT_S}s"; }
        sleep 1
    done
}
# wait_resume <tag> <records at the kill> <description> : progress grew by RESUME_RECORDS since the kill (the new leader serves).
wait_resume() {
    local tag="$1" base="$2" what="$3" deadline=$(( $(now_s) + RESUME_WAIT_S ))
    while [ "$(progress "${tag}")" -lt $(( base + RESUME_RECORDS )) ]; do
        if bg_done "${tag}"; then
            [ "$(bg_rc "${tag}")" = "0" ] && return 0     # finished while the node was down: nothing left to resume
            bg_out "${tag}" | tail -20 >&2; fail "${what}: ${tag} exited $(bg_rc "${tag}")"
        fi
        [ "$(now_s)" -lt "${deadline}" ] || { bg_out "${tag}" | tail -20 >&2; fail "${what}: no progress for ${RESUME_WAIT_S}s after the kill (stuck at $(progress "${tag}"), killed at ${base})"; }
        sleep 1
    done
}

# ---------------------------------------------------------------------------
# Step 1: images, key, cluster, client container, bootstrap, topics, the probe.
# ---------------------------------------------------------------------------
log "Step 1a: building the broker image (tag ${RUNE_IMAGE_TAG})..."
set +e
out=$(dc build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Step 1b: building the rune CLI image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"

log "Step 1c: generating the cluster key; the 3-node cluster up..."
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"
dc up -d --no-build rune-1 rune-2 rune-3

log "Step 1d: the client container..."
docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
docker run -d --name "${TOOLS}" --hostname "${TOOLS}" --network "${NETWORK}" --platform linux/amd64 --shm-size=512m \
    --security-opt seccomp=unconfined \
    -v "${WORK_DIR}:/work" -v "${SCRIPT_DIR}/topics:/topics:ro" -v "${RUNE_SECRETS_DIR}:/run/secrets:ro" \
    -e RUNE_USER="${RUNE_AUTH_E2E_USER}" -e RUNE_PASSWORD_FILE=/work/e2e.pw \
    "${QL_IMAGE}" sleep infinity >/dev/null || fail "could not start the client container"

log "Step 1e: waiting for a common consensus leader (up to ${LEADER_WAIT_S}s)..."
wait_until "${LEADER_WAIT_S}" "a common consensus leader" cluster_has_leader \
    || { for n in 1 2 3; do docker logs --tail 40 "$(cname "${n}")" >&2 || true; done; fail "no common consensus leader within ${LEADER_WAIT_S}s"; }
log "Step 1f: bootstrapping the first admin + the e2e user..."
rune_auth_prepare "${NETWORK}" "rune-1:${BROKER_PORT}" || fail "bootstrap / e2e user"

log "Step 1g: topics eos_in (plain) and eos_out (typed), 8 partitions, rf 3..."
ddl_ok=0
for attempt in 1 2 3 4 5; do
    if txt "${OP_TIMEOUT_S}" rune ql -f /topics/eos-e2e.rql --broker "rune-2:${BROKER_PORT}"; then ddl_ok=1; break; fi
    log "eos-e2e.rql attempt ${attempt} failed; retrying"; sleep 3
done
[ "${ddl_ok}" = "1" ] || fail "step 1g: eos-e2e.rql through rune-2 failed"

log "Step 1h: building eos_probe against the header 'rune schema generate eos_out' writes..."
tx mkdir -p /tmp/eos-gen
txt "${OP_TIMEOUT_S}" rune schema generate eos_out --out /tmp/eos-gen --broker "aeron:udp?endpoint=rune-1:${BROKER_PORT}" \
    --cli-egress "aeron:udp?endpoint=${TOOLS}:${EGRESS_BASE}" --cli-data-dir /tmp/cli_eos_gen || fail "step 1h: rune schema generate eos_out"
tx test -s /tmp/eos-gen/eos_out.gen.h || fail "step 1h: /tmp/eos-gen/eos_out.gen.h was not written"
set +e
out=$(tx sh -c 'cmake -S /build -B /build/build -DEOS_GEN_DIR=/tmp/eos-gen >/dev/null && cmake --build /build/build --target eos_probe -j2' 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "step 1h: building eos_probe failed (exit ${rc})"; }
tx env RUNE_MODE=selfcheck /build/build/eos_probe | grep -q "EOS-SELFCHECK: OK" || fail "step 1h: eos_probe selfcheck"
log "Step 1 OK: cluster up and bootstrapped, topics created, eos_probe built"

# ---------------------------------------------------------------------------
# Step 2: fill.
# ---------------------------------------------------------------------------
log "Step 2: a plain acked producer writes ${N} records to eos_in..."
probe_run fill fill "${FILL_S}" 3
[ "${PROBE_RC}" -eq 0 ] || { echo "${PROBE_OUT}" | tail -20 >&2; fail "step 2: fill exited ${PROBE_RC}"; }
echo "${PROBE_OUT}" | grep -q "EOS-FILL: OK count=${N}" || { echo "${PROBE_OUT}" | tail -20 >&2; fail "step 2: fill did not ack ${N} records"; }
[ -s "${WORK_DIR}/eos-counts" ] || fail "step 2: no per-partition end offsets were written"
log "Step 2 OK: $(grep -c . "${WORK_DIR}/eos-counts") partitions filled"

# ---------------------------------------------------------------------------
# Step 3: the pipeline, with the faults.
# ---------------------------------------------------------------------------
PIPE_START=$(now_s)
pipe_env=(RUNE_GROUP_ID=eos-1 RUNE_TXN_ID=eos-worker-1 "RUNE_TXN_PAUSE_MS=${PAUSE_MS}" "RUNE_PIPELINE_TIMEOUT_S=${PIPELINE_S}")
log "Step 3: starting the pipeline (transactionalId eos-worker-1, group eos-1)..."
probe_start pipe-a pipeline "${PIPELINE_S}" 1 "${pipe_env[@]}" RUNE_PROGRESS_FILE=/work/pipe-a.progress
wait_until "${PROGRESS_WAIT_S}" "the pipeline READY" bg_ready pipe-a || { bg_out pipe-a | tail -20 >&2; fail "step 3: the pipeline never became ready"; }

log "Step 3a: ~30 % - killing the transaction coordinator (the consensus leader)..."
wait_progress pipe-a $(( N * 30 / 100 )) "step 3a"
coord=$(consensus_leader) || fail "step 3a: no common consensus leader to kill"
at=$(progress pipe-a)
log "  coordinator = node ${coord} (rune-$(( coord + 1 ))) at ${at} committed records; docker kill"
kill_node "${coord}"
wait_resume pipe-a "${at}" "step 3a"
log "  progress resumed without it ($(progress pipe-a) records); restarting rune-$(( coord + 1 ))"
restart_node "${coord}"

log "Step 3b: ~60 % - killing an eos_out partition leader..."
wait_progress pipe-a $(( N * 60 / 100 )) "step 3b"
pl=$(partition_leader 1 0) || fail "step 3b: no 'becomeLeader topic=1 part=0' line in the broker logs"
at=$(progress pipe-a)
log "  eos_out partition 0 leader = node ${pl} (rune-$(( pl + 1 ))) at ${at} committed records; docker kill"
kill_node "${pl}"
wait_resume pipe-a "${at}" "step 3b"
log "  progress resumed without it ($(progress pipe-a) records); restarting rune-$(( pl + 1 ))"
restart_node "${pl}"

log "Step 3c: ~80 % - the zombie: a second pipeline with the same transactionalId..."
wait_progress pipe-a $(( N * 80 / 100 )) "step 3c"
probe_start pipe-b pipeline $(( PIPELINE_S - ($(now_s) - PIPE_START) )) 2 "${pipe_env[@]}" RUNE_PROGRESS_FILE=/work/pipe-b.progress
wait_until "${PROGRESS_WAIT_S}" "the second pipeline READY" bg_ready pipe-b || { bg_out pipe-b | tail -20 >&2; fail "step 3c: the second pipeline never became ready"; }
wait_until "${FENCE_WAIT_S}" "the first pipeline to report Fenced" bg_done pipe-a \
    || { bg_out pipe-a | tail -20 >&2; fail "step 3c: the first pipeline did not stop within ${FENCE_WAIT_S}s of the second one's start"; }
[ "$(bg_rc pipe-a)" = "4" ] || { bg_out pipe-a | tail -20 >&2; fail "step 3c: the first pipeline exited $(bg_rc pipe-a) (want 4 = Fenced; 0 means it finished before the zombie started: raise EOS_E2E_N or EOS_E2E_PAUSE_MS)"; }
bg_out pipe-a | grep -q "EOS-PIPELINE: Fenced" || { bg_out pipe-a | tail -20 >&2; fail "step 3c: the first pipeline did not report Fenced"; }
log "Step 3c OK: the first pipeline reported Fenced"

left=$(( PIPELINE_S - ($(now_s) - PIPE_START) ))
wait_until "${left}" "the second pipeline to finish" bg_done pipe-b || { bg_out pipe-b | tail -20 >&2; fail "step 3: the pipeline did not finish within ${PIPELINE_S}s of its first start (second one at $(progress pipe-b))"; }
[ "$(bg_rc pipe-b)" = "0" ] || { bg_out pipe-b | tail -30 >&2; fail "step 3: the second pipeline exited $(bg_rc pipe-b) (want 0)"; }
bg_out pipe-b | grep -q "EOS-PIPELINE: DONE" || { bg_out pipe-b | tail -20 >&2; fail "step 3: the second pipeline did not report DONE"; }
log "Step 3 OK: finished in $(( $(now_s) - PIPE_START )) s: $(bg_out pipe-b | grep 'EOS-PIPELINE: DONE')"
for t in pipe-a pipe-b; do bg_out "${t}" | grep "aborted (" | sed "s/^/[eos-e2e] ${t}: /" | sort | uniq -c | sort -rn | head -5 >&2 || true; done

# ---------------------------------------------------------------------------
# Step 4: verify.
# ---------------------------------------------------------------------------
log "Step 4: a read_committed reader of eos_out..."
probe_run verify verify "${VERIFY_S}" 3
[ "${PROBE_RC}" -eq 0 ] || { echo "${PROBE_OUT}" | tail -20 >&2; fail "step 4: verify exited ${PROBE_RC}"; }
echo "${PROBE_OUT}" | grep -q "EOS-VERIFY: OK count=${N}" || { echo "${PROBE_OUT}" | tail -20 >&2; fail "step 4: verify did not print 'EOS-VERIFY: OK count=${N}'"; }
echo "${PROBE_OUT}" | grep "EOS-VERIFY: uncommitted=" >&2 || fail "step 4: verify did not report the read_uncommitted count"
log "Step 4 OK: ${N} records, each id once, v' = 2 * v, the zombie fenced"

# ---------------------------------------------------------------------------
# Step 5: dr-e2e's test_tx_fencing.py (its own cluster: ours goes down first).
# ---------------------------------------------------------------------------
log "Step 5: tearing the cluster down; dr-e2e test_tx_fencing.py..."
cluster_down
PY="${EOS_E2E_PYTHON:-python3}"
if ! "${PY}" -c 'import pytest' >/dev/null 2>&1; then
    log "  pytest is missing: creating a venv from dr-e2e/requirements.txt"
    VENV="${WORK_DIR}/venv"
    "${PY}" -m venv "${VENV}" || fail "step 5: could not create a venv"
    "${VENV}/bin/pip" install -q -r "${BROKER_REPO}/dr-e2e/requirements.txt" || fail "step 5: pip install -r dr-e2e/requirements.txt"
    PY="${VENV}/bin/python"
fi
# The harness picks its own image tags (broker HEAD) and credentials: none of this script's are passed down.
( cd "${BROKER_REPO}/dr-e2e" && env -u RUNE_IMAGE_TAG -u QL_IMAGE -u RUNE_SECRETS_DIR -u RUNE_AUTH_TOOLS -u RUNE_AUTH_WORK_DIR -u RUNE_AUTH_DIR \
      DR_E2E_BUILD=1 "${PY}" -m pytest tests/test_tx_fencing.py -v ) \
    || fail "step 5: dr-e2e tests/test_tx_fencing.py failed"
log "Step 5 OK: test_tx_fencing.py passed"

echo "EOS-E2E: OK"
