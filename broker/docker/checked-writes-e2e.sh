#!/usr/bin/env bash
# docker/checked-writes-e2e.sh - Checked writes Part 05: write intents and the key index on 3 authenticated brokers.
#
#   1. SETUP:     build the broker, the rune CLI and the checked_writes_probe images; generate the cluster key; `up` 3 nodes
#                 with RUNE_VERDICT_INTERVAL_MS=20 (docker-compose.checked-writes.yml); bootstrap the first admin + the e2e
#                 user (docker/lib/auth.sh); the probe defines ki_account (4 partitions, replicas 3, cleanup.policy = compact,
#                 key.index on).  Bounded: a common consensus leader within 180 s;
#   2. RACE:      4 probe containers insert the same 20,000 ids concurrently.  At ~30 % `docker kill` of the leader of
#                 partition 0, restarted once progress resumes.  Check: for every id exactly one Ok across all workers and
#                 every other result for it is 56 (KeyExists); no VerdictUnknown, no Timeout.  Bounded at 600 s;
#   3. LIFECYCLE: for 2,000 ids update -> ok; update of an absent id -> 57; removeExisting -> ok, again -> 57; remove -> ok;
#                 re-insert -> ok (every 4th id is then removed again: the removed set of leg 5);
#   4. VERIFY:    a subscription from 0 on every partition sees exactly the accepted records, each with the op() of the write
#                 that produced it, and none of the 56 / 57 rejected ones;
#   5. RESTART:   `docker restart` of all three nodes; re-inserting every live id gives 56, every removed id gives ok; VERIFY
#                 again over the grown log (the key index was rebuilt from the log);
#   6. REFUSALS:  key.index on a transient topic, an untyped topic, a keyless topic and a string(9) key give InvalidRequest with
#                 the four exact messages;
#   7. print CHECKED-WRITES-E2E: OK.
#
# Usage (on the VM, under the host lock):
#   ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'
# Environment (all optional):
#   FLIGHT_RECORDER_DIR     the flight-recorder checkout (default: <monorepo>/../flight-recorder)
#   ARCANUM_METRICS_DIR     the arcanum-metrics checkout (default: <monorepo>/../arcanum-metrics)
#   KI_E2E_IDS              ids the racers insert (default 20000)       KI_E2E_WORKERS   racing containers (default 4)
#   KI_E2E_THREADS          writer threads per container (default 16)
#   E2E_LOG_DIR             when set, the brokers' and the probes' logs are kept there at teardown
#
# House style: bounded waits, trap teardown (`docker compose down -v` on every exit path), every failure names its step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
export FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
# arcanum-metrics (header-only) is its own repo too; compose additional_contexts read ARCANUM_METRICS_DIR.
export ARCANUM_METRICS_DIR="${ARCANUM_METRICS_DIR:-${PROJECTS}/../arcanum-metrics}"
CLIENT_REPO="${PROJECTS}/client-cpp"
COMPOSE_PROJECT="rune-checked-writes-e2e"
NETWORK="${COMPOSE_PROJECT}_default"
WORK_DIR="$(mktemp -d "${BROKER_REPO}/.checked-writes-e2e-work.XXXXXX")"
SHARED="${WORK_DIR}/shared"
mkdir -p "${SHARED}"

IDS="${KI_E2E_IDS:-20000}"
WORKERS="${KI_E2E_WORKERS:-4}"
THREADS="${KI_E2E_THREADS:-16}"
LIFECYCLE_IDS=2000     # the probe's KI_E2E_LIFECYCLE_IDS default; the restart leg's removed set is derived from it
PARTS=4
BROKER_PORT=40200
LEADER_WAIT_S=180      # a common consensus leader after `up`
RACE_S=600             # the race, from the first container start to the last container exit
RESUME_WAIT_S=180      # progress resumes after the kill
RESUME_WRITES=2000     # "resumed" = this many more completed writes since the kill
LINK_WAIT_S=120        # a restarted node serving again
LEG_S=600              # lifecycle / verify / restart / refusals

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"

log()  { echo "[checked-writes-e2e] $*" >&2; }
fail() { echo "[checked-writes-e2e] FAIL: $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.checked-writes-e2e-work*' -not -path './.auth.*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
rune_auth_init
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-checked-writes-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"
PROBE_IMAGE="rune-checked-writes-probe:$(repo_tag "${CLIENT_REPO}")"

dc() {
    docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" -f "${SCRIPT_DIR}/docker-compose.auth.yml" \
        -f "${SCRIPT_DIR}/docker-compose.checked-writes.yml" "$@"
}
cname() { dc ps -a -q "rune-$1" | head -1; }   # the container id of node <n> (1-3)
now_s() { date +%s; }

[ -d "${FLIGHT_RECORDER_DIR}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
[ -d "${ARCANUM_METRICS_DIR}" ] || fail "missing arcanum-metrics checkout (set ARCANUM_METRICS_DIR)"
for d in protocol codec crypto ql client-cpp conan/aeron; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done
[ -f "${CLIENT_REPO}/docker/Dockerfile.checked-writes-probe" ] || fail "missing client-cpp/docker/Dockerfile.checked-writes-probe"

PROBES=""       # names of the probe containers this run started (removed at teardown)
cluster_down() {
    local p
    for p in ${PROBES}; do docker rm -f "${p}" >/dev/null 2>&1 || true; done
    dc down -v --remove-orphans >/dev/null 2>&1 || true
}
teardown() {
    local rc=$?
    log "Tearing down (exit ${rc})..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then
        mkdir -p "${E2E_LOG_DIR}"
        for n in 1 2 3; do docker logs --timestamps "$(cname "${n}")" > "${E2E_LOG_DIR}/rune-${n}.log" 2>&1 || true; done
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.err "${E2E_LOG_DIR}/" 2>/dev/null || true
        cp -f "${SHARED}"/*.txt "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    cluster_down
    rm -rf "${WORK_DIR}" 2>/dev/null || true
    rune_auth_cleanup        # the cluster key and the generated passwords
    return "${rc}"
}
trap teardown EXIT

# wait_until <budget_s> <description> <command...> : polls the command (0 = done) every 1 s.
wait_until() {
    local budget="$1"; shift 2
    local deadline=$(( $(now_s) + budget ))
    while [ "$(now_s)" -lt "${deadline}" ]; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

# The consensus leader every node agrees on (node id 0-2, printed).
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

kill_node() { docker kill "$(cname $(( $1 + 1 )))" >/dev/null || fail "docker kill of rune-$(( $1 + 1 )) failed"; }
SINCE=""
links_since() {
    local n="$1" id c
    for id in 0 1 2; do
        [ "${id}" = "$(( n - 1 ))" ] && continue
        c=$(docker logs --since "${SINCE}" "$(cname "${n}")" 2>&1 | grep -c "peer link with node ${id} established" || true)
        [ "${c:-0}" -gt 0 ] || return 1
    done
}
restart_node() {
    local n=$(( $1 + 1 ))
    SINCE="$(date +%s)"   # epoch seconds: no timezone ambiguity
    docker start "$(cname "${n}")" >/dev/null || fail "docker start of rune-${n} failed"
    wait_until "${LINK_WAIT_S}" "rune-${n}'s peer links" links_since "${n}" \
        || { docker logs --tail 60 "$(cname "${n}")" >&2; fail "rune-${n} did not re-establish both peer links within ${LINK_WAIT_S}s"; }
}
# The consensus leader line since SINCE on every node, all naming one leader.
leader_since() {
    local ids="" n id
    for n in 1 2 3; do
        id=$(docker logs --since "${SINCE}" "$(cname "${n}")" 2>&1 | grep -oE 'node [0-9]+ is LEADER' | tail -1 | grep -oE '[0-9]+') || return 1
        [ -n "${id}" ] || return 1
        ids="${ids} ${id}"
    done
    [ "$(echo "${ids}" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')" = "1" ]
}
restart_all() {   # docker restart of all three nodes at once, then both peer links on each and one common leader
    SINCE="$(date +%s)"
    docker restart "$(cname 1)" "$(cname 2)" "$(cname 3)" >/dev/null || fail "docker restart of the three nodes failed"
    local n
    for n in 1 2 3; do
        wait_until "${LINK_WAIT_S}" "rune-${n}'s peer links" links_since "${n}" \
            || { docker logs --tail 60 "$(cname "${n}")" >&2; fail "rune-${n} did not re-establish both peer links within ${LINK_WAIT_S}s"; }
    done
    wait_until "${LEADER_WAIT_S}" "a common consensus leader" leader_since \
        || fail "no common consensus leader within ${LEADER_WAIT_S}s of the restart"
}

# ---- probe runs (checked_writes_probe in its own container on the compose network) -----------------------------------
SEED_NODE=1            # the broker the probes connect to (1-3); moved off the partition-0 leader after setup
# probe_args <container name> : the docker-run arguments (one per line).
probe_args() {
    local name="$1"
    printf -- '--name\n%s\n--hostname\n%s\n--network\n%s\n--platform\nlinux/amd64\n--shm-size=256m\n--security-opt\nseccomp=unconfined\n' "${name}" "${name}" "${NETWORK}"
    printf -- '-e\nRUNE_TARGET=aeron:udp?endpoint=rune-%s:%s\n-e\nRUNE_EGRESS_HOST=%s\n-e\nRUNE_EGRESS_PORT=40500\n' "${SEED_NODE}" "${BROKER_PORT}" "${name}"
    printf -- '-e\nRUNE_DATA_DIR=/tmp/fp\n-e\nRUNE_TIMEOUT_MS=30000\n-e\nKI_E2E_SHARED=/shared\n-e\nKI_E2E_THREADS=%s\n-v\n%s:/shared\n' "${THREADS}" "${SHARED}"
    printf -- '-e\nRUNE_USER=%s\n-e\nRUNE_PASSWORD_FILE=/work/e2e.pw\n-e\nRUNE_TLS_CA_FILE=/work/ca.pem\n-v\n%s:/work:ro\n' "${RUNE_AUTH_E2E_USER}" "${RUNE_AUTH_WORK_DIR}"
}
# probe_run <tag> <timeout_s> <mode> [flags...] : foreground; sets PROBE_RC; files ${WORK_DIR}/<tag>.out|.err.
PROBE_RC=0
probe_run() {
    local tag="$1" t="$2"; shift 2
    local name="rune-ki-${tag}"
    local -a envargs
    mapfile -t envargs < <(probe_args "${name}")
    PROBES="${PROBES} ${name}"
    docker rm -f "${name}" >/dev/null 2>&1 || true
    set +e
    timeout -k 10 "${t}" docker run --rm "${envargs[@]}" "${PROBE_IMAGE}" "$@" > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
    PROBE_RC=$?
    set -e
    docker rm -f "${name}" >/dev/null 2>&1 || true
}
# probe_start <tag> <timeout_s> <mode> [flags...] : background (the exit status lands in ${WORK_DIR}/<tag>.rc).
probe_start() {
    local tag="$1" t="$2"; shift 2
    local name="rune-ki-${tag}"
    local -a envargs
    mapfile -t envargs < <(probe_args "${name}")
    PROBES="${PROBES} ${name}"
    docker rm -f "${name}" >/dev/null 2>&1 || true
    rm -f "${WORK_DIR}/${tag}.rc" "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err"
    (
        set +e
        timeout -k 10 "${t}" docker run --rm "${envargs[@]}" "${PROBE_IMAGE}" "$@" > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
        echo $? > "${WORK_DIR}/${tag}.rc"
    ) &
}
bg_done()  { [ -s "${WORK_DIR}/$1.rc" ]; }
bg_rc()    { cat "${WORK_DIR}/$1.rc"; }
out_of()   { cat "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null; }
# out_has <tag> <pattern> : the leg's output holds the pattern (a file grep: `cat | grep -q` fails under pipefail on SIGPIPE)
out_has()  { grep -q -e "$2" "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null; }
# leg <step> <tag> <timeout_s> <ok marker> <mode> [flags...] : one foreground probe leg that must exit 0 and print its marker.
leg() {
    local step="$1" tag="$2" t="$3" marker="$4"; shift 4
    probe_run "${tag}" "${t}" "$@"
    [ "${PROBE_RC}" -eq 0 ] || { out_of "${tag}" | tail -30 >&2; fail "${step}: ${tag} exited ${PROBE_RC}"; }
    out_has "${tag}" "${marker}" || { out_of "${tag}" | tail -30 >&2; fail "${step}: no ${marker} from ${tag}"; }
}
race_progress() {
    local s=0 f v
    for f in "${SHARED}"/race-progress-w*; do
        [ -f "${f}" ] || continue
        v=$(cat "${f}" 2>/dev/null) || v=0
        s=$(( s + ${v:-0} ))
    done
    echo "${s}"
}
progress_at_least() { [ "$(race_progress)" -ge "$1" ]; }

# ---------------------------------------------------------------------------
# Leg 1: images, the key, the cluster, bootstrap, ki_account.
# ---------------------------------------------------------------------------
log "Leg 1a: building the broker image (tag ${RUNE_IMAGE_TAG})..."
set +e
out=$(dc build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "leg 1: broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "leg 1: broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Leg 1b: building the rune CLI image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "leg 1: could not build the rune CLI image"

log "Leg 1c: building the checked_writes_probe image (${PROBE_IMAGE})..."
set +e
out=$(docker build --platform linux/amd64 -f "${CLIENT_REPO}/docker/Dockerfile.checked-writes-probe" \
    --build-context rune-protocol="${PROJECTS}/protocol" --build-context rune-codec="${PROJECTS}/codec" \
    --build-context rune-crypto="${PROJECTS}/crypto" --build-context aeron-recipe="${PROJECTS}/conan/aeron" --build-context flight-recorder="${FLIGHT_RECORDER_DIR}" \
    --build-context rune-ql="${PROJECTS}/ql" -t "${PROBE_IMAGE}" "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "leg 1: checked_writes_probe image build failed (exit ${rc})"; }
docker image inspect "${PROBE_IMAGE}" >/dev/null 2>&1 || fail "leg 1: checked_writes_probe image not built: ${PROBE_IMAGE}"

log "Leg 1d: generating the cluster key..."
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "leg 1: could not generate the cluster key"

log "Leg 1e: the 3-node cluster up..."
dc up -d --no-build rune-1 rune-2 rune-3
log "Leg 1f: waiting for a common consensus leader (up to ${LEADER_WAIT_S}s)..."
wait_until "${LEADER_WAIT_S}" "a common consensus leader" cluster_has_leader \
    || { for n in 1 2 3; do docker logs --tail 40 "$(cname "${n}")" >&2 || true; done; fail "leg 1: no common consensus leader within ${LEADER_WAIT_S}s"; }
log "Leg 1g: bootstrapping the first admin + the e2e user..."
rune_auth_prepare "${NETWORK}" "rune-1:${BROKER_PORT}" || fail "leg 1: bootstrap / e2e user"

log "Leg 1h: SETUP: ki_account (${PARTS} partitions, replicas 3, compact, key.index)..."
leg "leg 1 (setup)" setup 300 "CHECKED-WRITES-SETUP: OK" setup
TOPIC_ID=$(grep -oE '^KI_TOPIC_ID=[0-9]+' "${WORK_DIR}/setup.out" | cut -d= -f2) || true
[ -n "${TOPIC_ID}" ] || fail "leg 1 (setup): setup printed no topic id"
all_leaders() { local p; for p in $(seq 0 $(( PARTS - 1 ))); do partition_leader "${TOPIC_ID}" "${p}" >/dev/null || return 1; done; }
wait_until 120 "all partition leaders" all_leaders || fail "leg 1 (setup): not every ki_account partition has a leader"
LEADER0=$(partition_leader "${TOPIC_ID}" 0) || fail "leg 1 (setup): no leader of partition 0"
if [ "${LEADER0}" = "0" ]; then SEED_NODE=2; fi   # the probes never connect through the node the race kills
log "Leg 1 OK: topic ${TOPIC_ID}; partition 0 led by node ${LEADER0}; probes connect through rune-${SEED_NODE}"

# ---------------------------------------------------------------------------
# Leg 2: RACE.
# ---------------------------------------------------------------------------
TOTAL_WRITES=$(( WORKERS * IDS ))
log "Leg 2: RACE: ${WORKERS} containers insert ids 1-${IDS} (${TOTAL_WRITES} writes, ${THREADS} threads each); the partition-0 leader is killed at ~30 %..."
RACE_START=$(now_s)
for i in $(seq 0 $(( WORKERS - 1 ))); do probe_start "race-${i}" "${RACE_S}" race --worker "${i}" --ids "1-${IDS}"; done
wait_until $(( RACE_S )) "30 % of the race" progress_at_least $(( TOTAL_WRITES * 30 / 100 )) \
    || { out_of race-0 | tail -20 >&2; fail "leg 2: the race never reached 30 % within ${RACE_S}s (at $(race_progress) of ${TOTAL_WRITES})"; }
LEADER0=$(partition_leader "${TOPIC_ID}" 0) || fail "leg 2: no leader of partition 0 at the kill"
BASE=$(race_progress)
log "Leg 2: killing node ${LEADER0}, the partition-0 leader, at ${BASE} of ${TOTAL_WRITES} writes"
kill_node "${LEADER0}"
RESUME_AT=$(( BASE + RESUME_WRITES ))
[ "${RESUME_AT}" -le "${TOTAL_WRITES}" ] || RESUME_AT="${TOTAL_WRITES}"
wait_until "${RESUME_WAIT_S}" "the race resuming" progress_at_least "${RESUME_AT}" \
    || { out_of race-0 | tail -20 >&2; fail "leg 2: no progress past ${RESUME_AT} within ${RESUME_WAIT_S}s of the kill (at $(race_progress))"; }
restart_node "${LEADER0}"
log "Leg 2: node ${LEADER0} restarted; waiting for the racers..."
for i in $(seq 0 $(( WORKERS - 1 ))); do
    left=$(( RACE_S - ($(now_s) - RACE_START) ))
    [ "${left}" -gt 0 ] || left=1
    wait_until "${left}" "racer ${i}" bg_done "race-${i}" || fail "leg 2: racer ${i} did not finish within ${RACE_S}s of the race start"
    [ "$(bg_rc "race-${i}")" = "0" ] || { out_of "race-${i}" | tail -20 >&2; fail "leg 2: racer ${i} exited $(bg_rc "race-${i}")"; }
done

# Every id: written once by every worker; exactly one Ok, every other result 56.
race_files=("${SHARED}"/race-w*.txt)
[ "${#race_files[@]}" -eq "${WORKERS}" ] || fail "leg 2: ${#race_files[@]} result files, want ${WORKERS}"
awk -v ids="${IDS}" -v workers="${WORKERS}" '
    { n[$1]++; if ($3 == 0) ok[$1]++; else if ($3 == 56) rej[$1]++; else { other[$3]++; if (!(first[$3])) first[$3] = $1 } }
    END {
        bad = 0
        for (c in other) { printf "result code %s x%d (first at id %s)\n", c, other[c], first[c]; bad = 1 }
        shown = 0
        for (id = 1; id <= ids; id++) {
            if (n[id] != workers || ok[id] != 1 || rej[id] != workers - 1) {
                if (shown++ < 10) printf "id %d: %d results, %d Ok, %d KeyExists\n", id, n[id], ok[id], rej[id]
                bad = 1
            }
        }
        exit bad
    }' "${race_files[@]}" > "${WORK_DIR}/race-check.txt" \
    || { cat "${WORK_DIR}/race-check.txt" >&2; fail "leg 2: the race broke 'exactly one Ok, every other 56' (codes 10014 = VerdictUnknown, 10002 = Timeout)"; }
log "Leg 2 OK: every id has exactly one Ok and ${WORKERS} - 1 KeyExists; $(( $(now_s) - RACE_START )) s"

# ---------------------------------------------------------------------------
# Leg 3: LIFECYCLE.
# ---------------------------------------------------------------------------
log "Leg 3: LIFECYCLE over ${LIFECYCLE_IDS} ids..."
leg "leg 3" lifecycle "${LEG_S}" "CHECKED-WRITES-LIFECYCLE: OK" lifecycle
log "Leg 3 OK"

# ---------------------------------------------------------------------------
# Leg 4: VERIFY.
# ---------------------------------------------------------------------------
log "Leg 4: VERIFY: a subscription from 0 on every partition against the accepted writes..."
leg "leg 4" verify "${LEG_S}" "CHECKED-WRITES-VERIFY: OK" verify
log "Leg 4 OK: $(grep -o 'CHECKED-WRITES-VERIFY: OK.*' "${WORK_DIR}/verify.out")"

# ---------------------------------------------------------------------------
# Leg 5: RESTART.
# ---------------------------------------------------------------------------
log "Leg 5: RESTART: docker restart of all three nodes..."
restart_all
log "Leg 5: re-inserting ids 1-${IDS}: a live id gives 56, a removed id (every 4th of 1-${LIFECYCLE_IDS}) gives ok..."
leg "leg 5" restart "${RACE_S}" "CHECKED-WRITES-RACE: DONE" race --worker 0 --ids "1-${IDS}" --tag restart
awk -v ids="${IDS}" -v life="${LIFECYCLE_IDS}" '
    { n++; seen[$1]++; want = ($1 <= life && $1 % 4 == 0) ? 0 : 56
      if ($3 != want) { if (shown++ < 10) printf "id %d: code %d, want %d\n", $1, $3, want; bad = 1 } }
    END { if (n != ids) { printf "%d results, want %d\n", n, ids; bad = 1 } exit bad }' "${SHARED}/restart-w0.txt" > "${WORK_DIR}/restart-check.txt" \
    || { cat "${WORK_DIR}/restart-check.txt" >&2; fail "leg 5: after the restart a live id did not give 56, or a removed id did not give ok"; }
log "Leg 5: VERIFY again over the grown log..."
leg "leg 5 (verify)" verify2 "${LEG_S}" "CHECKED-WRITES-VERIFY: OK" verify
log "Leg 5 OK"

# ---------------------------------------------------------------------------
# Leg 6: REFUSALS.
# ---------------------------------------------------------------------------
log "Leg 6: REFUSALS: key.index on a transient, an untyped, a keyless and a string(9)-keyed topic..."
leg "leg 6" refusals "${LEG_S}" "CHECKED-WRITES-REFUSALS: OK" refusals
log "Leg 6 OK"

echo "CHECKED-WRITES-E2E: OK"
