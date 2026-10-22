#!/usr/bin/env bash
# docker/filter-e2e.sh — Phase 2c Task 17: broker-side subscription filters on 3 authenticated brokers.
#
#   1. build the broker, the rune CLI and the filter_probe images; generate the cluster key; `up` 3 nodes with
#      RUNE_FILTER_MAX_SUBS_PER_PARTITION=256 and RUNE_FILTER_NOTE_INTERVAL_MS=20 (docker-compose.filter.yml);
#      bootstrap the first admin + the e2e user (docker/lib/auth.sh);
#   2. SETUP:    filter_probe defines player_event (8 partitions, rf 3, typed) and untyped_t;
#   3. CONSUME + PRODUCE: five subscriptions ((a) AllPartitions worldId == 7 read_committed, (b) AllPartitions
#                worldId == 8 and channel in {1, 2}, (c) Key(k) + playerId == k, (d) group worlds-9 with two members,
#                (e) an unfiltered Partition(0) control) are checked for their exact sets against the model while a
#                producer (one committed and one aborted transaction, then N records) runs;
#      ~30 %:    `docker kill` of the leader of partition 3; restarted once progress resumes;
#      ~60 %:    `docker restart` of a node that is neither the seed nor the original partition-3 leader (after the kill it may lead
#                partition 3 now: extra stress);
#      the consumer must exit 0 within 600 s;
#   4. ERRORS:   the refusals (untyped topic, stale descriptor, the subscription cap, a second group filter);
#   5. ALTER:    ADD guild (v2), DROP channel (v3) under a live subscription; the broker log names the absent field once
#                per partition;
#   6. print FILTER-E2E: OK.
#
# With --scenario the script runs only the MMORPG scenario (one box: 200 world servers on 8 containers, a producer at
# FILTER_E2E_RATE), ABAB x 3, filtered then unfiltered, and checks
#     every world's batch bytes <= 1.2 x the ideal bytes (n x a one-record batch), and
#     the filtered p50 / p99 (median over the runs) are no worse than unfiltered beyond the run-to-run band,
# and prints FILTER-SCENARIO: OK (or FAIL <why>).  The absolute docker p50 / p99 are printed as information only.
# The scenario needs the clean benchmark box (broker/bench/vm-bench-prepare.sh, restored in the trap) and so the host
# lock: run it as   flock -w 7200 "${RUNE_VM_LOCK_FILE:-/root/vm.lock}" bash broker/docker/filter-e2e.sh --scenario
#
# Usage:  bash broker/docker/filter-e2e.sh [--scenario]      (the monorepo is found from this file's location)
# Environment (all optional):
#   FLIGHT_RECORDER_DIR     the flight-recorder checkout (default: <monorepo>/../flight-recorder)
#   FILTER_E2E_N            records (default 40000)          FILTER_E2E_RATE   records/s (default 1000, see the comment at RATE)
#   FILTER_E2E_SECONDS      scenario: seconds one run measures (default 30)
#   FILTER_E2E_WORLDS       scenario: world servers (default 200 = 8 containers x 25)
#   FILTER_E2E_RUNS         scenario: ABAB pairs (default 3)
#   E2E_LOG_DIR             when set, the brokers' and the probes' logs are kept there at teardown
#
# House style: bounded waits, trap teardown, every failure names its step.

set -euo pipefail

SCENARIO=0
for a in "$@"; do
    case "${a}" in
        --scenario) SCENARIO=1 ;;
        *) echo "usage: $0 [--scenario]" >&2; exit 2 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
export FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
CLIENT_REPO="${PROJECTS}/client-cpp"
COMPOSE_PROJECT="rune-filter-e2e"
NETWORK="${COMPOSE_PROJECT}_default"
WORK_DIR="$(mktemp -d "${BROKER_REPO}/.filter-e2e-work.XXXXXX")"
SHARED="${WORK_DIR}/shared"
mkdir -p "${SHARED}"

N="${FILTER_E2E_N:-40000}"
RATE="${FILTER_E2E_RATE:-1000}"   # scenario: the highest rate this 4 vCPU box sustained UNFILTERED (200 world servers each receiving every record): 1000/s kept up (99.5 % delivered); 2500/s did not (74 %); the spec's 1M/s is far beyond it
SECONDS_PER_RUN="${FILTER_E2E_SECONDS:-30}"
WORLDS="${FILTER_E2E_WORLDS:-200}"
WORLDS_PER_PROC=25
IDLE_US="${FILTER_E2E_IDLE_US:-20}"   # scenario clients sleep this long when idle instead of spinning (see filter_probe.cpp)
SEED=1
CAP=256
BROKER_PORT=40200
LEADER_WAIT_S=180      # a common consensus leader after `up`
PROGRESS_WAIT_S=300    # a progress threshold reached
RESUME_WAIT_S=180      # progress resumes after a kill
RESUME_RECORDS=2000    # "resumed" = this many more acked records since the kill
LINK_WAIT_S=120        # a restarted node serving again
CONSUME_S=600          # consume must exit 0 within this, from its start
LEG_S=600              # errors / alter

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export RUNE_FILTER_MAX_SUBS_PER_PARTITION="${CAP}"
export RUNE_FILTER_NOTE_INTERVAL_MS=20

log()  { echo "[filter-e2e] $*" >&2; }
fail() { echo "[filter-e2e] FAIL: $*" >&2; if [ "${SCENARIO}" = "1" ]; then echo "FILTER-SCENARIO: FAIL $*"; fi; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.filter-e2e-work*' -not -path './.auth.*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
rune_auth_init
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-filter-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"
PROBE_IMAGE="rune-filter-probe:$(repo_tag "${CLIENT_REPO}")"

dc() {
    docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" -f "${SCRIPT_DIR}/docker-compose.auth.yml" \
        -f "${SCRIPT_DIR}/docker-compose.filter.yml" "$@"
}
cname() { dc ps -a -q "rune-$1" | head -1; }   # the container id of node <n> (1-3)
now_s() { date +%s; }

[ -d "${FLIGHT_RECORDER_DIR}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
for d in protocol codec crypto ql client-cpp; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done
[ -f "${CLIENT_REPO}/docker/Dockerfile.filter-probe" ] || fail "missing client-cpp/docker/Dockerfile.filter-probe"

PROBES=""       # names of the probe containers this run started (removed at teardown)
BENCH_PREPARED=""
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
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.err "${WORK_DIR}"/*.txt "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    cluster_down
    rm -rf "${WORK_DIR}" 2>/dev/null || true
    rune_auth_cleanup        # the cluster key and the generated passwords
    if [ -n "${BENCH_PREPARED}" ]; then
        log "restoring the benchmark box..."
        RUNE_VM_LOCK_HELD=1 bash "${BROKER_REPO}/bench/vm-bench-restore.sh" || log "WARNING: vm-bench-restore.sh failed"
        cluster_down   # the network and volumes again: a removal on the prepared box may have been refused
    fi
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
    local n="$1" id
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
bounce_node() {   # docker restart of a running node, then both peer links back
    local n=$(( $1 + 1 ))
    SINCE="$(date +%s)"
    docker restart "$(cname "${n}")" >/dev/null || fail "docker restart of rune-${n} failed"
    wait_until "${LINK_WAIT_S}" "rune-${n}'s peer links" links_since "${n}" \
        || { docker logs --tail 60 "$(cname "${n}")" >&2; fail "rune-${n} did not re-establish both peer links within ${LINK_WAIT_S}s"; }
}

# ---- probe runs (filter_probe in its own container on the compose network) -------------------------------------------
SEED_NODE=1            # the broker the probes connect to (1-3); chosen after setup so a killed node is never the seed
TOPIC_TABLE=""
# probe_args <container name> <mode> [KEY=VALUE ...] : the docker-run arguments (one per line).
probe_args() {
    local name="$1" mode="$2"; shift 2
    printf -- '--name\n%s\n--hostname\n%s\n--network\n%s\n--platform\nlinux/amd64\n--shm-size=256m\n--security-opt\nseccomp=unconfined\n' "${name}" "${name}" "${NETWORK}"
    printf -- '-e\nRUNE_MODE=%s\n-e\nRUNE_TARGET=aeron:udp?endpoint=rune-%s:%s\n-e\nRUNE_EGRESS_HOST=%s\n-e\nRUNE_EGRESS_PORT=40500\n' "${mode}" "${SEED_NODE}" "${BROKER_PORT}" "${name}"
    printf -- '-e\nRUNE_DATA_DIR=/tmp/fp\n-e\nRUNE_TIMEOUT_MS=30000\n-e\nRUNE_SEED=%s\n-e\nFILTER_E2E_N=%s\n-e\nFILTER_E2E_RATE=%s\n-e\nFILTER_E2E_CAP=%s\n' "${SEED}" "${N}" "${RATE}" "${CAP}"
    printf -- '-e\nFILTER_E2E_SHARED=/shared\n-v\n%s:/shared\n' "${SHARED}"
    [ -z "${TOPIC_TABLE}" ] || printf -- '-e\nRUNE_CLIENT_TOPIC_TABLE=%s\n' "${TOPIC_TABLE}"
    printf -- '-e\nRUNE_USER=%s\n-e\nRUNE_PASSWORD_FILE=/work/e2e.pw\n-e\nRUNE_TLS_CA_FILE=/work/ca.pem\n-v\n%s:/work:ro\n' "${RUNE_AUTH_E2E_USER}" "${RUNE_AUTH_WORK_DIR}"
    local kv
    for kv in "$@"; do printf -- '-e\n%s\n' "${kv}"; done
}
# probe_run <tag> <mode> <timeout_s> [KEY=VALUE ...] : foreground; sets PROBE_RC; files ${WORK_DIR}/<tag>.out|.err.
PROBE_RC=0
probe_run() {
    local tag="$1" mode="$2" t="$3"; shift 3
    local name="rune-fp-${tag}"
    local -a envargs
    mapfile -t envargs < <(probe_args "${name}" "${mode}" "$@")
    PROBES="${PROBES} ${name}"
    docker rm -f "${name}" >/dev/null 2>&1 || true
    set +e
    timeout -k 10 "${t}" docker run --rm "${envargs[@]}" "${PROBE_IMAGE}" > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
    PROBE_RC=$?
    set -e
    docker rm -f "${name}" >/dev/null 2>&1 || true
}
# probe_start <tag> <mode> <timeout_s> [KEY=VALUE ...] : background (the exit status lands in ${WORK_DIR}/<tag>.rc).
probe_start() {
    local tag="$1" mode="$2" t="$3"; shift 3
    local name="rune-fp-${tag}"
    local -a envargs
    mapfile -t envargs < <(probe_args "${name}" "${mode}" "$@")
    PROBES="${PROBES} ${name}"
    docker rm -f "${name}" >/dev/null 2>&1 || true
    rm -f "${WORK_DIR}/${tag}.rc" "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err"
    (
        set +e
        timeout -k 10 "${t}" docker run --rm "${envargs[@]}" "${PROBE_IMAGE}" > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
        echo $? > "${WORK_DIR}/${tag}.rc"
    ) &
}
bg_out()   { cat "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null || true; }
bg_done()  { [ -s "${WORK_DIR}/$1.rc" ]; }
bg_rc()    { cat "${WORK_DIR}/$1.rc"; }
out_of()   { cat "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null; }
# out_has <tag> <pattern> : the leg's output holds the pattern (a file grep: `cat | grep -q` fails under pipefail on SIGPIPE)
out_has()  { grep -q -e "$2" "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null; }
progress() { local p; p=$(cat "${SHARED}/progress" 2>/dev/null) || p=0; echo "${p:-0}"; }

# ---------------------------------------------------------------------------
# Step 1: images, the box (scenario), the key, the cluster, bootstrap.
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

log "Step 1c: building the filter_probe image (${PROBE_IMAGE})..."
set +e
out=$(docker build --platform linux/amd64 -f "${CLIENT_REPO}/docker/Dockerfile.filter-probe" \
    --build-context rune-protocol="${PROJECTS}/protocol" --build-context rune-codec="${PROJECTS}/codec" \
    --build-context rune-crypto="${PROJECTS}/crypto" --build-context flight-recorder="${FLIGHT_RECORDER_DIR}" \
    --build-context rune-ql="${PROJECTS}/ql" -t "${PROBE_IMAGE}" "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "filter_probe image build failed (exit ${rc})"; }
docker image inspect "${PROBE_IMAGE}" >/dev/null 2>&1 || fail "filter_probe image not built: ${PROBE_IMAGE}"

log "Step 1d: generating the cluster key..."
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"

if [ "${SCENARIO}" = "1" ]; then
    # The prepared box has no iptables chains: docker cannot create a network on it.  Create the compose network (and the
    # volumes) first; vm-bench-prepare.sh removes the rune-* containers, `up` below recreates them on the existing network.
    log "Step 1d (scenario): creating the compose network, then preparing the benchmark box (vm-bench-prepare.sh; the caller holds the host lock)..."
    dc up --no-start --no-build rune-1 rune-2 rune-3 >/dev/null || fail "could not create the compose network"
    # prepare refuses a box with load1 >= 1; the 1-minute load average lags the container churn above, so wait for it
    # to settle and retry a refused prepare (it restores what it changed itself).
    prepared=""
    for attempt in 1 2 3 4 5; do
        wait_until 300 "load1 below 0.7" bash -c 'awk "BEGIN{exit !($(cut -d\  -f1 /proc/loadavg) < 0.7)}"' || log "load1 is still $(cut -d' ' -f1 /proc/loadavg)"
        BENCH_PREPARED=1
        if RUNE_VM_LOCK_HELD=1 bash "${BROKER_REPO}/bench/vm-bench-prepare.sh"; then prepared=1; break; fi
        BENCH_PREPARED=""
        log "vm-bench-prepare.sh refused (attempt ${attempt}); retrying"
    done
    [ -n "${prepared}" ] || fail "vm-bench-prepare.sh failed 5 times (hold the lock: flock -w 7200 ${RUNE_VM_LOCK_FILE:-/root/vm.lock} bash $0 --scenario)"
fi

log "Step 1e: the 3-node cluster up..."
dc up -d --no-build rune-1 rune-2 rune-3
log "Step 1f: waiting for a common consensus leader (up to ${LEADER_WAIT_S}s)..."
wait_until "${LEADER_WAIT_S}" "a common consensus leader" cluster_has_leader \
    || { for n in 1 2 3; do docker logs --tail 40 "$(cname "${n}")" >&2 || true; done; fail "no common consensus leader within ${LEADER_WAIT_S}s"; }
log "Step 1g: bootstrapping the first admin + the e2e user..."
rune_auth_prepare "${NETWORK}" "rune-1:${BROKER_PORT}" || fail "bootstrap / e2e user"

# ---------------------------------------------------------------------------
# Step 2: setup.
# ---------------------------------------------------------------------------
log "Step 2: filter_probe setup (player_event: 8 partitions, rf 3, typed; untyped_t)..."
probe_run setup setup 300
[ "${PROBE_RC}" -eq 0 ] || { out_of setup | tail -30 >&2; fail "step 2: setup exited ${PROBE_RC}"; }
! out_has setup "FILTER-PROBE-SETUP: OK" && { out_of setup | tail -30 >&2; fail "step 2: no FILTER-PROBE-SETUP: OK"; }
TOPIC_ID=$(grep -oE '^FILTER_TOPIC_ID=[0-9]+' "${WORK_DIR}/setup.out" | cut -d= -f2)
UNTYPED_ID=$(grep -oE '^FILTER_UNTYPED_ID=[0-9]+' "${WORK_DIR}/setup.out" | cut -d= -f2)
[ -n "${TOPIC_ID}" ] && [ -n "${UNTYPED_ID}" ] || fail "step 2: setup printed no topic ids"
TOPIC_TABLE="player_event:${TOPIC_ID}:8,untyped_t:${UNTYPED_ID}:1"
log "Step 2 OK: ${TOPIC_TABLE}"
# Every partition must have a leader before the legs start.
all_leaders() { local p; for p in 0 1 2 3 4 5 6 7; do partition_leader "${TOPIC_ID}" "${p}" >/dev/null || return 1; done; }
wait_until 120 "all partition leaders" all_leaders || fail "step 2: not every player_event partition has a leader"

# ===========================================================================
# The MMORPG scenario.
# ===========================================================================
if [ "${SCENARIO}" = "1" ]; then
    PROCS=$(( WORLDS / WORLDS_PER_PROC ))
    [ $(( PROCS * WORLDS_PER_PROC )) -eq "${WORLDS}" ] || fail "FILTER_E2E_WORLDS must be a multiple of ${WORLDS_PER_PROC}"
    RUNS="${FILTER_E2E_RUNS:-3}"
    TOTAL_S=$(( RUNS * 2 * (SECONDS_PER_RUN + 90) + 120 ))
    log "Scenario: ${WORLDS} world servers on ${PROCS} containers, producer at ${RATE}/s, ${SECONDS_PER_RUN} s per run, ABAB x ${RUNS}"
    probe_start load produce $(( TOTAL_S + 120 )) "FILTER_E2E_IDLE_US=${IDLE_US}" "FILTER_E2E_DURATION_S=${TOTAL_S}" "FILTER_E2E_PROGRESS_FILE=/shared/progress"
    progress_over_1000() { [ "$(progress)" -gt 1000 ]; }
    wait_until 120 "the load producer to ack" progress_over_1000 || { out_of load | tail -20 >&2; fail "the load producer never got going"; }

    run_worlds() {   # run_worlds <label> <unfiltered 0|1> : starts PROCS containers, waits, writes ${WORK_DIR}/<label>.txt
        local label="$1" unf="$2" i
        for i in $(seq 0 $(( PROCS - 1 ))); do
            probe_start "w-${label}-${i}" world $(( SECONDS_PER_RUN + 240 )) "FILTER_E2E_WORLD_BASE=$(( i * WORLDS_PER_PROC ))" \
                "FILTER_E2E_WORLDS_PER_PROC=${WORLDS_PER_PROC}" "FILTER_E2E_SECONDS=${SECONDS_PER_RUN}" "FILTER_E2E_UNFILTERED=${unf}" "FILTER_E2E_IDLE_US=${IDLE_US}"
        done
        for i in $(seq 0 $(( PROCS - 1 ))); do
            wait_until $(( SECONDS_PER_RUN + 240 )) "world container ${i}" bg_done "w-${label}-${i}" || fail "run ${label}: world container ${i} did not finish"
            [ "$(bg_rc "w-${label}-${i}")" = "0" ] || { bg_out "w-${label}-${i}" | tail -20 >&2; fail "run ${label}: world container ${i} exited $(bg_rc "w-${label}-${i}")"; }
            ! out_has "w-${label}-${i}" "FILTER-WORLD: OK" && { out_of "w-${label}-${i}" | grep -vE "WARNING|^WORLD " | tail -12 >&2; fail "run ${label}: world container ${i} did not print FILTER-WORLD: OK"; }
        done
        cat "${WORK_DIR}"/w-"${label}"-*.out | grep '^WORLD ' > "${WORK_DIR}/${label}.txt"
        [ "$(wc -l < "${WORK_DIR}/${label}.txt" | tr -d ' ')" = "${WORLDS}" ] || fail "run ${label}: $(wc -l < "${WORK_DIR}/${label}.txt") WORLD lines, want ${WORLDS}"
    }
    # run_stat <file> : "<median p50> <median p99> <max bytes/ideal> <median bytes/ideal> <records>" over the worlds of a run.
    run_stat() {
        awk '
            function med(a, n,   i, j, t) { for (i = 2; i <= n; i++) { t = a[i]; for (j = i - 1; j >= 1 && a[j] > t; j--) a[j + 1] = a[j]; a[j + 1] = t } return (n % 2) ? a[(n + 1) / 2] : (a[n / 2] + a[n / 2 + 1]) / 2 }
            { for (i = 1; i <= NF; i++) { split($i, kv, "="); v[kv[1]] = kv[2] }
              n++; p50[n] = v["p50_us"]; p99[n] = v["p99_us"]; r = (v["ideal_bytes"] > 0) ? v["bytes"] / v["ideal_bytes"] : 1e9; ratio[n] = r; if (r > mx) mx = r; rec += v["records"] }
            END { printf "%.1f %.1f %.3f %.3f %d\n", med(p50, n), med(p99, n), mx, med(ratio, n), rec }' "$1"
    }
    : > "${WORK_DIR}/stats.txt"
    for r in $(seq 1 ${RUNS}); do
        log "Scenario run ${r}/${RUNS}: filtered..."
        run_worlds "f${r}" 0
        echo "f ${r} $(run_stat "${WORK_DIR}/f${r}.txt")" >> "${WORK_DIR}/stats.txt"
        log "Scenario run ${r}/${RUNS}: unfiltered..."
        run_worlds "u${r}" 1
        echo "u ${r} $(run_stat "${WORK_DIR}/u${r}.txt")" >> "${WORK_DIR}/stats.txt"
        tail -2 "${WORK_DIR}/stats.txt" | sed 's/^/[filter-e2e] run: /' >&2
    done
    docker rm -f rune-fp-load >/dev/null 2>&1 || true

    # The gates.  Columns: mode run p50_us p99_us max_bytes/ideal median_bytes/ideal records.
    summary=$(awk '
        function med(a, n,   i, j, t) { for (i = 2; i <= n; i++) { t = a[i]; for (j = i - 1; j >= 1 && a[j] > t; j--) a[j + 1] = a[j]; a[j + 1] = t } return (n % 2) ? a[(n + 1) / 2] : (a[n / 2] + a[n / 2 + 1]) / 2 }
        function range(a, n,   i, lo, hi) { lo = hi = a[1]; for (i = 2; i <= n; i++) { if (a[i] < lo) lo = a[i]; if (a[i] > hi) hi = a[i] } return hi - lo }
        { m = $1; k[m]++; i = k[m]; p50[m, i] = $3; p99[m, i] = $4; if ($5 + 0 > mx[m] + 0) mx[m] = $5; rb[m, i] = $6 }
        END {
            for (mm = 1; mm <= 2; mm++) {
                m = (mm == 1) ? "f" : "u"
                for (i = 1; i <= k[m]; i++) { a[i] = p50[m, i]; b[i] = p99[m, i]; c[i] = rb[m, i] }
                r50[m] = range(a, k[m]); r99[m] = range(b, k[m])
                M50[m] = med(a, k[m]); M99[m] = med(b, k[m]); MR[m] = med(c, k[m])
            }
            band50 = (r50["f"] > r50["u"]) ? r50["f"] : r50["u"]
            band99 = (r99["f"] > r99["u"]) ? r99["f"] : r99["u"]
            printf "%.1f %.1f %.1f %.1f %.1f %.1f %.3f %.3f %.3f\n", M50["f"], M50["u"], band50, M99["f"], M99["u"], band99, mx["f"], MR["f"], MR["u"] }' "${WORK_DIR}/stats.txt")
    read -r F50 U50 B50 F99 U99 B99 FMAXR FMEDR UMEDR <<< "${summary}"
    echo "FILTER-SCENARIO: runs (mode run p50_us p99_us max_bytes/ideal median_bytes/ideal records):"
    sed 's/^/FILTER-SCENARIO:   /' "${WORK_DIR}/stats.txt"
    echo "FILTER-SCENARIO: p50_us filtered=${F50} unfiltered=${U50} band=${B50} (median over ${RUNS} runs; band = the larger run-to-run range of the two)"
    echo "FILTER-SCENARIO: p99_us filtered=${F99} unfiltered=${U99} band=${B99}"
    echo "FILTER-SCENARIO: bytes/ideal filtered max=${FMAXR} median=${FMEDR}; unfiltered median=${UMEDR} (ideal = records x a one-record batch)"
    echo "FILTER-SCENARIO: info (absolute, not a gate): filtered p50=${F50} us p99=${F99} us at ${RATE} records/s across ${WORLDS} world servers"
    why=""
    awk -v x="${FMAXR}" 'BEGIN { exit !(x <= 1.2) }' || why="${why} a world's bytes/ideal reached ${FMAXR} (> 1.2);"
    awk -v f="${F50}" -v u="${U50}" -v b="${B50}" 'BEGIN { exit !(f <= u + b) }' || why="${why} filtered p50 ${F50} us is worse than unfiltered ${U50} us beyond the band ${B50};"
    awk -v f="${F99}" -v u="${U99}" -v b="${B99}" 'BEGIN { exit !(f <= u + b) }' || why="${why} filtered p99 ${F99} us is worse than unfiltered ${U99} us beyond the band ${B99};"
    if [ -n "${why}" ]; then echo "FILTER-SCENARIO: FAIL${why}"; exit 1; fi
    echo "FILTER-SCENARIO: OK"
    exit 0
fi

# ===========================================================================
# The functional legs.
# ===========================================================================
# Seed = a node that is neither the partition-3 leader (killed at ~30 %) nor the node bounced at ~60 % (which may lead
# partition 3 by then: the bounce is extra stress, not a non-leader restart).
LEADER3=$(partition_leader "${TOPIC_ID}" 3) || fail "no leader for player_event partition 3"
SEED_NODE=$(( (LEADER3 + 1) % 3 + 1 ))
BOUNCE=$(( (LEADER3 + 2) % 3 ))     # node id 0-2: neither the leader of partition 3 nor the seed (SEED_NODE - 1)
log "partition 3 is led by node ${LEADER3}; the probes connect to rune-${SEED_NODE}; node ${BOUNCE} is bounced at ~60 %"

log "Step 3: consume (five subscriptions) + produce ${N} records at ${RATE}/s..."
probe_start consume consume "${CONSUME_S}" "FILTER_E2E_CONSUME_TIMEOUT_S=$(( CONSUME_S - 20 ))"
wait_until 180 "consume READY" bash -c "grep -q 'FILTER-CONSUME: READY' '${WORK_DIR}/consume.out'" \
    || { bg_out consume | tail -20 >&2; fail "step 3: consume never became ready"; }
CONSUME_START=$(now_s)
probe_start produce produce 900 "FILTER_E2E_PROGRESS_FILE=/shared/progress"

wait_progress() {
    local want="$1" what="$2" deadline=$(( $(now_s) + PROGRESS_WAIT_S ))
    while [ "$(progress)" -lt "${want}" ]; do
        if bg_done produce; then
            [ "$(bg_rc produce)" = "0" ] && return 0
            bg_out produce | tail -20 >&2; fail "${what}: produce exited $(bg_rc produce) at $(progress) of ${N}"
        fi
        if bg_done consume; then bg_out consume | tail -20 >&2; fail "${what}: consume exited $(bg_rc consume) early"; fi
        [ "$(now_s)" -lt "${deadline}" ] || { bg_out produce | tail -20 >&2; fail "${what}: produce stuck at $(progress) records (want ${want}) for ${PROGRESS_WAIT_S}s"; }
        sleep 1
    done
}
wait_resume() {
    local base="$1" what="$2" deadline=$(( $(now_s) + RESUME_WAIT_S ))
    while [ "$(progress)" -lt $(( base + RESUME_RECORDS )) ]; do
        if bg_done produce; then [ "$(bg_rc produce)" = "0" ] && return 0; bg_out produce | tail -20 >&2; fail "${what}: produce exited $(bg_rc produce)"; fi
        [ "$(now_s)" -lt "${deadline}" ] || { bg_out produce | tail -20 >&2; fail "${what}: no progress for ${RESUME_WAIT_S}s after the kill (stuck at $(progress), killed at ${base})"; }
        sleep 1
    done
}

log "Step 3a: ~30 % - docker kill of the leader of partition 3..."
wait_progress $(( (N + 40) * 30 / 100 )) "step 3a"
pl=$(partition_leader "${TOPIC_ID}" 3) || fail "step 3a: no leader for partition 3"
at=$(progress)
log "  partition 3 leader = node ${pl} (rune-$(( pl + 1 ))), consensus leader / group coordinator = node $(consensus_leader || echo unknown), at ${at} acked records; docker kill"
kill_node "${pl}"
wait_resume "${at}" "step 3a"
log "  progress resumed ($(progress) records); restarting rune-$(( pl + 1 ))"
restart_node "${pl}"

log "Step 3b: ~60 % - docker restart of node ${BOUNCE} (rune-$(( BOUNCE + 1 )))..."
wait_progress $(( (N + 40) * 60 / 100 )) "step 3b"
at=$(progress)
bounce_node "${BOUNCE}"
wait_resume "${at}" "step 3b"
log "  progress resumed ($(progress) records)"

wait_until 900 "produce to finish" bg_done produce || { bg_out produce | tail -20 >&2; fail "step 3: produce did not finish"; }
[ "$(bg_rc produce)" = "0" ] || { bg_out produce | tail -20 >&2; fail "step 3: produce exited $(bg_rc produce)"; }
! out_has produce "FILTER-PRODUCE: OK" && fail "step 3: produce did not print FILTER-PRODUCE: OK"
left=$(( CONSUME_S - ($(now_s) - CONSUME_START) ))
wait_until "${left}" "consume to finish" bg_done consume || { bg_out consume | tail -40 >&2; fail "step 3: consume did not finish within ${CONSUME_S} s of its start"; }
[ "$(bg_rc consume)" = "0" ] || { bg_out consume | tail -40 >&2; fail "step 3: consume exited $(bg_rc consume)"; }
! out_has consume "FILTER-CONSUME: OK sets=5" && { bg_out consume | tail -40 >&2; fail "step 3: consume did not print 'FILTER-CONSUME: OK sets=5'"; }
out_of consume | grep -E "FILTER-CONSUME" >&2 || true
log "Step 3 OK: all five sets exact, positions at the ends, through a leader kill and a node restart"

log "Step 4: the refusals..."
probe_run errors errors "${LEG_S}"
[ "${PROBE_RC}" -eq 0 ] || { out_of errors | tail -30 >&2; fail "step 4: errors exited ${PROBE_RC}"; }
! out_has errors "FILTER-ERRORS: OK" && { out_of errors | tail -30 >&2; fail "step 4: no FILTER-ERRORS: OK"; }
grep -E "^FILTER-ERRORS " "${WORK_DIR}/errors.out" >&2 || true
log "Step 4 OK"

log "Step 5: ALTER (ADD guild, DROP channel) under a live subscription..."
probe_run alter alter "${LEG_S}"
[ "${PROBE_RC}" -eq 0 ] || { out_of alter | tail -30 >&2; fail "step 5: alter exited ${PROBE_RC}"; }
! out_has alter "FILTER-ALTER: OK" && { out_of alter | tail -30 >&2; fail "step 5: no FILTER-ALTER: OK"; }
# The broker names the absent field once per partition (subscription (b), tag 2 = channel, schema version 3).
absent=""
for n in 1 2 3; do
    absent="${absent}$(docker logs "$(cname "${n}")" 2>&1 | grep -E "filter: filtered field absent: sub [0-9]+ topic ${TOPIC_ID} partition [0-9]+ tag 2 schema version 3" || true)
"
done
for p in 0 1 2 3 4 5 6 7; do
    c=$(echo "${absent}" | grep -cE "topic ${TOPIC_ID} partition ${p} tag 2 schema version 3" || true)
    [ "${c}" = "1" ] || { echo "${absent}" >&2; fail "step 5: the broker logged 'filtered field absent' for partition ${p} ${c} times (want once)"; }
done
log "Step 5 OK: FILTER-ALTER: OK; 'filtered field absent' logged once per partition"

echo "FILTER-E2E: OK"
