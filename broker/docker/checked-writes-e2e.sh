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
#   5. RESTART:   rolling restart of all three nodes (`docker restart`); re-inserting every live id gives 56, every removed id gives ok; VERIFY
#                 again over the grown log (the key index was rebuilt from the log);
#   6. REFUSALS:  key.index on a transient topic, an untyped topic and a keyless topic give InvalidRequest with the three exact
#                 messages; on a string(9) key it is accepted (keys up to 16 bytes);
#   7. AUTOINC:   ai_user (1 partition, replicas 3, compact, autoincrement); the `backfill` user (write + import) imports ids 1-4999
#                 in steps of 2 (ids kept); 4 `app` probes (write) create key-0 x 5,000 each, the partition leader `docker kill`ed at
#                 ~30 % and restarted once progress resumes.  Check: every returned id is unique and above 4,999, none is StoredIdUnknown /
#                 VerdictUnknown / Timeout, a subscription from 0 holds exactly the imported and the returned ids.  Bounded at 600 s.
#                 Then the highest returned id is removed, all three nodes are restarted and one more create gives an id above it.
#                 Refusals: `app` importing id 2 gives 45; autoincrement on a cleanup.policy = delete topic and an IMPORT grant on a
#                 view are refused.  Prints AUTOINC: OK;
#   8. REGRESSORSPATH: ql/tests/golden/regressorspath.rql through the CLI (segment.bytes = 65536 and a day-long compaction lag added to its topics), then
#                 RP-LOGIN-RACE: 4 probes race first logins over the same 1,000 subjects, the `user` leader `docker kill`ed at ~30 %
#                 and restarted once progress resumes (per subject exactly one Ok, every other 55 user_identity.unique, each loser's
#                 minOffset read returned the winner's userId); RP-CHARACTERS: 200 users, 4 probes create 13 characters per user
#                 concurrently plus a Foo<n> / foo<n> pair per user pair, the `character` leader killed at ~30 % (per user exactly 12 Ok,
#                 the rest 55 account_stats.max_characters; per pair one Ok and one 55 character_name.unique); RP-RESTART: all three
#                 nodes restarted, every account_stats row is 12 and a 13th create is 55; RP-LIFECYCLE: 100 users removeExisting a
#                 character (count 11), another user reuses its name, the second removeExisting gives 57; RP-VERIFY: a subscription
#                 from 0 on user and character holds no offset a write was refused at; after `alter topic` makes the cleaner eager,
#                 `rune stats topic <t> --json` on the leader reports rejectedNotCompacted 0 for both.  Bounded at 600 s per race.
#                 Prints REGRESSORSPATH: OK;
#   9. TXN:       a `ledger` topic (1 partition, replicas 3, compact) and the view `balances` (balance >= 0) beside the character topic.
#                 TXN-RACE: 4 probes run transactions of two characters and two ledger entries over the same users (per user six
#                 fit account_stats, a seventh and an overdraw are refused), the `character` leader `docker kill`ed at ~30 % (first half of the users) and
#                 the consensus leader, the transaction coordinator, at ~60 % (second half, its probes seeded on another node), each restarted once progress resumes (per user at most
#                 six Ok, every other answer 55; the workers share the names u<user>k<k>a / b and race for them; a lost answer only on a
#                 transaction that overlaps a kill window, no transaction longer than the transaction timeout and a pass); TXN-VERIFY: a read_committed
#                 subscription from 0 holds every committed transaction whole, no refused one, a lost one whole or not at all, each name
#                 in one committed transaction with character_name mapping it to that transaction's character, and account_stats and balances agree; TXN-RESTART: all three nodes restarted, TXN-VERIFY again and a new transaction
#                 commits.  Bounded at 600 s per race.  Prints TXN: OK;
#  10. print CHECKED-WRITES-E2E: OK.
#
# Usage (on the VM, under the host lock):
#   ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'
# Environment (all optional):
#   FLIGHT_RECORDER_DIR     the flight-recorder checkout (default: <monorepo>/../flight-recorder)
#   ARCANUM_METRICS_DIR     the arcanum-metrics checkout (default: <monorepo>/../arcanum-metrics)
#   KI_E2E_IDS              ids the racers insert (default 20000)       KI_E2E_WORKERS   racing containers (default 4)
#   KI_E2E_THREADS          writer threads per container (default 16)
#   AI_E2E_CREATES          key-0 creates per racing container in leg 7 (default 5000)
#   RP_E2E_SUBJECTS         subjects the login race covers in leg 8 (default 1000)
#   RP_E2E_USERS            users the character race covers (default 200; even: a Foo / foo pair takes two)
#   RP_E2E_LIFECYCLE_USERS  users of the delete and name-reuse step (default 100, at most RP_E2E_USERS)
#   TXN_E2E_USERS           users the transaction race covers in leg 9 (default 64: 4 probes x 64 users x 8 transactions = 2,048)
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
AI_CREATES="${AI_E2E_CREATES:-5000}"
RP_SUBJECTS="${RP_E2E_SUBJECTS:-1000}"
RP_USERS="${RP_E2E_USERS:-200}"
RP_LIFE_USERS="${RP_E2E_LIFECYCLE_USERS:-100}"
TXN_USERS="${TXN_E2E_USERS:-64}"
TXN_USER_BASE=10000    # the transaction users are 10001..: none has characters from leg 8
TXN_PER_USER=8         # the probe's per-user transactions: six that fit, the cap's, the overdraw
TXN_FITTING=6          # transactions of two characters that fit account_stats.max_characters
TXN_TIMEOUT_MS=60000   # the probe's transactionTimeoutMs
TXN_PASS_MS=5000       # one pass of the coordinator's timeout sweep and the vote retry on top of it
RP_CHARS=13            # creates per user and probe (the probe's kRpCharsPerUser)
RP_CAP=12              # account_stats.max_characters
RP_PAIR_BASE=1000000   # the probe's kRpPairBase: the users of the Foo / foo pairs
RP_LIFE_BASE=2000000   # the probe's kRpLifeBase: the users that reuse a removed name
AI_IMPORT_MAX=5000     # the back-fill imports the odd ids below it; every id the creators are answered with is above 4999
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
LEG="preflight"   # the leg a failure belongs to
fail() { echo "[checked-writes-e2e] FAIL (${LEG}): $*" >&2; exit 1; }

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
now_ms() { date +%s%3N; }   # GNU date on the VM; the probes write epoch ms from the same host clock (the containers share it), so the two are comparable

[[ "$(now_ms)" =~ ^[0-9]{13}$ ]] || fail "now_ms needs GNU date"
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
        cp -f "${WORK_DIR}"/*-check.txt "${E2E_LOG_DIR}/" 2>/dev/null || true
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

KILLED_AT=0
kill_node() { docker kill "$(cname $(( $1 + 1 )))" >/dev/null || fail "docker kill of rune-$(( $1 + 1 )) failed"; KILLED_AT="$(now_s)"; }
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
    # a restart inside the driver dir lock's 1 s heartbeat window of the kill is refused (same pid 1): wait it out
    [ "$(( $(now_s) - KILLED_AT ))" -ge 3 ] || sleep 3
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
# probe_args <container name> : the docker-run arguments (one per line).  PROBE_USER / PROBE_PW (a file in the work dir) name another user.
probe_args() {
    local name="$1"
    printf -- '--name\n%s\n--hostname\n%s\n--network\n%s\n--platform\nlinux/amd64\n--shm-size=256m\n--security-opt\nseccomp=unconfined\n' "${name}" "${name}" "${NETWORK}"
    printf -- '-e\nRUNE_TARGET=aeron:udp?endpoint=rune-%s:%s\n-e\nRUNE_EGRESS_HOST=%s\n-e\nRUNE_EGRESS_PORT=40500\n' "${SEED_NODE}" "${BROKER_PORT}" "${name}"
    printf -- '-e\nRUNE_DATA_DIR=/tmp/fp\n-e\nRUNE_TIMEOUT_MS=30000\n-e\nKI_E2E_SHARED=/shared\n-e\nKI_E2E_THREADS=%s\n-v\n%s:/shared\n' "${THREADS}" "${SHARED}"
    printf -- '-e\nRUNE_USER=%s\n-e\nRUNE_PASSWORD_FILE=/work/%s\n-e\nRUNE_TLS_CA_FILE=/work/ca.pem\n-v\n%s:/work:ro\n' "${PROBE_USER:-${RUNE_AUTH_E2E_USER}}" "${PROBE_PW:-e2e.pw}" "${RUNE_AUTH_WORK_DIR}"
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
# leg <tag> <timeout_s> <ok marker> <mode> [flags...] : one foreground probe leg that must exit 0 and print its marker.
leg() {
    local tag="$1" t="$2" marker="$3"; shift 3
    probe_run "${tag}" "${t}" "$@"
    [ "${PROBE_RC}" -eq 0 ] || { out_of "${tag}" | tail -30 >&2; fail "${tag} exited ${PROBE_RC}"; }
    out_has "${tag}" "${marker}" || { out_of "${tag}" | tail -30 >&2; fail "no ${marker} from ${tag}"; }
}
race_progress() {   # [tag]: the completed writes of the tag's workers (default race)
    local s=0 f v
    for f in "${SHARED}/${1:-race}"-progress-w*; do
        [ -f "${f}" ] || continue
        case "${f}" in *.tmp) continue ;; esac
        v=$(cat "${f}" 2>/dev/null) || v=0
        s=$(( s + ${v:-0} ))
    done
    echo "${s}"
}
progress_at_least() { [ "$(race_progress "${2:-race}")" -ge "$1" ]; }

# ---------------------------------------------------------------------------
# Leg 1: images, the key, the cluster, bootstrap, ki_account.
# ---------------------------------------------------------------------------
LEG="leg 1"
log "Leg 1a: building the broker image (tag ${RUNE_IMAGE_TAG})..."
set +e
out=$(dc build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"

log "Leg 1b: building the rune CLI image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"

log "Leg 1c: building the checked_writes_probe image (${PROBE_IMAGE})..."
set +e
out=$(docker build --platform linux/amd64 -f "${CLIENT_REPO}/docker/Dockerfile.checked-writes-probe" \
    --build-context rune-protocol="${PROJECTS}/protocol" --build-context rune-codec="${PROJECTS}/codec" \
    --build-context rune-crypto="${PROJECTS}/crypto" --build-context aeron-recipe="${PROJECTS}/conan/aeron" --build-context flight-recorder="${FLIGHT_RECORDER_DIR}" \
    --build-context rune-ql="${PROJECTS}/ql" -t "${PROBE_IMAGE}" "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "checked_writes_probe image build failed (exit ${rc})"; }
docker image inspect "${PROBE_IMAGE}" >/dev/null 2>&1 || fail "checked_writes_probe image not built: ${PROBE_IMAGE}"

log "Leg 1d: generating the cluster key..."
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"

log "Leg 1e: the 3-node cluster up..."
dc up -d --no-build rune-1 rune-2 rune-3
log "Leg 1f: waiting for a common consensus leader (up to ${LEADER_WAIT_S}s)..."
wait_until "${LEADER_WAIT_S}" "a common consensus leader" cluster_has_leader \
    || { for n in 1 2 3; do docker logs --tail 40 "$(cname "${n}")" >&2 || true; done; fail "no common consensus leader within ${LEADER_WAIT_S}s"; }
log "Leg 1g: bootstrapping the first admin + the e2e user..."
rune_auth_prepare "${NETWORK}" "rune-1:${BROKER_PORT}" || fail "bootstrap / e2e user"

log "Leg 1h: SETUP: ki_account (${PARTS} partitions, replicas 3, compact, key.index)..."
leg setup 300 "CHECKED-WRITES-SETUP: OK" setup
TOPIC_ID=$(grep -oE '^KI_TOPIC_ID=[0-9]+' "${WORK_DIR}/setup.out" | cut -d= -f2) || true
[ -n "${TOPIC_ID}" ] || fail "setup printed no topic id"
all_leaders() { local p; for p in $(seq 0 $(( PARTS - 1 ))); do partition_leader "${TOPIC_ID}" "${p}" >/dev/null || return 1; done; }
wait_until 120 "all partition leaders" all_leaders || fail "not every ki_account partition has a leader"
LEADER0=$(partition_leader "${TOPIC_ID}" 0) || fail "no leader of partition 0"
if [ "${LEADER0}" = "0" ]; then SEED_NODE=2; fi   # the probes never connect through the node the race kills
log "Leg 1 OK: topic ${TOPIC_ID}; partition 0 led by node ${LEADER0}; probes connect through rune-${SEED_NODE}"

# ---------------------------------------------------------------------------
# Leg 2: RACE.
# ---------------------------------------------------------------------------
LEG="leg 2"
TOTAL_WRITES=$(( WORKERS * IDS ))
log "Leg 2: RACE: ${WORKERS} containers insert ids 1-${IDS} (${TOTAL_WRITES} writes, ${THREADS} threads each); the partition-0 leader is killed at ~30 %..."
RACE_START=$(now_s)
for i in $(seq 0 $(( WORKERS - 1 ))); do probe_start "race-${i}" "${RACE_S}" race --worker "${i}" --ids "1-${IDS}"; done
wait_until $(( RACE_S )) "30 % of the race" progress_at_least $(( TOTAL_WRITES * 30 / 100 )) \
    || { out_of race-0 | tail -20 >&2; fail "the race never reached 30 % within ${RACE_S}s (at $(race_progress) of ${TOTAL_WRITES})"; }
LEADER0=$(partition_leader "${TOPIC_ID}" 0) || fail "no leader of partition 0 at the kill"
BASE=$(race_progress)
log "Leg 2: killing node ${LEADER0}, the partition-0 leader, at ${BASE} of ${TOTAL_WRITES} writes"
kill_node "${LEADER0}"
RESUME_AT=$(( BASE + RESUME_WRITES ))
[ "${RESUME_AT}" -le "${TOTAL_WRITES}" ] || RESUME_AT="${TOTAL_WRITES}"
wait_until "${RESUME_WAIT_S}" "the race resuming" progress_at_least "${RESUME_AT}" \
    || { out_of race-0 | tail -20 >&2; fail "no progress past ${RESUME_AT} within ${RESUME_WAIT_S}s of the kill (at $(race_progress))"; }
restart_node "${LEADER0}"
log "Leg 2: node ${LEADER0} restarted; waiting for the racers..."
for i in $(seq 0 $(( WORKERS - 1 ))); do
    left=$(( RACE_S - ($(now_s) - RACE_START) ))
    [ "${left}" -gt 0 ] || left=1
    wait_until "${left}" "racer ${i}" bg_done "race-${i}" || fail "racer ${i} did not finish within ${RACE_S}s of the race start"
    [ "$(bg_rc "race-${i}")" = "0" ] || { out_of "race-${i}" | tail -20 >&2; fail "racer ${i} exited $(bg_rc "race-${i}")"; }
done

# Every id: written once by every worker; exactly one Ok, every other result 56.
race_files=("${SHARED}"/race-w*.txt)
[ "${#race_files[@]}" -eq "${WORKERS}" ] || fail "${#race_files[@]} result files, want ${WORKERS}"
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
    || { cat "${WORK_DIR}/race-check.txt" >&2; fail "the race broke 'exactly one Ok, every other 56' (codes 10014 = VerdictUnknown, 10002 = Timeout, 10009 = DeliveryFailed)"; }
log "Leg 2 OK: every id has exactly one Ok and ${WORKERS} - 1 KeyExists; $(( $(now_s) - RACE_START )) s"

# ---------------------------------------------------------------------------
# Leg 3: LIFECYCLE.
# ---------------------------------------------------------------------------
LEG="leg 3"
log "Leg 3: LIFECYCLE over ${LIFECYCLE_IDS} ids..."
leg lifecycle "${LEG_S}" "CHECKED-WRITES-LIFECYCLE: OK" lifecycle
log "Leg 3 OK"

# ---------------------------------------------------------------------------
# Leg 4: VERIFY.
# ---------------------------------------------------------------------------
LEG="leg 4"
log "Leg 4: VERIFY: a subscription from 0 on every partition against the accepted writes..."
leg verify "${LEG_S}" "CHECKED-WRITES-VERIFY: OK" verify
log "Leg 4 OK: $(grep -o 'CHECKED-WRITES-VERIFY: OK.*' "${WORK_DIR}/verify.out")"

# ---------------------------------------------------------------------------
# Leg 5: RESTART.
# ---------------------------------------------------------------------------
LEG="leg 5"
log "Leg 5: RESTART: docker restart of all three nodes..."
restart_all
log "Leg 5: re-inserting ids 1-${IDS}: a live id gives 56, a removed id (every 4th of 1-${LIFECYCLE_IDS}) gives ok..."
leg restart "${RACE_S}" "CHECKED-WRITES-RACE: DONE" race --worker 0 --ids "1-${IDS}" --tag restart
awk -v ids="${IDS}" -v life="${LIFECYCLE_IDS}" '
    { n++; seen[$1]++; want = ($1 <= life && $1 % 4 == 0) ? 0 : 56
      if ($3 != want) { if (shown++ < 10) printf "id %d: code %d, want %d\n", $1, $3, want; bad = 1 } }
    END { if (n != ids) { printf "%d results, want %d\n", n, ids; bad = 1 } exit bad }' "${SHARED}/restart-w0.txt" > "${WORK_DIR}/restart-check.txt" \
    || { cat "${WORK_DIR}/restart-check.txt" >&2; fail "after the restart a live id did not give 56, or a removed id did not give ok"; }
log "Leg 5: VERIFY again over the grown log..."
leg verify2 "${LEG_S}" "CHECKED-WRITES-VERIFY: OK" verify
log "Leg 5 OK"

# ---------------------------------------------------------------------------
# Leg 6: REFUSALS.
# ---------------------------------------------------------------------------
LEG="leg 6"
log "Leg 6: REFUSALS: key.index on a transient, an untyped and a keyless topic; accepted on a string(9)-keyed one..."
leg refusals "${LEG_S}" "CHECKED-WRITES-REFUSALS: OK" refusals
log "Leg 6 OK"

# ---------------------------------------------------------------------------
# Leg 7: AUTOINC.
# ---------------------------------------------------------------------------
LEG="leg 7"
log "Leg 7a: AI-SETUP: ai_user (1 partition, replicas 3, compact, autoincrement); users backfill (write, import) and app (write)..."
leg ai-setup 300 "CHECKED-WRITES-AI-SETUP: OK" ai-setup
AI_TOPIC_ID=$(grep -oE '^AI_TOPIC_ID=[0-9]+' "${WORK_DIR}/ai-setup.out" | cut -d= -f2) || true
[ -n "${AI_TOPIC_ID}" ] || fail "ai-setup printed no topic id"
for u in backfill app; do rune_auth_gen_password "${RUNE_AUTH_WORK_DIR}/${u}.pw" || fail "no password for ${u}"; done
(
    umask 077
    for u in backfill app; do
        printf "CREATE USER %s WITH PASSWORD '%s';\n" "${u}" "$(cat "${RUNE_AUTH_WORK_DIR}/${u}.pw")"
        echo "GRANT WRITE ON TOPIC 'ai_user' TO USER ${u};"
    done
    echo "GRANT IMPORT ON TOPIC 'ai_user' TO USER backfill;"
) > "${RUNE_AUTH_WORK_DIR}/ai-users.rql"
set +e
out=$(rune_auth_cli_admin "${RUNE_AUTH_NETWORK}" ql -f /work/ai-users.rql --broker "rune-1:${BROKER_PORT}" 2>&1); rc=$?
set -e
rm -f "${RUNE_AUTH_WORK_DIR}/ai-users.rql"
[ "${rc}" -eq 0 ] || { echo "${out}" >&2; fail "creating the backfill and app users failed (exit ${rc})"; }
wait_until 120 "the ai_user leader" partition_leader "${AI_TOPIC_ID}" 0 || fail "ai_user partition 0 has no leader"
AI_LEADER=$(partition_leader "${AI_TOPIC_ID}" 0) || fail "no leader of ai_user partition 0"
if [ "${AI_LEADER}" = "0" ]; then SEED_NODE=2; else SEED_NODE=1; fi   # the probes never connect through the node the race kills

log "Leg 7b: AI-BACKFILL: backfill imports ids 1-$(( AI_IMPORT_MAX - 1 )) in steps of 2..."
PROBE_USER=backfill PROBE_PW=backfill.pw leg ai-backfill "${LEG_S}" "CHECKED-WRITES-AI-BACKFILL: DONE" ai-backfill --ids "1-${AI_IMPORT_MAX}" --step 2
# An explicit-id import is answered ok with key 0 (spec P6-10): the ids are proven kept by the verify below.
awk -v rows=$(( AI_IMPORT_MAX / 2 )) '
    { n++; if ($3 != 0) { if (shown++ < 10) printf "id %d: code %d\n", $1, $3; bad = 1 } }
    END { if (n != rows) { printf "%d results, want %d\n", n, rows; bad = 1 } exit bad }' "${SHARED}/ai-backfill-w0.txt" > "${WORK_DIR}/ai-backfill-check.txt" \
    || { cat "${WORK_DIR}/ai-backfill-check.txt" >&2; fail "the back-fill was not all ok"; }

AI_TOTAL=$(( WORKERS * AI_CREATES ))
log "Leg 7c: AI-RACE: ${WORKERS} app containers create ${AI_CREATES} ids each (${AI_TOTAL}); the ai_user leader is killed at ~30 %..."
AI_START=$(now_s)
for i in $(seq 0 $(( WORKERS - 1 ))); do
    PROBE_USER=app PROBE_PW=app.pw probe_start "ai-create-${i}" "${RACE_S}" ai-create --worker "${i}" --count "${AI_CREATES}"
done
wait_until "${RACE_S}" "30 % of the creates" progress_at_least $(( AI_TOTAL * 30 / 100 )) ai-create \
    || { out_of ai-create-0 | tail -20 >&2; fail "the creates never reached 30 % within ${RACE_S}s (at $(race_progress ai-create) of ${AI_TOTAL})"; }
AI_LEADER=$(partition_leader "${AI_TOPIC_ID}" 0) || fail "no leader of ai_user partition 0 at the kill"
BASE=$(race_progress ai-create)
log "Leg 7: killing node ${AI_LEADER}, the ai_user leader, at ${BASE} of ${AI_TOTAL} creates"
kill_node "${AI_LEADER}"
RESUME_AT=$(( BASE + RESUME_WRITES ))
[ "${RESUME_AT}" -le "${AI_TOTAL}" ] || RESUME_AT="${AI_TOTAL}"
wait_until "${RESUME_WAIT_S}" "the creates resuming" progress_at_least "${RESUME_AT}" ai-create \
    || { out_of ai-create-0 | tail -20 >&2; fail "no progress past ${RESUME_AT} within ${RESUME_WAIT_S}s of the kill (at $(race_progress ai-create))"; }
restart_node "${AI_LEADER}"
log "Leg 7: node ${AI_LEADER} restarted; waiting for the creators..."
for i in $(seq 0 $(( WORKERS - 1 ))); do
    left=$(( RACE_S - ($(now_s) - AI_START) ))
    [ "${left}" -gt 0 ] || left=1
    wait_until "${left}" "creator ${i}" bg_done "ai-create-${i}" || fail "creator ${i} did not finish within ${RACE_S}s of the start"
    [ "$(bg_rc "ai-create-${i}")" = "0" ] || { out_of "ai-create-${i}" | tail -20 >&2; fail "creator ${i} exited $(bg_rc "ai-create-${i}")"; }
done
ai_files=("${SHARED}"/ai-create-w*.txt)
[ "${#ai_files[@]}" -eq "${WORKERS}" ] || fail "${#ai_files[@]} create result files, want ${WORKERS}"
awk -v total="${AI_TOTAL}" -v top=$(( AI_IMPORT_MAX - 1 )) '
    { n++
      if ($3 != 0) { other[$3]++; next }
      if ($1 <= top) { if (low++ < 10) printf "id %d is not above %d\n", $1, top; bad = 1 }
      if (seen[$1]++) { if (dup++ < 10) printf "id %d was returned twice\n", $1; bad = 1 } }
    END {
        for (c in other) { printf "result code %s x%d\n", c, other[c]; bad = 1 }
        if (n != total) { printf "%d results, want %d\n", n, total; bad = 1 }
        exit bad
    }' "${ai_files[@]}" > "${WORK_DIR}/ai-create-check.txt" \
    || { cat "${WORK_DIR}/ai-create-check.txt" >&2; fail "the creators broke 'unique ids above ${AI_IMPORT_MAX}, all ok' (codes 10013 = StoredIdUnknown, 10014 = VerdictUnknown, 10002 = Timeout)"; }
AI_TOP=$(awk '$1 > max { max = $1 } END { print max }' "${ai_files[@]}")
log "Leg 7: ${AI_TOTAL} unique ids above $(( AI_IMPORT_MAX - 1 )), highest ${AI_TOP}; $(( $(now_s) - AI_START )) s"
leg ai-verify "${LEG_S}" "CHECKED-WRITES-AI-VERIFY: OK" ai-verify

log "Leg 7d: AI-DELETE-TOP: removing id ${AI_TOP}, restarting all three nodes, one more create..."
PROBE_USER=app PROBE_PW=app.pw leg ai-remove "${LEG_S}" "CHECKED-WRITES-AI-REMOVE: OK" ai-remove --ids "${AI_TOP}-${AI_TOP}"
restart_all
PROBE_USER=app PROBE_PW=app.pw leg ai-after "${LEG_S}" "CHECKED-WRITES-AI-CREATE: DONE" ai-create --worker 0 --count 1 --tag ai-after
AI_AFTER=$(awk 'NR == 1 && $3 == 0 { print $1 }' "${SHARED}/ai-after-w0.txt")
[ -n "${AI_AFTER}" ] || { cat "${SHARED}/ai-after-w0.txt" >&2; fail "the create after the restart was not ok"; }
[ "${AI_AFTER}" -gt "${AI_TOP}" ] || fail "the create after the restart gave id ${AI_AFTER}, not above the removed ${AI_TOP}"
log "Leg 7: the create after the restart gave ${AI_AFTER}, above the removed ${AI_TOP}"

log "Leg 7e: AI-REFUSALS: a non-granted import, autoincrement on a delete topic, IMPORT on a view..."
PROBE_USER=app PROBE_PW=app.pw leg ai-denied "${LEG_S}" "CHECKED-WRITES-AI-BACKFILL: DONE" ai-backfill --ids 2-2 --tag ai-denied
[ "$(awk '{ print $3 }' "${SHARED}/ai-denied-w0.txt")" = "45" ] || { cat "${SHARED}/ai-denied-w0.txt" >&2; fail "app importing id 2 did not give 45"; }
leg ai-refusals "${LEG_S}" "CHECKED-WRITES-AI-REFUSALS: OK" ai-refusals
set +e
out=$(rune_auth_cli_admin "${RUNE_AUTH_NETWORK}" ql -e "grant import on view 'ai_view' to user app;" --broker "rune-${SEED_NODE}:${BROKER_PORT}" 2>&1); rc=$?
set -e
[ "${rc}" -ne 0 ] || fail "GRANT IMPORT ON VIEW was accepted"
[[ "${out}" == *InvalidGrant* ]] || { echo "${out}" >&2; fail "GRANT IMPORT ON VIEW was refused, but not as InvalidGrant"; }
log "Leg 7 OK"
echo "AUTOINC: OK"

# ---------------------------------------------------------------------------
# Leg 8: REGRESSORSPATH.
# ---------------------------------------------------------------------------
LEG="leg 8"
GOLDEN="${PROJECTS}/ql/tests/golden/regressorspath.rql"
[ -f "${GOLDEN}" ] || fail "missing ../ql/tests/golden/regressorspath.rql"
[ $(( RP_USERS % 2 )) -eq 0 ] || fail "RP_E2E_USERS must be even (${RP_USERS})"
[ "${RP_LIFE_USERS}" -le "${RP_USERS}" ] || fail "RP_E2E_LIFECYCLE_USERS (${RP_LIFE_USERS}) is above RP_E2E_USERS (${RP_USERS})"
[ "${WORKERS}" -ge 2 ] || fail "the Foo / foo pairs need KI_E2E_WORKERS >= 2"

LEG="leg 8a RP-SETUP"
log "Leg 8a: RP-SETUP: the golden DDL through the CLI..."
# The brokers' default segment is far larger than the run: a small one seals the segments that hold the refused records, and a long
# compaction lag keeps the cleaner off them until RP-VERIFY has looked (the `alter topic` below lowers it).
rp_ddl=$(cat "${GOLDEN}")
rp_ddl="${rp_ddl//"with (cleanup.policy = compact)"/"with (cleanup.policy = compact, segment.bytes = 65536, min.compaction.lag.ms = 86400000)"}"
[[ "${rp_ddl}" == *"segment.bytes = 65536"* ]] || fail "the golden script has no 'with (cleanup.policy = compact)' to add segment.bytes to"
printf '%s\n' "${rp_ddl}" > "${RUNE_AUTH_WORK_DIR}/regressorspath.rql"
set +e
out=$(rune_auth_cli_admin "${RUNE_AUTH_NETWORK}" ql -f /work/regressorspath.rql --broker "rune-${SEED_NODE}:${BROKER_PORT}" 2>&1); rc=$?
set -e
rm -f "${RUNE_AUTH_WORK_DIR}/regressorspath.rql"
[ "${rc}" -eq 0 ] || { echo "${out}" >&2; fail "the golden regressorspath.rql failed (exit ${rc})"; }
set +e
out=$(rune_auth_cli_admin "${RUNE_AUTH_NETWORK}" ql -e "show view account_stats;" --broker "rune-${SEED_NODE}:${BROKER_PORT}" 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" >&2; fail "show view account_stats failed (exit ${rc})"; }
[[ "${out}" == *max_characters* ]] || { echo "${out}" >&2; fail "show view account_stats does not list max_characters"; }
leg rp-setup 300 "CHECKED-WRITES-RP-SETUP: OK" rp-setup
RP_USER_TOPIC=$(grep -oE '^RP_USER_TOPIC_ID=[0-9]+' "${WORK_DIR}/rp-setup.out" | cut -d= -f2) || true
RP_CHARACTER_TOPIC=$(grep -oE '^RP_CHARACTER_TOPIC_ID=[0-9]+' "${WORK_DIR}/rp-setup.out" | cut -d= -f2) || true
[ -n "${RP_USER_TOPIC}" ] && [ -n "${RP_CHARACTER_TOPIC}" ] || fail "rp-setup printed no topic ids"
wait_until 120 "the user leader" partition_leader "${RP_USER_TOPIC}" 0 || fail "user partition 0 has no leader"
wait_until 120 "the character leader" partition_leader "${RP_CHARACTER_TOPIC}" 0 || fail "character partition 0 has no leader"

# rp_race <tag> <topic id> <total writes> <probe mode> <range flag> <range> : WORKERS probes of <mode> race, the topic's leader is
# killed at ~30 % and restarted once progress resumes; returns when every probe exited 0.
rp_race() {
    local tag="$1" topic="$2" total="$3" mode="$4" flag="$5" range="$6" i leader left start
    leader=$(partition_leader "${topic}" 0) || fail "no leader of the ${tag} topic"
    if [ "${leader}" = "0" ]; then SEED_NODE=2; else SEED_NODE=1; fi   # the probes never connect through the node the race kills
    start=$(now_s)
    for i in $(seq 0 $(( WORKERS - 1 ))); do probe_start "${tag}-${i}" "${RACE_S}" "${mode}" --worker "${i}" "${flag}" "${range}"; done
    wait_until "${RACE_S}" "30 % of ${tag}" progress_at_least $(( total * 30 / 100 )) "${tag}" \
        || { out_of "${tag}-0" | tail -20 >&2; fail "${tag} never reached 30 % within ${RACE_S}s (at $(race_progress "${tag}") of ${total})"; }
    leader=$(partition_leader "${topic}" 0) || fail "no leader of the ${tag} topic at the kill"
    BASE=$(race_progress "${tag}")
    log "Leg 8: ${tag}: killing node ${leader}, the leader, at ${BASE} of ${total} writes"
    kill_node "${leader}"
    RESUME_AT=$(( BASE + RESUME_WRITES ))
    [ "${RESUME_AT}" -le "${total}" ] || RESUME_AT="${total}"
    wait_until "${RESUME_WAIT_S}" "${tag} resuming" progress_at_least "${RESUME_AT}" "${tag}" \
        || { out_of "${tag}-0" | tail -20 >&2; fail "${tag}: no progress past ${RESUME_AT} within ${RESUME_WAIT_S}s of the kill (at $(race_progress "${tag}"))"; }
    restart_node "${leader}"
    for i in $(seq 0 $(( WORKERS - 1 ))); do
        left=$(( RACE_S - ($(now_s) - start) ))
        [ "${left}" -gt 0 ] || left=1
        wait_until "${left}" "${tag} probe ${i}" bg_done "${tag}-${i}" || fail "${tag} probe ${i} did not finish within ${RACE_S}s of the race start"
        [ "$(bg_rc "${tag}-${i}")" = "0" ] || { out_of "${tag}-${i}" | tail -20 >&2; fail "${tag} probe ${i} exited $(bg_rc "${tag}-${i}")"; }
    done
    log "Leg 8: ${tag}: done in $(( $(now_s) - start )) s"
}

LEG="leg 8b RP-LOGIN-RACE"
log "Leg 8b: RP-LOGIN-RACE: ${WORKERS} probes race first logins over ${RP_SUBJECTS} subjects; the user leader is killed at ~30 %..."
rp_race rp-login "${RP_USER_TOPIC}" $(( WORKERS * RP_SUBJECTS )) rp-login --subjects "1-${RP_SUBJECTS}"
rp_files=("${SHARED}"/rp-login-w*.txt)
[ "${#rp_files[@]}" -eq "${WORKERS}" ] || fail "${#rp_files[@]} login result files, want ${WORKERS}"
awk -v subjects="${RP_SUBJECTS}" -v workers="${WORKERS}" '
    { if ($3 != 0 && $3 != 55) { other[$3]++; next }
      n[$2]++
      if ($3 == 0) { ok[$2]++; winner[$2] = $7 }
      else { if ($4 != "user_identity.unique") { wrong[$4]++ } lose[$2]++; losers[$2] = losers[$2] " " $7 } }
    END {
        for (c in other) { printf "result code %s x%d\n", c, other[c]; bad = 1 }
        for (c in wrong) { printf "a refusal named %s x%d\n", c, wrong[c]; bad = 1 }
        for (s = 1; s <= subjects; s++) {
            if (n[s] != workers || ok[s] != 1 || lose[s] != workers - 1) {
                if (shown++ < 10) printf "subject %d: %d results, %d Ok, %d refused\n", s, n[s], ok[s], lose[s]
                bad = 1; continue
            }
            if (winner[s] < 1 || issued[winner[s]]++) { if (shown++ < 10) printf "subject %d: userId %s is not a fresh id\n", s, winner[s]; bad = 1 }
            m = split(losers[s], l, " ")
            for (j = 1; j <= m; j++) if (l[j] != winner[s]) { if (shown++ < 10) printf "subject %d: a loser read userId %s, the winner is %s\n", s, l[j], winner[s]; bad = 1 }
        }
        exit bad
    }' "${rp_files[@]}" > "${WORK_DIR}/rp-login-check.txt" \
    || { cat "${WORK_DIR}/rp-login-check.txt" >&2; fail "the login race broke 'one winner per subject, the rest 55 user_identity.unique, losers read the winner' (codes 10014 = VerdictUnknown, 10013 = StoredIdUnknown, 10002 = Timeout)"; }
log "Leg 8b OK: every subject has one winner and ${WORKERS} - 1 refusals that read the winner's userId"

LEG="leg 8c RP-CHARACTERS"
log "Leg 8c: RP-CHARACTERS: ${RP_USERS} users, ${WORKERS} probes create ${RP_CHARS} characters per user plus the Foo / foo pairs; the character leader is killed at ~30 %..."
rp_race rp-characters "${RP_CHARACTER_TOPIC}" $(( WORKERS * RP_CHARS * RP_USERS + RP_USERS )) rp-characters --users "1-${RP_USERS}"
rp_files=("${SHARED}"/rp-characters-w*.txt)
[ "${#rp_files[@]}" -eq "${WORKERS}" ] || fail "${#rp_files[@]} character result files, want ${WORKERS}"
awk -v users="${RP_USERS}" -v workers="${WORKERS}" -v per="${RP_CHARS}" -v cap="${RP_CAP}" -v pairbase="${RP_PAIR_BASE}" '
    { if ($3 != 0 && $3 != 55) { other[$3]++; next }
      if ($2 >= pairbase) { g = tolower($6); pn[g]++; if ($3 == 0) pok[g]++; else if ($4 == "character_name.unique") prej[g]++; next }
      n[$2]++
      if ($3 == 0) ok[$2]++; else if ($4 == "account_stats.max_characters") rej[$2]++ }
    END {
        for (c in other) { printf "result code %s x%d\n", c, other[c]; bad = 1 }
        for (u = 1; u <= users; u++)
            if (n[u] != workers * per || ok[u] != cap || rej[u] != workers * per - cap) {
                if (shown++ < 10) printf "user %d: %d results, %d Ok, %d account_stats.max_characters\n", u, n[u], ok[u], rej[u]
                bad = 1
            }
        for (u = 1; u <= users; u += 2) {
            g = "foo" u
            if (pn[g] != 2 || pok[g] != 1 || prej[g] != 1) {
                if (shown++ < 10) printf "pair %s: %d results, %d Ok, %d character_name.unique\n", g, pn[g], pok[g], prej[g]
                bad = 1
            }
        }
        exit bad
    }' "${rp_files[@]}" > "${WORK_DIR}/rp-characters-check.txt" \
    || { cat "${WORK_DIR}/rp-characters-check.txt" >&2; fail "the character race broke '12 Ok per user, the rest 55 account_stats.max_characters, one winner per Foo / foo pair'"; }
log "Leg 8c OK: every user has ${RP_CAP} characters, every pair one winner"

LEG="leg 8d RP-RESTART"
log "Leg 8d: RP-RESTART: docker restart of all three nodes; every account_stats row is ${RP_CAP}, a 13th create is refused..."
restart_all
if [ "$(partition_leader "${RP_CHARACTER_TOPIC}" 0)" = "0" ]; then SEED_NODE=2; else SEED_NODE=1; fi
leg rp-restart "${RACE_S}" "CHECKED-WRITES-RP-RESTART: DONE" rp-restart --worker 0 --users "1-${RP_USERS}"
awk -v users="${RP_USERS}" -v cap="${RP_CAP}" '
    { if ($1 == "stats") { st[$2]++; if ($3 != 0 || $7 != cap) { if (shown++ < 10) printf "user %d: account_stats says %s (code %d)\n", $2, $7, $3; bad = 1 } }
      else if ($1 == "create") { cr[$2]++; if ($3 != 55 || $4 != "account_stats.max_characters") { if (shown++ < 10) printf "user %d: the 13th create gave %d %s\n", $2, $3, $4; bad = 1 } } }
    END { for (u = 1; u <= users; u++) if (st[u] != 1 || cr[u] != 1) { if (shown++ < 10) printf "user %d: %d stats, %d creates\n", u, st[u], cr[u]; bad = 1 } exit bad }' \
    "${SHARED}/rp-restart-w0.txt" > "${WORK_DIR}/rp-restart-check.txt" \
    || { cat "${WORK_DIR}/rp-restart-check.txt" >&2; fail "after the restart a user did not have ${RP_CAP} characters, or a 13th create was not refused"; }
log "Leg 8d OK"

LEG="leg 8e RP-LIFECYCLE"
log "Leg 8e: RP-LIFECYCLE: ${RP_LIFE_USERS} users remove a character, another user reuses its name, the second remove gives 57..."
leg rp-lifecycle "${RACE_S}" "CHECKED-WRITES-RP-LIFECYCLE: DONE" rp-lifecycle --worker 0 --users "1-${RP_LIFE_USERS}"
awk -v users="${RP_LIFE_USERS}" -v lifebase="${RP_LIFE_BASE}" '
    { if ($1 == "remove") { if ($3 == 0) rm0[$2]++; else if ($3 == 57) rm57[$2]++; else { if (shown++ < 10) printf "user %d: remove gave %d\n", $2, $3; bad = 1 } }
      else if ($1 == "create") { if ($3 == 0) cr[$2 - lifebase]++; else { if (shown++ < 10) printf "user %d: the name reuse gave %d\n", $2 - lifebase, $3; bad = 1 } }
      else if ($1 == "stats") { st[$2]++; if ($3 != 0 || $7 != 11) { if (shown++ < 10) printf "user %d: account_stats says %s after the remove\n", $2, $7; bad = 1 } } }
    END { for (u = 1; u <= users; u++) if (rm0[u] != 1 || rm57[u] != 1 || cr[u] != 1 || st[u] != 1) { if (shown++ < 10) printf "user %d: %d removes ok, %d removes 57, %d reuses, %d counts\n", u, rm0[u], rm57[u], cr[u], st[u]; bad = 1 } exit bad }' \
    "${SHARED}/rp-lifecycle-w0.txt" > "${WORK_DIR}/rp-lifecycle-check.txt" \
    || { cat "${WORK_DIR}/rp-lifecycle-check.txt" >&2; fail "the lifecycle broke 'remove ok, count 11, name reused, second remove 57'"; }
log "Leg 8e OK"

LEG="leg 8f RP-VERIFY"
log "Leg 8f: RP-VERIFY: a subscription from 0 on user and character against the refused writes, then compaction..."
leg rp-verify "${RACE_S}" "CHECKED-WRITES-RP-VERIFY: OK" rp-verify
for t in user character; do
    set +e
    out=$(rune_auth_cli_admin "${RUNE_AUTH_NETWORK}" ql -e "alter topic ${t} set delete.retention.ms = 0, min.cleanable.dirty.ratio = 0.000001, min.compaction.lag.ms = 50;" --broker "rune-${SEED_NODE}:${BROKER_PORT}" 2>&1); rc=$?
    set -e
    [ "${rc}" -eq 0 ] || { echo "${out}" >&2; fail "alter topic ${t} failed (exit ${rc})"; }
done
leg rp-compact "${RACE_S}" "CHECKED-WRITES-RP-COMPACT: OK" rp-compact
# Only a partition's leader compacts; a follower keeps what it fetched.
rp_not_compacted_zero() {   # <topic name> <topic id>
    local leader out
    leader=$(partition_leader "$2" 0) || return 1
    out=$(_rune_auth_cli "${RUNE_AUTH_NETWORK}" stats topic "$1" --json --broker "rune-$(( leader + 1 )):${BROKER_PORT}" --user "${RUNE_AUTH_ADMIN}" --password-file /work/admin.pw 2>&1) || return 1
    [[ "${out}" == *'"rejectedNotCompacted": 0'* ]]
}
wait_until "${LEG_S}" "the user topic compacted" rp_not_compacted_zero user "${RP_USER_TOPIC}" || fail "user: rejectedNotCompacted is not 0 on the leader within ${LEG_S}s"
wait_until "${LEG_S}" "the character topic compacted" rp_not_compacted_zero character "${RP_CHARACTER_TOPIC}" || fail "character: rejectedNotCompacted is not 0 on the leader within ${LEG_S}s"
log "Leg 8 OK"
echo "REGRESSORSPATH: OK"

# ---------------------------------------------------------------------------
# Leg 9: TXN.
# ---------------------------------------------------------------------------
LEG="leg 9a TXN-SETUP"
log "Leg 9a: TXN-SETUP: the ledger topic and the balances view through the CLI..."
printf '%s\n' "topic ledger { entryId uint64, account uint64, amount int64, memo string(16) } partitions 1 replicas 3 key entryId with (cleanup.policy = compact);" \
    "create view balances key account as select sum(amount) as balance from ledger group by account constraint non_negative check (balance >= 0);" \
    > "${RUNE_AUTH_WORK_DIR}/ledger.rql"
set +e
out=$(rune_auth_cli_admin "${RUNE_AUTH_NETWORK}" ql -f /work/ledger.rql --broker "rune-${SEED_NODE}:${BROKER_PORT}" 2>&1); rc=$?
set -e
rm -f "${RUNE_AUTH_WORK_DIR}/ledger.rql"
[ "${rc}" -eq 0 ] || { echo "${out}" >&2; fail "the ledger DDL failed (exit ${rc})"; }
leg txn-setup 300 "CHECKED-WRITES-TXN-SETUP: OK" txn-setup
TXN_LEDGER_TOPIC=$(grep -oE '^TXN_LEDGER_TOPIC_ID=[0-9]+' "${WORK_DIR}/txn-setup.out" | cut -d= -f2) || true
[ -n "${TXN_LEDGER_TOPIC}" ] || fail "txn-setup printed no ledger topic id"
wait_until 120 "the ledger leader" partition_leader "${TXN_LEDGER_TOPIC}" 0 || fail "ledger partition 0 has no leader"

LEG="leg 9b TXN-RACE"
log "Leg 9b: TXN-RACE: ${WORKERS} probes run ${TXN_PER_USER} transactions per user over ${TXN_USERS} users in two halves; the character leader is killed at ~30 %, the coordinator at ~60 %..."
txn_half=$(( TXN_USERS / 2 ))
txn_total=$(( WORKERS * TXN_USERS * TXN_PER_USER ))
txn_start=$(now_s)
txn_windows="${WORK_DIR}/txn-kill-windows.txt"   # "<kill ms> <resumed ms>" per kill: a lost answer is allowed only inside one
: > "${txn_windows}"
# txn_kill <node 0-2> <progress share in percent> <what> <progress at the running half's end>: kill it at the share of all the
# transactions, restart it once progress resumes
txn_kill() {
    wait_until "${RACE_S}" "${3} kill point" progress_at_least $(( txn_total * $2 / 100 )) txn-run \
        || { out_of "txn-run-0" | tail -20 >&2; fail "txn-run never reached $2 % within ${RACE_S}s (at $(race_progress txn-run) of ${txn_total})"; }
    BASE=$(race_progress txn-run)
    log "Leg 9: killing node $1, the ${3}, at ${BASE} of ${txn_total} transactions"
    local killed_ms
    killed_ms=$(now_ms)
    kill_node "$1"
    RESUME_AT=$(( BASE + 20 ))
    [ "${RESUME_AT}" -le "$4" ] || RESUME_AT="$4"
    wait_until "${RESUME_WAIT_S}" "txn-run resuming" progress_at_least "${RESUME_AT}" txn-run \
        || { out_of "txn-run-0" | tail -20 >&2; fail "txn-run: no progress past ${RESUME_AT} within ${RESUME_WAIT_S}s of the ${3} kill"; }
    restart_node "$1"
    # the window ends once progress advances again after the restart
    BASE=$(race_progress txn-run)
    RESUME_AT=$(( BASE + 20 ))
    [ "${RESUME_AT}" -le "$4" ] || RESUME_AT="$4"
    wait_until "${RESUME_WAIT_S}" "txn-run resuming after the ${3} restart" progress_at_least "${RESUME_AT}" txn-run \
        || { out_of "txn-run-0" | tail -20 >&2; fail "txn-run: no progress past ${RESUME_AT} within ${RESUME_WAIT_S}s of the ${3} restart"; }
    echo "${killed_ms} $(now_ms)" >> "${txn_windows}"
}
# txn_half_run <first worker number> <first user> <last user> : WORKERS probes over the users; the seed node is neither the character
# leader nor the coordinator (a probe does not outlive its seed), so each half starts by reading both.
txn_half_run() {
    local first="$1" lo="$2" hi="$3" i leader coord n
    leader=$(partition_leader "${RP_CHARACTER_TOPIC}" 0) || fail "no leader of the character topic"
    coord=$(consensus_leader) || fail "no consensus leader"
    SEED_NODE=1
    for n in 1 2 3; do
        if [ "$(( n - 1 ))" != "${leader}" ] && [ "$(( n - 1 ))" != "${coord}" ]; then SEED_NODE="${n}"; break; fi
    done
    TXN_COORDINATOR="${coord}"
    for i in $(seq 0 $(( WORKERS - 1 ))); do probe_start "txn-run-$(( first + i ))" "${RACE_S}" txn-run --worker "$(( first + i ))" --users "${lo}-${hi}"; done
}
txn_half_wait() {   # <first worker number> : every probe of the half exited 0, within the race bound
    local first="$1" i left
    for i in $(seq 0 $(( WORKERS - 1 ))); do
        left=$(( RACE_S - ($(now_s) - txn_start) ))
        [ "${left}" -gt 0 ] || left=1
        wait_until "${left}" "txn-run probe $(( first + i ))" bg_done "txn-run-$(( first + i ))" || fail "txn-run probe $(( first + i )) did not finish within ${RACE_S}s of the race start"
        [ "$(bg_rc "txn-run-$(( first + i ))")" = "0" ] || { out_of "txn-run-$(( first + i ))" | tail -20 >&2; fail "txn-run probe $(( first + i )) exited $(bg_rc "txn-run-$(( first + i ))")"; }
    done
}
# First half: the character leader is killed at ~30 % of all the transactions (60 % of this half).
txn_half_run 0 $(( TXN_USER_BASE + 1 )) $(( TXN_USER_BASE + txn_half ))
txn_kill "$(partition_leader "${RP_CHARACTER_TOPIC}" 0)" 30 "character leader" $(( WORKERS * txn_half * TXN_PER_USER ))
txn_half_wait 0
# Second half, started with the coordinator read afresh: it is killed at ~60 % of all the transactions (20 % of this half).
txn_half_run "${WORKERS}" $(( TXN_USER_BASE + txn_half + 1 )) $(( TXN_USER_BASE + TXN_USERS ))
coordinator=$(consensus_leader) || fail "no consensus leader at the coordinator kill"
[ "${coordinator}" = "${TXN_COORDINATOR}" ] || fail "the coordinator moved from node ${TXN_COORDINATOR} to ${coordinator} while the second half started"
txn_kill "${coordinator}" 60 "coordinator" "${txn_total}"
txn_half_wait "${WORKERS}"
log "Leg 9b: done in $(( $(now_s) - txn_start )) s"
txn_files=("${SHARED}"/txn-run-w*.txt)
[ "${#txn_files[@]}" -eq $(( 2 * WORKERS )) ] || fail "${#txn_files[@]} transaction result files, want $(( 2 * WORKERS ))"
awk -v per="${TXN_PER_USER}" -v workers="${WORKERS}" -v fitting="${TXN_FITTING}" -v users="${TXN_USERS}" -v ubase="${TXN_USER_BASE}" \
    -v limit="$(( TXN_TIMEOUT_MS + TXN_PASS_MS ))" -v windows="$(tr '\n' ';' < "${txn_windows}")" '
    BEGIN { nw = split(windows, w, ";") }
    { if ($9 - $8 > limit) { if (shown++ < 10) printf "%s: took %d ms\n", $6, $9 - $8; bad = 1 }
      if ($3 == 20000) {
          inside = 0
          for (i = 1; i < nw; i++) { split(w[i], b, " "); if ($8 <= b[2] && $9 >= b[1]) inside = 1 }
          if (!inside) { if (shown++ < 10) printf "%s: a lost answer outside every kill window\n", $6; bad = 1 }
      }
      if ($3 != 0 && $3 != 55 && $3 != 20000) { other[$3]++; next }
      if ($6 ~ /od$/) { if ($3 == 0) { if (shown++ < 10) printf "%s: an overdraw committed\n", $6; bad = 1 } next }
      u = $2; n[u]++
      if ($3 == 0) ok[u]++; else if ($3 == 20000) lost[u]++ }
    END {
        for (c in other) { printf "result code %s x%d\n", c, other[c]; bad = 1 }
        for (i = 1; i <= users; i++) {
            u = ubase + i
            if (n[u] != workers * (per - 1) || ok[u] > fitting || (lost[u] == 0 && ok[u] != fitting)) {
                if (shown++ < 10) printf "user %d: %d transactions, %d Ok, %d lost\n", u, n[u], ok[u], lost[u]
                bad = 1
            }
        }
        exit bad
    }' "${txn_files[@]}" > "${WORK_DIR}/txn-run-check.txt" \
    || { cat "${WORK_DIR}/txn-run-check.txt" >&2; fail "the transaction race broke 'per user at most ${TXN_FITTING} Ok, every other answer 55, no overdraw commits' (20000 = a lost answer, only inside a kill window; none longer than ${TXN_TIMEOUT_MS} ms + ${TXN_PASS_MS} ms)"; }
log "Leg 9b OK"

LEG="leg 9c TXN-VERIFY"
log "Leg 9c: TXN-VERIFY: a read_committed subscription from 0 against the committed, refused and lost transactions..."
leg txn-verify "${RACE_S}" "CHECKED-WRITES-TXN-VERIFY: OK" txn-verify

LEG="leg 9d TXN-RESTART"
log "Leg 9d: TXN-RESTART: docker restart of all three nodes; the verification again, then a new transaction..."
restart_all
if [ "$(partition_leader "${RP_CHARACTER_TOPIC}" 0)" = "0" ]; then SEED_NODE=2; else SEED_NODE=1; fi
leg txn-verify-restart "${RACE_S}" "CHECKED-WRITES-TXN-VERIFY: OK" txn-verify
leg txn-run-restart "${RACE_S}" "CHECKED-WRITES-TXN-RUN: DONE" txn-run --worker 9 --users "$(( TXN_USER_BASE + TXN_USERS + 1 ))-$(( TXN_USER_BASE + TXN_USERS + 2 ))"
awk -v fitting="${TXN_FITTING}" -v per="${TXN_PER_USER}" -v u1="$(( TXN_USER_BASE + TXN_USERS + 1 ))" -v u2="$(( TXN_USER_BASE + TXN_USERS + 2 ))" '
    { if ($6 ~ /od$/) { if ($3 != 55) { if (shown++ < 10) printf "%s: the overdraw gave %d\n", $6, $3; bad = 1 } x = $6; sub(/^u/, "", x); sub(/od$/, "", x); od[x + 0]++; next }
      ok[$2] += ($3 == 0); refused[$2] += ($3 == 55) ; n[$2]++ }
    END { for (u = u1; u <= u2; u++)
              if (n[u] != per - 1 || od[u] != 1 || ok[u] != fitting || refused[u] != 1) { if (shown++ < 10) printf "user %d: %d transactions, %d Ok, %d refused, %d overdraws\n", u, n[u], ok[u], refused[u], od[u]; bad = 1 }
          exit bad }' \
    "${SHARED}/txn-run-w9.txt" > "${WORK_DIR}/txn-restart-check.txt" \
    || { cat "${WORK_DIR}/txn-restart-check.txt" >&2; fail "after the restart a fresh user did not get ${TXN_FITTING} committed transactions and one refused"; }
leg txn-verify-final "${RACE_S}" "CHECKED-WRITES-TXN-VERIFY: OK" txn-verify
log "Leg 9 OK"
echo "TXN: OK"

echo "CHECKED-WRITES-E2E: OK"
