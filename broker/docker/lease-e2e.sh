#!/usr/bin/env bash
# docker/lease-e2e.sh - lease topics Task 19: the Phase 1 end-to-end legs on 3 authenticated brokers, the DEFAULT config.
#
#   0. SETUP:      build the broker, the rune CLI and the lease_e2e_driver images; generate the cluster key; `up` 3 nodes
#                  (docker-compose.yml + docker-compose.auth.yml, no overlay: the default config); bootstrap the first admin;
#                  create the namespace `e2e` with its admin (e2e/admin) and the user e2e/app (every topic below lives in `e2e`);
#                  define the five lease topics, the GUARD data topic and one `lease topic` through QL;
#   1. BASIC:      grant -> a second grant is KeyExists -> a LeaseSession keeps it alive for 3 x TTL (live) -> keepalives stop
#                  at S (no revoke) -> `Expired` received by the cluster-admin watcher at <= S + TTL + BOUND_MS -> the lease
#                  view no longer holds the key -> a regrant has a larger version; the watcher checks exactly-once per
#                  (key, version) and its exit code is checked;
#   2. SCALE:      10 000 leases (3 LeaseSessions: one holds at most 4 095) kept alive for 2 x TTL, the holder SIGKILLed at K;
#                  every key's `Expired` exactly once, received at <= K + TTL + SCALE_BOUND_MS;
#   3. GUARD (R5): a producer bound to a lease sends sequence-numbered records; the driver container is cut from the lease
#                  leader (iptables); after onAtRisk(Entered) at E every send fails LeaseAtRisk; at the first pass at or after
#                  E + d an UNGUARDED producer (its own client session) appends a marker record (offset M), and no guarded
#                  record is at an offset > M (in-flight batches that failed LeaseAtRisk may still be appended within d of
#                  their send, spec 12.2: they sit before M) (data and lease leaders on different nodes);
#   4. CHECKPOINT: a 15 s one-shot lease (checkpoint on, interval 5 s), never kept alive; the watcher is seeded on a node W that
#                  does not lead the partition.  Kill 1: once a checkpoint has FOLDED (seen as lease_remaining_ms > 0 in the
#                  lease view row, R1) the partition leader is killed and restarted (K1, new leader seen at L1).  Kill 2: once
#                  a NEW checkpoint folded under the new leader (R2 < R1) that leader is killed too (K2, L2), unless it is W (the
#                  watcher's seed: then skipped and logged).  Bound (spec 9.6), for the last kill done:
#                  `Expired` received at <= L + extend + R + BOUND_MS (L stands in for T_ready; R of the checkpoint seen just before);
#   5. SUSPEND:    namespace e2e ReadOnly then RejectAll mid-TTL: an unkept lease still expires by S + TTL + BOUND_MS; resume;
#                  a new grant works.
#
# Clock.  Expire / Checkpoint records carry timestamp 0, so every lateness is measured on RECEIVE time: the watcher stamps each
# event with CLOCK_REALTIME (ms) in its handler, and the script's `date +%s%3N` is the same domain (containers share the host
# kernel clock).  Receive time >= append time, so `received <= bound` proves `appended <= bound`.  NO lower bound (not early) is
# asserted from receive time: early expiry is the seeded stress test's job (lease topics Task 18, spec 16.4 I3).
#
# Usage (on the Linux test VM, under the host lock):
#   ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/lease-e2e.sh'
# Environment (all optional):
#   FLIGHT_RECORDER_DIR   the flight-recorder checkout (default: <monorepo>/../flight-recorder)
#   ARCANUM_METRICS_DIR   the arcanum-metrics checkout (default: <monorepo>/../arcanum-metrics)
#   BOUND_MS              the lateness bound of BASIC / SUSPEND / CHECKPOINT (default 1500)
#   SCALE_BOUND_MS        the lateness bound of SCALE (default 5000)
#   E2E_LOG_DIR           when set, the brokers', the drivers' and the watchers' logs are kept there at teardown
#   LEASE_E2E_KEEP_IMAGES set to 1 to keep the images this run built (default: removed by exact name).  Only images that did
#                         not exist before this run are removed: the broker, the driver and the rune CLI (QL) image
#
# House style: bounded waits, trap teardown (`docker compose down -v` on every exit path), every failure names its leg.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
export FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
export ARCANUM_METRICS_DIR="${ARCANUM_METRICS_DIR:-${PROJECTS}/../arcanum-metrics}"
CLIENT_REPO="${PROJECTS}/client-cpp"
COMPOSE_PROJECT="rune-lease-e2e"
NETWORK="${COMPOSE_PROJECT}_default"
WORK_DIR="$(mktemp -d "${BROKER_REPO}/.lease-e2e-work.XXXXXX")"
SHARED="${WORK_DIR}/shared"
mkdir -p "${SHARED}"

BOUND_MS="${BOUND_MS:-1500}"                # BASIC / SUSPEND / CHECKPOINT: received <= deadline + BOUND_MS
SCALE_BOUND_MS="${SCALE_BOUND_MS:-5000}"    # SCALE: received <= K + TTL + SCALE_BOUND_MS
# extend: a promoted leader re-arms every lease at T_ready + extend + remaining (spec 9.6).  extend = leaderHeartbeatTimeoutNs =
# max(electionTimeout, 3 x electionStatusInterval) (broker/libs/node/include/rune/node/NodeConfig.h:1247).  The e2e cluster's value
# comes from broker/docker/docker-compose.yml:56-57 (RUNE_ELECTION_TIMEOUT_MS=300, RUNE_ELECTION_STATUS_INTERVAL_MS=50): max(300, 150).
# (The NodeConfig.h:346/348 struct defaults, 150 / 20 ms, give 150 ms; the compose value is the larger and what runs here.)
EXTEND_MS=300
CKPT_INTERVAL_MS=5000
BROKER_PORT=40200
LEADER_WAIT_S=180      # a common consensus leader after `up`
LINK_WAIT_S=120        # a restarted node serving again
LEG_S=300              # one leg (SCALE: SCALE_LEG_S)
SCALE_LEG_S=600
BASIC_TTL_MS=3000
SCALE_TTL_MS=10000
SCALE_COUNT=10000
GUARD_TTL_MS=3000
CKPT_TTL_MS=15000
NS="e2e"

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"

log()  { echo "[lease-e2e] $*" >&2; }
LEG="preflight"   # the leg a failure belongs to
fail() { echo "[lease-e2e] FAIL (${LEG}): $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.lease-e2e-work*' -not -path './.auth.*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
rune_auth_init
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-lease-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"
DRIVER_IMAGE="rune-lease-e2e-driver:$(repo_tag "${CLIENT_REPO}")"
BUILT_IMAGES=""   # images this run created (removed at teardown by exact name); an image that existed before the run is never listed
BROKER_IMAGE="rune-broker-node:${RUNE_IMAGE_TAG}"
image_exists() { docker image inspect "$1" >/dev/null 2>&1; }
# Recorded BEFORE any build: whether each image the script builds is already there.
BROKER_PREEXISTED=0; QL_PREEXISTED=0; DRIVER_PREEXISTED=0
if image_exists "${BROKER_IMAGE}"; then BROKER_PREEXISTED=1; fi
if image_exists "${QL_IMAGE}"; then QL_PREEXISTED=1; fi
if image_exists "${DRIVER_IMAGE}"; then DRIVER_PREEXISTED=1; fi

# The default config: docker-compose.yml + docker-compose.auth.yml, NO overlay (checked-writes adds RUNE_VERDICT_INTERVAL_MS; this does not).
dc() {
    docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" -f "${SCRIPT_DIR}/docker-compose.auth.yml" "$@"
}
cname() { dc ps -a -q "rune-$1" | head -1; }   # the container id of node <n> (1-3)
now_s() { date +%s; }
now_ms() { date +%s%3N; }

[ -d "${FLIGHT_RECORDER_DIR}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
[ -d "${ARCANUM_METRICS_DIR}" ] || fail "missing arcanum-metrics checkout (set ARCANUM_METRICS_DIR)"
for d in protocol codec crypto ql client-cpp conan/aeron; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done
[ -f "${CLIENT_REPO}/docker/Dockerfile.lease-e2e-driver" ] || fail "missing client-cpp/docker/Dockerfile.lease-e2e-driver"

DRIVERS=""       # names of the driver containers this run started (removed at teardown)
DF_BEFORE="$(df -h / | tail -1)"
cluster_down() {
    local p
    for p in ${DRIVERS}; do docker rm -f "${p}" >/dev/null 2>&1 || true; done
    dc down -v --remove-orphans >/dev/null 2>&1 || true
    if [ "${LEASE_E2E_KEEP_IMAGES:-0}" != "1" ]; then
        for p in ${BUILT_IMAGES}; do docker rmi "${p}" >/dev/null 2>&1 || true; done
    fi
}
teardown() {
    local rc=$?
    log "Tearing down (exit ${rc})..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then
        mkdir -p "${E2E_LOG_DIR}"
        for n in 1 2 3; do docker logs --timestamps "$(cname "${n}")" > "${E2E_LOG_DIR}/rune-${n}.log" 2>&1 || true; done
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.err "${E2E_LOG_DIR}/" 2>/dev/null || true
        cp -f "${SHARED}"/* "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    cluster_down
    rm -rf "${WORK_DIR}" 2>/dev/null || true
    rune_auth_cleanup        # the cluster key and the generated passwords
    log "df before: ${DF_BEFORE}"
    log "df after:  $(df -h / | tail -1)"
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
# sleep_until_ms <target realtime ms> : bounded by the target itself (and 120 s).
sleep_until_ms() {
    local target="$1" cap=$(( $(now_s) + 120 ))
    while [ "$(now_ms)" -lt "${target}" ] && [ "$(now_s)" -lt "${cap}" ]; do sleep 0.2; done
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
partition_leader() { local r; r=$(partition_leader_ep "$1" "$2") || return 1; echo "${r%% *}"; }
# partition_leader_ep <topic id> <partition> : "<node> <epoch>" of the becomeLeader line with the highest epoch.
partition_leader_ep() {
    local topic="$1" part="$2" n best_e=-1 best_n="" line e node
    for n in 1 2 3; do
        line=$(docker logs "$(cname "${n}")" 2>&1 | grep -E "PartitionReplica\[node=[0-9]+\] becomeLeader topic=${topic} part=${part} epoch=[0-9]+" | tail -1) || true
        [ -n "${line}" ] || continue
        e=$(echo "${line}" | sed -E 's/.*epoch=([0-9]+).*/\1/')
        node=$(echo "${line}" | sed -E 's/.*node=([0-9]+)\].*/\1/')
        if [ "${e}" -ge "${best_e}" ]; then best_e="${e}"; best_n="${node}"; fi
    done
    [ -n "${best_n}" ] || return 1
    echo "${best_n} ${best_e}"
}
# leader_not <topic id> <node id> : partition 0 has a leader and it is not <node id>.
leader_not() { local l; l=$(partition_leader "$1" 0) || return 1; [ "${l}" != "$2" ]; }

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
# start_node <id> : starts the killed node and returns at once (the peer links are awaited by await_links).
start_node() {
    local n=$(( $1 + 1 ))
    # a restart inside the driver dir lock's 1 s heartbeat window of the kill is refused (same pid 1): wait it out
    [ "$(( $(now_s) - KILLED_AT ))" -ge 3 ] || sleep 3
    SINCE="$(date +%s)"   # epoch seconds: no timezone ambiguity
    docker start "$(cname "${n}")" >/dev/null || fail "docker start of rune-${n} failed"
}
await_links() {
    local n=$(( $1 + 1 ))
    wait_until "${LINK_WAIT_S}" "rune-${n}'s peer links" links_since "${n}" \
        || { docker logs --tail 60 "$(cname "${n}")" >&2; fail "rune-${n} did not re-establish both peer links within ${LINK_WAIT_S}s"; }
}
restart_node() { start_node "$1"; await_links "$1"; }
ip_of() { docker inspect -f "{{(index .NetworkSettings.Networks \"${NETWORK}\").IPAddress}}" "$(cname "$1")"; }

# ---- the rune CLI as a principal ---------------------------------------------------------------------------------------------
# cli <user> <password file in the work dir> <rune args...> : `rune <args> --user U --password-file /work/<file>` (like namespace-e2e's ns_as).
cli() {
    local u="$1" p="$2"; shift 2
    _rune_auth_cli "${RUNE_AUTH_NETWORK}" "$@" --user "${u}" --password-file "/work/${p}"
}

# ---- driver runs (lease_e2e_driver in its own container on the compose network) ---------------------------------------------
SEED_NODE=1            # the broker the drivers connect through (1-3)
DRV_NET_ADMIN=""       # set to 1 for a driver that needs iptables (--cap-add NET_ADMIN)
# drv_args <container name> <user> <password file in the work dir> : the docker-run arguments (one per line).
drv_args() {
    local name="$1" user="$2" pw="$3"
    printf -- '--name\n%s\n--hostname\n%s\n--network\n%s\n--platform\nlinux/amd64\n--shm-size=256m\n--security-opt\nseccomp=unconfined\n' "${name}" "${name}" "${NETWORK}"
    printf -- '-e\nRUNE_TARGET=aeron:udp?endpoint=rune-%s:%s\n-e\nRUNE_EGRESS_HOST=%s\n-e\nRUNE_EGRESS_PORT=40500\n' "${SEED_NODE}" "${BROKER_PORT}" "${name}"
    printf -- '-e\nRUNE_DATA_DIR=/tmp/fp\n-e\nRUNE_TIMEOUT_MS=30000\n-v\n%s:/shared\n' "${SHARED}"
    printf -- '-e\nRUNE_USER=%s\n-e\nRUNE_PASSWORD_FILE=/work/%s\n-e\nRUNE_TLS_CA_FILE=/work/ca.pem\n-v\n%s:/work:ro\n' "${user}" "${pw}" "${RUNE_AUTH_WORK_DIR}"
    [ -z "${DRV_NET_ADMIN}" ] || printf -- '--cap-add\nNET_ADMIN\n'
}
# drv_run <tag> <timeout_s> <user> <pw> <subcommand> [flags...] : foreground; sets DRV_RC; files ${WORK_DIR}/<tag>.out|.err.
DRV_RC=0
drv_run() {
    local tag="$1" t="$2" user="$3" pw="$4"; shift 4
    local name="rune-lease-${tag}"
    local -a envargs
    mapfile -t envargs < <(drv_args "${name}" "${user}" "${pw}")
    DRIVERS="${DRIVERS} ${name}"
    docker rm -f "${name}" >/dev/null 2>&1 || true
    set +e
    timeout -k 10 "${t}" docker run --rm "${envargs[@]}" "${DRIVER_IMAGE}" "$@" > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
    DRV_RC=$?
    set -e
    docker rm -f "${name}" >/dev/null 2>&1 || true
}
# drv_start <tag> <timeout_s> <user> <pw> <subcommand> [flags...] : background (the exit status lands in ${WORK_DIR}/<tag>.rc).
drv_start() {
    local tag="$1" t="$2" user="$3" pw="$4"; shift 4
    local name="rune-lease-${tag}"
    local -a envargs
    mapfile -t envargs < <(drv_args "${name}" "${user}" "${pw}")
    DRIVERS="${DRIVERS} ${name}"
    docker rm -f "${name}" >/dev/null 2>&1 || true
    rm -f "${WORK_DIR}/${tag}.rc" "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err"
    (
        set +e
        timeout -k 10 "${t}" docker run --rm "${envargs[@]}" "${DRIVER_IMAGE}" "$@" > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
        echo $? > "${WORK_DIR}/${tag}.rc"
    ) &
}
bg_done()  { [ -s "${WORK_DIR}/$1.rc" ]; }
bg_rc()    { cat "${WORK_DIR}/$1.rc"; }
out_of()   { cat "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null; }
out_has()  { grep -q -e "$2" "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null; }
# drv_leg <tag> <timeout_s> <user> <pw> <marker> <subcommand> [flags...] : one foreground driver run that must exit 0 and print its marker.
drv_leg() {
    local tag="$1" t="$2" user="$3" pw="$4" marker="$5"; shift 5
    drv_run "${tag}" "${t}" "${user}" "${pw}" "$@"
    [ "${DRV_RC}" -eq 0 ] || { out_of "${tag}" | tail -30 >&2; fail "${tag} exited ${DRV_RC}"; }
    out_has "${tag}" "${marker}" || { out_of "${tag}" | tail -30 >&2; fail "no ${marker} from ${tag}"; }
}
# bg_leg_done <tag> <marker> : a background driver that finished must have exited 0 with its marker.
bg_leg_check() {
    [ "$(bg_rc "$1")" = "0" ] || { out_of "$1" | tail -30 >&2; fail "$1 exited $(bg_rc "$1")"; }
    out_has "$1" "$2" || { out_of "$1" | tail -30 >&2; fail "no $2 from $1"; }
}

# ---- the watcher's event log (/shared/<name>.log): `EV <kind> <key> <version> <receive realtime ms>` --------------------------
has_event()  { grep -q "^EV $2 $3 " "${SHARED}/$1.log" 2>/dev/null; }                       # <log> <kind> <key>
event_ms()   { awk -v k="$3" -v kind="$2" '$1=="EV" && $2==kind && $3==k {print $5; exit}' "${SHARED}/$1.log"; }
event_ver()  { awk -v k="$3" -v kind="$2" '$1=="EV" && $2==kind && $3==k {print $4; exit}' "${SHARED}/$1.log"; }
# kv_of <file in shared> <prefix> <field index> : the field of the last line starting with the prefix.
line_of()    { grep "^$2" "${SHARED}/$1" 2>/dev/null | tail -1; }
has_line()   { grep -q "^$2" "${SHARED}/$1" 2>/dev/null; }

WATCH_CAP_S=$(( LEG_S + 60 ))   # a leg's watcher: the leg's cap + 60 s
# stop_watcher <tag> : touches the stop file, waits (bounded) for the watcher and requires exit 0 with its OK marker.
stop_watcher() {
    touch "${SHARED}/$1.stop"
    wait_until 60 "the $1 watcher to stop" bg_done "$1" || { out_of "$1" | tail -20 >&2; fail "the $1 watcher did not stop within 60s"; }
    bg_leg_check "$1" "LEASE-E2E-WATCH: OK"
}

LEG_START=0
leg_begin() { LEG="$1"; LEG_START="$(now_s)"; log "$1: $2"; }
leg_end()   { local cap="$2"; local el=$(( $(now_s) - LEG_START )); [ "${el}" -le "${cap}" ] || fail "the leg took ${el}s, over its ${cap}s cap"; log "$1 OK in ${el}s"; echo "LEASE-E2E-$1: OK"; }

# expect_late <leg> <what> <received ms> <deadline ms> <bound ms> : fails when received > deadline + bound; logs the lateness.
expect_late() {
    local what="$1" recv="$2" deadline="$3" bound="$4"
    [ -n "${recv}" ] || fail "${what}: no Expired received"
    local late=$(( recv - deadline ))
    log "${LEG}: ${what}: Expired received ${late} ms after its deadline (bound ${bound} ms)"
    [ "${late}" -le "${bound}" ] || fail "${what}: Expired received ${late} ms after its deadline, over the ${bound} ms bound"
}

# ---------------------------------------------------------------------------
# Setup: images, the key, the cluster, bootstrap, the namespace, the topics.
# ---------------------------------------------------------------------------
LEG="setup"
log "Setup a: building the broker image (tag ${RUNE_IMAGE_TAG})..."
set +e
out=$(dc build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
image_exists "${BROKER_IMAGE}" || fail "broker image not built: ${BROKER_IMAGE}"
[ "${BROKER_PREEXISTED}" = "1" ] || BUILT_IMAGES="${BUILT_IMAGES} ${BROKER_IMAGE}"

log "Setup b: building the rune CLI image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"
image_exists "${QL_IMAGE}" || fail "rune CLI image not built: ${QL_IMAGE}"
[ "${QL_PREEXISTED}" = "1" ] || BUILT_IMAGES="${BUILT_IMAGES} ${QL_IMAGE}"

log "Setup c: building the lease_e2e_driver image (${DRIVER_IMAGE})..."
set +e
out=$(docker build --platform linux/amd64 -f "${CLIENT_REPO}/docker/Dockerfile.lease-e2e-driver" \
    --build-context rune-protocol="${PROJECTS}/protocol" --build-context rune-codec="${PROJECTS}/codec" \
    --build-context rune-crypto="${PROJECTS}/crypto" --build-context aeron-recipe="${PROJECTS}/conan/aeron" --build-context flight-recorder="${FLIGHT_RECORDER_DIR}" \
    --build-context rune-ql="${PROJECTS}/ql" -t "${DRIVER_IMAGE}" "${CLIENT_REPO}" 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "lease_e2e_driver image build failed (exit ${rc})"; }
image_exists "${DRIVER_IMAGE}" || fail "lease_e2e_driver image not built: ${DRIVER_IMAGE}"
[ "${DRIVER_PREEXISTED}" = "1" ] || BUILT_IMAGES="${BUILT_IMAGES} ${DRIVER_IMAGE}"

log "Setup d: generating the cluster key..."
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"

log "Setup e: the 3-node cluster up (the default config)..."
dc up -d --no-build rune-1 rune-2 rune-3
log "Setup f: waiting for a common consensus leader (up to ${LEADER_WAIT_S}s)..."
wait_until "${LEADER_WAIT_S}" "a common consensus leader" cluster_has_leader \
    || { for n in 1 2 3; do docker logs --tail 40 "$(cname "${n}")" >&2 || true; done; fail "no common consensus leader within ${LEADER_WAIT_S}s"; }
log "Setup g: bootstrapping the first admin..."
rune_auth_prepare "${NETWORK}" "rune-1:${BROKER_PORT}" || fail "bootstrap"

log "Setup h: namespace ${NS}; ${NS}/admin (ADMIN ON NAMESPACE) creates ${NS}/app with topic/view grants..."
cli admin admin.pw namespace create "${NS}" --broker "rune-1:${BROKER_PORT}" >/dev/null || fail "rune namespace create ${NS} failed"
for u in eadmin app; do rune_auth_gen_password "${RUNE_AUTH_WORK_DIR}/${u}.pw" || fail "no password for ${u}"; done
(
    umask 077
    {
        printf "CREATE USER '%s/admin' WITH PASSWORD '%s';\n" "${NS}" "$(cat "${RUNE_AUTH_WORK_DIR}/eadmin.pw")"
        printf "GRANT ADMIN ON NAMESPACE %s TO USER '%s/admin';\n" "${NS}" "${NS}"
    } > "${RUNE_AUTH_WORK_DIR}/ns-admin.rql"
    {
        printf "CREATE USER app WITH PASSWORD '%s';\n" "$(cat "${RUNE_AUTH_WORK_DIR}/app.pw")"
        for op in READ WRITE CREATE ALTER DROP; do echo "GRANT ${op} ON TOPIC PREFIX '' TO USER app;"; done
        for op in READ CREATE ALTER DROP; do echo "GRANT ${op} ON VIEW PREFIX '' TO USER app;"; done
    } > "${RUNE_AUTH_WORK_DIR}/ns-app.rql"
)
cli admin admin.pw ql -f /work/ns-admin.rql --broker "rune-2:${BROKER_PORT}" >/dev/null || fail "creating ${NS}/admin failed"
cli "${NS}/admin" eadmin.pw ql -f /work/ns-app.rql --broker "rune-3:${BROKER_PORT}" >/dev/null || fail "creating ${NS}/app and its grants failed"
rm -f "${RUNE_AUTH_WORK_DIR}/ns-admin.rql" "${RUNE_AUTH_WORK_DIR}/ns-app.rql"
APP_USER="${NS}/app"

log "Setup i: the topics (lease topics through the driver; one more through QL)..."
topic_id() {   # <tag> -> the TOPIC_ID the driver printed
    grep -oE '^TOPIC_ID=[0-9]+' "${WORK_DIR}/$1.out" | cut -d= -f2
}
drv_leg setup-basic 300 "${APP_USER}" app.pw "LEASE-E2E-SETUP: OK" setup --topic basic --partitions 2 --ttl-ms "${BASIC_TTL_MS}"
drv_leg setup-scale 300 "${APP_USER}" app.pw "LEASE-E2E-SETUP: OK" setup --topic scale --partitions 4 --ttl-ms "${SCALE_TTL_MS}"
drv_leg setup-guard 300 "${APP_USER}" app.pw "LEASE-E2E-SETUP: OK" setup --topic guard --partitions 1 --ttl-ms "${GUARD_TTL_MS}"
drv_leg setup-guard-data 300 "${APP_USER}" app.pw "LEASE-E2E-SETUP: OK" setup --topic guard_data --raw 1
drv_leg setup-ckpt 300 "${APP_USER}" app.pw "LEASE-E2E-SETUP: OK" setup --topic ckpt --partitions 1 --ttl-ms "${CKPT_TTL_MS}" --checkpoint 1 --ckpt-interval-ms "${CKPT_INTERVAL_MS}"
GUARD_ID=$(topic_id setup-guard) || true;  [ -n "${GUARD_ID}" ] || fail "setup printed no id for guard"
GUARD_DATA_ID=$(topic_id setup-guard-data) || true;  [ -n "${GUARD_DATA_ID}" ] || fail "setup printed no id for guard_data"
CKPT_ID=$(topic_id setup-ckpt) || true;  [ -n "${CKPT_ID}" ] || fail "setup printed no id for ckpt"
# The `lease topic` DDL through QL (the rune-ql CLI image).  The typed driver cannot open it: the descriptor name is the topic name and
# is part of the schema fingerprint of the generated lease_claim type; so the QL topic is checked by its own `show topic`.
(umask 077; echo "lease topic ql_lease { claim string(32), a uint64, b string(8) } partitions 1 replicas 3 key claim with (lease.ttl = 5000, lease.checkpoint = on, lease.checkpoint_interval = 5000);" > "${RUNE_AUTH_WORK_DIR}/ql-lease.rql")
cli "${APP_USER}" app.pw ql -f /work/ql-lease.rql --broker "rune-1:${BROKER_PORT}" >/dev/null || fail "the QL 'lease topic' DDL failed"
rm -f "${RUNE_AUTH_WORK_DIR}/ql-lease.rql"
show=$(cli "${APP_USER}" app.pw ql -e "show topic ql_lease;" --broker "rune-2:${BROKER_PORT}" 2>&1) || { echo "${show}" >&2; fail "show topic ql_lease failed"; }
echo "${show}" | grep -qi "lease" || { echo "${show}" >&2; log "setup: WARNING: 'show topic ql_lease' does not mention the lease"; }
log "Setup OK: topics guard ${GUARD_ID}, guard_data ${GUARD_DATA_ID}, ckpt ${CKPT_ID}"

# A cluster-admin watcher on e2e/basic serves BASIC, and a fresh one SUSPEND (restarted per leg: the GUARD and CHECKPOINT legs between them
# kill any node, so no seed node can be guaranteed to survive from BASIC to SUSPEND; neither leg kills a node itself).  Each has its own
# explicit cap (WATCH_CAP_S = the leg's cap + 60 s, the driver's --max-s 20 s below the container timeout) and its exit code is checked
# when the leg ends (stop_watcher).  It is the cluster admin (a user of `default`): suspension never applies to
# cluster admins (ClientGateway::gateExempt_, libs/gateway/src/ClientGateway.cpp:9074; NamespaceSuspendTable.h), while RejectAll closes the sessions
# of every other user of ${NS} (ClientGateway.cpp:9165), a namespace admin (${NS}/admin) included.
# ---------------------------------------------------------------------------
# Leg 1: BASIC.
# ---------------------------------------------------------------------------
leg_begin BASIC "grant, KeyExists, kept alive for 3 x TTL (${BASIC_TTL_MS} ms), stopped, Expired within TTL + ${BOUND_MS} ms, regrant..."
rm -f "${SHARED}"/basic-watch.*
drv_start basic-watch "${WATCH_CAP_S}" admin admin.pw watch --topic "${NS}/basic" --log /shared/basic-watch.log --ready /shared/basic-watch.ready --stop-file /shared/basic-watch.stop --max-s $(( WATCH_CAP_S - 20 ))
wait_until 120 "the basic watcher ready" has_line basic-watch.ready READY || { out_of basic-watch | tail -20 >&2; fail "the basic watcher was not ready within 120s"; }

KEEP_MS=$(( 3 * BASIC_TTL_MS ))
drv_start basic-hold $(( LEG_S )) "${APP_USER}" app.pw hold --topic basic --key key-1 --ttl-ms "${BASIC_TTL_MS}" --keep-ms "${KEEP_MS}" --out /shared/basic-hold.out
wait_until 90 "key-1 granted" has_line basic-hold.out GRANTED || { out_of basic-hold | tail -20 >&2; fail "key-1 was not granted within 90s"; }
V1=$(line_of basic-hold.out GRANTED | awk '{print $2}')
[ -n "${V1}" ] || fail "no version in the GRANTED line"
drv_leg basic-grant2 120 "${APP_USER}" app.pw "LEASE-E2E-GRANT: OK" grant --topic basic --key key-1 --expect exists
log "BASIC: key-1 granted at version ${V1}; the second grant gave KeyExists; keeping it alive for ${KEEP_MS} ms..."
wait_until $(( KEEP_MS / 1000 + 60 )) "the holder to stop" bg_done basic-hold || { out_of basic-hold | tail -20 >&2; fail "the holder did not finish"; }
bg_leg_check basic-hold "LEASE-E2E-HOLD: OK"
out_has basic-hold "live_before_stop=1" || fail "the holder did not see key-1 live after 3 x TTL"
S1=$(line_of basic-hold.out STOPPED | awk '{print $2}')
[ -n "${S1}" ] || { cat "${SHARED}/basic-hold.out" >&2; fail "no STOPPED line (the lease was lost while kept alive?)"; }
wait_until $(( BASIC_TTL_MS / 1000 + 30 )) "key-1 Expired" has_event basic-watch Expired key-1 || fail "no Expired for key-1 within TTL + 30s of the stop"
[ "$(event_ver basic-watch Expired key-1)" = "${V1}" ] || fail "key-1 Expired at version $(event_ver basic-watch Expired key-1), want ${V1}"
expect_late "key-1" "$(event_ms basic-watch Expired key-1)" $(( S1 + BASIC_TTL_MS )) "${BOUND_MS}"
drv_leg basic-scan 120 "${APP_USER}" app.pw "LEASE-E2E-SCAN: OK" scan --topic basic --expect-absent key-1
drv_leg basic-ttl 120 "${APP_USER}" app.pw "LEASE-E2E-TTL: OK" ttl --topic basic --key key-1 --expect absent
drv_leg basic-regrant 120 "${APP_USER}" app.pw "LEASE-E2E-GRANT: OK" grant --topic basic --key key-1
V2=$(grep -oE 'version=[0-9]+' "${WORK_DIR}/basic-regrant.out" | cut -d= -f2)
[ -n "${V2}" ] && [ "${V2}" -gt "${V1}" ] || fail "the regrant has version ${V2:-none}, not above ${V1}"
stop_watcher basic-watch
leg_end BASIC "${LEG_S}"

# ---------------------------------------------------------------------------
# Leg 2: SCALE.
# ---------------------------------------------------------------------------
leg_begin SCALE "${SCALE_COUNT} leases (3 LeaseSessions), TTL ${SCALE_TTL_MS} ms, the holder SIGKILLed; Expired within TTL + ${SCALE_BOUND_MS} ms..."
rm -f "${SHARED}"/scale*
drv_start scale-hold $(( SCALE_LEG_S + 120 )) "${APP_USER}" app.pw scale-hold --topic scale --count "${SCALE_COUNT}" --ttl-ms "${SCALE_TTL_MS}" --out /shared/scale-hold.out --stop-file /shared/scale-hold.stop --max-s $(( SCALE_LEG_S ))
wait_until 330 "the ${SCALE_COUNT} grants" has_line scale-hold.out "HELD ${SCALE_COUNT}" || { out_of scale-hold | tail -20 >&2; fail "the holder did not report HELD ${SCALE_COUNT} within 330s"; }
# The watcher starts BEFORE the kill and is past its initial scan (all ${SCALE_COUNT} leases live) when it writes its ready file.
drv_start scale-watch $(( SCALE_LEG_S )) admin admin.pw watch --topic "${NS}/scale" --log /shared/scale-watch.log --ready /shared/scale-watch.ready --stop-file /shared/scale-watch.stop --ready-live "${SCALE_COUNT}" --expect-expired "${SCALE_COUNT}" --max-s $(( SCALE_LEG_S - 120 ))
wait_until 180 "the scale watcher ready" has_line scale-watch.ready READY || { out_of scale-watch | tail -20 >&2; fail "the scale watcher was not past its initial scan (live ${SCALE_COUNT}) within 180s"; }
log "SCALE: all held; keepalives flow for 2 x TTL..."
sleep $(( 2 * SCALE_TTL_MS / 1000 ))
ALIVE=$(line_of scale-hold.out ALIVE)
echo "${ALIVE}" | grep -q "lost=0 " || fail "leases were lost while kept alive: ${ALIVE:-no ALIVE line}"
HOLDER="rune-lease-scale-hold"
docker kill -s KILL "${HOLDER}" >/dev/null || fail "docker kill -s KILL ${HOLDER} failed"
K=$(now_ms)
log "SCALE: holder killed at K=${K}; waiting for ${SCALE_COUNT} Expired..."
wait_until $(( (SCALE_TTL_MS + SCALE_BOUND_MS) / 1000 + 45 )) "the scale watcher to finish" bg_done scale-watch \
    || { touch "${SHARED}/scale-watch.stop"; wait_until 20 "the scale watcher to stop" bg_done scale-watch || true; out_of scale-watch | tail -10 >&2; }
awk -v K="${K}" -v ttl="${SCALE_TTL_MS}" -v bound="${SCALE_BOUND_MS}" '
    $1=="EV" && $2=="Expired" { n[$3]++; late=$5-(K+ttl); if (!seen || late>max) { max=late; seen=1 } if (late>bound) over++ }
    END { for (k in n) { keys++; if (n[k]!=1) dup++ } printf "%d %d %d %d\n", keys+0, dup+0, over+0, max+0 }' "${SHARED}/scale-watch.log" > "${WORK_DIR}/scale-check.txt"
read -r SKEYS SDUP SOVER SMAX < "${WORK_DIR}/scale-check.txt"
log "SCALE: ${SKEYS} keys Expired, ${SDUP} more than once, ${SOVER} later than K + TTL + ${SCALE_BOUND_MS} ms; max lateness ${SMAX} ms after K + TTL"
[ "${SKEYS}" = "${SCALE_COUNT}" ] || fail "${SKEYS} of ${SCALE_COUNT} keys received an Expired"
[ "${SDUP}" = "0" ] || fail "${SDUP} keys received more than one Expired"
[ "${SOVER}" = "0" ] || fail "${SOVER} Expired received later than K + TTL + ${SCALE_BOUND_MS} ms (max ${SMAX} ms after K + TTL)"
bg_done scale-watch && bg_leg_check scale-watch "LEASE-E2E-WATCH: OK"
leg_end SCALE "${SCALE_LEG_S}"

# ---------------------------------------------------------------------------
# Leg 3: GUARD (R5).
# ---------------------------------------------------------------------------
leg_begin GUARD "a guarded producer; the driver is cut from the lease leader after the data leader is on another node..."
placement_ok() {
    DL=$(partition_leader "${GUARD_DATA_ID}" 0) || return 1
    LL=$(partition_leader "${GUARD_ID}" 0) || return 1
}
wait_until 120 "guard leaders" placement_ok || fail "the guard topics have no leaders"
tries=0
while [ "${DL}" = "${LL}" ]; do
    tries=$(( tries + 1 ))
    [ "${tries}" -le 3 ] || fail "the data and the lease leader are still on node ${LL} after 3 moves"
    log "GUARD: both leaders on node ${LL}: moving them (try ${tries})"
    OLD="${LL}"
    kill_node "${OLD}"
    restart_node "${OLD}"
    wait_until 120 "the guard leaders moved" leader_not "${GUARD_ID}" "${OLD}" || true
    wait_until 120 "guard leaders" placement_ok || fail "the guard topics have no leaders after the move"
done
LIP=$(ip_of $(( LL + 1 )))
[ -n "${LIP}" ] || fail "no ip for the lease leader rune-$(( LL + 1 ))"
SEED_NODE=$(( DL + 1 ))   # the driver connects through the data leader, never through the node it is cut from
log "GUARD: data leader node ${DL}, lease leader node ${LL}; the driver connects through rune-${SEED_NODE}"
rm -f "${SHARED}"/guard*
DRV_NET_ADMIN=1
drv_start guard 400 "${APP_USER}" app.pw guard --lease-topic guard --data-topic guard_data --ttl-ms "${GUARD_TTL_MS}" --out /shared/guard.out --ready-file /shared/guard.ready
DRV_NET_ADMIN=""
GUARD_CTR="rune-lease-guard"
wait_until 120 "the guard driver ready" has_line guard.ready READY || { out_of guard | tail -20 >&2; fail "the guard driver was not ready (lease granted, producer bound, 200 acks) within 120s"; }
ipt() { docker exec "${GUARD_CTR}" iptables "$@" || docker exec "${GUARD_CTR}" iptables-legacy "$@"; }
CUT_START=$(now_ms)
ipt -A OUTPUT -d "${LIP}" -j DROP || fail "could not add the OUTPUT drop for ${LIP}"
ipt -A INPUT -s "${LIP}" -j DROP || fail "could not add the INPUT drop for ${LIP}"
log "GUARD: cut from the lease leader (rune-$(( LL + 1 )), ${LIP}) at ${CUT_START}"
wait_until 330 "the guard driver to finish" bg_done guard || { out_of guard | tail -20 >&2; fail "the guard driver did not finish within 330s of the cut"; }
ipt -D OUTPUT -d "${LIP}" -j DROP >/dev/null 2>&1 || true
ipt -D INPUT -s "${LIP}" -j DROP >/dev/null 2>&1 || true
bg_leg_check guard "LEASE-E2E-GUARD: OK"
E_MS=$(line_of guard.out ENTERED_MS= | cut -d= -f2)
[ -n "${E_MS}" ] || fail "the guard driver recorded no Entered time"
[ "${E_MS}" -ge "${CUT_START}" ] || fail "Entered at ${E_MS} is before the cut at ${CUT_START}"
log "GUARD: Entered ${E_MS} ms - ${CUT_START} ms = $(( E_MS - CUT_START )) ms after the cut (bound TTL + 2000 = $(( GUARD_TTL_MS + 2000 )) ms); $(grep '^GUARD ' "${WORK_DIR}/guard.out")"
[ $(( E_MS - CUT_START )) -le $(( GUARD_TTL_MS + 2000 )) ] || fail "Entered arrived $(( E_MS - CUT_START )) ms after the cut, over TTL + 2 s"
leg_end GUARD "${LEG_S}"
SEED_NODE=1

# ---------------------------------------------------------------------------
# Leg 4: CHECKPOINT.
# ---------------------------------------------------------------------------
leg_begin CHECKPOINT "a ${CKPT_TTL_MS} ms lease, never kept alive (checkpoint every ${CKPT_INTERVAL_MS} ms); the leader killed after a folded checkpoint, twice..."
rm -f "${SHARED}"/ckpt*
# W: a node that does NOT lead partition 0 now.  The watcher and the checkpoint pollers connect through it, and it is never killed
# (kill 2 is skipped when the new leader is W).
L0=$(partition_leader "${CKPT_ID}" 0) || fail "no leader of ckpt partition 0"
W=$(( (L0 + 1) % 3 ))
SEED_NODE=$(( W + 1 ))
log "CHECKPOINT: partition leader node ${L0}; the watcher and the pollers are seeded on node ${W} (rune-${SEED_NODE})"
drv_start ckpt-watch "${WATCH_CAP_S}" admin admin.pw watch --topic "${NS}/ckpt" --log /shared/ckpt-watch.log --ready /shared/ckpt-watch.ready --stop-file /shared/ckpt-watch.stop --max-s $(( WATCH_CAP_S - 20 ))
wait_until 120 "the ckpt watcher ready" has_line ckpt-watch.ready READY || { out_of ckpt-watch | tail -20 >&2; fail "the ckpt watcher was not ready within 120s"; }
# The first poller connects BEFORE the grant (it polls the absent key until the checkpoint folds), so its connect does not eat the 5 s interval.
drv_start ckpt-poll1 120 "${APP_USER}" app.pw ckpt-poll --topic ckpt --key ckpt-1 --below "${CKPT_TTL_MS}" --max-s 60
sleep 8
drv_leg ckpt-grant 120 "${APP_USER}" app.pw "LEASE-E2E-GRANT: OK" grant --topic ckpt --key ckpt-1 --ttl-ms "${CKPT_TTL_MS}"
G=$(grep -oE 'sent_ms=[0-9]+' "${WORK_DIR}/ckpt-grant.out" | cut -d= -f2)
[ -n "${G}" ] || fail "the grant printed no send time"
log "CHECKPOINT: G=${G} (taken before the grant was sent)"

# leader_ep_into <var node> <var epoch> <what> : "<node> <epoch>" of ckpt partition 0 into the two variables, or fail.
leader_ep_into() {
    local r
    r=$(partition_leader_ep "${CKPT_ID}" 0) || fail "no leader of ckpt partition 0 ($3)"
    printf -v "$1" '%s' "${r%% *}"; printf -v "$2" '%s' "${r##* }"
}

# ckpt_seen <tag> : waits (bounded) for the poller, then sets CKPT_R / CKPT_C (the folded checkpoint's R and the realtime it was seen).
CKPT_R=0; CKPT_C=0
ckpt_seen() {
    wait_until 90 "the $1 poller" bg_done "$1" || { out_of "$1" | tail -20 >&2; fail "the $1 poller did not finish within 90s"; }
    bg_leg_check "$1" "LEASE-E2E-CKPT-POLL: OK"
    CKPT_R=$(grep -oE 'remaining_ms=[0-9]+' "${WORK_DIR}/$1.out" | cut -d= -f2)
    CKPT_C=$(grep -oE 'seen_ms=[0-9]+' "${WORK_DIR}/$1.out" | cut -d= -f2)
    [ -n "${CKPT_R}" ] && [ -n "${CKPT_C}" ] || fail "$1 printed no checkpoint"
}
# wait_new_leader <old epoch> : sets NEWL (node) / NEWL_MS (realtime it first showed); polls every 50 ms, bounded by LEADER_WAIT_S.
NEWL=""; NEWL_MS=0
wait_new_leader() {
    local old_e="$1" deadline=$(( $(now_s) + LEADER_WAIT_S )) r
    while [ "$(now_s)" -lt "${deadline}" ]; do
        if r=$(partition_leader_ep "${CKPT_ID}" 0) && [ "${r##* }" -gt "${old_e}" ]; then NEWL="${r%% *}"; NEWL_MS=$(now_ms); return 0; fi
        sleep 0.05
    done
    return 1
}

# ---- kill 1: after a folded checkpoint ----
ckpt_seen ckpt-poll1
R1="${CKPT_R}"; C1="${CKPT_C}"
log "CHECKPOINT: checkpoint 1 folded: R1=${R1} ms, seen at C1=${C1} (G+$(( C1 - G )) ms)"
[ $(( C1 - G )) -lt 11000 ] || fail "checkpoint 1 was seen only at G+$(( C1 - G )) ms: too late to kill the leader before the ${CKPT_TTL_MS} ms lease ends"
leader_ep_into KL1 KE1 "kill 1"
[ "${KL1}" != "${W}" ] || fail "the leader moved to the watcher's seed node ${W} before kill 1"
drv_start ckpt-poll2 120 "${APP_USER}" app.pw ckpt-poll --topic ckpt --key ckpt-1 --below "${R1}" --max-s 40
kill_node "${KL1}"
K1=$(now_ms)
log "CHECKPOINT: killed node ${KL1} (the leader, epoch ${KE1}) at K1=${K1} (G+$(( K1 - G )) ms)"
start_node "${KL1}"
wait_new_leader "${KE1}" || fail "no new leader of ckpt partition 0 within ${LEADER_WAIT_S}s of kill 1"
L1="${NEWL_MS}"; NL1="${NEWL}"
log "CHECKPOINT: new leader node ${NL1} first shown at L1=${L1} (K1+$(( L1 - K1 )) ms)"
await_links "${KL1}"

# ---- kill 2: after a NEW checkpoint folded under the new leader ----
BK="${K1}"; BL="${L1}"; BR="${R1}"; KILLS="1"
ckpt_seen ckpt-poll2
R2="${CKPT_R}"; C2="${CKPT_C}"
log "CHECKPOINT: checkpoint 2 folded under node ${NL1}: R2=${R2} ms (< R1), seen at C2=${C2} (G+$(( C2 - G )) ms)"
leader_ep_into KL2 KE2 "kill 2"
K2="-"; L2="-"
if [ "${KL2}" = "${W}" ]; then
    log "CHECKPOINT: kill 2 skipped (leader is the watcher's seed, node ${W}); the bound is checked after kill 1 only"
elif has_event ckpt-watch Expired ckpt-1; then
    log "CHECKPOINT: kill 2 skipped (ckpt-1 already Expired); the bound is checked after kill 1 only"
else
    kill_node "${KL2}"
    K2=$(now_ms)
    log "CHECKPOINT: killed node ${KL2} (the leader, epoch ${KE2}) at K2=${K2} (G+$(( K2 - G )) ms)"
    start_node "${KL2}"
    wait_new_leader "${KE2}" || fail "no new leader of ckpt partition 0 within ${LEADER_WAIT_S}s of kill 2"
    L2="${NEWL_MS}"
    log "CHECKPOINT: new leader node ${NEWL} first shown at L2=${L2} (K2+$(( L2 - K2 )) ms)"
    BK="${K2}"; BL="${L2}"; BR="${R2}"; KILLS="1,2"
    await_links "${KL2}"
fi

# ---- the bound (spec 9.6): received <= L + extend + R + BOUND_MS, for the last kill done ----
CKPT_BUDGET=$(( (BL + EXTEND_MS + BR + BOUND_MS - $(now_ms)) / 1000 + 30 ))
[ "${CKPT_BUDGET}" -gt 30 ] || CKPT_BUDGET=30
wait_until "${CKPT_BUDGET}" "ckpt-1 Expired" has_event ckpt-watch Expired ckpt-1 || fail "no Expired for ckpt-1 within the bound plus 30s"
RECV=$(event_ms ckpt-watch Expired ckpt-1)
[ -n "${RECV}" ] || fail "ckpt-1: no Expired received"
DEADLINE=$(( BL + EXTEND_MS + BR + BOUND_MS ))
log "CHECKPOINT: G=${G} C1=${C1} R1=${R1} K1=${K1} L1=${L1} C2=${C2} R2=${R2} K2=${K2} L2=${L2} (kills done: ${KILLS}) RECV=${RECV}"
log "CHECKPOINT: plain lateness received - (G + TTL) = $(( RECV - (G + CKPT_TTL_MS) )) ms"
log "CHECKPOINT: received - (L + extend ${EXTEND_MS} + R ${BR}) = $(( RECV - (BL + EXTEND_MS + BR) )) ms (bound ${BOUND_MS} ms)"
if [ "${RECV}" -gt "${DEADLINE}" ]; then
    OVER=$(( RECV - DEADLINE ))
    [ "${OVER}" -ge 1000 ] || log "CHECKPOINT: over the bound by only ${OVER} ms (< 1 s): L stands in for T_ready, which is a little later; judge: L=${BL} extend=${EXTEND_MS} R=${BR} bound=${BOUND_MS} received=${RECV}"
    fail "ckpt-1 Expired received ${OVER} ms after L + extend + R + ${BOUND_MS} ms (L=${BL}, R=${BR}, received=${RECV})"
fi
stop_watcher ckpt-watch
leg_end CHECKPOINT "${LEG_S}"
SEED_NODE=1

# ---------------------------------------------------------------------------
# Leg 5: SUSPEND (ReadOnly, then RejectAll, on the namespace that holds every topic).
# ---------------------------------------------------------------------------
leg_begin SUSPEND "namespace ${NS} ReadOnly then RejectAll mid-TTL on topic basic..."
rm -f "${SHARED}"/hold2* "${SHARED}"/hold3* "${SHARED}"/suspend-watch.*
drv_start suspend-watch "${WATCH_CAP_S}" admin admin.pw watch --topic "${NS}/basic" --log /shared/suspend-watch.log --ready /shared/suspend-watch.ready --stop-file /shared/suspend-watch.stop --max-s $(( WATCH_CAP_S - 20 ))
wait_until 120 "the suspend watcher ready" has_line suspend-watch.ready READY || { out_of suspend-watch | tail -20 >&2; fail "the suspend watcher was not ready within 120s"; }
# ReadOnly: key-2 is kept alive for 1 x TTL, the namespace is suspended ReadOnly (keepalives are served), the keepalives stop at S2.
drv_start hold2 $(( LEG_S )) "${APP_USER}" app.pw hold --topic basic --key key-2 --ttl-ms "${BASIC_TTL_MS}" --keep-ms 0 --stop-file /shared/hold2.stop --out /shared/hold2.out --max-s $(( LEG_S - 60 ))
wait_until 90 "key-2 granted" has_line hold2.out GRANTED || { out_of hold2 | tail -20 >&2; fail "key-2 was not granted within 90s"; }
sleep $(( BASIC_TTL_MS / 1000 ))
cli admin admin.pw namespace suspend "${NS}" --read-only --broker "rune-1:${BROKER_PORT}" >/dev/null || fail "rune namespace suspend ${NS} --read-only failed"
sleep 1
has_line hold2.out LOST && fail "key-2 was lost under ReadOnly (its keepalives must still be served)"
touch "${SHARED}/hold2.stop"
wait_until 60 "the ReadOnly holder to stop" bg_done hold2 || { out_of hold2 | tail -20 >&2; fail "the ReadOnly holder did not finish"; }
bg_leg_check hold2 "LEASE-E2E-HOLD: OK"
S2=$(line_of hold2.out STOPPED | awk '{print $2}')
[ -n "${S2}" ] || { cat "${SHARED}/hold2.out" >&2; fail "no STOPPED line for key-2"; }
wait_until $(( BASIC_TTL_MS / 1000 + 30 )) "key-2 Expired under ReadOnly" has_event suspend-watch Expired key-2 || fail "no Expired for key-2 under ReadOnly within TTL + 30s"
expect_late "key-2 under ReadOnly" "$(event_ms suspend-watch Expired key-2)" $(( S2 + BASIC_TTL_MS )) "${BOUND_MS}"
cli admin admin.pw namespace resume "${NS}" --broker "rune-1:${BROKER_PORT}" >/dev/null || fail "rune namespace resume ${NS} failed"

# RejectAll: key-3 is kept alive for 1 x TTL, the namespace is suspended RejectAll (the holder's session is closed: its keepalives stop).
drv_start hold3 $(( LEG_S )) "${APP_USER}" app.pw hold --topic basic --key key-3 --ttl-ms "${BASIC_TTL_MS}" --keep-ms 0 --out /shared/hold3.out --max-s $(( LEG_S - 60 ))
wait_until 90 "key-3 granted" has_line hold3.out GRANTED || { out_of hold3 | tail -20 >&2; fail "key-3 was not granted within 90s"; }
sleep $(( BASIC_TTL_MS / 1000 ))
cli admin admin.pw namespace suspend "${NS}" --reject-all --broker "rune-1:${BROKER_PORT}" >/dev/null || fail "rune namespace suspend ${NS} --reject-all failed"
S3=$(now_ms)
wait_until $(( BASIC_TTL_MS / 1000 + 30 )) "key-3 Expired under RejectAll" has_event suspend-watch Expired key-3 || fail "no Expired for key-3 under RejectAll within TTL + 30s (the cluster-admin watcher must survive RejectAll)"
expect_late "key-3 under RejectAll" "$(event_ms suspend-watch Expired key-3)" $(( S3 + BASIC_TTL_MS )) "${BOUND_MS}"
wait_until 120 "the RejectAll holder to end" bg_done hold3 || log "SUSPEND: WARNING: the RejectAll holder is still running after 120s (its session should have been closed)"
cli admin admin.pw namespace resume "${NS}" --broker "rune-1:${BROKER_PORT}" >/dev/null || fail "rune namespace resume ${NS} failed"
drv_leg suspend-regrant 120 "${APP_USER}" app.pw "LEASE-E2E-GRANT: OK" grant --topic basic --key key-3
stop_watcher suspend-watch
leg_end SUSPEND "${LEG_S}"

echo "LEASE-E2E: OK"
