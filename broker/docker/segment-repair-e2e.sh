#!/usr/bin/env bash
# docker/segment-repair-e2e.sh - #880: a damaged sealed log segment on one node of a REAL 3-node cluster is repaired from a peer.
#
#   1. SETUP:    build the broker + the rune CLI images; cluster key + TLS; the 3-node cluster (docker-compose.yml) up and
#                bootstrapped (docker/lib/auth.sh); topic repair_e2e (1 partition, replicas 3, segment.bytes = 4096);
#   2. FILL:     the rune_produce_subscribe example (RUNE_MODE=ws) produces 200 acked records, one batch each, so the log
#                rolls into >= 3 sealed segments on every replica;
#   3. DAMAGE:   `docker compose stop rune-3`, flip the first record byte of the 2nd batch of the FIRST sealed segment on
#                rune-3's volume (position read from the file; below the recovery point), start it again;
#   4. ASSERT:   rune-3 starts and logs "1 damaged log segment(s) at boot", then "damaged segment <base> repaired from peer"
#                (the counter rune-log-segment-repairs, metric 1000272, rises with it; the counters themselves are read by the
#                in-process SegmentRepairClusterRegression tests) and never "cannot be repaired from a peer"; the repaired
#                segment file is byte-identical to the same segment on rune-2's volume; a subscription seeded on rune-3 from
#                the start delivers all 200 records;
#   5. print SEGMENT-REPAIR-E2E: OK.
#
# VM only (CLAUDE.md section 3): never run on a local Mac docker.
#   ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/segment-repair-e2e.sh'
# Environment (all optional): FLIGHT_RECORDER_DIR, ARCANUM_METRICS_DIR (sibling checkouts), E2E_LOG_DIR (keep the broker logs).
#
# House style: bounded waits, trap teardown (`docker compose down -v`), every failure names its step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
export FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
export ARCANUM_METRICS_DIR="${ARCANUM_METRICS_DIR:-${PROJECTS}/../arcanum-metrics}"
CLIENT_REPO="${PROJECTS}/client-cpp"
COMPOSE_PROJECT="rune-segment-repair-e2e"
NETWORK="${COMPOSE_PROJECT}_default"
VOLUME3="${COMPOSE_PROJECT}_rune-data-3"
VOLUME2="${COMPOSE_PROJECT}_rune-data-2"
WORK_DIR="$(mktemp -d "${BROKER_REPO}/.segment-repair-e2e-work.XXXXXX")"
TOOLS="segment-repair-e2e-tools"
TOPIC="repair_e2e"
TOPIC_TABLE="${TOPIC}:0:1"
BROKER_PORT=40200
RECORDS=200
LEADER_WAIT_S=120
REPAIR_WAIT_S=180
CLIENT_RUN_S=240
EGRESS_N=0

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export RUNE_AUTH_WORK_DIR="${WORK_DIR}"
export RUNE_AUTH_TOOLS="${TOOLS}"
export RUNE_AUTH_NETWORK="${NETWORK}"

log()  { echo "[segment-repair-e2e] $*" >&2; }
fail() { echo "[segment-repair-e2e] FAIL: $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.segment-repair-e2e-work*' -not -path './.auth.*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
rune_auth_init
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-segment-repair-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"

dc() { docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" "$@"; }
tx() { docker exec "${TOOLS}" "$@"; }
broker_id() { dc ps -a -q "$1" | head -1; }

[ -d "${FLIGHT_RECORDER_DIR}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
[ -d "${ARCANUM_METRICS_DIR}" ] || fail "missing arcanum-metrics checkout (set ARCANUM_METRICS_DIR)"
for d in protocol codec crypto ql client-cpp conan/aeron; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done

teardown() {
    local rc=$?
    log "Tearing down (exit ${rc})..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then
        mkdir -p "${E2E_LOG_DIR}"
        for s in rune-1 rune-2 rune-3; do docker logs --timestamps "$(broker_id "${s}")" > "${E2E_LOG_DIR}/${s}.log" 2>&1 || true; done
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.err "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
    dc down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "${WORK_DIR}" 2>/dev/null || true
    rune_auth_cleanup
    return "${rc}"
}
trap teardown EXIT

# wait_until <budget_s> <description> <command...> : polls the command (0 = done) every 1 s.
wait_until() {
    local budget="$1"; shift 2
    local deadline=$(( $(date +%s) + budget ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}
leader_up() { dc logs --no-log-prefix rune-1 rune-2 rune-3 2>/dev/null | grep -qE 'node [0-9]+ is LEADER'; }
node3_log() { docker logs "$(broker_id rune-3)" 2>&1; }
node3_repaired() { node3_log | grep -qE 'damaged segment [0-9]+ repaired from peer'; }

# ws_run <tag> <seed service> [KEY=VALUE ...] : one rune_produce_subscribe run (RUNE_MODE=ws) as the e2e user, its client seeded
# on that broker (it follows the leader); sets WS_RC and WS_OUT.
WS_RC=0; WS_OUT=""
ws_run() {
    local tag="$1" seed="$2"; shift 2
    local -a envargs=(-e RUNE_MODE=ws -e RUNE_USER="${RUNE_AUTH_E2E_USER}" -e RUNE_PASSWORD_FILE=/work/e2e.pw
        -e "RUNE_TARGET=aeron:udp?endpoint=${seed}:${BROKER_PORT}" -e RUNE_TLS_CA_FILE=/run/secrets/ca.pem
        -e RUNE_DATA_DIR="/tmp/ws-${tag}" -e RUNE_CLIENT_TOPIC_TABLE="${TOPIC_TABLE}" -e RUNE_TIMEOUT_MS=20000
        -e RUNE_QUIET=1)
    EGRESS_N=$(( EGRESS_N + 1 ))
    envargs+=(-e "RUNE_EGRESS=aeron:udp?endpoint=${TOOLS}:$(( 40500 + EGRESS_N ))")
    local kv
    for kv in "$@"; do envargs+=(-e "${kv}"); done
    set +e
    docker exec "${envargs[@]}" "${TOOLS}" timeout "${CLIENT_RUN_S}" /build/build/rune_produce_subscribe \
        > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
    WS_RC=$?
    set -e
    WS_OUT="$(cat "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err" 2>/dev/null)"
}
# in_volume <volume> <sh command> : a throwaway container with a broker's data volume at /data (stop the broker to write).
in_volume() { docker run --rm --platform linux/amd64 --entrypoint sh -v "$1:/data" "${QL_IMAGE}" -c "$2"; }

# ---------------------------------------------------------------------------
log "Step 1a: building the broker image (tag ${RUNE_IMAGE_TAG})..."
set +e
out=$(dc build rune-1 rune-2 rune-3 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built"

log "Step 1b: building the rune CLI image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"

log "Step 1c: the cluster up; the client container (building rune_produce_subscribe in it)..."
dc up -d --no-build rune-1 rune-2 rune-3
docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
docker run -d --name "${TOOLS}" --hostname "${TOOLS}" --network "${NETWORK}" --platform linux/amd64 --shm-size=512m \
    --security-opt seccomp=unconfined -v "${WORK_DIR}:/work" -v "${RUNE_SECRETS_DIR}:/run/secrets:ro" \
    -e RUNE_TLS_CA_FILE=/run/secrets/ca.pem "${QL_IMAGE}" sleep infinity >/dev/null || fail "could not start the client container"
set +e
out=$(tx cmake --build /build/build --target rune_produce_subscribe -j2 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "building rune_produce_subscribe failed (exit ${rc})"; }
tx test -x /build/build/rune_produce_subscribe || fail "rune_produce_subscribe missing after build"

wait_until "${LEADER_WAIT_S}" "a consensus leader" leader_up \
    || { dc logs --tail 40 rune-1 rune-2 rune-3 >&2 || true; fail "the cluster did not elect a leader within ${LEADER_WAIT_S}s"; }
rune_auth_prepare "${NETWORK}" "rune-1:${BROKER_PORT}" || fail "bootstrap / e2e user"

log "Step 1d: topic ${TOPIC} (replicas 3, segment.bytes = 4096)..."
tx timeout 120 rune ql --user "${RUNE_AUTH_E2E_USER}" --password-file /work/e2e.pw --broker "rune-1:${BROKER_PORT}" \
    -e "topic ${TOPIC} partitions 1 replicas 3 with (segment.bytes = 4096);" || fail "creating the topic failed"

log "Step 2: producing ${RECORDS} acked records (one batch each)..."
ws_run fill rune-1 RUNE_WS_ROLE=produce RUNE_PRODUCE_N="${RECORDS}"
[ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -30 >&2; fail "the fill run exited ${WS_RC}"; }
echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=${RECORDS} failed=0" || { echo "${WS_OUT}" | tail -30 >&2; fail "not all ${RECORDS} records were acked"; }

log "Step 3: stopping rune-3; flipping one byte of its first sealed segment..."
dc stop rune-3 >/dev/null || fail "docker compose stop rune-3 failed"
SEGS="$(in_volume "${VOLUME3}" "find /data -name '*.log' -path '*${TOPIC}*' | sort" 2>/dev/null)" || true
if [ -z "${SEGS}" ]; then SEGS="$(in_volume "${VOLUME3}" "find /data -name '*.log' | sort")"; fi
# The partition directory of the topic: the one holding >= 3 segment files.
PART_DIR=""
for d in $(echo "${SEGS}" | xargs -n1 dirname | sort -u); do
    [ "$(echo "${SEGS}" | grep -c "^${d}/")" -ge 3 ] && PART_DIR="${d}" && break
done
[ -n "${PART_DIR}" ] || { echo "${SEGS}" >&2; fail "no partition directory with >= 3 sealed segments on rune-3"; }
FIRST="$(echo "${SEGS}" | grep "^${PART_DIR}/" | head -1)"
log "Step 3: damaging ${FIRST} in the first record byte of its 2nd batch"
# The position comes from the file, never a fixed byte: batch 1 is 12 + its little-endian length field (bytes 8-11) long,
# and batch 2's records start 61 bytes (the batch header) after that: inside the CRC-covered part.  xor that byte with 0xff
# in place (POSIX sh: od, printf with an octal escape, dd conv=notrunc).
FLIP='f="$1"; set -- $(dd if="$f" bs=1 skip=8 count=4 2>/dev/null | od -An -tu1); len=$(( $1 + $2 * 256 + $3 * 65536 + $4 * 16777216 )); pos=$(( len + 12 + 61 )); b=$(dd if="$f" bs=1 skip=$pos count=1 2>/dev/null | od -An -tu1 | tr -d " "); [ -n "$b" ] || exit 1; n=$(( b ^ 255 )); o=$(printf "%03o" "$n"); printf "\\$o" | dd of="$f" bs=1 seek=$pos conv=notrunc 2>/dev/null'
docker run --rm --platform linux/amd64 --entrypoint sh -v "${VOLUME3}:/data" "${QL_IMAGE}" -c "${FLIP}" sh "${FIRST}" \
    || fail "flipping the byte failed"
DAMAGED_SUM="$(in_volume "${VOLUME3}" "sha256sum '${FIRST}'" | cut -d' ' -f1)"

log "Step 4: starting rune-3; asserting..."
dc start rune-3 >/dev/null || fail "docker compose start rune-3 failed"
wait_until "${LEADER_WAIT_S}" "rune-3 reporting the damage" \
    bash -c "docker logs '$(broker_id rune-3)' 2>&1 | grep -Eq '(^|[^0-9])1 damaged log segment\\(s\\) at boot'" \
    || { docker logs --tail 80 "$(broker_id rune-3)" >&2 || true; fail "rune-3 did not report exactly 1 damaged segment at boot"; }
log "Step 4a: rune-3 started and reports 1 damaged segment"

wait_until "${REPAIR_WAIT_S}" "rune-3 repairing the segment" node3_repaired \
    || { docker logs --tail 120 "$(broker_id rune-3)" >&2 || true; fail "rune-3 did not repair its damaged segment within ${REPAIR_WAIT_S}s"; }
node3_log | grep -q "cannot be repaired from a peer" && { node3_log | tail -40 >&2; fail "rune-3 reported that no peer could serve the segment"; }
log "Step 4b: rune-3 repaired the segment from a peer"

# The cleaner thread finishes the renames shortly after the swap: poll until the file equals the same segment on rune-2.
same_as_peer() {
    local mine peer
    mine="$(in_volume "${VOLUME3}" "sha256sum '${FIRST}'" 2>/dev/null | cut -d' ' -f1)"
    peer="$(in_volume "${VOLUME2}" "sha256sum '${FIRST}'" 2>/dev/null | cut -d' ' -f1)"
    [ -n "${mine}" ] && [ "${mine}" = "${peer}" ] && [ "${mine}" != "${DAMAGED_SUM}" ]
}
wait_until 60 "the repaired file equal to rune-2's copy" same_as_peer \
    || fail "rune-3's repaired segment is not byte-identical to rune-2's copy of ${FIRST}"
log "Step 4c: the repaired segment is byte-identical to a peer's copy"

ws_run sub-repaired rune-3 RUNE_WS_ROLE=consume RUNE_CONSUME_EXPECT="${RECORDS}"
[ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -30 >&2; fail "a subscription from the start did not deliver every record after the repair"; }
echo "${WS_OUT}" | grep -q "WS-CONSUMER: distinct=${RECORDS}" || { echo "${WS_OUT}" | tail -30 >&2; fail "the subscription did not deliver all ${RECORDS} records"; }
log "Step 4d: a subscription from the start delivers all ${RECORDS} records"

echo "SEGMENT-REPAIR-E2E: OK"
