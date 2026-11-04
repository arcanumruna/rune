#!/usr/bin/env bash
# docker/corrupt-segment-e2e.sh - #839: a damaged sealed log segment is kept, flagged and served around, on one real broker.
#
#   1. SETUP:    build the broker + the rune CLI images; cluster key + TLS; ONE broker (docker-compose.single.yml) up and
#                bootstrapped (docker/lib/auth.sh); topic corrupt_e2e (1 partition, segment.bytes = 4096);
#   2. FILL:     the rune_produce_subscribe example (RUNE_MODE=ws) produces 200 acked records, one batch each, so the log
#                rolls into >= 3 sealed segments;
#   3. DAMAGE:   `docker compose stop` the broker, flip the first record byte of the 2nd batch of the FIRST sealed segment
#                (position read from the file; below the recovery point), start;
#   4. ASSERT:   the broker starts and logs "1 damaged log segment(s) at boot" (the line written beside the gauge
#                rune-log-damaged-segments, metric 1000270, from the same value: the gauge itself is read by the in-process
#                BrokerMetricsRegressionTest); a subscription from the start ends CorruptBatch; a subscription from the
#                base of the LAST segment still delivers; a produce still acks.
#   5. print CORRUPT-SEGMENT-E2E: OK.
#
# VM only (CLAUDE.md section 3): never run on a local Mac docker.
#   ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/corrupt-segment-e2e.sh'
# Environment (all optional): FLIGHT_RECORDER_DIR, ARCANUM_METRICS_DIR (sibling checkouts), E2E_LOG_DIR (keep the broker log).
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
COMPOSE_PROJECT="rune-corrupt-segment-e2e"
NETWORK="${COMPOSE_PROJECT}_default"
VOLUME="${COMPOSE_PROJECT}_rune-data-1"
WORK_DIR="$(mktemp -d "${BROKER_REPO}/.corrupt-segment-e2e-work.XXXXXX")"
TOOLS="corrupt-segment-e2e-tools"
TOPIC="corrupt_e2e"
TOPIC_TABLE="${TOPIC}:0:1"
BROKER_PORT=40200
RECORDS=200
LEADER_WAIT_S=120
CLIENT_RUN_S=240
EGRESS_N=0

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export RUNE_AUTH_WORK_DIR="${WORK_DIR}"
export RUNE_AUTH_TOOLS="${TOOLS}"
export RUNE_AUTH_NETWORK="${NETWORK}"

log()  { echo "[corrupt-segment-e2e] $*" >&2; }
fail() { echo "[corrupt-segment-e2e] FAIL: $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.corrupt-segment-e2e-work*' -not -path './.auth.*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
rune_auth_init
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-corrupt-segment-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"

dc() { docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.single.yml" "$@"; }
tx() { docker exec "${TOOLS}" "$@"; }
broker_id() { dc ps -a -q rune-1 | head -1; }

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
        docker logs --timestamps "$(broker_id)" > "${E2E_LOG_DIR}/rune-1.log" 2>&1 || true
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
SINCE=0
leader_since() { docker logs --since "${SINCE}" "$(broker_id)" 2>&1 | grep -q 'node 0 is LEADER'; }

# ws_run <tag> [KEY=VALUE ...] : one rune_produce_subscribe run (RUNE_MODE=ws) as the e2e user; sets WS_RC and WS_OUT.
WS_RC=0; WS_OUT=""
ws_run() {
    local tag="$1"; shift
    local -a envargs=(-e RUNE_MODE=ws -e RUNE_USER="${RUNE_AUTH_E2E_USER}" -e RUNE_PASSWORD_FILE=/work/e2e.pw
        -e "RUNE_TARGET=aeron:udp?endpoint=rune-1:${BROKER_PORT}" -e RUNE_TLS_CA_FILE=/run/secrets/ca.pem
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
# in_volume <sh command> : a throwaway container with the broker's data volume at /data (the broker must be stopped to write).
in_volume() { docker run --rm --platform linux/amd64 --entrypoint sh -v "${VOLUME}:/data" "${QL_IMAGE}" -c "$1"; }

# ---------------------------------------------------------------------------
log "Step 1a: building the broker image (tag ${RUNE_IMAGE_TAG})..."
set +e
out=$(dc build rune-1 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "broker image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built"

log "Step 1b: building the rune CLI image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"

log "Step 1c: the broker up; the client container (building rune_produce_subscribe in it)..."
dc up -d --no-build rune-1
docker rm -f "${TOOLS}" >/dev/null 2>&1 || true
docker run -d --name "${TOOLS}" --hostname "${TOOLS}" --network "${NETWORK}" --platform linux/amd64 --shm-size=512m \
    --security-opt seccomp=unconfined -v "${WORK_DIR}:/work" -v "${RUNE_SECRETS_DIR}:/run/secrets:ro" \
    -e RUNE_TLS_CA_FILE=/run/secrets/ca.pem "${QL_IMAGE}" sleep infinity >/dev/null || fail "could not start the client container"
set +e
out=$(tx cmake --build /build/build --target rune_produce_subscribe -j2 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "building rune_produce_subscribe failed (exit ${rc})"; }
tx test -x /build/build/rune_produce_subscribe || fail "rune_produce_subscribe missing after build"

wait_until "${LEADER_WAIT_S}" "the node leading" leader_since \
    || { docker logs --tail 40 "$(broker_id)" >&2 || true; fail "the broker did not lead within ${LEADER_WAIT_S}s"; }
rune_auth_prepare "${NETWORK}" "rune-1:${BROKER_PORT}" || fail "bootstrap / e2e user"

log "Step 1d: topic ${TOPIC} (segment.bytes = 4096)..."
tx timeout 120 rune ql --user "${RUNE_AUTH_E2E_USER}" --password-file /work/e2e.pw --broker "rune-1:${BROKER_PORT}" \
    -e "topic ${TOPIC} partitions 1 with (segment.bytes = 4096);" || fail "creating the topic failed"

log "Step 2: producing ${RECORDS} acked records (one batch each)..."
ws_run fill RUNE_WS_ROLE=produce RUNE_PRODUCE_N="${RECORDS}"
[ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -30 >&2; fail "the fill run exited ${WS_RC}"; }
echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=${RECORDS} failed=0" || { echo "${WS_OUT}" | tail -30 >&2; fail "not all ${RECORDS} records were acked"; }

log "Step 3: stopping the broker; flipping one byte of the first sealed segment..."
dc stop rune-1 >/dev/null || fail "docker compose stop failed"
SEGS="$(in_volume "find /data -name '*.log' -path '*${TOPIC}*' | sort" 2>/dev/null)" || true
if [ -z "${SEGS}" ]; then SEGS="$(in_volume "find /data -name '*.log' | sort")"; fi
# The partition directory of the topic: the one holding >= 3 segment files.
PART_DIR=""
for d in $(echo "${SEGS}" | xargs -n1 dirname | sort -u); do
    [ "$(echo "${SEGS}" | grep -c "^${d}/")" -ge 3 ] && PART_DIR="${d}" && break
done
[ -n "${PART_DIR}" ] || { echo "${SEGS}" >&2; fail "no partition directory with >= 3 sealed segments"; }
FIRST="$(echo "${SEGS}" | grep "^${PART_DIR}/" | head -1)"
LAST="$(echo "${SEGS}" | grep "^${PART_DIR}/" | tail -1)"
LAST_BASE="$(basename "${LAST}" .log | sed 's/^0*//')"
[ -n "${LAST_BASE}" ] || fail "the last segment has base 0: the log never rolled"
log "Step 3: damaging ${FIRST} in the first record byte of its 2nd batch; the last segment starts at offset ${LAST_BASE}"
# The position comes from the file, never a fixed byte: batch 1 is 12 + its little-endian length field (bytes 8-11) long,
# and batch 2's records start 61 bytes (the batch header) after that: inside the CRC-covered part.  xor that byte with 0xff
# in place (POSIX sh: od, printf with an octal escape, dd conv=notrunc).
FLIP='f="$1"; set -- $(dd if="$f" bs=1 skip=8 count=4 2>/dev/null | od -An -tu1); len=$(( $1 + $2 * 256 + $3 * 65536 + $4 * 16777216 )); pos=$(( len + 12 + 61 )); b=$(dd if="$f" bs=1 skip=$pos count=1 2>/dev/null | od -An -tu1 | tr -d " "); [ -n "$b" ] || exit 1; n=$(( b ^ 255 )); o=$(printf "%03o" "$n"); printf "\\$o" | dd of="$f" bs=1 seek=$pos conv=notrunc 2>/dev/null'
docker run --rm --platform linux/amd64 --entrypoint sh -v "${VOLUME}:/data" "${QL_IMAGE}" -c "${FLIP}" sh "${FIRST}" \
    || fail "flipping the byte failed"

log "Step 4: starting the broker; asserting..."
SINCE="$(date +%s)"
dc start rune-1 >/dev/null || fail "docker compose start failed"
wait_until "${LEADER_WAIT_S}" "the node leading again" leader_since \
    || { docker logs --tail 60 "$(broker_id)" >&2 || true; fail "the broker did not lead again within ${LEADER_WAIT_S}s: a damaged sealed segment must not stop it starting"; }
docker logs "$(broker_id)" 2>&1 | grep -Eq "(^|[^0-9])1 damaged log segment\(s\) at boot" \
    || { docker logs --tail 80 "$(broker_id)" >&2 || true; fail "the broker did not report exactly 1 damaged segment (the gauge rune-log-damaged-segments)"; }
log "Step 4a: the broker serves and reports 1 damaged segment"

ws_run sub-damaged RUNE_WS_ROLE=consume
[ "${WS_RC}" -ne 0 ] || { echo "${WS_OUT}" | tail -30 >&2; fail "a subscription from the start reached the end past the damage"; }
echo "${WS_OUT}" | grep -q "subscription ended: CorruptBatch" \
    || { echo "${WS_OUT}" | tail -30 >&2; fail "the subscription over the damage did not end CorruptBatch"; }
log "Step 4b: a subscription from the start ends CorruptBatch"

ws_run sub-later RUNE_WS_ROLE=consume RUNE_CONSUME_FROM="${LAST_BASE}" RUNE_CONSUME_EXPECT=1
[ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -30 >&2; fail "a subscription from offset ${LAST_BASE} (the last segment) failed"; }
echo "${WS_OUT}" | grep -q "WS-CONSUMER: distinct=[1-9]" || { echo "${WS_OUT}" | tail -30 >&2; fail "nothing delivered from the last segment"; }
log "Step 4c: a subscription from the last segment delivers"

ws_run produce-after RUNE_WS_ROLE=produce RUNE_PRODUCE_N=5
[ "${WS_RC}" -eq 0 ] || { echo "${WS_OUT}" | tail -30 >&2; fail "a produce after the damage failed"; }
echo "${WS_OUT}" | grep -q "WS-PRODUCER: acked=5 failed=0" || { echo "${WS_OUT}" | tail -30 >&2; fail "the produce after the damage was not acked"; }
log "Step 4d: a produce acks"

echo "CORRUPT-SEGMENT-E2E: OK"
