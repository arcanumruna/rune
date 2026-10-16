#!/usr/bin/env bash
# docker/create-topics.sh — create a docker e2e cluster's topics by DDL (catalog history, spec §8).
#
# The brokers no longer read topics from the environment: every topic is created through the
# catalog.  This script runs one docker/topics/<name>.rql through the `rune ql -f` CLI of the
# rune-ql-probe image, on the cluster's compose network, once a catalog leader is up and before any
# producer or consumer touches the topics.
#
# Usage:  COMPOSE_NETWORK=<project>_default bash docker/create-topics.sh <name>
#         bash docker/create-topics.sh --build-image          (just make sure the CLI image exists)
#   <name>  e2e | transient-failover   (docker/topics/<name>.rql)
#
# Environment:
#   COMPOSE_NETWORK            the compose network of the cluster (required unless --build-image)
#   SEED                       broker the CLI connects to (default rune-1:40200); it follows a Redirect
#                              to the catalog leader by itself
#   CREATE_TOPICS_DEADLINE_S   total budget in seconds (default 60)
#   QL_IMAGE                   the CLI image (default rune-ql-probe:<rune-client-cpp HEAD>, always rebuilt)
#
# Behaviour:
#   * `rune ql -f` runs the statements in order and stops at the first error, naming its statement.
#   * Each file has a FIXED, distinct, non-zero --request-id (statement N uses lo + N - 1), so a re-run
#     after a partial run (or a second run on the same cluster) returns the original results of the
#     statements that already committed: it never creates a duplicate and never answers TopicExists.
#     The ids below are the registry (spaced 0x100 apart so statement ranges never overlap); give a new file a new id.
#         e2e                 0123456789abcdef00000000e2e00001
#         transient-failover  0123456789abcdef00000000e2e00100
#   * Retriable while the cluster forms (500 ms apart, bounded by the deadline): connect timed out /
#     connection refused, NotLeader, Redirect, and a per-attempt timeout.  Anything else (a parse error,
#     TopicExists with other options, InvalidRequest, ...) fails at once with the CLI's message.
#   * On expiry it prints the file, the statement that was running and the last error, and exits 1.
#   * The topic ids are checked: statement N must have created topicId N-1 (the e2e clients hard-code
#     them), else it fails.

set -euo pipefail
# The Dockerfiles use BuildKit cache mounts (ccache/conan/build dir): BuildKit is mandatory.
export DOCKER_BUILDKIT=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
CLIENT_REPO="${PROJECTS}/rune-client-cpp"

DEADLINE_S="${CREATE_TOPICS_DEADLINE_S:-60}"
SEED="${SEED:-rune-1:40200}"
ATTEMPT_CAP_S=45                  # one `rune ql` run (connect + DDL budget) never runs longer than this
RETRY_PAUSE_S=0.5

log()  { echo "[create-topics] $*" >&2; }
fail() { echo "[create-topics] FAIL: $*" >&2; exit 1; }

QL_IMAGE="${QL_IMAGE:-rune-ql-probe:$(git -C "${CLIENT_REPO}" rev-parse --short HEAD 2>/dev/null || echo unknown)}"

ensure_image() {
    # Always build (the layer cache makes an unchanged tree a few seconds): a dirty client tree must never run on a
    # stale image that happens to carry the HEAD sha tag.
    local dockerfile="${CLIENT_REPO}/docker/Dockerfile.rune-ql-probe" d
    [ -f "${dockerfile}" ] || fail "missing ${dockerfile} (the rune CLI image)"
    for d in rune-protocol rune-codec rune-ql rune-fr rune-crypto; do
        [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d} (needed to build ${QL_IMAGE})"
    done
    log "Building the rune CLI image ${QL_IMAGE}..."
    local out rc
    set +e
    out=$(docker build --platform linux/amd64 -f "${dockerfile}" \
        --build-context rune-protocol="${PROJECTS}/rune-protocol" \
        --build-context rune-codec="${PROJECTS}/rune-codec" \
        --build-context rune-ql="${PROJECTS}/rune-ql" \
        --build-context rune-fr="${PROJECTS}/rune-fr" \
        --build-context rune-crypto="${PROJECTS}/rune-crypto" \
        -t "${QL_IMAGE}" "${CLIENT_REPO}" 2>&1); rc=$?
    set -e
    [ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "building ${QL_IMAGE} failed (exit ${rc})"; }
    docker image inspect "${QL_IMAGE}" >/dev/null 2>&1 || fail "image not built: ${QL_IMAGE}"
}

if [ "${1:-}" = "--build-image" ]; then ensure_image; exit 0; fi

NAME="${1:-}"
case "${NAME}" in
    e2e)                REQUEST_ID="0123456789abcdef00000000e2e00001" ;;
    transient-failover) REQUEST_ID="0123456789abcdef00000000e2e00100" ;;
    *) fail "usage: COMPOSE_NETWORK=<net> create-topics.sh e2e|transient-failover   (or --build-image)" ;;
esac
[ -n "${COMPOSE_NETWORK:-}" ] || fail "COMPOSE_NETWORK is not set (e.g. rune-broker_default)"
RQL_DIR="${SCRIPT_DIR}/topics"
[ -f "${RQL_DIR}/${NAME}.rql" ] || fail "missing ${RQL_DIR}/${NAME}.rql"
EXPECTED_TOPICS="$(grep -cE '^(transient )?topic ' "${RQL_DIR}/${NAME}.rql")"
ensure_image

CONTAINER="rune-create-topics-$$"
OUT_FILE="$(mktemp -t rune-create-topics.XXXXXX)"
cleanup() { docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true; rm -f "${OUT_FILE}"; }
trap cleanup EXIT

# One bounded attempt.  Sets ATTEMPT_RC (124 = the per-attempt cap fired) and leaves the output in OUT_FILE.
run_attempt() {
    local cap="$1" waited=0
    docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
    docker run --rm --name "${CONTAINER}" \
        --platform linux/amd64 --network "${COMPOSE_NETWORK}" --shm-size=256m --security-opt seccomp=unconfined \
        -v "${RQL_DIR}:/topics:ro" \
        "${QL_IMAGE}" \
        rune ql -f "/topics/${NAME}.rql" --request-id "${REQUEST_ID}" --broker "${SEED}" > "${OUT_FILE}" 2>&1 &
    local pid=$!
    while kill -0 "${pid}" 2>/dev/null; do
        if [ "${waited}" -ge $(( cap * 2 )) ]; then
            docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
            wait "${pid}" 2>/dev/null || true
            ATTEMPT_RC=124
            return
        fi
        sleep 0.5
        waited=$(( waited + 1 ))
    done
    set +e; wait "${pid}"; ATTEMPT_RC=$?; set -e
}

# Statement N must have created topicId N-1 ("created topic '<n>' topicId=<id> version=<v>").
check_ids() {
    local want=0 line id created=0
    while IFS= read -r line; do
        case "${line}" in
            "created topic "*)
                id="$(echo "${line}" | sed -n 's/.*topicId=\([0-9][0-9]*\).*/\1/p')"
                [ "${id}" = "${want}" ] || fail "${NAME}.rql: '${line}' — expected topicId=${want} (the e2e clients hard-code the ids; is the cluster fresh?)"
                want=$(( want + 1 )); created=$(( created + 1 )) ;;
        esac
    done < "${OUT_FILE}"
    [ "${created}" -eq "${EXPECTED_TOPICS}" ] || fail "${NAME}.rql: ${created} topics created, expected ${EXPECTED_TOPICS}"
}

log "Creating ${NAME}.rql via ${SEED} on ${COMPOSE_NETWORK} (request id ${REQUEST_ID}, budget ${DEADLINE_S}s)..."
deadline=$(( $(date +%s) + DEADLINE_S ))
last_err="(no attempt ran)"
last_cli_err=""
attempt=0
while :; do
    remaining=$(( deadline - $(date +%s) ))
    [ "${remaining}" -gt 0 ] || break
    attempt=$(( attempt + 1 ))
    cap=$(( remaining < ATTEMPT_CAP_S ? remaining : ATTEMPT_CAP_S ))
    run_attempt "${cap}"
    if [ "${ATTEMPT_RC}" -eq 0 ]; then
        check_ids
        cat "${OUT_FILE}" >&2
        log "OK: ${EXPECTED_TOPICS} topic(s) of ${NAME}.rql exist (attempt ${attempt})."
        exit 0
    fi
    if [ "${ATTEMPT_RC}" -eq 124 ]; then
        last_err="attempt ${attempt} ran longer than ${cap}s and was stopped"
    else
        last_err="$(tail -5 "${OUT_FILE}")"
        last_cli_err="${last_err}"
        if ! grep -qiE 'NotLeader|Redirect|timed out|connection refused|not connected|unreachable' "${OUT_FILE}"; then
            cat "${OUT_FILE}" >&2
            fail "${NAME}.rql: not retriable (exit ${ATTEMPT_RC}): $(tail -1 "${OUT_FILE}")"
        fi
    fi
    log "attempt ${attempt}: retriable (${last_err//$'\n'/ | }); retrying in ${RETRY_PAUSE_S}s"
    sleep "${RETRY_PAUSE_S}"
done
cat "${OUT_FILE}" >&2 || true
fail "${NAME}.rql: not created within ${DEADLINE_S}s (${attempt} attempts); last error: ${last_err//$'\n'/ | }${last_cli_err:+ ; last CLI output: ${last_cli_err//$'\n'/ | }}"
