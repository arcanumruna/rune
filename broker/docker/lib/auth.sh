#!/usr/bin/env bash
# docker/lib/auth.sh — authentication helpers shared by the docker e2e launchers (authentication Task 20).
#
# Every multi-node (and single-node) launcher now needs a cluster key and credentials, or it refuses to start
# (spec §9, D31).  This library is `source`d; it defines functions only.
#
# The model every launcher follows:
#   1. rune_auth_init                       a per-run directory: secrets/ (the cluster key) and work/ (passwords)
#   2. build the `rune` CLI image            bash docker/create-topics.sh --build-image   (QL_IMAGE)
#   3. rune_gen_cluster_key "${RUNE_SECRETS_DIR}"   BEFORE `docker compose up`
#   4. rune_auth_prepare <network> <broker>   AFTER the cluster has a consensus leader: bootstrap the first admin,
#                                            change its password, create the e2e role + user
#   5. every probe container gets `rune_auth_probe_args` (RUNE_USER / RUNE_PASSWORD_FILE + the work mount)
#   6. rune_auth_cleanup                    in the teardown trap, AFTER `docker compose down`
#
# Paths: arguments named "...-file" of rune_bootstrap are CONTAINER paths (/work/..., /run/secrets/...), as in the
# docs; the same files on the host live under ${RUNE_AUTH_WORK_DIR} / ${RUNE_SECRETS_DIR}.
#
# Environment:
#   QL_IMAGE               the rune CLI image (default rune-ql-probe:<rune-client-cpp HEAD>, as create-topics.sh)
#   RUNE_AUTH_TOOLS        a running client container (e.g. catalog-e2e-tools) to `docker exec` the CLI in, instead of
#                          a `docker run` per command.  It must mount ${RUNE_AUTH_WORK_DIR} at /work and
#                          ${RUNE_SECRETS_DIR} at /run/secrets and be attached to the cluster's network.
#   RUNE_AUTH_CLI_TIMEOUT  seconds one CLI command may run (default 120)
#   RUNE_AUTH_READY_S      budget for the bootstrap to be accepted after the leader election (default 150)

# shellcheck shell=bash

RUNE_AUTH_ADMIN="${RUNE_AUTH_ADMIN:-admin}"
RUNE_AUTH_E2E_USER="${RUNE_AUTH_E2E_USER:-e2e}"

_rune_auth_lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_rune_auth_docker_dir="$(cd "${_rune_auth_lib_dir}/.." && pwd)"
_rune_auth_projects="$(cd "${_rune_auth_docker_dir}/../.." && pwd)"

_rune_auth_log()  { echo "[auth] $*" >&2; }
_rune_auth_fail() { echo "[auth] FAIL: $*" >&2; return 1; }

if command -v timeout >/dev/null 2>&1; then _RUNE_TIMEOUT=timeout
elif command -v gtimeout >/dev/null 2>&1; then _RUNE_TIMEOUT=gtimeout
else _RUNE_TIMEOUT=""; fi

_rune_auth_ql_image() {
    echo "${QL_IMAGE:-rune-ql-probe:$(git -C "${_rune_auth_projects}/rune-client-cpp" rev-parse --short HEAD 2>/dev/null || echo unknown)}"
}

# rune_auth_init : makes the per-run directory and exports RUNE_AUTH_DIR / RUNE_SECRETS_DIR / RUNE_AUTH_WORK_DIR.
# RUNE_SECRETS_DIR is what docker-compose*.yml mounts the key from.  A caller that already exported
# RUNE_SECRETS_DIR (auth-e2e.sh: docker/secrets) keeps it.
rune_auth_init() {
    RUNE_AUTH_DIR="$(mktemp -d "${_rune_auth_docker_dir}/.auth.XXXXXX")"
    chmod 700 "${RUNE_AUTH_DIR}"
    RUNE_AUTH_WORK_DIR="${RUNE_AUTH_WORK_DIR:-${RUNE_AUTH_DIR}/work}"   # a caller may pre-set it (catalog-e2e.sh: its /work)
    mkdir -p "${RUNE_AUTH_WORK_DIR}"
    if [ -z "${RUNE_SECRETS_DIR:-}" ]; then
        RUNE_SECRETS_DIR="${RUNE_AUTH_DIR}/secrets"
        _RUNE_AUTH_OWNS_SECRETS=1
    fi
    mkdir -p "${RUNE_SECRETS_DIR}"
    export RUNE_AUTH_DIR RUNE_AUTH_WORK_DIR RUNE_SECRETS_DIR
}

# rune_auth_cleanup : removes the generated files (cluster key, passwords).  Safe to call twice / uninitialised.
rune_auth_cleanup() {
    [ -n "${RUNE_AUTH_DIR:-}" ] || return 0
    rm -rf "${RUNE_AUTH_DIR}" 2>/dev/null || true
    if [ -z "${_RUNE_AUTH_OWNS_SECRETS:-}" ] && [ -n "${RUNE_SECRETS_DIR:-}" ]; then
        rm -f "${RUNE_SECRETS_DIR}/cluster.key" "${RUNE_SECRETS_DIR}/cluster.next.key" 2>/dev/null || true
    fi
    RUNE_AUTH_DIR=""
}

# rune_auth_gen_password <file> : a fresh 24-character alphanumeric password (>= 12, the CLI's minimum), mode 0600.
rune_auth_gen_password() {
    local f="$1" pw
    pw="$(LC_ALL=C head -c 96 /dev/urandom | base64 | LC_ALL=C tr -dc 'A-Za-z0-9' | head -c 24)" || true
    [ "${#pw}" -ge 12 ] || { _rune_auth_fail "could not generate a password"; return 1; }
    ( umask 077; printf '%s\n' "${pw}" > "${f}" )
}

# _rune_auth_cli <network> <rune args...> : runs `rune <args>` with the work + secrets mounts on the cluster network
# (or in RUNE_AUTH_TOOLS).  stdout/stderr pass through; the exit status is the CLI's (124 = the timeout fired).
_rune_auth_cli() {
    local net="$1"; shift
    local t="${RUNE_AUTH_CLI_TIMEOUT:-120}"
    local -a to=()
    [ -z "${_RUNE_TIMEOUT}" ] || to=("${_RUNE_TIMEOUT}" -k 5 "${t}")
    if [ -n "${RUNE_AUTH_TOOLS:-}" ]; then
        ${to[@]+"${to[@]}"} docker exec "${RUNE_AUTH_TOOLS}" rune "$@"
        return
    fi
    local name="rune-auth-cli-$$-${RANDOM}" rc
    ${to[@]+"${to[@]}"} docker run --rm --name "${name}" \
        --platform linux/amd64 --network "${net}" --shm-size=256m --security-opt seccomp=unconfined \
        -v "${RUNE_AUTH_WORK_DIR}:/work" -v "${RUNE_SECRETS_DIR}:/run/secrets:ro" \
        -v "${_rune_auth_docker_dir}/topics:/topics:ro" \
        "$(_rune_auth_ql_image)" rune "$@" && rc=0 || rc=$?
    [ "${rc}" -ne 124 ] || docker rm -f "${name}" >/dev/null 2>&1 || true
    return "${rc}"
}

# rune_auth_cli_admin <network> <rune args...> : the CLI as the bootstrap admin (password in /work/admin.pw).
rune_auth_cli_admin() {
    local net="$1"; shift
    _rune_auth_cli "${net}" "$1" --user "${RUNE_AUTH_ADMIN}" --password-file /work/admin.pw "${@:2}"
}

# rune_gen_cluster_key <dir> : writes <dir>/cluster.key (64 hex characters, mode 0600) with `rune security
# gen-cluster-key` — the CLI image must exist.  An existing key file is never overwritten (the CLI refuses).
rune_gen_cluster_key() {
    local dir="$1" name="${2:-cluster.key}"
    mkdir -p "${dir}"
    local abs; abs="$(cd "${dir}" && pwd)"
    docker run --rm --platform linux/amd64 -v "${abs}:/out" "$(_rune_auth_ql_image)" \
        rune security gen-cluster-key --out "/out/${name}" >&2 || { _rune_auth_fail "gen-cluster-key ${abs}/${name}"; return 1; }
    [ -s "${abs}/${name}" ] || { _rune_auth_fail "${abs}/${name} was not written"; return 1; }
}

# rune_bootstrap <broker host:port> <admin> <otp-file> <key-file> : bootstraps the first security admin of a fresh
# cluster (the one-time password is read from <otp-file>, generated if missing), then changes the admin's password
# with ALTER USER ... SET PASSWORD from a generated file (/work/admin.pw) — the bootstrap leaves the admin
# must-change, so every other statement would be refused (PasswordChangeRequired) until then.
# Retries (bounded by RUNE_AUTH_READY_S) while the cluster is still forming; "already bootstrapped" is final.
# <otp-file> and <key-file> are CONTAINER paths (/work/otp, /run/secrets/cluster.key).  The network is
# ${RUNE_AUTH_NETWORK} (a name); in a RUNE_AUTH_TOOLS setup it is ignored.
rune_bootstrap() {
    local broker="$1" admin="$2" otp="$3" key="$4"
    local net="${RUNE_AUTH_NETWORK:-}"
    local otp_host="${RUNE_AUTH_WORK_DIR}/$(basename "${otp}")"
    [ -s "${otp_host}" ] || rune_auth_gen_password "${otp_host}" || return 1
    local out rc deadline
    deadline=$(( $(date +%s) + ${RUNE_AUTH_READY_S:-150} ))
    while :; do
        set +e
        out="$(_rune_auth_cli "${net}" security bootstrap --admin "${admin}" --password-file "${otp}" \
                  --cluster-key-file "${key}" --broker "${broker}" 2>&1)"; rc=$?
        set -e
        if [ "${rc}" -eq 0 ]; then break; fi
        if echo "${out}" | grep -qiE 'already bootstrapped|already has a security admin'; then
            echo "${out}" >&2
            _rune_auth_fail "bootstrap: the cluster is already bootstrapped"; return 1
        fi
        if [ "$(date +%s)" -ge "${deadline}" ] || ! echo "${out}" | grep -qiE 'NotLeader|Redirect|timed out|connection refused|not connected|unreachable|not ready|no leader|Unavailable'; then
            echo "${out}" >&2
            _rune_auth_fail "bootstrap of ${broker} failed (exit ${rc})"; return 1
        fi
        _rune_auth_log "bootstrap not accepted yet ($(echo "${out}" | tail -1)); retrying in 2s"
        sleep 2
    done
    _rune_auth_log "bootstrapped ${admin}; changing its one-time password"
    [ -s "${RUNE_AUTH_WORK_DIR}/admin.pw" ] || rune_auth_gen_password "${RUNE_AUTH_WORK_DIR}/admin.pw" || return 1
    # The password literal travels in a file (the CLI refuses one on the command line: ps / shell history).
    ( umask 077; printf "ALTER USER %s SET PASSWORD '%s';\n" "${admin}" "$(cat "${RUNE_AUTH_WORK_DIR}/admin.pw")" \
        > "${RUNE_AUTH_WORK_DIR}/alter-admin.rql" )
    set +e
    out="$(_rune_auth_cli "${net}" ql --user "${admin}" --password-file "${otp}" -f /work/alter-admin.rql \
              --broker "${broker}" 2>&1)"; rc=$?
    set -e
    rm -f "${RUNE_AUTH_WORK_DIR}/alter-admin.rql"
    [ "${rc}" -eq 0 ] || { echo "${out}" >&2; _rune_auth_fail "ALTER USER ${admin} SET PASSWORD failed (exit ${rc})"; return 1; }
}

# rune_create_e2e_user <broker> <user> <password-file> : a role `e2e` with READ, WRITE, CREATE, ALTER, DROP on
# PREFIX '' topics and READ, CREATE, ALTER, DROP on PREFIX '' views (WRITE is not a view operation), READ GROUP PREFIX '', WRITE TRANSACTIONAL_ID PREFIX '' and DESCRIBE ON CLUSTER,
# granted to <user>, whose password is the content of <password-file> (a CONTAINER path under /work, generated
# on the host if missing).  Run as the bootstrap admin.
rune_create_e2e_user() {
    local broker="$1" user="$2" pwfile="$3"
    local net="${RUNE_AUTH_NETWORK:-}"
    local pw_host="${RUNE_AUTH_WORK_DIR}/$(basename "${pwfile}")"
    [ -s "${pw_host}" ] || rune_auth_gen_password "${pw_host}" || return 1
    local rql="${RUNE_AUTH_WORK_DIR}/create-e2e-user.rql" op
    (
        umask 077
        echo "CREATE ROLE e2e;"
        for op in READ WRITE CREATE ALTER DROP; do echo "GRANT ${op} ON TOPIC PREFIX '' TO ROLE e2e;"; done
        for op in READ CREATE ALTER DROP; do echo "GRANT ${op} ON VIEW PREFIX '' TO ROLE e2e;"; done   # WRITE is not a view operation (D17)
        echo "GRANT READ ON GROUP PREFIX '' TO ROLE e2e;"
        echo "GRANT WRITE ON TRANSACTIONAL_ID PREFIX '' TO ROLE e2e;"
        echo "GRANT DESCRIBE ON CLUSTER TO ROLE e2e;"
        printf "CREATE USER %s WITH PASSWORD '%s';\n" "${user}" "$(cat "${pw_host}")"
        echo "GRANT ROLE e2e TO USER ${user};"
    ) > "${rql}"
    local out rc
    set +e
    out="$(rune_auth_cli_admin "${net}" ql -f "/work/$(basename "${rql}")" --broker "${broker}" 2>&1)"; rc=$?
    set -e
    rm -f "${rql}"
    [ "${rc}" -eq 0 ] || { echo "${out}" >&2; _rune_auth_fail "creating the e2e role/user failed (exit ${rc})"; return 1; }
}

# rune_auth_prepare <network> <broker host:port> : bootstrap + the e2e user on a fresh cluster (idempotent: when the
# e2e user already logs in — a re-run on a persisted cluster — nothing is done).  Exports RUNE_AUTH_NETWORK and
# leaves /work/otp, /work/admin.pw, /work/e2e.pw in RUNE_AUTH_WORK_DIR.
rune_auth_prepare() {
    export RUNE_AUTH_NETWORK="$1"
    local broker="$2"
    if [ -s "${RUNE_AUTH_WORK_DIR}/e2e.pw" ] && \
       _rune_auth_cli "${RUNE_AUTH_NETWORK}" ql --user "${RUNE_AUTH_E2E_USER}" --password-file /work/e2e.pw -e "show history;" \
           --broker "${broker}" >/dev/null 2>&1; then
        return 0
    fi
    rune_bootstrap "${broker}" "${RUNE_AUTH_ADMIN}" /work/otp /run/secrets/cluster.key || return 1
    rune_create_e2e_user "${broker}" "${RUNE_AUTH_E2E_USER}" /work/e2e.pw || return 1
    _rune_auth_log "cluster bootstrapped: admin '${RUNE_AUTH_ADMIN}', e2e user '${RUNE_AUTH_E2E_USER}'"
}

# rune_auth_probe_args : the docker-run arguments that give a probe container the e2e credentials.
#   docker run ... $(rune_auth_probe_args) IMAGE
rune_auth_probe_args() {
    echo "-e RUNE_USER=${RUNE_AUTH_E2E_USER} -e RUNE_PASSWORD_FILE=/work/e2e.pw -v ${RUNE_AUTH_WORK_DIR}:/work:ro"
}
