#!/bin/sh
# rune-broker entrypoint — BR-6
# POSIX sh, set -eu.
# src/main.cpp already parses NodeConfig from env; this script only:
#   1. defaults RUNE_DATA_DIR and creates the directory;
#   2. echoes key env vars to stderr for observability;
#   3. refuses to start without a cluster key file (authentication is ON by default; the broker
#      binary refuses too — this check only surfaces it before any side effect, with the broker's
#      own wording).  It NEVER generates a key: a key made inside one container would differ from
#      its peers' and silently split the cluster.  `rune security gen-cluster-key --out <path>`
#      makes one; mount it read-only at RUNE_CLUSTER_KEY_FILE on every node.
#   4. execs the binary as PID 1 so SIGTERM/SIGINT reach it → clean stop().

set -eu

# ── Default RUNE_DATA_DIR ─────────────────────────────────────────────────────
: "${RUNE_DATA_DIR:=/var/lib/rune}"
export RUNE_DATA_DIR

mkdir -p "${RUNE_DATA_DIR}"

# ── Observability echo (stderr) ───────────────────────────────────────────────
echo "[entrypoint] RUNE_NODE_ID=${RUNE_NODE_ID:-<unset>}" >&2
echo "[entrypoint] RUNE_PEERS=${RUNE_PEERS:-<unset>}" >&2
echo "[entrypoint] RUNE_DATA_DIR=${RUNE_DATA_DIR}" >&2
echo "[entrypoint] RUNE_CLIENT_ENDPOINT=${RUNE_CLIENT_ENDPOINT:-<unset>}" >&2

# ── Authentication is on by default: a cluster key file is mandatory ───────────
# Only the broker process itself is subject to the check (not `rune_broker storage format`, `sh`, ...).
case "${1:-}" in
    */rune_broker|rune_broker)
        if [ "${2:-}" != "storage" ] && [ "${RUNE_AUTH:-enabled}" != "disabled" ]; then
            if [ -z "${RUNE_CLUSTER_KEY_FILE:-}" ]; then
                echo "rune-broker: configuration error: RUNE_CLUSTER_KEY_FILE is not set — a cluster key file is required" \
                     "(authentication is on by default; RUNE_AUTH=disabled is for single-node development only)" >&2
                exit 2
            fi
            if [ ! -r "${RUNE_CLUSTER_KEY_FILE}" ]; then
                echo "rune-broker: configuration error: RUNE_CLUSTER_KEY_FILE=${RUNE_CLUSTER_KEY_FILE}: cannot be read" \
                     "(mount the key file read-only; \`rune security gen-cluster-key --out <path>\` makes one)" >&2
                exit 2
            fi
        fi
        ;;
esac

# ── Exec binary as PID 1 ─────────────────────────────────────────────────────
exec "$@"
