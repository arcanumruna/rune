#!/bin/sh
# rune-broker entrypoint — BR-6
# POSIX sh, set -eu.
# src/main.cpp already parses NodeConfig from env; this script only:
#   1. defaults RUNE_DATA_DIR and creates the directory;
#   2. echoes key env vars to stderr for observability;
#   3. execs the binary as PID 1 so SIGTERM/SIGINT reach it → clean stop().

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

# ── Exec binary as PID 1 ─────────────────────────────────────────────────────
exec "$@"
