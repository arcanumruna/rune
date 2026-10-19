#!/usr/bin/env bash
# Regression: the bench launchers must use a topology authentication accepts (validateAuthTopology): no per-worker
# ingress/egress outside shard mode, and no legacy worker mode as a default or a forced setting.
set -u
d="$(cd "$(dirname "$0")" && pwd)"; rc=0
bad() { echo "FAIL: $*" >&2; rc=1; }
grep -nE '^[^#]*RUNE_PER_WORKER_(INGRESS|EGRESS)=true' "$d/lib.sh" "$d/cluster.sh" && bad "a launcher sets per-worker ingress/egress"
grep -nE '^[^#]*RUNE_WORKER_MODE[=:-]+[^ ]*legacy' "$d/lib.sh" "$d/replbench.sh" && bad "a launcher defaults/forces legacy worker mode"
grep -q 'RUNE_WORKER_MODE:-shard' "$d/lib.sh" || bad "rune_start_broker does not default to shard mode"
[ "$rc" -eq 0 ] && echo "bench launchers are auth-compatible"
exit "$rc"
