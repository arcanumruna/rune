#!/usr/bin/env bash
# Push live-development events to the Rune dashboard (rune-live).
# Used by AI sessions working on Rune to stream chat/plans and test/benchmark results.
#
#   scripts/rune-live.sh chat <kind> <text...>
#       kind: plan | thought | status
#   scripts/rune-live.sh test <suite> <status> [k=v ...]
#       <suite> = the STABLE test name (e.g. rune-broker/storage, dr-e2e). The dashboard
#                 CONFLATES by suite: re-runs of the same name fold into one row with a run
#                 count. Do NOT pass a per-run or ticket id here — use the stable test name.
#       status:   passed | failed | running
#       common k=v: passed=.. failed=.. total=.. p99Us=.. durationMs=.. commit=.. kind=test|benchmark
#   scripts/rune-live.sh raw-test '<json>'
#
# Config: RUNE_LIVE_URL (default https://live.regressorspath.com),
#         RUNE_PUSH_TOKEN (default = dev token).
set -euo pipefail
LIVE_URL="${RUNE_LIVE_URL:-https://live.regressorspath.com}"
TOKEN="${RUNE_PUSH_TOKEN:-***REMOVED***}"

post() { curl -sS -m 10 -X POST "$LIVE_URL/api/$1" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "$2"; echo; }

sub="${1:-}"; shift || true
case "$sub" in
  chat)
    kind="${1:-thought}"; shift || true; text="$*"
    body=$(python3 -c 'import json,sys;print(json.dumps({"kind":sys.argv[1],"text":sys.argv[2]}))' "$kind" "$text")
    post chat "$body" ;;
  test)
    # first arg is the STABLE test/suite name — the dashboard conflates by it
    suite="${1:?suite (stable test name) required}"; status="${2:?status required}"; shift 2 || true
    body=$(python3 - "$suite" "$status" "$@" <<'PY'
import json, sys
d = {"suite": sys.argv[1], "status": sys.argv[2], "kind": "test"}
for kv in sys.argv[3:]:
    if "=" in kv:
        k, v = kv.split("=", 1)
        try: v = int(v)
        except ValueError: pass
        d[k] = v
print(json.dumps(d))
PY
)
    post tests "$body" ;;
  raw-test) post tests "${1:?json body required}" ;;
  *) echo "usage: rune-live.sh {chat <kind> <text...>|test <suite> <status> [k=v...]|raw-test <json>}" >&2; exit 1 ;;
esac
