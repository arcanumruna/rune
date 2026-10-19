#!/usr/bin/env bash
# The FULL broker `*Regression*` suite — the checkpoint gate (CLAUDE.md rule 7).
#
#   scripts/run-regression.sh [build-dir] [jobs] [extra ctest args...]
#     build-dir  default $RUNE_BUILD_DIR, else build-fast; a broker-only build (cmake -S broker) or the monorepo
#                root build (cmake -S . -B build/root); the stage-c scripts handle both layouts
#     jobs       default 10 (stage a's -j)
#
# Three stages, ALL mandatory at every checkpoint (a checkpoint is green only when all are):
#   (a) every *Regression* test EXCEPT the latency gates, in parallel:
#         ctest -R "Regression|^batch:" -LE "latency|small" -j<jobs>
#       Every real-BrokerNode / UDP suite runs here concurrently: each process binds only ports it owns
#       (libs/aeronutil/include/rune/aeronutil/testing/FreePort.h — flock-owned port blocks) and pid-scoped
#       dirs, so there is no RESOURCE_LOCK serializing them.  The binaries made of thousands of millisecond
#       tests (label `small`) run as gtest-sharded batches "batch:<binary>:<k>of<n>" (label `batch`) — the
#       same *Regression* tests, a few processes instead of one per test (rune_regression_batches, root CMakeLists.txt).
#   (b) the latency gates (ctest label `latency`, RUN_SERIAL in libs/node/CMakeLists.txt), serially, on an
#       otherwise idle host — a latency measurement must not share the CPU with other test processes:
#         ctest -R Regression -L latency
#       Never skip (b): the gates are part of the regression suite, only scheduled apart.
#   (c) the Linux-only tests (review B-I1): on the Mac, scripts/run-linux-regression-vm.sh runs them on the VM;
#       RUNE_SKIP_LINUX_STAGE=1 skips it and the run is reported NOT GREEN (exit 3).  On Linux,
#       scripts/linux-stage-c.sh runs them (needs CAP_NET_ADMIN + a configurable netns, else NOT GREEN, exit 3).
# Prints each stage's wall time and the total; exits non-zero if any stage failed.  A latency stage over
# 60 s is reported (it is the serial tail of every checkpoint).
set -u
B="${1:-${RUNE_BUILD_DIR:-build-fast}}"; shift || true
J="${1:-10}"; shift || true

now() { perl -MTime::HiRes=time -e 'printf "%.1f\n", time'; }
el()  { perl -e "printf '%.1f', $2 - $1"; }

# R-N2 / R2-M2: the 4 BenchGateRegression gate self-tests must be registered in this tree (a tree configured without
# them would pass the checkpoint silently).
ctest --test-dir "$B" -N -R BenchGateRegression | grep -q 'Total Tests: 4$' || { echo "BenchGateRegression missing (want exactly 4 registered in $B)"; exit 1; }

# R3-M1: every non-injected slice / cluster cell whose exact codes were relaxed ([ perturbed ]: product evidence that a scheduler
# stall preceded) is appended here by the test processes and listed after the run for a human look (it does not fail the run).
export RUNE_PERTURBED_LOG="$(cd "$B" && pwd)/perturbed-cells.log"
: > "$RUNE_PERTURBED_LOG"

t0=$(now)
echo "== stage a: *Regression* without the latency gates (-j$J)"
ctest --test-dir "$B" -R "Regression|^batch:" -LE "latency|small" -j "$J" --output-on-failure "$@"; rcA=$?
t1=$(now)
echo "== stage b: *Regression* latency gates (serial)"
ctest --test-dir "$B" -R Regression -L latency --output-on-failure "$@"; rcB=$?
t2=$(now)

# (c) review B-I1: the Linux-only tests (UDP GSO / GRO incl. the DR-BUG-MTU regressions, the bind-order proof).
#     They are gated (RUNE_LINUX_GATE / RUNE_BIND_ORDER_GATE): stage a's plain run does not exercise them.  On
#     Linux, scripts/linux-stage-c.sh runs both for real in private netns (re-check M5) — or, without
#     CAP_NET_ADMIN and a configurable netns, says NOT GREEN (exit 3).  On the Mac they run on the VM.
echo "== stage c: Linux-only tests"
if [ "$(uname -s)" = "Linux" ]; then
    "$(dirname "$0")/linux-stage-c.sh" "$B"; rcC=$?
elif [ "${RUNE_SKIP_LINUX_STAGE:-0}" = "1" ]; then
    echo "== NOT GREEN: the Linux-only stage (UdpGsoBindings*, BindOrder) was skipped (RUNE_SKIP_LINUX_STAGE=1)"; rcC=3
else
    "$(dirname "$0")/run-linux-regression-vm.sh" 3 "$B"; rcC=$?
fi
t3=$(now)

if [ -s "$RUNE_PERTURBED_LOG" ]; then
  echo "== NOTE: $(wc -l < "$RUNE_PERTURBED_LOG" | tr -d ' ') non-injected perturbed cell(s) - look at them ($RUNE_PERTURBED_LOG):"
  cat "$RUNE_PERTURBED_LOG"
else
  echo "== perturbed cells (non-injected): 0"
fi
a=$(el "$t0" "$t1"); b=$(el "$t1" "$t2"); c=$(el "$t2" "$t3"); tot=$(el "$t0" "$t3")
echo "== wall: stage a ${a} s (rc $rcA), stage b ${b} s (rc $rcB), stage c ${c} s (rc $rcC), total ${tot} s"
if perl -e "exit !($b > 60)"; then echo "== NOTE: the latency stage took ${b} s (> 60 s)"; fi
[ "$rcA" -eq 0 ] && [ "$rcB" -eq 0 ] && [ "$rcC" -eq 0 ]
