#!/usr/bin/env bash
# The FULL broker `*Regression*` suite — the checkpoint gate (CLAUDE.md rule 7).
#
#   scripts/run-regression.sh [build-dir] [jobs] [extra ctest args...]
#     build-dir  default build-fast
#     jobs       default 10 (stage a's -j)
#
# Two stages, BOTH mandatory at every checkpoint (a checkpoint is green only when both are):
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
#       RUNE_SKIP_LINUX_STAGE=1 skips it and the run is reported NOT GREEN (exit 3).
# Prints each stage's wall time and the total; exits non-zero if any stage failed.  A latency stage over
# 60 s is reported (it is the serial tail of every checkpoint).
set -u
B="${1:-build-fast}"; shift || true
J="${1:-10}"; shift || true

now() { perl -MTime::HiRes=time -e 'printf "%.1f\n", time'; }
el()  { perl -e "printf '%.1f', $2 - $1"; }

t0=$(now)
echo "== stage a: *Regression* without the latency gates (-j$J)"
ctest --test-dir "$B" -R "Regression|^batch:" -LE "latency|small" -j "$J" --output-on-failure "$@"; rcA=$?
t1=$(now)
echo "== stage b: *Regression* latency gates (serial)"
ctest --test-dir "$B" -R Regression -L latency --output-on-failure "$@"; rcB=$?
t2=$(now)

# (c) review B-I1: the Linux-only tests (UDP GSO / GRO incl. the DR-BUG-MTU regressions, the bind-order proof).
#     On Linux they already ran in stage a (label linux-only); on the Mac they run on the VM.  Never green without.
echo "== stage c: Linux-only tests"
if [ "$(uname -s)" = "Linux" ]; then
    ctest --test-dir "$B" -L linux-only -N | tail -1; rcC=0
elif [ "${RUNE_SKIP_LINUX_STAGE:-0}" = "1" ]; then
    echo "== NOT GREEN: the Linux-only stage (UdpGsoBindings*, BindOrder) was skipped (RUNE_SKIP_LINUX_STAGE=1)"; rcC=3
else
    "$(dirname "$0")/run-linux-regression-vm.sh" 3 "$B"; rcC=$?
fi
t3=$(now)

a=$(el "$t0" "$t1"); b=$(el "$t1" "$t2"); c=$(el "$t2" "$t3"); tot=$(el "$t0" "$t3")
echo "== wall: stage a ${a} s (rc $rcA), stage b ${b} s (rc $rcB), stage c ${c} s (rc $rcC), total ${tot} s"
if perl -e "exit !($b > 60)"; then echo "== NOTE: the latency stage took ${b} s (> 60 s)"; fi
[ "$rcA" -eq 0 ] && [ "$rcB" -eq 0 ] && [ "$rcC" -eq 0 ]
