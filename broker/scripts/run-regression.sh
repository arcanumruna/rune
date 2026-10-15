#!/usr/bin/env bash
# The FULL broker `*Regression*` suite — the checkpoint gate (CLAUDE.md rule 7).
#
#   scripts/run-regression.sh [build-dir] [jobs] [extra ctest args...]
#     build-dir  default build-fast
#     jobs       default 10 (stage a's -j)
#
# Two stages, BOTH mandatory at every checkpoint (a checkpoint is green only when both are):
#   (a) every *Regression* test EXCEPT the latency gates, in parallel:
#         ctest -R Regression -LE latency -j<jobs>
#       Every real-BrokerNode / UDP suite runs here concurrently: each process binds only ports it owns
#       (libs/aeronutil/include/rune/aeronutil/testing/FreePort.h — flock-owned port blocks) and pid-scoped
#       dirs, so there is no RESOURCE_LOCK serializing them.
#   (b) the latency gates (ctest label `latency`, RUN_SERIAL in libs/node/CMakeLists.txt), serially, on an
#       otherwise idle host — a latency measurement must not share the CPU with other test processes:
#         ctest -R Regression -L latency
#       Never skip (b): the gates are part of the regression suite, only scheduled apart.
# Prints each stage's wall time and the total; exits non-zero if either stage failed.  A latency stage over
# 60 s is reported (it is the serial tail of every checkpoint).
set -u
B="${1:-build-fast}"; shift || true
J="${1:-10}"; shift || true

now() { perl -MTime::HiRes=time -e 'printf "%.1f\n", time'; }
el()  { perl -e "printf '%.1f', $2 - $1"; }

t0=$(now)
echo "== stage a: *Regression* without the latency gates (-j$J)"
ctest --test-dir "$B" -R Regression -LE latency -j "$J" --output-on-failure "$@"; rcA=$?
t1=$(now)
echo "== stage b: *Regression* latency gates (serial)"
ctest --test-dir "$B" -R Regression -L latency --output-on-failure "$@"; rcB=$?
t2=$(now)

a=$(el "$t0" "$t1"); b=$(el "$t1" "$t2"); tot=$(el "$t0" "$t2")
echo "== wall: stage a ${a} s (rc $rcA), stage b ${b} s (rc $rcB), total ${tot} s"
if perl -e "exit !($b > 60)"; then echo "== NOTE: the latency stage took ${b} s (> 60 s)"; fi
[ "$rcA" -eq 0 ] && [ "$rcB" -eq 0 ]
