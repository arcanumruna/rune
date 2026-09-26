#!/usr/bin/env bash
# Tiny run of ALL run-matrix.sh phases to prove each phase emits cells.
# Correctness only (loaded box) — records are tiny, cores unpinned.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TIERS="D0 D1" HEAD_REC="256" PART_SWEEP="1 8" REC_SWEEP="256 1024" \
       WK_SWEEP="1 2" CORE_SWEEP="1 2" CONS_SWEEP="1 2" \
       PARTS_MAIN=8 WK_MAIN=2 REC=20000 LATREC=2000 WARMUP=500
exec bash "$HERE/run-matrix.sh"
