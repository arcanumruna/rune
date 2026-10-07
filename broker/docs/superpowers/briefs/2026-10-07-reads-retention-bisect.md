# Reads+Retention/2p tmpfs regression: bisect (2026-10-07)

Measurement only. No code was changed and nothing was committed.

## Result

- **First bad commit: `2b5524d`** ("WIP (incomplete, does not build/pass yet): hot-path latency fix checkpoint"). It reaches the main line through the merge **`9239f13`** (Merge integ/perf-defaults into perf/read-role-hotpath; parents `2b5524d` and `70217bd`).
- **Last good: `70217bd`.**
  - The diff `70217bd → 9239f13` has exactly the same changed lines as `e09b2f7 → 2b5524d`: 7 files, +496/-96.
  - Files: `libs/node/include/rune/node/Worker.h` (+305/-96 churn), `WorkerReturnCodec.h`, `WorkerOp.h`, `WorkerPool.h`, `libs/gateway/src/ClientGateway.cpp`, `libs/node/CMakeLists.txt`, and the new `tests/integration/HotPathBRegressionTest.cpp`.
- **Bad commits:** every hot-path B commit after it (be9f307 … 4a94bd1) and every main-line point from `f1f4ebd` (the hot-path B merge) to `dc274e8`. Nothing later makes it measurably better or worse.
- **Main-line answer:** the first bad main-line point is **`f1f4ebd`** (merge perf/read-role-hotpath), not `375a539`. `375a539` only merges the DR-10 lows (consensus) on top of `f1f4ebd`.

### Clearest signature

The clearest signal is not p99.9. It is the **worker part of the worst solo `read` pass**: the gate field `solo max: read=X(wN)`.

| | Runs with solo-read worker part ≥ 300 µs | Median solo-read worker part |
|---|---|---|
| 70217bd (all baseline runs) | 26 / 156 (17 %) | about 100-150 µs |
| 9239f13 and every later point | 9-10 / 10 in every batch, 8 / 8 on the main line (about 99 %) | about 850-1,100 µs |

- Bad side: the worst passes are `produce+read` with `i/w/r ≈ 1/500-4500/1`, so the cost is in the worker and the intake part is about zero.
- Baseline: the worst passes are intake-dominated `produce+roll+read` (`i` = 200-2,200 µs, `w` < 50 µs).
- This matches the final-verification observation.

## Method

- **Box:** k3s-node-01 (178.104.16.102), bare metal, 4 cores. k3s was stopped (inactive) throughout. `docker ps -q` was empty before every batch, and every run line logged `containers=0`.
- **Recipe:** the one from `2026-10-07-final-verification.md` §2, reused exactly.
  - Builder image `rune-latgate-builder:dc274e8` (gcc-13, conan Release; rune-protocol/rune-codec/rune-fr baked into the image as in `v/prep.sh`).
  - Only the target `rune_duty_pass_latency_gate_tests` was built, in Release.
  - Each run was `docker run --tmpfs /data:rw,size=6g -e TMPDIR=/data`, preceded by `sync; drop_caches`.
  - Baseline binary: the existing `/root/e3g/gate/build-70217bd`. dc274e8 used `/root/e3g/gate/build-dc274e8`. Every other SHA was built under `/root/e3g/bisect/<sha>/build` from `git archive <sha>`.
- **Filter:** only `--gtest_filter=DutyPassLatencyGateRegression.ConsumerReadsDuringRetention`.
- **Interleaving:** per round, `A(70217bd), X1, A, X2, A, X3, …`, repeated N rounds. Every X run is paired with its own adjacent A run.
- **Numbers used:** the `try=1` line. Values are median (p25-p75) in µs.
- **Verdicts:** "ovl batch" compares the IQR with that SHA's own interleaved A runs. "ovl pooled" compares it with all A runs of the batch.
- **Caveat (the effect is smaller in single-scenario mode):**
  - Run alone, the baseline p99.9 is about 160 µs (pooled n = 156), against 128 µs inside the 7-scenario sequence.
  - The bad side is about 200-235 µs, against 226 µs.
  - So p99.9 shifts by about +50 µs (+30 %), with some IQR overlaps at individual points.
  - The solo-read worker part separates every point without ambiguity (table above).

## Level 1: main line (first-parent of integ/perf-defaults), n = 8 per side

`f1f4ebd` was added because it sits between `70217bd` and `375a539` on the first-parent line.

Pooled A (n = 56): p99.9 161 (148-175), max 780 (437-1,421), solo-read w 130 (103-211).

| SHA | A p99.9 (batch) | X p99.9 | IQR overlap batch / pooled | A max | X max | A solo-read w | X solo-read w |
|---|---|---|---|---|---|---|---|
| f1f4ebd (hot-path B merge) | 180 (157-218) | 210 (201-223) | yes / **NO** | 523 (442-1,048) | 1,609 (1,211-2,287) | 124 (88-213) | **1,351 (1,172-1,839)** |
| 375a539 (D18 / DR-10 lows) | 166 (154-177) | 229 (212-262) | **NO / NO** | 955 (679-1,109) | 1,728 (1,022-2,460) | 108 (98-128) | **951 (875-1,188)** |
| 8af912f (D11) | 169 (156-174) | 188 (176-257) | **NO / NO** | 757 (597-1,513) | 1,202 (994-1,409) | 157 (145-324) | **1,046 (927-1,290)** |
| cedafb9 (D20) | 157 (153-162) | 214 (200-238) | **NO / NO** | 978 (354-2,223) | 971 (841-1,566) | 118 (100-166) | **680 (586-904)** |
| 4dfeb0b (D15) | 161 (145-171) | 214 (200-232) | **NO / NO** | 290 (244-618) | 1,224 (1,053-2,416) | 115 (99-128) | **1,220 (774-2,415)** |
| 4a3130c (D19/D20b/D21) | 150 (140-164) | 209 (198-289) | **NO / NO** | 807 (655-1,281) | 973 (784-1,417) | 352 (133-927) | **843 (724-1,004)** |
| dc274e8 (bad) | 172 (146-184) | 226 (211-244) | **NO / NO** | 1,189 (858-1,580) | 895 (784-1,206) | 121 (107-323) | **864 (681-934)** |

## Level 2: inside hot-path B (f1f4ebd^1..f1f4ebd^2), n = 10 per side

Pooled A (n = 80): p99.9 159 (143-182), max 837 (406-1,636), solo-read w 138 (109-203).

| SHA | A p99.9 (batch) | X p99.9 | overlap batch / pooled | A max | X max | A solo-read w | X solo-read w |
|---|---|---|---|---|---|---|---|
| be9f307 | 173 (161-188) | 200 (178-232) | yes / yes | 693 (485-1,989) | 1,013 (856-1,096) | 148 (100-205) | **971 (650-1,093)** |
| f9ad01e | 158 (145-182) | 201 (180-230) | yes / yes | 1,418 (917-2,391) | 913 (851-1,212) | 143 (123-186) | **857 (756-948)** |
| 4c6fa49 | 150 (136-165) | 220 (186-261) | NO / NO | 489 (334-1,297) | 1,168 (852-3,147) | 122 (103-171) | **896 (819-1,234)** |
| c2f8d9b | 161 (142-163) | 239 (218-280) | NO / NO | 645 (506-848) | 972 (849-1,562) | 115 (98-130) | **959 (825-1,237)** |
| 80b61f1 | 171 (143-197) | 201 (194-218) | yes / NO | 1,061 (576-1,814) | 830 (746-1,327) | 138 (106-242) | **777 (675-1,318)** |
| fb4c46b | 154 (138-164) | 190 (165-242) | NO / yes | 880 (470-3,081) | 1,536 (940-2,315) | 150 (111-170) | **1,098 (622-2,082)** |
| ef1782d | 151 (141-155) | 266 (248-288) | NO / NO | 585 (302-1,167) | 1,264 (1,094-3,350) | 193 (130-270) | **1,077 (853-1,498)** |
| 4a94bd1 | 164 (148-182) | 206 (192-241) | NO / NO | 1,039 (673-1,412) | 1,118 (827-1,510) | 150 (119-255) | **934 (568-1,353)** |

The oldest commit, `be9f307`, was already bad by the solo-read worker part, although its p99.9 overlapped at n = 10. Its parent is the merge `9239f13`, so that point was measured too.

### Level 2b: be9f307's parent, n = 10 per side

| SHA | A p99.9 (batch) | X p99.9 | overlap | A max | X max | A solo-read w | X solo-read w |
|---|---|---|---|---|---|---|---|
| **9239f13** (= 70217bd + WIP 2b5524d) | 164 (143-182) | 206 (184-222) | **NO** | 937 (712-1,612) | 1,110 (884-1,719) | 95 (82-137) | **924 (508-1,127)** |
| be9f307 (re-measured) | 164 (153-173) | 233 (215-241) | **NO** | 658 (432-1,053) | 2,501 (763-4,326) | 106 (92-113) | **1,066 (739-3,256)** |

- `9239f13`'s only change against `70217bd` is the WIP commit `2b5524d`.
- `2b5524d` itself is based on `e09b2f7`, an ancestor of `70217bd` that is 31 commits older. It was not built separately, because its changes are line-identical to those of `9239f13`.

Note on the coordinator's mid-task request for a binary search: the linear sweep of all 8 hot-path B commits had already finished when it arrived. The sweep plus the 9239f13 point answer the same question with more data.

## Worst-pass breakdown: first bad (9239f13) vs last good (70217bd), same batch

**70217bd (A), the 3 worst passes per run:**

- Intake-dominated `produce+roll+read`, for example `720.5 (i/w/r=688/31/2)`, `1420.2 (1407/13/1)`, `2216.7 (2193/22/2)`, `2807.0 (2763/41/3)`.
- The solo-read max is mostly 49-149 µs worker.
- Outliers: 2 of 10 runs had a solo read with w = 680 µs and w = 1,673 µs.

**9239f13 (X), the 3 worst passes per run:**

- Worker-dominated `produce+read`, for example:
  - `1582.7 (i/w/r=12/1570/1)`
  - `1069.6 (0/1069/1)`
  - `4499.2 (6/4489/5)`, `2940.8 (1/2939/1)`, `2290.5 (4/2284/3)`
  - `876.5 (1/874/1)`
  - `1150.4 (4/1146/1)`
- Also `produce+retention+read 1469.9 (1/1468/2)`.
- The solo-read worker part is ≥ 300 µs in 10 of 10 runs.

**Counters:** the same order of magnitude on both sides, so there is no workload drift.

- reads = 20,000 and readMiB ≈ 100.
- retentionDrops: A 96-155; X 73-240.
- rolls ≈ 4,990.

## Exact commands

```bash
# source per SHA (local)
git -C /Users/vradu/Projects/rune-broker-wt26 archive <sha> | ssh root@178.104.16.102 'mkdir -p /root/e3g/bisect/<sha>/src && tar -x -C /root/e3g/bisect/<sha>/src'

# /root/e3g/bisect/build.sh <sha>...   (under flock -w 7200 /root/vm.lock)
docker run --rm -v $d/src:/src:ro -v $d/build:/work rune-latgate-builder:dc274e8 bash -c "cmake -S /src -B /work/build -G Ninja \
  -DCMAKE_TOOLCHAIN_FILE=/build/conan-release/build/Release/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON -DBUILD_BENCHMARKS=OFF -DBUILD_INTEGRATION_TESTING=OFF -DRUNE_PROTOCOL_INCLUDE=/rune-protocol/include \
  -DRUNE_CODEC_ROOT=/rune-codec && cmake --build /work/build --target rune_duty_pass_latency_gate_tests -j 4"

# /root/e3g/bisect/run.sh <sha>...   (N=8|10 TAG=main|hpb|wip, under flock -w 7200 /root/vm.lock)
# per round: for each sha: run 70217bd A-<sha>-i; run <sha> X-<sha>-i
sync; echo 3 > /proc/sys/vm/drop_caches
docker run --rm --ulimit nofile=1048576:1048576 -v <build-dir>:/work:ro --tmpfs /data:rw,size=6g -e TMPDIR=/data \
  rune-latgate-builder:dc274e8 /work/build/libs/node/rune_duty_pass_latency_gate_tests \
  --gtest_filter=DutyPassLatencyGateRegression.ConsumerReadsDuringRetention

# batches actually run
N=8  TAG=main ./run.sh dc274e8 f1f4ebd 375a539 8af912f cedafb9 4dfeb0b 4a3130c
N=10 TAG=hpb  ./run.sh be9f307 f9ad01e 4c6fa49 c2f8d9b 80b61f1 fb4c46b ef1782d 4a94bd1
N=10 TAG=wip  ./run.sh 9239f13 be9f307
```

- **Parsing:** from the `[LatencyGate] Reads+Retention/2p … Release try=1` line, take `p99.9=`, `max=` and `solo max: read=…(wN)`. Quartiles use Python `statistics.quantiles(method='inclusive')`.
- **Logs on the box:** `/root/e3g/bisect/runs/{main,hpb,wip}/{A,X}-<sha>-<i>.log`. Also copied locally to `/tmp/bisect/runs/`.
- **Discarded batch:** `/root/e3g/bisect/runs/main-bug-allA/` is a discarded first main-line batch. A shell bug (a non-local variable in `run()`) made every run use the 70217bd binary. It is kept only as extra baseline data: n = 56, p99.9 158 (144-180).

## Box state at the end

- Lock free, no runner processes, `docker ps -aq` empty, k3s inactive.
- Only per-container tmpfs mounts were used, so nothing is left to unmount.
- Disk: 22 GiB free (> 10 GiB), so the `/root/e3g/bisect/<sha>/{src,build}` trees are kept (about 25 MB build each) for follow-up runs.
