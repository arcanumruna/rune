# Final box verification — rune-broker 6b5ea62 (2026-10-08)

Run-and-report only; no root-cause interpretation.

## Environment

| item | value |
|---|---|
| box | k3s-node-01 ($RUNE_VM_HOST), k3s stopped throughout |
| virtualization | KVM guest (`QEMU HARDDISK` root disk), not physical bare metal |
| kernel | 5.15.0-173-generic (Ubuntu 22.04.5 LTS) |
| CPU | 4 vCPU, Intel Xeon Processor (Skylake, IBRS, no TSX), 1 thread/core, 1 NUMA node, 16 MiB L3 |
| RAM | 7 GiB |
| root fs | ext4 (rw,relatime) on /dev/sda1, 75G (14G free) |
| docker | 29.1.3 |
| broker image | `rune-broker-node:6b5ea62` (id sha256:938b3cc9…, created 2026-10-08T11:51:25Z); built from `/root/e3g/rune-broker` checked out at 6b5ea62 (incremental bundle from dc274e8) |
| DR client image | `rune-dr-client:4c36d0b` |
| gate builder image | `rune-latgate-builder:dc274e8` |
| gate binaries | A = 70217bd (`/root/e3g/gate/build-70217bd`), B = m6b = 6b5ea62 (`/root/e3g/bisect/m6b/build`), Release, target `rune_duty_pass_latency_gate_tests` |

Image build note: `docker compose build` (3 services sharing one tag) returned non-zero with
`target rune-1: failed to solve: image "docker.io/library/rune-broker-node:6b5ea62": already exists`
(the tag was written by a sibling service). A follow-up `docker compose build rune-1` completed rc=0
fully cached (`rune-broker-node:6b5ea62  Built`). Logs: `/root/e3g/broker-build-6b5ea62.log`, `/root/e3g/broker-build-6b5ea62-rune1.log`.

## DR (dockerized, scoped)

Runner `/root/e3g/dr6b.sh` (same invocation as `all.sh`: drcapture plugin, `--runxfail` for dr10, `plugin/clean.sh` between files, `RUNE_DR_COMPOSE_OVERRIDES` unset, harness tag = git HEAD = 6b5ea62).

| file | result | wall |
|---|---|---|
| test_dr10_zombie_leader (`--runxfail`) | 8 passed / 0 failed | 1725 s |
| test_dr3_leaderkill | 10 passed / 0 failed, 4 warnings | 1666 s |
| test_dr4_quorum | 3 passed / 0 failed | 364 s |
| test_dr7_crash_restart | 9 passed / 0 failed | 1345 s |
| **total** | **30 / 30 passed** | |

Per-test call durations (s):

- dr10: pause_idle_leader_zombie_fencing[1] 188.1, [3] 187.2; pause_during_produce_zombie_fencing[1] 188.7, [3] 189.5; short_vs_long_pause[short] 183.0, [long] 201.6; repeated_flap_zombie[3] 212.9, [5] 272.5
- dr3: leader_kill[0-idle-1] 68.2, [0-idle-3] 69.6, [1-idle-1] 68.1, [1-idle-3] 69.1, [0-mid_produce-1] 305.9, [0-mid_produce-3] 306.9, [1-mid_produce-1] 305.9, [1-mid_produce-3] 307.0; rolling_leader_kill[2] 17.8, [3] 24.8
- dr4: quorum_holds_kill_follower[1] 70.9, [3] 72.2; quorum_lost_safety_and_recovery 183.0
- dr7: follower_crash_restart_catchup[0-0] 115.1, [0-1] 115.3, [1-0] 115.1, [1-1] 115.3; leader_crash_restart_rejoins_follower[0] 203.0, [1] 203.0; sigterm_vs_sigkill_parity[SIGTERM] 112.3, [SIGKILL] 112.6; full_cluster_cold_restart 138.0

dr3 warnings (all 4 `mid_produce` cases, `PytestUnhandledThreadExceptionWarning` in a harness client thread), verbatim (one shown; the other three are identical but for client id):

```
TimeoutError: command timed out after 240s: docker run --rm --name dr-client-321000-c0064a34-a0 --network rune-dr-fault_default ... -e RUNE_MODE=produce -e RUNE_RECORDS=200 ... -e RUNE_ACKS=all -e RUNE_EXPECT_CID_EXACT=321000 rune-dr-client:4c36d0b
```

The dc274e8 run of the same file (`/root/e3g/results/default-test_dr3_leaderkill.log`) shows the same 4 warnings (4 "timed out after 240 seconds" occurrences in each log).

No failures, so the drcapture dirs are empty. Full pytest logs, junit xml, and client logs are kept anyway.

## D27 latency gate re-measurement

Method: `/root/e3g/gate6b.sh` → `/root/e3g/gate6b-one.sh`. Each run uses its own container (`--ulimit nofile=1048576:1048576`, `TMPDIR=/data`, `RUNE_LATENCY_GATE_BENCH=1`) and filters to one scenario. Every run gets a fresh `/data`:
- **ext4**: a new 8 GiB fallocated file, `mkfs.ext4 -q`, loop-mounted at `/mnt/gate`, bind-mounted as `/data`, then unmounted and deleted after the run.
- **tmpfs**: `--tmpfs /data:rw,size=6g`.

`sync; echo 3 > /proc/sys/vm/drop_caches` runs before each run. The order is interleaved ABAB: in each of 5 rounds, every scenario runs on ext4 (A, B) and then on tmpfs (A, B). FollowerFetchDuringRetention is m6b-only. Every run started with `containers=0`.

**Tries:** in bench mode, each run printed exactly **one** `try=1` line. Bench mode never fails, so it never retries. So best-of-tries = worst-of-tries = that single try, and the table shows one value per run. The bench pass scale is 4x (`passes=80000` for 2p scenarios).

Values are HOT p99.9 and HOT max in µs, shown as median [min–max] over n runs. ⚠ marks where m6b is worse beyond the run-to-run range: m6b median > 70217bd max (✖ means even m6b min > 70217bd max).

| scenario | fs | 70217bd p99.9 | m6b p99.9 | 70217bd max | m6b max |
|---|---|---|---|---|---|
| LeadershipChangeEightPartitions | ext4 | 552.7 [530.1–576.7] | 242.0 [184.3–284.0] | 5273.2 [4290.6–16398.1] | 11721.4 [5949.3–22458.9] |
| LeadershipChangeEightPartitions | tmpfs | 188.2 [186.3–193.4] | 173.5 [170.3–187.9] | 2810.6 [1279.1–9697.0] | 6944.1 [5314.0–9899.4] |
| ConsumerReadsDuringRetention | ext4 | 3114.4 [2988.6–3378.6] | 3157.3 [2965.6–3246.7] | 10871.8 [9321.3–15627.7] | 11745.1 [6003.0–16088.4] |
| ConsumerReadsDuringRetention | tmpfs | 148.7 [133.6–172.1] | 99.8 [90.6–163.3] | 1099.8 [877.0–1911.6] | 623.7 [292.4–3541.4] |
| FollowerTruncationDuringCleanerPass | ext4 | 381.4 [357.0–644.0] | **2442.6 [749.5–3499.7] ✖** | 730.0 [575.4–2195.9] | **6187.4 [5476.1–9365.3] ✖** |
| FollowerTruncationDuringCleanerPass | tmpfs | 76.9 [75.8–90.1] | 77.9 [67.0–81.9] | 176.2 [139.1–899.6] | 227.0 [116.0–562.4] |
| Compaction64MiBGroup | ext4 | 93.0 [92.0–107.6] | 100.3 [84.2–106.6] | 2114.4 [1107.0–6732.2] | 2298.8 [974.6–4931.2] |
| Compaction64MiBGroup | tmpfs | 37.7 [33.0–45.1] | 39.2 [34.5–47.0] | 600.0 [157.5–1876.5] | 329.1 [168.0–476.8] |
| TxnMarkersFourPartitions | ext4 | 259.0 [240.1–272.5] | **277.1 [271.6–284.6] ⚠** | 1334.3 [993.0–4778.0] | 1306.2 [664.0–3328.3] |
| TxnMarkersFourPartitions | tmpfs | 90.4 [77.7–102.6] | 83.9 [75.5–94.0] | 252.8 [212.4–3728.2] | 394.6 [275.1–2392.9] |
| SegmentRollSinglePartition | ext4 | 5528.6 [5424.6–5797.1] | **5843.8 [5468.2–6023.6] ⚠** | 16194.7 [14380.7–42084.6] | 12833.0 [12735.7–19397.6] |
| SegmentRollSinglePartition | tmpfs | 79.8 [67.4–105.1] | 57.7 [56.5–60.4] | 267.3 [212.3–564.9] | 202.3 [161.7–230.2] |
| SegmentRollEightPartitions (bench scale 4) | ext4 | n=2: 13146.4 [12264.9–14027.9] | n=1: 12774.6 | n=2: 27438.2 [22018.9–32857.6] | n=1: 21459.3 |
| SegmentRollEightPartitions (bench scale 4) | tmpfs | n=0 | n=0 | n=0 | n=0 |
| FollowerFetchDuringRetention (new) | ext4 | — | 3035.4 [2972.3–3176.4] | — | 10650.5 [7236.9–15681.7] |
| FollowerFetchDuringRetention (new) | tmpfs | — | 110.4 [103.0–897.2] | — | 363.3 [269.9–25014.3] |

FollowerTruncationDuringCleanerPass ext4 per-run values (p99.9 / max, in run order 1–5):
- 70217bd: 412.0/948.1, 357.0/730.0, 365.1/622.6, 381.4/575.4, 644.0/2195.9
- m6b: 3499.7/9365.3, 2442.6/5602.9, 749.5/5476.1, 2210.6/6187.4, 3490.4/7248.0

FollowerFetchDuringRetention tmpfs per-run: 110.4/3456.9, 103.0/363.3, **897.2/25014.3** (run 3), 116.3/321.3, 109.7/269.9.

### SegmentRollEightPartitions: bench-mode failure (both binaries) and scale-1 supplement

At the bench default scale (4x), **all 20** SegmentRollEightPartitions runs failed on **both** binaries and **both** filesystems (gtest FAILED, rc=1). 17 of them printed no LatencyGate line. 3 (ext4: 70217bd runs 2 and 5, m6b run 5) printed try=1 and then failed. Verbatim:

```
[warning] Log /data/rune_latgate_1_0/1_5: segment provisioning failed (errno=5, failures=1): the next roll creates its segment inline
[error] ProducePath: appendNoSyncRestamp threw: mmap failed: /data/rune_latgate_1_0/1_5/00000000000000278336.index; Internal
[critical] Worker 0: fsync FAILED on /data/rune_latgate_1_0/1_3 (errno=5) — log permanently failed; signalling the runtime to halt the broker
C++ exception with description "std::bad_alloc" thrown in the test body.      (17 runs)
/src/libs/node/tests/integration/DutyPassLatencyGateTest.cpp:994: Failure
Expected equality of these values:
  rig.produceErrors
    Which is: 26457
  0u                                                                             (3 runs that printed try=1)
```

`vm.max_map_count = 65530` on the box. dmesg showed no OOM-killer lines.

Supplement (`/root/e3g/gate6b-s1.sh`): this used the same method, ABAB, n=5 per binary per fs, adding `RUNE_LATENCY_GATE_SCALE=1` (bench mode still on). All 20 runs passed (rc=0).

| scenario (scale 1) | fs | 70217bd p99.9 | m6b p99.9 | 70217bd max | m6b max |
|---|---|---|---|---|---|
| SegmentRollEightPartitions | ext4 | 13563.0 [12841.0–14934.5] | 13261.1 [12028.0–13923.0] | 19804.6 [18015.1–25192.4] | 17907.3 [16320.4–23797.8] |
| SegmentRollEightPartitions | tmpfs | 499.4 [406.8–689.7] | 362.8 [335.5–398.8] | 1417.0 [477.6–1947.1] | 438.7 [406.2–1621.6] |

### Regressions vs 70217bd beyond noise (m6b median > 70217bd max)

1. **FollowerTruncationDuringCleanerPass / ext4 / HOT p99.9**: m6b 2442.6 [749.5–3499.7] vs 70217bd 381.4 [357.0–644.0]. No overlap.
2. **FollowerTruncationDuringCleanerPass / ext4 / HOT max**: m6b 6187.4 [5476.1–9365.3] vs 70217bd 730.0 [575.4–2195.9]. No overlap.
3. **TxnMarkersFourPartitions / ext4 / HOT p99.9**: m6b 277.1 [271.6–284.6] vs 70217bd 259.0 [240.1–272.5]. Ranges overlap.
4. **SegmentRollSinglePartition / ext4 / HOT p99.9**: m6b 5843.8 [5468.2–6023.6] vs 70217bd 5528.6 [5424.6–5797.1]. Ranges overlap.

There are no tmpfs regressions. Observed but within range: LeadershipChangeEightPartitions HOT max m6b median is higher on both fs (ext4 11721 vs 5273; tmpfs 6944 vs 2811), but each m6b median falls inside the 70217bd range.

## Raw data

On the box:
- DR: `/root/e3g/results/6b5ea62/<test>/{pytest.log,junit.xml,clientlogs/,capture/}` and `/root/e3g/results/6b5ea62/matrix.txt`
- Gate: `/root/e3g/gate6b/runs/<scenario>.<fs>.<bin>.<i>.log`, `/root/e3g/gate6b/runs-s1/` (scale-1 supplement), and `/root/e3g/gate6b/progress.txt` (per-run timestamps, container count, load)
- Scripts: `/root/e3g/{build6b.sh,dr6b.sh,gate6b.sh,gate6b-one.sh,gate6b-s1.sh,gate6b-one-s1.sh,master6b.sh}`

Copied locally to `/Users/vradu/.claude/jobs/ceedc822/tmp/box6b/` (`dr/`, `gate/runs`, `gate/runs-s1`, `gate/table-*.md`, `gate/progress.txt`). The tabulator is `/Users/vradu/.claude/jobs/ceedc822/tmp/gate_tab.py`.

## Box state at end

`docker ps -q` is empty (and no `rune*` containers exist in `ps -a`). k3s is not running (`systemctl is-active k3s` = `failed`, the same as at session start). There are no loop devices or `/mnt/gate` mounts, and the loop image is deleted. `/root/vm.lock` is free.

## Follow-up (main session, 2026-10-08 ~15:40): the FollowerTruncation/ext4 "regression" is not a code regression

The 70217bd binary runs the OLD FollowerTruncation scenario (before fa90b75's fixed-length scenario: its truncation
passes are long COLD passes, 27-31 ms, outside the HOT histogram), so it is not comparable with m6b. Same-scenario
bisect on fresh ext4, interleaved, n=4 (box /root/e3g/gate6b/trunc), HOT p99.9 per run, µs:

| build | run 1 | run 2 | run 3 | run 4 |
|---|---|---|---|---|
| fa90b75 (this morning's parity point) | 475 | 4097 | 3192 | 3967 |
| 14678b1 (D26) | 3500 | 4137 | 3937 | 4264 |
| b09fb45 (D28) | 520 | 2389 | 766 | 600 |
| 6b5ea62 (D24, main) | 3583 | 2297 | 2963 | 436 |

All four builds alternate between ~450 µs runs and ~3-4 ms runs, and their ranges overlap. The worst passes are plain
produce passes whose time is all intake, i.e. worker appends stalled in the ext4 journal. That is the D27 mechanism
(journal-free appends, DECLINED by the user), not a change in D26/D28/D24. No action beyond D27.
