# Final verification on the bench box (2026-10-07)

- **Box:** k3s-node-01 ($RUNE_VM_HOST): amd64, 4 cores, ext4 root disk (/dev/sda1), k3s stopped.
- **Code under test:** broker main `dc274e8`, with image `rune-broker-node:dc274e8` built on the box.
- **Client image:** `rune-dr-client:4c36d0b`.
- **Latency baseline:** `70217bd`, which is hot-path A merged and hot-path B not yet merged.
- **Working dir:** `/root/e3g`.
  - DR logs and captures are in `/root/e3g/results/` (`default-*.log`, `capture/`, `clientlogs/`, `diewatch/`).
  - Gate logs are in `/root/e3g/v/runs/` and `/tmp/e3gv/runs` locally.
  - D21 output is in `/root/e3g/v/d21.txt`.
- **Box hygiene:** every docker and bench step ran under `flock /root/vm.lock`. At the end: `docker ps -a` is empty, the lock is free, no runner processes are left, and diewatch was stopped.

## 1. Scoped DR (default arm)

| Suite | Result | Wall |
|---|---|---|
| test_dr10_zombie_leader.py (--runxfail) | **8/8 passed** | 28m44s |
| test_dr3_leaderkill.py | **10/10 passed** | 27m45s |
| test_dr4_quorum.py | **2 passed, 1 FAILED** | 6m01s |
| test_dr7_crash_restart.py | **9/9 passed** | 22m23s |

### FAILURE: `test_dr4_quorum.py::test_dr4_quorum_lost_safety_and_recovery`

**Assertion** (`tests/test_dr4_quorum.py:327` → `harness/invariants.py:54`, `assert_no_committed_loss`):

`committed-loss: 49 acked records never delivered`, for example `(421000,5,5) (421000,5,23) ...`.

**Timeline (UTC):**

- **17:34:13:** cluster formed. Node 1 (rune-2) is consensus leader, term 1. The multi topic (3 partitions) is led as follows: p0 by node 1, p1 by node 2, p2 by node 0. ISR=3, minIsr=2.
- **Pre-kill produce:** cid 421000, acks=all, 150 records through rune-2. All 150 were ACKED.
  - p0: 49 records (offsets 0..48).
  - p1: 85 records.
  - p2: 16 records.
- **About 17:34:20:** SIGKILL of leader rune-2 (node 1), then follower rune-1 (node 0). The survivor is rune-3 (node 2).
- **Safety window:** cid 424000 produced 0/5 through rune-3 (TIMEOUT). Node 2 logged `acks=all timeout for partition=1 ... acking Internal error`. This is correct: the minority survivor refuses writes.
- **17:35:43-45:** both killed nodes restart. Node 2 becomes consensus leader, term 3.
  - The restarted nodes REFUSE their old epoch-0 leaderships ("restart fence 0 ... asking the controller for a new epoch").
  - At 17:35:45.787 they also log `ControlledShutdown: controller 2 does not answer node-control requests`.
- **17:35:54.78:** new partition leaders.
  - multi p0 (`topic=1 part=0`) goes to node 2 (rune-3) at epoch 1 with isr.size=2.
  - Other topics also move. Node 2 takes topic 0 p0, topic 1 p2 and topic 2 p2 at epoch 1 (isr 1). Node 0 then takes the same partitions at epoch 2 (isr 2) at 17:35:54.87.
- **Durability subscribe:** cid 427000 through rune-3, all 3 partitions, 90 s window. It delivered 103 records: **87 from p1, 16 from p2, and ZERO from p0**.
  - All 49 missing records are exactly the p0 records. None of p0 was delivered.
  - From this capture it is undetermined whether node 2's p0 log lacks the data or the p0 subscription was never served.

**Artifacts:**

- Broker logs: `/root/e3g/results/capture/default-test_dr4_quorum/tests_test_dr4_quorum.py_test_dr4_quorum_lost_safety_and_recovery__call/` (rune-dr-fault-rune-{1,2,3}-1.log, .state, ps.txt).
- Client logs: `/root/e3g/results/clientlogs/default-test_dr4_quorum/{421000,424000,427000}.log`.
- pytest log: `/root/e3g/results/default-test_dr4_quorum.log`.

Diagnosis is left to the main session. No code was changed.

## 2. Bare-metal Release latency gate: interleaved ABAB

**Method:**

- The `rune-latgate-builder:dc274e8` image (gcc-13 Release, conan) compiled `rune_duty_pass_latency_gate_tests` from both trees.
- Each run executed the 7 scenarios in gate mode (up to 3 attempts). The tables use **try=1** numbers.
- **n = 8 per side per filesystem.** Order per round: A-ext4, B-ext4, A-tmpfs, B-tmpfs.
- ext4 is a bind mount of the host ext4 root disk (bare metal, no VM). tmpfs is `--tmpfs /data`.
- Before each run: data dir wiped, sync, drop_caches. Docker was otherwise idle (0 other containers logged at every run).
- Values are in µs, shown as median (p25-p75). **A = 70217bd, B = dc274e8.**
- Scenarios:
  - Reads+Retention/2p is `ConsumerReadsDuringRetention`.
  - FollowerTruncation/2p is `FollowerTruncationDuringCleanerPass`.
  - Compaction 64 MiB/1p is `Compaction64MiBGroup`.

### ext4
| Scenario | Metric | 70217bd (A) | dc274e8 (B) | IQR overlap |
|---|---|---|---|---|
| Compaction/64MiB/1p | p999 | 113 (111-126) | 120 (118-126) | yes |
| Compaction/64MiB/1p | max | 6,247 (5,189-11,235) | 4,395 (3,538-5,642) | yes |
| FollowerTruncation/cleanerPass/2p | p999 | 600 (574-656) | 1,025 (888-1,114) | **no: B worse** |
| FollowerTruncation/cleanerPass/2p | max | 2,374 (1,266-22,440) | 2,829 (2,256-3,747) | yes |
| FollowerTruncation/cleanerPass/2p | coldmax | 9,105 (8,539-9,829) | 535 (277-564) | **no: B better** |
| LeadershipChange/8p | p999 | 880 (849-910) | 552 (529-573) | **no: B better** |
| LeadershipChange/8p | max | 3,137 (2,542-3,816) | 2,643 (1,458-5,266) | yes |
| LeadershipChange/8p | coldmax | 40,554 (36,559-42,813) | 408 (403-414) | **no: B better** |
| Reads+Retention/2p | p999 | 1,394 (1,319-1,434) | 1,353 (1,334-1,474) | yes |
| Reads+Retention/2p | max | 4,489 (4,258-5,092) | 5,886 (4,430-6,538) | yes |
| SegmentRoll/1p | p999 | 1,496 (1,420-1,519) | 1,495 (1,447-1,538) | yes |
| SegmentRoll/1p | max | 5,595 (5,326-6,544) | 6,559 (5,384-6,853) | yes |
| SegmentRoll/8p | p999 | 4,690 (4,494-4,815) | 4,755 (4,550-5,341) | yes |
| SegmentRoll/8p | max | 10,624 (9,537-12,516) | 10,943 (9,261-13,227) | yes |
| TxnMarkers/4p | p999 | 506 (494-507) | 504 (488-700) | yes |
| TxnMarkers/4p | max | 2,767 (2,345-3,058) | 3,575 (1,536-5,369) | yes |

### tmpfs
| Scenario | Metric | 70217bd (A) | dc274e8 (B) | IQR overlap |
|---|---|---|---|---|
| Compaction/64MiB/1p | p999 | 37.0 (35.4-39.3) | 39.2 (38.7-41.4) | yes |
| Compaction/64MiB/1p | max | 2,476 (2,157-3,808) | 2,238 (1,625-3,927) | yes |
| FollowerTruncation/cleanerPass/2p | p999 | 79.6 (75.0-83.2) | 82.9 (78.1-86.0) | yes |
| FollowerTruncation/cleanerPass/2p | max | 282 (197-537) | 256 (202-325) | yes |
| FollowerTruncation/cleanerPass/2p | coldmax | 6,981 (6,377-8,202) | 6,979 (6,476-7,564) | yes |
| LeadershipChange/8p | p999 | 153 (136-159) | 134 (123-144) | yes |
| LeadershipChange/8p | max | 341 (288-2,519) | 274 (235-312) | yes |
| LeadershipChange/8p | coldmax | 596 (532-735) | 132 (125-159) | **no: B better** |
| Reads+Retention/2p | p999 | 128 (105-144) | 226 (171-269) | **no: B worse** |
| Reads+Retention/2p | max | 814 (660-973) | 1,068 (886-1,278) | yes |
| SegmentRoll/1p | p999 | 79.3 (77.0-85.3) | 76.2 (71.2-88.9) | yes |
| SegmentRoll/1p | max | 149 (128-444) | 161 (109-261) | yes |
| SegmentRoll/8p | p999 | 436 (415-451) | 453 (396-469) | yes |
| SegmentRoll/8p | max | 523 (490-564) | 580 (504-660) | yes |
| TxnMarkers/4p | p999 | 83.2 (78.1-86.2) | 90.1 (85.3-90.6) | yes |
| TxnMarkers/4p | max | 212 (180-242) | 176 (158-191) | yes |

### Gate failures per side (out of 8 runs)

| fs / side | Compaction64MiB | TxnMarkers/4p | FollowerTruncation (op-gate) | LeadershipChange/8p |
|---|---|---|---|---|
| ext4 A | 8 | 8 | 2 (TruncatePending 112-208 µs) | 0 |
| ext4 B | 8 | 8 | 5 (retention drop 100-128 µs ×5, TruncatePending 100-164 µs ×2) | 1 (best max 7.1 ms > 4.5 ms ceiling) |
| tmpfs A | 5 | 8 | 2 | 0 |
| tmpfs B | 0 | 8 | 0 | 0 |

- **TxnMarkers/4p:** fails on every run, on both sides and both filesystems. The cause is the p99.9 ceiling (50 µs) against a measured 83-90 µs on tmpfs and about 500 µs on ext4.
- **Compaction64MiB:** fails on its known-failing max and p99.9 ceilings.
- The listed `kKnownFailures` ceilings come from the macOS / Docker-VM measurements and do not hold on this box. They need re-baselining from these numbers. Nothing was changed.

### F6 probe (standalone `f6probe`, 200 fresh 64 MiB segments, 1 KiB pwritev)

| fs | fallocate(KEEP_SIZE) first pwritev p50 / p99 / max | second pwritev max | steady max | no-falloc first pwritev p50 / max |
|---|---|---|---|---|
| ext4 run 1 | 44 / 95 / 110 | 26 | 629 | 44 / 159 |
| ext4 run 2 | 36 / 95 / 113 | 37 | 164 | 52 / 122 |
| ext4 run 3 | 47 / 124 / 135 | 43 | 774 | 49 / 137 |
| tmpfs | 17 / 40 / 45 | 14 | 518 | 18 / 128 |

## Verdicts

- **F6 (first write into a new segment: pwritev stall): REFUTED as stated on bare metal.**
  - In the probe, the first pwritev into a fresh FALLOC_FL_KEEP_SIZE segment on bare-metal ext4 costs 36-47 µs p50 and at most 135 µs. That is the same as a first write into a plain, non-fallocated file (44-52 µs p50, max 159 µs). There is no 0.5-3.7 ms unwritten-extent conversion stall; that number was a Docker-VM artefact.
  - Separately, ms-class intake stalls do remain inside the gate on ext4. The worst passes are `produce` with i (intake) = 3-8 ms and the worker part under 150 µs; they are absent on tmpfs.
  - They occur with concurrent seal, bg and dir fsync traffic (about 1.2-1.4k io fsyncs per scenario). They are ext4 journal / writeback contention on the intake pwritev, not a first-write cost.
  - This attribution is consistent with the data but was not instrumented per frame here. Pre-writing the first page will not fix it.
- **LeadershipChange/8p ext4 regression: NOT REAL on bare metal.**
  - B improves: p99.9 is 552 vs 880 µs (no IQR overlap), and COLD max is 408 µs vs 40.6 ms.
  - HOT max is 2.6 vs 3.1 ms, with overlapping IQRs.
  - The wt59 ABAB "864 → 1,652 µs max" does not reproduce.
  - One B run exceeded the 4.5 ms max ceiling (best of 3 attempts: 7.1 ms, intake-dominated, i=8.4 ms). That is ext4 intake noise of the same class as above.
- **N4 (ext4 p99.9 ABAB):** no regression in Retention-class, Reads+Retention, SegmentRoll/1p, SegmentRoll/8p, TxnMarkers/4p or Compaction (IQRs overlap), with one exception:
  - **FollowerTruncation/2p on ext4 p99.9 is a REAL regression: 600 (574-656) → 1,025 (888-1,114) µs, no overlap.**
  - Its COLD max fell from 9.1 ms to 535 µs. This is the M2 "cost moved from COLD into HOT" trade: in-place truncation and the fsync attribution.
  - The op-gate also tripped more often on B on ext4: the retention-drop worker part was 100-128 µs in 5 of 8 runs, against 0 of 8 on A.
  - On tmpfs it is flat: 80 vs 83 µs.
- **M2:** ext4 regression real for FollowerTruncation p99.9 and op-gate as above. Its HOT max is flat: 2.4 vs 2.8 ms, with IQRs overlapping.
- **New, tmpfs: Reads+Retention/2p p99.9 regressed: 128 (105-144) → 226 (171-269) µs, no overlap.** Max overlaps.
  - The B worst passes are `produce+read` with the **worker** part at 0.8-1.9 ms (i≈1).
  - The A worst passes are intake-dominated `produce+roll+read`.
  - This is a worker-side read-path cost introduced between 70217bd and dc274e8 (hot-path B read role?). It is masked on ext4 by intake stalls.

## 3. D20: FollowerTruncation precondition (truncations ≥ 3) on tmpfs

**HOLDS.** tmpfs had truncations = 12 in 8 of 8 runs on both sides (A and B). ext4 had A = 12 in 8 of 8, and B = 8-12 (12, 12, 8, 10, 12, 8, 11, 12). The precondition was never violated.

## 4. D21: leader-epoch-state is sparse on Linux

**CONFIRMED.** Method: a default 3-node cluster at `rune-broker-node:dc274e8` on docker named volumes on the host ext4 `/var/lib/docker` (`/dev/sda1 ext4`), idle for 75 s after start.

- All 24 `leader-epoch-state` files (8 per node: events/0, multi/0-2, cg/0-3) have **st_size = 106,496 B (104 KiB)**.
- Their **st_blocks = 8 (4 KiB) in 16 files and 16 (8 KiB) in 8 files**. That is 4-8 KiB allocated against 104 KiB logical: sparse.

## 5. D2b / D2c

- **D2b (thread-placement p99 tail; FR waterfall on 2013c2e): SKIPPED.**
  - It needs FR-instrumented bare-metal broker and client builds of 2013c2e, the frcap2 capture recipe and waterfall analysis. This is a diagnostic capture, not a benchmark.
  - No such build of 2013c2e is on the box, and it would mean bare-metal broker builds outside the docker / lock recipe used here.
- **D2c (sequential control round-trips under the 2 ms control-plane park: EOS transactions, offset commits, group join, metadata, gateway-routed produce): SKIPPED.**
  - No benchmark tool for those paths exists. The only client bench binary on the box is `rune_produce_throughput`.
  - Running D2c needs new per-path round-trip bench drivers to be written first.
