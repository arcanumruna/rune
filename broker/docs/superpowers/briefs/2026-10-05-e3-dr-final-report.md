# E3 DR matrix — box run final report (2026-10-05/06)

Box: k3s-node-01 (amd64, 4 cores), results `/root/e3f/results`.
Images: `rune-broker-node:a940b8b`, `rune-dr-client:4c36d0b`.
Window: 2026-10-05 19:03 → 2026-10-06 02:13 UTC.

## Result

| Suite | This run | Previous box run (/root/e3) |
|---|---|---|
| dr2_multiclient | 52/52 | 13 passed, 39 failed |
| dr3_leaderkill | 10/10 | 9 passed, 1 failed |
| dr4_quorum | 3/3 | 2 passed, 1 failed |
| dr6_lossy | 20/20 | 19 passed, 1 failed |
| dr7_crash_restart | 9/9 | 8 passed, 1 failed |
| **dr10_zombie_leader** | **6 passed, 2 failed** (repeated_flap[3], [5]) | 5 passed, 1 failed, 2 xfail |
| dr11_flapping | 3 passed, 2 skipped | same |
| dr12_reorder_dup | 18 passed, 1 skipped | 17 passed, 1 failed |
| dr13_client_faults | 4 passed, 1 skipped | same |
| dr15_integrity | 7/7 | 4 passed, 3 failed |
| multi_leader_smoke | 2/2 | 1 passed, 1 failed |
| cg_atleastonce / failover / rebalance | 2/2, 2/2, 6/6 | all failed |
| tx_atomicity / eos / fencing / read_committed | 7/7, 5/5, 3/3, 3/3 | 9 failed total |
| survivor: dr3 / dr4 / dr7 / dr8 | 10/10, 3/3, 9/9, 4/4 | dr3, dr4 and dr7 each had 1 failure |

All dr10 failures are the two repeated-flap cases.  Brokers: 0 crashes (docker die events: exit 0 ×387, test SIGKILL 137 ×87; no core dumps).

## DR-10 repeated flap — root-caused, fixed (branch `fix/dr10-flap`)

The test pauses the consensus leader, then (round 2) pauses the next one.  It was xfailed on all
platforms before 382c65d; on Linux it exposed three layered bugs, each reproduced RED in the
fastest deterministic layer, then fixed:

1. **CLOSED leader never stepped down** (`0f8c9b7`, Election.cpp) — no inactive-follower-quorum
   check, higher-term NLT/CommitPosition ignored in CLOSED, RequestVote silently dropped in CLOSED.
   The resumed ex-leader stayed a zombie of the old term and withheld its vote: the 2 live nodes
   were headless 4+ min.  Now Aeron-faithful (`enterElection` on all three); a denied candidacy
   never unseats a live leader (MAJOR-R3 stays green).  `ElectionLog.ClosedLeaderStepDownRegression_*` (6).
2. **New controller saw every peer dead** (`779880a`) — the peer liveness grace was seeded only at
   CLOSED, but the runtime treats LEADER_LOG_REPLICATION as leader: eligN=1 → every partition
   fenced to ISR {self}.  `ElectionLog.LeaderLivenessGraceRegression_*`.
3. **Truncation rebuild dropped the committed map** (`fdf06e0`, MetadataService) — a demoted
   follower's HW reads 0; the logGeneration rebuild replayed nothing, published an EMPTY map, and
   the bridge quarantined ALL of the node's replicas forever (no fetch → out of every ISR → the
   next pause was an outage).  Rebuild now replays through the committed end via
   `PartitionReplica::readUpTo`.  `MetadataReplicationTest.MetadataTruncationRebuildRegression_*`.

Plus `45c3734`: the "short" 1.5 s pause is 5× the 300 ms leader-liveness window; the oracle now
decides that documented ambiguity on the settled cluster (moved leadership → full fencing asserts).

Verification on `fix/dr10-flap`: ctest 3840/3840; local Docker (arm64) DR-10 8/8 with `--runxfail`.

Follow-up (backlog D17): a resumed zombie controller still runs one controller tick before it
steps down (appends uncommitted reassignment records, later truncated) — gate controller ticks on
an active follower quorum.

## Next

1. Merge `feat/controlled-shutdown`, `fix/subscribe-dedupe`, `fix/dr10-flap` → main; full ctest.
2. Local Docker re-verify on the merged SHA (smoke, dr15, dr7, dr10, cg, tx).
3. Opus review of the DR-10 fixes (held for the usage limit).
4. Scoped box rerun: dr10 (+ dr3/dr4/dr7 for the consensus change) on the merged image.
