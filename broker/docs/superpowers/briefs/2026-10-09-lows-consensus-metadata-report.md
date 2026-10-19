# Lows batch A: consensus and metadata (report)

Branch `fix/lows-consensus-metadata` (from integ/perf-defaults 4b0b6a6), worktree rune-broker-wt70.
Build: `build-fast` (Debug, Ninja + ccache, the wt26 cache values, BUILD_TESTING=ON).

## Status

| Item | Outcome | Commit | Test |
|---|---|---|---|
| D18 LOW-1 | already done (merged 375a539) | 3e16eaf | MetadataReplicationDeathTest.MetadataTruncationBelowCommittedRegression_HaltsNeverPublishes (green) |
| D18 LOW-2 | already done | a103315 | ElectionStandDownRegression.GracefulLeaderSkipEndsWithItsElectionRegression (green) |
| D18 LOW-3 | already done: `hasActiveFollowerQuorum_` delegates to `ClusterMember::hasActiveQuorum(..., leaderMemberId)` (Election.cpp ~1310) | 33293c4 | ClusterMember.HasActiveQuorum_LeaderExemptWhenNamed (green) |
| D18 LOW-4 | already done (SELF-HOSTING.md + tests) | 773213e | TwoMemberClusterRegression_* (green) |
| D18 LOW-5 | already done: `_wait_for_settled_leader` (bounded 90 s, 5 stable reads) | e4afb63 | py_compile OK; no Docker run (per brief) |
| D18 LOW-6 | re-evaluated against D24: covered; doc corrected, property pinned | 50e323f | ElectionLog.ClosedLeaderStepDownRegression_AdoptedTermAckTermSurvivesRestart |
| D15b l1 | fixed (RED-first) | 87dfea4 | D15MemoryEpochRegression.MoreMarksThanOneBatchFirstRebalanceSkipsQueuedKeysRegression |
| D15b l2 | fixed (RED-first) | 712fa01 | D15MemoryEpochRegression.UnitLargerThanTheRecordBudgetIsStillDecidedRegression |
| D15b l5 | ruled out; ruling pinned | 0971857 | D15MemoryEpochRegression.UnmarkedLegacyLeaderlessRecordDecidedAsOnMainRegression |
| D15b m4 | documented (broker side); rune-protocol text below | cc47e0b | doc only |

D18 had already been done on fix/d18-dr10-lows and merged (375a539, an ancestor of 4b0b6a6). Only the backlog
checkbox was stale. I re-verified each commit is in HEAD and its test is green, and re-ruled LOW-6, which
predates D24.

## D18 LOW-6: adopted leadershipTermId in memory only

**Ruling: covered by D24. No code change.** Doc corrected, property pinned.

- `leaveClosedForElection_` calls `raiseFollowFloor_(term)` before it assigns `leadershipTermId_`. That call
  writes NodeStateFile slot 24 (the follow floor) and the candidate term with one sync.
- `init()` restores `followFloorTermId_` from the file and sets `votedTermId_ = max(votedTermId_, floor)`.
- `ackTermId() = max(votedTermId_, leadershipTermId_)` is therefore at least the adopted term after a crash.
  The a4353d3 comment ("the ack term may fall back below it after a crash") was no longer true; it is now
  corrected on `leadershipTermId_`.
- The new test runs both adoption paths (higher-term NLT and CommitPosition), crashes, reopens the
  NodeStateFile and asserts:
  - slot 24 holds a term at least the adopted term;
  - after INIT, `ackTermId()` is at least the adopted term.
- Mutation evidence: with `raiseFollowFloor_` removed from `leaveClosedForElection_`, the test fails ("the
  adopted term did not reach the durable floor"). The in-memory `leadershipTermId_` itself stays as in Aeron
  (recovered from the TermLog).

## D15b l1: more than 256 restored marks at a claim

- **RED** (at 4b0b6a6): 700 leaderless partitions carry a LogDirOffline mark with ISR lineage kept. The
  controller fails over to node 2, and the test drives it only until it catches up.
  - The claim cycle drains twice (from `maybeCatchUp_` and the end of `doWork`), so 512 records are emitted and
    188 stay queued.
  - The first rebalance (`takePreferredRecheck` true) returned **188** records, one per still-queued key, each
    at the queued re-arm's leaderEpoch. Expected 0.
- **Fix:** `runDynamicRebalanceOnTick` filters balancer output against `pendingDynamicRecords_` as well as
  `inflight_`.
  - It uses a sorted member scratch (`pendingKeysScratch_`) plus `binary_search`.
  - The filter runs only while the queue and the output are both non-empty.
- **GREEN.** The test also checks that every partition is decided on later ticks.
- **Cost:** control thread only, on a cold path (non-empty queue). It does not touch worker, owner or duty-pass
  code. Scratch capacity persists, so there is no steady-state allocation.

## D15b l2: a unit larger than the remaining record budget

- **The re-tick already exists in the runtime.** NodeRuntimeAgent runs `reassignDeadPartitionLeaders` on every
  slow tick. `SlowTick::done(now, work)` makes the tick due on the very next cycle whenever records were emitted.
  - A deferral with a non-empty `recs` always emits, so the next cycle re-decides without any new event.
- **Real gap found:** a unit whose leaderless member count exceeds the whole budget never fit, even on an empty
  tick, so it stayed leaderless forever.
- **RED:** a 2-member co-partition unit, all fresh reports in, ticked four times with `maxRecordsPerCycle = 1`,
  stayed NO_LEADER.
- **Fix:** an empty tick always admits the unit (`!recs.empty() && ... > maxRecords` → defer). The drain stays
  bounded by kMaxRecordsPerBatch. **GREEN:** decided, colocated, not on the empty memory leader.
- **Cost:** control thread, cold path.

## D15b l5: unmarked NO_LEADER records from main controllers

**Ruling: not fixed. This is main's behaviour, which is the intended upgrade semantics.**

- A pre-D15 controller kept the reason for a leaderless partition in its own memory, and that memory died with
  it. A D15 successor cannot re-derive the reason, so the balancer decides the partition on the committed ISR,
  exactly as a main successor would. This is no regression against main.
- The only generic alternative is to re-arm every unmarked leaderless partition at the claim and decide it on
  fresh reports through `decideLeaderless_`. That would also apply to the balancer's own empty-intersection
  NO_LEADER records.
- That alternative changes liveness. It holds a partition whose available ISR member trails a non-ISR replica
  (no clean candidate, survivor election off). Kafka and main elect the ISR member there. This is a design
  trade, not a low.
- **Bounded:** it affects only partitions that a main controller made leaderless and that are still leaderless
  when the first D15 controller claims.
- **Operator note for a rolling upgrade from main:** upgrade while no user partition is NO_LEADER.
- Documented on `restoreLeaderlessMarks_`. The pin test checks three things:
  - an unmarked NO_LEADER record is not re-armed at the claim;
  - no mark is invented;
  - the next tick elects from the ISR at epoch + 1.

## D15b m4: documentation

**Broker side (cc47e0b).** The `MetadataStateMachine` snapshot format comment now records the gap:
- The image carries neither `featureLevel()` nor the per-assignment `LeaderlessMark`.
- There is no production caller.
- It lists the fields a caller must add, plus a format version.

**Text for rune-protocol** (sibling repo, not edited here). Add it to the `MetadataRecordType` documentation in
`rune/protocol/MetadataRecords.h`, next to the enum:

```
// Record-type id space.
//   0x0001..0x7FFF  rune-protocol (this enum).  New protocol record types take the
//                   next free id here.
//   0x8000..0xFFFF  RESERVED for broker-owned extension records (never assigned here).
//                   Assigned by rune-broker (libs/metadata/MetadataExtensions.h):
//                     0x8001  FeatureLevelRecord  [0..1] type  [2] version=0  [3..4] level int16 (LE)
//                   An MSM that does not know a type skips the record deterministically.
//
// UpdatePartitionAssignmentRecord trailer (recordVersion 0 only).
//   Decoders MUST ignore bytes past the replicas array of a recordVersion-0 record.
//   rune-broker appends a fixed 14-byte "leaderless mark" there on a record that makes a
//   partition leaderless:
//     [0] tag = 0xD1 (RESERVED; a future protocol trailer must use another tag or a new
//         recordVersion)
//     [1] kind uint8 (1 = EmptyMemoryLeader, 2 = LogDirOffline; unknown kinds ignored)
//     [2..5] node int32 LE    [6..13] incarnation int64 LE
//   A later protocol version that appends its own fields must bump recordVersion; the
//   broker parses the trailer only after a recordVersion-0 record.
```

**Snapshot/restore** (for whoever adds the first caller). The MSM image must be versioned and carry:
- `int16 featureLevel` after `committedPos`;
- per entry, `uint8 markKind`, `int32 markNode` and `int64 markIncarnation`.

An image without a version restores as level 0 with no marks, the same as a pre-D15 log. Add a RED test with
the caller: snapshot, restore, then a successor claim must re-arm the marked partitions.

## Tests run (affected targets only)

| Target | Result |
|---|---|
| rune_consensus_tests | 447/447 |
| rune_static_assignment_emit_tests | 52/52 |
| rune_metadata_replication_tests | 9/9 (incl. the LOW-1 death test) |
| rune_rebalance_failover_tests | 13/13 |
| rune_epoch_restart_tests | 4/4 |
| rune_metadata_state_machine_tests | 33/33 |
| rune_dynamic_balancer_tests | 25/25 |
| rune_controlled_shutdown_tests | 9/9 |
| rune_multi_leader_produce_tests | 9/9 |
| rune_bootstrap_routing_image_tests | 5/5 |
| rune_worker_pool_parity_tests | 2/2 |
| rune_runtime_topic_replica_tests | 1/1 |

- `check_no_atomics.py`: OK.
- No latency-gate tests and no Docker were run.

## Hot path

- No worker, owner or duty-pass code changed. All changes are on the control thread, in MetadataService's
  controller paths, and only on cold branches (a non-empty pending queue, a leaderless decision).
- Consensus: test and doc only.
