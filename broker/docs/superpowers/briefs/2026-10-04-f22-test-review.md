# F22 test-adaptation review (commit 7a2f634, rune-broker-wt43 / integ/f22)

Scope: the 4 rewritten replication integration tests in
`libs/replication/tests/integration/ClusterDisasterRecoveryIntegrationTest.cpp` and the
E1d compaction positive-control fix in `libs/node/tests/integration/CompactionCleanerAgentTest.cpp`.
Checked against the binding Kafka model (HW = min LEO over the ISR, lag shrink after
replicaLagMaxMs, minIsr gates acks=all, no majority commit). The 6 relevant tests were
run from `build-fast` and pass (35 ms total).

## Verdict: APPROVE WITH CHANGES (0 critical, 0 major, 2 medium, 2 low)

The direction is correct: every test now expects the cut-off ISR member to pin the HW
and the HW to move only after the lag shrink. No test was weakened to a vacuous pass. Each
still proves eventual commit and catch-up with no data loss after the heal. There is no
wall-clock dependence: `driveUntil` and the manual loops step the harness's injected
`clock_` by `kStep1ms` and stop at a step-count bound. Nothing calls sleep. The compaction
fix is sound.

The two medium findings are about precision. The "frozen" window the tests assert is half
the real one, and none of the tests checks ISR membership, which is what proves *why* the
HW moved.

## Findings

### MEDIUM-1: the frozen-HW lower bound is half the lag window
`ClusterDisasterRecoveryIntegrationTest.cpp:83` (`kInsideLagWindow = 500`), used at `:156`, `:556`, `:811`, `:1152`.

The harness sets `replicaLagMaxMs = 1000`, and the cut-off follower was caught up at
partition time (or at clock ≈ 0 in the 5-node test). Under Kafka semantics the HW therefore
cannot move before about 1000 steps. The tests only require that it not move for 500.

A regression that shrinks too early, or that lets a timer-driven partial or majority commit
advance the HW anywhere between 500 and 999 ms, still passes. That is exactly the semantic
the rewrite is meant to pin down.

Fix: bound against the configured window, for example
`EXPECT_GE(steps, lagMaxMs - isrCheckIntervalMs)`, or record the clock at `partitionOff`
and assert `clockAtAdvance - partitionClock >= replicaLagMaxMs`. In the 5-node test, call
`driveUntil(..., kLagMaxMs - 1)` for the "must stay frozen" leg. Also assert
`highWatermark() == hwBefore` across the window, not only `< leo`, so that a partial
advance is caught too.

### MEDIUM-2: no ISR-membership assertions, so the cause of the advance and the rejoin are not proven
`:145-163`, `:533-571`, `:782-822`, `:1128-1162`.

Under the Kafka model the defining facts are these:
1. When the HW advances, the cut-off member is no longer in the leader's ISR (`ldr->isr()`).
2. After the heal and catch-up, the member re-enters the ISR.

None of the 4 tests asserts either one. The HW advance is accepted without checking that it
came from the shrink. `SelfHealing.UnderReplicationRepair_ISRSizeRestored` (`:782`) is named
for ISR restoration but never asserts the ISR size after the heal. That gap predates F22, but
the test was touched in F22 and ISR membership is now its central invariant.

Fix: after the commit loop, assert that the cut-off node is absent from `ldr->isr()` and that
`ldr->isr().size() == n-1`. After the heal, call `driveUntil` until the node is back in the
ISR, then `ASSERT` it (in `ISRSizeRestored`, `== 3`). These assertions also restore the
original intent "correct ISR membership".

### LOW-1: acks=all waiting and minIsr-below-ISR are not exercised by these tests
All 4 tests use the harness default `minIsr = 1` and `append()` without `acksAll`. So
"acks=all producers wait while the HW is pinned" and "a shrink below minIsr refuses acks=all"
are not covered here. Coverage exists elsewhere:
- `dr/DrIntegrationTest.cpp:124` `DrIntegration.IsrShrinkExpand_UnderFaults_MinIsrGates`
- `DrIntegrationRegression.PendingShrink_HwWaitsForTheMemberUntilCommitted` (`:1098`)
- `EdgeCases.MinIsr_*` (`:1061`, `:1098`)

This is acceptable. Optionally, a single `ThreeNode` variant with `minIsr = 3` could assert
that acks=all is refused once the shrink lands.

### LOW-2: stale majority vocabulary and names contradict the new semantics
- `:145` `FiveNode_SingleLaggingReplicaDoesNotBlockHW`: the test now asserts that the replica
  *does* block the HW for the lag window. A better name would be `..._BlocksHWUntilLagShrunk`.
- `:557` "Majority side must commit" and `:812` "Majority (2/3) must still be able to commit":
  under the Kafka model this is a commit by the shrunk ISR, not by a majority.
- The neighbouring tests have the same problem:
  - `:502-527` `MinorityPartition_HWFrozen_5Node` justifies the freeze with "2/5 is minority
    (majority=3)". The real reason is that the 3 cut-off nodes are still in the ISR
    (lagMax 5000).
  - `:130-140` `FiveNode_MajorityIsThree_HWAdvances` only requires ≥3 of 5 replicas to hold
    committed data. Under Kafka all 5 ISR members must hold it.

These comments will mislead the next reader into believing majority commit is intended.
The redundant `steps` counter (it always equals `i`) is cosmetic.

## Compaction fix (E1d `NeverCompactsAboveRecoveryPoint`, `CompactionCleanerAgentTest.cpp:3046`): no findings
- The test publishes `kNoCleanerLso` (INT64_MAX). This removes only the D3b read_committed cap,
  which starts at 0 because `topicId >= 0` and no Worker RcState exists in this rig. It matches
  the convention used by the 10 other tests in the file (`:188`, `:275`, ...).
- The value is published before `becomeLeader`. `becomeLeader` does not reset it.
- With the LSO cap lifted and HW = LEO (single-member ISR), the only remaining bound on
  seg1/seg2 is the persisted-recovery-point gate. So the two negative `EXPECT_EQ(file_size)`
  assertions still test the intended invariant: no compaction above the persisted RP.
- The positive control is real. seg0 lies below the persisted RP and holds only "same-key"
  records, so it must shrink or be deleted. Before the fix it could never compact, because
  LSO = 0 clamped the cleaner to nothing. That made the negative assertions vacuous, and the
  fix correctly makes them meaningful again.

## Wall-clock / sleeps: clean
There is no `sleep` or `std::this_thread` anywhere in the diff. Every wait advances the
injected harness clock (`advanceClock(kStep1ms)` / `driveUntil` with `clock_ += stepNs`) and
stops at a step-count bound (`kLagShrinkCap = 3000`). The suite runs in milliseconds of real
time.
