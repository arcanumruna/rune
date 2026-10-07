# D15: memory-backed replica epoch edge cases (report, 2026-10-07)

- **Branch:** `fix/d15-memory-epoch-edges` (worktree `rune-broker-wt63`, based on main `375a539`). Not pushed.
- **Origin:** the risks left open by the epoch-restart fix `9c80eab` (its round-2 report, "Remaining risks" 1 and 2, and the N5 backlog item).

## 1. Controller-memory marks survive a controller failover

**Before:** the offline-log-dir and empty-memory-leader marks, plus the fresh leaderless reports, lived only in the controller's memory.
- A successor controller's balancer re-elected the marked replica blind: either the empty memory leader, which wiped followers holding data, or the offline log dir, which caused one more refusal and one more record.
- The reports were also lost. They had been answered, so they were never re-sent.

**Fix:** the mark is now replicated state.
- **Mark on the record.** The NO_LEADER `UpdatePartitionAssignment` record carries a fixed 14-byte trailer: `LeaderlessMark{kind, node, incarnation}`. `MetadataStateMachine` installs it on `PartitionAssignment::mark`.
- **Restore at the claim.** When a new controller catches up (R3 edge), `restoreLeaderlessMarks_()` re-derives the offline and empty-memory exclusions from the committed map.
- **Re-arm.** It then commits each marked partition again at leaderEpoch+1, still leaderless and carrying the same mark. Every replica re-reports to this controller, and the decision is taken on those fresh reports, as before.
- **Cost:** one record per marked partition per controller change, with no churn otherwise. Convergence is time-independent: it is driven by the claim event and the reports.

**Tests:**
- `D15MemoryEpochRegression.EmptyMemoryLeaderMarkSurvivesControllerFailoverRegression`
- `D15MemoryEpochRegression.LogDirOfflineMarkSurvivesControllerFailoverRegression`

## 2. The leaderless decision is per co-partition unit

**Before:** `decideLeaderless_` decided each partition on its own reports. The same-index partitions of a co-partition group could split, for example topic A to the empty node and topic B to a node holding data.

**Fix:** the decision is made per unit (`unitKeys_`: the same index across the group's topics).
1. The unit waits until every leaderless member has its fresh reports and none is in flight.
2. It picks one leader that is clean for every member, preferring a live sibling's current leader so the unit stays colocated.
3. With no common clean leader and survivor election on (opt-in), it picks one replica that is available for all members. It is unclean only where it has to be.
4. Otherwise the whole unit is held, with no records. This is the same rule as the balancer's "empty group ISR intersection means the whole group goes NO_LEADER".

**Test:** `D15MemoryEpochRegression.CoPartitionUnitLeaderlessDecidedAsOneUnitRegression`, run with survivor election on and off.

## 3. metadata.version feature gate for rolling upgrades

- **`MetadataFeature` levels** (`ReplicationMessages.h`): `kBase` = 0 for pre-D15, `kEpochFence` = 1, `kLatest`.
- **Record:** `FeatureLevelRecord` is broker-owned record type 6. An older MSM skips the unknown type; a newer one commits it at its commit point, monotonically.
- **Raising the level:**
  - Every node advertises the level it supports in `isr[6]` of each node-control request (Register / Shutdown).
  - The controller raises the level to L only once **every** cluster member (`setClusterMembers`) has advertised L. A member that has not registered, or runs an older binary, holds the level back.
  - A brand-new metadata log (claim LEO 0) is bootstrapped at `kLatest` before any assignment, as KRaft does at format time.
- **Sender-side gate:**
  - The bridge stamps the committed level on every role op (it travels in `WorkerOp::acks` for BecomeLeader/BecomeFollower, so the SBE schema is unchanged).
  - Below `kEpochFence`, `PartitionReplica` behaves as before the fence, so an old controller never strands the partition. It leads at a fenced epoch, or with no fence at all, and sends no bump request and no leaderless report. It still persists its fence, so the fence is exact once the level is raised.
  - At `kEpochFence` and above, the fence protocol from `9c80eab` applies unchanged.
- **Old-format compatibility:**
  - The trailer is ignored by `UpdatePartitionAssignmentRecord::decode`, which is what older binaries run.
  - The unknown record type is skipped.
  - A log with no `FeatureLevelRecord` reads as level 0.

**Tests:**
- `D15FeatureGateRegression.BridgeStampsCommittedFeatureLevelOnRolesRegression`
- `D15FeatureGateRegression.LevelRaisedOnlyOnceEveryMemberSupportsItRegression`
- `LeaderEpochFenceRegression.BelowFeatureLevelLeadsUnfencedAndSendsNoBumpRegression`
- `MetadataStateMachineRegression.D15ExtensionsReadableByOlderDecodersRegression`

## Consistency with hot-path B (leader-epoch-state, C1)

- **Consistent:**
  - Disk replicas: under the legacy level the fence write still goes through the deferred async leader-epoch-state path (F7). The leadership waits for the durable fence exactly as before.
  - An async fence-write failure under the legacy level leads unfenced instead of refusing: the offline log dir stays sticky, but no bump is sent.
  - C1 ordering (history durable before the recovery point rises) is untouched.
  - Memory replicas have no recovery point; the "fence durable before the first append" invariant holds.
- **Remaining inconsistency (not changed here):**
  - The memory-backed fence (`<dataDir>/memory-epoch-fence/<t>-<p>`) is still written synchronously on the owning worker (tmp + fsync + rename + dir fsync) on each new leadership epoch.
  - Disk replicas moved this write to the io backend in hot-path B. This is not a steady-state cost, but it is the L4 class of problem. Moving it to the async path means decoupling `es_` from `diskLog_->dir()`. Backlog.

## RED evidence

- **Method:** each fix was disabled in place:
  - the `restoreLeaderlessMarks_` call;
  - unit grouping (`unitKeys_` returning the key alone);
  - the bootstrap;
  - the level check in `fenceProtocol_()`.
- **Result:** all 6 new controller and bridge tests and the replica feature test FAILED.
  - The successor re-elected node 1 blind.
  - The unit split 1 / 2.
  - The bridge refused leadership at level 0.
  - The fresh log stayed at level 0.
- All of them are GREEN with the fixes.

## Verification

- **Targeted run** (consensus, election, metadata/MSM, StaticAssignmentEmit, rebalance, AlterIsr, controlled shutdown, epoch-restart, fence, memory, bridge, hot-path B; `ctest -j4 -E DutyPassLatencyGate`): **513/513 passed**.
- **Earlier full `ctest -j6`**, before the coordinator asked for targeted runs only: 3966/3986 passed.
  - 16 failures were `DutyPassLatencyGateRegression.*` latency gates (machine load about 154, three agents). They were not chased and not loosened.
  - The other 4 were wall-clock or election timeouts under load, and all pass serially: `GatewayReplicationWiring.ThreeNode_FollowersConverge_HWAdvances`, `GR5_Integration.AcksAll_Deferred_HWGate`, `GR6_Integration.SubscriberBoundedByHW_FullyReplicated` and `CompactionRetentionSmoke.TombstoneLifecycle_KIP534`.
- `check_no_atomics.py` OK, with no category grown. `check_single_driver_pumper.py` OK.

## Notes and residuals

- **Raising the level needs every configured member to register.** A permanently dead member blocks it, as in Kafka. Downgrade below a committed level is unsupported, because the level is monotonic.
- **`MetadataStateMachine::snapshot/restore`** does not carry the feature level or the marks. It has no production caller today. If snapshots land, both must be added.
- **The offline mark is restored only while its partition is still leaderless.** After a failover, the offline replica has already left the ISR, so a clean election cannot pick it.
