# DUR-1 fix round (Kafka model for acks=all) — review

Scope: rune-broker-wt31 `feat/dur-1` 0ff9322..e5d41b4 (e422e40, 48653fb, e5d41b4) and rune-protocol-fe
`feat/fetch-epoch` f312823 + 0e9b529, checked against the binding decision (HW = min LEO over the maximal ISR,
lag shrink, minIsr, elections only from the controller-known ISR, no completeness or majority machinery).
Read-only review. I confirmed the findings by building scratch-only gtests (compiled from a scratch TU against
the e5d41b4 sources, linked against build-fast, output in the session scratchpad). Nothing in the worktrees
was edited.

## Verdict: CHANGES REQUIRED

1 CRITICAL, 3 MAJOR, 4 MEDIUM, 2 LOW.

The design is right: the user-partition AlterIsr path, the maximal-ISR HW, the controller CAS, the fencing
pass, the removal of position-based restoration and the preferred-ISR fix are all sound. Each RED→GREEN in
the report is plausible, and I found no hard-rule violation. Two problems on the leader's data plane break
the core Kafka invariant ("every ISR member holds everything ≤ HW"), and I reproduced both. The first one
makes the controller prefer the deficient node, so a single crash loses acked records. The
internal-partition design has a real loss gap: rolling restarts trigger it, so it is not exotic. It also has
a single-gray-fault freeze of the whole control plane.

---

## CRITICAL-1 — The leader's per-follower LEO map survives across leaderships: HW counts an ISR member at a position from an earlier leadership, and acks=all is acked without it

**Where**
- `libs/replication/include/rune/replication/HighWatermark.h:127-131`: `followerLeo_` is SET per fetch and
  never cleared.
- `HighWatermark.h:230-240`: `computeHw()` uses it for every ISR member.
- `libs/replication/src/PartitionReplica.cpp:250-345`: `becomeLeader` resets `prevHw_` and `hwFloor_` (via
  `setDurabilityMode`), follower slots and the proposal, but NOT `leaderState_`'s follower LEOs.
- `PartitionReplica.cpp:1781-1787`: `publishCleanerState` then publishes those stale LEOs to the controller
  position snapshot, stamped with the CURRENT `epoch_`.

**Failure scenario** (RF=3, minIsr=2, ISR {A,B,C} throughout, no unclean shutdown). Reproduced end to end
with three real `PartitionReplica`s on `InMemoryTransport` (R1b below):
1. Epoch 1, leader B: B appends [0,20). A fetches to 20, so B records followerLeo[A]=20. C stalls at 5, so
   HW=5.
2. Epoch 2: C is elected (ISR member). A and B diverge and truncate to 5 (KIP-279). C appends [5,8), which A
   and B replicate. Then A goes slow (still inside replicaLagMaxMs, so still in the ISR). C appends [8,12)
   and B gets it.
3. Epoch 3: a preferred election moves leadership back to B with the ISR {B,A,C} carried over. A has not
   fetched from B yet.
4. B appends [12,15) acks=all. C fetches 15. HW = min(B 15, **A 20 (stale, epoch 1)**, C 15) = **15**. A
   holds 8.
   - Result: `R1b: B.HW=15  A.LEO=8 (A in ISR=1)  C.LEO=15`. Records 8..14 are committed and acked while
     ISR member A lacks them. Idempotent retries of C's in-flight epoch-2 batches are also acked here.
5. B crashes (single fault). `FailoverElection` picks the highest (epoch, LEO) in {A,C} from the positions B
   published. A is published as (epoch 3, 20) and C as (epoch 3, 15), so **A is elected**. C fetches with
   lastFetchedEpoch 3, gets DivergingEpoch, and truncates to 8. **Acked records 12..14 are lost**,
   deterministically.

The window lasts from B's election until A's first fetch to B. The lag clock reset (DR-BUG-6) keeps a
silent member in the ISR for a full replicaLagMaxMs (10 s by default). The same defect hits internal
partitions on every consensus leader change, because their ISR is re-seeded with every replica. The bug
predates this round, but it now carries the whole safety argument: with HW = min over the ISR it is a direct
violation. Under majority-HW it was partly masked.

**Fix (Kafka `Partition.makeLeader` → `resetReplicaState` on a new leader epoch)**
- On every new-epoch `becomeLeader` (not the same-epoch re-assert), forget all follower LEOs, so unknown = 0.
  The HW then sits at the floor until each ISR member fetches in this leadership.
- Add `ReplicaLeaderState::resetFollowers()` that clears in place (no realloc).
- Publish a follower's position to the controller snapshot only once its slot is `validated` in this
  leadership. Clear `publishedReplicaLeo_` follower slots at becomeLeader and becomeFollower.
- Regression test: `PartitionReplicaRegression.NewLeadershipForgetsFollowerPositions_HwNeverFromPreviousLeadership`
  (RED today, see R1/R1b below), plus a balancer-level assertion that the position snapshot never carries a
  follower that was not validated in this leadership.

## MAJOR-1 — ISR expansion has no leader-epoch-start-offset gate: a replica missing committed records is admitted at a stale HW and becomes electable

**Where:** `PartitionReplica.cpp:1921-1926` (`mayJoin`) and `IsrManager.h` `maybeExpandIsrIf`. The only
offset test is `lastFetchOffset >= highWatermark()`. On a new leader that HW is the floor seeded from its
follower view (`followerHw_`, `PartitionReplica.cpp:271`). That view lags the true committed offset by at
least one round trip, and by more when the old leader died just after advancing the HW. Kafka's
`isFollowerInSync` also requires `followerEnd >= leaderEpochStartOffset`, which exists for exactly this
reason. `electionLeo_` already holds that value and is unused here.

**Failure scenario** (reproduced, R2): RF=3, ISR {L,A,B}, C outside the ISR (lagging, LEO 7).
1. L commits [0,10) on {L,A,B} and acks it, then crashes before A or B learn HW 10. A's view is 5.
2. The controller elects A with ISR {A,B} (L removed). A's HW floor is 5, and B is not yet heard from.
3. C fetches at 7 (lineage-valid). The ISR check admits C because 7 ≥ 5, giving committed ISR {A,B,C}.
   - R2 output: `cInIsr=1` with C at 7 and election LEO 10.
4. A crashes. `FailoverElection` over {B,C} decides on soft positions, so C can win: it ties, or it wins on
   the lower id when B's position is unknown or stale (CRITICAL-1 makes stale positions likely). The acked
   records 7..9 are lost even though ISR member B, which holds them, is alive.

Kafka keeps the data here, because C is never admitted below 10. A preferred-leader election (planned, no
crash) followed by one crash reaches the same state whenever the stale-HW window overlaps C's fetch.

**Fix:** in `mayJoin`, also require `isrManager_->followerState(n)->lastFetchOffset >= electionLeo_`. For the
internal leader-local ISR, apply the same rule. Regression test:
`PartitionReplicaRegression.IsrExpansionRequiresLeaderEpochStartOffset` (RED today, R2).

## MAJOR-2 — Internal partitions: the "two-fault" E1b gap is real, and an ordinary rolling restart reaches it

**Where:** `PartitionReplica.cpp:1898-1912` (no lag shrink for internal partitions; dead members fenced by
`shrinkIsrToLive`) and `NodeRuntimeAgent.h:336-347`. The consensus election stays data-blind with respect to
internal logs.

**Scenario** (zero real faults; operator rolling restart of a 3-node cluster):
1. X restarts and is fenced out of the internal ISRs while it is down. Metadata, offset and txn records
   commit on {L,Y}.
2. X rejoins. Its consensus log catches up quickly through Aeron replay, but its internal replicas are still
   catching up, and `__consumer_offsets` can be large.
3. The operator restarts the next node, L, which is the consensus leader. Aeron's election compares
   consensus-log positions only, so X can win.
4. X leads every internal partition with a short log. Y's fetches get DivergingEpoch, and Y truncates
   committed internal records. That loses committed offsets, txn state, and metadata, including ISR and
   leadership records.

A whole-cluster restart after any fencing reaches the same state. Calling this "a double fault" understates
it: it is a routine operations sequence. Kafka never has this gap. `__consumer_offsets` and
`__transaction_state` are ordinary controller-managed partitions, and the metadata log is Raft with the vote
rule on that same log.

**Cheap Kafka-consistent closure.** This also fixes MAJOR-3, and it is not completeness machinery.
- Make internal ISR changes durable in the consensus log, which plays the role of the controller for these
  partitions. A fence or re-admit becomes one small consensus record. While the record is uncommitted the
  internal HW counts the maximal ISR, as in KIP-497/841. That reuses the existing pending-proposal logic,
  with the consensus log as the channel instead of the metadata partition.
- Add a candidacy rule: a node absent from the latest internal ISR it has applied from the consensus log
  does not canvass or nominate (Aeron Election CANVASS → no NOMINATE).
- Raft's vote rule guarantees the winner applied every committed ISR record, so the winner is an ISR member.
  Every ISR member holds every committed internal record (HW = min over the ISR), so no truncation of
  committed data can happen.
- Whole-cluster restart is safe because the ISR lives in the durable consensus log. This is the KIP-966 ELR
  idea expressed through the consensus log. The ordering requirement, "HW may pass X only after the fence
  record is consensus-committed", is exactly the maximal-ISR rule.
- Cost: one consensus record per internal ISR change (deaths and rejoins only).
- A narrower interim option: move `__consumer_offsets` and `__transaction_state` onto the user-partition path
  (controller AlterIsr plus controller election from the ISR), and apply the candidacy rule only to
  `__cluster_metadata`.

Until this lands, the E1b DISABLED tests should be promoted to the backlog as release blockers, and rolling
restarts need a documented "wait for internal catch-up" step.

## MAJOR-3 — Internal partitions: one consensus-live but data-plane-stuck node freezes every internal commit indefinitely

**Where:** same code as MAJOR-2. Internal ISR members leave only when the consensus eligible set drops them.

**Scenario** (single gray fault): node X keeps consensus heartbeats but its internal replica stops advancing.
Possible causes: an asymmetric partition on the replication plane only, a wedged follower (F18
`wedgedAtOffset_`), an unreplicable batch (`kErrRecordTooLarge`, which explicitly does not count as
liveness), or a full disk on X's log volume. HW = min over an ISR that contains X, so the HW freezes for
`__cluster_metadata`, offsets and txn state at once.
- Every controller decision stops committing: failover records, AlterIsr commits, fencing.
- User-partition leader failover is therefore also frozen. One sick node halts the cluster's control plane.

This is the same class of failure as CRITICAL-2, which the round fixed. The report's "a slow live node
throttles internal commits" is the mild version of it. Kafka shrinks X after replicaLagMaxMs. That is safe in
Kafka only because X then cannot be elected, which is what MAJOR-2's candidacy rule provides. With that rule
in place, restore the normal lag shrink for internal partitions (as a consensus-committed ISR change).

## MEDIUM-1 — The AlterIsr compare-and-set compares ISR *sets* (ABA), not a partitionEpoch

**Where:** `MetadataService.h` `onAlterIsr` (the `sameBase` block) and protocol 0e9b529 (`baseIsr`).

**Scenario:**
1. Committed ISR {L,Y}. The leader proposes P1 = {L,Y,X} with base {L,Y}.
2. The controller fences dead Y, committing {L}. The leader clears P1.
3. Y returns and is re-admitted through P2, committing {L,Y}.
4. A late copy of P1 arrives. The AeronPartitionTransport hold router can deliver a frame held for a
   disconnected ex-controller once it regains leadership. Its base {L,Y} equals the committed set again, so
   P1 commits and X enters the committed ISR.
5. The leader's HW stopped counting X at step 2 and may have passed X's LEO, so X is now electable while
   lacking committed records.

The window is narrow, and in-order delivery on one publication normally prevents it. Kafka's monotonic
partitionEpoch removes the case entirely.

**Fix:** carry the assignment's `partitionEpoch` (the leader learns it from the record it applied) instead
of, or in addition to, `baseIsr`, and compare it for equality. The protocol is unpublished, so this is free
now.

## MEDIUM-2 — No AlterIsr response and no KIP-841 "never add a fenced replica": a flapping loop amplified by partition count

**Where:** `MetadataService.h` `onAlterIsr` (accepts adds of non-eligible nodes) together with the fencing
pass in `rebalance()`.

**Scenario:** an asymmetric partition. The controller misses X's consensus heartbeats while the partition
leaders still receive X's fetches. For every partition X follows:
1. Fencing commits "remove X".
2. The leader re-proposes "add X" (validated, ≥ HW).
3. The controller commits it.
4. The next tick removes X again.

That is two metadata records plus two same-epoch becomeLeader re-applies per partition per cycle (cycle ≈
ISR check + commit latency). With 1000 partitions this is thousands of records per second for as long as the
views disagree: unbounded metadata-log growth, and competition with failover records in the bounded drain.
This is a liveness and resource issue, not a safety one: X is excluded from elections by the liveness
intersection.

Making the controller simply refuse such adds is not enough. The pending proposal would then pin all other
ISR changes for that partition, including needed shrinks, which stalls acks=all. Expiring the proposal on
the leader is unsafe without a response, because a late commit re-admits a member the HW has passed.

**Fix:**
- Controller: drop non-eligible nodes from added members. If the result differs from the base, commit it.
  Otherwise answer.
- Add an AlterIsr response frame (accepted, INELIGIBLE_REPLICA, FENCED_LEADER_EPOCH, INVALID_UPDATE_VERSION),
  and clear the pending proposal only on commit or on an explicit reject. This is Kafka's AlterPartition
  response.

## MEDIUM-3 — The position-proven survivor election (no live ISR member) stays on by default: a non-Kafka availability escape whose "proof" rests on soft positions

**Where:** `libs/metadata/src/DynamicBalancer.cpp:313-380`.

The pigeonhole argument holds only if three things are true:
- the positions are current;
- no replica lost acknowledged data (no unclean shutdown, i.e. KIP-966);
- every topic's commit floor ≥ the controller's global `minIsr_`.

The positions are explicitly "soft, may be stale", and today CRITICAL-1 publishes stale-high LEOs stamped
with the current epoch, which is exactly what this election maximises over. Requiring a report fixed the
"unreported" case but not the "stale" case.

Kafka keeps the partition offline unless `unclean.leader.election.enable` is set. `ElectionInputs.allowUnclean`
already exists.

**Recommendation:** gate the survivor election behind the per-topic unclean opt-in (or a separate,
default-off "clean-survivor" flag), so the default is offline, as in Kafka. Keep it as an opt-in for DR
availability once CRITICAL-1 is fixed and positions carry only validated-this-leadership LEOs.

## MEDIUM-4 — KIP-966 / KIP-903 gap: a follower restarted after power loss, faster than failure detection, keeps its committed-ISR seat

**Where:** `PartitionReplica.cpp:787-806`. The regression check is per-leadership: `prevAckedLeo` is -1 on
the first fetch after a leadership change.

**Scenario:**
1. Follower F loses its unflushed tail (fsync off) and restarts within failoverTimeout, so it is never
   fenced. Its first fetch arrives below the HW.
2. If the leader changed meanwhile, or F's slot was reset, nothing marks F unclean. F stays in the committed
   ISR, and is electable, while lacking acked records.

This is the known `DISABLED_E1b_UncleanRestartMovedBeforeFirstFetch`. It is a single fault, but it only
applies with periodic fsync off.

**Fix (Kafka KIP-903):** a per-process incarnation (broker epoch) in FetchRequest. When the incarnation
changes and there is no clean-shutdown marker, the leader marks the slot unclean until the follower fetches
back to ≥ HW. The controller drops replicas whose registration incarnation is stale from the committed ISR.

## LOW-1 — Stale "majority" wording on the min-HW code

- `IsrManager.h:14` ("count toward the high-watermark majority")
- `PartitionReplica.cpp:774` ("lifted the majority high-watermark")
- `PartitionReplica.cpp:1705` ("Raw majority-of-ISR HW")
- the per-step comments in `HighWatermarkTest.cpp`, which the report already noted

These mislead the next reader about the commit rule.

## LOW-2 — First-contact allocations and the virtual AlterIsr hook

- `IsrManager::onFollowerFetch` uses `followerState_[node]` (operator[]) and `updateFollowerFetchOffset`
  inserts into an `unordered_map`. Both allocate on a follower's first fetch only. That is cold, but
  `find()` plus a seeded map (all replicas are known at construction) would be strictly allocation-free.
- `PartitionTransport::sendAlterIsr` is virtual. It is on a cold path (one proposal per change, re-sent
  every 50 ms), so it does not violate the hot-path rule, but it is the only new virtual in the diff.

---

## Hard-rule check

| Rule | Result |
|---|---|
| No atomics or mutexes beyond rings and the cleaner | OK. `scripts/check_no_atomics.py` is clean, and no new atomics appear in the diff (the publish atomics touched are the pre-existing cleaner and gateway snapshots). AlterIsr crosses threads through the MPSC outbound ring. Internal replicas and `shrinkIsrToLive` both run on the control thread. |
| No virtual calls, `std::function` or allocation on hot paths | OK. The ISR check now works in place. The allocations in `shrinkIsrToLive`, `onAlterIsr` and the fencing pass happen only when something changes. The only new virtual is cold (LOW-2). |
| Tests never sleep | OK. No `sleep_for` or `usleep` in the diff. |
| RED-first `*Regression*` tests | The listed tests follow the convention, and the RED evidence (port builds and toggles) is credible. Gaps: CRITICAL-1 and MAJOR-1 have no tests. The scratch repros below are RED on e5d41b4 and should land as `PartitionReplicaRegression.*`. |

## Scratch repros (RED on e5d41b4)

Helpers come from `PartitionReplicaTest.cpp`: `fr(ep, follower, offset, lastFetchedEpoch)` builds a
FetchRequestMsg with maxBytes 0.

```cpp
// CRITICAL-1, unit: R1 → HW=20 followerLeo(A)=20 although A never fetched in epoch 3 (expected <= 5)
PartitionReplica b(makeConfig(kB,10'000,1'000'000,2), Role::Leader, 1, all, all, &log, &t);
for (int i=0;i<20;++i) b.append(makePayload(1));
b.doWork(clock);
b.onFetchRequest(fr(1,kA,20,1), clock);
b.onFetchRequest(fr(1,kC,5,1), clock);       // HW 5
b.becomeFollower(2, kC);
b.becomeLeader(3, {kB,kA,kC});
for (int i=0;i<3;++i) b.append(makePayload(3));
b.doWork(clock += 1'000'000);
b.onFetchRequest(fr(3,kC,b.logEndOffset(),3), clock);
EXPECT_LE(b.highWatermark(), 5);             // FAILS: 20

// CRITICAL-1, end to end (3 real replicas, InMemoryTransport): R1b → B.HW=15 while ISR member A.LEO=8
// (steps 1-4 of the scenario above; full test kept in the session scratchpad, ReviewRepro.cpp)

// MAJOR-1: R2 → C (LEO 7) admitted to the ISR of a leader elected at LEO 10
PartitionReplica a(makeConfig(kA,10'000,1'000'000,2), Role::Follower, 1, all, all, &log, &t);
seedStamped(a, 0, 10, 1, /*hw=*/5);
a.becomeLeader(2, {kA,kB});
a.doWork(clock);
a.onFetchRequest(fr(2,kC,7,1), clock);
for (int i=0;i<5;++i) a.doWork(clock += 1'000'000);
EXPECT_FALSE(inIsr(a, kC));                  // FAILS: admitted
```

## Answers to the focus questions (short)

1. **Safety under single faults:** not safe yet.
   - CRITICAL-1: a single leader crash after routine leadership moves loses acked records, and the
     controller actively picks the deficient node.
   - MAJOR-1: Kafka-unsafe expansion.
   - Otherwise sound: the HW formula itself, the maximal ISR, the CAS (modulo ABA, MEDIUM-1), the fencing
     pass, election eligibility from the committed ISR, KIP-279 truncation paths, and the preferred-election
     ISR carry-over.
2. **Internal-partition choice:** single-fault-safe for data only once CRITICAL-1 and MAJOR-1 are fixed.
   - E1b is real, and rolling restarts reach it (MAJOR-2).
   - The no-lag-shrink rule turns any gray failure into a control-plane freeze (MAJOR-3).
   - Both close cheaply and in a Kafka-consistent way: put internal ISR changes in the consensus log, count
     the maximal ISR until they commit, and stop non-ISR nodes from standing for election.
3. **No AlterIsr response / no fenced-replica rule / flapping:** MEDIUM. It costs liveness and resources,
   amplified by partition count, but is not a safety issue. A response frame plus a partitionEpoch CAS is the
   fix.
4. **Survivor election:** MEDIUM. It should be a default-off opt-in, as Kafka's unclean election is. Its
   proof currently rests on positions that CRITICAL-1 corrupts.
5. **Hard rules:** OK. CRITICAL-1 and MAJOR-1 need RED-first regression tests.

---

# Re-review (fix round 2)

Scope: rune-broker-wt31 `feat/dur-1` e5d41b4..a2d4092 (4949530, 902cd03, cf08da0, 667cd5c, a2d4092) and
rune-protocol-fe `feat/fetch-epoch` a9787dc. Read-only. To confirm findings I compiled one scratch gtest TU
against the a2d4092 DR harness and linked it against the existing `build-fast` libraries; the output is in the
session scratchpad (`rr2/Repro.cpp`). Nothing in the worktrees was edited. The existing
`rune_dr_integration_tests` binary passes 50/50 on a2d4092.

## Verdict: CHANGES REQUIRED

1 MAJOR, 3 MEDIUM, 2 LOW (all new). Every finding from the previous review is fixed, and the regression tests
are genuine.

The internal-ISR mechanism is sound for safety under single faults and rolling restarts. A committed version
is durable on a majority. Every published member was validated at ≥ max(HW, electionLeo). The HW counts
committed ∪ proposed. The seed counts every replica, fenced ones included, until the first commit, so nothing
commits in a new leadership before its first ISR is majority-durable. The voter-side gate intersects the commit
majority. I found no committed-record loss under a single crash, a rolling restart, a whole-cluster restart, or
a crash between persist and ack. I also found no split-brain between two versions: (leaderEpoch, version) is
totally ordered, and an older-epoch state never overrides a newer one.

The new problems are liveness and one narrow zombie window. The self-candidacy check turns ordinary
knowledge staleness into a permanently headless cluster, and I reproduced that.

### Status of the previous findings

| Finding | Status | Notes |
|---|---|---|
| CRITICAL-1 | **Fixed** | `resetFollowers()` runs on every new-epoch `becomeLeader` (PartitionReplica.cpp:306). The same-epoch re-assert path correctly keeps positions. The snapshot publishes only followers `validated` in this leadership. Slots are cleared on becomeLeader, becomeFollower and self-demotion. R1, R1b and the DR test are RED without the fix, per the report and my reading. |
| MAJOR-1 | **Fixed** | `joinFloor = max(HW, electionLeo_)` (PartitionReplica.cpp:~2040). It also gates the internal first publish. |
| MAJOR-2 | **Fixed for the reported scenario** | The rolling-restart and E1b tests cannot pass without the gate: the harness elects through the real `InternalIsrKnowledge`. Residuals: MAJOR-A, MEDIUM-A, MEDIUM-B, MEDIUM-C. |
| MAJOR-3 | **Fixed** | Lag shrink is restored. The removal commits on a majority, and the HW counts the member until then. |
| MEDIUM-1 | **Fixed** | `partitionEpoch` CAS. An applied assignment with any other partitionEpoch resolves the proposal. AdminWorker's `-1` is fine there, because the schema replica is not controller-managed. |
| MEDIUM-2 | **Fixed** | KIP-841 drop plus the response frame. A mixed shrink+fenced-add commits the shrink part. Shrinks are never held back. |
| MEDIUM-3 | **Fixed** | Default OFF, plus env and NodeConfig switches. The dr-e2e tree never sets `RUNE_SURVIVOR_LEADER_ELECTION` (see LOW-B). |
| MEDIUM-4 | **Fixed** (data plane) | The incarnation is sticky across leaderships, and an older one is dropped. The "committed member below HW ⇒ unclean" proof holds: a new leader's floor ≤ committed ≤ every member's LEO. The controller-side gap is documented. |
| LOW-1 | Fixed | |
| LOW-2 | As ruled | The new `sendInternalIsr`/`sendInternalIsrAck` virtuals are cold. |

### Changed test expectations: all legitimate

- **`NB1_…HwFloorIsCommittedNotLeo`.** kF2 sits at LEO 0 below the floor 5 while it is a committed member, so
  it provably lacks committed records. Evicting it is the Kafka invariant. The HW then rising over {self} with
  minIsr 1 is Kafka-correct: the new leader's tail commits once its ISR holds it.
- **`Critical2_InternalLeaderFailover_…` cut to 5 steps.** The 300-step variant asserted the no-lag-shrink
  behaviour that MAJOR-3 removed, and it lives on as the MAJOR-3 test.
- **`…ConsensusFailoverUnderCommitLoad` via `consensusElect`.** This now models the real election, so the
  lagging node can no longer be forced in.
- **`LaggingReplicaShrunkFromControllerIsr_NeverElectable` (fence counted as remover).** It accepts the
  controller's fencing as an equivalent remover, which is valid.

---

## MAJOR-A — Stale self-knowledge plus the self-candidacy gate leaves the consensus permanently leaderless with a majority up and an eligible, caught-up node (reproduced)

**Where**
- `Election.cpp:334` (CANVASS: no NOMINATE unless `mayVoteFor(self)`) and `Election.cpp:364`.
- `PartitionReplica.cpp:2010`: InternalIsr is re-sent only while `isrProposal_.pending`.
- `PartitionReplica.cpp:2156`: on commit (`maybeCommitInternalIsr_`) the pending flag clears, so a replica
  that missed the frame is never told again.
- `InternalIsrKnowledge.cpp:50` (`eligible`).

**Event sequence** (RF=3, internal partition, leader 1). Reproduced as `ReReview2.StaleSelfKnowledgeDeadlocksConsensus`:
1. v1 = {1,2,3} is held by all nodes.
2. Node 3 is fenced (consensus-dead for a moment, data plane up), so v2 = {1,2} commits and node 3 receives it.
   Node 3's newest state excludes node 3.
3. Node 3 returns while node 2 turns consensus-dead or lag-shrunk. Node 3 is re-admitted and node 2 removed,
   ending at v4 = {1,3}. That commits on {1,2}: node 2 persists and acks a state that excludes itself. Node 3
   misses the frame (one lost `enqueueOutbound`/link blip). The leader never re-sends a committed state.
   - Knowledge: `n1=(1,4)={1 3}  n2=(1,4)={1 3}  n3=(1,2)={1 2}`. Node 3's LEO equals the leader's.
4. Node 1 dies.
   - Node 2 grants only {1,3} and will not stand itself.
   - Node 3 grants only {1,2} and will not stand itself.
   - Nobody ever nominates: `consensusElect → -1`.

**Impact**
- The cluster has no controller: no metadata commits, no user-partition failover, and no AlterIsr. Acks=all
  partitions with a dead member stall.
- This lasts until node 1 returns. If node 1 is lost for good, it is permanent, and there is no operator
  escape hatch.
- It violates the time-independent-liveness rule, and needs only one lost frame plus an ordinary ISR swap.
- Safety does not need the self-check. Voters gate, and any grant majority intersects the commit majority. The
  self-check is a liveness optimisation that turns stale knowledge into a deadlock.

**Fix**
- Drop the self-gate in CANVASS/NOMINATE: let a node stand and let the voters decide. With that, node 3 stands
  here, node 2 grants (3 ∈ v4), and node 3 wins. Node 3 holds everything committed, because it was validated
  at ≥ joinFloor when v4 was published.
- Also add anti-entropy, so knowledge converges without depending on a lucky frame. Either:
  - keep re-sending the latest committed state to replicas that have not acked it, or
  - piggyback (epoch, version) on FetchResponse and let a stale follower ask for the state.
- Land the scratch repro as `DrIntegrationRegression.StaleSelfKnowledge_ConsensusNotHeadless`.

## MEDIUM-A — Zombie-leader ack window: an ISR change can commit after its acker has already voted for a newer term (acked internal records lost); a deposed leader's state is persisted unconditionally

**Where**
- `PartitionReplica.cpp:2181`: the ack is gated on the internal replica's `epoch_`/`leaderId_`.
- `ReplicationMetadataBridge.h:186-199`: those change only when the bridge sees a new leader id ≥ 0, which is
  after NewLeadershipTerm, not when the node votes.
- `PartitionReplica.cpp:2178`: `offer()` runs before any role or epoch check.

**Event sequence** (RF=3: L leads at term e; ISR {L,X,Y} committed):
1. The L↔Y link drops on both planes, and L↔X consensus heartbeats are degraded (asymmetric, L→X replication
   still up).
2. Y stops fetching and lags. After `replicaLagMaxMs` (10 s), L publishes (e, v') = {L,X}. Y's election timer
   fires on the same clock (10 s), so the two race.
3. X grants Y's RequestVote for term e+1, because X's newest state still includes Y. X's internal replica still
   follows L at e until NewLeadershipTerm arrives.
4. In that gap, X persists and acks (e, v'), so it commits on {L,X}.
   - L's HW now advances on {L,X} past Y's LEO, and L acks client writes (metadata, offsets, txn).
5. Y becomes leader at e+1 (it was legitimately elected). X fetches with lastFetchedEpoch e, gets
   DivergingEpoch, and truncates the records L acked in step 4. **Acked internal records are lost.**

Kafka cannot reach this. The controller linearises AlterPartition against the leader-epoch bump. Raft cannot
either: a vote for term e+1 rejects term-e appends. The report's claim that "a deposed leader is never helped"
does not hold in this window. The window is narrow (vote → NewLeadershipTerm, during a coincident 10 s lag
shrink), so it is MEDIUM.

**Liveness side of the same gap:** `offer()` also persists states from a deposed leader. A zombie's
(e, v'') = {L}, received before the new leader's first commit, makes every receiver grant only L. If the new
leader then crashes before its first commit, the consensus waits for the zombie.

**Fix**
- Expose the Election's `candidateTermId`/`leadershipTermId` to the internal replicas, as the consensus term.
- Refuse to ack (and to persist) an InternalIsr whose `leaderEpoch` < the node's consensus term (voted or
  observed). Raft's term rule, applied to the ack.
- Optionally also refuse data fetch credit to a leader whose epoch is below that term.

## MEDIUM-B — The liveness residual "no node in every internal ISR ⇒ leaderless until L returns" is not Kafka-equivalent: it has a larger blast radius and no escape hatch

**Where:** `InternalIsrKnowledge::eligible` (`InternalIsrKnowledge.cpp:50`) ANDs the ISRs of `__consumer_offsets`,
`__transaction_state` and `__cluster_metadata`, because the consensus leader leads all three.

**Event sequence** (RF=3):
1. A is lag-shrunk from offsets (hot partition, GC pause), giving ISR_offsets = {L,B}.
2. B is lag-shrunk from metadata, giving ISR_metadata = {L,A}.
3. L dies. No node is eligible, so there is no controller, no user-partition failover and no AlterIsr. The
   whole cluster's control plane is down.

Kafka in the same state: only one `__consumer_offsets` partition and the metadata partition lose their ISR.
The KRaft quorum still elects a controller (Raft on its own log), user-partition failover continues, and
unclean election is available.

Compared with today (e5d41b4): today always elects, but unsafely. So this is a deliberate safety-for-liveness
trade, and the trade is acceptable. What is not acceptable is that one partition's ISR takes out the whole
control plane, with no override. Reaching it needs two degraded nodes plus the leader's death, which is why it
is MEDIUM. MAJOR-A makes it reachable with one lost frame instead, so fix MAJOR-A first.

**Fix**
- Interim, as in the previous review: move offsets and txn onto the user-partition path (controller AlterIsr,
  election from the ISR), so they go offline per partition as in Kafka. The candidacy gate then covers only
  `__cluster_metadata`.
- Add a default-OFF operator override for metadata (an unclean consensus election), mirroring MEDIUM-3.

## MEDIUM-C — Lag shrink is restored for every internal partition, including `__topic_schemas` (-4), which is outside the quorum and vote-gate mechanism (latent regression)

**Where**
- `PartitionReplica.cpp:2026`: the `if (!internalPartition_())` guard is gone.
- The schema replica (`BrokerNode.cpp:2484-2493`) is not in `replicaRegistry_`, so `setInternalIsrKnowledge`
  is never wired (`internalQuorum_()` is false). Its lag shrink therefore commits at once, self-managed, and
  its ISR is never part of `InternalIsrKnowledge`.

**Effect**
- Today: the replica runs on a `NullPartitionTransport` (Task 8b, not replicated multi-node). After 10 s it
  shrinks to {L}, and DDL fails with NotEnoughReplicas instead of hanging. Both are broken, so this is
  immaterial now.
- Once Task 8b wires real replication, this reopens MAJOR-2 for schemas:
  1. A lagging node is shrunk at once.
  2. It is still electable as consensus leader, and so as schemas leader.
  3. Followers are truncated by DivergingEpoch, so acked schema registrations are lost after one leader crash.

**Fix:** shrink only when `!internalPartition_() || internalQuorum_()`. Better, wire the schema replica's ISR
into the same knowledge store when Task 8b lands. Add a guard test.

## LOW-A — The knowledge file fails open

**Where:** `InternalIsrKnowledge.cpp:112-150` and `:108`.

- **Corrupt, short or unknown-version file.** It is logged and treated as no knowledge, so the node votes for
  anyone. A voter in the commit/vote intersection with a corrupt file breaks the safety argument. KRaft refuses
  to start on a corrupt quorum-state file.
  - Fix: fail closed. Refuse to vote, or refuse to start, when the file exists but does not validate. Only an
    absent file on a node that has never acked is "fresh".
- **Directory fsync.** Its result is ignored, so a failed dir fsync still acks.
- **Control-thread fsyncs.** Each internal ISR change costs two fsyncs (file plus directory) on every node, on
  the control/consensus duty-cycle thread. It is cold (≤3 partitions), but worth a note next to the heartbeat
  timeouts.

## LOW-B — The gated election and InternalIsr wiring are proven only in the DR model

- `DrCluster::consensusElect` re-implements the vote rule; it does not drive `Election`.
- Nothing in the round exercises a real-Aeron BrokerNode failover through `Election::setVoteGate` plus
  InternalIsr frames over `AeronPartitionTransport`.
- The Docker DR matrix was not run, and nothing under `dr-e2e/` sets `RUNE_SURVIVOR_LEADER_ELECTION`.
- Per the real-e2e rule, add one multi-node in-process BrokerNode test that kills the consensus leader while a
  node is out of the internal ISR, and run the matrix before calling DUR-1 done.

## Hard-rule check

| Rule | Result |
|---|---|
| Atomics and mutexes | OK. `check_no_atomics.py` is clean. The new `publishedReplicaLeo_` clear is allowlisted (SLICE-B). The test fake's locks are now consolidated (13 → 3). |
| Virtual calls, std::function, allocation on hot paths | OK. The fetch-path additions (incarnation check, `committedMember` find in the below-HW cold branch) do not allocate. `InternalIsrKnowledge` and InternalIsr handling are cold (ISR changes only). The new virtuals are cold. |
| Tests never sleep | OK. No `sleep_for` or `usleep` in the diff. |
| RED-first `*Regression*` | OK for every listed fix. MAJOR-A and MEDIUM-A need new regression tests (the MAJOR-A scratch repro is ready). |

---

# Re-review (fix round 3)

Scope: rune-broker-wt31 `feat/dur-1` a2d4092..e359e44 (2796c8a, 3933caa, 703b087, c319e9b, e359e44).
rune-protocol-fe `feat/fetch-epoch` has no commits after a9787dc. Read-only. To confirm findings I compiled one
scratch gtest TU against the e359e44 DR harness and linked it against the existing (up-to-date) `build-fast`
libraries: `repros/rev5-dur1-round3-Repro.cpp`. Both tests in it are RED on e359e44. `check_no_atomics.py` is
OK.

## Verdict: CHANGES REQUIRED

1 CRITICAL, 1 MAJOR, 0 MEDIUM, 3 LOW (all new; the CRITICAL and the MAJOR are regressions that round 3 introduced).
Every round-2 finding has a commit and a test that fails without its fix. But two of the fixes create new
problems:
- **MAJOR-A fix.** Dropping the candidate self-gate removed the safety check on the candidate's own
  (unconditional) vote.
- **MEDIUM-A fix.** The term rule reads a term that a denied vote bumps.

### Status of the round-2 findings

| Finding | Status | Notes |
|---|---|---|
| MAJOR-A | **Liveness fixed; the fix is unsafe (CRITICAL-R3)** | The `ElectionBallotRegression.VoteGate_StaleSelfKnowledgeStillStands` and `DrIntegrationRegression.StaleSelfKnowledge_ConsensusNotHeadless` tests are landed, and the scratch repro is used as-is. Anti-entropy works: `CommittedInternalIsrResentUntilAcked_KnowledgeConverges` fails without the re-send. The DR test passes because of the harness change (`consensusElectable_`, `votes = 1`), and the Election change is proven by the ballot test. |
| MEDIUM-A | **Zombie window closed; the term source is wrong (MAJOR-R3)** | `ZombieLeaderInternalIsrAckRefusedAfterVote_NoAckedLoss` and `InternalIsrRegression.StateOlderThanConsensusTermNeitherPersistedNorAcked` are genuine RED→GREEN tests. The BrokerNode term-source wiring is proven only by reading the code (see LOW-B). |
| MEDIUM-B | **Fixed** | Only `-3` gates candidacy (`InternalIsrKnowledge.cpp:57`, `ReplicaRegistry.h:65`). `-1`/`-2` run on the controller path: AlterIsr is accepted for coordinator keys, the bridge drives them from the map, inline on the control thread (`ReplicationMetadataBridge.cpp` direct call for topicId<0), and the AlterIsrResponse for topicId<0 is handled inline. The override is OFF by default (`NodeConfig.h:291`), is env-parsed and tested, logs a WARN when ON, and is documented in NodeConfig.h, at the same level as `RUNE_SURVIVOR_LEADER_ELECTION`. Durability holds for single faults, rolling restarts and whole-cluster restarts: the controller moves leadership to itself only when it is in the committed ISR, the election runs over ISR ∩ live and is never unclean, an offline partition keeps its ISR lineage, and the map is persisted in `__cluster_metadata`. The map-wide fencing pass also covers the coordinator partitions. Residual: LOW-R3-2 (bootstrap seed). |
| MEDIUM-C | **Fixed** | The guard is `!internalPartition_() \|\| internalQuorum_() \|\| isrChannel_`. The guard test is genuine. |
| LOW-A | **Fixed** | The file fails closed, the directory fsync is checked, and the fsync placement is documented. Residual: LOW-R3-1. |
| LOW-B | **Open** (follow-up, as ruled) | Nothing yet drives the real `Election` + `setConsensusTermSource` + InternalIsr frames over Aeron. Both new findings live exactly in that gap. |

---

## CRITICAL-R3 — A node that acked its own removal stands, votes for itself unconditionally, and wins with one stale voter: it leads `__cluster_metadata` without committed records (reproduced, acked loss)

**Where**
- `Election.cpp:337`: the CANVASS self-gate is removed. The NOMINATE stand-down is removed as well.
- `Election.cpp:373`: `becomeCandidate` records the candidate's own vote as *For*, with no gate.
- `DrHarness.h` `consensusElectable_` encodes the same rule (`votes = 1`).

**Why the round-2 argument does not hold.** "Any grant majority intersects the commit majority" holds only if every vote in
the grant majority is gated, the candidate's own vote included. A commit majority can contain the candidate
itself: a replica persists and acks a state that removes it. That is exactly what MAJOR-A's node 2 did. The
intersection is then only the candidate, and it now ignores its own knowledge.

**Event sequence** (RF=3, `__cluster_metadata`, leader 1, ISR {1,2,3}). This is `ReReview3.SelfVoteOfExcludedNodeElectsReplicaMissingCommittedRecords`:
1. Node 2 turns consensus-dead (GC pause or partition), so the leader fences it: proposal (1,2) = {1,3}.
   - Node 2 receives the proposal, persists it and acks it. It commits on {1,2}.
   - The frame to node 3 is lost (a 1→3 blip). Knowledge: `n1=(1,2)={1 3}  n2=(1,2)={1 3}  n3=(1,1)={1 2 3}`.
2. Node 2 stops fetching. Node 3's link heals, but the next two anti-entropy re-sends are lost too (one blip of
   about 100 ms). Five records commit and are acked over ISR {1,3}: LEO1=15, LEO3=15, LEO2=10.
3. Node 1 crashes. Node 2 comes back.
4. Node 2 stands. It votes for itself (unconditional), and node 3 grants, because its stale state still lists 2.
   `consensusElect → 2`.
5. Node 2 leads at LEO 10. Node 3 truncates by DivergingEpoch. **Five acked metadata records are lost on both
   survivors** (`expectAckedPresent`: "lost 5 ACKED records (first id 11)").

**Reach**
- The anti-entropy only narrows the window to `kAlterIsrResendNs` (50 ms) × lost frames. A slow fsync on node 3
  opens the same window with no loss at all: node 3 has not persisted yet when the commit lands on {1,2}.
- The LOW-A fail-closed node is not actually "refusing to vote". It still votes for itself, so a node with a
  corrupt file can elect itself the same way.

**Fix.** Keep standing ungated, as MAJOR-A required, but gate the self-vote: a candidate counts its own vote only if
`eligible(self)` holds under its own durable knowledge. Liveness for MAJOR-A then comes from knowledge
convergence among the survivors, not from ignoring the candidate's knowledge:
- Carry the newest `-3` state (epoch, version, members) on CanvassPosition, RequestVote and Vote.
- Adopt a newer state into the knowledge store as knowledge.

That adoption must bypass the MEDIUM-A term rule. That rule gates **acks** to a leader; it does not gate learning
a peer's persisted state, because the dead leader's states are all older than the election term. Split `offer()`
into "adopt" (newest wins) and "adopt and ack" (term-gated). In MAJOR-A, node 3 then learns (1,4) = {1,3} from
node 2 during canvass, counts itself, and wins. Here, node 2 learns nothing that re-admits it, so it cannot count
itself.

Land the scratch test as `DrIntegrationRegression.SelfVoteOfExcludedCandidateGated_NoAckedLoss`. Change
`consensusElectable_` to count the candidate only when `consensusVoteGate_(cand, cand)` holds, after the knowledge
exchange is modelled. Also add an `ElectionBallotRegression` test for the self-vote.

## MAJOR-R3 — The term rule reads `max(candidateTermId, leadershipTermId)`, and a denied RequestVote bumps `candidateTermId` on a live leader and its followers: the live leader can never again commit a metadata ISR change (reproduced: a dead member pins the HW)

**Where**
- `BrokerNode.cpp:870-876` (the term source).
- `InternalIsrKnowledge.cpp:69` (refusal when `leaderEpoch < term`).
- `Election.cpp:713-722`: `onRequestVote`'s deny branch, taken for a stale log or for `!mayVoteFor`, calls
  `proposeMaxCandidateTermId(candidateTermId)`. Only `Init` is excluded, so this also runs in **Closed**, on the
  steady-state leader and its followers.
  - The bump is persisted in the node-state file, so restarting does not clear it.
  - Nothing makes the Closed leader step down. Aeron's ConsensusModuleAgent enters an election on an unexpected
    higher-term vote request; this port never does.
  - The leader's internal replica keeps `epoch_ = leadershipTermId = T`.

**Event sequence** (RF=3, leader 1 at term T). This is `ReReview3.DeniedVoteBumpFreezesInternalIsrOfLiveLeader`, with the denial bump modelled by `consensusVote`:
1. Node 3 loses consensus heartbeats from node 1 (asymmetric partition, or load) and stands at T+1. Round 3 makes
   this more likely: a node outside the metadata ISR now stands as well.
2. Nodes 1 and 2 are in Closed. They deny the vote (3 is not in the ISR, or 3's log is older), and both persist
   `candidateTermId = T+1`.
3. From then on, every `-3` InternalIsr state at epoch T is refused:
   - by node 2 (no ack);
   - by node 1 itself (`publishInternalIsr_` → `offer` false, so the self bit is never set).
4. Node 3 then dies. Fencing proposes {1,2}, and it can never commit. The HW counts committed ∪ proposed, so it
   stays pinned at node 3's LEO.
   - Dump: `n1 L ep=1 leo=15 hw=10 isr=1,2,3`, `n3 DOWN`.
   - The metadata log is frozen with a live majority {1,2} and a live leader: no assignment commits, no AlterIsr,
     no user-partition failover.
5. This lasts until the leader changes, which nothing triggers. It violates the time-independent-liveness rule.
   Even without step 4, re-admissions never commit (a returning node stays out of the metadata ISR) while
   node 1 leads.

**Fix.** The term that may refuse an ack must be one in which this node **granted a vote or followed a leader**: track
`max(votedTermId, leadershipTermId)` separately. A denial, and an own candidacy the node abandons, must not raise
it. An own candidacy cannot let a deposed leader commit anyway: a node that stood at T+1 ignores T's NLT,
`Election.cpp:875-877`.
- Alternatively, mirror Aeron: a higher-term RequestVote outside an election makes the leader enter an election,
  so the terms resync. The minimal fix is the voted-term source.
- Add an `ElectionBallotRegression` test showing that a Closed follower denying a higher-term candidate does not
  raise the ack term.
- Land the DR repro: deny-bump, then crash a member, then expect the shrink to commit and the HW to advance.

**Possible link to the `LargeBatchReplicationRegression` flakes.** Round 3 changed no user-partition data path:
- Every round-3 `PartitionReplica` change is guarded by `internalQuorum_()` or `internalPartition_()`.
- The transport change applies only to `topicId < 0`.

So the ISR lag-shrink and eviction code those tests exercise is unchanged. But they run real 3-BrokerNode
clusters whose controller commits through `__cluster_metadata`. Under `-j6`, consensus heartbeats lapse, so a node
stands and is denied, which reaches this MAJOR. A lagging metadata follower then pins the metadata HW, and the
tests' committed-ISR or assignment waits time out. The 45 s deadlines, and
`UnservableStoredBatchEvictsFollowersLegacyTransport` waiting for a committed ISR of 1, fit that pattern. This is
plausible but not proven. Check the next failing run's stderr for e359e44's WARN, `refused internal ISR state ...
older than this node's consensus term`. If it is there, this finding is the cause.

## LOW-R3-1 — A fail-closed (or fresh) node relearns its knowledge only on the next ISR change; transient open() errors fail closed

- `internalIsrAcks_` is cleared only on becomeLeader and on a new version (`PartitionReplica.cpp:337/2109`).
  - A follower that restarts cleanly (fetch ≥ HW, so no MEDIUM-4 eviction) keeps its ack bit, so anti-entropy
    never re-sends to it.
  - If its file was corrupt, it stays non-votable until some unrelated `-3` ISR change.
  - An absent file on a node that has acked before (deleted file, or one upgraded from before DUR-1) is "fresh":
    the node votes for anyone and is never corrected.
- `load_()` treats any `open()` failure other than ENOENT as corrupt (`InternalIsrKnowledge.cpp:141-144`),
  including EMFILE, ENFILE and EIO at startup.
- If every node fails closed (a format-version downgrade, or a permission change during a rollout), the cluster is
  headless. The only escape is the unclean override.
- Today the unconditional self-vote (CRITICAL-R3) masks this. Once the self-vote is gated, one non-votable node
  plus a dead leader in RF=3 is headless, so this becomes MEDIUM.
- **Fix:** clear the follower's ack bit on `incarnationChanged` (fetch handler, `PartitionReplica.cpp:673`), and
  retry transient open errors instead of failing closed.

## LOW-R3-2 — A coordinator partition's first assignment seeds the ISR with every replica, even over existing data

`MetadataService.h:197-207` (`cur == nullptr`): leader = controller, ISR = all replicas. That is right for a fresh
cluster, because every log is empty. On a cluster upgraded with existing `__consumer_offsets` or
`__transaction_state` data, there is no assignment record yet. A controller that was lag-shrunk under the old
internal-ISR model becomes leader at the consensus term, and the followers truncate to it, losing committed
offsets or transaction state. This is upgrade-only. Fix: seed ISR = {self} plus the replicas whose reported
position is ≥ the max reported, and lead only if self holds that max; otherwise let the max holder lead.

## LOW-R3-3 — Per-cycle allocation in coordinator reconcile while the controller is not the coordinator leader

`reconcileCoordinatorPartitions` runs on every control duty cycle. While a live non-controller leads (the
controller catching up), `decideCoordinatorPartition` copies `live = eligible` (`MetadataService.h:209`) and builds
`ElectionInputs` vectors on every cycle. This is cold relative to produce and fetch, but it breaks the
allocation-free per-cycle liveness poll convention (`Election::eligibleNodeIds`). Return early before the copy when
`cur->leaderNode` is live and in the ISR.

## Hard-rule check

| Rule | Result |
|---|---|
| Atomics and mutexes | OK. `check_no_atomics.py` is clean and the diff adds none. |
| Virtual calls, std::function, allocation on hot paths | OK on produce and fetch. The term source is a plain function pointer with a captureless lambda (cold). Anti-entropy is internal-only and cold. Control-cycle allocation: LOW-R3-3. |
| Tests never sleep | OK. No `sleep_for` or `usleep`. The new BrokerNode test paces itself on blocking control-thread round-trips. |
| RED-first `*Regression*` | OK for every round-2 fix. CRITICAL-R3 and MAJOR-R3 need regression tests; both repros are ready in `repros/rev5-dur1-round3-Repro.cpp`. |

---

# Re-review (fix round 4)

Scope: rune-broker-wt31 `feat/dur-1` e359e44..93fbab1 (dc9df66, 93fbab1). rune-protocol-fe unchanged at a9787dc.
Read-only. `build-fast` was up to date ("no work to do"). I ran every round-4 regression, plus the round-2 and round-3
ones they touch, from the existing binaries:
- consensus: 5/5 (`CandidacyStateRoundTrips…`, `VoteGate_StaleSelfKnowledgeStillStands`,
  `DeniedRequestVoteNeverRaisesAckTerm_GrantDoes`, `SelfVoteGatedByOwnKnowledge…`, `CandidacyStateExchanged…`);
- replication: 2/2 (`StateOlderThanConsensusTerm…`, `TransientOpenErrorRetriedNotFailClosed`);
- DR: 5/5 (`StaleSelfKnowledge_ConsensusNotHeadless`, `ZombieLeader…NoAckedLoss`,
  `DeniedVoteNeverFreezesLiveLeaderInternalIsr`, `SelfVoteOfExcludedCandidateGated_NoAckedLoss`,
  `RestartedFollowerRelearnsCommittedState_FailClosedHeals`).

`check_no_atomics.py` is OK. The diff adds no `sleep_for`, `usleep`, `std::atomic`, `std::mutex` or `std::function`.

## Verdict: APPROVED (no blocking findings)

0 CRITICAL, 0 MAJOR, 0 MEDIUM, 4 LOW.

### Status of the round-3 findings

| Finding | Status | Evidence |
|---|---|---|
| CRITICAL-R3 | **Fixed** | `Election.cpp:1278` `gateSelfVote_` sets the self-vote to For/None from `mayVoteFor(self)`. It runs after `becomeCandidate` (`:374`) and at the top of every `candidateBallot` step (`:399`), before `isUnanimousLeader`/`isQuorumLeader`. `None` makes both counters skip self, and `isUnanimousLeader` fails. The only leader path that skips the ballot is the single-node cluster (`:300`), which has no peers to lose records against. The reviewer's repro is landed as `SelfVoteOfExcludedCandidateGated_NoAckedLoss` and is green. |
| MAJOR-R3 | **Fixed** | `votedTermId_` is raised only in the grant branch (`Election.cpp:740`). `ackTerm` = max(voted, leadership) (`AckTerm.h`), and `BrokerNode.cpp:908` reads `ackTermId()`. The deny branch and an abandoned own candidacy leave it unchanged. The DR repro and the ballot test are both green. |
| LOW-R3-1 | **Fixed** | A changed incarnation clears the follower's ack bit, guarded by `internalQuorum_()` (`PartitionReplica.cpp:679-681`). Clearing it while a proposal is pending is conservative: a replica that may have lost its file must re-ack. Transient errno values put the store in "load pending", which refuses votes and persists and retries on use. Other errors still fail closed. |
| LOW-R3-2/3 | Backlog (as ruled) | — |
| LOW-B | **Open** (follow-up) | Nothing yet drives template 58 + `Election` + the BrokerNode exchange lambdas (`BrokerNode.cpp:872`) over real Aeron. CRITICAL-R3's liveness (MAJOR-A) now depends on that wiring, which only the DR model (all-to-all, instantaneous `exchangeKnowledge_`) and the unit tests prove. Keep it as the first follow-up. |

### Safety analysis of the template-58 exchange (focus 2)

- **Stale states.** Adoption is newest-wins on (leaderEpoch, version), compared lexicographically by `newerThan`.
  A stale state can never displace a newer one.
- **A higher version at an older term.** A zombie's high-version pending state at epoch E loses to any state at E+1.
- **Learning cannot admit an unsafe candidate.** Let C be the latest committed metadata-ISR state.
  - Every grant majority contains a voter that persisted something ≥ C, and that voter gates by its own newest state.
  - A state newer than C is one of two things:
    - a pending proposal of C's leader. An expansion is validated at ≥ max(HW, electionLeo), and HW counts committed ∪ proposed;
    - a state of a later leadership. Its HW counts the all-replica seed until its first state commits on a majority, and from then on that newer state supersedes every older one on that majority.
  - So a learned state never admits a replica missing committed records. A zombie's adopted pending *expansion* at E
    can sit only on nodes that hold no E+1 state. Any grant majority still intersects the E+1 commit majority, and
    that intersection voter ignores it.
- **Storing without acking.** `onInternalIsr` acks only as `Follower` of `m.leaderId` at `m.leaderEpoch`. A node
  that adopted a newer state does ack an older proposal without holding it, because `adoptNewest_` returns "holds
  s or newer" (`InternalIsrKnowledge.cpp:97`). That stays safe:
  - The newer state it holds is itself safe.
  - A zombie commit still needs a node of the new grant majority to ack. Every granter refuses through the term rule.
  - The winner refuses too, because its `leadershipTermId_` becomes E+1 the moment it wins, inside the same serialized
    control-thread step as the self-gate.
- **A candidate's own acks.** A candidate's ack term stays E, so it can ack a zombie's E proposal while standing at
  E+1. If that proposal removes the candidate, the candidate's own gate then drops its self-vote. The two steps are
  serialized on one thread, so it cannot both count itself and help commit its own removal.
- **The rounds 2-3 arguments still hold.**
- **The changed `ZombieLeader…` expectation.** "Any leader, no acked loss" is correct. The zombie's pending
  shrink only restricts the votes.

### Term source across restart (focus 3)

`votedTermId_` is not persisted. After a restart, ackTerm = max(0, restored leadershipTermId) can be lower than a
term the node granted in before the crash. This cannot let a zombie commit:
- The internal replica restarts as Follower, epoch 0, leader -1 (`BrokerNode.cpp:650-653`).
- The durable `candidateTermId` makes the Election ignore the stale term's NewLeadershipTerm.
- So no ack goes to the zombie.

It cannot freeze ISR changes either: denials no longer count. See LOW-R4-4 for the overstated comment.

### Fused-runtime flake (focus 4)

`ShardModeFusedRegression.PartitionLeaderFailsOverBetweenFusedRuntimes` needs the controller to commit the
reassignment through `__cluster_metadata`. MAJOR-R3 can freeze exactly that commit on c319e9b: a denied vote under
load followed by a stalled metadata HW. It was introduced in 3933caa and fixed by dc9df66. Round 4 adds no
user-partition data-path change: the incarnation clear is `internalQuorum_()`-only, and the exchange is
consensus-only. MAJOR-R3 is the plausible cause, now fixed. That remains unproven, because neither the report's ABAB
run nor this review reproduced the flake. Keep it a watch item and capture stderr for the stale-term WARN.

---

## LOW-R4-1 — A leader exports its own *pending* proposal; a pending {L} can leave survivors admitting only a dead L

**Where:** `InternalIsrKnowledge.h:146`. `candidacyState()` returns `latest()`. On the leader that includes its own
pending proposal, which it persisted as its self-ack (`PartitionReplica.cpp:2167`).

**Event sequence.** RF=3, L leads at epoch E, committed {L,A,B}@v.
1. A and B stall. L fences or lag-shrinks them, giving pending {L}@(E,v+1), held only on L.
2. A and B resume and move to a newer term without L: A stands at E+1 and B grants.
   - B refuses L's data-plane re-send under the term rule.
   - A's InternalIsr frames from L are lost, or A is not fetching.
   - So {L} never commits.
3. L, now out of Closed, canvasses and sends {L} through template 58. A and B adopt it durably.
4. L dies before any newer state commits.
5. A and B now admit only L, so the cluster is leaderless until L returns. Yet the HW counted committed ∪ proposed
   = {L,A,B}, so A and B hold every committed record.

**Severity.**
- **Narrow.** It needs both followers to refuse or miss L's data-plane frames while receiving its canvass.
  Usually A, as a candidate whose ack term is still E, persists and acks {L} through the data plane, and then {L}
  is committed.
- **Kafka-equivalent or better.** Kafka's controller commits a shrink to {L} without follower acks, and an
  ISR-{L} partition goes offline when L dies (unclean disabled).
- **Safe automatic fix.** The exchange is liveness-only, so exporting less is always safe.
  - Have the leader export its last *committed* (epoch, version, members), not `isrProposal_` while pending.
    Followers keep exporting their newest persisted state.
  - In RF=3 this closes the gap entirely: a follower-persisted pending state is L+F, a majority, so it is committed.
  - At RF≥5 only the "follower acked its own removal" corner remains.
  - It also restores the zombie test's original expectation.

## LOW-R4-2 — `adopt()` does no plausibility check; one malformed state poisons the knowledge permanently

**Where:**
- `InternalIsrKnowledge.cpp:82` checks only the topic, partition and `0 ≤ count ≤ 16`.
- `BrokerNode.cpp:872` (learn lambda) passes the state through.
- `ConsensusAdapter.h` (template 58) decodes it.

A state with `count == 0`, members outside the configured cluster, or an implausibly high `leaderEpoch` is adopted
as newest. Newest wins, so no real state ever supersedes it:
- The node then refuses every candidate.
- Its `offer()` returns "holds newer", so it acks real proposals without holding them.
- It also re-exports the bad state to every participant.

The consensus channel is trusted and unauthenticated, as all of Aeron's is, so this needs a bug or a future
template-58 layout change. It is not an attack path. **Fix (cheap):** in `adopt`, require `count ≥ 1` and every member in
the configured replica set. A further check that the epoch is ≤ the highest consensus term seen is optional.

## LOW-R4-3 — Residual: with the self-vote gated, one fail-closed (corrupt-file) node plus a dead leader is headless in RF=3; a deleted file votes for anyone

This was predicted in LOW-R3-1. Transient errors now retry, but a genuinely corrupt file still fails closed:
- The node neither votes nor exports.
- Its `votable()` can be re-proven only by a current-leadership `offer`, so it needs a leader.
- With the leader dead, the RF=3 survivor S has only its self-vote. The cluster stays headless until the old leader
  returns or an operator sets `RUNE_UNCLEAN_CONSENSUS_ELECTION`.

This is acceptable: Kafka also needs an operator for on-disk corruption, and the node cannot be made votable from
one peer's view without losing safety. Document it as an operator runbook item.

An *absent* file on a node that has acked before (deleted by an operator) is still treated as fresh, so it votes
for anyone. This is now mitigated: the candidate's CandidacyState precedes each RequestVote on the same
publication, so the node usually learns the candidate's view before it votes. Operator-error only.

## LOW-R4-4 — Comments overstate "never persisted" after a restart

- **Where:** `Election.h:431` (the `votedTermId_` note), `InternalIsrKnowledge.cpp` `offer`, and the MEDIUM-A wording.
- **The overstatement.** These comments say a deposed leader's state is "neither persisted nor acked".
- **What actually happens after a restart.** The ack term can be below a pre-crash grant, so the node may
  *persist* a zombie's state. It still never acks it, because the replica starts as Follower at epoch 0, leader -1.
- **Why this is safe.** Persisting without acking is exactly what `adopt` does, which is shown safe above.
- **Fix.** Reword the comments to "never acked; persisting is knowledge only". No code change is needed.

## Hard-rule check

| Rule | Result |
|---|---|
| Atomics and mutexes | OK. `check_no_atomics.py` is clean. |
| Virtual calls, std::function, allocation on hot paths | OK. The exchange uses plain function pointers and captureless lambdas on the control thread (cold). `publishCandidacyState_` encodes into the publisher's fixed buffer. `adopt` writes files only when a state is newer (cold). The new `ConsensusMessageHandler::onCandidacyState` virtual follows the existing consensus-handler pattern, and the adapter dispatches through `if constexpr requires`. None of it is on the produce or fetch path. |
| Tests never sleep | OK. |
| Mixed version | OK. An old adapter's `default:` ignores template 58 (`ConsensusAdapter.h:161`). A new adapter decodes fixed offsets after a `len ≥ TOTAL_SIZE` check, so appended fields stay forward-compatible. An old node voting ungated preserves liveness during a rolling upgrade. The send result is ignored, which costs liveness only. |
| RED-first `*Regression*` | OK. Every round-3 finding has a marked regression, and all are green. |
