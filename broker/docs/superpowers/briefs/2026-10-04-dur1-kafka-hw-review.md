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
