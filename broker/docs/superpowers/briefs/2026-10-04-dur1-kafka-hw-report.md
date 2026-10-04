# DUR-1 fix round — Kafka model for acks=all (report)

Broker: `/Users/vradu/Projects/rune-broker-wt31` branch `feat/dur-1` (local, not pushed), on top of 0ff9322.
Protocol: `/Users/vradu/Projects/rune-protocol-fe` branch `feat/fetch-epoch` (local, not pushed).
Commits: see §6.  Nothing pushed; no conan command run; no subagents.

Binding decision implemented (user, 2026-10-03): acks=all = Kafka model — HW = min LEO over the
(maximal) ISR, lag shrink after replicaLagMaxMs, minIsr gates acks=all, any ISR member is a complete
leader, elections only from the controller-known ISR; completeness / adoption / majority-commit
machinery removed; KIP-320/595 epoch validation, KIP-279 truncation, KIP-966 data-plane ISR soundness
kept from 0ff9322.

The interrupted round left 34 uncommitted files (backup: `briefs/wip/2026-10-04-wt31-dur1-kafka-hw-WIP.patch`,
untouched).  Assessment: user-partition Kafka model + AlterIsr was mostly done and good; the internal-
partition design was NOT (it kept a "pigeonhole" completeness gate that still froze the control plane —
reproduced, see §3); several correctness/liveness holes in the ISR logic and the controller were found
and fixed (§3).

## 1. Design as built

### 1.1 High watermark (HighWatermark.h)
- `ReplicaLeaderState::computeHw` = min LEO over the ISR it is given (no sort, no buffer, no
  allocation; the old majority-th-highest path and its >32-member heap fallback are gone).  Majority vs
  AllInSync now differ only in min-ISR (cfg.minIsr vs RF) and AllInSync's pre-existing non-monotonic floor.
- The ISR handed to it is the MAXIMAL ISR (KIP-497/841): committed ∪ the pending AlterIsr proposal.

### 1.2 User partitions — controller-committed ISR (Kafka AlterPartition)
- The leader holds `committedIsr_` (what the controller committed; min-ISR and elections use it;
  `isr()` returns it) and proposes changes with `AlterIsr` (rune-protocol template 7) through
  `PartitionTransport::sendAlterIsr` (AeronPartitionTransport routes it to the current controller =
  consensus leader; local delivery when it is the controller).  BrokerNode wires the channel for every
  user replica in multi-node mode; single node / unit rigs stay self-managed (leader = own controller).
- ONE proposal in flight, re-sent every 50 ms until a committed change resolves it.  While pending, the
  HW counts committed ∪ proposed: a member being shrunk keeps holding the HW back until its removal is
  committed (it is electable until then); a member being added counts at once (validated at >= HW).
- NEW (this round): every AlterIsr carries `baseIsr` = the committed ISR it was derived from; the
  controller commits only while that is still the committed ISR (set compare) — the analog of Kafka's
  partitionEpoch check.  A stale / re-ordered proposal can never re-admit a member a later change
  removed while the HW moved past it.  Protocol: AlterIsr block 88 → 156 B (unpublished template).
- Controller (`MetadataService::onAlterIsr`): accepts only from the current leader at the current
  leaderEpoch, base == committed ISR, leader ∈ ISR ⊆ replicas, no other change for the partition in
  flight; appends UpdatePartitionAssignment (same leader / leaderEpoch, partitionEpoch + 1).  The
  leader applies it as a same-epoch becomeLeader (only the ISR changes).  Duplicate detection is
  set-based.  The controller never refuses a well-formed proposal from the current leader for good
  (only transiently), so a pending proposal always resolves.
- Controller broker fencing (rebalance tick): a node outside the eligible set leaves the committed ISR
  of every partition whose leader is alive (no allocation in steady state).
- Lag rule (IsrManager): Kafka lastCaughtUpTimeMs (caught up to the leader LEO at a fetch, or to the
  LEO of the previous fetch) instead of "last fetch time"; shrink/expand now run IN PLACE (no vector
  allocation per ISR check; `ReplicaLeaderState::setIsr`/`IsrManager::setIsr` copy into existing capacity).
- Expansion only considers replicas OUTSIDE the committed ISR, validated in this leadership, fetch
  offset >= HW, recent fetch (fix: see §3 FetchingButLagging).
- Elections (DynamicBalancer / FailoverElection): only from the committed ISR.  REMOVED the position-
  based "restorability" (preferred election of / ISR re-seeding with a replica OUTSIDE the committed
  ISR on a LEO report — lineage-blind).  The leader's AlterIsr re-admission is what un-collapses an ISR
  now.  The position-proven survivor election (no committed-ISR member live, > RF − minIsr live
  replicas) is kept but now counts only replicas that REPORTED a position.

### 1.3 Internal partitions (__cluster_metadata, __consumer_offsets, __transaction_state)
Led by the CONSENSUS leader, which is elected data-blind (bridge); their ISR is leader-local (seeded
with every replica at becomeLeader).  To make "the leader is an ISR member" hold without a controller:
- NO lag shrink.  The internal ISR drops only members the cluster considers dead
  (`PartitionReplica::shrinkIsrToLive`, called by NodeRuntimeAgent with the consensus-eligible set —
  Kafka broker fencing) and unclean (regressed) members.  So every live node the consensus can elect
  holds every committed record (HW = min over the ISR) and is writable at once.  A fenced node re-enters
  through the normal expansion rule once caught up.
- Cost: a live but slow member slows internal commits (acks=all waits for it).
- The internal min-ISR is cfg.minIsr again (the WIP's "raise to a majority" is gone with the gate).
- Residual gap (E1b, consensus side): a node fenced out (dead) that rejoins and wins a consensus
  election BEFORE it caught up lacks records committed while it was dead — a double fault (its death +
  the leader's failure during its catch-up).  Kafka's controller would never elect it; closing it needs
  the consensus election to respect the internal ISR (candidacy rule / vote on internal positions).
  Pinned by `DrIntegration.DISABLED_E1b_InternalFencedMemberElectedBeforeCatchUp_NoAckedLoss`.

### 1.4 Kept from 0ff9322 / the WIP (reviewed, kept)
Real partitionLeaderEpoch per batch; lastFetchedEpoch validated on every fetch; DivergingEpoch →
KIP-279 truncation (payload now rides as a 12-byte ErrorPayload in the record-less error frame — data
frames keep the 46-byte framing; OffsetOutOfRange carries the leader end offset and the follower
truncates to it); KIP-966 data-plane unclean eviction (`FollowerSlot::unclean` → removed / proposed out
until a validated fetch is back at the HW); mixed v2/v3 replication fails closed; epoch cache rebuilt from
stamps when the checkpoint is missing/corrupt; `endOffsetForEpoch` of an epoch older than the earliest
cached answers (requested, earliest start) (Kafka); compaction keeps each batch's partitionLeaderEpoch;
demotion drops a phantom (empty) epoch entry; same-epoch re-assert only applies the ISR.

## 2. Removed
- Leader-completeness gate entirely: `leaderComplete_`, `completenessFloor_`, `completenessIsr_` (WIP had
  already dropped it), `maybeCompleteLeadership`, `isLeaderComplete()`, the kErrLeaderNotComplete HOLD
  answer (-8 reserved), `MetadataService::leaderPendingCompletion`, NodeRuntimeAgent's "eligible-set
  change pending while the metadata leader is incomplete", the completeness clauses of TCS::loadable_ /
  stillLoaded_, ClientGateway inline PSM gate, RcState `leaderComplete` fact.  `acceptsProduce()` =
  `role_ == Leader`.
- Majority-of-ISR HW (and its sort/heap path); internal min-ISR majority raise.
- DynamicBalancer position-based restoration outside the committed ISR.
- Tests of the removed mechanisms: LeaderCompletenessGateTest.{DeficientNewLeaderHoldsHwUntilReconciled,
  DoubleElectionChurnNoCommittedLossNoRegression}, PsmLogConsistencyRegression.InlineIncompleteLeader*
  (×2), the WIP's internal "stays incomplete" DR scenarios; rewritten: MetadataGateRegression,
  TxnCoordinatorLoadRegression.{NotCoordinatorUntilLeaderComplete → NewTxnStateLeaderLoadsAtOnceWhenHwEqualsLeo,
  CommittedPrepareAboveStaleFloorIsNeverMissed}, WorkerProduceGateRegression, TxnRcRegression.BuildStartsWithLeadership,
  DynamicBalancerRegression.RestoresLeadershipAfterIsrCollapseWhenReplicaCaughtUp (now via committed ISR).

## 3. RED → GREEN per finding

| Finding | Regression test(s) | RED evidence | GREEN |
|---|---|---|---|
| CRITICAL-1 (0ff9322 review): majority HW + position-blind ISR election → elected ISR member lacks committed records (stuck / loss) | DrIntegrationRegression.Critical1_ControllerPicksLaggingIsrMember_WritableNoCommittedLoss (WIP); ReviewRed port (below) | RED on 0ff9322 (port DrIntegrationRegression.Review0ff9322Port_Critical1_… built against the 0ff9322 tree + protocol 40ab510): every node lost 5 ACKED records (ids 11–15) | elected member writable, nothing past its LEO was ever acked, converged, no acked loss |
| CRITICAL-2: internal partitions freeze the control plane (incomplete consensus-led leader) | DrIntegrationRegression.Critical2_InternalLeaderFailover_UncommittedTailOnSurvivor_WritableNoLoss (new), Critical2_InternalPartition_WholeClusterKill9_LaggingConsensusLeader_NoLoss (rewritten), Critical2_InternalPartition_ConsensusFailoverUnderCommitLoad_NoLoss, MetadataGateRegression.NewMetadataLeaderWritableAtOnce_NoCompletenessFreeze, TxnCoordinatorLoadRegression.NewTxnStateLeaderLoadsAtOnceWhenHwEqualsLeo | the first one RED on the interrupted WIP (pigeonhole gate): `writable=0`, n3 L leo=10 complete=0 forever, n2 holds an uncommitted tail; RED on 0ff9322 (port Review0ff9322Port_Critical2_…): "the new leader froze" — n3 L leo=10 complete=0, n2 holds leo=15; with the internal lag shrink re-enabled (toggle) both DR Critical2 tests fail | writable at once, uncommitted tail truncated (KIP-279), no acked loss |
| MAJOR-3 (KIP-497/841 maximal ISR: HW may not pass a member the controller can still elect) | DrIntegrationRegression.PendingShrink_HwWaitsForTheMemberUntilCommitted, ReadmittedMemberCaughtUpBeforeItCounts (WIP) | (WIP-era) | green |
| NEW: shrink/expand oscillation — a follower whose fetches arrive but answers are lost sits AT the HW it pins; it was shrunk and re-expanded in the same ISR check, the HW never moved, acks=all stalled forever (user partitions too) | DrIntegrationRegression.FetchingButLaggingFollowerShrunk_HwAdvances | RED on the WIP: acks never settled, n3 at leo=10 = HW, ISR {1,2,3} forever | 3 shrunk (committed), HW advances, 3 re-admitted after heal |
| NEW: internal ISR lag-shrank a live member → the consensus could elect a leader lacking committed records | InternalIsrRegression.InternalPartitionNeverLagShrinksALiveMember_FencesTheDead | toggle-RED (lag shrink re-enabled): "an internal ISR lag-shrank a live member", HW passed its LEO; DR Critical2 ×2 RED too | live member kept, dead member fenced at once |
| NEW: controller elected / re-seeded replicas OUTSIDE the committed ISR on a LEO report (lineage-blind "restorability") | DynamicBalancerRegression.NeverElectsOutsideCommittedIsrEvenWithCaughtUpPosition | RED: elected node 0 (epoch-4 tail, LEO 1200 > leader 1000) and added it to the ISR | never; preferred move once the leader's AlterIsr re-admitted it |
| NEW: survivor election counted unreported live replicas toward its pigeonhole gate | DynamicBalancerRegression.SurvivorElectionCountsOnlyReportedReplicas | RED: elected the only reported replica while the unreported one may hold the records | NO_LEADER until it reports; then the most complete is elected |
| NEW: AlterIsr had no compare-and-set — a stale / re-ordered proposal could re-admit a removed member | AlterIsrControllerRegression.ControllerCommitsOnlyValidProposalsFromTheCurrentLeader (extended) | toggle-RED (CAS disabled): stale-base proposal committed | refused; current-base proposal committed |
| NEW (found by the full suite): a preferred-leader election EMPTIED the committed ISR (`newIsr = {}` meant "unchanged" but mergeChange applies it literally; pre-existing, masked by the leader-local ISR) → the new leader's AlterIsr proposals lacked itself, acks=all hung | DynamicBalancerRegression.PreferredElectionKeepsTheCommittedIsr; ShardModeRegression.AdvertisedIngressRoutesAreLiveForEndpointUnawareAndAwareClients | RED: ISR size 0; the shard test hung 2 of ~8 runs (leader: committed isr={}, pending AlterIsr {1}) | ISR carried over; shard test 0/15 failures; becomeLeader also always keeps self in its ISR |
| Pending proposal must always resolve (no permanent HW pin by a dead proposed member) | DrIntegrationRegression.PendingExpansionOfAMemberThatDies_ResolvesHwNotPinned | (design guard) | commits, then the dead member is shrunk; acks settle |
| Controller broker fencing (WIP feature, was untested) | AlterIsrControllerRegression.ControllerFencesDeadMembersOutOfTheCommittedIsr | toggle-RED (fencing pass disabled): the dead member stayed in the committed ISR | dead member removed, leader/epoch kept, steady state emits nothing |
| rev2-Repro2 #1/#2/#3, rev2-Repro3, Repro.cpp #1–#4 (0ff9322 ports) | Kip279_DivergentEpochTail_LeaderCompletesAndFollowerTruncates, Kip279_SameLengthDivergentTail_TruncatedToOwnEpochEnd, SameEpochReassert_OnlyAppliesIsr_NoDivergence, OneBatchPerAnswer_NoLivelock_ConvergesBatchByBatch, Kip279_DivergentTail_OneBatchPerAnswer_Converges, DeficientLeaderMissingWholeEpoch_NotElectable, DeficientLeaderTwoFollowersAheadDifferentAmounts_NoLossNoDivergence, MisElectionPrevented_LeaderlessUntilCleanHolderReachable, NonIdempotentProduceOnIncompleteDeficientLeader_Refused, InternalDeadMemberFencedThenReadmitted_NoFreezeNoLoss | RED evidence for these is in the R-report (f153662) | all green (Kafka contract: a lagging replica is not electable; preferred moves only to committed-ISR members — every move in the DR suite is now ASSERTed) |
| WIP MEDIUM-5/6, LOW (protocol/epoch cache/compaction/phantom epoch) | PartitionReplicaRegression.{MixedV2V3ReplicationFailsClosed, FollowerOutOfRangeTruncatesToLeaderEnd, FailedEpochCheckpointRestoreRebuildsFromStamps, DemotionDropsPhantomEpochEntry}, LeaderEpochCacheRegression.EndOffsetForEpoch_OlderThanEarliest…, LogCleanerRegression.CompactionPreservesPartitionLeaderEpoch | (WIP) | green |

## 4. Suite results (Mac, build-fast Debug, protocol include = rune-protocol-fe)
- DR suite `rune_dr_integration_tests`: 43 passed + 3 DISABLED (E1b ×2, E1d), ~0.3 s; `--gtest_repeat=30`: 30/30 clean.
- `rune_replication_tests` 403/403; `rune_dynamic_balancer_tests` 22/22; `rune_static_assignment_emit_tests` 8/8.
- Full ctest (build-fast, every test binary the branch builds, -j8) on the final commit e5d41b4:
  **3225/3225 passed** (3 DISABLED, 1 uring skip on macOS), 360 s.
  Earlier full runs during the round showed only Aeron "Address already in use" port collisions under
  -j8 (ShardMode ×4, GR5 once) — all green serially (104 ShardMode/GR/PSM/PerWorker/Rebalance tests,
  100%), except the AdvertisedIngress hang which was the real bug above.
- `scripts/check_no_atomics.py`: OK.
- RED logs (scratch): red-0ff9322-review-ports.log, red-fetching-but-lagging.log,
  red-toggle-internal-lagshrink(-dr).log, red-balancer.log, red-cas.log, red-fencing.log, red-preferred-isr.log.

## 5. Concerns
1. **Internal partitions vs a data-blind consensus (E1b, consensus side).**  The internal ISR never
   lag-shrinks a live member, so a single fault is safe and never freezes; but a node fenced out while
   dead that rejoins and wins the consensus election before it caught up (its death + the leader's
   failure during its catch-up, or a whole-cluster restart after it was fenced) lacks committed
   internal records and its KIP-279 directives truncate them on the others (DISABLED_E1b_Internal…).
   The WIP's pigeonhole gate avoided that loss but froze the control plane in plain single faults
   (reproduced) — I chose liveness + single-fault safety per the binding "no completeness machinery".
   Real fix needs the consensus election to respect internal positions (candidacy rule / vote on the
   internal log — Raft's vote rule, which Aeron's election already has but is fed position 0).
   With RUNE_MIN_ISR=1 an eligible-set jitter collapse can additionally expose internal records on
   the leader alone (min-ISR 2 keeps them unexposed).
2. **Internal partitions: a live but slow member slows internal commits** (no lag shrink) — offset
   commits / txn state / metadata wait for it.  Data-plane-only partitions of a consensus-live node
   would stall internal commits until it is fenced.
3. **AlterIsr has no response.**  Liveness relies on the controller never refusing a well-formed
   proposal for good (true now: only transient refusals); one proposal in flight, re-sent every 50 ms.
   KIP-841's "never add a fenced replica" is NOT implemented: if the controller considers a node dead
   while its leader sees it fetching, fencing and re-admission alternate (one metadata record pair per
   rebalance tick) until the views agree.  A real AlterIsr response would allow both.
4. **Position-proven survivor election kept** (no committed-ISR member live, > RF − minIsr reported
   live replicas → highest (epoch, LEO)): an availability escape Kafka does not have (it stays
   offline).  Sound by pigeonhole under HW = min over an ISR of >= minIsr, except after unclean
   restarts (KIP-966, E1b).  Now counts only reporting replicas.
5. KIP-966 controller-side gap unchanged: a power-lost replica elected before its first fetch
   (DISABLED_E1b_UncleanRestartMovedBeforeFirstFetch).
6. AllInSync keeps its pre-existing non-monotonic HW floor (not Kafka); untouched.
7. Protocol: AlterIsr grew to 156 B (baseIsr) — rune-protocol-fe 0e9b529 must be published (CI) with
   the earlier f312823/5260f39/40ab510 before the broker builds against the default include.
8. Stale per-step "majority" arithmetic comments remain in HighWatermarkTest.cpp (header note added;
   EXPECTs carry the Kafka values).

## 6. Commits
- rune-broker-wt31 `feat/dur-1`: e422e40 (fix round: Kafka model, AlterIsr CAS, internal-ISR rule,
  completeness removal, controller election policy), 48653fb (0ff9322 review ports), e5d41b4
  (preferred election emptied the ISR).
- rune-protocol-fe `feat/fetch-epoch`: f312823 (interrupted round, already committed: ErrorPayload on
  error frames, AlterIsr template 7), 0e9b529 (AlterIsr baseIsr — compare-and-set).
- Nothing pushed; no conan command run.

---

## Fix round 2 (review 2026-10-04)

Review: `2026-10-04-dur1-kafka-hw-review.md` (1 CRITICAL, 3 MAJOR, 4 MEDIUM, 2 LOW). Broker
`rune-broker-wt31` `feat/dur-1` on top of e5d41b4; protocol `rune-protocol-fe` `feat/fetch-epoch` on
top of 0e9b529. Nothing pushed, no Conan package operations, no subagents. RED logs: session
scratchpad `rev3fix/red-*.log`.

### RED → GREEN per finding

| Finding | Fix | Regression test(s) | RED (before the fix) | GREEN |
|---|---|---|---|---|
| CRITICAL-1: stale follower LEO across leaderships | `ReplicaLeaderState::resetFollowers()` (in place) on every new-epoch `becomeLeader` (Kafka `resetReplicaState`). The controller snapshot publishes a follower only after its fetch was validated in THIS leadership. Published follower slots are cleared on becomeLeader, becomeFollower and self-demotion. | `PartitionReplicaRegression.NewLeadershipForgetsFollowerPositions_HwNeverFromPreviousLeadership` (R1), `…_EndToEnd` (R1b), `PositionSnapshotCarriesOnlyFollowersValidatedThisLeadership`; `DrIntegrationRegression.Rev3Critical1_StaleFollowerLeoAcrossLeaderships_NoAckedLoss` (R1b + step 5: the controller elects off the leader's published positions) | R1: HW=20, followerLeo(A)=20. R1b: HW 15 vs A.LEO 8. The snapshot published A=20 under epoch 3. DR: **every node lost 3 acked records** (A elected off (3,20)). | HW stays at the floor until A fetches in the new leadership; nothing is published for A; no acked loss |
| MAJOR-1: ISR join below the leader-epoch start offset | Expansion requires `fetchOffset >= max(HW, electionLeo_)` (Kafka `isFollowerInSync`), for user and internal partitions | `PartitionReplicaRegression.IsrExpansionRequiresLeaderEpochStartOffset` (R2) | C (LEO 7) admitted to the ISR of a leader elected at LEO 10 | refused at 7, admitted at 10 |
| MAJOR-2: internal rolling restart truncates committed records | Internal ISR changes commit on a majority's durable knowledge, and the consensus votes and stands only for members of the newest durable internal ISRs (design notes below) | `DrIntegrationRegression.Rev3Major2_InternalRollingRestart_LaggingNodeNotElected_NoCommittedLoss`; `E1b_InternalFencedMemberNotElectableBeforeCatchUp_NoAckedLoss` (replaces the DISABLED internal E1b test — fixed); `ElectionBallotRegression.VoteGate_DeniesCandidateOutsideInternalIsr`, `VoteGate_IneligibleNodeNeverNominates`; `InternalIsrRegression.KnowledgeIsDurableAndMonotonic` | Rolling restart: the lagging node 3 won the consensus election; **every node lost 20 committed internal records**. The DISABLED E1b test failed the same way. | Node 3 is never voted for (node 2's durable knowledge excludes it); node 2 leads; nothing is lost; node 3 is re-admitted (and electable again) once caught up |
| MAJOR-3: a stuck live internal replica freezes the control plane | Lag shrink restored for internal partitions. The shrink commits on the majority's durable knowledge; the HW counts the member until then. | `DrIntegrationRegression.Rev3Major3_StuckLiveInternalReplicaShrunk_ControlPlaneNotFrozen`; `InternalIsrRegression.InternalLagShrinkCommitsOnMajorityKnowledge_StuckMemberIneligible` (replaces `InternalPartitionNeverLagShrinksALiveMember_FencesTheDead`) | Acks never settled (HW frozen); the stuck node stayed in the ISR and won the consensus election | Shrunk after replicaLagMaxMs; removal durable on {1,2}; HW advances; the stuck node is not electable |
| MEDIUM-1: AlterIsr CAS on ISR sets (ABA) | Protocol: `AlterIsr.partitionEpoch` replaces `baseIsr`, and the controller commits only on equality. The leader learns the committed partitionEpoch from every applied assignment (`becomeLeader(…, partitionEpoch)`, bridge + `WorkerOp.rolePartitionEpoch`). An applied assignment with another partitionEpoch resolves the pending proposal. | `AlterIsrControllerRegression.PartitionEpochCas_LateProposalNeverCommitsAfterAba`; `ControllerCommitsOnlyValidProposalsFromTheCurrentLeader` (now asserts INVALID_UPDATE_VERSION) | The late P1 committed X after {L,Y}→{L}→{L,Y} | refused (INVALID_UPDATE_VERSION) |
| MEDIUM-2: no AlterIsr response; a fenced replica gets re-added | The controller drops additions outside its eligible set (KIP-841). New `AlterIsrResponse` frame (OK / INELIGIBLE_REPLICA / FENCED_LEADER_EPOCH / INVALID_UPDATE_VERSION / INVALID_REQUEST), routed to the leader replica's owning thread (`WorkerOp ALTER_ISR_RESPONSE`). The leader clears its proposal only on a commit or an explicit reject. After INELIGIBLE it proposes no addition for 50 ms; shrinks are never held back. | `AlterIsrControllerRegression.ControllerNeverAddsAFencedReplica`; `DrIntegrationRegression.AsymmetricPartition_FencedReplicaNotReaddedNoFlapping` | The controller committed the fenced replica. DR: **150 AlterIsr commits + 150 fences in 300 steps**. | 1 fence and 0 re-adds while partitioned; acks=all keeps settling; re-admitted after heal |
| MEDIUM-3: survivor election on by default | `DynamicBalancerInputs::allowSurvivorElection` (MetadataService / `NodeConfig::survivorLeaderElection` / `RUNE_SURVIVOR_LEADER_ELECTION=1`). Default OFF: NO_LEADER, ISR lineage kept. | `DynamicBalancerRegression.SurvivorElectionIsOptInDefaultOff` (`SurvivorElectionCountsOnlyReportedReplicas` now opts in) | Elected node 1 without the opt-in | NO_LEADER by default; with the opt-in, the most complete reporter |
| MEDIUM-4: an unclean restart keeps its ISR seat (KIP-903) | Protocol `FetchRequest.incarnation`: a durable per-node counter (`<dataDir>/process-incarnation`) bumped on every start and stamped by every replica (`ReplicaRegistry::add`). The leader's slot keeps the newest incarnation across leaderships: an OLDER one is dropped (never credited or answered); a changed one means a restart. A validated fetch below the HW marks the follower unclean (out of the ISR until ≥ max(HW, electionLeo)) when it regressed, restarted, **or is a committed-ISR member**. Every member holds everything below the HW, so this needs no history of the follower. | `DrIntegrationRegression.Rev3Medium4_UncleanRestartSlotReset_LosesIsrSeat_NoAckedLoss`, `…_LeaderChanged_…`; `PartitionReplicaRegression.StaleIncarnationFetchNeverCredited` | Both DR tests (fix stashed): the unclean follower kept its seat and was elected; **every node lost 30 acked records**. Unit test: a dead process's fetch at 20 was credited. | Evicted at its first fetch; the other member is elected; no loss; the stale fetch is ignored |
| LOW-1: majority wording | IsrManager / PartitionReplica / HighWatermarkTest comments now describe min over the ISR | — | — | — |
| LOW-2: first-contact allocations, virtual `sendAlterIsr` | Left as ruled (cold paths). The new `sendInternalIsr`/`sendInternalIsrAck` virtuals are cold too (ISR changes only). | — | — | — |

### Design notes — MAJOR-2 / MAJOR-3 (internal partitions)

**Option chosen, and why not a consensus-log record.** The consensus layer here is the Aeron election
only. There is no replicated consensus log: every member's log position is 0, and
`__cluster_metadata` is itself an ISR-replicated partition led by the consensus leader. The reviewer's
mechanism relies on two properties of a committed consensus record:

- it is durable on a majority;
- every winning vote quorum intersects that majority.

This round provides both properties directly, which is far smaller than adding a replicated log to the
consensus. It is also sounder than the fallback ("the elected node does not lead internal partitions
until caught up"), which would need a second leader-selection mechanism plus the same durable ISR
knowledge anyway.

1. **Versioned, majority-durable internal ISR.**
   - The internal leader publishes each change as `InternalIsr{topic, partition, leaderEpoch,
     version, members}` (template 9) to every replica.
   - A replica persists the newest `(leaderEpoch, version)` in `InternalIsrKnowledge`
     (`<dataDir>/internal-isr.state`, tmp + fsync + rename + dir fsync, checksummed). Only after that
     does it ack (`InternalIsrAck`, template 10).
   - A replica acks only the leader it follows, at that epoch, so a deposed leader is never helped.
   - The change commits once a majority of the cluster holds it. Internal partitions are replicated on
     every node, so that majority is the set of consensus voters.
   - Until then the HW counts committed ∪ proposed (the existing KIP-497/841 maximal-ISR machinery:
     one proposal in flight, re-sent every 50 ms).
2. **Candidacy rule.** `Election::setVoteGate`: a node votes for a candidate only if the candidate is
   a member of the newest durable ISR of EVERY internal partition it knows.
   - The same rule decides whether the node stands itself (CANVASS → NOMINATE, re-checked in
     NOMINATE).
   - A denied RequestVote is answered like a "my log is newer" deny.
   - BrokerNode wires the gate to `InternalIsrKnowledge::eligible`.
3. **New-leadership seed.** A new internal leader keeps every replica as the HW-counting seed, as
   before, but never publishes that seed.
   - Its first published ISR names only members that fetched (validated) in this leadership at
     ≥ max(HW, electionLeo_).
   - Undecided members stay counted until they confirm or the lag rule shrinks them.
   - Nodes the cluster considers dead (`shrinkIsrToLive`) are fenced out of every proposal until the
     cluster sees them again.

**Safety.**
- A committed change is durable on a majority M1, and a consensus winner needs yes-votes from a
  majority M2. M1 ∩ M2 is non-empty, and a voter in it knows the committed version or a newer one.
  That voter refuses any non-member.
- Every published member was validated at ≥ the HW / leader-epoch start, and every member of the
  newest ISR holds every committed internal record (HW = min over the maximal ISR).
- So the consensus winner, which becomes the next internal leader, never lacks committed records, and
  its KIP-279 directives cut only uncommitted tails.
- An uncommitted removal is still counted in the HW, so any member the voters may still elect holds
  everything committed.
- The knowledge is durable, so the rule survives a whole-cluster restart.

With this in place lag shrink is restored for internal partitions (MAJOR-3): a stuck member's removal
commits on the majority, and the stuck node cannot be elected until it is re-admitted. Cost: one small
frame per replica plus one fsync'd file write per node per internal ISR change (deaths, rejoins, lag
shrinks only).

**Residuals (documented, not fixed this round).**
- Liveness: if no node belongs to every internal ISR (e.g. ISR_metadata = {L, A} and ISR_offsets =
  {L, B}, then L dies), the consensus stays leaderless until L returns. This is the per-partition
  offline state Kafka also has without unclean election.
- A deposed internal leader can gather acks from followers that have not yet applied the new
  consensus term. The window is one bridge cycle, because acks require following that leader at that
  epoch.
- MEDIUM-4, controller side (KIP-966 broker registration + clean-shutdown marker) is not covered:
  - A restarted member elected BEFORE its first fetch is not caught on the data plane
    (`DISABLED_E1b_UncleanRestartMovedBeforeFirstFetch`, comment updated).
  - Neither is a restarted member whose position sits above a new leader's stale HW view.
  - Internal partitions have the same gap: a restarted node that wins a consensus election before its
    first internal fetch.
- Docker DR e2e that relied on the survivor election being on by default must now set
  `RUNE_SURVIVOR_LEADER_ELECTION=1` (MEDIUM-3). The Docker matrix was not run this round.

### Other changes
- `InMemoryTransport` (test fake) routes every pending queue through two locked helpers
  (`enqueue_` / `take_`). The allowlisted `lock_guard` count drops from 13 to 3.
- The CRITICAL-1 clear of published follower slots is allowlisted alongside the slice-B snapshot
  atomics. `check_no_atomics.py`: OK.
- Existing tests whose old expectations pinned the bugs:
  - `NB1_BecomeLeaderUncommittedTail.HwFloorIsCommittedNotLeo`: the below-HW member now loses its
    seat.
  - `Critical2_InternalLeaderFailover_UncommittedTailOnSurvivor_WritableNoLoss`: runs 5 steps inside
    the lag window. The 300-step variant became MAJOR-3's test.
  - `Critical2_InternalPartition_ConsensusFailoverUnderCommitLoad_NoLoss`: moves via the consensus
    election model.
  - `LaggingReplicaShrunkFromControllerIsr_NeverElectable`: accepts controller fencing as the
    remover.
- DR harness additions:
  - the controller's broker fencing for user partitions;
  - AlterIsr responses and the partitionEpoch CAS;
  - per-node durable internal-ISR knowledge;
  - the consensus election with the vote rule (`consensusElect`). On internal partitions,
    `moveLeader` refuses an unelectable node.

### Suite results (Mac, build-fast Debug, protocol include = rune-protocol-fe)
- Full ctest on a2d4092: **3245/3245 passed** (2 DISABLED, 1 uring skip on macOS), 370 s.
- DR `rune_dr_integration_tests`: 50 passed + 2 DISABLED, ~0.4 s. `--gtest_repeat=30`: 30/30 clean.
- `rune_replication_tests` 411/411, `rune_consensus_tests` 390/390, `rune_dynamic_balancer_tests`
  23/23, `rune_static_assignment_emit_tests` 10/10.
- rune-protocol-fe `rune_protocol_tests` 475/475.
- `scripts/check_no_atomics.py`: OK.

### Commits
- rune-broker-wt31 `feat/dur-1`:
  - 4949530 CRITICAL-1 + MAJOR-1
  - 902cd03 MEDIUM-3 + LOW-1
  - cf08da0 MEDIUM-1 + MEDIUM-2
  - 667cd5c MEDIUM-4
  - a2d4092 MAJOR-2 + MAJOR-3
- rune-protocol-fe `feat/fetch-epoch`: a9787dc (AlterIsr partitionEpoch, AlterIsrResponse,
  InternalIsr/Ack, FetchRequest incarnation, WorkerOp fields). It must be published (CI) together with
  f312823/0e9b529/5260f39/40ab510 before the broker builds against the default include.

## Fix round 3 (re-review round 2) — completed by the controller

The round-3 agent committed every finding, then went idle (no process, last activity 06:17); the controller
finished the round.

| Finding | Commit |
|---|---|
| MAJOR-A permanent no-leader (candidate self-check; committed internal ISR not re-sent) | 2796c8a — voters-only candidacy rule; committed internal ISR re-sent until acked |
| MEDIUM-A deposed-leader commit window | 3933caa — internal ISR state older than the node's consensus term never persisted or acked |
| MEDIUM-C schemas lag-shrink outside the store + LOW-A fail-open state file | 703b087 — no lag shrink outside a quorum-decided ISR; internal-ISR knowledge fails closed (corrupt/short file -> refuse to vote; directory fsync checked) |
| MEDIUM-B one internal ISR takes down the control plane | c319e9b — only __cluster_metadata decides consensus candidacy; offsets/txn partitions on the controller-ISR path |
| (leftover) | rate-limited WARN for stale-term refusals |

Verification (controller, Mac build-fast, protocol-fe include): DR suite 30x clean; full ctest 3257/3258 —
LargeBatchReplicationRegression.MultiBatchPushAboveFollowerFrameNeverWedges4MiBRing failed once under -j6 and passed
5/5 alone; in a stress loop (LargeBatch|DrIntegration|PartitionReplica, -j6) AcksAll2MiBBatchCommitsPerWorkerPlane and
UnservableStoredBatchEvictsFollowersLegacyTransport failed once each, then the same loop passed 3x and LargeBatch alone
passed 5x under -j6 -> load-sensitive, unreproduced on demand (watch item; capture output on next occurrence).
check_no_atomics OK.  LOW-B (real-election-over-Aeron coverage, docker matrix RUNE_SURVIVOR_LEADER_ELECTION) = follow-up.
