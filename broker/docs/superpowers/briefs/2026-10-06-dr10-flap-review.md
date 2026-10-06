# DR-10 repeated-flap fix — review (merged 5c93fda)

Reviewer: Opus, 2026-10-06.  Scope: 0f8c9b7 (Election CLOSED exits), fdf06e0 (MetadataService
truncation rebuild), 45c3734 (DR-10 short-pause oracle), merge 5c93fda (779880a dropped in favour of
`leaderSinceNs_` grace).  Line numbers are at 5c93fda.

Verification run: `build-fast` is current at 5c93fda (`ninja: no work to do`);
`ctest -R "ClosedLeaderStepDown|LeaderLivenessGrace|MetadataTruncationRebuild|ElectionLog|MetadataReplication"`
62/62 green.  No Docker was run.

## Verdict: APPROVE WITH CHANGES

The consensus changes are **safe**: no path creates a leader, every vote still goes through the
existing persisted-then-placed Canvass grant, and a denied candidacy cannot unseat a live leader.
The MetadataService fix is correct under Rune's internal-ISR invariant.  The test oracle change does
not weaken any safety assertion.

The changes do expose a **runtime liveness gap** (MAJOR-1).  A CLOSED leader can now step down, and
when it does it keeps acting as the metadata controller for the whole re-election window.  During
that window it uses an eligible set that leaves **itself** out, so it fails its own partitions over
to its peers.  This needs a gate before the box rerun.

| Severity | Count |
|---|---|
| Critical | 0 |
| Major | 1 |
| Medium | 3 |
| Low | 6 |

---

## Findings

### MAJOR-1: a stepped-down consensus leader keeps controlling and evicts itself

**Mechanism (all at 5c93fda):**
1. `leaveClosedForElection_` (Election.cpp:1206-1216) sets `leaderMember_ = nullptr` and goes to
   Canvass.  `leaderMemberId()` is now -1.
2. `NodeRuntimeAgent` feeds that -1 to the bridge (NodeRuntimeAgent.h:287-291).
   `ReplicationMetadataBridge::doWork` acts only on `currentLeaderId >= 0`
   (ReplicationMetadataBridge.h:193-198).  So the `__cluster_metadata` replica **stays Leader** at
   epoch T.
3. `MetadataService::leaderReady_()` is `replica_->acceptsProduce()` (role only,
   MetadataService.h:1728-1735).  This means `controllerReady_()` stays true.
4. The failover slow tick (NodeRuntimeAgent.h:337-470) is gated only on `election_ &&
   failoverTick_.due`, not on `isLocalLeader_()`.  It calls `eligibleNodeIds(now, 3*timeout)`.
   With `selfLeads == false` (Election.cpp:129-141) that set:
   - drops the self-leader exemption and the `leaderSinceNs_` grace;
   - includes self only if self's `timeOfLastAppendPositionNs` is fresh.  That value is set only on
     LeaderLogReplication entry (Election.cpp:1252-1256) and the leader never refreshes it, so self
     is **absent** in any cluster older than 3×timeout.
   `ClusterMember::reset()` (ClusterMember.h:92-98) does not clear peer timestamps, so live peers
   stay eligible.
5. `reassignDeadPartitionLeaders(eligible-without-self)` therefore fails every partition this node
   leads over to a peer.  Because `changed == true`, it also runs the preferred-balance pass
   (`maxPref = 1024`).  `shrinkIsrToLive` keeps running too.

**When do these records commit?**
- **Paths 1-2: they commit.** In two of the new step-down paths the followers are still CLOSED and
  following this leader:
  1. A false-positive inactive-follower-quorum step-down (see MEDIUM-2).
  2. A RequestVote grant in CLOSED, where the other follower has not moved on yet.

  In both cases the followers' metadata replicas are still fetching at epoch T.  With all ISR members
  fetching, the HW advances and the reassignment records **commit**.
- **DR-10 zombie case: they are appended but never commit.** The peers have moved to T+1, so the
  records are truncated later.  That truncation is exactly the third bug fdf06e0 had to absorb.

**Failure scenario.** 3 nodes, L leads:
1. Both followers' control threads stall for more than 150 ms (3 brokers on one 4-core box is the
   DR topology, and starvation there is already documented).
2. L's quorum check fires and L goes to Canvass.
3. On L's next slow ticks the eligible set is {F1, F2}.  L reassigns all of its partitions to
   F1/F2, and the reassignment commits.
4. The followers then time out L's silenced heartbeat and elect.  L may well win again.
5. Leadership of about one third of all user partitions has moved for nothing.  The eligible-set
   "grow" pass on the next leader moves it back, so each spurious step-down costs two full
   leadership churns.

This is a liveness / throughput regression, not a safety one: Kafka HW + epochs keep every move
consistent.

**Before 0f8c9b7 this window did not exist for CLOSED leaders.** The only other ways into it are A1
(the brief Leader* window) and `resign()`, which by design hands off partitions first.

**Backlog D17 understates it.** It describes "one controller tick before it steps down".  In reality
the controller runs for the whole `lid == -1` window **after** the step-down, and it treats itself
as dead.

**Fix.** Gate controller decisions on consensus leadership, not on the metadata replica's role.
Any of these works:
- `isLocalLeader_()` on the failover block (keep `controlledShutdown_.doWork` outside the gate);
- a consensus-leader predicate inside `MetadataService::controllerReady_()`, which also covers
  control ticks and AlterIsr answers;
- at minimum, `eligibleNodeIds` returning 0 when `!selfLeads`.

Add a regression test: a CLOSED leader loses its quorum, and the next slow tick must emit no
reassignment.

### MEDIUM-1: granting RequestVote in CLOSED has no leader stickiness (partial-partition ping-pong)

Election.cpp:751-760 leaves CLOSED for any **grantable** higher-term RequestVote.  This applies to a
CLOSED **follower** with a live leader, and to a CLOSED **leader** that still has an active follower
quorum.

**Scenario.** Links L↔F1 are cut; L↔F2 and F1↔F2 still work (partial quorum):
1. F1 times out L and becomes a candidate at T+1.
2. F2 is CLOSED and its leader L is alive, but F2 grants anyway.  F1 wins.
3. L loses F2's AppendPositions, steps down, and times out in Canvass.  It then campaigns at T+2.
4. F2 grants again and L wins.  Repeat.

`mayVoteFor` only stops this once the internal-ISR shrink is durable.  That takes longer than the
150 ms election cadence, and each flip shrinks the other side.  Before the fix this topology was
stable: F2 ignored the RequestVote, F1 looped alone, and L kept leading.  With MAJOR-1, every flip
also churns user partitions.

Aeron does the same thing (`"unexpected vote request"` → `enterElection`), so the behaviour is
faithful.  But Rune already deviates on this exact branch (it is grant-gated), and Aeron's 10 s
timeout makes the effect far milder there.  DR-11's partial-quorum and asymmetric cases are skip
placeholders (test_dr11_flapping.py:656-726), so nothing covers this.

**Recommendation:** Raft §9.6 / etcd CheckQuorum stickiness.  Do not leave CLOSED on a RequestVote
while either of these holds:
- (leader) `hasActiveFollowerQuorum_(now)`;
- (follower) the leader heartbeat is inside `leaderHeartbeatTimeoutNs_`.

Still answer with a deny.  This is compatible with DR-10: there the old leader is paused or dead, so
neither condition holds.

### MEDIUM-2: the quorum-check timeout is correct but has thin margin; analysis of the fragment and stall concern

- **Right constant.** Aeron's leader check is `hasActiveQuorum(..., leaderHeartbeatTimeoutNs)`, and
  Rune uses `leaderHeartbeatTimeoutNs_ = max(electionTimeout, 3·statusInterval)`
  (Election.cpp:77-78).  By default that is max(150, 60) = **150 ms**.

  The 45c3734 commit message and the test comment say the window is "300 ms".  That is wrong at
  default config.
- **Leader duty-thread stalls do not cause false positives.** `onAppendPosition` stamps
  `timeOfLastAppendPositionNs = nowNs` at **processing** time (Election.cpp:~1011).  The leader's
  `ConsensusAgent` drains ingress (10 fragments) before `Election::doWork` (ConsensusAgent.cpp:39-63,
  NodeConsensusAgent.h:61-68).  After a stall, the first queued AppendPosition in the first 10
  fragments makes that follower fresh again.  In steady state only AppendPositions flow to the
  leader, so one per follower almost certainly lands in the first batch.
- **Side effect on the zombie.** Queued **stale** AppendPositions from before the pause also look
  fresh.  A resumed zombie therefore passes the quorum check for up to one extra window.  The actual
  step-down comes from the higher-term NLT path (event-driven, good).  Meanwhile the controller keeps
  running; see MAJOR-1 and D17.
- **Real false positives** come from the **followers'** send side: `FollowerLiveness` sends only from
  `closedLeaderHeartbeat` inside the follower's `doWork`, every 20 ms.  If both followers stall for
  more than 150 ms, the leader steps down.  The margin is 7.5 send intervals; Aeron's defaults give
  10 s / 200 ms = 50.  Followers already carried this sensitivity, but a follower false-positive
  leaves the leader in place, whereas a leader false-positive now (via MAJOR-1) moves partitions.

**Recommendation:** none needed for the check itself once MAJOR-1 is gated.  Fix the 300 ms wording.
Consider adding a separate leader-side headroom only if box runs show spurious step-downs; find that
out from the `"inactive follower quorum"` reason logs.

### MEDIUM-3: regression-test gaps for the new paths

ElectionLogTest.cpp:2175-2311 covers six cases.  These are missing:
- **The `mayVoteFor`-denied candidacy.** A candidate outside the internal ISR with an equally fresh
  log must keep a CLOSED leader in place.  MAJOR-R3 names exactly this, and only the stale-log
  variant is tested.
- **The CLOSED-follower variants** of NLT, CommitPosition and RequestVote.
- **Durability.** After a higher-term NLT step-down, reopening the `NodeStateFile` must show
  `candidateTermId >= T+1`.
- **Controlled-shutdown interplay.** A withdrawn (`resigned_`) CLOSED follower that gets a
  higher-term NLT must canvass silently and then follow.  `ControlledShutdown::Draining` must
  tolerate a quorum-check step-down (it re-evaluates `leadsConsensus_()` each tick, which looks
  fine).
- **MAJOR-1 gating** (see that finding).

### LOW-1: MetadataService rebuild trusts `committedEnd_` without checking the truncation point

`rebuildFromLog` replays to `max(HW, min(committedEnd_, LEO))` (MetadataService.h:1544-1546).  The
claim "committed records are never truncated" holds **only under the internal-ISR candidacy gate**.

With `RUNE_UNCLEAN_CONSENSUS_ELECTION=1` the gate is unwired (BrokerNode.cpp:955-963).  A node
missing committed metadata can then be elected, and a follower can truncate below `committedEnd_`.

The hazard is theoretical in current wiring.  `MetadataService::doWork` (step 2b,
NodeRuntimeAgent.h:299) runs before the transport poll and internal-replica `doWork` (566-612).  So
the generation-bump rebuild always happens before the post-truncation fetch can regrow the LEO,
`min(committedEnd_, LEO)` equals the truncation point, and `committedEnd_` is lowered correctly.

However, after a demotion `followerHw_` is 0, so `onDivergingEpoch_`'s "truncating below committed"
critical log (PartitionReplica.cpp:1494-1503) never fires.  `committedEnd_` is now the only place
that knows.

**Recommendation:**
- Cap the bound with `replica_->lastTruncationOffset()` on a generation-triggered rebuild.
- Log critical when that offset is below `committedEnd_`.
- Add a test that truncates below `committedEnd_`.

Other checks on this fix:
- The reset semantics are right: on restart `committedEnd_ = 0`, which is the old behaviour; the
  leader-edge rebuild leaves it unchanged; a generation rebuild lowers it to the cap.
- `msm_.commit(-1)` after the rebuild is a no-op, because commit is monotonic
  (MetadataStateMachine.cpp:103-108).
- Sibling stores (TCS, GCS) rebuild only as leader with `hw == LEO` / `hw > 0` gating
  (TransactionCoordinationService.cpp:115-131, GroupCoordinationService.cpp:150-170), so they do not
  share the empty-map-on-demotion bug.

### LOW-2: `gracefulClosedLeaderId_` persists across elections

Rune's `Election` is long-lived, while Aeron builds a new one per election with the graceful id only
on a leader close.  `gracefulClosedLeaderId_` is cleared only by a CanvassPosition from that member
(Election.cpp:711-713).  It is not cleared at `electionComplete` (only `skipFromStandDown_` is,
Election.cpp:1332) or in `leaveClosedForElection_`.

After a resign hand-off, later elections entered through the new paths still exclude the resigned
member from `isUnanimousCandidate` / `isUnanimousLeader`.  The quorum paths still work, so this is
low.  Suggest clearing it in `electionComplete`.

### LOW-3: duplicated quorum helper

`hasActiveFollowerQuorum_` duplicates `ClusterMember::hasActiveQuorum` (ClusterMember.h:171-185),
which lacks Aeron's `member.isLeader()` exemption.  The new private copy is correct.  Prefer adding
the exemption (a self id) to the static, so there is one faithful port.

### LOW-4: behaviour change for 2-member clusters

`quorumThreshold(2) == 2`, so a 2-member leader now steps down when its only follower dies.  Before,
it kept leading and controlling.  This is Raft-correct (and matches DR-4's "a minority must not
lead"), but it is a visible change for 2-node bench and dev setups.  Document it.

### LOW-5: the oracle's late-election branch settles for only 4 s

In 45c3734, `settle_s` is computed before the post-pause re-check (test_dr10_zombie_leader.py:667).
A late-detected election therefore runs `_full_convergence_assertions` after 4 s instead of 10 s.
The 90 s subscribe window absorbs this.

The mid-pause split-brain and zombie-write checks are skipped on that branch, but they were
unobservable there anyway: no claim existed mid-pause.  Cosmetic.

### LOW-6: term adoption is durable for votes but leadershipTermId stays in memory

`leaveClosedForElection_` persists the adopted term through `proposeMaxCandidateTermId`, which does
msync + fsync (NodeStateFile.cpp:140-152, 160-172) before any further action.  So the node never
votes at or below the adopted term after a crash.

`leadershipTermId_`, and with it `ackTermId()`, is in memory only.  After a crash, ackTerm falls
back to TermLog and `votedTermId_`.  This matches the A1 path and is not introduced here.  Note it
for the MAJOR-R3 restart story.

---

## Answers to the questions

**1. Safety.**
- **Two leaders: no.** All three new exits only move CLOSED → Canvass and set
  `leaderMember_ = nullptr` (Election.cpp:1206-1216).  Leadership still comes only from
  `candidateBallot` quorum or unanimity.
- **Stale-log votes: no new exposure.** The RequestVote exit condition (Election.cpp:751-757) is
  exactly the grant condition that follows (761-789): higher than `candidateTermId_`, `compareLog
  <= 0`, `mayVoteFor`, and state Canvass after the exit.  So the node leaves **if and only if** it
  grants, and the grant persists `candidateTermId` before `placeVote` (780-783).

  `compareLog` is nearly vacuous in Rune, because `logLeadershipTermId_` and the positions are
  constructor-time values that are never advanced in CLOSED.  The real data-freshness gate is
  `mayVoteFor` (internal ISR), just as for Canvass voters.  A CLOSED **leader** is the
  `__cluster_metadata` leader, so its ISR knowledge is the freshest.
- **MAJOR-R3: holds.** A denied candidacy hits the deny branch, which raises only `candidateTermId_`
  and not ackTerm (AckTerm.h).  The node stays CLOSED.  Tested for a stale log; the `mayVoteFor` deny
  is untested (MEDIUM-3).
- **Durability of the adopted term: yes.** `proposeMaxCandidateTermId` persists the term with fsync
  before `leadershipTermId_` is assigned.  If the persisted value was already higher, nothing new
  needs writing.  On the RequestVote path the term is not adopted at the exit; the grant persists
  it.

**2. Liveness.**
- **Leader-thread stalls and the 10-fragment ingress batch: no flap**, because AppendPositions are
  stamped at receipt (MEDIUM-2).
- **Follower-side stalls over 150 ms: they can flap a healthy leader**, and with MAJOR-1 that flap
  turns into partition churn.
- **Partial partitions: they ping-pong** (MEDIUM-1).
- **Timeout constant: right** (`leaderHeartbeatTimeoutNs`, as in Aeron); the margin is thin.
- **`leaderSinceNs_` grace: consistent.** It applies only while `selfLeads`.  The quorum check uses
  raw timestamps seeded at `electionComplete` (Election.cpp:1326-1330), so a fresh CLOSED leader
  never steps down on its first tick.
- **Controlled shutdown: unaffected.**
  - `resign()` and `onStandDown` work only in CLOSED, and the new exits do not touch `resigned_`.
  - `skipFromStandDown_` is false outside a hand-off election.
  - A resigned or withdrawn node that reaches Canvass through the new paths canvasses and votes but
    never stands (Election.cpp:349-354).
  - `ControlledShutdown::Draining` re-reads `leadsConsensus_()` each tick (ControlledShutdown.h:
    380-410).
  - Residual issue: LOW-2.

**3. Aeron fidelity** (ConsensusModuleAgent, outside an election).

| Trigger | Aeron | Rune now |
|---|---|---|
| `slowTickWork`: `!hasActiveQuorum(leaderHeartbeatTimeoutNs)` → "inactive follower quorum" → `enterElection` | yes | faithful, same timeout, leader self-exempt |
| `onNewLeadershipTerm`: `leadershipTermId > this.leadershipTermId` → `enterElection` | yes | faithful, plus the term is adopted durably at the exit (stricter, same as A1) |
| `onCommitPosition`: higher term → `enterElection` | yes | faithful (no sender check, like Aeron) |
| `onRequestVote`: `candidateTermId > leadershipTermId` → `enterElection`; the request itself is not answered | yes | **deviates on purpose**: leaves only if it would grant, then answers in the same call |

The RequestVote deviation is defensible.  It is stricter than Aeron, and answering immediately is
equivalent to the candidate re-sending.  The stickiness gap in MEDIUM-1 is present in both.

**4. MetadataService.**
- **"Committed records are never truncated": true in Rune under the internal-ISR gate.** The HW
  (min LEO over ISR) commits only with every ISR member, and ISR changes need majority acks under the
  ackTerm rule (AckTerm.h).  Voters admit only candidates in the newest durable ISR.  So the elected
  leader holds everything any node saw below its HW.
- **When it can be false:** with `RUNE_UNCLEAN_CONSENSUS_ELECTION=1`, or if the internal-ISR
  machinery has a bug.
- **What happens if a truncation goes below `committedEnd_`:** the LEO cap plus the cycle ordering
  make the rebuild stop at the truncation point and lower `committedEnd_`.  The only gap is the lost
  critical signal (LOW-1).
- **Reset semantics: correct.**
- **`readUpTo`:** returns whole batches below the bound and has the same semantics as `read()`
  (PartitionReplica.cpp:1061-1069).

**5. Oracle.** It does not weaken safety.  A late-detected moved leadership now gets
`_full_convergence_assertions`, a strict superset of the short-path checks: single leader, zombie
rejected, no committed loss, ordering, exactly-once, post-recovery usability.  The only assertion it
removes is a **stability** expectation ("the same leader resumes"), which the Aeron-faithful
step-down legitimately violates.  Nits: LOW-5 and the 300 ms wording (MEDIUM-2).

**6. Hot path: untouched.**
- The Election changes run on the consensus duty thread (`NodeConsensusAgent`).
- `MetadataService` and the internal-replica `readUpTo` run on the control thread.
- `PartitionReplica::read()` (now a forward to `readUpTo`) is called only by control and admin
  stores: ReplicatedOffsetStore, ReplicatedTxnStateStore, AdminWorker, BrokerNode's schema load.
  Worker produce and fetch use `log->read` (Worker.h:2764).
- The new cost in CLOSED is one O(members) scan per 20 ms status interval.
- The fsync in `leaveClosedForElection_` is on a cold transition only.  Under fused placement it
  lands on worker 0, like every existing vote path.
