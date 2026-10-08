# Consensus PreVote (D29) — design

Status: APPROVED by the user 2026-10-08 ("Let's do it then"); IMPLEMENTED (see Implementation notes). Origin: backlog D29 (D24 re-review P1-MAJOR-1,
briefs/2026-10-08-d24r2-d28-review.md).

## Problem

D24 gave every node a durable follow floor: it follows any leader at or above the highest term it
granted, led, followed or adopted. That ended the DR-4 stranding caused by a node's *own* failed
candidacies. One stranding remains (pre-existing):

- A node V that **granted** a vote at term f to a candidacy that then failed has floor f.
- The live leader L of term T < f is refused by V, and L's stickiness denies V's own candidacies.
- V stays out of the cluster until L dies. The cluster stays available; V does not serve.

The obvious fix was tried and removed: V tells L its floor and L steps down (etcd's lower-term
answer). With a partial partition it ping-pongs leadership:

- a cut-off node X keeps standing at rising terms;
- a bridge node V keeps granting them;
- each grant deposes the majority's newly elected leader.

That is etcd's "CheckQuorum without PreVote" class.

The root cause is that V granted a *binding* vote to X while a live leader existed. PreVote (Raft
thesis §9.6) removes that root cause.

## Design

1. **Pre-ballot before every candidacy.** When `nominate()`'s deadline passes, the node does NOT
   propose `candidateTermId_ + 1` yet. It enters a new state `PreCandidate` and sends
   `PreVoteRequest{term = candidateTermId_ + 1, logLeadershipTermId, logPosition, candidateId}` to
   every member.
   - No durable state changes: no candidate term, no floor.
2. **Voters answer without side effects.** Any state, including CLOSED, sends
   `PreVoteResponse{term, voterId, granted}`. It grants iff ALL of:
   - the candidate's log is at least as fresh as ours (`compareLog`);
   - `mayVoteFor(candidate)`, the internal-ISR candidacy gate;
   - `term > candidateTermId_`;
   - we have NOT heard a live leader within `leaderHeartbeatTimeoutNs_`. "Heard" means any
     NewLeadershipTerm or CommitPosition from a member, **including one we refused** because of our
     floor. A new `leaderHeardNs_` stamp is updated in onNewLeadershipTerm / onCommitPosition before
     any gate.
   - A pre-vote never raises `candidateTermId_`, the floor or `votedTermId_`, and never changes state.
3. **Majority, then the real ballot.**
   - On a majority of grants (self included) within `electionTimeoutNs_`, the candidate proceeds
     exactly as today: proposeMax `candidateTermId_ + 1`, `CandidateBallot`, RequestVote.
   - Otherwise it returns to CANVASS with no term change.
4. **Leader-side.** A leader in CLOSED answers PreVoteRequests like any voter: it has heard itself,
   so it denies. Stickiness for real RequestVotes stays as it is.
5. **Wire.** Two new consensus templates, 60 `PreVoteRequest` (blockLength 32) and 61
   `PreVoteResponse` (blockLength 24), hand-encoded in ConsensusMessages.h like StandDown and
   CandidacyState.
   - Wiring: publisher, adapter, handler and agent.
   - A node that ignores unknown templates never grants, so a mixed-version cluster degrades to "no
     candidacy can start" while an old node holds the majority. Rolling upgrades need all nodes on
     the new version before a leader change. This is documented and acceptable before release.
6. **Single-node / unanimous paths** are unchanged: the pre-ballot is skipped when `memberCount == 1`.

## Why this closes both issues

- **Ping-pong:** X (cut off from L) never collects a pre-vote majority. A and B hear L, and V hears
  L, even though V refuses L. So X never bumps a real term, V's floor never moves above L's term,
  and nothing deposes L. No timing race.
- **Residual stranding:** V can only have granted f if V had not heard a live leader for a full
  heartbeat timeout, i.e. L really was unreachable from V. When the link heals, V still refuses L,
  because f > T. With PreVote:
  - X can no longer drive f upward;
  - V's own candidacy at f+1 still needs a pre-vote majority, which it cannot get while A and B
    hear L.
  - **So V stays stranded until L changes.** PreVote does not fix this case by itself. It needs the
    D24 step-down notice too, and with PreVote in place the notice is safe: the notice only fires
    for a floor gained while no leader was heard, and that can no longer be ratcheted by a cut-off
    candidate.
  - **Proposal:** implement PreVote, then re-add the TermFloorNotice from the D24 round-2 diff (git
    history in briefs/2026-10-08-d24-diff-r2.patch). A notice step-down broadcasts StandDown(T), so
    followers drop their leader lease at once instead of denying pre-votes for a heartbeat timeout
    (review P1-MEDIUM-2). Add the 5-node partial-partition test the
    reviewer asked for: X–L link down, V bridge, A/B sticky. Assert no leader change for 20
    election timeouts, and assert that V re-joins after the heal with at most one leader change.

## Aeron-faithfulness

Aeron Cluster has no PreVote; its canvass phase only gathers log positions. This is a documented
Raft tightening layered on the port, like stickiness and the follow floor. CANVASS / NOMINATE /
ballot transitions are otherwise unchanged.

## Tests (RED first)

- **Unit:**
  - a pre-vote never changes candidateTermId_ / floor / state;
  - pre-votes are denied while a leader was heard, including a refused one;
  - a candidacy with no pre-vote majority returns to CANVASS with the term unchanged;
  - the wire round-trip of 60/61.
- **Multi-Election harness:**
  - 5-node partial partition (above);
  - 3-node DR-4 restart shapes: still converge with no extra leader change.
- **Docker DR:** dr3 / dr4 / dr10 unchanged pass rates; dr5 partition no new leadership churn.

## Cost

Every election gains one extra round trip: the pre-ballot, about one network RTT plus one
status-interval. It adds no durable writes.

## Implementation notes (2026-10-08)

- **Pre-ballot.** It is a sub-phase of NOMINATE (`Election::startPreBallot_` / `preBallotWon_`), so Aeron's state
  ordinals are unchanged. Requests are re-sent each status interval. With no majority by `electionTimeoutNs_`, the
  node goes back to CANVASS with its term unchanged.
- **Voter.** The grant rule is in `Election::onPreVoteRequest`. The lease is `leaderLeaseLive_`, stamped by
  `noteLeaderHeard_` on any NewLeadershipTerm or CommitPosition from a member, including one we refused. A candidate
  that is the leader we heard from is exempt. A real RequestVote in CLOSED is exempt from stickiness when it comes
  from our own leader.
- **TermFloorNotice.** Re-added as template 62. A notice step-down raises only the candidate term and broadcasts
  StandDown, so followers drop the lease at once (review round 1).
- **Tests:**
  - `ElectionLog.PreVoteRegression_*`, with an in-memory N-node `SimCluster` (cut links, restarts).
  - The stranded-member tests FAIL with the notice disabled.
  - `ConsensusMessagesRegression.PreVoteAndFloorNotice…` covers the wire round trip.
- **Honest finding.** With PreVote disabled, the partial-partition tests still pass in this codebase. Three existing
  mechanisms already close the windows PreVote targets:
  - Aeron's canvass-quorum nomination: a node nominates only if a majority is canvassing.
  - D24: a canvassing node follows any leader at or above its floor.
  - CLOSED stickiness.

  So PreVote is kept as a cheap guard (one extra round trip per election, no durable writes). It stops terms
  inflating whenever a canvassing majority exists, and it is what makes the step-down notice provably bounded. The
  notice is the part that fixes the residual stranding.

## Review rounds (2026-10-08)

**Round 1 (briefs/2026-10-08-d29-review.md).** Safety held; every finding was a liveness or test-quality issue.
- **CRITICAL-1 — term catch-up lost.** A denied pre-vote never carried the voter's term, which removed Aeron's term
  catch-up (it rode on the real RequestVote). A term/eligibility inversion then wedged 2-of-3 survivors. Fix:
  PreVoteResponse carries `voterTermId`, and the candidate adopts it as its candidate term.
- **MAJOR-1 — StandDown kept the lease.** Fix: StandDown drops the lease.
- **MAJOR-2 — stale-term messages stamped the lease.** Fix: a message below our term is ignored.
- **MAJOR-3 — tests did not discriminate.** Each fix above now has a test that fails without it.
- **MEDIUMs:**
  - a real vote is denied in lease, without term adoption;
  - candidacy state and canvass position are still published during the pre-ballot;
  - the exemption also covers our own leader;
  - the notice raises the candidate term only and broadcasts StandDown.

**Round 2 (briefs/2026-10-08-d29-review-r2.md).** No CRITICAL or MAJOR findings. MEDIUMs fixed:
- **M1.** Any explicit denial at our term loses the pre-ballot, matching Aeron's real ballot, which fails on any
  Against.
- **M2.** A term catch-up restarts the pre-ballot window.
- **M3.** The lease ignores leader messages below our durable floor. Production restarts at leadershipTermId 0, and a
  zombie must not hold the lease.

  Trade-off: a node refusing a live leader by floor no longer holds that leader's lease. Liveness wins over the
  theoretical ratchet; the notice deposes that leader anyway.
- **M4.** No latency in the sim harness — moved to the backlog.

## D29b follow-ups (2026-10-08)

- **Leader-state CheckQuorum** (r1 LOW-6 / r2 MEDIUM-3). In LeaderLogReplication / LeaderReady, after
  `leaderHeartbeatTimeoutNs_` without an active follower quorum, the would-be leader broadcasts StandDown and
  canvasses (`Election::leaderStatesTimedOut_` / `standDownFromLeaderStates_`). Aeron has no such timeout: its
  CheckQuorum runs in the agent only outside elections. That gap was harmless in Aeron, but here a leader-elect that
  lost its inbound links kept stamping the D29 lease on its peers.
  - Test: `D29bRegression_LeaderElectWithDeadInboundStandsDownAndTheRestElect`, 5 nodes, 8 seeds. It is RED without
    the timeout.
- **Sim latency, reordering and stalls** (r2 M4).
  - `SimNet` has a seeded per-message delay (`maxDelayMs`) and per-node stalls (`stallUntilNs`). `SIM_JITTER=<ms>`
    re-runs every sim with jitter.
  - All PreVote and D29b sims pass at 0, 2, 5 and 10 ms.
- **Split-vote mitigation, found and measured with the jittered sim.** A NOMINATE node that grants a peer's pre-vote
  abandons its own unwon pre-ballot and re-arms its nomination one random back-off later.
  - This is non-binding, so only our own candidacy is delayed.
  - Measured on a 5-node controlled hand-off over 200 seeds (rounds lost to a split ballot):

    | Jitter | PreVote off | PreVote (D29) | + mitigation | Worst case, before → after |
    |---|---|---|---|---|
    | 0 ms | 13 | 13 | 1 | 356 → 151 ms |
    | 5 ms | 60 | 36 | 12 | 751 → 257 ms |
    | 10 ms | 82 | 48 | 38 | 888 → 324 ms |

  - Killed-leader worst case: 1095 → 594 ms (0 ms jitter) and 2118 → 878 ms (10 ms jitter).
- **r1 LOWs.**
  - LOW-1: the self pre-vote follows the current gate.
  - LOW-2: a won pre-ballot is due at once in `nextDeadlineNs`.
  - LOW-3: the pre-ballot restarts when a deny-with-raise lifts `candidateTermId_`.
  - Each has a RED-first test.
- **r2 LOW-6.** The inversion regression also runs with production-style restarts (`boot(i, 0)`).
- **Ruled, not changed.**
  - r2 LOW-1 (the notice StandDown is sent once): a lost StandDown costs at most one lease (H). The leader-state
    CheckQuorum bounds the leader side.
  - r2 LOW-2 (the lease deny in leader states skips Aeron's deny-with-raise): a leader state either reaches CLOSED or
    times out within H. The raise then happens on the next real vote.
