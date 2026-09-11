# Opus Audit — libs/consensus (Phase 2, Aeron Cluster Raft port)

**Date:** 2026-09-11
**Auditor:** Opus (read-only, no code modified)
**Scope:** `libs/consensus/include/rune/consensus/*.h`, `src/*.cpp`, `tests/**`
**Reference:** `docs/specs/2026-09-10-rune-aeron-cluster-port-reference.md`, `…-phase2-aeron-raft-notes.md`

---

## Verdict: **FINDINGS**

The code is well-structured and the pure quorum/compareLog math is a faithful, correct port. However there are **two Raft-safety concerns** (one a genuine double-leader risk, one a durability gap that breaks the single-vote-per-term guarantee across a crash), several correctness deviations from Aeron's `onNewLeadershipTerm`/`onRequestVote`, and a cluster of **vacuous / weak tests** that do not actually exercise the safety properties they claim.

A structural note first: there are **two parallel consensus cores** in this library:
- the **Aeron-faithful** `Election` + `ClusterMember` + `NodeStateFile` + `TermLog` (the audited Phase-2 port), and
- a **vanilla-Raft** `RaftNode` + `PersistentState` + `RaftLog` (`RaftNode.cpp`, `PersistentState.cpp`, `Messages.h`).

They do not share the vote/term durable store (`NodeStateFile` vs `PersistentState`) and implement voting twice with different rules. This is the single biggest design smell and the source of several findings below.

Severity counts: **BLOCKER 2 · CORRECTNESS 6 · SILENT 3 · TEST 5 · CLEAN 3**

---

## BLOCKER

### B1. `NodeStateFile` never fsyncs by default → double-vote after crash
`libs/consensus/src/NodeStateFile.cpp:158-162`, `include/…/NodeStateFile.h:44`

`NodeStateFile` defaults `fileSyncLevel = 0`, and `syncFile()` only `msync`s when `fileSyncLevel_ > 0`. `Election` is constructed with a `NodeStateFile&` supplied by the caller, and every test constructs it as `NodeStateFile nsf(path)` — i.e. **sync disabled**. With sync disabled, `proposeMaxCandidateTermId` writes the new `candidateTermId` only into the mmap page cache; a power loss / kernel crash before writeback loses the vote-term.

The whole point of `candidateTermId` persistence (ref §5.1, and phase2-notes §A.4 "Never grant two votes in one term across a crash") is defeated: after restart the node reads a stale `candidateTermId` and can grant a *second* vote in a term it already voted in → **two leaders in one term / split-brain**.

Aeron's `ConsensusModule` runs with `fileSyncLevel > 0` for exactly this reason. In Rune the *default* is the unsafe value and nothing in `Election` forces it.

**Fix direction:** default `fileSyncLevel` to a safe value for the consensus path (or make `Election` assert/require sync-on for the node-state file); make the vote-critical path fsync unconditionally. Add a crash-injection test (write term, drop page cache / reopen without sync) that proves the term survives.

### B2. `onNewLeadershipTerm` accepts a leader for a term this node never voted in
`libs/consensus/src/Election.cpp:692-740`

The accept gate is only:
```
inAcceptingState (FollowerBallot|CandidateBallot|Canvass|Nominate)
&& leadershipTermIdArg >= leadershipTermId_
```
It then unconditionally sets `leaderMember_`, `leadershipTermId_ = leadershipTermIdArg`, and drives to CLOSED as a follower of that leader. There is **no check that `nextLeadershipTermId` (or `leadershipTermIdArg`) equals this node's `candidateTermId_`** — i.e. the term it actually participated in / voted in. In Aeron (`Election.java:411-533`, ref §3.1 row NLT) the follower only follows the NLT whose term matches the ballot it took part in; the message is keyed on `candidateTermId`.

Consequence: a node sitting in `Canvass` will accept **any** NLT with a term `>=` its current leadershipTermId from **any** member id present in the roster, without ever having canvassed or voted for that node. Combined with the fact that the accept path never validates the leader's log against the follower's own log (`compareLog`), a node can commit to following a leader whose log is *behind* its own — a Leader-Completeness violation. The comment block at `:692-696` even acknowledges the faithful check is "if (leadershipTermId != candidateTermId_) return;" and then does **not** implement it.

**Fix direction:** gate acceptance on `nextLeadershipTermId == candidateTermId_` (adopting via `proposeMaxCandidateTermId` when strictly greater, exactly as Aeron does), and reject if the leader's advertised `(logLeadershipTermId, logPosition)` is behind this node's `(logLeadershipTermId_, appendPosition_)`. Add a test that injects an NLT for a term the node never balloted in and asserts it is ignored.

---

## CORRECTNESS

### C1. Two independent vote stores; `Election` and `RaftNode` disagree on voting rules
`Election.cpp` (uses `NodeStateFile.candidateTermId`) vs `RaftNode.cpp:139-183` + `PersistentState.cpp` (uses `currentTerm/votedFor`)

`RaftNode::handleRequestVote` persists `votedFor` in `PersistentState` (fsync+rename, durable — good), while `Election::onRequestVote` persists only `candidateTermId` in `NodeStateFile` (not durable by default, see B1) and has **no per-candidate `votedFor`** at all — it relies solely on the monotonic term bump to prevent a second grant. These are two different safety models for the same cluster. Whichever one is actually wired into the running node must be the single source of truth; the other is dead/misleading. Decide which core is authoritative and delete or clearly quarantine the other.

### C2. `onRequestVote` compares candidate log against `appendPosition_`, but advertises/votes with mixed positions
`Election.cpp:619-625, 349-354, 941-955`

`onRequestVote` uses `compareLog(logLeadershipTermId_, appendPosition_, …)` (voter side uses **appendPosition**), while `candidateBallot` sends `requestVote(…, appendPosition_, …)` and `placeVote`/`canvassPosition` advertise `appendPosition_` as the "logPosition". Aeron's `compareLog` in `onRequestVote` compares against the node's *appendPosition* consistently, so this is arguably faithful — but `willVoteFor`/`isQuorumCandidate` (`ClusterMember.h:234-290`) compare on `member.logPosition`, which for peers is populated from **canvass logPosition = a peer's appendPosition**, yet for the up-to-date gate in `onRequestVote` the local side uses `appendPosition_` and the candidate side sends `logPosition` (its appendPosition). The mixing of `logPosition` vs `appendPosition` across the canvass/vote/compare paths is not clearly reconciled and is exactly where an off-by-one "elect a leader with a shorter log" bug hides. Needs an explicit invariant comment + a directed test where candidate.appendPosition < voter.appendPosition but same term → must be denied. (Current tests only exercise equal positions or a coarse term difference — see T3.)

### C3. `nominate` re-enters `CANDIDATE_BALLOT` without random re-backoff on repeated failures
`Election.cpp:286-310, 330-342`

On `CandidateBallot` timeout without quorum the code returns to `CANVASS` (`:338`). Good. But when it re-nominates, `becomeCandidate` bumps `candidateTermId` every cycle. There is no upper bound / no "isExtendedCanvass reset only once" issue — the term can be driven up arbitrarily under a partition (term inflation). Aeron mitigates dueling via the canvass/nominate backoff; here `isExtendedCanvass_` is cleared permanently on the first `Canvass` exit (`:798-800`), so subsequent canvass rounds use the shorter `electionTimeoutNs`, accelerating term inflation during a partition. Confirm this matches Aeron (Aeron keeps using `leaderHeartbeatTimeoutNs` for non-startup canvass) — the current standalone approximation at `:245-249` explicitly deviates ("we use timeOfLastStateChangeNs_ + electionTimeoutNs_ as a standalone approximation").

### C4. `onVote` records a vote without validating the voter's advertised log/term freshness
`Election.cpp:646-666`

`onVote` records `follower->vote = For/Against` after only checking `state==CandidateBallot && candidateTermId==candidateTermId_ && candidateId==self`. It does **not** verify `voterId` is a real member other than self before trusting it, nor that the vote's `candidateTermId` on the *member* isn't being overwritten to satisfy a later `isUnanimousLeader` scan. Because `isUnanimousLeader`/`isQuorumLeader` (`ClusterMember.h:196-227`) count any member whose `candidateTermId==candidateTermId_ && vote==For`, a duplicate/forged `onVote` for the same voter is idempotent (fine), but a vote attributed to a non-existent `voterId` is silently dropped by `memberById==nullptr` (fine) — however there is no dedup guarantee that a single physical voter can't be counted under two member slots if roster has duplicate ids. Enforce roster-id uniqueness as an invariant at construction.

### C5. `leaderReplay`/`followerReplay` etc. are unconditional pass-throughs that discard the Aeron gap check
`Election.cpp:420-426, 489-496, 674-746`

`leaderReplay` always transitions to `LEADER_INIT` (`:424`) regardless of `logPosition_` vs `appendPosition_`; `followerReplay` always goes to `FOLLOWER_LOG_INIT`; `onNewLeadershipTerm` always routes to `FOLLOWER_REPLAY` (`:740`) and drops the `appendPosition < termBaseLogPosition → FollowerLogReplication` fork (`:735-739`). This is documented as a Phase-2 archive pass-through, which is acceptable *for a no-archive build* — but it means the "gap / needs-catchup" case (a follower behind the leader's term base) is **silently treated as caught-up**, so such a follower will publish `AppendPosition(logPosition)` and be counted toward the leader's quorum at a position it does not actually hold. That is a latent Leader-Completeness hole the moment any real divergence exists. Must be gated behind an explicit "Phase-2: logs assumed identical" precondition that is actually checked (assert `logPosition_ == appendPosition_` and `appendPosition_ >= termBaseLogPosition`), otherwise it is a silent correctness trap.

### C6. `hasReachedPosition` ignores `logLeadershipTermId`; quorum-at-position can count a member on a different log branch
`ClusterMember.h:70-72, 176-190`

`hasReachedPosition` checks `isActive && ltId == leadershipTermId && logPosition >= position`. It matches on `leadershipTermId` (the term the member *thinks* is current) but not on `logLeadershipTermId` (the term of the member's last appended entry). Two members can share `leadershipTermId` yet have divergent log tails; counting them together toward `hasQuorumAtPosition` at `logPosition` can over-count a quorum for a position that isn't actually replicated on a common branch. Aeron's position accounting is over the same log; in Phase 2 with pass-through replays (C5) this is masked, but it is not faithful and will bite in Phase 3.

---

## SILENT (swallowed errors / silent divergence)

### S1. `ConsensusAdapter.onFragment` silently ignores unknown/mis-sized template ids
`ConsensusAgent.cpp:47-57` (+ adapter). The drain counts the raw ring message as "processed" even when `onFragment` drops it. A corrupt or wrong-schema consensus frame is silently discarded with no error counter — a stale/adversarial message injection would be invisible. Add an error/ignored counter and surface it.

### S2. `NodeStateFile` fresh-file init writes but only `msync`s when sync>0
`NodeStateFile.cpp:55-69`. On a fresh file with `fileSyncLevel==0`, the initial `NULL_VALUE` header is not durably written; combined with B1 the file's very existence + contents are not guaranteed after a crash during first boot. Minor vs B1 but same root cause.

### S3. `msync` return value ignored
`NodeStateFile.cpp:159-161`. `::msync(...)` result is discarded; an EIO on writeback (media error) is swallowed, so a "synced" term may never hit stable storage while the code believes it did. Check and propagate.

---

## TEST QUALITY

### T1. No test injects an NLT for a term the node never voted in (the B2 hole is untested)
`ElectionLogTest.cpp:655-795`. Every follower test feeds an NLT whose term matches a prior `onRequestVote`. `OnNewLeadershipTerm_Stale_Ignored` (`:705-722`) only checks `term < current`. There is no test for `term > current from a non-canvassed leader` or `term matches but leader log is behind`. The safety property "only follow the leader of the term you balloted in, and only if its log ≥ yours" is entirely unverified. This is why B2 slipped through green.

### T2. Fencing tests assert on a term that's trivially out of range, not a realistic stale replay
`ElectionLogTest.cpp:512-555, 606-626, 705-722` use `ltid + 99` / `term=4 vs 5`. These prove "wrong term ignored" but never inject a **valid-looking same-term message from the wrong sender** or a **duplicate at an old position**, which is where real fencing bugs live.

### T3. compareLog voting tests only cover equal positions or coarse term gaps
`ElectionBallotTest.cpp:367-406, 698-721`. The up-to-date gate (C2) is exercised only with `(term,pos)` equal (grant) or `(3,50) vs self (5,100)` (deny). No case with **same term, candidate strictly behind on position** (the classic off-by-one). The most important `compareLog` boundary is untested.

### T4. `driveLeaderToClosed` fabricates follower AppendPositions at the leader's own position
`ElectionLogTest.cpp:191-217, 933-936`. The driver feeds `onAppendPosition(ltid, leader.appendPosition(), fid)` for every follower — i.e. it *asserts the followers are already caught up* rather than replicating. Combined with C5 pass-throughs, the "leader reaches quorum and CLOSES" tests prove almost nothing about replication; they would pass even if replication were a no-op (which, in Phase 2, it is). These are near-vacuous with respect to Leader Completeness. At minimum add a test where a follower reports a **lower** position and assert the leader does **not** advance quorum.

### T5. `OnAppendPosition_FromSelf_Processed` asserts nothing
`ElectionLogTest.cpp:558-572`: "No assertion on state; just confirm no crash." A no-assert test is false-green surface area. Either assert the quorum effect or delete.

---

## CLEAN (verified correct / good practice)

### K1. Quorum math is faithful and correct
`ClusterMember.h:114-153` — `quorumThreshold = (N>>1)+1`; `quorumPosition` insertion-sort into a caller-owned `rankedPositions[K]` returning `[K-1]`, zero allocation, matches ref §2.3 exactly. `isActive` uses the overflow-safe `nowNs - timeoutNs < timeOfLastAppendPositionNs` form (`:63-66`). `compareLog` (`:92-103`) is byte-for-byte the Aeron algorithm.

### K2. No busy-spin in runtime paths
`AgentRunner.h:94-101` calls `idle_.idle(w)` every cycle (backs off when `w==0`); `IdleStrategy.h:34-50` defaults to Agrona `BackoffIdleStrategy` (spin→yield→park), `BusySpinIdleStrategy` is opt-in and documented as non-default. Test drivers are bounded (`ElectionLogTest.cpp:175-187` hard 100-step cap via `nextDeadlineNs()`), no unbounded `while` in tests. `TermLog::fullPwrite` retries only on `EINTR` and asserts full-length writes (`TermLog.cpp:83-102`). This satisfies the no-spin mandate.

### K3. `TermLog` per-entry offset tracking + torn-tail handling is correct
`TermLog.cpp:181-217` loads only whole 64-byte records (`offset + recordSize <= fileSize`), stops on partial read (torn tail tolerated); `commitLogPosition` patches at the stored `e.fileOffset` (`:316-338`) so it is correct regardless of append order; `invalidateEntry` sets the high-bit flag faithfully. `PersistentState.persist()` is a correct write-tmp→fsync→rename→dir-fsync atomic update (`PersistentState.cpp:65-110`) — this is the durability model `NodeStateFile` should have (contrast B1).

---

## Memory-ordering note (informational)
`NodeStateFile` release/acquire pairing is correct in principle (`updateCandidateTermId` writes logPosition/timestamp as plain stores, then `candidateTermId` with `memory_order_release`; readers acquire — `NodeStateFile.cpp:115-132, 90-96`). The single-writer discipline holds (all `Election`/`ConsensusAgent` mutation is on the duty-cycle thread; only `stateCode_` and the mmap `candidateTermId` slot are cross-thread, both release/acquire). The ordering is fine; the **durability** (B1/S3) is the problem, not the fences.

---

## Priority fix order
1. **B1** (fsync the vote term) and **B2** (gate NLT on `candidateTermId` + log freshness) — both are split-brain / leader-completeness risks.
2. **C1** — pick one authoritative vote store; the `Election`/`RaftNode` dualism is dangerous.
3. **C5/C6** — make the Phase-2 pass-through preconditions asserted, not assumed.
4. **T1–T4** — add the missing safety tests (they currently mask B2/C2/C5).

---

## Re-audit (commit 800b5e6)

**Date:** 2026-09-11
**Auditor:** Opus (read-only)
**Fixes reviewed:** `fd1b3d2` (C1), `ede6fed` (B1/S2/S3), `89430d2` (B2), `779fe5b` (C6), `b830c4d` (C5), `800b5e6` (tests)

### Verdict: **FINDINGS REMAIN** — the C6 fix introduced a NEW liveness deadlock (N1, BLOCKER-liveness).

### Per-finding status

- **B1 — FIXED.** `NodeStateFile.cpp:158-175` now `msync(MS_SYNC)` **and** `fsync(fd_)` on every write, default `fileSyncLevel=1` (`NodeStateFile.h:44`). Vote path is durable *before* the grant: `onRequestVote` (`Election.cpp:641-652`) and `becomeCandidate`/`nominate` (`:221-222`, `:292-293`) all call `proposeMaxCandidateTermId → updateCandidateTermId → syncFile()` synchronously before `placeVote`/state change. No path grants a vote without the term on disk. No init regression (fresh-file `syncFile()` at `:68`; single-node path `:222`). S2/S3 also fixed (fresh-init fsync; msync+fsync EIO now `throw`).
- **B2 — FIXED.** `Election.cpp:740-763`: NLT now gated on `nextLeadershipTermId == candidateTermId_` (adopt-higher via `proposeMaxCandidateTermId` at `:742`; reject-lower at `:744-746`), **and** a leader-log-freshness gate `compareLog(logLeadershipTermId_, appendPosition_, logLeadershipTermIdArg, logPositionArg) > 0 → reject` (`:759-763`). Adopt-higher path stays safe because the compareLog gate still runs after it. Test `NLT_LeaderLogBehindFollower_IsIgnored` (`ElectionLogTest.cpp:1089`) is genuine and non-vacuous (real log term=5/pos=200 vs behind leader term=3/pos=50; would fail if gate removed). Aeron-faithful.
- **C1 — FIXED.** `RaftNode/RaftLog/PersistentState/Messages.h` + 5 test files removed (`fd1b3d2`). Grep confirms zero live refs (only `ConsensusMessages.h`, a distinct SBE file, remains). CMake refs removed. One vote store (`NodeStateFile`) remains authoritative.
- **C2 — FIXED.** `onRequestVote` (`Election.cpp:634-658`) is correct: stale-term deny, `compareLog>0` deny, else grant. New tests `CompareLog_SameTerm_ShorterCandidateIsBehind`, `OnRequestVote_SameTerm_CandidateShorterLog_Denies` (`ElectionBallotTest`/`ClusterMemberTest`) exercise the same-term one-behind boundary and are non-vacuous (would fail if a behind candidate could win).
- **C5 — DEFERRAL-OK.** `assert(logPosition_ == appendPosition_)` added to `leaderReplay` (`Election.cpp:429-431`) and `followerReplay` (`:509-511`). In the no-Archive Phase-2 config all nodes start at position 0, so the precondition holds and the assert fires the moment Phase-3 introduces real divergence. Deferral is genuinely safe for the current build.
- **C6 — NOT-FIXED (regressed).** The fix (`779fe5b`) added `logLeadershipTermId` to `hasReachedPosition`/`hasQuorumAtPosition` (`ClusterMember.h:74-80,184-197`) and the Election call site passes `logLeadershipTermId_` (`Election.cpp:484-486`). **This is not an Aeron-faithful port** — ref §Quorum (`docs/…-reference.md:125`, citing `ClusterMember.java:907-925`) states Aeron counts on `isActive && leadershipTermId==this.leadershipTermId && logPosition>=position` only; it does NOT check `logLeadershipTermId`. See **N1**.

### NEW findings

- **N1 (BLOCKER — liveness deadlock). C6 fix breaks quorum via a field the wire never sets.**
  `ClusterMember.h:74-80` (`hasReachedPosition` now requires `logLtId == logLeadershipTermId`), quorum check `Election.cpp:484-486`, follower-update `Election.cpp:790-806` (`onAppendPosition`).
  `onAppendPosition` — the *only* message a follower sends to advance its position in LEADER_LOG_REPLICATION/LEADER_READY — sets `follower->logPosition/leadershipTermId/timeOfLastAppendPositionNs` but **never** `follower->logLeadershipTermId`. The AppendPosition wire message (ref `:145`) does not even carry that field. So once `logLeadershipTermId_ != 0` (any run after the first leadership term with real log entries), the leader's `hasQuorumAtPosition` can never match a follower (`follower->logLeadershipTermId` stays stale/`kNullValue`) → quorum never reached → **election never completes / permanent leader-election deadlock**. The end-to-end tests pass only by luck: everything is term/logLtId 0 (`ElectionLogTest.cpp:135,1103`) and `makeActiveMember` sets `logLeadershipTermId=ltId` manually (`ClusterMemberTest.cpp:43`), so the field happens to match. The unit test `HasQuorumAtPosition_DifferentLogBranch_NotCounted` verifies the new predicate but not the live `onAppendPosition` feed path.
  Fix direction: revert C6 to the Aeron-faithful predicate (drop the `logLeadershipTermId` match in `hasReachedPosition`/`hasQuorumAtPosition`). The original C6 over-counting concern is real but is solved the Aeron way — via the FOLLOWER_REPLAY/catchup gate (the very path C5 stubs in Phase 2), so a follower only reports AppendPosition on the leader's branch. Do NOT enforce branch identity in the quorum predicate. If a defensive check is wanted, either (a) plumb `logLeadershipTermId` into the AppendPosition message and set it in `onAppendPosition`, or (b) gate it behind the same Phase-2 `logPosition_==appendPosition_` precondition as C5.

### Deferrals scrutinized

- **C3 (re-canvass backoff) — DEFERRAL-OK (liveness only).** `Election.cpp:245-249` term-inflation-under-partition is a liveness/convergence-rate concern, not a safety violation; standalone-approximation deviation is documented. No split-brain.
- **C4 (roster dedup) — DEFERRAL-OK (caller invariant).** `onVote`/`memberById` remain idempotent; forged/duplicate voter ids are handled (nullptr drop / idempotent For). Duplicate-member-id roster is a construction-time caller invariant; no live over-count path introduced.
- **S1 (ConsensusAdapter silent drop) — NOT-FIXED, DEFERRAL-OK.** `ConsensusAgent.cpp:47-57` still counts unknown/mis-sized templates as "processed" with no ignored/error counter. Observability gap only — a corrupt frame is dropped, not misprocessed; no safety impact. Recommend adding an ignored counter but not blocking.
- **T2 (fencing-test boundary) — PARTIAL.** Same-term/wrong-sender and behind-leader cases are now covered by B2/C2 tests; the "duplicate at old position" fencing case remains untested. Low risk.

### Original findings not fully addressed
- **C6/N1** — the intended fix regressed into a liveness bug (above).
- **S1** — observability only, unaddressed (acceptable).
- **T2** — partially addressed.

### No other regressions
B1's new `throw` on msync/fsync EIO is fail-stop during construction/vote (safe, not silent). B2's NLT gate does not reject a valid same-term leader (adopt-equal path `:748` continues; adopt-higher `:740-743` then re-checks freshness). The ONLY new bug is N1 (under-counting → deadlock), not over-counting.

**Report:** this file.
