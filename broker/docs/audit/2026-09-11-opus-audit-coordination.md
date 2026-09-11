# Opus Audit — libs/coordination (Phase 4: consumer groups + offsets + compaction glue)

Date: 2026-09-11
Scope: `libs/coordination/include/rune/coordination/*.h`, `src/*.cpp`, `tests/**` (unit + integration).
Method: read-only trace of commit→fetch→recover, join→rebalance→sync→assign, session-timeout→reap→rebalance.
Components audited: Types, OffsetStore, OffsetMessages (SBE), GroupCoordinator, Assignor (Range/RoundRobin), JoinGroup/SyncGroup rebalance, integration harness + compaction glue.

## Verdict: FINDINGS

The durable offset store (commit/fetch/CRC/torn-tail recovery/fsync) and the SBE codecs are correct and well tested. The group state machine is broadly Kafka-faithful. However there are several real correctness/silent bugs — the most serious being a **stale-generation fencing gap in `leave()`** and a **RoundRobin partial-subscription deviation from Kafka**, plus a family of **vacuous/false-green tests** that would not catch these.

Severity counts:
- BLOCKER: 0
- CORRECTNESS: 4
- SILENT: 4
- CLEAN: 5
- TEST: 5

---

## CORRECTNESS

### C1. `leave()` does not fence the leaving member's generation (unfenced membership mutation)
`src/GroupCoordinator.cpp:103-123` — `leave(memberId, nowNs)` removes the member purely by id, with **no generation parameter and no generation check**. A stale/zombie consumer that was already fenced out (its heartbeats returning `kErrStaleGeneration`) can still call `leave()` and mutate live membership: it will bump the generation and force a rebalance of the *current* healthy group. Kafka's LeaveGroup carries the member's generation and the coordinator ignores unknown/stale leavers. Compare `heartbeat()` (`:139`) and `syncGroup()` (`:314`), which both fence. `leave()` is the odd one out.
Fix direction: add a `generationId` parameter to `leave()` and no-op when it does not match current (or when the member is unknown, which it already handles). At minimum document that callers MUST validate generation before invoking `leave()`.

### C2. RoundRobinAssignor advances the rotation pointer only on assignment — diverges from Kafka on partial subscriptions
`src/Assignor.cpp:161-179` — the rotating index `idx` is advanced to `candidate+1` **only when a subscriber is found** (`:174`). Kafka's canonical `RoundRobinAssignor` advances the member pointer **unconditionally for every partition** and simply skips (without consuming a turn differently) members that don't subscribe. The two schemes coincide for the uniform-subscription cases the tests cover (RR_1..RR_9 all use equal subscriptions, or single-subscriber topics), but they diverge whenever a topic has a strict subset of subscribers interleaved with a non-subscriber in the rotation: this implementation "sticks" the pointer at the skipped member and can bias assignment toward one subscriber, producing a different (still valid/disjoint, but not Kafka-identical and potentially less balanced) distribution than a real Kafka broker. Because the stated contract (`Assignor.h:31-40`) is "mirrors Kafka's RoundRobinAssignor behaviour," this is a spec deviation, not just a style choice.
Fix direction: mirror Kafka exactly — iterate a single monotonic pointer over the sorted member ring, advancing once per partition; when the pointed member doesn't subscribe to the partition's topic, advance and retry (bounded), but do not reset the pointer's phase. Add a partial-subscription RR test asserting *exact* partitions (see T2).

### C3. `syncGroup()` accepts leader re-submission while already Stable and silently re-transitions
`src/GroupCoordinator.cpp:309, 326-339` — `syncGroup` permits calls in `Stable` (by design, so late followers can fetch). But the leader branch (`:328`) is reached in `Stable` too: if the leader re-sends a non-empty assignment map while `Stable`, it will **overwrite every member's assignment and call `applyAssignmentAndTransitionStable` again**, with no generation change. A retransmit or a duplicate/delayed leader SyncGroup thus silently rewrites live assignments outside any rebalance. There is no `leaderSynced_` guard on the re-entry.
Fix direction: only honor the leader-assignment branch when `state_ == CompletingRebalance` (and/or `!leaderSynced_`); in `Stable`, a leader re-sync should be a no-op ack (or fenced), never a re-assignment.

### C4. Follower `syncGroup` never drives the transition to Stable — leader-only transition can strand followers
`src/GroupCoordinator.cpp:342-345` and `applyAssignmentAndTransitionStable` (`:414-418`) — the CompletingRebalance→Stable transition happens **exclusively** inside the leader's non-empty-map branch. `syncedMembers_` is tracked (`:343`) but never consulted; there is no path where "all members synced" completes the rebalance. The header (`GroupCoordinator.h:215-216`) says followers "succeed but do not advance state," and the leader transitions immediately on submit — so a follower that calls `syncGroup` *before* the leader gets `kOk` while still `CompletingRebalance` (RebalanceTest Test 8 asserts exactly this) and receives an **empty/stale assignment** with no signal to retry. Real Kafka holds the SyncGroup response until the leader's assignment is available. As written, a follower must re-poll/re-sync after the leader, but nothing in the API tells it to, and `syncedMembers_` is dead state.
Fix direction: either (a) make follower sync return a "retry/await-leader" status when `!leaderSynced_`, or (b) buffer follower calls and complete them when the leader submits. Remove `syncedMembers_` if it is truly unused, or use it to gate completion.

---

## SILENT

### S1. New/joining member is never inserted into `rejoinedMembers_`, so a same-tick single re-join relies on `>=` slack
`src/GroupCoordinator.cpp:64-70, 287-297` — `transitionToCompletingRebalance` fires when `rejoinedMembers_.size() >= expectedMemberCount_`. On a fresh join, `triggerRebalance` clears `rejoinedMembers_` and sets `expectedMemberCount_ = members_.size()`; the just-added member is not counted as re-joined. The two-phase harness protocol re-joins every member explicitly, so it works, but the `>=` (rather than `==`) masks off-by-one/duplicate-insert bugs: e.g. if a member re-joins twice it's a set so idempotent, but the invariant "size can only reach expected when every distinct expected member re-joined" is not actually enforced against membership changes mid-rebalance (a member could leave after re-joining, shrinking `members_` while `rejoinedMembers_` still counts it, making size >= expected with a phantom). 
Fix direction: prune `rejoinedMembers_` on `removeMember`, and assert `rejoinedMembers_ ⊆ current member ids` before transitioning.

### S2. `expectedMemberCount_` can exceed live membership after a reap during PreparingRebalance
`src/GroupCoordinator.cpp:185-190, 199-227` — when `doWork` reaps a member during PreparingRebalance it calls `triggerRebalance` (resetting `expectedMemberCount_` to the new, smaller `members_.size()`), which is correct. But a member reaped by the *session-timeout* branch that then leaves the group Empty vs. non-empty is handled; the subtle case is a leaver/reap that does NOT re-trigger (none here) — currently every path re-triggers, so this is latent, not live. Flagging because the join-timeout branch (`:199`) reads `rejoinedMembers_` that may reference members already reaped in section 1 of the same `doWork`, and it iterates `members_` fresh so it's safe today, but the coupling is fragile and undocumented.
Fix direction: recompute `expectedMemberCount_` from live membership at the top of the join-timeout branch, and add an invariant assert.

### S3. Leader determinism uses generated memberIds that sort by unpadded decimal sequence
`src/GroupCoordinator.cpp:236-244, 271-285` — `generateMemberId()` returns `"<group>-<seq>"` with `std::to_string(seq)` (no zero-padding). `electLeader()` picks the lexicographically smallest id. For seq 2 vs 10, `"grp-10" < "grp-2"` lexicographically, so the "alphabetically smallest" leader is **not** the earliest joiner and flips non-intuitively as the counter crosses power-of-ten boundaries. Determinism holds (same set → same leader) so tests pass, but "leader = smallest id" is a surprising, seq-order-violating rule for generated ids.
Fix direction: zero-pad the sequence, or elect by (joinTime, id), or document that generated-id lexicographic order is intentional.

### S4. `s_memberSeq` / `memberSeq_` global counter is computed but discarded — no cross-instance uniqueness
`src/GroupCoordinator.cpp:240-243` — `global` is fetched from the atomic and then `(void)global;`. The comment claims "unique within process," but the returned id uses only the **per-instance** `memberSeq_`. Two `GroupCoordinator` instances with the same groupId will mint identical memberIds (`"<group>-1"`, `"<group>-2"`, …). The atomic increment is pure overhead and the uniqueness claim is false. Not exploitable in current single-coordinator-per-group tests, but the harness `recreateCoordinator()` path and multi-group tests rely on distinct groupIds to avoid collision, not on the counter.
Fix direction: incorporate `global` into the id, or delete the atomic and the misleading comment.

---

## CLEAN

### CL1. Header field-offset comment in OffsetMessages contains live "thinking-out-loud" text
`include/rune/coordination/OffsetMessages.h:291-297` — the block comment literally contains "Wait — re-count: … = 22 bytes". Correct value, but scratch commentary shipped in a header.

### CL2. `OffsetStore` "single-writer, no locks, no hot-path allocation" but every `commit` heap-allocates
`src/OffsetStore.cpp:194` — `std::vector<uint8_t> buf(totalLength)` allocates per commit; `applyRecord` also copies `std::string metadata`. The header (`OffsetStore.h:15`) says "no hot-path allocation beyond the file append." Contradicts the stated invariant (correctness is fine; it's a doc/design mismatch). A reusable scratch buffer would honor the contract.

### CL3. `autoAssignAndSync` duplicates the store-assignment loop already in `syncGroup`
`src/GroupCoordinator.cpp:376-385` vs `:330-338` — two independent copies of "store each member's assignment then go Stable." Divergence risk if one is fixed and the other isn't.

### CL4. `kVersion` in OffsetStore is defined but never written to or checked
`include/rune/coordination/OffsetStore.h:102` — `kVersion = 1u` is unused; the on-disk record carries no version field. Either add a version byte to the framing or drop the constant (silent forward-compat gap: a v2 format could not be distinguished from v1 except via magic).

### CL5. `applyAssignmentAndTransitionStable(const Assignment&)` ignores its argument
`src/GroupCoordinator.cpp:414-418` — parameter is unused (`/*a*/`); the function only sets state. Misleading signature implies it applies the assignment (it doesn't — the caller already did). Rename to `transitionToStable()`.

---

## TEST (vacuous / false-green / missing coverage)

### T1. No OffsetStore durability test destroys+reopens to prove **fsync ordering / partial-write** recovery beyond truncation
`tests/coordination/OffsetStoreTest.cpp:273-342` — the reopen tests correctly destroy the object and reopen (good, not vacuous). BUT torn-tail tests (`:368-429`) only `ftruncate` a cleanly-fsync'd file; none corrupts a **middle byte** of a complete record to prove the **CRC** actually rejects it (magic/length still match, only content differs). As written, the CRC path in `replayAndTruncate` (`:298-302`) is never exercised by a byte-flip — a broken CRC check would still pass every test. Add a test that flips one body byte of the last record and asserts it is dropped.

### T2. Assignor tests never assert exact partitions for a **partial-subscription RoundRobin** case
`tests/coordination/AssignorTest.cpp` RR_5 (`:412-430`) uses only 2 members where the non-subscriber simply gets nothing; it does not exercise the pointer-advance divergence in C2. The `Both_*` invariant tests assert only union/disjoint/±1-balance (`:521-664`) — never Kafka-exact placement for mixed subscriptions. This is exactly why C2 slips through. Add an RR test with ≥3 members and interleaved partial subscriptions asserting the **exact** per-member partition lists a real Kafka broker produces.

### T3. Rebalance "disjoint union" tests re-run the assignor by hand instead of reading coordinator state
`tests/coordination/RebalanceTest.cpp:137-155, 170-188` etc. — the leader assignment is computed in the test via `RangeAssignor ra; ra.assign(...)` and passed into `syncGroup`, then the test asserts on that same map. The coordinator's own storage is checked via `m.assignment`, which is fine — but several assertions (e.g. Test 2 `:145`) only check `errorCode == 0` and state, not that the **stored** assignment equals the computed one. `autoAssignAndSync` tests (Test 9, 21) do verify union/disjoint against coordinator state (good). Tighten the manual-sync tests to assert `assignmentFor(m) == computed[m]` for every member.

### T4. Fencing test C1 gap: no test sends a **stale-generation `leave()`**
`tests/coordination/RebalanceTest.cpp` / `GroupCoordinatorTest.cpp` — every fencing test targets `heartbeat` (`GroupCoordinatorTest.cpp:169-184`) and `syncGroup` (`RebalanceTest.cpp:277-304`). Because `leave()` takes no generation, there is literally no test that a zombie cannot force a rebalance — matching the C1 hole. A regression test should exist and would fail today only after C1 is fixed (so add both together).

### T5. Integration "no double-consume" assertions are tautological
`tests/integration/ConsumerGroupHarness.h:389-413`, used by ConsumerGroupIntegrationTest Tests 10/33 (`:312-317, 1096`) — `assertNoDoubleConsume` only fetches the committed offset and asserts `co->offset >= committed`, where `committed` is the value the test itself just committed. It re-reads the store and compares to what was written — it cannot detect an actual double-consume because no consumer read cursor is modeled. The comment even admits "The log is inside the harness; the assertion is that the committed offset exactly equals what consumer 2 wrote." This is a durability re-check mislabeled as a double-consume check. Either model a resume cursor and assert reads start at `committed`, or rename the helper to `assertCommittedOffsetDurable`.

---

## Notes on things that are CORRECT (verified, not bugs)
- OffsetStore commit is fsync'd **before** return and `writePos_` advances only after success (`OffsetStore.cpp:222-236`); torn-tail truncation restores a writable store (`:328-336`). Durability reopen tests genuinely destroy+reopen.
- CRC32C table init is idempotent; checksum covers `[0, totalLength-4)` on both write and verify.
- Session-timeout math is consistently ms→ns (`* 1'000'000LL`) with strict `>` (not `>=`), matching the before/after boundary tests (`GroupCoordinator.cpp:170-171`, tests `:216-236`). No ms/ns unit bug found.
- `doWork` idle returns 0 (no members, or Dead, or before-timeout) — no busy-spin, no sleep, clock sampled once per call (`:158-230`).
- Generation is monotonic (`++generationId_`) on every membership change including reap and Empty transition; `heartbeat` fences both lower and higher generations (`:139`).
- TopicPartition `==`/`<`/`std::hash` are consistent and mix both fields; no cross-key leakage found. SBE codecs round-trip byte-exact (little-endian asserted).
- RangeAssignor matches Kafka exactly on uneven splits and partial subscriptions (extras to the first `N%S` lexicographic subscribers) — verified against tests Range_1..12.

---

# Re-audit (commit b970401)

Date: 2026-09-11. Method: read current `src/*.cpp`, `include/**`, `tests/**` at commit `b970401`; independent trace of the RoundRobin algorithm against Kafka's `CircularIterator`-based `RoundRobinAssignor` (200k-case exhaustive differential simulation); rebuilt and ran the coordination suites. **Build clean; all 195 unit + 50 integration tests PASS.**

## Verdict: **CLEAN** (with one minor NEW cosmetic/design note, N1 — low severity, non-blocking)

## Per-finding status

### C1 — leave() fencing → **FIXED**
`GroupCoordinator::leave(memberId, generationId, nowNs)` (`src/GroupCoordinator.cpp:105-133`) now takes a `Generation` and no-ops on `generationId != generationId_` (`:111-113`) — no rebalance, no generation bump. Unknown member is a no-op via `removeMember` returning false (`:115-117`). A current-generation leave still shrinks the group / goes Empty (`:119-132`). Header updated (`GroupCoordinator.h:200`). All 20+ call sites (tests + integration harness) pass a generation; no production caller outside the lib. Tests `StaleGenerationLeave_IsNoOp` (`GroupCoordinatorTest.cpp:850`) and `CurrentGenerationLeave_ShrinksMembership` (`:875`) assert both directions (membership/state/generation unchanged on stale; shrink+bump on current).

### C2 — RoundRobin partial-subscription divergence → **FALSE-POSITIVE-CONFIRMED** (independent verdict)
I independently traced Kafka's canonical `RoundRobinAssignor`: `CircularIterator` over sorted members; per partition `while(!subscribed(peek(),topic)) next(); assign(next())`. Net cursor position after each assignment = (assigned-member index + 1). The code (`Assignor.cpp:170-190`) scans forward from `idx` via bounded `attempt`, and on assignment sets `idx = (candidate+1) % M` — **exactly** where Kafka's iterator lands. The intermediate non-subscriber skips are transparent because the next partition re-scans from `idx` and re-skips them; the persistent cursor is only ever moved to `assigned+1`, identical to Kafka's `while`-skip + `next()`. Verified by a differential simulator on the prompt's cases (m0⊆{t0,t1},m1⊆{t1}; 3-member heterogeneous with an interleaved non-subscriber sitting between two subscribers of the skipped topic) and by **200,000 random heterogeneous configurations (1-5 members, 1-4 topics, 0-4 partitions each) — zero mismatches**. The original C2 claim ("advances the pointer only on assignment ⇒ diverges") was mistaken: advancing only on assignment *is* Kafka's behaviour, because Kafka's skip-advances are relative and get re-derived each partition. **The finding was a false positive; the fixer's decision to keep `candidate+1` is correct.**

### C3 — syncGroup leader re-sync clobber → **FIXED**
`syncGroup` (`GroupCoordinator.cpp:392-400`): the leader-assignment branch stores + transitions **only** under `state_ == CompletingRebalance && !leaderSynced_`; in Stable (or after `leaderSynced_`) it returns `kOk` without re-writing assignments. Test `SyncGroupLeaderInStable_NoClobber` (`GroupCoordinatorTest.cpp:958`) submits a real assignment, reaches Stable, then re-submits a bogus single-member map and asserts every member's stored assignment is unchanged.

### C4 — follower stranded (no Stable transition) → **FIXED**
Follower syncing before the leader now returns `SyncResult::kErrRebalanceInProgress` (=4, `GroupCoordinator.h:102`) instead of a silent empty `kOk` (`GroupCoordinator.cpp:403-407`). Dead `syncedMembers_` is now cleared but no longer relied on for completion; the leader drives `CompletingRebalance→Stable` via `transitionToStable()`. Test 8 `FollowerSyncBeforeLeader_GetsRetrySignal` (`RebalanceTest.cpp:309-345`) asserts the full ordering completes: follower→retry, leader→kOk+Stable, follower retry→kOk. Note: `syncedMembers_` remains present but effectively vestigial (see N1-b); not a correctness issue.

### S3 — leader ordering across power-of-ten → **FIXED (for the tested id form)**
`electLeader` uses `memberIdLess` (`GroupCoordinator.cpp:301-350`) which extracts the trailing decimal run after the last `-` and compares numerically, falling back to lexicographic for non-numeric-suffixed ids. `LeaderElection_NumericallyCorrectAcrossPowerOfTen` (`:923`) verifies `test-group-2 < test-group-10` and the correct successor after removing the leader. Determinism preserved; comparator is a valid strict-weak order (verified antisymmetric). See N1-a for a residual on the *generated* two-number id form.

### S4 — memberId cross-instance uniqueness → **FIXED**
`generateMemberId` (`GroupCoordinator.cpp:246-259`) now embeds the process-global `s_memberSeq.fetch_add` as `<group>-g<global>-<local>`; the global counter is monotonic across all coordinator instances, so two coordinators with the same groupId cannot collide. Tests `GeneratedMemberIds_UniqueWithinInstance` (`:894`) and `..._UniqueAcrossInstances` (`:905`) confirm. Scheme cannot collide: `global` is a unique atomically-incremented value per mint.

### S1 — rejoinedMembers_ pruning → **FIXED**
`removeMember` (`GroupCoordinator.cpp:277-286`) now erases the id from `rejoinedMembers_`, keeping it a subset of live membership; removes the phantom `size() >= expected` risk.

### T1 — CRC byte-flip test → **FIXED (real)**
`commitThenFlipBodyByte` (`OffsetStoreTest.cpp:524-573`) walks the framing, finds the last record start, and XORs a byte at `recordStart+36` (first byte of the group-id field, i.e. **past** the 36-byte header holding magic/length/CRC) — magic and length stay intact, only the CRC-covered body changes. `CrcByteFlip_CorruptedLastRecordDropped` (`:575`) asserts records 0/1 survive and the corrupted record 2 is absent; `CrcByteFlip_StoreIsWritableAfterRecovery` (`:594`) asserts the store commits + fetches after recovery. Genuinely exercises the CRC drop path.

### T2 — exact partial-subscription RR test → **FIXED (distinguishing)**
`KafkaExact_ThreeMember_HeterogeneousSubscriptions` (`AssignorTest.cpp:667`) uses m0⊆{t0,t1}, m1⊆{t1}, m2⊆{t0,t1} (m1 is an interleaved non-subscriber of t0 sitting between t0-subscribers m0 and m2) and asserts exact lists m0→t0{0,2},t1{0,3}; m1→t1{1}; m2→t0{1,3},t1{2} — matching my independent Kafka reference simulation exactly. `KafkaExact_TwoMember_SingleSubscriberTopic` (`:705`) similarly asserts exact placement. These are the cases that would distinguish a broken cursor-advance from Kafka's; not cases where the two algorithms coincide.

### T4 — stale-generation leave test → **FIXED** (covered by C1's `StaleGenerationLeave_IsNoOp`).

### T5 — assertNoDoubleConsume tautology → **FIXED (real)**
The harness now models a read cursor `consumedUpTo_` set via `recordConsumed` (`ConsumerGroupHarness.h:398-407`), and `assertNoDoubleConsume` (`:420-444`) asserts both store durability (`committed <= stored`) **and** `committed >= consumedUpTo_[tp]` — i.e. a resuming consumer will not re-read already-consumed records. Both integration call sites (`ConsumerGroupIntegrationTest.cpp:304-320, 1089-1102`) call `recordConsumed` first, so the non-tautological cursor branch is actually exercised (not short-circuited by the empty-cursor early-return at `:434`).

### S2 / CL1-5 / T3 — **DEFERRAL-OK**
Not targeted by this fix commit; all were latent/style/doc-level in the original audit. CL3 was additionally addressed (both store-assignment loops now share `storeAssignments`, `Assignor`/`GroupCoordinator.cpp:416-424,454`); CL5 addressed (`applyAssignmentAndTransitionStable` renamed to `transitionToStable`, `:484-488`).

## Regression checks
- **leave() generation param:** no legitimate flow broken. Every call site passes `generationId()`; a current-gen leave still shrinks/goes-Empty. No production caller outside the lib. PASS.
- **sync signal change:** no follower stranded. Test 8 proves the follower reaches Stable after retry; leader still transitions on submit. `kErrRebalanceInProgress` is a new distinct code, not conflated with existing ones. PASS.
- **memberId scheme change:** exact-match lookup (`findMember`) uses string `==`, unaffected. Leader election determinism preserved (comparator antisymmetric; same set → same leader). PASS.

## NEW findings

### N1-a (CLEAN / low severity). Residual power-of-ten flip on the GLOBAL-seq field of generated ids
`GroupCoordinator.cpp:323-329` — `memberIdLess` compares numerically **only the trailing decimal run** (the `<local>` field) and compares the *prefix* (`<group>-g<global>`) **lexicographically**. For generated ids `<group>-g<global>-<local>`, this means the global-seq component is still ordered lexicographically: e.g. `grp-g10-2` sorts before `grp-g2-3` (`g10` < `g2` lexically). So across a power-of-ten boundary of the *global* counter, "leader = smallest id" again stops being "earliest-minted." Determinism is fully preserved (valid strict-weak order; same set → same leader), so this is not a correctness bug — but it re-introduces exactly the S3 surprise the fix set out to remove, one level up, and the S3 regression test only covers single-suffix hand-supplied ids (`test-group-2` vs `test-group-10`), not the generated two-number form. Fix direction (optional): parse *all* `-`-separated numeric fields numerically (compare `(global, local)` as a tuple), or zero-pad, or elect by explicit join order. Low severity: leader identity is arbitrary-but-deterministic and Kafka does not require earliest-joiner leadership.

### N1-b (CLEAN / trivial). `syncedMembers_` is now dead state
`GroupCoordinator.cpp:359,477` — `syncedMembers_` is cleared in `triggerRebalance`/`transitionToCompletingRebalance` but never inserted into or read; completion is driven solely by `leaderSynced_`. Harmless, but should be removed (the original C4 note flagged it; the fix chose the `leaderSynced_` gate but left the field). Cosmetic.

**Bottom line:** All eight targeted findings are resolved or confirmed false-positive; the RoundRobin false-positive claim is upheld by independent trace + exhaustive differential test. Only two low-severity cosmetic notes remain (N1-a residual leader-ordering surprise on generated global-seq; N1-b dead field). Coordination is CLEAN.

---

# Re-audit 2 (commit 991670f)

Date: 2026-09-11. Scope: focused confirmation of the two residuals N1-a and N1-b from the previous re-audit. Method: read current `src/GroupCoordinator.cpp` + header + tests at `991670f` (HEAD); `grep` for all `syncedMembers_`/`leaderSynced_` references; extracted the exact `memberIdLess` (new + old) into a standalone harness and ran an exhaustive strict-weak-ordering check (irreflexivity, antisymmetry, transitivity of `<` and of equivalence) plus a distinct-id-collision check under ASan+UBSan. (Note: `cmake` is unavailable in this environment, and the checked-in `build/` binary predates the commit, so the full gtest suite was not re-run here; the comparator was validated in isolation against copied-verbatim source.)

## Verdict: **CLEAN** (libs/coordination fully clean — zero remaining findings)

### N1-a — numeric tuple ordering across ALL fields → **FIXED**
`memberIdLess` (`src/GroupCoordinator.cpp:306-352`) is now a natural-order comparator: it walks both ids run-by-run, comparing digit runs numerically (leading zeros skipped, then by significant-digit count `:329`, then char-order `:330-332`, then raw-run length `:335-336`) and non-digit runs lexicographically (`:339-342`), with digit-runs sorting before non-digit runs at a mismatch (`:343-346`) and the shorter id first on a shared prefix (`:351`). This orders both the `<global>` and `<local>` fields of `<group>-g<global>-<local>` numerically, so `grp-g2-3 < grp-g10-2` (independently confirmed = `true`; the old trailing-field-only comparator returns `false` here and would elect `grp-g10-2`). **Valid strict-weak ordering:** 0 violations over a corpus spanning power-of-ten boundaries, leading zeros (`00`, `007`, `g010`), empty string, and non-conforming ids (`member-a`, `abc`) — irreflexive, antisymmetric, transitive (both `<` and `~`); `std::sort` runs clean under ASan+UBSan with no crash/UB on non-conforming ids; no two distinct generated ids compare equivalent (leader determinism preserved). The new test `LeaderElection_NumericAcrossGlobalFieldPowerOfTen` (`tests/coordination/GroupCoordinatorTest.cpp:963-996`) spans g2 vs g10/g11 in the GLOBAL field and asserts `grp-g2-3` leads, with a same-global local-field tie (g5-2 vs g5-10) — independently confirmed to **FAIL under the old comparator** and pass under the new one. No new determinism bug.

### N1-b — dead `syncedMembers_` removed → **FIXED**
Field and both former clear-sites are gone: header declaration removed (only a historical mention survives in the `leaderSynced_` doc comment at `GroupCoordinator.h:344`), and the two `syncedMembers_.clear()` calls in `triggerRebalance`/`transitionToCompletingRebalance` are deleted. `grep -rn syncedMembers_ libs/coordination` returns only that comment — no live references. `<unordered_set>` is still legitimately included (used by `rejoinedMembers_`). The `CompletingRebalance→Stable` completion path is unchanged and driven solely by `leaderSynced_`: set in the leader branch (`src/GroupCoordinator.cpp:412`) and `autoAssignAndSync` (`:472`), reset in `triggerRebalance` (`:377`) and `transitionToCompletingRebalance` (`:494`), gating both the leader clobber-guard (`:410`) and the follower retry signal (`:420`). Rebalance/sync tests unaffected.

### Regression checks (previously-fixed items)
This commit touches ONLY `memberIdLess` and the `syncedMembers_` removal (verified via `git show 991670f`); no other logic changed. Confirmed intact at HEAD:
- **C1 leave() fencing** (`:105-133`): stale/future generation is a no-op, unknown member no-op, current-gen leave shrinks/goes-Empty. Untouched.
- **C3 syncGroup Stable-guard** (`:409-417`): leader assignment stored only under `CompletingRebalance && !leaderSynced_`; Stable re-sync is a no-op ack. Untouched.
- **C4 follower retry** (`:420-423`): returns `kErrRebalanceInProgress` when leader hasn't synced. Untouched.
- **S4 memberId uniqueness:** id FORMAT is unchanged (still `<group>-g<global>-<local>`); only the comparator changed. `findMember` uses string `==`, so lookups/uniqueness are unaffected — confirmed no distinct-id equivalence collision. A memberId order change did not break uniqueness or lookups.
- **T1 CRC byte-flip test / T5 double-consume cursor:** in unrelated files (`OffsetStoreTest.cpp`, `ConsumerGroupHarness.h`), not in this commit's diff. Unaffected.

**Bottom line:** N1-a and N1-b both FIXED with a correct, distinguishing test for N1-a; no regression to any previously-fixed item. Zero remaining findings. **libs/coordination is CLEAN — section closed.**

---

# Full re-audit + coverage (HEAD)

Date: 2026-09-11. Scope: complete from-scratch, whole-library re-review of `libs/coordination/**` at HEAD (`21a0c86`; last coordination-touching commit `991670f`). Fresh independent read of every class — Types, OffsetStore, OffsetMessages (SBE), GroupCoordinator (state machine / generation / heartbeat / doWork / join-leave-sync), Assignor (Range/RoundRobin), rebalance, integration harness — plus an edge-case test-coverage assessment. Not a fix-check; treated the whole library as new. Prior findings C1/C3/C4/S1/S3/S4/T1/T2/T4/T5/N1-a/N1-b re-verified as still holding. (`cmake`/`ctest` unavailable; reasoned from source. C2 RoundRobin re-traced and re-confirmed Kafka-faithful — see prior re-audit.)

## Part A verdict: **BUGS REMAIN** (1 CORRECTNESS + 2 SILENT/low; no BLOCKER)

### A1. (CORRECTNESS, medium — latent) Existing-member re-join is inert outside PreparingRebalance — Stable/CompletingRebalance re-join does NOT trigger a rebalance
`src/GroupCoordinator.cpp:59-72`. In `join()`, only the **new-member** branch (`:73-88`) calls `triggerRebalance`. When an **existing** member re-joins, the code refreshes `sessionTimeoutMs`/`subscribedTopics`/`lastHeartbeatNs` and then only reacts **if `state_ == PreparingRebalance`** (`:66`). Consequences:
  - **Stable + existing-member re-join** (the standard Kafka "rejoin to change subscription / request new assignment" flow): silently absorbed. New `subscribedTopics` are stored on the MemberInfo but **no generation bump, no rebalance, no reassignment** happens — the group stays Stable with a stale assignment that ignores the changed subscription. This directly contradicts the coordinator's own documented state table (`GroupCoordinator.h:24`: "Stable + join/leave → PreparingRebalance") and Kafka semantics. Only a *new* member re-triggers (tested by RebalanceTest Test 19 / `NewMemberWhileStable`); the existing-member case is untested and broken.
  - **CompletingRebalance + existing-member re-join**: also inert (guard is PreparingRebalance-only), so a member cannot force the group back to PreparingRebalance by re-joining mid-sync.
Latent today only because every test/harness re-join happens *during* PreparingRebalance (harness phase-2 rejoin immediately follows the phase-1 join that put the group in Preparing; `ConsumerGroupHarness.h:201-212`). Fix direction: when an existing member re-joins while `state_ ∈ {Stable, CompletingRebalance}` (or when its `subscribedTopics` changed), call `triggerRebalance(nowNs)` + `electLeader()` exactly as the new-member branch does, then insert into `rejoinedMembers_`. Add a `Stable + existing-member-rejoin → PreparingRebalance` test and a subscription-change-triggers-rebalance test.

### A2. (SILENT, low) Multiple simultaneous session-timeout reaps bump generation once *per evicted member*, not once per rebalance
`src/GroupCoordinator.cpp:186-201`. The `toEvict` loop calls `triggerRebalance` (→ `++generationId_`) **and** `electLeader` for **each** reaped member. If N members expire in the same `doWork` tick, the generation advances by N and the group re-prepares N times in one call. Monotonicity (the only hard invariant, fenced by heartbeat/sync) is preserved, so this is not a correctness bug, but it is a Kafka-fidelity deviation (Kafka collapses a batch of expirations into a single rebalance/generation bump) and inflates the epoch. Same shape is avoided in the join-timeout branch (`:221-235`), which evicts all non-rejoined members first and transitions **once**. Fix direction: collect all expired ids, `removeMember` them all, then do a single Empty-or-`triggerRebalance`+`electLeader`.

### A3. (SILENT, low / robustness) `replayAndTruncate` trusts `totalLength` without cross-checking the body-length fields
`src/OffsetStore.cpp:278-316`. The only length sanity check is `totalLength >= kFixedHeaderSize + kChecksumSize` (`:281`). It never verifies `kFixedHeaderSize + groupLen + topicLen + metaLen + kChecksumSize == totalLength` before slicing the body (`:314-316`). For genuinely-written records the CRC (`:298-302`) catches any inconsistency, so this is safe against the writer's own output and against random tail corruption. But a record whose header/length fields were adversarially crafted to keep a valid CRC yet have `groupLen+topicLen+metaLen` overrun `totalLength` would read past the record body within `recBuf` (still inside the heap buffer only if the sum ≤ `totalLength-header-4`; a larger sum would over-read the `std::string` constructions). Defense-in-depth: assert the body-length identity before constructing the three strings and `break` (torn) on mismatch. Low severity (CRC makes it non-exploitable for non-malicious data), flagged as hardening.

**Everything else re-verified CORRECT (fresh trace):** OffsetStore commit fsync-before-return + `writePos_`-after-success; CRC covers `[0,totalLength-4)` on write and replay; torn-tail truncation restores a writable store; overwrite-with-lower-offset allowed (Kafka). SBE codecs round-trip byte-exact, LE static_assert, `BLOCK_LENGTH`s match field sums (12/6/4/22). `TopicPartition` ==/</hash mix both fields consistently. `leave()` fences stale/future generation and unknown member (C1 holds). `heartbeat` fences unknown + stale + rebalance-in-progress; session-timeout math ms→ns with strict `>` (`:181`). `syncGroup` Stable clobber-guard (`:409-417`) + follower retry signal (`:420-423`) hold. `memberIdLess` is a valid natural-order strict-weak ordering (N1-a); `syncedMembers_` gone (N1-b). RangeAssignor Kafka-exact; RoundRobin cursor-advance = Kafka (C2 confirmed false-positive). `doWork` idle-returns-0, samples clock once, no spin. `removeMember` prunes `rejoinedMembers_` (S1 holds).

## Part B — edge-case test-coverage gaps (12 missing)

Well-covered already: offset overwrite-with-lower-value (`OffsetStoreTest OverwriteWithLowerOffsetAllowed`), fetchAll unknown group (`FetchAllEmptyForUnknownGroup`), mid-record CRC byte-flip (`CrcByteFlip_*`), torn tail + entire-file truncation, single-member rebalance (`SingleMemberGetsAllPartitions`), all-members-leave→Empty (`StateEmptyAfterAllLeave`), heartbeat exact-timeout boundary (before/after helpers), assignor 0-partition / empty-counts / zero-partition-topic, RR exact heterogeneous/partial-subscription. **Still missing:**

1. **[A1] Stable + existing-member re-join** — assert it transitions to PreparingRebalance + bumps generation (currently proves nothing; would FAIL and expose A1). **TOP.**
2. **[A1] Subscription-change on re-join triggers reassignment** — existing member re-joins Stable with a *different* `subscribedTopics`; assert a rebalance + new assignment reflecting the change. **TOP.**
3. **[A2] Two members expire in the SAME doWork tick** — assert generation bumps by exactly 1 (not 2) and one CompletingRebalance/Empty transition. **TOP.**
4. **Torn-tail at EACH field boundary** — truncate at every kFixedHeaderSize sub-field offset (after magic, after totalLength, mid-commitTs, mid-offset, mid-partition, mid-lengths, mid-body-of-each-string) and assert clean recovery to the prior record. Current tests only cut mid-body of the last record.
5. **[A3] Body-length-vs-totalLength inconsistency** — hand-craft a record with valid magic + valid CRC but `groupLen+topicLen+metaLen != totalLength-header-4`; assert it is rejected/truncated rather than mis-decoded.
6. **fetch() (not fetchAll) for unknown group AND known-group/unknown-tp** — assert `nullopt` on both misses (fetchAll-unknown is covered; single-fetch miss paths are not explicitly asserted).
7. **Join during CompletingRebalance** — both a NEW member (should re-Prepare) and an EXISTING member (currently inert per A1) while state == CompletingRebalance; assert the state/generation outcome.
8. **Empty-group operations** — `syncGroup`/`heartbeat`/`leave`/`doWork` on a group with zero members: assert kErrWrongState/kErrUnknownMember/no-op, no crash, generation unchanged.
9. **Assignor: more members than partitions** — e.g. 5 members, 2 partitions, one topic; assert exactly 2 members get 1 partition each and the other 3 get empty lists (Range and RoundRobin), Kafka-exact placement (only "balanced-within-one" with the reverse ratio exists).
10. **Assignor: member subscribing to NO known topic** — member whose `subscribedTopics` is empty or all-unknown; assert it appears as a key with an empty list and never receives a partition (partially implied by UnknownTopicIgnored, not asserted for a truly no-topic member alongside real subscribers).
11. **Generation wrap / large-generation churn** — drive generation near/over INT32 via many rebalances (or seed) and assert monotonic fencing still holds (no signed overflow UB assumption); at minimum document the epoch is int32 and wrap is out of scope.
12. **Concurrent join + timeout in one doWork window** — member A about to expire while member B joins in the interleaving; assert no double-eviction, `expectedMemberCount_`/`rejoinedMembers_` stay consistent, exactly one coherent transition.

## Bottom line
Whole-library re-audit at HEAD is **not fully clean**: one real (latent) correctness divergence (A1: existing-member re-join outside PreparingRebalance never rebalances — contradicts the coordinator's own state table), plus two low-severity items (A2 per-member generation inflation on batch reap; A3 offset-record body-length hardening). Durable OffsetStore, SBE codecs, Range/RoundRobin, leave/heartbeat/sync fencing, and leader determinism all re-verified correct. Test suite is strong but 12 niche edge cases remain unexercised — the top three (A1 Stable-rejoin, A1 subscription-change, A2 batch-reap generation) also serve as regression tests for the Part A findings.
