# Batch C19 — producer / claim / crypto follow-ups (issues #692 #693 #689 #656 #655 #657 #675 #674 #673)

Repo the repo root, branch claude/kind-bardeen-ze6yl0 (checked out — do NOT switch, commit, push, stash, or run
git checkout/restore; the coordinator commits). Read CLAUDE.md and client-cpp/CLAUDE.md first.
Never spawn subagents.

Rules:
- Build named targets only, under the lock: `flock build-client/.build.lock ninja -C build-client <targets>`.
  Find each test file's target in client-cpp/tests/CMakeLists.txt. Never a whole-tree build, never a new build dir,
  never reconfigure. The disk is nearly full: create no large files; delete any scratch binary you make.
  If a build fails in a file you did not touch, wait (a capped polling loop) and retry: another agent edits the tree.
- Failing test FIRST (paste the RED line), then the fix, then GREEN. New test names end in `Regression`.
- Every new assertion gets a mutation check (revert the fix line(s) → RED, restore). Leave no marker behind.
- Injected clocks only, no sleeps, capped drive loops.
  `python3 client-cpp/scripts/check_no_atomics_or_mutexes.py --tests client-cpp` must print OK.
- Hot paths stay zero-alloc and branch-light. Short comments citing the issue (`// #655: ...`). No secrets, local paths
  or model names in code or comments.

---------------------------------------------------------------------------------------------------------------------
## Part A — #692: ProducerRef acked claimBatch overloads; abandonOpenClaimForClose callers

1. client-cpp/include/rune/client/producer/ProducerRef.h: next to the plain `claimBatch`, add the two forwarding
   overloads that Producer.h (~749-753) declares, same signatures, `const noexcept`, each calling `check_()` first:
   `claimBatch(topic, key, n, recLen, Completions& cq, Token token)` and `claimBatch(topic, key, n, recLen, AckToken token)`.
   Add any include ProducerRef.h then needs (Completions / AckToken) — check what Producer.h already pulls in.
2. ProducerImpl.cpp `abandonOpenClaimForClose()` (~4002): the in-function guard IS the check (Direct mode and the
   owner thread, else a no-op). Replace the comment with one that lists every caller and why each is legal:
   Client::checkDrain_ (Client.cpp ~5310, the Client's close drain - may run off the owner thread, then a no-op),
   Producer.cpp ~658 (the owner's blocking close, after an accepted beginClose), ProducerTxn.cpp ~334/360/371/1216
   (the owner / duty thread ending a transaction). Grep for every caller and list exactly what you find.
Test (client-cpp/tests/producer/ProducerClaimTest.cpp, next to `AckedClaimBatchCompletesWithItsBaseOffsetRegression`):
`ProducerClaimRegression.ProducerRefForwardsTheAckedClaimBatchOverloadsRegression`: same setup as that test, but make
both acked claims through a `ProducerRef` (one with cq+token, one with an AckToken + onAck handler); commit, ack;
assert each completes exactly once with the batch's base offset. RED = it does not compile before step 1 (paste the
compiler error line as the RED evidence). Mutation: make the cq overload forward to the UNACKED claimBatch → RED
(no completion).

---------------------------------------------------------------------------------------------------------------------
## Part B — #693: enableCoordinator reports its refusal

ProducerImpl::enableCoordinator (ProducerImpl.cpp ~4053, decl ProducerImpl.h ~1294) becomes
`[[nodiscard]] bool enableCoordinator(CoordinatorSink sink, uint32_t seqBase = 0);`:
- with in-flight batches: keep the stderr line, REMOVE the `assert(false ...)`, return false (nothing wired);
- otherwise wire as today and return true.
Callers: ProducerHub.cpp ~50 (a freshly added impl, never in flight): `if (!impl->enableCoordinator(...))` →
`assert(false && ...)` there is fine (it is a broken internal invariant in the hub, not a caller-visible case), and
grep tests for every other call (`enableCoordinator(`) and handle the result (`ASSERT_TRUE(...)` where it must succeed).
Test (tests/producer/ProducerIdTest.cpp): rewrite `ProducerIdRegression.EnableCoordinatorWithInFlightBatchesNeverReStampsThemRegression`
(~528): drop the `#if GTEST_HAS_DEATH_TEST` wrapper and the `EXPECT_DEBUG_DEATH`; call
`EXPECT_FALSE(r.impl->enableCoordinator(r.fake.sink(), 0)) << "refused: batches in flight";` and keep every assertion
after it unchanged (they now run in debug builds too). Update the comment above the test. Add a positive leg in the
same file: `ProducerIdRegression.EnableCoordinatorBeforeTheFirstSendReportsSuccessRegression` (a fresh rig, nothing
sent: `EXPECT_TRUE(...)` and `hasCoordinator()` true). RED: the first test does not compile / dies today. Mutation:
return true from the refusal branch → RED.

---------------------------------------------------------------------------------------------------------------------
## Part C — #689: records held in a partition's builder fail as soon as the broker refuses the partition

Today a case-D error answer (ProducerImpl.cpp, comment block ~2420-2470 and `onProduceResponse`) fails the answered
batch only; records still lingering in that partition's builder wait for linger / flush / the delivery timeout and are
then sent (and refused) again. Decision (recorded here): a PARTITION-REFUSAL code means every later batch to that
partition is refused the same way, so the builder's records fail at once with the same code. The partition-refusal
codes are exactly: UnknownTopic (7), UnknownPartition (8), NotAuthorized (45), NamespaceSuspended (51)
(protocol/include/rune/protocol/client/Protocol.h). (46/44 Rejected sessions are already covered by
failRoutedBroker, which calls failBuilder_.)
Fix: in the case-D branch of onProduceResponse, after the answered batch is failed, if the code is one of the four,
call `failBuilder_(ps, OpStatus::BrokerError, code)` for that partition (only that partition). Put the code set in a
small `constexpr bool isPartitionRefusal_(uint16_t)` helper with a `// #689` comment. Do not touch retriable codes,
OOS/UnknownProducerId logic, or in-flight batches behind the answered one (they are answered on their own).
Read failBuilder_ (~2992) first: it abandons an open Direct claim on that partition — that is the existing behaviour
for a failed partition and stays.
Test (tests/producer — the producer-rig file that already tests case-D answers; grep `onProduceResponse(` with an
error code in tests/producer to find it): `ProducerErrorRegression.ARefusedPartitionFailsItsLingeringRecordsAtOnceRegression`
(use the file's existing suite name if it has one): acks=Leader, lingerUs huge (60 s), deliveryTimeout huge; send A,
flush, tick (A in flight); send B and C (lingering in the builder, NOT flushed); answer A with NotAuthorized (45);
tick once with the clock NOT advanced: assert A, B and C each completed exactly once with BrokerError/45, and nothing
new was sent. Second leg in the same test or a sibling: answer with a retriable code (MinIsrViolation 10): B and C are
NOT completed. Mutation: remove the failBuilder_ call → RED.

---------------------------------------------------------------------------------------------------------------------
## Part D — #655: an Invoker-mode BatchBuilder destroyed off its batch thread returns its slot

Facts: `ProducerImpl::tier2DiscardOffThread` (~760) writes `kMsgTier2Discard` to the producer ring only in Thread mode;
Direct mode logs and loses the slot. Direct mode HAS a ring (allocated in the constructor, ~298) but `drainRing`
returns 0 at once in Direct mode (~1468). The Thread-mode drain applies the message with `t2Return_(slot, 2)` (~1589).
Fix:
1. tier2DiscardOffThread: write to the ring in BOTH modes; the stderr line stays only for a failed write (ring full
   or closed).
2. drainRing, Direct mode: instead of `return 0`, call a new `int drainDirectRing_(int messageLimit) noexcept` that
   returns 0 at once when the ring is empty (one head/tail compare - check what cheap emptiness test the ring class
   offers; `ring_.headPosition() == ring_.tailPosition()` is fine), else `ring_.read(...)` and applies
   kMsgTier2Discard (`t2Return_`, same as the Thread path) and the new type from Part E; any other type in a
   Direct ring is a bug: count it in the existing foreign-message counter (grep `ringForeignMessages`) and skip.
   Comment: `// #655/#656: a Direct producer's ring carries only off-owner settle messages`.
3. Update the client-cpp/CLAUDE.md §2.1 sentence "Direct (Invoker) mode loses the slot and logs an error" to say
   the slot comes back on the owner's next duty pass (only a full or closed ring loses it, logged).
Test (tests/producer/ProducerTier2PoolTest.cpp, next to `Tier2SlotRegression.BuilderDestroyedOffTheBatchThreadReturnsItsSlotRegression`):
`Tier2SlotRegression.InvokerBuilderDestroyedOffTheBatchThreadReturnsItsSlotRegression`: Direct-mode producer (see how
the file builds one, or ProducerClaimTest's Direct rig), `batch()` + `add()` one record on the owner thread, read
`tier2FreeCount()` = F, move the builder into a `std::thread` and destroy it there, join; drive one duty pass on the
owner; assert `tier2FreeCount() == F + 1` (the full pool) and the foreign counter == 0. Mutation: restore the
Direct-mode `return 0` in drainRing → RED.

---------------------------------------------------------------------------------------------------------------------
## Part E — #656: an Invoker claim settled off its owner thread no longer wedges the producer

Facts: `ProducerImpl::abortClaimOffOwner` (~3847) logs and returns in Direct mode, so the DirectClaimOpen state stays and
every later send returns ClaimHeld until close. `detail::ClaimCredit::abort` (src/producer/Claim.cpp ~37) calls it
whenever `mayCommit` is false (off the impl's owner thread, OR the cq/bq owner differs).
Fix:
1. New ring type in ProducerImpl.h next to kMsgClaimCreditLost: `inline constexpr int32_t kMsgDirectClaimAbort = 8;`
   (`// #656: an Invoker claim aborted / destroyed off its owner thread: the owner rolls it back`), payload = the
   claim's gen (8 bytes LE, the `token`).
2. abortClaimOffOwner, Direct mode:
   - on the owner thread (`std::this_thread::get_id() == dutyThreadId_`; reachable when only the cq/bq owner differs):
     `abortDirectClaim_(gen)` and `noteClaimCreditLost_()` (the queue credit cannot be returned off its owner —
     same accounting as Thread mode's kMsgClaimCreditLost), return;
   - off the owner thread: `writeToRing(kMsgDirectClaimAbort, tok, 8)`; on a failed write keep today's stderr line.
3. drainDirectRing_ (Part D) applies kMsgDirectClaimAbort: `abortDirectClaim_(gen)` (its gen check makes a stale
   message a no-op, and settles an abandoned one) then `noteClaimCreditLost_()`.
   The ring's release/acquire orders the off-thread writer's span writes before the owner's rollback.
4. Thread-mode refusal passes (`refuseRingTxn_`, ~3306 and ~3358/3380): type 8 never appears in a Thread ring; add it
   to nothing there.
5. Off-owner `commit()` keeps returning false with the claim left open (unchanged) — the Claim's later destructor takes
   the abort path above.
Tests (tests/client/ClaimClientTest.cpp): update `ClaimClientRegression.InvokerClaimDestroyedOffOwnerIsHeldUntilCloseAdversarialRegression`
- it asserts the OLD wedge. Rename it to `ClaimClientRegression.InvokerClaimDestroyedOffOwnerIsReleasedOnTheNextDutyPassAdversarialRegression`
and change its expectation: after the off-thread destroy, ONE owner duty pass (`doWork()`), then `trySend` returns Ok
(not ClaimHeld), the claimed bytes were never sent (assert on the gateway's produced records), and the producer's
claim-credit-lost counter (grep the accessor: `claimCreditLost` / stats) is 1. Keep its other assertions that still
hold. Check `ProducerClaimRegression.DirectClaimSettledOffTheOwnerThreadNeverTouchesOwnerStateRegression` too: it must
still pass as is (the off-thread call itself touches no owner state); if it asserts the producer stays held AFTER a
duty pass, update that assertion the same way and say so in the report.
Add `ClaimClientRegression.InvokerBatchClaimDestroyedOffOwnerIsReleasedRegression` (same with `claimBatch`).
Mutation: drop the kMsgDirectClaimAbort branch in drainDirectRing_ → RED.

Benchmark (rule 6 — Part D/E add one ring-empty check per Direct producer per duty pass): run the Invoker-mode
produce bench (client-cpp/bench/ClientProduceBench.cpp or ProducerClaimBench.cpp — whichever has an Invoker mode; find
the bench target name in client-cpp/bench/CMakeLists.txt, and only if it is already configured in build-client; if no
bench target exists in build-client, say so and skip — do NOT reconfigure). ABAB n=3 baseline (the change stashed by
copying the two edited lines back temporarily, NOT git stash) vs change; report p50/p99.

---------------------------------------------------------------------------------------------------------------------
## Part F — #657: test for refuseRingTxn_ handling kMsgTier2Discard

Test only, in tests/producer/ProducerTxnDataTest.cpp next to
`ProducerClaimRegression.OffThreadCreditLostMessageIsSkippedByTheTxnRefusalPassRegression` (copy its rig):
`ProducerTxnDataRegression.OffThreadTier2DiscardIsAppliedByTheTxnRefusalPassRegression`: transactional Thread-mode
producer OUTSIDE a transaction (intake shut); a slot-holding BatchBuilder destroyed off the batch thread (std::thread +
join); drive the duty side; assert `tier2FreeCount()` is back to the full pool and `ringForeignMessages() == 0`.
Mutation: in refuseRingTxn_, skip the kMsgTier2Discard branch (treat it as foreign) → RED. Restore.

---------------------------------------------------------------------------------------------------------------------
## Part G — #673: a receiver key that never derives is logged, counted and not retried per offer

client-cpp/include/rune/client/codec/CryptoFrame.h `selectSendSlot` (~132) and CryptoState
(client-cpp/include/rune/client/session/SessionMachine.h ~273):
1. `ReceiverSend` gets `bool deriveFailed = false;`. CryptoState gets `uint32_t receiverDeriveFailures = 0;`
   (`// #673: receiver keys that failed to derive this episode (each counted once)`).
2. selectSendSlot: `if (slot.deriveFailed) return {};` before the derive; on a failed derive set
   `slot.deriveFailed = true`, `++crypto.receiverDeriveFailures`, write ONE stderr line
   (`rune client: receiver key %u failed to derive; frames for it are held until the session reconnects`) and return {}.
   Each receiver fails at most once per episode, so the log is naturally bounded.
3. Confirm how CryptoState is reset per episode (grep `CryptoState{}` / the episode reset in SessionMachine): the new
   fields must reset with it. Report what you find.
Test (tests/crypto/ClientCryptoRegressionTest.cpp, near the tests using `deriveFailsForReceiverThree` ~1609/1682):
`ClientCryptoRegression.AFailedReceiverDeriveIsCountedOnceAndNeverRetriedRegression`: install a derive fn that fails
for receiver 3 AND counts its calls (a file-static counter); call sealFrame / selectSendSlot for receiver 3 five times:
every call returns 0 / null cipher; the derive fn ran exactly once; `receiverDeriveFailures == 1`; receiver 2 still
seals. Then reset the episode the way the file's neighbour test does and assert the derive runs again (count 2).
Mutation: remove the `deriveFailed` early return → RED (5 calls).

---------------------------------------------------------------------------------------------------------------------
## Part H — #675: C10 review minors (all four)

1. CryptoState::deriveReceiverKeyFn becomes private: rename to `deriveFn_` under `private:` at the END of CryptoState
   with its default; add a public `[[nodiscard]] bool deriveReceiverKeyFor(uint8_t receiverId, rune::crypto::AeadKey& out) const noexcept
   { return deriveFn_(c2bKey, receiverId, out); }` used by selectSendSlot, and `friend struct CryptoStateTestAccess;`.
   Define `struct CryptoStateTestAccess { static void setDeriveFn(CryptoState& c, decltype(...) fn) noexcept { c.deriveFn_ = fn; } };`
   in a NEW test header client-cpp/tests/support/CryptoStateTestAccess.h (only tests include it). Update the four test
   uses (ClientCryptoRegressionTest.cpp ~1609/1630/1682/1695, and any other grep hit). Check nothing aggregate-
   initialises CryptoState with braces listing fields (a private member makes it a non-aggregate); if something does,
   report it before changing approach.
2. ProduceFrameBuilder::rollback (include/rune/client/producer/ProduceFrameBuilder.h ~131): replace the debug-only
   assert with a release check: `if (m.size > frame_.size()) [[unlikely]] { assert(false && ...); return; }`
   — a stale (larger) mark is refused (no change). Keep `noexcept`. Test in tests/producer/ProduceFrameBuilderTest.cpp:
   `ProduceFrameBuilderRegression.RollbackToALargerMarkIsRefusedRegression` — guarded by `#ifdef NDEBUG` (in debug the
   assert fires; then use `EXPECT_DEBUG_DEATH` instead so it runs in both builds: take a mark after 3 records, rollback
   to an earlier mark, then try rollback to the later (now larger) mark: in release, size and recordCount unchanged).
   Mutation: drop the return → RED in release (and the death test RED in debug when the assert is removed).
3. ProduceFrameBuilderTest.cpp "r3" comment that claims offsetDelta is decoded: ADD an assertion that decodes each
   record's offsetDelta and checks it (0, 1, 2, ...), using the decoding the test already does for maxTimestamp. Do not
   change the 128 MiB case.
4. tests/support/OpenSslAllocCounter.cpp `countingRealloc`: for n == 0 free p and return nullptr (libc/OpenSSL
   semantics) — `if (n == 0) { std::free(p); return nullptr; }`. No new test (test support); run the tests that use
   the counter (grep `openSslAllocCounterInstalled`).

---------------------------------------------------------------------------------------------------------------------
## Part I — #674: AckedClaimsHeldAcrossClientCloseCompleteOrFailRegression on an injected clock

tests/client/ClaimClientTest.cpp ~581 `ackedClaimsHeldAcrossClientClose`: `Client::close()` blocks until the drain
settles, bounded by the 200 ms delivery timeout measured with the Client's clock. Give the rig's Client config a
`nanoClock` that STEPS: a file-static `int64_t` advanced by 1 ms on every call (deterministic; the blocking close
advances it by calling it). Check `cfgFor(...)` / the Rig for how cfg.nanoClock is set elsewhere in the file.
Thread mode: the clock may be called from the runner thread AND the app thread — verify by recording the first
caller's thread id in the clock function and counting calls from any other thread. If more than one thread calls it,
do NOT add an atomic (the ratchet forbids it): keep the Thread leg on the real clock, convert only the two Invoker legs,
and report it. Keep every assertion. Run the test under `-j8 --repeat until-fail:20` and paste the summary and the
test's wall time before/after.

---------------------------------------------------------------------------------------------------------------------
## Run before reporting
Build and run every executable holding a test you touched or added (ProducerClaimTest, ProducerIdTest, the Part C
file, ProducerTier2PoolTest, ClaimClientTest, ProducerTxnDataTest, ClientCryptoRegressionTest, ProduceFrameBuilderTest,
the OpenSSL-counter users), all their tests. Then `ctest -R "Producer|Claim|Crypto|Tier2" -j6` in build-client.
Ratchet OK.

## Report
Files changed; each test with RED and GREEN lines; each mutation result; every existing assertion you changed and why;
the Part G episode-reset finding; the Part I thread finding; the bench numbers; deviations with reasons.

## Shared tree and disk (read this - incidents happened)
- Other agents edit this tree at the same time. For a mutation check make the smallest possible edit, run, then undo
  exactly that edit (Edit tool, old/new swapped). NEVER save a whole-file copy and restore it over the live file - that
  erased another agent's work once. Before reporting, grep your mutation marker / `if (false &&` and confirm none left.
- The disk is nearly full. Build with `TMPDIR=/dev/shm/<yourname>` and `-j2`; delete that dir at the end. Run test
  executables with gtest filters for what you need rather than full suites in parallel. If you see "No space left on
  device", it is the environment - wait (a capped loop) and retry, do not treat it as a product failure. Never delete
  files you did not create.
