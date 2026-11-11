# Rune Prerequisites for Herald and Discovery — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the C++ client the three things Herald P1 and the discovery library's consumer side cannot start
without: a `LeaseWatch` that closes and restarts without blocking or allocating and reports re-runs (R12 = RQ4 + RQ5),
a lease guard that also covers a transactional producer's commit (R7), and pause / resume of one subscription (R9).

**Architecture:** All three are client-side changes in `client-cpp/`; no wire format and no broker code changes.
`LeaseWatch` gets an explicit life state and three new events. The producer's guard check is added at the one place
that sends `EndTxn(commit)`, and a guard trip poisons an open transaction. Pause / resume reuses the hub's existing
"move a subscription and resume at its position" path (`rearmMovedAway`, `resumeFloor`).

**Tech Stack:** C++20, `client-cpp` (`rune::client`), GoogleTest, the in-process `InvokerGateway` stub for unit
tests, the real-broker suites under `broker/libs/node/tests/client/`.

**Spec:** the requests are specified in two documents outside this repo. Executors read the quoted rows below; they
are complete.

- Herald spec 02 (`2026-11-07-herald-02-runtime-and-rune-integration-design.md`) §14, rows R7, R9, R12, R6, R10.
- Discovery spec D (`2026-11-11-discovery-on-rune-design.md`) §15, rows RQ4, RQ5, RQ1; §7.2 and §7.4 use them.
- Rune lease spec: `broker/docs/superpowers/specs/2026-11-07-rune-lease-topics-design.md`.

**Requests, as asked (verbatim):**

- **R12 / RQ4:** "`LeaseWatch::closeAsync()` and `isClosed()`: cancel the subscriptions and report when every one is
  finished, so the destructor of a closed watch returns at once and never calls `client.doWork()`. And
  `restart(prefixes)`: start a closed or failed watch again in its own memory."
- **R12 / RQ5:** "`LeaseWatch` handler events: `Confirmed(key, version, row)` when a re-run's scan finds a lease it
  already holds at the same version (today `grant_` returns without an event and the scanned row is dropped), and
  `RerunBegin` / `RerunEnd` around every re-run."
- **R7:** "`bindProducer` on a transactional producer guards its record sends **and its commit**. At `Entered` an
  open transaction fails; after `Cleared` the same producer can begin again; `initTransactions` by a new instance
  with the same `transactionalId` aborts the old instance's open transaction and refuses its later sends and commits."
- **R9:** "A way to stop taking records from one subscription and resume it later, without stopping the client's
  other work (lease keepalives, other subscriptions)."

**Not in this plan (and why):** R11 `segment.ms` (wanted for Herald P2 only; Herald 04 rolls the segment with filler
rows until then; it is broker storage work and gets its own plan). RQ2 key-prefix `WRITE` grants (owner ruling:
required before production, not before P1; its own plan). R1, R2, R3, R4, R5, R8, RQ3: not blocking, each has a
stated fallback in Herald 02 §14.

## Global Constraints

- CLAUDE.md §1 applies to every line: shared-nothing, rings only, no locks, no allocation in steady state, no virtual
  call on a hot path, rich states instead of scattered booleans.
- No wire change and no broker change. A task that seems to need one stops and reports.
- New enum values are appended; existing numeric values never change.
- Every new call is safe on the duty thread and inside a handler (invoker mode), and never calls `client.doWork()`.
- Tests advance the injected clock and call `doWork` with a hard iteration cap; no sleeps, no wall time.
- Every test added by a fix-like change is named `*Regression*`.
- One build at a time, `-j4`. Run focused `--gtest_filter` runs only. Docker e2e runs on the Linux VM, never on a Mac.
- Never run `conan create/export/upload`. No secrets, addresses or local paths in the repo.
- Commit messages end with the session's `Co-Authored-By:` trailer. Branch `feature/herald-prerequisites`; never
  push to `main`.

## Review Focus

Failure modes the requests imply but do not name. Each has a test in the task that owns the code.

1. **`closeAsync()` called from inside the watch's own handler** (the discovery consumer does this on `Failed`). It
   must not destroy state the running `doWork` still uses, and no further event may be delivered after it returns.
   Test: Task 1, `CloseAsyncInsideHandlerRegression`.
2. **A partition breaks again while its re-run is still scanning.** The reader must see one `RerunBegin` and one
   `RerunEnd`, not a nested or unbalanced pair, or it stays `Stale` for ever. Test: Task 3,
   `RerunBreaksAgainOnePairRegression`.
3. **`restart` with a different prefix set.** Leases held under a prefix that is no longer watched must end with
   `Vanished`, or a consumer keeps routing to them. Test: Task 2, `RestartWithNarrowerPrefixesVanishesRegression`.
4. **A commit issued while the guard is tripped, with every record already acknowledged.** No record fails, so
   nothing poisons the transaction today and the commit goes out after the lease is at risk. Test: Task 4,
   `CommitWithAllRecordsAckedBlockedAtRiskRegression`.
5. **A session loss or leader change while a subscription is paused, then `resume()`.** The subscription is bound to
   no session while paused; it must come back at its exact position, once, on the new leader. Test: Task 6,
   `PausedAcrossSessionLossResumesExactlyOnceRegression`, and Task 7 on a 3-node cluster.

## Interaction matrix

Rows are operations, columns are the state they meet. Every cell is a test case in the named task's table-driven
test; "—" means the combination cannot be reached.

**LeaseWatch life (Tasks 1–2)**

| operation \ life | Fresh | Running | Failed | Closing | Closed |
|---|---|---|---|---|---|
| `startFromNow` | → Running | refused | refused | refused | refused |
| `closeAsync` | → Closed | → Closing | → Closing | no-op | no-op |
| `isClosed` | true | false | false | false | true |
| `restart` | refused | refused | refused | refused | → Running |
| `doWork` | 0 | normal | finishes subscriptions | finishes subscriptions → Closed | 0 |
| record / scan answer arrives | — | applied | dropped | dropped | dropped |
| destructor | returns at once | waits (as today) | waits (as today) | waits (as today) | returns at once |

**Re-run events × timing (Task 3)**: first scan (no `RerunBegin`), re-run after a break, re-run after a data reset,
re-run after `restart`, a second break during a re-run, watch failure during a re-run (`Failed`, no `RerunEnd`),
`closeAsync` during a re-run (no further event) — each × {lease unchanged, updated while broken, ended while broken,
re-granted at a new version while broken, granted while broken}.

**Guard × transaction state (Tasks 4–5)**: guard event {trip, re-arm, end, bind refused} × transaction state
{`Ready` (none open), `InTransaction` with unacked records, `InTransaction` all acked, `Committing/AwaitData`,
`Committing/EndSent`, `Committing/ResendEnd`, `Aborting`, `AbortableError`, `FatalError`} × the next call
{send, commit, abort, begin, sendOffsetsToTransaction}.

**Pause × subscription state (Tasks 6–7)**: call {`pause()`, `pause(offset)`, `resume()`, `unsubscribe()`} ×
state {pending (no response yet), active idle, active inside its own handler, paused, moved (re-arming), finished} ×
event {more batches in flight, session lost, leader moved, client closed, data reset}.

## File Structure

| File | Change |
|---|---|
| `client-cpp/include/rune/client/lease/LeaseWatch.h` | `Life` state, `closeAsync`, `isClosed`, `restart`, three event kinds |
| `client-cpp/src/lease/LeaseWatch.cpp` | the same; destructor fast path; `Confirmed` in `grant_`; re-run bracket |
| `client-cpp/src/lease/*.cpp` (`LeaseSession.cpp`, recipes) | every `switch` over `LeaseEventKind` ignores the new kinds |
| `client-cpp/tests/lease/LeaseWatchTest.cpp` | `Recorder` learns the new kinds |
| `client-cpp/tests/lease/LeaseWatchLifeTest.cpp` (new) | Tasks 1–3 |
| `client-cpp/include/rune/client/producer/ProducerImpl.h`, `client-cpp/src/producer/ProducerTxn.cpp` | guard on commit and offsets; trip poisons |
| `client-cpp/tests/producer/ProducerTxnGuardTest.cpp` (new) | Task 4 |
| `broker/libs/node/tests/client/LeaseTxnGuardBrokerRegressionTest.cpp` (new) | Task 5 |
| `client-cpp/include/rune/client/subscriber/Subscription.h`, `client-cpp/include/rune/client/detail/HubCommand.h`, `client-cpp/src/subscriber/SubscriberHub.cpp`, `Subscription.cpp` | pause / resume |
| `client-cpp/tests/subscriber/SubscriberPauseTest.cpp` (new) | Task 6 |
| `broker/libs/node/tests/client/SubscriptionPauseBrokerRegressionTest.cpp` (new) | Task 7 |
| `docs/api/client-cpp.md`, `broker/docs/superpowers/specs/2026-11-07-rune-lease-topics-design.md` | Task 8 |

New unit test files go into the executable that already holds their neighbour (`LeaseWatchTest.cpp` and
`ProducerTxnTest.cpp` are in `rune_invoker_tests`, `SubscriberHubCommandTest.cpp` in `rune_subscriber_tests`); the
two broker files go into `rune_lease_topic_broker_tests` and the executable that holds
`ClientFailoverBrokerRegressionTest.cpp`.

---

## Phase 1 — LeaseWatch (R12)

### Task 1: `closeAsync()`, `isClosed()`, a destructor that returns at once

**Files:**
- Modify: `client-cpp/include/rune/client/lease/LeaseWatch.h`, `client-cpp/src/lease/LeaseWatch.cpp`
- Create: `client-cpp/tests/lease/LeaseWatchLifeTest.cpp`; add it to `client-cpp/CMakeLists.txt` beside
  `tests/lease/LeaseWatchTest.cpp`
- Modify: `client-cpp/tests/lease/LeaseWatchTest.cpp` (move `Row`, `Recorder`, `WRig`, `kb` into
  `client-cpp/tests/lease/LeaseWatchRig.h` so both files use them)

**Interfaces:**
- Produces:
  ```cpp
  // LeaseWatchBase
  enum class Life : uint8_t { Fresh, Running, Failed, Closing, Closed };
  void closeAsync() noexcept;                       // never blocks, never calls client.doWork()
  [[nodiscard]] bool isClosed() const noexcept;     // Fresh or Closed
  [[nodiscard]] Life life() const noexcept;
  ```
  `failed()` stays and means `life() == Life::Failed`. `isReady()` is `ready_ && life() == Life::Running`.
- `Life` replaces the members `started_` and `failed_` (one state, CLAUDE.md §1 rule 3).

- [ ] **Step 1: Write the failing tests** in `LeaseWatchLifeTest.cpp`, suite `LeaseWatchLifeRegression`, on `WRig`
  (2 partitions, 3 leases granted before the watch starts, the watch driven to `Ready`):
  - `CloseAsyncThenClosedRegression`: `closeAsync()`; `isClosed() == false`; drive `client->doWork()` and
    `watch.doWork(now)` at most 200 times until `isClosed()`; expect it true, `life() == Life::Closed`, the stub saw
    one `Unsubscribe` per partition, and `Recorder` got no event after the call.
  - `DestructorOfClosedWatchDoesNoWorkRegression`: as above, then record `client->dutyCyclesForTest()` (add this
    counter to `Client` if no equivalent exists), destroy the watch, expect the counter unchanged.
  - `CloseAsyncInsideHandlerRegression`: the handler calls `closeAsync()` on the first `Granted`; the other two
    leases' events are not delivered; no crash under ASan; the watch reaches `Closed`.
  - `CloseAsyncOnFreshIsClosedRegression`: a watch never started: `closeAsync()`; `isClosed()` at once; the
    destructor does no work.
  - `CloseAsyncOnFailedRegression`: make the scan answer a definitive error (as `DefinitiveScanErrorFailsRegression`
    does); after `Failed`, `closeAsync()` → `Closing` → `Closed`; exactly one `Failed` event in total.
  - `CloseAsyncTwiceRegression`: the second call changes nothing; still one `Unsubscribe` per partition.
  - `LateRecordsAfterCloseDroppedRegression`: the stub pushes a `Granted` record for each partition after
    `closeAsync()` and before the unsubscribe is answered; no event; `liveCount()` unchanged.
  - `LifeMatrixRegression`: table-driven over the "LeaseWatch life" matrix above (the `restart` column is added in
    Task 2).

- [ ] **Step 2: Run them, expect a compile failure** (`closeAsync` is not a member).
  Run: `cmake --build build -j4 --target rune_invoker_tests`

- [ ] **Step 3: Implement.**
  - `closeAsync()`: `Fresh` → `Closed`. `Running` → for every partition set `curGen = 0`, bump `scanSeq` (so an
    outstanding scan answer is stale), `unsubscribe()` its subscription if it is not finished, set the partition
    state to `Idle`; life → `Closing`. `Failed` → `Closing` (`fail_()` already unsubscribed). Others: no-op.
  - `doWork` in `Closing` and `Failed`: drain the record ring and the scan completions, dropping every item; for each
    partition whose subscription is `finished()`, reset the `optional`; in `Closing`, when no partition holds a
    subscription, life → `Closed`. No event is emitted in `Closing`.
  - `emit_` returns without calling anyone when life is `Closing` or `Closed`. The loops that call `emit_` more than
    once (`scanDone_`, `onScanRows_`, the ring drain) stop when life is no longer `Running` (they test `failed_`
    today; test `life_ != Life::Running`).
  - Destructor: run the `gone` callbacks as today; if no partition holds an unfinished subscription, return without
    touching the client. Otherwise keep today's wait loop unchanged.

- [ ] **Step 4: Run the tests.**
  Run: `build/client-cpp/rune_invoker_tests --gtest_filter='LeaseWatch*'`
  Expected: all pass, including every existing `LeaseWatch*Regression` test.

- [ ] **Step 5: Commit** `feat(client): LeaseWatch closeAsync / isClosed, destructor fast path (R12)`.

### Task 2: `restart(prefixes)` in the watch's own memory

**Files:**
- Modify: `client-cpp/include/rune/client/lease/LeaseWatch.h`, `client-cpp/src/lease/LeaseWatch.cpp`
- Test: `client-cpp/tests/lease/LeaseWatchLifeTest.cpp`

**Interfaces:**
- Consumes: `Life`, `closeAsync`, `isClosed` (Task 1).
- Produces: `[[nodiscard]] OpResult restart(std::span<const std::span<const uint8_t>> prefixes);`
  Accepted only in `Life::Closed`; any other state returns what `startFromNow` returns today on a second call. The
  prefix rules (count, length, nested prefixes dropped) are those of `startFromNow`: move them into one private
  `setPrefixes_` both call.

**What restart keeps and what it resets** (the invariant: after `restart` and the next `Ready`, the events delivered
since the watch was created fold to exactly the leases live under the new prefixes):

| kept | reset |
|---|---|
| the live table and its name heaps, `registerName` ids, the handler, observers, bound names, every buffer and ring | `ready_ = false`; every live entry `confirmed = false`; per partition: pages, handoff counts, backoff, `curGen = 0`, state `Scanning`; `nextGen` and `scanSeq` keep growing so answers of the earlier run stay stale |

Every partition starts as a re-run, so `scanDone_` ends each lease the new scan does not confirm with `Vanished`
(a lease under a prefix that is no longer watched is in that set). `Ready` is emitted again when every partition is
`Live`.

- [ ] **Step 1: Write the failing tests**, suite `LeaseWatchRestartRegression`:
  - `RestartAfterCloseFoldsToLiveSetRegression`: 5 leases, `Ready`, `closeAsync` to `Closed`; while closed the stub
    revokes lease 1, updates lease 2, grants lease 6; `restart(same prefixes)`; drive to the second `Ready`. Expect
    `Recorder::held` == {0, 2, 3, 4, 6}, `bad == 0`, `readies == 2`, exactly one `Vanished` (lease 1), one `Granted`
    (lease 6).
  - `RestartAfterFailedRegression`: fail the watch (definitive scan error), `closeAsync` to `Closed`, clear the stub's
    error, `restart`; reaches `Ready`; `failed() == false`.
  - `RestartWithNarrowerPrefixesVanishesRegression`: start with prefixes `{0x00}`, `{0x01}` (2 leases under each);
    close; `restart({0x00})`; the two leases under `0x01` get `Vanished`; `liveCount() == 2`.
  - `RestartNoAllocRegression`: with the allocation counter that `WatchNoAllocRegression` uses, count from just
    before `closeAsync()` to the second `Ready`; expect 0 allocations on the watch's thread.
  - `RestartRefusedUnlessClosedRegression`: `restart` in `Fresh`, `Running`, `Failed`, `Closing` returns the refusal
    and changes nothing; completes the `restart` row of `LifeMatrixRegression`.
  - `RestartKeepsNamesRegression`: a name registered before the first start still answers `lowest(id)` after the
    restart's `Ready`, and a recipe bound with `bindName` gets the events of the second run.

- [ ] **Step 2: Run, expect failure** (no `restart`).

- [ ] **Step 3: Implement** per the table. No `new`, no container growth: every array was sized in the constructor.

- [ ] **Step 4: Run** `--gtest_filter='LeaseWatch*'`; all pass.

- [ ] **Step 5: Commit** `feat(client): LeaseWatch restart in place (R12)`.

### Task 3: `Confirmed`, `RerunBegin`, `RerunEnd`

**Files:**
- Modify: `client-cpp/include/rune/client/lease/LeaseWatch.h`, `client-cpp/src/lease/LeaseWatch.cpp`
- Modify: every `switch` over `LeaseEventKind` under `client-cpp/src/lease/`, `client-cpp/include/rune/client/lease/`
  and `client-cpp/tests/` (find them with `git grep -n "LeaseEventKind::"`)
- Test: `client-cpp/tests/lease/LeaseWatchLifeTest.cpp`; `LeaseWatchRig.h` (`Recorder`)

**Interfaces:**
- Produces:
  ```cpp
  enum class LeaseEventKind : uint8_t { Granted, Updated, Revoked, Expired, Vanished, Ready, Failed,
                                        Confirmed, RerunBegin, RerunEnd };   // 7, 8, 9: appended
  ```

  | kind | when | `partition` | `key`, `version` | `bytes` | `fromScan` | `offset` |
  |---|---|---|---|---|---|---|
  | `Confirmed` | a scan row names a held lease at the same version, and that entry was unconfirmed | the row's | the lease | the scanned view row | true | the row's `lease_granted_offset` |
  | `RerunBegin` | a partition that was `Live` once starts a new scan (`startRerun_`), or every partition at `restart` | that partition | empty, 0 | empty | false | -1 |
  | `RerunEnd` | that partition's scan is done, after its `Vanished` events and after the buffered records were applied | that partition | empty, 0 | empty | false | -1 |

- Rules the implementer must hold:
  - `Confirmed` is emitted only on the change unconfirmed → confirmed, so a retried scan page never repeats it, and a
    first scan (where a subscription record may already have granted the lease) never emits it.
  - One open bracket per partition: `Part` gets a field `rerunOpen`. `startRerun_` emits `RerunBegin` only when it is
    false, then sets it. `scanDone_` emits `RerunEnd` and clears it when the partition goes `Live` (not when it goes
    `Resubscribing`; then the end comes when the resubscribed partition is `Live`). A partition that breaks again
    during a re-run keeps the bracket open and emits nothing.
  - `Failed` and `closeAsync` end the run with no `RerunEnd`; `restart` clears `rerunOpen` before it opens new ones.
  - `Confirmed` goes to the handler, the observers and the name bound to that key. `RerunBegin` / `RerunEnd` go to
    the handler and the observers only.
  - `LeaseSessionCore`'s watch observer and the recipes ignore all three kinds.

- [ ] **Step 1: Extend `Recorder`** : count `Confirmed` (the id must be in `held`, else `++bad`), and keep an
  `open[partition]` flag for the bracket (`RerunBegin` when open or `RerunEnd` when not open → `++bad`).

- [ ] **Step 2: Write the failing tests**, suite `LeaseWatchRerunRegression`:
  - `UpdateWhileBrokenIsConfirmedRegression` (the bug RQ5 names): lease A is `Live`; break partition 0's
    subscription (as `BreakRerunsRegression` does); while broken the stub updates A's row (new bytes, same version)
    and writes no record the resubscribed stream will replay; drive to `Live`. Expect the sequence
    `RerunBegin(p0)`, `Confirmed(A, version, bytes == the new row)`, `RerunEnd(p0)`; no `Granted`, no `Updated`.
  - `RerunFoldMatrixRegression`: table-driven over the "Re-run events × timing" matrix; each case checks the exact
    event sequence for the partition and `bad == 0`.
  - `FirstScanHasNoRerunEventsRegression`: start to `Ready` with 3 leases: zero `Confirmed`, `RerunBegin`,
    `RerunEnd`.
  - `RerunBreaksAgainOnePairRegression`: break partition 0, let its re-run issue one scan page, break it again, drive
    to `Live`: exactly one `RerunBegin(p0)` and one `RerunEnd(p0)`.
  - `RerunEndAfterOverflowRegression`: the handoff buffer overflows during the re-run (as
    `OverflowResubscribesFromCursorRegression`): `RerunEnd` arrives only after the resubscribed partition is `Live`.
  - `FailDuringRerunNoEndRegression`: a definitive scan error during a re-run: `Failed`, no `RerunEnd`.
  - `RestartBracketsEveryPartitionRegression`: after `restart`, one `RerunBegin` and one `RerunEnd` per partition,
    all before the second `Ready`.
  - `RetriedPageConfirmsOnceRegression`: the stub answers the same scan page twice (a transient error between):
    one `Confirmed` per lease.
  - `SessionObserverIgnoresRerunEventsRegression`: a `LeaseSession` observing the watch (as
    `SessionObservesWatchRegression`) keeps its lease through a re-run; `onLost` is not called.

- [ ] **Step 3: Run, expect failures.**

- [ ] **Step 4: Implement** per the rules. In `grant_`: `if (e.version == version) { if (fromScan && !e.confirmed) {
  e.confirmed = true; emit_(Confirmed, …row…); } else e.confirmed = true; return; }`.

- [ ] **Step 5: Run** `--gtest_filter='Lease*'` (watch, session and recipe suites); all pass.

- [ ] **Step 6: Commit** `feat(client): LeaseWatch Confirmed / RerunBegin / RerunEnd events (R12)`.

---

## Phase 2 — Lease-guarded transactional producer (R7)

### Task 4: the guard covers the commit; a trip fails the open transaction

**Files:**
- Modify: `client-cpp/include/rune/client/producer/ProducerImpl.h` (`failGuarded`), `client-cpp/src/producer/ProducerTxn.cpp`
  (`endStep_`, `offsetsStep_`)
- Create: `client-cpp/tests/producer/ProducerTxnGuardTest.cpp` (beside `ProducerTxnTest.cpp` in `client-cpp/CMakeLists.txt`)

**Interfaces:**
- Consumes: `guardBlocks_()`, `poison_(OpStatus, uint16_t, AbortKind)`, `ClientErrorCode::LeaseAtRisk`,
  `LeaseSessionCore::bindProducer`.
- Produces (behaviour, no new API):

  | moment | transaction state | result |
  |---|---|---|
  | guard trips (`Entered`), ends, or the bind is refused | `InTransaction` (records acked or not) | → `AbortableError(DeliveryFailed, LeaseAtRisk)`; unacked records complete `DeliveryFailed / LeaseAtRisk` (as today) |
  | same | `Committing`, `EndTxn` not sent yet (`AwaitData`) | the commit completes `DeliveryFailed / LeaseAtRisk`; → `AbortableError` |
  | same | `Committing`, `EndTxn` sent (`EndSent`, `ResendEnd`, `InDoubt`) | unchanged: the commit was sent inside the lease; its answer decides. A re-send is never blocked |
  | same | `Aborting`, `Ready`, `AbortableError`, `FatalError` | unchanged |
  | `commitTransaction` issued while `guardBlocks_()` | any open | never sends `EndTxn(commit)`; completes `DeliveryFailed / LeaseAtRisk`; → `AbortableError` |
  | `sendOffsetsToTransaction` issued while `guardBlocks_()` | `InTransaction` | completes `DeliveryFailed / LeaseAtRisk`; → `AbortableError` |
  | `abortTransaction` while `guardBlocks_()` | any | allowed: `EndTxn(abort)` is sent |
  | after `Cleared` (re-armed) | `Ready` after the abort | `beginTransaction`, sends and commit work |
  | `beginTransaction` while blocked | `Ready` | allowed; every send fails `LeaseAtRisk`; the commit is refused as above |

- [ ] **Step 1: Write the failing tests**, suite `ProducerTxnGuardRegression`, with the transactional rig of
  `ProducerTxnTest.cpp` plus a `LeaseSession` bound to the producer as `LeaseSessionTest.cpp`'s `bindProducer` tests
  do; the trip is made by advancing the clock past the armed at-risk instant with keepalive answers withheld:
  - `CommitWithAllRecordsAckedBlockedAtRiskRegression`: begin, 3 sends, all acked; trip; `commitTransaction`. Expect
    the commit completion `DeliveryFailed` with `errorCode == LeaseAtRisk`, the gateway stub saw **no** `EndTxn` with
    `commit == 1`, and the producer's status is `AbortableError`.
  - `TripFailsOpenTransactionRegression`: begin, 2 sends unacked; trip. Both sends complete `DeliveryFailed /
    LeaseAtRisk`; a third send is refused with the same code; `commitTransaction` as above.
  - `TripDuringAwaitDataFailsCommitRegression`: begin, 1 send, `commitTransaction` issued before the ack; trip before
    the ack; the commit completes `DeliveryFailed / LeaseAtRisk`; no `EndTxn(commit)` leaves.
  - `TripAfterEndSentKeepsCommitRegression`: `EndTxn(commit)` left; trip; the stub answers OK; the commit completes
    OK; the next `beginTransaction` + send fails `LeaseAtRisk` until re-arm.
  - `AbortAllowedWhileBlockedRegression`: after a trip, `abortTransaction` sends `EndTxn(commit == 0)` and completes
    OK.
  - `BeginAgainAfterClearedRegression`: trip, abort, answer the keepalive (re-arm → `Cleared`), begin, send, commit:
    all OK, one `EndTxn(commit == 1)`.
  - `SendOffsetsBlockedAtRiskRegression`: `sendOffsetsToTransaction` while tripped completes `DeliveryFailed /
    LeaseAtRisk`; no `TxnOffsetCommit` leaves.
  - `GuardEndedIsFinalRegression`: the lease is lost (`GuardEnd`): commit refused; a re-arm of the old generation
    does not unblock; binding to a new lease of the session does.
  - `GuardTxnMatrixRegression`: table-driven over the "Guard × transaction state" matrix; each row checks the frames
    that left and the completion codes.
  - `UnboundTransactionalProducerUnchangedRegression`: a transactional producer with no guard: one extra predicted
    compare, same frames as before (run the first 5 cases of `ProducerTxnTest` through it).

- [ ] **Step 2: Run, expect failures** (`EndTxn(commit)` is sent in the first test).

- [ ] **Step 3: Implement.**
  - `failGuarded`: after `failAllPending(st, errorCode)`, when the producer is transactional call
    `poison_(st, errorCode, AbortKind::Plain)`. `poison_` already leaves a commit that may have landed alone.
  - `endStep_`: at the top of the `AwaitData` branch, `if (cm != nullptr && guard_.bound && guardBlocks_())` →
    `poison_(OpStatus::DeliveryFailed, LeaseAtRisk, AbortKind::Plain); return 1;`. Not in the `ResendEnd` branch and
    never for an abort.
  - `offsetsStep_`: before a `TxnOffsetCommit` / `AddOffsets` frame is first sent, the same check; end the op with
    `offsetsEnd_(DeliveryFailed, LeaseAtRisk, …)` and poison.
  - `LeaseGuards::bind` refusing a producer, and `GuardEnd`, already call `guardKill` / `failGuarded`; check each call
    site reaches the new poison.

- [ ] **Step 4: Run** `build/client-cpp/rune_invoker_tests --gtest_filter='ProducerTxn*:LeaseSession*:TxnMatrix*'`;
  all pass.

- [ ] **Step 5: Commit** `feat(client): lease guard covers the transactional commit (R7)`.

### Task 5: real-broker proof — guard and `transactionalId` fence

**Files:**
- Create: `broker/libs/node/tests/client/LeaseTxnGuardBrokerRegressionTest.cpp`; add to
  `rune_lease_topic_broker_tests` in `broker/libs/node/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 4; the cluster helpers the neighbouring `LeaseTopicBrokerTest.cpp` and `EosBrokerRegressionTest.cpp`
  use (`OneBroker.h`, `ClusterClient.h`); invoker-mode clients so the test owns each client's clock.

Each test runs on 1 node and on 3 nodes (`INSTANTIATE_TEST_SUITE_P` over `{1, 3}`), default config. "A" and "B" are
two clients with the same `transactionalId` `tx-1`, each with a `LeaseSession` on one lease key `role/1` (ttl 2 s);
a read-committed subscriber `R` reads the data topic.

- [ ] **Step 1: Write the tests**, suite `LeaseTxnGuardBrokerRegression`:
  - `GuardedCommitHappyPath`: A holds the lease, binds, begin, 10 sends, commit OK; `R` reads 10.
  - `StalledHolderCannotCommit`: A begins and sends 5 (acked). Stop driving A's client for the whole ttl (advance
    A's clock only); B is granted the lease after expiry, `initTransactions(tx-1)`, begins, sends 5, commits. Then A
    is driven again and calls `commitTransaction`. Expect: A's commit is refused (`LeaseAtRisk`, or `Fenced` if the
    guard event had not been processed: either code, never OK), `R` reads exactly B's 5 records, A's 5 are never
    visible, A's next send completes `Fenced`.
  - `InitTransactionsAbortsOldOpenTransaction`: no leases involved. A begins, sends 3 (acked), does not commit. B
    `initTransactions(tx-1)`. `R` (read committed) never sees A's 3; A's `commitTransaction` completes `Fenced`; A's
    send completes `Fenced`; B commits 2 and `R` reads exactly those 2.
  - `AtRiskThenClearedContinues`: A begins, sends 2; withhold A's keepalive sends until the at-risk instant passes
    but not the ttl; A's records fail `LeaseAtRisk`; resume keepalives (`Cleared`); A aborts, begins, sends 4,
    commits; `R` reads exactly 4.
  - `LeaderChangeMidCommitGuarded` (3 nodes only): A commits while the data partition's leader is stopped; the lease
    stays held; the commit completes OK after failover and `R` reads each record once.

- [ ] **Step 2: Run on the Linux VM build** (one build, `-j4`):
  `build/broker/libs/node/rune_lease_topic_broker_tests --gtest_filter='*LeaseTxnGuardBrokerRegression*'`
  Expected: all pass. A failure of `InitTransactionsAbortsOldOpenTransaction` is a broker bug: diagnose it at the
  coordinator (CLAUDE.md §1 rules 13–15) and fix it in this task with its own `*Regression*` test.

- [ ] **Step 3: Repeat-run** the suite with `--gtest_repeat=20 --gtest_break_on_failure`.

- [ ] **Step 4: Commit** `test(node): lease-guarded transactional producer on a real broker (R7)`.

---

## Phase 3 — Pause and resume of one subscription (R9), docs

### Task 6: `Subscription::pause` / `resume` in the hub

**Files:**
- Modify: `client-cpp/include/rune/client/subscriber/Subscription.h`, `client-cpp/src/subscriber/Subscription.cpp`,
  `client-cpp/include/rune/client/detail/HubCommand.h`, `client-cpp/src/subscriber/SubscriberHub.cpp` (+ its header)
- Create: `client-cpp/tests/subscriber/SubscriberPauseTest.cpp` (in `rune_subscriber_tests`)

**Interfaces:**
- Produces:
  ```cpp
  // class Subscription
  /// Stops delivery. The broker-side subscription is released; the handle stays valid.
  /// resumeAt < 0: delivery continues after the last record handed to the handler.
  /// resumeAt >= 0: the next record delivered after resume() is the one at resumeAt (a handler that could not take
  /// `rec` calls pause(rec.offset)). resumeAt may not be above (last delivered offset + 1).
  /// Safe on any thread and inside this subscription's handler. Returns false when finished or unsubscribed.
  bool pause(int64_t resumeAt = -1);
  /// Subscribes again at the stored position. Returns false when not paused.
  bool resume();
  [[nodiscard]] bool paused() const;

  // HubCommand::Op
  SubscribePause = 3,    // arg: resumeAt
  SubscribeResume = 4,
  // SubscriptionStatus
  bool paused = false;
  ```
- Semantics the implementer must hold:
  - **Inside the handler** (duty thread): the hub applies the pause at once. `onRecords` checks the subscription's
    pause flag after every handler call and stops delivering the rest of that batch and of every later batch of the
    old broker subscription id.
  - **Position:** `nextOffset = resumeAt >= 0 ? resumeAt : nextOffset`. On resume, the subscription is re-armed
    exactly as `rearmMovedAway` does for a moved subscription: a new correlation id, `From::offset(nextOffset)` (the
    original `from` if nothing was ever delivered and `resumeAt < 0`), `resumeFloor = nextOffset`, so a batch the
    broker pushes from below the position is trimmed and no record is delivered twice or skipped.
  - **While paused** the subscription is in neither `pendingByCorrelation_` nor `activeById_`; its
    `SubscribeRequest` is parked in the `SubscriptionImpl`. An `Unsubscribe` frame releases the broker side (through
    the existing `trySendUnsubscribe_` / pending-unsubscribe retry). A subscribe answer that arrives for the old
    correlation id goes through the existing `releaseUnboundOk_`.
  - `active()` is false while paused; `finished()` is false; `position()` is the stored position.
  - `unsubscribe()`, client close, `endAllLive` (session revoked) and `deactivateAll` finish a paused subscription.
    A session loss or a leader move does nothing to it (it is bound to no session).
  - Pausing a pending subscription (no answer yet) parks it the same way. `pause` when paused only lowers / sets the
    position. `resume` when not paused returns false.
  - Group-consumer member subscriptions and a `LeaseWatch`'s subscriptions are not paused: `pause` returns false.
  - The client's other work is untouched: no state outside this subscription changes.
  - After a subscription's first pause / resume cycle, later cycles allocate nothing: keep the map nodes with
    `extract` / `insert(node_type&&)` in the `SubscriptionImpl` rather than erase / emplace.

- [ ] **Step 1: Write the failing tests**, suite `SubscriberPauseRegression`, on the in-process gateway stub that
  `SubscriberHubCommandTest.cpp` uses (it lets the test push `Records` frames and read the frames the client sent):
  - `PauseInHandlerStopsMidBatchRegression`: one batch of offsets 0..9; the handler calls `pause(rec.offset)` at
    offset 4. Delivered: 0,1,2,3,4 (4 is the refused one). `paused()`; the client sent `Unsubscribe`;
    `position() == 4`.
  - `ResumeRedeliversFromOffsetRegression`: continue: `resume()`; the client sends `Subscribe` with start offset 4;
    the stub pushes the batch 0..9 again (a whole batch from below); delivered now: 4,5,…,9, each once.
  - `PauseDefaultContinuesAfterLastRegression`: `pause()` from the app thread after 0..9 were delivered; resume
    subscribes at 10.
  - `LateBatchesOfOldIdDroppedRegression`: after `pause`, the stub pushes two more batches with the old subscription
    id; no handler call; `position()` unchanged.
  - `OtherSubscriptionKeepsFlowingRegression`: two subscriptions; one paused; 100 records pushed to the other are
    all delivered; a `LeaseSession` on the same client keeps answering keepalives (its lease is never at risk over
    3 × ttl of fake time).
  - `PausedAcrossSessionLossResumesExactlyOnceRegression`: pause at 4; the session is lost and a new one is
    established (as the hub's session-loss tests do); the paused subscription is not re-subscribed by the recovery;
    `resume()` subscribes at 4 on the new session; 4..9 delivered once.
  - `PausePendingSubscriptionRegression`: `pause()` before the subscribe answer; the late OK answer is released with
    an `Unsubscribe`; `resume()` subscribes again with the original `from`.
  - `UnsubscribeWhilePausedFinishesRegression`, `ClientCloseWhilePausedFinishesRegression`: `finished()` becomes true;
    `resume()` returns false.
  - `PauseAboveDeliveredRefusedRegression`: `pause(last + 2)` returns false and changes nothing.
  - `PauseResumeNoAllocRegression`: one warm-up cycle, then 100 pause / resume cycles under the allocation counter:
    0 allocations.
  - `PauseMatrixRegression`: table-driven over the "Pause × subscription state" matrix.
  - `PauseStressModelRegression`: seeded (seed printed), 10 000 steps of random {push batch, pause, pause(offset),
    resume, session loss, leader move}; the model is "the delivered offsets, with each `pause(offset)` rewinding the
    expected next offset"; delivered sequence equals the model exactly.

- [ ] **Step 2: Run, expect a compile failure.**

- [ ] **Step 3: Implement** per the semantics. `applyCommand` handles the two new ops; the handle's `pause` on the
  duty thread calls the hub directly (as `unsubscribe` does through `onDutyThread_()`), otherwise it enqueues the
  command.

- [ ] **Step 4: Run** `build/client-cpp/rune_subscriber_tests` (whole executable: the change is in its shared path)
  and `rune_invoker_tests --gtest_filter='LeaseWatch*'`; all pass.

- [ ] **Step 5: Commit** `feat(client): pause and resume one subscription (R9)`.

### Task 7: real-broker proof of pause / resume

**Files:**
- Create: `broker/libs/node/tests/client/SubscriptionPauseBrokerRegressionTest.cpp` (same executable as
  `ClientFailoverBrokerRegressionTest.cpp`)

**Interfaces:** consumes Task 6.

- [ ] **Step 1: Write the tests**, suite `SubscriptionPauseBrokerRegression`, default config:
  - `BackpressuredTailLosesNothing` (1 node): produce 5 000 records to one partition. The handler pushes each record
    into a 64-slot test ring and, when the ring is full, calls `pause(rec.offset)`; the test thread drains the ring
    and calls `resume()` when it is empty. Expect the drained offsets to be exactly 0..4999 in order, at least 10
    pauses happened, and a `LeaseSession` on the same client (ttl 2 s) never reports at-risk or lost.
  - `SecondSubscriptionUnaffected` (1 node): subscription X paused for the whole test; subscription Y on another
    topic receives all of its 1 000 records.
  - `LeaderChangeWhilePaused` (3 nodes, acks=all): pause at offset 500 of 1 000; stop the partition leader; after
    the new leader is elected `resume()`; offsets 500..999 delivered once each, none below 500.
  - `ReadCommittedPauseInsideTransaction` (1 node): a read-committed subscription pauses on a record in the middle of
    a committed transaction's batch and resumes: each record once, the control marker skipped as usual.

- [ ] **Step 2: Run on the Linux VM build**; expected: all pass. Then `--gtest_repeat=20`.

- [ ] **Step 3: Commit** `test(node): subscription pause / resume on a real broker (R9)`.

### Task 8: documents

**Files:**
- Modify: `docs/api/client-cpp.md`, `broker/docs/superpowers/specs/2026-11-07-rune-lease-topics-design.md`

- [ ] **Step 1: `docs/api/client-cpp.md`:** document `LeaseWatch::closeAsync / isClosed / restart / life`, the three
  events with the table of Task 3, the guard's transaction table of Task 4, and `Subscription::pause / resume /
  paused` with the handler example `if (!ring.tryPush(rec)) sub.pause(rec.offset);`.

- [ ] **Step 2: Lease spec (R6, RQ1, R10):**
  - §3, §13, §19.1 and Phase 3: the discovery library is specified by the discovery repo's
    `2026-11-11-discovery-on-rune-design.md` (topic `platform-sys/discovery`, one runtime per process, no watchdog,
    domain and region as columns); remove the `RuneEndpoint`-per-object description.
  - §14.1: drop the `herald-pool` grants on `herald-sys/discovery`.
  - §17: owner question Q1 is open, not settled.
  - §19: Herald uses the tenant's own `herald.leases.<tag>` topics for role elections, `herald-sys/herald.elections`
    only for singletons, no `herald.workers` or `herald.idslots`, epochs from `herald.epochs`.
  - Add one sentence where TTLs are defined (R10): "A lease TTL is a duration. Rune has no calendar deadline for a
    lease and will not add one."
  - §12.2 / R5: the guard also covers a transactional producer's commit (Task 4's table).

- [ ] **Step 3: Commit** `docs: lease watch life, guarded commit, subscription pause; lease spec follow-ups (R6, R10, RQ1)`.

---

## Checkpoint and hand-off

- After Task 8: the `*Regression*` tests of the three touched executables under ASan+UBSan and TSan
  (`rune_invoker_tests`, `rune_subscriber_tests`, client build with `-DBUILD_TESTING=ON`), filtered to
  `Lease*:ProducerTxn*:TxnMatrix*:Subscriber*`. A sanitizer report is a finding and is fixed with a test.
- Hot path: `onRecords` gains one predicted compare per handler call, `endStep_` one per commit. Benchmarks are
  parked program-wide (issues #841–#845); note this in the PR body.
- Open one PR `feature/herald-prerequisites` → `main`. Its merge unblocks the discovery plan's Phase 2 (Tasks 10–16)
  and Herald P1; both need the next published `rune-client-cpp` package (CI publishes it).

## Self-review

- Coverage: R12 → Tasks 1–3; R7 → Tasks 4–5; R9 → Tasks 6–7; R6, R10, RQ1 → Task 8. R11 and RQ2 are named as out of
  scope with the reason.
- Names are consistent across tasks: `Life`, `closeAsync`, `isClosed`, `restart`, `Confirmed`, `RerunBegin`,
  `RerunEnd`, `pause`, `resume`, `paused`, `SubscribePause`, `SubscribeResume`. The discovery plan's Rune port uses
  exactly these.
- Each Review Focus line has its test in Tasks 1, 2, 3, 4, 6 and 7.
