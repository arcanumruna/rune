# Sanitizer triage — first real TSan / ASan+UBSan checkpoint (2026-10-14)

Branch `feat/catalog-history`, rune-broker. Builds: `build-tsan` (`-fsanitize=thread -O1`),
`build-asan` (`-fsanitize=address,undefined -O1`), both x86_64 binaries (they run under
Rosetta on this arm64 host). Starting point (checkpoint D logs): TSan 1158 / 6274
Regression failures (1154 "Subprocess aborted"); ASan 1 failure (`rune_broker_process_tests`
discovery aborted, so nothing else ran).

## Method

1. Re-ran the 1158 failed TSan tests with `halt_on_error=0` so each process reports every
   race, not just the first: **11,715 reports**. A clusterer grouped them by
   (report kind, top rune frame of each access).
2. 76 raw clusters. 10,624 reports had both accesses inside `M2ORingBuffer.h` /
   `O2ORingBuffer.h` / `ControlRing.h` / `RingDoorbell.h`. The remaining clusters were
   checked line by line. All but one had ring code inlined into the caller frame:
   `AeronPartitionTransport.cpp:588/530/334` = `outboundRing_.controlledDrain/commit`,
   `BrokerNode.cpp:3946/4121/4129` = `failureRing/catalogReadRing/egressRing().drain`,
   `ClientGateway.cpp:6056` = `drainAllReturnsView`, `Worker.h:1706/1766` =
   `ret_.freeSlotsForMsgSize` / `sessionRing_.producerPosition()`, `WorkerViewSet.cpp:264`,
   and ring test bodies. Garbled clusters came from interleaved `-j8` output.
   **Result: 2 TSan root causes.**
3. ASan: re-ran with `detect_leaks=0` (see A0), then fixed. LeakSanitizer is
   **not supported on this platform** (x86_64 macOS / Rosetta), so leaks were checked
   with `MallocStackLogging=1 arch -x86_64 leaks --atExit` on build-fast samples.
   Plain `leaks` hangs against a translated process.

## Clusters

| # | Representative report | Class | Proof | Fix | Commit |
|---|---|---|---|---|---|
| **T1** | `data race M2ORingBuffer.h:79 tryClaim` (write 4, `ControlRing::offer`, main) vs `M2ORingBuffer.h:165 controlledRead` (previous read 4, `AgentRunner::stopRequested_`, T2); same pair for `O2ORingBuffer.h:110/118` vs `:216/263`, head words `O2O:394` vs `ScopeExit:329`, `RingDoorbell.h:130`. 10.6k direct + about 1.1k inlined into callers. **1157 of 1158 tests.** | **(a)** false positive | The racing pair is the consumer's *empty-slot* load of the record-length word (it reads 0) and a producer's later store of `-length` into that word. No happens-before edge exists between them, and none is needed: in the C++ model both are atomic accesses. Aeron's `getInt32Volatile` / `putInt32Ordered` (`Atomic64_gcc_x86_64.h`) are a plain `volatile` access plus a standalone `atomic_thread_fence`. TSan models neither, so it sees two plain accesses. `RUNE_TSAN_ACQUIRE/RELEASE` only order the payload *after* the length is observed. They cannot make the control word itself atomic. The `race:aeron::concurrent::AtomicBuffer::getInt32Volatile` suppression did not match because -O1 inlined the accessor and the symbolized stack has no AtomicBuffer frame (`#0 … M2ORingBuffer.h:165`). | New `rune/consensus/detail/RingAtomics.h`: `getInt32Acquire` / `putInt32Release` / `getInt64Acquire` / `putInt64Release` / `compareAndSetInt64` via `std::atomic_ref`. These replace every Aeron volatile/ordered/CAS call in M2O, O2O and the doorbell watch. **Hot-path cost: none.** x86-64 emits the same `mov` / `mov` / `lock cmpxchg` as Aeron; AArch64 emits `ldar` / `stlr` instead of `ldr + dmb` / `dmb + str`. No alloc, inlined. The names keep `check_no_atomics` exhaustive, and the allowlist was regenerated as RING entries. No suppression was added. Regression: `RingAtomicsTsanRegression.ControlRingEmptyPollsRacingOffersAreRaceFree`, `…O2OEmptyReadsRacingClaimsAndPositionReadsAreRaceFree`. | `1070219b` |
| **T2** | `data race Log.cpp:1031 Log::appendNoSyncRestamp` (write 8, worker T15, `Worker::produceCore_`) vs `ClientGateway.cpp:4638 handleSubscribe` (read 8, control thread T14, `NodeRuntimeAgent`). Heap block allocated by `TopicRegistry::buildLive_`. Seen in `DemotedLeaderReadCommittedRegression.SubscriptionMovesToTheNewLeaderRegression`. | **(b)** real race | The Log is registered worker-owned (`addWorkerOwnedLog`). Its `logEndOffset_` is written by the worker's append. The gateway read it directly, with no ring and no happens-before edge. TSan's only "edge" was "as if synchronized via sleep", i.e. none. `logStartOffset()` on the same path walks the worker's `segments_` std::map while the worker rolls segments: a torn or invalid read. `addGroupSubscription` (`:7454`) has the same LEO read. | `ClientGateway::gatewayOwnsLog_`: the gateway reads only a gateway-owned Log. For a worker-owned one, the clamp is the replica's published HW (`gatewayHighWatermark`). With no replica it returns a retriable redirect, or refuses. Earliest / below-start resolution moves to the owner: `Worker::raiseFreshDiskCursor_` raises a fresh bind-time cursor to the log start, in both the owned push loop and `servePushRead_` (`ret.newNextOffset` carries it back). This keeps the CP-5.2 behaviour. Shared-nothing; zero-alloc. Regression: `WorkerPushFixture.FreshCursorBelowTheDiskLogStartResumesAtTheStartOnTheWorkerRegression`, which fails when the worker raise is removed. | `fda40e71` |
| **A0** | `AddressSanitizer: detect_leaks is not supported on this platform.` → `rune_broker_process_tests` gtest discovery "Subprocess aborted" → ctest stopped. | config | ASan runtime: `detect_leaks=1` is fatal on x86_64 macOS. Not a code defect; **no test had actually run under ASan**. | Run ASan with `detect_leaks=0`. Leaks are checked with `leaks` (C1). | n/a |
| **C1** | `leaks`: `2 leaks for 208 total leaked bytes`, ROOT LEAK `make_unique<MappedFile>` ← `LogSegment::openReadMap` ← `Log::Log` (`SealedSegmentBeyondItsMapPostsNoGrowthRequestRegression`) | **(c)** real leak | A `kDisposeMsgType` record **owns** its retired `MappedFile`: the poster `release()`s it, and the provisioner deletes it. When a `ProvisionChannel` is destroyed with records still queued (provisioner stopped first, or never served), nothing frees them, so the mapping and its mmap leak. `Log::~Log` already drains its own map ring in the same way. | `~ProvisionChannel` (all threads stopped, so it is the sole consumer) frees queued dispose records. Regression: `SegmentLifecycleRegression.QueuedReadMapDisposalIsFreedWithTheChannelRegression`; `msync` returns `ENOMEM` once the region is unmapped. It fails when the drain is removed. `leaks` after the fix: 0. | `690650e8` |
| **U1** | `-Wunsequenced` on `Log.cpp:616`: `(++readyDrops_ & (readyDrops_ - 1))` | **(c)** UB | The modification and the read of `readyDrops_` are unsequenced operands of `&`, which is undefined behaviour. UBSan does not instrument this; the compiler warning found it during the triage builds. The rate-limited "ready ring full" warning could skip the first drop. | Read once into a local. Regression: `SegmentLifecycleRegression.FirstReadyRingDropIsLoggedRegression` (first drop logged with `drops=1`). It cannot be shown red under clang's actual evaluation order; it guards the contract. | `690650e8` |
| **E1** | `TransientFailoverLatencyGateRegression.KTransientPartitionsBecomingLeaderNeverStallAWorkerPass`: p99 331 µs > 250, max 3453 > 2000 (TSan) | test env | A µs budget under a 5–15× sanitizer slowdown. Same convention as `DutyPassLatencyGateTest` (`RUNE_GATE_SANITIZED`). | Budgets are skipped under TSan/ASan; the functional checks (all Ready, rebuilds spread over passes) still run. | `3dd71c17` |
| **E2** | UBSan `format.h:927 member call … does not point to an object of type 'fmt::detail::buffer<char>' — object has invalid vptr` (`InternalIsrRegression.TransientOpenErrorRetriedNotFailClosed`) | false positive (tool) | The test exhausts `RLIMIT_NOFILE` to force `EMFILE`. The process's first log line (spdlog registry construction) fell inside that window. UBSan's vptr check probes memory readability through a pipe, which fails with `EMFILE`, so it reports "invalid vptr". It was reproduced only inside the window. A suppression could not match because the symbolizer cannot spawn under `EMFILE` either. | The test warms the logger and the formatting path before the window. No suppression. | `3dd71c17` |
| **E3** | `DdlConformanceBrokerRegression.T7_RestartPreservesTheCatalogAndTheCollisions`: `define("t_disk")` after restart answered `0` (OK), expected 25; `T5_RefusedDdlLeavesNoTraceAndTheBrokerStillRestarts` −1 (TSan only). Previously masked by T1 aborts. | test bug | `ShardTestClient::nextRequestId_` keyed the requestId on `this` plus a counter. The post-restart client is constructed at the same address, so its counter restarts and it re-sends ids an earlier client used. The broker's durable DDL dedupe (Task 11) correctly returns the original outcome. Which ids collide depends on how many NotLeader probes each boot needed, so it only showed up under TSan timing. | A random 48-bit nonce per client instance. | `3dd71c17` |

Not sanitizer findings (out of scope, noted):

- **`CatalogReadServerRegression.{ATruncationBelowAServedHw…, ADroppedNoteOnAStreamOpenedAtTheHw…}`** failed functionally in the first ASan run. That build was compiled while Task 13 fix round 3 (another agent, `CatalogReadServer*`) was still in progress in the working tree; `bf1bfce4` landed it.
- **`TransientLineageClusterRegression.FullClusterRestartAnswersAGroupsOffsetAsNoneRegression`** (ASan, `-j8` load): `ShardTestClient::subscribe` → `send()` timed out on a fresh publication after a full 3-node restart. It is a client-side transport-timing flake and passes 3/3 in isolation under ASan. Unrelated to T2: that path is transient (no disk Log).

## Real bugs found: 3

T2 (cross-thread race), C1 (leak), U1 (UB). The rest are false positives or tool limits
(T1, E2), config (A0), or test environment / test bugs (E1, E3).

## Final counts

Final full `ctest -R Regression -j8` at `3dd71c17`. The working tree also held the other
agent's uncommitted Task 13 test edits (`libs/node/CMakeLists.txt`, `tests/client/OneBroker.h`).

| Build | Options | Result | Sanitizer reports |
|---|---|---|---|
| build-tsan | `TSAN_OPTIONS=halt_on_error=1` (+ default-options suppressions) | **6314 / 6315 passed** | **0** `WARNING: ThreadSanitizer` |
| build-asan | `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1`, `UBSAN_OPTIONS=halt_on_error=1` | **6315 / 6315 passed** | **0** ASan / UBSan |
| build-fast | none | **6315 / 6315 passed** | n/a |
| leaks (x86_64 `leaks --atExit`) | build-fast samples: `rune_o2o_ring_tests`, `rune_consensus_tests` ring/AgentRunner, `rune_storage_tests` SegmentLifecycle, `rune_worker_push_tests` | 0 leaks each, after C1 | n/a |

The one TSan failure, `ConsensusCandidacyElectionRegression.DeniedCandidacyNeverFreezesLiveLeaderMetadataIsr`
(`:463`, "leadership changed — the scenario no longer pins MAJOR-R3"), is a load-timing flake.
Under `-j8` TSan the heartbeats are slow enough to allow a re-election. It produced no TSan report,
passed in the previous full TSan run, and passes 3/3 in isolation under TSan.

The checkpoint went from 1158 failed to 1 flake.

Detect-leaks note: LeakSanitizer is unsupported on this platform (x86_64 macOS under
Rosetta: `detect_leaks is not supported on this platform`, fatal). Leak coverage is the
`MallocStackLogging` + `leaks` sample above, which found C1.
