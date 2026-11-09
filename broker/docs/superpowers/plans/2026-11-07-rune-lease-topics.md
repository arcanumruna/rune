# Rune — Lease Topics (etcd-style leases) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Owner ruling 2026-11-07:** Phase 3 (discovery library on Rune) is dropped; Herald builds its pod registry on Phase 1–2.

**Owner ruling 2026-11-07 (later):** transient lease topics are allowed and persist nothing across a full restart
(spec, top note and Appendix A). Tasks 3 and 4 below still refuse `transient lease topic`; the next session designs
the transient tasks (eviction floor and carry, follower floor, the `max_ttl` grant wait after rebirth) and changes
those refusals. Settled by the owner: nothing goes to disk, versions restart with the ring, and grants wait out
`max_ttl` after a full restart (spec top note). Task 1 is unaffected.


**Goal:** A **lease topic** is a typed, keyed, compacted topic in which every live key **is** a lease with etcd v3
semantics: granted only when the key has no live lease (no takeover), kept alive by `LeaseKeepAlive` requests served
by the partition leader under the view-read lease (no log record), ended by a fenced `Revoke` or by a broker-appended
`Expire` record that replicates, folds into every view and is a compaction tombstone. The grant's offset is the lease
version and fencing token. Long TTLs survive leader changes and whole-cluster restarts through always-persisted
`Checkpoint` records. Expiry never pauses, in any namespace state, and is never early. On top of that: the async C++
client (`LeaseTopic<T>`, `LeaseSession`, `LeaseWatch`), the `LeaseMutex` / `LeaseElection` recipes, limits, metrics
and QL. Non-lease topics pay nothing: byte-identical behaviour and an unchanged ABAB bench.

**Architecture:**
- **Protocol (`protocol/`).** Record attribute bits 2–3 carry the lease op on lease topics only. New header
  `rune/protocol/LeaseRecord.h` holds the op enum, the fixed system-column offsets and the two predicates
  `isTombstone(record, traits)` / `isLeaseControl(record)`. New templates `LeaseKeepAlive`, `LeaseTimeToLive`,
  `ScanLeases` and their responses; three error codes; a lease trailer on the `View` produce ack; a lease trailer on
  `TopicSchemaResponse`; `kClientVersionLeases`, `kBrokerVersionLeases`, `kFeatureLevelLeases`.
- **Catalog (`codec/`).** `DefineTopic` and the `CreateTopic` catalog event carry an optional `LeaseSpec`. One event
  creates the topic and its `<topic>.leases` `ViewObject` (`VerdictViewKind::LeaseIndex`). `AlterTopicConfig` takes
  the alterable `lease.*` keys.
- **Broker admin thread (`AdminWorker`).** Builds lease DDL: the 128-byte lease-key rule, the four system columns
  placed first, the refusals.
- **Broker worker thread (`Worker`, single writer per partition).** Everything lease-runtime lives on the partition
  leader's worker, next to the lease view, and is owned by it outright:
  - the write hook (`CheckedWriteHook`): refusals, TTL raise, version stamping, revoke re-encode, `lastAppended`;
  - the LeaseIndex judge (member 0 of the constraint set, `VerdictSource`): the §7.2 judgment table;
  - the two intrusive hierarchical wheels (deadline, checkpoint) and the four-state entry machine;
  - the expiry/checkpoint emitter, appending one data `RecordBatch` through the new
    `PartitionReplica::appendLeaseBatch(span)`;
  - the keepalive server and its preallocated park table; `LeaseTimeToLive`; `ScanLeases` pages.
  Requests arrive through the gateway → worker op ring and answers leave through the worker return ring, as every
  view read does today. No new thread, no lock, no atomic, no `thread_local`.
- **Storage.** The cleaner gets a lease-topic mode (op from attributes, `lease_version` at value bytes `[4, 12)`, a
  second `OffsetMap` for checkpoints). Every tombstone site routes through `isTombstone`.
- **Gateway (control thread).** Routes the three new requests to the partition leader's worker, authorises them
  (`WRITE` for keepalive, `READ` for TimeToLive / ScanLeases / lease-view reads), gates them on the client version.
- **Client (`client-cpp/`).** Async-first (F8c `Completion` pattern): `LeaseTopic<T>`, a lease-only 128-byte
  `SEND_RECORD` form, `Record::leaseOp()`, `LeaseSession<Capacity, OnLost, OnAtRisk>` driven by `doWork(nowNs)`, the
  producer guard (R5) enforced on the client agent thread, `LeaseWatch` with the hash-range handoff, then
  `LeaseMutex` / `LeaseElection`.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark, CMake/Ninja, real Aeron UDP for the
in-process `BrokerNode` / `TestCluster` tests, docker compose (3 nodes) on the shared Linux test VM for e2e.

**Spec:** `broker/docs/superpowers/specs/2026-11-07-rune-lease-topics-design.md`, revision 4 ("spec 03"), plus the
Herald consistency pass settlements K15 R1–R11, F3, B28 (already folded into rev 4). This plan covers the whole spec:
§1–§21 and Appendix A (deferred, nothing to build), except the discovery library (§3, §13), dropped with Phase 3 by
the owner ruling above. The Self-review maps each section to its task.

**Builds on / order:** `main` with checked writes Parts 01–10, view-read lease (VLS Part A), namespaces Phases 1–2
and NAT egress merged. What exists today (line numbers are hints; use the quoted anchors):

- **Wire (`protocol/include/rune/protocol/`):**
  - `client/Protocol.h`: `ErrorCode` ends at `ViewUniqueInvalid = 61` (`:246`); `kClientVersion =
    kClientVersionCheckedWrites = 17` (`:403-404`); `kBrokerVersion = kBrokerVersionCheckedWrites = 10` (`:447-448`);
    templates up to `StatsResponse = 92`, 93–95 free (`Dispatch.h:70`, `:123`).
  - `client/DataMessages.h`: the Produce `acks` byte (`kAcksWireAll | kAcksWireView`, `:48-51`); `ProduceResponse`
    with the verdict trailer (`:206-227`); `GetViewRowK::kMaxKeyBytes = 128` (`:969`).
  - `client/SchemaMessages.h`: `DefineTopic` with the optional trailing `persistence` byte (`:191-263`).
  - `RecordBatch.h`: `kAttrIntentMask = 0x03`, `kAttrReservedMask = 0xFC` (`:42-45`), `kAttributesOffset = 21`.
  - `NamespaceLimits.h`: `reserved1` (`:37`), the field count (`:46`).
- **Codec:** `rune/codec/PrimType.h`: `kMaxTopicKeyBytes = 16`, `isTopicKeyType` (`:19-26`);
  `rune/codec/ViewKeyEncoding.h:16` `kMaxViewKeyBytes = 128`; `rune/catalog/CatalogEvent.h`: `Persistence` (`:63`,
  `:89`); `rune/catalog/CatalogState.h`: `Applied` (`:62-70`).
- **Replication:** `ReplicationMessages.h:481-483` (`kCheckedWrites = 5`, `kLatest`); `PartitionReplica.h:601-624`
  (`appendControlBatch`), `PartitionReplica.cpp:62-79` (`makeBatch_`, timestamp 0) and `:565-590` (`append`).
- **Storage:** `LogSegment.h:122` (`appendNoSyncRestamp`); `ProducePathImpl.h:203-214` (in-window retry → original
  base offset, known-old → −1); `LogCleaner.h:401-403` (`isTombstone(string_view)`), `LogCleaner.cpp:907-947`
  (KIP-534 horizon, legacy `nowNs` path).
- **Node (worker):**
  - `Worker.h`: `doWork` (`:1866`), `driveViews_` (`:7356`, leader-only fold), the namespace hot block `NsHot`
    (`:6798-6806`), `setLeaseClockForTest` (`:596-597`), `viewLeaseEndNs_` (the read lease, `:588`).
  - `WorkerOp.h`: op types up to `GetStats = 35`; `ReadLease = 24`.
  - `CheckedWriteHook.h:40-57` (`TopicHookFacts`, `topicHookFactsOf`), `CheckedWriteHook.cpp:54`, `:72`, `:114-132`
    (the key-0 scratch-copy patch + CRC recompute).
  - `WriteState.h:110-121` (`TopicWriteSpec` key width), `:180-200` (`setKeyOffset_`), `:240-260`
    (`checkTypedRecord`).
  - `VerdictSource.cpp:36`, `:47`, `:96`, `:155` (`addVerdictView`), `:350`, `:370-390` (judge, capacity stall,
    `growOwed`), `:475`.
  - `WorkerViewSet.cpp:1127-1143` (`rebuildKeyIndex_`); `ViewInstance.h:40-52` (`CatchingUp` / `Ready`, HW ≥
    leadership start offset).
  - `AdminWorker.cpp:1440-1475` (`buildDefine_`, the key-type refusal).
  - `rune/consensus/LeaseClock.h` (`CLOCK_BOOTTIME`, `LeaseClockFn`), `ReadLease.h`, `ReadLeaseFanout.h`.
- **View lib:** `ViewTable.h:60-90`, `ViewTable.cpp:86`; `ViewFold.cpp:177`, `:343`, `:377`; `AggFold.cpp:628`;
  `Verdict.h:18` (`VerdictViewKind { KeyIndex = 1, Unique = 2, Check = 3, TestReject = 200 }`).
- **Gateway:** `ClientGateway.cpp`: the dispatch (`:1305-1436`), `handleGetViewRowK` (`:10820-10850`, tail buffer
  `:10836`), `handleScanViewRows` (`:10852`), the `ViewScanResult` answer (`:7440-7465`).
- **Client:** `Key.h:39` (`kMaxInline = 16`); `producer/ProducerImpl.h:125-138` (`SEND_RECORD` layout, value at
  `:38`); `src/producer/ProducerImpl.cpp:661`, `:4021`; `src/producer/Producer.cpp:157-167`;
  `producer/Producer.h:89` (`Acks`), `:186` (`ProducerConfig`); `Completion.h:30-63` (`OpKind`), `:65-80`
  (`OpStatus`, `DeliveryFailed`); `TypedTopic.h:352`, `:428-430`, `:473-479`; `subscriber/Subscription.h:121-136`
  (`RecordOp`, `op()`); `Client.cpp:58` (`static_assert(Key::kMaxInline == kMaxTopicKeyBytes)`); `Client.h:349`
  (`RunnerMode { Thread, Invoker }`), `:1425` (`subscribe`); `tools/rune_schema_gen.cpp` (generated `gen::` types).
- **QL:** `ql/src/Parser.cpp`, `ql/src/Ql.cpp`, `ql/src/ConfigKeys.h`.
- **Rigs:** `broker/libs/node/tests/support/`: `TestCluster.h`, `CheckedWritesRig.h`, `CheckedWritesModel.h`,
  `CheckedWritesStressDriver.h`, `CheckedWritesViewReadRig.h`, `NamespaceLifecycleRig.h`, `AuthTrio.h`;
  `broker/docker/checked-writes-e2e.sh`, `namespace-e2e.sh`; `broker/scripts/run-regression.sh`.

**Deliverables per phase (public API surface other plans build on):**

| Phase | Delivers (stable after the phase's checkpoint) | Consumed by |
|---|---|---|
| 1 | QL `lease topic … with (lease.*)`; wire templates and codes; broker lease semantics (grant, update, revoke, keepalive, TimeToLive, Expire, Checkpoint, ScanLeases, lease view reads); client `rune/client/lease/LeaseTopic.h` (`Client::openLeaseTopic<T>`, `grant`, `update`, `revoke`, `keepAliveOnce`, `timeToLive`, `get`, `scan`, `*AndWait`), `LeaseOptions`, `LeaseResult`, `Record::leaseOp()`, `rune/client/lease/LeaseSession.h` (`LeaseSession<Capacity, OnLost, OnAtRisk>`: `add`, `remove`, `revoke`, `update`, `observe`, `close`, `isClosed`, `doWork`), `ProducerConfig::guard`, `ErrorCode::LeaseAtRisk` (client-local), `rune/client/lease/LeaseWatch.h` (`LeaseWatch<T>`, `LeaseWatchBase`, `startFromNow`, `OpKind::LeaseWatchReady`) | `herald/docs/superpowers/plans/2026-11-07-herald-02-runtime-and-rune-integration.md` (Phase 1 / Task 5 RuneAgent's `RealRune` port, Task 10 `idslots` minting, Task 16 worker slots; Phase 2 / Task 21 tenant election keys and leader map, Task 23 invites); `…-herald-04-safety-reporting-evidence-sanctions.md` (Phase 2 / Task 21: `sanction-expiry` / `strike-expiry` one-shot checkpointed leases); `…-herald-05-moderation-console-appeals-transparency.md` (Phase A / Task 6 case claims; Phase B / Tasks 16–17 appeal tokens, Task 20 break-glass grants); `…-herald-01-core-domain-and-tenancy.md` (`onAtRisk` at Tasks 29 and 40); `…-herald-00-conformance-suite.md` (the `InProcRune` double's lease conformance against this behaviour: herald-02 Phase 1 / Tasks 4–5, used by 00's `LEASE` gate) |
| 2 | `rune/client/lease/LeaseMutex.h`, `LeaseElection.h` (`campaign`, `proclaim`, `resign`, `leader`, `observe`, `isLeader`, `token`, `OpKind::LeaseCampaign`); namespace limit `max-leases`; the suspension matrix; metrics; `show lease(s)`, `revoke lease`; latency gates; docs | herald-02 (Phase 2 / Task 21 tenant lease `tenant/<nsId>`, Phase 1 / Task 15 singletons `singleton/<role>`, Phase 1 / Task 12 `herald-provisioner` setting `max-leases`); herald-01 (`LeaseElection` at Tasks 29 and 40) |

## Execution context

- **Branches.**
  - Phases 1–2: `feature/lease-topics` in this repo, created off the latest `main` (`git fetch origin main` first).
    Merge `main` in before each task if it moved and before review. Push after every task with
    `git push -u origin feature/lease-topics` (standing permission; never `main`, never force).
- **Builds:** see README.md#build and `docs/testing.md`.
  - `build` (Debug, `BUILD_TESTING=ON`, `BUILD_INTEGRATION_TESTING=ON`) for everything;
  - `build-asan` (ASan+UBSan) and `build-tsan` at the checkpoints (a client TSan build needs `-DBUILD_TESTING=ON`);
  - `build-bench` (Release, `BUILD_BENCHMARKS=ON`) for the bench tasks.
  - Re-run cmake only after a `CMakeLists.txt` edit. New test files are added to the existing test targets'
    `CMakeLists.txt` (`broker/libs/node/tests/CMakeLists.txt`, `client-cpp/CMakeLists.txt`, `protocol/CMakeLists.txt`,
    `codec/CMakeLists.txt`, `ql/CMakeLists.txt`, `broker/benchmark/CMakeLists.txt`).
- **Test gate (CLAUDE.md rule 10):**
  - **Per task:** build, then `ctest --test-dir build -R '<filter>' --output-on-failure -j8` with the task's "Run"
    filter, plus every new or changed test.
  - **Checkpoints:** `cd broker && scripts/run-regression.sh ../build 8`, then the same suite under `build-asan` and
    `build-tsan` (`-R Regression -LE latency`), broker and client. A checkpoint failure is bisected over the commits
    since the last green checkpoint; after the fix only the failed and affected tests re-run.
  - **Checkpoints:**
    - **A** after Task 4: wire, catalog, DDL, QL. No runtime yet.
    - **B** after Task 8: write hook, judge, predicates, cleaner (log semantics complete without expiry).
    - **C** after Task 13: the whole broker side (wheels, emitter, keepalive, checkpoints, reads).
    - **D** after Task 16: the client (LeaseTopic, LeaseSession, LeaseWatch).
    - **E** after Tasks 17–19 (one merged task): Phase 1 done (matrix, 3-node, stress, e2e). Herald 02 may start
      building on Phase 1.
    - **F** after Task 23 (reduced): Phase 2 done (recipes, limits). Herald P0 leadership unblocked.
      Before the final whole-branch review.
- **Docker e2e** (Tasks 19, 20) runs on the shared Linux test VM from `$RUNE_VM_HOST`: hold the shared lock
  for the whole run, work only in your own directory, remove only containers and images you created (never
  `docker system prune`), check `df` before and after, never restart the machine. Never on local Mac docker. The
  host's address and login are never written into the repo, issues or commits. Long runs go in the background.
- **Benchmarks** (Tasks 9, 11, 23) follow `broker/bench/BENCHMARKING.md`: prepare the clean box once
  (`broker/bench/vm-bench-prepare.sh`), run interleaved ABAB against the merge base, n ≥ 5 for latency and ≥ 3 for
  throughput (n ≥ 15 before claiming a small p99 shift), restore once (`vm-bench-restore.sh`), record new baselines
  in BENCHMARKING.md. If the bench box is unavailable, the task's PR says so and files one
  `enhancement` / `sev:medium` / `area:bench` issue per gate (spec §16.6); the gate still has to pass before Herald
  P0 is declared done.
- **Commits** end with the session's `Co-Authored-By:` and `Claude-Session:` trailers. Never `conan create/export/
  upload` locally.

## Scope cut (owner, 2026-11-09)

- **Reviews run per batch, not per task:** the tasks up to a checkpoint run back to back, then one review and one fix
  round cover the whole batch.
- **Per-task tests are focused:** each task runs its new and changed tests plus the suites covering the touched code
  (`--gtest_filter`), never whole binaries. The full Regression suite runs only at the checkpoints.
- **Tasks 17, 18 and 19 are one task** (interaction matrix and broker integration, seeded stress, Phase 1 e2e); its
  steps are the union of the three below.
- **Task 22 is deferred** (metrics, `show lease(s)`, `revoke lease`, CLI, and the items carried from Task 4). It is
  not part of this branch.
- **Task 23 is reduced** to shrinking `AckEntry` back to 64 bytes. The latency gates stay owed under #841–#845.
- **Task 24 is folded into each task:** every task updates the spec and docs text it changes in its own commit.

## Global Constraints

**HARD RULES (CLAUDE.md §1, checked in every task review):**
1. **One owner per state.**
   - Lease view, wheels, park table, `lastAppended`, emitter scratch: owned by the partition leader's worker thread.
     Created at leadership (lease view `Ready`), discarded at demotion or topic drop. Followers hold **no** lease
     runtime state (spec §9.1).
   - Gateway (control thread) owns routing and authorisation only; it never reads lease state.
   - Client: `LeaseSession`, `LeaseWatch`, recipes are owned by the thread that calls their `doWork`. The producer
     guard deadline is owned by the client agent thread (Decision L7); the two talk only through the client's
     existing app → agent command ring and agent → app completion ring.
   - Nothing crosses a thread except through an existing ring (`WorkerOp` ring, worker return ring, client command
     and completion rings). No new mutex, atomic-as-coordination or `thread_local`
     (`broker/scripts/check_no_atomics.py` stays green with an unchanged allowlist).
2. **Zero allocation on the hot path and in steady state.** Keepalive serving, wheel arm/re-arm/poll, the emitter, the
   park table, the write-hook patch, `LeaseSession::doWork`, `LeaseWatch` delivery and the guard check allocate
   nothing after construction / the cold-path chunk growth. Each task with such a path adds an
   `AllocWindow`-based (`tests/support/AllocWindow.h`) `*NoAllocRegression` test.
3. **No virtual calls on any lease path.** Callbacks are template functors (`OnLost`, `OnAtRisk`, handlers); no
   `std::function`.
4. **Rich-state state machines.** The wheel entry is `Armed{deadlineNs, checkpointNs}` / `Checkpointing{ckptOffset,
   park}` / `Clearing{clearOffset, park}` / `Emitting{emitOffset}` (one tagged layout in the 48-byte entry; the
   accessors expose only the active state's fields). Client: each `LeaseSession` entry is
   `Granted{lastOkSentNs} / AtRisk{since} / Lost{reason} / Revoking{corr}`; `LeaseWatch` is
   `Idle / Scanning{prefixIdx, page, resumeHash} / Buffering{cursor} / Live{} / Broken{retryAtNs}`. Every state handles every event; the table tests enumerate the product.
5. **Rule 16: no convergence timeouts.** TTL is product semantics; `extend` only moves *when* a promoted leader
   proposes. Nothing waits on wall time; every drive loop has a hard iteration cap and fails instead of hanging.
6. **Never early, never stale, never paused.** No keepalive answered `Ok` without the read lease or while a
   checkpoint is in flight; promotion arms at `T_ready + extend + R`; no `Expire`/`Checkpoint` built outside `Armed`
   or behind an unfolded client update/revoke; namespace suspension never enters the emission gate.
7. **Non-lease topics unchanged.** Every new branch on a shared path is `[[unlikely]]` on a per-partition trait bit
   already in cache; golden byte corpora prove identical output; `ProducePathBenchmark`, `WorkerProduceLatencyBench`,
   `ViewFoldBench` and `ReplicationBenchmark` ABAB unchanged (Task 23).
8. **Regression naming.** Every new test is `*Regression*`; the failing test is written first in every task and its
   mutation is recorded in the commit message.

**Numbers (symbolic in the code until Task 1 assigns them; Decision L14).** Values free at the current tip:
- error codes `kErrLeaseNotFound = 62`, `kErrLeaseLimitExceeded = 63`, `kErrLeaseTtlTooLarge = 64` (client
  `ErrorCode::LeaseNotFound`, `LeaseLimitExceeded`, `LeaseTtlTooLarge`);
- `kClientVersionLeases = 18` (= `kClientVersion`), `kBrokerVersionLeases = 11` (= `kBrokerVersion`);
- `MetadataFeature::kLeases = 6` (= `kLatest`);
- `VerdictViewKind::LeaseIndex = 4`;
- templates (Decision L1): `LeaseKeepAlive = 96`, `LeaseKeepAliveResponse = 97`, `LeaseTimeToLive = 98`,
  `LeaseTimeToLiveResponse = 99`, `ScanLeases = 100`, `ScanLeasesResponse = 101`; reserved lease range 96–111.
- `OpStatus` is unchanged; the client-local at-risk code is `ErrorCode::LeaseAtRisk = 0xFF01` in the client's
  local-code space (Decision L7), never on the wire.

**Constants:** `kMaxLeaseKeyBytes = 128`, `kLeaseMinTtlFloorMs = 1000`, `kLeaseMaxTtlMs = 9'000'000'000'000`,
`kLeaseKeepAliveMaxEntries = 256`, `kLeaseParkTableEntries = 4096`, `kLeasePromotionChunk = 4096`,
`kLeaseWheelLevels = 5`, `kLeaseWheelSlots = 1024`, `kLeaseWheelTickNs = 1'000'000`, `kLeaseEntryBytes = 48`,
`kLeaseCheckpointBatchMax = 1000`, `kScanLeasesMaxRows = 1024`, `kLeaseHandoffBufferDefault = 4096`,
`kLeaseGuardMaxProducers = 8`.

**Configuration:** no new environment variable. Every knob is a per-topic `lease.*` setting (spec §5.2) or the
namespace limit `max-leases`.

## Review Focus

1. **etcd parity, row by row (spec §4).** Expired-not-revoked keepalive waits; synchronous `Checkpoint(0)`;
   `Promote(extend)`; put does not renew; TTL raised not refused. Every difference is D1–D4.
   Tests: `LeaseKeepAliveTableRegression.*` (Task 11), `LeaseJudgeTableRegression.*` (Task 6),
   `LeasePromotionArmingRegression.*` (Task 12), `LeaseWriteHookRegression.TtlRaisedNotRefusedRegression` (Task 5).
2. **Never early (§9.2).** Keepalive acknowledged only with the read lease (`viewLeaseEndNs_ > leaseNow`); promotion
   at `T_ready`, not refold time; no `Ok` from `Checkpointing`; `Ok` after `Checkpoint(0)` folds. Attack: a move
   that keeps the old leader in the ISR. Tests: `LeaseTrioRegression.PromotionKeepsOldLeaderInIsrRegression`,
   `…CheckpointInFlightRegression` (Task 18), stress invariant I3 (Task 18), mutations M1–M4.
3. **Exactly one end, never stale (§9.3, §9.4).** `lastAppended` set by updates and revokes only, checked before every
   broker record; `stale_broker` stays 0 everywhere. Tests: `LeaseEmitterRegression.*` (Task 10),
   `LeaseRaceRegression.*` (Task 17), stress I4.
4. **Expiry never pauses (G3).** Broker appends bypass the namespace-suspension refusal; `RejectAll` refuses
   keepalives; `ReadOnly` serves them. Tests: `LeaseNamespaceRegression.*` (Task 21), stress I8.
5. **No takeover (G4).** No `upsert`, no plain delete, no force revoke, no unversioned end anywhere: wire, QL, CLI,
   client, recipes. Tests: `LeaseWriteHookRegression.RefusalTableRegression`,
   `LeaseQlRegression.RevokeLeaseIsFencedRegression` (Task 22).
6. **Fencing (§7.3).** Version = grant offset; strictly increasing per key across expiry, compaction, leader change,
   restart; a stale holder's update/keepalive/revoke is `LeaseNotFound`. Tests: `LeaseIntegrationRegression.FencingRegression`.
7. **Checkpoint compaction (§6.6).** Cleaner keeps the data row + the latest same-version checkpoint; drops
   checkpoints of ended or re-granted leases; rebuild after clean = before. Tests: `LeaseCleanerRegression.*` (Task 8).
8. **Predicate blast radius.** Every `value.empty()` site listed in Task 7 is routed or proven unreachable for lease
   topics; non-lease golden corpora byte-identical. Tests: `LeasePredicateSitesRegression.*` (Task 7).
9. **No stall (§9.7).** No lease path can stall the verdict pipeline; broker records take no rejection capacity.
   Tests: `LeaseJudgeTableRegression.BrokerRecordsNeverStallRegression`, `LeaseEmitterRegression.FullBatchContinuesNextCycleRegression`.
10. **Bounds and arithmetic.** Saturating deadlines (`INT64_MAX − ttl`, `max_ttl + extend`), wheel level 4, key 128
    vs 129, keepalive 256 vs 257 entries, park table 4096 vs 4097, ScanLeases `maxRows` 0/1/1024/1025, prefix 127/128,
    handoff buffer full. Tests in Tasks 1, 5, 9, 11, 13, 16.
11. **Client guard (R5).** At `Entered`, before the callback, every pending and unsent produce of a bound producer
    fails `LeaseAtRisk`, and nothing is sent or re-sent (no reconnect retry) until `Cleared`. Attack: a reconnect
    racing `Entered`. Tests: `LeaseSessionRegression.GuardFailsPendingAndUnsentRegression`,
    `…GuardBlocksReconnectResendRegression` (Task 15), e2e leg (Task 19).
12. **Hash-order scan and handoff (R2, §12.4).** Order by `wyhash64(key, seed = topicId)`, never the per-node probe
    seed; exclusive `asOf`; rule 3 over hash ranges; nested prefixes dropped; overflow re-subscribes from the cursor
    with nothing lost or duplicated. Tests: `LeaseWatchHandoffRegression.*` (Task 16), `ScanLeasesRegression.*`
    (Task 13).
13. **Zero alloc, no virtuals, no new atomics.** `*NoAllocRegression` in Tasks 9, 10, 11, 15, 16; the atomics
    script; a `nm`-free review of every new class for `virtual`.
14. **Vacuous tests.** Every §16.5 mutation (list M1–M20 in the Interaction matrix) turns a named test red; the
    stress invariants are checked after every step, not at the end.

## Decisions

Spec decisions (owner, call, Coordinator's call; D1–D4 divergences) are binding as written in rev 4 and the K15
settlements. The items below are open points the spec leaves to the implementation, decided here.

- **L1. Template ids.** The view range has only 93–95 free and the lease surface needs six ids. A new reserved
  range **lease 96–111** is added to the `TemplateId` comment and to `isKnownTemplate` (`Dispatch.h`):
  `LeaseKeepAlive = 96`, `LeaseKeepAliveResponse = 97`, `LeaseTimeToLive = 98`, `LeaseTimeToLiveResponse = 99`,
  `ScanLeases = 100`, `ScanLeasesResponse = 101`; 102–111 stay a gap (`UnknownTemplate`). 93–95 stay free for the
  view range.
- **L2. Lease trailer on the `View` ack** (spec §6.3 "the View ack gains a per-record `leaseVersion`"). For a lease
  topic and a session ≥ `kClientVersionLeases`, `ProduceResponse` carries, after the verdict trailer, `u8 hasLease =
  1 | u16 count | count × { i64 leaseVersion, u64 ttlMs }`, one entry per record in batch order. `leaseVersion` is the
  stamped version for an accepted grant (it equals `baseOffset + recordIndex`, which a test asserts), the row's
  version for an accepted update or revoke, and −1 for any refused record. `ttlMs` is the raised TTL for a grant and
  the row's TTL otherwise. A known-old duplicate (`baseOffset = −1`) is answered per spec §6.3's table, from the
  lease view, with `leaseVersion = −1`. `kMaxEncodedSize` grows by `3 + 16 × kMaxVerdictEntries`; the gateway scratch
  is sized from it.
- **L3. The client learns the topic's lease settings** from a lease trailer on `TopicSchemaResponse` (template 67),
  sent only to a session ≥ `kClientVersionLeases` for a lease topic: `u8 isLease = 1 | LeaseSpec fixed block`. It is
  how `LeaseOptions.ttl` unset resolves to `lease.ttl` and how `openLeaseTopic` refuses a non-lease topic
  (`InvalidArgument` locally, no wire op). An alter of `lease.ttl` reaches open handles on the next schema refresh
  (the existing `SchemaUpdate` path), never mid-op.
- **L4. Catalog encoding.** `LeaseSpec` is an optional trailing block of the `CreateTopic` catalog event body, after
  `persistence`, introduced by a presence byte `0x01`; absent = not a lease topic. `CatalogFingerprint` of a lease
  topic mixes the encoded `LeaseSpec` with the existing `mix`; a non-lease topic's fingerprint is unchanged (the
  frozen v1/v2 goldens stay byte-identical). The applier (`ApplyV2`) creates the topic and the lease `ViewObject`
  from that one event and fills `Applied.leaseViewObjectId`. `kFeatureLevelLeases` is a leader **build** rule in
  `AdminWorker` (never an apply rule), so replay never depends on the level. The alterable keys are applied by the
  existing `AlterTopicConfig` event kind, with the `lease.*` keys added to `ConfigKeys.h`.
  **Amended 2026-11-08:** implemented as catalog formatVer 4 (the admin writes V4 for every event at level ≥ 6),
  with the lease view as its own object (objectId + 1, kind `LeaseIndex`), not a trailer on a V2 event.
- **L5. System columns.** `buildDefine_` prepends `lease_version int64`, `lease_ttl_ms uint64`, `lease_holder uint64`,
  `lease_remaining_ms uint64` as field ids 0–3, in that order, and renumbers the user fields from 4 (a new topic has
  no history). Fixed-block offsets 0, 8, 16, 24, so `lease_version` sits at value bytes `[4, 12)`.
  `TopicWriteSpec::installVersion` asserts this layout for every version of a lease topic and refuses (logs, counts
  `rune_lease_layout_refused_total`, does not install) a version that breaks it. Evolution can only append.
- **L6. Where the known-old duplicate is answered.** On the leader's worker, in the produce path after
  `ProducePathImpl` reports `isDuplicate && baseOffset == −1`, by a lease-view lookup per record (the table in spec
  §6.3), before the ack is built. Never in the gateway.
- **L7. The producer guard runs on the client agent thread.** `LeaseSession::doWork` (app or agent thread) posts
  `GuardArm{ref, atRiskNs}` on the client command ring after every `Ok` (and `GuardArm{ref, kNever}` on `remove`).
  The client agent keeps a fixed table (8 producers per lease, `kLeaseGuardMaxProducers`) and checks `nowNs ≥
  atRiskNs` at the top of every duty cycle **before** it sends anything; on trip it fails every pending and unsent
  produce of the bound producers with `OpStatus::DeliveryFailed`, `errorCode = LeaseAtRisk`, marks them blocked (no
  send, no retry, no reconnect resend), and posts `GuardTripped{ref}` to the session, which then fires
  `onAtRisk(Entered)`. A later `GuardArm` with a future deadline (`Cleared`) unblocks. So "before the callback"
  holds by construction, and the bound does not depend on how often the app thread polls. In `RunnerMode::Invoker`
  both run on the caller's thread, in that order.
- **L8. Client clock.** `client-cpp/include/rune/client/detail/BootClock.h`: `CLOCK_BOOTTIME` on Linux,
  `mach_continuous_time` on macOS, injectable (`BootClockFn`). `LeaseSession`, `LeaseWatch` and the guard take
  `nowNs` from it; tests inject.
- **L9. Partitioning.** Lease keys partition with the topic's existing partitioner over the key bytes
  (`partitionForKey(bytes)`); `get`, `timeToLive`, `revoke` and keepalive batching use the same function client-side.
- **L10. ScanLeases page cap.** A page also stops at the response scratch cap (the existing `ScanViewRowsResponse`
  bound) on a hash boundary, with `done = 0`. If a single hash-tie group cannot fit, the page answers
  `MessageTooLarge` (never happens with wyhash64 in practice; tested with an injected hash).
- **L11. Park table.** One per worker, `kLeaseParkTableEntries = 4096`, a free-list of `{sessionReplySlot,
  correlationId, entryIndex u16, leaseRowId u32, next u32}`, chained per wheel entry. Full → that entry is answered
  `Throttled` with a 1 ms retry-after.
- **L12. `show leases` walks every partition** client-side, one `ScanLeases` per partition, `limit 100` total by
  default.
- **L13. Keepalive accounting.** Each entry costs one record against the namespace `produce-records-per-sec`
  through the existing `NsHot` record counter; over it, the entry is `Throttled` (the whole request is decided
  before any entry is served, so a request is either charged in full or throttled in full).
- **L14. Numbering.** Task 1 assigns the values listed in Global Constraints. If checked-writes Part 11 (or any other
  branch) merges first and takes one, Task 1 is rebased to the next free values and
  `LeaseProtocolRegression.NumbersPinnedRegression` pins the result; every other task refers to the symbols only.
- **L15. Lease view name.** `<topic>.leases`. While the lease topic exists, creating a TOPIC of that name
  is refused `TopicExists` (25) and creating a VIEW of that name is refused `ViewExists` (32); defining a lease topic whose `.leases` name is taken is refused `TopicExists`.
  `drop view <topic>.leases` is refused `InvalidRequest` ("drop the lease topic").
- **L16. QL `where path like`.** Only the form `like '<prefix>%'` (one trailing `%`, no other wildcard, prefix 0–127
  bytes) maps to a `ScanLeases` prefix; any other `where` on `show leases` is refused at parse with
  `show leases filters by key prefix only`.
- **L17, L23:** removed with Phase 3; numbers not reused.
- **L18. Real-broker clock seam.** In-process `TestCluster` / `BrokerNode` integration tests drive the lease clock
  through the existing `Worker::setLeaseClockForTest` and a new `TestCluster::advanceLeaseClock(ns)` that advances
  every node's override and the read-lease clock together. Docker e2e uses real time with short TTLs (1–15 s) and
  asserts bounds, never exact instants.
- **L19. Revoke re-encode answers `Throttled`** with the existing retry-after field = 1 ms; the client library
  resends it inside the op's own deadline (`opTimeout`).
- **L20. `LeaseRef`** is `{u16 index, u16 generation}` into the session's fixed table; a stale ref is a no-op
  returning `InvalidArgument`.
- **L21. Op bits.** Bits 2–3 of a lease-topic record's attributes: `Put = 0`, `Revoke = 1`, `Expire = 2`,
  `Checkpoint = 3`. Intent bits 0–1 as today: grant = `Put | insert(1)`, update = `Put | update(2)`, revoke =
  `Revoke | update(2)`, broker records intent 0.
- **L22. Election leader-change budget (Herald F25).** Task 17 measures keepalive outage across a forced partition
  leader change on a one-partition lease topic on the 3-node rig and pins p99 < 1.5 s as an integration assertion
  (`LeaseTrioRegression.KeepAliveOutageUnderLeaderChangeRegression`), because Herald 02 §4.3 depends on it. If it
  fails, it is an urgent blocker for Herald, fixed in this plan.
- **L23. Amendments from implementation (2026-11-08).** Spec §5.1, §6.2–§6.4, §6.6 and §7.1 carry the details.
  - Task 6 (the judge) ran before Task 5 (the hook), because the hook's revoke throttle and known-old answers read the
    judge's `judgedThrough` and the index's readiness.
  - Readiness: `leaseReady_(inst, key, leaderGen, isLeader, hwConfirmed)` (`WorkerViewSet.h`). It needs the judge
    activated for this generation, `foldCursor >= judgedThrough`, and `judgedThrough > leaseUntrackedThrough`.
  - Lease fields are fixed-width; an update or revoke version below 0 is refused, and 0 is valid; every grant is
    refused 52 while view memory is over; a non-idempotent lease write is refused; the first failing record decides,
    after the batch-wide rules.
  - The revoke re-encode carries the row's TTL and remaining. The judge does not check an update's or revoke's TTL or
    remaining.
  - Task 5c: the lease ack trailer is filled at `View`-ack fire; a known-old duplicate is answered from the index
    (grant 21, live update no verdict, else 62, entries `{-1, 0}`, `Throttled` 1 ms while not ready or the arena is
    full).
  - Task 8 keeps the live version's grant record as well as its checkpoint and its latest record. This is a merge
    gate.

## Interaction matrix

The single source of truth is `broker/libs/node/tests/support/LeaseMatrixExpectations.h`. It defines `enum class
Op`, `enum class Entity`, `enum class Timing`, `bool naRule(Op, Entity)`, `std::span<const Timing>
timingsOf(Op)`, `Outcome expect(Op, Entity, Timing)`. The unit table rig (Task 17a), the 1-node broker rig (Task 17),
the 3-node slice (Task 17), the seeded stress test (Task 18) and the e2e script (Tasks 19, 20) all read it.

**Cell meaning.** A lease `L = (k, v)` is live on partition 0 of lease topic `T` (with a user view `U` over `T`). The
op is performed against the entity; the cell asserts the op's outcome **and** that every other lease, topic and
view is unchanged (or changed exactly as `expect` says).

- **Ops (22):** `Grant`, `Update`, `Revoke`, `KeepAlive1`, `KeepAliveBatch`, `TimeToLive`, `PointRead`,
  `ScanLeases`, `WatchStart`, `BrokerExpire`, `BrokerCheckpoint`, `ClearCheckpoint0`, `AlterBounds`,
  `AlterCheckpoint`, `DropTopic`, `MutexLock`, `MutexTryLock`, `MutexUnlock`, `ElectCampaign`, `ElectProclaim`,
  `ElectResign`, `ElectObserve`.
- **Entities (9):** `SameKeyVer` (L itself), `StaleVer` (k with v′ ≠ v), `OtherKeySamePart`, `OtherPart`,
  `OtherLeaseTopic`, `NonLeaseSameWorker`, `UserView` (U), `OtherNs` (same names in another namespace),
  `OldSession` (client version 17).
- **n/a rule (`naRule`), 48 cells:**
  - `StaleVer` applies only to ops that carry a version: `Update`, `Revoke`, `KeepAlive1`, `KeepAliveBatch`,
    `BrokerExpire`, `BrokerCheckpoint`, `ClearCheckpoint0` (broker records injected stale through the emitter test
    seam: expected defensive `StaleExpiry` / `StaleCheckpoint` and `stale_broker` + 1, which is the only place the
    counter may move), `MutexUnlock`, `ElectProclaim`, `ElectResign` → 10 applicable,
    **12 n/a**.
  - `UserView` applies to `Grant`, `Update`, `Revoke`, `KeepAlive1`, `KeepAliveBatch`, `BrokerExpire`,
    `BrokerCheckpoint`, `ClearCheckpoint0`, `DropTopic` → **13 n/a**.
  - `OtherNs` applies to the 9 client-surface ops (`Grant` … `WatchStart`) plus `AlterBounds`, `AlterCheckpoint`,
    `DropTopic` → **10 n/a**.
  - `OldSession` applies to the 9 client-surface ops → **13 n/a**.
  - Every other (op, entity) is applicable.
- **Timings (28):** `Sequential`; `BackToBack` (second op before the first's ack); `AroundExpire{BeforePoll,
  AfterBuild, AfterCommit, AfterFold}`; `KeepAliveIn{Clearing, Checkpointing, Emitting, DueNotEmitted}`;
  `ContenderGrants`; `RevokeBehindUpdate`; `KnownOldDuplicate`; `HandoffOverflow`; `AtRiskPendingUnsent`;
  `LeaderChange{Uncommitted, CommittedNotFolded, Folded}` (of the in-flight `Expire` / `Checkpoint(0)` /
  `Checkpoint(R)`); `LeaderChangeGrantAck`; `LeaderChangeMidScan`; `Zombie` (leader partitioned, not yet deposed);
  `RestartOne`; `RestartAll` (between a checkpoint and the deadline); `CompactionMid`; `SuspendReadOnly`;
  `SuspendRejectAll` (each followed by resume); `AlterMidLife`; `DropWithParked`.
- **Timing cells (`timingsOf`, on `SameKeyVer` unless stated), 89:**

  | Op | Timings | # |
  |---|---|---|
  | Grant | BackToBack, LeaderChangeGrantAck, KnownOldDuplicate, ContenderGrants, RestartOne, RestartAll, CompactionMid, SuspendReadOnly, SuspendRejectAll, AlterMidLife | 10 |
  | Update | BackToBack, AroundExpire ×4, KnownOldDuplicate, SuspendReadOnly, SuspendRejectAll | 8 |
  | Revoke | AroundExpire ×4, RevokeBehindUpdate, KnownOldDuplicate, LeaderChange ×3, SuspendRejectAll | 10 |
  | KeepAlive1 | AroundExpire ×4, KeepAliveIn ×4, LeaderChange ×3, Zombie, SuspendReadOnly, SuspendRejectAll, DropWithParked, RestartOne, AtRiskPendingUnsent | 17 |
  | KeepAliveBatch | KeepAliveIn ×4 (one entry per state plus decided entries), DropWithParked | 5 |
  | TimeToLive | LeaderChange.Folded, Zombie | 2 |
  | PointRead | BackToBack (read-your-writes `minOffset`), LeaderChange.Folded | 2 |
  | ScanLeases | LeaderChangeMidScan, CompactionMid | 2 |
  | WatchStart | HandoffOverflow, LeaderChangeMidScan, CompactionMid | 3 |
  | BrokerExpire | LeaderChange ×3, RestartAll, SuspendReadOnly, SuspendRejectAll, ContenderGrants | 7 |
  | BrokerCheckpoint | LeaderChange ×3, RestartOne, RestartAll, CompactionMid, AlterMidLife | 7 |
  | ClearCheckpoint0 | LeaderChange ×3 | 3 |
  | AlterBounds, AlterCheckpoint, DropTopic | AlterMidLife, AlterMidLife, DropWithParked | 3 |
  | MutexLock, MutexTryLock, MutexUnlock | {LeaderChange.Folded, KnownOldDuplicate}, {BackToBack}, {AroundExpire.AfterFold} | 4 |
  | ElectCampaign, ElectProclaim, ElectResign, ElectObserve | {Zombie, KnownOldDuplicate, AtRiskPendingUnsent}, {AroundExpire.AfterFold}, {BackToBack}, {LeaderChange.Folded} | 6 |

- **Pinned counts** (`LeaseMatrixRegression.MatrixCountRegression`): 22 × 9 = **198 sequential cells**, **48 n/a**,
  **150 applicable**; **89 timing cells**; **239 executed**.
- **Expected outcomes.** `expect` encodes spec §21's table verbatim plus these rules, in order:
  1. `OldSession` × any → `InvalidRequest` (39) naming the version, before any lease state is touched; L unchanged.
  2. `OtherNs` × any → the answer of a missing object (byte-equal to a truly missing name), L unchanged.
  3. `NonLeaseSameWorker` × a lease-only op (`KeepAlive*`, `TimeToLive`, `ScanLeases`, `openLeaseTopic`) →
     answered like a missing id (UnknownTopic / NotAuthorized, spec §8.3) / local `InvalidArgument`; × a broker op → the non-lease partition's log
     and views byte-identical to a run without the lease activity.
  4. `OtherKeySamePart`, `OtherPart`, `OtherLeaseTopic` × any → that entity's own expected result; L unchanged.
  5. `UserView` × `Expire`/`Revoke` → row deleted in the same fold batch as the lease view's; × `Checkpoint`,
     `Clear`, keepalive → U byte-identical.
  6. `StaleVer` × client op → `LeaseNotFound` (definite), L unchanged; × broker op → defensive stale verdict.
- **Layers per cell:**
  - every applicable sequential cell: the unit table rig (in-process worker + stub gateway, Task 17a) **and** the
    1-node real broker (`LeaseMatrixTest`, Task 17);
  - timing cells: 1-node broker, except `LeaderChange*`, `Zombie`, `RestartAll` and `LeaderChangeGrantAck`, which
    run on the 3-node trio (`LeaseTrioTest`, Task 17);
  - the seeded model-checked stress test (Task 18) draws every op × entity × timing from the same enums;
  - e2e on the default config (Task 19 `lease-e2e.sh`, Task 20 election leg) covers one
    representative cell per op plus the §16.3 scenarios.
- **Mutation checks (spec §16.5), each turning the named test red** (recorded in the task's commit):
  M1 promoted arm without `extend` → `LeasePromotionArmingRegression.ExtendAddedRegression`;
  M2 arm at refold time → `LeaseTrioRegression.PromotionKeepsOldLeaderInIsrRegression`;
  M3 drop `Checkpointing` → `LeaseKeepAliveTableRegression.CheckpointingParksRegression`;
  M4 grant sets `lastAppended` → `LeaseRaceRegression.ContenderGrantsDoNotDeferExpireRegression`;
  M5 no revoke re-encode → `LeaseWriteHookRegression.RevokeReencodedFullRowRegression` and the filtered-watch leg;
  M6 known-old update answered `LeaseNotFound` while live → `LeaseWriteHookRegression.KnownOldDuplicateTableRegression`;
  M7 guarded resend after `Entered` → `LeaseSessionRegression.GuardBlocksReconnectResendRegression`;
  M8 scan by probe seed → `ScanLeasesRegression.ResumeAcrossLeaderChangeRegression`;
  M9 arm at `ttl` ignoring the checkpoint / at the checkpoint ignoring a clear →
  `LeasePromotionArmingRegression.SeededLogRegression`;
  M10 answer before `Checkpoint(0)` folds → `LeaseKeepAliveTableRegression.ClearAnswersAfterFoldRegression`;
  M11 serve without the read lease → `LeaseKeepAliveTableRegression.NoReadLeaseNotLeaderRegression`;
  M12 due lease answered `Ok` → `LeaseKeepAliveTableRegression.DueParksRegression`;
  M13 drop the `lastAppended` rule → `LeaseRaceRegression.RevokeBeforeExpireBuiltRegression`;
  M14 skip the version compare → `LeaseJudgeTableRegression.TableRegression`;
  M15 update re-arms → `LeaseIntegrationRegression.UpdateDoesNotRenewRegression`;
  M16 pause expiry under `RejectAll` → `LeaseNamespaceRegression.RejectAllStillExpiresRegression`;
  M17 accept `upsert` → `LeaseWriteHookRegression.RefusalTableRegression`;
  M18 refuse TTL 0 → `LeaseWriteHookRegression.TtlRaisedNotRefusedRegression`;
  M19 checkpoint supersedes the data row in the cleaner → `LeaseCleanerRegression.CheckpointNeverSupersedesRowRegression`;
  M20 deliver a `Checkpoint` to a user view → `LeasePredicateSitesRegression.CheckpointInvisibleToUserViewsRegression`.

## File Structure

New:
- `protocol/include/rune/protocol/LeaseRecord.h`: `LeaseOp`, `leaseOpOf(attributes)`, `withLeaseOp`, system-column
  offsets, `TopicTraits { bool lease; }`, `isTombstone`, `isLeaseControl`, `kMaxLeaseKeyBytes`.
- `protocol/include/rune/protocol/LeaseSpec.h`: the `LeaseSpec` fixed block and codec, the constants.
- `protocol/include/rune/protocol/client/LeaseMessages.h`: templates 96–101.
- `codec/include/rune/catalog/LeaseSpecCatalog.h` (+ `codec/src/catalog/LeaseSpecCatalog.cpp`): catalog trailer,
  fingerprint mix, settings validation.
- `broker/libs/node/include/rune/node/lease/`:
  - `LeaseWheel.h`: the five-level intrusive wheel (header-only, templated on the entry store);
  - `LeaseEntryStore.h`: chunked 48-byte entries indexed by lease-view row id, the four states;
  - `LeaseIndex.h` / `broker/libs/node/src/lease/LeaseIndex.cpp`: the LeaseIndex verdict member (judge + fold);
  - `LeaseRuntime.h` / `src/lease/LeaseRuntime.cpp`: per-partition owner of view instance, wheels, `lastAppended`,
    emitter, promotion pass;
  - `LeaseEmitter.h` / `src/lease/LeaseEmitter.cpp`: batch building into the per-worker scratch;
  - `LeaseParkTable.h`: L11;
  - `LeaseKeepAliveServer.h` / `src/lease/LeaseKeepAliveServer.cpp`: the §8.1 table, TimeToLive, ScanLeases pages;
  - `LeaseHash.h`: `scanHash(key, topicId) = wyhash64(key, seed = topicId)`.
- `client-cpp/include/rune/client/lease/`: `LeaseOptions.h`, `LeaseResult.h`, `LeaseTopic.h`, `LeaseSession.h`,
  `LeaseWatch.h`, `LeaseMutex.h`, `LeaseElection.h`; `client-cpp/include/rune/client/detail/BootClock.h`;
  `client-cpp/src/lease/LeaseTopic.cpp`, `LeaseWire.cpp`.
- Tests (broker): `broker/libs/node/tests/node/Lease{WriteHook,Judge,Predicate,Cleaner,Wheel,Emitter,KeepAlive,
  Checkpoint,Promotion,Scan,Ddl}Test.cpp`; `broker/libs/node/tests/integration/Lease{Matrix,Integration,Race,Trio,
  Namespace,Stress}Test.cpp`; `broker/libs/node/tests/support/{LeaseMatrixExpectations.h, LeaseRig.h, LeaseTrio.h,
  LeaseModel.h, LeaseStressDriver.h}`.
- Tests (others): `protocol/tests/LeaseProtocolTest.cpp`; `codec/tests/catalog/LeaseCatalogTest.cpp`;
  `ql/tests/LeaseQlTest.cpp`; `client-cpp/tests/Lease{Topic,Session,Watch,Recipe}Test.cpp`;
  `broker/libs/storage/tests/LeaseCleanerModeTest.cpp`.
- Benches: `broker/benchmark/Lease{Wheel,KeepAlive,ExpiryLag,Checkpoint}Bench.cpp`.
- e2e: `broker/docker/lease-e2e.sh`, `broker/docker/docker-compose.lease.yml`.

Modified files are listed per task.

---

## Phase 1: lease topics in the broker and the core client

### Task 1: protocol — op bits, predicates, codes, versions, feature level, templates, ack and schema trailers

Spec §5.3 (gates), §6.1, §6.2, §6.6 (predicates), §8.1–§8.3 (wire), §20 P1.1; L1, L2, L3, L14, L21.

**Files:**
- Create: `protocol/include/rune/protocol/LeaseRecord.h`, `protocol/include/rune/protocol/LeaseSpec.h`,
  `protocol/include/rune/protocol/client/LeaseMessages.h`; test `protocol/tests/LeaseProtocolTest.cpp`.
- Modify: `protocol/include/rune/protocol/client/Protocol.h` (codes 62–64, templates 96–101, `kClientVersionLeases`,
  `kClientVersion`, `kBrokerVersionLeases`, `kBrokerVersion`); `client/Dispatch.h` (`isKnownTemplate` lease range
  96–101; comments); `client/DataMessages.h` (`ProduceResponse` lease trailer, L2); `client/SchemaMessages.h`
  (`DefineTopic` optional `LeaseSpec` after `persistence`, presence byte; `TopicSchemaResponse` lease trailer, L3);
  `broker/libs/replication/include/rune/replication/ReplicationMessages.h` (`kLeases = 6`, `kLatest = kLeases`);
  `broker/libs/view/include/rune/view/Verdict.h` (`LeaseIndex = 4`).

**Interfaces:**
```cpp
namespace rune::protocol::record {
enum class LeaseOp : uint8_t { Put = 0, Revoke = 1, Expire = 2, Checkpoint = 3 };
inline constexpr uint8_t kAttrLeaseOpShift = 2, kAttrLeaseOpMask = 0x0C;
[[nodiscard]] constexpr LeaseOp leaseOpOf(uint8_t attrs) noexcept;
[[nodiscard]] constexpr uint8_t withLeaseOp(uint8_t attrs, LeaseOp) noexcept;
inline constexpr std::size_t kLeaseVersionValueOffset = 4, kLeaseTtlValueOffset = 12,
                             kLeaseHolderValueOffset = 20, kLeaseRemainingValueOffset = 28,
                             kLeaseSystemBytes = 32;
struct TopicTraits { bool lease = false; };
// one predicate for every tombstone site; lease topics: Revoke | Expire (value is the full row), else value.empty()
[[nodiscard]] constexpr bool isTombstone(uint8_t attrs, std::size_t valueLen, TopicTraits t) noexcept;
[[nodiscard]] constexpr bool isLeaseControl(uint8_t attrs, TopicTraits t) noexcept;   // Checkpoint on a lease topic
}
namespace rune::protocol {
struct LeaseSpec { uint64_t ttlMs, minTtlMs, maxTtlMs; uint32_t maxKeys; uint16_t expiryBatch; uint8_t checkpoint;
                   uint32_t checkpointIntervalMs, checkpointRate; };     // 47 bytes on the wire, little-endian
inline constexpr std::size_t kLeaseSpecBytes = 47;
void encodeLeaseSpec(const LeaseSpec&, uint8_t* out) noexcept;
bool decodeLeaseSpec(const uint8_t* in, std::size_t len, LeaseSpec&) noexcept;
[[nodiscard]] std::optional<std::string_view> validateLeaseSpec(const LeaseSpec&) noexcept;   // spec §5.2 ranges
}
namespace rune::protocol::client {
struct LeaseKeepAlive { int64_t sessionId, correlationId; int32_t topicId, partition; uint16_t count;
                        struct Entry { std::span<const uint8_t> key; int64_t version; }; /* count ≤ 256 */ };
struct LeaseKeepAliveResponse { int64_t sessionId, correlationId; uint8_t final; uint16_t count;
                                struct Entry { uint16_t index; uint16_t code; uint64_t ttlMs; }; };
struct LeaseTimeToLive { int64_t sessionId, correlationId; int32_t topicId, partition; std::span<const uint8_t> key; };
struct LeaseTimeToLiveResponse { int64_t sessionId, correlationId; uint16_t code; uint64_t grantedTtlMs;
                                 int64_t remainingMs /* -1 = none */, version; uint64_t holder; };
struct ScanLeases { int64_t sessionId, correlationId; int32_t viewTopicId, partition; std::span<const uint8_t> prefix;
                    uint64_t resumeHash; uint8_t first; uint16_t maxRows; };
struct ScanLeasesResponse { int64_t sessionId, correlationId; uint16_t code; int64_t asOfOffset; uint16_t rowCount;
                            std::span<const uint8_t> rows; uint64_t lastHash; uint8_t done; };
// encode/decode in the DataMessages.h style: bounds-checked, never reads past `len`, zero-copy spans on decode.
}
```

- [ ] **Step 1: failing tests** in `LeaseProtocolTest.cpp`:
  - `LeaseProtocolRegression.OpBitsRoundTripRegression`: every `LeaseOp` × every intent: `leaseOpOf(withLeaseOp(a, op))
    == op` and the intent bits unchanged.
  - `…PredicatesRegression`: table: non-lease (empty → tombstone, non-empty → not, any attrs → not control); lease
    (`Revoke`/`Expire` with a full value → tombstone; `Put` empty → not tombstone (refused upstream); `Checkpoint` →
    control, not tombstone).
  - `…NonLeaseGoldenRegression`: 64 seeded non-lease records: `isTombstone(a, len, {false}) == (len == 0)` for every
    attribute byte 0..255.
  - `…KeepAliveRoundTripRegression`: 0 (refused), 1, 256 entries with 0-, 17- and 128-byte keys; 257 refused; key 129
    refused.
  - `…KeepAliveTruncatedAtEveryOffsetRegression` and the same for the other five messages: decode of every prefix
    returns `nullopt` (ASan run checks no over-read).
  - `…ScanLeasesBoundsRegression`: `maxRows` 0 and 1025 refused, 1 and 1024 accepted; prefix 127 accepted, 128 refused.
  - `…ProduceLeaseTrailerRegression`: `hasLease` trailer after verdicts, count 1 and 4096; absent for a pre-18 session.
  - `…SchemaLeaseTrailerRegression`, `…DefineTopicLeaseSpecRegression`: present/absent; old bytes decode unchanged.
  - `…LeaseSpecValidateRegression`: each §5.2 bound at min−1, min, max, max+1; `min_ttl < 1000` refused (R11).
  - `…TemplateDispatchRegression`: 96–101 known; 93–95 and 102–111 `UnknownTemplate`.
  - `…NumbersPinnedRegression`: codes 62–64, client 18, broker 11, `kLeases = 6`, `LeaseIndex = 4` (L14).
  Mutation: swap two `LeaseSpec` fields → the golden hex prefix in `DefineTopicLeaseSpecRegression` fails; make
  `isTombstone` ignore `TopicTraits` → `PredicatesRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseProtocol|ProtocolDispatch|DataMessages|SchemaMessages' --output-on-failure -j8`.
- [ ] **Step 3:** update existing version pins (`grep -rn "kClientVersion\b\|kBrokerVersion\b\|kLatest" */tests`) to the
  new values; nothing else changes.
- [ ] **Step 4:** commit `protocol: lease op bits, predicates, codes, templates 96-101, client v18 / broker v11 / level 6` and push.

**Don't:** reuse 93–95; put the lease op in the intent bits; give `isTombstone` a value-content heuristic.

### Task 2: codec catalog — `LeaseSpec` trailer, one event for topic + view, settings, key-width rule

Spec §5.1 (key width row 1), §5.2, §5.3; L4, L15.

**Files:**
- Create: `codec/include/rune/catalog/LeaseSpecCatalog.h`, `codec/src/catalog/LeaseSpecCatalog.cpp`; test
  `codec/tests/catalog/LeaseCatalogTest.cpp`; golden `codec/tests/catalog/golden/v2/lease.rcat` and its pin.
- Modify: `codec/include/rune/codec/PrimType.h` (`kMaxLeaseKeyBytes = 128`, `isLeaseTopicKeyType(type, width)`);
  `codec/include/rune/catalog/CatalogEvent.h` (`CreateTopic` gains `std::optional<LeaseSpec> lease`);
  `codec/src/catalog/CatalogEvent.cpp` (encode/decode trailer after `persistence`); `codec/include/rune/catalog/
  CatalogState.h` (`TopicRecord.lease`, `Applied.leaseViewObjectId`); `codec/src/catalog/ApplyV2.cpp` (a lease
  `CreateTopic` also creates the `ViewObject` `<topic>.leases`, kind `LeaseIndex`, member 0; `DropTopic` drops it;
  `DropView` of it refused; `AlterTopicConfig` with `lease.*` keys validated by `validateLeaseSpec` on the merged
  spec); `CatalogFingerprintV2.h` (mix the encoded spec only when present); `codec/tools/make_catalog_golden.cpp`.

- [ ] **Step 1: failing tests** `LeaseCatalogRegression.*`:
  - `OneEventCreatesTopicAndViewRegression`: apply one lease `CreateTopic`; the state has the topic and
    `<t>.leases` with kind `LeaseIndex`; `Applied.leaseViewObjectId` set.
  - `DropTopicDropsViewRegression`; `DropLeaseViewRefusedRegression`; `LeaseViewNameTakenRegression` (L15, both
    directions).
  - `NonLeaseFingerprintUnchangedRegression`: the v1 and v2 goldens replay to their pinned fingerprints.
  - `LeaseGoldenRegression`: `lease.rcat` (create, alter `lease.ttl`, alter `lease.checkpoint=off`, drop) replays
    to its pin.
  - `AlterKeysRegression`: alterable keys accepted; `lease.max_keys` and `lease.expiry_batch` refused
    (`InvalidRequest`); `min_ttl > max_ttl` after merge refused.
  - `KeyWidthRegression`: `isLeaseTopicKeyType(FixedStr, 128)` true, 129 false; `isTopicKeyType(FixedStr, 17)` still
    false.
  Mutation: fingerprint always mixes the spec → `NonLeaseFingerprintUnchangedRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseCatalog|CatalogV2|CatalogGolden' --output-on-failure -j8`.
- [ ] **Step 3:** commit `codec: lease topics in the catalog (LeaseSpec trailer, lease view object)` and push.

**Don't:** make `kFeatureLevelLeases` an apply rule; touch `ApplyV1.cpp` or
`CatalogFingerprint.h` (frozen).

### Task 3: broker admin + write spec — lease DDL, system columns, 128-byte keys, gates

Spec §5.1 (all rows), §5.3, §6.1; L5, L15.

**Files:**
- Modify: `broker/libs/node/src/AdminWorker.cpp` (`buildDefine_` ~:1440-1475: when `DefineTopic` carries a
  `LeaseSpec`: refuse below `kFeatureLevelLeases` (`InvalidRequest`, "cluster below lease feature level"); refuse
  without a field list or key ("lease topic needs a typed, keyed topic"); refuse `autoincrement`, transient
  persistence, `cleanup.policy` ≠ compact, retention ≠ −1, any user field named `lease_*`; key through
  `isLeaseTopicKeyType` with the refusal text "a lease topic key is an integer or string(N) with N <= 128"; prepend
  the four system columns (L5); default `delete.retention.ms` = 3 600 000; `key.index` implied; raise nothing
  here — TTL raising is the write hook's); namespace accounting (`maxTopics`, partitions count; the lease view does
  not count toward `maxViews`).
- Modify: `broker/libs/node/include/rune/node/WriteState.h` (`TopicWriteSpec`: `bool lease`, `LeaseSpec leaseSpec`;
  `keyWidth` from `isLeaseTopicKeyType` for lease topics (`:118`); `installVersion` asserts and refuses a layout
  break, L5).
- Modify: `broker/libs/node/src/VerdictSource.cpp:155` (`addVerdictView`: accept key width ≤ 128 for `LeaseIndex`).
- Modify: `broker/libs/node/include/rune/node/CheckedWriteHook.h:57` (`providesKeyIndex(kind)` = `KeyIndex ||
  LeaseIndex`; `topicHookFactsOf` uses it).
- Test: `broker/libs/node/tests/node/LeaseDdlTest.cpp` (on `CatalogDdlRig.h`).

- [ ] **Step 1: failing tests** `LeaseDdlRegression.*`:
  - `RefusalTableRegression`: one row per refusal above, asserting code and text.
  - `KeyWidthRegression`: lease keys `string(17)` and `string(128)` accepted, `string(129)` refused; a non-lease
    `string(17)` still refused with the old text (R1).
  - `SystemColumnsFirstRegression`: the stored descriptor's first four fields and fixed offsets 0, 8, 16, 24; value
    byte offset of `lease_version` = 4.
  - `KeyWidthRealRegression`: `TopicWriteSpec.keyWidth == 128` for a `string(128)` lease key, so `checkTypedRecord`'s
    key = row-key rule stays on (a mismatching 128-byte key is `Reject`).
  - `LayoutBreakRefusedRegression`: a hand-built version that moves `lease_version` is not installed and counted.
  - `NamespaceCountsRegression`: `maxTopics = 1` → a second lease topic refused `NamespaceLimitExceeded`; `maxViews
    = 0` does not refuse a lease topic.
  - `FeatureLevelGateRegression`: level 5 → refused; level 6 → accepted.
  Mutation: compute `keyWidth` with `isTopicKeyType` → `KeyWidthRealRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseDdl|CheckedWritesWriteState|CheckedWritesKeyIndexWide|AdminWorker' --output-on-failure -j8`.
- [ ] **Step 3:** commit `broker: lease topic DDL, system columns, 128-byte lease keys` and push.

**Don't:** widen `kMaxTopicKeyBytes`; change any non-lease refusal text; let user fields precede system columns.

### Task 4: QL — `lease topic` DDL and the lease statements (parse + plan)

Spec §5.1, §5.2, §11; L16.

**Files (corrected 2026-11-08; the original list named files that do not exist):** `ql/src/Parser.cpp`
(`lease topic …` and `transient lease topic …` at statement level — there is no `create topic`; `show leases <topic>
[where <key> like '<prefix>%'] [limit n]`; `show lease <topic> key <literal>`; `revoke lease <topic> key <literal>`),
`ql/include/rune/ql/Ast.h` (`TopicDef::lease`; `ShowLeasesDef`, `ShowLeaseDef`, `RevokeLeaseDef` appended to
`Statement`), `ql/src/ConfigKeys.h` (`lease.*` rows and `applyLeaseSetting`, sharing the `validateLeaseSpec` texts in
`protocol/.../LeaseSpec.h`), `ql/src/Backends.cpp` + `Backends.h` (`DefineTopicPlan::lease`; script-scope view
sources carry the lease system columns), `codec/include/rune/codec/LeaseColumns.h` (the one prepend/strip helper),
`client-cpp/src/cli/QlEngine.cpp` (`run(PDefine)` sends the lease; printPlan; the three lease statements are refused at
prepare until Task 22); tests `ql/tests/LeaseQlTest.cpp` (also built into the CI target `rune_checked_writes_tests`),
`client-cpp/tests/LeaseQlCliTest.cpp`, `LeaseTopicQlBrokerRegression` in
`broker/libs/node/tests/client/CheckedWritesQlBrokerRegressionTest.cpp`; bench case `BM_LeaseTopicDdl` in
`ql/benchmark/ParseBench.cpp`.

Decisions taken during implementation:
- `transient lease topic` parses; the broker answers it (owner ruling: transient lease topics will be supported).
- QL refuses in the broker's rule order with the broker's texts.
- In `like`, only `%` is a wildcard; `_` is literal (discovery paths contain `_`).
- Encoding the literal key with the topic's key type moves to Task 22 (it needs the topic's descriptor at execution).
- The settings are named as the catalog names them: `lease.checkpoint_interval` (no `_ms`).

- [x] Done: 1e9d0dcd, review fixes 0953bacb. **Checkpoint A.**

**Don't:** execute `show`/`revoke lease` here (Task 22 wires them to the client).

### Task 5: write hook — refusals, TTL raise, version stamping, revoke re-encode, `lastAppended`, known-old duplicates, ack trailer

Spec §6.2, §6.3, §6.4, §6.5; R8, R9; L2, L6, L19.

**Files:**
- Modify: `broker/libs/node/src/CheckedWriteHook.cpp` and `.h`: a lease branch taken when `spec->lease`
  (`[[unlikely]]` on the existing per-partition trait), run on the partition's single writer immediately before
  `appendNoSyncRestamp` (`LogSegment.h:122`):
  1. refusals, in spec §6.4's order, each definite: session `< kClientVersionLeases` or level `< kLeases` →
     `InvalidRequest`; acks without `View` → `InvalidRequest` ("lease writes need acks=view"); `upsert` (intent 0 or 3
     on `Put`), `insert` on `Revoke`, client `Expire`/`Checkpoint`, empty value → `InvalidRequest`;
     `lease_remaining_ms ≠ 0` → `InvalidRequest`; update/revoke with `lease_version ≤ 0` → `InvalidRequest`; grant
     with `lease_ttl_ms > maxTtl` → `LeaseTtlTooLarge`; new-key grant while the node is over `max-leases` or view
     memory → `NamespaceLimitExceeded` (wired to the Task 21 counters; until then the check reads the existing view
     memory flag `NsViewMemOver`); namespace `ReadOnly`/`RejectAll` → `NamespaceSuspended` (the existing check);
     transactional produce → `InvalidRequest`.
  2. patch, in the existing key-0 scratch (`patchKeyZeroIds_` pattern, `:114-132`): for each grant, stamp
     `lease_version = baseOffset + recordIndex` and raise `lease_ttl_ms` to `minTtl` when below (0 included); for each
     revoke, replace the value with the lease view's row for the key re-encoded at the latest writer version, with the
     op bits kept (R9) — if the view has no row or another version, leave it unchanged (the judge answers
     `LeaseNotFound`); if `lastAppended[k] > foldCursor`, refuse the whole batch `Throttled` with retry-after 1 ms
     (L19). Recompute CRC32C over `[kAttributesOffset, len)` once. Updates are not patched (zero-copy).
  3. after the checks, for every key the batch **updates or revokes**, store the batch's last offset in
     `lastAppended[k]` (the wheel entry; a key without an entry gets none). Grants never touch it (R8).
- Modify: `broker/libs/storage/include/rune/storage/ProducePathImpl.h:203-214` call site in the worker produce path
  (`Worker.h`): on `isDuplicate && baseOffset == −1` for a lease topic, answer each record from the lease view per
  spec §6.3's table (L6).
- Modify: the worker's ack builder (the place that writes the verdict trailer for `View` acks) to append the L2 lease
  trailer.
- Test: `broker/libs/node/tests/node/LeaseWriteHookTest.cpp` (on `CheckedWritesRig.h` with a lease topic; the
  LeaseIndex member from Task 6 is stubbed by a test `LeaseViewLookup` until Task 6 lands, then switched to the
  real one in Task 6's Step 3).

- [ ] **Step 1: failing tests** `LeaseWriteHookRegression.*`:
  - `RefusalTableRegression`: every §6.4 row → its code; the batch is not stored (log end unchanged).
  - `TtlRaisedNotRefusedRegression`: TTL 0, `min−1`, `min`, `max`, `max+1` → stored TTL `min`, `min`, `min`, `max`,
    refused `LeaseTtlTooLarge`; the ack's `ttlMs` matches.
  - `VersionStampingRegression`: batches of 1, 2 and 1000 grants: `lease_version == offset` for each, CRC valid.
  - `AckTrailerRegression`: the trailer's `leaseVersion == baseOffset + i` for grants, the row version for
    updates/revokes, −1 for refused records.
  - `LastAppendedRegression`: set to the batch's last offset for updated and revoked keys; unchanged by grants (R8).
  - `RevokeReencodedFullRowRegression`: a client revoke carrying only key + version is stored with the full live row
    at the latest writer version; CRC valid; a filtered subscription on a user column receives it.
  - `RevokeBehindUpdateThrottledRegression`: an update then an immediate revoke → `Throttled` (1 ms); after the update
    folds, the resent revoke is accepted with the updated row.
  - `InWindowRetryRegression`: an idempotent retry returns the original base offset and version.
  - `KnownOldDuplicateTableRegression`: grant → `VerdictUnknown` (status 14) live or not; update → `Ok` if live else
    `LeaseNotFound`; revoke → `LeaseNotFound`.
  - `NonLeaseUnchangedRegression`: the checked-writes golden batches through the hook produce identical bytes and
    answers (`CheckedWritesHookTest` corpus).
  - `HookNoAllocRegression`: 10 000 lease batches after warm-up, zero allocations.
  Mutations M5, M6, M17, M18 (Interaction matrix).
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseWriteHook|CheckedWritesHook|CheckedWritesAutoIncHook|CheckedWritesVerdictAck' --output-on-failure -j8`.
- [ ] **Step 3:** commit `broker: lease write hook (stamping, revoke re-encode, lastAppended)` and push.

**Don't:** patch updates; let a grant set `lastAppended`; answer a known-old duplicate by matching `lease_holder`;
read any wall clock.

### Task 6: LeaseIndex judge — member 0, the §7.2 table, partition-aligned instance, rebuild per leadership

Spec §7.1, §7.2, §7.3, §9.7 (judge side).

**Files:**
- Create: `broker/libs/node/include/rune/node/lease/LeaseIndex.h`, `broker/libs/node/src/lease/LeaseIndex.cpp`: a
  `ViewTable`-backed instance (columns: user + system columns + `lease_granted_offset`, `lease_updated_offset`)
  with `prepare` / `verdictOf` / `apply` in the `VerdictSource` member shape. Rows reserved up to
  `lease.max_keys` at chunk growth (75 % `growOwed`), so the member never returns `CapacityExhausted` (§9.7).
  `apply` reports each accepted transition to the `LeaseRuntime` hook (grant fold, end fold, checkpoint fold), which
  Tasks 9–12 implement; in this task the hook is a recording stub.
- Modify: `VerdictSource.cpp` (`verdictOf_` dispatch for `LeaseIndex`; broker records never take rejection
  capacity: a stale broker record is rejected **without** `rejected.add` stalling — it is marked through the
  rejected index's reserved slot (one per partition) and counted `rune_lease_rejected_total{reason="stale_broker"}`,
  logged once); `WorkerViewSet.cpp` (`rebuildLeaseIndex_` beside `rebuildKeyIndex_` `:1127-1143`, same leadership
  generation rule, partition-aligned instance per source partition on its leader); `Worker.h:7356` (`driveViews_`
  drives lease partitions even with no user view, like a key index).
- Test: `broker/libs/node/tests/node/LeaseJudgeTest.cpp`.

- [ ] **Step 1: failing tests** `LeaseJudgeTableRegression.*`:
  - `TableRegression`: every record kind (grant, update, revoke, Expire, Checkpoint) × {absent, equal version,
    different version, at `max_keys`} → the §7.2 code and the resulting row, including `lease_remaining_ms`,
    `lease_granted_offset`, `lease_updated_offset`; update leaves version/TTL/remaining unchanged.
  - `CapacityVerdictDeterministicRegression`: a grant past `max_keys` → `LeaseLimitExceeded`; refold on a fresh
    instance gives the same verdicts.
  - `BrokerRecordsNeverStallRegression`: fill the rejected index to capacity with client rejections, then fold a stale
    broker `Expire` (injected): the judge does not stall; `stale_broker == 1`.
  - `RebuildPerLeadershipRegression`: demote/promote with a new generation rebuilds from the local log; the same
    generation does not.
  - `ProvidesKeyIndexRegression`: `topicHookFactsOf` reports `keyIndexActive` for a lease topic.
  - `TokensOnlyGrowRegression`: grant, expire, grant again → the second version is larger.
  Mutation M14 (skip the version compare on update) → `TableRegression` fails.
- [ ] **Step 2:** implement; switch Task 5's `LeaseViewLookup` stub to this instance. Run `ctest --test-dir build -R 'LeaseJudge|LeaseWriteHook|CheckedWritesVerdictSource|CheckedWritesKeyIndex' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

**Don't:** checkpoint the lease view (it is rebuilt per leadership); keep deadlines in view columns.

### Task 7: predicates at every tombstone site

Spec §6.6 (tombstones, checkpoints invisible), §15.1 (non-lease cost).

**Files (each site routed through `isTombstone(attrs, len, traits)` / `isLeaseControl(attrs, traits)`, with the trait
bit read from the per-partition write spec or view source already in cache):**
- `broker/libs/view/src/ViewFold.cpp:177`, `:343`, `:377` (user views: lease `Revoke`/`Expire` delete the row;
  `Checkpoint` skipped before any decode);
- `broker/libs/view/src/AggFold.cpp:628` (same rule for aggregates);
- `broker/libs/node/src/VerdictSource.cpp:36`, `:96` (removed / stats), `:350` (TestReject: tombstones never rejected),
  `:475` (writer version only for non-tombstones; a lease tombstone has a value, so it gets its writer version);
- `broker/libs/node/src/CheckedWriteHook.cpp:54`, `:72` (the insert-needs-a-value rule already holds; prove lease
  revokes take the lease branch first);
- `broker/libs/node/include/rune/node/WriteState.h:247` (`checkTypedRecord`: a lease `Revoke`/`Expire` carries a
  full row and is validated as a row, never as a key-only tombstone);
- `broker/libs/storage/include/rune/storage/LogCleaner.h:401` (Task 8 replaces it with lease mode);
- `client-cpp/include/rune/client/subscriber/Subscription.h:131-132` (`op()` returns `Delete` for `Revoke`/`Expire`
  when the record's topic is a lease topic; `Record` gains `bool leaseTopic` set by the subscription at bind);
- `client-cpp/include/rune/client/TypedTopic.h:473-479`, `client-cpp/src/producer/Producer.cpp:157-167`,
  `ProducerImpl.cpp:661`, `:4021`: client send paths — proven unreachable for lease topics because lease writes go
  only through `LeaseTopic<T>` (Task 14), which never sends an empty value; a `static_assert`-free runtime test
  pins that `TypedTopic<T>::open` on a lease topic is refused locally.
- **Amended 2026-11-08.** The client sites above (`Subscription.h`, `TypedTopic.h`, `Producer.cpp`, `ProducerImpl.cpp`)
  move to Task 14: the client has no per-subscription lease flag until `LeaseTopic<T>` exists. Broker side, the data
  folds normalize in one place (`leaseDataView` in `FoldSkip.h`, used by `walkCommitted`, `walkAggregate` and the
  verdict members other than the lease index): a `Checkpoint` is skipped and a `Revoke` / `Expire` is folded as an
  empty value, so the existing delete paths at `ViewFold.cpp:177/343/377` and `AggFold.cpp:628` are unchanged. The
  trait comes from the partition's write spec through `FoldSkip::traits`.
- Step 0 of the task re-runs `grep -rn "value\.empty()\|value()\.empty()" --include=*.h --include=*.cpp broker/libs
  client-cpp/include client-cpp/src codec/src codec/include | grep -v /tests/` and the reviewer checks the list above
  equals its output (21 sites at the plan's tip); any new site is added.
- Test: `broker/libs/node/tests/node/LeasePredicateTest.cpp`, `client-cpp/tests/LeaseTopicTest.cpp` (subscription
  part).

- [ ] **Step 1: failing tests** `LeasePredicateSitesRegression.*`: one test per site: a lease `Expire` with a full row
  deletes the user-view row; `Checkpoint` leaves user views and aggregates byte-identical
  (`CheckpointInvisibleToUserViewsRegression`); `op()` is `Delete` for both ends; `NonLeaseGoldenCorpusRegression`
  replays the `ViewFoldBench` / `CheckedWritesModel` corpora with the predicates and compares view bytes with the
  pinned pre-change result.
  Mutation M20.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeasePredicate|ViewFold|AggFold|CheckedWrites' --output-on-failure -j8`.
- [ ] **Step 3:** quick ABAB (n = 3) of `ViewFoldBench` and `ProducePathBenchmark` against the merge base; record
  numbers in the commit (the verdict is Task 23's).
- [ ] **Step 4:** commit and push.

**Don't:** decode a schema to classify a record; add a branch that non-lease topics take without `[[unlikely]]`.

### Task 8: cleaner lease mode

Spec §6.6 (compaction, cleaner reading), §16.1 (cleaner).

**Files:** modify `broker/libs/storage/include/rune/storage/LogCleaner.h` and `broker/libs/storage/src/LogCleaner.cpp`:
a `CleanerTopicMode { Plain, Lease }` passed per log by `LogCleanerAgent` from the topic's traits. In `Lease` mode:
the op comes from per-record attributes; `lease_version` read at value bytes `[4, 12)` (LE int64); a second
`OffsetMap` holds the latest `Checkpoint` per key and its version; keep rule: the latest non-`Checkpoint` record per
key (as today), plus the live version's grant record (offset = `lease_version`; amended 2026-11-08, merge gate), plus
that key's latest `Checkpoint` iff its version equals the kept record's and the kept record is not a tombstone; tombstone horizon = the cleaner's own KIP-534 stamp only — assert `nowMs ≥ 0` and never run the
legacy `nowNs` path (`:942-947`) for a lease log; rejected records removed as in checked writes §8.5. Modify
`broker/libs/node/include/rune/node/LogCleanerAgent.h` to pass the mode. Test:
`broker/libs/storage/tests/LeaseCleanerModeTest.cpp` and `broker/libs/node/tests/node/LeaseCleanerRebuildTest.cpp`.

- [ ] **Step 1: failing tests** `LeaseCleanerRegression.*`:
  - `KeepsGrantOfLiveVersionRegression`: grant, 100 updates with other TTLs, clean, rebuild: the row's TTL is the
    grant's;
  - `KeepsRowAndSameVersionCheckpointRegression`; `DropsCheckpointOfEndedLeaseRegression`;
    `DropsCheckpointOfRegrantedLeaseRegression`; `CheckpointNeverSupersedesRowRegression` (M19);
  - `VersionOffsetAcrossSchemaVersionsRegression`: three writer versions (columns appended), `lease_version` read
    correctly;
  - `TombstoneHorizonIsCleanerStampRegression`: broker records with timestamp 0 are kept until the KIP-534 horizon;
  - `RebuildAfterCleanEqualsBeforeRegression`: 10 000 updates and 100 checkpoints per key over 50 keys, clean, rebuild
    the lease view: rows, versions and `lease_remaining_ms` identical;
  - `PlainModeUnchangedRegression`: the existing `CompactionBenchmark` corpus cleans byte-identically.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseCleaner|LogCleaner|Compaction' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push. **Checkpoint B.**

**Don't:** decode schemas in the cleaner; key retention on a record timestamp for lease logs.

### Task 9: the wheels, the entry store and the four-state machine

Spec §9.1 (saturating), §9.6 (states), §10; R4, R11.

**Files:** create `broker/libs/node/include/rune/node/lease/LeaseWheel.h` and `LeaseEntryStore.h`; test
`broker/libs/node/tests/node/LeaseWheelTest.cpp`; bench `broker/benchmark/LeaseWheelBench.cpp`.

**Algorithm and invariants:**
- Agrona `DeadlineTimerWheel`-style hierarchical hashed wheel, 5 levels × 1024 slots, base tick 1 ms (level `l`
  tick = 1024^l ms). Intrusive doubly linked lists by `u32` row id inside the entry; no hash, no allocation.
  `arm(row, deadlineNs)` places at the lowest level whose span covers `deadline − now`; `poll(nowNs, sink, max)`
  advances the cursor, cascades higher-level slots whose tick passed (each entry cascades ≤ 4 times), and hands due
  rows to `sink` up to `max`; O(due + slots passed); an idle partition costs one compare (`nowNs < nextCheckNs`).
- Entry (48 bytes, `static_assert`): `deadlineNs`, `checkpointNs`, `inflightOffset` (i64 each; the one
  `emitOffset`/`ckptOffset`/`clearOffset` field), deadline links and checkpoint links (`u32` ×4), `parkHead u32`,
  `levelD u8`, `levelC u8`, `state u8`, `pad`. `lastAppended` stays in the lease table's aux (`kLeaseAuxBytes = 8`,
  `LeaseIndex.h`, written by the produce hook); it is not in the entry (with it the entry would be 56 bytes). States `Armed`, `Checkpointing`, `Clearing`,
  `Emitting`; only `Armed` is linked in the deadline wheel for emission; transitions per spec §9.6's table; demotion
  discards the whole store.
- Saturating arithmetic: `satAdd(a, b) = b > INT64_MAX − a ? INT64_MAX : a + b`; a saturated deadline lands in level 4
  and is never due.
- Storage: chunks of 4096 entries, grown on the cold path at 75 % use (`growOwed`), never past `max_keys`, never
  freed while the partition lives.

- [ ] **Step 1: failing tests** `LeaseWheelRegression.*`:
  - `BoundariesRegression`: arm/re-arm/cancel/cascade at 0, 1, 1023, 1024, 1025 ticks of every level; `MaxLeaseTTL`
    and `+1 ms`; poll returns exactly the due set (compared with a sorted reference model over 100 000 seeded ops).
  - `StateMachineRegression`: every state × every event (fall due, gate open/closed, keepalive, checkpoint due, own
    record folds, other record folds, demotion): the next state and that nothing is emitted outside `Armed` (R4).
  - `SaturationRegression`: `INT64_MAX − ttl`, `max_ttl + extend` → `INT64_MAX`, never due, level 4.
  - `ChunkGrowthNoAllocRegression`: zero allocations after the high-water mark across 10⁶ arm/re-arm/poll.
  - `IdlePollOneCompareRegression`: with 1 M armed and nothing due, `poll` touches no entry (instrumented counter).
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R LeaseWheel --output-on-failure -j8`.
- [ ] **Step 3:** `LeaseWheelBench` (arm, re-arm, poll at 1 k / 100 k / 1 M / 16 M); record; gates (arm/re-arm p99 <
  50 ns, idle poll with 1 M < 100 ns) are asserted in Task 23.
- [ ] **Step 4:** commit and push.

**Don't:** use `std::map`/`priority_queue`; allocate per arm; store deadlines in the view.

### Task 10: the expiry emitter and `PartitionReplica::appendLeaseBatch`

Spec §9.1 (gate, appending), §9.3, §9.4, §9.7 (emitter side), §10 (scratch); R4, R8.

**Files:**
- Create: `broker/libs/node/include/rune/node/lease/LeaseRuntime.h`, `src/lease/LeaseRuntime.cpp` (per lease
  partition on its leader: owns the `LeaseIndex` instance, the entry store, both wheels, the emitter state; created
  when the lease view enters `Ready`, discarded on demotion/drop), `LeaseEmitter.h`, `src/lease/LeaseEmitter.cpp`.
- Modify: `broker/libs/replication/include/rune/replication/PartitionReplica.h` and `src/PartitionReplica.cpp`: add
  `Offset appendLeaseBatch(std::span<const uint8_t> batch)` — the data-batch sibling of `appendControlBatch`: same
  role gate (`kAppendNotLeader`), min-ISR gate (`kAppendMinIsr`, nothing appended), `acksAll = true`, base offset
  restamped in place (`appendNoSyncRestamp`) and `stampLeaderEpoch`, both before the CRC region, same post-append
  sequence; no `makeBatch_` wrapping.
- Modify: `Worker.h` (`doWork` calls `leaseDuty_(leaseNow)` for owned lease leaders after `driveViews_`; the
  per-worker scratch `min(1 MiB, ns maxBatchBytes when set)`).

**Algorithm:** each duty cycle, for each lease partition led here: read `leaseNow` once; if the emission gate is
open (view `Ready` under the current generation, HW ≥ leadership start offset), poll the deadline wheel for up to
`lease.expiry_batch` due `Armed` entries with `lastAppended ≤ foldCursor` (others stay due; `rune_lease_expiry_deferred_total`
counts them); build one `RecordBatch`: producer fields −1, timestamps 0, each record = the lease key, the row copied
from the lease view re-encoded at the latest writer version, op `Expire`; CRC32C once; `appendLeaseBatch`. On
success each entry → `Emitting{offset}`; on `kAppendMinIsr` entries stay `Armed` and due
(`rune_lease_expiry_backlog`); on `kAppendNotLeader` stop. The namespace suspension state is **not** consulted. A
full batch continues next cycle. The fold of an accepted `Expire` frees the entry and answers parked keepalives
`LeaseNotFound` (Task 11 wires the answers).

- Test: `broker/libs/node/tests/node/LeaseEmitterTest.cpp` (worker rig with an injected lease clock).

- [ ] **Step 1: failing tests** `LeaseEmitterRegression.*`:
  - `BatchShapeRegression`: producer fields −1, timestamps 0, CRC valid after restamp, op bits `Expire`, full row.
  - `MinIsrAppendsNothingRegression`; `NotLeaderStopsRegression`.
  - `GateRegression`: not `Ready`, HW below the start offset, `lastAppended > foldCursor` → nothing built; opening the
    gate emits next cycle.
  - `OnlyArmedEmitsRegression`: due entries in `Clearing`/`Checkpointing`/`Emitting` are not emitted (R4).
  - `RevokeBeforeExpireBuiltRegression` / `RevokeAfterExpireRegression` (§9.3 both orders) → exactly one end,
    `stale_broker == 0` (M13).
  - `SuspensionDoesNotPauseRegression`: `ReadOnly` and `RejectAll` → the `Expire` still appended.
  - `FullBatchContinuesNextCycleRegression`: 10 000 due at batch 256 drain in 40 cycles.
  - `EmitterNoAllocRegression`.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseEmitter|LeaseJudge|PartitionReplica' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

**Don't:** use `appendControlBatch` or `append()`; retry an append; read a wall clock; consult namespace suspension.

### Task 11: keepalive and TimeToLive serving, the park table, gateway routing

Spec §8.1, §8.2, §9.3 (keepalive race), §9.6 (demotion, drop); L11, L13.

**Files:**
- Create: `broker/libs/node/include/rune/node/lease/LeaseParkTable.h`, `LeaseKeepAliveServer.h`,
  `src/lease/LeaseKeepAliveServer.cpp`.
- Modify: `WorkerOp.h` (op types `LeaseKeepAlive = 36`, `LeaseTimeToLive = 37`, `ScanLeases = 38` (served in Task
  13)); the worker return codec (answers per entry with correlation id and `index`; `final`); `Worker.h` (dispatch).
- Modify: `broker/libs/gateway/src/ClientGateway.cpp` (dispatch 96, 98, 100: decode, session version gate
  (`InvalidRequest` below 18), namespace-scoped topic resolution (foreign = missing), authorisation (`WRITE` for
  keepalive, `READ` for TimeToLive/Scan, new permission mapping), route to the partition leader's worker op ring
  (`NotLeader` if not led here), forward every worker answer frame for that correlation id).

**Per-entry algorithm (spec §8.1 table, top to bottom, on the leader's worker):** not leader / view not `Ready` /
`viewLeaseEndNs_ ≤ leaseNow` → `NotLeader`; namespace `RejectAll` → `NamespaceSuspended`; row absent or version ≠ →
`LeaseNotFound`; `Armed`, not due, `remaining == 0` → re-arm at `satAdd(leaseNow, ttl)`, `Ok(ttlMs)`; `Armed`, not
due, `remaining > 0` → gate open (Task 10's gate incl. `lastAppended`) ? append `Checkpoint(0)` via
`appendLeaseBatch`, → `Clearing`, park : park in `Armed` and retry the append when the gate opens; `Clearing` →
park on the same clear; `Checkpointing` → park; due or `Emitting` → park until the `Expire` folds; demotion →
every parked entry `NotLeader`; topic drop applied → `UnknownTopic`. Folds: `Checkpoint(0)` folded → re-arm, answer
parked `Ok`; `Checkpoint(R)` folded → `Armed`, parked entries go through the `remaining > 0` row; `Expire` or a
`Revoke` folded → parked entries `LeaseNotFound`. The first response carries every entry decided at once with `final
= 0` if any parked; each parked entry answers in its own response; the last has `final = 1`. Rate: the request is
charged `count` records against `produce-records-per-sec` before any entry is served (L13). `ReadOnly` serves.
TimeToLive: `remainingMs = floor((deadline − leaseNow)/1e6)`, −1 if absent, `READ`.

- Test: `broker/libs/node/tests/node/LeaseKeepAliveTest.cpp` (worker rig, injected lease clock and read lease) and
  `broker/libs/gateway/tests/LeaseGatewayRouteTest.cpp`.

- [ ] **Step 1: failing tests** `LeaseKeepAliveTableRegression.*`, one per §8.1 row and the spec §16.1 keepalive list:
  `NoReadLeaseNotLeaderRegression` (M11), `NotReadyNotLeaderRegression`, `RejectAllRegression`,
  `ReadOnlyServedRegression`, `AbsentAndWrongVersionRegression`, `ArmedNoCheckpointRegression`,
  `ArmedCheckpointClearsRegression` and `ClearAnswersAfterFoldRegression` (M10), `ClearBehindClosedGateRegression`
  (not `Ready`; an update in the tail), `DueParksRegression` (M12), `EmittingParksRegression`,
  `ClearingParksRegression`, `CheckpointingParksRegression` (M3), `BatchMixedAnswersRegression` (decided at once,
  parked later by `index`, last `final = 1`), `DropWithParkedUnknownTopicRegression`, `ParkTableFullThrottledRegression`
  (4096 then 4097), `ParkedThenDemotedRegression`, `KeepAliveBeforePollReArmsRegression`,
  `KeepAliveAfterBuildParksRegression`, `RateChargedPerEntryRegression`, `TimeToLiveRegression`,
  `KeepAliveNoAllocRegression`; gateway: `VersionGateRegression`, `ForeignNamespaceIsMissingRegression`,
  `AuthorisationRegression` (keepalive with READ only → `NotAuthorized`).
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseKeepAlive|LeaseGatewayRoute|LeaseEmitter' --output-on-failure -j8`.
- [ ] **Step 3:** `LeaseKeepAliveBench` skeleton (1-entry and 256-entry, ABAB against `GetViewRowServeBench`'s point
  read); record; the gate is Task 23's.
- [ ] **Step 4:** commit and push.

**Don't:** write any log record for a plain keepalive; set a broker-side park timeout; answer `Ok` before the clear
folds; hold a whole request for its slowest entry.

### Task 12: checkpoints and promotion arming

Spec §9.2, §9.5, §9.6 (promotion pass, table); R3, R4.

**Files:** modify `LeaseRuntime.cpp` / `LeaseEmitter.cpp`: the checkpoint wheel (only leases with `ttl >
checkpoint_interval_ms` when `lease.checkpoint = on`), armed at `grantFold + interval` and after each checkpoint
folds; emission: `Checkpoint(remaining = ceil_ms(deadline − leaseNow))`, skipped when remaining ≤ interval, same
gate, at most `checkpoint_rate`/s (a token bucket on the lease clock, preallocated) and `kLeaseCheckpointBatchMax`
per batch, shared scratch; appending moves the entry to `Checkpointing{offset}`; the fold returns it to `Armed`.
Promotion: at the lease-clock time `T_ready` the view enters `Ready` (observed in `driveViews_`'s transition, not at
refold), arm every live row at `satAdd(T_ready, extend, remaining > 0 ? remaining : ttl)` with `extend =
leaderHeartbeatTimeoutNs` from the node config, linking in chunks of `kLeasePromotionChunk` per duty cycle; a
keepalive for a not-yet-linked row is served as `Armed` and links it. Alters of the checkpoint keys apply to
checkpoints scheduled after the catalog event. Test: `broker/libs/node/tests/node/LeaseCheckpointTest.cpp`,
`LeasePromotionTest.cpp`.

- [ ] **Step 1: failing tests**
  - `LeaseCheckpointRegression.*`: `OnlyWhenTtlAboveIntervalRegression`, `SkippedWhenRemainingBelowIntervalRegression`,
    `RateAndBatchCapsRegression` (1 M one-shot leases at 1 000/s: never more per second, never > 1000 per batch),
    `CeilRoundingRegression`, `ClearedByFirstKeepAliveOnlyRegression`, `AtMostTwoRecordsPerIntervalRegression`,
    `CheckpointKeyAlterAppliesLaterRegression`.
  - `LeasePromotionArmingRegression.*`: `ExtendAddedRegression` (M1), `SeededLogRegression` (seeded log of grants,
    checkpoints and clears; every key armed at exactly `T_ready + extend + (remaining or ttl)`; M9),
    `ArmsAtReadyNotRefoldRegression` (refold done at t₀, `Ready` at t₁ > t₀ → deadline uses t₁; M2 unit twin),
    `ChunkedLinkingRegression` (1 M leases, ≤ 4096 linked per cycle, a keepalive for an unlinked key links it),
    `NotLinkedNeverExpiresEarlyRegression`.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseCheckpoint|LeasePromotion|LeaseKeepAlive' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

**Don't:** spread promotions (etcd's spreading is a non-goal); arm from `refoldJudgedNow`; checkpoint when off.

### Task 13: reads — lease-view point reads, `ScanLeases` in hash order

Spec §7.1 (reads), §8.3; R2; L10.

**Files:** create `broker/libs/node/include/rune/node/lease/LeaseHash.h`; modify `LeaseKeepAliveServer.cpp` (ScanLeases
page: one full walk of the lease `ViewTable` in one duty cycle; rows whose key starts with `prefix` and whose
`scanHash(key, topicId) > resumeHash` (any when `first`); keep the `maxRows` smallest hashes in a preallocated
fixed-capacity heap (1024 + tie headroom), extended to the hash boundary; `asOfOffset` = fold cursor at build;
`lastHash`; `done`; L10 byte cap); `ClientGateway.cpp` (route 100 by `partition` to its leader; `getViewRow` /
`getViewRowK` on a lease view authorised as `READ` on the **topic** (new mapping in `compilePerms_`); the lease
view's readiness gate is the existing one). Test: `broker/libs/node/tests/node/LeaseScanTest.cpp`.

- [ ] **Step 1: failing tests** `ScanLeasesRegression.*`: `HashOrderPagesRegression`; `HashTiesInOnePageRegression`
  (injected colliding hash); `ResumeAcrossLeaderChangeRegression` (M8: resume token from leader A works on leader B
  with a different probe seed); `ResumeAcrossTableGrowRegression`; `PrefixFilteredOnBrokerRegression` (only
  matching rows cross the ring); `AsOfExclusiveRegression`; `ChurnBetweenPagesRegression` (scan + later records = a
  fresh fold); `MaxRowsBoundsRegression`; `ByteCapStopsOnHashBoundaryRegression`; `PointReadReadYourWritesRegression`
  (`minOffset` = grant ack offset); `ReadOnTopicAuthorisesViewReadRegression`; `ScanNoAllocRegression`.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'ScanLeases|LeaseScan|CheckedWritesViewRead' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push. **Checkpoint C.**

**Don't:** reuse `ScanViewRows` (routes to partition 0, no `asOf`); order by the table's probe seed.

**As built (Task 13b).** The scan hash is `wyhash64(key, seed = source topic id)`, not xxh3 (spec §8.3): the storage tables already link
it; `ScanLeasesRegression.ScanHashGoldenRegression` pins it. The page is selected in `Worker::executeScanLeases_` (worker op 38,
return `ScanLeasesResult = 13`) from one walk of the rids with a bounded max-heap plus a 64-entry tie list, then heapsorted; the
gateway (`handleScanLeases`) requires a lease view id and authorises READ on the SOURCE topic. The lease-view sink frame also carries the
key type and width, so the gateway narrows a legacy u64 `GetViewRow` with the worker's helper before it picks the partition.

### Task 14: client — `LeaseTopic<T>`, the lease `SEND_RECORD` form, `Record::leaseOp()`, codegen

Spec §5.1 (client row), §6.6 (consumers), §12.1; L2, L3, L8, L9, L19.

**Files:**
- Create: `client-cpp/include/rune/client/lease/LeaseOptions.h` (`ttl` (0 = topic `lease.ttl`), `holder u64`,
  `opTimeout` (default `ttl/3`), `maxDelay` (100 ms), `atRiskFraction` (1/2)), `LeaseResult.h`, `LeaseTopic.h`,
  `client-cpp/src/lease/LeaseTopic.cpp`, `LeaseWire.cpp`, `client-cpp/include/rune/client/detail/BootClock.h`.
- Modify: `Completion.h` (`OpKind`: `OpenLeaseTopic`, `LeaseGrant`, `LeaseUpdate`, `LeaseRevoke`, `LeaseKeepAlive`,
  `LeaseTimeToLive`, `ScanLeases`, `LeaseCampaign`, `LeaseWatchReady`; `value` = version / `asOfOffset`, `aux` =
  `ttlMs`); `Client.h` / `Client.cpp` (`openLeaseTopic<T>(name, target, token)`, `openLeaseTopicAndWait<T>`;
  `Client.cpp:58` static_assert kept for `Key`, plus `static_assert(kLeaseKeyInline == kMaxLeaseKeyBytes)`);
  `producer/ProducerImpl.h` (a second, lease-only `SEND_RECORD` layout with a 128-byte key slot, selected per topic
  at open, fixed-size; non-lease layout unchanged); `subscriber/Subscription.h` (`Record::leaseOp()`: `Granted` iff
  `lease_version == offset`, `Updated`, `Revoked`, `Expired`, `Checkpoint`; the subscription drops `Checkpoint`
  before the handler); `tools/rune_schema_gen.cpp` (generated `T::withLease(version, ttlMs, holder)`; no setter for
  `lease_remaining_ms`; generated `T…LeasesRow` for the lease view); client gating on `kBrokerVersionLeases`.
- Test: `client-cpp/tests/LeaseTopicTest.cpp` (stub transport) and `client-cpp/tests/LeaseTopicBrokerTest.cpp` (1-node
  real broker).

- [ ] **Step 1: failing tests** `LeaseTopicRegression.*`: every op's success and error completion with the right
  `OpKind`, `value`, `aux`, `errorCode` (typed `LeaseNotFound`, `LeaseLimitExceeded`, `LeaseTtlTooLarge`; `KeyExists`;
  grant `VerdictUnknown`); `exactly one completion per accepted op`; synchronous rejection on a non-lease topic (L3);
  `RevokeThrottledResentRegression` (L19); `opTimeout` → `Timeout`; 17- and 128-byte keys through the lease form,
  129 refused locally; `NonLeaseSendRecordUnchangedRegression` (the existing `ProducerEncodeBench` golden bytes);
  `LeaseOpClassificationRegression`; `CheckpointDroppedRegression`; `WithLeaseCodegenRegression`;
  `AsyncNoAllocRegression` (steady-state grant/update/revoke/keepAliveOnce); real broker: `GrantKeyExistsRevokeRegrantRegression`
  (second version larger), `GetMinOffsetRegression`.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseTopic|ProducerEncode|Subscription' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

**Don't:** let an agent thread call `*AndWait`; widen `Key::kMaxInline`; use `std::function`.

### Task 15: client — `LeaseSession<Capacity, OnLost, OnAtRisk>` and the producer guard

Spec §12.2; R5, R7, F1 (Herald), F19; L7, L8, L20.

**Files:** create `client-cpp/include/rune/client/lease/LeaseSession.h`; modify `producer/Producer.h` (`ProducerConfig::guard`:
up to 8 producers per `LeaseRef`, on the session's client), the client agent duty cycle (`client-cpp/src/agent/`:
the guard table and the pre-send check, L7), the command ring (`GuardArm`, `GuardTripped`); test
`client-cpp/tests/LeaseSessionTest.cpp`.

**Algorithm:** a fixed table of `Capacity` entries, each a rich state (`Granted{lastOkSentNs, nextSendNs}`,
`AtRisk{since}`, `Revoking{corr}`, `Lost{reason}`), keyed `(topic, partition, keyBytes ≤ 128, version, ttlMs)`.
`add` validates `ttl/2 + d < ttl·(1 − 2ρ)` (`InvalidArgument`). `doWork(nowNs)`: per `(topic, partition)` group,
send one `LeaseKeepAlive` of ≤ 256 due entries, spreading entries across the `ttl/3` period (each key's
`nextSendNs` offset by its slot index); completions from a preallocated ring, one per entry; `Ok` sets
`lastOkSentNs = sendNs`, `M = satAdd(sendNs, ttl·(1 − 2ρ)) − d`, posts `GuardArm(ref, atRiskNs = min(sendNs +
ttl·atRiskFraction, M − d))`; `onAtRisk(Entered)` fires once per episode when `GuardTripped` arrives (or at
`atRiskNs` for an unguarded lease); a later `Ok` before `M` fires `Cleared`; `M` passing, `LeaseNotFound`,
`UnknownTopic` (`reason = TopicDropped`), or a watched end for `(key, version)` fires `onLost`, drops the key, and
sends one best-effort fenced revoke if connected. `observe(watch)` connects a `LeaseWatchBase`. `close()` revokes
every key fenced, async; `isClosed()`. `update(ref, rowBytes)` is a separate fenced write.

- [ ] **Step 1: failing tests** `LeaseSessionRegression.*` (stub transport, injected clock):
  `CadenceTtlThirdRegression`; `BatchingPerTopicPartitionRegression` (two topics, 8 partitions: one request per
  `(topic, partition)`); `SpreadNotBurstyRegression`; `MaxDelayValidationRegression`; `OnLostEachTriggerRegression`;
  `AtRiskEachCauseRegression` (error `NotLeader`/`Throttled`/`NamespaceSuspended`, timeout, missing reply, disconnect,
  long park → `Entered` at `t + ttl/2`, then `Cleared` on a late `Ok`); `AtRiskOncePerEpisodeRegression`;
  `GuardFailsPendingAndUnsentRegression` (R5: pending and unsent fail `LeaseAtRisk` before the callback);
  `GuardBlocksReconnectResendRegression` (M7: reconnect after `Entered` resends nothing until `Cleared`);
  `GuardNineProducersRefusedRegression`; `LocalOnLostRevokesBestEffortRegression`; `CloseRevokesAllRegression`;
  `StaleRefNoopRegression`; `SessionNoAllocRegression`; `InvokerModeOrderRegression` (guard check before
  `doWork`'s callback in `RunnerMode::Invoker`).
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseSession|Producer' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

**Don't:** call user callbacks from the agent thread in `Thread` mode; let `onAtRisk` be terminal; re-campaign
inside the session (recipes and Herald decide).

### Task 16: client — `LeaseWatch` and `startFromNow` (hash-range handoff)

Spec §12.4; R2, R7.

**Files:** create `client-cpp/include/rune/client/lease/LeaseWatch.h` (`LeaseWatchBase` untyped, `LeaseWatch<T>`
typed, `LeaseWatchOptions{handoffBuffer = 4096, Capacity names, Waiters per name}`); test
`client-cpp/tests/LeaseWatchTest.cpp`, `LeaseWatchBrokerTest.cpp`.

**Algorithm:** drop every prefix that extends another; per partition, per prefix: scan pages (`first = 1`, then
`resumeHash = lastHash`) recording `(h_i, asOf_i)`; subscribe once per partition from `min asOf`; deliver a record at
offset `o` on key `k` only if `k` matches a remaining prefix and `o ≥ asOf(page covering h(k))` (binary search over
the recorded page bounds); identity `(key, lease_version)` (a `Granted` already held from the scan is ignored; an end
for an unknown pair is ignored); records arriving during the scan are buffered (fixed capacity); on overflow cancel
the subscription and keep the cursor = offset after the last buffered record; at scan end apply the buffer and
re-subscribe from the cursor (`rune_client_lease_watch_handoff_resubscribes_total`); `LeaseWatchReady` completes
once; any later break re-runs `startFromNow`. The watch keeps, per registered name, a preallocated min-heap of
live `(lease_version, key)` for the recipes (Task 20).

- [ ] **Step 1: failing tests** `LeaseWatchHandoffRegression.*`: `ScanPlusSubscriptionEqualsFreshFoldRegression` (seeded
  churn log, 20 seeds); `RuleThreeHashRangesRegression`; `NestedPrefixesDroppedRegression`;
  `DisjointPrefixesOneSubscriptionRegression`; `OverflowResubscribesFromCursorRegression` (nothing lost or
  duplicated); `IdentityDedupRegression`; `BreakRerunsRegression`; `ReadyOnceRegression`; `WatchNoAllocRegression`;
  real broker: `LeaderChangeMidScanRegression` (3-node, via `LeaseTrio.h` from Task 17 — added there) and
  `CompactedLogStartRegression`.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseWatch' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push. **Checkpoint D.**

**Don't:** order by key; re-scan on overflow; deliver `Checkpoint`.

### Task 17: interaction matrix, broker integration (1-node and 3-node)

Spec §16.2, §21; L18, L22.

**Files:** create `broker/libs/node/tests/support/LeaseMatrixExpectations.h`, `LeaseRig.h` (on `TestCluster.h`,
injected lease clock via L18; lease topic `t` with a user view, a second lease topic, a non-lease topic on the same
worker, namespaces `a` and `b`, a v17 session), `LeaseTrio.h` (3-node, on `AuthTrio.h`/`TestCluster.h`); tests
`broker/libs/node/tests/integration/LeaseMatrixTest.cpp`, `LeaseIntegrationTest.cpp`, `LeaseRaceTest.cpp`,
`LeaseTrioTest.cpp`. **Task 17a** (same task, first step): the unit table rig `broker/libs/node/tests/node/LeaseMatrixUnitTest.cpp`
running the applicable sequential cells against the in-process worker + stub gateway.

- [ ] **Step 1:** `LeaseMatrixRegression.MatrixCountRegression`: 198 / 48 / 150 / 89 / 239.
- [ ] **Step 2:** `TEST_P` over the 150 applicable sequential cells, unit rig and 1-node broker; `OtherNs` cells
  compare full response bytes with a truly missing name.
- [ ] **Step 3:** `TEST_P` over the 89 timing cells on their layer (Interaction matrix). Drive loops capped; no sleeps.
- [ ] **Step 4:** the §16.2 scenarios as named tests: `LeaseIntegrationRegression.GrantExpiryRegression`,
  `…FencingRegression`, `…UpdateDoesNotRenewRegression` (M15), `…CompactionRestartRegression` (10 000 updates + 100
  checkpoints per key, clean, restart, identical view and remaining times), `…GatingRegression`;
  `LeaseRaceRegression.KeepAliveAtFourPointsRegression`, `…RevokeAndUpdateAtFourPointsRegression`,
  `…ContenderGrantsDoNotDeferExpireRegression` (M4); `LeaseTrioRegression.CheckpointClearLeaderKillRegression`
  (uncommitted and committed), `…PromotionKeepsOldLeaderInIsrRegression` (R3, M2), `…CheckpointInFlightRegression`
  (R4), `…PromotionBoundRegression` (with and without checkpoints), `…WholeClusterRestartRegression` (24 h lease at 5
  min ± 1 interval; 400-day lease after three restarts, injected clock), `…NoZombieAckRegression`,
  `…KeepAliveOutageUnderLeaderChangeRegression` (L22, p99 < 1.5 s over 50 forced changes).
- [ ] **Step 5:** mutation pass: apply each of M1–M20 that this task owns, confirm red, revert; record in the commit.
  Run `ctest --test-dir build -R 'LeaseMatrix|LeaseIntegration|LeaseRace|LeaseTrio' --output-on-failure -j8`, then
  `--repeat until-fail:20` and once under `-j8` load.
- [ ] **Step 6:** commit and push.

### Task 18: seeded model-checked stress

Spec §16.4.

**Files:** create `broker/libs/node/tests/support/LeaseModel.h` (the reference: live versions per key, every
acknowledged deadline per leader on simulated real time with drift ρ), `LeaseStressDriver.h` (on
`CheckedWritesStressDriver.h`'s 3-node in-process shape: the real worker, replication and fold code), test
`broker/libs/node/tests/integration/LeaseStressTest.cpp`.

- [ ] `LeaseStressRegression.SeededModelRegression`, seeds 1..20 (printed on failure), 3 000 steps per seed. Steps
  drawn from the matrix enums: grants, keepalive gaps, updates, revokes, leader kills, restarts, partitions,
  follower lag, compaction, checkpoint intervals, suspend/resume, lease-clock advances (to the next deadline, never
  wall time). After **every** step: I1–I8 of spec §16.4 (I3 against the model's acknowledged deadlines with ρ = 1 %;
  I4 also `stale_broker == 0`; I5 recompute from the log after every rebuild; I6 no ack by a node that was not the
  serving leader; I7/I8 liveness bounds).
- [ ] Mutations: M1, M3, M4, M13, M16 each make a seed fail within 3 000 steps (record which).
- [ ] Run `ctest --test-dir build -R LeaseStress --output-on-failure --repeat until-fail:20`, and once under `-j8`
  load. Commit and push.

### Task 19: Phase 1 e2e on the default config

Spec §16.3 (session scale, produce deadline), CLAUDE.md rule 11.

**Files:** create `broker/docker/lease-e2e.sh`, `broker/docker/docker-compose.lease.yml` (3 nodes, default config),
modelled on `checked-writes-e2e.sh`; a small driver `client-cpp/examples/lease_e2e_driver.cpp`.

- [ ] On the VM, holding the lock, default config, 3 nodes:
  1. `lease topic` DDL through QL; grant → `KeyExists` on a second grant → keepalives hold it for 3 × TTL → stop →
     `Expired` delivered within `ttl + bound`, view empty → regrant has a larger version;
  2. **session scale:** 10 000 leases through one `LeaseSession`, SIGKILL the process, every `Expired` delivered by
     the bound;
  3. **produce deadline (R5):** a guarded producer with sends in flight; partition the lease's leader (iptables in
     the container); after `Entered`, no record of that producer appended later than `Entered + d` (checked from the
     log offsets and the driver's send log);
  4. a 15 s one-shot checkpointed lease (interval 5 s), kill the leader twice, it ends within `extend + interval`
     late;
  5. `ReadOnly` then `RejectAll` on the namespace mid-TTL: an unkept lease still expires; resume.
  Check `df` before and after; remove only your own containers and images.
- [ ] Commit and push. **Checkpoint E** (Phase 1 done; announce the Phase 1 deliverables to the Herald 02 plan).

## Phase 2: recipes, limits, operations

### Task 20: `LeaseMutex` and `LeaseElection`

Spec §12.3, §12.2 (election at-risk), §16.1 (recipes), §16.3 (election e2e); R7; F1.

**Files:** create `client-cpp/include/rune/client/lease/LeaseMutex.h`, `LeaseElection.h`; tests
`client-cpp/tests/LeaseRecipeTest.cpp` (stub), `LeaseRecipeBrokerTest.cpp` (1-node and trio); extend
`lease-e2e.sh` with the election leg.

**Algorithm:** recipes take `(session, watch, name ≤ 102 B)`; key `name/<sessionId 16 hex>-<campaignNo 8 hex>`
(`sessionId` drawn at session construction, `campaignNo` incremented per `lock`/`campaign`); act only after
`LeaseWatchReady`; `lock()` grants (new key, so granted), waits for the ack and for the watch to pass that offset,
owns iff its version is the lowest under `name/` (from the watch heap), else waits for its immediate predecessor's
`Revoked`/`Expired` and re-checks (`OpKind::LeaseCampaign` on ownership); `VerdictUnknown` grant → read the key;
adopt if live with `lease_holder == sessionId`, else re-grant with the next `campaignNo`; `tryLock()` revokes its own
key if not lowest (`ErrLocked`); `unlock()`/`resign()` fenced revoke; `proclaim(value)` fenced update; `leader()`
and `observe()` from the watch; `onAtRisk(Entered)` suspends (`isLeader() == false`, callback runs), `Cleared`
resumes iff still lowest; `token()` = version.

- [ ] **Step 1: failing tests** `LeaseRecipeRegression.*`: `SharedWatchOneSubscriptionRegression` (2 000 elections, one
  subscription); `KeyShapeRegression`; `VerdictUnknownAdoptOrRecampaignRegression`; `MutexOrderByVersionRegression`;
  `WaitDeletesPredecessorOnlyRegression`; `TryLockCleansUpRegression`; `ObserveOncePerChangeRegression`;
  `AtRiskSuspendsResumesRegression`; `RecampaignAlwaysNewKeyRegression` (F1); `RecipeNoAllocRegression`; broker:
  `HolderExpiresNextWaiterOwnsRegression` (larger token), `LeaderChangeFoldedRegression`.
- [ ] **Step 2:** e2e election leg on the VM: two campaigners, SIGSTOP the leader past the TTL, the other leads with a
  higher token, the first wakes, gets `onLost`, its guarded write is refused (spec §16.3).
- [ ] **Step 3:** run `ctest --test-dir build -R 'LeaseRecipe' --output-on-failure -j8`; commit and push.

### Task 21: `max-leases`, view memory, the suspension matrix

Spec §14.2, §14.3, §9.6 (namespace rows), G3.

**Files:** modify `protocol/include/rune/protocol/NamespaceLimits.h` (`reserved1` → `maxLeases`, default
16 777 216; field mask 30 behind `kLeases`); `codec` namespace apply and the QL/CLI limit key `max-leases`
(`client-cpp/include/rune/client/NamespaceLimitFields.h`, `ql/src/ConfigKeys.h`); `NsSlotTable` / `NsHot` per-node
live-lease counter fed by the LeaseIndex fold (`rune_ns_leases`); `CheckedWriteHook` new-key-grant refusal while
over `max-leases` or view memory; tests `broker/libs/node/tests/integration/LeaseNamespaceTest.cpp`.

- [ ] **Step 1: failing tests** `LeaseNamespaceRegression.*`: `ReadOnlyServesKeepAlivesRefusesWritesRegression`,
  `ReadOnlyUnkeptStillExpiresRegression`, `RejectAllStillExpiresRegression` (M16), `RejectAllRefusesKeepAlivesRegression`
  (every lease ends within TTL + bound), `ResumeNothingToRearmRegression`, `MaxLeasesRefusesNewKeyGrantRegression`
  (keepalive/update/revoke/expiry continue), `ViewMemoryOverRefusesNewGrantsOnlyRegression`,
  `NamespaceDeleteDropsLeaseTopicRegression`, `LimitFieldOldLevelRefusedRegression`.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseNamespace|NamespaceLifecycle|NamespaceLimit' --output-on-failure -j8`;
  commit and push.

### Task 22: metrics, `show lease(s)`, `revoke lease`, CLI

Spec §11, §15.2; L12, L16.

**Files:** modify `broker/libs/node/include/rune/node/RuneMetrics.h` (every §15.2 metric with `{ns, ns_id, topic,
partition}`, fed from worker-owned counters through the existing metric slots, `RuneMetricSlot.h`); the QL executor
in `client-cpp/src/cli/` (`show leases` → per-partition `ScanLeases`, `show lease` → `LeaseTimeToLive`, `revoke lease`
→ read version then fenced revoke; `show topic` prints lease settings and the live count); tests
`client-cpp/tests/LeaseCliTest.cpp`, `broker/libs/node/tests/node/LeaseMetricsTest.cpp`.

**Carried from Task 4 (2026-11-08):** encode the literal key with the topic's key type and range-check it (a signed
u64 magnitude, `-0`, strings over 128 bytes); lift the prepare-time refusal of the three lease statements; let
`show view` / `show topic` take a quoted or dotted name (`inst.leases`, `'ns/t'`); `rune stats view <t>.leases` reads
the lease view's partition stats without a plan.

- [ ] **Step 1: failing tests**: `LeaseMetricsRegression.EveryMetricMovesRegression` (each metric moved by its
  event, `stale_broker` stays 0 in every non-injected run); `LeaseQlRegression.ShowLeasesAllPartitionsRegression`,
  `…ShowLeaseRegression`, `RevokeLeaseIsFencedRegression` (a version change between read and revoke reports
  `LeaseNotFound`; no force path exists), `…AuthorisationRegression` (`show` READ, `revoke` WRITE).
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LeaseMetrics|LeaseQl|LeaseCli' --output-on-failure -j8`;
  commit and push.

### Task 23: latency gates and the non-lease ABAB

Spec §15.1 (budgets), §16.6; CLAUDE.md §2 latency budgets.

**Files:** complete `broker/benchmark/LeaseWheelBench.cpp`, `LeaseKeepAliveBench.cpp`, create
`LeaseExpiryLagBench.cpp`, `LeaseCheckpointBench.cpp`; gate wiring through `broker/benchmark/BenchGate.h`
(`-L latency`); BENCHMARKING.md baselines table rows.

- [ ] On the clean box (BENCHMARKING.md), interleaved ABAB, n ≥ 5 (latency) / ≥ 3 (throughput):
  - keepalive request→response p99 < 50 µs single node; 256-entry p99 < 100 µs;
  - wheel arm/re-arm p99 < 50 ns; idle poll with 1 M armed < 100 ns;
  - expiry lag p99 < tick + 1 ms single node (TTL 1 s / 15 s / 30 s × 10 k / 100 k / 1 M), ≥ 100 k expiries/s in a
    mass expiry; 3-node figures recorded against the acks=all baselines;
  - lease-view point read < 50 µs; QL lease DDL text → wire ≤ 2–5 µs;
  - `LeaseCheckpointBench`: 1 M one-shot leases at the default interval sustain `checkpoint_rate` with no expiry lag
    regression;
  - non-lease: `ProducePathBenchmark`, `WorkerProduceLatencyBench`, `ViewFoldBench`, `ReplicationBenchmark` ABAB
    against the merge base, unchanged (prove any shift with n ≥ 15 and name the code path).
- [ ] A gate over budget is a failing gate: fix in this task (diagnose at the component boundary first).
- [ ] If the bench box is unavailable: the PR says so and one issue per gate is filed (Execution context).
- [ ] Commit and push.

### Task 24: docs

Spec §20 P2.5.

**Files:** `docs/api/ql.md` (lease topic DDL, settings, `show lease(s)`, `revoke lease`), `docs/api/client-cpp.md`
(`LeaseTopic`, `LeaseSession`, `LeaseWatch`, `LeaseMutex`, `LeaseElection`, the guard, the at-risk / lost
semantics, the local-`onLost` note of §12.2), `docs/api/wire-protocol.md` (templates 96–101, codes, trailers),
`docs/operations.md` (sizing `checkpoint_rate`, the lease clock, `max-leases`, metrics), `README.md` link, root
`CLAUDE.md` §8 note that lease topics exist.

- [ ] Write; check every code sample compiles in `client-cpp/examples/` (one example per recipe). Commit and push.
  **Checkpoint F** (Herald P0 leadership unblocked), then the final whole-branch review.

## Spec gaps (decided here, recorded for review)

- **Template ids.** The spec keeps the three request ids symbolic, but only 93–95 are free in the view range and six
  ids are needed. L1 opens a lease range 96–111.
- **`leaseVersion` in the ack.** The spec says the `View` ack gains a per-record `leaseVersion` without a layout, and
  `LeaseResult::ttlMs()` needs the raised TTL too. L2 defines a lease trailer `{leaseVersion, ttlMs}` per record.
- **How the client learns `lease.ttl`** (the default for an unset `LeaseOptions.ttl`) is unspecified. L3 adds a lease
  trailer to `TopicSchemaResponse`.
- **Catalog encoding of `LeaseSpec`** ("encoded like `Persistence`") does not say how fingerprints and goldens stay
  stable. L4: optional trailer, fingerprint mixed only when present, level gate at build only.
- **System-column field ids** are not given; L5 fixes ids 0–3 and renumbers user fields from 4.
- **Thread placement of the R5 guard** is not stated; in `RunnerMode::Thread` the producer state lives on the client
  agent thread. L7 runs the guard there, so "before the callback" holds by construction.
- **Real-broker injected clock** (§16.2 "driven with an injected clock") has no named seam; L18 uses the existing
  `Worker::setLeaseClockForTest` through `TestCluster`, and docker e2e uses real time with bounds.
- **QL `where path like`** accepts only a trailing-`%` prefix (L16).
- **Herald F25 budget** (a Rune leader change on `herald.elections` must finish within 1.5 s) is a Herald dependency
  the spec does not test; L22 adds an integration assertion.
- **Out of this spec, noted for the Herald plans:** Herald 02 §14 R10 (resolved start offset of a `From::latest`
  subscription), R13 (insert-only grant) and R14 (cross-client `ProducerOptions.guard`) are not in spec 03 and are
  not built here; herald-02 keeps its fallbacks (markers; the accepted risk; closing the tenant client in
  `onAtRisk`). Herald 00 §13's H2 "a new hit refreshes the `sanction-expiry` lease" works with this plan's
  `keepAliveOnce` (re-arms at the full TTL and clears a checkpoint) or a revoke + regrant; herald-04 chooses.
- **Herald 01 phase mapping.** `herald/docs/superpowers/plans/2026-11-07-herald-01-core-domain-and-tenancy.md`
  (Tasks 29 and 40) needs `onAtRisk` (Phase 1 / Task 15) and `LeaseElection` (Phase 2 / Task 20). Its pod registry
  builds on Phase 1 lease topics and `LeaseWatch`. Phase 3 (the discovery library) was dropped by owner ruling
  2026-11-07, so herald-01's G1 should cite Phase 1 / Task 15, Phase 2 / Task 20 and Phase 1 lease topics +
  `LeaseWatch`, not Phase 3 / Tasks 25–26. herald-01 consumes them only at Tasks 29 and 40.
- **Decisions-file conflicts:** none found. K15 R1–R11, F3 and B28 are already in rev 4 and are followed as written;
  K16 rulings do not touch Rune.

## Self-review (spec section → task)

| Spec | Task |
|---|---|
| §1 why, §2 goals / non-goals / anti-goals | all; anti-goals = Review Focus 2–6, 13 |
| §3 discovery library (§3.1–§3.6) | not built: Phase 3 dropped by owner ruling 2026-11-07 (DG9 → 20) |
| §4 etcd parity, D1–D4 | 5, 6, 10, 11, 12, 15, 20; Review Focus 1 |
| §5.1 QL and key width (every row of its table) | 2, 3, 4, 14 |
| §5.2 settings | 1, 2, 4, 12 |
| §5.3 admin wire, catalog, gates, namespaces | 1, 2, 3, 11, 21 |
| §6.1–§6.2 system columns, ops, revoke re-encode, refusals | 1, 3, 5 |
| §6.3 version = offset, ack, duplicates | 5, 14 |
| §6.4 write rules, §6.5 transactions | 5 |
| §6.6 tombstones, checkpoints invisible, compaction, consumers | 7, 8, 14 |
| §7 lease view, judgment table, fencing | 6, 13, 17 |
| §8.1 keepalive, §8.2 TimeToLive, §8.3 ScanLeases | 11, 13 |
| §9.1 emitter and appending, §9.3 never stale, §9.4 one end | 10 |
| §9.2 clocks, §9.5 checkpoints, §9.6 promotion | 12, 17 |
| §9.7 no stall | 6, 10 |
| §10 wheels | 9 |
| §11 QL and CLI | 4, 22 |
| §12.1 LeaseTopic, §12.2 LeaseSession + guard, §12.3 recipes, §12.4 watching | 14, 15, 20, 16 |
| §13 discovery library | not built: Phase 3 dropped |
| §14.1 grants, §14.2 limits, §14.3 suspension | 11, 13, 21 |
| §15.1 hot path, §15.2 metrics | 7, 9, 11, 23, 22 |
| §16.1 unit | 1–16, 20 |
| §16.2 integration | 17, 21 |
| §16.3 e2e | 19, 20 |
| §16.4 stress | 18 |
| §16.5 mutations | M1–M20 (Interaction matrix), Tasks 5–20 |
| §16.6 benches | 9, 11, 23 |
| §17 Q1 (settled), §19 Herald examples | no Rune work; §19 DDLs are exercised as fixtures in 17 (`herald.elections`, `herald.workers`, `tenant-delete-grace`, `sanction-expiry`) |
| §18 later work, Appendix A | not built |
| §20 phases | Phase 1 = Tasks 1–19, Phase 2 = 20–24 (spec Phase 3 dropped) |
| §21 interaction matrix | Interaction matrix, 17, 18 |
| §22 review focus | Review Focus 1–14 (discovery parity dropped with Phase 3) |

Every matrix cell maps to a layer (Interaction matrix, "Layers per cell"); no step is left TBD.

## Execution handoff

Run the plan with superpowers:subagent-driven-development:
- one implementer per task (mid-tier model) with the exact recipe above; implementers never spawn subagents and get
  no design decisions (open points are the L-decisions);
- a task review after each task (spec compliance + code quality);
- checkpoints A–F as listed, with the full `*Regression*` suite under plain, ASan+UBSan and TSan builds;
- a final whole-branch review by the most capable model, exhaustive and adversarial against the Review Focus list,
  the Interaction matrix and every mutation M1–M20; the reviewer applies the attack classes of
  `docs/adversarial-review.md` that fit (input bounds, protocol state, version skew, concurrency, crash/restart,
  replication, compaction, auth, resources, data integrity, hot path);
- at most 3 agents at once; root-cause diagnosis of any red test stays with the controller (CLAUDE.md rule 14);
  non-urgent findings are filed as labelled GitHub issues (type, severity, area) per CLAUDE.md §5.

Phase 1 (Checkpoint E) unblocks herald-02's RuneAgent and lease bindings (its Tasks 5, 10, 16, 23) and herald-04/05's
expiry and claim leases (herald-04 Task 21, herald-05 Task 6); Phase 2 (Checkpoint F) unblocks herald-02's tenant
election and singletons (its Tasks 21 and 15). Herald's pod registry and singletons are unblocked by Phase 1 (lease
topics, `LeaseWatch`, the F25 bound in Task 17) and Phase 2 / Task 20 (`LeaseElection`).
