# Rune — Topic Truncate (Phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `TruncateTopic(topic, entries[(partition, beforeOffset)])` moves a topic's log start forward on every
replica, deletes the segments below it, and cuts every view over the topic so that each view equals a recompute over
`[newStart, HW)`. The leader resolves `min(beforeOffset, HW, LSO)` once, persists it before it moves its start, and
replies only when every ISR member has adopted it. The start never moves back on any replica. A follower below the
leader's start, on disk or transient, always gets an answer and resumes empty at the start; the disk hang that exists
today (a fetch below the start that is never answered) is fixed first, repro first. Retention moves the start through
the same path. A consumer fetch below the start gets `OffsetOutOfRange`. Views rebuild into a shadow that is reserved
against the namespace's view memory before resolve, while the old state keeps serving, and swap in once caught up.
Internal `__*` topics are refused for everyone. The CLI gains `rune topic truncate`. The produce and fetch hot paths
are unchanged, proven by an ABAB bench.

**Architecture:**
- **Gateway (control thread).** Decodes TruncateTopic (template 86), refuses internal topics, authorizes the
  Truncate grant, applies the Phase 2 suspend table, reserves the view shadow memory, then fans the entries out to
  each owner worker over the existing worker ingress ring and aggregates the per-entry replies into one
  TruncateTopicResponse (template 87) (D37, D38).
- **Workers (replica owner).** A user partition's replica runs on its owner worker (NodeRuntimeAgent.h ~:725). The
  leader resolves the new start, writes the log-start checkpoint, moves its in-memory start, enqueues the segment
  deletes, and parks the reply in a preallocated `pendingTruncates_` queue until the ISR low watermark reaches the
  new start, an ISR shrink completes it, a leader change answers NotLeader, or the deadline answers RequestTimedOut
  (D40, D41).
- **Replication.** `FetchRequest` carries the follower's `logStartOffset`; the leader keeps it in the follower slot.
  `FetchResponse` carries the leader's real `logStartOffset` and `truncateStartOffset` (disk too). A follower adopts
  a higher start on every response, the error branch included: at or below its LEO it advances, above its LEO it
  truncates fully and resumes empty at the start (D36, D39).
- **Storage.** `Log::advanceLogStart(newStart, reason)` (checkpoint, then the in-memory start, then the async
  deletes) and `Log::truncateFullyAndStartAt(off)`. The log-start checkpoint gains `truncateStart` (D35). Boot treats
  a checkpoint above every segment as an empty log at that offset. Retention calls `advanceLogStart(…, Retention)`.
- **Views (view host worker).** A cut posts `ViewSourceCut` to the view's host. The view moves to
  `ViewRebuilding{shadow, cutStarts, swapAt}`: the shadow folds from each source's new start while the old table
  serves reads, then swaps in and the snapshot is rewritten (tmp + fsync + rename). A host that cannot fit the shadow
  pauses the view at `ViewPaused{OverLimit, newStart}` (D42–D45).
- **Clients.** `Client::truncateTopic` and `truncateTopicAndWait`; an explicit subscribe or fetch offset below the
  start gets `OffsetOutOfRange` and runs the reset policy (D46). The CLI gains `rune topic truncate`.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark, CMake/Ninja, real Aeron UDP for the
in-process BrokerNode tests, docker compose (3 nodes) on the shared Linux test VM for e2e.

**Spec:** `broker/docs/superpowers/specs/2026-10-24-rune-namespaces-and-truncate-design.md` (approved 2026-10-26).
This plan covers §6 (TruncateTopic), the §7 order for Phase 3, the Phase 3 rows of §8 and §9, and §10
(`rune topic truncate`). The Self-review maps each spec section to its task.

**Builds on / order:** the Phase 1 plan (`2026-10-26-rune-namespaces.md`) and the Phase 2 plan
(`2026-10-26-rune-namespace-lifecycle-limits.md`) merged. Phase 1 gives the Truncate operation (stored only) and the
namespace resolve; Phase 2 gives the suspend table, the view-memory accounting and `ViewPaused`. What exists today
(line numbers are hints; use the quoted anchors):

- **Phase 1 and 2 (assumed merged):**
  - `SecurityOperation::Truncate = 7` (`SecurityTypes.h` ~:10), stored and printed but never checked.
  - Codes 51–54 gated on client ≥ 14 (`NamespaceSuspended`, `NamespaceLimitExceeded`, `Throttled`, `ViewPaused`);
    templates 81–85; `Dispatch.h`'s gap comment 86–95.
  - `NsGate = std::variant<Open, ReadOnly, RejectAll, Gone>`; the suspend table refuses DDL under ReadOnly,
    truncate included (Phase 2 D21), and keeps the worker `nsGate[]` hook for this phase.
  - View memory = chunkCount × chunkBytes; `ViewPaused{OverLimit, offset}` in `ViewReadiness` (Phase 2 Task 12,
    D26); the limit aggregator on the control agent.
  - Rigs: `NamespaceRig.h`, `AuthTrio.h`, `CatalogTrio.h`, `CatalogDdlRig.h`, `AuthBroker.h`, `AuthWireClient.h`.
- **Replication, leader fetch** (`broker/libs/replication/src/PartitionReplica.cpp`):
  - `onFetchRequest` (~:756): gates NotLeader (~:766), StaleEpoch (~:771), step-down (~:788), incarnation
    (~:833–862); epoch checks (~:890–907); a follower ahead gets OutOfRange `ErrorPayload{-1, LEO}` (~:918–928);
    `decideFetchServe` (~:960), `readLog_` (~:981), `isr.onFollowerFetch` (~:1009),
    `updateFollowerFetchOffset` (~:1011).
  - **The hang** (~:1088): `belowRingStart = memoryBacked_ && req.fetchOffset < memLog_->logStartOffset()`; when
    `serve == Data && fetchOffset < LEO && view.empty() && !belowRingStart` the handler returns `kErrNone` with no
    answer, so a disk follower below the start waits forever. `retryOwedFetchAnswers_` (~:1149) skips the same case
    with `if (view.empty()) continue;` (~:1167).
  - `makeFetchResponse` (`PartitionReplica.h` ~:1444/:1452) advertises `logStartOffset = 0` for a disk log.
  - `sendFetchAnswer_` (~:1128); `doWork` (~:2460: HW ~:2554, retryOwed ~:2566).
- **Replication, follower:** `applyReplicated` (~:1268); its error branch (~:1279–1322) returns before any start
  check; `followerSync_.onResponse` (~:1369); `resetTransientLogTo_` (~:3757, one caller ~:1397); a disk follower
  ignores `logStartOffset`.
- **ISR:** `FollowerSlot` (`FollowerSlots.h` ~:39: ackedLeo ~:53, OwedAnswer ~:68, resetLeadership ~:145);
  `ReplicaLeaderState` (`HighWatermark.h` ~:75: setIsr ~:90, computeHw ~:236); ISR changes `applyCommittedIsr_`
  (~:2901), `shrinkIsrToLive` (~:3056), `updateIsr_` (~:3100), `changeIsr_` (~:3217), `syncIsrToLeaderState`
  (~:3750).
- **LSO:** `LsoTracker::lso` (`ReadCommittedView.cpp` ~:226), `RcState::lso` (`RcState.h` ~:613),
  `cleanerSwapBound()` (`PartitionReplica.h` ~:899).
- **Wire** (`ReplicationMessages.h`): `kSchemaVersion 4` (~:56), `kMinReplicationVersion 3`; `FetchRequestMsg`
  (~:190: template 1, `BLOCK_LENGTH 60`, last field `lastEpochStartOffset i64@52`; encode ~:224, decode ~:257);
  `FetchResponseMsg` (~:309, `logStartOffset i64@20`); XML `protocol/schema/rune-replication.xml` (~:70–86).
- **Storage** (`broker/libs/storage/src/Log.cpp`, `Log.h`): `writeLogStartCheckpoint_` (~:2674, 12 bytes
  `i64 | crc32c`, tmp + fsync + rename + dir fsync), `readLogStartCheckpoint` (~:2695); boot (~:120–176, checkpoint
  read ~:140, drop ~:141–150, never drops the last segment); `logStartOffset()` (~:1249, `segments_.begin()->first`);
  `readInto` (~:1058/:1062) returns `{}` below the start; `applyCleanerMsgRange_` (~:2496), `enqueueDeletedSegment`
  (~:2882), `drainRetentionDeletes` (~:2898); `Log.h` truncateTo (~:586), deleteOldSegments (~:618),
  persistedLogStart_ (~:1304). There is no API that advances the start.
- **Transient:** `InMemoryLog.h` resetTo (~:50), logStartOffset (~:267).
- **Retention:** `LogCleanerAgent::runRetention` (`LogCleanerAgent.h` ~:812) runs on leaders and disk logs only,
  skips compacted topics, applies the logStart rule (~:942) and enqueues (~:958). It never reaches followers.
- **Worker** (`broker/libs/node/include/rune/node/Worker.h`): `PendingAcksAll` (~:5849) and `pendingAcksAll_`
  (`OrderedWaitQueue`, ~:5862) with sweeps (~:6871, ~:7523, ~:7639) — the parked-reply pattern to copy, together
  with `releaseParkedCommits_` (`GroupCoordinationService.cpp` ~:1506); `driveViews_` (~:6460, called ~:1858 and
  ~:1989); `skipEvictedCursor_` (~:3272, ~:3444, ~:3767, ~:4285); `raiseFreshDiskCursor_` (~:3770, ~:4286);
  `executeFetchRequestWire_` (~:4808); `enqueueOp` (~:1219).
- **Views** (`WorkerViewSet.h`): `ViewReadiness` (~:92); `ViewInstance` (~:192–249, `foldCursor` ~:202 shared
  across sources); `viewSourceTopic()` (~:460, returns [0]); `driveView_` (~:599–701); `driveRebuild` (~:374);
  `RowStatus` (~:343); `reconcileTransient_` (cpp ~:378–451); there is no disk reset path. `ViewTable.h` (~:82,
  `(RowLayout, 4096, 32 MiB)`). Snapshots: `viewCheckpointFilePath` (`ViewSnapshotChannel.h` ~:46,
  `view-<id>-p<n>.committed.delta`); the writer appends (`ViewSnapshotIoAgent.cpp` ~:139/:161/:240); recovery
  `replayInstance` (cpp ~:87), `installViewForRecovery` (cpp ~:550). Views run on leaders only, bounded by
  `lso(liveHw)`.
- **Gateway** (`broker/libs/gateway/src/ClientGateway.cpp`): `onClientFrame_` (~:1148, DropTopic case ~:1371);
  `handleCatalogDdl_` (~:8287); `compilePerms_` (~:8521); `authorizeCatalogDdl_` (~:8844); routing by
  `workerPool_->ownerWorkerId(topicId, partition)` (~:3817, ~:4402, ~:4437, ~:4483, ~:4552); subscribe clamps
  silently to logStart (~:5169–5185); `PendingAck` (`ClientGateway.h` ~:1549). No request fans out across workers
  today.
- **Wire, client and CLI:** `Protocol.h` templates (~:31) and code gating (~:383–459); `Dispatch.h` (~:59–70);
  DropTopic 74 (`SchemaMessages.h` ~:603), DdlResponse (~:829), DdlExtension (~:25); the per-partition response
  pattern `OffsetCommitResponse` (`OffsetMessages.h` ~:204, `FixedArrayView`); `kReservedTopicPrefix = "__"`
  (`codec/include/rune/catalog/TopicName.h` ~:33, no helper); `Client.h` (~:462; dropTopic ~:866,
  dropTopicAndWait ~:959); `Client.cpp` issueDdl_ (~:1442), OpKind (~:1429); `Cli.cpp` printUsage (~:88),
  dispatch (~:446–477); `client-cpp/CMakeLists.txt` (~:1715).

## Execution context

- **Branch:** `claude/tender-wozniak-co3crp` in this repo; every path is repo-relative. Before each task, merge
  `main` in if it moved (`git fetch origin main && git merge origin/main`). Push after every task with
  `git push -u origin claude/tender-wozniak-co3crp`.
- **Builds:** see README.md#build and `docs/testing.md`.
  - `build` (Debug, `BUILD_TESTING=ON`, `BUILD_INTEGRATION_TESTING=ON`) for everything;
  - `build-asan` and `build-tsan` at the checkpoints;
  - `build-bench` (Release, `BUILD_BENCHMARKS=ON`) for Task 14's hot-path bench.
  - Re-run cmake only after a `CMakeLists.txt` edit.
- **Test gate (CLAUDE.md rule 10):**
  - **Per task:** build, then `ctest --test-dir build -R '<filter>' --output-on-failure -j8`, where the filter is the
    one named in the task's "Run" step.
  - **Checkpoints:** `cd broker && scripts/run-regression.sh ../build 8`, then the same suite under `build-asan` and
    `build-tsan` (`-R Regression -LE latency`). A failure is bisected over the commits since the last green
    checkpoint.
  - **Checkpoints:**
    - **A** after Task 4: the hang fixed, the storage API, the wire, the grant and the internal-topic helper.
    - **B** after Task 7: truncate works on one partition end to end on the broker side (resolve, followers adopt,
      reply at the low watermark).
    - **C** after Task 10: gateway fan-out, the client API, retention through the same path and consumer
      OffsetOutOfRange. A truncate from a real client works on a topic without views from here.
    - **D** after Task 14: views cut and rebuild, the memory reservation, the CLI and the bench.
    - **E** after Task 18: the matrix, the stress test, the 3-node slice, e2e and docs, before the final
      whole-branch review.
- **Docker e2e** (Task 18) runs on the shared Linux test VM from `$RUNE_VM_HOST`: hold the shared lock for the whole
  run, work only in your own directory, remove only containers and images you created, and check `df` before and
  after. Never on local Mac docker. The host's address and login are never written into the repo.
- **Commits** end with the session's `Co-Authored-By:` and `Claude-Session:` trailers.

## Global Constraints

**HARD RULES (CLAUDE.md §1, checked in every task review):**
1. **One owner per state.**
   - A partition's log, its start, its checkpoint and its `pendingTruncates_` belong to the replica owner thread:
     the owner worker for a user partition. Nothing else calls `advanceLogStart`; the LogCleanerAgent asks the owner
     through its existing cleaner ring (D47).
   - The follower slot's `logStartOffset` belongs to the leader replica (same thread).
   - A view, its shadow and its snapshot belong to the view host worker. A cut reaches it only as a `ViewSourceCut`
     ring message.
   - The fan-out aggregate (`PendingTruncateAgg`) and the shadow memory reservation belong to the gateway and the
     limit aggregator on the control thread.
   - Cross-thread hand-off is only the existing Aeron rings (worker ingress, `sessionRing_`, the cleaner ring, the
     view snapshot channel). No new shared memory.
2. **Hot path unchanged.**
   - Produce gains nothing.
   - The leader fetch handler already compares `fetchOffset` with the start; it now also stores the request's
     `logStartOffset` into the slot (one store). The follower compares two i64 per response.
   - The view fold checks the readiness variant it already checks; a `ViewRebuilding` view folds the shadow on the
     same cycle budget as `driveRebuild`.
   - No map lookup, no allocation and no virtual call is added on the produce, fetch or query path. Task 14 proves
     it with an interleaved ABAB bench against the Phase 2 tip.
3. **Zero allocation in steady state.**
   - `pendingTruncates_` is a preallocated `OrderedWaitQueue` sized `RUNE_TRUNCATE_MAX_PENDING`.
   - `PendingTruncateAgg` entries come from a preallocated pool sized `RUNE_TRUNCATE_MAX_PENDING`, each with room
     for `kMaxTruncateEntries` (= the broker's partition cap) entries.
   - A shadow takes its chunks from the view's chunk pool after a successful reservation; a failed reservation
     allocates nothing.
4. **Rich-state variants.**
   - The leader's per-partition truncate wait is `PendingTruncate{corr, gatewaySlot, newStart, deadlineNs}` in the
     queue, completed exactly once.
   - The gateway aggregate is `PendingTruncateAgg{corr, session, entries[], owed}`.
   - A view under a cut is `ViewRebuilding{shadow, cutStarts[kMaxViewSources], swapAt}` in `ViewReadiness`. A
     second cut before the swap replaces `cutStarts` and restarts the shadow; it is never a flag.
   - A follower's reaction to a response is one of `Adopt::None`, `Adopt::Advance{start}`,
     `Adopt::ResetEmpty{start}`, chosen by one function (`decideAdopt`).
5. **Time-independent convergence (rule 16).** Followers converge on the leader's start from every fetch response,
   whatever the timing; a crash at any step re-derives everything from the checkpoint and the segments. The reply
   deadline (`RUNE_TRUNCATE_TIMEOUT_MS`) only bounds how long a client waits for an answer; correctness never
   depends on it, because the retry is idempotent.
6. **No silent fallback.**
   - Every refused request or entry gets its precise code (Error codes, below).
   - A fetch below the leader's start always gets an answer (never `kErrNone` with nothing sent).
   - A view read below its source's start never stalls silently: the view is either rebuilding (reads served from
     the old state) or paused with its offset.
   - A consumer's explicit offset below the start gets `OffsetOutOfRange` (client ≥ 14), never a silent clamp.
7. **Regression naming.** Every new test is `*Regression*`. The failing test is written first in every task.
8. **Fix on sight.**
   - The below-start fetch hang (Task 1).
   - `makeFetchResponse` advertising 0 for disk (Task 1).
   - Boot never dropping the last segment when the checkpoint is above it (Task 1).
   - Retention never reaching followers (Task 10).
   - `Log::readInto` returning `{}` below the start, which stalls a view silently (Task 11).
   Each has its own regression test.

**Error codes**, each gated on `clientVersion >= 14` with no version bump (D34):
- `OffsetOutOfRange = 55`: a consumer fetch or subscribe at an explicit offset below the start. An older client keeps
  today's clamp to the start.
- `RequestTimedOut = 56`: the truncate reply deadline passed before the low watermark reached the new start. An older
  client gets `Internal` (6).
- Reused: `InvalidRequest` (39) for an internal topic and for an unknown partition entry; `NotAuthorized` (45);
  `UnknownTopic` (7) for a missing or hidden topic; `NamespaceSuspended` (51) under ReadOnly; `NamespaceLimitExceeded`
  (52) when the shadow reservation fails; `NotLeader` (2) for an entry whose leader moved.

**Versions:** client and broker versions do not change: `kClientVersion` 14, `kBrokerVersion` 8, `kProtocolVersion`
3. The replication schema does: `kSchemaVersion` 5 and `kMinReplicationVersion` 5, because `FetchRequest` and
`FetchResponse` gain fields and the spec rules out mixed-version clusters (D36). Templates:
- `TruncateTopic = 86`, `TruncateTopicResponse = 87`;
- `Dispatch.h`'s gap comment becomes 88–95.

**Configuration:**
- `RUNE_TRUNCATE_TIMEOUT_MS` (default 30000): the reply deadline, on the injected clock.
- `RUNE_TRUNCATE_MAX_PENDING` (default 64): parked truncates per worker and aggregates per gateway. Over it, the
  request gets `Throttled` (53) with `retryAfterNs = 1 ms`, the existing backpressure code (D41).

## Review Focus

1. **No reply before the low watermark.** A successful entry is answered only when the leader's start and the
   `logStartOffset` of every ISR member are at or above the new start, or the ISR shrank to members that all are.
   Tested by `TruncateLowWatermarkRegression.NoReplyBeforeEveryIsrMemberAdoptsRegression` and
   `…IsrShrinkCompletesRegression` (Task 7).
2. **No replica's start moves back.** Not on the leader, not on a follower that adopts, not after a restart, not on
   a new leader with a lower start whose followers are ahead. Tested by
   `TruncateFollowerAdoptRegression.StartNeverMovesBackRegression` (Task 6) and the stress invariant (Task 16).
3. **A follower below the start always gets an answer**, on disk and transient, on the first fetch and on the owed
   retry. Tested by `FetchBelowStartRegression.DiskFollowerGetsAnswerRegression` and
   `…OwedRetryAnswersRegression` (Task 1).
4. **The clamps hold.** The resolved start is `min(beforeOffset, HW, LSO)` (`-1` → `min(HW, LSO)`), never above
   either, never below the current start, and the same value is persisted, adopted and returned. Tested by
   `TruncateResolveRegression.ClampTable/*` (Task 5).
5. **Open-txn records survive.** A truncate with an open txn stops at the LSO; the txn commits later and every one of
   its records is visible to `read_committed`. Tested by `TruncateResolveRegression.OpenTxnSurvivesRegression`
   (Task 5).
6. **Internal topics are refused for everyone**, the cluster admin included, with InvalidRequest, before any lookup
   or reservation. Tested by `TruncateGatewayRegression.InternalTopicRefusedForEveryCallerRegression` (Task 8).
7. **Views equal a recompute**, after the swap, after a second truncate before the swap, after a restart at every
   step and after a leader change. Tested by `ViewTruncateRegression.EqualsRecompute*` (Task 11) and the stress
   invariant (Task 16).
8. **Crash safety.** A crash between the checkpoint and the in-memory move, between the in-memory move and the
   segment deletes, and on a follower between its checkpoint and its segment drop, boots to the checkpointed start
   with no segment below it. Tested by `LogAdvanceStartRegression.CrashAtEveryStep*` (Task 2) and
   `TruncateFollowerAdoptRegression.CrashBetweenCheckpointAndDropRegression` (Task 6).
9. **The hot path stays zero-alloc and within noise**, proven by ABAB. Tested by Task 14.
10. **No silent clamp for new consumers.** Tested by `ConsumerOffsetOutOfRangeRegression.*` (Task 10).

## Decisions

- **D34. Codes 55 and 56 without a version bump.** They are gated on client ≥ 14 like 51–54. Older clients get the
  closest code they know: no code for OffsetOutOfRange (they keep the clamp) and Internal for RequestTimedOut.
- **D35. The checkpoint gains `truncateStart`.**
  - The log-start checkpoint becomes 20 bytes: `i64 logStart | i64 truncateStart | crc32c`. A 12-byte file (today's
    format) reads as `truncateStart = 0`.
  - `truncateStart` is the start set by the last TruncateTopic; retention moves only `logStart`.
  - Views need it: on recovery, a snapshot whose recorded cut for a source is below that source's `truncateStart`
    is discarded and the view rebuilds from it. Without it a new leader could not tell a truncate (cut the view)
    from retention (keep the view).
- **D36. Replication wire, schema 5.**
  - `FetchRequest` gains `logStartOffset i64@60`; `BLOCK_LENGTH` 60 → 68.
  - `FetchResponse` gains `truncateStartOffset i64` at the end of its block.
  - `kSchemaVersion` 5 and `kMinReplicationVersion` 5. There are no mixed-version clusters.
- **D37. Gateway fan-out.** The gateway splits the entries by `ownerWorkerId(topicId, partition)`, sends one
  `TruncatePartitions` message per owner worker on its ingress ring, and aggregates the per-entry replies in a
  `PendingTruncateAgg`. The response is sent when every entry is answered. This is the first fan-out request; it
  reuses the ingress and return rings, so it adds no new ring.
- **D38. `isInternalTopicName`.** A `constexpr` helper in `TopicName.h` (`name.starts_with(kReservedTopicPrefix)`).
  The gateway checks it before authorization and lookup, so the answer leaks nothing and is the same for every
  caller.
- **D39. Follower adoption is one function.** `decideAdopt(followerStart, followerLeo, leaderStart)` returns None
  when `leaderStart <= followerStart`, Advance when `leaderStart <= followerLeo`, else ResetEmpty. It runs on every
  response, the error branch included, before the error is handled. A transient follower uses
  `InMemoryLog::resetTo` for ResetEmpty and moves its ring start for Advance.
- **D40. The low watermark.** `lowWatermark = min(leader.logStart, slot.logStartOffset for every ISR member)`. It is
  recomputed in `doWork` after the HW and on every ISR change. A parked truncate completes when
  `lowWatermark >= newStart`.
- **D41. The parked reply.**
  - `pendingTruncates_` is an `OrderedWaitQueue<PendingTruncate>` on the owner worker, ordered by `newStart`, so a
    completion pops a prefix.
  - Leadership loss answers every parked entry NotLeader; the client retries and the new leader resolves again.
  - The deadline answers RequestTimedOut; the start has already moved and the retry is idempotent.
  - A full queue answers Throttled with `retryAfterNs = 1 ms`.
- **D42. Views run on leaders only.** "Every replica cuts its own views" means the host of each view instance. A new
  leader restores from the rewritten snapshot, or rebuilds when the snapshot predates `truncateStart` (D35).
- **D43. A cut is a full shadow rebuild.** `foldCursor` is shared across a view's sources, so a cut of any source
  rebuilds the whole view: each source from its own new start (the others from their current start), up to the
  live bound. Reads keep the old table until the swap.
- **D44. The reservation.**
  - The gateway reserves, before fan-out, the current size (chunkCount × chunkBytes) of every view over the topic
    against the namespace's view memory in the limit aggregator. Failure refuses the whole request
    NamespaceLimitExceeded before any start moves.
  - The reservation is released per view at its swap (the old table frees the same amount) or when every entry
    failed.
  - A host that cannot fit its shadow at cut time (another node, or a leader change since) pauses the view at
    `ViewPaused{OverLimit, offset = newStart}`; when memory frees it rebuilds from there.
- **D45. Snapshot rewrite.** After the swap the host writes a full snapshot to `view-<id>-p<n>.committed.delta.tmp`,
  fsyncs, renames over the old file and fsyncs the directory. The snapshot header records each source's applied
  cut. Older deltas are discarded.
- **D46. Consumers.** An explicit offset below the start gets OffsetOutOfRange (client ≥ 14) on subscribe and on
  fetch; Earliest is unchanged (it means the start); old clients keep the clamp. The client runs its reset policy
  (`earliest`, `latest` or `none` → the error is raised to the caller).
- **D47. Retention through the same path.** `runRetention` no longer deletes on its own: it sends the owner
  `AdvanceStart{topic, partition, newStart, Retention}`, and the owner calls `advanceLogStart`. The start then
  reaches followers through the fetch response like a truncate's. Retention never moves `truncateStart` and never
  cuts views.
- **D48. Compacted topics.** A truncate removes every record below the start, compacted or not: the cleaner's
  segment range starts at the log start, and the dirty-ratio bookkeeping drops the deleted segments.
- **D49. ReadOnly refuses every caller.** The Phase 2 suspend table row for template 86 refuses TruncateTopic under
  ReadOnly and RejectAll for every caller, the cluster admin included: it is a namespace state, not a permission.
- **D50. Authorization.** The cluster admin passes (it is root). Everyone else needs `Truncate` on the topic or its
  namespace; a namespace admin has it on its own namespace. Anonymous never has it.

## Interaction matrix

The single source of truth is `broker/libs/node/tests/support/TruncateMatrixExpectations.h`. It defines:

```cpp
enum class TrOp : uint8_t { Explicit, Available, BelowStart, AllPartitions, UnknownPartition };          // 5
enum class TrCaller : uint8_t { ClusterAdmin, NsAdminSame, NsAdminForeign, UserGranted, UserNoGrant,
                                Anonymous };                                                              // 6
enum class TrTarget : uint8_t { UserTopic, TxnTopicOpenTxn, TransientTopic, CompactedTopic, InternalTopic,
                                ViewSource, MultiSourceView, PausedViewSource, ReadOnlyNs, MissingTopic }; // 10
enum class TrTiming : uint8_t { Sequential, BackToBack, FollowerOffline, FollowerBelowStart, CrashMidStep,
                                WaitInterrupted, ViewMidRebuild, LeaderChangeMidOp, RetentionRace,
                                ConsumerAtOldStart };
enum class TrStart : uint8_t { Before, HwLsoMin, Current, Lso, Unchanged };   // the expected new start
struct TrOutcome { enum Kind : uint8_t { Ok, PartialOk, Missing, Refused } kind; ErrorCode code; TrStart start; };
TrOutcome expect(TrOp, TrCaller, TrTarget);
bool naRule(TrOp, TrCaller, TrTarget);
std::span<const ErrorCode> expectedCodes();   // every code the matrix can emit
```

The 1-node rig (Task 15), the seeded stress test (Task 16) and the 3-node slice (Task 17) all read it.

- **Ops (5):** Explicit (`beforeOffset` between the start and the HW), Available (`-1`), BelowStart (`beforeOffset`
  below the current start), AllPartitions (empty entries), UnknownPartition (one valid entry plus one partition past
  the count).
- **Callers (6):** ClusterAdmin, NsAdminSame (namespace admin of the topic's namespace), NsAdminForeign (namespace
  admin of another namespace), UserGranted (Truncate on the topic), UserNoGrant (Read and Write only), Anonymous
  (AnonymousRead on for `default`).
- **Targets (10):** UserTopic, TxnTopicOpenTxn (an open txn spans the requested offset), TransientTopic,
  CompactedTopic, InternalTopic (`__catalog`), ViewSource (one single-source view), MultiSourceView (one source of a
  two-source view), PausedViewSource (its view paused OverLimit by the rig), ReadOnlyNs (a UserTopic in a ReadOnly
  namespace), MissingTopic.
- **Timings:** Sequential for every cell; the timing cells below on top.
- **Outcomes:** Ok, PartialOk (the valid entries Ok, the unknown partition InvalidRequest), Missing (exactly the
  answer for an unknown name, UnknownTopic), Refused(code). `start` is the start observed afterwards on every
  replica by a follow-up `ListOffsets(Earliest)`, plus a recompute check for the view targets.

**Rules in `expect`, evaluated in this order:**
1. `naRule`: UnknownPartition on MissingTopic (6 n/a cells: one per caller; a missing topic has no partitions).
2. InternalTopic: Refused(InvalidRequest) for every caller (D38).
3. Anonymous and UserNoGrant: Refused(NotAuthorized), checked before the topic lookup, on every other target.
4. NsAdminForeign: Missing on every other target (the name resolves in its own namespace, `hiddenAsMissing`).
5. MissingTopic: Missing for ClusterAdmin, NsAdminSame and UserGranted.
6. ReadOnlyNs: Refused(NamespaceSuspended) for ClusterAdmin, NsAdminSame and UserGranted (D49).
7. PausedViewSource: Refused(NamespaceLimitExceeded) for the same three (the shadow cannot be reserved, D44); the
   start is Unchanged.
8. Otherwise, for ClusterAdmin, NsAdminSame and UserGranted:
   - Explicit: Ok, start Before; on TxnTopicOpenTxn, start Lso (the open txn's first offset) and the txn survives.
   - Available: Ok, start HwLsoMin; on TxnTopicOpenTxn, start Lso.
   - BelowStart: Ok, start Current (the response carries the current start).
   - AllPartitions: Ok on every partition, each with the Available start.
   - UnknownPartition: PartialOk; the valid entry gets the Explicit start.
   - ViewSource and MultiSourceView: the view equals a recompute over `[newStart, HW)` after the swap; reads during
     the rebuild return the pre-truncate rows.
   - CompactedTopic: no record below the start remains after the cleaner's next pass.

**Timing cells (22)**, each on ClusterAdmin with a seeded workload, on the 1-node rig with two in-process followers
(`TruncateRig.h`):
- **Back-to-back (2):** the same truncate twice before the first reply (both Ok, same start); a truncate then a lower
  one before the reply (the second returns the first's start).
- **Follower offline (2):** a disk and a transient follower offline across the truncate, then caught up; their start
  equals the leader's and the reply waited for the ISR shrink.
- **Follower below start (2):** a disk and a transient follower whose LEO is below the new start; each resumes empty
  at the start and catches up (the hang repro, Task 1).
- **Crash mid-step (4):** the leader after the checkpoint and before the in-memory move; the leader after the move
  and before the segment deletes; a follower after its checkpoint and before its segment drop; a follower mid
  ResetEmpty. Each boots to the checkpointed start with no segment below it.
- **Wait interrupted (3):** an ISR shrink while parked (Ok); a leader change while parked (NotLeader, then the retry
  Ok on the new leader); the deadline (RequestTimedOut, then the retry Ok).
- **View mid-rebuild (4):** a truncate during a view rebuild; a second truncate before the swap; a namespace suspend
  during the rebuild; a restart during the rebuild. Each ends equal to a recompute.
- **Leader change mid-op (2):** before the checkpoint (the retry resolves on the new leader); after the checkpoint
  and before the reply (the new leader may have a lower start; the retry moves it).
- **Retention race (2):** retention fires between the resolve and the checkpoint; retention fires while the reply
  is parked. The start is the max of both; `truncateStart` is the truncate's.
- **Consumer at old start (1):** a consumer fetching at the old start across the truncate gets OffsetOutOfRange
  once and resumes per its reset policy.

**Pinned counts** (`TruncateMatrixCountRegression`): 300 cells, 6 n/a, 294 applicable, 22 timing, 316 executed.

**One more table**, with its count pinned:
- **Clamp table** (`TruncateResolveRegression.ClampTable/*`, Task 5): `beforeOffset` ∈ {-1, below start, at start,
  between, at LSO, at HW, above HW, INT64_MAX} × {LSO = HW, LSO < HW} × {disk, transient} = 32 cells, each with its
  expected start.

## File Structure

New:
- `protocol/include/rune/protocol/client/TruncateMessages.h`: templates 86/87.
- `broker/libs/node/include/rune/node/TruncateRingMessages.h`: `TruncatePartitions`, `TruncatePartitionsReply`,
  `AdvanceStart`, `ViewSourceCut`.
- `broker/libs/gateway/include/rune/gateway/PendingTruncateAgg.h`: the fan-out aggregate and its pool.
- `broker/libs/replication/include/rune/replication/TruncateWait.h`: `PendingTruncate`, `decideAdopt`,
  `lowWatermark`.
- `client-cpp/tools/TopicCommands.cpp`: `rune topic truncate`.
- `broker/docker/truncate-e2e.sh`.
- Tests: `FetchBelowStartRegressionTest.cpp`, `LogAdvanceStartRegressionTest.cpp`,
  `TruncateMessagesTest.cpp`, `TruncateGrantRegressionTest.cpp`, `TruncateResolveRegressionTest.cpp`,
  `TruncateFollowerAdoptRegressionTest.cpp`, `TruncateLowWatermarkRegressionTest.cpp`,
  `TruncateGatewayRegressionTest.cpp`, `ClientTruncateRegressionTest.cpp`, `RetentionAdvanceStartRegressionTest.cpp`,
  `ConsumerOffsetOutOfRangeRegressionTest.cpp`, `ViewTruncateRegressionTest.cpp`,
  `ViewTruncateMemoryRegressionTest.cpp`, `TopicTruncateCliRegressionTest.cpp`, `TruncateMatrixTest.cpp`,
  `TruncateStressTest.cpp`, `TruncateTrioRegressionTest.cpp`.
- Support: `broker/libs/node/tests/support/TruncateMatrixExpectations.h`,
  `broker/libs/node/tests/support/TruncateRig.h` (one leader worker plus two in-process followers on real Aeron UDP,
  an advanceable clock, crash points).
- Bench: `broker/bench/TruncateHotPathBench.cpp`.

Modified (listed per task below).

---

## Tasks

### Task 1: replication — a fetch below the start always gets an answer

Spec §6 step 6 ("the disk hang is fixed first, repro first"), step 4; D39.

**Files:**
- Modify:
  - `broker/libs/replication/src/PartitionReplica.cpp`: in `onFetchRequest` (~:1088), when
    `req.fetchOffset < log start` (disk or transient), send a record-less `FetchResponse` carrying the start instead
    of returning `kErrNone`; in `retryOwedFetchAnswers_` (~:1167) answer the same case instead of `continue`;
  - `broker/libs/replication/include/rune/replication/PartitionReplica.h`: `makeFetchResponse` (~:1444/:1452)
    advertises the real disk start;
  - the follower (`applyReplicated` ~:1268): a disk follower whose LEO is below the response's start calls
    `Log::truncateFullyAndStartAt(start)` (this task adds only this ResetEmpty arm of `decideAdopt`; Task 6 adds
    Advance);
  - `broker/libs/storage/src/Log.cpp`, `Log.h`: `truncateFullyAndStartAt(off)`: checkpoint `off`, close and delete
    every segment, open an empty active segment based at `off`; boot (~:120–176): a checkpoint above every segment
    drops all of them and opens an empty segment at the checkpoint.
- Test: `broker/libs/replication/tests/FetchBelowStartRegressionTest.cpp`,
  `broker/libs/storage/tests/LogAdvanceStartRegressionTest.cpp` (created here, extended in Task 2).

**Interfaces:**
```cpp
namespace rune::storage {
class Log {
public:
    // Checkpoint `off`, delete every segment, resume empty at `off`.  Requires off >= logStartOffset().
    [[nodiscard]] StorageResult truncateFullyAndStartAt(int64_t off);
};
}
```

- [ ] **Step 1: failing tests** (repro first, against today's code):
  - `FetchBelowStartRegression.DiskFollowerGetsAnswerRegression`: leader on disk with retention having moved its
    start to 1000; a follower fetches at 10; assert an answer within 3 `doWork` calls carrying `logStartOffset =
    1000` and no records. Today: no answer (fails on the iteration cap).
  - `FetchBelowStartRegression.OwedRetryAnswersRegression`: the same fetch parked as owed; the retry answers it.
  - `FetchBelowStartRegression.DiskFollowerResumesEmptyAtStartRegression`: the follower ends with start = LEO = 1000,
    then catches up to the leader's LEO and rejoins the ISR.
  - `FetchBelowStartRegression.TransientFollowerUnchangedRegression`: the transient path still answers as today.
  - `FetchBelowStartRegression.DiskStartAdvertisedRegression`: every disk `FetchResponse` carries the real start.
  - `LogAdvanceStartRegression.BootCheckpointAboveEverySegmentRegression`: write segments to 500, a checkpoint of
    1000, reopen: start = LEO = 1000, no segment file below.
  - `LogAdvanceStartRegression.TruncateFullyCrashAfterCheckpointRegression`: crash after the checkpoint and before
    the deletes (the rig's crash hook); reopen gives the same result.
  - Mutation: restore the `!belowRingStart` early return → `DiskFollowerGetsAnswerRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'FetchBelowStartRegression|LogAdvanceStartRegression|PartitionReplica|Replication' --output-on-failure -j8`.
- [ ] **Step 3:** commit `replication: answer a fetch below the log start; follower resumes empty there` and push.

### Task 2: storage — advance the log start; the truncateStart checkpoint

Spec §6 steps 1, 4, 7; D35.

**Files:**
- Modify: `broker/libs/storage/src/Log.cpp`, `broker/libs/storage/include/rune/storage/Log.h`:
  - `advanceLogStart(newStart, reason)`: if `newStart <= start` return the start; write the checkpoint
    (`logStart = newStart`, `truncateStart = newStart` for Truncate, unchanged for Retention); move the in-memory
    start (`persistedLogStart_`, a read below it returns OffsetBelowStart); enqueue every segment wholly below
    `newStart` through `enqueueDeletedSegment` (the same async path as retention). A segment that straddles
    `newStart` stays and is read from `newStart`.
  - the checkpoint format: 20 bytes `i64 logStart | i64 truncateStart | crc32c`; `readLogStartCheckpoint` accepts
    12 bytes as `truncateStart = 0`;
  - `truncateStart()` accessor.
  - `InMemoryLog.h`: `advanceStart(newStart)` moves the ring start, never back.
- Test: `LogAdvanceStartRegressionTest.cpp` (extend), `broker/libs/storage/tests/InMemoryLogTest.cpp` (extend).

**Interfaces:**
```cpp
namespace rune::storage {
enum class StartReason : uint8_t { Truncate, Retention };
class Log {
public:
    // Returns the start after the call (>= the previous start).  Checkpoint first, then memory, then async deletes.
    [[nodiscard]] Result<int64_t> advanceLogStart(int64_t newStart, StartReason);
    int64_t truncateStart() const noexcept;
};
}
```

- [ ] **Step 1: failing tests**:
  - `LogAdvanceStartRegression.NeverMovesBackRegression`: advance to 500, then to 300: the start stays 500.
  - `LogAdvanceStartRegression.ReadBelowStartReportsRegression`: a read at 499 after advancing to 500 returns
    OffsetBelowStart, not `{}`.
  - `LogAdvanceStartRegression.StraddlingSegmentKeptRegression`.
  - `LogAdvanceStartRegression.CrashAtEveryStepRegression`: crash after the checkpoint, after the memory move, after
    each delete; each reopen gives start = 500 and no segment wholly below it.
  - `LogAdvanceStartRegression.OldCheckpointFormatReadsRegression`: a 12-byte file reads with `truncateStart = 0`.
  - `LogAdvanceStartRegression.RetentionKeepsTruncateStartRegression`.
  - `LogAdvanceStartRegression.TornCheckpointKeepsOldRegression`: a short or bad-CRC tmp file is ignored.
  - `InMemoryLogRegression.AdvanceStartNeverBackRegression`.
  - Mutation: move the in-memory start before the checkpoint write → `CrashAtEveryStepRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LogAdvanceStartRegression|InMemoryLog|Log' --output-on-failure -j8`.
- [ ] **Step 3:** commit `storage: advanceLogStart and the truncateStart checkpoint` and push.

### Task 3: protocol — TruncateTopic 86/87, codes 55/56, replication schema 5

Spec §6, §10; D34, D36.

**Files:**
- Modify:
  - `protocol/include/rune/protocol/client/Protocol.h`: templates `TruncateTopic = 86`,
    `TruncateTopicResponse = 87`; `OffsetOutOfRange = 55`, `RequestTimedOut = 56` and their gating (≥ 14; else no
    code and Internal respectively) in the gating function (~:383–459);
  - `protocol/include/rune/protocol/client/Dispatch.h`: route 86/87, gap comment 88–95;
  - `broker/libs/replication/include/rune/replication/ReplicationMessages.h` and
    `protocol/schema/rune-replication.xml`: `FetchRequest.logStartOffset i64@60` (`BLOCK_LENGTH` 68),
    `FetchResponse.truncateStartOffset i64` at the end of its block, `kSchemaVersion 5`,
    `kMinReplicationVersion 5`.
- Create: `protocol/include/rune/protocol/client/TruncateMessages.h`.
- Test: `protocol/tests/TruncateMessagesTest.cpp`, `broker/libs/replication/tests/ReplicationMessagesTest.cpp`
  (extend).

**Interfaces:**
```cpp
namespace rune::protocol::client {
struct TruncateEntry { int32_t partition; int64_t beforeOffset; };            // -1 = as far as available
struct TruncateTopicRequest {                                                  // template 86
    std::string_view topic;
    FixedArrayView<TruncateEntry> entries;                                     // empty = every partition
    DdlExtension ext;
};
struct TruncateEntryResult { int32_t partition; ErrorCode code; int64_t newStart; };
struct TruncateTopicResponse {                                                 // template 87
    ErrorCode code;                                                            // whole-request refusal, else None
    FixedArrayView<TruncateEntryResult> entries;
};
size_t encodeTruncateTopic(const TruncateTopicRequest&, uint8_t* out, size_t cap);          // 0 if cap short
bool   decodeTruncateTopic(const uint8_t* in, size_t len, TruncateTopicRequest&);
size_t encodeTruncateTopicResponse(const TruncateTopicResponse&, uint8_t* out, size_t cap);
bool   decodeTruncateTopicResponse(const uint8_t* in, size_t len, TruncateTopicResponse&);
}
```

- [ ] **Step 1: failing tests**:
  - `TruncateMessagesRegression.RoundTripRegression`: 0, 1 and `kMaxTruncateEntries` entries.
  - `TruncateMessagesRegression.TruncatedAtEveryOffsetRegression`: every prefix of each frame decodes false.
  - `TruncateMessagesRegression.EntryCountOverMaxRefusedRegression`.
  - `TruncateMessagesRegression.BeforeOffsetBelowMinusOneRefusedRegression`: `-2` decodes false.
  - `ErrorGatingRegression.Codes55And56GatedRegression`: client 13 gets no OffsetOutOfRange and Internal for 56.
  - `ReplicationMessagesRegression.FetchRequestLogStartRoundTripRegression` and
    `…FetchResponseTruncateStartRoundTripRegression`; `…SchemaBelow5RefusedRegression`.
  - `DispatchRegression.Templates86And87RoutedRegression`.
  - Mutation: encode `logStartOffset` at offset 52 → `FetchRequestLogStartRoundTripRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'TruncateMessagesRegression|ErrorGatingRegression|ReplicationMessages|DispatchRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `protocol: TruncateTopic templates, codes 55/56, replication schema 5` and push.

### Task 4: auth — the Truncate grant and isInternalTopicName

Spec §6 (request, grant, internal topics); D38, D50.

**Files:**
- Modify:
  - `codec/include/rune/catalog/TopicName.h`: `constexpr bool isInternalTopicName(std::string_view)`; replace every
    open-coded `starts_with(kReservedTopicPrefix)` (grep) with it;
  - `broker/libs/gateway/src/ClientGateway.cpp`: `compilePerms_` (~:8521) compiles `Truncate` into the session's
    permission bits; the cluster admin has every bit;
  - `SecurityState.cpp` (~:110–137): `Truncate` granted on a topic or a namespace; a namespace admin implies it on
    its namespace.
- Test: `broker/libs/gateway/tests/TruncateGrantRegressionTest.cpp`, `codec/tests/TopicNameTest.cpp` (extend).

- [ ] **Step 1: failing tests**:
  - `TopicNameRegression.IsInternalTopicNameRegression`: `__catalog`, `__security`, `__x` true; `_x`, `x__`, empty
    false.
  - `TruncateGrantRegression.GrantOnTopicAllowsRegression`, `…GrantOnNamespaceAllowsRegression`,
    `…NsAdminImpliesRegression`, `…ClusterAdminAlwaysRegression`, `…ReadWriteOnlyDeniedRegression`,
    `…AnonymousNeverRegression`, `…RevokeTakesEffectNextRequestRegression`.
  - Mutation: compile `Truncate` from the `Write` bit → `ReadWriteOnlyDeniedRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'TopicNameRegression|TruncateGrantRegression|Security' --output-on-failure -j8`.
- [ ] **Step 3:** commit `auth: compile the Truncate grant; isInternalTopicName` and push.

**Checkpoint A.**

### Task 5: replication — leader resolve, clamps and checkpoint

Spec §6 (resolve, never back, retry), steps 1–2; D40.

**Files:**
- Modify:
  - `broker/libs/replication/src/PartitionReplica.cpp`, `PartitionReplica.h`: `resolveTruncate(beforeOffset)` returns
    `max(start, min(before == -1 ? INT64_MAX : before, HW, LSO))`, with the LSO from `cleanerSwapBound()`
    (~:899) for txn topics and the HW otherwise; `applyTruncate(newStart)` calls `advanceLogStart(…, Truncate)` (or
    the transient `advanceStart`) and, on a parked fetch, completes it so the follower learns the start at once;
  - a non-leader answers NotLeader.
- Test: `broker/libs/replication/tests/TruncateResolveRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace rune::replication {
class PartitionReplica {
public:
    int64_t resolveTruncate(int64_t beforeOffset) const noexcept;   // pure; never below the current start
    [[nodiscard]] Result<int64_t> applyTruncate(int64_t newStart); // leader only; returns the start after
};
}
```

- [ ] **Step 1: failing tests**:
  - `TruncateResolveRegression.ClampTable/*` (TEST_P, 32 cells as the Interaction matrix defines).
  - `TruncateResolveRegression.OpenTxnSurvivesRegression`: an open txn from offset 100, truncate `-1` with HW 200:
    start 100; commit the txn; `read_committed` returns all of its records.
  - `TruncateResolveRegression.BelowStartReturnsCurrentRegression`.
  - `TruncateResolveRegression.ParkedFetchCompletedOnStartChangeRegression`: a follower's fetch parked at the LEO
    is answered on the truncate with the new start.
  - `TruncateResolveRegression.NotLeaderRefusedRegression`.
  - `TruncateResolveRegression.CheckpointBeforeMemoryRegression`: the crash hook after the checkpoint; reopen has
    the new start.
  - Mutation: drop the LSO from the min → `OpenTxnSurvivesRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'TruncateResolveRegression|PartitionReplica' --output-on-failure -j8`.
- [ ] **Step 3:** commit `replication: resolve and apply a truncate on the leader` and push.

### Task 6: replication — follower adoption, disk and transient

Spec §6 steps 3–5; D36, D39.

**Files:**
- Modify:
  - `broker/libs/replication/include/rune/replication/TruncateWait.h` (create): `decideAdopt`;
  - `PartitionReplica.cpp`: the follower runs `decideAdopt` on every `FetchResponse`, before the error branch
    (~:1279) and before `followerSync_.onResponse` (~:1369): Advance → `advanceLogStart(start, reason)` where the
    reason is Truncate when the response's `truncateStartOffset` moved, else Retention; ResetEmpty →
    `truncateFullyAndStartAt` (disk) or `resetTransientLogTo_` (transient, ~:3757);
  - the follower fills `FetchRequest.logStartOffset` with its own start; the leader stores it in
    `FollowerSlot::logStartOffset` (`FollowerSlots.h` ~:39) on every fetch, reset in `resetLeadership` (~:145).
- Test: `broker/libs/replication/tests/TruncateFollowerAdoptRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace rune::replication {
struct AdoptNone {}; struct AdoptAdvance { int64_t start; }; struct AdoptResetEmpty { int64_t start; };
using Adopt = std::variant<AdoptNone, AdoptAdvance, AdoptResetEmpty>;
constexpr Adopt decideAdopt(int64_t followerStart, int64_t followerLeo, int64_t leaderStart) noexcept;
}
```

- [ ] **Step 1: failing tests**:
  - `TruncateFollowerAdoptRegression.DecideAdoptTableRegression`: every ordering of the three values.
  - `TruncateFollowerAdoptRegression.DiskAdvanceRegression`, `…TransientAdvanceRegression`.
  - `TruncateFollowerAdoptRegression.DiskResetEmptyRegression`, `…TransientResetEmptyRegression`.
  - `TruncateFollowerAdoptRegression.ErrorResponseStillAdoptsRegression`: a DivergingEpoch response carrying a
    higher start is adopted.
  - `TruncateFollowerAdoptRegression.StartNeverMovesBackRegression`: a new leader with a lower start; the follower
    keeps its own.
  - `TruncateFollowerAdoptRegression.SlotKeepsFollowerStartRegression`.
  - `TruncateFollowerAdoptRegression.CrashBetweenCheckpointAndDropRegression`.
  - `TruncateFollowerAdoptRegression.TruncateStartFollowsLeaderRegression`.
  - Mutation: skip adoption on the error branch → `ErrorResponseStillAdoptsRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'TruncateFollowerAdoptRegression|FetchBelowStartRegression|PartitionReplica' --output-on-failure -j8`.
- [ ] **Step 3:** commit `replication: followers adopt the leader's start on every response` and push.

### Task 7: replication — the low watermark and the parked reply

Spec §6 (reply timing, ISR shrink, timeout); D40, D41.

**Files:**
- Modify:
  - `TruncateWait.h`: `PendingTruncate`, `lowWatermark(leaderStart, slots, isr)`;
  - `Worker.h`: `pendingTruncates_` (`OrderedWaitQueue<PendingTruncate>`, sized `RUNE_TRUNCATE_MAX_PENDING`) next
    to `pendingAcksAll_` (~:5862); a sweep after the HW step completes every entry with
    `newStart <= lowWatermark`, answers the expired ones RequestTimedOut, and on leadership loss answers all of them
    NotLeader, using the sweep points of `pendingAcksAll_` (~:6871, ~:7523, ~:7639);
  - the ISR change paths (`applyCommittedIsr_` ~:2901, `shrinkIsrToLive` ~:3056, `updateIsr_` ~:3100) mark the
    watermark dirty so a shrink completes in the same `doWork`.
- Test: `broker/libs/node/tests/TruncateLowWatermarkRegressionTest.cpp` (with `TruncateRig.h`, created here).

- [ ] **Step 1: failing tests**:
  - `TruncateLowWatermarkRegression.NoReplyBeforeEveryIsrMemberAdoptsRegression`: two followers; hold one's fetches;
    no reply; release; reply in the next `doWork`.
  - `TruncateLowWatermarkRegression.IsrShrinkCompletesRegression`.
  - `TruncateLowWatermarkRegression.LeaderLossAnswersNotLeaderRegression`.
  - `TruncateLowWatermarkRegression.DeadlineAnswersTimedOutRegression`: advance the clock to the deadline.
  - `TruncateLowWatermarkRegression.RetryAfterTimeoutIdempotentRegression`.
  - `TruncateLowWatermarkRegression.QueueFullThrottledRegression`.
  - `TruncateLowWatermarkRegression.EachEntryAnsweredExactlyOnceRegression`: every path above, counted.
  - `TruncateLowWatermarkRegression.SingleNodeRepliesAtOnceRegression`.
  - Mutation: compute the watermark from the leader alone → `NoReplyBeforeEveryIsrMemberAdoptsRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'TruncateLowWatermarkRegression|Truncate|Worker' --output-on-failure -j8`.
- [ ] **Step 3:** commit `replication: reply to a truncate at the ISR low watermark` and push.

**Checkpoint B.**

### Task 8: gateway — fan-out, aggregation and refusals

Spec §6 (request, internal topics, unknown partition); D37, D38, D44 (the hook only), D49, D50.

**Files:**
- Modify:
  - `broker/libs/gateway/src/ClientGateway.cpp`: `onClientFrame_` (~:1148) routes template 86 to
    `handleTruncateTopic_`: decode → internal-topic check (InvalidRequest) → authorize (NotAuthorized) → resolve the
    name in the session's namespace (UnknownTopic, `hiddenAsMissing`) → the suspend table (NamespaceSuspended) →
    the shadow reservation hook (no-op until Task 12) → split entries per owner worker (unknown partitions answered
    InvalidRequest in place) → send `TruncatePartitions` on each ingress ring;
  - the worker handles `TruncatePartitions` by calling `resolveTruncate`/`applyTruncate` and parking each entry, and
    returns `TruncatePartitionsReply` on its return ring;
  - `PendingTruncateAgg.h` (create): the pool, the per-entry slots, `owed`; the response goes out when `owed == 0`;
  - the suspend table (Phase 2 `NamespaceSuspendTable.h`): template 86 refused under ReadOnly and RejectAll for every
    caller.
- Create: `broker/libs/node/include/rune/node/TruncateRingMessages.h`.
- Test: `broker/libs/gateway/tests/TruncateGatewayRegressionTest.cpp` (real broker via `NamespaceRig.h`).

- [ ] **Step 1: failing tests**:
  - `TruncateGatewayRegression.InternalTopicRefusedForEveryCallerRegression`: all six callers on `__catalog` and
    `__security`.
  - `TruncateGatewayRegression.FanOutAcrossWorkersRegression`: `RUNE_WORKERS=2`, a topic with 8 partitions; every
    entry answered with its start.
  - `TruncateGatewayRegression.UnknownPartitionPartialRegression`.
  - `TruncateGatewayRegression.EmptyEntriesMeansAllRegression`.
  - `TruncateGatewayRegression.ForeignNamespaceMissingRegression`.
  - `TruncateGatewayRegression.ReadOnlyRefusedForAdminRegression`.
  - `TruncateGatewayRegression.SessionClosedMidAggregateRegression`: the aggregate is freed and the late replies are
    dropped and counted.
  - `TruncateGatewayRegression.PoolExhaustedThrottledRegression`.
  - Mutation: check internal names after authorization → `InternalTopicRefusedForEveryCallerRegression` fails for
    the denied callers (they get NotAuthorized).
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'TruncateGatewayRegression|ClientGateway' --output-on-failure -j8`.
- [ ] **Step 3:** commit `gateway: TruncateTopic fan-out and aggregation` and push.

### Task 9: client — truncateTopic and truncateTopicAndWait

Spec §6 (response, retry); D41.

**Files:**
- Modify: `client-cpp/include/rune/client/Client.h` (~:462, next to `dropTopic` ~:866): `truncateTopic`,
  `truncateTopicAndWait`; `client-cpp/src/Client.cpp`: `OpKind::TruncateTopic` (~:1429), issued through
  `issueDdl_` (~:1442); `truncateTopicAndWait` retries NotLeader and RequestTimedOut entries only, up to the
  client's DDL retry cap, and returns the per-entry results.
- Test: `client-cpp/tests/ClientTruncateRegressionTest.cpp` (real broker).

**Interfaces:**
```cpp
namespace rune::client {
struct TruncateResult { int32_t partition; ErrorCode code; int64_t newStart; };
class Client {
public:
    // entries empty = every partition; beforeOffset -1 = as far as available.
    Status truncateTopic(std::string_view topic, std::span<const TruncateEntry> entries,
                         std::span<TruncateResult> out, Callback done);
    Result<size_t> truncateTopicAndWait(std::string_view topic, std::span<const TruncateEntry> entries,
                                        std::span<TruncateResult> out, std::chrono::milliseconds timeout);
};
}
```

- [ ] **Step 1: failing tests**:
  - `ClientTruncateRegression.AndWaitReturnsStartsRegression`.
  - `ClientTruncateRegression.RetriesNotLeaderEntryOnlyRegression`: a fake gateway answers one entry NotLeader; only
    it is resent.
  - `ClientTruncateRegression.RetriesTimedOutIdempotentRegression`.
  - `ClientTruncateRegression.OutSpanTooSmallRefusedRegression`.
  - Mutation: resend every entry on retry → `RetriesNotLeaderEntryOnlyRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'ClientTruncateRegression|Client' --output-on-failure -j8`.
- [ ] **Step 3:** commit `client: truncateTopic and truncateTopicAndWait` and push.

### Task 10: retention through the same path; consumer OffsetOutOfRange

Spec §6 step 7, consumers; D46, D47; fix on sight (retention never reaches followers).

**Files:**
- Modify:
  - `broker/libs/storage/include/rune/storage/LogCleanerAgent.h` (`runRetention` ~:812): at ~:942–958 send
    `AdvanceStart{…, Retention}` to the owner instead of enqueuing deletes directly; the owner calls
    `advanceLogStart`;
  - the gateway subscribe (`ClientGateway.cpp` ~:5169–5185) and the fetch path (`executeFetchRequestWire_` ~:4808):
    an explicit offset below the start answers OffsetOutOfRange for client ≥ 14, else the clamp;
  - the client: OffsetOutOfRange runs the reset policy (`earliest` → the start, `latest` → the end, `none` → the
    error to the caller).
- Test: `broker/libs/storage/tests/RetentionAdvanceStartRegressionTest.cpp`,
  `client-cpp/tests/ConsumerOffsetOutOfRangeRegressionTest.cpp`.

- [ ] **Step 1: failing tests**:
  - `RetentionAdvanceStartRegression.FollowersAdoptRetentionStartRegression`: today the followers keep every segment.
  - `RetentionAdvanceStartRegression.RetentionDoesNotMoveTruncateStartRegression`.
  - `RetentionAdvanceStartRegression.OnlyOwnerAdvancesRegression`: the cleaner never calls `advanceLogStart`.
  - `ConsumerOffsetOutOfRangeRegression.ExplicitBelowStartRegression` (client 14) and
    `…OldClientKeepsClampRegression` (client 13).
  - `ConsumerOffsetOutOfRangeRegression.ResetEarliestRegression`, `…ResetLatestRegression`, `…ResetNoneRaisesRegression`.
  - `ConsumerOffsetOutOfRangeRegression.EarliestUnchangedRegression`.
  - Mutation: keep the clamp for client 14 → `ExplicitBelowStartRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'RetentionAdvanceStartRegression|ConsumerOffsetOutOfRangeRegression|Retention|Subscribe' --output-on-failure -j8`.
- [ ] **Step 3:** commit `storage: retention moves the start through the owner; consumers get OffsetOutOfRange` and push.

**Checkpoint C.**

### Task 11: views — the shadow rebuild, the swap and the snapshot rewrite

Spec §6 (views); D35, D42, D43, D45; fix on sight (a view stalls silently below the start).

**Files:**
- Modify:
  - `broker/libs/node/include/rune/node/WorkerViewSet.h`, `WorkerViewSet.cpp`: `ViewReadiness` (~:92) gains
    `ViewRebuilding{shadow, cutStarts[kMaxViewSources], swapAt}`; `ViewSourceCut` starts it (a second cut replaces
    `cutStarts` and resets the shadow); `driveView_` (~:599–701) folds the shadow from each source's cut start up to
    `lso(liveHw)` on the `driveRebuild` budget (~:374); reads use the old table until the swap; at the swap the old
    table's chunks return to the pool;
  - the replica owner posts `ViewSourceCut{topicId, partition, truncateStart}` to every view host over the topic
    after `applyTruncate`;
  - `ViewSnapshotIoAgent.cpp` (~:139/:161/:240): a full rewrite through tmp + fsync + rename + dir fsync; the header
    records each source's applied cut;
  - recovery (`replayInstance` cpp ~:87, `installViewForRecovery` cpp ~:550): a snapshot whose applied cut is below
    the source's `truncateStart` is discarded and the view rebuilds from it;
  - `Log::readInto` below the start returns OffsetBelowStart; the view fold treats it as a cut it has not yet
    received only for Truncate, and skips to the start for Retention.
- Test: `broker/libs/node/tests/ViewTruncateRegressionTest.cpp`.

- [ ] **Step 1: failing tests**:
  - `ViewTruncateRegression.EqualsRecomputeAfterSwapRegression`: a single-source aggregate view.
  - `ViewTruncateRegression.EqualsRecomputeMultiSourceRegression`.
  - `ViewTruncateRegression.OldStateServesDuringRebuildRegression`.
  - `ViewTruncateRegression.SecondCutBeforeSwapRegression`.
  - `ViewTruncateRegression.EqualsRecomputeAfterRestartAtEveryStepRegression`: before the swap, after the swap and
    before the rewrite, after the rewrite.
  - `ViewTruncateRegression.EqualsRecomputeAfterLeaderChangeRegression`.
  - `ViewTruncateRegression.RetentionDoesNotCutRegression`.
  - `ViewTruncateRegression.SuspendDuringRebuildRegression`.
  - Mutation: swap before the shadow reaches `swapAt` → `OldStateServesDuringRebuildRegression` or
    `EqualsRecomputeAfterSwapRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'ViewTruncateRegression|View' --output-on-failure -j8`.
- [ ] **Step 3:** commit `view: cut views on truncate with a shadow rebuild and snapshot rewrite` and push.

### Task 12: views — the memory reservation and PAUSED on other hosts

Spec §6 (views); D44.

**Files:**
- Modify:
  - the limit aggregator (Phase 2 `NsLimitAggregator.h`): `tryReserveViewMem(nsSlot, bytes)` and `release`, on the
    control thread;
  - the gateway's reservation hook (Task 8): sum the current sizes of the views over the topic; NamespaceLimitExceeded
    on failure, before fan-out;
  - `WorkerViewSet.cpp`: a host that cannot take the shadow's chunks moves the view to
    `ViewPaused{OverLimit, newStart}` and resumes by rebuilding from there.
- Test: `broker/libs/node/tests/ViewTruncateMemoryRegressionTest.cpp`.

- [ ] **Step 1: failing tests**:
  - `ViewTruncateMemoryRegression.ReservationFailureRefusesBeforeResolveRegression`: the start is unchanged on every
    replica.
  - `ViewTruncateMemoryRegression.ReservationReleasedAtSwapRegression`.
  - `ViewTruncateMemoryRegression.ReservationReleasedWhenAllEntriesFailRegression`.
  - `ViewTruncateMemoryRegression.HostWithoutRoomPausesAtStartRegression` and
    `…PausedResumesEqualsRecomputeRegression`.
  - Mutation: reserve after fan-out → `ReservationFailureRefusesBeforeResolveRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'ViewTruncateMemoryRegression|NsViewMemoryRegression|View' --output-on-failure -j8`.
- [ ] **Step 3:** commit `view: reserve the truncate shadow; pause hosts without room` and push.

### Task 13: CLI — rune topic truncate

Spec §10.

**Files:**
- Create: `client-cpp/tools/TopicCommands.cpp`; list it in `client-cpp/CMakeLists.txt` (~:1715).
- Modify: `client-cpp/tools/Cli.cpp`: `printUsage` (~:88) and the dispatch (~:446–477).
- Test: `client-cpp/tests/TopicTruncateCliRegressionTest.cpp`.

- [ ] **Step 1: failing tests**:
  - `TopicTruncateCliRegression.AllPartitionsAvailableRegression`: `rune topic truncate t` prints one line per
    partition with its new start.
  - `TopicTruncateCliRegression.PartitionAndBeforeRegression`: `--partition 2 --before 500`.
  - `TopicTruncateCliRegression.BadArgsExitNonZeroRegression`: `--before -2`, `--partition x`, a missing topic.
  - `TopicTruncateCliRegression.PartialFailureExitsNonZeroRegression`.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'TopicTruncateCliRegression|Cli' --output-on-failure -j8`.
- [ ] **Step 3:** commit `cli: rune topic truncate` and push.

### Task 14: hot path — zero allocation and the ABAB bench

Spec §9 (hot path); rule 12.

**Files:**
- Create: `broker/bench/TruncateHotPathBench.cpp` (fetch serve with the slot store, follower apply with
  `decideAdopt`).
- Run: `WorkerProduceLatencyBench`, `ViewFoldBench`, `GetViewRowServeBench`, `TruncateHotPathBench`.

- [ ] **Step 1:** the allocation tests: `TruncateHotPathRegression.FetchServeZeroAllocRegression`,
  `…FollowerApplyZeroAllocRegression`, `…ViewFoldZeroAllocRegression` (the counting allocator), and
  `…TruncateSteadyStateZeroAllocRegression` (100 truncates after warm-up allocate nothing on the worker).
- [ ] **Step 2:** on the clean box (BENCHMARKING.md), interleaved ABAB against the Phase 2 tip, n ≥ 5 per bench
  (n ≥ 15 for any p99 claim). A p99 shift outside noise is a failing gate: name the code path and fix it.
- [ ] **Step 3:** record the results in BENCHMARKING.md, commit `bench: truncate hot-path ABAB` and push.

**Checkpoint D.**

### Task 15: the interaction matrix rig

Spec §8; the Interaction matrix section above.

**Files:**
- Create: `broker/libs/node/tests/support/TruncateMatrixExpectations.h`,
  `broker/libs/node/tests/TruncateMatrixTest.cpp`; extend `TruncateRig.h`.

- [ ] **Step 1:** write the expectations header exactly as the Interaction matrix section defines it (`TrOp`,
  `TrCaller`, `TrTarget`, `TrTiming`, `TrStart`, `TrOutcome`, `expect`, `naRule`, `expectedCodes`).
- [ ] **Step 2: tests**:
  - `TruncateMatrixCountRegression.PinnedRegression`: 300 cells, 6 n/a, 294 applicable, 22 timing, 316 executed.
  - `TruncateMatrixRegression.Cell/*` (TEST_P over the 294 applicable cells, real broker): each asserts the code,
    the start on every replica and, for view targets, equality with a recompute.
  - `TruncateMatrixRegression.Timing/*` (TEST_P over the 22 timing cells).
  - Mutation check: let UserNoGrant truncate → more than 30 cells turn red; record the exact count in the commit.
- [ ] **Step 3:** run `ctest --test-dir build -R 'TruncateMatrix' --repeat until-fail:20 --output-on-failure` and
  once under `-j8` load. Fix every red cell at its root (rule 13), each with its own regression test.
- [ ] **Step 4:** commit `test: truncate interaction matrix` and push.

### Task 16: randomized model-checked stress

Spec §8.

**Files:**
- Create: `broker/libs/node/tests/TruncateStressTest.cpp`.

- [ ] **Step 1:** the op set: produce (plain and txn), truncate (every op kind), retention tick, consumer fetch at a
  random offset, follower pause and resume, crash and restart of any replica at any crash point, leader change, view
  create, namespace suspend and resume. A model holds each partition's expected start, `truncateStart`, records and
  each view's recompute.
- [ ] **Step 2:** after every step check the invariants: no replica's start moves back; every replica's start ≤ the
  leader's HW; a replied truncate's start ≤ every ISR member's start; produced records at or above the start are
  consumed exactly once; open-txn records survive; every view equals the model's recompute once no rebuild is
  pending; no fetch goes unanswered past 3 `doWork` calls.
- [ ] **Step 3:** seeds 1..20, a 2 000-step cap per seed, fail on the cap; `--repeat until-fail:20` and one run
  under `-j8`. Mutation: drop the follower-slot store of `logStartOffset` → a seed fails; record which.
- [ ] **Step 4:** commit `test: truncate model-checked stress` and push.

### Task 17: the 3-node slice

Spec §8.

**Files:**
- Create: `broker/libs/node/tests/TruncateTrioRegressionTest.cpp` (`CatalogTrio.h`, `AuthTrio.h`).

- [ ] **Step 1: tests**:
  - `TruncateTrioRegression.EveryNodeAdoptsBeforeReplyRegression`.
  - `TruncateTrioRegression.LeaderKillWhileParkedRetrySucceedsRegression`.
  - `TruncateTrioRegression.FollowerDownRejoinsEmptyAtStartRegression`: disk and transient.
  - `TruncateTrioRegression.ViewOnNewLeaderEqualsRecomputeRegression`: kill the leader after the swap and before the
    rewrite, and after the rewrite.
  - `TruncateTrioRegression.AcksAllNoLossAcrossTruncateRegression`: produced at or above the start = consumed.
  - `TruncateTrioRegression.RetentionReachesFollowersRegression`.
- [ ] **Step 2:** run `ctest --test-dir build -R 'TruncateTrioRegression' --repeat until-fail:20 --output-on-failure`.
- [ ] **Step 3:** commit `test: truncate 3-node slice` and push.

### Task 18: e2e and docs

Spec §8; rule 11.

**Files:**
- Create: `broker/docker/truncate-e2e.sh`.
- Modify: `docs/namespaces.md` (or the truncate section of the Phase 1 doc), `CLAUDE.md` §8.

- [ ] **Step 1:** the e2e script, on the default config, 3 nodes:
  1. Create a topic with 4 partitions, a txn topic and a view; produce 10 000 records.
  2. `rune topic truncate t --before 5000`: every partition's start is 5000 on every node (`ListOffsets` against
     each).
  3. A consumer at offset 100 gets OffsetOutOfRange and resets to earliest = 5000.
  4. The view equals a recompute over `[5000, HW)`.
  5. Truncate `__catalog`: refused InvalidRequest.
  6. Stop a follower, truncate `-1`, restart it: it resumes empty at the start and rejoins the ISR.
  7. Restart every broker; all of the above still holds.
- [ ] **Step 2:** run it on the shared VM (`$RUNE_VM_HOST`):
  1. Hold the shared lock for the whole run.
  2. Work only in your own directory.
  3. Check `df` before and after, and clean up only your own containers.
- [ ] **Step 3:** docs: TruncateTopic, its guarantees (reply at the low watermark, never back, views recompute),
  its codes and the CLI. CLAUDE.md §8: one line that topics can be truncated and views are cut with them.
- [ ] Commit and push.

**Checkpoint E**, then the final whole-branch review (most capable model, exhaustive and adversarial, against the
Review Focus list and the matrix).

## Spec gaps (decided here, recorded for review)

- **`truncateStart` in the checkpoint and the fetch response (D35, D36):** the spec says a new leader restores from
  the rewritten snapshot, but not how it tells a truncate from retention when the snapshot was not yet rewritten.
  One more i64 on disk and on the wire makes it durable and replicated.
- **Retention does not cut views (D47):** the spec routes retention through the truncate path; cutting views on
  retention would change what every aggregate means, so only a TruncateTopic cuts them.
- **The fan-out (D37):** the spec does not say how a multi-partition request reaches several workers; one message
  per owner worker on the existing rings, aggregated in the gateway.
- **Views on leaders only (D42):** "every replica cuts its own views" is read as every view host.
- **A full shadow rebuild (D43):** `foldCursor` is shared across sources, so a cut of one source rebuilds the view.
- **ReadOnly refuses the cluster admin (D49):** a suspend is a namespace state, not a permission.
- **Full queue answers Throttled (D41):** a bounded queue needs a backpressure answer; Throttled with 1 ms is the
  existing one.
- **No client version bump (D34):** codes 55 and 56 are gated on client ≥ 14; the replication schema moves to 5
  because the spec rules out mixed-version clusters.
- **Compacted topics (D48):** the spec's "every record below the start is removed" is enforced by the cleaner
  starting at the log start.

## Self-review (spec section → task)

| Spec | Task |
|---|---|
| §6 request, grant, internal topics | 3, 4, 8 |
| §6 resolve, clamps, never back, retry | 5, 9 |
| §6 reply at the low watermark, ISR shrink, timeout | 7 |
| §6 step 1 checkpoint then memory | 2, 5 |
| §6 steps 2–5 fetch response, follower adoption, boot, FetchRequest field | 1, 3, 6 |
| §6 step 6 the below-start hang | 1 |
| §6 step 7 async deletes, retention on the same path | 2, 10 |
| §6 step 8 a new leader with a lower start | 6, 7, 17 |
| §6 consumers | 10 |
| §6 views: cut, shadow, swap, snapshots | 11 |
| §6 views: reservation, PAUSED | 12 |
| §6 open txn, compacted topics | 5, 15 |
| §7 order | 1 → 10 → 11–12 → 13 |
| §8 matrix, stress, 3-node, e2e | 15, 16, 17, 18 |
| §9 review focus | Review Focus 1–10 |
| §10 CLI | 13 |

## Execution handoff

Run the plan with superpowers:subagent-driven-development:
- one implementer per task (mid-tier model) with the exact recipe;
- a task review after each task;
- checkpoints A–E as listed.

Implementers never spawn subagents; at most 3 agents run at once. Root-cause diagnosis of any red test stays with the
controller (CLAUDE.md rule 14).
