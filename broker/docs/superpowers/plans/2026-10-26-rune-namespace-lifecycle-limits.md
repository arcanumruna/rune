# Rune — Namespace Lifecycle and Limits (Phase 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A namespace can be suspended (ReadOnly or RejectAll), resumed and deleted, and every limit stored in Phase 1
is enforced. Suspend and delete take effect on every node at the apply of one catalog event, close or restrict the
affected sessions, and abort open transactions. Delete removes every object, offset, transaction state and security
record of the namespace, survives a crash or a leader change at any step, and frees the name at once. Groups and
transactions work in every namespace (D8 is lifted). Limits are refused with one precise code each: object counts,
durability and features at DDL; connections at login; rates as THROTTLED with a retry time; disk as an over-disk
state that refuses only leader produce; view memory as a paused view. The hot path pays one add and one compare,
proven by an ABAB bench.

**Architecture:**
- **Catalog.** Suspend and resume are `NamespaceStateChanged (34)`. Delete is one `NamespaceDeleted (35)` event, and
  that event is the commit point (D17). A new kind 36, `NamespaceDeleteStep`, records each cleanup step that is not
  itself a catalog drop (D18). `NsDeleting` holds the pending objects and a step mask; the entry is erased when both
  are complete.
- **Admin thread.** The AdminWorker validates the new NamespaceDdl kinds 3–5 and appends them. A `NamespaceReaper`
  rich-state machine on the `__catalog` leader's AdminWorker drives each DELETING namespace to removal through
  idempotent steps, and is rebuilt from CatalogState on boot and on leadership change (rule 16). Object-count,
  durability and feature limits are checked here, from CatalogState.
- **Control thread.** The gateway enforces the suspend table (one constexpr lookup per request), closes sessions on
  RejectAll and Deleted, refuses logins, and owns connection counts. The group coordinator and the transaction
  coordinator key their maps by `(nsId, id)`; the offset and txn-state stores gain an nsId key prefix. A per-namespace
  limit aggregator on the control agent reads the arcanum-metrics slots each tick and broadcasts `Throttle`,
  `OverDisk` and `ViewMemOver` (D25, D26, D28).
- **Workers.** Each worker owns per-namespace `throttleUntilNs[]`, `overDisk[]` and per-log byte counters, indexed
  by `nsSlot`, written only from its own ring messages. The produce path adds bytes and records to its slot and
  compares the throttle deadline: one add and one compare.
- **Storage.** A deleted namespace's `ns-<id>` directory is renamed to `ns-<id>.deleting` and removed; the boot
  reconcile purges leftovers. Retention is clamped to the namespace maximum in the LogCleanerAgent.
- **Clients.** The CLI gains `rune namespace delete|suspend|resume`, and `limits` prints usage next to each limit.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark, CMake/Ninja, real Aeron UDP for the
in-process BrokerNode tests, docker compose (3 nodes) on the shared Linux test VM for e2e.

**Spec:** `broker/docs/superpowers/specs/2026-10-24-rune-namespaces-and-truncate-design.md` (approved 2026-10-26).
This plan covers §4, the enforcement half of §5.1, §5.2, §5.3 (the broker side; arcanum-metrics itself is Phase 0),
§5.4, the Phase 2 rows of §8 and §9, and §10 (`delete|suspend|resume`, usage in `limits`). Phase 3 (TruncateTopic)
and Phase 0 (arcanum-metrics) each have their own plan. The Self-review maps each spec section to its task.

**Builds on / order:** the Phase 1 plan (`2026-10-26-rune-namespaces.md`) merged, and, from Task 8 on, the Phase 0
plan (arcanum-metrics) merged (D31). Tasks 1–7 need only Phase 1. What exists today (line numbers are hints; use the
quoted anchors):

- **Phase 1 (assumed merged):**
  - `codec/include/rune/catalog/CatalogEvent.h`: kinds 32–35, `NsStateTag { Active, SuspendedReadOnly,
    SuspendedRejectAll, Deleting }`, `Ops` with 13 alternatives, `kKindOfAlt` / `altOf`; `checkKind` refuses 36–63.
  - `codec/include/rune/catalog/NamespaceRegistry.h`: `NsState = std::variant<NsActive, NsSuspended{mode},
    NsDeleting{...}>`; `byName`, `byId`; `ApplyV2.cpp` defines every transition, but only `NsActive` is produced.
  - `protocol/include/rune/protocol/NamespaceLimits.h`: the 256-byte, 29-field record (28 u64 limits plus
    `featureBits`).
  - `protocol/include/rune/protocol/client/NamespaceMessages.h`: `NamespaceDdlKind { Create = 1, SetLimits = 2 }`,
    templates 81–83; ErrorCodes 51–54 gated on client ≥ 14 (InvalidRequest 39 otherwise).
  - `broker/libs/gateway/src/NsResolve.cpp`: `resolveName`, `hiddenAsMissing`; `Session.auth.nsId`.
  - `ClientGateway.cpp`: `denyGroupRequest_` (~:8721) and `denyTxnRequest_` (~:8779) refuse groups and txns outside
    `default` (Phase 1 D8).
  - `broker/libs/node/include/rune/node/NamespacePaths.h`: `topicsRoot(data, nsId)`; the reconcile walks `ns-*`.
  - Rigs: `NamespaceRig.h`, `NamespaceMatrixExpectations.h`, `AuthTrio.h`, `CatalogTrio.h`, `CatalogDdlRig.h`,
    `SecurityDdlRig.h`, `AuthBroker.h`, `AuthWireClient.h`; `broker/docker/namespace-e2e.sh`;
    `client-cpp/src/cli/NamespaceCommands.cpp`.
  - Leftovers this plan closes: the delete invariant TODO in `NamespaceStressTest.cpp`; namespace-e2e step 10 ("stored
    only; enforcement is Phase 2"); the CLAUDE.md §8 note that groups and txns are refused outside `default`.
- **Phase 0 (assumed merged from Task 8):** arcanum-metrics single-writer slots, totals read by an aggregator with an
  injected clock, `regId` per registration, a capacity fixed at start (spec §5.3).
- **Admin thread:**
  - `broker/libs/node/src/AdminWorker.cpp`: `startNext_` (~:732), `buildEvent_` (~:809), `fdBudgetRefusal_` (~:836),
    `appendEvent_` (~:872), `buildDropTopic_` (~:1388).
  - `AdminChannels.h`: `kMsgDropTopic = 10`, `kMsgPurgeDroppedDir = 16`, `kA2cCatalogTopicDropped = 6`,
    `kA2cViewUninstall = 8`. `CatalogApplier` effects (~:511).
- **Control thread:**
  - `ClientGateway.cpp`: the dispatch (~:1306-1405), `closeSession_` (~:1547), `handleProduce` (~:3529),
    `handleGroupMessage` (~:7748), `handleTxnMessage` (~:7915), `compilePerms_` (~:8521), `handleAuthClientFirst_`
    (~:8920), `onSecurityApplied` (~:9236), `revokeSession_` (~:9346; does not abort txns today),
    `handleMetadata` (~:10206).
  - `GroupCoordinationService` (`.h:79`, `coordinators_` ~:587): `join` (~:576), `onTopicDropped` (~:374),
    `removeSessionMembers` (~:1135), `offsetCommit` (~:1284), `offsetFetch` (~:1566).
  - `ReplicatedOffsetStore`: `encodeKey` (~:97), `commitAt` (~:127), `applyRecord` (~:233), `rebuildFromLog` (~:309).
  - `TransactionCoordinator` (`.h:275`, `txnMap_` ~:608): `endTransaction` (~:306), `doWork` (~:532).
    `ReplicatedTxnStateStore`: `putAt` (~:152), `applyRecord` (~:215). Both run on the control thread
    (`NodeRuntimeAgent` ~:642 / ~:648).
  - `NodeRuntimeAgent::doWorkImpl_` (~:288).
  - `BrokerNode.cpp`: role counters (~:515-560), `publishCatalogMetrics_` (~:3284), `serviceAdminControl_` (~:4866),
    `dropRuntimeTopic_` (~:5756), `serviceDroppedTopics_` (~:5852), `pumpCatalogOutboxes_` (~:5905),
    `onCatalogTopicConfig_` (~:5942), `onViewUninstall_` (~:5958). RoleCounter type ids `1'000'201..241`.
- **Workers:**
  - `Worker.h`: `enqueueOp` (~:1219), `executeFromWire_` (~:2541), `executeFetchRequestWire_` (~:4808),
    `ingress_` (~:5398), `sessionRing_` (~:5405), `markTopicDropped_` (~:6174), `produceCore_` (~:8349).
  - `WorkerViewSet.h`: `ViewReadiness` (~:92), `ViewInstance` (~:192), `install` (~:269), `uninstall` (~:279),
    `RowStatus` (~:343), `driveFolds` (~:418), `driveView_` (~:599). `ViewTable.h`: 32 MiB chunks (~:90),
    `chunkCount` (~:109). `ViewRowResponse` status: `DataMessages.h` (~:826).
  - `Log::totalBytes()` (`Log.cpp` ~:1938) is O(segments) and is never called per produce.
- **Storage:** `TopicDirReconcile.cpp`: `purgeDroppedEntries` (~:67), `quarantineOne` (~:111), `retireToDropped`
  (~:153), `reconcileTopicDirs` (~:206), `decideBootReconcile` (~:371), `stepBootReconcile_` (~:404).
  `LogCleaner::isTombstone` (`.h` ~:364). `LogCleanerAgent` overrides (~:184-220), `runRetention` (~:812).
- **CLI:** `client-cpp/src/cli/Cli.cpp`: dispatch (~:451-478), `printUsage` (~:88).
- **Benches:** `WorkerProduceLatencyBench`, `GetViewRowServeBench`, `ViewFoldBench` (`broker/bench/`).

## Execution context

- **Branch:** `claude/tender-wozniak-co3crp` in this repo; every path is repo-relative. Before each task, merge
  `main` in if it moved (`git fetch origin main && git merge origin/main`). Push after every task with
  `git push -u origin claude/tender-wozniak-co3crp`.
- **Builds:** see README.md#build and `docs/testing.md`.
  - `build` (Debug, `BUILD_TESTING=ON`, `BUILD_INTEGRATION_TESTING=ON`) for everything;
  - `build-asan` and `build-tsan` at the checkpoints;
  - `build-bench` (Release, `BUILD_BENCHMARKS=ON`) for Task 15's hot-path bench.
  - Re-run cmake only after a `CMakeLists.txt` edit.
- **Test gate (CLAUDE.md rule 10):**
  - **Per task:** build, then `ctest --test-dir build -R '<filter>' --output-on-failure -j8`, where the filter is the
    one named in the task's "Run" step.
  - **Checkpoints:** `cd broker && scripts/run-regression.sh ../build 8`, then the same suite under `build-asan` and
    `build-tsan` (`-R Regression -LE latency`). A failure is bisected over the commits since the last green
    checkpoint.
  - **Checkpoints:**
    - **A** after Task 5: wire, catalog lifecycle, suspend enforcement, txn abort and nsId keys. Groups and txns work
      in every namespace from here.
    - **B** after Task 7: delete end to end on one node (tombstones, reaper, disk).
    - **C** after Task 13: slots and every non-rate limit.
    - **D** after Task 16: rates, the bench and the CLI. Every limit is enforced from here.
    - **E** after Task 20: the matrix, the stress test, the 3-node slice, e2e and docs, before the final
      whole-branch review.
- **Docker e2e** (Task 20) runs on the shared Linux test VM from `$RUNE_VM_HOST`: hold the shared lock for the whole
  run, work only in your own directory, remove only containers and images you created, and check `df` before and
  after. Never on local Mac docker. The host's address and login are never written into the repo.
- **Commits** end with the session's `Co-Authored-By:` and `Claude-Session:` trailers.

## Global Constraints

**HARD RULES (CLAUDE.md §1, checked in every task review):**
1. **One owner per state.**
   - The namespace state (`NamespaceRegistry`, including `NsDeleting`) belongs to `CatalogState`: the admin thread's
     applier and the control thread's mirror copy, as in Phase 1.
   - The `NamespaceReaper` belongs to the leader's AdminWorker. It only reads CatalogState and appends events; it
     never mutates state directly.
   - Connection counts, session lists and the suspend gate belong to the gateway (control thread).
   - Group and txn maps belong to their coordinators (control thread).
   - Each worker owns its `throttleUntilNs[]`, `overDisk[]`, `nsSlotOf[]` and per-log byte counters. The control
     agent's aggregator owns the per-namespace totals and the limit view.
   - Cross-thread hand-off is only the existing Aeron rings (worker ingress, `sessionRing_`, the admin channels) plus
     the arcanum-metrics slots, which are single-writer by construction (Phase 0). No new shared memory.
2. **Hot path: one add and one compare.**
   - Produce adds the batch's bytes and records to the worker's own slot counters and compares `nowNs` with
     `throttleUntilNs[nsSlot]` and `overDisk[nsSlot]` (one cache line per slot). Fetch adds bytes. The gateway adds
     one request op.
   - There is no map lookup, no nsId check by name and no virtual call on the produce, fetch or query path. `nsSlot`
     is carried in the topic metadata and the session.
   - Task 15 proves it with an interleaved ABAB bench against the Phase 1 tip.
3. **Zero allocation in steady state.**
   - Per-slot arrays are sized `RUNE_MAX_NAMESPACES + 1` at start.
   - The reaper batches into a preallocated buffer (`RUNE_NS_DELETE_BATCH`).
   - Usage responses are encoded into the reused response buffer.
4. **Rich-state variants.**
   - `NsDeleting{pendingTopics, pendingViews, pendingSchemas, stepMask}`.
   - The reaper is `std::variant<Idle, AbortingTxns, DroppingViews, DroppingTopics, DroppingSchemas, Tombstoning,
     AwaitRemoval>`, each holding only its cursor.
   - A view's pause is `ViewPaused{reason = OverLimit, offset}` in `ViewReadiness`, not a flag.
   - The gateway's per-namespace gate is `NsGate = std::variant<Open, ReadOnly, RejectAll, Gone>`.
5. **Time-independent convergence (rule 16).** Delete never waits on a timer. Every step is idempotent and is
   re-derived from CatalogState after a crash or a leader change. Rates use the injected clock only.
6. **No silent fallback.**
   - A request refused by the suspend table, a limit or a throttle always gets its precise code (Error codes, below).
   - A ring message carrying a `(nsSlot, nsId)` pair that does not match the receiver's slot table is dropped and
     counted (`rune.ns.slot_mismatch`), never applied to another namespace (D22).
7. **Regression naming.** Every new test is `*Regression*`. The failing test is written first in every task.
8. **Fix on sight.** `revokeSession_` (~:9346) does not abort the session's open transactions. Task 4 fixes it for
   every revoke reason, not only suspend, with its own regression test.

**Error codes** (Phase 1 added them; Phase 2 is the first to emit them), each gated on `clientVersion >= 14`. An older
client gets `InvalidRequest` (39):
- `NamespaceSuspended = 51`: any request refused by the suspend table, and a login to a RejectAll namespace after a
  successful SCRAM exchange;
- `NamespaceLimitExceeded = 52`: object counts, durability, features, connections, sessions, open txns, txn time, and
  produce while over disk;
- `Throttled = 53`: a rate limit. The response carries `u64 retryAfterNs`;
- `ViewPaused = 54`: GetViewRow on a view paused over its namespace's memory limit.

A DELETING namespace answers exactly as a missing one (`hiddenAsMissing`). A login to it gets
`AuthenticationFailed` (44).

**Versions:** none change (D32). `kClientVersion` stays 14, `kBrokerVersion` 8, `kProtocolVersion` 3. The new
NamespaceDdl kinds and the usage templates are refused InvalidRequest by a Phase 1 broker, and the spec rules out
mixed-version clusters. Templates:
- `NamespaceUsage = 84`, `NamespaceUsageResponse = 85`;
- `Dispatch.h`'s gap comment becomes 86–95.

**Configuration:**
- `RUNE_NS_DELETE_BATCH` (default 256): tombstones and drops issued per reaper `doWork`.
- `RUNE_NS_LIMIT_TICK_US` (default 1000): the aggregator tick.
- `RUNE_ANONYMOUS_MAX_CONNECTIONS` (default 16) and `RUNE_ANONYMOUS_REQUEST_OPS_PER_SEC` (default 1000): the anonymous
  caps (#236).
- `RUNE_MAX_NAMESPACES` (Phase 1, default 1024) also sizes every per-slot array.

## Review Focus

1. **Delete is crash-safe at every step.** A crash or a `__catalog` leader change after each of the seven delete steps
   (spec §4) ends with the namespace removed, every object, offset, txn state and security record gone, the
   directory purged and the slot freed, with no step run twice to a different result. Tested by
   `NamespaceReaperRegression.CrashAtEveryStep*` (Task 7) and the 14 delete timing cells (Task 17).
2. **No data from a deleted namespace is visible after its name is reused.** A recreate of the same name gets a new
   nsId, and no topic, view, schema, offset, txn state, user, grant or metric of the old one is readable through it.
   Tested by `NamespaceDeleteRegression.RecreateSeesNothingOld` (Task 7) and the stress test's isolation invariant
   (Task 18).
3. **The suspend table is complete.** Every dispatched template has one row; RO refuses exactly produce, DDL, txns and
   offset commits; RA closes sessions; cluster-admin sessions are exempt. Tested by
   `NamespaceSuspendTableRegression.CoversEveryDispatchedTemplate` and the suspend table (Task 3).
4. **Open transactions are aborted, never left hanging,** on suspend (either mode), delete and every session revoke.
   Tested by `TxnAbortOnSuspendRegression.*` (Task 4) and the 3 txn timing cells (Task 17).
5. **nsId keys never collide across namespaces.** The same group, topic and transactional id in two namespaces keep
   independent offsets and txn state, through restart and log rebuild; v1 keys decode as `default`. Tested by
   `NsKeyedOffsetsRegression.*` and `NsKeyedTxnStateRegression.*` (Task 5).
6. **Tombstones really remove.** After delete, the offsets and txn-state logs compact to no key of the namespace, and
   a rebuild from the log restores none. Tested by `NamespaceTombstoneRegression.*` (Task 6).
7. **Slots never leak across namespaces.** A worker or gateway never applies a `(nsSlot, nsId)` message to a slot that
   now holds another namespace; a reused slot starts from zero usage. Tested by `NsSlotRegression.*` (Task 8) and the
   2 delete-recreate-under-Throttle timing cells (Task 17).
8. **Every limit field is enforced with the right code.** All 29 fields × below/at/above. Tested by
   `LimitEnforcementTableRegression.*` (Tasks 9–14).
9. **Lowering a limit never deletes data, except max retention.** Objects over a lowered count stay, a new create is
   refused; a lowered retention deletes old segments and answers Ok with a warning. Tested by
   `NamespaceLimitsLowerRegression.*` (Task 9) and `RetentionClampRegression.*` (Task 13).
10. **Follower apply is never refused.** Over disk, throttled or suspended, a follower still applies every replicated
    batch, so acks=all never stalls on a limit. Tested by `OverDiskRegression.FollowerApplyNeverRefused` (Task 11)
    and the 3-node slice (Task 19).
11. **The hot path is still one add and one compare.** Zero allocations, no new virtual call, p99 inside the budget.
    Tested by Task 15's ABAB bench and `NsHotPathAllocRegression.*`.
12. **Throttle is fair and converges.** A namespace over its rate is throttled for `excess / limit`, recovers without a
    timer constant, and a limit change between a tick and a Throttle is applied on the next tick. Tested by
    `NamespaceThrottleRegression.*` (Task 14) and the 5 rate timing cells (Task 17).
13. **Groups and transactions work in every namespace** (D8 lifted), with membership, offsets and fencing isolated
    per namespace. Tested by `NsGroupsRegression.*` and `NsTxnsRegression.*` (Task 5), and e2e (Task 20).

## Decisions

- **D17. Delete is one `NamespaceDeleted (35)` event, and that event is the commit point.**
  - This amends Phase 1 D5, which modelled delete as `StateChanged → Deleting` followed by `Deleted`. Phase 2 refuses
    `NamespaceStateChanged` with `to = Deleting` at the AdminWorker and in `ApplyV2` (InvalidRequest), so there is
    one path into DELETING.
  - Apply of `NamespaceDeleted` moves the entry to `NsDeleting` and fills its pending sets from the catalog's
    current topics, views and schemas in that nsId.
  - Catalog drop kinds (DropTopic, DropView, DropSchema) are accepted on a DELETING nsId; every other DDL in it is
    refused as Missing.
- **D18. Kind 36 `NamespaceDeleteStep(nsId, step)` records each non-catalog cleanup step.**
  - Step bits: `TxnsAborted = 1`, `GroupsTombstoned = 2`, `TxnStateTombstoned = 4`, `SecurityPurged = 8`.
  - `NsDeleting{pendingTopics, pendingViews, pendingSchemas, stepMask}`. A catalog drop in the nsId removes the object
    from its pending set. The entry is erased, and the name's byId slot released, when all three sets are empty and
    all four bits are set. The broker frees the metric slot on that apply (Task 8).
  - The `NamespaceReaper` is a rich-state machine on the `__catalog` leader's AdminWorker:
    `Idle → AbortingTxns → DroppingViews → DroppingTopics → DroppingSchemas → Tombstoning → AwaitRemoval → Idle`.
    Each state holds only its cursor. On boot and on leadership change it is rebuilt from CatalogState: the first
    state whose work is not yet recorded. Each step is idempotent: dropping a dropped object is a no-op, a tombstone
    of an absent key is harmless, and a step event already applied does not change the mask.
  - Gateways close every session of the namespace on apply of `NamespaceDeleted` and refuse its logins with
    AuthenticationFailed from then on.
  - On apply of the erase, each node renames `ns-<id>` to `ns-<id>.deleting` and removes it off-thread
    (`kMsgPurgeDroppedDir`). The boot reconcile purges any `ns-*.deleting` it finds.
  - A DELETING namespace counts toward `maxNamespaces` until it is erased. `byName` skips DELETING entries, so the
    name can be created again at once with a new nsId.
- **D19. The offsets and txn-state keys carry the nsId.**
  - The offsets key v2 is `u8 ver=2 | u32 nsId | group | topic | partition`; the txn-state key v2 is
    `u8 ver=2 | u32 nsId | transactionalId`. A v1 key (no version byte today: decided by the existing first-byte
    layout, see Task 5) decodes as nsId 0. New writes are always v2.
  - `GroupCoordinationService::coordinators_` and `TransactionCoordinator::txnMap_` are keyed by `(nsId, id)`.
  - This lifts Phase 1 D8: `denyGroupRequest_` and `denyTxnRequest_` no longer refuse outside `default`.
- **D20. Tombstones use the log's null-value marker.**
  - Each tombstone is a record with the key and the null value that `LogCleaner::isTombstone` recognises.
    `applyRecord` erases the key from the in-memory map, and compaction removes it from the log.
  - The reaper writes them in batches of `RUNE_NS_DELETE_BATCH` per `doWork`, through the control channel (the stores
    live on the control thread), and sets the step bit only after the last batch is acked.
  - Security records are removed by a new `SecurityEventKind = 11 NamespacePurged(nsId)`: on apply, every user,
    grant and credential with that nsId is erased, and every live session of those users is revoked.
- **D21. The suspend table is a constexpr array in `NamespaceSuspendTable.h`.**
  - One row per dispatched template: `{templateId, roAction, raAction}`, with `Allow` or `Refuse`. The gateway does
    one lookup per request, after the session's `NsGate` check.
  - **ReadOnly** refuses produce, every DDL (including truncate and user and grant changes), txn templates (45–54)
    and OffsetCommit. It allows Fetch, Metadata, view reads, QL reads, OffsetFetch and group JoinGroup, SyncGroup,
    Heartbeat and LeaveGroup. On entry, the namespace's open txns are aborted (Task 4).
  - **RejectAll** closes every session of the namespace on apply. A login whose SCRAM proof verifies gets
    NamespaceSuspended; a wrong password still gets AuthenticationFailed, so suspend leaks nothing to a guesser.
  - Cluster-admin sessions are exempt from both modes.
  - The control thread broadcasts `NsState(nsSlot, nsId, tag)` to the workers, which apply it to their own
    `nsGate[]` for the paths that bypass the gateway dispatch (none today; the table keeps the hook for Phase 3).
- **D22. Every namespace has a dense metric slot.**
  - The slot is allocated on apply of `NamespaceCreated`, on every node independently, lowest free first. Slot 0 is
    `default`. Because apply order is the same on every node, the slot numbers agree, but nothing relies on it.
  - `NsSlotAssign(nsSlot, nsId)` is broadcast on every worker ring and the admin channel before any object of the
    namespace can exist (the catalog apply that allocates it runs before the next event's apply).
  - Every later ring message carries `(nsSlot, nsId)`; a receiver whose table holds another nsId in that slot drops
    and counts it.
  - The slot is freed on apply of the erase (D18) and its counters zeroed on reassign. The arcanum-metrics `regId` is
    the nsId, so a scrape never merges two namespaces' series.
  - Sessions and topic metadata carry `nsSlot`, so the hot path indexes an array.
- **D23. Count, durability and feature limits are checked where the state lives.**
  - Object counts (topics, partitions per topic, total partitions, views, schemas, versions per schema) in the
    AdminWorker from CatalogState, before `appendEvent_`, refused NamespaceLimitExceeded.
  - `maxGroups` in the group coordinator at the first JoinGroup of a new group; `maxUsers` in SecurityAdmin at
    CreateUser.
  - Durability at topic DDL: `minReplicationFactor <= rf <= maxReplicationFactor`, `minInsync >= lowestMinInsync`.
  - `lowestAcks` per produce at the gateway: one compare of the request's acks against the session's cached
    namespace floor.
  - `featureBits` at DDL (a compacted topic needs Compaction; CreateView needs Views) and at connect or login (Ql,
    Sql, Redis, AnonymousRead, AuthScram, Txns at InitProducerId).
- **D24. Connections are counted per node, at the gateway.**
  - `maxConnections` counts sessions of the namespace on this node; `maxConnectionsPerUser` and `maxSessionsPerUser`
    use a per-namespace user map preallocated at slot assign.
  - `maxOpenTxns` and `maxTxnTimeMs` are enforced in the txn coordinator, at InitProducerId and AddPartitions.
  - Anonymous sessions have their own caps, `RUNE_ANONYMOUS_MAX_CONNECTIONS` (16) and
    `RUNE_ANONYMOUS_REQUEST_OPS_PER_SEC` (1000), applied before any namespace limit (#236).
- **D25. Disk usage is tracked incrementally.**
  - Each log keeps `bytesOnDisk_`, updated on append, segment roll, retention delete and compaction swap, on its
    owning worker. The worker publishes a per-slot sum into its metric slot. `Log::totalBytes()` is never called on
    the hot path.
  - The aggregator sums the per-slot values and, on a crossing, broadcasts `OverDisk(nsSlot, nsId, on)`.
  - While over, leader produce is refused NamespaceLimitExceeded. Follower apply, replication catch-up and the
    log cleaner are never refused.
- **D26. View memory is counted in chunks.**
  - A view's memory is `chunkCount × chunkBytes` (`ViewTable.h`), summed per slot on the worker.
  - On a crossing the aggregator sends `ViewMemOver(nsSlot, nsId, on)`; each worker moves the namespace's views to
    `ViewPaused{OverLimit, offset}` and stops folding. `RowStatus::Paused` answers `ViewPaused (54)`.
  - On `on = false` (limit raised or memory fell) the views resume folding from `offset`.
  - The overshoot is bounded by one tick of folds plus one chunk per view.
- **D27. Effective retention is `min(topic retention, namespace maxRetentionMs / maxRetentionBytes)`.**
  - Applied in the `LogCleanerAgent` overrides, so lowering the namespace maximum deletes old segments on the next
    retention pass. The SetLimits DdlResponse is Ok with the warning `retention clamped on N topics`.
  - A topic DDL that sets retention above the namespace maximum is refused NamespaceLimitExceeded.
- **D28. Rates use a per-namespace aggregator and a broadcast deadline.**
  - The aggregator runs on the control agent every `RUNE_NS_LIMIT_TICK_US`. For each rate it reads the slot totals,
    computes the excess over `limit × elapsed`, and if positive sets `untilNs = now + excess / limit`.
  - It broadcasts `Throttle(nsSlot, nsId, kind, untilNs)` to the threads that count that rate; `untilNs = 0` clears it.
    Each thread owns its own `throttleUntilNs[slot][kind]`.
  - Refused requests get Throttled (53) with `retryAfterNs = untilNs - now`.
  - Counting: requestOps at the gateway; produce bytes and records, fetch bytes on the worker; queryCycles on the
    view read path (the cycle counter already measured there).
  - The hot path pays one add and one compare (Task 15).
- **D29. CLI and usage.** `rune namespace delete|suspend [--read-only|--reject-all]|resume <name>`. `rune namespace
  limits <name>` prints each limit next to its usage, from `NamespaceUsage (84)` → `NamespaceUsageResponse (85)`
  (the aggregator's last totals plus the catalog counts).
- **D30. Phase 1 D8's template ranges are corrected.** Groups are 32–44 and txns 45–54 (verified in `Protocol.h`), not
  32–47 and 48–54. Task 5 removes the check, so only the plan text was wrong; the suspend table uses the verified
  ranges.
- **D31. Tasks 8 onward need Phase 0.** The slots and every rate and usage limit read arcanum-metrics. Tasks 1–7 do
  not.
- **D32. No version bump.** The new kinds and templates are refused InvalidRequest by a Phase 1 broker, and the spec
  rules out mixed-version clusters.
- **D33. NamespaceDdl kinds 3–5.** `Suspend = 3` carries `u8 mode` (1 ReadOnly, 2 RejectAll); `Resume = 4` and
  `Delete = 5` carry only the name. The `presentMask` and `limits` are encoded only for kinds 1 and 2. All three are
  cluster admin only (as D15).

## Interaction matrix

The single source of truth is `broker/libs/node/tests/support/NamespaceLifecycleMatrixExpectations.h`. It defines:

```cpp
enum class LcOp : uint8_t { SuspendRO, SuspendRA, SwitchMode, Resume, Delete, Recreate,
                            SetLimitsRaise, SetLimitsLowerBelowUsage, SetLimitsLowerRetention };   // 9
enum class LcCaller : uint8_t { ClusterAdmin, NsAdminSame, NsAdminForeign, UserInNs, UserForeign, Anonymous };  // 6
enum class LcTarget : uint8_t { Active, SuspendedRO, SuspendedRA, Deleting, Missing, Default };    // 6
enum class LcTiming : uint8_t { Sequential, BackToBack, CrashMidDelete, LeaderChangeMidDelete,
                                FollowerCatchUp, LimitChangeMidTick, RecreateUnderThrottle, TxnOpen };
struct LcOutcome { enum Kind : uint8_t { Ok, OkWithWarning, Missing, Refused } kind; ErrorCode code; LcTarget after; };
LcOutcome expect(LcOp, LcCaller, LcTarget);
bool naRule(LcOp, LcCaller, LcTarget);
std::span<const ErrorCode> expectedCodes();   // every code the matrix can emit
```

The 1-node rig (Task 17), the seeded stress test (Task 18) and the 3-node slice (Task 19) all read it.

- **Ops (9):** SuspendRO, SuspendRA, SwitchMode (RO ⇄ RA; RA from Active), Resume, Delete, Recreate (Create of the
  same name), SetLimitsRaise, SetLimitsLowerBelowUsage (maxTopics below the current count),
  SetLimitsLowerRetention (maxRetentionMs below the age of the oldest segment).
- **Callers (6):** ClusterAdmin, NsAdminSame (namespace admin of the target), NsAdminForeign (namespace admin of
  another namespace), UserInNs (a plain user of the target), UserForeign, Anonymous (AnonymousRead on for `default`).
- **Targets (6):** Active, SuspendedRO, SuspendedRA, Deleting (the reaper held at `DroppingTopics` by the rig),
  Missing (never created), Default.
- **Timings:** Sequential for every cell; the timing cells below on top.
- **Outcomes:** Ok, OkWithWarning, Missing (exactly the answer for an unknown name), Refused(code). `after` is the
  target state observed by a follow-up `ListNamespaces` from the cluster admin.

**Rules in `expect`, evaluated in this order:**
1. `naRule`: Recreate applies only to Deleting and Missing (24 n/a cells: 6 callers × 4 other targets).
2. Login-time: on SuspendedRA, NsAdminSame and UserInNs cannot log in: Refused(NamespaceSuspended). On Deleting,
   they get Refused(AuthenticationFailed). The op is never sent.
3. Every caller other than ClusterAdmin gets Refused(NotAuthorized), checked before any lookup, so a non-admin learns
   nothing about which names exist (D15, D33).
4. ClusterAdmin:
   - SuspendRO, SuspendRA: Ok on Active, either Suspended and Default (the same mode again is an Ok no-op, `after`
     unchanged); Missing on Deleting and Missing.
   - SwitchMode: Ok to the opposite mode (RA from Active); Missing on Deleting and Missing.
   - Resume: Ok on Active (no-op), either Suspended and Default; Missing on Deleting and Missing.
   - Delete: Ok on Active and either Suspended (`after` = Missing as seen by name); Missing on Deleting and Missing;
     Refused(InvalidRequest) on Default.
   - Recreate: Ok on Deleting and Missing, with a new nsId (`after` = Active).
   - SetLimitsRaise, SetLimitsLowerBelowUsage: Ok on Active, either Suspended and Default; Missing on Deleting and
     Missing. After LowerBelowUsage the objects remain and a follow-up CreateTopic gets
     Refused(NamespaceLimitExceeded).
   - SetLimitsLowerRetention: OkWithWarning on the same targets, and the oldest segment is gone after the next
     retention pass (the rig advances the cleaner clock); Missing on Deleting and Missing.

**Timing cells (36)**, each on ClusterAdmin with a seeded workload in the target namespace:
- **Back-to-back (9):** each op sent twice before the first reply; the second gets the answer of the state after
  the first (same-mode suspend Ok, second delete Missing, second recreate Refused(AlreadyExists)).
- **Crash mid-delete (7):** a crash and restart of the `__catalog` leader after each delete step (Deleted event,
  sessions fenced, txns aborted, objects dropped, offsets/txn/security tombstoned, directory renamed, slot freed).
  Removal completes after restart.
- **Leader change mid-delete (7):** the same seven points, with a `__catalog` leader change instead of a crash.
- **Follower catch-up (3):** a follower offline across a suspend, a delete and a recreate, then caught up; its gate,
  slot and directories match the leader's.
- **Limit change mid-tick (5):** one per rate (produceBytes, produceRecords, fetchBytes, requestOps, queryCycles): a
  SetLimits between the aggregator's read and its Throttle broadcast; the next tick applies the new limit.
- **Recreate under Throttle (2):** delete and recreate while a Throttle for the old nsId is in a ring; the new
  namespace is never throttled by it (one for a worker ring, one for the gateway).
- **Txn open (3):** SuspendRO, SuspendRA and Delete with an open txn; it is aborted, its records are never visible to
  read_committed, and the producer gets InvalidProducerEpoch or NamespaceSuspended on its next call.

**Pinned counts** (`NamespaceLifecycleMatrixCountRegression`): 324 cells, 24 n/a, 300 applicable, 36 timing,
336 executed.

**Two more tables**, each with its count pinned:
- **Suspend table** (`NamespaceSuspendTableRegression`, Task 3): every dispatched template × {SuspendedRO,
  SuspendedRA, Deleting} × {UserInNs, ClusterAdmin}, with the count pinned to `kDispatchedTemplates.size() × 6`.
- **Limit enforcement table** (`LimitEnforcementTableRegression`, Tasks 9–14): the 29 fields of `NamespaceLimits`
  (28 limits plus `featureBits`) × {below, at, above} = 87 cells. Each row names the operation that exercises the
  limit, the code at `above` and the observation at `at`.

## File Structure

New:
- `protocol/include/rune/protocol/client/NamespaceUsageMessages.h`: templates 84/85.
- `codec/include/rune/catalog/NamespaceDeleteStep.h`: kind 36 ops and step bits.
- `broker/libs/node/include/rune/node/NamespaceReaper.h`, `broker/libs/node/src/NamespaceReaper.cpp`.
- `broker/libs/gateway/include/rune/gateway/NamespaceSuspendTable.h`: the constexpr table.
- `broker/libs/gateway/include/rune/gateway/NsGate.h`: the per-namespace gate variant and connection counters.
- `broker/libs/node/include/rune/node/NsSlotTable.h`: slot allocation and the `(nsSlot, nsId)` check.
- `broker/libs/node/include/rune/node/NsLimitAggregator.h`, `broker/libs/node/src/NsLimitAggregator.cpp`.
- `broker/libs/node/include/rune/node/NsRingMessages.h`: `NsSlotAssign`, `NsState`, `Throttle`, `OverDisk`,
  `ViewMemOver`.
- Tests: `NamespaceLifecycleApplyRegressionTest.cpp`, `NamespaceSuspendTableRegressionTest.cpp`,
  `TxnAbortOnSuspendRegressionTest.cpp`, `NsKeyedStoresRegressionTest.cpp`, `NamespaceTombstoneRegressionTest.cpp`,
  `NamespaceReaperRegressionTest.cpp`, `NsSlotRegressionTest.cpp`, `LimitEnforcementTableRegressionTest.cpp`,
  `NamespaceConnectionLimitRegressionTest.cpp`, `OverDiskRegressionTest.cpp`, `ViewMemoryLimitRegressionTest.cpp`,
  `RetentionClampRegressionTest.cpp`, `NamespaceThrottleRegressionTest.cpp`, `NamespaceLifecycleMatrixTest.cpp`,
  `NamespaceLifecycleStressTest.cpp`, `NamespaceLifecycleTrioTest.cpp`.
- Support: `broker/libs/node/tests/support/NamespaceLifecycleMatrixExpectations.h`,
  `broker/libs/node/tests/support/LimitEnforcementTable.h`.
- Bench: `broker/bench/NamespaceHotPathBench.cpp`.

Modified (listed per task below).

---

## Tasks

### Task 1: protocol — NamespaceDdl kinds 3–5, usage templates 84/85

Spec §4, §10; D29, D32, D33.

**Files:**
- Modify:
  - `protocol/include/rune/protocol/client/NamespaceMessages.h`: `NamespaceDdlKind { Create = 1, SetLimits = 2,
    Suspend = 3, Resume = 4, Delete = 5 }`, `NsSuspendMode { ReadOnly = 1, RejectAll = 2 }`; encode and decode of
    the mode byte for kind 3, no `presentMask`/limits for kinds 3–5;
  - `protocol/include/rune/protocol/client/Protocol.h`: templates `NamespaceUsage = 84`,
    `NamespaceUsageResponse = 85`; `ThrottledResponse` trailer `u64 retryAfterNs`;
  - `protocol/include/rune/protocol/client/Dispatch.h`: route 84/85, gap comment 86–95.
- Create: `protocol/include/rune/protocol/client/NamespaceUsageMessages.h`.
- Test: `protocol/tests/NamespaceMessagesTest.cpp` (extend).

**Interfaces:**
```cpp
namespace rune::protocol::client {
enum class NsSuspendMode : uint8_t { ReadOnly = 1, RejectAll = 2 };
struct NamespaceUsageRequest { std::string_view name; };                       // template 84
struct NamespaceUsageResponse {                                                // template 85
    ErrorCode code;
    NamespaceLimits limits;                                                    // the stored record
    uint64_t usage[kNamespaceLimitFieldCount];                                 // same order as the limits; 0 = n/a
    uint8_t  state;                                                            // NsStateTag
};
size_t encodeNamespaceUsageResponse(const NamespaceUsageResponse&, uint8_t* out, size_t cap); // 0 if cap short
bool   decodeNamespaceUsageResponse(const uint8_t* in, size_t len, NamespaceUsageResponse&);
}
```

- [ ] **Step 1: failing tests** in `NamespaceMessagesTest.cpp`:
  - `NamespaceDdlRegression.SuspendCarriesModeByteRegression`: kind 3 with mode 1 and 2 round-trips; mode 0 and 3
    decode false.
  - `NamespaceDdlRegression.ResumeAndDeleteCarryNameOnlyRegression`: the encoded size of kinds 4 and 5 is the header
    plus the name; a trailing limits block is refused (decode false).
  - `NamespaceDdlRegression.TruncatedAtEveryOffsetRegression`: every prefix of each kind 1–5 frame decodes false.
  - `NamespaceUsageRegression.RoundTripRegression` and `…ShortCapReturnsZeroRegression`.
  - `ThrottledRegression.RetryAfterRoundTripRegression`.
  - `DispatchRegression.Templates84And85RoutedRegression`.
  - Mutation: drop the mode byte from kind 3's encoder → `SuspendCarriesModeByteRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceDdlRegression|NamespaceUsageRegression|ThrottledRegression|DispatchRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `protocol: namespace suspend/resume/delete kinds and usage templates` and push.

### Task 2: catalog and admin — suspend, resume, delete apply; kind 36

Spec §4; D17, D18, D33.

**Files:**
- Modify:
  - `codec/include/rune/catalog/CatalogEvent.h`: kind 36 `NamespaceDeleteStep`, `Ops` gains it (14 alternatives),
    `checkKind` refuses 37–63;
  - `codec/include/rune/catalog/NamespaceRegistry.h`, `codec/src/catalog/ApplyV2.cpp`: `NsDeleting` fields, the
    DELETING transition on `NamespaceDeleted`, step bits, erase on completion, `byName` skips DELETING;
    `StateChanged → Deleting` refused;
  - `broker/libs/node/src/AdminWorker.cpp`: `startNext_` and `buildEvent_` for kinds 3–5 (cluster admin only,
    D15); delete of `default` refused InvalidRequest; drop kinds accepted on a DELETING nsId;
  - `broker/libs/node/src/CatalogApplier.cpp`: effects for `NsStateChanged`, `NamespaceDeleted`, the erase.
- Create: `codec/include/rune/catalog/NamespaceDeleteStep.h`.
- Test: `codec/tests/catalog/NamespaceLifecycleApplyRegressionTest.cpp`, `broker/libs/node/tests/NamespaceAdminTest.cpp`
  (extend), the kind-table golden under `codec/tests/catalog/golden/v2/`.

**Interfaces:**
```cpp
namespace rune::catalog {
enum NsDeleteStepBit : uint8_t { TxnsAborted = 1, GroupsTombstoned = 2, TxnStateTombstoned = 4, SecurityPurged = 8 };
inline constexpr uint8_t kNsDeleteAllSteps = 0x0F;
struct NamespaceDeleteStep { uint32_t nsId; uint8_t step; };                   // kind 36, exactly one bit
struct NsDeleting {
    FixedIdSet pendingTopics, pendingViews, pendingSchemas;                    // ids, capacity from the ns limits
    uint8_t stepMask;
    bool complete() const { return pendingTopics.empty() && pendingViews.empty() && pendingSchemas.empty()
                                   && stepMask == kNsDeleteAllSteps; }
};
// Apply effects reported to the broker (CatalogApplier):
struct NsGateChanged { uint32_t nsId; NsStateTag tag; };
struct NsErased     { uint32_t nsId; };
}
```

- [ ] **Step 1: failing tests** in `NamespaceLifecycleApplyRegressionTest.cpp` (pure apply, table-driven over every
  `NsState` × every namespace kind 32–36):
  - `NamespaceLifecycleApplyRegression.TransitionTableRegression`: every (state, event) pair gives the expected next
    state or the expected refusal; the table's size is pinned.
  - `NamespaceLifecycleApplyRegression.StateChangedToDeletingRefusedRegression`.
  - `NamespaceLifecycleApplyRegression.DeletedFillsPendingSetsRegression`: 3 topics, 2 views, 1 schema → the sets hold
    exactly those ids.
  - `NamespaceLifecycleApplyRegression.DropInDeletingClearsPendingRegression`.
  - `NamespaceLifecycleApplyRegression.StepIdempotentRegression`: the same step applied twice leaves the mask and
    the event count's effect unchanged.
  - `NamespaceLifecycleApplyRegression.EraseOnlyWhenCompleteRegression`: every one of the 2^4 masks × empty/non-empty
    sets; erase only on the complete one.
  - `NamespaceLifecycleApplyRegression.NameReusableWhileDeletingRegression`: Create of the same name during DELETING
    gets a new nsId; `byId(old)` is still DELETING.
  - `NamespaceLifecycleApplyRegression.DeletingCountsTowardMaxNamespacesRegression`.
  - `NamespaceLifecycleApplyRegression.OtherDdlInDeletingIsMissingRegression`: CreateTopic, CreateView, SetLimits.
  - In `NamespaceAdminTest.cpp`: `NamespaceAdminRegression.Kinds3To5ClusterAdminOnlyRegression` (every caller of the
    matrix), `…DeleteDefaultRefusedRegression`, `…SuspendSameModeIsNoOpRegression`.
  - Mutation: let `byName` return DELETING entries → `NameReusableWhileDeletingRegression` fails.
- [ ] **Step 2:** implement. Regenerate the kind-table golden with the repo's golden tool, never by hand. Run
  `ctest --test-dir build -R 'NamespaceLifecycleApplyRegression|NamespaceAdminRegression|CatalogGolden' --output-on-failure -j8`.
- [ ] **Step 3:** commit `catalog: namespace suspend, resume and delete apply; delete step kind 36` and push.

### Task 3: gateway — the suspend table and its enforcement

Spec §4; D21, D30.

**Files:**
- Create:
  - `broker/libs/gateway/include/rune/gateway/NamespaceSuspendTable.h`: the constexpr table and `kDispatchedTemplates`;
  - `broker/libs/gateway/include/rune/gateway/NsGate.h`: `NsGate` and the per-namespace array.
- Modify: `broker/libs/gateway/src/ClientGateway.cpp`:
  - the dispatch (~:1306-1405): one `NsGate` check and one table lookup per request, before the handler;
  - `onCatalogApplied`: `NsGateChanged` → update the gate; RejectAll and Deleted → `closeSession_` for every session
    of the nsId except cluster admins;
  - `handleAuthClientFirst_` (~:8920) and the final SCRAM step: a verified proof to a RejectAll namespace gets
    NamespaceSuspended; DELETING gets AuthenticationFailed.
- Test: `broker/libs/gateway/tests/NamespaceSuspendTableRegressionTest.cpp`,
  `broker/libs/node/tests/NamespaceSuspendRegressionTest.cpp` (real broker, `NamespaceRig.h`).

**Interfaces:**
```cpp
namespace rune::gateway {
enum class SuspendAction : uint8_t { Allow, Refuse };
struct SuspendRow { uint16_t templateId; SuspendAction ro; SuspendAction ra; };
inline constexpr std::array<uint16_t, N> kDispatchedTemplates = { /* every template the dispatch routes */ };
inline constexpr std::array<SuspendRow, N> kSuspendTable = { /* one row per kDispatchedTemplates entry, same order */ };
constexpr SuspendAction suspendAction(uint16_t templateId, NsStateTag);         // O(1): indexed by templateId
struct NsOpen {}; struct NsReadOnly {}; struct NsRejectAll {}; struct NsGone {};
using NsGate = std::variant<NsOpen, NsReadOnly, NsRejectAll, NsGone>;
}
```

- [ ] **Step 1: failing tests**:
  - `NamespaceSuspendTableRegression.CoversEveryDispatchedTemplate`: a `static_assert` and a runtime check that every
    template in `Dispatch.h`'s routing has exactly one row, and the row order matches.
  - `NamespaceSuspendTableRegression.TableRegression` (TEST_P): every dispatched template × {SuspendedRO,
    SuspendedRA, Deleting} × {UserInNs, ClusterAdmin}, count pinned to `kDispatchedTemplates.size() × 6`. Expected:
    the D21 lists for RO; for RA and Deleting the session is gone, so the cell asserts the close; ClusterAdmin always
    Allow.
  - `NamespaceSuspendRegression.ReadOnlyRefusesProduceAllowsFetchRegression` (real client, real broker).
  - `NamespaceSuspendRegression.RejectAllClosesSessionsRegression`: two sessions in `a`, one in `b`; suspend `a`
    RA; both `a` sessions closed, `b` still serves.
  - `NamespaceSuspendRegression.RejectAllLoginRightProofSuspendedRegression` and `…WrongProofAuthFailedRegression`.
  - `NamespaceSuspendRegression.ResumeRestoresServiceRegression`.
  - `NamespaceSuspendRegression.OldClientGetsInvalidRequestRegression`: client version 13 → 39.
  - Mutation: mark OffsetCommit Allow under RO → `TableRegression` fails on that row.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceSuspendTableRegression|NamespaceSuspendRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `gateway: namespace suspend table, session close and login refusal` and push.

### Task 4: txn — abort open transactions on suspend, delete and every revoke

Spec §4; D21; Global Constraint 8.

**Files:**
- Modify:
  - `broker/libs/transaction/include/rune/transaction/TransactionCoordinator.h`,
    `broker/libs/transaction/src/TransactionCoordinator.cpp`: `abortAllForSession(sessionId)` and
    `abortAllForNamespace(nsId)`, both through the existing abort path of `endTransaction` (~:306), so markers are
    written and the epoch is bumped;
  - `broker/libs/gateway/src/ClientGateway.cpp`: `revokeSession_` (~:9346) calls `abortAllForSession` for every
    reason (password change, user drop, grant revoke, suspend, delete); the gate change to ReadOnly calls
    `abortAllForNamespace`.
- Test: `broker/libs/node/tests/TxnAbortOnSuspendRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace rune::transaction {
class TransactionCoordinator {
public:
    // Abort through the normal end path (markers, epoch bump). Returns the number aborted. Never allocates:
    // iterates txnMap_ in place.
    uint32_t abortAllForSession(uint64_t sessionId, int64_t nowNs);
    uint32_t abortAllForNamespace(uint32_t nsId, int64_t nowNs);
};
}
```

- [ ] **Step 1: failing tests** in `TxnAbortOnSuspendRegressionTest.cpp` (real broker, real client, `read_committed`
  consumer):
  - `TxnAbortOnSuspendRegression.SuspendReadOnlyAbortsRegression`, `…SuspendRejectAllAbortsRegression`,
    `…DeleteAbortsRegression`: an open txn with 10 records; after the op the consumer sees none of them, the
    abort marker is in the log, and the producer's next call gets NamespaceSuspended or InvalidProducerEpoch.
  - `TxnAbortOnSuspendRegression.RevokeReason{PasswordChange,UserDropped,GrantRevoked}AbortsRegression`: the same
    for each existing revoke reason (the fix on sight).
  - `TxnAbortOnSuspendRegression.OtherNamespaceUntouchedRegression`: a txn in `b` commits normally.
  - Mutation: skip the call in `revokeSession_` → the three revoke-reason tests fail.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'TxnAbortOnSuspendRegression|Transaction' --output-on-failure -j8`.
- [ ] **Step 3:** commit `txn: abort open transactions on namespace suspend, delete and every session revoke` and
  push.

### Task 5: groups and txns — nsId keys; lift D8

Spec §3, §4; D19.

**Files:**
- Modify:
  - `broker/libs/coordination/src/ReplicatedOffsetStore.cpp`: `encodeKey` (~:97) writes v2; the decoder reads v1 as
    nsId 0. First, read the current key layout and record in the commit how a v1 key is told apart (its first byte
    is a length prefix today; v2's first byte is `0xF2`, which no v1 length can start with because group names are
    capped below it; verify the cap and pick another marker if it is not true);
  - `broker/libs/transaction/src/ReplicatedTxnStateStore.cpp`: the same for the txn key;
  - `GroupCoordinationService.h/.cpp`: `coordinators_` keyed by `NsKey{nsId, groupId}`; `join`, `offsetCommit`,
    `offsetFetch`, `removeSessionMembers` and `onTopicDropped` take the nsId;
  - `TransactionCoordinator.h/.cpp`: `txnMap_` keyed by `(nsId, transactionalId)`;
  - `ClientGateway.cpp`: pass `session.auth.nsId`; delete `denyGroupRequest_` and `denyTxnRequest_` and their
    callers.
- Test: `broker/libs/node/tests/NsKeyedStoresRegressionTest.cpp`; update the Phase 1 tests that asserted the D8
  refusal.

**Interfaces:**
```cpp
namespace rune::coordination {
struct NsKey { uint32_t nsId; std::string_view id; };       // hashed with the existing open-addressing hash
inline constexpr uint8_t kNsKeyV2 = 0xF2;
size_t encodeOffsetKeyV2(uint32_t nsId, std::string_view group, std::string_view topic, int32_t partition,
                         uint8_t* out, size_t cap);
bool   decodeOffsetKey(const uint8_t* in, size_t len, uint32_t& nsId, std::string_view& group,
                       std::string_view& topic, int32_t& partition);   // v1 → nsId 0
}
```

- [ ] **Step 1: failing tests** in `NsKeyedStoresRegressionTest.cpp`:
  - `NsKeyedOffsetsRegression.SameGroupTwoNamespacesIndependentRegression`: group `g`, topic `t` in `a` and `b`;
    commits 5 and 9; each fetch returns its own.
  - `NsKeyedOffsetsRegression.SurvivesRestartAndRebuildRegression`.
  - `NsKeyedOffsetsRegression.V1KeyDecodesAsDefaultRegression`: a log written with the v1 encoder rebuilds into
    `default`.
  - `NsKeyedOffsetsRegression.KeyTruncatedAtEveryOffsetRegression`.
  - `NsKeyedTxnStateRegression.SameTransactionalIdTwoNamespacesRegression`: fencing in `a` does not fence `b`.
  - `NsKeyedTxnStateRegression.SurvivesRestartAndRebuildRegression`, `…V1KeyDecodesAsDefaultRegression`.
  - `NsGroupsRegression.JoinSyncCommitFetchInNamedNamespaceRegression` (real client against a real broker).
  - `NsTxnsRegression.CommitAndAbortInNamedNamespaceRegression`.
  - Mutation: drop the nsId from `NsKey`'s hash and equality → `SameGroupTwoNamespacesIndependentRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NsKeyed|NsGroupsRegression|NsTxnsRegression|GroupCoordination|Transaction|Offset' --output-on-failure -j8`.
- [ ] **Step 3:** update the Phase 1 tests that expected groups and txns refused outside `default` to expect them to
  work; record each changed assertion in the commit.
- [ ] **Step 4:** commit `coordination: nsId-keyed offsets and txn state; groups and txns in every namespace` and
  push.

**Checkpoint A.**

### Task 6: tombstones — groups, txn state and security for a deleted namespace

Spec §4; D18, D20.

**Files:**
- Modify:
  - `broker/libs/coordination/src/ReplicatedOffsetStore.cpp`: `tombstoneNamespace(nsId)` writes a null-value record
    for every live key of the nsId, in batches of `RUNE_NS_DELETE_BATCH`; rebuild treats a null value as erase;
  - `broker/libs/transaction/src/ReplicatedTxnStateStore.cpp`: the same;
  - `codec/src/security/ApplyV2.cpp`: SecurityEventKind 11 `NamespacePurged{nsId}` erases every user, role and grant
    of the nsId; idempotent;
  - `broker/libs/node/src/SecurityAdmin.cpp`: emits kind 11.
- Test: `broker/libs/node/tests/NamespaceTombstoneRegressionTest.cpp`, `codec/tests/security/NamespacePurgedApplyTest.cpp`.

**Interfaces:**
```cpp
namespace rune::coordination {
// Writes at most `batch` tombstones per call; returns true once no live key of nsId remains.
bool tombstoneNamespace(uint32_t nsId, uint32_t batch, int64_t nowNs);
}
namespace rune::security { struct NamespacePurged { uint32_t nsId; }; }   // SecurityEventKind 11
```

- [ ] **Step 1: failing tests**:
  - `NamespaceTombstoneRegression.OffsetsGoneAfterRebuildRegression`: 600 group keys in `a` (three batches), 10 in
    `b`; tombstone `a`; restart; `a` has none, `b` all 10.
  - `NamespaceTombstoneRegression.TxnStateGoneAfterRebuildRegression`.
  - `NamespaceTombstoneRegression.CompactionDropsTombstonesRegression`: after the cleaner runs past the delete
    retention, the log holds no key of `a`.
  - `NamespaceTombstoneRegression.BatchBoundsRegression`: 0, 1, 255, 256, 257 keys; the call count is
    `ceil(n / 256)` plus the final true.
  - `NamespaceTombstoneRegression.FollowerConvergesRegression` (3-node `CatalogTrio.h`): followers' rebuilt stores
    match the leader's.
  - `NamespacePurgedApplyRegression.ErasesUsersRolesGrantsRegression`, `…IdempotentRegression`,
    `…OtherNamespaceUntouchedRegression`, `…TruncatedAtEveryOffsetRegression`.
  - Mutation: rebuild ignores null values → `OffsetsGoneAfterRebuildRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceTombstoneRegression|NamespacePurgedApplyRegression|Offset|Security' --output-on-failure -j8`.
- [ ] **Step 3:** commit `coordination, security: tombstones and purge for a deleted namespace` and push.

### Task 7: the reaper — drive delete to completion, crash-safe

Spec §4; D17, D18; rule 16.

**Files:**
- Create: `broker/libs/node/include/rune/node/NamespaceReaper.h`, `broker/libs/node/src/NamespaceReaper.cpp`.
- Modify:
  - `broker/libs/node/src/AdminWorker.cpp`: owns one `NamespaceReaper`; `doWork` steps it on the catalog leader only;
  - `broker/libs/node/src/CatalogApplier.cpp`: `NamespaceDeleted` renames `ns-<id>` to `ns-<id>.deleting`;
    `NsErased` posts `kMsgPurgeDroppedDir`;
  - `broker/libs/node/src/TopicDirReconcile.cpp`: at boot, a `.deleting` dir whose nsId is erased is removed; one
    whose nsId is DELETING is kept; one whose nsId is live is a fatal mismatch, logged and counted.
- Test: `broker/libs/node/tests/NamespaceReaperRegressionTest.cpp`, `broker/libs/node/tests/NamespaceDeleteRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace rune::node {
struct ReapIdle {};
struct ReapAbortingTxns  { uint32_t nsId; };
struct ReapDroppingViews { uint32_t nsId; uint64_t inFlightCorrelation; };   // 0 = none in flight
struct ReapDroppingTopics{ uint32_t nsId; uint64_t inFlightCorrelation; };
struct ReapDroppingSchemas{ uint32_t nsId; uint64_t inFlightCorrelation; };
struct ReapTombstoning   { uint32_t nsId; uint8_t stepInFlight; };
struct ReapAwaitRemoval  { uint32_t nsId; };
using ReapState = std::variant<ReapIdle, ReapAbortingTxns, ReapDroppingViews, ReapDroppingTopics,
                               ReapDroppingSchemas, ReapTombstoning, ReapAwaitRemoval>;

class NamespaceReaper {
public:
    // Derives its state from the registry every time it is Idle: the lowest DELETING nsId, then the first
    // unfinished step. No state is persisted; restart and leader change resume from the registry (rule 16).
    int doWork(int64_t nowNs, const catalog::NamespaceRegistry&, AdminChannels&);
    void onDdlResult(uint64_t correlation, protocol::ErrorCode);
    void onLeadershipLost();                     // → ReapIdle; in-flight results are ignored
};
}
```

- [ ] **Step 1: failing tests** in `NamespaceReaperRegressionTest.cpp` (deterministic: injected clock, `doWork` to
  the next deadline, iteration cap 10 000, fail on cap):
  - `NamespaceReaperRegression.DrivesEveryStepInOrderRegression`: 3 topics, 2 views, 1 schema, 2 groups, 1 txn,
    1 user; the event sequence on `__catalog` and `__security` is exactly the expected one.
  - `NamespaceReaperRegression.CrashAtEveryStep*`: for each of the 7 durable steps, crash the node after it applies,
    restart, run to completion; the end state equals the no-crash run and no event is applied twice with effect.
  - `NamespaceReaperRegression.LeaderChangeAtEveryStepRegression` (`CatalogTrio.h`): move leadership after each
    step; the new leader finishes.
  - `NamespaceReaperRegression.InFlightResultAfterLeadershipLossIgnoredRegression`.
  - `NamespaceReaperRegression.DropRefusedIsRetriedNotSkippedRegression`: an injected NotLeader on a drop; the next
    tick retries it.
  - `NamespaceReaperRegression.TwoDeletingNamespacesSerialRegression`.
  - `NamespaceReaperRegression.EmptyNamespaceErasesRegression`.
  - In `NamespaceDeleteRegressionTest.cpp` (real broker, real client):
    `NamespaceDeleteRegression.RecreateSeesNothingOld`: delete `a` with data in every kind, recreate `a`; topics,
    views, schemas, groups, offsets, txns, users and grants are all empty; files under the old `ns-<id>` are gone.
  - `NamespaceDeleteRegression.DeletingDirRemovedAtBootRegression` and `…LiveNsDeletingDirIsFatalRegression`.
  - `NamespaceDeleteRegression.LoginDuringDeletingAuthFailedRegression`.
  - Mutation: persist nothing but skip re-deriving on restart (start Idle forever) → every `CrashAtEveryStep*`
    case fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceReaperRegression|NamespaceDeleteRegression|TopicDirReconcile' --output-on-failure -j8`.
- [ ] **Step 3:** commit `node: namespace reaper drives delete to completion across crash and leader change` and
  push.

**Checkpoint B.**

### Task 8: slots — per-node namespace slots and the broadcast

Spec §5.3, §5.4; D22. Needs Phase 0 (D31).

**Files:**
- Create:
  - `broker/libs/node/include/rune/node/NsSlotTable.h`: `NsSlotTable` owned by `BrokerNode`; slot 0 = `default`;
    capacity `RUNE_MAX_NAMESPACES`;
  - `broker/libs/node/include/rune/node/NsRingMessages.h`: `NsSlotAssign{slot, nsId, limits}`,
    `NsSlotRelease{slot, nsId}`, `NsState{slot, nsId, tag}`, `Throttle{slot, nsId, kind, untilNs}`.
- Modify:
  - `BrokerNode.cpp`: assign on Create apply, release on erase; broadcast on the existing broadcast ring;
  - `Worker.h`, `WorkerViewSet.cpp`, `ClientGateway.cpp`: each holds its own fixed array indexed by slot and checks
    `nsId` matches on every use; a mismatch counts `rune.ns.slot_mismatch` and refuses with InvalidRequest;
  - every arcanum-metrics registration for a namespace uses `regId = nsId`.
- Test: `broker/libs/node/tests/NsSlotRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace rune::node {
struct NsSlotAssign  { uint16_t slot; uint32_t nsId; protocol::NamespaceLimits limits; };
struct NsSlotRelease { uint16_t slot; uint32_t nsId; };
class NsSlotTable {
public:
    std::optional<uint16_t> assign(uint32_t nsId);   // lowest free slot; nullopt at capacity
    void release(uint16_t slot, uint32_t nsId);      // asserts the pair
    uint16_t slotOf(uint32_t nsId) const;            // kNoSlot if none
};
}
```

- [ ] **Step 1: failing tests**:
  - `NsSlotRegression.AssignReleaseReuseRegression`: 0, 1, max−1, max, max+1 namespaces; at max+1 Create is refused
    NamespaceLimitExceeded before any event is written.
  - `NsSlotRegression.StaleSlotMismatchCountedRegression`: release slot 3 for nsId 7, assign it to nsId 9, then
    deliver a stale message tagged (3, 7); the metric is 1 and the request is refused.
  - `NsSlotRegression.EveryWorkerSeesAssignBeforeFirstRequestRegression`.
  - `NsSlotRegression.RestartRebuildsSameSlotsRegression`.
  - `NsSlotRegression.MetricsRegIdIsNsIdRegression`.
  - Mutation: skip the nsId check on use → `StaleSlotMismatchCountedRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NsSlotRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `node: per-node namespace slots broadcast to every worker` and push.

### Task 9: limits — counts, durability and features

Spec §5.1, §5.2, §5.4; D23.

**Files:**
- Modify:
  - `broker/libs/node/src/AdminWorker.cpp`: before `buildEvent_`, check the D23 count limits (topics, partitions,
    views, schemas, users, roles, groups) against the registry; durability limits (min/max replication factor,
    `acks` floor) on CreateTopic and on produce; feature flags (views, txns, QL) on the first op of each kind;
  - `ClientGateway.cpp`: the `acks` floor per produce (lowestAcks), the txn and QL feature flags;
  - SetLimits below current usage is accepted; new creates are refused, existing objects are kept (spec §5.1).
- Create: `broker/libs/node/tests/support/LimitEnforcementTable.h` (87 cells: limit × op × {under, at, over}).
- Test: `broker/libs/node/tests/LimitEnforcementRegressionTest.cpp`, `broker/libs/node/tests/NamespaceLimitsLowerRegressionTest.cpp`.

- [ ] **Step 1: failing tests**:
  - `LimitEnforcementTableRegression.CountsRegression`, `…DurabilityRegression`, `…FeaturesRegression` (TEST_P over
    the table rows this task owns): `under` and `at−1 → at` succeed, `at → over` gets NamespaceLimitExceeded with
    no event written.
  - `NamespaceLimitsLowerRegression.ExistingKeptNewRefusedRegression`: 10 topics, limit lowered to 5; all 10 still
    serve; create refused; drop 6 then create succeeds.
  - `NamespaceLimitsLowerRegression.RaceWithCreateRegression`: SetLimits and CreateTopic back to back before either
    reply; the outcome matches the order on `__catalog`.
  - `LimitEnforcementTableRegression.AcksFloorPerProduceRegression`: acks=1 under floor all → refused;
    acks=all → served.
  - Mutation: check `>` instead of `>=` for topic count → the `at → over` cell fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LimitEnforcementTableRegression|NamespaceLimitsLowerRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `node: namespace count, durability and feature limits` and push.

### Task 10: limits — connections, open txns, txn time; the anonymous caps

Spec §5.2, §5.4, §11; D24; #236.

**Files:**
- Modify:
  - `ClientGateway.cpp`: a per-slot connection count, per node; checked at login after the proof verifies; released
    on close (every close path, one helper); anonymous sessions counted against `RUNE_ANONYMOUS_MAX_CONNECTIONS` and
    a token bucket of `RUNE_ANONYMOUS_REQUEST_OPS_PER_SEC`;
  - `TransactionCoordinator.cpp`: open-txn count per nsId and the max txn time per nsId (the existing timeout
    sweep uses the namespace's value).
- Test: `broker/libs/node/tests/NsConnectionLimitRegressionTest.cpp`; rows added to `LimitEnforcementTable.h`.

- [ ] **Step 1: failing tests**:
  - `LimitEnforcementTableRegression.ConnectionsRegression`, `…OpenTxnsRegression`, `…TxnTimeRegression`.
  - `NsConnectionLimitRegression.ReleasedOnEveryClosePathRegression`: client close, idle timeout, revoke, suspend
    RA, gateway shutdown; after each the count is back to 0.
  - `NsConnectionLimitRegression.PerNodeNotClusterRegression` (`AuthTrio.h`): limit 2; 2 per node succeed.
  - `NsConnectionLimitRegression.AnonymousCapRegression` and `…AnonymousOpsBucketRegression` (clock advanced, no
    sleeps).
  - Mutation: skip the release on idle timeout → `ReleasedOnEveryClosePathRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LimitEnforcementTableRegression|NsConnectionLimitRegression|Transaction' --output-on-failure -j8`.
- [ ] **Step 3:** commit `gateway, txn: connection, open-txn and txn-time limits; anonymous caps` and push.

### Task 11: limits — disk and over-disk

Spec §5.2, §5.4; D25.

**Files:**
- Modify:
  - `Worker.cpp` and the partition log: an incremental `bytesOnDisk_` per partition (append adds, segment delete
    subtracts); rebuilt from segment sizes at boot;
  - the per-slot sum is sent to the aggregator (Task 14) each tick; until then, a local check in the worker;
  - leader produce for a slot in OverDisk gets NamespaceLimitExceeded; follower apply, replication catch-up and
    internal topics are never refused.
- Test: `broker/libs/node/tests/NsDiskLimitRegressionTest.cpp`; rows in `LimitEnforcementTable.h`.

- [ ] **Step 1: failing tests**:
  - `LimitEnforcementTableRegression.DiskRegression`.
  - `OverDiskRegression.FollowerApplyNeverRefused` (3-node): the leader's namespace goes over; followers still apply
    every acked record; no ISR shrink.
  - `OverDiskRegression.RetentionFreesAndProduceResumesRegression`.
  - `OverDiskRegression.BytesOnDiskMatchesFilesAfterRestartRegression`.
  - Mutation: apply the check on the follower path → `FollowerApplyNeverRefused` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LimitEnforcementTableRegression|OverDiskRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `node: namespace disk limit; over-disk refuses leader produce only` and push.

### Task 12: limits — view memory and PAUSED

Spec §5.2, §5.4; D26.

**Files:**
- Modify:
  - `broker/libs/view/src/ViewTable.cpp`: memory = chunkCount × chunkBytes, kept incrementally;
  - `WorkerViewSet.cpp`: a slot's views over the limit move to `ViewPaused{OverLimit, offset}`; the view stops
    consuming at `offset`; reads get ViewPaused (54) with the paused offset; when usage drops or the limit is raised,
    the view resumes from `offset` and catches up;
  - the overshoot is at most one batch, recorded in Spec gaps.
- Test: `broker/libs/node/tests/NsViewMemoryRegressionTest.cpp`; rows in `LimitEnforcementTable.h`.

- [ ] **Step 1: failing tests**:
  - `LimitEnforcementTableRegression.ViewMemoryRegression`.
  - `NsViewMemoryRegression.PausedReadReturnsCodeAndOffsetRegression`.
  - `NsViewMemoryRegression.ResumeCatchesUpEqualsRecomputeRegression`: after resume the view equals a recompute
    from the log.
  - `NsViewMemoryRegression.OvershootAtMostOneBatchRegression`.
  - `NsViewMemoryRegression.OtherNamespaceViewsUnaffectedRegression`.
  - Mutation: resume from the current log end instead of `offset` → `ResumeCatchesUpEqualsRecomputeRegression`
    fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'LimitEnforcementTableRegression|NsViewMemoryRegression|View' --output-on-failure -j8`.
- [ ] **Step 3:** commit `view: namespace view-memory limit pauses views at an offset and resumes` and push.

### Task 13: limits — the retention clamp

Spec §5.1, §5.2; D27.

**Files:**
- Modify: `broker/libs/node/include/rune/node/LogCleanerAgent.h`, its `.cpp`, and
  `broker/libs/storage/src/LogCleaner.cpp`: the effective retention of a topic is
  `min(topic.retention, ns.maxRetention)` for time and bytes; read at each cleaner pass, so a lowered limit takes
  effect on the next pass.
- Test: `broker/libs/node/tests/RetentionClampRegressionTest.cpp`; rows in `LimitEnforcementTable.h`.

- [ ] **Step 1: failing tests**:
  - `RetentionClampRegression.TimeClampedRegression`, `…BytesClampedRegression`, `…TopicLowerWinsRegression`,
    `…LoweredLimitNextPassRegression`, `…InternalTopicsNotClampedRegression`.
  - `LimitEnforcementTableRegression.RetentionRegression`.
  - Mutation: use `max` → `TimeClampedRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'RetentionClampRegression|LimitEnforcementTableRegression|LogCleaner' --output-on-failure -j8`.
- [ ] **Step 3:** commit `storage: clamp topic retention to the namespace maximum` and push.

**Checkpoint C.**

### Task 14: the limit aggregator and Throttle

Spec §5.3, §5.4; D28. Needs Phase 0 (D31).

**Files:**
- Create: `broker/libs/node/include/rune/node/NsLimitAggregator.h`, `broker/libs/node/src/NsLimitAggregator.cpp`:
  owned by the control thread; every `RUNE_NS_LIMIT_TICK_US` it reads the per-slot rate and byte counters from
  arcanum-metrics (produce bytes/s, fetch bytes/s, request ops/s, disk bytes), compares them with the slot's limits
  and broadcasts `Throttle(nsSlot, nsId, kind, untilNs)` on change only.
- Modify:
  - `ClientGateway.cpp`, `Worker.h`: `throttleUntilNs[slot][kind]`; the hot path pays one add (the counter) and one
    compare (`nowNs < throttleUntilNs`); a throttled request gets Throttled (53) with `retryAfterNs`, old clients
    InvalidRequest;
  - Task 11's local disk check moves here (OverDisk becomes a Throttle kind with `untilNs = INT64_MAX` until it
    clears).
- Test: `broker/libs/node/tests/NsLimitAggregatorRegressionTest.cpp`, `broker/libs/node/tests/NamespaceThrottleRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace rune::node {
enum class ThrottleKind : uint8_t { ProduceBytes = 0, FetchBytes = 1, RequestOps = 2, Disk = 3, kCount = 4 };
struct Throttle { uint16_t slot; uint32_t nsId; ThrottleKind kind; int64_t untilNs; };
class NsLimitAggregator {
public:
    int doWork(int64_t nowNs);       // one tick per RUNE_NS_LIMIT_TICK_US; zero allocation
};
}
```

- [ ] **Step 1: failing tests**:
  - `NsLimitAggregatorRegression.OverRateEmitsThrottleOnceRegression`, `…ClearsWhenUnderRegression`,
    `…NoBroadcastWhenUnchangedRegression`, `…RetryAfterIsNextTickRegression`, `…LimitRaisedMidTickRegression`.
  - `NamespaceThrottleRegression.ProduceThrottledReturnsRetryAfterRegression` (real client, real broker).
  - `NamespaceThrottleRegression.OtherNamespaceNotThrottledRegression`.
  - `NamespaceThrottleRegression.RecreateUnderThrottleStartsClearRegression`: a throttled `a` is deleted and
    recreated with the same slot; the stale Throttle tagged with the old nsId is ignored.
  - `NamespaceThrottleRegression.ClientBacksOffAndSucceedsRegression`.
  - `LimitEnforcementTableRegression.RatesRegression`.
  - Mutation: ignore the nsId on a Throttle → `RecreateUnderThrottleStartsClearRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NsLimitAggregatorRegression|NamespaceThrottleRegression|LimitEnforcementTableRegression|OverDiskRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `node: namespace limit aggregator and throttle broadcast` and push.

### Task 15: hot path — zero allocation and the ABAB bench

Spec §5.4; Global Constraints 2–3; rules 4, 6, 12.

**Files:**
- Create: `broker/bench/NamespaceHotPathBench.cpp` (produce, fetch and point read with namespaces on and limits set,
  against tip without the gate and counter).
- Modify: `broker/bench/CMakeLists.txt`, `broker/bench/BENCHMARKING.md` (a new baseline row).
- Test: `broker/libs/node/tests/NsHotPathAllocRegressionTest.cpp`.

- [ ] **Step 1: failing tests**: `NsHotPathAllocRegression.ProduceZeroAllocRegression`, `…FetchZeroAllocRegression`,
  `…ViewReadZeroAllocRegression`, `…ThrottledRefusalZeroAllocRegression`, `…SuspendedRefusalZeroAllocRegression`,
  using the repo's allocation-counting hook over 10 000 steady-state requests. Mutation: build the Throttled reply
  in a `std::vector` → `ThrottledRefusalZeroAllocRegression` fails.
- [ ] **Step 2:** run the bench per BENCHMARKING.md: clean box, Release, interleaved ABAB, n≥5 for latency and
  n≥3 for throughput. The gate: no p99 above the CLAUDE.md budgets, and no shift beyond the noise band named in
  BENCHMARKING.md. If a p99 shift is small, n≥15 before any claim. Name the code path for any shift.
- [ ] **Step 3:** record the baseline row; commit `bench: namespace hot path ABAB bench and zero-alloc regression`
  and push.

### Task 16: CLI and usage

Spec §4, §5.1; D29.

**Files:**
- Modify:
  - `client-cpp/src/cli/NamespaceCommands.cpp`, `client-cpp/include/rune/cli/Cli.h`, `client-cpp/src/cli/Cli.cpp`:
    `rune ns suspend <name> --read-only|--reject-all`, `rune ns resume <name>`, `rune ns delete <name> [--yes]`,
    `rune ns usage <name>`;
  - `ClientGateway.cpp`: template 84 handler; usage values read from the slot's counters (cluster admin, or a user
    in the namespace with `DescribeNamespace`).
- Test: `client-cpp/tests/NamespaceClientTest.cpp` (extend), `broker/libs/node/tests/NamespaceUsageRegressionTest.cpp`.

- [ ] **Step 1: failing tests**:
  - `NamespaceCliRegression.SuspendResumeDeleteRegression`, `…DeleteWithoutYesPromptsRegression`,
    `…UsageTableRegression`, `…UnknownNamespaceRegression`.
  - `NamespaceUsageRegression.ValuesMatchCountsRegression`: 3 topics, 7 partitions, 2 views → usage shows 3, 7, 2.
  - `NamespaceUsageRegression.NotAuthorizedOutsideNamespaceRegression`.
  - Mutation: report the limits instead of the usage → `ValuesMatchCountsRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceCliRegression|NamespaceUsageRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `cli: namespace suspend, resume, delete and usage` and push.

**Checkpoint D.**

### Task 17: the interaction matrix rig

Spec §8; the Interaction matrix section above.

**Files:**
- Create: `broker/libs/node/tests/support/NamespaceLifecycleMatrixExpectations.h`,
  `broker/libs/node/tests/NamespaceLifecycleMatrixTest.cpp`.

- [ ] **Step 1:** write the expectations header exactly as the Interaction matrix section defines it
  (`LcOp`, `LcCaller`, `LcTarget`, `LcTiming`, `LcOutcome`, `expect`, `naRule`, `expectedCodes`).
- [ ] **Step 2: tests**:
  - `NamespaceLifecycleMatrixCountRegression.PinnedRegression`: 324 cells, 24 n/a, 300 applicable, 36 timing,
    336 executed.
  - `NamespaceLifecycleMatrixRegression.Cell/*` (TEST_P over the 300 applicable cells, real broker via
    `NamespaceRig.h`): each asserts the code and the `after` state.
  - `NamespaceLifecycleMatrixRegression.Timing/*` (TEST_P over the 36 timing cells): back-to-back before the reply,
    crash mid-delete, leader change mid-delete, follower catch-up, limit change mid-tick, recreate under Throttle,
    txn open.
  - Mutation check: let a non-admin caller suspend → more than 30 cells turn red; record the exact count in the
    commit.
- [ ] **Step 3:** run `ctest --test-dir build -R 'NamespaceLifecycleMatrix' --repeat until-fail:20 --output-on-failure`
  and once under `-j8` load. Fix every red cell at its root (rule 13), each with its own regression test.
- [ ] **Step 4:** commit `test: namespace lifecycle interaction matrix` and push.

### Task 18: randomized model-checked stress

Spec §8; closes the Phase 1 delete-invariant TODO in `NamespaceStressTest.cpp`.

**Files:**
- Modify: `broker/libs/node/tests/NamespaceStressTest.cpp`.

- [ ] **Step 1:** extend the op set with suspend (both modes), resume, delete, recreate, SetLimits up and down,
  produce, commit offset, open/commit/abort txn, crash and restart, leader change (3-node variant). A model map
  holds the expected registry, gate, limits and per-namespace contents.
- [ ] **Step 2:** after every step check the invariants: the registry equals the model; no object of a DELETING or
  erased nsId is visible to any read; usage ≤ limits except as the lowered-limit rule allows; no committed txn
  record is lost; no aborted one is visible to `read_committed`; offsets never move back.
- [ ] **Step 3:** seeds 1..20, a 2 000-step cap per seed, fail on the cap; `--repeat until-fail:20` and one run
  under `-j8`. Mutation: skip `TxnStateTombstoned` in the reaper → a seed fails; record which.
- [ ] **Step 4:** commit `test: namespace lifecycle model-checked stress` and push.

### Task 19: the 3-node slice

Spec §8.

**Files:**
- Create: `broker/libs/node/tests/NamespaceLifecycleTrioRegressionTest.cpp` (`CatalogTrio.h`, `AuthTrio.h`).

- [ ] **Step 1: tests**:
  - `NamespaceLifecycleTrioRegression.SuspendConvergesOnEveryNodeRegression`: a session on each node; suspend RA;
    all three closed.
  - `…DeleteFinishesAfterLeaderKillRegression`: kill the catalog leader mid-delete; the new leader finishes; every
    node's dirs are gone.
  - `…FollowerRejoinsAfterDeleteRegression`: a follower down for the whole delete rejoins and converges.
  - `…ThrottleOnEveryNodeRegression`: limits per node as D24 and D28 define.
  - `…AcksAllNoLossAcrossSuspendResumeRegression`: produced = consumed across a suspend RO / resume cycle.
- [ ] **Step 2:** run `ctest --test-dir build -R 'NamespaceLifecycleTrioRegression' --repeat until-fail:20 --output-on-failure`.
- [ ] **Step 3:** commit `test: namespace lifecycle 3-node slice` and push.

### Task 20: e2e and docs

Spec §8; rule 11.

**Files:**
- Modify: `broker/docker/namespace-e2e.sh`, `docs/namespaces.md` (or the Phase 1 doc), `CLAUDE.md` §8.

- [ ] **Step 1:** extend the e2e script on the default config:
  1. Step 10 (limits) now asserts enforcement: a create over the topic limit gets NamespaceLimitExceeded.
  2. Suspend `a` RO: produce refused, fetch served; resume; produce served.
  3. Suspend `a` RA: the session is closed; login gets NamespaceSuspended; resume.
  4. A produce rate over the limit gets Throttled; the client backs off and succeeds.
  5. Delete `a` with data in every kind; wait for erase via `rune ns list`; recreate `a`; it is empty.
  6. Restart every broker; all of the above still holds.
- [ ] **Step 2:** run it on the shared VM (`$RUNE_VM_HOST`):
  1. Hold the shared lock for the whole run.
  2. Work only in your own directory.
  3. Check `df` before and after, and clean up only your own containers.
- [ ] **Step 3:** docs: the lifecycle, the limits and their error codes, the CLI. CLAUDE.md §8: one line that
  namespaces have suspend, delete and enforced limits, and groups and txns work in every namespace.
- [ ] Commit and push.

**Checkpoint E**, then the final whole-branch review (most capable model, exhaustive and adversarial, against the
Review Focus list and the matrix).

## Spec gaps (decided here, recorded for review)

- **Kind 36 `NamespaceDeleteStep`:** the spec names the delete steps but not how their progress is made durable;
  one catalog event per step makes the reaper restartable from the registry alone (rule 16).
- **D5 amendment:** delete is one `NamespaceDeleted` event that is the commit point; `StateChanged → Deleting` is
  refused, so there is one path into DELETING.
- **Template ranges:** Phase 1's D8 listed the group and txn template ranges wrongly; D30 gives groups 32–44 and txns
  45–54.
- **lowestAcks per produce:** the `acks` floor is checked on every produce, not only at topic create, because a
  client picks `acks` per request.
- **Connections per node:** the connection limit is per node, not cluster-wide, so it needs no cross-node
  coordination on the login path.
- **View-memory overshoot:** a view may exceed its limit by at most one event batch before it pauses.
- **No version bump:** the new codes are gated on client ≥ 14 and no wire layout changes for older peers; there are
  no mixed-version clusters.
- **Phase 1 limits size:** the limits record is 256 bytes with 29 fields (`28*8 + 4 + 4 + 3*8`), not 272 bytes and
  30; the Phase 1 plan is corrected in the same commit.
- **D33 kinds 3–5:** Suspend, Resume and Delete are NamespaceDdl kinds 3, 4 and 5, all cluster admin only.

## Self-review (spec section → task)

| Spec | Task |
|---|---|
| §1 groups and txns in every namespace | 5 |
| §4 suspend (RO, RA) | 1, 2, 3, 4 |
| §4 resume | 1, 2, 3 |
| §4 delete and reaper | 1, 2, 6, 7 |
| §4 recreate after delete | 2, 7 |
| §5.2, §5.4 count, durability, feature limits | 9 |
| §5.2, §5.4, §11 connections, txns, anonymous caps | 10 |
| §5.2, §5.4 disk | 11, 14 |
| §5.2, §5.4 view memory | 12 |
| §5.1, §5.2 retention | 13 |
| §5.1 lowering a limit | 9 |
| §5.3, §5.4 slots, aggregator, throttle | 8, 14 |
| §5.4 hot-path cost | 15 |
| §10 CLI and usage | 1, 16 |
| §8 interaction matrix, stress, 3-node, e2e | 17, 18, 19, 20 |

## Execution handoff

Run the plan with superpowers:subagent-driven-development:
- one implementer per task (mid-tier model) with the exact recipe;
- a task review after each task;
- checkpoints A–E as listed.

Implementers never spawn subagents; at most 3 agents run at once. Root-cause diagnosis of any red test stays with the
controller (CLAUDE.md rule 14).
