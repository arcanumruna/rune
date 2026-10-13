# Rune — Catalog History and Replication (`__catalog`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every catalog change (topics, schemas, views) becomes a fingerprinted, versioned event in one replicated, never-compacted internal topic `__catalog`; every broker derives its catalog by applying that log in offset order through one shared replay library (also linked by the client and the `rune` CLI), which gives audit (`SHOW HISTORY`), time travel and diff (`AS OF`, `DIFF`), replay-to-rebuild (`rune catalog export|replay|diff`), cluster-wide DDL serialization, and working multi-broker runtime DDL; `RUNE_TOPICS` and every static-topic path are deleted and topics exist only through DDL.

**Architecture:** A new library `rune_catalog` (in the rune-codec sibling, namespace `rune::catalog`) defines the fixed-layout event (`formatVer` 1), the catalog state, the frozen v1 apply rules, per-object fingerprints, an incremental verifying replayer, the read queries (history / as-of / diff) and the `.rcat` export file. In the broker, `__catalog` is internal partition `kCatalogTopicId = -6`, one partition replicated to every broker, disk-backed, never compacted or retention-deleted, minIsr = majority, placed and led on the controller-ISR path the coordinator partitions already use (never unclean); its replica is owned and driven by the AdminWorker thread over a dedicated admin-thread replication plane. The catalog leader's AdminWorker validates DDL against its applied `CatalogState`, assigns `objectId` / versions / topic ids, stamps the fingerprint (computed by the same library), appends, and answers once the event committed and applied locally; every broker's `CatalogApplier` applies committed events in offset order, verifies each fingerprint (a mismatch halts apply loudly), persists an applied-offset checkpoint before any side effect, and hands the effects to the control thread and workers over the existing rings. Placement stays in the consensus metadata log; a topic serves on a broker only when its `CreateTopic` is applied there and its placement is committed (C1 generalized). Clients read `__catalog` with the existing Subscribe (topic id −6) from any broker and replay it with the shared library.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark, CMake/Ninja (`build-fast`, `build-bench-rel`), real Aeron UDP for the in-process BrokerNode integration tests, docker compose (3 nodes) for e2e.

**Spec:** `docs/superpowers/specs/2026-10-13-rune-catalog-history-design.md` (approved 2026-10-13; scheduled before Phase 2b Part D). Every section §1–§11 is mapped to tasks in the Self-review.

**Builds on / order:** executes on `feature/materialized-views` after the E2 merge (broker `7d86db94`, protocol `efc25fb`, client `d0a5c74`, rune-ql `0814827` on `feat/phase2b-e-transient-topics`, rune-codec `0ccf109` on `feat/schema-evolution-durable-tags`). What exists today and what this plan replaces (each verified in the code at those SHAs; line numbers are hints — use the quoted anchors):

- **Catalog today:** two admin-owned compacted logs, `__topic_schemas` (topicId −4, `TopicSchema` records keyed `(topicId,version)`) and `__views` (−5, `ViewRecord`), both built in `BrokerNode::buildAdminWorker()` on a **`NullPartitionTransport`** (`BrokerNode.cpp` ~`:3010`: "Real multi-node follower fetch routing on an admin-thread-owned transport is Docker-gated (Task 8b)") — i.e. **not replicated**. That, plus the admin role following the *consensus* leader (`serviceAdminControl_`, `AdminRoleMsg` from `leaderObserver_.computeOnControl()`), is the E2 carry-over "multi-node runtime DDL fails".
- **DDL write path today:** `AdminWorker` (`libs/node/src/AdminWorker.cpp`) → `requestCreateTopic` ring round-trip to the control thread (`createTopicForAdmin`, `AdminCreateTopic.h`) which allocates the dense `topicId` in `TopicRegistry` → append to `__topic_schemas`/`__views` → HW gate → `kA2cTopicCommitted` → `TopicRegistry::commitTopic` + `createRuntimeUserReplicas` + `MetadataService::addRuntimeTopic` → `DdlAnswerGate` holds the OK until the topic serves (C1).
- **Boot today:** `BrokerNode::start` step 1b (anchor: `// 1b. Restart recovery of RUNTIME-defined topics`) reads both catalogs off disk (`recoverRuntimeTopics`), appends recovered specs to `config_.topics` (the `RUNE_TOPICS` list), fills id gaps with `__orphan.<id>` placeholders, runs `refuseStaticNamesCollidingWithRecovered` and `reconcileTopicDirs` (two-phase `topic-id` markers, quarantine).
- **`RUNE_TOPICS`:** parsed in `NodeConfig.cpp` (anchor `// --- RUNE_TOPICS (optional) ---`, options `group= key= rf= persistence= durability= tier=` + positional cleanup/retention fields), consumed through `config_.topics` in ~20 places in `BrokerNode.cpp`; 43 files reference it (libs/, docker/, docs/, dr-e2e/, bench/), 22 in-process test files set `NodeConfig::topics`.

## Execution context

- **Repos / branch:** create `feat/catalog-history` in each worktree under `/Users/vradu/Projects/se-worktrees/`:
  - `rune-broker`: `git -C /Users/vradu/Projects/se-worktrees/rune-broker switch -c feat/catalog-history 7d86db94`
  - `rune-protocol`: `git -C /Users/vradu/Projects/se-worktrees/rune-protocol switch -c feat/catalog-history efc25fb`
  - `rune-client-cpp`: `git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp switch -c feat/catalog-history d0a5c74`
  - `rune-ql`: `git -C /Users/vradu/Projects/se-worktrees/rune-ql switch -c feat/catalog-history 0814827`
  - `rune-codec`: `git -C /Users/vradu/Projects/se-worktrees/rune-codec switch -c feat/catalog-history 0ccf109`

  Verify each with `git -C <repo> log --oneline -1` before Task 1. Never touch `/Users/vradu/Projects/rune-protocol` or `/Users/vradu/Projects/rune-client-cpp` (other branches, dirty).
- **Builds (already configured — re-run cmake only after a `CMakeLists.txt` edit):**
  - broker tests: `build-fast` (Ninja, Debug, `BUILD_TESTING=ON`, `RUNE_PROTOCOL_INCLUDE` → `../rune-protocol/include`, `RUNE_CODEC_ROOT` → `../rune-codec`). After a CMake edit: `cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-fast`. If missing: `cmake -S . -B build-fast -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DCMAKE_TOOLCHAIN_FILE=/Users/vradu/Projects/se-worktrees/rune-broker/build/Debug/generators/conan_toolchain.cmake -DRUNE_PROTOCOL_INCLUDE=/Users/vradu/Projects/se-worktrees/rune-protocol/include -DRUNE_CODEC_ROOT=/Users/vradu/Projects/se-worktrees/rune-codec`.
  - broker benchmarks: `build-bench-rel` (Ninja, `BUILD_BENCHMARKS=ON`, `-O3 -DNDEBUG`).
  - client: `build` (Unix Makefiles; `RUNE_PROTOCOL_INCLUDE` → `../rune-protocol/include`, `RUNE_CODEC_ROOT` → `../rune-codec`, `RUNE_QL_ROOT` → `../rune-ql`).
  - protocol: `build` (target `rune_protocol_tests`). rune-ql: `build` (target `rune_ql_tests`). rune-codec: `build` (targets `rune_codec_tests`, new `rune_catalog_tests`).
- **Test gate (CLAUDE.md rule 7):**
  - **Per task / fix round:** build, then run the executables covering the code the task touches plus every new or changed test, at `-j14` — each task's "Run" step names the exact filter: `ctest --test-dir <build> -R '<filter>' --output-on-failure -j14`.
  - **The FULL `*Regression*` suite runs only at the checkpoints**, in every repo touched since the last checkpoint: broker `ctest --test-dir build-fast -R Regression --output-on-failure -j14`; client `ctest --test-dir build -R Regression --output-on-failure -j14`; protocol `ctest --test-dir build -R Regression --output-on-failure`; rune-ql `ctest --test-dir build -R Regression --output-on-failure`; rune-codec `ctest --test-dir build -R Regression --output-on-failure -j14`. A checkpoint failure is bisected over the commits since the last green checkpoint.
  - **Checkpoints:** A after Task 3 (shared library); B after Task 5 (wire + client DDL); C after Task 8 (catalog log + apply + effects); D after Task 12 (write path, conformance port, leadership, reads); E after Task 15 (client reads, QL, CLI); F after Task 19 (boot from the catalog, `RUNE_TOPICS` gone, tests and scripts migrated); G after Task 25 — before the final whole-branch review.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Global Constraints

- **HARD RULES (CLAUDE.md, all eight), as they bind here:**
  1. **Shared-nothing.** The `__catalog` replica, `CatalogState`, `CatalogApplier`, `SchemaRegistry` and `ViewRegistry` are owned by the admin thread only; `TopicRegistry`, `ReplicaRegistry`, `MetadataService` by the control thread only; the gateway never reads catalog state — it gets a control-thread-owned `CatalogRoute` value. No locks, no atomics as coordination, no `thread_local`.
  2. **Rings are the only cross-thread channel.** Every new hop is a message on an existing SPSC ring (`AdminChannels.h`): role + ISR relays, applied-topic / dropped-topic / config / view-uninstall / halted effects, dropped-dir purges, catalog subscribe forwarding. No cross-thread callback, no shared pointer read across threads.
  3. **Per-state variables.** `AdminWorker` leadership is `std::variant<CatalogFollower, CatalogLeaderCatchingUp, CatalogLeaderServing>`; the in-flight DDL is `std::variant<Idle, AwaitingCommit>`; `CatalogApplier` is `std::variant<Applying, Halted>`; a TopicRegistry entry is `Serving` or `Dropped`; a create behind a not-yet-retired drop of the same name is `PendingCreateBehindDrop`. No scattered booleans.
  4. **Zero steady-state allocation.** The produce/consume/query hot path is untouched (gate: Task 23). The catalog path is control plane (cold), but its steady state still allocates nothing: the applier, the catalog read server and an idle catalog subscription reuse preallocated buffers (capacity persists); an idle cluster's admin pass allocates nothing (Task 23 counts it).
  5. **Test and benchmark everything:** unit, parameterized matrix, seeded model-checked stress, 3-node BrokerNode integration over real Aeron, docker e2e; benchmarks: library replay (Task 3), boot replay of 10 000 events < 1 s (Task 23), hot-path produce/consume benches unchanged (Task 23).
  6. **A `*Regression*`-named test on every fix** (every fix-on-sight below carries one).
  7. **Affected-area tests per task; the FULL `*Regression*` suite at checkpoints A–G.**
  8. **Fix-on-sight.** Found while planning and fixed in the owning task: the unreplicated `__topic_schemas`/`__views` (`NullPartitionTransport`, Task 6); the admin role following the consensus leader instead of a clean catalog leader (Task 6); the rebuild-on-promotion path that applies an unverified prefix and only on the leader (replaced by the verified applier on every broker, Tasks 7 and 16); a non-leader admin answering DDL `Internal` instead of a retriable redirect (Task 11); a demoted admin answering an in-flight DDL `Internal` instead of the retriable `NotLeader` (Task 9).
- **No hangs, no silent fallback (spec §9):** every DDL with a decodable header is answered exactly once — OK, a specific code, `Redirect`, retriable `NotLeader`, or `Internal`; every client DDL ends in a result or an explicit failure within its budget; a replay / diff / apply fingerprint mismatch is an explicit error naming the offset; an unknown `formatVer` or kind stops the replay with an error, never a skip.
- **Spec §3 (verbatim):** "One partition, replicated to every broker. Written with acks=all; minIsr is a majority. Unclean leader election is disabled. Never compacted, never retention-deleted. Always disk-backed. It is never transient. It is the single source of truth for the catalog."
- **Spec §4:** the event fields are exactly `formatVer u16, kind u16, objectId u64, objectName, prevVersion/newVersion u32, statement, ops, principal, replayedFrom, timestampMs i64, fingerprint u64`; the fingerprint is of the object's **resulting state**, which is NOT stored; drops are events; a reused name gets a new `objectId`.
- **Spec §6:** apply rules per `formatVer` are frozen; one shared library; every replay verifies every fingerprint; golden logs per `formatVer` replay to fixed fingerprints forever.
- **Spec §8:** `replicationFactor ≤ broker count` and `replicationFactor ≥ minIsr`, each an explicit error; no `RUNE_TOPICS` mention remains (grep gate, Task 19).
- **No migration** (spec §1): `__catalog` replaces `__topic_schemas` / `__views` outright; old data dirs of those logs are ignored (logged once) and never read.
- **Error codes (spec §9) — no new protocol error codes:** Redirect `3` (ErrorResponse, message = leader client endpoint), NotLeader `2`, TopicExists `25`, the existing specific codes, InvalidRequest `39` with a detail message, Internal `6` on a halted broker.
- **Carry-overs folded in:** this phase closes the E2 carry-over "multi-node runtime DDL fails" (Tasks 6, 9, 22) and the deferred "multi-node `__topic_schemas` replication" (schema-evolution spec §8); the static/runtime name-collision boot rule (`refuseStaticNamesCollidingWithRecovered`) is deleted with `RUNE_TOPICS` (Tasks 16–17). Part D (`rune console` / `rune schema`) is scheduled after this phase and builds on Task 13's reads and Task 15's `rune ql`; the console must also map codes 37/42/43 (recorded by E/E2) — still Part D's.

## Review Focus

Most likely to bite a user, each pinned by a named test in its owning task:

1. **A DDL whose leader dies after the append must be neither lost nor applied twice**, and the retrying client must get the original success, not `TopicExists` → `CatalogWritePathRegression.DuplicateRequestIdAnswersTheOriginalResultRegression` (Task 9, request-id dedupe), `CatalogLeadershipRegression.RetryAfterLeaderChangeReturnsTheOriginalResultOnceRegression` (Task 11) and the matrix's `LeaderChangeMidDdl` column (Task 20), stress invariant "no DDL lost or duplicated" (Task 21).
2. **A broker that restarts between applying an event and creating its topic directory must neither quarantine nor lose that topic's data** → `CatalogApplierRegression.CheckpointPrecedesSideEffectsSoARestartNeverQuarantinesRegression` (Task 7) and the matrix's `RestartMidApply` column.
3. **A dropped name reused immediately must get a fresh, empty topic — never the dropped topic's data or directory** → `CatalogEffectsRegression.CreateBehindDropWaitsForTheDirRenameAndStartsEmptyRegression` (Task 8), matrix `DropAndReuseName`.
4. **A producer that learns a topic from one broker and produces to a partition leader that has not applied it yet must get a retriable answer, not `UnknownTopic`** → `CatalogGatewayRegression.PlacedButNotYetAppliedTopicAnswersNotLeaderRegression` (Task 8), matrix `CreateRacesPlacement`.
5. **A follower whose apply diverges must stop loudly and refuse DDL, while every other broker keeps working** → `CatalogApplierRegression.FingerprintMismatchHaltsApplyRaisesTheMetricAndKeepsServingRegression` (Task 7), `CatalogLeadershipRegression.HaltedBrokerAnswersDdlInternalNotRedirectRegression` (Task 11), `CatalogBootRegression.HaltedBootReplayStillBootsAndRefusesDdl` (Task 16), matrix `FingerprintMismatch`.
6. **A catalog read from a follower must never show an event that is not committed** → `CatalogReadServerRegression.FollowerServesOnlyUpToItsHighWatermarkRegression` (Task 12).

Also pinned: a new catalog leader refuses DDL until it applied up to its HW (`CatalogLeadershipRegression.NewLeaderAnswersNotLeaderUntilCaughtUpRegression`, Task 11); `rf` above the broker count / below minIsr is an explicit `InvalidRequest` with detail (`CatalogWritePathRegression.ReplicationFactorBoundsAreExplicitErrorsRegression`, Task 9); an unknown `formatVer` stops a replay at that offset (`CatalogReplayerRegression.UnknownFormatVersionStopsAtItsOffsetRegression`, Task 3); a golden log replays to its pinned fingerprints (`CatalogGoldenRegression.V1LogsReplayToPinnedFingerprints`, Task 3); no `RUNE_TOPICS` survives (`NoRuneTopicsGateRegression.RepositoryHasNoRuneTopicsMention`, Task 19); a set `RUNE_TOPICS` refuses to load instead of being ignored (`NodeConfigRegression.RuneTopicsIsRefusedWithAPointerToDdl`, Task 17).

---

## Spec ambiguities resolved by this plan (binding for executors)

1. **Where the shared replay library lives.** Spec §6 / Repos put it under rune-client-cpp, but the broker must link it and has no client dependency. **Ruling:** `rune_catalog` lives in **rune-codec** (`include/rune/catalog/`, `src/catalog/`) — the one sibling every consumer (broker, client, CLI) already compiles from source — so all three link byte-identical rules. The client exposes it through its own API (Task 13).
2. **Schema events vs objects.** **Ruling:** a schema is an aspect of its topic object. `CreateSchema` / `AlterSchema` / `SetCompatibility` carry the topic's `objectId`; `prevVersion`/`newVersion` are the topic object's version (every event on the object bumps it by 1); the schema's own contiguous version (`u16`, the wire/`SchemaRegistry` version) is part of the resulting state. `SHOW SCHEMA t AS OF VERSION n` selects schema version `n`; `SHOW TOPIC t AS OF VERSION n` selects object version `n`.
3. **Which DDL emits which kind.** `DefineTopic` on a free name → `CreateTopic` (carrying the schema when the descriptor is non-empty; an empty descriptor is an **untyped** topic); `DefineTopic` with a descriptor on an existing untyped topic → `CreateSchema` (today's "schema-less adoption"); `AlterTopic` whose ops are all `SET COMPATIBILITY` → `SetCompatibility`, any other `AlterTopic` → `AlterSchema`; new wire ops `DropTopic`, `DropView`, `AlterView`, `AlterTopicConfig` → their kinds.
4. **`AlterView` semantics.** The view-engine spec defers the live background cut-over. **Ruling:** `AlterView` replaces the view's plan and output under the same `objectId` with a **fresh sink topic** (new `viewTopicId`); the old sink is dropped; the view re-folds from its source and `GetViewRow` answers `NotReady` until caught up. The event layout does not change when the live cut-over lands later (it changes serving, not definitions).
5. **`AlterTopicConfig` keys.** Alterable: `retention.ms`, `retention.bytes`, `delete.retention.ms`, `min.cleanable.dirty.ratio`, `min.compaction.lag.ms`, `max.compaction.lag.ms` (the cleaner consumes them; applied through a cleaner policy override on its own ring). Every other property (partitions, `replicationFactor`, persistence, cleanup policy, segment size, durability, tier, co-partition group, partition key) is immutable → `InvalidRequest` with detail.
6. **Drop rules.** `DropTopic` of a topic a view reads → `InvalidRequest` naming the view; `DropTopic` of a view name → `InvalidRequest` ("is a view; use DROP VIEW"); unknown names → `UnknownTopic (7)` with detail (there is no `UnknownView` error code; `ViewRowResponse` status 4 is a read status, not a DDL code).
7. **`__catalog` leadership.** "Unclean disabled" + "every broker" rules out following the consensus leader (a lagging consensus leader would lose committed events). **Ruling:** `__catalog` joins the controller-ISR path of the coordinator partitions (re-review MEDIUM-B, `MetadataService::decideCoordinatorPartition`): replicas = every broker, leader elected only from the ISR (`allowUnclean = false`), the controller prefers itself when it is in the ISR (so the catalog leader is normally the controller). Single-node keeps the consensus-driven path.
8. **What "applied" survives a restart.** There is no persisted HW. **Ruling:** the applier persists `dataDir/__catalog/applied` = (last applied offset, state digest) **before** posting any side effect; boot replays `[0, applied]` only (always committed) and catches up the rest live.
9. **Exactly-once DDL across a leader change.** **Ruling:** every DDL carries a client `requestId` (u64, stable across retries) in the event's principal; the leader answers a retry whose `requestId` already committed with that event's original result.
10. **Catalog reads and "caught up".** Spec §7 reuses Subscribe; a client must know when it has the whole committed prefix. **Ruling:** for topic id −6 only, the serving broker pushes one `Records` frame with `baseOffset = HW` and an empty records payload as the caught-up marker after the backlog, documented in `Protocol.h`; no layout change.
11. **View fingerprints across clusters.** Ids differ between clusters (spec §7 replay). **Ruling:** fingerprints cover definitions, never ids: a view's fingerprint covers its source **names** and its authored plan **normalized** (each source `topicId` replaced by its ordinal), never the compiled plan (which embeds ids).
12. **"New broker joining".** Rune peers are static configuration. **Ruling:** a new broker is a configured peer started with an empty data dir after the catalog has events; the coordinator replica set already names it; it replays from offset 0 and joins the ISR once caught up (AlterIsr).
13. **Where `principal.sourceAddress` comes from.** Connect carries no client name. **Ruling:** the DDL extension carries `clientName` + `requestId`; the gateway adds `sessionId` and the session's egress channel as the source address.

---

## Interaction matrix

**Single source of truth:** `libs/node/tests/support/CatalogMatrixExpectations.h` (Task 20) — `expect(Kind, Entity, Timing) → Outcome`. The unit matrix (Task 20), the seeded stress (Task 21), the 3-node integration slice (Task 22) and the docker e2e (Task 24) all read it; no test hard-codes an outcome.

### Operations (9 event kinds, spec §2)

| Kind | Issued as | Precondition the harness builds |
|---|---|---|
| `CreateTopic` | `DefineTopic` on a free name (typed when `Persist` says, untyped otherwise — both alternate by cell parity) | name free |
| `DropTopic` | `DropTopic` | an untyped topic `t` exists, no view reads it |
| `AlterTopicConfig` | `AlterTopicConfig{retention.ms = 60000}` | `t` exists |
| `CreateSchema` | `DefineTopic` with a descriptor on an existing untyped topic | untyped `t` exists |
| `AlterSchema` | `AlterTopic{ADD COLUMN extra int32}` | typed `t` exists |
| `SetCompatibility` | `AlterTopic{SET COMPATIBILITY forward}` | typed `t` exists |
| `CreateView` | `CreateView` (single source, Replace) | typed source `s` exists |
| `AlterView` | `AlterView` (output gains one column) | view `v` over `s` exists |
| `DropView` | `DropView` | view `v` exists |

### Entities (2 × 2 = 4)

`Via {Leader, Follower}` × `Persist {Disk, Transient}`. `Via` is the broker the client sends the DDL to (a follower answers `Redirect`, the client follows). `Persist` is the persistence of the topic (or the view and its transient source) the op creates or targets.

### Timings (8) + failure (1)

| Timing | What the harness does |
|---|---|
| `Steady` | issue the op; all three brokers up |
| `LeaderChangeMidDdl` | the leader appends the event, then loses leadership before answering; the controller elects another ISR member; the client retries (same `requestId`) |
| `RestartMidApply` | one follower is stopped after persisting its applied checkpoint and before its side effects run, then restarted from its data dir |
| `NewBrokerJoins` | the op commits with brokers {1,2}; broker 3 starts with an empty data dir, replays from offset 0 |
| `CatalogBelowMinIsr` | two brokers are down; the op is issued to the survivor; then one broker returns and the op is retried |
| `ConcurrentDdlDifferentBrokers` | the same op (same name / same `expectedVersion`) is sent to two different brokers in the same pass, different `requestId`s |
| `DropAndReuseName` | create → drop → create the same name, then the op (on the new object) |
| `CreateRacesPlacement` | the op's topic is applied on a broker before its placement commits; produce/subscribe are issued in that window |
| `FingerprintMismatch` (failure) | follower 3's next apply is fed a tampered fingerprint (test seam on its applier); the op is then sent to broker 3 and to the leader |

### Expected outcome per cell (`expect()`; rules applied top-down)

| # | Rule | Outcome |
|---|---|---|
| R1 | `CreateRacesPlacement` × kind ∉ {`CreateTopic`, `CreateView`, `AlterView`} | `NotApplicable` (no new topic / sink is placed) |
| R2 | `FingerprintMismatch` | `HaltedBrokerInternalOthersOk`: broker 3's applier is `Halted` at that offset, its halt counter = 1, a DDL sent to broker 3 is answered `Internal (6)` (never redirected), the same op via the leader is `Ok`, brokers 1 and 2 agree on the digest; broker 3 still serves produce/consume on topics it applied |
| R3 | `CatalogBelowMinIsr` | `NotLeaderUntilBudgetThenOk`: first attempt answered `NotLeader (2)` until the client budget ends → explicit client failure `NotLeader`, nothing appended past the HW; produce/consume on an existing topic led by the survivor still succeed (acks=leader); after one broker returns, the retried op is `Ok` exactly once |
| R4 | `LeaderChangeMidDdl` | `NotLeaderThenOkOnce`: first answer `NotLeader (2)`, the retry (same `requestId`) answers `Ok` with the same `objectId` / version; exactly one event with that `requestId` in every broker's log |
| R5 | `ConcurrentDdlDifferentBrokers` | `OneWinsOneConflict`: exactly one `Ok`; the other gets the kind's conflict code — creates `TopicExists (25)` / `ViewExists (32)`; `AlterSchema`/`SetCompatibility`/`AlterTopicConfig` with the same `expectedVersion` `SchemaConflict (31)`; drops `UnknownTopic (7)`; `AlterView` `SchemaConflict (31)` |
| R6 | `DropAndReuseName` | `OkNewObjectId`: the op applies to the new object (new `objectId`, its version chain restarts at 1); history keeps both objects distinct |
| R7 | `CreateRacesPlacement` (applicable kinds) | `OkAfterPlacement`: produce/subscribe in the window answer `NotLeader (2)`; the DDL `Ok` is released only after the topic serves on the leader's broker; afterwards produce/subscribe succeed |
| R8 | `Via = Follower` × {`Steady`, `RestartMidApply`, `NewBrokerJoins`} | `RedirectThenOk`: first answer `ErrorResponse{Redirect, leader endpoint}`, the followed request `Ok` |
| R9 | `Via = Leader` × {`Steady`, `RestartMidApply`, `NewBrokerJoins`} | `Ok` |

`Persist = Transient` changes no outcome; it changes what is asserted (a transient topic exists and is empty after `RestartMidApply`; a transient view re-folds).

### Cell counts (pinned by `CatalogMatrixCountRegression.CellCountsArePinned`, Task 20)

9 kinds × 4 entities × 9 timings = **324 cells**: **300 applicable**, 24 `NotApplicable` (R1: 6 kinds × 4 entities). Applicable by rule: R2 `HaltedBrokerInternalOthersOk` 36, R3 `NotLeaderUntilBudgetThenOk` 36, R4 `NotLeaderThenOkOnce` 36, R5 `OneWinsOneConflict` 36, R6 `OkNewObjectId` 36, R7 `OkAfterPlacement` 12, R8 `RedirectThenOk` 54, R9 `Ok` 54 = **300**.

### Every cell asserts

1. The exact first answer (template + code, `Redirect` endpoint = the leader's client endpoint) and the final client-visible result.
2. **Every DDL answered exactly once** (`AdminWorker::startedCount() == answeredCount()` on every broker; the client saw one terminal result per `requestId`).
3. **Identical catalogs:** every running, non-halted broker reaches the same `CatalogState::digest()` at the same applied offset.
4. **No DDL lost or duplicated:** each `requestId` that was answered `Ok` appears exactly once in the log; each refused one appears zero times.
5. **History:** the shared library's `history()` over the log shows the op with the right `kind`, `objectId`, `prevVersion → newVersion`, `statement`, `principal.requestId`.
6. **Serving (C1 generalized):** a topic serves on a broker iff its `CreateTopic` is applied there and its placement is committed.

### Coverage by layer

| Layer | Task | Cells |
|---|---|---|
| Unit, parameterized (`TEST_P` over all 324; N/A skipped with its rule) — `CatalogClusterHarness`: 3 AdminWorkers + replicas over `FilteringTransport`, real applier, fake control/gateway | 20 | all 300 applicable |
| Seeded model-checked stress (independent `CatalogModel`) | 21 | random sequences of all 9 kinds with leader moves, restarts, joins, minIsr loss, concurrent sends and tampered fingerprints; 64 seeds × 400 steps |
| Integration slice — 3 real BrokerNodes over Aeron | 22 | {`CreateTopic`, `CreateView`, `DropTopic`, `AlterSchema`} × {`Steady` via broker 1, 2 and 3, `LeaderChangeMidDdl` (leader killed), `RestartMidApply`, `NewBrokerJoins`, `DropAndReuseName`, `CreateRacesPlacement`} = 32 cells + produce/consume through every broker after each |
| Docker e2e (3 nodes, real client + `rune` CLI) | 24 | DDL through each broker; leader killed mid-DDL; `export` → `replay` into a fresh cluster → `diff` empty |

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-codec/include/rune/catalog/CatalogEvent.h`, `src/catalog/CatalogEvent.cpp` | event model, kinds, v1 fixed-layout encode/decode | 1 |
| `rune-codec/include/rune/catalog/CatalogState.h`, `src/catalog/CatalogState.cpp`, `src/catalog/ApplyV1.cpp`, `include/rune/catalog/CatalogFingerprint.h`, `src/catalog/CatalogFingerprint.cpp` | state, frozen v1 rules, fingerprints, digest | 2 |
| `rune-codec/include/rune/catalog/CatalogReplayer.h`, `CatalogQueries.h`, `CatalogFile.h` + `src/catalog/*.cpp`, `tools/make_catalog_golden.cpp`, `tests/catalog/golden/v1/*.rcat`, `benchmark/CatalogReplayBench.cpp` | replayer, history / as-of / diff, `.rcat`, golden logs, replay bench | 3 |
| `rune-codec/CMakeLists.txt`, `tests/catalog/*Test.cpp` | `rune_catalog` lib + `rune_catalog_tests` | 1–3 |
| `rune-protocol/include/rune/protocol/client/Protocol.h`, `SchemaMessages.h`, `tests/protocol/client/CatalogDdlMessagesTest.cpp`, `conformance/*` | templates 74–78, DDL extension, DefineTopic rf/config, `kCatalogTopicId`, client version 8 | 4 |
| `rune-client-cpp/include/rune/client/Client.h`, `src/Client.cpp`, `include/rune/client/codec/Codec.h`, `tests/client/ClientDdlRedirectRegressionTest.cpp`, `tests/client/ClientCatalogDdlTest.cpp` | new DDL ops, DDL options, Redirect / NotLeader / InvalidRequest handling for control ops, request ids | 5 |
| `libs/node/include/rune/node/CatalogPartition.h`, `AdminIsrChannel.h`, `libs/node/src/BrokerNode.cpp` (`buildCatalog_`), `MetadataService.h`, `NodeConfig.{h,cpp}` (`catalogReplStreamId`), `libs/node/tests/integration/CatalogReplicationTest.cpp` | `__catalog` partition, admin replication plane, controller-ISR placement, role/ISR relays | 6 |
| `libs/node/include/rune/node/CatalogApplier.h`, `CatalogCheckpoint.h`, `libs/node/src/CatalogApplier.cpp`, `libs/node/tests/node/CatalogApplierTest.cpp` | apply in offset order, verify, halt, checkpoint, projections, effects | 7 |
| `libs/gateway/include/rune/gateway/TopicRegistry.h`, `libs/gateway/src/TopicRegistry.cpp`, `libs/node/src/BrokerNode.cpp` (control drain), `libs/node/include/rune/node/AdminChannels.h`, `LogCleanerAgent.h`, `WorkerViewSet.{h,cpp}`, `libs/metadata/include/rune/metadata/MetadataExtensions.h`, `MetadataStateMachine.cpp`, `libs/node/tests/node/CatalogEffectsTest.cpp`, `libs/gateway/tests/gateway/CatalogGatewayTest.cpp` | control-thread effects, drop/purge, config override, view uninstall, placement add/remove | 8 |
| `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp`, `AdminChannels.h`, `DdlAnswerGate.h`, `AdminCreateTopic.h` (deleted), `libs/gateway/include/rune/gateway/IAdminRequestSink.h`, `ClientGateway.{h,cpp}`, `TopicRegistry.{h,cpp}`, `libs/node/tests/support/CatalogDdlRig.h`, `libs/node/tests/node/CatalogWritePathTest.cpp` + ported DDL tests | write path through the catalog leader; new DDL ops; rf bounds; dedupe | 9 |
| `libs/node/tests/support/{DdlHarness,DdlExpectations}.h`, `libs/node/tests/node/Ddl*Test.cpp`, `libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp` | DDL conformance suite ported to the catalog | 10 |
| `libs/gateway/include/rune/gateway/CatalogRoute.h`, `ClientGateway.cpp`, `AdminWorker.cpp`, `BrokerNode.cpp`, `libs/node/tests/node/CatalogLeadershipTest.cpp` | Redirect, NotLeader, catch-up gate, minIsr, halted | 11 |
| `libs/node/include/rune/node/CatalogReadServer.h`, `libs/node/src/CatalogReadServer.cpp`, `ClientGateway.cpp`, `libs/node/tests/node/CatalogReadServerTest.cpp` | `__catalog` Subscribe served by the admin thread | 12 |
| `rune-client-cpp/include/rune/client/catalog/CatalogReader.h`, `src/catalog/CatalogReader.cpp`, `TopicHandle.h`, `Subscription.h`, `tests/catalog/CatalogReaderTest.cpp`, `CMakeLists.txt` | history / as-of / diff client API | 13 |
| `rune-ql/include/rune/ql/{Ast,Parser,Backends}.h`, `src/{Parser,Backends}.cpp`, `tests/CatalogStatementsTest.cpp` | `SHOW HISTORY`, `AS OF`, `DIFF`, `DROP`, `ALTER … SET`, `replicas`, `with (…)`, untyped topics | 14 |
| `rune-client-cpp/src/cli/Cli.cpp`, `src/cli/CatalogCommands.cpp`, `include/rune/cli/CatalogCommands.h`, `include/rune/client/view/ViewPlanAuthor.h`, `tests/RuneCliCatalogTest.cpp`, `tests/support/CatalogFakes.h` | `rune ql`, `rune catalog export|replay|diff` | 15 |
| `libs/node/src/BrokerNode.cpp`, `RuntimeTopicRecovery.{h,cpp}` (deleted), `TopicDirReconcile.{h,cpp}` (new), `SchemaRegistry.{h,cpp}`, `ViewRegistry.{h,cpp}`, `AdminWorker.{h,cpp}`, `libs/node/tests/node/CatalogBootTest.cpp` | boot from the catalog; retire `__topic_schemas`/`__views`/recovery | 16 |
| `NodeConfig.{h,cpp}`, `libs/node/tests/support/TestCluster.h`, `libs/node/tests/node/{BrokerNode,BrokerNodeGateway,NodeConfig,DdlHarnessRegression,TestCluster}Test.cpp`, `benchmark/FusedRuntimePassBench.cpp` | delete `RUNE_TOPICS`/`topics`/`rf=`; `TestCluster::defineTopic`; node tests migrated | 17 |
| the 21 `libs/node/tests/integration/*Test.cpp` listed in Task 18, `tests/integration/ClusterHarness.h` | integration tests create topics by DDL | 18 |
| `docker/*`, `docker/topics/*.rql`, `docker/create-topics.sh`, `dr-e2e/**`, `bench/kafka-compare/*`, `docs/**`, `scripts/check_no_rune_topics.py`, `CMakeLists.txt` | scripts migrated to DDL; grep gate | 19 |
| `libs/node/tests/support/{CatalogMatrixExpectations,CatalogClusterHarness}.h`, `libs/replication/include/rune/replication/testing/FilteringTransport.h`, `libs/node/tests/node/CatalogInteractionMatrixRegressionTest.cpp` | matrix table + parameterized unit matrix | 20 |
| `libs/node/tests/support/CatalogModel.h`, `libs/node/tests/node/CatalogStressRegressionTest.cpp` | seeded model-checked stress | 21 |
| `libs/node/tests/integration/CatalogClusterIntegrationTest.cpp` | 3-node BrokerNode slice | 22 |
| `benchmark/CatalogBootReplayBench.cpp`, `libs/node/tests/node/CatalogGatesRegressionTest.cpp` | 10k boot replay < 1 s, hot-path parity, idle zero-alloc | 23 |
| `docker/catalog-e2e.sh`, `docker/docker-compose.catalog.yml`, `docker/topics/*.rql` | docker e2e | 24 |
| `docs/SELF-HOSTING.md`, `docs/CATALOG.md`, `docs/release-notes/catalog-history.md` | docs | 25 |

---
### Task 1: `rune_catalog` — the event model and its frozen v1 encoding (rune-codec)

Spec §4: one fixed-layout, versioned record per event. This task defines the in-memory event (`CatalogEvent`), the nine kinds of §2 plus the reserved tenant/ACL range, and the `formatVer = 1` wire layout (encode + bounds-checked decode). Nothing else in the plan encodes or decodes an event by hand.

**Files:**
- Create: `rune-codec/include/rune/catalog/CatalogEvent.h`
- Create: `rune-codec/src/catalog/Wire.h` (internal LE writer / reader)
- Create: `rune-codec/src/catalog/CatalogEvent.cpp`
- Modify: `rune-codec/CMakeLists.txt` (new `rune_catalog` library + `rune_catalog_tests`)
- Test: `rune-codec/tests/catalog/CatalogEventTest.cpp`

**Interfaces:**
- Consumes: `rune::codec::CompatMode` (`include/rune/codec/AlterOps.h`).
- Produces (namespace `rune::catalog`) — used by every later task:
  ```cpp
  enum class EventKind : uint16_t { CreateTopic = 1, DropTopic = 2, AlterTopicConfig = 3,
      CreateSchema = 4, AlterSchema = 5, SetCompatibility = 6, CreateView = 7, AlterView = 8, DropView = 9 };
  inline constexpr uint16_t kFormatV1 = 1, kCurrentFormatVer = kFormatV1;
  inline constexpr uint16_t kReservedKindFirst = 32, kReservedKindLast = 63;   // tenants + ACLs
  enum class Persistence : uint8_t { Disk = 0, Transient = 1 };
  struct TopicConfig;  struct SchemaDef;  struct ViewDef;  enum class ConfigKey : uint8_t;  struct ConfigChange;
  struct CreateTopicOps; struct DropTopicOps; struct AlterTopicConfigOps; struct CreateSchemaOps;
  struct AlterSchemaOps; struct SetCompatibilityOps; struct CreateViewOps; struct AlterViewOps; struct DropViewOps;
  using Ops = std::variant<CreateTopicOps, DropTopicOps, AlterTopicConfigOps, CreateSchemaOps, AlterSchemaOps,
                           SetCompatibilityOps, CreateViewOps, AlterViewOps, DropViewOps>;
  struct Principal; struct ReplayedFrom; struct CatalogEvent;
  enum class DecodeError : uint8_t { Truncated, UnknownFormatVer, ReservedKind, UnknownKind, KindOpsMismatch, BadEnum, TrailingBytes };
  [[nodiscard]] EventKind kindOf(const Ops&) noexcept;
  void encodeInto(const CatalogEvent&, std::vector<uint8_t>& out);              // clears + reuses capacity
  [[nodiscard]] std::vector<uint8_t> encode(const CatalogEvent&);
  [[nodiscard]] std::expected<CatalogEvent, DecodeError> decode(std::span<const uint8_t>) noexcept;
  [[nodiscard]] std::string_view kindName(EventKind) noexcept;
  [[nodiscard]] std::string_view decodeErrorName(DecodeError) noexcept;
  ```

- [ ] **Step 1: Write the failing tests** — create `rune-codec/tests/catalog/CatalogEventTest.cpp`:

```cpp
// Catalog history (spec §4): the frozen formatVer-1 event encoding.
#include <rune/catalog/CatalogEvent.h>
#include <gtest/gtest.h>

namespace rc = rune::catalog;

namespace {
rc::CatalogEvent baseEvent(rc::Ops ops) {
    rc::CatalogEvent e;
    e.kind = rc::kindOf(ops);
    e.objectId = 7;
    e.objectName = "players";
    e.prevVersion = 0;
    e.newVersion = 1;
    e.statement = "topic players { id int64 } partitions 3 key id;";
    e.ops = std::move(ops);
    e.principal = rc::Principal{.sessionId = 42, .clientName = "cli", .sourceAddress = "aeron:udp?endpoint=10.0.0.5:40500",
                                .authenticated = "", .requestId = 0xABCDEFull};
    e.timestampMs = 1'760'000'000'000;
    e.fingerprint = 0x1122334455667788ull;
    return e;
}
rc::CreateTopicOps typedCreate() {
    rc::CreateTopicOps o;
    o.topicId = 3; o.partitions = 3; o.replicationFactor = 3; o.persistence = rc::Persistence::Disk;
    o.config.retentionMs = 86'400'000;
    o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = {1, 2, 3, 4}, .nextTag = 1};
    return o;
}
rc::ViewDef viewDef() {
    rc::ViewDef v;
    v.viewTopicId = 9; v.sinkPartitions = 1; v.persistence = rc::Persistence::Transient; v.keyField = "id";
    v.sourceNames = {"players"};
    v.normalizedPlanBlob = {9, 9}; v.compiledPlanBlob = {8, 8, 8}; v.outputDescriptorBlob = {7};
    return v;
}
std::vector<rc::Ops> everyKind() {
    return {typedCreate(), rc::DropTopicOps{.topicId = 3},
            rc::AlterTopicConfigOps{.changes = {{rc::ConfigKey::RetentionMs, 1000}, {rc::ConfigKey::RetentionBytes, -1}}},
            rc::CreateSchemaOps{.schema = {.keyField = "id", .validate = false, .descriptorBlob = {5, 6}, .nextTag = 2}},
            rc::AlterSchemaOps{.alterOpsBlob = {1, 0, 2}},
            rc::SetCompatibilityOps{.mode = rune::codec::CompatMode::Forward},
            rc::CreateViewOps{.view = viewDef()}, rc::AlterViewOps{.view = viewDef()}, rc::DropViewOps{.viewTopicId = 9}};
}
} // namespace

TEST(CatalogEventRegression, EveryKindRoundTripsByteExactly) {
    for (const rc::Ops& ops : everyKind()) {
        rc::CatalogEvent e = baseEvent(ops);
        e.replayedFrom = rc::ReplayedFrom{.clusterId = "src-cluster", .offset = 12, .principal = "cli@10.0.0.9"};
        const std::vector<uint8_t> bytes = rc::encode(e);
        auto d = rc::decode(bytes);
        ASSERT_TRUE(d.has_value()) << rc::kindName(e.kind) << ": " << rc::decodeErrorName(d.error());
        EXPECT_EQ(rc::encode(*d), bytes) << rc::kindName(e.kind);
        EXPECT_EQ(d->kind, e.kind);
        EXPECT_EQ(d->ops.index(), e.ops.index());
        EXPECT_EQ(d->principal.requestId, 0xABCDEFull);
        ASSERT_TRUE(d->replayedFrom.has_value());
        EXPECT_EQ(d->replayedFrom->offset, 12);
    }
}

TEST(CatalogEventRegression, HeaderLayoutIsFrozen) {
    // formatVer u16 LE, kind u16 LE, objectId u64 LE, prevVersion u32, newVersion u32,
    // timestampMs i64, fingerprint u64 — the first 36 bytes, in this order, forever.
    const auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 3}));
    ASSERT_GE(b.size(), 36u);
    EXPECT_EQ(b[0], 1); EXPECT_EQ(b[1], 0);                 // formatVer 1
    EXPECT_EQ(b[2], 2); EXPECT_EQ(b[3], 0);                 // DropTopic
    EXPECT_EQ(b[4], 7);                                      // objectId low byte
    EXPECT_EQ(b[12], 0); EXPECT_EQ(b[16], 1);                // prev 0, new 1
    EXPECT_EQ(b[28], 0x88); EXPECT_EQ(b[35], 0x11);          // fingerprint LE
}

TEST(CatalogEventRegression, EveryTruncationIsRefusedNeverMisread) {
    for (const rc::Ops& ops : everyKind()) {
        const auto b = rc::encode(baseEvent(ops));
        for (std::size_t n = 0; n < b.size(); ++n) {
            auto d = rc::decode(std::span<const uint8_t>(b.data(), n));
            ASSERT_FALSE(d.has_value()) << "prefix " << n << " of " << b.size() << " decoded";
            EXPECT_EQ(d.error(), rc::DecodeError::Truncated);
        }
    }
}

TEST(CatalogEventRegression, TrailingBytesAreRefused) {
    auto b = rc::encode(baseEvent(rc::DropViewOps{.viewTopicId = 1}));
    b.push_back(0);
    auto d = rc::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::TrailingBytes);
}

TEST(CatalogEventRegression, UnknownFormatVersionIsAnErrorNotASkip) {
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1}));
    b[0] = 2;   // formatVer 2 does not exist yet
    auto d = rc::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::UnknownFormatVer);
}

TEST(CatalogEventRegression, ReservedTenantAndAclKindsAreRefusedByV1) {
    for (uint16_t k : {uint16_t{32}, uint16_t{40}, uint16_t{63}}) {
        auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1}));
        b[2] = static_cast<uint8_t>(k & 0xff); b[3] = static_cast<uint8_t>(k >> 8);
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value());
        EXPECT_EQ(d.error(), rc::DecodeError::ReservedKind) << k;
    }
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1}));
    b[2] = 10; b[3] = 0;
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::UnknownKind);
}

TEST(CatalogEventRegression, KindDisagreeingWithOpsIsRefused) {
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1}));
    b[2] = 9;   // claims DropView; the ops blob is a DropTopic (same 4-byte shape) — the ops tag catches it
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::KindOpsMismatch);
}

TEST(CatalogEventRegression, BadEnumBytesAreRefused) {
    rc::CreateTopicOps o = typedCreate();
    auto b = rc::encode(baseEvent(o));
    // The CreateTopic body is the event's tail; persistence follows topicId(4) partitions(4) rf(2).
    const std::size_t bodyStart = b.size() - rc::detail_testing::createTopicOpsSize(o);
    b[bodyStart + 10] = 7;
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::BadEnum);
}

TEST(CatalogEventRegression, EncodeIntoReusesCapacity) {
    std::vector<uint8_t> buf;
    buf.reserve(4096);
    const auto* data = buf.data();
    rc::encodeInto(baseEvent(typedCreate()), buf);
    EXPECT_EQ(buf.data(), data) << "encodeInto must not reallocate a buffer that fits";
}
```

(`rc::detail_testing::createTopicOpsSize` is a test-only helper declared in `CatalogEvent.h` under `namespace detail_testing`: the encoded size of a `CreateTopicOps` body without its tag byte.)

- [ ] **Step 2: Add the build targets** — in `rune-codec/CMakeLists.txt`, after the `rune_codec` library block (anchor: `add_library(rune_codec STATIC`), add:

```cmake
# ── rune_catalog — the shared catalog-history library (spec §6): event model,
#    frozen per-formatVer apply rules, fingerprints, replayer, queries, .rcat.
#    Linked by the broker (AdminWorker), the client and the rune CLI.
add_library(rune_catalog STATIC
    src/catalog/CatalogEvent.cpp)
target_include_directories(rune_catalog PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(rune_catalog PUBLIC rune_codec)
target_compile_features(rune_catalog PUBLIC cxx_std_23)
```

and after `gtest_discover_tests(rune_codec_tests)`:

```cmake
add_executable(rune_catalog_tests
    tests/catalog/CatalogEventTest.cpp)
target_link_libraries(rune_catalog_tests PRIVATE rune_catalog GTest::gtest_main)
gtest_discover_tests(rune_catalog_tests)
```

- [ ] **Step 3: Run to verify they fail**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-codec/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests`
Expected: compile error `'rune/catalog/CatalogEvent.h' file not found`.

- [ ] **Step 4: Implement `CatalogEvent.h`**

```cpp
#pragma once
// rune/catalog/CatalogEvent.h — Catalog history (spec §4): one event per catalog
// change, stored as one record in __catalog.  The formatVer-1 layout below is
// FROZEN: a change ships under a new formatVer, and old events keep decoding
// and applying under their own rules (spec §6).
//
// formatVer-1 layout (all little-endian):
//   u16 formatVer | u16 kind | u64 objectId | u32 prevVersion | u32 newVersion |
//   i64 timestampMs | u64 fingerprint |
//   str16 objectName | str32 statement |
//   principal: i64 sessionId, str16 clientName, str16 sourceAddress,
//              str16 authenticated, u64 requestId |
//   u8 hasReplayedFrom [str16 clusterId, i64 offset, str16 principal] |
//   bytes32 ops  = u8 opsTag (== kind) + the kind's body (see CatalogEvent.cpp)
// str16 = u16 length + bytes; str32/bytes32 = u32 length + bytes.
#include <rune/codec/AlterOps.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::catalog {

enum class EventKind : uint16_t {
    CreateTopic = 1, DropTopic = 2, AlterTopicConfig = 3,
    CreateSchema = 4, AlterSchema = 5, SetCompatibility = 6,
    CreateView = 7, AlterView = 8, DropView = 9,
};
inline constexpr uint16_t kFormatV1          = 1;
inline constexpr uint16_t kCurrentFormatVer  = kFormatV1;
/// Reserved for the multi-tenancy design (tenants + ACLs).  formatVer 1 refuses them.
inline constexpr uint16_t kReservedKindFirst = 32;
inline constexpr uint16_t kReservedKindLast  = 63;

enum class Persistence : uint8_t { Disk = 0, Transient = 1 };

/// Every TopicSpec property a CreateTopic fixes.  Defaults = today's TopicSpec defaults.
struct TopicConfig {
    uint8_t     cleanupPolicy             = 0;            // 0 Delete, 1 Compact, 2 CompactDelete
    int64_t     retentionMs               = -1;
    int64_t     retentionBytes            = -1;
    int64_t     deleteRetentionMs         = 86'400'000;
    int32_t     minCleanableDirtyRatioPpm = 500'000;      // 0.5
    int64_t     segmentRollBytes          = 64LL * 1024 * 1024;
    int64_t     minCompactionLagMs        = 0;
    int64_t     maxCompactionLagMs        = -1;
    int64_t     retentionCheckMs          = 300'000;
    int64_t     fileDeleteDelayMs         = 60'000;
    uint8_t     durability                = 0;            // 0 Majority, 1 AllInSync
    uint8_t     durabilityTier            = 1;            // 0 Leader, 1 Majority, 2 Strict
    std::string coPartitionGroup;
    std::string partitionKey;
    bool operator==(const TopicConfig&) const = default;
};

/// AlterTopicConfig keys (ruling 5).  Append-only.
enum class ConfigKey : uint8_t {
    RetentionMs = 1, RetentionBytes = 2, DeleteRetentionMs = 3,
    MinCleanableDirtyRatioPpm = 4, MinCompactionLagMs = 5, MaxCompactionLagMs = 6,
};
struct ConfigChange { ConfigKey key; int64_t value; bool operator==(const ConfigChange&) const = default; };

struct SchemaDef {
    std::string          keyField;
    bool                 validate = false;
    std::vector<uint8_t> descriptorBlob;     // rune::codec::serialize(Descriptor) — the initial EVOLVING blob
    uint16_t             nextTag  = 0;       // = descriptor field count
    bool operator==(const SchemaDef&) const = default;
};

struct ViewDef {
    int32_t                  viewTopicId    = -1;   // the view's sink topic id (dense, from the catalog)
    int32_t                  sinkPartitions = 1;
    Persistence              persistence    = Persistence::Disk;
    std::string              keyField;
    std::vector<std::string> sourceNames;           // resolved source topic NAMES, in plan order
    std::vector<uint8_t>     normalizedPlanBlob;    // authored plan, source topicId := ordinal (ruling 11)
    std::vector<uint8_t>     compiledPlanBlob;      // rune::view::serialize(CompiledViewPlan) — embeds ids, NOT fingerprinted
    std::vector<uint8_t>     outputDescriptorBlob;  // rune::codec::serialize(output Descriptor)
    bool operator==(const ViewDef&) const = default;
};

struct CreateTopicOps {
    int32_t                  topicId           = -1;    // dense id, from CatalogState::nextTopicId()
    int32_t                  partitions        = 1;
    int16_t                  replicationFactor = 0;     // 0 = every broker
    Persistence              persistence       = Persistence::Disk;
    TopicConfig              config;
    std::optional<SchemaDef> schema;                    // nullopt = untyped topic
};
struct DropTopicOps        { int32_t topicId = -1; };
struct AlterTopicConfigOps { std::vector<ConfigChange> changes; };
struct CreateSchemaOps     { SchemaDef schema; };
struct AlterSchemaOps      { std::vector<uint8_t> alterOpsBlob; };   // rune::codec::encodeAlterOps
struct SetCompatibilityOps { rune::codec::CompatMode mode = rune::codec::CompatMode::Backward; };
struct CreateViewOps       { ViewDef view; };
struct AlterViewOps        { ViewDef view; };                          // a FRESH sink (ruling 4)
struct DropViewOps         { int32_t viewTopicId = -1; };

using Ops = std::variant<CreateTopicOps, DropTopicOps, AlterTopicConfigOps, CreateSchemaOps,
                         AlterSchemaOps, SetCompatibilityOps, CreateViewOps, AlterViewOps, DropViewOps>;

/// Until authentication exists: session id, client name and source address.  Afterwards
/// `authenticated` carries the principal (same field, no layout change).  `requestId` is
/// the client's id of the logical DDL, stable across retries (ruling 9).
struct Principal {
    int64_t     sessionId = 0;
    std::string clientName;
    std::string sourceAddress;
    std::string authenticated;
    uint64_t    requestId = 0;
};
/// Set only on events written by `rune catalog replay` (spec §7).
struct ReplayedFrom {
    std::string clusterId;
    int64_t     offset = -1;
    std::string principal;
};

struct CatalogEvent {
    uint16_t                    formatVer   = kCurrentFormatVer;
    EventKind                   kind        = EventKind::CreateTopic;
    uint64_t                    objectId    = 0;
    std::string                 objectName;
    uint32_t                    prevVersion = 0;
    uint32_t                    newVersion  = 0;
    std::string                 statement;
    Ops                         ops;
    Principal                   principal;
    std::optional<ReplayedFrom> replayedFrom;
    int64_t                     timestampMs = 0;   // informational; the OFFSET is the order
    uint64_t                    fingerprint = 0;   // of the object's resulting state
};

enum class DecodeError : uint8_t {
    Truncated, UnknownFormatVer, ReservedKind, UnknownKind, KindOpsMismatch, BadEnum, TrailingBytes,
};

[[nodiscard]] EventKind kindOf(const Ops& ops) noexcept;
void encodeInto(const CatalogEvent& e, std::vector<uint8_t>& out);
[[nodiscard]] std::vector<uint8_t> encode(const CatalogEvent& e);
[[nodiscard]] std::expected<CatalogEvent, DecodeError> decode(std::span<const uint8_t> bytes) noexcept;
[[nodiscard]] std::string_view kindName(EventKind k) noexcept;
[[nodiscard]] std::string_view decodeErrorName(DecodeError e) noexcept;

namespace detail_testing {
/// Encoded size of a CreateTopicOps body (no tag byte).  Test-only.
[[nodiscard]] std::size_t createTopicOpsSize(const CreateTopicOps& o);
}

} // namespace rune::catalog
```

- [ ] **Step 5: Implement `src/catalog/Wire.h`**

```cpp
#pragma once
// Internal little-endian writer/reader for rune_catalog encodings.  The reader
// never reads past its span: every get returns false on truncation.
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace rune::catalog::wire {

class Writer {
public:
    explicit Writer(std::vector<uint8_t>& out) noexcept : out_(out) {}
    void u8(uint8_t v)   { out_.push_back(v); }
    void u16(uint16_t v) { raw(&v, 2); }
    void i16(int16_t v)  { raw(&v, 2); }
    void u32(uint32_t v) { raw(&v, 4); }
    void i32(int32_t v)  { raw(&v, 4); }
    void u64(uint64_t v) { raw(&v, 8); }
    void i64(int64_t v)  { raw(&v, 8); }
    void str16(std::string_view s) { u16(static_cast<uint16_t>(s.size())); raw(s.data(), s.size()); }
    void str32(std::string_view s) { u32(static_cast<uint32_t>(s.size())); raw(s.data(), s.size()); }
    void bytes32(std::span<const uint8_t> b) { u32(static_cast<uint32_t>(b.size())); raw(b.data(), b.size()); }
    [[nodiscard]] std::size_t size() const noexcept { return out_.size(); }
    /// Patch a u32 written earlier (length back-fill).
    void patchU32(std::size_t at, uint32_t v) noexcept { std::memcpy(out_.data() + at, &v, 4); }
private:
    void raw(const void* p, std::size_t n) {
        const auto* b = static_cast<const uint8_t*>(p);
        out_.insert(out_.end(), b, b + n);
    }
    std::vector<uint8_t>& out_;
};

class Reader {
public:
    explicit Reader(std::span<const uint8_t> in) noexcept : in_(in) {}
    bool u8(uint8_t& v) noexcept   { return raw(&v, 1); }
    bool u16(uint16_t& v) noexcept { return raw(&v, 2); }
    bool i16(int16_t& v) noexcept  { return raw(&v, 2); }
    bool u32(uint32_t& v) noexcept { return raw(&v, 4); }
    bool i32(int32_t& v) noexcept  { return raw(&v, 4); }
    bool u64(uint64_t& v) noexcept { return raw(&v, 8); }
    bool i64(int64_t& v) noexcept  { return raw(&v, 8); }
    bool str16(std::string& s) { uint16_t n; if (!u16(n) || rem() < n) return false; take(s, n); return true; }
    bool str32(std::string& s) { uint32_t n; if (!u32(n) || rem() < n) return false; take(s, n); return true; }
    bool bytes32(std::vector<uint8_t>& b) {
        uint32_t n; if (!u32(n) || rem() < n) return false;
        b.assign(in_.data() + pos_, in_.data() + pos_ + n); pos_ += n; return true;
    }
    /// A sub-reader over the next n bytes (advances past them).
    bool sub(uint32_t n, Reader& out) noexcept {
        if (rem() < n) return false;
        out = Reader(in_.subspan(pos_, n)); pos_ += n; return true;
    }
    [[nodiscard]] std::size_t rem() const noexcept { return in_.size() - pos_; }
private:
    bool raw(void* p, std::size_t n) noexcept {
        if (rem() < n) return false;
        std::memcpy(p, in_.data() + pos_, n); pos_ += n; return true;
    }
    void take(std::string& s, std::size_t n) {
        s.assign(reinterpret_cast<const char*>(in_.data() + pos_), n); pos_ += n;
    }
    std::span<const uint8_t> in_;
    std::size_t pos_ = 0;
};

} // namespace rune::catalog::wire
```

- [ ] **Step 6: Implement `src/catalog/CatalogEvent.cpp`** — the per-kind bodies, in this frozen order:

```cpp
#include <rune/catalog/CatalogEvent.h>
#include "Wire.h"

namespace rune::catalog {
namespace {

using wire::Reader;
using wire::Writer;

// ── body writers (frozen, formatVer 1) ─────────────────────────────────────
void putConfig(Writer& w, const TopicConfig& c) {
    w.u8(c.cleanupPolicy); w.i64(c.retentionMs); w.i64(c.retentionBytes); w.i64(c.deleteRetentionMs);
    w.i32(c.minCleanableDirtyRatioPpm); w.i64(c.segmentRollBytes); w.i64(c.minCompactionLagMs);
    w.i64(c.maxCompactionLagMs); w.i64(c.retentionCheckMs); w.i64(c.fileDeleteDelayMs);
    w.u8(c.durability); w.u8(c.durabilityTier); w.str16(c.coPartitionGroup); w.str16(c.partitionKey);
}
void putSchema(Writer& w, const SchemaDef& s) {
    w.str16(s.keyField); w.u8(s.validate ? 1 : 0); w.bytes32(s.descriptorBlob); w.u16(s.nextTag);
}
void putView(Writer& w, const ViewDef& v) {
    w.i32(v.viewTopicId); w.i32(v.sinkPartitions); w.u8(static_cast<uint8_t>(v.persistence));
    w.str16(v.keyField);
    w.u16(static_cast<uint16_t>(v.sourceNames.size()));
    for (const auto& n : v.sourceNames) w.str16(n);
    w.bytes32(v.normalizedPlanBlob); w.bytes32(v.compiledPlanBlob); w.bytes32(v.outputDescriptorBlob);
}
void putOps(Writer& w, const Ops& ops) {
    w.u8(static_cast<uint8_t>(kindOf(ops)));   // ops tag == kind (catches a kind/ops mix-up)
    std::visit([&](const auto& o) {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, CreateTopicOps>) {
            w.i32(o.topicId); w.i32(o.partitions); w.i16(o.replicationFactor);
            w.u8(static_cast<uint8_t>(o.persistence)); putConfig(w, o.config);
            w.u8(o.schema ? 1 : 0);
            if (o.schema) putSchema(w, *o.schema);
        } else if constexpr (std::is_same_v<T, DropTopicOps>) {
            w.i32(o.topicId);
        } else if constexpr (std::is_same_v<T, AlterTopicConfigOps>) {
            w.u16(static_cast<uint16_t>(o.changes.size()));
            for (const auto& c : o.changes) { w.u8(static_cast<uint8_t>(c.key)); w.i64(c.value); }
        } else if constexpr (std::is_same_v<T, CreateSchemaOps>) {
            putSchema(w, o.schema);
        } else if constexpr (std::is_same_v<T, AlterSchemaOps>) {
            w.bytes32(o.alterOpsBlob);
        } else if constexpr (std::is_same_v<T, SetCompatibilityOps>) {
            w.u8(static_cast<uint8_t>(o.mode));
        } else if constexpr (std::is_same_v<T, CreateViewOps> || std::is_same_v<T, AlterViewOps>) {
            putView(w, o.view);
        } else {   // DropViewOps
            w.i32(o.viewTopicId);
        }
    }, ops);
}

// ── body readers ────────────────────────────────────────────────────────────
// Each returns Truncated / BadEnum explicitly; an enum byte is never cast unchecked.
std::optional<DecodeError> getPersistence(Reader& r, Persistence& p) {
    uint8_t b; if (!r.u8(b)) return DecodeError::Truncated;
    if (b > 1) return DecodeError::BadEnum;
    p = static_cast<Persistence>(b); return std::nullopt;
}
std::optional<DecodeError> getConfig(Reader& r, TopicConfig& c) {
    if (!r.u8(c.cleanupPolicy)) return DecodeError::Truncated;
    if (c.cleanupPolicy > 2) return DecodeError::BadEnum;
    if (!r.i64(c.retentionMs) || !r.i64(c.retentionBytes) || !r.i64(c.deleteRetentionMs) ||
        !r.i32(c.minCleanableDirtyRatioPpm) || !r.i64(c.segmentRollBytes) || !r.i64(c.minCompactionLagMs) ||
        !r.i64(c.maxCompactionLagMs) || !r.i64(c.retentionCheckMs) || !r.i64(c.fileDeleteDelayMs) ||
        !r.u8(c.durability) || !r.u8(c.durabilityTier) || !r.str16(c.coPartitionGroup) || !r.str16(c.partitionKey))
        return DecodeError::Truncated;
    if (c.durability > 1 || c.durabilityTier > 2) return DecodeError::BadEnum;
    return std::nullopt;
}
std::optional<DecodeError> getSchema(Reader& r, SchemaDef& s) {
    uint8_t v;
    if (!r.str16(s.keyField) || !r.u8(v) || !r.bytes32(s.descriptorBlob) || !r.u16(s.nextTag))
        return DecodeError::Truncated;
    if (v > 1) return DecodeError::BadEnum;
    s.validate = v == 1; return std::nullopt;
}
std::optional<DecodeError> getView(Reader& r, ViewDef& v) {
    if (!r.i32(v.viewTopicId) || !r.i32(v.sinkPartitions)) return DecodeError::Truncated;
    if (auto e = getPersistence(r, v.persistence)) return e;
    uint16_t n;
    if (!r.str16(v.keyField) || !r.u16(n)) return DecodeError::Truncated;
    v.sourceNames.resize(n);
    for (auto& s : v.sourceNames) if (!r.str16(s)) return DecodeError::Truncated;
    if (!r.bytes32(v.normalizedPlanBlob) || !r.bytes32(v.compiledPlanBlob) || !r.bytes32(v.outputDescriptorBlob))
        return DecodeError::Truncated;
    return std::nullopt;
}
std::expected<Ops, DecodeError> getOps(Reader& r, EventKind kind) {
    uint8_t tag; if (!r.u8(tag)) return std::unexpected(DecodeError::Truncated);
    if (tag != static_cast<uint8_t>(kind)) return std::unexpected(DecodeError::KindOpsMismatch);
    switch (kind) {
    case EventKind::CreateTopic: {
        CreateTopicOps o;
        if (!r.i32(o.topicId) || !r.i32(o.partitions) || !r.i16(o.replicationFactor))
            return std::unexpected(DecodeError::Truncated);
        if (auto e = getPersistence(r, o.persistence)) return std::unexpected(*e);
        if (auto e = getConfig(r, o.config)) return std::unexpected(*e);
        uint8_t has; if (!r.u8(has)) return std::unexpected(DecodeError::Truncated);
        if (has > 1) return std::unexpected(DecodeError::BadEnum);
        if (has) { SchemaDef s; if (auto e = getSchema(r, s)) return std::unexpected(*e); o.schema = std::move(s); }
        return o;
    }
    case EventKind::DropTopic: { DropTopicOps o; if (!r.i32(o.topicId)) return std::unexpected(DecodeError::Truncated); return o; }
    case EventKind::AlterTopicConfig: {
        AlterTopicConfigOps o; uint16_t n; if (!r.u16(n)) return std::unexpected(DecodeError::Truncated);
        o.changes.resize(n);
        for (auto& c : o.changes) {
            uint8_t k; if (!r.u8(k) || !r.i64(c.value)) return std::unexpected(DecodeError::Truncated);
            if (k < 1 || k > 6) return std::unexpected(DecodeError::BadEnum);
            c.key = static_cast<ConfigKey>(k);
        }
        return o;
    }
    case EventKind::CreateSchema: { CreateSchemaOps o; if (auto e = getSchema(r, o.schema)) return std::unexpected(*e); return o; }
    case EventKind::AlterSchema: { AlterSchemaOps o; if (!r.bytes32(o.alterOpsBlob)) return std::unexpected(DecodeError::Truncated); return o; }
    case EventKind::SetCompatibility: {
        uint8_t m; if (!r.u8(m)) return std::unexpected(DecodeError::Truncated);
        if (m > 1) return std::unexpected(DecodeError::BadEnum);
        return SetCompatibilityOps{static_cast<rune::codec::CompatMode>(m)};
    }
    case EventKind::CreateView: { CreateViewOps o; if (auto e = getView(r, o.view)) return std::unexpected(*e); return o; }
    case EventKind::AlterView:  { AlterViewOps o;  if (auto e = getView(r, o.view)) return std::unexpected(*e); return o; }
    case EventKind::DropView:   { DropViewOps o; if (!r.i32(o.viewTopicId)) return std::unexpected(DecodeError::Truncated); return o; }
    }
    return std::unexpected(DecodeError::UnknownKind);
}

} // namespace

EventKind kindOf(const Ops& ops) noexcept {
    return static_cast<EventKind>(ops.index() + 1);   // variant order == kind order (1..9)
}

void encodeInto(const CatalogEvent& e, std::vector<uint8_t>& out) {
    out.clear();
    Writer w(out);
    w.u16(e.formatVer); w.u16(static_cast<uint16_t>(e.kind)); w.u64(e.objectId);
    w.u32(e.prevVersion); w.u32(e.newVersion); w.i64(e.timestampMs); w.u64(e.fingerprint);
    w.str16(e.objectName); w.str32(e.statement);
    w.i64(e.principal.sessionId); w.str16(e.principal.clientName); w.str16(e.principal.sourceAddress);
    w.str16(e.principal.authenticated); w.u64(e.principal.requestId);
    w.u8(e.replayedFrom ? 1 : 0);
    if (e.replayedFrom) { w.str16(e.replayedFrom->clusterId); w.i64(e.replayedFrom->offset); w.str16(e.replayedFrom->principal); }
    const std::size_t lenAt = w.size();
    w.u32(0);
    putOps(w, e.ops);
    w.patchU32(lenAt, static_cast<uint32_t>(w.size() - lenAt - 4));
}

std::vector<uint8_t> encode(const CatalogEvent& e) { std::vector<uint8_t> b; encodeInto(e, b); return b; }

std::expected<CatalogEvent, DecodeError> decode(std::span<const uint8_t> bytes) noexcept {
    try {
        Reader r(bytes);
        CatalogEvent e;
        uint16_t kind;
        if (!r.u16(e.formatVer)) return std::unexpected(DecodeError::Truncated);
        if (e.formatVer != kFormatV1) return std::unexpected(DecodeError::UnknownFormatVer);
        if (!r.u16(kind)) return std::unexpected(DecodeError::Truncated);
        if (kind >= kReservedKindFirst && kind <= kReservedKindLast) return std::unexpected(DecodeError::ReservedKind);
        if (kind < 1 || kind > 9) return std::unexpected(DecodeError::UnknownKind);
        e.kind = static_cast<EventKind>(kind);
        if (!r.u64(e.objectId) || !r.u32(e.prevVersion) || !r.u32(e.newVersion) || !r.i64(e.timestampMs) ||
            !r.u64(e.fingerprint) || !r.str16(e.objectName) || !r.str32(e.statement) ||
            !r.i64(e.principal.sessionId) || !r.str16(e.principal.clientName) ||
            !r.str16(e.principal.sourceAddress) || !r.str16(e.principal.authenticated) || !r.u64(e.principal.requestId))
            return std::unexpected(DecodeError::Truncated);
        uint8_t hasReplay; if (!r.u8(hasReplay)) return std::unexpected(DecodeError::Truncated);
        if (hasReplay > 1) return std::unexpected(DecodeError::BadEnum);
        if (hasReplay) {
            ReplayedFrom rf;
            if (!r.str16(rf.clusterId) || !r.i64(rf.offset) || !r.str16(rf.principal)) return std::unexpected(DecodeError::Truncated);
            e.replayedFrom = std::move(rf);
        }
        uint32_t opsLen; Reader ops(std::span<const uint8_t>{});
        if (!r.u32(opsLen) || !r.sub(opsLen, ops)) return std::unexpected(DecodeError::Truncated);
        auto o = getOps(ops, e.kind);
        if (!o) return std::unexpected(o.error());
        if (ops.rem() != 0 || r.rem() != 0) return std::unexpected(DecodeError::TrailingBytes);
        e.ops = std::move(*o);
        return e;
    } catch (const std::bad_alloc&) {
        return std::unexpected(DecodeError::Truncated);   // a length the process cannot hold is never trusted
    }
}

std::string_view kindName(EventKind k) noexcept {
    switch (k) {
    case EventKind::CreateTopic: return "CreateTopic";       case EventKind::DropTopic: return "DropTopic";
    case EventKind::AlterTopicConfig: return "AlterTopicConfig"; case EventKind::CreateSchema: return "CreateSchema";
    case EventKind::AlterSchema: return "AlterSchema";       case EventKind::SetCompatibility: return "SetCompatibility";
    case EventKind::CreateView: return "CreateView";         case EventKind::AlterView: return "AlterView";
    case EventKind::DropView: return "DropView";
    }
    return "?";
}
std::string_view decodeErrorName(DecodeError e) noexcept {
    switch (e) {
    case DecodeError::Truncated: return "Truncated"; case DecodeError::UnknownFormatVer: return "UnknownFormatVer";
    case DecodeError::ReservedKind: return "ReservedKind"; case DecodeError::UnknownKind: return "UnknownKind";
    case DecodeError::KindOpsMismatch: return "KindOpsMismatch"; case DecodeError::BadEnum: return "BadEnum";
    case DecodeError::TrailingBytes: return "TrailingBytes";
    }
    return "?";
}

namespace detail_testing {
std::size_t createTopicOpsSize(const CreateTopicOps& o) {
    std::vector<uint8_t> b; Writer w(b); putOps(w, Ops{o}); return b.size() - 1;
}
}

} // namespace rune::catalog
```

Note the truncation test: a prefix that cuts inside the length-prefixed ops blob fails `sub()` → `Truncated`; a prefix that ends exactly after a complete header but before `opsLen` also fails → `Truncated`; `TrailingBytes` is only reachable with a complete event, so every strict prefix is `Truncated`.

- [ ] **Step 7: Run the tests**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-codec/build -R 'CatalogEvent' --output-on-failure -j14`
Expected: all 8 `CatalogEventRegression.*` PASS.

- [ ] **Step 8: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-codec add include/rune/catalog/CatalogEvent.h src/catalog/Wire.h src/catalog/CatalogEvent.cpp tests/catalog/CatalogEventTest.cpp CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-codec commit -m "feat(catalog): event model + frozen formatVer-1 encoding (catalog history §4)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `rune_catalog` — catalog state, the frozen v1 apply rules, fingerprints

Spec §5.3 / §6: applying an event = the rules of its `formatVer`, then verifying the fingerprint of the resulting object. The state is a pure function of the applied prefix. Apply is atomic: on any error the state is unchanged.

**Files:**
- Create: `rune-codec/include/rune/catalog/CatalogState.h`, `rune-codec/src/catalog/CatalogState.cpp`
- Create: `rune-codec/include/rune/catalog/CatalogFingerprint.h`, `rune-codec/src/catalog/CatalogFingerprint.cpp`
- Create: `rune-codec/src/catalog/ApplyV1.h`, `rune-codec/src/catalog/ApplyV1.cpp`
- Modify: `rune-codec/CMakeLists.txt` (add the three `.cpp` to `rune_catalog`, the test to `rune_catalog_tests`)
- Test: `rune-codec/tests/catalog/CatalogApplyV1Test.cpp`

**Interfaces:**
- Consumes: Task 1's `CatalogEvent`, ops structs, `kindOf`; `rune::codec::{deserialize, serialize, decodeAlterOps, applyOps, materializeAsOf, fingerprint}`.
- Produces (namespace `rune::catalog`):
  ```cpp
  struct SchemaState { std::string keyField; bool validate; std::vector<uint8_t> evolvingBlob;
                       uint16_t schemaVersion; uint16_t nextTag; rune::codec::CompatMode compat; };
  struct TopicObject { uint64_t objectId; std::string name; uint32_t version; int32_t topicId; int32_t partitions;
                       int16_t replicationFactor; Persistence persistence; TopicConfig config;
                       std::optional<SchemaState> schema; };
  struct ViewObject  { uint64_t objectId; std::string name; uint32_t version; ViewDef def; };
  struct Tombstone   { uint64_t objectId; std::string name; uint32_t finalVersion; bool wasView; };
  struct Applied     { EventKind kind; uint64_t objectId; uint32_t newVersion; uint64_t fingerprint;
                       int32_t createdTopicId; int32_t droppedTopicId; };   // -1 when none
  struct RequestRecord { int64_t offset; EventKind kind; uint64_t objectId; uint32_t newVersion; int32_t topicId; uint16_t schemaVersion; };
  struct ApplyError  { int64_t offset; enum class Code : uint8_t { UnsupportedFormat, Precondition,
                       FingerprintMismatch, BadOps } code; uint64_t expected; uint64_t actual; std::string detail; };
  class CatalogState { /* see Step 3 */ };
  [[nodiscard]] std::expected<Applied, ApplyError> apply(CatalogState&, const CatalogEvent&, int64_t offset);
  [[nodiscard]] std::expected<uint64_t, ApplyError> previewFingerprint(const CatalogState&, const CatalogEvent&);
  [[nodiscard]] uint64_t fingerprintOf(const TopicObject&) noexcept;
  [[nodiscard]] uint64_t fingerprintOf(const ViewObject&) noexcept;
  [[nodiscard]] uint64_t fingerprintOf(const Tombstone&) noexcept;
  [[nodiscard]] std::string_view applyErrorName(ApplyError::Code) noexcept;
  ```

**The frozen v1 apply rules** (in `ApplyV1.cpp`; every rule failing is `ApplyError::Precondition` naming the offset):

| Kind | Precondition | Effect on the state |
|---|---|---|
| `CreateTopic` | `objectName` free (no live topic or view), `objectId == nextObjectId`, `ops.topicId == nextTopicId`, `prevVersion == 0`, `newVersion == 1`, `partitions ≥ 1` | new `TopicObject` (version 1); `schema` → `SchemaState{evolvingBlob = descriptorBlob, schemaVersion = 1, nextTag, compat = Backward}`; `nextObjectId++`, `nextTopicId++` |
| `DropTopic` | live topic with that `objectId`, name, `topicId`; `prevVersion == version`; `newVersion == version + 1`; no live view lists it in `sourceNames` | topic removed; `Tombstone{finalVersion = newVersion}` recorded; its name is free |
| `AlterTopicConfig` | live topic; versions chain; `changes` non-empty, no key twice, `MinCleanableDirtyRatioPpm ∈ [0, 1'000'000]`, retention values `≥ -1` | the keys set; version + 1 |
| `CreateSchema` | live **untyped** topic; versions chain | `schema` set (schemaVersion 1); version + 1 |
| `AlterSchema` | live typed topic; versions chain; `decodeAlterOps` succeeds; `applyOps(evolving, ops, nextTag, schemaVersion+1)` succeeds | evolving blob replaced, `nextTag` advanced, `schemaVersion + 1`, a `SET COMPATIBILITY` op inside sets `compat`, a RENAME of the key field moves `keyField`; version + 1 |
| `SetCompatibility` | live typed topic; versions chain | `compat = mode`, `schemaVersion + 1` (a compat change is a schema version, as today's `startAlter_`); version + 1 |
| `CreateView` | name free, `objectId == nextObjectId`, `view.viewTopicId == nextTopicId`, every `sourceNames[i]` a live topic, `prevVersion 0 → 1` | new `ViewObject`; `nextObjectId++`, `nextTopicId++` |
| `AlterView` | live view; versions chain; `view.viewTopicId == nextTopicId` (a fresh sink, ruling 4); sources live | `def` replaced; version + 1; `nextTopicId++`; `Applied.droppedTopicId` = old sink id, `createdTopicId` = new |
| `DropView` | live view with that `viewTopicId`; versions chain | view removed; tombstone; `droppedTopicId` = its sink |

Fingerprints (`CatalogFingerprint.cpp`, FNV-1a-64 via `rune::codec::detail::fnv1a_*`, every variable-length field length-prefixed — ruling 11: **never** an id): a topic hashes `tag 'T'`, name, version, partitions, rf, persistence, every `TopicConfig` field in declaration order, then `hasSchema` and (keyField, validate, evolvingBlob bytes, schemaVersion, nextTag, compat); a view hashes `tag 'V'`, name, version, sinkPartitions, persistence, keyField, each source name, `normalizedPlanBlob`, `rune::codec::fingerprint(deserialize(outputDescriptorBlob))` (0 when the blob does not deserialize — the apply rule rejects that event before); a tombstone hashes `tag 'D'`, `wasView`, name, finalVersion. `CatalogState::digest()` = FNV over (appliedOffset, nextObjectId, nextTopicId, then each live object's fingerprint in `objectId` order, then each tombstone's) — "identical fingerprint at the same applied offset" in the stress and integration tests compares digests.

- [ ] **Step 1: Write the failing tests** — create `rune-codec/tests/catalog/CatalogApplyV1Test.cpp`:

```cpp
// Catalog history (spec §5.3, §6): the frozen formatVer-1 apply rules + fingerprints.
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <gtest/gtest.h>

namespace rc = rune::catalog;
namespace cd = rune::codec;

namespace {
std::vector<uint8_t> blob2() {   // {id int64 key, hp int32}
    cd::DescriptorBuilder b("players");
    b.add("id", cd::PrimType::Int64, /*sinceVersion=*/1, /*isKey=*/true);
    b.add("hp", cd::PrimType::Int32);
    return cd::serialize(b.build());
}

/// The leader's way of building an event: fill everything, then stamp the
/// fingerprint the library previews — exactly what AdminWorker does (Task 9).
rc::CatalogEvent stamped(const rc::CatalogState& s, rc::Ops ops, std::string name, uint64_t objectId,
                         uint32_t prev, uint32_t next) {
    rc::CatalogEvent e;
    e.kind = rc::kindOf(ops); e.ops = std::move(ops); e.objectName = std::move(name);
    e.objectId = objectId; e.prevVersion = prev; e.newVersion = next;
    auto fp = rc::previewFingerprint(s, e);
    EXPECT_TRUE(fp.has_value()) << (fp ? "" : fp.error().detail);
    e.fingerprint = fp.value_or(0);
    return e;
}
rc::CreateTopicOps createOps(int32_t topicId, bool typed) {
    rc::CreateTopicOps o; o.topicId = topicId; o.partitions = 2; o.replicationFactor = 3;
    if (typed) o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = blob2(), .nextTag = 2};
    return o;
}
} // namespace

TEST(CatalogApplyV1Regression, CreateTopicAssignsDenseIdsAndVersionOne) {
    rc::CatalogState s;
    auto e = stamped(s, createOps(0, true), "players", 1, 0, 1);
    auto a = rc::apply(s, e, 0);
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    const rc::TopicObject* t = s.topicByName("players");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->topicId, 0); EXPECT_EQ(t->version, 1u); EXPECT_EQ(t->schema->schemaVersion, 1);
    EXPECT_EQ(s.nextObjectId(), 2u); EXPECT_EQ(s.nextTopicId(), 1); EXPECT_EQ(s.appliedOffset(), 0);
    EXPECT_EQ(a->createdTopicId, 0);
}

TEST(CatalogApplyV1Regression, FingerprintMismatchIsAnErrorAndLeavesStateUnchanged) {
    rc::CatalogState s;
    auto e = stamped(s, createOps(0, false), "t", 1, 0, 1);
    e.fingerprint ^= 1;
    const uint64_t before = s.digest();
    auto a = rc::apply(s, e, 5);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(a.error().offset, 5);
    EXPECT_EQ(a.error().expected, e.fingerprint);
    EXPECT_NE(a.error().actual, e.fingerprint);
    EXPECT_EQ(s.digest(), before);
    EXPECT_EQ(s.topicByName("t"), nullptr);
}

TEST(CatalogApplyV1Regression, EveryPreconditionIsExplicit) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, false), "t", 1, 0, 1), 0));
    struct Case { const char* what; rc::CatalogEvent e; };
    std::vector<Case> cases;
    { auto e = stamped(s, createOps(1, false), "t", 2, 0, 1); cases.push_back({"name taken", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = createOps(7, false); e.objectName = "u";
      e.objectId = 2; e.newVersion = 1; cases.push_back({"topicId != nextTopicId", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = createOps(1, false); e.objectName = "u";
      e.objectId = 9; e.newVersion = 1; cases.push_back({"objectId != nextObjectId", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::DropTopic; e.ops = rc::DropTopicOps{0}; e.objectName = "t";
      e.objectId = 1; e.prevVersion = 3; e.newVersion = 4; cases.push_back({"version chain broken", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::AlterSchema; e.ops = rc::AlterSchemaOps{{1}}; e.objectName = "t";
      e.objectId = 1; e.prevVersion = 1; e.newVersion = 2; cases.push_back({"alter of an untyped topic", e}); }
    for (auto& c : cases) {
        const uint64_t before = s.digest();
        auto a = rc::apply(s, c.e, 1);
        ASSERT_FALSE(a.has_value()) << c.what;
        EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition) << c.what;
        EXPECT_EQ(s.digest(), before) << c.what;
    }
}

TEST(CatalogApplyV1Regression, SchemaLifecycleOnOneObject) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, false), "t", 1, 0, 1), 0));            // untyped
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::CreateSchemaOps{{.keyField = "id", .validate = false,
        .descriptorBlob = blob2(), .nextTag = 2}}, "t", 1, 1, 2), 1));                     // CreateSchema
    cd::AlterOp add{}; add.type = cd::AlterOpType::Add; add.name = "mana"; add.primType = cd::PrimType::Int32;
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::AlterSchemaOps{cd::encodeAlterOps({add})}, "t", 1, 2, 3), 2));
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::SetCompatibilityOps{cd::CompatMode::Forward}, "t", 1, 3, 4), 3));
    const rc::TopicObject* t = s.topicByName("t");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->objectId, 1u); EXPECT_EQ(t->version, 4u);
    EXPECT_EQ(t->schema->schemaVersion, 3);   // create 1, alter 2, set-compat 3
    EXPECT_EQ(t->schema->compat, cd::CompatMode::Forward);
    EXPECT_EQ(t->schema->nextTag, 3);
}

TEST(CatalogApplyV1Regression, DropAndReuseGetsANewObjectIdAndTopicId) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, false), "t", 1, 0, 1), 0));
    auto d = rc::apply(s, stamped(s, rc::DropTopicOps{0}, "t", 1, 1, 2), 1);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->droppedTopicId, 0);
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(1, false), "t", 2, 0, 1), 2));
    EXPECT_EQ(s.topicByName("t")->objectId, 2u);
    EXPECT_EQ(s.topicByName("t")->topicId, 1);
    ASSERT_EQ(s.tombstones().size(), 1u);
    EXPECT_EQ(s.tombstones()[0].objectId, 1u);
}

TEST(CatalogApplyV1Regression, DropTopicReadByAViewIsRefused) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "src", 1, 0, 1), 0));
    rc::ViewDef v; v.viewTopicId = 1; v.keyField = "id"; v.sourceNames = {"src"};
    v.outputDescriptorBlob = blob2(); v.normalizedPlanBlob = {1};
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::CreateViewOps{v}, "v", 2, 0, 1), 1));
    rc::CatalogEvent e; e.kind = rc::EventKind::DropTopic; e.ops = rc::DropTopicOps{0};
    e.objectName = "src"; e.objectId = 1; e.prevVersion = 1; e.newVersion = 2;
    auto a = rc::apply(s, e, 2);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
}

TEST(CatalogApplyV1Regression, AlterViewTakesAFreshSinkAndReportsBoth) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "src", 1, 0, 1), 0));
    rc::ViewDef v; v.viewTopicId = 1; v.keyField = "id"; v.sourceNames = {"src"};
    v.outputDescriptorBlob = blob2(); v.normalizedPlanBlob = {1};
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::CreateViewOps{v}, "v", 2, 0, 1), 1));
    rc::ViewDef v2 = v; v2.viewTopicId = 2; v2.normalizedPlanBlob = {2};
    auto a = rc::apply(s, stamped(s, rc::AlterViewOps{v2}, "v", 2, 1, 2), 2);
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    EXPECT_EQ(a->createdTopicId, 2); EXPECT_EQ(a->droppedTopicId, 1);
    EXPECT_EQ(s.viewByName("v")->def.viewTopicId, 2);
    EXPECT_EQ(s.viewByViewTopicId(1), nullptr);
}

TEST(CatalogApplyV1Regression, FingerprintsIgnoreIdsSoClustersCompare) {
    rc::CatalogState a, b;
    ASSERT_TRUE(rc::apply(a, stamped(a, createOps(0, true), "x", 1, 0, 1), 0));
    // Cluster b already holds another topic: same definition, different ids.
    ASSERT_TRUE(rc::apply(b, stamped(b, createOps(0, false), "other", 1, 0, 1), 0));
    ASSERT_TRUE(rc::apply(b, stamped(b, createOps(1, true), "x", 2, 0, 1), 1));
    EXPECT_EQ(rc::fingerprintOf(*a.topicByName("x")), rc::fingerprintOf(*b.topicByName("x")));
}

TEST(CatalogApplyV1Regression, RequestIdsAreRecordedAndADuplicateIsRefused) {
    rc::CatalogState s;
    auto e = stamped(s, createOps(0, true), "x", 1, 0, 1);
    e.principal.requestId = 77;
    ASSERT_TRUE(rc::apply(s, e, 0));
    auto r = s.request(77);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->offset, 0); EXPECT_EQ(r->objectId, 1u); EXPECT_EQ(r->topicId, 0); EXPECT_EQ(r->schemaVersion, 1);
    auto again = stamped(s, createOps(1, false), "y", 2, 0, 1);
    again.principal.requestId = 77;
    auto a = rc::apply(s, again, 1);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
    EXPECT_FALSE(s.request(0).has_value()) << "requestId 0 (pre-v8 clients) is never recorded";
}

TEST(CatalogApplyV1Regression, FingerprintCoversEveryDefinitionField) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "x", 1, 0, 1), 0));
    rc::TopicObject t = *s.topicByName("x");
    const uint64_t base = rc::fingerprintOf(t);
    auto differs = [&](auto mutate) { rc::TopicObject m = t; mutate(m); return rc::fingerprintOf(m) != base; };
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.name = "y"; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.version = 2; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.partitions = 3; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.replicationFactor = 1; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.persistence = rc::Persistence::Transient; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.config.retentionMs = 5; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.config.partitionKey = "k"; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.schema->compat = cd::CompatMode::Forward; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.schema->evolvingBlob.push_back(0); }));
    EXPECT_FALSE(differs([](rc::TopicObject& m) { m.objectId = 99; m.topicId = 42; }));
}
```

(`DescriptorBuilder::add(name, type, sinceVersion, isKey)` / `build()` and `AlterOp{type, name, newName, primType, fixedLen, mode}` are rune-codec's existing APIs, `include/rune/codec/Descriptor.h:78`, `AlterOps.h:22`.)

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests`
Expected: compile error `'rune/catalog/CatalogState.h' file not found`.

- [ ] **Step 3: Implement `CatalogState.h`**

```cpp
#pragma once
// rune/catalog/CatalogState.h — the catalog as a pure function of the applied
// __catalog prefix (spec §3: "A broker's in-memory catalog is purely a function of
// the __catalog prefix it has applied").  Single-threaded; owned by whoever replays
// (the broker's admin thread, a client, the CLI).  Mutated ONLY by apply().
#include <rune/catalog/CatalogEvent.h>
#include <rune/codec/AlterOps.h>

#include <cstdint>
#include <expected>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::catalog {

struct SchemaState {
    std::string             keyField;
    bool                    validate      = false;
    std::vector<uint8_t>    evolvingBlob;
    uint16_t                schemaVersion = 1;
    uint16_t                nextTag       = 0;
    rune::codec::CompatMode compat        = rune::codec::CompatMode::Backward;
};
struct TopicObject {
    uint64_t                   objectId          = 0;
    std::string                name;
    uint32_t                   version           = 0;
    int32_t                    topicId           = -1;
    int32_t                    partitions        = 1;
    int16_t                    replicationFactor = 0;
    Persistence                persistence       = Persistence::Disk;
    TopicConfig                config;
    std::optional<SchemaState> schema;
};
struct ViewObject {
    uint64_t    objectId = 0;
    std::string name;
    uint32_t    version  = 0;
    ViewDef     def;
};
struct Tombstone {
    uint64_t    objectId     = 0;
    std::string name;
    uint32_t    finalVersion = 0;
    bool        wasView      = false;
};
struct Applied {
    EventKind kind;
    uint64_t  objectId       = 0;
    uint32_t  newVersion     = 0;
    uint64_t  fingerprint    = 0;
    int32_t   createdTopicId = -1;   // CreateTopic / CreateView / AlterView: the new (sink) topic id
    int32_t   droppedTopicId = -1;   // DropTopic / DropView / AlterView: the retired (sink) topic id
};
/// What an applied DDL answered — replayed to a retry with the same requestId (ruling 9).
struct RequestRecord {
    int64_t   offset        = -1;
    EventKind kind          = EventKind::CreateTopic;
    uint64_t  objectId      = 0;
    uint32_t  newVersion    = 0;
    int32_t   topicId       = -1;   // the topic / view sink id the answer names
    uint16_t  schemaVersion = 0;    // topic kinds with a schema: the resulting schema version
};
struct ApplyError {
    enum class Code : uint8_t { UnsupportedFormat, Precondition, FingerprintMismatch, BadOps };
    int64_t     offset   = -1;
    Code        code     = Code::Precondition;
    uint64_t    expected = 0;    // FingerprintMismatch: the event's fingerprint
    uint64_t    actual   = 0;    // FingerprintMismatch: the recomputed one
    std::string detail;
};

class CatalogState {
public:
    [[nodiscard]] const TopicObject* topicByName(std::string_view n) const;
    [[nodiscard]] const TopicObject* topicByObjectId(uint64_t id) const;
    [[nodiscard]] const TopicObject* topicByTopicId(int32_t topicId) const;
    [[nodiscard]] const ViewObject*  viewByName(std::string_view n) const;
    [[nodiscard]] const ViewObject*  viewByObjectId(uint64_t id) const;
    [[nodiscard]] const ViewObject*  viewByViewTopicId(int32_t topicId) const;
    /// A live topic or view owns this name.
    [[nodiscard]] bool nameTaken(std::string_view n) const { return topicByName(n) || viewByName(n); }
    /// A live view whose sources include `topicName` (nullptr: none).
    [[nodiscard]] const ViewObject* viewReading(std::string_view topicName) const;
    /// Live objects in objectId order.
    [[nodiscard]] std::vector<const TopicObject*> topics() const;
    [[nodiscard]] std::vector<const ViewObject*>  views() const;
    [[nodiscard]] const std::vector<Tombstone>&  tombstones() const noexcept { return tombstones_; }

    [[nodiscard]] uint64_t nextObjectId() const noexcept { return nextObjectId_; }
    [[nodiscard]] int32_t  nextTopicId()  const noexcept { return nextTopicId_; }
    [[nodiscard]] int64_t  appliedOffset() const noexcept { return appliedOffset_; }   // -1 = nothing applied
    /// The applied event carrying `requestId` (ruling 9) — what its DDL answered.  Cold.
    [[nodiscard]] std::optional<RequestRecord> request(uint64_t requestId) const;
    /// FNV over the whole state (see Task 2 text).  Equal digests at equal offsets = identical catalogs.
    [[nodiscard]] uint64_t digest() const noexcept;

private:
    friend std::expected<Applied, ApplyError> apply(CatalogState&, const CatalogEvent&, int64_t);
    friend struct ApplyV1;   // src/catalog/ApplyV1.cpp
    std::map<uint64_t, TopicObject>             topics_;        // objectId → object (ordered: digest order)
    std::map<uint64_t, ViewObject>              views_;
    std::unordered_map<std::string, uint64_t>   topicNames_;
    std::unordered_map<std::string, uint64_t>   viewNames_;
    std::unordered_map<int32_t, uint64_t>       topicIds_;      // topicId → topic objectId
    std::unordered_map<int32_t, uint64_t>       viewTopicIds_;  // sink topicId → view objectId
    std::unordered_map<uint64_t, RequestRecord> requests_;      // requestId → its applied event (0 never stored)
    std::vector<Tombstone>                      tombstones_;
    uint64_t nextObjectId_  = 1;
    int32_t  nextTopicId_   = 0;
    int64_t  appliedOffset_ = -1;
};

/// Apply `e` (at log `offset`) under the rules of e.formatVer, verify its fingerprint,
/// and commit it.  Atomic: on any error the state is unchanged.
[[nodiscard]] std::expected<Applied, ApplyError> apply(CatalogState& s, const CatalogEvent& e, int64_t offset);
/// The fingerprint `e` WOULD produce on `s` (the leader stamps it).  Never mutates `s`;
/// fails exactly where apply() would (except FingerprintMismatch).
[[nodiscard]] std::expected<uint64_t, ApplyError> previewFingerprint(const CatalogState& s, const CatalogEvent& e);
[[nodiscard]] std::string_view applyErrorName(ApplyError::Code c) noexcept;

} // namespace rune::catalog
```

- [ ] **Step 4: Implement `ApplyV1.cpp`** — the rule table above, as one `struct ApplyV1` with a static `run(const CatalogState&, const CatalogEvent&, int64_t offset) → std::expected<Staged, ApplyError>` that builds a **staged change** without touching the state, and `commit(CatalogState&, Staged&&)` that installs it:

```cpp
// src/catalog/ApplyV1.h
#pragma once
#include <rune/catalog/CatalogState.h>
#include <variant>
namespace rune::catalog {
/// A validated, not-yet-committed change.  Exactly one of these per event kind family.
struct StagedPut      { std::optional<TopicObject> topic; std::optional<ViewObject> view;
                        bool newObject = false; int32_t retiredSinkTopicId = -1; };
struct StagedRemove   { uint64_t objectId; bool isView; Tombstone tomb; int32_t droppedTopicId; };
using Staged = std::variant<StagedPut, StagedRemove>;
struct ApplyV1 {
    static std::expected<Staged, ApplyError> run(const CatalogState& s, const CatalogEvent& e, int64_t offset);
    static uint64_t fingerprintOfStaged(const Staged& st) noexcept;
    static Applied commit(CatalogState& s, const CatalogEvent& e, int64_t offset, Staged&& st);
};
}
```

`run` implements each row (a `Precondition` error's `detail` names the rule, e.g. `"CreateTopic: name 'players' is taken"`, `"AlterSchema: version chain 3 -> 5, object at 4"`). `AlterSchema` runs `rune::codec::decodeAlterOps` (nullopt → `BadOps`), then `rune::codec::applyOps(deserialize(evolvingBlob), ops, nextTag, schemaVersion + 1)` inside a `try` (a throw or an `EvolveError` → `BadOps` with the message); the key field follows a RENAME exactly like `AdminWorker::startAlter_` (anchor `DDL conformance B8 / D3a`). `commit` updates the maps, counters (`nextObjectId_` / `nextTopicId_` exactly as the table says), `appliedOffset_ = offset`, and `requests_[e.principal.requestId] = RequestRecord{…}` when the id is non-zero (a requestId seen twice in a log is a `Precondition` error in `run` — the leader never writes one twice).

`apply()` in `CatalogState.cpp`:

```cpp
std::expected<Applied, ApplyError> apply(CatalogState& s, const CatalogEvent& e, int64_t offset) {
    if (e.formatVer != kFormatV1)
        return std::unexpected(ApplyError{offset, ApplyError::Code::UnsupportedFormat, 0, 0,
                                          "formatVer " + std::to_string(e.formatVer) + " has no apply rules"});
    auto staged = ApplyV1::run(s, e, offset);
    if (!staged) return std::unexpected(std::move(staged.error()));
    const uint64_t actual = ApplyV1::fingerprintOfStaged(*staged);
    if (actual != e.fingerprint)
        return std::unexpected(ApplyError{offset, ApplyError::Code::FingerprintMismatch, e.fingerprint, actual,
                                          std::string(kindName(e.kind)) + " '" + e.objectName + "'"});
    return ApplyV1::commit(s, e, offset, std::move(*staged));
}
std::expected<uint64_t, ApplyError> previewFingerprint(const CatalogState& s, const CatalogEvent& e) {
    if (e.formatVer != kFormatV1)
        return std::unexpected(ApplyError{-1, ApplyError::Code::UnsupportedFormat, 0, 0, "formatVer"});
    auto staged = ApplyV1::run(s, e, -1);
    if (!staged) return std::unexpected(std::move(staged.error()));
    return ApplyV1::fingerprintOfStaged(*staged);
}
```

A future `formatVer` adds `ApplyV2` and a dispatch arm here; `ApplyV1` is never edited again (Global Constraint, spec §6).

- [ ] **Step 5: Implement `CatalogFingerprint.{h,cpp}`** — exactly the field order of the "Fingerprints" paragraph above, using `rune::codec::detail::fnv1a_byte/u16le/u32le` plus local `u64`/`bytes` helpers (`bytes` hashes a u32 LE length then each byte). `CatalogState::digest()` lives in `CatalogState.cpp` and uses the same helpers.

- [ ] **Step 6: Run the tests**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-codec/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-codec/build -R 'CatalogEvent|CatalogApplyV1' --output-on-failure -j14`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-codec add include/rune/catalog src/catalog tests/catalog CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-codec commit -m "feat(catalog): catalog state + frozen v1 apply rules + id-free fingerprints

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `rune_catalog` — the verifying replayer, history / as-of / diff, `.rcat` files, golden logs, replay benchmark

**Files:**
- Create: `rune-codec/include/rune/catalog/CatalogReplayer.h`, `src/catalog/CatalogReplayer.cpp`
- Create: `rune-codec/include/rune/catalog/CatalogQueries.h`, `src/catalog/CatalogQueries.cpp`
- Create: `rune-codec/include/rune/catalog/CatalogFile.h`, `src/catalog/CatalogFile.cpp`
- Create: `rune-codec/include/rune/catalog/testing/GoldenScript.h` (the deterministic event script the golden generator and the golden test share)
- Create: `rune-codec/tools/make_catalog_golden.cpp`
- Create: `rune-codec/tests/catalog/golden/v1/basic.rcat` (generated, checked in), `rune-codec/tests/catalog/CatalogGoldenPins.h` (pinned digests, checked in)
- Create: `rune-codec/benchmark/CatalogReplayBench.cpp`
- Modify: `rune-codec/CMakeLists.txt`
- Test: `rune-codec/tests/catalog/CatalogReplayerTest.cpp`, `CatalogQueriesTest.cpp`, `CatalogGoldenTest.cpp`

**Interfaces:**
- Consumes: Tasks 1–2.
- Produces (namespace `rune::catalog`):
  ```cpp
  struct RecordedEvent { int64_t offset; CatalogEvent event; };
  struct ReplayError { int64_t offset; enum class Code : uint8_t { Decode, Gap, Apply } code;
                       std::optional<DecodeError> decode; std::optional<ApplyError> apply; std::string detail; };
  [[nodiscard]] std::string describe(const ReplayError&);       // "offset 17: FingerprintMismatch expected=… actual=…"
  class CatalogReplayer {
  public:
      explicit CatalogReplayer(CatalogState start = {});        // a cached prefix resumes here
      std::expected<Applied, ReplayError> feed(int64_t offset, std::span<const uint8_t> record);
      std::expected<Applied, ReplayError> feedDecoded(int64_t offset, const CatalogEvent& e);
      [[nodiscard]] const CatalogState& state() const noexcept;
      [[nodiscard]] int64_t nextOffset() const noexcept;        // appliedOffset()+1
  };
  enum class ObjectType : uint8_t { Topic, View, Schema };
  struct HistoryFilter { std::optional<ObjectType> type; std::string name; int64_t sinceMs = INT64_MIN; };
  [[nodiscard]] std::vector<const RecordedEvent*> history(std::span<const RecordedEvent>, const HistoryFilter&);
  struct AsOf { enum class By : uint8_t { Offset, TimeMs, Version } by; int64_t value;
                ObjectType type = ObjectType::Topic; std::string name; };       // name/type only for Version
  [[nodiscard]] std::expected<CatalogState, ReplayError> stateAsOf(std::span<const RecordedEvent>, const AsOf&);
  struct DiffEntry { enum class Change : uint8_t { Added, Removed, Changed } change; ObjectType type;
                     std::string name; uint64_t fingerprintA; uint64_t fingerprintB; std::vector<std::string> fields; };
  [[nodiscard]] std::vector<DiffEntry> diff(const CatalogState& a, const CatalogState& b);
  // .rcat: "RCAT" magic, u16 file version 1, then per record: i64 offset, u32 len, bytes.
  void writeRcatHeader(std::ostream&);  void writeRcatRecord(std::ostream&, int64_t offset, std::span<const uint8_t>);
  [[nodiscard]] std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string> readRcat(std::istream&);
  ```

`history` semantics: `type = Topic` matches topic kinds (`CreateTopic`, `DropTopic`, `AlterTopicConfig`) and schema kinds of that topic; `Schema` matches `CreateSchema`/`AlterSchema`/`SetCompatibility` and a `CreateTopic` that carried a schema; `View` matches the three view kinds; `name` matches `objectName` (all objects ever named so — a drop-and-reuse shows both objects, distinguishable by `objectId`); `sinceMs` filters on `timestampMs ≥ sinceMs`. `stateAsOf`: `Offset o` replays offsets `≤ o`; `TimeMs t` replays events with `timestampMs ≤ t` **in offset order, stopping at the first later one** (time is informational, the offset is the order); `Version n` replays up to and including the event that produced object version `n` of the latest object named `name` of `type` (for `Schema`: the event that produced `schemaVersion == n`) — not found → `ReplayError{Code::Apply, detail "no version n of …"}`. `diff` pairs objects by (type, name): `Added` in b only, `Removed` in a only, `Changed` when fingerprints differ, with `fields` naming each differing top-level field (`partitions`, `replicationFactor`, `persistence`, `config.<field>`, `schema.version`, `schema.compat`, `schema.fields`, `view.plan`, `view.sources`, `view.output`).

- [ ] **Step 1: Write the failing tests** — `CatalogReplayerTest.cpp`:

```cpp
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <gtest/gtest.h>

namespace rc = rune::catalog;

TEST(CatalogReplayerRegression, ReplaysTheGoldenScriptAndVerifiesEveryFingerprint) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < events.size(); ++i) {
        auto a = r.feed(static_cast<int64_t>(i), rc::encode(events[i]));
        ASSERT_TRUE(a.has_value()) << rc::describe(a.error());
    }
    EXPECT_EQ(r.nextOffset(), static_cast<int64_t>(events.size()));
}

TEST(CatalogReplayerRegression, AGapIsAnErrorNamingTheOffset) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    ASSERT_TRUE(r.feed(0, rc::encode(events[0])));
    auto a = r.feed(2, rc::encode(events[1]));
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ReplayError::Code::Gap);
    EXPECT_EQ(a.error().offset, 2);
}

TEST(CatalogReplayerRegression, UnknownFormatVersionStopsAtItsOffsetRegression) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    ASSERT_TRUE(r.feed(0, rc::encode(events[0])));
    auto bytes = rc::encode(events[1]);
    bytes[0] = 9;
    auto a = r.feed(1, bytes);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ReplayError::Code::Decode);
    EXPECT_EQ(*a.error().decode, rc::DecodeError::UnknownFormatVer);
    EXPECT_EQ(a.error().offset, 1);
    EXPECT_EQ(r.nextOffset(), 1) << "nothing past the bad event is applied";
    EXPECT_NE(rc::describe(a.error()).find("offset 1"), std::string::npos);
}

TEST(CatalogReplayerRegression, TamperedFingerprintIsAnExplicitErrorNamingTheOffset) {
    auto events = rc::testing::goldenScriptV1();
    events[3].fingerprint ^= 0x10;
    rc::CatalogReplayer r;
    for (int64_t i = 0; i < 3; ++i) ASSERT_TRUE(r.feedDecoded(i, events[static_cast<std::size_t>(i)]));
    auto a = r.feedDecoded(3, events[3]);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().apply->code, rc::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(a.error().offset, 3);
}

TEST(CatalogReplayerRegression, ResumingFromACachedPrefixEqualsAFullReplay) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer full;
    for (std::size_t i = 0; i < events.size(); ++i) ASSERT_TRUE(full.feedDecoded(static_cast<int64_t>(i), events[i]));
    rc::CatalogReplayer head;
    const std::size_t mid = events.size() / 2;
    for (std::size_t i = 0; i < mid; ++i) ASSERT_TRUE(head.feedDecoded(static_cast<int64_t>(i), events[i]));
    rc::CatalogReplayer tail(head.state());
    for (std::size_t i = mid; i < events.size(); ++i) ASSERT_TRUE(tail.feedDecoded(static_cast<int64_t>(i), events[i]));
    EXPECT_EQ(tail.state().digest(), full.state().digest());
}
```

`GoldenScript.h` (in `include/rune/catalog/testing/`, header-only, deterministic: fixed timestamps `1'760'000'000'000 + 1000*i`, fixed principals, request ids `i+1`): builds **every kind at least twice** on a growing `CatalogState`, stamping each event with `previewFingerprint` — in order: CreateTopic `players` (typed, disk, rf 3); CreateTopic `events` (untyped, 4 partitions); CreateTopic `presence` (untyped, transient); CreateSchema on `events`; AlterSchema `players` (ADD `mana`); AlterSchema `players` (RENAME `hp`→`health`); SetCompatibility `players` forward; AlterTopicConfig `events` {retention.ms 60000}; AlterTopicConfig `events` {retention.bytes 1 GiB, min.cleanable.dirty.ratio 0.25}; CreateView `top_players` over `players`; AlterView `top_players`; CreateView `ev_view` over `events`; DropView `ev_view`; DropTopic `presence`; CreateTopic `presence` (reuse: new objectId, transient); DropView `top_players`; DropTopic `players`; CreateTopic `players` (reuse, untyped) — 18 events. `goldenScriptV1()` returns them as `std::vector<CatalogEvent>`.

`CatalogQueriesTest.cpp`:

```cpp
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <gtest/gtest.h>

namespace rc = rune::catalog;

namespace {
std::vector<rc::RecordedEvent> recorded() {
    std::vector<rc::RecordedEvent> out;
    int64_t o = 0;
    for (auto& e : rc::testing::goldenScriptV1()) out.push_back({o++, std::move(e)});
    return out;
}
}

TEST(CatalogQueriesRegression, HistoryOfAReusedNameShowsBothObjectsDistinctly) {
    const auto log = recorded();
    auto h = rc::history(log, {.type = rc::ObjectType::Topic, .name = "players"});
    ASSERT_FALSE(h.empty());
    std::set<uint64_t> ids;
    for (auto* r : h) ids.insert(r->event.objectId);
    EXPECT_EQ(ids.size(), 2u) << "drop + reuse = two objects, never conflated";
    EXPECT_EQ(h.front()->event.kind, rc::EventKind::CreateTopic);
}

TEST(CatalogQueriesRegression, SchemaHistoryIsTheSchemaKindsOfThatTopic) {
    const auto log = recorded();
    auto h = rc::history(log, {.type = rc::ObjectType::Schema, .name = "players"});
    // CreateTopic(with schema), AlterSchema x2, SetCompatibility — of the FIRST players.
    ASSERT_EQ(h.size(), 4u);
    EXPECT_EQ(h[3]->event.kind, rc::EventKind::SetCompatibility);
}

TEST(CatalogQueriesRegression, SinceFiltersOnTimestamp) {
    const auto log = recorded();
    auto h = rc::history(log, {.sinceMs = log[10].event.timestampMs});
    EXPECT_EQ(h.size(), log.size() - 10);
}

TEST(CatalogQueriesRegression, AsOfOffsetTimeAndVersionAgree) {
    const auto log = recorded();
    auto byOffset = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 5});
    auto byTime   = rc::stateAsOf(log, {.by = rc::AsOf::By::TimeMs, .value = log[5].event.timestampMs});
    auto byVer    = rc::stateAsOf(log, {.by = rc::AsOf::By::Version, .value = 3,
                                        .type = rc::ObjectType::Schema, .name = "players"});
    ASSERT_TRUE(byOffset && byTime && byVer);
    EXPECT_EQ(byOffset->digest(), byTime->digest());
    EXPECT_EQ(byOffset->digest(), byVer->digest()) << "event 5 produced players schema version 3";
    EXPECT_EQ(byOffset->topicByName("players")->schema->schemaVersion, 3);
}

TEST(CatalogQueriesRegression, AsOfAMissingVersionIsAnExplicitError) {
    const auto log = recorded();
    auto s = rc::stateAsOf(log, {.by = rc::AsOf::By::Version, .value = 99, .type = rc::ObjectType::Topic, .name = "events"});
    ASSERT_FALSE(s.has_value());
    EXPECT_NE(s.error().detail.find("no version 99"), std::string::npos);
}

TEST(CatalogQueriesRegression, DiffReportsAddedRemovedAndChangedFields) {
    const auto log = recorded();
    auto a = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 6});
    auto b = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 17});
    ASSERT_TRUE(a && b);
    auto d = rc::diff(*a, *b);
    auto find = [&](std::string_view n) { for (auto& e : d) if (e.name == n) return &e; return (const rc::DiffEntry*)nullptr; };
    ASSERT_NE(find("events"), nullptr);
    EXPECT_EQ(find("events")->change, rc::DiffEntry::Change::Changed);
    EXPECT_NE(std::find(find("events")->fields.begin(), find("events")->fields.end(), "config.retentionMs"),
              find("events")->fields.end());
    EXPECT_TRUE(rc::diff(*b, *b).empty());
}

TEST(CatalogQueriesRegression, RcatRoundTripsAndRefusesGarbage) {
    const auto log = recorded();
    std::stringstream ss;
    rc::writeRcatHeader(ss);
    for (const auto& r : log) rc::writeRcatRecord(ss, r.offset, rc::encode(r.event));
    auto back = rc::readRcat(ss);
    ASSERT_TRUE(back.has_value()) << back.error();
    ASSERT_EQ(back->size(), log.size());
    EXPECT_EQ((*back)[7].first, 7);
    std::stringstream bad("NOPE");
    EXPECT_FALSE(rc::readRcat(bad).has_value());
}
```

`CatalogGoldenTest.cpp`:

```cpp
// Spec §6 golden logs: a recorded formatVer-1 log, checked in, must replay to the
// SAME pinned digests forever.  A failure here means a v1 rule changed — ship the
// change under formatVer 2 instead (Global Constraints).
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogReplayer.h>
#include "CatalogGoldenPins.h"
#include <fstream>
#include <gtest/gtest.h>

TEST(CatalogGoldenRegression, V1LogsReplayToPinnedFingerprints) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v1/basic.rcat", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rune::catalog::readRcat(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rune::catalog::golden::kBasicV1Digests));
    rune::catalog::CatalogReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << rune::catalog::describe(a.error());
        EXPECT_EQ(r.state().digest(), rune::catalog::golden::kBasicV1Digests[i]) << "offset " << i;
    }
}

TEST(CatalogGoldenRegression, TheGoldenScriptStillProducesTheCheckedInBytes) {
    // Guards the generator itself: re-encoding the script must equal the file.
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v1/basic.rcat", std::ios::binary);
    auto records = rune::catalog::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rune::catalog::testing::goldenScriptV1();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i)
        EXPECT_EQ((*records)[i].second, rune::catalog::encode(script[i])) << "offset " << i;
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests`
Expected: compile error `'rune/catalog/CatalogReplayer.h' file not found`.

- [ ] **Step 3: Implement** `CatalogReplayer.cpp` (decode → `Gap` if `offset != nextOffset()` → `apply` → wrap errors; `describe` formats `offset N: <Decode|Gap|Apply> <code name> [expected=0x… actual=0x…] <detail>`), `CatalogQueries.cpp` (the semantics above; `stateAsOf` replays through a fresh `CatalogReplayer`, so an as-of point past a bad event surfaces that event's error), `CatalogFile.cpp` (`"RCAT"` + u16 1 header; `readRcat` refuses a bad magic, an unknown file version, a truncated record, or non-increasing offsets, each with a message naming the byte position).

- [ ] **Step 4: Golden generator + pins.** `tools/make_catalog_golden.cpp`:

```cpp
// Writes tests/catalog/golden/v1/basic.rcat from GoldenScript and prints the
// per-offset digests as a C++ array for tests/catalog/CatalogGoldenPins.h.
// Run ONCE when a formatVer is introduced; never re-run for an existing one.
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <cstdio>
#include <fstream>
int main(int argc, char** argv) {
    if (argc != 2) { std::fprintf(stderr, "usage: make_catalog_golden <out.rcat>\n"); return 2; }
    std::ofstream out(argv[1], std::ios::binary | std::ios::trunc);
    rune::catalog::writeRcatHeader(out);
    rune::catalog::CatalogReplayer r;
    std::printf("inline constexpr uint64_t kBasicV1Digests[] = {\n");
    int64_t off = 0;
    for (const auto& e : rune::catalog::testing::goldenScriptV1()) {
        const auto bytes = rune::catalog::encode(e);
        rune::catalog::writeRcatRecord(out, off, bytes);
        auto a = r.feed(off, bytes);
        if (!a) { std::fprintf(stderr, "%s\n", rune::catalog::describe(a.error()).c_str()); return 1; }
        std::printf("    0x%016llxull,   // offset %lld %s\n", static_cast<unsigned long long>(r.state().digest()),
                    static_cast<long long>(off), std::string(rune::catalog::kindName(e.kind)).c_str());
        ++off;
    }
    std::printf("};\n");
    return 0;
}
```

CMake (in `rune-codec/CMakeLists.txt`): `add_executable(make_catalog_golden tools/make_catalog_golden.cpp)` linked to `rune_catalog`; `target_compile_definitions(rune_catalog_tests PRIVATE RUNE_CATALOG_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/catalog/golden")`; add `CatalogReplayerTest.cpp`, `CatalogQueriesTest.cpp`, `CatalogGoldenTest.cpp` to `rune_catalog_tests` and the three new `.cpp` to `rune_catalog`. Then generate and freeze:

```bash
cmake /Users/vradu/Projects/se-worktrees/rune-codec/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target make_catalog_golden
mkdir -p /Users/vradu/Projects/se-worktrees/rune-codec/tests/catalog/golden/v1
/Users/vradu/Projects/se-worktrees/rune-codec/build/make_catalog_golden /Users/vradu/Projects/se-worktrees/rune-codec/tests/catalog/golden/v1/basic.rcat > /tmp/pins.txt
```

Create `tests/catalog/CatalogGoldenPins.h` as `#pragma once`, `#include <cstdint>`, `namespace rune::catalog::golden {` + the printed array verbatim + `}`. These 18 constants are frozen from now on.

- [ ] **Step 5: Replay benchmark** — `benchmark/CatalogReplayBench.cpp` (added to `rune_codec_bench` when `benchmark_FOUND`): `BM_CatalogReplay10k` builds 10 000 events once (cycling CreateTopic / AlterSchema / AlterTopicConfig / CreateView / DropView / DropTopic over 500 names, each stamped), encodes them, and per iteration replays all 10 000 through a fresh `CatalogReplayer`; reports `items_per_second`. Gate (asserted in Task 23 at broker level, recorded here): one iteration < 200 ms Release.

- [ ] **Step 6: Run the tests + bench**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-codec/build -R 'Catalog' --output-on-failure -j14`
Expected: all `CatalogEventRegression.*`, `CatalogApplyV1Regression.*`, `CatalogReplayerRegression.*`, `CatalogQueriesRegression.*`, `CatalogGoldenRegression.*` PASS.
Run (bench, Release build dir if present, else skip and note): `/Users/vradu/Projects/se-worktrees/rune-codec/build-rel/rune_codec_bench --benchmark_filter=CatalogReplay`
Expected: < 200 ms per iteration; record the number in the commit body.

- [ ] **Step 7: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-codec add include/rune/catalog src/catalog tools/make_catalog_golden.cpp tests/catalog benchmark/CatalogReplayBench.cpp CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-codec commit -m "feat(catalog): verifying replayer, history/as-of/diff, .rcat, v1 golden log + replay bench

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint A (controller):** full `*Regression*` suite in rune-codec (`ctest --test-dir build -R Regression --output-on-failure -j14`), plus the existing `rune_codec_tests`.

---

### Task 4: Protocol — DDL templates 74–78, the DDL extension, `DefineTopic` replication factor + config, `kCatalogTopicId`, client version 8

Wire for spec §5.1 (Redirect / NotLeader are existing codes), §7 (reads use Subscribe), §8 (`replicationFactor` on CreateTopic). Append-only: old clients keep working (they send no extension; a v7 client never receives `DdlResponse` because it never sends 74–77).

**Files:**
- Modify: `rune-protocol/include/rune/protocol/client/Protocol.h` (`TemplateId` after `ViewRowResponse = 73`; the client-version block; new constant)
- Modify: `rune-protocol/include/rune/protocol/client/SchemaMessages.h` (`DdlExtension`, `TopicConfigWire`; `DefineTopic`, `AlterTopic`, `CreateView` trailers; new `DropTopic`, `AlterTopicConfig`, `DropView`, `AlterView`, `DdlResponse`)
- Modify: `rune-protocol/include/rune/protocol/client/DataMessages.h` (comment only: the catalog subscription + caught-up marker)
- Create: `rune-protocol/tests/protocol/client/CatalogDdlMessagesTest.cpp`; modify `rune-protocol/CMakeLists.txt` (add it to `rune_protocol_tests`)
- Modify: `rune-protocol/conformance/gen_vectors.cpp`, `conformance/manifest.md`, add `conformance/vectors/{define_topic_v8,drop_topic,alter_topic_config,drop_view,alter_view,ddl_response}.bin`

**Interfaces:**
- Produces (namespace `rune::protocol::client`):
  ```cpp
  // TemplateId: DropTopic = 74, AlterTopicConfig = 75, DropView = 76, AlterView = 77, DdlResponse = 78
  inline constexpr int32_t kClientVersionCatalog = 8;
  inline constexpr int32_t kClientVersion        = 8;
  inline constexpr int32_t kCatalogTopicId       = -6;   // Subscribe to __catalog (partition 0)
  struct DdlExtension {                      // trailer on every DDL request (v8)
      std::string_view statement;            // original DDL text ("" for API-issued)
      std::string_view clientName;
      uint64_t         requestId = 0;        // stable across retries (ruling 9)
      uint8_t          hasReplayedFrom = 0;
      std::string_view replayedClusterId; int64_t replayedOffset = -1; std::string_view replayedPrincipal;
  };
  struct TopicConfigWire { /* every rune::catalog::TopicConfig field, same order + types */ };
  struct ConfigChangeWire { uint8_t key; int64_t value; };
  // DefineTopic gains: int16_t replicationFactor; TopicConfigWire config; bool hasExt; DdlExtension ext;
  // AlterTopic / CreateView gain: bool hasExt; DdlExtension ext;
  struct DropTopic        { int64_t sessionId, correlationId; std::string_view topicName; DdlExtension ext; };
  struct DropView         { int64_t sessionId, correlationId; std::string_view viewName;  DdlExtension ext; };
  struct AlterView        { int64_t sessionId, correlationId; std::string_view viewName; std::string_view keyField;
                            uint8_t persistence; uint32_t expectedVersion; const uint8_t* planBlob; uint32_t planLen;
                            DdlExtension ext; };
  struct AlterTopicConfig { int64_t sessionId, correlationId; std::string_view topicName; uint32_t expectedVersion;
                            std::span<const uint8_t> changes /* count:u16 + (key:u8, value:i64)* */; DdlExtension ext; };
  struct DdlResponse      { int64_t sessionId, correlationId; uint16_t errorCode; uint64_t objectId;
                            uint32_t newVersion; int32_t gateTopicId; std::string_view detail; };
  ```

Layouts (all LE; `varstr` = u16 + bytes; `varbytes32` = u32 + bytes):
- `DdlExtension`: `varstr statement` (encoded as u32 length: statements can exceed 64 KiB), `varstr clientName`, `u64 requestId`, `u8 hasReplayedFrom`, then when 1: `varstr clusterId`, `i64 offset`, `varstr principal`.
- `DefineTopic` v8: the existing body, then the persistence byte **always** written when the extension follows, then `i16 replicationFactor`, `TopicConfigWire` (`u8 cleanupPolicy, i64 retentionMs, i64 retentionBytes, i64 deleteRetentionMs, i32 minCleanableDirtyRatioPpm, i64 segmentRollBytes, i64 minCompactionLagMs, i64 maxCompactionLagMs, i64 retentionCheckMs, i64 fileDeleteDelayMs, u8 durability, u8 durabilityTier, varstr coPartitionGroup, varstr partitionKey`), then `DdlExtension`. Decode: after the persistence byte, `pos == avail` → no extension (`hasExt = false`, rf 0, default config); otherwise the full trailer must decode exactly to `avail` (anything else → nullopt).
- `AlterTopic` / `CreateView` v8: the existing body, then `DdlExtension` (same exact-to-end rule).
- `DropTopic` / `DropView`: `varstr name` + `DdlExtension`. `AlterView`: `varstr viewName, varstr keyField, u8 persistence, u32 expectedVersion, varbytes32 planBlob` + ext. `AlterTopicConfig`: `varstr topicName, u32 expectedVersion, u16 count, (u8 key, i64 value)*` + ext; a key outside 1..6 → nullopt.
- `DdlResponse`: `u16 errorCode, u64 objectId, u32 newVersion, i32 gateTopicId, varstr detail`.

Also, in the "Client versions" comment block, add: *"8 — catalog history: DDL extension trailer (statement, clientName, requestId, replayedFrom), DefineTopic replicationFactor + topic config, DropTopic/AlterTopicConfig/DropView/AlterView (74–77) answered by DdlResponse (78). DDL to a broker that is not the `__catalog` leader is answered ErrorResponse{Redirect, leader client endpoint}; a leader change, a catching-up leader or `__catalog` below minIsr answers NotLeader (retriable); a broker whose catalog apply halted answers Internal. Any DDL may be answered ErrorResponse{InvalidRequest, detail} (e.g. replicationFactor bounds)."* — and in `DataMessages.h` above `struct Subscribe`: *"Subscribe{topicId = kCatalogTopicId (−6), partition 0, partitionKnown 1} reads `__catalog` from ANY broker, served from that broker's replica up to its high watermark. After the committed backlog the broker pushes ONE Records frame with baseOffset = its HW and an EMPTY records payload — the caught-up marker (catalog subscriptions only); live events follow as they commit."*

- [ ] **Step 1: Write the failing tests** — `CatalogDdlMessagesTest.cpp`:

```cpp
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <array>
#include <gtest/gtest.h>

namespace pc = rune::protocol::client;

TEST(CatalogDdlWireRegression, ConstantsArePinned) {
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::DropTopic), 74);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::AlterTopicConfig), 75);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::DropView), 76);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::AlterView), 77);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::DdlResponse), 78);
    EXPECT_EQ(pc::kClientVersionCatalog, 8);
    EXPECT_GE(pc::kClientVersion, pc::kClientVersionCatalog);
    EXPECT_EQ(pc::kCatalogTopicId, -6);
}

TEST(CatalogDdlWireRegression, DefineTopicV8RoundTripsRfConfigAndExtension) {
    pc::DefineTopic m;
    m.sessionId = 1; m.correlationId = 2; m.topicName = "events"; m.partitionCount = 4;
    m.persistence = 0; m.replicationFactor = 3;
    m.config.retentionMs = 60'000; m.config.cleanupPolicy = 1; m.config.partitionKey = "k";
    m.hasExt = true; m.ext.statement = "topic events partitions 4 replicas 3;"; m.ext.clientName = "cli";
    m.ext.requestId = 77; m.ext.hasReplayedFrom = 1; m.ext.replayedClusterId = "src"; m.ext.replayedOffset = 5;
    std::array<uint8_t, 1024> buf{};
    const std::size_t n = m.encode(buf.data());
    ASSERT_EQ(n, m.encodedSize());
    auto d = pc::DefineTopic::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->replicationFactor, 3);
    EXPECT_EQ(d->config.retentionMs, 60'000);
    EXPECT_EQ(d->config.cleanupPolicy, 1);
    EXPECT_EQ(d->config.partitionKey, "k");
    EXPECT_TRUE(d->hasExt);
    EXPECT_EQ(d->ext.requestId, 77u);
    EXPECT_EQ(d->ext.replayedOffset, 5);
    EXPECT_EQ(d->descriptorLen, 0u) << "an empty descriptor is an untyped topic";
}

TEST(CatalogDdlWireRegression, PreV8DefineTopicStillDecodes) {
    pc::DefineTopic m; m.topicName = "t"; m.partitionCount = 1; m.persistence = 1;   // v7 frame: no ext
    std::array<uint8_t, 256> buf{};
    const std::size_t n = m.encode(buf.data());
    auto d = pc::DefineTopic::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_FALSE(d->hasExt);
    EXPECT_EQ(d->replicationFactor, 0);
    EXPECT_EQ(d->persistence, 1);
}

TEST(CatalogDdlWireRegression, EveryTruncatedTrailerIsRefused) {
    pc::DropTopic m; m.topicName = "t"; m.ext.statement = "drop topic t;"; m.ext.requestId = 9;
    std::array<uint8_t, 256> buf{};
    const std::size_t n = m.encode(buf.data());
    for (std::size_t k = 0; k < n; ++k) EXPECT_FALSE(pc::DropTopic::decode(buf.data(), k).has_value()) << k;
    EXPECT_TRUE(pc::DropTopic::decode(buf.data(), n).has_value());
}

TEST(CatalogDdlWireRegression, AlterTopicConfigRefusesAnUnknownKey) {
    std::array<uint8_t, 9> change{7, 0, 0, 0, 0, 0, 0, 0, 0};   // key 7 does not exist
    std::array<uint8_t, 11> changes{1, 0};
    std::copy(change.begin(), change.end(), changes.begin() + 2);
    pc::AlterTopicConfig m; m.topicName = "t"; m.changes = changes;
    std::array<uint8_t, 256> buf{};
    const std::size_t n = m.encode(buf.data());
    EXPECT_FALSE(pc::AlterTopicConfig::decode(buf.data(), n).has_value());
}

TEST(CatalogDdlWireRegression, DdlResponseRoundTrips) {
    pc::DdlResponse r; r.errorCode = 39; r.objectId = 12; r.newVersion = 3; r.gateTopicId = 5;
    r.detail = "replicationFactor 5 > broker count 3";
    std::array<uint8_t, 256> buf{};
    const std::size_t n = r.encode(buf.data());
    auto d = pc::DdlResponse::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->detail, r.detail);
    EXPECT_EQ(d->gateTopicId, 5);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-protocol/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-protocol/build -j14 --target rune_protocol_tests`
Expected: compile errors (`DropTopic` is not a member of `TemplateId`, …).

- [ ] **Step 3: Implement** the constants, structs and the trailer rules above, following `DefineTopic`'s existing encode/decode style (`putVarStr`, `getVarStr`, `putVarBytes32`, `getVarBytes32`, `detail::putLE*`). Each new struct has `TEMPLATE_ID`, `encodedSize()`, `encode()`, `static decode()` returning `std::optional`. `DdlExtension` gets free functions `extEncodedSize`, `putExt`, `getExt` (the statement uses a u32 length).

- [ ] **Step 4: Conformance vectors** — extend `conformance/gen_vectors.cpp` with one vector per new frame (fixed field values as in the tests), regenerate (`cmake --build … --target gen_vectors && ./build/gen_vectors conformance/vectors`), list them in `conformance/manifest.md`.

- [ ] **Step 5: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-protocol/build -j14 --target rune_protocol_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-protocol/build -R 'CatalogDdlWire|DefineTopic|Conformance' --output-on-failure`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-protocol add include tests conformance CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-protocol commit -m "feat(protocol): catalog DDL wire — templates 74-78, DDL extension, DefineTopic rf/config, kCatalogTopicId, client v8

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 5: Client — the new DDL ops, DDL options, and Redirect / NotLeader / InvalidRequest handling for every DDL

Spec §5.1–§5.2, §5.5, §9: "A broker that is not the `__catalog` leader answers with the existing Redirect … which clients already follow"; "Clients retry within their budget and then fail explicitly." Today the client follows `Redirect` for data ops only; a DDL answered `ErrorResponse` is left pending until its deadline. This task makes every DDL op (`DefineTopic`, `AlterTopic`, `CreateView`, and the new four) follow `Redirect`, retry `NotLeader` with a backoff inside its budget, surface `InvalidRequest` detail, and carry the DDL extension with a stable `requestId`.

**Files:**
- Modify: `rune-client-cpp/include/rune/client/Completion.h` (`OpKind`: append `DropTopic, DropView, AlterView, AlterTopicConfig` after `GetViewRow`)
- Modify: `rune-client-cpp/include/rune/client/Client.h` (`DdlOptions`, `TopicOptions`, `DdlResult`; new ops + `AndWait` forms; `defineTopic` overload taking `TopicOptions` + `DdlOptions`; `PendingCtl` gains the DDL retry state)
- Modify: `rune-client-cpp/src/Client.cpp` (encode the extension on every DDL; `DdlResponse` (78) dispatch; the `ErrorResponse` branch for pending DDL ops; the retry pump in the duty cycle next to `retryRedirectedSubscribes_`)
- Modify: `rune-client-cpp/include/rune/client/agent/ClientAgent.h`, `src/agent/ClientAgent.cpp` (`offerToEndpoint(channel, frame)` — the per-endpoint session pool `offerToPartition` already uses)
- Test: `rune-client-cpp/tests/client/ClientDdlRedirectRegressionTest.cpp`, `rune-client-cpp/tests/client/ClientCatalogDdlTest.cpp` (both against the existing `tests/support/InvokerGateway.h` fake broker); add both to `CMakeLists.txt`

**Interfaces:**
- Consumes: Task 4's frames, `kClientVersion = 8`.
- Produces (namespace `rune::client`):
  ```cpp
  struct DdlOptions {
      std::string statement;              // original DDL text (QL/CLI fill it; "" for API-issued)
      std::string clientName;             // defaults to ClientConfig::clientName
      uint64_t    requestId = 0;          // 0 = the client generates one; reused on every retry
      struct ReplayedFrom { std::string clusterId; int64_t offset = -1; std::string principal; };
      std::optional<ReplayedFrom> replayedFrom;   // `rune catalog replay` only (Task 15) → DdlExtension.hasReplayedFrom
  };
  struct TopicOptions { int16_t replicationFactor = 0; rune::protocol::client::TopicConfigWire config{}; };
  struct DdlResult { uint16_t errorCode = 0; uint64_t objectId = 0; uint32_t newVersion = 0; std::string detail; };
  DefineTopicResult defineTopicAndWait(std::string_view name, uint32_t partitions, std::string_view keyField,
                                       std::span<const uint8_t> descriptorBlob /* empty = untyped */, bool validate,
                                       Persistence persistence, const TopicOptions& topic, const DdlOptions& ddl,
                                       std::chrono::milliseconds timeout);
  DdlResult dropTopicAndWait(std::string_view name, const DdlOptions&, std::chrono::milliseconds);
  DdlResult dropViewAndWait(std::string_view name, const DdlOptions&, std::chrono::milliseconds);
  DdlResult alterViewAndWait(std::string_view name, std::string_view keyField, uint8_t persistence,
                             uint32_t expectedVersion, std::span<const uint8_t> planBlob, const DdlOptions&,
                             std::chrono::milliseconds);
  DdlResult alterTopicConfigAndWait(std::string_view name, uint32_t expectedVersion,
                                    std::span<const rune::protocol::client::ConfigChangeWire> changes,
                                    const DdlOptions&, std::chrono::milliseconds);
  // DefineTopicResult / AlterTopicResult / CreateViewResult gain `std::string detail`.
  // ClientConfig gains `std::string clientName = "rune-client"` and
  //   `std::chrono::milliseconds ddlRetryBudget{10'000}` (NotLeader retries; bounded by the op timeout too).
  static constexpr int kMaxDdlRedirects = 3;
  ```

Behaviour (all on the duty thread, cold path):
- **Encode:** every DDL frame carries `DdlExtension{statement, clientName, requestId}`; `requestId = ddl.requestId ? ddl.requestId : nextRequestId_()` where `nextRequestId_()` = `(random 32-bit client nonce << 32) | ++counter` — the nonce is drawn once at `Client` construction from `std::random_device`. The encoded frame is kept in `PendingCtl::frame` (re-sent verbatim, so the `requestId` is stable).
- **`ErrorResponse{Redirect, endpoint}` for a pending DDL corrId:** `++redirects`; above `kMaxDdlRedirects` → complete `BrokerError` with `errorCode = 3` and detail `"redirected more than 3 times"`. Otherwise, multi-node routing on → `agent_.offerToEndpoint(toChannel(endpoint), frame)`; off → the existing seed retarget (`submitReconnect`, the same call the data-op Redirect path makes) and the frame waits in `unsentFrame` to be re-offered once connected.
- **`ErrorResponse{NotLeader}` or a response with `errorCode == 2`:** schedule a resend at `now + backoff` (100 ms doubling to 1 s); past `min(deadline, firstNotLeader + ddlRetryBudget)` → complete `BrokerError` with `errorCode = 2` ("explicit client failure", spec §5.5).
- **`ErrorResponse{InvalidRequest | other}`:** complete `BrokerError` with that code and `detail = message`.
- **`DdlResponse` (78):** complete with `errorCode`, `objectId`, `newVersion`, `detail`; `errorCode == 2` follows the NotLeader rule.

- [ ] **Step 1: Write the failing tests** — `ClientDdlRedirectRegressionTest.cpp` (the fake broker answers by script; `InvokerGateway` exposes `onFrame(templateId, handler)` and `reply(frame)` — use its existing API names):

```cpp
// Catalog history §5.1/§5.2/§9: every DDL follows Redirect, retries NotLeader inside
// its budget, then fails explicitly; the requestId is stable across every resend.
#include "support/InvokerGateway.h"
#include <rune/client/Client.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <gtest/gtest.h>

namespace pc = rune::protocol::client;
using namespace std::chrono_literals;

TEST(ClientDdlRedirectRegression, NotLeaderIsRetriedWithTheSameRequestIdThenSucceeds) {
    rune::client::test::InvokerGateway gw;
    std::vector<uint64_t> seenRequestIds;
    int n = 0;
    gw.onFrame(pc::TemplateId::DropTopic, [&](const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        seenRequestIds.push_back(m->ext.requestId);
        if (n++ < 2) { gw.replyError(m->sessionId, m->correlationId, pc::ErrorCode::NotLeader, ""); return; }
        pc::DdlResponse r; r.sessionId = m->sessionId; r.correlationId = m->correlationId; r.objectId = 4; r.newVersion = 2;
        gw.reply(r);
    });
    auto client = gw.connectClient();
    const auto res = client->dropTopicAndWait("t", {}, 5s);
    EXPECT_EQ(res.errorCode, 0);
    EXPECT_EQ(res.objectId, 4u);
    ASSERT_EQ(seenRequestIds.size(), 3u);
    EXPECT_NE(seenRequestIds[0], 0u);
    EXPECT_EQ(seenRequestIds[0], seenRequestIds[1]);
    EXPECT_EQ(seenRequestIds[1], seenRequestIds[2]);
}

TEST(ClientDdlRedirectRegression, NotLeaderPastTheBudgetIsAnExplicitFailureNotAHang) {
    rune::client::test::InvokerGateway gw;
    gw.onFrame(pc::TemplateId::DefineTopic, [&](const uint8_t* f, std::size_t len) {
        auto m = pc::DefineTopic::decode(f, len);
        gw.replyError(m->sessionId, m->correlationId, pc::ErrorCode::NotLeader, "");
    });
    rune::client::ClientConfig cfg = gw.clientConfig();
    cfg.ddlRetryBudget = 400ms;
    auto client = gw.connectClient(cfg);
    const auto t0 = std::chrono::steady_clock::now();
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 5s);
    EXPECT_EQ(r.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 3s) << "the budget, not the 5 s op timeout, ends it";
}

TEST(ClientDdlRedirectRegression, RedirectIsFollowedToTheLeaderEndpoint) {
    rune::client::test::InvokerGateway follower, leader;
    follower.onFrame(pc::TemplateId::AlterTopicConfig, [&](const uint8_t* f, std::size_t len) {
        auto m = pc::AlterTopicConfig::decode(f, len);
        follower.replyError(m->sessionId, m->correlationId, pc::ErrorCode::Redirect, leader.clientEndpoint());
    });
    bool leaderSaw = false;
    leader.onFrame(pc::TemplateId::AlterTopicConfig, [&](const uint8_t* f, std::size_t len) {
        auto m = pc::AlterTopicConfig::decode(f, len);
        leaderSaw = true;
        pc::DdlResponse r; r.sessionId = m->sessionId; r.correlationId = m->correlationId; r.newVersion = 3;
        leader.reply(r);
    });
    auto client = follower.connectClient(follower.clientConfigWithPeer(leader));
    const pc::ConfigChangeWire ch{1, 60'000};
    const auto r = client->alterTopicConfigAndWait("t", 0, std::span(&ch, 1), {}, 5s);
    EXPECT_TRUE(leaderSaw);
    EXPECT_EQ(r.errorCode, 0);
    EXPECT_EQ(r.newVersion, 3u);
}

TEST(ClientDdlRedirectRegression, RedirectLoopEndsExplicitly) {
    rune::client::test::InvokerGateway gw;
    gw.onFrame(pc::TemplateId::DropView, [&](const uint8_t* f, std::size_t len) {
        auto m = pc::DropView::decode(f, len);
        gw.replyError(m->sessionId, m->correlationId, pc::ErrorCode::Redirect, gw.clientEndpoint());   // to itself
    });
    auto client = gw.connectClient();
    const auto r = client->dropViewAndWait("v", {}, 5s);
    EXPECT_EQ(r.errorCode, static_cast<uint16_t>(pc::ErrorCode::Redirect));
    EXPECT_NE(r.detail.find("redirected more than"), std::string::npos);
}

TEST(ClientDdlRedirectRegression, InvalidRequestDetailReachesTheCaller) {
    rune::client::test::InvokerGateway gw;
    gw.onFrame(pc::TemplateId::DefineTopic, [&](const uint8_t* f, std::size_t len) {
        auto m = pc::DefineTopic::decode(f, len);
        gw.replyError(m->sessionId, m->correlationId, pc::ErrorCode::InvalidRequest,
                      "replicationFactor 5 > broker count 3");
    });
    auto client = gw.connectClient();
    rune::client::TopicOptions topic; topic.replicationFactor = 5;
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, topic, {}, 5s);
    EXPECT_EQ(r.errorCode, 39);
    EXPECT_EQ(r.detail, "replicationFactor 5 > broker count 3");
}
```

`ClientCatalogDdlTest.cpp` pins the encodings the client sends: `defineTopicAndWait` with an empty descriptor sends `descriptorLen 0`, `replicationFactor`, the config and the extension (`statement` and `clientName` from `DdlOptions` / `ClientConfig`); `alterViewAndWait` sends `AlterView` with `expectedVersion`; every one of the 7 DDL ops sets `ext.requestId != 0`; two different ops get different `requestId`s.

(If `InvokerGateway` lacks `replyError`, `onFrame`, `clientEndpoint` or `clientConfigWithPeer`, add them in this task as thin wrappers over its existing frame-injection API — test support only.)

- [ ] **Step 2: Run to verify they fail**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-client-cpp/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14 --target rune_client_tests`
Expected: compile errors (`dropTopicAndWait` is not a member of `rune::client::Client`).

- [ ] **Step 3: Implement** the ops, options, `PendingCtl` retry state (`std::vector<uint8_t> frame; int redirects = 0; int64_t firstNotLeaderNs = 0; int64_t resendAtNs = 0;`), the `ErrorResponse` branch (placed **before** the data-op Redirect branch, matched by `findCtl_(corrId)` and an `isDdlOp(op)` predicate covering `DefineTopic, AlterTopic, CreateView, DropTopic, DropView, AlterView, AlterTopicConfig`), the `DdlResponse` case, and `int Client::resendDueDdl_(int64_t nowNs)` called in the duty cycle next to `retryRedirectedSubscribes_`. `ClientAgent::offerToEndpoint(const std::string& channel, const uint8_t*, std::size_t)` reuses the per-endpoint session lookup `offerToPartition` uses.

- [ ] **Step 4: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'ClientDdlRedirect|ClientCatalogDdl|ClientSchema|Redirect|Conformance' --output-on-failure -j14`
Expected: PASS (existing `RedirectTest`, `ClientSchemaTest` unchanged).

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add include src tests CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(client): catalog DDL ops + DDL extension; every DDL follows Redirect and retries NotLeader in budget

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint B (controller):** full `*Regression*` suites in protocol and client; broker `build-fast` still builds against the new protocol headers (`cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14`).

---

### Task 6: Broker — the `__catalog` partition: bootstrap, admin-thread replication plane, controller-ISR placement (never unclean)

Spec §3, §5.6, §5.3 "New broker". Today's catalog logs ride a `NullPartitionTransport` and their role follows the consensus leader (fix-on-sight, the E2 carry-over's root cause). This task builds `__catalog` (topic id −6, partition 0) on every broker, replicated for real, and places it on the coordinator-partition controller path so its leader is always elected from its ISR. The AdminWorker still drives the old `__topic_schemas`/`__views` replicas until Task 9; this task only adds the new partition next to them.

**Files:**
- Create: `libs/node/include/rune/node/CatalogPartition.h`
- Create: `libs/node/include/rune/node/AdminIsrChannel.h`
- Modify: `libs/node/include/rune/node/NodeConfig.h` / `src/NodeConfig.cpp` (`catalogReplStreamId`, env `RUNE_CATALOG_REPL_STREAM_ID`, default 30; validation: distinct from `replicationStreamId` and outside `[replBaseStreamId, replBaseStreamId + workerCount)`)
- Modify: `libs/node/include/rune/node/MetadataService.h` (`isCoordinatorTopic` → also −6, renamed `isControllerIsrInternalTopic`; every call site)
- Modify: `libs/node/include/rune/node/AdminChannels.h` (`AdminRoleMsg` carries `isr`, `partitionEpoch`, `leaderEpoch`; `kA2cCatalogAlterIsr = 10`)
- Modify: `libs/node/src/ReplicationMetadataBridge.cpp`, `libs/node/include/rune/node/ReplicationMetadataBridge.h` (route −6 role changes onto the admin role ring instead of a direct call)
- Modify: `libs/node/src/BrokerNode.cpp` (`buildCatalog_()`, called from `buildAdminWorker()` before the schema block; `setCoordinatorPartitions` includes `{-6, 0}`; the control drain forwards `kA2cCatalogAlterIsr`; single-node role from the consensus leader exactly as today's `AdminRoleMsg`)
- Modify: `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp` (own + drive the catalog replica: `doWork`, `onCatalogRole`)
- Modify: `libs/node/CMakeLists.txt` (new test executable `rune_catalog_replication_tests`)
- Test: `libs/node/tests/integration/CatalogReplicationTest.cpp`, `libs/node/tests/node/AdminIsrChannelTest.cpp` (in `rune_node_tests`)

**Interfaces:**
- Produces:
  ```cpp
  // CatalogPartition.h (namespace rune::node)
  inline constexpr int32_t kCatalogTopicId = -6;          // == rune::protocol::client::kCatalogTopicId
  [[nodiscard]] inline int32_t catalogMinIsr(std::size_t brokers) noexcept { return static_cast<int32_t>(brokers / 2 + 1); }
  [[nodiscard]] rune::replication::PartitionConfig makeCatalogPartitionConfig(const NodeConfig&, std::size_t brokers);
  //   cleanupPolicy Delete, retentionMs -1, retentionBytes -1, logStartOffset 0  (never compacted / deleted)
  //   minIsr catalogMinIsr(brokers), durability Majority, acks=all on every append
  // AdminIsrChannel.h — the catalog replica's ISR channel: a PartitionTransport whose
  //   sendAlterIsr(msg) encodes kA2cCatalogAlterIsr on the admin→control ring (every other send: unused, false).
  class AdminIsrChannel final : public rune::replication::PartitionTransport { … };
  // AdminWorker
  void onCatalogRole(const AdminRoleMsg& role);          // becomeLeader(epoch, isr, partitionEpoch) / becomeFollower
  [[nodiscard]] rune::replication::PartitionReplica* catalogReplica() const noexcept;
  // BrokerNode (tests)
  rune::storage::Log* catalogLogForTest() const noexcept;   // only when the admin thread is quiesced
  ```
- Consumed by Tasks 7–11 (`kCatalogTopicId`, the replica, `onCatalogRole`).

Construction (`BrokerNode::buildCatalog_`):
1. `dataDir/__catalog/` + `rune::storage::Log` recovered (disk only — never `InMemoryLog`).
2. `PartitionReplica(makeCatalogPartitionConfig(config_, peers), Role::Follower, 0, allNodeIds, allNodeIds, log, catalogPlane)`, where `catalogPlane` is a `WorkerReplicationTransport` built exactly like a legacy per-worker plane (anchor: `auto t = std::make_unique<WorkerReplicationTransport>(w, inboundChannel, streamId, WorkerReplicationTransport::DeferredAttach{});`) with `streamId = config_.catalogReplStreamId`, `inboundChannel = config_.selfReplicationBindChannel()`, its own `PubAddBroker` registered on `replTransport_`, attached to the node client `aeron_`, and the replica registered on it. **Only the admin thread polls it** (`AdminWorker::doWork` calls the plane's `poll` before `catalogReplica_->doWork`). Single node / no replication port: a `NullPartitionTransport`, as today.
3. `catalogReplica_->setIsrChannel(&adminIsrChannel_)` (the ring adapter; never `replTransport_`, which the control thread owns).
4. The replica is NOT added to `replicaRegistry_` (RULING-2 A: admin-owned). The bridge learns its assignment from the MSM like the coordinator partitions and, for `topicId == kCatalogTopicId`, encodes `AdminRoleMsg{leader, epoch = leaderEpoch, leaderId, isr, partitionEpoch}` on `adminWorkerAgent_->roleRing()` instead of calling `becomeLeader` (a full role ring keeps the op pending and retries next cycle, like `enqueueRoleOp_`).
5. The control drain decodes `kA2cCatalogAlterIsr` and calls `replTransport_->sendAlterIsr(msg)` — the same controller channel the coordinator partitions use.

- [ ] **Step 1: Write the failing tests** — `libs/node/tests/integration/CatalogReplicationTest.cpp` (3 BrokerNodes over real Aeron; reuse the `TmpDir` / `Ports::discover` / `makeConfig` helpers pattern of `TransientTopicDataPathTest.cpp`, copied into an anonymous namespace):

```cpp
// Catalog history §3/§5.6: __catalog exists on every broker from cluster formation,
// is replicated for real (acks=all, minIsr = majority), and its leader is always an
// ISR member (unclean election disabled).
TEST(CatalogReplicationRegression, FormationCreatesTheCatalogOnEveryBrokerWithACleanLeader) {
    ThreeNodes c = ThreeNodes::start("cat_form");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.catalogLeaderId();
    for (auto& n : c.nodes) {
        const auto pa = n->onControlThreadForTest([&] {
            return *n->metadataServiceForTest()->assignmentForTest(rune::node::kCatalogTopicId, 0);
        });
        EXPECT_EQ(pa.leaderNode, leader);
        EXPECT_EQ(pa.replicas.size(), 3u) << "replicated to every broker";
        EXPECT_NE(std::find(pa.isr.begin(), pa.isr.end(), leader), pa.isr.end());
        EXPECT_TRUE(pa.cleanElection);
    }
}

TEST(CatalogReplicationRegression, AppendsReplicateToEveryBrokerBeforeTheHighWatermarkMoves) {
    ThreeNodes c = ThreeNodes::start("cat_repl");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendRawOnCatalogLeaderForTest({1, 2, 3});   // admin-thread task (onAdminThreadForTest)
    ASSERT_GE(hw, 1);
    for (auto& n : c.nodes)
        EXPECT_TRUE(c.awaitCatalogEndOffset(*n, hw, 10s)) << "node " << n->nodeIdForTest();
}

TEST(CatalogReplicationRegression, LeaderKillElectsAnIsrMemberAndKeepsEveryCommittedRecord) {
    ThreeNodes c = ThreeNodes::start("cat_kill");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendRawOnCatalogLeaderForTest({7});
    const int32_t old = c.catalogLeaderId();
    c.stop(old);
    ASSERT_TRUE(c.awaitCatalogLeaderOtherThan(old, 30s));
    auto& nl = c.node(c.catalogLeaderId());
    EXPECT_GE(c.catalogHighWatermark(nl), hw) << "never unclean: the new leader holds the committed prefix";
}

TEST(CatalogReplicationRegression, CatalogIsNeverCompactedOrRetentionDeleted) {
    const auto cfg = rune::node::makeCatalogPartitionConfig(rune::node::NodeConfig{}, 3);
    EXPECT_EQ(cfg.cleanupPolicy, rune::replication::CleanupPolicy::Delete);
    EXPECT_EQ(cfg.retentionMs, -1);
    EXPECT_EQ(cfg.retentionBytes, -1);
    EXPECT_EQ(cfg.logStartOffset, 0);
    EXPECT_EQ(cfg.minIsr, 2);
    EXPECT_EQ(rune::node::catalogMinIsr(5), 3);
}

TEST(CatalogReplicationRegression, ANewBrokerWithAnEmptyDataDirFetchesTheCatalogFromOffsetZero) {
    ThreeNodes c = ThreeNodes::start("cat_join", /*startOnly=*/{1, 2});
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendRawOnCatalogLeaderForTest({4, 5});
    c.startNode(3);                                       // empty data dir
    EXPECT_TRUE(c.awaitCatalogEndOffset(c.node(3), hw, 20s));
    EXPECT_TRUE(c.awaitInCatalogIsr(3, 20s)) << "AlterIsr admits it once caught up";
}
```

`ThreeNodes` is a file-local helper (start/stop/startNode by id, `awaitCatalogLeader`, `catalogLeaderId` read from node 1's MSM, `appendRawOnCatalogLeaderForTest` via a new `BrokerNode::onAdminThreadForTest(f)` — the admin-thread counterpart of `onOwnerThreadForTest`, built on `OwnerThreadTestTask` posted on a new admin test ring drained in `AdminWorkerAgent::doWork`). `AdminIsrChannelTest.cpp` asserts `sendAlterIsr` encodes exactly one `kA2cCatalogAlterIsr` frame whose decode equals the message, and that a full ring returns `false` (the replica re-sends on its cadence).

- [ ] **Step 2: Run to verify they fail**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_catalog_replication_tests rune_node_tests`
Expected: compile errors (`rune/node/CatalogPartition.h` not found).

- [ ] **Step 3: Implement** the files and edits listed above. `MetadataService::isControllerIsrInternalTopic(id)` returns `id == -1 || id == -2 || id == kCatalogTopicId`; `BrokerNode` (anchor `metadataService_->setCoordinatorPartitions(std::move(coord), balancerNodes);`) appends `rune::metadata::PartitionKey{kCatalogTopicId, 0}` to `coord`. `decideCoordinatorPartition` needs no change (replicas = `balancerNodes`, never unclean — re-read its doc comment, it already elects only from `ISR ∩ live`).

- [ ] **Step 4: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogReplication|AdminIsrChannel|MetadataService|Coordinator|ReplicationMetadataBridge|NodeConfig' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs/node CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(catalog): __catalog partition on every broker — real replication, controller-ISR leader, never unclean

Fix-on-sight: the admin catalogs rode a NullPartitionTransport and followed the
consensus leader (E2 carry-over root cause).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Broker — `CatalogApplier`: apply committed events in offset order, verify, halt on mismatch, checkpoint before side effects

Spec §5.3: each broker applies strictly in offset order from its last applied offset; for each event: apply under its `formatVer`, verify the fingerprint, update the local catalog, distribute to the workers through the existing rings. A mismatch stops apply loudly (error + metric); the broker keeps serving what it applied and accepts no further catalog change. Ruling 8: the applied checkpoint is persisted before any side effect.

**Files:**
- Create: `libs/node/include/rune/node/CatalogCheckpoint.h` (+ `libs/node/src/CatalogCheckpoint.cpp`)
- Create: `libs/node/include/rune/node/CatalogApplier.h`, `libs/node/src/CatalogApplier.cpp`
- Modify: `libs/node/include/rune/node/AdminChannels.h` (effect messages, below)
- Modify: `libs/node/include/rune/node/SchemaRegistry.h` / `src/SchemaRegistry.cpp` (`erase(int32_t topicId)`), `ViewRegistry.h` / `src/ViewRegistry.cpp` (`erase(int32_t viewTopicId)`)
- Modify: `libs/node/include/rune/node/RoleCounter.h` (`kCatalogApplyHaltedCounterTypeId = 1'000'217`, `kCatalogAppliedOffsetCounterTypeId = 1'000'218`), `libs/node/src/BrokerNode.cpp` (register both counters next to `outOfSyncCounterRegId_`; set them from `kA2cCatalogApplied` / `kA2cCatalogHalted`)
- Modify: `libs/node/CMakeLists.txt` (`CatalogApplier.cpp`, `CatalogCheckpoint.cpp` in `rune_node`; link `rune_catalog`; root `CMakeLists.txt` defines `rune_catalog` from `${RUNE_CODEC_ROOT}/src/catalog/*.cpp` next to `rune_codec`)
- Test: `libs/node/tests/node/CatalogApplierTest.cpp` (in `rune_node_tests`)

**Interfaces:**
- Consumes: `rune::catalog::{CatalogState, CatalogReplayer, apply, decode, Applied}`; the catalog replica (Task 6).
- Produces:
  ```cpp
  // CatalogCheckpoint.h
  struct CatalogCheckpoint { int64_t appliedOffset = -1; uint64_t digest = 0; };
  [[nodiscard]] std::optional<CatalogCheckpoint> readCatalogCheckpoint(const std::string& catalogDir);  // nullopt = none
  [[nodiscard]] bool writeCatalogCheckpoint(const std::string& catalogDir, const CatalogCheckpoint&);    // tmp+fsync+rename+dir fsync
  // CatalogApplier.h
  struct CatalogEffects {     // admin-thread effect sink; production = ring encoders (AdminWorkerAgent)
      std::function<void(const rune::catalog::TopicObject&)>               topicApplied;     // CreateTopic, sink of Create/AlterView
      std::function<void(int32_t topicId, const std::string& name)>        topicDropped;     // DropTopic, retired sink
      std::function<void(int32_t topicId, std::span<const rune::catalog::ConfigChange>)> topicConfig;
      std::function<void(const rune::catalog::TopicObject&)>               schemaChanged;    // validators fan-out
      std::function<void(const rune::catalog::ViewObject&, bool recovery)> viewInstalled;
      std::function<void(int32_t viewTopicId)>                             viewUninstalled;
      std::function<void(int64_t offset, uint64_t digest)>                 applied;          // metric + tests
      std::function<void(const rune::catalog::ApplyError&)>               halted;
  };
  class CatalogApplier {
  public:
      struct Applying { int64_t nextOffset = 0; };
      struct Halted   { rune::catalog::ApplyError error; };
      CatalogApplier(std::string catalogDir, SchemaRegistry&, ViewRegistry&, CatalogEffects);
      /// Boot: replay [0, checkpoint] from the local log; returns the applied count or the halt.
      int replayLocal(rune::replication::PartitionReplica& r, bool recoveryInstalls);
      /// Steady: apply every committed record in [nextOffset, hw).  Returns work done.
      int applyCommitted(rune::replication::PartitionReplica& r);
      [[nodiscard]] const rune::catalog::CatalogState& state() const noexcept;
      [[nodiscard]] bool halted() const noexcept;
      [[nodiscard]] int64_t nextOffset() const noexcept;     // -1 when halted
      [[nodiscard]] std::string_view stateName() const noexcept;   // "Applying" | "Halted"
      void tamperNextFingerprintForTest() noexcept;          // matrix FingerprintMismatch seam (test-only)
  private:
      std::variant<Applying, Halted> st_;
      …
  };
  ```
- New admin → control messages (`AdminChannels.h`, failure ring, after `kA2cTopicCommitted = 4`): `kA2cCatalogTopicApplied = 5` (`[topicId:i32][name][partitions:i32][rf:i16][persistence:u8][TopicConfig block]` — the exact `TopicSpec` the control thread builds), `kA2cCatalogTopicDropped = 6` (`[topicId][name]`), `kA2cCatalogTopicConfig = 7` (`[topicId][count:u16][(key:u8,value:i64)*]`), `kA2cViewUninstall = 8` (`[viewTopicId]`), `kA2cCatalogHalted = 9` (`[offset:i64][expected:u64][actual:u64][detail]`), `kA2cCatalogApplied = 11` (`[offset:i64][digest:u64]`) — `10` is `kA2cCatalogAlterIsr` (Task 6). Encoders/decoders follow `encodeViewPlanUpdate` / `decodeViewPlanUpdate`.

Per committed record, `applyCommitted` does, in this order:
1. read the batch at `nextOffset` into a **reused** buffer (`std::vector<uint8_t> readBuf_` sized once to `kCatalogReadChunk = 1 MiB`, capacity kept), iterate records with `RecordBatchReader` (the same loop shape as `AdminWorker::rebuildFromLog_`);
2. `rune::catalog::decode` → `rune::catalog::apply(state_, event, offset)` (a copy of the touched object is staged inside the library; the state changes only on success);
3. on any error → `st_ = Halted{err}`; `spdlog::critical("CatalogApplier: apply HALTED at offset {} — {}; this broker serves what it applied and accepts no catalog change until an operator intervenes (docs/CATALOG.md#halted)", …)`; `effects_.halted(err)`; return — **never** skip;
4. `writeCatalogCheckpoint(dir, {offset, state_.digest()})` — a failed write is a storage failure (posted as `kA2cStorageFailure`, the existing halt path), never ignored;
5. projections: `SchemaRegistry` (a `TopicSchema` built from the topic object's schema aspect: topicId, schemaVersion, partitions, keyField, validate, evolving blob, nextTag, compat, name, persistence — exactly the fields `AdminWorker::advance_` sets today) / `erase` on drops; `ViewRegistry` (`ViewRecord{viewTopicId, name, compiledPlanBlob, persistence, outputDescriptorBlob}`) / `erase`;
6. effects by kind: `CreateTopic` → `topicApplied`; `DropTopic` → `topicDropped`; `AlterTopicConfig` → `topicConfig`; `CreateSchema`/`AlterSchema`/`SetCompatibility` → `schemaChanged`; `CreateView` → `topicApplied(sink)` then `viewInstalled(view, recovery)`; `AlterView` → `topicApplied(new sink)`, `viewInstalled(new)`, `viewUninstalled(old)`, `topicDropped(old sink)`; `DropView` → `viewUninstalled`, `topicDropped(sink)`;
7. `effects_.applied(offset, digest)`.

`replayLocal` replays `[0, checkpoint.appliedOffset]` (no checkpoint → nothing) through the same steps 1–3 and 5–6 (no checkpoint rewrite; `recovery = true` on view installs, so workers rebuild from their checkpoints as `recoverViewsOnPromotion_` does today); a digest that differs from the checkpoint's → `Halted` (local log damage).

- [ ] **Step 1: Write the failing tests** — `CatalogApplierTest.cpp` uses a real `Log` + a follower `PartitionReplica` fed by `appendAsFollowerForTest` (or a leader replica with ISR {self} and `onLeaderLogAppended()`, so HW = LEO), events from `rune::catalog::testing::goldenScriptV1()`, and a recording `CatalogEffects`:

```cpp
TEST(CatalogApplierRegression, AppliesCommittedEventsInOffsetOrderOnly) {
    CatalogRig rig("ap_order");                       // log + leader replica (ISR {self}) + registries + recorder
    rig.appendEvents(0, 5);                            // golden events 0..4, committed
    rig.appendUncommitted(5);                          // event 5 appended, HW held at 5 (ISR {self, 2}, 2 silent)
    EXPECT_GT(rig.applier.applyCommitted(rig.replica()), 0);
    EXPECT_EQ(rig.applier.nextOffset(), 5) << "never past the HW";
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2, 3, 4}));
}

TEST(CatalogApplierRegression, FingerprintMismatchHaltsApplyRaisesTheMetricAndKeepsServingRegression) {
    CatalogRig rig("ap_halt");
    rig.appendEvents(0, 3);
    rig.appendTampered(3);                             // event 3 with fingerprint ^ 1
    rig.appendEvents(4, 6);
    rig.applier.applyCommitted(rig.replica());
    EXPECT_TRUE(rig.applier.halted());
    EXPECT_EQ(rig.applier.stateName(), "Halted");
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 3);
    EXPECT_EQ(rig.rec.halts[0].code, rune::catalog::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(rig.rec.appliedOffsets.back(), 2) << "what was applied stays applied (still served)";
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0) << "halted: no further apply, ever";
    EXPECT_NE(rig.schemas.byName("players"), nullptr) << "projection of the applied prefix intact";
}

TEST(CatalogApplierRegression, CheckpointPrecedesSideEffectsSoARestartNeverQuarantinesRegression) {
    CatalogRig rig("ap_ckpt");
    rig.appendEvents(0, 2);
    std::optional<rune::node::CatalogCheckpoint> seenAtEffect;
    rig.rec.onTopicApplied = [&] { seenAtEffect = rune::node::readCatalogCheckpoint(rig.catalogDir()); };
    rig.applier.applyCommitted(rig.replica());
    ASSERT_TRUE(seenAtEffect.has_value()) << "the checkpoint exists before the first side effect";
    EXPECT_GE(seenAtEffect->appliedOffset, 0);
}

TEST(CatalogApplierRegression, BootReplaysExactlyTheCheckpointedPrefix) {
    CatalogRig rig("ap_boot");
    rig.appendEvents(0, 10);
    rig.applier.applyCommitted(rig.replica());
    const uint64_t digest = rig.applier.state().digest();
    rig.restart();                                      // same dir: log recovered, fresh applier + registries
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), /*recoveryInstalls=*/true), 10);
    EXPECT_EQ(rig.applier.state().digest(), digest);
    EXPECT_TRUE(std::all_of(rig.rec.viewInstallRecovery.begin(), rig.rec.viewInstallRecovery.end(),
                            [](bool r) { return r; })) << "boot installs are recovery installs";
}

TEST(CatalogApplierRegression, BootDigestDisagreeingWithTheCheckpointHalts) {
    CatalogRig rig("ap_dmg");
    rig.appendEvents(0, 4);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {3, 0xBAD}));
    rig.applier.replayLocal(rig.replica(), true);
    EXPECT_TRUE(rig.applier.halted());
}

TEST(CatalogApplierRegression, AlterViewEffectsRunInstallBeforeUninstallAndRetireTheOldSink) {
    CatalogRig rig("ap_view");
    rig.appendEvents(0, 11);                            // through golden AlterView top_players (offset 10)
    rig.applier.applyCommitted(rig.replica());
    const auto& seq = rig.rec.sequence;                 // e.g. "topicApplied:4", "viewInstalled:4", …
    auto at = [&](const std::string& s) { return std::find(seq.begin(), seq.end(), s) - seq.begin(); };
    EXPECT_LT(at("topicApplied:5"), at("viewInstalled:5"));
    EXPECT_LT(at("viewInstalled:5"), at("viewUninstalled:4"));
    EXPECT_LT(at("viewUninstalled:4"), at("topicDropped:4"));
}

TEST(CatalogApplierRegression, SteadyIdleApplyAllocatesNothing) {
    CatalogRig rig("ap_alloc");
    rig.appendEvents(0, 18);
    rig.applier.applyCommitted(rig.replica());
    rune::test::AllocCounter ac;                        // libs/node/tests support (used by HotPathBAllocRegressionTest)
    for (int i = 0; i < 1000; ++i) (void)rig.applier.applyCommitted(rig.replica());
    EXPECT_EQ(ac.count(), 0u);
}
```

`CatalogRig` is a file-local fixture (log under a `TmpDir`, the replica, `SchemaRegistry`, `ViewRegistry`, a `Recorder` whose `CatalogEffects` lambdas push into vectors and `sequence` strings) — written once in this file, reused in Task 8's `CatalogEffectsTest.cpp` by moving it to `libs/node/tests/support/CatalogRig.h` there.

- [ ] **Step 2: Run to verify they fail**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_node_tests`
Expected: compile error (`rune/node/CatalogApplier.h` not found).

- [ ] **Step 3: Implement** as specified. `CatalogApplier` holds the state machine `std::variant<Applying, Halted>`; `nextOffset()` returns `std::get<Applying>(st_).nextOffset` or −1. The production `CatalogEffects` (built in `AdminWorkerAgent`) encodes the `kA2c*` messages; `schemaChanged` → `kA2cSchemaUpdate` (existing) when `validate`; `viewInstalled` → `kA2cViewPlanUpdate` (existing, `recovery` flag); a full ring is the existing backlog path (`schemaBacklog_` / `topicCommittedBacklog_` pattern: preallocated, flushed next pass, never dropped).

- [ ] **Step 4: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_node_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogApplier|SchemaRegistry|ViewRegistry' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs/node CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(catalog): CatalogApplier — offset-ordered verified apply, halt on mismatch, checkpoint before effects

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Broker — control-thread effects: topics created and dropped by the catalog, config overrides, view uninstall, placement add/remove, C1 generalized

Spec §5.4: "When the controller sees a `CreateTopic` applied, it assigns that topic's partitions. A topic serves on a broker only when both: its `CreateTopic` is applied on that broker; its placement is committed." This task makes the control thread the executor of the applier's effects.

**Files:**
- Modify: `libs/gateway/include/rune/gateway/TopicRegistry.h`, `libs/gateway/src/TopicRegistry.cpp`:
  - `int32_t createTopicAt(int32_t topicId, const rune::node::TopicSpec& spec)` — creates the entry at exactly `topicId` (filling `topics_` up to it with `Dropped` tombstones if a gap exists: an id whose create this broker never saw because it was created **and** dropped before this broker's checkpoint is a `Dropped` slot), creates `dataDir/topics/<name>/<p>/` for a disk topic, opens + recovers each `Log`; idempotent for the same id + name.
  - `bool dropTopic(int32_t topicId)` — entry → `Dropped` (no partitions served, name freed from `nameIndex_`); the `Log`s stay owned until `releaseDroppedLogs(topicId)`.
  - `enum class TopicState : uint8_t { Unknown, Serving, Dropped }; TopicState state(int32_t topicId) const noexcept;`
  - `servableLog()` keeps its name and also returns `nullptr` for `Dropped`. The old runtime path (`createTopic`, `commitTopic`, `awaitingCommit`, `markTopicCommitted`, the `committed` flag, the topic-id markers) stays until Task 9 removes its last caller — a catalog-applied entry is created committed (C1 becomes structural: an entry exists only once applied).
- Modify: `libs/node/src/BrokerNode.cpp` (control drain of `kA2cCatalogTopicApplied/Dropped/Config`, `kA2cViewUninstall`, `kA2cCatalogHalted`, `kA2cCatalogApplied`; `createRuntimeUserReplicas` called from the applied handler; `dropRuntimeTopic_(topicId)`; `PendingCreateBehindDrop` queue; the dropped-dir rename + purge post)
- Modify: `libs/node/include/rune/node/MetadataService.h` (`removeRuntimeTopic(int32_t topicId)` — removes it from `configuredTopics_` and, when controller-ready, appends one `RemoveTopicPlacementRecord`)
- Modify: `libs/metadata/include/rune/metadata/MetadataExtensions.h` (`kRemoveTopicPlacementRecordType = 0x8003`: `[0..1] type [2] version=0 [3..6] topicId`), `libs/metadata/src/MetadataStateMachine.cpp` (apply it: at commit, erase every `(topicId, p)` assignment; `LocalRoleDelta.deletes` lists them)
- Modify: `libs/node/include/rune/node/LogCleanerAgent.h`, `libs/node/include/rune/node/CleanerViewChannel.h` (`kPolicyMsgType = 3` on the `view` ring: `[topicId:i32][count:u16][(key:u8,value:i64)*]`; the cleaner keeps a `std::unordered_map<int32_t, PolicyOverride>` reserved at start and consults it before `replica->retentionMs()` & co. in `runRetention` / the compaction gates)
- Modify: `libs/node/include/rune/node/WorkerViewSet.h`, `libs/node/src/WorkerViewSet.cpp` (`uninstallView(int32_t viewTopicId)`: drops its instances, recovery templates and dirty state on the worker's own thread), `libs/node/include/rune/node/WorkerOp.h` / the view-install ring encoding (`ViewPlanUpdateMsg` gains `uninstall`)
- Modify: `libs/gateway/src/ClientGateway.cpp` (C1 generalized: produce / subscribe / metadata for a topic id this broker has not applied → `NotLeader (2)` when the MSM holds a committed placement for it, `UnknownTopic (7)` otherwise; `Dropped` → `UnknownTopic`)
- Modify: `libs/node/include/rune/node/AdminChannels.h` (`kMsgPurgeDroppedDir = 16` on the control → admin ring — the former create-topic reply ring, renamed `controlToAdminRing()`)
- Test: `libs/node/tests/node/CatalogEffectsTest.cpp`, `libs/gateway/tests/gateway/CatalogGatewayTest.cpp`, `libs/metadata/tests/MetadataStateMachineTest.cpp` (append), `libs/node/tests/node/LogCleanerPolicyOverrideTest.cpp`, `libs/node/tests/node/WorkerViewSetTest.cpp` (append)

**Interfaces:**
- Consumes: Task 7's messages.
- Produces: `TopicRegistry::{createTopicAt, dropTopic, releaseDroppedLogs, state}`, `MetadataService::removeRuntimeTopic`, `WorkerViewSet::uninstallView`, `BrokerNode::catalogTopicServesForTest(topicId)`.

The drop sequence on the control thread (one explicit state per step):
1. `kA2cCatalogTopicDropped{id, name}` → `TopicRegistry::dropTopic(id)` (serves nothing from now on: gateway answers `UnknownTopic`), `metadataService_->removeRuntimeTopic(id)`, `replicaRegistry_->remove(id, p)` for every partition, transient ring freed with the replica; record `DroppedTopic{id, name, retireGen = replicaRegistry_->generation()}` in `droppedAwaitingRetire_`.
2. Each pass: when every worker and the cleaner/provisioner acked a generation ≥ `retireGen` (the same condition `collectRetired` uses), `TopicRegistry::releaseDroppedLogs(id)` closes the logs, then `rename(topics/<name>, dropped/<id>.<name>)` (atomic; a failed rename is logged at error and retried next pass — never deleted in place) and post `kMsgPurgeDroppedDir{path}` to the admin thread, which `std::filesystem::remove_all`s it (cold; a failure is logged and the dir stays for the boot purge, Task 12).
3. A `kA2cCatalogTopicApplied` whose name is still held by an entry of `droppedAwaitingRetire_` → `PendingCreateBehindDrop{spec}` queued; it runs (step `createTopicAt` + `createRuntimeUserReplicas` + `addRuntimeTopic`) right after that name's rename succeeded. FIFO per name.

- [ ] **Step 1: Write the failing tests.** `CatalogEffectsTest.cpp` (a single-node `BrokerNode` with the catalog wired and a test hook `BrokerNode::postCatalogEffectForTest(frame)` that encodes onto the admin→control ring from the admin thread via `onAdminThreadForTest`):

```cpp
TEST(CatalogEffectsRegression, AppliedTopicIsCreatedAtItsCatalogIdAndPlaced) {
    SingleNode n("eff_create");
    n.applyCreateTopic(/*topicId=*/0, "events", /*partitions=*/2);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    EXPECT_EQ(n.topicIdForName("events"), 0);
    EXPECT_TRUE(n.hasPlacement(0, 0) && n.hasPlacement(0, 1));
}

TEST(CatalogEffectsRegression, CreateBehindDropWaitsForTheDirRenameAndStartsEmptyRegression) {
    SingleNode n("eff_reuse");
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    n.produceSync(0, 0, "old-record");
    n.applyDropTopic(0, "t");
    n.applyCreateTopic(1, "t", 1);                     // same name, immediately
    ASSERT_TRUE(n.awaitServes(1, 10s));
    EXPECT_EQ(n.logEndOffset(1, 0), 0) << "the reused name starts EMPTY";
    EXPECT_FALSE(std::filesystem::exists(n.dataDir() / "topics" / "t" / "0" / "00000000000000000000.log") &&
                 n.logEndOffset(1, 0) != 0);
    EXPECT_TRUE(n.awaitPathGone(n.dataDir() / "dropped" / "0.t", 10s)) << "purged by the admin thread";
}

TEST(CatalogEffectsRegression, DroppedTopicAnswersUnknownTopicAtOnce) {
    SingleNode n("eff_drop");
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    n.applyDropTopic(0, "t");
    ASSERT_TRUE(n.awaitTopicState(0, rune::gateway::TopicRegistry::TopicState::Dropped, 10s));
    EXPECT_EQ(n.produceErrorCode(0, 0), static_cast<int16_t>(rune::protocol::client::ErrorCode::UnknownTopic));
}

TEST(CatalogEffectsRegression, ConfigChangeReachesTheCleanerNotTheReplica) {
    SingleNode n("eff_cfg");
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    n.applyTopicConfig(0, {{rune::catalog::ConfigKey::RetentionMs, 1}});
    EXPECT_TRUE(n.awaitCleanerOverride(0, /*retentionMs=*/1, 10s));
}
```

`CatalogGatewayTest.cpp`:

```cpp
TEST(CatalogGatewayRegression, PlacedButNotYetAppliedTopicAnswersNotLeaderRegression) {
    GatewayRig g;                                      // existing gateway unit rig + a fake MSM placement view
    g.placeWithoutApply(/*topicId=*/3, /*partitions=*/1);
    EXPECT_EQ(g.produce(3, 0).errorCode, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader));
    EXPECT_EQ(g.subscribeError(3, 0), rune::protocol::client::ErrorCode::NotLeader);
}
TEST(CatalogGatewayRegression, NeverPlacedUnknownTopicAnswersUnknownTopic) {
    GatewayRig g;
    EXPECT_EQ(g.produce(9, 0).errorCode, static_cast<int16_t>(rune::protocol::client::ErrorCode::UnknownTopic));
}
TEST(CatalogGatewayRegression, AppliedButNotPlacedAnswersNotLeader) {
    GatewayRig g;
    g.applyWithoutPlacement(2, 1);
    EXPECT_EQ(g.produce(2, 0).errorCode, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader));
}
```

Plus: `MetadataStateMachineTest` — `RemoveTopicPlacementRecordErasesEveryPartitionAtCommit` (pending until commit; the delta lists every self replica as a delete; an unknown topic id is a no-op); `LogCleanerPolicyOverrideTest` — `OverrideRetentionDeletesSegmentsTheReplicaConfigWouldKeep`; `WorkerViewSetTest` — `UninstallViewStopsFoldingAndFreesItsInstances` and `UninstallUnknownViewIsANoOp`.

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_node_tests rune_gateway_tests rune_metadata_tests`
Expected: compile errors (`createTopicAt` / `uninstallView` / `removeRuntimeTopic` missing).

- [ ] **Step 3: Implement** the edits above. In `BrokerNode`'s admin→control drain (anchor `if (tid == kA2cTopicCommitted) {`), add the new `tid` branches before the dispatcher; `kA2cCatalogTopicApplied` builds a `TopicSpec` from the message and runs `topicRegistry_->createTopicAt(id, spec)` + `createRuntimeUserReplicas(spec)` (unchanged body, it already registers the topic with `MetadataService::addRuntimeTopic`) inside the existing `try`/`catch` (a failure logs the error and leaves the topic not serving — produce answers `NotLeader` — exactly the current `commitReplicaFault_` behaviour). The gateway's new rule is additive here (the old `awaitingCommit` check stays for the old write path until Task 9).

- [ ] **Step 4: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogEffects|CatalogGateway|MetadataStateMachine|LogCleanerPolicyOverride|WorkerViewSet|TopicServability|TransientGateway|TransientInteractionMatrix|TopicRegistry' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(catalog): control-thread effects — create/drop at catalog ids, placement add/remove, config override, view uninstall; C1 generalized

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint C (controller):** full broker `*Regression*` suite (the old `__topic_schemas` write path still serves DDL; the catalog log, apply and effects are proven next to it).

---
### Task 9: Broker — the write path through the catalog leader's AdminWorker (every DDL becomes an event)

Spec §5.1: on the leader, the single AdminWorker validates against its applied catalog, assigns `objectId` and versions, computes the fingerprint, appends; once committed it applies locally and answers OK (`CreateTopic`'s OK held until the topic serves — `DdlAnswerGate`, unchanged). This task replaces the `requestCreateTopic` round-trip and the `__topic_schemas`/`__views` appends with one event per DDL, adds the four new DDL ops end to end (gateway → admin), validates `replicationFactor` (spec §8), records the principal and statement, and deduplicates retries by `requestId` (ruling 9). After this task nothing writes `__topic_schemas` or `__views` (Task 16 deletes them).

**Files:**
- Modify: `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp` (rewrite of the DDL state machine; see Interfaces)
- Modify: `libs/node/include/rune/node/AdminChannels.h` (gateway → admin: `kMsgDropTopic = 10`, `kMsgDropView = 11`, `kMsgAlterView = 12`, `kMsgAlterTopicConfig = 13`; every DDL message gains `DdlMeta`; **delete** `kMsgCreateTopicReq`, `kMsgCreateTopicReply`, `CreateTopicReqMsg`, `CreateTopicReplyMsg` and their codecs)
- Delete: `libs/node/include/rune/node/AdminCreateTopic.h`
- Modify: `libs/gateway/include/rune/gateway/IAdminRequestSink.h` (every submit takes a `DdlMetaView`; four new submits), `libs/gateway/include/rune/gateway/ClientGateway.h`, `libs/gateway/src/ClientGateway.cpp` (`handleDropTopic`, `handleDropView`, `handleAlterView`, `handleAlterTopicConfig`; template dispatch 74–77; the extension decoded on 64/68/70)
- Modify: `libs/node/include/rune/node/DdlAnswerGate.h` (`gatedTopicOf` also gates a `DdlResponse` with `errorCode == 0 && gateTopicId >= 0`)
- Modify: `libs/node/src/BrokerNode.cpp` (`buildAdminWorker`: the `AdminDeps` lose `requestCreateTopic`/`topicCommitted`, gain `respondDdl`, `respondError`, `brokerCount`, `topicMinIsr`; the create-request drain (anchor `// (2) Drain create-topic requests (admin → control).`) is deleted; the admin thread calls `applier.replayLocal(...)` once before its first `doWork`)
- Modify: `libs/gateway/include/rune/gateway/TopicRegistry.h`, `libs/gateway/src/TopicRegistry.cpp` (delete the runtime `createTopic`, `commitTopic`, `awaitingCommit`, `markTopicCommitted`, the `committed` flag and the topic-id marker helpers — their last caller is gone)
- Port (they exercised the deleted round-trip): `libs/node/tests/node/AdminWorkerTest.cpp`, `libs/node/tests/node/DdlFinalFixRegressionTest.cpp`, `libs/node/tests/integration/SchemaGatewayTest.cpp`, `CreateViewRegressionTest.cpp`, `GetViewRowRegressionTest.cpp`, `SchemaPersistenceTest.cpp` (now "a schema survives restart through `__catalog`"), `libs/gateway/tests/gateway/TopicServabilityTest.cpp`, `TransientGatewayTest.cpp`, `TransientInteractionMatrixTest.cpp` (C1 precondition "uncommitted" → "applied but not placed", same assertions; `TransientMatrixExpectations.h` unchanged)
- Create: `libs/node/tests/support/CatalogDdlRig.h` (one AdminWorker + catalog leader replica with ISR {self} + applier + recording deps; reused by Tasks 10, 11, 20)
- Test: `libs/node/tests/node/CatalogWritePathTest.cpp` (in `rune_node_tests`)

**Interfaces:**
- Consumes: `rune::catalog::{CatalogState, previewFingerprint, encodeInto, RequestRecord}`, `CatalogApplier` (Task 7), the catalog replica (Task 6), `rune::view::{deserializeAuthored, serializeAuthored, compileView, serialize}` (broker `libs/view`).
- Produces:
  ```cpp
  // AdminChannels.h
  struct DdlMeta { std::string statement, clientName, sourceAddress; int64_t sessionId = 0; uint64_t requestId = 0;
                   bool hasReplayedFrom = false; std::string replayedClusterId; int64_t replayedOffset = -1;
                   std::string replayedPrincipal; };
  struct DropTopicMsg        { int64_t session, corr; std::string name; DdlMeta meta; };
  struct DropViewMsg         { int64_t session, corr; std::string name; DdlMeta meta; };
  struct AlterViewMsg        { int64_t session, corr; std::string name, keyField; uint8_t persistence;
                               uint32_t expectedVersion; std::vector<uint8_t> planBlob; DdlMeta meta; };
  struct AlterTopicConfigMsg { int64_t session, corr; std::string name; uint32_t expectedVersion;
                               std::vector<rune::catalog::ConfigChange> changes; DdlMeta meta; };
  // DefineTopicMsg gains: int16_t replicationFactor; rune::catalog::TopicConfig config; DdlMeta meta;
  // AlterTopicMsg / CreateViewMsg gain: DdlMeta meta;
  // AdminWorker.h
  struct CatalogFollower         { int32_t leaderId = -1; int32_t epoch = 0; };
  struct CatalogLeaderCatchingUp { int32_t epoch = 0; };
  struct CatalogLeaderServing    { int32_t epoch = 0; };
  using CatalogRole = std::variant<CatalogFollower, CatalogLeaderCatchingUp, CatalogLeaderServing>;
  using DdlRequest  = std::variant<DefineTopicMsg, AlterTopicMsg, CreateViewMsg, DropTopicMsg, DropViewMsg,
                                   AlterViewMsg, AlterTopicConfigMsg>;
  struct Idle {};
  struct AwaitingCommit { DdlRequest req; rune::catalog::CatalogEvent event; int64_t offset = -1; int64_t startedNs = 0; };
  using InFlight = std::variant<Idle, AwaitingCommit>;
  struct DdlRejection { uint16_t code; std::string detail; int32_t topicId = -1; uint16_t version = 0; };
  void submit(DdlRequest req);                                   // replaces submitDefineTopic/AlterTopic/CreateView
  [[nodiscard]] uint64_t answeredCount() const noexcept;          // matrix invariant 2
  [[nodiscard]] const CatalogApplier& applier() const noexcept;
  [[nodiscard]] std::string_view roleName() const noexcept;       // "Follower" | "LeaderCatchingUp" | "LeaderServing"
  // AdminDeps (replaces requestCreateTopic + topicCommitted):
  std::function<void(int64_t session, int64_t corr, uint16_t code, uint64_t objectId, uint32_t newVersion,
                     int32_t gateTopicId, std::string_view detail)> respondDdl;          // DdlResponse (78)
  std::function<void(int64_t session, int64_t corr, uint16_t code, std::string_view message)> respondError;  // ErrorResponse
  int32_t brokerCount = 1;     // config_.peers.size()
  int32_t topicMinIsr = 1;     // config_.gatewayMinIsr
  ```

**Kind mapping and validation** (`AdminWorker::buildEvent_(const DdlRequest&, const CatalogState&) → std::expected<CatalogEvent, DdlRejection>`; every rejection appends nothing and is answered at once in the request's own response type, or `ErrorResponse` when it carries a detail):

| Request | Rejections (code) | Event |
|---|---|---|
| `DefineTopic` | bad persistence byte / partitions < 1 / malformed non-empty descriptor → `InvalidSchema 23`; name fails `isValidTopicName` → `InvalidTopicName 40`; a live name differing only in ASCII case (`TopicName.h` fold) → `TopicExists 25`; name is a view or a typed topic → `TopicExists 25` (+ its id/version); untyped topic + empty descriptor → `TopicExists 25`; untyped **disk** topic + explicit `transient` → `TopicExists 25` (final review I1); `replicationFactor > brokerCount` → `ErrorResponse{InvalidRequest 39, "replicationFactor R > broker count N"}`; `0 < replicationFactor < topicMinIsr` → `ErrorResponse{39, "replicationFactor R < minIsr M"}`; `segmentRollBytes > Log::kMaxRollBytes`, ratio outside `[0, 1e6]`, `cleanupPolicy > 2`, `durability > 1`, `tier > 2`, retention < −1 → `ErrorResponse{39, "<field> …"}` | free name → `CreateTopic{topicId = nextTopicId, …, schema iff descriptor non-empty}`, `objectId = nextObjectId`, `0 → 1`; untyped topic + descriptor → `CreateSchema` on that object |
| `AlterTopic` | as today's `startAlter_`: view name → `ViewNotAlterable 38`; unknown → `UnknownTopic 7`; untyped → `InvalidSchema 23`; stale `expectedVersion` (schema version) → `SchemaConflict 31`; bad ops → `InvalidSchema 23`; compat gate → `SchemaIncompatible 30`; `applyOps` error → `InvalidSchema 23` | all ops `SET COMPATIBILITY` → `SetCompatibility`; else `AlterSchema{opsBlob}` |
| `CreateView` | as today's `startCreateView_` (codes 23, 40, 32, 25, 34, 41, 33, 37, 35/34, 36) | `CreateView{viewTopicId = nextTopicId, sinkPartitions = source's, sourceNames, normalizedPlanBlob, compiledPlanBlob (viewTopicId stamped), outputDescriptorBlob}` |
| `AlterView` | unknown view → `UnknownTopic 7` (detail "no view named …"); stale `expectedVersion` (object version) → `SchemaConflict 31`; plan errors as `CreateView` | `AlterView` with a fresh `viewTopicId = nextTopicId` |
| `DropView` | unknown → `UnknownTopic 7` (detail) | `DropView{viewTopicId}` |
| `DropTopic` | unknown → `UnknownTopic 7`; a view name → `ErrorResponse{39, "'v' is a view; use DROP VIEW"}`; read by view `v` → `ErrorResponse{39, "topic 't' is the source of view 'v'"}` | `DropTopic{topicId}` |
| `AlterTopicConfig` | unknown → `UnknownTopic 7`; a view → `ViewNotAlterable 38`; stale `expectedVersion` (object version, 0 = unconditional) → `SchemaConflict 31`; empty / duplicate keys / out-of-range values → `ErrorResponse{39, detail}` | `AlterTopicConfig{changes}` |

`normalizedPlanBlob` = `rune::view::serializeAuthored` of the authored plan after replacing each `sources[i].topicId` with `i` (ruling 11). The event's `statement`, `principal{sessionId, clientName, sourceAddress, requestId}`, `replayedFrom`, `timestampMs = system_clock now` come from `DdlMeta`; then `fingerprint = previewFingerprint(state, event)` (a failure here is an internal bug → answered `Internal 6`, logged at error, nothing appended).

**Steps of one DDL on the leader** (`startNext_`, `advance_`):
1. Role gate: `CatalogFollower` / `CatalogLeaderCatchingUp` → answer `NotLeader 2` (retriable; Task 11 makes the gateway redirect before the request reaches here); applier `Halted` → `Internal 6`.
2. Dedupe: `meta.requestId != 0 && state.request(requestId)` → answer that record's result (same response type, `errorCode 0`); a request whose `requestId` equals the in-flight event's waits in `pending_` and is answered after it.
3. `buildEvent_` → reject, or encode into the reused `eventBuf_` and `catalogReplica_->appendKeyed(key = objectId LE 8 bytes, eventBuf_, acksAll = true)`; `< 0` (minIsr not met) → `NotLeader 2` (spec §5.5), nothing in flight.
4. `AwaitingCommit{req, event, offset}`; `catalogReplica_->onLeaderLogAppended()`.
5. Each `doWork`: plane poll → `catalogReplica_->doWork` → `applier_.applyCommitted(*catalogReplica_)` → `advance_`: `applier_.nextOffset() > offset` → answer OK from `state.request(requestId)` (or, for `requestId 0`, from the `Applied` the applier recorded for that offset): `DefineTopicResponse{topicId, schemaVersion}` / `AlterTopicResponse{topicId, schemaVersion}` / `CreateViewResponse{viewTopicId, outputFingerprint}` / `DdlResponse{objectId, newVersion, gateTopicId}` (`AlterView`: the new sink; drops / config: −1); applier halted at or before `offset` → `Internal 6`; `now − startedNs > kDdlTimeoutNs` → `Internal 6` (error bound, unchanged).
6. Demotion while `AwaitingCommit` → answer `NotLeader 2` (spec §5.2: explicit, retriable — today it is `Internal`); the retry dedupes on the new leader if the event committed.

- [ ] **Step 1: Write the failing tests** — `CatalogDdlRig.h` (support) + `CatalogWritePathTest.cpp`:

```cpp
// Catalog history §5.1/§8: every DDL is one event, validated against the applied
// catalog, answered after commit + local apply; rf bounds are explicit errors.
#include "support/CatalogDdlRig.h"
#include <rune/catalog/CatalogQueries.h>
#include <gtest/gtest.h>

using rune::node::test::CatalogDdlRig;
namespace rc = rune::catalog;

TEST(CatalogWritePathRegression, DefineTopicIsOneCreateTopicEventWithPrincipalAndStatement) {
    CatalogDdlRig rig("wp_define", /*brokers=*/3);
    rig.becomeServingLeader();
    const auto a = rig.define({.name = "players", .partitions = 3, .typed = true, .rf = 3,
                               .statement = "topic players {...} partitions 3 replicas 3;",
                               .clientName = "cli", .requestId = 11});
    EXPECT_EQ(a.code, 0); EXPECT_EQ(a.topicId, 0); EXPECT_EQ(a.version, 1);
    const auto log = rig.recordedLog();
    ASSERT_EQ(log.size(), 1u);
    EXPECT_EQ(log[0].event.kind, rc::EventKind::CreateTopic);
    EXPECT_EQ(log[0].event.statement, "topic players {...} partitions 3 replicas 3;");
    EXPECT_EQ(log[0].event.principal.clientName, "cli");
    EXPECT_EQ(log[0].event.principal.requestId, 11u);
    EXPECT_EQ(log[0].event.principal.sessionId, rig.sessionId());
    EXPECT_FALSE(log[0].event.principal.sourceAddress.empty());
}

TEST(CatalogWritePathRegression, EveryDdlMapsToItsKind) {
    CatalogDdlRig rig("wp_kinds", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "u", .partitions = 1, .typed = false}).code, 0);           // CreateTopic
    ASSERT_EQ(rig.define({.name = "u", .partitions = 1, .typed = true}).code, 0);            // CreateSchema
    ASSERT_EQ(rig.alterAdd("u", "mana").code, 0);                                            // AlterSchema
    ASSERT_EQ(rig.alterSetCompat("u", rune::codec::CompatMode::Forward).code, 0);            // SetCompatibility
    ASSERT_EQ(rig.alterConfig("u", {{rc::ConfigKey::RetentionMs, 1000}}).code, 0);           // AlterTopicConfig
    ASSERT_EQ(rig.createView("v", "u").code, 0);                                             // CreateView
    ASSERT_EQ(rig.alterView("v", "u", /*extraColumn=*/true).code, 0);                        // AlterView
    ASSERT_EQ(rig.dropView("v").code, 0);                                                    // DropView
    ASSERT_EQ(rig.dropTopic("u").code, 0);                                                   // DropTopic
    std::vector<rc::EventKind> kinds;
    for (const auto& r : rig.recordedLog()) kinds.push_back(r.event.kind);
    EXPECT_EQ(kinds, (std::vector<rc::EventKind>{rc::EventKind::CreateTopic, rc::EventKind::CreateSchema,
        rc::EventKind::AlterSchema, rc::EventKind::SetCompatibility, rc::EventKind::AlterTopicConfig,
        rc::EventKind::CreateView, rc::EventKind::AlterView, rc::EventKind::DropView, rc::EventKind::DropTopic}));
}

TEST(CatalogWritePathRegression, ReplicationFactorBoundsAreExplicitErrorsRegression) {
    CatalogDdlRig rig("wp_rf", /*brokers=*/3, /*topicMinIsr=*/2);
    rig.becomeServingLeader();
    auto hi = rig.define({.name = "a", .partitions = 1, .rf = 4});
    EXPECT_EQ(hi.code, 39); EXPECT_EQ(hi.detail, "replicationFactor 4 > broker count 3");
    auto lo = rig.define({.name = "b", .partitions = 1, .rf = 1});
    EXPECT_EQ(lo.code, 39); EXPECT_EQ(lo.detail, "replicationFactor 1 < minIsr 2");
    EXPECT_EQ(rig.define({.name = "c", .partitions = 1, .rf = 0}).code, 0) << "0 = every broker";
    EXPECT_EQ(rig.define({.name = "d", .partitions = 1, .rf = 2}).code, 0);
    EXPECT_EQ(rig.recordedLog().size(), 2u) << "rejected DDL appends nothing";
}

TEST(CatalogWritePathRegression, DuplicateRequestIdAnswersTheOriginalResultRegression) {
    CatalogDdlRig rig("wp_dedupe", 3);
    rig.becomeServingLeader();
    const auto first  = rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = 99});
    const auto second = rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = 99});
    EXPECT_EQ(second.code, 0) << "a retry of a committed DDL is its original success, not TopicExists";
    EXPECT_EQ(second.topicId, first.topicId);
    EXPECT_EQ(second.version, first.version);
    EXPECT_EQ(rig.recordedLog().size(), 1u);
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = 100}).code, 25);
}

TEST(CatalogWritePathRegression, DropRulesAreExplicit) {
    CatalogDdlRig rig("wp_drop", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "s", .partitions = 1, .typed = true}).code, 0);
    ASSERT_EQ(rig.createView("v", "s").code, 0);
    auto inUse = rig.dropTopic("s");
    EXPECT_EQ(inUse.code, 39); EXPECT_EQ(inUse.detail, "topic 's' is the source of view 'v'");
    auto isView = rig.dropTopic("v");
    EXPECT_EQ(isView.code, 39); EXPECT_EQ(isView.detail, "'v' is a view; use DROP VIEW");
    EXPECT_EQ(rig.dropTopic("nope").code, 7);
    EXPECT_EQ(rig.dropView("nope").code, 7);
}

TEST(CatalogWritePathRegression, BelowMinIsrIsNotLeaderAndAppendsNothing) {
    CatalogDdlRig rig("wp_minisr", 3);
    rig.becomeServingLeader(/*isr=*/{1});                 // ISR below the catalog minIsr (2)
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1}).code, 2);
    EXPECT_EQ(rig.recordedLog().size(), 0u);
}

TEST(CatalogWritePathRegression, DemotionMidDdlAnswersNotLeaderNotInternal) {
    CatalogDdlRig rig("wp_demote", 3);
    rig.becomeServingLeader(/*isr=*/{1, 2});              // follower 2 never fetches: the event stays uncommitted
    const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1});
    rig.becomeFollower(/*leaderId=*/2);
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->code, 2);
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
}

TEST(CatalogWritePathRegression, AnswerComesAfterLocalApplyNotJustCommit) {
    CatalogDdlRig rig("wp_order", 3);
    rig.becomeServingLeader();
    std::optional<int64_t> appliedAtAnswer;
    rig.onAnswer([&] { appliedAtAnswer = rig.admin().applier().state().appliedOffset(); });
    ASSERT_EQ(rig.define({.name = "t", .partitions = 1}).code, 0);
    ASSERT_TRUE(appliedAtAnswer.has_value());
    EXPECT_EQ(*appliedAtAnswer, 0);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_node_tests`
Expected: compile error (`support/CatalogDdlRig.h` / `AdminWorker::submit` missing).

- [ ] **Step 3: Implement** the AdminWorker rewrite, the gateway handlers (each mirrors `handleDefineTopic`: decode → `rejectRequest_(InvalidRequest)` on an undecodable body → known session → `adminSink_->submitX(...)` with the `DdlMetaView{statement, clientName, requestId, replayedFrom…, sessionId, sourceAddress = session.egressChannel}`; ring full → `Internal 6`, as today), the `AdminDeps` wiring in `BrokerNode`, the deletions. `startedCount()` / `answeredCount()` are incremented at exactly one place each (`startNext_` dequeue; the single `answer_(…)` helper every exit uses).

- [ ] **Step 4: Port the listed tests** to the catalog path (same scenarios and assertions where the behaviour is unchanged; a scenario that only existed for the deleted round-trip — e.g. "control thread reports `existed`" — becomes the catalog-state equivalent, named the same with `Catalog` prefixed). Their orphan cases go in Task 10 and their `RUNE_TOPICS` set-up in Tasks 16–18 (their own tasks).

- [ ] **Step 5: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogWritePath|AdminWorker|DdlFinalFix|SchemaGateway|CreateView|GetViewRow|SchemaPersistence|TopicServability|TransientGateway|TransientInteractionMatrix|DdlAnswerGate' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(catalog): every DDL is a __catalog event written by the catalog leader; new Drop/AlterView/AlterTopicConfig ops; rf bounds; request-id dedupe

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Broker — port the DDL conformance suite (harness, occupant × op, pair × timing, stress) to the catalog

The DDL conformance plan's suites (`DdlHarness.h`, `DdlExpectations.h`, `DdlHarnessRegressionTest`, `DdlOccupantMatrixRegressionTest` 456 cells, `DdlPairTimingMatrixRegressionTest` 560 cells, `DdlStressRegressionTest`, `DdlConformanceBrokerRegressionTest`) drive the deleted create-topic round-trip, the `__orphan.<id>` placeholders and static `RUNE_TOPICS` occupants. Their **error-code expectations stay binding** — the catalog must answer every occupant × op cell exactly as before — so they are ported, not dropped.

**Files:**
- Modify: `libs/node/tests/support/DdlHarness.h` (built on `CatalogDdlRig`: one serving catalog leader; `ControlMode::Hold` becomes "hold the HW" — the follower in the ISR stops fetching — so pair × timing interleavings still land between append and commit)
- Modify: `libs/node/tests/support/DdlExpectations.h`:
  - Occupants: `SchemaLessDisk` / `SchemaLessTransient` are now **untyped topics created by DDL** (were static `RUNE_TOPICS`); `OrphanSink` is **deleted** (an uncommitted create no longer leaves anything behind — there is no orphan); every other occupant unchanged.
  - Ops: append `DropTopic`, `DropView`, `AlterView`, `AlterTopicConfig` (+ their expected codes per occupant, from the Task 9 table).
  - "†: detected by the control thread" rows are gone — every rejection is decided on the admin thread from the catalog state; `requestCreateTopic` call counts become "events appended" (0 for every rejection, 1 for an accepted op).
- Modify: `libs/node/tests/node/DdlHarnessRegressionTest.cpp`, `DdlOccupantMatrixRegressionTest.cpp`, `DdlPairTimingMatrixRegressionTest.cpp`, `DdlStressRegressionTest.cpp`, `libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp` (pins updated: occupant rows 38 → 36 (two `OrphanSink` rows removed), ops 12 → 16; cell count pin recomputed in the same test from the table, never hand-typed)

**Interfaces:**
- Consumes: `CatalogDdlRig` (Task 9).
- Produces: `rune::node::test::DdlHarness` with the same public API it has today minus `statics` (options) and `orphan…` helpers; `Op::{DropTopic, DropView, AlterView, AlterTopicConfig}`.

- [ ] **Step 1: Port the harness and table** as described; every former `OrphanSink` cell and every placeholder / marker / quarantine assertion is deleted with a one-line comment in the table file: `// OrphanSink removed (catalog history): a create whose event never committed leaves no topic, dir or id.`

- [ ] **Step 2: Add the new-op cells** — for each occupant, the expected code of `DropTopic` / `DropView` / `AlterView` / `AlterTopicConfig` exactly per the Task 9 table (e.g. `DropTopic` on `ViewPersisted` → 39, on `Free` → 7, on `TypedDisk` with no view → 0; `AlterTopicConfig` on a view → 38).

- [ ] **Step 3: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'Ddl' --output-on-failure -j14`
Expected: PASS — every occupant × op cell answers its pinned code; the stress (`DdlStressRegression`) still finds no divergence from its model.

- [ ] **Step 4: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs/node/tests
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(catalog): port the DDL conformance suite to the catalog write path (+ drop/alter-view/config ops; orphan occupant retired)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Broker — leadership and availability: Redirect, leader change, catch-up gate, below minIsr, halted

Spec §5.1 (Redirect), §5.2 (leader change), §5.5 (unavailable), §9 (halted → Internal). The gateway decides the DDL route from a control-thread value; the admin thread decides the rest from its role state.

**Files:**
- Create: `libs/gateway/include/rune/gateway/CatalogRoute.h`
- Modify: `libs/gateway/include/rune/gateway/ClientGateway.h`, `libs/gateway/src/ClientGateway.cpp` (`setCatalogRoute(CatalogRoute)`; every DDL handler (64, 68, 70, 74–77) consults `ddlRoute_()` before submitting; `GetTopicSchema` (66) and `GetViewRow` (72) are reads — served locally from the broker's projection, never redirected)
- Modify: `libs/node/src/BrokerNode.cpp` (compute `CatalogRoute` each control pass from the MSM assignment of `(−6, 0)` + `kA2cCatalogHalted`; publish on change only)
- Modify: `libs/node/src/AdminWorker.cpp` (role transitions: `onCatalogRole(leader)` → `CatalogLeaderCatchingUp`; `doWork` promotes to `CatalogLeaderServing` once `catalogReplica_->highWatermark() == catalogReplica_->logEndOffset()` (every record the new leader holds — including a previous epoch's tail — is committed: HW = min LEO over the ISR) **and** `applier.nextOffset() == highWatermark()` (all of it applied); a follower edge → `CatalogFollower`, an in-flight op → `NotLeader`; every transition logs `AdminWorker: __catalog role -> <roleName()> (epoch N)` at info — Task 24 finds the leader by it)
- Test: `libs/node/tests/node/CatalogLeadershipTest.cpp` (on `CatalogDdlRig` extended to 3 rigs over one `FilteringTransport` — Task 20 promotes this to `CatalogClusterHarness`), `libs/gateway/tests/gateway/CatalogGatewayTest.cpp` (append)

**Interfaces:**
- Produces:
  ```cpp
  // CatalogRoute.h (namespace rune::gateway) — control-thread value, read by the gateway on the same thread.
  struct RouteLocalLeader {};
  struct RouteRemote      { std::string leaderClientEndpoint; };
  struct RouteUnknown     {};                          // NO_LEADER / not yet placed
  struct RouteHalted      { int64_t offset; };          // this broker's applier halted
  using CatalogRoute = std::variant<RouteLocalLeader, RouteRemote, RouteUnknown, RouteHalted>;
  // ClientGateway
  void setCatalogRoute(CatalogRoute r) noexcept;
  ```
- DDL answers by route: `RouteHalted` → `ErrorResponse{Internal 6, "catalog apply halted at offset N"}` (spec §9 — never a redirect, matrix R2); `RouteRemote` → `ErrorResponse{Redirect 3, endpoint}`; `RouteUnknown` → `ErrorResponse{NotLeader 2}`; `RouteLocalLeader` → submit to the admin ring.

- [ ] **Step 1: Write the failing tests** — `CatalogLeadershipTest.cpp`:

```cpp
TEST(CatalogLeadershipRegression, NewLeaderAnswersNotLeaderUntilCaughtUpRegression) {
    CatalogTrio t("ld_catchup");                       // 3 rigs, FilteringTransport, leader 1
    t.commitEvents(1, 5);                              // 5 DDLs committed via broker 1
    t.isolate(3);                                      // 3 stops fetching but keeps its log
    t.commitEvents(1, 3);                              // 3 more, 3 misses them
    t.deisolate(3);
    t.moveCatalogLeadership(/*to=*/2);                 // 2 is in the ISR and has everything
    t.holdApply(2);                                    // test seam: 2's applier is paused
    EXPECT_EQ(t.admin(2).roleName(), "LeaderCatchingUp");
    EXPECT_EQ(t.define(2, {.name = "x", .partitions = 1}).code, 2) << "never validates against a stale catalog";
    t.releaseApply(2);
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    EXPECT_EQ(t.define(2, {.name = "x", .partitions = 1}).code, 0);
}

TEST(CatalogLeadershipRegression, RetryAfterLeaderChangeReturnsTheOriginalResultOnceRegression) {
    CatalogTrio t("ld_retry");
    t.holdHighWatermark(1);                            // 1 appends, the HW does not move yet
    const int64_t corr = t.submitDefineNoWait(1, {.name = "t", .partitions = 1, .typed = true, .requestId = 5});
    t.replicateTo({2, 3});                             // the event reaches 2 and 3 …
    t.moveCatalogLeadership(2);                        // … then 1 loses leadership before answering
    EXPECT_EQ(t.answerFor(1, corr)->code, 2);
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    const auto retry = t.define(2, {.name = "t", .partitions = 1, .typed = true, .requestId = 5});
    EXPECT_EQ(retry.code, 0);
    EXPECT_EQ(t.countEventsWithRequestId(5), 1u) << "no DDL lost or duplicated";
    EXPECT_TRUE(t.allDigestsEqual());
}

TEST(CatalogLeadershipRegression, BelowMinIsrEveryDdlIsNotLeaderAndProduceIsUnaffected) {
    CatalogTrio t("ld_minisr");
    ASSERT_EQ(t.define(1, {.name = "data", .partitions = 1}).code, 0);
    t.kill(2); t.kill(3);
    EXPECT_EQ(t.define(1, {.name = "more", .partitions = 1}).code, 2);
    EXPECT_EQ(t.admin(1).startedCount(), t.admin(1).answeredCount());
}
```

`CatalogGatewayTest.cpp` (append):

```cpp
TEST(CatalogGatewayRegression, NonLeaderRedirectsEveryDdlToTheCatalogLeader) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteRemote{"10.0.0.2:40200"});
    for (auto tmpl : g.allDdlTemplates()) {                    // 64, 68, 70, 74, 75, 76, 77
        const auto r = g.sendDdl(tmpl);
        EXPECT_EQ(r.code, rune::protocol::client::ErrorCode::Redirect) << tmpl;
        EXPECT_EQ(r.message, "10.0.0.2:40200");
        EXPECT_EQ(g.adminSubmits(), 0u);
    }
}
TEST(CatalogLeadershipRegression, HaltedBrokerAnswersDdlInternalNotRedirectRegression) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteHalted{41});
    const auto r = g.sendDdl(rune::protocol::client::TemplateId::DefineTopic);
    EXPECT_EQ(r.code, rune::protocol::client::ErrorCode::Internal);
    EXPECT_NE(r.message.find("offset 41"), std::string::npos);
}
TEST(CatalogGatewayRegression, UnknownCatalogLeaderIsNotLeader) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteUnknown{});
    EXPECT_EQ(g.sendDdl(rune::protocol::client::TemplateId::DropTopic).code, rune::protocol::client::ErrorCode::NotLeader);
}
TEST(CatalogGatewayRegression, SchemaReadsAreServedLocallyOnAFollower) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteRemote{"x:1"});
    EXPECT_EQ(g.sendGetTopicSchema("t").forwardedToAdmin, true) << "reads are never redirected";
}
```

- [ ] **Step 2: Run to verify they fail**, then **Step 3: implement** as specified. `BrokerNode` recomputes the route in `serviceAdminControl_` (control thread): halted (latched from `kA2cCatalogHalted`) → `RouteHalted`; else MSM `(−6,0)` leader == self → `RouteLocalLeader`; leader ≥ 0 → `RouteRemote{peer.clientChannel()}` (as `leaderClientEndpointFor_` builds it); else `RouteUnknown`. Single node → `RouteLocalLeader` once the consensus leader is self, else `RouteUnknown`.

- [ ] **Step 4: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogLeadership|CatalogGateway|CatalogWritePath|SchemaGateway' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(catalog): DDL Redirect to the catalog leader, NotLeader on leader change / catch-up / below minIsr, Internal when halted

Fix-on-sight: a non-leader admin answered DDL Internal (non-retriable) instead of a redirect.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Broker — catalog reads: Subscribe on `__catalog` from any broker, served by the admin thread

Spec §7: reads are client-side; the client consumes `__catalog` with the existing Subscribe from offset 0. The log is admin-thread-owned, so the gateway forwards a catalog Subscribe/Unsubscribe to the admin thread over the request ring, and the admin thread pushes `Records` frames through its egress ring (the path DDL answers already take). Ruling 10: the caught-up marker.

**Files:**
- Create: `libs/node/include/rune/node/CatalogReadServer.h`, `libs/node/src/CatalogReadServer.cpp`
- Modify: `libs/node/include/rune/node/AdminChannels.h` (`kMsgCatalogSubscribe = 14` `[session][corr][fromOffset]`, `kMsgCatalogUnsubscribe = 15` `[session][subscriptionId]`; session-close notice reuses the unsubscribe with `subscriptionId = -1` = all of that session)
- Modify: `libs/gateway/src/ClientGateway.cpp` (Subscribe with `topicId == kCatalogTopicId`: partition must be 0 → else `InvalidRequest`; forwarded; `SubscribeResponse` is sent by the admin thread; Unsubscribe of a catalog subscription id (ids from the admin's range, below) forwarded; session expiry posts the session-wide unsubscribe)
- Modify: `libs/node/src/AdminWorker.cpp` (`readServer_.pump()` in `doWork` after the applier)
- Test: `libs/node/tests/node/CatalogReadServerTest.cpp`

**Interfaces:**
- Produces:
  ```cpp
  class CatalogReadServer {
  public:
      struct Sub { int64_t session; int64_t subscriptionId; int64_t nextOffset; bool caughtUpSent; };
      CatalogReadServer(rune::replication::PartitionReplica& catalog, EgressFn send, std::size_t maxSubs = 64);
      void subscribe(int64_t session, int64_t corr, int64_t fromOffset);   // answers SubscribeResponse
      void unsubscribe(int64_t session, int64_t subscriptionId);           // -1 = every sub of the session
      int  pump();                                                         // push [nextOffset, HW) then the marker
      [[nodiscard]] std::size_t subscriptions() const noexcept;
  };
  inline constexpr int64_t kCatalogSubscriptionIdBase = int64_t{1} << 62;  // never collides with gateway ids
  ```
- Rules: `fromOffset` −2 (earliest) → 0; −1 (latest) → HW; an explicit offset above HW → HW; over `maxSubs` → `SubscribeResponse{errorCode = Internal}` with a log line (bounded, never silent). `pump()` reads at most `kCatalogReadChunk` per sub per pass into one **preallocated** buffer, never past the HW (followers serve their own HW — committed data only), encodes `Records{subscriptionId, topicId = −6, partition 0, baseOffset}` frames (whole batches, like `servePushRead_`), and after the backlog sends the empty-payload marker `Records{baseOffset = HW}` once; new commits are pushed as they arrive (no new marker). Egress full → the sub keeps its `nextOffset` and resumes next pass.

- [ ] **Step 1: Write the failing tests**:

```cpp
TEST(CatalogReadServerRegression, BacklogThenCaughtUpMarkerThenLiveEvents) {
    ReadRig r("rd_basic");                             // catalog leader replica (ISR {self}) + recorder egress
    r.commitEvents(4);
    r.server.subscribe(/*session=*/7, /*corr=*/1, /*from=*/-2);
    r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 1));
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{4}));
    r.commitEvents(1);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7).back(), 4);
    EXPECT_EQ(r.markers(7).size(), 1u) << "one marker per subscription";
}

TEST(CatalogReadServerRegression, FollowerServesOnlyUpToItsHighWatermarkRegression) {
    ReadRig r("rd_follower", /*asFollower=*/true);
    r.followerAppend(6);                               // follower log holds 6 records …
    r.followerSetHighWatermark(4);                     // … only 4 are known committed
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{4}));
}

TEST(CatalogReadServerRegression, SessionCloseFreesEverySubscription) {
    ReadRig r("rd_close");
    r.server.subscribe(7, 1, -2); r.server.subscribe(7, 2, -2);
    r.server.unsubscribe(7, -1);
    EXPECT_EQ(r.server.subscriptions(), 0u);
}

TEST(CatalogReadServerRegression, TooManySubscriptionsAreAnsweredNotDropped) {
    ReadRig r("rd_cap", false, /*maxSubs=*/2);
    r.server.subscribe(1, 1, -2); r.server.subscribe(2, 1, -2);
    r.server.subscribe(3, 1, -2);
    EXPECT_EQ(r.subscribeError(3, 1), rune::protocol::client::ErrorCode::Internal);
}

TEST(CatalogReadServerRegression, IdleSubscriptionsAllocateNothing) {
    ReadRig r("rd_alloc");
    r.commitEvents(3);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    rune::test::AllocCounter ac;
    for (int i = 0; i < 1000; ++i) (void)r.server.pump();
    EXPECT_EQ(ac.count(), 0u);
}
```

plus a gateway test `CatalogGatewayRegression.CatalogSubscribeIsForwardedOnAnyBrokerAndPartitionMustBeZero` (forwarded even with `RouteRemote`; partition 1 → `InvalidRequest`).

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement**, **Step 4: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogReadServer|CatalogGateway' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(catalog): __catalog readable with Subscribe on any broker (admin-thread read server, caught-up marker)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint D (controller):** full broker `*Regression*` suite (write path, conformance port, leadership, reads).

---
### Task 13: Client — `catalogHistory` / `catalogAsOf` / `catalogDiff` over the shared library, with a cached prefix

Spec §7: "the client consumes it, with the existing Subscribe from offset 0, and replays it with the shared library … the client caches the replayed prefix." Shared-nothing: the record handler runs on the runner thread and only copies records into an SPSC ring; the app thread owns the cache and the replayer.

**Files:**
- Modify: `rune-client-cpp/CMakeLists.txt` (`rune_catalog` from `${RUNE_CODEC_ROOT}/src/catalog/*.cpp`, next to `rune_codec`; `rune_client_api` links it)
- Modify: `rune-client-cpp/include/rune/client/TopicHandle.h` (`static constexpr TopicHandle catalog() noexcept { return {rune::protocol::client::kCatalogTopicId, 1}; }`), `include/rune/client/subscriber/Subscription.h` (`SubscribeOptions::onCaughtUp` — `std::function<void(int64_t highWatermark)>`, invoked on the runner thread for an empty-payload `Records` frame of a catalog subscription), `src/subscriber/SubscriberHub.cpp` (dispatch of that frame), `src/Client.cpp` (`subscribe` accepts `TopicHandle::catalog()` with partition 0)
- Create: `rune-client-cpp/include/rune/client/catalog/CatalogReader.h`, `src/catalog/CatalogReader.cpp`
- Modify: `rune-client-cpp/include/rune/client/Client.h` (the three API calls)
- Test: `rune-client-cpp/tests/catalog/CatalogReaderTest.cpp` (fake gateway pushing golden events), add to `CMakeLists.txt`

**Interfaces:**
- Consumes: `rune::catalog::{CatalogReplayer, RecordedEvent, history, stateAsOf, diff, HistoryFilter, AsOf, DiffEntry, ReplayError}`; `Client::subscribe`.
- Produces (namespace `rune::client`):
  ```cpp
  struct CatalogReadError { enum class Kind : uint8_t { Timeout, Replay, Closed } kind; std::string detail;
                            int64_t offset = -1; };
  template <class T> using CatalogResult = std::expected<T, CatalogReadError>;
  class CatalogReader {                     // app-thread object; one per Client (lazily created)
  public:
      explicit CatalogReader(Client&);
      CatalogResult<void> refresh(std::chrono::milliseconds timeout);   // subscribe from cache end → caught-up marker
      [[nodiscard]] std::span<const rune::catalog::RecordedEvent> events() const noexcept;
      [[nodiscard]] const rune::catalog::CatalogState& state() const noexcept;
  };
  // Client
  CatalogResult<std::vector<rune::catalog::RecordedEvent>> catalogHistory(const rune::catalog::HistoryFilter&,
                                                                          std::chrono::milliseconds timeout);
  CatalogResult<rune::catalog::CatalogState> catalogAsOf(const rune::catalog::AsOf&, std::chrono::milliseconds timeout);
  CatalogResult<std::vector<rune::catalog::DiffEntry>> catalogDiff(const rune::catalog::AsOf& a, const rune::catalog::AsOf& b,
                                                                   std::chrono::milliseconds timeout);
  ```
- `refresh`: subscribe `TopicHandle::catalog()` partition 0 `From::offset(cache.nextOffset())`; the handler copies `(offset, value)` into a `detail::OneToOneRingBuffer` (4 MiB, preallocated once per reader; a record larger than a quarter of it is split over a length-prefixed sequence of frames); `onCaughtUp(hw)` writes an end frame; the app thread drains, `feed`s each record into the cached `CatalogReplayer`, appends a `RecordedEvent`, stops at the end frame whose `hw ≤ nextOffset`, then unsubscribes. A replay error → `Replay` with the offset (the cache keeps the verified prefix; the next refresh retries from it — and fails the same way: explicit, never silently skipped). Deadline → `Timeout`.

- [ ] **Step 1: Write the failing tests** — `CatalogReaderTest.cpp`:

```cpp
TEST(CatalogReaderRegression, HistoryAsOfAndDiffMatchTheLibraryOverTheSameLog) {
    FakeCatalogBroker b;                                // InvokerGateway + a catalog Subscribe handler
    b.setLog(rune::catalog::testing::goldenScriptV1());
    auto client = b.connectClient();
    auto h = client->catalogHistory({.type = rune::catalog::ObjectType::Schema, .name = "players"}, 5s);
    ASSERT_TRUE(h.has_value()) << h.error().detail;
    EXPECT_EQ(h->size(), 4u);
    auto s = client->catalogAsOf({.by = rune::catalog::AsOf::By::Offset, .value = 5}, 5s);
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->topicByName("players")->schema->schemaVersion, 3);
    auto d = client->catalogDiff({.by = rune::catalog::AsOf::By::Offset, .value = 6},
                                 {.by = rune::catalog::AsOf::By::Offset, .value = 17}, 5s);
    ASSERT_TRUE(d.has_value());
    EXPECT_FALSE(d->empty());
}

TEST(CatalogReaderRegression, SecondCallFetchesOnlyTheNewTail) {
    FakeCatalogBroker b;
    auto log = rune::catalog::testing::goldenScriptV1();
    b.setLog({log.begin(), log.begin() + 10});
    auto client = b.connectClient();
    ASSERT_TRUE(client->catalogHistory({}, 5s));
    b.append({log.begin() + 10, log.end()});
    ASSERT_TRUE(client->catalogHistory({}, 5s));
    EXPECT_EQ(b.subscribeFromOffsets(), (std::vector<int64_t>{0, 10})) << "the replayed prefix is cached";
}

TEST(CatalogReaderRegression, TamperedEventIsAReplayErrorNamingItsOffset) {
    FakeCatalogBroker b;
    auto log = rune::catalog::testing::goldenScriptV1();
    log[4].fingerprint ^= 1;
    b.setLog(log);
    auto client = b.connectClient();
    auto h = client->catalogHistory({}, 5s);
    ASSERT_FALSE(h.has_value());
    EXPECT_EQ(h.error().kind, rune::client::CatalogReadError::Kind::Replay);
    EXPECT_EQ(h.error().offset, 4);
}

TEST(CatalogReaderRegression, NoCaughtUpMarkerIsATimeoutNotAHang) {
    FakeCatalogBroker b;
    b.setLog(rune::catalog::testing::goldenScriptV1());
    b.suppressCaughtUpMarker();
    auto client = b.connectClient();
    const auto t0 = std::chrono::steady_clock::now();
    auto h = client->catalogHistory({}, 500ms);
    ASSERT_FALSE(h.has_value());
    EXPECT_EQ(h.error().kind, rune::client::CatalogReadError::Kind::Timeout);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 2s);
}
```

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement**, **Step 4: Run**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-client-cpp/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'CatalogReader|Subscriber|OneToOneRingBuffer' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add include src tests CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(client): catalogHistory / catalogAsOf / catalogDiff over the shared replay library (cached prefix)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: rune-ql — `SHOW HISTORY`, `SHOW … AS OF`, `DIFF`, `DROP TOPIC|VIEW`, `ALTER TOPIC … SET`, `replicas N`, untyped topics

Spec §7 (QL column of the reads table) and §8 (`replicationFactor` on CreateTopic). Keywords are case-insensitive like the existing grammar's.

**Files:**
- Modify: `rune-ql/include/rune/ql/Ast.h`, `include/rune/ql/Parser.h`, `src/Parser.cpp`, `include/rune/ql/Backends.h`, `src/Backends.cpp`
- Create: `rune-ql/tests/CatalogStatementsTest.cpp`; modify `rune-ql/CMakeLists.txt`

**Interfaces:**
- Produces (namespace `rune::ql`):
  ```cpp
  // TopicDef gains: uint16_t replicas = 0; std::vector<ConfigSetting> with;
  //   (fields may be EMPTY → an untyped topic; then `key` is absent)
  struct ConfigSetting { std::string key; int64_t value; std::string text; };   // text: enum / string settings
  struct AlterTopicConfigDef { std::string name; std::vector<ConfigSetting> settings; };
  struct DropTopicDef { std::string name; };
  struct DropViewDef  { std::string name; };
  enum class ObjectKind { Topic, View, Schema };
  struct ShowHistoryDef { std::optional<ObjectKind> kind; std::string name; std::optional<std::string> sinceIso; };
  struct AsOfPoint { enum class By { Version, Time, Offset } by; int64_t number = 0; std::string timeIso; };
  struct ShowAsOfDef { ObjectKind kind; std::string name; AsOfPoint at; };
  struct DiffDef { enum class Scope { Schema, Catalog } scope; std::string name; int64_t from = 0, to = 0; };  // Schema: versions; Catalog: offsets
  using Statement = std::variant<TopicDef, AlterTopicDef, AlterTopicConfigDef, DropTopicDef, DropViewDef,
                                 ShowHistoryDef, ShowAsOfDef, DiffDef>;
  std::expected<Statement, ParseError> parse(std::string_view src) noexcept;     // (was variant<TopicDef, AlterTopicDef>)
  // Backends:
  struct AlterTopicConfigPlan { std::string name; std::vector<std::pair<uint8_t /*ConfigKey*/, int64_t>> changes; };
  [[nodiscard]] std::expected<AlterTopicConfigPlan, ParseError> applyConfigPlan(const AlterTopicConfigDef&);
  struct TopicCreateConfig {   // rune-ql stays protocol-free: the client maps this 1:1 onto TopicConfigWire
      uint8_t cleanupPolicy = 0; int64_t retentionMs = -1, retentionBytes = -1, deleteRetentionMs = 86'400'000;
      int32_t minCleanableDirtyRatioPpm = 500'000; int64_t segmentRollBytes = 64LL << 20, minCompactionLagMs = 0,
      maxCompactionLagMs = -1, retentionCheckMs = 300'000, fileDeleteDelayMs = 60'000;
      uint8_t durability = 0, durabilityTier = 1; std::string coPartitionGroup, partitionKey; };
  // DefineTopicPlan gains: uint16_t replicas; TopicCreateConfig config (from `with`);
  //   an empty descriptorBlob for an untyped topic.
  ```
- Grammar additions (to the header comment of `Parser.cpp`):
  ```
  topicStmt   := "topic" IDENT [ "{" field ("," field)* ","? "}" ] "partitions" INT [ "replicas" INT ] [ "key" IDENT ]
                 [ "with" "(" createSetting ("," createSetting)* ")" ] ";"
                 (an untyped topic has no "{…}" and no "key"; a typed topic requires both)
  createSetting := setting
                 | "cleanup.policy" "=" ("delete" | "compact" | "compact_delete")
                 | ("segment.bytes" | "retention.check.ms" | "file.delete.delay.ms") "=" INT
                 | "durability" "=" ("majority" | "all-in-sync") | "tier" "=" ("leader" | "majority" | "strict")
                 | ("copartition.group" | "partition.key") "=" STRING
                 (every former RUNE_TOPICS option has exactly one spelling here; creation-only)
  alterStmt   := "alter" "topic" IDENT ( alterOp ("," alterOp)* | "set" setting ("," setting)* ) ";"
  setting     := CONFIGKEY "=" INT | "min.cleanable.dirty.ratio" "=" DECIMAL
  CONFIGKEY   := "retention.ms" | "retention.bytes" | "delete.retention.ms" | "min.compaction.lag.ms" | "max.compaction.lag.ms"
  dropStmt    := "drop" ("topic" | "view") IDENT ";"
  showStmt    := "show" "history" [ ("topic" | "view" | "schema") IDENT ] [ "since" STRING ] ";"
               | "show" ("schema" | "view" | "topic") IDENT "as" "of" ( "version" INT | "time" STRING | "offset" INT ) ";"
  diffStmt    := "diff" "schema" IDENT "version" INT "to" INT ";"
               | "diff" "catalog" "offset" INT "to" INT ";"
  ```
  `min.cleanable.dirty.ratio` is a decimal in [0, 1] converted to ppm (`0.25` → 250000); any other key → `ParseError` "unknown or immutable topic setting '<key>'" (ruling 5). Time strings are ISO-8601 UTC (`'2026-10-13T12:00:00Z'`), converted by the CLI (Task 15), validated here by shape.

- [ ] **Step 1: Write the failing tests** — `CatalogStatementsTest.cpp`:

```cpp
#include <rune/ql/Parser.h>
#include <rune/ql/Backends.h>
#include <gtest/gtest.h>

namespace ql = rune::ql;

TEST(QlCatalogStatementsRegression, UntypedTopicWithReplicas) {
    auto r = ql::parse("topic events partitions 3 replicas 2;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& t = std::get<ql::TopicDef>(*r);
    EXPECT_TRUE(t.fields.empty()); EXPECT_EQ(t.partitions, 3u); EXPECT_EQ(t.replicas, 2);
    EXPECT_TRUE(ql::applyPlan(t).descriptorBlob.empty());
}
TEST(QlCatalogStatementsRegression, WithClauseCarriesEveryCreationSetting) {
    auto r = ql::parse("topic c partitions 1 with (cleanup.policy = compact, segment.bytes = 1048576, "
                       "retention.ms = 1000, tier = leader, durability = all-in-sync, copartition.group = 'g');");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto plan = ql::applyPlan(std::get<ql::TopicDef>(*r));
    EXPECT_EQ(plan.config.cleanupPolicy, 1);
    EXPECT_EQ(plan.config.segmentRollBytes, 1'048'576);
    EXPECT_EQ(plan.config.retentionMs, 1000);
    EXPECT_EQ(plan.config.durabilityTier, 0);
    EXPECT_EQ(plan.config.durability, 1);
    EXPECT_EQ(plan.config.coPartitionGroup, "g");
    EXPECT_FALSE(ql::parse("topic c partitions 1 with (tier = fast);").has_value());
}
TEST(QlCatalogStatementsRegression, TypedTopicStillRequiresKey) {
    EXPECT_FALSE(ql::parse("topic t { id int64 } partitions 1;").has_value());
    EXPECT_TRUE(ql::parse("topic t { id int64 } partitions 1 replicas 3 key id;").has_value());
}
TEST(QlCatalogStatementsRegression, DropTopicAndView) {
    EXPECT_EQ(std::get<ql::DropTopicDef>(*ql::parse("drop topic users;")).name, "users");
    EXPECT_EQ(std::get<ql::DropViewDef>(*ql::parse("DROP VIEW top;")).name, "top");
}
TEST(QlCatalogStatementsRegression, AlterTopicSetSettings) {
    auto r = ql::parse("alter topic t set retention.ms = 60000, min.cleanable.dirty.ratio = 0.25;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    auto plan = ql::applyConfigPlan(std::get<ql::AlterTopicConfigDef>(*r));
    ASSERT_TRUE(plan.has_value());
    ASSERT_EQ(plan->changes.size(), 2u);
    EXPECT_EQ(plan->changes[0], (std::pair<uint8_t, int64_t>{1, 60000}));
    EXPECT_EQ(plan->changes[1], (std::pair<uint8_t, int64_t>{4, 250000}));
}
TEST(QlCatalogStatementsRegression, ImmutableSettingIsAParseErrorNamingIt) {
    auto r = ql::parse("alter topic t set partitions = 4;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("partitions"), std::string::npos);
}
TEST(QlCatalogStatementsRegression, ShowHistoryForms) {
    auto all = std::get<ql::ShowHistoryDef>(*ql::parse("show history;"));
    EXPECT_FALSE(all.kind.has_value());
    auto one = std::get<ql::ShowHistoryDef>(*ql::parse("show history schema players since '2026-10-01T00:00:00Z';"));
    EXPECT_EQ(*one.kind, ql::ObjectKind::Schema); EXPECT_EQ(one.name, "players");
    EXPECT_EQ(*one.sinceIso, "2026-10-01T00:00:00Z");
}
TEST(QlCatalogStatementsRegression, ShowAsOfForms) {
    auto v = std::get<ql::ShowAsOfDef>(*ql::parse("show schema players as of version 3;"));
    EXPECT_EQ(v.at.by, ql::AsOfPoint::By::Version); EXPECT_EQ(v.at.number, 3);
    auto o = std::get<ql::ShowAsOfDef>(*ql::parse("show view top as of offset 17;"));
    EXPECT_EQ(o.kind, ql::ObjectKind::View); EXPECT_EQ(o.at.by, ql::AsOfPoint::By::Offset);
    auto t = std::get<ql::ShowAsOfDef>(*ql::parse("show topic t as of time '2026-10-13T12:00:00Z';"));
    EXPECT_EQ(t.at.by, ql::AsOfPoint::By::Time);
    EXPECT_FALSE(ql::parse("show topic t as of time 'yesterday';").has_value());
}
TEST(QlCatalogStatementsRegression, DiffForms) {
    auto s = std::get<ql::DiffDef>(*ql::parse("diff schema players version 1 to 3;"));
    EXPECT_EQ(s.scope, ql::DiffDef::Scope::Schema); EXPECT_EQ(s.from, 1); EXPECT_EQ(s.to, 3);
    auto c = std::get<ql::DiffDef>(*ql::parse("diff catalog offset 0 to 42;"));
    EXPECT_EQ(c.scope, ql::DiffDef::Scope::Catalog); EXPECT_EQ(c.to, 42);
}
TEST(QlCatalogStatementsRegression, ExistingStatementsAreUnchanged) {
    EXPECT_TRUE(std::holds_alternative<ql::TopicDef>(*ql::parse("topic t { id int64 } partitions 1 key id;")));
    EXPECT_TRUE(std::holds_alternative<ql::AlterTopicDef>(*ql::parse("alter topic t add column x int32;")));
    EXPECT_TRUE(std::holds_alternative<ql::TopicDef>(*ql::parse("transient topic t { id int64 } partitions 1 key id;")));
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-ql/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-ql/build -j14 --target rune_ql_tests`
Expected: compile errors (`DropTopicDef` undeclared).

- [ ] **Step 3: Implement** the grammar (one new `parseX()` per statement in the existing recursive-descent style; `parseStatement()` dispatches on the first keyword: `topic`, `transient`, `alter`, `drop`, `show`, `diff`). Dotted config keys are tokenised as `IDENT ('.' IDENT)*` joined.

- [ ] **Step 4: Update the client** for the new variant (`rune-client-cpp/src/cli/Cli.cpp` uses `parse`'s result: replace its `std::get_if<TopicDef>` / `<AlterTopicDef>` handling with a `std::visit` over `Statement`, every new alternative answering "not supported by 'schema apply' — use 'rune ql'" until Task 15). Build the client to prove it compiles.

- [ ] **Step 5: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-ql/build -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-ql/build --output-on-failure && cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14`
Expected: all rune-ql tests PASS; client builds.

- [ ] **Step 6: Commit** (two repos)

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-ql add include src tests CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-ql commit -m "feat(ql): SHOW HISTORY / AS OF / DIFF, DROP TOPIC|VIEW, ALTER TOPIC SET, replicas, untyped topics

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli/Cli.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "refactor(cli): visit the rune-ql Statement variant

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 15: CLI — `rune ql` (execute any statement) and `rune catalog export | replay | diff`

Spec §7 "Replay to rebuild (CLI)". `replay` re-issues each event as DDL on the target (the target assigns its own offsets and ids and records `replayedFrom`) and requires the target's resulting fingerprint to equal the source event's after each step; a mismatch aborts loudly at that event.

**Files:**
- Create: `rune-client-cpp/include/rune/cli/CatalogCommands.h`, `src/cli/CatalogCommands.cpp`
- Modify: `rune-client-cpp/src/cli/Cli.cpp` (`ql` and `catalog` subcommands; `printUsage`), `include/rune/client/view/ViewPlanAuthor.h` (`deserializeAuthored(span) → std::optional<ViewPlan>`; `remapSources(ViewPlan&, std::span<const int32_t> topicIds)`)
- Test: `rune-client-cpp/tests/RuneCliCatalogTest.cpp` (offline: `.rcat` files + a fake broker), `rune-client-cpp/tests/RuneCliLiveTest.cpp` (append a live `ql` case gated like its existing cases)

**Interfaces:**
- Consumes: Tasks 5, 13, 14.
- Produces — CLI surface (exit 0 success, 1 explicit error with a message on stderr, 2 usage):
  ```
  rune ql (-e "<statement>" | -f <file|->) [--dry-run] [--verbose] --broker host:port
        executes DDL (statement text recorded) and reads; --dry-run parses and prints the plan without a broker;
        --verbose prints every followed Redirect and NotLeader retry to stderr
  rune catalog export [--up-to <offset>] --broker host:port > file.rcat
  rune catalog replay --from <file.rcat|host:port> --into host:port [--up-to <offset>] [--cluster-id <id>]
  rune catalog diff --from <file.rcat|host:port> --to <file.rcat|host:port>
  ```
  DDL statements map onto Task 5's calls 1:1 (`TopicDef` → `defineTopicAndWait` with `TopicOptions{replicas, config}` copied field-for-field from rune-ql's `TopicCreateConfig`; `AlterTopicConfigDef` → `alterTopicConfigAndWait`; drops → `drop*AndWait`; `AlterTopicDef` → `alterTopicAndWait`). `rune ql` prints reads as aligned text tables: history rows `offset  time(UTC)  kind  object(objectId)  version prev→new  principal  statement`; `SHOW … AS OF` prints the object's definition (topic: partitions, replicas, persistence, config, schema via `rune::codec::toText`; view: sources, key, output columns); `DIFF` prints `+ added`, `- removed`, `~ changed: field, field`. `DIFF SCHEMA t VERSION a TO b` = `diff(asOf(schema t version a), asOf(schema t version b))` restricted to `t`.
- Replay algorithm (`CatalogCommands.cpp`): read the source events (`readRcat` or a `CatalogReader` on the source cluster), stop after `--up-to`; for each event in order build the target DDL from its `ops`: `CreateTopic` → `defineTopicAndWait(name, partitions, keyField, descriptorBlob, validate, persistence, {rf, config}, ddl)`; `CreateSchema` → `defineTopicAndWait` on the existing name with the descriptor; `AlterSchema` / `SetCompatibility` → `alterTopic` with the ops blob (expectedVersion 0); `AlterTopicConfig` → `alterTopicConfigAndWait`; `CreateView` / `AlterView` → the normalized authored plan, `remapSources` from `sourceNames` to the **target** topic ids (from the target's `CatalogReader` state), then `createViewAndWait` / `alterViewAndWait`; drops → `dropTopicAndWait` / `dropViewAndWait`. Each with `DdlOptions{statement = event.statement, requestId = hash(sourceClusterId, offset), replayedFrom = {clusterId, offset, principal = clientName@sourceAddress}}` — rerunning a replay after a crash is idempotent (same request ids). After each answer: `catalogHistory` refresh on the target; the target event whose `principal.requestId` equals this step's request id (new, or the one a rerun deduplicated to) must carry `replayedFrom.offset == source offset` and **the same fingerprint** as the source event; else exit 1 with `replay aborted at source offset N (<kind> '<name>'): target fingerprint 0x… != source 0x…`. A DDL error → exit 1 naming the offset and the broker's code/detail.
- `diff` compares the two final states with `rune::catalog::diff`; prints differences; exit 0 when there are none, 3 when there are (scripts test `== 0`).

- [ ] **Step 1: Write the failing tests** — `RuneCliCatalogTest.cpp`:

```cpp
TEST(RuneCliCatalogRegression, ExportWritesEveryEventAndUpToStops) {
    FakeCatalogBroker b; b.setLog(rune::catalog::testing::goldenScriptV1());
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"catalog", "export", "--up-to", "9", "--broker", b.endpoint()}, out, err), 0) << err.str();
    auto recs = rune::catalog::readRcat(out);
    ASSERT_TRUE(recs.has_value());
    EXPECT_EQ(recs->size(), 10u);
}

TEST(RuneCliCatalogRegression, ReplayReissuesEveryKindAndVerifiesFingerprints) {
    FakeCatalogBroker src; src.setLog(rune::catalog::testing::goldenScriptV1());
    FakeDdlCluster target;                              // applies DDL through rune_catalog, assigns ids (offset by +5)
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", target.endpoint(),
                              "--cluster-id", "src"}, out, err), 0) << err.str();
    EXPECT_EQ(target.eventCount(), 18u);
    EXPECT_TRUE(std::all_of(target.events().begin(), target.events().end(),
                            [](const auto& e) { return e.replayedFrom && e.replayedFrom->clusterId == "src"; }));
    std::stringstream o2, e2;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", src.endpoint(), "--to", target.endpoint()}, o2, e2), 0)
        << o2.str();
}

TEST(RuneCliCatalogRegression, ReplayAbortsLoudlyAtTheFirstDivergingEvent) {
    FakeCatalogBroker src; src.setLog(rune::catalog::testing::goldenScriptV1());
    FakeDdlCluster target;
    target.corruptFingerprintOfNthEvent(6);
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", target.endpoint()}, out, err), 1);
    EXPECT_NE(err.str().find("replay aborted at source offset 6"), std::string::npos) << err.str();
    EXPECT_EQ(target.eventCount(), 7u) << "nothing issued past the divergence";
}

TEST(RuneCliCatalogRegression, ReplayIsIdempotentAfterACrash) {
    FakeCatalogBroker src; src.setLog(rune::catalog::testing::goldenScriptV1());
    FakeDdlCluster target;
    std::stringstream o, e;
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", target.endpoint(),
                              "--up-to", "8"}, o, e), 0);
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", target.endpoint()}, o, e), 0);
    EXPECT_EQ(target.eventCount(), 18u) << "same request ids: the first 9 are deduplicated";
}

TEST(RuneCliCatalogRegression, DiffReportsDifferencesWithExit3) {
    FakeCatalogBroker a; a.setLog(rune::catalog::testing::goldenScriptV1());
    auto shorter = rune::catalog::testing::goldenScriptV1(); shorter.resize(12);
    FakeCatalogBroker b; b.setLog(shorter);
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", a.endpoint(), "--to", b.endpoint()}, out, err), 3);
    EXPECT_NE(out.str().find("players"), std::string::npos);
}

TEST(RuneCliCatalogRegression, QlDryRunParsesWithoutABroker) {
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic t partitions 2 replicas 3 with (tier = leader);", "--dry-run"}, out, err), 0);
    EXPECT_NE(out.str().find("replicas 3"), std::string::npos);
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic t partitions;", "--dry-run"}, out, err), 1);
}

TEST(RuneCliCatalogRegression, QlExecutesDdlWithItsStatementAndPrintsHistory) {
    FakeDdlCluster c;
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "topic events partitions 2 replicas 1;", "--broker", c.endpoint()}, out, err), 0);
    EXPECT_EQ(c.events().back().statement, "topic events partitions 2 replicas 1;");
    std::stringstream h;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "show history topic events;", "--broker", c.endpoint()}, h, err), 0);
    EXPECT_NE(h.str().find("CreateTopic"), std::string::npos);
}
```

(`FakeCatalogBroker` / `FakeDdlCluster` are client test-support fakes over `InvokerGateway` in `tests/support/CatalogFakes.h`: the first serves a catalog Subscribe from a vector; the second also accepts DDL, builds events with `rune_catalog` (`previewFingerprint`), appends them to its own log and answers like a leader — so `replay` sees real fingerprints.)

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement**, **Step 4: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli' --output-on-failure -j14`
Expected: PASS (the live cases skip without a broker, as today).

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add include src tests CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(cli): rune ql + rune catalog export|replay|diff (fingerprint-verified, idempotent replay)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint E (controller):** full `*Regression*` suites in client, rune-ql, rune-codec; broker `build-fast` builds.

---
### Task 16: Broker — boot from `__catalog`; retire `__topic_schemas`, `__views` and runtime-topic recovery

Spec §3 ("single source of truth"), §5.3 ("Restart: the broker replays its local `__catalog` copy, then catches up from the leader"), §1 (no migration). Boot replays the checkpointed catalog prefix **on the startup thread, before any boot builder runs**, so the TopicRegistry, replication, worker placement and the MetadataService see every topic exactly as the catalog says — then the admin thread takes ownership of the applier and continues live.

**Files:**
- Modify: `libs/node/src/BrokerNode.cpp`:
  - step 1b (anchor `// 1b. Restart recovery of RUNTIME-defined topics`) is replaced by `bootFromCatalog_()`: build the catalog `Log` + replica (deferred plane, Task 6's `buildCatalog_` moves here), construct the `CatalogApplier` with a `BootCatalogSink` effects object, `applier.replayLocal(*catalogReplica_, /*recoveryInstalls=*/true)`; the sink fills `bootTopics_` (`std::vector<TopicSpec>`, live topics in topic-id order) + `bootDroppedIds_` and stashes recovery view installs and validator pushes for after the workers exist. A halted boot replay does **not** refuse to start: the broker boots with the applied prefix, halted (route `RouteHalted`), logged at critical — spec §5.3 "keeps serving what it had already applied".
  - `TopicRegistry` is constructed **empty**, then `createTopicAt(id, spec)` for each `bootTopics_` entry and a `Dropped` slot for each `bootDroppedIds_` id.
  - every `config_.topics` consumer (~20 sites, `grep -n 'config_.topics' libs/node/src/BrokerNode.cpp`) reads `bootTopics_` instead.
  - `buildAdminWorker()`: delete the `__topic_schemas` block (anchor `const fs::path schemaDir = fs::path{config_.dataDir} / "__topic_schemas";`) and the `__views` block (anchor `const fs::path viewsDir = fs::path{config_.dataDir} / "__views";`) and their members (`schemaLog_`, `schemaReplica_`, `viewsLog_`, `viewsReplica_`); hand the applier + catalog replica to `AdminWorkerAgent`; post the stashed recovery installs once the workers are built.
  - `firstRuntimeTopicId_` and everything that reads it are deleted.
- Delete: `libs/node/include/rune/node/RuntimeTopicRecovery.h`, `libs/node/src/RuntimeTopicRecovery.cpp`, `libs/node/tests/node/RuntimeTopicRecoveryTest.cpp`
- Create: `libs/node/include/rune/node/TopicDirReconcile.h`, `libs/node/src/TopicDirReconcile.cpp` — `reconcileTopicDirs(dataDir, liveTopics, now)`: (1) `remove_all` every entry of `dataDir/dropped/` (left by a purge the admin thread could not finish), (2) **quarantine** (never delete) every `dataDir/topics/<name>` that no live topic claims (exactly or by ASCII case fold) to `dataDir/quarantine/<name>-<now>[-N]`, logged at warn, symlinks never followed (the existing `quarantine` helper code moves here verbatim from `RuntimeTopicRecovery.cpp`), (3) a `dataDir/__topic_schemas` or `dataDir/__views` dir is left untouched and logged once at warn ("retired catalog dir ignored — `__catalog` is the catalog; no migration").
- Modify: `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp` (delete `rebuildFromLog_`, `recoverViewsOnPromotion_`, `rebuildFailed_`, `viewsRebuildFailed_`, `refusedForFailedRebuild_` — every broker now has the verified catalog from boot, not only a promoted one)
- Modify: `libs/node/include/rune/node/SchemaRegistry.h`, `libs/node/src/SchemaRegistry.cpp` (delete `TopicSchema::serialize/deserialize`, `kSchemaTopicId`, `rebuildFromRecords`, `allocateRuntimeId`, `RUNTIME_TOPIC_ID_BASE`), `ViewRegistry.h/.cpp` (delete `ViewRecord::serialize/deserialize`, `kViewsTopicId`, `rebuildFromRecords`), `libs/node/include/rune/node/TopicName.h` (reserved-name comment: `__catalog`, `__consumer_offsets`, `__transaction_state`, `__cluster_metadata`), `BrokerNode.h` (`schemaLogForTest`/`viewsLogForTest` → `catalogLogForTest`; the `onOwnerThreadForTest` guard names `__catalog (-6)`)
- Port: `libs/node/tests/node/SchemaRegistryTest.cpp`, `ViewRegistryTest.cpp` (drop the (de)serialize cases), `libs/node/tests/node/PerWorkerReplicationWiringTest.cpp`, `TopicNameTest.cpp`, `libs/replication/tests/replication/PartitionReplicaTest.cpp` (their `__topic_schemas` references), `libs/node/tests/node/ViewRecoveryRegressionTest.cpp` / `ViewReadinessRegressionTest.cpp` (recovery installs now come from boot replay on every broker)
- Test: `libs/node/tests/node/CatalogBootTest.cpp`

**Interfaces:**
- Consumes: Tasks 6–9.
- Produces: `BrokerNode::bootTopics_` (private), `reconcileTopicDirs(const std::string&, const std::vector<TopicSpec>&, int64_t) → TopicDirReconciliation{quarantined, purged}`.

- [ ] **Step 1: Write the failing tests** — `CatalogBootTest.cpp` (single-node BrokerNode, DDL through `BrokerNode::submitDdlForTest` — added here: posts a `DdlRequest` from the control thread with the reserved test session `kTestDdlSession = -42`, answers captured on the control thread and read with `takeTestDdlAnswerForTest()`):

```cpp
TEST(CatalogBootRegression, TopicsSchemasAndViewsComeBackFromTheCatalogAfterRestart) {
    BootRig r("boot_all");
    r.start();
    ASSERT_EQ(r.define({.name = "players", .partitions = 2, .typed = true}).code, 0);
    ASSERT_EQ(r.define({.name = "events", .partitions = 3}).code, 0);
    ASSERT_EQ(r.createView("top", "players").code, 0);
    ASSERT_EQ(r.dropTopic("events").code, 0);
    r.produce("players", 0, "k", "v");
    const uint64_t digest = r.catalogDigest();
    r.restart();
    EXPECT_EQ(r.catalogDigest(), digest);
    EXPECT_EQ(r.topicIdForName("players"), 0);
    EXPECT_FALSE(r.topicIdForName("events").has_value()) << "a dropped topic stays dropped";
    EXPECT_EQ(r.topicState(1), rune::gateway::TopicRegistry::TopicState::Dropped);
    EXPECT_EQ(r.logEndOffset(0, 0), 1) << "data survived";
    EXPECT_TRUE(r.awaitViewReady("top", 10s));
}

TEST(CatalogBootRegression, BootReplaysBeforeAnyBuilderSoTopicsServeImmediately) {
    BootRig r("boot_order");
    r.start();
    ASSERT_EQ(r.define({.name = "t", .partitions = 1}).code, 0);
    r.restart();
    EXPECT_TRUE(r.registryHadTopicBeforeReplicationBuild(0)) << "startup-phase hook: TopicRegistry built with t";
}

TEST(CatalogBootRegression, UnclaimedTopicDirIsQuarantinedNeverDeleted) {
    BootRig r("boot_quar");
    r.start();
    r.stop();
    std::filesystem::create_directories(r.dataDir() / "topics" / "ghost" / "0");
    { std::ofstream(r.dataDir() / "topics" / "ghost" / "0" / "00000000000000000000.log") << "x"; }
    r.start();
    EXPECT_FALSE(std::filesystem::exists(r.dataDir() / "topics" / "ghost"));
    EXPECT_TRUE(r.quarantineHasEntryStartingWith("ghost-"));
}

TEST(CatalogBootRegression, LeftoverDroppedDirIsPurgedAtBoot) {
    BootRig r("boot_purge");
    r.start(); r.stop();
    std::filesystem::create_directories(r.dataDir() / "dropped" / "3.old");
    r.start();
    EXPECT_FALSE(std::filesystem::exists(r.dataDir() / "dropped" / "3.old"));
}

TEST(CatalogBootRegression, RetiredCatalogDirsAreIgnoredNotRead) {
    BootRig r("boot_retired");
    std::filesystem::create_directories(r.dataDir() / "__topic_schemas");
    { std::ofstream(r.dataDir() / "__topic_schemas" / "00000000000000000000.log") << "garbage"; }
    r.start();
    EXPECT_TRUE(r.isServing()) << "no migration: the old catalog is never read";
    EXPECT_TRUE(std::filesystem::exists(r.dataDir() / "__topic_schemas"));
}

TEST(CatalogBootRegression, HaltedBootReplayStillBootsAndRefusesDdl) {
    BootRig r("boot_halt");
    r.start();
    ASSERT_EQ(r.define({.name = "a", .partitions = 1}).code, 0);
    ASSERT_EQ(r.define({.name = "b", .partitions = 1}).code, 0);
    r.stop();
    r.corruptCheckpointDigest();
    r.start();
    EXPECT_TRUE(r.isServing());
    EXPECT_EQ(r.define({.name = "c", .partitions = 1}).code, 6);
    EXPECT_EQ(r.catalogHaltedCounter(), 1);
}
```

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement**, **Step 4: Run**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogBoot|SchemaRegistry|ViewRegistry|ViewRecovery|ViewReadiness|TopicName|PerWorkerReplicationWiring|PartitionReplica|CatalogApplier|CatalogWritePath|SchemaPersistence' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add -A libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(catalog): boot from __catalog; retire __topic_schemas, __views and runtime-topic recovery (no migration)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 17: Broker — delete `RUNE_TOPICS`, `NodeConfig::topics`, `rf=` and the name-collision boot rule; `TestCluster::defineTopic`; migrate the node unit tests

Spec §8: delete the setting and its parsing, static topic creation and recovery at boot, the static-versus-runtime collision rule, the `rf=N` option; in-process tests use a one-line `TestCluster::defineTopic(...)`.

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h` (delete `topics`, the `RUNE_TOPICS` doc lines, the `rf=`/`RUNE_TOPICS` mentions in `TopicSpec`'s comments — `TopicSpec` itself stays: it is the internal topic description `TopicRegistry` and `createRuntimeUserReplicas` use), `libs/node/src/NodeConfig.cpp` (delete the block from anchor `// --- RUNE_TOPICS (optional) ---` to `// Absent or empty RUNE_TOPICS → cfg.topics stays empty (valid).` inclusive; a set `RUNE_TOPICS` env var is now refused at config load: `"RUNE_TOPICS was removed — topics are created by DDL (see docs/SELF-HOSTING.md#topics-are-created-by-ddl)"` — an explicit error, never silently ignored)
- Modify: `libs/node/src/BrokerNode.cpp` (`refuseStaticNamesCollidingWithRecovered` call gone with Task 16's step 1b; grep that no `RUNE_TOPICS` / `config_.topics` remains)
- Modify: `libs/node/include/rune/node/TopicName.h`, `AdminChannels.h`, `BrokerNode.h`, `libs/gateway/include/rune/gateway/TopicRegistry.h` (comment mentions)
- Create: `libs/node/tests/support/TestCluster.h`
- Migrate: `libs/node/tests/node/BrokerNodeTest.cpp`, `BrokerNodeGatewayTest.cpp`, `NodeConfigTest.cpp` (delete every `RUNE_TOPICS` parse case — 58 references — and add `NodeConfigRegression.RuneTopicsIsRefusedWithAPointerToDdl`), `DdlHarnessRegressionTest.cpp`, `libs/node/tests/support/DdlHarness.h` (the `statics` option is gone since Task 10 — remove its last comment), `benchmark/FusedRuntimePassBench.cpp`
- Test: `libs/node/tests/node/TestClusterTest.cpp`

**Interfaces:**
- Produces:
  ```cpp
  // libs/node/tests/support/TestCluster.h (namespace rune::node::test) — non-owning view over a test's nodes.
  class TestCluster {
  public:
      explicit TestCluster(std::vector<BrokerNode*> nodes);
      /// DDL through the catalog leader (found via each node's CatalogRoute); waits for the OK and then for
      /// the topic to serve on every node that holds a replica (C1 generalized).  Returns the topic id.
      /// Fails the test (ADD_FAILURE + returns -1) on an error answer or the deadline — never hangs.
      int32_t defineTopic(const TopicSpec& spec, std::chrono::milliseconds timeout = std::chrono::seconds(30));
      int32_t defineTopic(std::string name, int32_t partitions, std::chrono::milliseconds timeout = std::chrono::seconds(30));
  };
  // BrokerNode (test hooks, Task 16): submitDdlForTest(DdlRequest), takeTestDdlAnswerForTest(),
  //   catalogTopicServesForTest(int32_t topicId)
  ```
  `defineTopic(spec)` maps every `TopicSpec` field (partitions, `replicationFactor`, persistence, cleanup policy, retention / segment / compaction / durability / tier / co-partition group / partition key) onto a `DefineTopicMsg` with an empty descriptor (untyped) — the exact properties the old `RUNE_TOPICS` entry carried. Topic ids follow creation order from 0, so a test that defines its topics in the old `RUNE_TOPICS` order keeps its ids.

Migration rule for every site (mechanical, one edit per old `topics` assignment):

```cpp
// before
cfg.topics = {TopicSpec{"events", 3, 0}};
auto node = std::make_unique<BrokerNode>(cfg);
node->start();
// after
auto node = std::make_unique<BrokerNode>(cfg);
node->start();
ASSERT_EQ(TestCluster({node.get()}).defineTopic("events", 3), 0);
```

(multi-node tests: start every node, then one `TestCluster({n1, n2, n3}).defineTopic(spec)` per former entry, in the former order; tests that restart a node define nothing again — the topic comes back from the catalog, which is now part of what they test.)

- [ ] **Step 1: Write the failing tests** — `TestClusterTest.cpp`:

```cpp
TEST(TestClusterRegression, DefineTopicReturnsDenseIdsInOrderAndTheTopicServes) {
    SingleNodeForTest n("tc_basic");
    rune::node::test::TestCluster c({n.node()});
    EXPECT_EQ(c.defineTopic("a", 1), 0);
    EXPECT_EQ(c.defineTopic("b", 2), 1);
    EXPECT_TRUE(n.node()->catalogTopicServesForTest(1));
}
TEST(TestClusterRegression, DefineTopicCarriesEveryFormerRuneTopicsOption) {
    SingleNodeForTest n("tc_opts");
    rune::node::TopicSpec s; s.name = "c"; s.partitionCount = 1;
    s.cleanupPolicy = rune::node::TopicSpec::CleanupPolicy::Compact; s.segmentRollBytes = 1 << 20;
    s.retentionMs = 1000; s.durabilityTier = 0; s.persistence = rune::node::PersistenceMode::Transient;
    const int32_t id = rune::node::test::TestCluster({n.node()}).defineTopic(s);
    const auto got = n.topicSpecOnControl(id);
    EXPECT_EQ(got.cleanupPolicy, s.cleanupPolicy); EXPECT_EQ(got.segmentRollBytes, s.segmentRollBytes);
    EXPECT_EQ(got.retentionMs, 1000); EXPECT_EQ(got.durabilityTier, 0);
    EXPECT_EQ(got.persistence, rune::node::PersistenceMode::Transient);
}
TEST(NodeConfigRegression, RuneTopicsIsRefusedWithAPointerToDdl) {
    auto r = rune::node::NodeConfig::fromEnv([](const char* k) -> const char* {
        if (std::string_view(k) == "RUNE_TOPICS") return "events:1";
        return rune::node::test::minimalEnv(k);
    });
    ASSERT_FALSE(r.ok());
    EXPECT_NE(r.error().find("topics are created by DDL"), std::string::npos);
}
```

(Use `NodeConfigTest.cpp`'s existing env-injection helper and result type names verbatim — the snippet shows the intent: the load fails, the message points to DDL.)

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement** + migrate the listed files, **Step 4: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'TestCluster|NodeConfig|BrokerNode|DdlHarness' --output-on-failure -j14`
Expected: PASS. `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-bench-rel -j14 --target rune_broker_benchmark` builds.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add -A libs benchmark
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(catalog)!: remove RUNE_TOPICS, NodeConfig::topics, rf= and the static/runtime collision rule; TestCluster::defineTopic

Topics exist only through DDL. A set RUNE_TOPICS refuses to load with a pointer to the DDL docs.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 18: Broker — migrate the in-process integration tests to `TestCluster::defineTopic`

Mechanical, by the Task 17 rule; split from it so each commit stays reviewable. Each file keeps its scenario and assertions; only how its topics come to exist changes.

**Files (migrate):** `libs/node/tests/integration/{BootstrapRoutingImageTest, CompactionRetentionSmokeTest, ConsensusCandidacyElectionTest, ControlledShutdownRegressionTest, D9CrossNodeMarkerTest, DdlConformanceBrokerRegressionTest, EpochRestartRegressionTest, GatewayAcksAllDeferredAckTest, GatewayFailoverReplicationTest, GatewayProduceReplicationTest, GatewayPsmFailoverTest, GatewayReplicationWiringTest, GatewaySubscribeHwBoundTest, MultiLeaderProduceTest, PerWorkerReplicationTest, RebalanceFailoverIntegrationTest, ShardModeTest, ShardNonBlockingTest, StaticAssignmentEmitTest, TransientTopicDataPathTest, WorkerPoolParityTest}.cpp`, plus `tests/integration/ClusterHarness.h` if it builds topics.

Two files change meaning, not just mechanics:
- `StaticAssignmentEmitTest.cpp` / `BootstrapRoutingImageTest.cpp` tested "boot topics are assigned at boot". Rename the suites `CatalogAssignmentEmitTest` / `CatalogBootstrapRoutingImageTest`: the topics are defined by DDL, the cluster is restarted, and the assertions (assignment emitted once, routing image complete) now hold for **catalog-recovered** topics.
- `TransientTopicDataPathTest.cpp` has static-transient cases (`RUNE_TOPICS … persistence=transient`); they become `defineTopic(spec with persistence Transient)` — their "restart → exists and empty" assertions are unchanged (the topic now comes back from the catalog).

- [ ] **Step 1: Migrate** every file above by the rule; delete each `cfg.topics` line; build.
- [ ] **Step 2: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'Compaction|Consensus|ControlledShutdown|D9|DdlConformanceBroker|EpochRestart|Gateway|MultiLeader|PerWorkerReplication|Rebalance|ShardMode|ShardNonBlocking|CatalogAssignmentEmit|CatalogBootstrapRoutingImage|TransientTopicDataPath|WorkerPoolParity|ClusterDisasterRecovery' --output-on-failure -j14`
Expected: PASS — the same scenarios as before the migration.
- [ ] **Step 3: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add -A libs tests
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(catalog): integration tests create their topics by DDL (TestCluster::defineTopic)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 19: Scripts and docs off `RUNE_TOPICS` (docker, dr-e2e, kafka-compare) + the grep gate

Spec §8: docker e2e scripts create their topics through the CLI or the probe before producing; every existing `RUNE_TOPICS` use is migrated, and none remain (grep gate); SELF-HOSTING.md loses every mention (its new section lands in Task 25).

**Files:**
- Create: `docker/topics/e2e.rql` (`topic events partitions 1;` `topic multi partitions 3;` `topic cg partitions 4;` — the ids 0, 1, 2 the e2e client uses), `docker/topics/transient-failover.rql` (+ `transient topic presence_failover partitions 1;` as the 4th, id 3), `docker/create-topics.sh` (runs the rune-ql-probe image's `rune ql -f /topics/<file>.rql --broker <seed>` on the compose network; bounded retries on `NotLeader` / connection refused while the cluster forms, then an explicit failure)
- Modify: `docker/docker-compose.yml`, `docker/docker-compose.transient-failover.yml`, `docker/docker-compose.single.yml` (delete `RUNE_TOPICS` and its comment), `docker/e2e.sh`, `docker/transient-failover-e2e.sh` (call `create-topics.sh` after the leader is up, before any producer; `TOPIC_ID=3` comment rewritten)
- Modify: `dr-e2e/harness/cluster.py`, `dr-e2e/harness/driver.py`, `dr-e2e/harness/dockerutil.py`, `dr-e2e/tests/{test_cg_atleastonce,test_cg_failover,test_cg_rebalance,test_compaction_e2e,test_dr2_multiclient,test_multi_leader_smoke,test_per_worker_ingress}.py` (the harness gains `create_topics(spec_list)`: builds a `.rql` from the old `name:partitions[:options]` strings — options mapped to the `with (…)` clause / `replicas` / `transient` — and runs it through the CLI container; every former `RUNE_TOPICS` env becomes a `create_topics` call)
- Modify: `bench/kafka-compare/{cluster.sh,lib.sh,run-rune.sh,replbench.sh,README.md}` (native brokers: after start, `"$RUNE_CLI" ql -e "topic bench partitions $P with (tier = leader);" --broker …`; `RUNE_CLI` defaults to `../rune-client-cpp/build/rune`; the README's D0 row reads `topic bench … with (tier = leader)`)
- Modify: `docs/release-notes/phase2b-c-typed-topic-routing.md`, `docs/release-notes/phase2b-e-transient-topics.md`, `docs/SELF-HOSTING.md`, `docs/superpowers/2026-09-14-rune-client-milestone-b-acceptance.md` (replace each `RUNE_TOPICS` example with the DDL equivalent; release notes keep their history in prose: "topics were then declared with an environment variable, since removed — see SELF-HOSTING")
- Create: `scripts/check_no_rune_topics.py`; modify root `CMakeLists.txt` (`add_test(NAME NoRuneTopicsGateRegression.RepositoryHasNoRuneTopicsMention COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/check_no_rune_topics.py ${CMAKE_SOURCE_DIR})`)

`scripts/check_no_rune_topics.py`:

```python
#!/usr/bin/env python3
"""Grep gate (catalog history spec §8): no RUNE_TOPICS mention may remain.

Scans every tracked file except historical design records (docs/superpowers/**,
which describe what existed when they were written) and this script.  Exit 1
listing every hit; exit 0 when clean.  The broker's config loader refuses the
variable explicitly (NodeConfig.cpp), and that one refusal message is allowed.
"""
import subprocess, sys, pathlib

root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
files = subprocess.run(["git", "-C", str(root), "ls-files"], capture_output=True, text=True, check=True).stdout.split()
allowed_exact = {
    "scripts/check_no_rune_topics.py",
}
allowed_lines = {
    ("libs/node/src/NodeConfig.cpp", 'RUNE_TOPICS was removed'),
    ("libs/node/tests/node/NodeConfigTest.cpp", 'RUNE_TOPICS'),
}
hits = []
for f in files:
    if f.startswith("docs/superpowers/") or f in allowed_exact:
        continue
    p = root / f
    try:
        text = p.read_text(errors="ignore")
    except (IsADirectoryError, FileNotFoundError):
        continue
    for n, line in enumerate(text.splitlines(), 1):
        if "RUNE_TOPICS" in line and not any(f == af and frag in line for af, frag in allowed_lines):
            hits.append(f"{f}:{n}: {line.strip()}")
if hits:
    print("RUNE_TOPICS must not appear (topics are created by DDL):")
    print("\n".join(hits))
    sys.exit(1)
print("ok: no RUNE_TOPICS mention")
```

(`NodeConfigTest.cpp`'s single allowed reference is the refusal test of Task 17.)

- [ ] **Step 1: Run the gate to see it fail**

Run: `python3 /Users/vradu/Projects/se-worktrees/rune-broker/scripts/check_no_rune_topics.py /Users/vradu/Projects/se-worktrees/rune-broker`
Expected: exit 1 listing the docker / dr-e2e / bench / docs hits.

- [ ] **Step 2: Migrate** every listed file.

- [ ] **Step 3: Run the gate + the scripts**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-fast && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R NoRuneTopicsGate --output-on-failure`
Expected: PASS (`ok: no RUNE_TOPICS mention`).
Run: `bash /Users/vradu/Projects/se-worktrees/rune-broker/docker/e2e.sh` and `bash /Users/vradu/Projects/se-worktrees/rune-broker/docker/transient-failover-e2e.sh`
Expected: both end with their existing success line (topics now created by `create-topics.sh`).
Run: `cd /Users/vradu/Projects/se-worktrees/rune-broker/dr-e2e && python3 -m pytest tests/test_multi_leader_smoke.py -x`
Expected: PASS (the smoke the others share the harness with; the full dr-e2e matrix runs at Checkpoint F).

- [ ] **Step 4: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add -A docker dr-e2e bench docs scripts CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "chore(catalog): docker, dr-e2e and kafka-compare create topics by DDL; RUNE_TOPICS grep gate

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint F (controller):** full broker `*Regression*` suite (incl. `NoRuneTopicsGateRegression`), the docker e2e scripts touched in Task 19, and the dr-e2e suite (`python3 -m pytest dr-e2e/tests -x`).

---
### Task 20: Interaction matrix — the expectation table, the 3-broker harness, the parameterized unit matrix (324 cells)

**Files:**
- Create: `libs/replication/include/rune/replication/testing/FilteringTransport.h` (moved verbatim from `libs/replication/tests/integration/ReplicationClusterHarness.h:101`; that header now includes it)
- Create: `libs/node/tests/support/CatalogMatrixExpectations.h`
- Create: `libs/node/tests/support/CatalogClusterHarness.h` (promotes Task 11's `CatalogTrio`: 3 × {AdminWorker, catalog replica on one `FilteringTransport`, `CatalogApplier`, a fake control side with its own `TopicRegistry` + placement map, a fake gateway applying `CatalogRoute`}, a scripted "controller" electing the catalog leader from the ISR exactly as `MetadataService::decideCoordinatorPartition` does (it calls that static function), a deterministic clock, and a client model with `requestId`s, budget and redirect-following identical to Task 5's rules)
- Create: `libs/node/tests/node/CatalogInteractionMatrixRegressionTest.cpp`; modify `libs/node/CMakeLists.txt` (`rune_catalog_matrix_tests`)

**Interfaces:**
- Consumes: Tasks 5 (client rules, mirrored), 7, 9, 11, 12.
- Produces:
  ```cpp
  // CatalogMatrixExpectations.h (namespace rune::node::test::cmatrix)
  enum class Kind : uint8_t { CreateTopic, DropTopic, AlterTopicConfig, CreateSchema, AlterSchema, SetCompatibility,
                              CreateView, AlterView, DropView };
  enum class Via : uint8_t { Leader, Follower };
  enum class Persist : uint8_t { Disk, Transient };
  struct Entity { Via via; Persist persist; };
  enum class Timing : uint8_t { Steady, LeaderChangeMidDdl, RestartMidApply, NewBrokerJoins, CatalogBelowMinIsr,
                                ConcurrentDdlDifferentBrokers, DropAndReuseName, CreateRacesPlacement, FingerprintMismatch };
  enum class Outcome : uint8_t { NotApplicable, Ok, RedirectThenOk, NotLeaderThenOkOnce, NotLeaderUntilBudgetThenOk,
                                 OneWinsOneConflict, OkNewObjectId, OkAfterPlacement, HaltedBrokerInternalOthersOk };
  [[nodiscard]] Outcome expect(Kind, Entity, Timing) noexcept;
  [[nodiscard]] int naRule(Kind, Entity, Timing) noexcept;          // 1 for R1, 0 = applicable
  [[nodiscard]] uint16_t conflictCode(Kind) noexcept;               // R5
  [[nodiscard]] std::string_view name(Kind) noexcept; name(Timing); name(Outcome);
  inline constexpr int kKinds = 9, kEntities = 4, kTimings = 9;
  ```

- [ ] **Step 1: Write the expectation table** — `CatalogMatrixExpectations.h`:

```cpp
#pragma once
// CatalogMatrixExpectations.h — catalog history plan: THE interaction-matrix table.
// Kind x Entity x Timing -> Outcome.  The unit matrix, the seeded stress, the
// 3-node slice and the docker e2e read this; the plan's markdown table is a copy.
#include <cstdint>
#include <string_view>

namespace rune::node::test::cmatrix {

enum class Kind : uint8_t { CreateTopic, DropTopic, AlterTopicConfig, CreateSchema, AlterSchema, SetCompatibility,
                            CreateView, AlterView, DropView };
enum class Via : uint8_t { Leader, Follower };
enum class Persist : uint8_t { Disk, Transient };
struct Entity { Via via; Persist persist; };
enum class Timing : uint8_t { Steady, LeaderChangeMidDdl, RestartMidApply, NewBrokerJoins, CatalogBelowMinIsr,
                              ConcurrentDdlDifferentBrokers, DropAndReuseName, CreateRacesPlacement,
                              FingerprintMismatch };
enum class Outcome : uint8_t { NotApplicable, Ok, RedirectThenOk, NotLeaderThenOkOnce, NotLeaderUntilBudgetThenOk,
                               OneWinsOneConflict, OkNewObjectId, OkAfterPlacement, HaltedBrokerInternalOthersOk };
inline constexpr int kKinds = 9, kEntities = 4, kTimings = 9;

[[nodiscard]] constexpr bool createsATopic(Kind k) noexcept {
    return k == Kind::CreateTopic || k == Kind::CreateView || k == Kind::AlterView;
}
[[nodiscard]] constexpr int naRule(Kind k, Entity, Timing t) noexcept {
    return (t == Timing::CreateRacesPlacement && !createsATopic(k)) ? 1 : 0;   // R1
}
[[nodiscard]] constexpr Outcome expect(Kind k, Entity e, Timing t) noexcept {
    if (naRule(k, e, t) != 0) return Outcome::NotApplicable;                          // R1
    switch (t) {
    case Timing::FingerprintMismatch:           return Outcome::HaltedBrokerInternalOthersOk;   // R2
    case Timing::CatalogBelowMinIsr:            return Outcome::NotLeaderUntilBudgetThenOk;     // R3
    case Timing::LeaderChangeMidDdl:            return Outcome::NotLeaderThenOkOnce;            // R4
    case Timing::ConcurrentDdlDifferentBrokers: return Outcome::OneWinsOneConflict;             // R5
    case Timing::DropAndReuseName:              return Outcome::OkNewObjectId;                  // R6
    case Timing::CreateRacesPlacement:          return Outcome::OkAfterPlacement;               // R7
    case Timing::Steady: case Timing::RestartMidApply: case Timing::NewBrokerJoins:
        return e.via == Via::Follower ? Outcome::RedirectThenOk : Outcome::Ok;                  // R8 / R9
    }
    return Outcome::NotApplicable;
}
/// R5: the loser's code.
[[nodiscard]] constexpr uint16_t conflictCode(Kind k) noexcept {
    switch (k) {
    case Kind::CreateTopic: case Kind::CreateSchema: return 25;   // TopicExists
    case Kind::CreateView:                           return 32;   // ViewExists
    case Kind::DropTopic: case Kind::DropView:       return 7;    // UnknownTopic
    default:                                         return 31;   // SchemaConflict (expectedVersion)
    }
}
[[nodiscard]] constexpr std::string_view name(Kind k) noexcept {
    constexpr std::string_view n[] = {"CreateTopic", "DropTopic", "AlterTopicConfig", "CreateSchema", "AlterSchema",
                                      "SetCompatibility", "CreateView", "AlterView", "DropView"};
    return n[static_cast<int>(k)];
}
[[nodiscard]] constexpr std::string_view name(Timing t) noexcept {
    constexpr std::string_view n[] = {"Steady", "LeaderChangeMidDdl", "RestartMidApply", "NewBrokerJoins",
                                      "CatalogBelowMinIsr", "ConcurrentDdlDifferentBrokers", "DropAndReuseName",
                                      "CreateRacesPlacement", "FingerprintMismatch"};
    return n[static_cast<int>(t)];
}
[[nodiscard]] constexpr std::string_view name(Outcome o) noexcept {
    constexpr std::string_view n[] = {"NotApplicable", "Ok", "RedirectThenOk", "NotLeaderThenOkOnce",
                                      "NotLeaderUntilBudgetThenOk", "OneWinsOneConflict", "OkNewObjectId",
                                      "OkAfterPlacement", "HaltedBrokerInternalOthersOk"};
    return n[static_cast<int>(o)];
}

} // namespace rune::node::test::cmatrix
```

- [ ] **Step 2: Write the count pin and the matrix** — `CatalogInteractionMatrixRegressionTest.cpp`:

```cpp
// Catalog history spec §10 — Event kind x timing, every cell asserts the six
// invariants of the plan's "Every cell asserts" list.
#include "support/CatalogClusterHarness.h"
#include "support/CatalogMatrixExpectations.h"
#include <gtest/gtest.h>

namespace cm = rune::node::test::cmatrix;
using rune::node::test::CatalogClusterHarness;

TEST(CatalogMatrixCountRegression, CellCountsArePinned) {
    int total = 0, applicable = 0;
    std::array<int, 9> byOutcome{};
    for (int k = 0; k < cm::kKinds; ++k)
        for (int v = 0; v < 2; ++v)
            for (int p = 0; p < 2; ++p)
                for (int t = 0; t < cm::kTimings; ++t) {
                    ++total;
                    const auto o = cm::expect(static_cast<cm::Kind>(k), {static_cast<cm::Via>(v), static_cast<cm::Persist>(p)},
                                              static_cast<cm::Timing>(t));
                    ++byOutcome[static_cast<int>(o)];
                    if (o != cm::Outcome::NotApplicable) ++applicable;
                }
    EXPECT_EQ(total, 324);
    EXPECT_EQ(applicable, 300);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::NotApplicable)], 24);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::HaltedBrokerInternalOthersOk)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::NotLeaderUntilBudgetThenOk)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::NotLeaderThenOkOnce)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::OneWinsOneConflict)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::OkNewObjectId)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::OkAfterPlacement)], 12);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::RedirectThenOk)], 54);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::Ok)], 54);
}

struct Cell { cm::Kind kind; cm::Entity entity; cm::Timing timing; };
std::vector<Cell> allCells() {
    std::vector<Cell> out;
    for (int k = 0; k < cm::kKinds; ++k)
        for (int v = 0; v < 2; ++v)
            for (int p = 0; p < 2; ++p)
                for (int t = 0; t < cm::kTimings; ++t)
                    out.push_back({static_cast<cm::Kind>(k), {static_cast<cm::Via>(v), static_cast<cm::Persist>(p)},
                                   static_cast<cm::Timing>(t)});
    return out;
}
std::string cellName(const testing::TestParamInfo<Cell>& i) {
    return std::string(cm::name(i.param.kind)) + "_" + (i.param.entity.via == cm::Via::Leader ? "Leader" : "Follower") +
           "_" + (i.param.entity.persist == cm::Persist::Disk ? "Disk" : "Transient") + "_" + std::string(cm::name(i.param.timing));
}

class CatalogInteractionMatrixRegression : public testing::TestWithParam<Cell> {};

TEST_P(CatalogInteractionMatrixRegression, Cell) {
    const Cell c = GetParam();
    const cm::Outcome want = cm::expect(c.kind, c.entity, c.timing);
    if (want == cm::Outcome::NotApplicable) GTEST_SKIP() << "R" << cm::naRule(c.kind, c.entity, c.timing);

    CatalogClusterHarness h(cellName({c, 0}), /*brokers=*/3);
    h.buildPrecondition(c.kind, c.entity.persist);            // the "Precondition" column of the plan's ops table
    const auto run = h.runCell(c.kind, c.entity, c.timing);   // drives the timing, records every answer

    // 1. exact first answer + final result
    switch (want) {
    case cm::Outcome::Ok:                         EXPECT_EQ(run.first.code, 0); break;
    case cm::Outcome::RedirectThenOk:             EXPECT_EQ(run.first.code, 3);
                                                  EXPECT_EQ(run.first.message, h.leaderClientEndpoint()); break;
    case cm::Outcome::NotLeaderThenOkOnce:        EXPECT_EQ(run.first.code, 2); break;
    case cm::Outcome::NotLeaderUntilBudgetThenOk: EXPECT_EQ(run.first.code, 2);
                                                  EXPECT_EQ(run.budgetExhaustedCode, 2);
                                                  EXPECT_TRUE(run.produceOnExistingTopicSucceeded); break;
    case cm::Outcome::OneWinsOneConflict:         EXPECT_EQ(run.okCount, 1);
                                                  EXPECT_EQ(run.loserCode, cm::conflictCode(c.kind)); break;
    case cm::Outcome::OkNewObjectId:              EXPECT_NE(run.objectIdAfter, run.objectIdBefore);
                                                  EXPECT_EQ(run.newVersionAfter, run.firstVersionOfNewObject); break;
    case cm::Outcome::OkAfterPlacement:           EXPECT_EQ(run.produceInWindowCode, 2);
                                                  EXPECT_TRUE(run.okReleasedOnlyAfterServing); break;
    case cm::Outcome::HaltedBrokerInternalOthersOk:
        EXPECT_TRUE(h.applierHalted(3)); EXPECT_EQ(h.haltCounter(3), 1);
        EXPECT_EQ(run.answerFromHaltedBroker, 6); EXPECT_EQ(run.answerViaLeader, 0);
        EXPECT_TRUE(run.haltedBrokerStillServesAppliedTopics); break;
    case cm::Outcome::NotApplicable: break;
    }
    EXPECT_EQ(run.finalCode, want == cm::Outcome::OneWinsOneConflict ? run.finalCode : 0);
    // 2. every DDL answered exactly once
    for (int b = 1; b <= 3; ++b) EXPECT_EQ(h.admin(b).startedCount(), h.admin(b).answeredCount()) << "broker " << b;
    EXPECT_TRUE(run.everyRequestHadOneTerminalResult);
    // 3. identical catalogs on every running non-halted broker
    EXPECT_TRUE(h.awaitDigestsEqual(/*excludeHalted=*/true));
    // 4. no DDL lost or duplicated
    for (uint64_t rid : run.okRequestIds)      EXPECT_EQ(h.countEventsWithRequestId(rid), 1u);
    for (uint64_t rid : run.refusedRequestIds) EXPECT_EQ(h.countEventsWithRequestId(rid), 0u);
    // 5. history shows the op
    EXPECT_TRUE(h.historyShows(c.kind, run.objectIdAfter, run.statement, run.okRequestIds));
    // 6. serving iff applied + placed
    EXPECT_TRUE(h.servingMatchesAppliedAndPlaced());
}

INSTANTIATE_TEST_SUITE_P(AllCells, CatalogInteractionMatrixRegression, testing::ValuesIn(allCells()), cellName);
```

- [ ] **Step 3: Implement `CatalogClusterHarness.h`** — `runCell` drives each timing exactly as the plan's timing table describes, using: `FilteringTransport::isolate/deisolate` (minIsr, catch-up), `holdHighWatermark(b)` (stop the followers' fetches so an append stays uncommitted), `moveCatalogLeadership(to)` (commits a new `(−6,0)` assignment through the scripted controller), `stopBroker(b)` / `restartBroker(b)` (rebuilds the rig on its dirs: checkpoint + log survive), `startFreshBroker(3)`, `applier(3).tamperNextFingerprintForTest()`, `placeAfter(b, topicId, delayPasses)` (delays the placement commit for `CreateRacesPlacement`), and the client model's `send(via, request)` (follows `Redirect`, retries `NotLeader` with the budget in deterministic passes). Every wait is bounded by a pass cap (`ASSERT`-fails with the cell name on overrun — never hangs).

- [ ] **Step 4: Run**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_catalog_matrix_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogMatrixCount|CatalogInteractionMatrix' --output-on-failure -j14`
Expected: 1 count pin PASS; 300 cells PASS, 24 SKIPPED (R1).

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(catalog): interaction matrix — expectation table + 3-broker harness + 324-cell parameterized unit matrix

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 21: Interaction matrix — seeded, model-checked stress with an independent catalog model

Spec §10: random DDL sequences with leader moves and restarts, checked against an independent catalog model. Invariants: identical fingerprint at the same applied offset on every broker; no DDL lost or duplicated; every DDL answered; two DDLs on one name never both succeed.

**Files:**
- Create: `libs/node/tests/support/CatalogModel.h` — **independent** of `rune_catalog`: plain `std::map<std::string, ModelTopic>` / `ModelView`, its own version counters, its own rules written from the spec's prose (create on a free name, drop needs no reader view, alters bump versions, reuse = new object); it never calls `rune::catalog::apply` — that is the point (a shared bug cannot hide in both).
- Create: `libs/node/tests/node/CatalogStressRegressionTest.cpp`; modify `libs/node/CMakeLists.txt` (`rune_catalog_stress_tests`, its own binary: long-running)

**Interfaces:**
- Consumes: `CatalogClusterHarness` (Task 20).
- Produces: `CatalogModel::{accept(const Request&) → ModelAnswer, apply(const ModelAnswer&), snapshot() → std::map<std::string, ModelObject>}`.

- [ ] **Step 1: Write the stress**:

```cpp
// 64 seeds x 400 steps.  Each step picks (weights): a DDL of a random kind on a random
// name from a 12-name pool via a random broker (55%), a concurrent pair of DDLs on the
// same name via two brokers (10%), move catalog leadership (8%), stop+restart a random
// broker (8%), isolate one broker for 20 passes (8%), start a fresh broker 3 (3%),
// tamper broker 3's next fingerprint (2%, then restart it with a cleared seam), pump 50
// passes (6%).  The client model follows Redirect and retries NotLeader with the budget.
class CatalogStressRegression : public testing::TestWithParam<uint32_t> {};

TEST_P(CatalogStressRegression, RandomDdlUnderChurnMatchesTheModel) {
    std::mt19937 rng(GetParam());
    rune::node::test::CatalogClusterHarness h("stress_" + std::to_string(GetParam()), 3);
    rune::node::test::CatalogModel model;
    std::map<std::string, std::set<uint64_t>> okRequestsByName;
    for (int step = 0; step < 400; ++step) {
        const auto action = h.randomAction(rng);
        const auto answers = h.perform(action);                       // every answer of this step, terminal ones marked
        for (const auto& a : answers) {
            if (!a.terminal) continue;
            const auto m = model.accept(a.request);                    // what the model says this DDL must answer
            if (a.code == 0) {
                ASSERT_TRUE(m.accepted) << "seed " << GetParam() << " step " << step << ": broker accepted, model refused";
                model.apply(m);
                okRequestsByName[a.request.name].insert(a.request.requestId);
            } else if (a.code == m.refusalCode) {
                // refused exactly as the model refuses
            } else {
                ASSERT_TRUE(a.code == 2 || a.code == 3 || a.code == 6)
                    << "seed " << GetParam() << " step " << step << ": unexpected code " << a.code;
            }
        }
        // Invariant: identical digests at equal applied offsets (non-halted brokers).
        ASSERT_TRUE(h.digestsEqualAtEqualOffsets()) << "seed " << GetParam() << " step " << step;
    }
    h.healAll();
    ASSERT_TRUE(h.awaitDigestsEqual(/*excludeHalted=*/true));
    // Invariant: every DDL answered.
    for (int b = 1; b <= 3; ++b) EXPECT_EQ(h.admin(b).startedCount(), h.admin(b).answeredCount());
    // Invariant: no DDL lost or duplicated.
    for (const auto& [name, rids] : okRequestsByName)
        for (uint64_t rid : rids) EXPECT_EQ(h.countEventsWithRequestId(rid), 1u) << name;
    // Invariant: two creates on one name never both succeed (per live object generation).
    EXPECT_TRUE(h.noNameHadTwoConcurrentLiveObjects());
    // The broker catalog equals the independent model.
    EXPECT_EQ(h.catalogAsModelSnapshot(), model.snapshot());
}

INSTANTIATE_TEST_SUITE_P(Seeds, CatalogStressRegression, testing::Range(1u, 65u));
```

- [ ] **Step 2: Implement `CatalogModel.h`** and the harness additions (`randomAction`, `perform`, `digestsEqualAtEqualOffsets`, `noNameHadTwoConcurrentLiveObjects` — checks the log: between a create and the drop of the same name, no other create of that name; `catalogAsModelSnapshot` — the leader's `CatalogState` mapped to the model's shapes).

- [ ] **Step 3: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_catalog_stress_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogStress' --output-on-failure -j14`
Expected: 64 seeds PASS. A failure prints its seed + step: rerun that seed alone (`--gtest_filter='*Seeds/<n>'`), fix the bug, add a named `*Regression` unit test for it in the owning task's test file.

- [ ] **Step 4: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(catalog): seeded model-checked catalog stress (64 seeds x 400 steps, independent model)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 22: Interaction matrix — 3-node BrokerNode integration slice over real Aeron (closes the E2 carry-over)

Spec §10: "BrokerNode integration, 3 nodes: runtime `CreateTopic` and `CreateView` through every broker, with a leader kill mid-DDL. This closes the E2 carry-over."

**Files:**
- Create: `libs/node/tests/integration/CatalogClusterIntegrationTest.cpp`; modify `libs/node/CMakeLists.txt` (`rune_catalog_integration_tests`)

**Interfaces:**
- Consumes: `ShardTestClient` (wire DDL: `sendDefineTopic`, `awaitDefine`, plus new `sendDropTopic`, `sendAlterTopic`, `sendCreateView`, `awaitDdl` added here, following its existing `sendDefineTopic` pattern and carrying the DDL extension with a `requestId`), `cmatrix::expect`, Task 6's `ThreeNodes` helper (moved to `libs/node/tests/support/ThreeNodes.h`).

Cells: `{CreateTopic, CreateView, DropTopic, AlterSchema}` × `{Steady via node 1, Steady via node 2, Steady via node 3, LeaderChangeMidDdl (the catalog leader is stopped right after the DDL frame is sent), RestartMidApply (a follower is stopped and restarted while the DDL commits), NewBrokerJoins (node 3 first started after the DDL), DropAndReuseName, CreateRacesPlacement}` = **32** cells, each followed by: produce acks=all + subscribe from earliest **through every running broker's client endpoint** to the topic (or the view's source, then `GetViewRow`) and assert delivery — the E2 carry-over's failure mode.

- [ ] **Step 1: Write the slice** (parameterized over `(Kind, Timing)`; outcomes from `cmatrix::expect` with `Via` = the broker the DDL went to):

```cpp
TEST_P(CatalogClusterSliceRegression, RuntimeDdlThroughEveryBrokerThenDataThroughEveryBroker) {
    const auto [kind, timing, viaNode] = GetParam();
    ThreeNodes c = ThreeNodes::start(sliceDirName(GetParam()), timing == cm::Timing::NewBrokerJoins
                                     ? std::vector<int>{1, 2} : std::vector<int>{1, 2, 3});
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    c.buildPrecondition(kind);                                     // via DDL, through the leader
    const auto res = c.runSliceCell(kind, timing, viaNode);        // the DDL + the timing, real nodes
    EXPECT_EQ(res.finalCode, 0) << res.trace;
    if (timing == cm::Timing::NewBrokerJoins) c.startNode(3);
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(30s)) << "identical catalogs on every broker";
    for (int n : c.runningNodes()) {
        if (kind == cm::Kind::DropTopic) { EXPECT_EQ(c.produceCode(n, res.topicName), 7) << "node " << n; continue; }
        EXPECT_TRUE(c.produceAcksAllAndSubscribe(n, res.dataTopicName, 10s)) << "data through node " << n;
    }
    if (kind == cm::Kind::CreateView) EXPECT_TRUE(c.awaitViewRow(res.topicName, 15s));
    EXPECT_EQ(c.countEventsWithRequestId(res.requestId), 1u);
}
```

plus two named regressions that pin the carry-over by its old symptom:

```cpp
TEST(CatalogClusterRegression, MultiNodeRuntimeDefineTopicServesOnEveryBrokerRegression) { … define via node 2, produce/subscribe via 1, 2, 3 … }
TEST(CatalogClusterRegression, MultiNodeRuntimeCreateViewFoldsOnTheSourceLeaderRegression) { … }
```

- [ ] **Step 2: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_catalog_integration_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogCluster' --output-on-failure -j4`
Expected: PASS (`-j4`: each cell runs three brokers).

- [ ] **Step 3: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(catalog): 3-node BrokerNode slice — runtime DDL through every broker, leader kill mid-DDL (closes E2 multi-node DDL carry-over)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 23: Gates — boot replay of 10 000 events in < 1 s; hot-path benchmarks unchanged; zero allocations in steady state; every DDL answered

Spec §10 "Gates".

**Files:**
- Create: `benchmark/CatalogBootReplayBench.cpp` (in `rune_broker_benchmark`: builds a `__catalog` log of 10 000 events — 2 000 topics created, 2 000 schema alters, 2 000 config alters, 1 000 views created + altered, 1 000 drops — once, then times `BrokerNode` boot step `bootFromCatalog_()` alone via a `BrokerNode::bootFromCatalogForBench(dataDir)` static seam)
- Create: `libs/node/tests/node/CatalogGatesRegressionTest.cpp` (in `rune_node_tests`)
- Modify: root `CMakeLists.txt` (benchmark source list, after the last `benchmark/*.cpp` entry)

- [ ] **Step 1: Write the gate tests**:

```cpp
TEST(CatalogGatesRegression, BootReplayOfTenThousandEventsIsUnderOneSecond) {
    GateDir d("gate_boot");
    rune::node::test::writeCatalogLog(d.path, /*events=*/10'000);      // shared with the bench (support header)
    const auto t0 = std::chrono::steady_clock::now();
    const auto r = rune::node::BrokerNode::bootFromCatalogForBench(d.path);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    ASSERT_EQ(r.appliedEvents, 10'000);
    ASSERT_FALSE(r.halted);
#ifdef NDEBUG
    EXPECT_LT(ms, 1000) << "spec §10 gate";
#else
    EXPECT_LT(ms, 8000) << "Debug build: the Release gate is the benchmark (Step 3)";
#endif
}

TEST(CatalogGatesRegression, IdleCatalogAdminPassAllocatesNothing) {
    rune::node::test::CatalogDdlRig rig("gate_alloc", 3);
    rig.becomeServingLeader();
    for (int i = 0; i < 50; ++i) ASSERT_EQ(rig.define({.name = "t" + std::to_string(i), .partitions = 1}).code, 0);
    rig.subscribeCatalog(/*session=*/9);                                // an idle catalog reader too
    rig.pumpUntilIdle();
    rune::test::AllocCounter ac;
    for (int i = 0; i < 10'000; ++i) (void)rig.admin().doWork(rig.nowNs());
    EXPECT_EQ(ac.count(), 0u);
}

TEST(CatalogGatesRegression, ProduceAndPushReadAllocateNothingWithACatalogPresent) {
    // The existing hot-path zero-alloc rig (HotPathBAllocRegressionTest's), on a topic created by DDL.
    rune::node::test::HotPathBRig rig("gate_hot", rune::node::test::HotPathBRig::TopicFrom::CatalogDdl);
    rig.warm();
    rune::test::AllocCounter ac;
    rig.produceAndPushRead(100'000);
    EXPECT_EQ(ac.count(), 0u);
}
```

(`HotPathBRig::TopicFrom::CatalogDdl` is added in this task: the rig defines its topic through `TestCluster` instead of the removed static list.)

- [ ] **Step 2: Run the tests**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogGates|HotPathBAlloc|TransientDataPathAlloc|ZeroAlloc' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 3: Benchmarks** — boot replay (Release gate) and hot-path parity against the base commit:

```bash
cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-bench-rel && cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-bench-rel -j14 --target rune_broker_benchmark
/Users/vradu/Projects/se-worktrees/rune-broker/build-bench-rel/rune_broker_benchmark --benchmark_filter='CatalogBootReplay' --benchmark_repetitions=5
# hot-path parity: the same filter on the base (7d86db94, built in a scratch worktree) and on this branch
git -C /Users/vradu/Projects/se-worktrees/rune-broker worktree add /tmp/catalog-base 7d86db94
cmake -S /tmp/catalog-base -B /tmp/catalog-base/build-bench-rel -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON -DBUILD_TESTING=OFF -DCMAKE_TOOLCHAIN_FILE=/Users/vradu/Projects/se-worktrees/rune-broker/build/Debug/generators/conan_toolchain.cmake -DRUNE_PROTOCOL_INCLUDE=/Users/vradu/Projects/se-worktrees/rune-protocol/include -DRUNE_CODEC_ROOT=/Users/vradu/Projects/se-worktrees/rune-codec && cmake --build /tmp/catalog-base/build-bench-rel -j14 --target rune_broker_benchmark
for b in /tmp/catalog-base/build-bench-rel /Users/vradu/Projects/se-worktrees/rune-broker/build-bench-rel; do
  $b/rune_broker_benchmark --benchmark_filter='WorkerProduceLatency|ProducePath|PushRead|FusedRuntimePass' --benchmark_repetitions=5 --benchmark_report_aggregates_only=true
done
git -C /Users/vradu/Projects/se-worktrees/rune-broker worktree remove /tmp/catalog-base
```

Expected: `CatalogBootReplay` median < 1 s; every hot-path benchmark's median within ±5 % of the base (noise band; the catalog adds nothing to these paths). A regression beyond 5 % is a bug to find and fix in this task (with a `*Regression` test), not a number to accept. Record all medians in the commit body. (`/tmp/catalog-base` is a throwaway worktree the step removes; it is built against the base protocol headers' superset, which compile unchanged.)

- [ ] **Step 4: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add benchmark libs CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -F /tmp/catalog-gates-msg.txt
```

where `/tmp/catalog-gates-msg.txt` is written in Step 3 from the benchmark output: first line `test(catalog): gates — 10k-event boot replay < 1 s, idle catalog + hot path allocate nothing, hot-path benches at parity`, a blank line, one line per benchmark `<name>: base <median> -> branch <median> (<delta %>)` plus `CatalogBootReplay: <median>`, a blank line, `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

---

### Task 24: Docker e2e, 3 nodes — DDL through each broker, leader killed mid-DDL, export → replay into a fresh cluster → diff

Spec §10 "Docker e2e, 3 nodes": 1. DDL through each broker; 2. the `__catalog` leader killed mid-DDL; 3. `rune catalog export`, then `replay` into a fresh cluster; 4. `rune catalog diff` reports no differences.

**Files:**
- Create: `docker/docker-compose.catalog.yml` (two 3-node clusters as two compose projects from one file with `${CLUSTER}`-prefixed service names and distinct host ports; `RUNE_CATALOG_REPL_STREAM_ID` default), `docker/catalog-e2e.sh`, `docker/topics/catalog-e2e.rql`
- Modify: `rune-client-cpp/examples/view_probe.cpp` only if a view assertion needs it (it already reads views by name)

`docker/catalog-e2e.sh` (structure; bounded waits, `trap` teardown, no fixed sleeps as gates — the house style of `docker/view-engine-e2e.sh`):
1. Build the broker image (`docker compose -f docker/docker-compose.catalog.yml build`, exit status checked directly) and the client `rune-ql-probe` image (rune CLI).
2. `CLUSTER=a` up; wait (bounded, 180 s) until each node's role counter shows a consensus leader **and** `rune ql -e "show history;"` succeeds against node 1.
3. **DDL through each broker:** `rune ql -f /topics/catalog-e2e.rql --broker a-rune-1:40200` (topic `players` typed, `events` untyped `replicas 3`, `presence` transient), then `rune ql -e "alter topic players add column mana int32;" --broker a-rune-2:40200`, then `rune ql -e "alter topic events set retention.ms = 600000;" --broker a-rune-3:40200`, a view through node 2 (`view_probe --create-only`). Each must exit 0 (a follower answers `Redirect`; the CLI follows it).
4. **Leader killed mid-DDL:** find the catalog leader by a bounded poll of `docker logs` for the last `AdminWorker: __catalog role -> LeaderServing (epoch N)` line (Task 11 logs it on every promotion; the highest epoch wins), start `rune ql -e "topic midkill partitions 2;"` against it in the background, `docker kill` the leader container 50 ms later, wait for the CLI: it must exit 0 (retried on the new leader, deduplicated) **or** exit 1 with `NotLeader` after its budget — then re-run the same statement once: exit 0. Assert `show history topic midkill;` lists exactly one `CreateTopic`. Restart the killed node; wait until `rune catalog diff --from a-rune-1:40200 --to a-rune-<killed>:40200` exits 0 (all brokers converge).
5. Produce + consume through every broker on `events` (`rune_produce_subscribe` example, existing image) — data path after runtime DDL on 3 nodes.
6. **Export → replay → diff:** `rune catalog export --broker a-rune-1:40200 > /work/a.rcat`; `CLUSTER=b` up (fresh, empty); `rune catalog replay --from /work/a.rcat --into b-rune-1:40200 --cluster-id a`; `rune catalog diff --from a-rune-1:40200 --to b-rune-1:40200` → exit 0 and prints no differences. `rune ql -e "show history;" --broker b-rune-2:40200` shows every event with `replayedFrom a@<offset>`.
7. Print `CATALOG-E2E: OK`; teardown both projects.

- [ ] **Step 1: Write the compose file, the `.rql` and the script.**
- [ ] **Step 2: Run**

Run: `bash /Users/vradu/Projects/se-worktrees/rune-broker/docker/catalog-e2e.sh`
Expected: ends with `CATALOG-E2E: OK`, exit 0.
- [ ] **Step 3: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add docker
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(catalog): docker e2e — 3-node DDL via each broker, leader killed mid-DDL, export/replay/diff into a fresh cluster

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 25: Docs — "Topics are created by DDL", the catalog, halted-apply operations, release notes

**Files:**
- Modify: `docs/SELF-HOSTING.md` — new section **"Topics are created by DDL"** (no `RUNE_TOPICS`; `rune ql -e "topic events partitions 3 replicas 3;"`; the `with (…)` creation settings table mapping every former option; `replicationFactor` rules: `≤ broker count`, `≥ minIsr`, 0 = every broker; typed vs untyped topics; `alter topic … set` for the six alterable settings; `drop topic` / `drop view` rules), and **"The catalog (`__catalog`)"** (one partition on every broker, majority, never unclean / compacted / deleted; `RUNE_CATALOG_REPL_STREAM_ID`; restart and new-broker behaviour; `dataDir/__catalog/applied`; old `__topic_schemas` / `__views` dirs are ignored and may be deleted by hand).
- Create: `docs/CATALOG.md` — the event model (fields, kinds, `formatVer` rules), reads (`SHOW HISTORY` / `AS OF` / `DIFF`, client API), `rune catalog export|replay|diff` with examples, the error table of spec §9, and **"#halted"**: what a fingerprint-mismatch halt means, the metric (`catalog apply halted` counter type 1 000 217, applied-offset counter 1 000 218), and the operator procedure (stop the broker, move `dataDir/__catalog` aside, restart: it re-fetches the whole catalog from the leader and re-applies from offset 0; if the leader itself halted, `rune catalog export` from a healthy broker and compare with `rune catalog diff`).
- Create: `docs/release-notes/catalog-history.md` (breaking: `RUNE_TOPICS` removed — a set variable refuses to start; client version 8; new DDL ops and codes used; no migration).
- Modify: `rune-client-cpp/README.md` (the new API + CLI commands), `rune-ql/README.md` if present (the new statements).

- [ ] **Step 1: Write the docs.**
- [ ] **Step 2: Verify** every statement in them parses with the built CLI (`/Users/vradu/Projects/se-worktrees/rune-client-cpp/build/rune ql -e "<each statement>" --dry-run`), and rerun the grep gate: `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R NoRuneTopicsGate --output-on-failure`.
- [ ] **Step 3: Commit** (broker docs; client/ql READMEs in their repos)

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add docs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "docs(catalog): topics are created by DDL; __catalog; history/as-of/diff; replay; halted-apply runbook; release notes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint G (controller):** full `*Regression*` suites in broker, client, protocol, rune-ql and rune-codec; `docker/catalog-e2e.sh`, `docker/e2e.sh`, `docker/view-engine-e2e.sh`, `docker/transient-failover-e2e.sh`; then the final whole-branch review.

---
## Self-review

- **Spec coverage, section by section:**
  - §1 Goal (audit, time travel + diff, replay-to-rebuild, replication; fixes multi-broker DDL; serialization; no migration) → Tasks 1–3 (library), 9 (one leader, one AdminWorker: serialization), 6 + 7 (replication + apply on every broker), 13–15 (reads, QL, CLI replay), 16 (no migration: old dirs ignored), 22 (multi-broker DDL closed).
  - §2 Scope: the nine kinds → Task 1 (`EventKind`), Task 9 (each issued), Task 20 (each in the matrix); reserved tenant/ACL kinds → Task 1 (`ReservedKind` refused by v1); internal topics not in the log → the catalog only ever holds DDL objects (Task 9), `__catalog` / `__consumer_offsets` / txn state are built at formation (Task 6); placement stays in the metadata log → Task 8.
  - §3 `__catalog` properties → Task 6 (`makeCatalogPartitionConfig`, `CatalogReplicationRegression.CatalogIsNeverCompactedOrRetentionDeleted`, controller-ISR leader, disk-only), single source of truth → Tasks 7 + 16 (state = f(applied prefix); boot builds everything from it).
  - §4 Events (every field, drops are events, reused names get new ids) → Tasks 1–2; fingerprint of the resulting state, not stored → Task 2 (`previewFingerprint` / `apply` verify; the event carries only the hash).
  - §5.1 write path (validate, assign ids/versions, fingerprint, append, apply, answer; CreateTopic OK held until serving) → Task 9 (+ `DdlAnswerGate` unchanged and extended to `DdlResponse`); Redirect → Task 11 (gateway) + Task 5 (client follows).
  - §5.2 leader change (old leader answers NotLeader, new leader only after applying to the HW, client budget then explicit failure) → Task 9 (demotion → NotLeader), Task 11 (`CatalogLeaderCatchingUp`), Task 5 (budget), matrix R4.
  - §5.3 apply on every broker (formatVer rules, verify, update, distribute; restart; new broker; mismatch stops with error + metric, keeps serving) → Task 7 (applier, checkpoint, halt, counters), Task 8 (distribution), Task 16 (restart from the local copy), Task 6 (new broker from offset 0), matrix R2.
  - §5.4 definitions vs placement, C1 generalized → Task 8 (`createTopicAt` on apply, `addRuntimeTopic` / `removeRuntimeTopic`, the gateway's applied-and-placed rule), matrix R7.
  - §5.5 below minIsr → Task 9 (append refused → NotLeader), Task 11 (route Unknown), matrix R3 (produce/consume unaffected asserted).
  - §5.6 bootstrap during cluster formation, first leader empty → Task 6 (`FormationCreatesTheCatalogOnEveryBrokerWithACleanLeader`).
  - §6 frozen per-`formatVer` rules, one shared library, verify everywhere, golden logs → Task 2 (`ApplyV1` never edited; dispatch by `formatVer`), Task 3 (golden log + pins, `TheGoldenScriptStillProducesTheCheckedInBytes`), Tasks 7/13/15 (broker, client, CLI all replay through `rune_catalog`).
  - §7 reads (Subscribe from 0, client-side replay, cached prefix; history / as-of / diff in QL and API; CLI export / replay / diff with fingerprint check and `replayedFrom`) → Tasks 12, 13, 14, 15.
  - §8 removing `RUNE_TOPICS` (setting, parsing, static creation + recovery, collision rule, `rf=`; CreateTopic carries partitions / persistence / schema / `replicationFactor` with explicit bounds; `TestCluster::defineTopic`; docker via CLI; grep gate; SELF-HOSTING section) → Tasks 16 (recovery + collision rule gone), 17 (setting, parsing, `rf=`, `TestCluster`), 9 (rf bounds), 18 (integration tests), 19 (scripts + gate), 25 (SELF-HOSTING).
  - §9 errors table → Task 11 (Redirect, NotLeader, Internal when halted), Task 9 (TopicExists, specific codes, InvalidRequest with detail), Tasks 3/13/15 (replay / diff errors naming the offset).
  - §10 testing → matrix section + Tasks 20 (unit), 21 (stress, the four invariants), 22 (3-node slice), 24 (docker e2e, all four steps), 3 + 23 (gates: 10k boot replay < 1 s, hot-path parity, zero-alloc; "every DDL answered" is invariant 2 of every matrix cell and of the stress).
  - §11 relation to other work → Global Constraints "Carry-overs folded in"; Part D noted.
- **Placeholder scan:** no "TBD"/"TODO"/"fill in"/"similar to Task N". Runtime data is produced by named steps, never invented: the golden digests (Task 3 Step 4, printed by `make_catalog_golden`, then frozen) and the benchmark medians (Task 23 Step 3, written into the commit message file). Mechanical migrations (Tasks 17–19) give the exact rule, the exact file list and a before/after.
- **Type consistency:** `rune::catalog::{EventKind, CatalogEvent, Ops, CreateTopicOps…DropViewOps, TopicConfig, ConfigKey, ConfigChange, SchemaDef, ViewDef, Principal, ReplayedFrom, DecodeError, encode, encodeInto, decode, kindOf}` (Task 1) are used unchanged by Tasks 2, 3, 7, 9, 13, 15; `CatalogState`, `RequestRecord`, `Applied`, `ApplyError`, `apply`, `previewFingerprint`, `fingerprintOf`, `digest()` (Task 2) by 3, 7, 9, 13, 20, 21; `CatalogReplayer`, `RecordedEvent`, `history`, `stateAsOf`, `AsOf`, `HistoryFilter`, `diff`, `DiffEntry`, `ReplayError`, `describe`, `readRcat`/`writeRcat*`, `testing::goldenScriptV1` (Task 3) by 13, 15; protocol `DdlExtension`, `TopicConfigWire`, `ConfigChangeWire`, `DropTopic`/`AlterTopicConfig`/`DropView`/`AlterView`/`DdlResponse`, `kCatalogTopicId`, `kClientVersionCatalog` (Task 4) by 5, 9, 12, 13, 15; `kCatalogTopicId` (broker, Task 6) equals the protocol constant; `CatalogApplier`, `CatalogEffects`, `CatalogCheckpoint` (Task 7) by 8, 9, 16, 20; the `kA2c*` ids 5–11 and `kMsg*` ids 10–16 are assigned once (Tasks 6, 7, 8, 9, 12) with no reuse; `CatalogRole` variants, `InFlight{Idle, AwaitingCommit}`, `DdlRequest`, `DdlMeta`, `answeredCount()` (Task 9) by 10, 11, 20, 21; `CatalogRoute` (Task 11) by 20; `CatalogReadServer` (Task 12) by 20, 23; `TestCluster::defineTopic(TopicSpec)` / `(name, partitions)` (Task 17) by 18, 23; `cmatrix::{Kind, Via, Persist, Entity, Timing, Outcome, expect, naRule, conflictCode, name}` (Task 20) by 21, 22, 24.
- **Review Focus:** each of the six has its named test in the owning task (Tasks 9 + 11 + 20 + 21; 7 + 20; 8 + 20; 8 + 20; 7 + 11 + 16 + 20; 12).
- **Philosophy:** shared-nothing — admin owns the catalog replica / state / registries, control owns TopicRegistry / placement, the gateway reads a control-owned `CatalogRoute`, the cleaner gets overrides on its own ring, the client's record handler only feeds an SPSC ring; rings only — every new hop is a typed message on an existing SPSC ring (no new thread, no callback across threads); per-state variables — `CatalogRole`, `InFlight`, `CatalogApplier::{Applying, Halted}`, `TopicState`, `PendingCreateBehindDrop`, `CatalogRoute`; zero allocation — hot path untouched and re-measured (Task 23), idle applier / read server / admin pass proven allocation-free (Tasks 7, 12, 23); test + benchmark — every task has its tests, Tasks 3 and 23 benchmark; regression naming — every test of a fix is `*Regression*`; fix-on-sight — listed in Global Constraints rule 8, each with its task.

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-10-13-rune-catalog-history.md`. The user's overnight instruction (memory: autonomous run, merge locally, never push) applies to execution; the recommended method is **subagent-driven** — 25 tasks across five repos with many cross-task interfaces (the library, the wire, the applier, the write path), where a fresh reviewer per task catches an interface drift before the next task builds on it.
