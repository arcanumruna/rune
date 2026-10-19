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

## Pre-flight rulings (2026-10-13)

Binding rulings from the pre-flight scan (`.superpowers/sdd/2026-10-13-rune-catalog-history/preflight-rulings.md`), applied below. One line each, with the tasks they changed:

- **F1** — `Dispatch.h::isKnownTemplate` covers 64–78; the `DispatchTest.cpp` "unknown" pins move to 79 and `ControlMessagesConnectTest.cpp` pins client version 8 → Task 4.
- **F2** — the AlterView effect-order test uses the golden sinks (new 4, old 3) and asserts every step was found → Task 7.
- **F3** — `AlterTopic` on an untyped topic answers `UnknownTopic 7` (today's code, binding for Task 10) → Task 9.
- **F4** — the applier's effects are a per-state phase variant (`AwaitingBoot → Booted → Live`, no `recovery` flag); exactly one boot replay; Task 16 deletes Task 9's admin-thread replay → Tasks 7, 9, 16.
- **F5** — the grep gate reads an explicit allowlist file (`scripts/rune_topics_allowlist.txt`: the refusal check, its Regression test, the release note); `NodeConfig.cpp` keeps only a presence refusal; the refusal test lives in `NodeConfigTest.cpp`; tracked `build-release/` is untracked and build dirs are ignored; `AdminWorker.cpp`, `CreateViewRegressionTest.cpp`, `SchemaGatewayTest.cpp` are listed → Tasks 17, 18, 19, 25.
- **F6** — `FdBudget.cpp:75` estimates from the catalog's boot topics (the check moves from `main.cpp` into `BrokerNode::start`) → Task 17.
- **F7** — the single-threaded fake (`test_support::InvokerGateway`) serves only non-blocking unit tests; blocking `*AndWait`, catalog reads and `rune::cli::run(--broker)` are tested against real in-process `BrokerNode`s in the broker's new `rune_client_broker_tests` target → Tasks 5, 11, 13, 15, 22.
- **F8** — `ql::parseScript` (strict, multi-statement); `ql::parse` refuses trailing input; `rune ql -f` runs every statement in order and stops at the first error naming its index → Tasks 14, 15.
- **F9** — `SetCompatibility` applies through `applyOps` (the evolving descriptor's version bumps, as today) → Task 2.
- **F10** — the "name taken" precondition case is built unstamped → Task 2.
- **F11** — `CatalogGoldenTest.cpp` / `CatalogQueriesTest.cpp` includes → Task 3.
- **F12** — allocation gates use the repo's real pattern (per-binary `operator new` replacement with plain single-thread globals, own executable `rune_catalog_alloc_regression_tests`); the hot-path gate is the existing `HotPathBRig.h` `Rig` tests → Tasks 7, 12, 23.
- **F13** — a DDL whose commit times out answers the retriable `NotLeader 2`; `Internal 6` only on a halted broker → Task 9.
- **F14** — `RequestId` is 128-bit (random client instance id + counter); the event records a `requestFingerprint`; dedupe keys on (requestId, fingerprint); the same id with a different statement answers `InvalidRequest` → Tasks 1, 2, 3, 4, 5, 9 (types also in 11, 15, 20, 21, 22).
- **F15/F16** — every matrix cell's expected codes come from `cmatrix::expectedCodes`; R2/R5 final codes fixed; R3/R4 via a follower answer `Redirect` first → Tasks 20, 22.
- **F17** — `rune ql --request-id`; the Task 24 re-run reuses it → Tasks 15, 24.
- **F18** — history rows print `replayedFrom` → Task 15.
- **F19** — Task 12 kept: serving the existing Subscribe for −6 is transport; spec §7 wording amended.
- **F20** — `AlterSchema` ops are stored resolved to durable tags (`ResolvedAlterOp`) → Tasks 1, 2, 3, 9, 15.
- **F21** — the shared library lives in rune-codec; spec Repos line amended.
- **F22** — `timestampMs` = the leader's wall clock at append; spec §4 amended → Task 9.
- **F23** — the purge cross-reference is Task 16; the reuse test checks the old data is unreadable and that `dropped/0.t` existed then vanished → Task 8.
- **F24** — the below-minIsr test produces and consumes → Task 11.
- **F25** — Task 1 expects 9 tests → Task 1.
- **F26** — `MetadataService::assignment`; the bridge's `isCoordinatorTopic_`/`mapDriven_` include −6; `BrokerNode.h` listed → Task 6.
- **F27** — view sinks go through a separate `sinkApplied(SinkSpec)` effect → Task 7.
- **F28** — `CatalogApplier::lastApplied()` → Tasks 7, 9.
- **F29** — `CatalogReadServer::Sub` is `variant<Backlog, Live>`; the client's DDL retry state is a variant → Tasks 5, 12.
- **F30** — the stress accepts `Internal 6` only from a broker whose applier is halted → Task 21.
- **F31** — `replicas` is `int16_t` everywhere (QL parse bound) → Task 14.
- **F32** — one `TopicConfig` definition (`rune-protocol/include/rune/protocol/CatalogTypes.h`), reused by codec, client and rune-ql → Tasks 1, 4, 5, 14, 15.
- **F33** — `view_probe --create-only` is a listed change → Task 24.
- **F34** — Task 22 lists `CatalogReplicationTest.cpp` (it moves `ThreeNodes` out of it) → Task 22.
- **F35** — rune-codec gets a Release build dir; `rune_codec_bench` links `rune_catalog` → Task 3.
- **F36** — Task 10's occupant literal matches `kOccupantCount` (10 → 9) → Task 10.
- **F37** — a pre-v8 session's DDL on a broker that is not the catalog leader is answered `InvalidRequest` ("client too old for cluster DDL routing; upgrade") in its own response type, never left to wait out → Tasks 4, 11, 25.

## Execution context

- **Repos / branch:** create `feat/catalog-history` in each worktree under `~/Projects/se-worktrees/`:
  - `rune-broker`: `git -C ~/Projects/se-worktrees/rune-broker switch -c feat/catalog-history 7d86db94`
  - `rune-protocol`: `git -C ~/Projects/se-worktrees/rune-protocol switch -c feat/catalog-history efc25fb`
  - `rune-client-cpp`: `git -C ~/Projects/se-worktrees/rune-client-cpp switch -c feat/catalog-history d0a5c74`
  - `rune-ql`: `git -C ~/Projects/se-worktrees/rune-ql switch -c feat/catalog-history 0814827`
  - `rune-codec`: `git -C ~/Projects/se-worktrees/rune-codec switch -c feat/catalog-history 0ccf109`

  Verify each with `git -C <repo> log --oneline -1` before Task 1. Never touch `~/Projects/rune-protocol` or `~/Projects/rune-client-cpp` (other branches, dirty).
- **Builds (already configured — re-run cmake only after a `CMakeLists.txt` edit):**
  - broker tests: `build-fast` (Ninja, Debug, `BUILD_TESTING=ON`, `RUNE_PROTOCOL_INCLUDE` → `../rune-protocol/include`, `RUNE_CODEC_ROOT` → `../rune-codec`). After a CMake edit: `cmake ~/Projects/se-worktrees/rune-broker/build-fast`. If missing: `cmake -S . -B build-fast -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DCMAKE_TOOLCHAIN_FILE=~/Projects/se-worktrees/rune-broker/build/Debug/generators/conan_toolchain.cmake -DRUNE_PROTOCOL_INCLUDE=~/Projects/se-worktrees/rune-protocol/include -DRUNE_CODEC_ROOT=~/Projects/se-worktrees/rune-codec`.
  - broker benchmarks: `build-bench-rel` (Ninja, `BUILD_BENCHMARKS=ON`, `-O3 -DNDEBUG`).
  - client: `build` (Unix Makefiles; `RUNE_PROTOCOL_INCLUDE` → `../rune-protocol/include`, `RUNE_CODEC_ROOT` → `../rune-codec`, `RUNE_QL_ROOT` → `../rune-ql`).
  - protocol: `build` (target `rune_protocol_tests`). rune-ql: `build` (target `rune_ql_tests`). rune-codec: `build` (targets `rune_codec_tests`, new `rune_catalog_tests`) and `build-rel` (Release, `rune_codec_bench`; created in Task 3).
  - broker client-integration tests (F7): `build-fast` target `rune_client_broker_tests` (created in Task 11) compiles the client from `RUNE_CLIENT_ROOT` (default `../rune-client-cpp`) — the real `rune_client_api` / `rune_cli_lib` against real in-process `BrokerNode`s.
- **Test gate (CLAUDE.md rule 7):**
  - **Per task / fix round:** build, then run the executables covering the code the task touches plus every new or changed test, at `-j14` — each task's "Run" step names the exact filter: `ctest --test-dir <build> -R '<filter>' --output-on-failure -j14`.
  - **The FULL `*Regression*` suite runs only at the checkpoints**, in every repo touched since the last checkpoint: broker `ctest --test-dir build-fast -R Regression --output-on-failure -j14`; client `ctest --test-dir build -R Regression --output-on-failure -j14`; protocol `ctest --test-dir build -R Regression --output-on-failure`; rune-ql `ctest --test-dir build -R Regression --output-on-failure`; rune-codec `ctest --test-dir build -R Regression --output-on-failure -j14`. A checkpoint failure is bisected over the commits since the last green checkpoint.
  - **Checkpoints:** A after Task 3 (shared library); B after Task 5 (wire + client DDL); C after Task 8 (catalog log + apply + effects); D after Task 12 (write path, conformance port, leadership, reads); E after Task 15 (client reads, QL, CLI); F after Task 19 (boot from the catalog, `RUNE_TOPICS` gone, tests and scripts migrated); G after Task 25 — before the final whole-branch review.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Global Constraints

- **HARD RULES (CLAUDE.md, all eight), as they bind here:**
  1. **Shared-nothing.** The `__catalog` replica, `CatalogState`, `CatalogApplier`, `SchemaRegistry` and `ViewRegistry` are owned by the admin thread only; `TopicRegistry`, `ReplicaRegistry`, `MetadataService` by the control thread only; the gateway never reads catalog state — it gets a control-thread-owned `CatalogRoute` value. No locks, no atomics as coordination, no `thread_local`.
  2. **Rings are the only cross-thread channel.** Every new hop is a message on an existing SPSC ring (`AdminChannels.h`): role + ISR relays, applied-topic / dropped-topic / config / view-uninstall / halted effects, dropped-dir purges, catalog subscribe forwarding. No cross-thread callback, no shared pointer read across threads.
  3. **Per-state variables.** `AdminWorker` leadership is `std::variant<CatalogFollower, CatalogLeaderCatchingUp, CatalogLeaderServing>`; the in-flight DDL is `std::variant<Idle, AwaitingCommit>`; `CatalogApplier` is `std::variant<Applying, Halted>` and its effects phase `std::variant<AwaitingBoot, Booted, Live>` (boot vs live effects — never a `recovery` flag, F4); a `CatalogReadServer` subscription is `std::variant<Backlog, Live>` (F29); the client's DDL retry state is `std::variant<DdlSent, DdlNotLeaderSeen, DdlAwaitingResend>` (F29); a TopicRegistry entry is `Serving` or `Dropped`; a create behind a not-yet-retired drop of the same name is `PendingCreateBehindDrop`. No scattered booleans.
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
- **Error codes (spec §9) — no new protocol error codes:** Redirect `3` (ErrorResponse, message = leader client endpoint), NotLeader `2` (also a DDL whose commit did not complete within `kDdlTimeoutNs` — it may still commit, the retry dedupes, F13), TopicExists `25`, the existing specific codes, InvalidRequest `39` with a detail message (also: a reused `requestId` with a different statement, F14; a pre-v8 session's DDL on a non-leader, F37), Internal `6` on a halted broker only.
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

1. **Where the shared replay library lives.** Spec §6 / Repos put it under rune-client-cpp, but the broker must link it and has no client dependency. **Ruling (spec Repos line amended, F21):** `rune_catalog` lives in **rune-codec** (`include/rune/catalog/`, `src/catalog/`) — the one sibling every consumer (broker, client, CLI) already compiles from source — so all three link byte-identical rules. The client exposes it through its own API (Task 13).
2. **Schema events vs objects.** **Ruling:** a schema is an aspect of its topic object. `CreateSchema` / `AlterSchema` / `SetCompatibility` carry the topic's `objectId`; `prevVersion`/`newVersion` are the topic object's version (every event on the object bumps it by 1); the schema's own contiguous version (`u16`, the wire/`SchemaRegistry` version) is part of the resulting state. `SHOW SCHEMA t AS OF VERSION n` selects schema version `n`; `SHOW TOPIC t AS OF VERSION n` selects object version `n`.
3. **Which DDL emits which kind.** `DefineTopic` on a free name → `CreateTopic` (carrying the schema when the descriptor is non-empty; an empty descriptor is an **untyped** topic); `DefineTopic` with a descriptor on an existing untyped topic → `CreateSchema` (today's "schema-less adoption"); `AlterTopic` whose ops are all `SET COMPATIBILITY` → `SetCompatibility`, any other `AlterTopic` → `AlterSchema`; new wire ops `DropTopic`, `DropView`, `AlterView`, `AlterTopicConfig` → their kinds.
4. **`AlterView` semantics.** The view-engine spec defers the live background cut-over. **Ruling:** `AlterView` replaces the view's plan and output under the same `objectId` with a **fresh sink topic** (new `viewTopicId`); the old sink is dropped; the view re-folds from its source and `GetViewRow` answers `NotReady` until caught up. The event layout does not change when the live cut-over lands later (it changes serving, not definitions).
5. **`AlterTopicConfig` keys.** Alterable: `retention.ms`, `retention.bytes`, `delete.retention.ms`, `min.cleanable.dirty.ratio`, `min.compaction.lag.ms`, `max.compaction.lag.ms` (the cleaner consumes them; applied through a cleaner policy override on its own ring). Every other property (partitions, `replicationFactor`, persistence, cleanup policy, segment size, durability, tier, co-partition group, partition key) is immutable → `InvalidRequest` with detail.
6. **Drop rules.** `DropTopic` of a topic a view reads → `InvalidRequest` naming the view; `DropTopic` of a view name → `InvalidRequest` ("is a view; use DROP VIEW"); unknown names → `UnknownTopic (7)` with detail (there is no `UnknownView` error code; `ViewRowResponse` status 4 is a read status, not a DDL code).
7. **`__catalog` leadership.** "Unclean disabled" + "every broker" rules out following the consensus leader (a lagging consensus leader would lose committed events). **Ruling:** `__catalog` joins the controller-ISR path of the coordinator partitions (re-review MEDIUM-B, `MetadataService::decideCoordinatorPartition`): replicas = every broker, leader elected only from the ISR (`allowUnclean = false`), the controller prefers itself when it is in the ISR (so the catalog leader is normally the controller). Single-node keeps the consensus-driven path.
8. **What "applied" survives a restart.** There is no persisted HW. **Ruling:** the applier persists `dataDir/__catalog/applied` = (last applied offset, state digest) **before** posting any side effect; boot replays `[0, applied]` only (always committed) and catches up the rest live.
9. **Exactly-once DDL across a leader change.** **Ruling (F14):** every DDL carries a client `RequestId` (128-bit: a random 64-bit client instance id + a 64-bit counter, stable across retries) in the event's principal, next to the `requestFingerprint` the leader computes over the request's statement and operation bytes; the leader answers a retry whose `(requestId, requestFingerprint)` already committed with that event's original result, and a request whose `requestId` committed with a **different** fingerprint with `InvalidRequest 39` ("requestId reused for a different statement") — never a silent wrong success.
10. **Catalog reads and "caught up".** Spec §7 reuses Subscribe (amended F19: the broker serving that Subscribe for −6 is transport; replay stays client-side); a client must know when it has the whole committed prefix. **Ruling:** for topic id −6 only, the serving broker pushes one `Records` frame with `baseOffset = HW` and an empty records payload as the caught-up marker after the backlog, documented in `Protocol.h`; no layout change.
11. **View fingerprints across clusters.** Ids differ between clusters (spec §7 replay). **Ruling:** fingerprints cover definitions, never ids: a view's fingerprint covers its source **names** and its authored plan **normalized** (each source `topicId` replaced by its ordinal), never the compiled plan (which embeds ids).
12. **"New broker joining".** Rune peers are static configuration. **Ruling:** a new broker is a configured peer started with an empty data dir after the catalog has events; the coordinator replica set already names it; it replays from offset 0 and joins the ISR once caught up (AlterIsr).
13. **Where `principal.sourceAddress` comes from.** Connect carries no client name. **Ruling:** the DDL extension carries `clientName` + `requestId`; the gateway adds `sessionId` and the session's egress channel as the source address.
14. **"Resolved" ops (spec §4, F20).** **Ruling:** `AlterSchema` stores `ResolvedAlterOp`s — every field named by durable tag (`FieldDescriptor::id`; an ADD carries the tag it is assigned) — never the name-based `encodeAlterOps` blob. The leader resolves (`rune::catalog::resolveAlterOps`); apply maps tags back onto the evolving descriptor; `rune catalog replay` un-resolves against the target (`unresolveAlterOps`).
15. **`timestampMs` (spec §4 amended, F22).** **Ruling:** the leader's wall clock when it builds the event for the append; the commit time is unknown before the append and the event is immutable.

---

## Interaction matrix

**Single source of truth:** `libs/node/tests/support/CatalogMatrixExpectations.h` (Task 20) — `expect(Kind, Entity, Timing) → Outcome` and `expectedCodes(Kind, Entity, Timing) → ExpectedCodes` (every answer code a cell asserts, F15/F16). The unit matrix (Task 20), the seeded stress (Task 21), the 3-node integration slice (Task 22) and the docker e2e (Task 24) all read it; no test hard-codes an outcome.

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
| `CatalogBelowMinIsr` | `Via = Leader`: two followers are down and the op is issued to the surviving leader. `Via = Follower`: the op is issued to a follower, which answers `Redirect`; then both followers (the via broker included) are stopped before the followed request reaches the leader. Then one broker returns and the op is retried |
| `ConcurrentDdlDifferentBrokers` | the same op (same name / same `expectedVersion`) is sent to two different brokers in the same pass, different `requestId`s |
| `DropAndReuseName` | create → drop → create the same name, then the op (on the new object) |
| `CreateRacesPlacement` | the op's topic is applied on a broker before its placement commits; produce/subscribe are issued in that window |
| `FingerprintMismatch` (failure) | follower 3's next apply is fed a tampered fingerprint (test seam on its applier); the op is then sent to broker 3 and to the leader |

### Expected outcome per cell (`expect()`; rules applied top-down)

| # | Rule | Outcome |
|---|---|---|
| R1 | `CreateRacesPlacement` × kind ∉ {`CreateTopic`, `CreateView`, `AlterView`} | `NotApplicable` (no new topic / sink is placed) |
| R2 | `FingerprintMismatch` | `HaltedBrokerInternalOthersOk`: broker 3's applier is `Halted` at that offset, its halt counter = 1, a DDL sent to broker 3 is answered `Internal (6)` (never redirected), the same op via the leader is `Ok`, brokers 1 and 2 agree on the digest; broker 3 still serves produce/consume on topics it applied |
| R3 | `CatalogBelowMinIsr` | `NotLeaderUntilBudgetThenOk`: the leader answers `NotLeader (2)` until the client budget ends → explicit client failure `NotLeader` (final 2), nothing appended past the HW; produce/consume on an existing topic led by the survivor still succeed (acks=leader); after one broker returns, the retried op is `Ok` exactly once. Via a follower the first answer is `Redirect (3)`, then `NotLeader (2)` |
| R4 | `LeaderChangeMidDdl` | `NotLeaderThenOkOnce`: the leader's answer is `NotLeader (2)`, the retry (same `requestId`) answers `Ok` with the same `objectId` / version; exactly one event with that `requestId` in every broker's log. Via a follower the first answer is `Redirect (3)`, then `NotLeader (2)`, final `Ok` |
| R5 | `ConcurrentDdlDifferentBrokers` | `OneWinsOneConflict`: exactly one `Ok`; the other gets the kind's conflict code — creates `TopicExists (25)` / `ViewExists (32)`; `AlterSchema`/`SetCompatibility`/`AlterTopicConfig` with the same `expectedVersion` `SchemaConflict (31)`; drops `UnknownTopic (7)`; `AlterView` `SchemaConflict (31)`. The DDL sent to the leader is applied first (deterministic pass order): via the leader the cell's DDL wins (final 0, the other gets the conflict code); via a follower it is redirected and loses (first 3, final = the conflict code, the other 0) |
| R6 | `DropAndReuseName` | `OkNewObjectId`: the op applies to the new object (new `objectId`, its version chain restarts at 1); history keeps both objects distinct |
| R7 | `CreateRacesPlacement` (applicable kinds) | `OkAfterPlacement`: produce/subscribe in the window answer `NotLeader (2)`; the DDL `Ok` is released only after the topic serves on the leader's broker; afterwards produce/subscribe succeed |
| R8 | `Via = Follower` × {`Steady`, `RestartMidApply`, `NewBrokerJoins`} | `RedirectThenOk`: first answer `ErrorResponse{Redirect, leader endpoint}`, the followed request `Ok` |
| R9 | `Via = Leader` × {`Steady`, `RestartMidApply`, `NewBrokerJoins`} | `Ok` |

`Persist = Transient` changes no outcome; it changes what is asserted (a transient topic exists and is empty after `RestartMidApply`; a transient view re-folds).

**Exact codes per cell (`cmatrix::expectedCodes`, F15/F16).** Every applicable cell sent via a follower answers `Redirect (3)` first (message = the leader's client endpoint), then the leader's answer — except R2, where broker 3 is halted and answers `Internal (6)`, never a redirect. `ExpectedCodes{first, afterRedirect, final, other, viaLeader, retried}` (−1 = no such answer): R2 `{6, −1, 6, −1, 0, −1}`; R3 Leader `{2, −1, 2, −1, −1, 0}`, Follower `{3, 2, 2, −1, −1, 0}`; R4 Leader `{2, −1, 0, …}`, Follower `{3, 2, 0, …}`; R5 Leader `{0, −1, 0, conflict, …}`, Follower `{3, conflict, conflict, 0, …}`; R6/R7/R8/R9 Leader `{0, −1, 0, …}`, Follower `{3, 0, 0, …}`.

### Cell counts (pinned by `CatalogMatrixCountRegression.CellCountsArePinned`, Task 20)

9 kinds × 4 entities × 9 timings = **324 cells**: **300 applicable**, 24 `NotApplicable` (R1: 6 kinds × 4 entities). Applicable by rule: R2 `HaltedBrokerInternalOthersOk` 36, R3 `NotLeaderUntilBudgetThenOk` 36, R4 `NotLeaderThenOkOnce` 36, R5 `OneWinsOneConflict` 36, R6 `OkNewObjectId` 36, R7 `OkAfterPlacement` 12, R8 `RedirectThenOk` 54, R9 `Ok` 54 = **300**.

### Every cell asserts

1. Every answer code `cmatrix::expectedCodes` names — the first answer (template + code, `Redirect` endpoint = the leader's client endpoint), the first answer after a followed `Redirect`, the final client-visible result, and where the timing has them the concurrent DDL's result, the same op via the leader and the retried op. No cell hard-codes a code.
2. **Every DDL answered exactly once** (`AdminWorker::startedCount() == answeredCount()` on every broker; the client saw one terminal result per `requestId`).
3. **Identical catalogs:** every running, non-halted broker reaches the same `CatalogState::digest()` at the same applied offset.
4. **No DDL lost or duplicated:** each `requestId` that was answered `Ok` appears exactly once in the log; each refused one appears zero times.
5. **History:** the shared library's `history()` over the log shows the op with the right `kind`, `objectId`, `prevVersion → newVersion`, `statement`, `principal.requestId` (128-bit) and `principal.requestFingerprint`.
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
| `rune-protocol/include/rune/protocol/CatalogTypes.h` | the ONE `TopicConfig` definition (owned + view instantiations) and `RequestId` (F32, F14) — header-only, std-only | 1 |
| `rune-codec/include/rune/catalog/CatalogEvent.h`, `src/catalog/CatalogEvent.cpp` | event model, kinds, resolved alter ops, v1 fixed-layout encode/decode | 1 |
| `rune-codec/include/rune/catalog/CatalogState.h`, `src/catalog/CatalogState.cpp`, `src/catalog/ApplyV1.cpp`, `include/rune/catalog/CatalogFingerprint.h`, `src/catalog/CatalogFingerprint.cpp` | state, frozen v1 rules, fingerprints, digest | 2 |
| `rune-codec/include/rune/catalog/CatalogReplayer.h`, `CatalogQueries.h`, `CatalogFile.h` + `src/catalog/*.cpp`, `tools/make_catalog_golden.cpp`, `tests/catalog/golden/v1/*.rcat`, `benchmark/CatalogReplayBench.cpp`, `build-rel/` (Release build dir, not tracked) | replayer, history / as-of / diff, `.rcat`, golden logs, replay bench | 3 |
| `rune-codec/CMakeLists.txt`, `tests/catalog/*Test.cpp` | `rune_catalog` lib + `rune_catalog_tests` | 1–3 |
| `rune-protocol/include/rune/protocol/client/Protocol.h`, `SchemaMessages.h`, `Dispatch.h`, `tests/protocol/client/CatalogDdlMessagesTest.cpp`, `DispatchTest.cpp`, `ControlMessagesConnectTest.cpp`, `conformance/*` | templates 74–78 (known to `isKnownTemplate`), DDL extension, DefineTopic rf/config, `kCatalogTopicId`, client version 8 | 4 |
| `rune-client-cpp/include/rune/client/Client.h`, `src/Client.cpp`, `include/rune/client/codec/Codec.h`, `tests/support/InvokerGateway.h`, `tests/client/ClientDdlRedirectRegressionTest.cpp`, `tests/client/ClientCatalogDdlTest.cpp`, `CMakeLists.txt` (embeddable: shared sibling targets guarded) | new DDL ops, DDL options, Redirect / NotLeader / InvalidRequest handling for control ops, 128-bit request ids | 5 |
| `libs/node/include/rune/node/CatalogPartition.h`, `AdminIsrChannel.h`, `libs/node/src/BrokerNode.cpp` (`buildCatalog_`), `BrokerNode.h` (test hooks), `MetadataService.h`, `ReplicationMetadataBridge.h`, `NodeConfig.{h,cpp}` (`catalogReplStreamId`), `libs/node/tests/integration/CatalogReplicationTest.cpp` | `__catalog` partition, admin replication plane, controller-ISR placement, role/ISR relays | 6 |
| `libs/node/include/rune/node/CatalogApplier.h`, `CatalogCheckpoint.h`, `libs/node/src/CatalogApplier.cpp`, `libs/node/tests/node/CatalogApplierTest.cpp`, `libs/node/tests/support/CatalogRig.h`, `libs/node/tests/node/CatalogAllocRegressionTest.cpp` (own binary) | apply in offset order, verify, halt, checkpoint, projections, effects (phase variant) | 7 |
| `libs/gateway/include/rune/gateway/TopicRegistry.h`, `libs/gateway/src/TopicRegistry.cpp`, `libs/node/src/BrokerNode.cpp` (control drain), `libs/node/include/rune/node/AdminChannels.h`, `LogCleanerAgent.h`, `WorkerViewSet.{h,cpp}`, `libs/metadata/include/rune/metadata/MetadataExtensions.h`, `MetadataStateMachine.cpp`, `libs/node/tests/node/CatalogEffectsTest.cpp`, `libs/gateway/tests/gateway/CatalogGatewayTest.cpp` | control-thread effects, drop/purge, config override, view uninstall, placement add/remove | 8 |
| `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp`, `AdminChannels.h`, `DdlAnswerGate.h`, `AdminCreateTopic.h` (deleted), `libs/gateway/include/rune/gateway/IAdminRequestSink.h`, `ClientGateway.{h,cpp}`, `TopicRegistry.{h,cpp}`, `libs/node/tests/support/CatalogDdlRig.h`, `libs/node/tests/node/CatalogWritePathTest.cpp` + ported DDL tests | write path through the catalog leader; new DDL ops; rf bounds; dedupe | 9 |
| `libs/node/tests/support/{DdlHarness,DdlExpectations}.h`, `libs/node/tests/node/Ddl*Test.cpp`, `libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp` | DDL conformance suite ported to the catalog | 10 |
| `libs/gateway/include/rune/gateway/CatalogRoute.h`, `ClientGateway.cpp`, `AdminWorker.cpp`, `BrokerNode.cpp`, `libs/node/tests/node/CatalogLeadershipTest.cpp`, root `CMakeLists.txt` + `libs/node/CMakeLists.txt` (`rune_client_broker_tests`), `libs/node/tests/client/ClientDdlBrokerRegressionTest.cpp` | Redirect, NotLeader, catch-up gate, minIsr, halted, pre-v8 sessions; the real client against a real broker | 11 |
| `libs/node/include/rune/node/CatalogReadServer.h`, `libs/node/src/CatalogReadServer.cpp`, `ClientGateway.cpp`, `libs/node/tests/node/CatalogReadServerTest.cpp` | `__catalog` Subscribe served by the admin thread | 12 |
| `rune-client-cpp/include/rune/client/catalog/CatalogReader.h`, `src/catalog/CatalogReader.cpp`, `TopicHandle.h`, `Subscription.h`, `tests/catalog/CatalogReaderTest.cpp`, `CMakeLists.txt`; broker `libs/node/tests/client/ClientCatalogReadBrokerRegressionTest.cpp` | history / as-of / diff client API | 13 |
| `rune-ql/include/rune/ql/{Ast,Parser,Backends}.h`, `src/{Parser,Backends}.cpp`, `tests/CatalogStatementsTest.cpp`, `CMakeLists.txt` (`RUNE_PROTOCOL_INCLUDE`) | `SHOW HISTORY`, `AS OF`, `DIFF`, `DROP`, `ALTER … SET`, `replicas`, `with (…)`, untyped topics, `parseScript` | 14 |
| `rune-client-cpp/src/cli/Cli.cpp`, `src/cli/CatalogCommands.cpp`, `include/rune/cli/CatalogCommands.h`, `include/rune/client/view/ViewPlanAuthor.h`, `tests/RuneCliCatalogTest.cpp` (offline); broker `libs/node/tests/client/RuneCliBrokerRegressionTest.cpp` | `rune ql`, `rune catalog export|replay|diff` | 15 |
| `libs/node/src/BrokerNode.cpp`, `RuntimeTopicRecovery.{h,cpp}` (deleted), `TopicDirReconcile.{h,cpp}` (new), `SchemaRegistry.{h,cpp}`, `ViewRegistry.{h,cpp}`, `AdminWorker.{h,cpp}`, `libs/node/tests/node/CatalogBootTest.cpp` | boot from the catalog; retire `__topic_schemas`/`__views`/recovery | 16 |
| `NodeConfig.{h,cpp}`, `FdBudget.{h,cpp}`, `src/main.cpp`, `AdminWorker.cpp` (comment), `libs/node/tests/support/TestCluster.h`, `libs/node/tests/node/{BrokerNode,BrokerNodeGateway,NodeConfig,DdlHarnessRegression,TestCluster,FdBudget}Test.cpp`, `benchmark/FusedRuntimePassBench.cpp` | delete `RUNE_TOPICS`/`topics`/`rf=`; FD budget from the catalog; `TestCluster::defineTopic`; node tests migrated | 17 |
| the 21 `libs/node/tests/integration/*Test.cpp` listed in Task 18 + `CreateViewRegressionTest.cpp`, `SchemaGatewayTest.cpp` (comments) | integration tests create topics by DDL | 18 |
| `docker/*`, `docker/topics/*.rql`, `docker/create-topics.sh`, `dr-e2e/**`, `bench/kafka-compare/*`, `docs/**`, `scripts/check_no_rune_topics.py`, `scripts/rune_topics_allowlist.txt`, `.gitignore`, `build-release/` (untracked), `CMakeLists.txt` | scripts migrated to DDL; grep gate with an explicit allowlist | 19 |
| `libs/node/tests/support/{CatalogMatrixExpectations,CatalogClusterHarness}.h`, `libs/replication/include/rune/replication/testing/FilteringTransport.h`, `libs/node/tests/node/CatalogInteractionMatrixRegressionTest.cpp` | matrix table + parameterized unit matrix | 20 |
| `libs/node/tests/support/CatalogModel.h`, `libs/node/tests/node/CatalogStressRegressionTest.cpp` | seeded model-checked stress | 21 |
| `libs/node/tests/integration/CatalogClusterIntegrationTest.cpp`, `libs/node/tests/support/ThreeNodes.h` (moved out of `CatalogReplicationTest.cpp`), `libs/node/tests/client/ClientDdlClusterRegressionTest.cpp` | 3-node BrokerNode slice; the real client through every broker | 22 |
| `benchmark/CatalogBootReplayBench.cpp`, `libs/node/tests/node/CatalogGatesRegressionTest.cpp`, `libs/node/tests/node/CatalogAllocRegressionTest.cpp` (append) | 10k boot replay < 1 s, hot-path parity, idle zero-alloc | 23 |
| `docker/catalog-e2e.sh`, `docker/docker-compose.catalog.yml`, `docker/topics/*.rql`, `rune-client-cpp/examples/view_probe.cpp` (`--create-only`) | docker e2e | 24 |
| `docs/SELF-HOSTING.md`, `docs/CATALOG.md`, `docs/release-notes/catalog-history.md` | docs | 25 |

---
### Task 1: `rune_catalog` — the event model and its frozen v1 encoding (rune-codec)

Spec §4: one fixed-layout, versioned record per event. This task defines the in-memory event (`CatalogEvent`), the nine kinds of §2 plus the reserved tenant/ACL range, and the `formatVer = 1` wire layout (encode + bounds-checked decode). Nothing else in the plan encodes or decodes an event by hand. It also creates the ONE `TopicConfig` definition and the 128-bit `RequestId` in rune-protocol (F32, F14), because this is the first task that needs them; Task 4 (wire), Task 5 (client) and Task 14 (rune-ql) reuse them.

**Files:**
- Create: `rune-protocol/include/rune/protocol/CatalogTypes.h` (header-only, std-only: `BasicTopicConfig<Str>`, `TopicConfig`, `TopicConfigView`, `convertTopicConfig`, `RequestId`, `RequestIdHash`)
- Create: `rune-codec/include/rune/catalog/CatalogEvent.h`
- Create: `rune-codec/src/catalog/Wire.h` (internal LE writer / reader)
- Create: `rune-codec/src/catalog/CatalogEvent.cpp`
- Modify: `rune-codec/CMakeLists.txt` (new `rune_catalog` library + `rune_catalog_tests`; `RUNE_PROTOCOL_INCLUDE` cache path, default `${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include`, on `rune_catalog`'s PUBLIC include dirs — the broker and the client already pass it)
- Test: `rune-codec/tests/catalog/CatalogEventTest.cpp`

**Interfaces:**
- Consumes: `rune::codec::{CompatMode, AlterOpType, PrimType}` (`include/rune/codec/AlterOps.h`, `PrimType.h`); `rune::protocol::{TopicConfig, RequestId, RequestIdHash}` (created here).
- Produces (namespace `rune::protocol`, `rune-protocol/include/rune/protocol/CatalogTypes.h`):
  ```cpp
  template <class Str> struct BasicTopicConfig;              // every field + its ONE default (F32)
  using TopicConfig     = BasicTopicConfig<std::string>;      // owned: catalog, client, rune-ql
  using TopicConfigView = BasicTopicConfig<std::string_view>; // the wire's zero-copy decode (Task 4)
  template <class To, class From> [[nodiscard]] To convertTopicConfig(const From&);   // the one field-for-field copy
  struct RequestId { uint64_t hi = 0, lo = 0; constexpr bool none() const noexcept; auto operator<=>(const RequestId&) const = default; };
  struct RequestIdHash { std::size_t operator()(const RequestId&) const noexcept; };
  ```
- Produces (namespace `rune::catalog`) — used by every later task:
  ```cpp
  enum class EventKind : uint16_t { CreateTopic = 1, DropTopic = 2, AlterTopicConfig = 3,
      CreateSchema = 4, AlterSchema = 5, SetCompatibility = 6, CreateView = 7, AlterView = 8, DropView = 9 };
  inline constexpr uint16_t kFormatV1 = 1, kCurrentFormatVer = kFormatV1;
  inline constexpr uint16_t kReservedKindFirst = 32, kReservedKindLast = 63;   // tenants + ACLs
  enum class Persistence : uint8_t { Disk = 0, Transient = 1 };
  using TopicConfig = rune::protocol::TopicConfig;  using RequestId = rune::protocol::RequestId;
  struct SchemaDef;  struct ViewDef;  enum class ConfigKey : uint8_t;  struct ConfigChange;
  struct ResolvedAlterOp;   // an AlterSchema op with every field named by durable tag (spec §4, F20)
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
                                .authenticated = "", .requestId = {.hi = 0x0123456789ABCDEFull, .lo = 0xABCDEFull},
                                .requestFingerprint = 0x5151515151515151ull};
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
            rc::AlterSchemaOps{.ops = {{.type = rune::codec::AlterOpType::Add, .tag = 2, .name = "mana",
                                        .primType = rune::codec::PrimType::Int32},
                                       {.type = rune::codec::AlterOpType::Rename, .tag = 1, .name = "health"},
                                       {.type = rune::codec::AlterOpType::Drop, .tag = 0}}},
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
        EXPECT_EQ(d->principal.requestId, (rc::RequestId{0x0123456789ABCDEFull, 0xABCDEFull})) << "128-bit request id (F14)";
        EXPECT_EQ(d->principal.requestFingerprint, 0x5151515151515151ull);
        ASSERT_TRUE(d->replayedFrom.has_value());
        EXPECT_EQ(d->replayedFrom->offset, 12);
        if (const auto* a = std::get_if<rc::AlterSchemaOps>(&d->ops)) {   // F20: durable tags, never names
            ASSERT_EQ(a->ops.size(), 3u);
            EXPECT_EQ(a->ops[0].tag, 2); EXPECT_EQ(a->ops[1].tag, 1); EXPECT_EQ(a->ops[1].name, "health");
            EXPECT_EQ(a->ops[2].type, rune::codec::AlterOpType::Drop); EXPECT_TRUE(a->ops[2].name.empty());
        }
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
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include" CACHE PATH
    "rune-protocol include dir (rune/protocol/CatalogTypes.h: the one TopicConfig + RequestId)")
add_library(rune_catalog STATIC
    src/catalog/CatalogEvent.cpp)
target_include_directories(rune_catalog PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${RUNE_PROTOCOL_INCLUDE})
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

Run: `cmake ~/Projects/se-worktrees/rune-codec/build && cmake --build ~/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests`
Expected: compile error `'rune/catalog/CatalogEvent.h' file not found`.

- [ ] **Step 4a: Create `rune-protocol/include/rune/protocol/CatalogTypes.h`** (F32, F14 — the one definition; Task 4 pins it with protocol tests):

```cpp
#pragma once
// rune/protocol/CatalogTypes.h — catalog history: the ONE definition of a topic's
// creation config (F32) and of a DDL request id (F14), shared by rune-codec's
// rune_catalog, the wire (client/SchemaMessages.h), the client and rune-ql.
// Header-only and std-only: rune-codec and rune-ql include it with no other
// protocol header.  Defaults = today's TopicSpec defaults; no other copy exists.
#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace rune::protocol {

/// Str = std::string (owned) or std::string_view (the wire's zero-copy decode).
template <class Str>
struct BasicTopicConfig {
    uint8_t cleanupPolicy             = 0;            // 0 Delete, 1 Compact, 2 CompactDelete
    int64_t retentionMs               = -1;
    int64_t retentionBytes            = -1;
    int64_t deleteRetentionMs         = 86'400'000;
    int32_t minCleanableDirtyRatioPpm = 500'000;      // 0.5
    int64_t segmentRollBytes          = 64LL * 1024 * 1024;
    int64_t minCompactionLagMs        = 0;
    int64_t maxCompactionLagMs        = -1;
    int64_t retentionCheckMs          = 300'000;
    int64_t fileDeleteDelayMs         = 60'000;
    uint8_t durability                = 0;            // 0 Majority, 1 AllInSync
    uint8_t durabilityTier            = 1;            // 0 Leader, 1 Majority, 2 Strict
    Str     coPartitionGroup{};
    Str     partitionKey{};
    bool operator==(const BasicTopicConfig&) const = default;
};
using TopicConfig     = BasicTopicConfig<std::string>;
using TopicConfigView = BasicTopicConfig<std::string_view>;

/// The one field-for-field copy between the two instantiations.
template <class To, class From>
[[nodiscard]] To convertTopicConfig(const From& f) {
    To t;
    t.cleanupPolicy = f.cleanupPolicy; t.retentionMs = f.retentionMs; t.retentionBytes = f.retentionBytes;
    t.deleteRetentionMs = f.deleteRetentionMs; t.minCleanableDirtyRatioPpm = f.minCleanableDirtyRatioPpm;
    t.segmentRollBytes = f.segmentRollBytes; t.minCompactionLagMs = f.minCompactionLagMs;
    t.maxCompactionLagMs = f.maxCompactionLagMs; t.retentionCheckMs = f.retentionCheckMs;
    t.fileDeleteDelayMs = f.fileDeleteDelayMs; t.durability = f.durability; t.durabilityTier = f.durabilityTier;
    t.coPartitionGroup = decltype(t.coPartitionGroup)(f.coPartitionGroup);
    t.partitionKey     = decltype(t.partitionKey)(f.partitionKey);
    return t;
}

/// A DDL's logical id (F14): hi = the issuing client instance's random 64-bit id
/// (never 0), lo = that instance's counter.  {0, 0} = none (a pre-v8 client).
/// Stable across every resend of the same DDL.
struct RequestId {
    uint64_t hi = 0;
    uint64_t lo = 0;
    [[nodiscard]] constexpr bool none() const noexcept { return hi == 0 && lo == 0; }
    auto operator<=>(const RequestId&) const = default;
};
struct RequestIdHash {
    std::size_t operator()(const RequestId& r) const noexcept {
        return std::hash<uint64_t>{}(r.hi * 0x9E3779B97F4A7C15ull ^ r.lo);
    }
};

} // namespace rune::protocol
```

Commit it in rune-protocol before Step 4 (the codec includes it through `RUNE_PROTOCOL_INCLUDE`):

```bash
git -C ~/Projects/se-worktrees/rune-protocol add include/rune/protocol/CatalogTypes.h
git -C ~/Projects/se-worktrees/rune-protocol commit -m "feat(protocol): CatalogTypes.h — the one TopicConfig definition + 128-bit RequestId (catalog history F32/F14)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

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
//              str16 authenticated, u64 requestIdHi, u64 requestIdLo,
//              u64 requestFingerprint |
//   u8 hasReplayedFrom [str16 clusterId, i64 offset, str16 principal] |
//   bytes32 ops  = u8 opsTag (== kind) + the kind's body (see CatalogEvent.cpp)
// str16 = u16 length + bytes; str32/bytes32 = u32 length + bytes.
#include <rune/codec/AlterOps.h>
#include <rune/codec/PrimType.h>
#include <rune/protocol/CatalogTypes.h>

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

/// Every TopicSpec property a CreateTopic fixes — the ONE definition lives in rune-protocol
/// (F32): the wire, the client and rune-ql use the same struct and the same defaults.
using TopicConfig   = rune::protocol::TopicConfig;
/// The client's id of the logical DDL (F14, ruling 9): 128-bit, stable across retries.
using RequestId     = rune::protocol::RequestId;
using RequestIdHash = rune::protocol::RequestIdHash;

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
/// One AlterSchema operation RESOLVED against the evolving descriptor (spec §4, F20):
/// every field is named by its durable tag (FieldDescriptor::id), never by name.
struct ResolvedAlterOp {
    rune::codec::AlterOpType type     = rune::codec::AlterOpType::Add;
    uint16_t                 tag      = 0;     // Add: the tag it is assigned; Drop / Rename: the field's tag; SetCompat: 0
    std::string              name;            // Add: the new field's name; Rename: the NEW name; otherwise empty
    rune::codec::PrimType    primType = rune::codec::PrimType::Int32;          // Add only
    uint16_t                 fixedLen = 0;                                     // Add only (FixedStr width)
    rune::codec::CompatMode  mode     = rune::codec::CompatMode::Backward;     // SetCompatibility only
    bool operator==(const ResolvedAlterOp&) const = default;
};
struct AlterSchemaOps      { std::vector<ResolvedAlterOp> ops; };   // resolved (F20) — never encodeAlterOps names
struct SetCompatibilityOps { rune::codec::CompatMode mode = rune::codec::CompatMode::Backward; };
struct CreateViewOps       { ViewDef view; };
struct AlterViewOps        { ViewDef view; };                          // a FRESH sink (ruling 4)
struct DropViewOps         { int32_t viewTopicId = -1; };

using Ops = std::variant<CreateTopicOps, DropTopicOps, AlterTopicConfigOps, CreateSchemaOps,
                         AlterSchemaOps, SetCompatibilityOps, CreateViewOps, AlterViewOps, DropViewOps>;

/// Until authentication exists: session id, client name and source address.  Afterwards
/// `authenticated` carries the principal (same field, no layout change).  `requestId` is
/// the client's id of the logical DDL, stable across retries (ruling 9, F14); the leader's
/// `requestFingerprint` (FNV-1a-64 over the request's statement and operation bytes, Task 9)
/// lets a retry be told apart from a different statement that reuses the id.
struct Principal {
    int64_t     sessionId = 0;
    std::string clientName;
    std::string sourceAddress;
    std::string authenticated;
    RequestId   requestId{};               // {0, 0} = none (a pre-v8 client)
    uint64_t    requestFingerprint = 0;
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
            w.u16(static_cast<uint16_t>(o.ops.size()));
            for (const auto& a : o.ops) {   // u8 type, u16 tag, str16 name, u8 primType, u16 fixedLen, u8 mode
                w.u8(static_cast<uint8_t>(a.type)); w.u16(a.tag); w.str16(a.name);
                w.u8(static_cast<uint8_t>(a.primType)); w.u16(a.fixedLen); w.u8(static_cast<uint8_t>(a.mode));
            }
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
    case EventKind::AlterSchema: {
        AlterSchemaOps o; uint16_t n; if (!r.u16(n)) return std::unexpected(DecodeError::Truncated);
        o.ops.resize(n);
        for (auto& a : o.ops) {
            uint8_t t, pt, m;
            if (!r.u8(t) || !r.u16(a.tag) || !r.str16(a.name) || !r.u8(pt) || !r.u16(a.fixedLen) || !r.u8(m))
                return std::unexpected(DecodeError::Truncated);
            if (t < 1 || t > 4 || pt > static_cast<uint8_t>(rune::codec::PrimType::FixedStr) || m > 1)
                return std::unexpected(DecodeError::BadEnum);
            a.type = static_cast<rune::codec::AlterOpType>(t); a.primType = static_cast<rune::codec::PrimType>(pt);
            a.mode = static_cast<rune::codec::CompatMode>(m);
        }
        return o;
    }
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
    w.str16(e.principal.authenticated); w.u64(e.principal.requestId.hi); w.u64(e.principal.requestId.lo);
    w.u64(e.principal.requestFingerprint);
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
            !r.str16(e.principal.sourceAddress) || !r.str16(e.principal.authenticated) ||
            !r.u64(e.principal.requestId.hi) || !r.u64(e.principal.requestId.lo) || !r.u64(e.principal.requestFingerprint))
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

Run: `cmake --build ~/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests && ctest --test-dir ~/Projects/se-worktrees/rune-codec/build -R 'CatalogEvent' --output-on-failure -j14`
Expected: all 9 `CatalogEventRegression.*` PASS.

- [ ] **Step 8: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-codec add include/rune/catalog/CatalogEvent.h src/catalog/Wire.h src/catalog/CatalogEvent.cpp tests/catalog/CatalogEventTest.cpp CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-codec commit -m "feat(catalog): event model + frozen formatVer-1 encoding (catalog history §4)

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
  struct RequestRecord { int64_t offset; EventKind kind; uint64_t objectId; uint32_t newVersion; int32_t topicId; uint16_t schemaVersion;
                         uint64_t requestFingerprint; };   // F14: dedupe keys on (requestId, requestFingerprint)
  struct ApplyError  { int64_t offset; enum class Code : uint8_t { UnsupportedFormat, Precondition,
                       FingerprintMismatch, BadOps } code; uint64_t expected; uint64_t actual; std::string detail; };
  class CatalogState { /* see Step 3 */ };
  [[nodiscard]] std::expected<Applied, ApplyError> apply(CatalogState&, const CatalogEvent&, int64_t offset);
  [[nodiscard]] std::expected<uint64_t, ApplyError> previewFingerprint(const CatalogState&, const CatalogEvent&);
  [[nodiscard]] uint64_t fingerprintOf(const TopicObject&) noexcept;
  [[nodiscard]] uint64_t fingerprintOf(const ViewObject&) noexcept;
  [[nodiscard]] uint64_t fingerprintOf(const Tombstone&) noexcept;
  [[nodiscard]] std::string_view applyErrorName(ApplyError::Code) noexcept;
  // F20: the leader resolves name-based ALTER ops to durable tags; replay un-resolves them on a target.
  [[nodiscard]] std::expected<std::vector<ResolvedAlterOp>, std::string>
      resolveAlterOps(const SchemaState&, std::span<const rune::codec::AlterOp>);
  [[nodiscard]] std::expected<std::vector<rune::codec::AlterOp>, std::string>
      unresolveAlterOps(const SchemaState&, std::span<const ResolvedAlterOp>);
  ```

`resolveAlterOps` walks the ops in order against the evolving descriptor (`deserialize(evolvingBlob)`): an ADD gets `tag = nextTag + (number of ADDs before it)`; a DROP / RENAME of a name resolves to that **live** field's `FieldDescriptor::id` (a later op sees the effect of an earlier RENAME in the same list); a SET COMPATIBILITY carries its mode; a name that is not live → error naming it. `unresolveAlterOps` is the inverse on any state with the same definition (same fingerprint): tag → the live field's current name; an ADD's tag must equal the running `nextTag` (else error). Both are pure; `apply` uses `unresolveAlterOps` (below), so a resolved op is checked against the exact state it applies to.

**The frozen v1 apply rules** (in `ApplyV1.cpp`; every rule failing is `ApplyError::Precondition` naming the offset):

| Kind | Precondition | Effect on the state |
|---|---|---|
| `CreateTopic` | `objectName` free (no live topic or view), `objectId == nextObjectId`, `ops.topicId == nextTopicId`, `prevVersion == 0`, `newVersion == 1`, `partitions ≥ 1` | new `TopicObject` (version 1); `schema` → `SchemaState{evolvingBlob = descriptorBlob, schemaVersion = 1, nextTag, compat = Backward}`; `nextObjectId++`, `nextTopicId++` |
| `DropTopic` | live topic with that `objectId`, name, `topicId`; `prevVersion == version`; `newVersion == version + 1`; no live view lists it in `sourceNames` | topic removed; `Tombstone{finalVersion = newVersion}` recorded; its name is free |
| `AlterTopicConfig` | live topic; versions chain; `changes` non-empty, no key twice, `MinCleanableDirtyRatioPpm ∈ [0, 1'000'000]`, retention values `≥ -1` | the keys set; version + 1 |
| `CreateSchema` | live **untyped** topic; versions chain | `schema` set (schemaVersion 1); version + 1 |
| `AlterSchema` | live typed topic; versions chain; `ops` non-empty; `unresolveAlterOps(schema, ops)` succeeds (every tag live, every ADD's tag = the running `nextTag` — F20); `applyOps(evolving, unresolved, nextTag, schemaVersion+1)` succeeds | evolving blob replaced, `nextTag` advanced, `schemaVersion + 1`, a `SET COMPATIBILITY` op inside sets `compat`, a RENAME of the key field moves `keyField`; version + 1 |
| `SetCompatibility` | live typed topic; versions chain; `applyOps(evolving, {SET_COMPATIBILITY mode}, nextTag, schemaVersion+1)` succeeds | exactly today's `startAlter_` (F9): the evolving blob is replaced by that `applyOps` result (its descriptor `version` becomes `schemaVersion + 1`), `compat = mode`, `schemaVersion + 1`; version + 1 |
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
    { rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = createOps(1, false); e.objectName = "t";
      e.objectId = 2; e.newVersion = 1; cases.push_back({"name taken", e}); }   // unstamped (F10): no preview of an invalid event
    { rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = createOps(7, false); e.objectName = "u";
      e.objectId = 2; e.newVersion = 1; cases.push_back({"topicId != nextTopicId", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = createOps(1, false); e.objectName = "u";
      e.objectId = 9; e.newVersion = 1; cases.push_back({"objectId != nextObjectId", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::DropTopic; e.ops = rc::DropTopicOps{0}; e.objectName = "t";
      e.objectId = 1; e.prevVersion = 3; e.newVersion = 4; cases.push_back({"version chain broken", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::AlterSchema;
      e.ops = rc::AlterSchemaOps{{rc::ResolvedAlterOp{.type = cd::AlterOpType::Drop, .tag = 0}}}; e.objectName = "t";
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
    auto resolved = rc::resolveAlterOps(*s.topicByName("t")->schema, std::vector<cd::AlterOp>{add});
    ASSERT_TRUE(resolved.has_value()) << resolved.error();
    ASSERT_EQ(resolved->size(), 1u);
    EXPECT_EQ((*resolved)[0].tag, 2) << "an ADD is stored with the durable tag it is assigned (F20)";
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::AlterSchemaOps{*resolved}, "t", 1, 2, 3), 2));
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::SetCompatibilityOps{cd::CompatMode::Forward}, "t", 1, 3, 4), 3));
    const rc::TopicObject* t = s.topicByName("t");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->objectId, 1u); EXPECT_EQ(t->version, 4u);
    EXPECT_EQ(t->schema->schemaVersion, 3);   // create 1, alter 2, set-compat 3
    EXPECT_EQ(t->schema->compat, cd::CompatMode::Forward);
    EXPECT_EQ(t->schema->nextTag, 3);
}

TEST(CatalogApplyV1Regression, SetCompatibilityBumpsTheEvolvingDescriptorVersionLikeTodayRegression) {
    // F9: today's startAlter_ runs applyOps for SET COMPATIBILITY, which stamps the evolving
    // descriptor with the new schema version (Evolve.cpp) — projections and GetTopicSchema see it.
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::SetCompatibilityOps{cd::CompatMode::Forward}, "t", 1, 1, 2), 1));
    const rc::TopicObject* t = s.topicByName("t");
    EXPECT_EQ(t->schema->schemaVersion, 2);
    EXPECT_EQ(cd::deserialize(t->schema->evolvingBlob).version, 2) << "the evolving blob went through applyOps";
}

TEST(CatalogApplyV1Regression, ResolvedOpsNameFieldsByDurableTagAndRoundTrip) {
    // F20: a RENAME / DROP is stored by tag; unresolving on the same definition gives the names back.
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    cd::AlterOp ren{}; ren.type = cd::AlterOpType::Rename; ren.name = "hp"; ren.newName = "health";
    cd::AlterOp drop{}; drop.type = cd::AlterOpType::Drop; drop.name = "health";   // sees the rename before it
    const auto& schema = *s.topicByName("t")->schema;
    auto r = rc::resolveAlterOps(schema, std::vector<cd::AlterOp>{ren, drop});
    ASSERT_TRUE(r.has_value()) << r.error();
    EXPECT_EQ((*r)[0].tag, 1); EXPECT_EQ((*r)[0].name, "health");
    EXPECT_EQ((*r)[1].tag, 1); EXPECT_TRUE((*r)[1].name.empty());
    auto back = rc::unresolveAlterOps(schema, std::span<const rc::ResolvedAlterOp>(*r));
    ASSERT_TRUE(back.has_value()) << back.error();
    EXPECT_EQ((*back)[0].name, "hp"); EXPECT_EQ((*back)[0].newName, "health");
    EXPECT_FALSE(rc::resolveAlterOps(schema, std::vector<cd::AlterOp>{drop}).has_value()) << "'health' is not live yet";
}

TEST(CatalogApplyV1Regression, AlterSchemaWithAStaleAddTagIsAPrecondition) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    rc::CatalogEvent e; e.kind = rc::EventKind::AlterSchema; e.objectName = "t"; e.objectId = 1;
    e.prevVersion = 1; e.newVersion = 2;
    e.ops = rc::AlterSchemaOps{{rc::ResolvedAlterOp{.type = cd::AlterOpType::Add, .tag = 7, .name = "mana",
                                                    .primType = cd::PrimType::Int32}}};   // nextTag is 2
    const uint64_t before = s.digest();
    auto a = rc::apply(s, e, 1);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
    EXPECT_EQ(s.digest(), before);
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
    const rc::RequestId rid{.hi = 0xC11E, .lo = 77};
    auto e = stamped(s, createOps(0, true), "x", 1, 0, 1);
    e.principal.requestId = rid;
    e.principal.requestFingerprint = 0xF1;
    ASSERT_TRUE(rc::apply(s, e, 0));
    auto r = s.request(rid);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->offset, 0); EXPECT_EQ(r->objectId, 1u); EXPECT_EQ(r->topicId, 0); EXPECT_EQ(r->schemaVersion, 1);
    EXPECT_EQ(r->requestFingerprint, 0xF1u) << "F14: the dedupe key carries the statement fingerprint";
    EXPECT_FALSE(s.request(rc::RequestId{.hi = 0xC11F, .lo = 77}).has_value()) << "all 128 bits are the key";
    auto again = stamped(s, createOps(1, false), "y", 2, 0, 1);
    again.principal.requestId = rid;
    auto a = rc::apply(s, again, 1);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
    EXPECT_FALSE(s.request(rc::RequestId{}).has_value()) << "requestId {0,0} (pre-v8 clients) is never recorded";
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

Run: `cmake --build ~/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests`
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
    int64_t   offset             = -1;
    EventKind kind               = EventKind::CreateTopic;
    uint64_t  objectId           = 0;
    uint32_t  newVersion         = 0;
    int32_t   topicId            = -1;   // the topic / view sink id the answer names
    uint16_t  schemaVersion      = 0;    // topic kinds with a schema: the resulting schema version
    uint64_t  requestFingerprint = 0;    // F14: a retry must match it; a different statement is InvalidRequest
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
    /// The applied event carrying `requestId` (ruling 9, F14) — what its DDL answered.  Cold.
    [[nodiscard]] std::optional<RequestRecord> request(const RequestId& requestId) const;
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
    std::unordered_map<RequestId, RequestRecord, RequestIdHash> requests_;   // requestId → its applied event ({0,0} never stored)
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
/// F20: name-based ALTER ops → durable tags (the leader), and back (apply; `rune catalog replay`).
[[nodiscard]] std::expected<std::vector<ResolvedAlterOp>, std::string>
    resolveAlterOps(const SchemaState& schema, std::span<const rune::codec::AlterOp> ops);
[[nodiscard]] std::expected<std::vector<rune::codec::AlterOp>, std::string>
    unresolveAlterOps(const SchemaState& schema, std::span<const ResolvedAlterOp> ops);

} // namespace rune::catalog
```

(`resolveAlterOps` / `unresolveAlterOps` live in `src/catalog/CatalogState.cpp`; they deserialize the evolving blob once and track renames / adds through the list as described above.)

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

`run` implements each row (a `Precondition` error's `detail` names the rule, e.g. `"CreateTopic: name 'players' is taken"`, `"AlterSchema: version chain 3 -> 5, object at 4"`). `AlterSchema` runs `unresolveAlterOps(schema, ops)` (an error → `Precondition` with its message, F20), then `rune::codec::applyOps(deserialize(evolvingBlob), unresolved, nextTag, schemaVersion + 1)` inside a `try` (a throw or an `EvolveError` → `BadOps` with the message); `SetCompatibility` runs the same `applyOps` with the single `{AlterOpType::SetCompatibility, mode}` op (F9); the key field follows a RENAME exactly like `AdminWorker::startAlter_` (anchor `DDL conformance B8 / D3a`). `commit` updates the maps, counters (`nextObjectId_` / `nextTopicId_` exactly as the table says), `appliedOffset_ = offset`, and `requests_[e.principal.requestId] = RequestRecord{…, requestFingerprint = e.principal.requestFingerprint}` when the id is not `{0, 0}` (a requestId seen twice in a log is a `Precondition` error in `run` — the leader never writes one twice).

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

Run: `cmake ~/Projects/se-worktrees/rune-codec/build && cmake --build ~/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests && ctest --test-dir ~/Projects/se-worktrees/rune-codec/build -R 'CatalogEvent|CatalogApplyV1' --output-on-failure -j14`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-codec add include/rune/catalog src/catalog tests/catalog CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-codec commit -m "feat(catalog): catalog state + frozen v1 apply rules + id-free fingerprints

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
- Create (not tracked): `rune-codec/build-rel/` — the Release build dir for `rune_codec_bench` (F35)
- Modify: `rune-codec/CMakeLists.txt` (`rune_codec_bench` gains `benchmark/CatalogReplayBench.cpp` and links `rune_catalog`, F35)
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

`GoldenScript.h` (in `include/rune/catalog/testing/`, header-only, deterministic: fixed timestamps `1'760'000'000'000 + 1000*i`, fixed principals, request ids `RequestId{0x601D, i+1}` with request fingerprints `0xF00D'0000 + i` (F14), every AlterSchema built with `resolveAlterOps` on the growing state so its ops carry durable tags (F20)): builds **every kind at least twice** on a growing `CatalogState`, stamping each event with `previewFingerprint` — in order: CreateTopic `players` (typed, disk, rf 3); CreateTopic `events` (untyped, 4 partitions); CreateTopic `presence` (untyped, transient); CreateSchema on `events`; AlterSchema `players` (ADD `mana`); AlterSchema `players` (RENAME `hp`→`health`); SetCompatibility `players` forward; AlterTopicConfig `events` {retention.ms 60000}; AlterTopicConfig `events` {retention.bytes 1 GiB, min.cleanable.dirty.ratio 0.25}; CreateView `top_players` over `players`; AlterView `top_players`; CreateView `ev_view` over `events`; DropView `ev_view`; DropTopic `presence`; CreateTopic `presence` (reuse: new objectId, transient); DropView `top_players`; DropTopic `players`; CreateTopic `players` (reuse, untyped) — 18 events. `goldenScriptV1()` returns them as `std::vector<CatalogEvent>`.

`CatalogQueriesTest.cpp`:

```cpp
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <sstream>

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
#include <rune/catalog/testing/GoldenScript.h>
#include "CatalogGoldenPins.h"
#include <fstream>
#include <iterator>
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

Run: `cmake --build ~/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests`
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
cmake ~/Projects/se-worktrees/rune-codec/build && cmake --build ~/Projects/se-worktrees/rune-codec/build -j14 --target make_catalog_golden
mkdir -p ~/Projects/se-worktrees/rune-codec/tests/catalog/golden/v1
~/Projects/se-worktrees/rune-codec/build/make_catalog_golden ~/Projects/se-worktrees/rune-codec/tests/catalog/golden/v1/basic.rcat > /tmp/pins.txt
```

Create `tests/catalog/CatalogGoldenPins.h` as `#pragma once`, `#include <cstdint>`, `namespace rune::catalog::golden {` + the printed array verbatim + `}`. These 18 constants are frozen from now on.

- [ ] **Step 5: Replay benchmark** — `benchmark/CatalogReplayBench.cpp` (added to `rune_codec_bench` when `benchmark_FOUND`; the target becomes `add_executable(rune_codec_bench benchmark/ValidatorBenchmark.cpp benchmark/CatalogReplayBench.cpp)` + `target_link_libraries(rune_codec_bench PRIVATE rune_codec rune_catalog benchmark::benchmark)`, F35): `BM_CatalogReplay10k` builds 10 000 events once (cycling CreateTopic / AlterSchema / AlterTopicConfig / CreateView / DropView / DropTopic over 500 names, each stamped), encodes them, and per iteration replays all 10 000 through a fresh `CatalogReplayer`; reports `items_per_second`. Gate (asserted in Task 23 at broker level, recorded here): one iteration < 200 ms Release.

- [ ] **Step 6: Run the tests + bench**

Run: `cmake --build ~/Projects/se-worktrees/rune-codec/build -j14 --target rune_catalog_tests && ctest --test-dir ~/Projects/se-worktrees/rune-codec/build -R 'Catalog' --output-on-failure -j14`
Expected: all `CatalogEventRegression.*`, `CatalogApplyV1Regression.*`, `CatalogReplayerRegression.*`, `CatalogQueriesRegression.*`, `CatalogGoldenRegression.*` PASS.
Run (bench — create the Release build dir once, F35; the existing conan Release toolchain provides Google Benchmark):

```bash
cmake -S ~/Projects/se-worktrees/rune-codec -B ~/Projects/se-worktrees/rune-codec/build-rel -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=~/Projects/se-worktrees/rune-codec/build/Release/generators/conan_toolchain.cmake
cmake --build ~/Projects/se-worktrees/rune-codec/build-rel -j14 --target rune_codec_bench
~/Projects/se-worktrees/rune-codec/build-rel/rune_codec_bench --benchmark_filter=CatalogReplay
```

Expected: the configure step reports `benchmark` found (a missing benchmark package is a failure of this step, never a skip); < 200 ms per iteration; record the number in the commit body.

- [ ] **Step 7: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-codec add include/rune/catalog src/catalog tools/make_catalog_golden.cpp tests/catalog benchmark/CatalogReplayBench.cpp CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-codec commit -m "feat(catalog): verifying replayer, history/as-of/diff, .rcat, v1 golden log + replay bench

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
- Modify: `rune-protocol/include/rune/protocol/client/Dispatch.h` (F1: `isKnownTemplate` — the schema/view range becomes `DefineTopic (64) .. DdlResponse (78)`; the range comment lists 74–78. Without it the broker gateway's `checkFrame` (`ClientGateway.cpp`, anchor `checkFrame(`) drops every new DDL frame as `UnknownTemplate`)
- Modify: `rune-protocol/tests/protocol/client/DispatchTest.cpp` (F1: `DispatchCheckFrame.UnknownTemplateSeventyFour` becomes `KnownTemplatesSeventyFourToSeventyEight` (Ok for 74..78) + `UnknownTemplateSeventyNine`; the `isKnownTemplate` pin list (anchor `EXPECT_FALSE(rpc::isKnownTemplate(74));`) moves its unknown pin to 79 and adds `EXPECT_TRUE` for 74..78; the comment "74+ (unassigned schema/view gap)" becomes "79+")
- Modify: `rune-protocol/tests/protocol/client/ControlMessagesConnectTest.cpp` (F1: `VersionGates.ClientBrokerVersionBumpedProtocolVersionUnchanged` pins `kClientVersion == 8` with the comment `// v8: catalog history DDL (v7: transient lineage note + claim)`)
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
      rune::protocol::RequestId requestId{}; // 128-bit, stable across retries (ruling 9, F14)
      uint8_t          hasReplayedFrom = 0;
      std::string_view replayedClusterId; int64_t replayedOffset = -1; std::string_view replayedPrincipal;
  };
  using TopicConfigWire = rune::protocol::TopicConfigView;   // F32: the one definition (CatalogTypes.h, Task 1), zero-copy view
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
- `DdlExtension`: `varstr statement` (encoded as u32 length: statements can exceed 64 KiB), `varstr clientName`, `u64 requestIdHi`, `u64 requestIdLo`, `u8 hasReplayedFrom`, then when 1: `varstr clusterId`, `i64 offset`, `varstr principal`. (The request fingerprint is computed by the leader, Task 9 — never trusted from the wire.)
- `DefineTopic` v8: the existing body, then the persistence byte **always** written when the extension follows, then `i16 replicationFactor`, `TopicConfigWire` (`u8 cleanupPolicy, i64 retentionMs, i64 retentionBytes, i64 deleteRetentionMs, i32 minCleanableDirtyRatioPpm, i64 segmentRollBytes, i64 minCompactionLagMs, i64 maxCompactionLagMs, i64 retentionCheckMs, i64 fileDeleteDelayMs, u8 durability, u8 durabilityTier, varstr coPartitionGroup, varstr partitionKey`), then `DdlExtension`. Decode: after the persistence byte, `pos == avail` → no extension (`hasExt = false`, rf 0, default config); otherwise the full trailer must decode exactly to `avail` (anything else → nullopt).
- `AlterTopic` / `CreateView` v8: the existing body, then `DdlExtension` (same exact-to-end rule).
- `DropTopic` / `DropView`: `varstr name` + `DdlExtension`. `AlterView`: `varstr viewName, varstr keyField, u8 persistence, u32 expectedVersion, varbytes32 planBlob` + ext. `AlterTopicConfig`: `varstr topicName, u32 expectedVersion, u16 count, (u8 key, i64 value)*` + ext; a key outside 1..6 → nullopt.
- `DdlResponse`: `u16 errorCode, u64 objectId, u32 newVersion, i32 gateTopicId, varstr detail`.

Also, in the "Client versions" comment block, add: *"8 — catalog history: DDL extension trailer (statement, clientName, 128-bit requestId, replayedFrom), DefineTopic replicationFactor + topic config, DropTopic/AlterTopicConfig/DropView/AlterView (74–77) answered by DdlResponse (78). DDL to a broker that is not the `__catalog` leader is answered ErrorResponse{Redirect, leader client endpoint}; a leader change, a catching-up leader, a commit that did not complete in time or `__catalog` below minIsr answers NotLeader (retriable; the retry with the same requestId is deduplicated); a broker whose catalog apply halted answers Internal. Any DDL may be answered ErrorResponse{InvalidRequest, detail} (e.g. replicationFactor bounds, a requestId reused for a different statement). A session below 8 never receives a Redirect for DDL: on a broker that is not the catalog leader its DefineTopic/AlterTopic/CreateView is answered in its own response type with InvalidRequest (client too old for cluster DDL routing; upgrade)."* — and `inline constexpr bool clientUnderstandsCatalogDdl(int32_t v) noexcept { return v >= kClientVersionCatalog; }` next to the other `clientUnderstands*` helpers (F37). — and in `DataMessages.h` above `struct Subscribe`: *"Subscribe{topicId = kCatalogTopicId (−6), partition 0, partitionKnown 1} reads `__catalog` from ANY broker, served from that broker's replica up to its high watermark. After the committed backlog the broker pushes ONE Records frame with baseOffset = its HW and an EMPTY records payload — the caught-up marker (catalog subscriptions only); live events follow as they commit."*

- [ ] **Step 1: Write the failing tests** — `CatalogDdlMessagesTest.cpp`:

```cpp
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <array>
#include <type_traits>
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
    for (uint16_t t = 74; t <= 78; ++t) EXPECT_TRUE(pc::isKnownTemplate(t)) << t << " (F1: else checkFrame drops it)";
    EXPECT_FALSE(pc::isKnownTemplate(79));
    EXPECT_FALSE(pc::clientUnderstandsCatalogDdl(7));
    EXPECT_TRUE(pc::clientUnderstandsCatalogDdl(8));
}

TEST(CatalogDdlWireRegression, TopicConfigWireIsTheOneTopicConfigDefinition) {
    // F32: the wire view, the owned config (catalog / client / rune-ql) and their defaults are one definition.
    static_assert(std::is_same_v<pc::TopicConfigWire, rune::protocol::BasicTopicConfig<std::string_view>>);
    const rune::protocol::TopicConfig owned{};
    EXPECT_EQ(rune::protocol::convertTopicConfig<pc::TopicConfigWire>(owned), pc::TopicConfigWire{});
    rune::protocol::TopicConfig c; c.retentionMs = 5; c.partitionKey = "k";
    EXPECT_EQ(rune::protocol::convertTopicConfig<rune::protocol::TopicConfig>(
                  rune::protocol::convertTopicConfig<pc::TopicConfigWire>(c)), c);
}

TEST(CatalogDdlWireRegression, DefineTopicV8RoundTripsRfConfigAndExtension) {
    pc::DefineTopic m;
    m.sessionId = 1; m.correlationId = 2; m.topicName = "events"; m.partitionCount = 4;
    m.persistence = 0; m.replicationFactor = 3;
    m.config.retentionMs = 60'000; m.config.cleanupPolicy = 1; m.config.partitionKey = "k";
    m.hasExt = true; m.ext.statement = "topic events partitions 4 replicas 3;"; m.ext.clientName = "cli";
    m.ext.requestId = {.hi = 0xC11E, .lo = 77}; m.ext.hasReplayedFrom = 1; m.ext.replayedClusterId = "src"; m.ext.replayedOffset = 5;
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
    EXPECT_EQ(d->ext.requestId, (rune::protocol::RequestId{0xC11E, 77})) << "all 128 bits (F14)";
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
    pc::DropTopic m; m.topicName = "t"; m.ext.statement = "drop topic t;"; m.ext.requestId = {.hi = 1, .lo = 9};
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

Run: `cmake ~/Projects/se-worktrees/rune-protocol/build && cmake --build ~/Projects/se-worktrees/rune-protocol/build -j14 --target rune_protocol_tests`
Expected: compile errors (`DropTopic` is not a member of `TemplateId`, …).

- [ ] **Step 3: Implement** the constants, structs and the trailer rules above, following `DefineTopic`'s existing encode/decode style (`putVarStr`, `getVarStr`, `putVarBytes32`, `getVarBytes32`, `detail::putLE*`). Each new struct has `TEMPLATE_ID`, `encodedSize()`, `encode()`, `static decode()` returning `std::optional`. `DdlExtension` gets free functions `extEncodedSize`, `putExt`, `getExt` (the statement uses a u32 length).

- [ ] **Step 4: Conformance vectors** — extend `conformance/gen_vectors.cpp` with one vector per new frame (fixed field values as in the tests), regenerate (`cmake --build … --target gen_vectors && ./build/gen_vectors conformance/vectors`), list them in `conformance/manifest.md`.

- [ ] **Step 5: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-protocol/build -j14 --target rune_protocol_tests && ctest --test-dir ~/Projects/se-worktrees/rune-protocol/build -R 'CatalogDdlWire|DefineTopic|Conformance|DispatchCheckFrame|Dispatch|VersionGates' --output-on-failure`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-protocol add include tests conformance CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-protocol commit -m "feat(protocol): catalog DDL wire — templates 74-78, DDL extension, DefineTopic rf/config, kCatalogTopicId, client v8

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 5: Client — the new DDL ops, DDL options, and Redirect / NotLeader / InvalidRequest handling for every DDL

Spec §5.1–§5.2, §5.5, §9: "A broker that is not the `__catalog` leader answers with the existing Redirect … which clients already follow"; "Clients retry within their budget and then fail explicitly." Today the client follows `Redirect` for data ops only; a DDL answered `ErrorResponse` is left pending until its deadline. This task makes every DDL op (`DefineTopic`, `AlterTopic`, `CreateView`, and the new four) follow `Redirect`, retry `NotLeader` with a backoff inside its budget, surface `InvalidRequest` detail, and carry the DDL extension with a stable 128-bit `requestId` (F14).

**Files:**
- Modify: `rune-client-cpp/include/rune/client/Completion.h` (`OpKind`: append `DropTopic, DropView, AlterView, AlterTopicConfig` after `GetViewRow`)
- Modify: `rune-client-cpp/include/rune/client/Client.h` (`DdlOptions`, `TopicOptions`, `DdlResult`; the new ops in both forms — non-blocking `op(…, CompletionTarget, Token)` and `*AndWait`; `defineTopic` / `alterTopic` / `createView` overloads taking `DdlOptions` (and `TopicOptions` for `defineTopic`); `PendingCtl` gains the encoded `frame` and the `DdlRetry` state variant)
- Modify: `rune-client-cpp/src/Client.cpp` (encode the extension on every DDL; `DdlResponse` (78) dispatch; the `ErrorResponse` branch for pending DDL ops; the retry pump in the duty cycle next to `retryRedirectedSubscribes_`; the 128-bit request id source)
- Modify: `rune-client-cpp/include/rune/client/agent/ClientAgent.h`, `src/agent/ClientAgent.cpp` (`offerToEndpoint(channel, frame)` — the per-endpoint session pool `offerToPartition` already uses)
- Modify: `rune-client-cpp/tests/support/InvokerGateway.h` (test support only, F7: `std::map<uint16_t, std::function<void(int endpoint, const uint8_t*, std::size_t)>> templateHandlers` — consulted first in `onFrame`, after the dead-endpoint check; `void sendDdlResponse(int endpoint, const rune::protocol::client::DdlResponse&)`. Error answers use the existing `sendError(from, code, message, sessionId, corrId)`)
- Modify: `rune-client-cpp/CMakeLists.txt` (add the two tests to `rune_client_tests`; F7: wrap the definitions of the shared sibling targets `rune_fr`, `rune_crypto`, `rune_codec` and `rune_gso` in `if(NOT TARGET <name>)` so a parent project — the broker's `rune_client_broker_tests`, Task 11 — can embed this project with `add_subdirectory` and supply its own copies; the standalone build is unchanged)
- Test: `rune-client-cpp/tests/client/ClientDdlRedirectRegressionTest.cpp`, `rune-client-cpp/tests/client/ClientCatalogDdlTest.cpp` — **non-blocking** ops on the real single-threaded fake `test_support::InvokerGateway` (invoker mode: the test thread alternates `client.doWork()` and `gw.pump()`, the `Rig` / `drive()` pattern of `tests/client/InvokerClientTest.cpp` and `IngressEndpointRoutingTest.cpp`, copied into each file's anonymous namespace; an injected `ClientConfig::nanoClock` walks the backoff and the budget). The fake cannot serve a blocking call (it is pumped by the calling thread), so the blocking `*AndWait` forms are tested against real in-process `BrokerNode`s: single node in Task 11, three nodes in Task 22 (F7).

**Interfaces:**
- Consumes: Task 4's frames, `kClientVersion = 8`; `rune::protocol::{TopicConfig, RequestId}` (Task 1, `CatalogTypes.h`).
- Produces (namespace `rune::client`):
  ```cpp
  struct DdlOptions {
      std::string statement;              // original DDL text (QL/CLI fill it; "" for API-issued)
      std::string clientName;             // defaults to ClientConfig::clientName
      rune::protocol::RequestId requestId{};   // {0,0} = the client generates one; reused on every resend (F14)
      struct ReplayedFrom { std::string clusterId; int64_t offset = -1; std::string principal; };
      std::optional<ReplayedFrom> replayedFrom;   // `rune catalog replay` only (Task 15) → DdlExtension.hasReplayedFrom
  };
  struct TopicOptions { int16_t replicationFactor = 0; rune::protocol::TopicConfig config{}; };   // F32: the one definition
  struct DdlResult { uint16_t errorCode = 0; uint64_t objectId = 0; uint32_t newVersion = 0; std::string detail; };
  // Non-blocking (Completion: value = objectId, aux = newVersion, payload = detail / the ErrorResponse message):
  OpResult dropTopic(std::string_view name, const DdlOptions&, CompletionTarget, Token);
  OpResult dropView(std::string_view name, const DdlOptions&, CompletionTarget, Token);
  OpResult alterView(std::string_view name, std::string_view keyField, uint8_t persistence, uint32_t expectedVersion,
                     std::span<const uint8_t> planBlob, const DdlOptions&, CompletionTarget, Token);
  OpResult alterTopicConfig(std::string_view name, uint32_t expectedVersion,
                            std::span<const rune::protocol::client::ConfigChangeWire> changes, const DdlOptions&,
                            CompletionTarget, Token);
  OpResult defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                       std::span<const uint8_t> descriptorBlob /* empty = untyped */, bool validate, Persistence,
                       const TopicOptions&, const DdlOptions&, CompletionTarget, Token);
  OpResult alterTopic(std::string_view name, uint16_t expectedVersion, std::span<const uint8_t> opsBlob,
                      const DdlOptions&, CompletionTarget, Token);
  OpResult createView(std::string_view viewName, std::string_view keyField, uint8_t persistence,
                      std::span<const uint8_t> planBlob, const DdlOptions&, CompletionTarget, Token);
  // Blocking conveniences (pump-until-done, like every *AndWait):
  DefineTopicResult defineTopicAndWait(std::string_view name, uint32_t partitions, std::string_view keyField,
                                       std::span<const uint8_t> descriptorBlob, bool validate, Persistence persistence,
                                       const TopicOptions& topic, const DdlOptions& ddl, std::chrono::milliseconds timeout);
  AlterTopicResult  alterTopicAndWait(std::string_view name, uint16_t expectedVersion, std::span<const uint8_t> opsBlob,
                                      const DdlOptions&, std::chrono::milliseconds);
  CreateViewResult  createViewAndWait(std::string_view viewName, std::string_view keyField, uint8_t persistence,
                                      std::span<const uint8_t> planBlob, const DdlOptions&, std::chrono::milliseconds);
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
  // PendingCtl's DDL retry state — one explicit state per phase (hard rule 3, F29):
  struct DdlSent           { int redirects = 0; };                                         // no NotLeader seen yet
  struct DdlNotLeaderSeen  { int redirects; int64_t budgetEndNs; int64_t backoffNs; };     // resent, awaiting its answer
  struct DdlAwaitingResend { int redirects; int64_t budgetEndNs; int64_t backoffNs; int64_t resendAtNs; };
  using DdlRetry = std::variant<DdlSent, DdlNotLeaderSeen, DdlAwaitingResend>;
  // PendingCtl gains: std::vector<uint8_t> frame (the encoded request, re-sent verbatim); DdlRetry retry;
  ```

Behaviour (all on the duty thread, cold path):
- **Request id (F14):** every DDL frame carries `DdlExtension{statement, clientName, requestId}`; `requestId = !ddl.requestId.none() ? ddl.requestId : RequestId{clientInstanceId_, ++requestCounter_}`, where `clientInstanceId_` is a random 64-bit value drawn once at `Client` construction from `std::random_device` (two 32-bit draws; redrawn while 0). Distinct `rune ql` processes therefore collide only with 2⁻⁶⁴ odds per pair, and the broker additionally refuses a reused id whose statement differs (Task 9). The encoded frame is kept in `PendingCtl::frame` (re-sent verbatim, so the `requestId` is stable).
- **`ErrorResponse{Redirect, endpoint}` for a pending DDL corrId:** `redirects + 1` in the current retry state; above `kMaxDdlRedirects` → complete `BrokerError` with `errorCode = 3` and detail `"redirected more than 3 times"`. Otherwise, multi-node routing on → `agent_.offerToEndpoint(toChannel(endpoint), frame)`; off → the existing seed retarget (`submitReconnect`, the same call the data-op Redirect path makes) and the frame waits in `unsentFrame` to be re-offered once connected.
- **`ErrorResponse{NotLeader}` or a response with `errorCode == 2`:** `DdlSent` → `DdlAwaitingResend{budgetEndNs = min(deadline, now + ddlRetryBudget), backoffNs = 100 ms, resendAtNs = now + 100 ms}`; `DdlNotLeaderSeen` → `DdlAwaitingResend` with the backoff doubled (cap 1 s); when `now ≥ budgetEndNs` → complete `BrokerError` with `errorCode = 2` ("explicit client failure", spec §5.5). `resendDueDdl_` moves a due `DdlAwaitingResend` to `DdlNotLeaderSeen` by re-offering `frame`.
- **`ErrorResponse{InvalidRequest | other}`:** complete `BrokerError` with that code and `detail = message`.
- **`DdlResponse` (78):** complete with `errorCode`, `objectId`, `newVersion`, `detail`; `errorCode == 2` follows the NotLeader rule.

- [ ] **Step 1: Write the failing tests** — `ClientDdlRedirectRegressionTest.cpp` (non-blocking ops; the fake answers through `templateHandlers`):

```cpp
// Catalog history §5.1/§5.2/§9: every DDL follows Redirect, retries NotLeader inside
// its budget, then fails explicitly; the requestId is stable across every resend.
// Non-blocking ops on the single-threaded fake (F7): the test thread drives
// client.doWork() + gw.pump(); the blocking *AndWait forms are proven against
// real BrokerNodes (Task 11: one node; Task 22: three nodes).
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <unistd.h>
#include <vector>

namespace pc = rune::protocol::client;
namespace rc = rune::client;

namespace {
int64_t g_now = 1'000'000'000;                      // test thread only: invoker mode has one thread
int64_t fakeNow() noexcept { return g_now; }
std::atomic<int> g_ctr{0};

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {   // as InvokerClientTest.cpp's invokerCfg, plus the clock
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = "/tmp/ddl_redirect_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock = &fakeNow;
    return c;
}
struct Rig {   // InvokerClientTest.cpp's Rig: client + fake on the client's embedded driver
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P> bool drive(P&& pred, int64_t stepNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            g_now += stepNs; client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
    void answerError(int ep, pc::ErrorCode code, const std::string& msg, int64_t session, int64_t corr) {
        gw.sendError(ep, static_cast<int16_t>(code), msg, session, corr);
    }
};
std::string payloadText(const test_support::PendingOp& f) {
    const auto b = f.bytes(); return {b.begin(), b.end()};
}
} // namespace

TEST(ClientDdlRedirectRegression, NotLeaderIsRetriedWithTheSameRequestIdThenSucceeds) {
    Rig r(invokerCfg(3960, 3961));
    r.connect();
    std::vector<rune::protocol::RequestId> seen;
    int n = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        seen.push_back(m->ext.requestId);
        if (n++ < 2) { r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId); return; }
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.objectId = 4; resp.newVersion = 2;
        r.gw.sendDdlResponse(ep, resp);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/50'000'000));   // walks the 100 ms → 200 ms backoff
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 4);
    EXPECT_EQ(f.outcome().c.aux, 2);
    ASSERT_EQ(seen.size(), 3u);
    EXPECT_FALSE(seen[0].none());
    EXPECT_NE(seen[0].hi, 0u) << "the random client instance id (F14)";
    EXPECT_EQ(seen[0], seen[1]);
    EXPECT_EQ(seen[1], seen[2]);
}

TEST(ClientDdlRedirectRegression, NotLeaderPastTheBudgetIsAnExplicitFailureNotAHang) {
    rc::ClientConfig cfg = invokerCfg(3962, 3963);
    cfg.ddlRetryBudget = std::chrono::milliseconds(400);
    Rig r(cfg);
    r.connect();
    r.gw.templateHandlers[pc::DefineTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DefineTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("t", 1, "", {}, false, rc::Persistence::Disk, {}, {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
    ASSERT_TRUE(f.failed());
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    EXPECT_LT(g_now - t0, int64_t{2'000'000'000}) << "the 400 ms budget, not the op timeout, ends it";
}

TEST(ClientDdlRedirectRegression, RedirectIsFollowedToTheLeaderEndpoint) {
    Rig r(invokerCfg(3964, 3965));
    const std::string leaderCh = "aeron:udp?endpoint=127.0.0.1:24801";
    const int leader = r.gw.addEndpoint(leaderCh, 3964);          // a second fake node (IngressEndpointRoutingTest)
    r.connect();
    int leaderSaw = 0;
    r.gw.templateHandlers[pc::AlterTopicConfig::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::AlterTopicConfig::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (ep != leader) { r.answerError(ep, pc::ErrorCode::Redirect, leaderCh, m->sessionId, m->correlationId); return; }
        ++leaderSaw;
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId; resp.newVersion = 3;
        r.gw.sendDdlResponse(ep, resp);
    };
    const pc::ConfigChangeWire ch{1, 60'000};
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.alterTopicConfig("t", 0, std::span(&ch, 1), {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(leaderSaw, 1);
    EXPECT_EQ(f.outcome().c.aux, 3);
}

TEST(ClientDdlRedirectRegression, RedirectLoopEndsExplicitly) {
    Rig r(invokerCfg(3966, 3967));
    const std::string loopCh = "aeron:udp?endpoint=127.0.0.1:24803";
    (void)r.gw.addEndpoint(loopCh, 3966);
    r.connect();
    r.gw.templateHandlers[pc::DropView::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropView::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::Redirect, loopCh, m->sessionId, m->correlationId);   // every node: "ask loopCh"
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropView("v", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.failed());
    EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::Redirect));
    EXPECT_NE(payloadText(f).find("redirected more than"), std::string::npos);
}

TEST(ClientDdlRedirectRegression, InvalidRequestDetailReachesTheCaller) {
    Rig r(invokerCfg(3968, 3969));
    r.connect();
    r.gw.templateHandlers[pc::DefineTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DefineTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::InvalidRequest, "replicationFactor 5 > broker count 3", m->sessionId,
                      m->correlationId);
    };
    rc::TopicOptions topic; topic.replicationFactor = 5;
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("t", 1, "", {}, false, rc::Persistence::Disk, topic, {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.errorCode, 39);
    EXPECT_EQ(payloadText(f), "replicationFactor 5 > broker count 3");
}
```

`ClientCatalogDdlTest.cpp` (same `Rig`, non-blocking) pins the encodings the client sends: `defineTopic` with an empty descriptor sends `descriptorLen 0`, `replicationFactor`, the config (`TopicConfigWire` equal to the `TopicOptions::config` converted with `convertTopicConfig`) and the extension (`statement` and `clientName` from `DdlOptions` / `ClientConfig`); `alterView` sends `AlterView` with `expectedVersion`; every one of the 7 DDL ops sets a non-`none()` `ext.requestId`; two different ops get different `requestId`s with the same `hi`; two `Client` instances get different `hi` (F14); an explicit `DdlOptions::requestId` is sent verbatim.

- [ ] **Step 2: Run to verify they fail**

Run: `cmake ~/Projects/se-worktrees/rune-client-cpp/build && cmake --build ~/Projects/se-worktrees/rune-client-cpp/build -j14 --target rune_client_tests`
Expected: compile errors (`dropTopic` is not a member of `rune::client::Client`; `templateHandlers` is not a member of `test_support::InvokerGateway`).

- [ ] **Step 3: Implement** the ops, options, the `DdlRetry` variant (no scattered retry fields), the `ErrorResponse` branch (placed **before** the data-op Redirect branch, matched by `findCtl_(corrId)` and an `isDdlOp(op)` predicate covering `DefineTopic, AlterTopic, CreateView, DropTopic, DropView, AlterView, AlterTopicConfig`), the `DdlResponse` case, and `int Client::resendDueDdl_(int64_t nowNs)` called in the duty cycle next to `retryRedirectedSubscribes_`. `ClientAgent::offerToEndpoint(const std::string& channel, const uint8_t*, std::size_t)` reuses the per-endpoint session lookup `offerToPartition` uses. The `*AndWait` forms are the usual pump-until-done wrappers over the non-blocking ops (their real-broker tests land in Tasks 11 and 22). Add the `if(NOT TARGET …)` guards to `CMakeLists.txt` and reconfigure the standalone build to prove it unchanged.

- [ ] **Step 4: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-client-cpp/build -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-client-cpp/build -R 'ClientDdlRedirect|ClientCatalogDdl|ClientSchema|Redirect|InvokerClient|IngressEndpointRouting|Conformance' --output-on-failure -j14`
Expected: PASS (existing `RedirectTest`, `ClientSchemaTest`, `InvokerClientTest` unchanged).

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-client-cpp add include src tests CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-client-cpp commit -m "feat(client): catalog DDL ops + DDL extension; every DDL follows Redirect and retries NotLeader in budget

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint B (controller):** full `*Regression*` suites in protocol and client; broker `build-fast` still builds against the new protocol headers (`cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14`).

---

### Task 6: Broker — the `__catalog` partition: bootstrap, admin-thread replication plane, controller-ISR placement (never unclean)

Spec §3, §5.6, §5.3 "New broker". Today's catalog logs ride a `NullPartitionTransport` and their role follows the consensus leader (fix-on-sight, the E2 carry-over's root cause). This task builds `__catalog` (topic id −6, partition 0) on every broker, replicated for real, and places it on the coordinator-partition controller path so its leader is always elected from its ISR. The AdminWorker still drives the old `__topic_schemas`/`__views` replicas until Task 9; this task only adds the new partition next to them.

**Files:**
- Create: `libs/node/include/rune/node/CatalogPartition.h`
- Create: `libs/node/include/rune/node/AdminIsrChannel.h`
- Modify: `libs/node/include/rune/node/NodeConfig.h` / `src/NodeConfig.cpp` (`catalogReplStreamId`, env `RUNE_CATALOG_REPL_STREAM_ID`, default 30; validation: distinct from `replicationStreamId` and outside `[replBaseStreamId, replBaseStreamId + workerCount)`)
- Modify: `libs/node/include/rune/node/MetadataService.h` (`isCoordinatorTopic` → also −6, renamed `isControllerIsrInternalTopic`; every call site)
- Modify: `libs/node/include/rune/node/AdminChannels.h` (`AdminRoleMsg` carries `isr`, `partitionEpoch`, `leaderEpoch`; `kA2cCatalogAlterIsr = 10`)
- Modify: `libs/node/src/ReplicationMetadataBridge.cpp`, `libs/node/include/rune/node/ReplicationMetadataBridge.h` (route −6 role changes onto the admin role ring instead of a direct call; F26: `isCoordinatorTopic_(t)` becomes `t == -1 || t == -2 || t == kCatalogTopicId`, so `mapDriven_` treats −6 as controller-driven when `coordinatorOnController_` — otherwise the bridge never forwards the −6 assignment)
- Modify: `libs/node/src/BrokerNode.cpp` (`buildCatalog_()`, called from `buildAdminWorker()` before the schema block; `setCoordinatorPartitions` includes `{-6, 0}`; the control drain forwards `kA2cCatalogAlterIsr`; single-node role from the consensus leader exactly as today's `AdminRoleMsg`)
- Modify: `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp` (own + drive the catalog replica: `doWork`, `onCatalogRole`)
- Modify: `libs/node/include/rune/node/BrokerNode.h` (F26: `buildCatalog_()` and the catalog members; test hooks `onAdminThreadForTest(f)`, `catalogLogForTest()`; the `AdminWorkerAgent` admin test ring the hook posts on)
- Modify: `libs/node/CMakeLists.txt` (new test executable `rune_catalog_replication_tests`)
- Test: `libs/node/tests/integration/CatalogReplicationTest.cpp`, `libs/node/tests/node/AdminIsrChannelTest.cpp` (in `rune_node_tests`), `libs/node/tests/node/ReplicationMetadataBridgePerPartitionTest.cpp` (append, F26)

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
            return *n->metadataServiceForTest()->assignment(rune::node::kCatalogTopicId, 0);   // F26: the real API
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

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_catalog_replication_tests rune_node_tests`
Expected: compile errors (`rune/node/CatalogPartition.h` not found).

- [ ] **Step 3: Implement** the files and edits listed above. `MetadataService::isControllerIsrInternalTopic(id)` returns `id == -1 || id == -2 || id == kCatalogTopicId`, and the bridge's `isCoordinatorTopic_` the same set (F26 — add `TEST_F(BridgePerPartitionTest, CatalogAssignmentIsForwardedOnTheControllerPathRegression)` to `libs/node/tests/node/ReplicationMetadataBridgePerPartitionTest.cpp` (listed in Files): with `coordinatorOnController_`, a committed `(−6, 0)` assignment reaches the admin role ring); `BrokerNode` (anchor `metadataService_->setCoordinatorPartitions(std::move(coord), balancerNodes);`) appends `rune::metadata::PartitionKey{kCatalogTopicId, 0}` to `coord`. `decideCoordinatorPartition` needs no change (replicas = `balancerNodes`, never unclean — re-read its doc comment, it already elects only from `ISR ∩ live`).

- [ ] **Step 4: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogReplication|AdminIsrChannel|MetadataService|Coordinator|ReplicationMetadataBridge|NodeConfig' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs/node CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-broker commit -m "feat(catalog): __catalog partition on every broker — real replication, controller-ISR leader, never unclean

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
- Create: `libs/node/tests/support/CatalogRig.h` (the applier fixture, below — shared by this task's two test files and Task 8)
- Modify: `libs/node/include/rune/node/AdminChannels.h` (effect messages, below)
- Modify: `libs/node/include/rune/node/SchemaRegistry.h` / `src/SchemaRegistry.cpp` (`erase(int32_t topicId)`), `ViewRegistry.h` / `src/ViewRegistry.cpp` (`erase(int32_t viewTopicId)`)
- Modify: `libs/node/include/rune/node/RoleCounter.h` (`kCatalogApplyHaltedCounterTypeId = 1'000'217`, `kCatalogAppliedOffsetCounterTypeId = 1'000'218`), `libs/node/src/BrokerNode.cpp` (register both counters next to `outOfSyncCounterRegId_`; set them from `kA2cCatalogApplied` / `kA2cCatalogHalted`)
- Modify: `libs/node/CMakeLists.txt` (`CatalogApplier.cpp`, `CatalogCheckpoint.cpp` in `rune_node`; link `rune_catalog`; root `CMakeLists.txt` defines `rune_catalog` from `${RUNE_CODEC_ROOT}/src/catalog/*.cpp` next to `rune_codec`, with `${RUNE_PROTOCOL_INCLUDE}` on its include path; F12: new executable `rune_catalog_alloc_regression_tests` — it replaces the global `operator new`, which is binary-wide, so it is its own binary like `rune_ddl_stress_tests` / `rune_transient_alloc_regression_tests`)
- Test: `libs/node/tests/node/CatalogApplierTest.cpp` (in `rune_node_tests`), `libs/node/tests/node/CatalogAllocRegressionTest.cpp` (in `rune_catalog_alloc_regression_tests`; Tasks 12 and 23 append to it)

**Interfaces:**
- Consumes: `rune::catalog::{CatalogState, CatalogReplayer, apply, decode, Applied}`; the catalog replica (Task 6).
- Produces:
  ```cpp
  // CatalogCheckpoint.h
  struct CatalogCheckpoint { int64_t appliedOffset = -1; uint64_t digest = 0; };
  [[nodiscard]] std::optional<CatalogCheckpoint> readCatalogCheckpoint(const std::string& catalogDir);  // nullopt = none
  [[nodiscard]] bool writeCatalogCheckpoint(const std::string& catalogDir, const CatalogCheckpoint&);    // tmp+fsync+rename+dir fsync
  // CatalogApplier.h
  /// A view's sink topic (F27): a view sink has no TopicObject — the applier builds this from the ViewDef.
  struct SinkSpec { int32_t topicId; std::string name; int32_t partitions; rune::catalog::Persistence persistence; };
  struct CatalogEffects {     // admin-thread effect sink; production = ring encoders (AdminWorkerAgent)
      std::function<void(const rune::catalog::TopicObject&)>               topicApplied;     // CreateTopic only
      std::function<void(const SinkSpec&)>                                 sinkApplied;      // sink of Create/AlterView (F27)
      std::function<void(int32_t topicId, const std::string& name)>        topicDropped;     // DropTopic, retired sink
      std::function<void(int32_t topicId, std::span<const rune::catalog::ConfigChange>)> topicConfig;
      std::function<void(const rune::catalog::TopicObject&)>               schemaChanged;    // validators fan-out
      std::function<void(const rune::catalog::ViewObject&)>                viewInstalled;    // recovery-ness = the phase (F4)
      std::function<void(int32_t viewTopicId)>                             viewUninstalled;
      std::function<void(int64_t offset, uint64_t digest)>                 applied;          // metric + tests
      std::function<void(const rune::catalog::ApplyError&)>               halted;
  };
  /// Boot vs live effects are selected by the applier's PHASE, never by a flag (hard rule 3, F4).
  struct BootEffects { CatalogEffects sink; };   // replayLocal: every view install is a RECOVERY install
  struct LiveEffects { CatalogEffects sink; };   // applyCommitted: every view install is a commit install
  class CatalogApplier {
  public:
      struct Applying { int64_t nextOffset = 0; };
      struct Halted   { rune::catalog::ApplyError error; };
      struct AwaitingBoot {};                          // constructed; the one boot replay has not run
      struct Booted { BootEffects effects; };          // replayLocal ran (exactly once); not yet live
      struct Live   { LiveEffects effects; };          // applyCommitted runs here only
      struct AppliedAt { int64_t offset = -1; rune::catalog::Applied applied{}; };   // F28
      CatalogApplier(std::string catalogDir, SchemaRegistry&, ViewRegistry&);
      /// Boot: replay [0, checkpoint] from the local log through `boot` — ONLY from AwaitingBoot
      /// (→ Booted).  Any other phase: logs an error, applies nothing, returns -1 (exactly one boot replay).
      int replayLocal(rune::replication::PartitionReplica& r, BootEffects boot);
      /// AwaitingBoot | Booted → Live.  The broker calls it once, on the admin thread.
      void goLive(LiveEffects live);
      /// Steady: apply every committed record in [nextOffset, hw) — Live only (any other phase: 0, nothing applied).
      int applyCommitted(rune::replication::PartitionReplica& r);
      [[nodiscard]] const rune::catalog::CatalogState& state() const noexcept;
      [[nodiscard]] const AppliedAt& lastApplied() const noexcept;   // F28: what the last applied event did, and where
      [[nodiscard]] bool halted() const noexcept;
      [[nodiscard]] int64_t nextOffset() const noexcept;     // -1 when halted
      [[nodiscard]] std::string_view stateName() const noexcept;   // "Applying" | "Halted"
      [[nodiscard]] std::string_view phaseName() const noexcept;   // "AwaitingBoot" | "Booted" | "Live"
      void tamperNextFingerprintForTest() noexcept;          // matrix FingerprintMismatch seam (test-only)
  private:
      std::variant<Applying, Halted>             st_;
      std::variant<AwaitingBoot, Booted, Live>   phase_;
      AppliedAt                                  lastApplied_;
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
6. effects by kind, into the current phase's sink (`Live` in steady state): `CreateTopic` → `topicApplied`; `DropTopic` → `topicDropped`; `AlterTopicConfig` → `topicConfig`; `CreateSchema`/`AlterSchema`/`SetCompatibility` → `schemaChanged`; `CreateView` → `sinkApplied(sink)` then `viewInstalled(view)`; `AlterView` → `sinkApplied(new sink)`, `viewInstalled(new)`, `viewUninstalled(old)`, `topicDropped(old sink)`; `DropView` → `viewUninstalled`, `topicDropped(sink)` (the `SinkSpec` is `{viewTopicId, view name, sinkPartitions, persistence}` — F27);
7. `lastApplied_ = {offset, applied}` (F28); `effects_.applied(offset, digest)`.

`replayLocal` replays `[0, checkpoint.appliedOffset]` (no checkpoint → nothing) through the same steps 1–3 and 5–7 into the `BootEffects` sink (no checkpoint rewrite; every view install there is a recovery install by construction, so workers rebuild from their checkpoints as `recoverViewsOnPromotion_` does today); a digest that differs from the checkpoint's → `Halted` (local log damage). It runs only from `AwaitingBoot`; a second call is refused (−1, error log, nothing applied) — the boot prefix is applied exactly once (a second replay would re-apply recorded request ids and halt on `Precondition`, F4).

- [ ] **Step 1: Write the failing tests** — `CatalogApplierTest.cpp` uses a real `Log` + a follower `PartitionReplica` fed by `appendAsFollowerForTest` (or a leader replica with ISR {self} and `onLeaderLogAppended()`, so HW = LEO), events from `rune::catalog::testing::goldenScriptV1()`, and a recording `CatalogEffects`:

```cpp
TEST(CatalogApplierRegression, AppliesCommittedEventsInOffsetOrderOnly) {
    CatalogRig rig("ap_order");                       // log + leader replica (ISR {self}) + registries + recorder
    rig.appendEvents(0, 5);                            // golden events 0..4, committed
    rig.appendUncommitted(5);                          // event 5 appended, HW held at 5 (ISR {self, 2}, 2 silent)
    EXPECT_GT(rig.applier.applyCommitted(rig.replica()), 0);
    EXPECT_EQ(rig.applier.nextOffset(), 5) << "never past the HW";
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2, 3, 4}));
    EXPECT_EQ(rig.applier.lastApplied().offset, 4) << "F28: the leader answers a requestId-less DDL from it";
    EXPECT_EQ(rig.applier.lastApplied().applied.kind, rune::catalog::EventKind::AlterSchema);   // golden offset 4
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
    rig.restart();                                      // same dir: log recovered, fresh applier (AwaitingBoot) + registries
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 10);
    EXPECT_EQ(rig.applier.phaseName(), "Booted");
    EXPECT_EQ(rig.applier.state().digest(), digest);
    EXPECT_FALSE(rig.rec.bootViewInstalls.empty()) << "golden views reinstalled through the BOOT sink (recovery installs)";
    EXPECT_TRUE(rig.rec.liveViewInstalls.empty()) << "the phase, not a flag, selects the sink (F4)";
}

TEST(CatalogApplierRegression, ExactlyOneBootReplayRegression) {
    // F4: a second replay of the same prefix would re-apply recorded request ids (Precondition → halt).
    CatalogRig rig("ap_once");
    rig.appendEvents(0, 6);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 6);
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), -1) << "refused, explicitly";
    EXPECT_FALSE(rig.applier.halted());
    rig.applier.goLive(rig.rec.liveEffects());
    EXPECT_EQ(rig.applier.phaseName(), "Live");
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), -1);
    EXPECT_EQ(rig.applier.nextOffset(), 6);
}

TEST(CatalogApplierRegression, BootDigestDisagreeingWithTheCheckpointHalts) {
    CatalogRig rig("ap_dmg");
    rig.appendEvents(0, 4);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {3, 0xBAD}));
    rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects());
    EXPECT_TRUE(rig.applier.halted());
}

TEST(CatalogApplierRegression, AlterViewEffectsRunInstallBeforeUninstallAndRetireTheOldSink) {
    CatalogRig rig("ap_view");
    rig.appendEvents(0, 11);                            // through golden AlterView top_players (offset 10)
    rig.applier.applyCommitted(rig.replica());
    const auto& seq = rig.rec.sequence;                 // e.g. "sinkApplied:3", "viewInstalled:3", …
    // Golden script: CreateView top_players (offset 9) took sink topic id 3; AlterView (offset 10) takes sink 4 (F2).
    auto at = [&](const std::string& s) {
        const auto i = static_cast<std::size_t>(std::find(seq.begin(), seq.end(), s) - seq.begin());
        EXPECT_LT(i, seq.size()) << "effect '" << s << "' never ran";
        return i;
    };
    EXPECT_LT(at("sinkApplied:4"), at("viewInstalled:4")) << "view sinks go through sinkApplied (F27)";
    EXPECT_LT(at("viewInstalled:4"), at("viewUninstalled:3"));
    EXPECT_LT(at("viewUninstalled:3"), at("topicDropped:3"));
}
```

`CatalogRig` (`libs/node/tests/support/CatalogRig.h`, namespace `rune::node::test`, created in this task: the alloc binary below needs it too) is the fixture: log under a `TmpDir`, the replica, `SchemaRegistry`, `ViewRegistry`, an applier already moved to `Live` with the recorder's `liveEffects()`, a `Recorder` whose `bootEffects()` / `liveEffects()` build `CatalogEffects` lambdas that push into vectors — `bootViewInstalls`, `liveViewInstalls`, … — and `sequence` strings `"<effect>:<topicId>"`; `restart()` rebuilds the applier in `AwaitingBoot`. Task 8's `CatalogEffectsTest.cpp` reuses it.

`CatalogAllocRegressionTest.cpp` (own binary `rune_catalog_alloc_regression_tests`, F12) — the repo's real allocation gate: a file-scope replacement of the global `operator new` / `new[]` / `delete` with **plain single-thread globals**, exactly the `DdlStressRegressionTest.cpp` / `TransientDataPathAllocTest.cpp` pattern (no `AllocCounter` type exists in `libs/node/tests`; none is invented):

```cpp
// CatalogAllocRegressionTest.cpp — catalog history: the admin thread's catalog steady
// state never allocates.  Replaces the global operator new (binary-wide), so it lives in
// its own binary (rune_catalog_alloc_regression_tests).  Tasks 12 and 23 append here.
#include "support/CatalogRig.h"
#include <gtest/gtest.h>
#include <cstdlib>
#include <new>

namespace {
bool     gArmed       = false;   // single-threaded test binary: plain globals, no atomics
uint64_t gArmedAllocs = 0;
}
void* operator new(std::size_t n) {
    if (gArmed) ++gArmedAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (gArmed) ++gArmedAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

using rune::node::test::CatalogRig;

TEST(CatalogAllocRegression, SteadyIdleApplyAllocatesNothingRegression) {
    CatalogRig rig("ap_alloc");
    rig.appendEvents(0, 18);
    rig.applier.applyCommitted(rig.replica());
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) (void)rig.applier.applyCommitted(rig.replica());
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}
```

(`CatalogRig` lives in `support/CatalogRig.h` from this task on, so no later task moves it.)

- [ ] **Step 2: Run to verify they fail**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_node_tests`
Expected: compile error (`rune/node/CatalogApplier.h` not found).

- [ ] **Step 3: Implement** as specified. `CatalogApplier` holds the state machine `std::variant<Applying, Halted>` and the effects phase `std::variant<AwaitingBoot, Booted, Live>` (F4); `nextOffset()` returns `std::get<Applying>(st_).nextOffset` or −1. The production `LiveEffects` (built in `AdminWorkerAgent`) encodes the `kA2c*` messages; `topicApplied` and `sinkApplied` both encode `kA2cCatalogTopicApplied` (a sink: default `TopicConfig`, rf 0); `schemaChanged` → `kA2cSchemaUpdate` (existing) when `validate`; `viewInstalled` → `kA2cViewPlanUpdate` (existing) with its recovery byte **0** — the production `BootEffects` sets it to **1** (until Task 16 replaces the boot sink): the byte is chosen by which phase's sink runs, never passed as a flag; a full ring is the existing backlog path (`schemaBacklog_` / `topicCommittedBacklog_` pattern: preallocated, flushed next pass, never dropped).

- [ ] **Step 4: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_node_tests rune_catalog_alloc_regression_tests && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogApplier|CatalogAlloc|SchemaRegistry|ViewRegistry' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs/node CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-broker commit -m "feat(catalog): CatalogApplier — offset-ordered verified apply, halt on mismatch, checkpoint before effects

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
- Modify: `libs/node/include/rune/node/BrokerNode.h` (test hooks `postCatalogEffectForTest`, `catalogTopicServesForTest`, `holdDroppedDirPurgeForTest` — F23)
- Test: `libs/node/tests/node/CatalogEffectsTest.cpp`, `libs/gateway/tests/gateway/CatalogGatewayTest.cpp`, `libs/metadata/tests/MetadataStateMachineTest.cpp` (append), `libs/node/tests/node/LogCleanerPolicyOverrideTest.cpp`, `libs/node/tests/node/WorkerViewSetTest.cpp` (append)

**Interfaces:**
- Consumes: Task 7's messages.
- Produces: `TopicRegistry::{createTopicAt, dropTopic, releaseDroppedLogs, state}`, `MetadataService::removeRuntimeTopic`, `WorkerViewSet::uninstallView`, `BrokerNode::catalogTopicServesForTest(topicId)`, and (F23, `BrokerNode.h`) `holdDroppedDirPurgeForTest(bool)` — an admin-thread-owned hold, set through `onAdminThreadForTest`, under which `kMsgPurgeDroppedDir` paths queue (preallocated) instead of being removed; releasing it purges them on the next admin pass.

The drop sequence on the control thread (one explicit state per step):
1. `kA2cCatalogTopicDropped{id, name}` → `TopicRegistry::dropTopic(id)` (serves nothing from now on: gateway answers `UnknownTopic`), `metadataService_->removeRuntimeTopic(id)`, `replicaRegistry_->remove(id, p)` for every partition, transient ring freed with the replica; record `DroppedTopic{id, name, retireGen = replicaRegistry_->generation()}` in `droppedAwaitingRetire_`.
2. Each pass: when every worker and the cleaner/provisioner acked a generation ≥ `retireGen` (the same condition `collectRetired` uses), `TopicRegistry::releaseDroppedLogs(id)` closes the logs, then `rename(topics/<name>, dropped/<id>.<name>)` (atomic; a failed rename is logged at error and retried next pass — never deleted in place) and post `kMsgPurgeDroppedDir{path}` to the admin thread, which `std::filesystem::remove_all`s it (cold; a failure is logged and the dir stays for the boot purge — `reconcileTopicDirs`, Task 16).
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
    n.node()->holdDroppedDirPurgeForTest(true);        // keep the renamed dir observable (F23)
    n.applyDropTopic(0, "t");
    n.applyCreateTopic(1, "t", 1);                     // same name, immediately
    ASSERT_TRUE(n.awaitServes(1, 10s));
    EXPECT_TRUE(std::filesystem::exists(n.dataDir() / "dropped" / "0.t"))
        << "the old data was renamed away before the reused name was created";
    EXPECT_EQ(n.logEndOffset(1, 0), 0) << "the reused name starts EMPTY";
    EXPECT_TRUE(n.consumeAllFromEarliest(1, 0).empty()) << "the old record is unreadable under the reused name";
    n.node()->holdDroppedDirPurgeForTest(false);
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

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_node_tests rune_gateway_tests rune_metadata_tests`
Expected: compile errors (`createTopicAt` / `uninstallView` / `removeRuntimeTopic` missing).

- [ ] **Step 3: Implement** the edits above. In `BrokerNode`'s admin→control drain (anchor `if (tid == kA2cTopicCommitted) {`), add the new `tid` branches before the dispatcher; `kA2cCatalogTopicApplied` builds a `TopicSpec` from the message and runs `topicRegistry_->createTopicAt(id, spec)` + `createRuntimeUserReplicas(spec)` (unchanged body, it already registers the topic with `MetadataService::addRuntimeTopic`) inside the existing `try`/`catch` (a failure logs the error and leaves the topic not serving — produce answers `NotLeader` — exactly the current `commitReplicaFault_` behaviour). The gateway's new rule is additive here (the old `awaitingCommit` check stays for the old write path until Task 9).

- [ ] **Step 4: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogEffects|CatalogGateway|MetadataStateMachine|LogCleanerPolicyOverride|WorkerViewSet|TopicServability|TransientGateway|TransientInteractionMatrix|TopicRegistry' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-broker commit -m "feat(catalog): control-thread effects — create/drop at catalog ids, placement add/remove, config override, view uninstall; C1 generalized

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
- Modify: `libs/node/src/BrokerNode.cpp` (`buildAdminWorker`: the `AdminDeps` lose `requestCreateTopic`/`topicCommitted`, gain `respondDdl`, `respondError`, `brokerCount`, `topicMinIsr`; the create-request drain (anchor `// (2) Drain create-topic requests (admin → control).`) is deleted; the admin thread calls `applier.replayLocal(*catalogReplica_, BootEffects{…})` exactly once — its sink is the ring encoders with the view-install recovery byte set — then `applier.goLive(LiveEffects{…})`, before its first `doWork`. **Task 16 deletes this call** and moves the one boot replay to the startup thread (F4))
- Modify: `libs/gateway/include/rune/gateway/TopicRegistry.h`, `libs/gateway/src/TopicRegistry.cpp` (delete the runtime `createTopic`, `commitTopic`, `awaitingCommit`, `markTopicCommitted`, the `committed` flag and the topic-id marker helpers — their last caller is gone)
- Port (they exercised the deleted round-trip): `libs/node/tests/node/AdminWorkerTest.cpp`, `libs/node/tests/node/DdlFinalFixRegressionTest.cpp`, `libs/node/tests/integration/SchemaGatewayTest.cpp`, `CreateViewRegressionTest.cpp`, `GetViewRowRegressionTest.cpp`, `SchemaPersistenceTest.cpp` (now "a schema survives restart through `__catalog`"), `libs/gateway/tests/gateway/TopicServabilityTest.cpp`, `TransientGatewayTest.cpp`, `TransientInteractionMatrixTest.cpp` (C1 precondition "uncommitted" → "applied but not placed", same assertions; `TransientMatrixExpectations.h` unchanged)
- Create: `libs/node/tests/support/CatalogDdlRig.h` (one AdminWorker + catalog leader replica with ISR {self} + applier + recording deps; reused by Tasks 10, 11, 20)
- Test: `libs/node/tests/node/CatalogWritePathTest.cpp` (in `rune_node_tests`)

**Interfaces:**
- Consumes: `rune::catalog::{CatalogState, previewFingerprint, encodeInto, RequestRecord}`, `CatalogApplier` (Task 7), the catalog replica (Task 6), `rune::view::{deserializeAuthored, serializeAuthored, compileView, serialize}` (broker `libs/view`).
- Produces:
  ```cpp
  // AdminChannels.h
  struct DdlMeta { std::string statement, clientName, sourceAddress; int64_t sessionId = 0;
                   rune::protocol::RequestId requestId{};           // 128-bit; {0,0} = a pre-v8 client (F14)
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
  /// F14: FNV-1a-64 over the request's template id, object name, statement text and every operation
  /// field (descriptor / ops / plan / config / changes / rf / partitions / persistence / expectedVersion) —
  /// never the requestId, clientName, session or replayedFrom.  Deterministic on every broker.
  [[nodiscard]] uint64_t requestFingerprint(const DdlRequest&) noexcept;
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
| `AlterTopic` | as today's `startAlter_`: view name → `ViewNotAlterable 38`; unknown → `UnknownTopic 7`; untyped → `UnknownTopic 7` (today's answer — an untyped topic has no schema in the registry, `AdminWorker.cpp` anchor `const RegistryEntry* latest = reg_.byName(msg.topicName);` — and `DdlExpectations` pins SchemaLess × Alter* = 7, binding for Task 10, F3); stale `expectedVersion` (schema version) → `SchemaConflict 31`; bad ops → `InvalidSchema 23`; a name `resolveAlterOps` cannot resolve → `InvalidSchema 23`; compat gate → `SchemaIncompatible 30`; `applyOps` error → `InvalidSchema 23` | all ops `SET COMPATIBILITY` → `SetCompatibility`; else `AlterSchema{resolveAlterOps(schema, decodeAlterOps(opsBlob))}` — stored resolved to durable tags (F20) |
| `CreateView` | as today's `startCreateView_` (codes 23, 40, 32, 25, 34, 41, 33, 37, 35/34, 36) | `CreateView{viewTopicId = nextTopicId, sinkPartitions = source's, sourceNames, normalizedPlanBlob, compiledPlanBlob (viewTopicId stamped), outputDescriptorBlob}` |
| `AlterView` | unknown view → `UnknownTopic 7` (detail "no view named …"); stale `expectedVersion` (object version) → `SchemaConflict 31`; plan errors as `CreateView` | `AlterView` with a fresh `viewTopicId = nextTopicId` |
| `DropView` | unknown → `UnknownTopic 7` (detail) | `DropView{viewTopicId}` |
| `DropTopic` | unknown → `UnknownTopic 7`; a view name → `ErrorResponse{39, "'v' is a view; use DROP VIEW"}`; read by view `v` → `ErrorResponse{39, "topic 't' is the source of view 'v'"}` | `DropTopic{topicId}` |
| `AlterTopicConfig` | unknown → `UnknownTopic 7`; a view → `ViewNotAlterable 38`; stale `expectedVersion` (object version, 0 = unconditional) → `SchemaConflict 31`; empty / duplicate keys / out-of-range values → `ErrorResponse{39, detail}` | `AlterTopicConfig{changes}` |

`normalizedPlanBlob` = `rune::view::serializeAuthored` of the authored plan after replacing each `sources[i].topicId` with `i` (ruling 11). The event's `statement`, `principal{sessionId, clientName, sourceAddress, requestId}`, `replayedFrom` come from `DdlMeta`; `principal.requestFingerprint = requestFingerprint(req)` (F14); `timestampMs = system_clock now` **when the event is built for the append** — the commit time is not known yet and the event is immutable (spec §4 as amended, F22); then `fingerprint = previewFingerprint(state, event)` (a failure here is an internal bug → answered `Internal 6`, logged at error, nothing appended).

**Steps of one DDL on the leader** (`startNext_`, `advance_`):
1. Role gate: `CatalogFollower` / `CatalogLeaderCatchingUp` → answer `NotLeader 2` (retriable; Task 11 makes the gateway redirect before the request reaches here); applier `Halted` → `Internal 6`.
2. Dedupe (F14) on `(requestId, requestFingerprint(req))`: `!meta.requestId.none() && state.request(requestId)` → when the record's `requestFingerprint` equals this request's, answer that record's result (same response type, `errorCode 0`); when it differs, answer `ErrorResponse{InvalidRequest 39, "requestId reused for a different statement"}` and append nothing — never another DDL's success. A request whose `requestId` equals the in-flight event's waits in `pending_` and is answered after it by the same rule.
3. `buildEvent_` → reject, or encode into the reused `eventBuf_` and `catalogReplica_->appendKeyed(key = objectId LE 8 bytes, eventBuf_, acksAll = true)`; `< 0` (minIsr not met) → `NotLeader 2` (spec §5.5), nothing in flight.
4. `AwaitingCommit{req, event, offset}`; `catalogReplica_->onLeaderLogAppended()`.
5. Each `doWork`: plane poll → `catalogReplica_->doWork` → `applier_.applyCommitted(*catalogReplica_)` → `advance_`: `applier_.nextOffset() > offset` → answer OK from `state.request(requestId)` (or, for a `none()` requestId, from `applier_.lastApplied()` — F28 — whose `offset` must equal the in-flight `offset`: the single AdminWorker has one event in flight and the catch-up gate applied everything before it; a mismatch is a bug → `Internal 6`, logged at error): `DefineTopicResponse{topicId, schemaVersion}` / `AlterTopicResponse{topicId, schemaVersion}` / `CreateViewResponse{viewTopicId, outputFingerprint}` / `DdlResponse{objectId, newVersion, gateTopicId}` (`AlterView`: the new sink; drops / config: −1); applier halted at or before `offset` → `Internal 6`; `now − startedNs > kDdlTimeoutNs` → the retriable **`NotLeader 2`** (F13, spec §9: the appended event may still commit — `Internal` would tell the client not to retry; the retry with the same `requestId` is deduplicated to the original result if it committed, and re-executed if it did not), nothing more appended, `AwaitingCommit` → `Idle`. `Internal 6` is answered only by a halted applier (and the internal-bug cases above).
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
                               .clientName = "cli", .requestId = {.hi = 0xC11E, .lo = 11}});
    EXPECT_EQ(a.code, 0); EXPECT_EQ(a.topicId, 0); EXPECT_EQ(a.version, 1);
    const auto log = rig.recordedLog();
    ASSERT_EQ(log.size(), 1u);
    EXPECT_EQ(log[0].event.kind, rc::EventKind::CreateTopic);
    EXPECT_EQ(log[0].event.statement, "topic players {...} partitions 3 replicas 3;");
    EXPECT_EQ(log[0].event.principal.clientName, "cli");
    EXPECT_EQ(log[0].event.principal.requestId, (rune::protocol::RequestId{0xC11E, 11}));
    EXPECT_NE(log[0].event.principal.requestFingerprint, 0u) << "F14: the leader's statement fingerprint";
    EXPECT_GT(log[0].event.timestampMs, 0) << "stamped at append (F22)";
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
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 99};
    const auto first  = rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    const auto second = rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    EXPECT_EQ(second.code, 0) << "a retry of a committed DDL is its original success, not TopicExists";
    EXPECT_EQ(second.topicId, first.topicId);
    EXPECT_EQ(second.version, first.version);
    EXPECT_EQ(rig.recordedLog().size(), 1u);
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = {.hi = 0xC11E, .lo = 100}}).code, 25);
}

TEST(CatalogWritePathRegression, RequestIdReusedForADifferentStatementIsInvalidRequestRegression) {
    // F14: two rune ql processes that drew the same id must never get each other's success.
    CatalogDdlRig rig("wp_reuse", 3);
    rig.becomeServingLeader();
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 7};
    ASSERT_EQ(rig.define({.name = "a", .partitions = 1, .requestId = rid}).code, 0);
    const auto other = rig.define({.name = "b", .partitions = 2, .requestId = rid});
    EXPECT_EQ(other.code, 39);
    EXPECT_EQ(other.detail, "requestId reused for a different statement");
    EXPECT_EQ(rig.recordedLog().size(), 1u) << "nothing appended for the reused id";
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
}

TEST(CatalogWritePathRegression, CommitTimeoutAnswersRetriableNotLeaderAndTheRetryIsExactRegression) {
    // F13: past kDdlTimeoutNs the appended event may still commit — the answer must be retriable,
    // and the retry (same requestId) must get the original result once it committed.
    CatalogDdlRig rig("wp_timeout", 3);
    rig.becomeServingLeader(/*isr=*/{1, 2});              // follower 2 silent: the event stays uncommitted
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 8};
    const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1, .requestId = rid});
    rig.advanceClockPast(rune::node::AdminWorker::ddlTimeoutNs());
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->code, 2) << "NotLeader (retriable), never Internal, on a healthy broker";
    rig.resumeFollower(2);                                 // the event commits after all
    const auto retry = rig.define({.name = "t", .partitions = 1, .requestId = rid});
    EXPECT_EQ(retry.code, 0) << "the retry is the original success, not TopicExists";
    EXPECT_EQ(rig.countEventsWithRequestId(rid), 1u);
}

TEST(CatalogWritePathRegression, AlterOfAnUntypedTopicIsUnknownTopicAsToday) {
    // F3: today's startAlter_ answers UnknownTopic (7) for a topic with no schema; DdlExpectations pins it.
    CatalogDdlRig rig("wp_untyped_alter", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "u", .partitions = 1, .typed = false}).code, 0);
    EXPECT_EQ(rig.alterAdd("u", "mana").code, 7);
    EXPECT_EQ(rig.recordedLog().size(), 1u);
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

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_node_tests`
Expected: compile error (`support/CatalogDdlRig.h` / `AdminWorker::submit` missing).

(`CatalogDdlRig` provides, besides the calls above: `advanceClockPast(ns)` (its deterministic clock), `resumeFollower(id)` (the silent follower fetches again), `countEventsWithRequestId(RequestId)`; the define/alter helpers take `requestId` as `rune::protocol::RequestId`.)

- [ ] **Step 3: Implement** the AdminWorker rewrite, the gateway handlers (each mirrors `handleDefineTopic`: decode → `rejectRequest_(InvalidRequest)` on an undecodable body → known session → `adminSink_->submitX(...)` with the `DdlMetaView{statement, clientName, requestId, replayedFrom…, sessionId, sourceAddress = session.egressChannel}`; ring full → `Internal 6`, as today), the `AdminDeps` wiring in `BrokerNode`, the deletions. `startedCount()` / `answeredCount()` are incremented at exactly one place each (`startNext_` dequeue; the single `answer_(…)` helper every exit uses).

- [ ] **Step 4: Port the listed tests** to the catalog path (same scenarios and assertions where the behaviour is unchanged; a scenario that only existed for the deleted round-trip — e.g. "control thread reports `existed`" — becomes the catalog-state equivalent, named the same with `Catalog` prefixed). Their orphan cases go in Task 10 and their `RUNE_TOPICS` set-up in Tasks 16–18 (their own tasks).

- [ ] **Step 5: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogWritePath|CatalogApplier|AdminWorker|DdlFinalFix|SchemaGateway|CreateView|GetViewRow|SchemaPersistence|TopicServability|TransientGateway|TransientInteractionMatrix|DdlAnswerGate' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-broker commit -m "feat(catalog): every DDL is a __catalog event written by the catalog leader; new Drop/AlterView/AlterTopicConfig ops; rf bounds; request-id dedupe

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
  - Existing codes are unchanged — in particular `SchemaLessDisk` / `SchemaLessTransient` × every `Alter*` op stays `7` (Task 9 answers an alter of an untyped topic `UnknownTopic`, F3).
  - "†: detected by the control thread" rows are gone — every rejection is decided on the admin thread from the catalog state; `requestCreateTopic` call counts become "events appended" (0 for every rejection, 1 for an accepted op).
- Modify: `libs/node/tests/node/DdlHarnessRegressionTest.cpp`, `DdlOccupantMatrixRegressionTest.cpp`, `DdlPairTimingMatrixRegressionTest.cpp`, `DdlStressRegressionTest.cpp`, `libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp` (pins updated: `kOccupantCount` 10 → 9 (the `OrphanSink` row removed — F36: the literal is the occupant count of `DdlExpectations.h`, not a matrix-row count), `kOpCount` 12 → 16; cell count pin recomputed in the same test from the table, never hand-typed)

**Interfaces:**
- Consumes: `CatalogDdlRig` (Task 9).
- Produces: `rune::node::test::DdlHarness` with the same public API it has today minus `statics` (options) and `orphan…` helpers; `Op::{DropTopic, DropView, AlterView, AlterTopicConfig}`.

- [ ] **Step 1: Port the harness and table** as described; every former `OrphanSink` cell and every placeholder / marker / quarantine assertion is deleted with a one-line comment in the table file: `// OrphanSink removed (catalog history): a create whose event never committed leaves no topic, dir or id.`

- [ ] **Step 2: Add the new-op cells** — for each occupant, the expected code of `DropTopic` / `DropView` / `AlterView` / `AlterTopicConfig` exactly per the Task 9 table (e.g. `DropTopic` on `ViewPersisted` → 39, on `Free` → 7, on `TypedDisk` with no view → 0; `AlterTopicConfig` on a view → 38).

- [ ] **Step 3: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'Ddl' --output-on-failure -j14`
Expected: PASS — every occupant × op cell answers its pinned code; the stress (`DdlStressRegression`) still finds no divergence from its model.

- [ ] **Step 4: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs/node/tests
git -C ~/Projects/se-worktrees/rune-broker commit -m "test(catalog): port the DDL conformance suite to the catalog write path (+ drop/alter-view/config ops; orphan occupant retired)

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
- Modify: root `CMakeLists.txt` (F7: `set(RUNE_CLIENT_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-client-cpp" CACHE PATH …)`; under `BUILD_TESTING`, after the broker's own `rune_codec` / `rune_fr` / `rune_gso` / `rune_crypto` / `rune_catalog` targets: `if(NOT EXISTS "${RUNE_CLIENT_ROOT}/CMakeLists.txt") message(FATAL_ERROR "RUNE_CLIENT_ROOT … is required for rune_client_broker_tests")` — never a silent skip — then `add_subdirectory(${RUNE_CLIENT_ROOT} ${CMAKE_BINARY_DIR}/rune-client EXCLUDE_FROM_ALL)`; the client's `if(NOT TARGET …)` guards (Task 5) reuse the broker's copies of the shared sibling targets)
- Modify: `libs/node/CMakeLists.txt` (F7: new executable `rune_client_broker_tests` — sources under `libs/node/tests/client/`, linking `rune_node`, the client's `rune_client_api`, `GTest::gtest_main`; Task 15 adds `rune_cli_lib`)
- Test: `libs/node/tests/node/CatalogLeadershipTest.cpp` (on `CatalogDdlRig` extended to 3 rigs over one `FilteringTransport` — Task 20 promotes this to `CatalogClusterHarness`), `libs/gateway/tests/gateway/CatalogGatewayTest.cpp` (append), `libs/node/tests/client/OneBroker.h` + `libs/node/tests/client/ClientDdlBrokerRegressionTest.cpp` (F7: the real client's blocking `*AndWait` DDL against a real single-node `BrokerNode`)

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
- **Pre-v8 sessions (F37):** a session with `!clientUnderstandsCatalogDdl(session.clientVersion)` (Task 4) cannot follow a DDL `Redirect`, and a v7 client leaves a DDL `ErrorResponse` pending until its deadline. Its `DefineTopic` / `AlterTopic` / `CreateView` (the only DDL templates below v8) on a broker whose route is not `RouteLocalLeader` is therefore answered **in the request's own response type** (`DefineTopicResponse` / `AlterTopicResponse` / `CreateViewResponse`) with `errorCode = InvalidRequest (39)`, and the reason `"client too old for cluster DDL routing; upgrade"` is logged at warn with the session id (those frames carry no message) — never a Redirect, never a wait-out. `RouteHalted` → own type with `Internal 6`; `RouteUnknown` → own type with `NotLeader 2`. On `RouteLocalLeader` a pre-v8 DDL is served as today (`requestId` none).

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
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 5};
    t.holdHighWatermark(1);                            // 1 appends, the HW does not move yet
    const int64_t corr = t.submitDefineNoWait(1, {.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    t.replicateTo({2, 3});                             // the event reaches 2 and 3 …
    t.moveCatalogLeadership(2);                        // … then 1 loses leadership before answering
    EXPECT_EQ(t.answerFor(1, corr)->code, 2);
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    const auto retry = t.define(2, {.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    EXPECT_EQ(retry.code, 0);
    EXPECT_EQ(t.countEventsWithRequestId(rid), 1u) << "no DDL lost or duplicated";
    EXPECT_TRUE(t.allDigestsEqual());
}

TEST(CatalogLeadershipRegression, BelowMinIsrEveryDdlIsNotLeaderAndProduceIsUnaffected) {
    CatalogTrio t("ld_minisr");
    ASSERT_EQ(t.define(1, {.name = "data", .partitions = 1}).code, 0);
    t.kill(2); t.kill(3);
    EXPECT_EQ(t.define(1, {.name = "more", .partitions = 1}).code, 2);
    EXPECT_EQ(t.admin(1).startedCount(), t.admin(1).answeredCount());
    // F24: produce and consume on the existing topic really happen, below the catalog's minIsr.
    const auto off = t.produce(1, "data", {0xAB, 0xCD});              // acks=leader on the survivor
    ASSERT_TRUE(off.has_value()) << "produce on an existing topic is unaffected (spec §5.5)";
    EXPECT_EQ(t.consumeFrom(1, "data", *off), (std::vector<uint8_t>{0xAB, 0xCD}));
}
```

(`CatalogTrio` places each applied topic's partitions as real `PartitionReplica`s on the same `FilteringTransport` — leader = the broker the trio's placement map names, `minIsr 1`, acks=leader — so `produce(broker, topic, bytes) → std::optional<int64_t>` appends through that leader replica and `consumeFrom(broker, topic, offset)` reads the record back; F24.)

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
TEST(CatalogGatewayRegression, PreV8DdlToANonLeaderIsInvalidRequestNotARedirectRegression) {
    // F37: a v7 client cannot follow a DDL Redirect and waits out an ErrorResponse — answer it explicitly.
    GatewayRig g(/*clientVersion=*/7);
    g.gateway().setCatalogRoute(rune::gateway::RouteRemote{"10.0.0.2:40200"});
    for (auto tmpl : {rune::protocol::client::TemplateId::DefineTopic, rune::protocol::client::TemplateId::AlterTopic,
                      rune::protocol::client::TemplateId::CreateView}) {
        const auto r = g.sendDdl(tmpl);
        EXPECT_EQ(r.responseTemplate, g.ownResponseTemplate(tmpl)) << "its own response type, not ErrorResponse";
        EXPECT_EQ(r.code, rune::protocol::client::ErrorCode::InvalidRequest);
        EXPECT_EQ(g.adminSubmits(), 0u);
    }
    EXPECT_TRUE(g.warnLogContains("client too old for cluster DDL routing; upgrade"));
}
```

`ClientDdlBrokerRegressionTest.cpp` (new binary `rune_client_broker_tests`, F7) — the **real** client (`rune::client::Client`, Thread runner mode) against one **real** `BrokerNode` started in-process (`OneBroker`, created here in `libs/node/tests/client/OneBroker.h` and reused by Tasks 13 and 15: `TmpDir` + `Ports::discover` + `makeConfig` as `TransientTopicDataPathTest.cpp` does, `pinLegacyRuntime` from `support/TestNodeConfig.h`; `connectClient()` points the client's `gatewayChannel` at the node's client endpoint; `catalogEvents()` reads the node's `__catalog` log through `onAdminThreadForTest` and decodes it with `rune_catalog`; `endpoint()` = `host:port` for the CLI). The fake cannot serve these calls: a blocking `*AndWait` pumps its own thread.

```cpp
TEST(ClientDdlBrokerRegression, EveryDdlAndWaitRoundTripsThroughARealBroker) {
    OneBroker b("cb_all");
    auto client = b.connectClient();
    rune::client::DdlOptions o; o.statement = "topic u partitions 1;";
    ASSERT_EQ(client->defineTopicAndWait("u", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 10s).errorCode, 0);
    const rune::protocol::client::ConfigChangeWire ch{1, 60'000};
    EXPECT_EQ(client->alterTopicConfigAndWait("u", 0, std::span(&ch, 1), {}, 10s).errorCode, 0);
    EXPECT_EQ(client->defineTopicAndWait("u", 1, "id", b.idDescriptor(), false, rune::client::Persistence::Disk,
                                         {}, {}, 10s).errorCode, 0) << "CreateSchema on the untyped topic";
    EXPECT_EQ(client->alterTopicAndWait("u", 0, b.addColumnOps("mana"), {}, 10s).errorCode, 0);
    EXPECT_EQ(client->createViewAndWait("v", "id", 0, b.viewPlanOver("u"), {}, 10s).errorCode, 0);
    EXPECT_EQ(client->alterViewAndWait("v", "id", 0, /*expectedVersion=*/1, b.viewPlanOver("u", /*extra=*/true), {},
                                       10s).errorCode, 0);
    EXPECT_EQ(client->dropViewAndWait("v", {}, 10s).errorCode, 0);
    EXPECT_EQ(client->dropTopicAndWait("u", {}, 10s).errorCode, 0);
    const auto log = b.catalogEvents();
    ASSERT_EQ(log.size(), 8u);
    EXPECT_EQ(log[0].event.statement, "topic u partitions 1;");
    EXPECT_FALSE(log[0].event.principal.requestId.none());
}

TEST(ClientDdlBrokerRegression, InvalidRequestDetailFromTheBrokerReachesTheCaller) {
    OneBroker b("cb_rf");
    auto client = b.connectClient();
    rune::client::TopicOptions topic; topic.replicationFactor = 5;
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, topic, {}, 10s);
    EXPECT_EQ(r.errorCode, 39);
    EXPECT_EQ(r.detail, "replicationFactor 5 > broker count 1");
}

TEST(ClientDdlBrokerRegression, ARetriedRequestIdGetsTheOriginalResultFromTheBroker) {
    OneBroker b("cb_dedupe");
    auto client = b.connectClient();
    rune::client::DdlOptions o; o.requestId = {.hi = 0xC11E, .lo = 42};
    const auto first  = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 10s);
    const auto second = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 10s);
    EXPECT_EQ(first.errorCode, 0);
    EXPECT_EQ(second.errorCode, 0) << "not TopicExists";
    EXPECT_EQ(second.topicId, first.topicId);
    EXPECT_EQ(b.catalogEvents().size(), 1u);
}
```

(The multi-node blocking cases — a follower's `Redirect` followed by `*AndWait`, `NotLeader` until the budget below minIsr — need three nodes and land in Task 22, F7.)

- [ ] **Step 2: Run to verify they fail**, then **Step 3: implement** as specified. `BrokerNode` recomputes the route in `serviceAdminControl_` (control thread): halted (latched from `kA2cCatalogHalted`) → `RouteHalted`; else MSM `(−6,0)` leader == self → `RouteLocalLeader`; leader ≥ 0 → `RouteRemote{peer.clientChannel()}` (as `leaderClientEndpointFor_` builds it); else `RouteUnknown`. Single node → `RouteLocalLeader` once the consensus leader is self, else `RouteUnknown`.

- [ ] **Step 4: Run**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogLeadership|CatalogGateway|CatalogWritePath|SchemaGateway|ClientDdlBroker' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-broker commit -m "feat(catalog): DDL Redirect to the catalog leader, NotLeader on leader change / catch-up / below minIsr, Internal when halted; pre-v8 DDL on a non-leader answered InvalidRequest; real client vs real broker tests

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
- Create: `libs/node/tests/support/CatalogReadRig.h` (`rune::node::test::ReadRig`: a catalog replica — leader with ISR {self}, or follower — + the server + a recording egress; used by both test files below)
- Test: `libs/node/tests/node/CatalogReadServerTest.cpp` (`using rune::node::test::ReadRig;`), `libs/node/tests/node/CatalogAllocRegressionTest.cpp` (append, F12)
- Spec note: spec §7 as amended (F19) — serving the existing Subscribe for −6 is transport, not a read API; history / as-of / diff and replay stay client-side (Task 13).

**Interfaces:**
- Produces:
  ```cpp
  class CatalogReadServer {
  public:
      // One explicit state per subscription phase (hard rule 3, F29) — no caughtUpSent flag:
      struct Backlog { int64_t nextOffset; };   // pushing [nextOffset, HW); the marker is sent on leaving this state
      struct Live    { int64_t nextOffset; };   // marker sent; pushes new commits as they arrive
      struct Sub { int64_t session; int64_t subscriptionId; std::variant<Backlog, Live> phase; };
      CatalogReadServer(rune::replication::PartitionReplica& catalog, EgressFn send, std::size_t maxSubs = 64);
      void subscribe(int64_t session, int64_t corr, int64_t fromOffset);   // answers SubscribeResponse
      void unsubscribe(int64_t session, int64_t subscriptionId);           // -1 = every sub of the session
      int  pump();                                                         // push [nextOffset, HW) then the marker
      [[nodiscard]] std::size_t subscriptions() const noexcept;
  };
  inline constexpr int64_t kCatalogSubscriptionIdBase = int64_t{1} << 62;  // never collides with gateway ids
  ```
- Rules: `fromOffset` −2 (earliest) → 0; −1 (latest) → HW; an explicit offset above HW → HW; over `maxSubs` → `SubscribeResponse{errorCode = Internal}` with a log line (bounded, never silent). `pump()` reads at most `kCatalogReadChunk` per sub per pass into one **preallocated** buffer, never past the HW (followers serve their own HW — committed data only), encodes `Records{subscriptionId, topicId = −6, partition 0, baseOffset}` frames (whole batches, like `servePushRead_`), and when a `Backlog` sub reaches the HW it sends the empty-payload marker `Records{baseOffset = HW}` and becomes `Live` (the transition is the "once"); a `Live` sub pushes new commits as they arrive (no new marker). Egress full → the sub keeps its state and `nextOffset` and resumes next pass.

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

```

and, appended to `CatalogAllocRegressionTest.cpp` (the own-binary allocation gate of Task 7, F12; `ReadRig` lives in `libs/node/tests/support/CatalogReadRig.h` so both files use it — add `#include "support/CatalogReadRig.h"` there):

```cpp
TEST(CatalogAllocRegression, IdleCatalogSubscriptionsAllocateNothingRegression) {
    rune::node::test::ReadRig r("rd_alloc");
    r.commitEvents(3);
    r.server.subscribe(7, 1, -2);
    r.server.pump();                                   // backlog + marker: the sub is Live
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) (void)r.server.pump();
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}
```

plus a gateway test `CatalogGatewayRegression.CatalogSubscribeIsForwardedOnAnyBrokerAndPartitionMustBeZero` (forwarded even with `RouteRemote`; partition 1 → `InvalidRequest`).

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement**, **Step 4: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogReadServer|CatalogGateway|CatalogAlloc' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs
git -C ~/Projects/se-worktrees/rune-broker commit -m "feat(catalog): __catalog readable with Subscribe on any broker (admin-thread read server, caught-up marker)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint D (controller):** full broker `*Regression*` suite (write path, conformance port, leadership, reads).

---
### Task 13: Client — `catalogHistory` / `catalogAsOf` / `catalogDiff` over the shared library, with a cached prefix

Spec §7: "the client consumes it, with the existing Subscribe from offset 0, and replays it with the shared library … the client caches the replayed prefix." Shared-nothing: the record handler runs on the runner thread and only copies records into an SPSC ring; the app thread owns the cache and the replayer.

**Files:**
- Modify: `rune-client-cpp/CMakeLists.txt` (`rune_catalog` from `${RUNE_CODEC_ROOT}/src/catalog/*.cpp`, next to `rune_codec`, with `${RUNE_PROTOCOL_INCLUDE}` on its include path and wrapped in `if(NOT TARGET rune_catalog)` like the other shared sibling targets (Task 5, F7); `rune_client_api` links it)
- Modify: `rune-client-cpp/include/rune/client/TopicHandle.h` (`static constexpr TopicHandle catalog() noexcept { return {rune::protocol::client::kCatalogTopicId, 1}; }`), `include/rune/client/subscriber/Subscription.h` (`SubscribeOptions::onCaughtUp` — `std::function<void(int64_t highWatermark)>`, invoked on the runner thread for an empty-payload `Records` frame of a catalog subscription), `src/subscriber/SubscriberHub.cpp` (dispatch of that frame), `src/Client.cpp` (`subscribe` accepts `TopicHandle::catalog()` with partition 0)
- Create: `rune-client-cpp/include/rune/client/catalog/CatalogReader.h`, `src/catalog/CatalogReader.cpp`
- Modify: `rune-client-cpp/include/rune/client/Client.h` (the three API calls)
- Test: `rune-client-cpp/tests/catalog/CatalogReaderTest.cpp` — **non-blocking** reader tests on `test_support::InvokerGateway` (a `templateHandlers[Subscribe]` handler answers the catalog Subscribe and pushes the golden events, one record per `Records` frame, then the empty-payload marker); add to `CMakeLists.txt`
- Test (broker repo, F7): `libs/node/tests/client/ClientCatalogReadBrokerRegressionTest.cpp` in `rune_client_broker_tests` (Task 11) — the blocking `catalogHistory` / `catalogAsOf` / `catalogDiff` against a real `BrokerNode` (`OneBroker`), committed in rune-broker

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
      // Non-blocking pair (F7: what the single-threaded fake tests): subscribe from the cache end, then
      // drain the ring each call; nullopt = not done yet (the caller keeps pumping doWork()).
      OpResult beginRefresh();
      std::optional<CatalogResult<void>> pollRefresh(int64_t nowNs, int64_t deadlineNs);
      // Blocking: beginRefresh + pollRefresh until done (pumps doWork() in Invoker mode, idles in Thread mode).
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

- [ ] **Step 1: Write the failing tests** — `CatalogReaderTest.cpp` (client repo; non-blocking: the invoker `Rig`, `invokerCfg` and fake clock `g_now` of Task 5's tests, copied into this file's anonymous namespace; `CatalogFeed` is a file-local helper that installs `gw.templateHandlers[Subscribe]` serving a vector of events from the requested offset, records each `fromOffset`, and can withhold the marker):

```cpp
namespace {
/// Drive one non-blocking refresh to completion (or the 10 s hang guard).
std::optional<rune::client::CatalogResult<void>> refreshOnce(Rig& r, rune::client::CatalogReader& reader,
                                                              int64_t budgetNs) {
    if (reader.beginRefresh() != rune::client::OpResult::Accepted) return std::nullopt;
    const int64_t deadline = g_now + budgetNs;
    std::optional<rune::client::CatalogResult<void>> done;
    r.drive([&] { done = reader.pollRefresh(g_now, deadline); return done.has_value(); }, /*stepNs=*/1'000'000);
    return done;
}
} // namespace

TEST(CatalogReaderRegression, RefreshReplaysEveryEventAndStopsAtTheMarker) {
    Rig r(invokerCfg(3980, 3981)); r.connect();
    CatalogFeed feed(r.gw, rune::catalog::testing::goldenScriptV1());
    rune::client::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(reader.events().size(), 18u);
    rune::catalog::CatalogReplayer lib;
    for (const auto& e : reader.events()) ASSERT_TRUE(lib.feedDecoded(e.offset, e.event));
    EXPECT_EQ(reader.state().digest(), lib.state().digest()) << "the reader replays with the shared library";
}

TEST(CatalogReaderRegression, SecondRefreshFetchesOnlyTheNewTail) {
    Rig r(invokerCfg(3982, 3983)); r.connect();
    auto log = rune::catalog::testing::goldenScriptV1();
    CatalogFeed feed(r.gw, {log.begin(), log.begin() + 10});
    rune::client::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.append({log.begin() + 10, log.end()});
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10})) << "the replayed prefix is cached";
    EXPECT_EQ(reader.events().size(), 18u);
}

TEST(CatalogReaderRegression, TamperedEventIsAReplayErrorNamingItsOffset) {
    Rig r(invokerCfg(3984, 3985)); r.connect();
    auto log = rune::catalog::testing::goldenScriptV1();
    log[4].fingerprint ^= 1;
    CatalogFeed feed(r.gw, log);
    rune::client::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rune::client::CatalogReadError::Kind::Replay);
    EXPECT_EQ(done->error().offset, 4);
    EXPECT_EQ(reader.events().size(), 4u) << "the verified prefix stays cached";
}

TEST(CatalogReaderRegression, NoCaughtUpMarkerIsATimeoutNotAHang) {
    Rig r(invokerCfg(3986, 3987)); r.connect();
    CatalogFeed feed(r.gw, rune::catalog::testing::goldenScriptV1());
    feed.suppressCaughtUpMarker();
    rune::client::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, /*budgetNs=*/500'000'000);
    ASSERT_TRUE(done.has_value()) << "the deadline ends it";
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rune::client::CatalogReadError::Kind::Timeout);
}
```

`ClientCatalogReadBrokerRegressionTest.cpp` (broker repo, `rune_client_broker_tests`, F7) — the **blocking** API against a real `BrokerNode`; the expected values come from replaying the broker's own log with the library, never hand-typed:

```cpp
TEST(ClientCatalogReadBrokerRegression, HistoryAsOfAndDiffMatchTheLibraryOverTheBrokersLog) {
    OneBroker b("cr_all");
    auto client = b.connectClient();
    b.runSchemaLifecycle(*client, "players");            // define typed, alter add, alter rename, set compat (via *AndWait)
    ASSERT_EQ(client->defineTopicAndWait("events", 2, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode, 0);
    const auto log = b.catalogEvents();                   // RecordedEvents decoded from the node's __catalog
    auto h = client->catalogHistory({.type = rune::catalog::ObjectType::Schema, .name = "players"}, 10s);
    ASSERT_TRUE(h.has_value()) << h.error().detail;
    EXPECT_EQ(h->size(), rune::catalog::history(log, {.type = rune::catalog::ObjectType::Schema, .name = "players"}).size());
    auto s = client->catalogAsOf({.by = rune::catalog::AsOf::By::Offset, .value = 2}, 10s);
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->digest(), rune::catalog::stateAsOf(log, {.by = rune::catalog::AsOf::By::Offset, .value = 2})->digest());
    auto d = client->catalogDiff({.by = rune::catalog::AsOf::By::Offset, .value = 0},
                                 {.by = rune::catalog::AsOf::By::Offset, .value = static_cast<int64_t>(log.size()) - 1}, 10s);
    ASSERT_TRUE(d.has_value());
    EXPECT_FALSE(d->empty());
}

TEST(ClientCatalogReadBrokerRegression, ASecondReadSeesNewDdlFromTheCachedPrefix) {
    OneBroker b("cr_tail");
    auto client = b.connectClient();
    ASSERT_EQ(client->defineTopicAndWait("a", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode, 0);
    ASSERT_EQ(client->catalogHistory({}, 10s)->size(), 1u);
    ASSERT_EQ(client->defineTopicAndWait("b", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode, 0);
    ASSERT_EQ(client->catalogHistory({}, 10s)->size(), 2u);
    EXPECT_EQ(b.catalogSubscribeFromOffsets(), (std::vector<int64_t>{0, 1})) << "the client resumed from its cache end";
}
```

(`OneBroker::runSchemaLifecycle` and `catalogSubscribeFromOffsets` — the `fromOffset` of each catalog Subscribe the node's `CatalogReadServer` accepted, read through `onAdminThreadForTest` — are added to `libs/node/tests/client/OneBroker.h` here.)

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement**, **Step 4: Run**

Run: `cmake ~/Projects/se-worktrees/rune-client-cpp/build && cmake --build ~/Projects/se-worktrees/rune-client-cpp/build -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-client-cpp/build -R 'CatalogReader|Subscriber|OneToOneRingBuffer' --output-on-failure -j14`
Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_client_broker_tests && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'ClientCatalogReadBroker|ClientDdlBroker' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit** (two repos)

```bash
git -C ~/Projects/se-worktrees/rune-client-cpp add include src tests CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-client-cpp commit -m "feat(client): catalogHistory / catalogAsOf / catalogDiff over the shared replay library (cached prefix)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C ~/Projects/se-worktrees/rune-broker add libs/node/tests/client
git -C ~/Projects/se-worktrees/rune-broker commit -m "test(catalog): the real client's catalog reads against a real broker

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: rune-ql — `SHOW HISTORY`, `SHOW … AS OF`, `DIFF`, `DROP TOPIC|VIEW`, `ALTER TOPIC … SET`, `replicas N`, untyped topics

Spec §7 (QL column of the reads table) and §8 (`replicationFactor` on CreateTopic). Keywords are case-insensitive like the existing grammar's.

**Files:**
- Modify: `rune-ql/include/rune/ql/Ast.h`, `include/rune/ql/Parser.h`, `src/Parser.cpp`, `include/rune/ql/Backends.h`, `src/Backends.cpp`
- Create: `rune-ql/tests/CatalogStatementsTest.cpp`; modify `rune-ql/CMakeLists.txt` (add the test; F32: `set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include" CACHE PATH …)` on `rune_ql`'s PUBLIC include dirs — rune-ql includes only the std-only `rune/protocol/CatalogTypes.h`; the client's own `rune_ql` target already has the protocol include)

**Interfaces:**
- Produces (namespace `rune::ql`):
  ```cpp
  // TopicDef gains: int16_t replicas = 0 (F31: the wire / client / catalog type; parse bound 0..32767);
  //   std::vector<ConfigSetting> with;  (fields may be EMPTY → an untyped topic; then `key` is absent)
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
  //   F8: exactly ONE statement — any non-blank input after its ';' is a ParseError
  //   ("expected end of input after statement 1; use a script (rune ql -f) for several statements").
  struct ScriptError { std::size_t statementIndex; ParseError error; };          // 1-based index of the failing statement
  std::expected<std::vector<Statement>, ScriptError> parseScript(std::string_view src) noexcept;
  //   F8: every ';'-terminated statement in order; strict — a statement that fails to parse, or trailing
  //   input that is not a complete statement, is an error naming its index.  Never "the first one only".
  // Backends:
  struct AlterTopicConfigPlan { std::string name; std::vector<std::pair<uint8_t /*ConfigKey*/, int64_t>> changes; };
  [[nodiscard]] std::expected<AlterTopicConfigPlan, ParseError> applyConfigPlan(const AlterTopicConfigDef&);
  using TopicCreateConfig = rune::protocol::TopicConfig;   // F32: the ONE definition (rune/protocol/CatalogTypes.h,
                                                           // std-only) — no hand-copied defaults in rune-ql
  // DefineTopicPlan gains: int16_t replicas (F31); TopicCreateConfig config (from `with`);
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
TEST(QlCatalogStatementsRegression, ReplicasIsInt16WithAParseBound) {
    // F31: int16_t like the wire, the client and the catalog.
    static_assert(std::is_same_v<decltype(ql::TopicDef{}.replicas), int16_t>);
    EXPECT_EQ(std::get<ql::TopicDef>(*ql::parse("topic t partitions 1 replicas 32767;")).replicas, 32767);
    auto r = ql::parse("topic t partitions 1 replicas 32768;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("replicas"), std::string::npos);
}
TEST(QlCatalogStatementsRegression, TopicCreateConfigIsTheOneProtocolDefinition) {
    static_assert(std::is_same_v<ql::TopicCreateConfig, rune::protocol::TopicConfig>);   // F32
    EXPECT_EQ(ql::applyPlan(std::get<ql::TopicDef>(*ql::parse("topic t partitions 1;"))).config,
              rune::protocol::TopicConfig{}) << "no `with`: exactly the shared defaults";
}
TEST(QlCatalogStatementsRegression, ParseRefusesTrailingStatementsRegression) {
    // F8: a second statement is never silently dropped.
    auto r = ql::parse("topic a partitions 1; topic b partitions 1;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("end of input"), std::string::npos);
    EXPECT_TRUE(ql::parse("topic a partitions 1;  \n").has_value()) << "trailing blanks are fine";
}
TEST(QlCatalogStatementsRegression, ParseScriptReturnsEveryStatementInOrder) {
    auto s = ql::parseScript("topic events partitions 1;\ntopic multi partitions 3;\ndrop topic events;\n");
    ASSERT_TRUE(s.has_value()) << s.error().error.message;
    ASSERT_EQ(s->size(), 3u);
    EXPECT_EQ(std::get<ql::TopicDef>((*s)[1]).name, "multi");
    EXPECT_TRUE(std::holds_alternative<ql::DropTopicDef>((*s)[2]));
}
TEST(QlCatalogStatementsRegression, ParseScriptErrorNamesTheStatementIndexRegression) {
    auto s = ql::parseScript("topic a partitions 1;\ntopic b partitions;\ntopic c partitions 1;");
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().statementIndex, 2u);
    auto t = ql::parseScript("topic a partitions 1; topic b");   // an unterminated tail is an error, not ignored
    ASSERT_FALSE(t.has_value());
    EXPECT_EQ(t.error().statementIndex, 2u);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake ~/Projects/se-worktrees/rune-ql/build && cmake --build ~/Projects/se-worktrees/rune-ql/build -j14 --target rune_ql_tests`
Expected: compile errors (`DropTopicDef` undeclared).

- [ ] **Step 3: Implement** the grammar (one new `parseX()` per statement in the existing recursive-descent style; `parseStatement()` dispatches on the first keyword: `topic`, `transient`, `alter`, `drop`, `show`, `diff`). Dotted config keys are tokenised as `IDENT ('.' IDENT)*` joined. `parse()` = `parseStatement()` + an end-of-input check (only whitespace / comments may follow); `parseScript()` loops `parseStatement()` until end of input, recording the 1-based index of each statement (F8). `replicas` parses as an integer checked against `[0, INT16_MAX]` (F31).

- [ ] **Step 4: Update the client** for the new variant (`rune-client-cpp/src/cli/Cli.cpp` uses `parse`'s result: replace its `std::get_if<TopicDef>` / `<AlterTopicDef>` handling with a `std::visit` over `Statement`, every new alternative answering "not supported by 'schema apply' — use 'rune ql'" until Task 15; `schema apply` keeps the single-statement `parse()`, so a file with several statements is now an explicit parse error naming `rune ql -f` instead of applying only the first — F8, pinned by a new `RuneCliOffline` case `SchemaApplyRefusesAMultiStatementFileRegression` in `rune-client-cpp/tests/RuneCliOfflineTest.cpp`). Build the client and run `ctest --test-dir ~/Projects/se-worktrees/rune-client-cpp/build -R RuneCliOffline --output-on-failure`.

- [ ] **Step 5: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-ql/build -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-ql/build --output-on-failure && cmake --build ~/Projects/se-worktrees/rune-client-cpp/build -j14`
Expected: all rune-ql tests PASS; client builds.

- [ ] **Step 6: Commit** (two repos)

```bash
git -C ~/Projects/se-worktrees/rune-ql add include src tests CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-ql commit -m "feat(ql): SHOW HISTORY / AS OF / DIFF, DROP TOPIC|VIEW, ALTER TOPIC SET, replicas, untyped topics

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C ~/Projects/se-worktrees/rune-client-cpp add src/cli/Cli.cpp tests/RuneCliOfflineTest.cpp
git -C ~/Projects/se-worktrees/rune-client-cpp commit -m "refactor(cli): visit the rune-ql Statement variant; schema apply refuses multi-statement files

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 15: CLI — `rune ql` (execute any statement) and `rune catalog export | replay | diff`

Spec §7 "Replay to rebuild (CLI)". `replay` re-issues each event as DDL on the target (the target assigns its own offsets and ids and records `replayedFrom`) and requires the target's resulting fingerprint to equal the source event's after each step; a mismatch aborts loudly at that event.

**Files:**
- Create: `rune-client-cpp/include/rune/cli/CatalogCommands.h`, `src/cli/CatalogCommands.cpp`
- Modify: `rune-client-cpp/src/cli/Cli.cpp` (`ql` and `catalog` subcommands; `printUsage`), `include/rune/client/view/ViewPlanAuthor.h` (`deserializeAuthored(span) → std::optional<ViewPlan>`; `remapSources(ViewPlan&, std::span<const int32_t> topicIds)`)
- Test: `rune-client-cpp/tests/RuneCliCatalogTest.cpp` (offline only: `--dry-run`, `.rcat` files, the pure row / step-check functions), `rune-client-cpp/tests/RuneCliLiveTest.cpp` (append a live `ql` case gated like its existing cases)
- Test (broker repo, F7): `libs/node/tests/client/RuneCliBrokerRegressionTest.cpp` in `rune_client_broker_tests` (link `rune_cli_lib` there) — every `rune::cli::run(… --broker …)` case runs against real in-process `BrokerNode`s (`OneBroker`, Task 11); the single-threaded fake cannot serve a CLI that builds its own `Client` and blocks

**Interfaces:**
- Consumes: Tasks 5, 13, 14; `rune::catalog::{unresolveAlterOps, RequestId}` (Tasks 1–2).
- Produces — CLI surface (exit 0 success, 1 explicit error with a message on stderr, 2 usage):
  ```
  rune ql (-e "<statement>" | -f <file|->) [--dry-run] [--verbose] [--request-id <32 hex digits>] --broker host:port
        executes DDL (statement text recorded) and reads; --dry-run parses and prints the plan without a broker;
        --verbose prints every followed Redirect and NotLeader retry to stderr;
        -f runs EVERY statement of the file in order (ql::parseScript) and stops at the first error — parse or
           broker — with "statement N: <error>" on stderr (exit 1); nothing after it is issued (F8);
        --request-id fixes the DDL's RequestId (hi = first 16 hex digits, lo = last 16), so re-running the same
           statement after a crash or a kill returns the ORIGINAL result instead of a conflict (F17); with -f,
           statement N uses {hi, lo + N - 1}
  rune catalog export [--up-to <offset>] --broker host:port > file.rcat
  rune catalog replay --from <file.rcat|host:port> --into host:port [--up-to <offset>] [--cluster-id <id>]
        (--cluster-id defaults to the --from argument verbatim)
  rune catalog diff --from <file.rcat|host:port> --to <file.rcat|host:port>
  ```
  ```cpp
  // include/rune/cli/CatalogCommands.h (namespace rune::cli) — pure helpers, unit-tested offline:
  [[nodiscard]] std::string formatHistoryRow(const rune::catalog::RecordedEvent&);   // F18: includes replayedFrom
  [[nodiscard]] std::optional<std::string> verifyReplayStep(const rune::catalog::RecordedEvent& source,
                                                            const rune::catalog::RecordedEvent& target);   // nullopt = ok
  [[nodiscard]] rune::catalog::RequestId replayRequestId(std::string_view sourceClusterId, int64_t sourceOffset) noexcept;
  [[nodiscard]] std::optional<rune::catalog::RequestId> parseRequestId(std::string_view hex32) noexcept;
  ```
  DDL statements map onto Task 5's calls 1:1 (`TopicDef` → `defineTopicAndWait` with `TopicOptions{replicas, config}` — `config` is the plan's `TopicCreateConfig`, which **is** `rune::protocol::TopicConfig` (F32), assigned, not copied field by field; `AlterTopicConfigDef` → `alterTopicConfigAndWait`; drops → `drop*AndWait`; `AlterTopicDef` → `alterTopicAndWait`), each with `DdlOptions{statement, requestId from --request-id}`. `rune ql` prints reads as aligned text tables: history rows `offset  time(UTC)  kind  object(objectId)  version prev→new  principal  replayedFrom  statement` — `replayedFrom` is `<clusterId>@<offset>` or `-` (F18; `formatHistoryRow`); `SHOW … AS OF` prints the object's definition (topic: partitions, replicas, persistence, config, schema via `rune::codec::toText`; view: sources, key, output columns); `DIFF` prints `+ added`, `- removed`, `~ changed: field, field`. `DIFF SCHEMA t VERSION a TO b` = `diff(asOf(schema t version a), asOf(schema t version b))` restricted to `t`.
- Replay algorithm (`CatalogCommands.cpp`): read the source events (`readRcat` or a `CatalogReader` on the source cluster) and **verify them with a `CatalogReplayer`** (a tampered source stops before anything is issued), stop after `--up-to`; for each event in order build the target DDL from its `ops`: `CreateTopic` → `defineTopicAndWait(name, partitions, keyField, descriptorBlob, validate, persistence, {rf, config}, ddl)`; `CreateSchema` → `defineTopicAndWait` on the existing name with the descriptor; `AlterSchema` → `unresolveAlterOps(<the target's current schema of that object, from its CatalogReader state>, ops)` (F20: the event stores durable tags; the target's state has the same definition, so the tags map to the same names) → `encodeAlterOps` → `alterTopicAndWait(expectedVersion 0)`; `SetCompatibility` → `alterTopicAndWait` with the single `SET_COMPATIBILITY mode` op; `AlterTopicConfig` → `alterTopicConfigAndWait`; `CreateView` / `AlterView` → the normalized authored plan, `remapSources` from `sourceNames` to the **target** topic ids (from the target's `CatalogReader` state), then `createViewAndWait` / `alterViewAndWait`; drops → `dropTopicAndWait` / `dropViewAndWait`. Each with `DdlOptions{statement = event.statement, requestId = replayRequestId(sourceClusterId, offset) = {fnv1a64(sourceClusterId), uint64(offset)}, replayedFrom = {clusterId, offset, principal = clientName@sourceAddress}}` — rerunning a replay after a crash is idempotent (same request ids, same statements → the broker's `(requestId, requestFingerprint)` dedupe returns the original results, F14). After each answer: `catalogHistory` refresh on the target; the target event whose `principal.requestId` equals this step's request id (new, or the one a rerun deduplicated to) is checked with `verifyReplayStep(source, target)`: `replayedFrom.offset == source offset` and **the same fingerprint**, else exit 1 with its message `replay aborted at source offset N (<kind> '<name>'): target fingerprint 0x… != source 0x…`. A DDL error → exit 1 with `replay aborted at source offset N (<kind> '<name>'): <code name> (<code>) <detail>`; nothing past it is issued.
- `diff` compares the two final states with `rune::catalog::diff`; prints differences; exit 0 when there are none, 3 when there are (scripts test `== 0`).

- [ ] **Step 1: Write the failing tests** — `RuneCliCatalogTest.cpp` (client repo, offline — no broker, nothing blocking on a fake):

```cpp
TEST(RuneCliCatalogRegression, QlDryRunParsesWithoutABroker) {
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic t partitions 2 replicas 3 with (tier = leader);", "--dry-run"}, out, err), 0);
    EXPECT_NE(out.str().find("replicas 3"), std::string::npos);
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic t partitions;", "--dry-run"}, out, err), 1);
}

TEST(RuneCliCatalogRegression, QlFileDryRunNamesTheFailingStatementIndexRegression) {
    // F8: -f is a script; a bad statement is named by its index, never skipped.
    const auto f = writeTemp("three.rql", "topic a partitions 1;\ntopic b partitions;\ntopic c partitions 1;\n");
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-f", f, "--dry-run"}, out, err), 1);
    EXPECT_NE(err.str().find("statement 2"), std::string::npos) << err.str();
    std::stringstream o2, e2;
    const auto g = writeTemp("ok.rql", "topic a partitions 1;\ntopic b partitions 2;\n");
    ASSERT_EQ(rune::cli::run({"ql", "-f", g, "--dry-run"}, o2, e2), 0) << e2.str();
    EXPECT_NE(o2.str().find("topic a"), std::string::npos);
    EXPECT_NE(o2.str().find("topic b"), std::string::npos) << "every statement is planned, not just the first";
}

TEST(RuneCliCatalogRegression, RequestIdFlagIsValidated) {
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic t partitions 1;", "--request-id", "xyz", "--dry-run"}, out, err), 2);
    EXPECT_NE(err.str().find("--request-id"), std::string::npos);
    const auto id = rune::cli::parseRequestId("0000000000000001000000000000002a");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(*id, (rune::catalog::RequestId{1, 42}));
}

TEST(RuneCliCatalogRegression, HistoryRowPrintsReplayedFromRegression) {
    // F18: Task 24 reads "replayedFrom a@<offset>" from `show history`.
    auto log = rune::catalog::testing::goldenScriptV1();
    rune::catalog::RecordedEvent r{7, log[7]};
    EXPECT_NE(rune::cli::formatHistoryRow(r).find(" - "), std::string::npos) << "no replayedFrom: '-'";
    r.event.replayedFrom = rune::catalog::ReplayedFrom{.clusterId = "a", .offset = 7, .principal = "cli@x"};
    EXPECT_NE(rune::cli::formatHistoryRow(r).find("a@7"), std::string::npos);
}

TEST(RuneCliCatalogRegression, ReplayStepMismatchNamesTheSourceOffset) {
    auto log = rune::catalog::testing::goldenScriptV1();
    rune::catalog::RecordedEvent src{6, log[6]}, tgt{40, log[6]};
    tgt.event.replayedFrom = rune::catalog::ReplayedFrom{.clusterId = "src", .offset = 6};
    EXPECT_FALSE(rune::cli::verifyReplayStep(src, tgt).has_value());
    tgt.event.fingerprint ^= 1;
    const auto m = rune::cli::verifyReplayStep(src, tgt);
    ASSERT_TRUE(m.has_value());
    EXPECT_NE(m->find("replay aborted at source offset 6"), std::string::npos) << *m;
}

TEST(RuneCliCatalogRegression, DiffOfTwoFilesReportsDifferencesWithExit3) {
    const auto full = writeRcatFile("full.rcat", rune::catalog::testing::goldenScriptV1());
    auto shorter = rune::catalog::testing::goldenScriptV1(); shorter.resize(12);
    const auto part = writeRcatFile("part.rcat", shorter);
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", full, "--to", part}, out, err), 3);
    EXPECT_NE(out.str().find("players"), std::string::npos);
    std::stringstream o2, e2;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", full, "--to", full}, o2, e2), 0);
}
```

(`writeTemp` / `writeRcatFile` are file-local helpers writing under a per-test temp dir.)

`RuneCliBrokerRegressionTest.cpp` (broker repo, `rune_client_broker_tests`, F7 — real `BrokerNode`s, the real CLI):

```cpp
TEST(RuneCliBrokerRegression, ExportWritesEveryEventAndUpToStops) {
    OneBroker b("cli_export");
    b.runEveryKind();                                     // all 9 kinds through the real client (OneBroker helper)
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"catalog", "export", "--up-to", "5", "--broker", b.endpoint()}, out, err), 0) << err.str();
    auto recs = rune::catalog::readRcat(out);
    ASSERT_TRUE(recs.has_value());
    EXPECT_EQ(recs->size(), 6u);
}

TEST(RuneCliBrokerRegression, ReplayReissuesEveryKindAndVerifiesFingerprints) {
    OneBroker src("cli_src"), dst("cli_dst");
    src.runEveryKind();
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(),
                              "--cluster-id", "src"}, out, err), 0) << err.str();
    const auto s = src.catalogEvents(), d = dst.catalogEvents();
    ASSERT_EQ(d.size(), s.size());
    for (std::size_t i = 0; i < d.size(); ++i) {
        ASSERT_TRUE(d[i].event.replayedFrom.has_value());
        EXPECT_EQ(d[i].event.replayedFrom->clusterId, "src");
        EXPECT_EQ(d[i].event.fingerprint, s[i].event.fingerprint) << "offset " << i;
    }
    std::stringstream o2, e2;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", src.endpoint(), "--to", dst.endpoint()}, o2, e2), 0) << o2.str();
    std::stringstream h, e3;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "show history;", "--broker", dst.endpoint()}, h, e3), 0);
    EXPECT_NE(h.str().find("src@0"), std::string::npos) << "F18: replayedFrom in the history rows";
}

TEST(RuneCliBrokerRegression, ReplayAbortsLoudlyAtTheFirstFailingEvent) {
    OneBroker src("cli_src2"), dst("cli_dst2");
    src.runEveryKind();
    ASSERT_EQ(dst.connectClient()->defineTopicAndWait(src.catalogEvents()[1].event.objectName, 1, "", {}, false,
                                                     rune::client::Persistence::Disk, {}, {}, 10s).errorCode, 0);
    const std::size_t before = dst.catalogEvents().size();
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint()}, out, err), 1);
    EXPECT_NE(err.str().find("replay aborted at source offset 1"), std::string::npos) << err.str();
    EXPECT_EQ(dst.catalogEvents().size(), before + 1) << "offset 0 replayed, nothing issued past the failure";
}

TEST(RuneCliBrokerRegression, ReplayIsIdempotentAfterACrash) {
    OneBroker src("cli_src3"), dst("cli_dst3");
    src.runEveryKind();
    std::stringstream o, e;
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint(),
                              "--up-to", "4"}, o, e), 0) << e.str();
    ASSERT_EQ(rune::cli::run({"catalog", "replay", "--from", src.endpoint(), "--into", dst.endpoint()}, o, e), 0) << e.str();
    EXPECT_EQ(dst.catalogEvents().size(), src.catalogEvents().size()) << "same request ids: the first 5 are deduplicated";
}

TEST(RuneCliBrokerRegression, QlExecutesDdlWithItsStatementAndPrintsHistory) {
    OneBroker b("cli_ql");
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "topic events partitions 2 replicas 1;", "--broker", b.endpoint()}, out, err), 0)
        << err.str();
    EXPECT_EQ(b.catalogEvents().back().event.statement, "topic events partitions 2 replicas 1;");
    std::stringstream h;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "show history topic events;", "--broker", b.endpoint()}, h, err), 0);
    EXPECT_NE(h.str().find("CreateTopic"), std::string::npos);
}

TEST(RuneCliBrokerRegression, QlFileRunsEveryStatementAndStopsAtTheFirstErrorRegression) {
    // F8: e2e.rql-style scripts create every topic, in order; a failure stops the script, named.
    OneBroker b("cli_script");
    const auto f = writeTemp("s.rql", "topic a partitions 1;\ntopic b partitions 3;\ntopic a partitions 1;\ntopic c partitions 1;\n");
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-f", f, "--broker", b.endpoint()}, out, err), 1);
    EXPECT_NE(err.str().find("statement 3"), std::string::npos) << err.str();
    EXPECT_EQ(b.catalogEvents().size(), 2u) << "a and b created; c never issued";
}

TEST(RuneCliBrokerRegression, QlRequestIdMakesARerunReturnTheOriginalResultRegression) {
    // F17: a re-run of a DDL whose first attempt may have committed returns its original success.
    OneBroker b("cli_rid");
    const std::string rid = "00000000000000c1000000000000002a";
    std::stringstream o1, e1, o2, e2;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "topic midkill partitions 2;", "--request-id", rid, "--broker", b.endpoint()},
                             o1, e1), 0) << e1.str();
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic midkill partitions 2;", "--request-id", rid, "--broker", b.endpoint()},
                             o2, e2), 0) << "not TopicExists: " << e2.str();
    EXPECT_EQ(b.catalogEvents().size(), 1u);
    std::stringstream o3, e3;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic other partitions 2;", "--request-id", rid, "--broker", b.endpoint()},
                             o3, e3), 1);
    EXPECT_NE(e3.str().find("requestId reused for a different statement"), std::string::npos) << e3.str();
}

TEST(RuneCliBrokerRegression, DiffOfTwoBrokersReportsDifferencesWithExit3) {
    OneBroker a("cli_da"), b("cli_db");
    a.runEveryKind();
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", a.endpoint(), "--to", b.endpoint()}, out, err), 3);
}
```

(`OneBroker::runEveryKind()` — added to `libs/node/tests/client/OneBroker.h` here — issues all nine kinds through the real client: CreateTopic typed + untyped, CreateSchema, AlterSchema, SetCompatibility, AlterTopicConfig, CreateView, AlterView, DropView, DropTopic; `writeTemp` writes under the test's `TmpDir`.)

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement**, **Step 4: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-client-cpp/build -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli' --output-on-failure -j14`
Expected: PASS (the live cases skip without a broker, as today).
Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_client_broker_tests && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'RuneCliBroker|ClientCatalogReadBroker|ClientDdlBroker' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-client-cpp add include src tests CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-client-cpp commit -m "feat(cli): rune ql (-f scripts, --request-id) + rune catalog export|replay|diff (fingerprint-verified, idempotent replay)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C ~/Projects/se-worktrees/rune-broker add libs/node/tests/client libs/node/CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-broker commit -m "test(catalog): the real rune CLI against real brokers (ql scripts, request ids, export/replay/diff)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint E (controller):** full `*Regression*` suites in client, rune-ql, rune-codec; broker `build-fast` builds and its `*Regression*` suite passes (it now includes `rune_client_broker_tests`: the real client and CLI against real brokers, F7).

---
### Task 16: Broker — boot from `__catalog`; retire `__topic_schemas`, `__views` and runtime-topic recovery

Spec §3 ("single source of truth"), §5.3 ("Restart: the broker replays its local `__catalog` copy, then catches up from the leader"), §1 (no migration). Boot replays the checkpointed catalog prefix **on the startup thread, before any boot builder runs**, so the TopicRegistry, replication, worker placement and the MetadataService see every topic exactly as the catalog says — then the admin thread takes ownership of the applier and continues live.

**Files:**
- Modify: `libs/node/src/BrokerNode.cpp`:
  - step 1b (anchor `// 1b. Restart recovery of RUNTIME-defined topics`) is replaced by `bootFromCatalog_()`: build the catalog `Log` + replica (deferred plane, Task 6's `buildCatalog_` moves here), construct the `CatalogApplier` (phase `AwaitingBoot`), `applier.replayLocal(*catalogReplica_, BootEffects{bootCatalogSink_})` — **the one boot replay** (F4); the `BootCatalogSink` fills `bootTopics_` (`std::vector<TopicSpec>`, live topics in topic-id order) + `bootDroppedIds_` and stashes recovery view installs and validator pushes for after the workers exist. A halted boot replay does **not** refuse to start: the broker boots with the applied prefix, halted (route `RouteHalted`), logged at critical — spec §5.3 "keeps serving what it had already applied".
  - `TopicRegistry` is constructed **empty**, then `createTopicAt(id, spec)` for each `bootTopics_` entry and a `Dropped` slot for each `bootDroppedIds_` id.
  - every `config_.topics` consumer (~20 sites, `grep -n 'config_.topics' libs/node/src/BrokerNode.cpp`) reads `bootTopics_` instead.
  - `buildAdminWorker()`: delete the `__topic_schemas` block (anchor `const fs::path schemaDir = fs::path{config_.dataDir} / "__topic_schemas";`) and the `__views` block (anchor `const fs::path viewsDir = fs::path{config_.dataDir} / "__views";`) and their members (`schemaLog_`, `schemaReplica_`, `viewsLog_`, `viewsReplica_`); hand the applier (phase `Booted`) + catalog replica to `AdminWorkerAgent`, whose first admin pass calls `applier.goLive(LiveEffects{ring encoders})` once; post the stashed recovery installs once the workers are built.
  - **delete Task 9's admin-thread boot replay** (the `replayLocal(*catalogReplica_, BootEffects{…})` + `goLive` call before the admin thread's first `doWork`, F4): the prefix is applied exactly once, on the startup thread. A second `replayLocal` would be refused by the applier anyway (Task 7) — the deletion removes the call, the refusal guards against its return.
  - `firstRuntimeTopicId_` and everything that reads it are deleted.
- Delete: `libs/node/include/rune/node/RuntimeTopicRecovery.h`, `libs/node/src/RuntimeTopicRecovery.cpp`, `libs/node/tests/node/RuntimeTopicRecoveryTest.cpp`
- Create: `libs/node/include/rune/node/TopicDirReconcile.h`, `libs/node/src/TopicDirReconcile.cpp` — `reconcileTopicDirs(dataDir, liveTopics, now)`: (1) `remove_all` every entry of `dataDir/dropped/` (left by a purge the admin thread could not finish), (2) **quarantine** (never delete) every `dataDir/topics/<name>` that no live topic claims (exactly or by ASCII case fold) to `dataDir/quarantine/<name>-<now>[-N]`, logged at warn, symlinks never followed (the existing `quarantine` helper code moves here verbatim from `RuntimeTopicRecovery.cpp`), (3) a `dataDir/__topic_schemas` or `dataDir/__views` dir is left untouched and logged once at warn ("retired catalog dir ignored — `__catalog` is the catalog; no migration").
- Modify: `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp` (delete `rebuildFromLog_`, `recoverViewsOnPromotion_`, `rebuildFailed_`, `viewsRebuildFailed_`, `refusedForFailedRebuild_` — every broker now has the verified catalog from boot, not only a promoted one)
- Modify: `libs/node/include/rune/node/SchemaRegistry.h`, `libs/node/src/SchemaRegistry.cpp` (delete `TopicSchema::serialize/deserialize`, `kSchemaTopicId`, `rebuildFromRecords`, `allocateRuntimeId`, `RUNTIME_TOPIC_ID_BASE`), `ViewRegistry.h/.cpp` (delete `ViewRecord::serialize/deserialize`, `kViewsTopicId`, `rebuildFromRecords`), `libs/node/include/rune/node/TopicName.h` (reserved-name comment: `__catalog`, `__consumer_offsets`, `__transaction_state`, `__cluster_metadata`), `BrokerNode.h` (`schemaLogForTest`/`viewsLogForTest` → `catalogLogForTest`; the `onOwnerThreadForTest` guard names `__catalog (-6)`)
- Port: `libs/node/tests/node/SchemaRegistryTest.cpp`, `ViewRegistryTest.cpp` (drop the (de)serialize cases), `libs/node/tests/node/PerWorkerReplicationWiringTest.cpp`, `TopicNameTest.cpp`, `libs/replication/tests/replication/PartitionReplicaTest.cpp` (their `__topic_schemas` references), `libs/node/tests/node/ViewRecoveryRegressionTest.cpp` / `ViewReadinessRegressionTest.cpp` (recovery installs now come from boot replay on every broker)
- Test: `libs/node/tests/node/CatalogBootTest.cpp`

**Interfaces:**
- Consumes: Tasks 6–9.
- Produces: `BrokerNode::bootTopics_` (private), `reconcileTopicDirs(const std::string&, const std::vector<TopicSpec>&, int64_t) → TopicDirReconciliation{quarantined, purged}`, test hooks `bootReplaysForTest()` (the number of `replayLocal` calls that returned ≥ 0 since construction) and `catalogApplierPhaseForTest()` (`phaseName()`, read on the admin thread).

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

TEST(CatalogBootRegression, TheCatalogPrefixIsAppliedExactlyOnceAtBootRegression) {
    // F4: Task 9 replayed on the admin thread; this task replays on the startup thread.  Both together would
    // re-apply recorded request ids (Precondition) and halt the broker on every restart.
    BootRig r("boot_once");
    r.start();
    ASSERT_EQ(r.define({.name = "a", .partitions = 1, .requestId = {.hi = 0xB0, .lo = 1}}).code, 0);
    ASSERT_EQ(r.define({.name = "b", .partitions = 1, .requestId = {.hi = 0xB0, .lo = 2}}).code, 0);
    r.restart();
    EXPECT_EQ(r.catalogHaltedCounter(), 0);
    EXPECT_EQ(r.applierPhaseName(), "Live");
    EXPECT_EQ(r.bootReplaysForTest(), 1) << "one replayLocal that returned >= 0, on the startup thread";
    EXPECT_EQ(r.define({.name = "c", .partitions = 1}).code, 0);
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

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogBoot|SchemaRegistry|ViewRegistry|ViewRecovery|ViewReadiness|TopicName|PerWorkerReplicationWiring|PartitionReplica|CatalogApplier|CatalogWritePath|SchemaPersistence' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add -A libs
git -C ~/Projects/se-worktrees/rune-broker commit -m "feat(catalog): boot from __catalog; retire __topic_schemas, __views and runtime-topic recovery (no migration)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 17: Broker — delete `RUNE_TOPICS`, `NodeConfig::topics`, `rf=` and the name-collision boot rule; `TestCluster::defineTopic`; migrate the node unit tests

Spec §8: delete the setting and its parsing, static topic creation and recovery at boot, the static-versus-runtime collision rule, the `rf=N` option; in-process tests use a one-line `TestCluster::defineTopic(...)`.

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h` (delete `topics`, the `RUNE_TOPICS` doc lines, the `rf=`/`RUNE_TOPICS` mentions in `TopicSpec`'s comments — `TopicSpec` itself stays: it is the internal topic description `TopicRegistry` and `createRuntimeUserReplicas` use), `libs/node/src/NodeConfig.cpp` (delete the block from anchor `// --- RUNE_TOPICS (optional) ---` to `// Absent or empty RUNE_TOPICS → cfg.topics stays empty (valid).` inclusive; F5: the env read is reduced to exactly one **presence refusal** — `if (getenv_fn("RUNE_TOPICS") != nullptr) return {{}, "RUNE_TOPICS was removed — topics are created by DDL (see docs/SELF-HOSTING.md#topics-are-created-by-ddl)"};` — nothing else reads, parses or mentions the variable in this file (2 mentions: the lookup and the message; Task 19's allowlist pins that count). A set variable is an explicit error, never silently ignored)
- Modify: `libs/node/src/BrokerNode.cpp` (`refuseStaticNamesCollidingWithRecovered` call gone with Task 16's step 1b; grep that no `RUNE_TOPICS` / `config_.topics` remains)
- Modify: `libs/node/include/rune/node/TopicName.h`, `AdminChannels.h`, `BrokerNode.h`, `libs/gateway/include/rune/gateway/TopicRegistry.h`, `libs/node/src/AdminWorker.cpp` (comment mentions — F5: `AdminWorker.cpp`'s "by name — a RUNE_TOPICS boot topic, a recovered runtime topic" becomes "by name — an untyped topic created by DDL")
- Modify (F6): `libs/node/include/rune/node/FdBudget.h`, `libs/node/src/FdBudget.cpp` — `applyFdLimitAtStartup(const NodeConfig&, std::span<const TopicSpec> topics, std::string& error)`: the estimate (anchor `estimateFdNeed(cfg.topics,`) uses the topics the catalog boot replay produced, not the deleted `NodeConfig::topics`; `estimateFdNeed` takes `std::span<const TopicSpec>`. `src/main.cpp`: the FD-1 block (anchor `if (!rune::node::applyFdLimitAtStartup(*cfg, fdErr)) {`) is deleted; `BrokerNode::start` calls `applyFdLimitAtStartup(config_, bootTopics_, err)` right after `bootFromCatalog_()` and throws `std::runtime_error(err)` on a refusal (main's existing `catch` logs it and exits non-zero) — the catalog's topics are counted at boot, which the static list never did for runtime topics. `BrokerNode.h` gains `fdNeedAtBootForTest()`.
- Create: `libs/node/tests/support/TestCluster.h`
- Migrate: `libs/node/tests/node/BrokerNodeTest.cpp`, `BrokerNodeGatewayTest.cpp`, `NodeConfigTest.cpp` (delete every `RUNE_TOPICS` parse case — 58 references — and add `NodeConfigRegression.RuneTopicsIsRefusedWithAPointerToDdl` **in this file**, F5), `DdlHarnessRegressionTest.cpp`, `libs/node/tests/support/DdlHarness.h` (the `statics` option is gone since Task 10 — remove its last comment), `benchmark/FusedRuntimePassBench.cpp`, `libs/node/tests/node/FdBudgetTest.cpp` (the `estimateFdNeed` calls take a span — F6)
- Test: `libs/node/tests/node/TestClusterTest.cpp`, `libs/node/tests/node/NodeConfigTest.cpp` (the refusal Regression test)

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
TEST(FdBudgetRegression, BootBudgetCountsTheCatalogTopicsRegression) {
    // F6: the FD estimate came from NodeConfig::topics (deleted); it now comes from the catalog's boot topics.
    SingleNodeForTest n("tc_fd");
    rune::node::test::TestCluster c({n.node()});
    ASSERT_EQ(c.defineTopic("a", 4), 0);
    ASSERT_EQ(c.defineTopic("b", 2), 1);
    n.restart();
    const std::vector<rune::node::TopicSpec> specs{n.topicSpecOnControl(0), n.topicSpecOnControl(1)};
    EXPECT_EQ(n.node()->fdNeedAtBootForTest(),
              rune::node::estimateFdNeed(specs, static_cast<uint64_t>(n.config().segmentProvisionDepth)));
    EXPECT_GT(n.node()->fdNeedAtBootForTest(), rune::node::estimateFdNeed({}, 0)) << "the catalog's partitions are counted";
}
```

and in `NodeConfigTest.cpp` (namespace `rune::node`, its existing `fakeEnv` / `base1node` helpers and `parseNodeConfig` result shape — F5: the refusal test lives here, the one test file the grep gate allows):

```cpp
TEST(NodeConfigRegression, RuneTopicsIsRefusedWithAPointerToDdl) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "events:1";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
    EXPECT_NE(r.error.find("topics are created by DDL"), std::string::npos) << r.error;
}
```

- [ ] **Step 2: Run to verify they fail**, **Step 3: implement** + migrate the listed files, **Step 4: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'TestCluster|NodeConfig|BrokerNode|DdlHarness|FdBudget' --output-on-failure -j14`
Expected: PASS. `cmake --build ~/Projects/se-worktrees/rune-broker/build-bench-rel -j14 --target rune_broker_benchmark` builds.

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add -A libs benchmark src
git -C ~/Projects/se-worktrees/rune-broker commit -m "feat(catalog)!: remove RUNE_TOPICS, NodeConfig::topics, rf= and the static/runtime collision rule; FD budget from the catalog; TestCluster::defineTopic

Topics exist only through DDL. A set RUNE_TOPICS refuses to load with a pointer to the DDL docs.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 18: Broker — migrate the in-process integration tests to `TestCluster::defineTopic`

Mechanical, by the Task 17 rule; split from it so each commit stays reviewable. Each file keeps its scenario and assertions; only how its topics come to exist changes.

**Files (migrate):** `libs/node/tests/integration/{BootstrapRoutingImageTest, CompactionRetentionSmokeTest, ConsensusCandidacyElectionTest, ControlledShutdownRegressionTest, D9CrossNodeMarkerTest, DdlConformanceBrokerRegressionTest, EpochRestartRegressionTest, GatewayAcksAllDeferredAckTest, GatewayFailoverReplicationTest, GatewayProduceReplicationTest, GatewayPsmFailoverTest, GatewayReplicationWiringTest, GatewaySubscribeHwBoundTest, MultiLeaderProduceTest, PerWorkerReplicationTest, RebalanceFailoverIntegrationTest, ShardModeTest, ShardNonBlockingTest, StaticAssignmentEmitTest, TransientTopicDataPathTest, WorkerPoolParityTest}.cpp`, plus `tests/integration/ClusterHarness.h` if it builds topics.

**Files (comment mentions, F5):** `libs/node/tests/integration/CreateViewRegressionTest.cpp` (anchor `// a RUNE_TOPICS boot topic: no schema`) and `libs/node/tests/integration/SchemaGatewayTest.cpp` (anchor `RUNE_TOPICS boot topic with no schema`) — each rewritten to "an untyped topic created by DDL"; without this the Task 19 gate fails on them.

Two files change meaning, not just mechanics:
- `StaticAssignmentEmitTest.cpp` / `BootstrapRoutingImageTest.cpp` tested "boot topics are assigned at boot". Rename the suites `CatalogAssignmentEmitTest` / `CatalogBootstrapRoutingImageTest`: the topics are defined by DDL, the cluster is restarted, and the assertions (assignment emitted once, routing image complete) now hold for **catalog-recovered** topics.
- `TransientTopicDataPathTest.cpp` has static-transient cases (`RUNE_TOPICS … persistence=transient`); they become `defineTopic(spec with persistence Transient)` — their "restart → exists and empty" assertions are unchanged (the topic now comes back from the catalog).

- [ ] **Step 1: Migrate** every file above by the rule; delete each `cfg.topics` line; build.
- [ ] **Step 2: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'Compaction|Consensus|ControlledShutdown|D9|DdlConformanceBroker|EpochRestart|Gateway|MultiLeader|PerWorkerReplication|Rebalance|ShardMode|ShardNonBlocking|CatalogAssignmentEmit|CatalogBootstrapRoutingImage|TransientTopicDataPath|WorkerPoolParity|ClusterDisasterRecovery|CreateView|SchemaGateway' --output-on-failure -j14`
Expected: PASS — the same scenarios as before the migration.
- [ ] **Step 3: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add -A libs tests
git -C ~/Projects/se-worktrees/rune-broker commit -m "test(catalog): integration tests create their topics by DDL (TestCluster::defineTopic)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 19: Scripts and docs off `RUNE_TOPICS` (docker, dr-e2e, kafka-compare) + the grep gate

Spec §8: docker e2e scripts create their topics through the CLI or the probe before producing; every existing `RUNE_TOPICS` use is migrated, and none remain (grep gate); SELF-HOSTING.md loses every mention (its new section lands in Task 25).

**Files:**
- Create: `docker/topics/e2e.rql` (`topic events partitions 1;` `topic multi partitions 3;` `topic cg partitions 4;` — the ids 0, 1, 2 the e2e client uses), `docker/topics/transient-failover.rql` (+ `transient topic presence_failover partitions 1;` as the 4th, id 3), `docker/create-topics.sh` (runs the rune-ql-probe image's `rune ql -f /topics/<file>.rql --request-id <fixed per file> --broker <seed>` on the compose network — `-f` runs every statement in order and stops at the first error naming it (F8), and the fixed request id makes a retried run after a partial one return the original results (F17); bounded retries on `NotLeader` / connection refused while the cluster forms, then an explicit failure)
- Modify: `docker/docker-compose.yml`, `docker/docker-compose.transient-failover.yml`, `docker/docker-compose.single.yml` (delete `RUNE_TOPICS` and its comment), `docker/e2e.sh`, `docker/transient-failover-e2e.sh` (call `create-topics.sh` after the leader is up, before any producer; `TOPIC_ID=3` comment rewritten)
- Modify: `dr-e2e/harness/cluster.py`, `dr-e2e/harness/driver.py`, `dr-e2e/harness/dockerutil.py`, `dr-e2e/tests/{test_cg_atleastonce,test_cg_failover,test_cg_rebalance,test_compaction_e2e,test_dr2_multiclient,test_multi_leader_smoke,test_per_worker_ingress}.py` (the harness gains `create_topics(spec_list)`: builds a `.rql` from the old `name:partitions[:options]` strings — options mapped to the `with (…)` clause / `replicas` / `transient` — and runs it through the CLI container; every former `RUNE_TOPICS` env becomes a `create_topics` call)
- Modify: `bench/kafka-compare/{cluster.sh,lib.sh,run-rune.sh,replbench.sh,README.md}` (native brokers: after start, `"$RUNE_CLI" ql -e "topic bench partitions $P with (tier = leader);" --broker …`; `RUNE_CLI` defaults to `../rune-client-cpp/build/rune`; the README's D0 row reads `topic bench … with (tier = leader)`)
- Modify: `docs/release-notes/phase2b-c-typed-topic-routing.md`, `docs/release-notes/phase2b-e-transient-topics.md`, `docs/SELF-HOSTING.md`, `docs/superpowers/2026-09-14-rune-client-milestone-b-acceptance.md` (replace each `RUNE_TOPICS` example with the DDL equivalent; release notes keep their history in prose: "topics were then declared with an environment variable, since removed — see SELF-HOSTING")
- Create: `scripts/check_no_rune_topics.py`, `scripts/rune_topics_allowlist.txt` (F5: the explicit allowlist the gate reads); modify root `CMakeLists.txt` (`add_test(NAME NoRuneTopicsGateRegression.RepositoryHasNoRuneTopicsMention COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/check_no_rune_topics.py ${CMAKE_SOURCE_DIR})`)
- Fix-on-sight (F5): `build-release/` is tracked (`git ls-files build-release` lists `Makefile`s, `librune_node.a`, `rune_broker`, …) and its binaries contain the string. Untrack it — `git -C ~/Projects/se-worktrees/rune-broker rm -r -q --cached build-release` (the files stay on disk) — and add `/build-*/` to `.gitignore` next to `/build/` so no build dir is ever tracked again. The gate's binary check below is its regression.

`scripts/rune_topics_allowlist.txt`:

```
# The ONLY files that may mention RUNE_TOPICS (catalog history F5), each with its exact
# maximum number of mentions.  Anything else — or more mentions — fails
# NoRuneTopicsGateRegression.RepositoryHasNoRuneTopicsMention.
# <path>                                   <max>  # why
libs/node/src/NodeConfig.cpp               2      # the presence refusal (Task 17): the lookup + its message
libs/node/tests/node/NodeConfigTest.cpp    1      # NodeConfigRegression.RuneTopicsIsRefusedWithAPointerToDdl
docs/release-notes/catalog-history.md      2      # the release note that names the removal (Task 25)
```

`scripts/check_no_rune_topics.py`:

```python
#!/usr/bin/env python3
"""Grep gate (catalog history spec §8): no RUNE_TOPICS mention may remain.

Scans every tracked file except historical design records (docs/superpowers/**,
which describe what existed when they were written) and the gate's own two files.
The only other mentions allowed are listed — path and maximum count — in
scripts/rune_topics_allowlist.txt (F5): the config loader's presence refusal, its
Regression test, and the release note naming the removal.  A tracked binary that
contains the string (e.g. committed build output) is a hit too.  Exit 1 listing
every hit; exit 0 when clean.
"""
import subprocess, sys, pathlib

root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
files = subprocess.run(["git", "-C", str(root), "ls-files"], capture_output=True, text=True, check=True).stdout.split()
allow = {}
for raw in (root / "scripts" / "rune_topics_allowlist.txt").read_text().splitlines():
    entry = raw.split("#", 1)[0].split()
    if entry:
        allow[entry[0]] = int(entry[1])
own = {"scripts/check_no_rune_topics.py", "scripts/rune_topics_allowlist.txt"}
hits = []
for f in files:
    if f.startswith("docs/superpowers/") or f in own:
        continue
    try:
        data = (root / f).read_bytes()
    except (IsADirectoryError, FileNotFoundError):
        continue
    if b"\0" in data[:8192]:
        if b"RUNE_TOPICS" in data:
            hits.append(f"{f}: tracked binary contains RUNE_TOPICS (build output must not be tracked)")
        continue
    lines = [(n, l) for n, l in enumerate(data.decode(errors="ignore").splitlines(), 1) if "RUNE_TOPICS" in l]
    mentions = sum(l.count("RUNE_TOPICS") for _, l in lines)
    if mentions == 0 or mentions <= allow.get(f, 0):
        continue
    hits += [f"{f}:{n}: {l.strip()}" for n, l in lines]
    if f in allow:
        hits.append(f"{f}: {mentions} mentions > allowlisted {allow[f]}")
if hits:
    print("RUNE_TOPICS must not appear (topics are created by DDL):")
    print("\n".join(hits))
    sys.exit(1)
print("ok: no RUNE_TOPICS mention outside scripts/rune_topics_allowlist.txt")
```

(The refusal check and its test are Task 17's; the release note is Task 25's, so its entry is allowed before the file exists. `SELF-HOSTING.md` is **not** allowlisted: spec §8 removes every mention there.)

- [ ] **Step 1: Run the gate to see it fail**

Run: `python3 ~/Projects/se-worktrees/rune-broker/scripts/check_no_rune_topics.py ~/Projects/se-worktrees/rune-broker`
Expected: exit 1 listing the docker / dr-e2e / bench / docs hits and the tracked `build-release/` binaries.

- [ ] **Step 2: Migrate** every listed file; untrack `build-release/` and ignore `/build-*/` (F5).

- [ ] **Step 3: Run the gate + the scripts**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R NoRuneTopicsGate --output-on-failure`
Expected: PASS (`ok: no RUNE_TOPICS mention outside scripts/rune_topics_allowlist.txt`).
Run: `bash ~/Projects/se-worktrees/rune-broker/docker/e2e.sh` and `bash ~/Projects/se-worktrees/rune-broker/docker/transient-failover-e2e.sh`
Expected: both end with their existing success line (topics now created by `create-topics.sh`).
Run: `cd ~/Projects/se-worktrees/rune-broker/dr-e2e && python3 -m pytest tests/test_multi_leader_smoke.py -x`
Expected: PASS (the smoke the others share the harness with; the full dr-e2e matrix runs at Checkpoint F).

- [ ] **Step 4: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add -A docker dr-e2e bench docs scripts CMakeLists.txt .gitignore
git -C ~/Projects/se-worktrees/rune-broker commit -m "chore(catalog): docker, dr-e2e and kafka-compare create topics by DDL; RUNE_TOPICS grep gate (explicit allowlist); untrack build-release

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
  struct ExpectedCodes { int first, afterRedirect, final, other, viaLeader, retried; };   // -1 = no such answer
  [[nodiscard]] ExpectedCodes expectedCodes(Kind, Entity, Timing) noexcept;   // F15/F16: every code a cell asserts
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

/// Every answer code a cell asserts (F15/F16) — the ONLY place a matrix code is written down.
///   first          the first answer to the cell's DDL
///   afterRedirect  the first answer after following a Redirect (-1: the first answer was not a Redirect)
///   final          the client-visible terminal result of the cell's DDL
///   other          R5: the concurrent DDL's terminal result
///   viaLeader      R2: the same op sent via the leader
///   retried        R3: the op retried after one broker returned
/// A DDL sent via a follower answers Redirect (3) first in every rule except R2 (broker 3 is halted
/// and answers Internal, never a redirect).  The DDL sent to the leader is applied first (R5).
struct ExpectedCodes { int first, afterRedirect, final, other, viaLeader, retried; };
[[nodiscard]] constexpr ExpectedCodes expectedCodes(Kind k, Entity e, Timing t) noexcept {
    const bool viaF = e.via == Via::Follower;   // a follower answers Redirect (3) first
    auto viaLeaderThen = [&](int leaderAnswer, int final) {
        return viaF ? ExpectedCodes{3, leaderAnswer, final, -1, -1, -1} : ExpectedCodes{leaderAnswer, -1, final, -1, -1, -1};
    };
    switch (expect(k, e, t)) {
    case Outcome::NotApplicable:                return {-1, -1, -1, -1, -1, -1};
    case Outcome::HaltedBrokerInternalOthersOk: return {6, -1, 6, -1, 0, -1};                       // R2
    case Outcome::NotLeaderUntilBudgetThenOk: {                                                    // R3
        ExpectedCodes c = viaLeaderThen(2, 2); c.retried = 0; return c;
    }
    case Outcome::NotLeaderThenOkOnce:          return viaLeaderThen(2, 0);                         // R4
    case Outcome::OneWinsOneConflict: {                                                            // R5
        const int conflict = conflictCode(k);
        return viaF ? ExpectedCodes{3, conflict, conflict, 0, -1, -1} : ExpectedCodes{0, -1, 0, conflict, -1, -1};
    }
    case Outcome::OkNewObjectId: case Outcome::OkAfterPlacement:                                   // R6 / R7
    case Outcome::RedirectThenOk: case Outcome::Ok:                                                // R8 / R9
        return viaLeaderThen(0, 0);
    }
    return {-1, -1, -1, -1, -1, -1};
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

TEST(CatalogMatrixCountRegression, ExpectedCodesAgreeWithTheOutcomeOfEveryCell) {
    // F15/F16: the code table and the outcome table cannot drift apart.
    for (int k = 0; k < cm::kKinds; ++k)
        for (int v = 0; v < 2; ++v)
            for (int p = 0; p < 2; ++p)
                for (int t = 0; t < cm::kTimings; ++t) {
                    const cm::Kind kind = static_cast<cm::Kind>(k);
                    const cm::Entity e{static_cast<cm::Via>(v), static_cast<cm::Persist>(p)};
                    const cm::Timing tm = static_cast<cm::Timing>(t);
                    const auto o = cm::expect(kind, e, tm);
                    const auto c = cm::expectedCodes(kind, e, tm);
                    if (o == cm::Outcome::NotApplicable) { EXPECT_EQ(c.first, -1); continue; }
                    if (o == cm::Outcome::HaltedBrokerInternalOthersOk) { EXPECT_EQ(c.first, 6); EXPECT_EQ(c.viaLeader, 0); continue; }
                    EXPECT_EQ(c.first == 3, e.via == cm::Via::Follower) << "a follower redirects first, a leader never";
                    EXPECT_EQ(c.afterRedirect != -1, e.via == cm::Via::Follower);
                    if (o == cm::Outcome::NotLeaderUntilBudgetThenOk) { EXPECT_EQ(c.final, 2); EXPECT_EQ(c.retried, 0); }
                    else if (o == cm::Outcome::OneWinsOneConflict) EXPECT_EQ((c.final == 0) + (c.other == 0), 1);
                    else EXPECT_EQ(c.final, 0);
                }
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

    // 1. every answer code, all from cmatrix::expectedCodes (F15/F16 — no code is hard-coded here)
    const cm::ExpectedCodes ec = cm::expectedCodes(c.kind, c.entity, c.timing);
    EXPECT_EQ(run.first.code, ec.first);
    if (ec.first == 3) EXPECT_EQ(run.first.message, h.leaderClientEndpoint());
    EXPECT_EQ(run.afterRedirectCode, ec.afterRedirect);
    EXPECT_EQ(run.finalCode, ec.final);
    EXPECT_EQ(run.otherFinalCode, ec.other);
    EXPECT_EQ(run.viaLeaderCode, ec.viaLeader);
    EXPECT_EQ(run.retriedCode, ec.retried);
    // 1b. what each outcome asserts besides codes
    switch (want) {
    case cm::Outcome::NotLeaderUntilBudgetThenOk: EXPECT_TRUE(run.produceOnExistingTopicSucceeded); break;
    case cm::Outcome::OneWinsOneConflict:         EXPECT_EQ(run.okCount, 1); break;
    case cm::Outcome::OkNewObjectId:              EXPECT_NE(run.objectIdAfter, run.objectIdBefore);
                                                  EXPECT_EQ(run.newVersionAfter, run.firstVersionOfNewObject); break;
    case cm::Outcome::OkAfterPlacement:           EXPECT_EQ(run.produceInWindowCode, 2);
                                                  EXPECT_TRUE(run.okReleasedOnlyAfterServing); break;
    case cm::Outcome::HaltedBrokerInternalOthersOk:
        EXPECT_TRUE(h.applierHalted(3)); EXPECT_EQ(h.haltCounter(3), 1);
        EXPECT_TRUE(run.haltedBrokerStillServesAppliedTopics); break;
    default: break;
    }
    // 2. every DDL answered exactly once
    for (int b = 1; b <= 3; ++b) EXPECT_EQ(h.admin(b).startedCount(), h.admin(b).answeredCount()) << "broker " << b;
    EXPECT_TRUE(run.everyRequestHadOneTerminalResult);
    // 3. identical catalogs on every running non-halted broker
    EXPECT_TRUE(h.awaitDigestsEqual(/*excludeHalted=*/true));
    // 4. no DDL lost or duplicated
    for (const rune::protocol::RequestId& rid : run.okRequestIds)      EXPECT_EQ(h.countEventsWithRequestId(rid), 1u);
    for (const rune::protocol::RequestId& rid : run.refusedRequestIds) EXPECT_EQ(h.countEventsWithRequestId(rid), 0u);
    // 5. history shows the op
    EXPECT_TRUE(h.historyShows(c.kind, run.objectIdAfter, run.statement, run.okRequestIds));
    // 6. serving iff applied + placed
    EXPECT_TRUE(h.servingMatchesAppliedAndPlaced());
}

INSTANTIATE_TEST_SUITE_P(AllCells, CatalogInteractionMatrixRegression, testing::ValuesIn(allCells()), cellName);
```

- [ ] **Step 3: Implement `CatalogClusterHarness.h`** — `runCell` drives each timing exactly as the plan's timing table describes, using: `FilteringTransport::isolate/deisolate` (minIsr, catch-up), `holdHighWatermark(b)` (stop the followers' fetches so an append stays uncommitted), `moveCatalogLeadership(to)` (commits a new `(−6,0)` assignment through the scripted controller), `stopBroker(b)` / `restartBroker(b)` (rebuilds the rig on its dirs: checkpoint + log survive), `startFreshBroker(3)`, `applier(3).tamperNextFingerprintForTest()`, `placeAfter(b, topicId, delayPasses)` (delays the placement commit for `CreateRacesPlacement`), and the client model's `send(via, request)` (follows `Redirect`, retries `NotLeader` with the budget in deterministic passes; 128-bit `RequestId`s as Task 5's client). `runCell` records, per the `ExpectedCodes` fields, `first`, `afterRedirectCode`, `finalCode`, `otherFinalCode`, `viaLeaderCode`, `retriedCode` (−1 where the timing has no such answer). `CatalogBelowMinIsr` with `Via = Follower` sends to a follower first, takes its `Redirect`, then stops both followers before the followed request reaches the leader (the timing table). Every wait is bounded by a pass cap (`ASSERT`-fails with the cell name on overrun — never hangs).

- [ ] **Step 4: Run**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_catalog_matrix_tests && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogMatrixCount|CatalogInteractionMatrix' --output-on-failure -j14`
Expected: 2 count pins PASS; 300 cells PASS, 24 SKIPPED (R1).

- [ ] **Step 5: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs
git -C ~/Projects/se-worktrees/rune-broker commit -m "test(catalog): interaction matrix — expectation table + 3-broker harness + 324-cell parameterized unit matrix

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
    std::map<std::string, std::set<rune::protocol::RequestId>> okRequestsByName;
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
            } else if (a.code == 6) {
                // F30: Internal only from a broker the model knows is halted (a tampered fingerprint) —
                // never from a healthy broker (e.g. a commit timeout must be the retriable NotLeader, F13).
                ASSERT_TRUE(h.applierHalted(a.broker))
                    << "seed " << GetParam() << " step " << step << ": Internal (6) from healthy broker " << a.broker;
            } else {
                ASSERT_TRUE(a.code == 2 || a.code == 3)
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
        for (const auto& rid : rids) EXPECT_EQ(h.countEventsWithRequestId(rid), 1u) << name;
    // Invariant: two creates on one name never both succeed (per live object generation).
    EXPECT_TRUE(h.noNameHadTwoConcurrentLiveObjects());
    // The broker catalog equals the independent model.
    EXPECT_EQ(h.catalogAsModelSnapshot(), model.snapshot());
}

INSTANTIATE_TEST_SUITE_P(Seeds, CatalogStressRegression, testing::Range(1u, 65u));
```

- [ ] **Step 2: Implement `CatalogModel.h`** and the harness additions (`randomAction`, `perform` — each answer carries the `broker` that sent it, F30 — `digestsEqualAtEqualOffsets`, `noNameHadTwoConcurrentLiveObjects` — checks the log: between a create and the drop of the same name, no other create of that name; `catalogAsModelSnapshot` — the leader's `CatalogState` mapped to the model's shapes).

- [ ] **Step 3: Run**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_catalog_stress_tests && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogStress' --output-on-failure -j14`
Expected: 64 seeds PASS. A failure prints its seed + step: rerun that seed alone (`--gtest_filter='*Seeds/<n>'`), fix the bug, add a named `*Regression` unit test for it in the owning task's test file.

- [ ] **Step 4: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs
git -C ~/Projects/se-worktrees/rune-broker commit -m "test(catalog): seeded model-checked catalog stress (64 seeds x 400 steps, independent model)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 22: Interaction matrix — 3-node BrokerNode integration slice over real Aeron (closes the E2 carry-over)

Spec §10: "BrokerNode integration, 3 nodes: runtime `CreateTopic` and `CreateView` through every broker, with a leader kill mid-DDL. This closes the E2 carry-over."

**Files:**
- Create: `libs/node/tests/integration/CatalogClusterIntegrationTest.cpp`; modify `libs/node/CMakeLists.txt` (`rune_catalog_integration_tests`; `ClientDdlClusterRegressionTest.cpp` joins `rune_client_broker_tests`)
- Create: `libs/node/tests/support/ThreeNodes.h` — Task 6's `ThreeNodes` helper, moved verbatim out of the file below
- Modify: `libs/node/tests/integration/CatalogReplicationTest.cpp` (F34: its file-local `ThreeNodes` is deleted and the file includes `support/ThreeNodes.h`; its five tests are unchanged)
- Modify: `libs/node/tests/support/ShardTestClient.h` (the new wire DDL sends, below)
- Create: `libs/node/tests/client/ClientDdlClusterRegressionTest.cpp` (F7: the real client's blocking `*AndWait` through a follower, across a leader kill and below minIsr — on three real `BrokerNode`s)

**Interfaces:**
- Consumes: `ShardTestClient` (wire DDL: `sendDefineTopic`, `awaitDefine`, plus new `sendDropTopic`, `sendAlterTopic`, `sendCreateView`, `awaitDdl` added here, following its existing `sendDefineTopic` pattern and carrying the DDL extension with a 128-bit `RequestId`), `cmatrix::{expect, expectedCodes}` (F16), `ThreeNodes` (`support/ThreeNodes.h`).

Cells: `{CreateTopic, CreateView, DropTopic, AlterSchema}` × `{Steady via node 1, Steady via node 2, Steady via node 3, LeaderChangeMidDdl (the catalog leader is stopped right after the DDL frame is sent), RestartMidApply (a follower is stopped and restarted while the DDL commits), NewBrokerJoins (node 3 first started after the DDL), DropAndReuseName, CreateRacesPlacement}` = **32** cells, each followed by: produce acks=all + subscribe from earliest **through every running broker's client endpoint** to the topic (or the view's source, then `GetViewRow`) and assert delivery — the E2 carry-over's failure mode.

- [ ] **Step 1: Write the slice** (parameterized over `(Kind, Timing)`; every expected code from `cmatrix::expectedCodes` with `Via` = whether the broker the DDL went to was the catalog leader at send time and `Persist::Disk` — F16, no hard-coded outcome):

```cpp
TEST_P(CatalogClusterSliceRegression, RuntimeDdlThroughEveryBrokerThenDataThroughEveryBroker) {
    const auto [kind, timing, viaNode] = GetParam();
    if (cm::naRule(kind, {cm::Via::Leader, cm::Persist::Disk}, timing) != 0) GTEST_SKIP() << "R1";   // e.g. DropTopic x CreateRacesPlacement
    ThreeNodes c = ThreeNodes::start(sliceDirName(GetParam()), timing == cm::Timing::NewBrokerJoins
                                     ? std::vector<int>{1, 2} : std::vector<int>{1, 2, 3});
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    c.buildPrecondition(kind);                                     // via DDL, through the leader
    const auto res = c.runSliceCell(kind, timing, viaNode);        // the DDL + the timing, real nodes
    const cm::ExpectedCodes ec = cm::expectedCodes(kind, {res.viaWasLeader ? cm::Via::Leader : cm::Via::Follower,
                                                          cm::Persist::Disk}, timing);
    EXPECT_EQ(res.firstCode, ec.first) << res.trace;
    EXPECT_EQ(res.afterRedirectCode, ec.afterRedirect) << res.trace;
    EXPECT_EQ(res.finalCode, ec.final) << res.trace;
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

and, in `ClientDdlClusterRegressionTest.cpp` (`rune_client_broker_tests`, F7 — the blocking client API across real brokers, which neither the fake nor one node can exercise):

```cpp
TEST(ClientDdlClusterRegression, AndWaitThroughAFollowerFollowsTheRedirect) {
    ThreeNodes c = ThreeNodes::start("cc_redirect");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    auto client = c.connectClient(c.aFollowerOfTheCatalog());
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 20s);
    EXPECT_EQ(r.errorCode, 0) << r.detail;
    EXPECT_TRUE(c.awaitCatalogDigestsEqual(30s));
}

TEST(ClientDdlClusterRegression, LeaderKilledMidDdlTheRetryGetsTheOriginalResultOnce) {
    ThreeNodes c = ThreeNodes::start("cc_kill");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    auto client = c.connectClient(c.catalogLeaderId());
    rune::client::DdlOptions o; o.requestId = {.hi = 0xC11E, .lo = 77};
    c.killCatalogLeaderAfterNextAppend();                          // test seam: stop the leader right after its append
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 30s);
    EXPECT_EQ(r.errorCode, 0) << "NotLeader retried within the budget, deduplicated on the new leader: " << r.detail;
    EXPECT_EQ(c.countEventsWithRequestId(o.requestId), 1u);
}

TEST(ClientDdlClusterRegression, BelowMinIsrAndWaitFailsExplicitlyWithNotLeader) {
    ThreeNodes c = ThreeNodes::start("cc_minisr");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.catalogLeaderId();
    rune::client::ClientConfig cfg = c.clientConfigFor(leader);
    cfg.ddlRetryBudget = std::chrono::milliseconds(1500);
    auto client = c.connectClient(cfg);
    for (int n : c.runningNodes()) if (n != leader) c.stop(n);
    const auto t0 = std::chrono::steady_clock::now();
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 30s);
    EXPECT_EQ(r.errorCode, 2) << "an explicit NotLeader failure once the budget is spent (spec §5.5)";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 10s) << "the budget, not the 30 s op timeout, ends it";
}
```

(`ThreeNodes` gains, here: `connectClient(nodeId | ClientConfig)`, `clientConfigFor(nodeId)`, `aFollowerOfTheCatalog()`, `killCatalogLeaderAfterNextAppend()` (an admin-thread seam on the leader's `BrokerNode`), `countEventsWithRequestId(RequestId)`.)

- [ ] **Step 2: Run**

Run: `cmake ~/Projects/se-worktrees/rune-broker/build-fast && cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_catalog_integration_tests rune_catalog_replication_tests rune_client_broker_tests && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogCluster|CatalogReplication|ClientDdlCluster' --output-on-failure -j4`
Expected: PASS (`-j4`: each cell runs three brokers).

- [ ] **Step 3: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add libs
git -C ~/Projects/se-worktrees/rune-broker commit -m "test(catalog): 3-node BrokerNode slice — runtime DDL through every broker, leader kill mid-DDL (closes E2 multi-node DDL carry-over)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 23: Gates — boot replay of 10 000 events in < 1 s; hot-path benchmarks unchanged; zero allocations in steady state; every DDL answered

Spec §10 "Gates".

**Files:**
- Create: `benchmark/CatalogBootReplayBench.cpp` (in `rune_broker_benchmark`: builds a `__catalog` log of 10 000 events — 2 000 topics created, 2 000 schema alters, 2 000 config alters, 1 000 views created + altered, 1 000 drops — once, then times `BrokerNode` boot step `bootFromCatalog_()` alone via a `BrokerNode::bootFromCatalogForBench(dataDir)` static seam)
- Create: `libs/node/tests/node/CatalogGatesRegressionTest.cpp` (in `rune_node_tests`: the boot-replay time gate)
- Modify: `libs/node/tests/node/CatalogAllocRegressionTest.cpp` (append the idle-admin-pass gate — F12: the allocation gates live in their own binary with the per-binary `operator new` replacement; there is no `AllocCounter` type)
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

```

and, appended to `CatalogAllocRegressionTest.cpp` (own binary, plain `gArmed` / `gArmedAllocs` globals — F12; add `#include "support/CatalogDdlRig.h"` there):

```cpp
TEST(CatalogAllocRegression, IdleCatalogAdminPassAllocatesNothingRegression) {
    rune::node::test::CatalogDdlRig rig("gate_alloc", 3);
    rig.becomeServingLeader();
    for (int i = 0; i < 50; ++i) ASSERT_EQ(rig.define({.name = "t" + std::to_string(i), .partitions = 1}).code, 0);
    rig.subscribeCatalog(/*session=*/9);                                // an idle catalog reader too
    rig.pumpUntilIdle();
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 10'000; ++i) (void)rig.admin().doWork(rig.nowNs());
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}
```

**The hot-path zero-allocation gate is the existing one, unchanged (F12 — real names, nothing invented):** `HotPathBRegression.SteadyStateDutyPassesNeverAllocateRegression` (binary `rune_hotpath_b_alloc_regression_tests`, `struct Rig` from `libs/node/tests/integration/HotPathBRig.h`) and `TransientDataPathRegression.SteadyStateProduceEvictAndPushReadNeverAllocateRegression` (`rune_transient_alloc_regression_tests`, `TransientRig.h`). Their `Rig` drives a `Worker` directly with a replica it builds itself — exactly what a catalog-created topic is on a worker (`createRuntimeUserReplicas` hands the worker the same replica), so the catalog cannot add an allocation on that path that these tests would miss; this task re-runs them as the gate (Step 2) and proves the produce/consume medians unchanged (Step 3).

- [ ] **Step 2: Run the tests**

Run: `cmake --build ~/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R 'CatalogGates|CatalogAlloc|SteadyStateDutyPassesNeverAllocate|SteadyStateProduceEvictAndPushReadNeverAllocate|RingRebuildWithMoreProducersThanTheMarkerSetNeverAllocates|ZeroAlloc' --output-on-failure -j14`
Expected: PASS.

- [ ] **Step 3: Benchmarks** — boot replay (Release gate) and hot-path parity against the base commit:

```bash
cmake ~/Projects/se-worktrees/rune-broker/build-bench-rel && cmake --build ~/Projects/se-worktrees/rune-broker/build-bench-rel -j14 --target rune_broker_benchmark
~/Projects/se-worktrees/rune-broker/build-bench-rel/rune_broker_benchmark --benchmark_filter='CatalogBootReplay' --benchmark_repetitions=5
# hot-path parity: the same filter on the base (7d86db94, built in a scratch worktree) and on this branch
git -C ~/Projects/se-worktrees/rune-broker worktree add /tmp/catalog-base 7d86db94
cmake -S /tmp/catalog-base -B /tmp/catalog-base/build-bench-rel -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON -DBUILD_TESTING=OFF -DCMAKE_TOOLCHAIN_FILE=~/Projects/se-worktrees/rune-broker/build/Debug/generators/conan_toolchain.cmake -DRUNE_PROTOCOL_INCLUDE=~/Projects/se-worktrees/rune-protocol/include -DRUNE_CODEC_ROOT=~/Projects/se-worktrees/rune-codec && cmake --build /tmp/catalog-base/build-bench-rel -j14 --target rune_broker_benchmark
for b in /tmp/catalog-base/build-bench-rel ~/Projects/se-worktrees/rune-broker/build-bench-rel; do
  $b/rune_broker_benchmark --benchmark_filter='WorkerProduceLatency|ProducePath|PushRead|FusedRuntimePass' --benchmark_repetitions=5 --benchmark_report_aggregates_only=true
done
git -C ~/Projects/se-worktrees/rune-broker worktree remove /tmp/catalog-base
```

Expected: `CatalogBootReplay` median < 1 s; every hot-path benchmark's median within ±5 % of the base (noise band; the catalog adds nothing to these paths). A regression beyond 5 % is a bug to find and fix in this task (with a `*Regression` test), not a number to accept. Record all medians in the commit body. (`/tmp/catalog-base` is a throwaway worktree the step removes; it is built against the base protocol headers' superset, which compile unchanged.)

- [ ] **Step 4: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-broker add benchmark libs CMakeLists.txt
git -C ~/Projects/se-worktrees/rune-broker commit -F /tmp/catalog-gates-msg.txt
```

where `/tmp/catalog-gates-msg.txt` is written in Step 3 from the benchmark output: first line `test(catalog): gates — 10k-event boot replay < 1 s, idle catalog admin pass allocates nothing, hot-path alloc gates green, hot-path benches at parity`, a blank line, one line per benchmark `<name>: base <median> -> branch <median> (<delta %>)` plus `CatalogBootReplay: <median>`, a blank line, `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

---

### Task 24: Docker e2e, 3 nodes — DDL through each broker, leader killed mid-DDL, export → replay into a fresh cluster → diff

Spec §10 "Docker e2e, 3 nodes": 1. DDL through each broker; 2. the `__catalog` leader killed mid-DDL; 3. `rune catalog export`, then `replay` into a fresh cluster; 4. `rune catalog diff` reports no differences.

**Files:**
- Create: `docker/docker-compose.catalog.yml` (two 3-node clusters as two compose projects from one file with `${CLUSTER}`-prefixed service names and distinct host ports; `RUNE_CATALOG_REPL_STREAM_ID` default), `docker/catalog-e2e.sh`, `docker/topics/catalog-e2e.rql`
- Modify: `rune-client-cpp/examples/view_probe.cpp` (F33 — a listed change, the script needs it: add `--create-only`, which creates the view over the given source through `createViewAndWait` and exits 0 on Ok, 1 printing the code + detail otherwise, without reading rows; committed in rune-client-cpp)

`docker/catalog-e2e.sh` (structure; bounded waits, `trap` teardown, no fixed sleeps as gates — the house style of `docker/view-engine-e2e.sh`):
1. Build the broker image (`docker compose -f docker/docker-compose.catalog.yml build`, exit status checked directly) and the client `rune-ql-probe` image (rune CLI).
2. `CLUSTER=a` up; wait (bounded, 180 s) until each node's role counter shows a consensus leader **and** `rune ql -e "show history;"` succeeds against node 1.
3. **DDL through each broker:** `rune ql -f /topics/catalog-e2e.rql --broker a-rune-1:40200` (topic `players` typed, `events` untyped `replicas 3`, `presence` transient), then `rune ql -e "alter topic players add column mana int32;" --broker a-rune-2:40200`, then `rune ql -e "alter topic events set retention.ms = 600000;" --broker a-rune-3:40200`, a view through node 2 (`view_probe --create-only`). Each must exit 0 (a follower answers `Redirect`; the CLI follows it).
4. **Leader killed mid-DDL:** find the catalog leader by a bounded poll of `docker logs` for the last `AdminWorker: __catalog role -> LeaderServing (epoch N)` line (Task 11 logs it on every promotion; the highest epoch wins), start `rune ql -e "topic midkill partitions 2;" --request-id 00000000000000c1000000000000006d` against it in the background, `docker kill` the leader container 50 ms later, wait for the CLI: it must exit 0 (retried on the new leader, deduplicated) **or** exit 1 with `NotLeader` after its budget — then re-run the same statement **with the same `--request-id`** once: exit 0 (F17: if the killed attempt committed, the broker's `(requestId, requestFingerprint)` dedupe returns its original result instead of `TopicExists`; if it did not, the re-run creates it). Assert `show history topic midkill;` lists exactly one `CreateTopic`. Restart the killed node; wait until `rune catalog diff --from a-rune-1:40200 --to a-rune-<killed>:40200` exits 0 (all brokers converge).
5. Produce + consume through every broker on `events` (`rune_produce_subscribe` example, existing image) — data path after runtime DDL on 3 nodes.
6. **Export → replay → diff:** `rune catalog export --broker a-rune-1:40200 > /work/a.rcat`; `CLUSTER=b` up (fresh, empty); `rune catalog replay --from /work/a.rcat --into b-rune-1:40200 --cluster-id a`; `rune catalog diff --from a-rune-1:40200 --to b-rune-1:40200` → exit 0 and prints no differences. `rune ql -e "show history;" --broker b-rune-2:40200` shows every event with `replayedFrom a@<offset>` (the history row's `replayedFrom` column, Task 15 — F18).
7. Print `CATALOG-E2E: OK`; teardown both projects.

- [ ] **Step 1: Write the compose file, the `.rql` and the script.**
- [ ] **Step 2: Run**

Run: `bash ~/Projects/se-worktrees/rune-broker/docker/catalog-e2e.sh`
Expected: ends with `CATALOG-E2E: OK`, exit 0.
- [ ] **Step 3: Commit**

```bash
git -C ~/Projects/se-worktrees/rune-client-cpp add examples/view_probe.cpp
git -C ~/Projects/se-worktrees/rune-client-cpp commit -m "feat(examples): view_probe --create-only (catalog e2e)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C ~/Projects/se-worktrees/rune-broker add docker
git -C ~/Projects/se-worktrees/rune-broker commit -m "test(catalog): docker e2e — 3-node DDL via each broker, leader killed mid-DDL, export/replay/diff into a fresh cluster

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 25: Docs — "Topics are created by DDL", the catalog, halted-apply operations, release notes

**Files:**
- Modify: `docs/SELF-HOSTING.md` — new section **"Topics are created by DDL"** (no `RUNE_TOPICS`; `rune ql -e "topic events partitions 3 replicas 3;"`; the `with (…)` creation settings table mapping every former option; `replicationFactor` rules: `≤ broker count`, `≥ minIsr`, 0 = every broker; typed vs untyped topics; `alter topic … set` for the six alterable settings; `drop topic` / `drop view` rules), and **"The catalog (`__catalog`)"** (one partition on every broker, majority, never unclean / compacted / deleted; `RUNE_CATALOG_REPL_STREAM_ID`; restart and new-broker behaviour; `dataDir/__catalog/applied`; old `__topic_schemas` / `__views` dirs are ignored and may be deleted by hand).
- Create: `docs/CATALOG.md` — the event model (fields, kinds, `formatVer` rules), reads (`SHOW HISTORY` / `AS OF` / `DIFF`, client API), `rune catalog export|replay|diff` with examples, the error table of spec §9, and **"#halted"**: what a fingerprint-mismatch halt means, the metric (`catalog apply halted` counter type 1 000 217, applied-offset counter 1 000 218), and the operator procedure (stop the broker, move `dataDir/__catalog` aside, restart: it re-fetches the whole catalog from the leader and re-applies from offset 0; if the leader itself halted, `rune catalog export` from a healthy broker and compare with `rune catalog diff`).
- Create: `docs/release-notes/catalog-history.md` (breaking: `RUNE_TOPICS` removed — a set variable refuses to start; client version 8; new DDL ops and codes used; **pre-v8 clients** (F37): DDL sent to a broker that is not the catalog leader is answered `InvalidRequest` in its own response type ("client too old for cluster DDL routing; upgrade", logged by the broker) — upgrade clients before running DDL against a multi-node cluster; `rune ql --request-id` and `-f` scripts; no migration). It names the variable at most twice — the count `scripts/rune_topics_allowlist.txt` allows (F5).
- Modify: `rune-client-cpp/README.md` (the new API + CLI commands), `rune-ql/README.md` if present (the new statements).

- [ ] **Step 1: Write the docs.**
- [ ] **Step 2: Verify** every statement in them parses with the built CLI (`~/Projects/se-worktrees/rune-client-cpp/build/rune ql -e "<each statement>" --dry-run`), and rerun the grep gate: `ctest --test-dir ~/Projects/se-worktrees/rune-broker/build-fast -R NoRuneTopicsGate --output-on-failure` — it passes with the release note's mentions inside its allowlisted count; any other doc mention (e.g. in `SELF-HOSTING.md` or `CATALOG.md`) fails it.
- [ ] **Step 3: Commit** (broker docs; client/ql READMEs in their repos)

```bash
git -C ~/Projects/se-worktrees/rune-broker add docs
git -C ~/Projects/se-worktrees/rune-broker commit -m "docs(catalog): topics are created by DDL; __catalog; history/as-of/diff; replay; halted-apply runbook; release notes

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
- **Pre-flight rulings:** F1–F37 applied (see "Pre-flight rulings (2026-10-13)" above); spec §4 (`timestampMs` at append), §7 (Subscribe serving is transport) and the Repos line (rune-codec hosts `rune_catalog`) amended with dated notes.
- **Placeholder scan:** no "TBD"/"TODO"/"fill in"/"similar to Task N". Runtime data is produced by named steps, never invented: the golden digests (Task 3 Step 4, printed by `make_catalog_golden`, then frozen) and the benchmark medians (Task 23 Step 3, written into the commit message file). Mechanical migrations (Tasks 17–19) give the exact rule, the exact file list and a before/after.
- **Type consistency:** `rune::protocol::{TopicConfig, TopicConfigView, convertTopicConfig, RequestId, RequestIdHash}` (Task 1, `CatalogTypes.h` — F32/F14) by Tasks 2, 4, 5, 9, 14, 15, 20–22; `rune::catalog::{EventKind, CatalogEvent, Ops, CreateTopicOps…DropViewOps, ResolvedAlterOp, TopicConfig, RequestId, ConfigKey, ConfigChange, SchemaDef, ViewDef, Principal, ReplayedFrom, DecodeError, encode, encodeInto, decode, kindOf}` (Task 1) are used unchanged by Tasks 2, 3, 7, 9, 13, 15; `CatalogState`, `RequestRecord`, `Applied`, `ApplyError`, `apply`, `previewFingerprint`, `fingerprintOf`, `digest()`, `resolveAlterOps`, `unresolveAlterOps` (Task 2) by 3, 7, 9, 13, 15, 20, 21; `CatalogReplayer`, `RecordedEvent`, `history`, `stateAsOf`, `AsOf`, `HistoryFilter`, `diff`, `DiffEntry`, `ReplayError`, `describe`, `readRcat`/`writeRcat*`, `testing::goldenScriptV1` (Task 3) by 13, 15; protocol `DdlExtension`, `TopicConfigWire`, `ConfigChangeWire`, `DropTopic`/`AlterTopicConfig`/`DropView`/`AlterView`/`DdlResponse`, `kCatalogTopicId`, `kClientVersionCatalog` (Task 4) by 5, 9, 12, 13, 15; `kCatalogTopicId` (broker, Task 6) equals the protocol constant; `CatalogApplier` (+ `BootEffects` / `LiveEffects`, `goLive`, `lastApplied`), `CatalogEffects` (+ `SinkSpec`), `CatalogCheckpoint`, `CatalogRig` (Task 7) by 8, 9, 16, 20; the `kA2c*` ids 5–11 and `kMsg*` ids 10–16 are assigned once (Tasks 6, 7, 8, 9, 12) with no reuse; `CatalogRole` variants, `InFlight{Idle, AwaitingCommit}`, `DdlRequest`, `DdlMeta`, `answeredCount()` (Task 9) by 10, 11, 20, 21; `CatalogRoute` (Task 11) by 20; `CatalogReadServer` (Task 12) by 20, 23; `TestCluster::defineTopic(TopicSpec)` / `(name, partitions)` (Task 17) by 18; `OneBroker` (Task 11, `libs/node/tests/client/OneBroker.h`) by 13, 15; `ThreeNodes` (Task 6, moved to `support/ThreeNodes.h` in Task 22) by 22; `ql::parseScript` (Task 14) by 15, 19, 24; `cmatrix::{Kind, Via, Persist, Entity, Timing, Outcome, ExpectedCodes, expect, expectedCodes, naRule, conflictCode, name}` (Task 20) by 21, 22, 24.
- **Review Focus:** each of the six has its named test in the owning task (Tasks 9 + 11 + 20 + 21; 7 + 20; 8 + 20; 8 + 20; 7 + 11 + 16 + 20; 12).
- **Philosophy:** shared-nothing — admin owns the catalog replica / state / registries, control owns TopicRegistry / placement, the gateway reads a control-owned `CatalogRoute`, the cleaner gets overrides on its own ring, the client's record handler only feeds an SPSC ring; rings only — every new hop is a typed message on an existing SPSC ring (no new thread, no callback across threads); per-state variables — `CatalogRole`, `InFlight`, `CatalogApplier::{Applying, Halted}` + `{AwaitingBoot, Booted, Live}`, `CatalogReadServer::{Backlog, Live}`, the client's `DdlRetry`, `TopicState`, `PendingCreateBehindDrop`, `CatalogRoute`; zero allocation — hot path untouched and re-measured (Task 23, the existing `HotPathBRig.h` / `TransientRig.h` alloc gates), idle applier / read server / admin pass proven allocation-free in their own binary with the repo's `operator new` replacement pattern (Tasks 7, 12, 23); test + benchmark — every task has its tests, Tasks 3 and 23 benchmark; regression naming — every test of a fix is `*Regression*`; fix-on-sight — listed in Global Constraints rule 8, each with its task.

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-10-13-rune-catalog-history.md`. The user's overnight instruction (memory: autonomous run, merge locally, never push) applies to execution; the recommended method is **subagent-driven** — 25 tasks across five repos with many cross-task interfaces (the library, the wire, the applier, the write path), where a fresh reviewer per task catches an interface drift before the next task builds on it.
