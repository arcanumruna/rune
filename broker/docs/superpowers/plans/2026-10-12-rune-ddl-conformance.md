# Rune DDL Conformance Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove, with an exhaustive table-driven conformance suite, that DDL on the Rune broker is serialized (topic creation, view creation and alteration never run concurrently) and that every name collision is rejected with an exact error code and no half-created state. Fix every bug the suite exposes. This is the user's merge gate for Phase 2b Part E.

**Architecture:** One expectation table, `Occupant × Op → (error code, requestCreateTopic count)` in `tests/support/DdlExpectations.h`, is the single source of truth. Every unit matrix, the stress model and the integration burst read it. The bulk runs on `DdlHarness`, which extends AdminWorkerTest's fake-control harness. It adds the `__views` catalog, a real `TopicRegistry` answered by `createTopicForAdmin`, control replies the test can hold, fail or deliver late, commits the test can hold (replica set {1,2}), leadership flaps, and a BrokerNode-style restart. The harness checks the serialization invariant after every admin step. Wire-level cells use SchemaGatewayTest's `realControl` harness. A real single-node BrokerNode over Aeron covers a slice of each timing class, and the docker view-engine e2e gets a two-client race leg and a catalog-diff-across-restart leg.

**Tech Stack:** C++23, GoogleTest (`TEST_P` / `INSTANTIATE_TEST_SUITE_P`), CMake + Ninja (`build-fast`), embedded Aeron (BrokerNode tests), docker compose (single-node e2e), rune-client-cpp.

**Spec:** the user's requirement, quoted verbatim:
- "It should serialize topic creation/view creation/alteration such that they can't happen concurrently and obviously if names collide it should reject."
- "There should've been hundreds of tests for this... including integration and e2e."

Supporting context:
- `.superpowers/sdd/2026-10-11-rune-phase2b-e-transient-topics/final-fix-findings.md` (U1/U2, committed in broker `a772347`, protocol `a8a06df`, client `0f67867`).
- The no-hang wave, already landed: broker `8a693c5`, protocol `dcfc468` + `4af59b1`, client `46f2ff6`. It added `ErrorCode::InvalidRequest = 39`, `ViewRowResponse` statuses 5 (InvalidRequest) and 6 (Busy), `libs/gateway/include/rune/gateway/RequestRejection.h`, and the admin DDL-answer backlog. It also fixed R1–R3.
- **Base for this plan:** broker `8a693c5`, protocol `4af59b1`, client `46f2ff6` (all on `feat/phase2b-e-transient-topics`).
- Format exemplar: `docs/superpowers/plans/2026-10-11-rune-phase2b-e-transient-topics.md`.

---

## User decisions (2026-10-12)

The user answered every open question of the first draft. This plan implements all of them.

| # | Decision | Implemented in |
|---|---|---|
| D1 | **B5 accepted.** A catalog-less topic id becomes an inert placeholder `__orphan.<id>` instead of refusing to boot. It has **zero partitions**, so it has no replica, no log, no metadata entry and no data path: produce and subscribe are refused, and listings (MetadataResponse) do not show it. Schema gets answer `UnknownTopic`, and DDL answers `InvalidTopicName` (reserved `__` name) or `UnknownTopic`. Boot logs the placeholders **once**, in a single warn line. | Task 3 (+ Task 12 integration proof) |
| D2a | New wire code `InvalidTopicName = 40` for an empty name, path characters (`/`, `\`, `.`, `..`), an over-long name or control characters. | Tasks 1, 2, 5 |
| D2b | User DDL can neither create nor alter any `__*` name. Create answers 40. Alter or get finds no schema and answers `UnknownTopic (7)`; internal topics never have a user schema. | Tasks 5, 8 |
| D2c | A name that collides with an existing one ignoring case answers `TopicExists (25)`, for topics and views alike (`Orders` vs `orders`). | Task 6 |
| D2d | The same name rule applies to static `RUNE_TOPICS` at config parse. A bad name is a boot config error that names the entry. Two static topics that differ only by case are also a config error. | Task 5a |
| D2e | `partitionCount < 1` → `InvalidSchema (23)`. | Task 5 |
| D3a | **Fix now:** an ALTER that renames the key field updates the schema's `keyField`, and the change survives a restart. | Task 7a |
| D3b | **Fix now:** a CREATE VIEW whose source is another view's output is rejected with the new append-only code `ViewSourceIsView = 41`. No existing code fits: `ViewSourceNotFound` would be false, because the source exists. This lasts until Phase 3 supports views over views, so a silently empty view is impossible. | Tasks 1, 7b |
| D4 (amended) | **Orphan topic directories.** Boot **deletes** only the data dir of a topic id the catalog marks as an `__orphan.<id>` placeholder (a failed CREATE VIEW or another uncommitted definition). It recognises that dir by the `topic-id` marker that `TopicRegistry` now writes. Every other topic dir the catalog does not claim is **moved, never deleted**, to `dataDir/quarantine/<name>-<unix-ts>/`, with a warn line naming both paths. That covers a topic removed from `RUNE_TOPICS`, a typo'd name, and a dir from another build. Re-adding the topic does **not** restore it; restore is a documented manual step. Guards: <ul><li>nothing outside `dataDir/topics/` (and the move target `dataDir/quarantine/`) is touched;</li><li>only a recognised `<name>/[topic-id] + <digits>/<regular files>` layout is acted on;</li><li>a symlink is never followed;</li><li>a name that case-folds onto a claimed name is kept.</li></ul> | Task 3a |

Consequence of D4, documented in Task 13a: a topic **removed from `RUNE_TOPICS`** has its data directory moved to `dataDir/quarantine/<name>-<unix-ts>/` at the next boot. Its data is intact. Re-adding the topic starts it empty, and restoring the data is a manual move.

---

## Bugs this matrix is expected to expose

Each bug was found by reading the code on `feat/phase2b-e-transient-topics` (found at `a772347`, re-checked at `8a693c5`). Each one has a fix step and a `*Regression*` test in the task named here. The matrix cells encode the **correct** outcome, so they pass only once the fix lands.

| # | Bug (today) | Where | Correct outcome (matrix cell) | Fixed in |
|---|---|---|---|---|
| **B1** | DefineTopic / CreateView names are not validated. `""`, `a/b`, `../escape`, `/abs`, `.`, `..`, names longer than 249 bytes, spaces, control bytes, non-ASCII bytes and embedded NUL all reach `TopicRegistry::createTopic`. It builds `dataDir/topics/<name>/<p>/`, so `../escape` writes a log **outside** `dataDir/topics`. `/abs` replaces the whole path. `""` registers a nameless topic. The comment in `decodeCreateTopicReq` ("createTopic rejects \"\"") is false. | `AdminWorker::startNext_` / `startCreateView_`, `AdminCreateTopic.h`, `TopicRegistry.cpp:75` | New wire code `InvalidTopicName = 40`. Nothing created, no `requestCreateTopic` call. | Task 5 |
| **B2** | Reserved internal names are accepted: `__topic_schemas`, `__views`, `__consumer_offsets`, `__transaction_state`, `__cluster_metadata` and `__view_snapshots` (all the `"__…"` names in `BrokerNode.cpp`), plus the new `__orphan.<id>` placeholders. | same | `InvalidTopicName (40)` for every name that starts with `__` | Task 5 |
| **B3** | Names are case-sensitive (`std::unordered_map<std::string,…>` in TopicRegistry, SchemaRegistry and ViewRegistry). On a case-insensitive filesystem (macOS APFS default), `Orders` and `orders` share `dataDir/topics/orders/`, so the second topic recovers the first topic's log. | `TopicRegistry::createTopic` | Names stay case-sensitive identifiers. A create whose name case-folds onto a **different** existing name gets `TopicExists (25)` and creates nothing. | Task 6 |
| **B4** | Demotion (`onRole(follower)`) leaves the in-flight op running. The client waits for the 30 s timeout. In AwaitingCommit the op polls a *follower* HW and can commit into the registry from a log the new leader may truncate. | `AdminWorker::onRole` | On demotion the in-flight op is answered `Internal (6)` at once and never applied. | Task 7 |
| **B5** | **The broker refuses to boot.** A create can reach the control thread but never commit its catalog record: CREATE VIEW failing after its sink exists (the parked ruling), a lost or failed reply, a timeout or a demotion in AwaitingId. That leaves a topic id with no record. If **any later** runtime topic commits, `recoverRuntimeTopicSpecs` throws "non-contiguous id sequence … refusing to boot". The parked ruling said the orphan "blocks the name until a broker restart". In fact the restart itself fails. | `RuntimeTopicRecovery.cpp:185-198` | Boot succeeds. Each gap id is recreated as the hidden, inert, **zero-partition** placeholder `__orphan.<id>` and logged once (D1). Every recorded topic keeps its id, and the orphan's name is free again. | Task 3 |
| **B6** | `DefineTopic` with `partitionCount < 1` is not rejected. `0` registers a zero-partition topic and answers OK. A negative count throws `length_error` inside `createTopic`, which becomes `Internal`. | `AdminWorker::startNext_`, `AdminCreateTopic.h` | `InvalidSchema (23)`, nothing created | Task 5 |
| **B7** | Static `RUNE_TOPICS` names are not validated. `__views:1`, `a/b:1` and `has space:1` boot, and so do `Orders:1,orders:1`; the duplicate check is exact-match, so the two share one directory on case-insensitive filesystems. | `NodeConfig.cpp` RUNE_TOPICS parse (~L800-808) | Boot config error naming the entry (D2d) | Task 5a |
| **B8** | `ALTER … RENAME <keyField> TO x` leaves `TopicSchema.keyField` naming a field that no longer exists. `startAlter_` copies `latest->keyField` unchanged. The stale name survives restarts. | `AdminWorker::startAlter_` step g (~L681) | `keyField` follows the rename, live and after restart (D3a) | Task 7a |
| **B9** | A CREATE VIEW whose source is another view's output is accepted (the output is a registered schema), but a view sink carries no change events, so the new view stays **empty forever** with no error. | `AdminWorker::startCreateView_` source loop (~L519) | `ViewSourceIsView (41)`, nothing created (D3b) | Task 7b |
| **B10** | An orphan topic's data directory (`dataDir/topics/<name>/`) survives the restart that frees its name, so a later topic with that name recovers the orphan's old log. | boot (`BrokerNode.cpp` ~L383-398) | At boot the orphan placeholder's directory is deleted, and any other unclaimed directory is quarantined; both are logged (D4) | Task 3a |

## Known limitations pinned (NOT fixed by this plan; each cell is labelled `KnownLimitation`)

- **KL1 (parked Part C ruling): orphan sink.** A CREATE VIEW that fails after its sink topic exists leaves the sink behind. A retry under the same view name gets `TopicExists (25)` until a restart; after a restart the name is free (B5 makes that true). Occupant row `OrphanSink_KnownLimitation`. The OPTIONAL Task 14 lets the retry adopt its own orphan sink.
- **KL1b: DefineTopic adopts an orphan sink.** A disk `DefineTopic` over an orphan sink adopts it: a keyed, Compact topic becomes a plain typed topic. The broker cannot tell an orphan sink from a schema-less compact `RUNE_TOPICS` topic. Same row, column `DefineDisk`.
- **KL2: in-doubt after an AwaitingCommit timeout.** The op is answered `Internal`, but its appended record can still commit and is replayed on restart. The live registry does not apply it before then. Pinned by `DdlFixRegression.KL2_TimeoutInAwaitingCommitIsInDoubtAndReplayedOnRestart` (Task 7).
- **KL3: reads bypass the DDL queue (by design).** `GetTopicSchema` is answered at once from the committed registry. It never waits behind a queued or in-flight DDL op, so a read issued while a create is in flight answers `UnknownTopic`. The pair matrix pins this for timings T2–T4.
- **KL4: multi-node failover mid-DDL is out of scope.** The `__topic_schemas` / `__views` replicas run on a `NullPartitionTransport` (`BrokerNode.cpp`, schema-replica block: "Real multi-node follower fetch routing on an admin-thread-owned transport is Docker-gated (Task 8b)"), and multi-node catalog replication is the deferred follow-on. No harness can run a 3-node leader failover mid-DDL. Timing class T6 is covered at unit level only (`DdlHarness::demote()`/`promote()`).

---

## The expected-outcome matrix

### Ops (columns)

| Op | What the harness sends |
|---|---|
| `DefineDisk` | `DefineTopicMsg{partitionCount 1, keyField "id", blob {id Int64 key, hp Int64}, persistence 0}` |
| `DefineTransient` | same, `persistence 1` |
| `ViewPersisted` | `CreateViewMsg{persistence 0, keyField "id", plan = {id, hp} Replace over "src"}` |
| `ViewTransient` | same, `persistence 1` |
| `AlterAdd` | ADD `extra<corr>` Int64, `expectedVersion 0` |
| `AlterDrop` | DROP `hp` |
| `AlterRename` | RENAME `hp` → `health` |
| `AlterRenameKey` | RENAME `id` (the key field) → `pk` — the schema's `keyField` must become `pk` (B8) |
| `AlterSetCompat` | SET_COMPATIBILITY Forward |
| `AlterCasCurrent` | ADD with `expectedVersion` = the version read **at submit time** (1 if absent) |
| `AlterCasStale` | ADD with `expectedVersion 99` |
| `GetSchema` | `GetSchemaMsg{fingerprint 0}` |

### Occupant × Op → error code (†: detected by the control thread, so one `requestCreateTopic` call is made and **nothing is created**)

| Occupant of the target name | DefD | DefT | ViewP | ViewT | Add | Drop | Rename | RenameKey | SetCompat | CasCur | CasStale | Get |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Free** | 0 | 0 | 0 | 0 | 7 | 7 | 7 | 7 | 7 | 7 | 7 | 7 |
| **TypedDisk** (DefineTopic'd, or recovered after restart) | 25 | 25 | 25 | 25 | 0 | 0 | 0 | 0 | 0 | 0 | 31 | 0 |
| **TypedTransient** | 25 | 25 | 25 | 25 | 0 | 0 | 0 | 0 | 0 | 0 | 31 | 0 |
| **ViewPersisted** (or recovered view) | 25 | 25 | 32 | 32 | 38 | 38 | 38 | 38 | 38 | 38 | 38 | 0 |
| **ViewTransient** | 25 | 25 | 32 | 32 | 38 | 38 | 38 | 38 | 38 | 38 | 38 | 0 |
| **SchemaLessDisk** (static `RUNE_TOPICS`; orphan from a failed define) | 0 (adopt) | 25† | 25† | 25† | 7 | 7 | 7 | 7 | 7 | 7 | 7 | 7 |
| **SchemaLessTransient** | 0 (adopt, records Transient) | 0 (adopt) | 25† | 25† | 7 | 7 | 7 | 7 | 7 | 7 | 7 | 7 |
| **OrphanSink** — KnownLimitation KL1/KL1b | 0 (adopt, KL1b) | 25† | 25† (KL1) | 25† (KL1) | 7 | 7 | 7 | 7 | 7 | 7 | 7 | 7 |
| **CaseVariant** (folds onto a different existing name) — B3 | 25† | 25† | 25† | 25† | 7 | 7 | 7 | 7 | 7 | 7 | 7 | 7 |
| **BadName** (invalid or `__` reserved) — B1/B2 | 40 | 40 | 40 | 40 | 7 | 7 | 7 | 7 | 7 | 7 | 7 | 7 |

`requestCreateTopic` count per cell is 1 for every successful create and every † cell, and 0 otherwise (alters and reads never call it).

### Occupant rows exercised (37 rows × 12 ops = 456 cells)

`Free`, `TypedDisk`, `TypedTransient`, `ViewPersisted`, `ViewTransient`, `StaticSchemaLessDisk`, `StaticSchemaLessTransient`, `RecoveredRuntimeTopic` (→TypedDisk), `RecoveredView` (→ViewPersisted), `OrphanDefineDisk` (→SchemaLessDisk), `OrphanDefineTransient` (→SchemaLessTransient), `OrphanSink_KnownLimitation`, `OrphanSinkAfterRestart` (→Free, B5), `CaseVariantOfTyped`, `CaseVariantOfView`, `CaseVariantOfStatic`, `CaseVariantOfOrphan`.

Reserved names (→BadName): `__topic_schemas`, `__views`, `__consumer_offsets`, `__transaction_state`, `__cluster_metadata`, `__view_snapshots`, `__orphan.7`.

Invalid names (→BadName): `""`, `a/b`, `../escape`, `/abs`, `.`, `..`, 250×`x`, `has space`, `tab\tname`, `caf\xC3\xA9`, `nul\0byte`.

Valid boundary names (→Free): 249×`x`, `a`, `.hidden_ok-1`.

### View-source cells (source kind × view persistence; 12 cells)

| Source of the view | persisted view | transient view |
|---|---|---|
| typed disk topic | 0 | 0 |
| typed transient topic | 37 | 0 |
| static schema-less topic | 33 | 33 |
| missing topic id | 33 | 33 |
| a persisted view's output (B9) | 41 | 41 |
| a transient view's output (B9) | 41 | 41 |

A rejected view requests no sink (0 `requestCreateTopic` calls) and leaves the catalog unchanged; B9 cells are also re-checked after a restart.

### Every cell asserts

1. The exact error code, plus the version where the op succeeded.
2. The exact `requestCreateTopic` count.
3. **No half-creation.** On a rejected op or a read, the full catalog is unchanged: TopicRegistry names and ids, SchemaRegistry and ViewRegistry entries, persistence, versions and fingerprints. The `__topic_schemas` and `__views` log end offsets are unchanged, and no `pushViewPlan` (worker install) happened.
4. The final registry state. On success exactly one entry changes, as specified: kind, persistence, version, id and `keyField` (`pk` after `AlterRenameKey`, B8). An adopted topic keeps its id.
5. **At most one op in flight throughout.** The harness checks this after every step: `startedCount() − answered == (hasInFlight() ? 1 : 0)`, no `requestCreateTopic` while an op is in flight, and answers arrive in FIFO submission order.
6. After a restart the catalog equals `expectedAfterRestart(before)`: committed entries unchanged (including `keyField`), orphans either replaced by the zero-partition `__orphan.<id>` (B5) or gone when they were the tail, with each orphan placeholder's data directory deleted and every other unclaimed one quarantined (B10).

### Pair × timing matrix (A then B; 560 cells)

- A ∈ {DefineDisk, DefineTransient, ViewPersisted, ViewTransient} × all 9 timings.
- A = AlterAdd × {T1, T2, T4, T7}. An ALTER has no control round trip, so T3, T5 and T6 do not apply.
- B ∈ {DefineDisk, DefineTransient, ViewPersisted, ViewTransient, AlterAdd, AlterCasCurrent, GetSchema}.
- Relation ∈ {Same, Different}.

A create uses the name `n`; `AlterAdd` targets `src`. **B names A's view output** is the `Same` relation when A is a view, which covers alter-on-view (38) and get-on-view (0). `Different` uses the name `m` for creates and `src2` for alters and reads.

| Timing | How the harness drives it | A answers | State of A's name when B executes |
|---|---|---|---|
| T1 sequential | A settles, then B | 0 | A's committed kind |
| T2 both queued | A and B submitted before any tick | 0 | A's committed kind (B waits) |
| T3 B during A's AwaitingId | control held; B submitted; 5 ticks: B not started, no new create request | 0 | A's committed kind |
| T4 B during A's AwaitingCommit | commits held (ISR {1,2}); B submitted after A's append; 5 ticks: neither answered | 0 | A's committed kind |
| T5a A's reply fails, nothing created | `failCreate(false)` | 6 | Free |
| T5b A's reply fails after the topic was created | `failCreate(true)` | 6 | orphan (`SchemaLessDisk` / `SchemaLessTransient` / `OrphanSink`) |
| T5c A times out in AwaitingId; the late reply creates the topic | `advance(timeout)`, then the stale `deliverCreate` | 6 | orphan |
| T6 A's node is demoted in AwaitingId; the late reply creates the topic; re-promoted | `demote`, stale `deliverCreate`, `promote` | 6 (at demotion, B4) | orphan |
| T7 restart between A and B | A settles, `restart()`, then B | 0 | A's committed kind (recovered) |

B's expected cell = `table[stateWhenBExecutes][B]`, with two **concurrency exceptions** (Same relation, T2–T4 only):
- `GetSchema` reads the state **before** A commits (KL3): `Free → 7`, or v1 of `src` when A is `AlterAdd`.
- `AlterCasCurrent` after `A = AlterAdd` gets `SchemaConflict (31)`, because its expected version was read at submit time (v1) and is stale once A commits v2. This is lost-update protection.

**Timing invariance:** for T2/T3/T4 (B ∉ {GetSchema, AlterCasCurrent}), the cell also runs a T1 twin and asserts the final catalog and B's code are identical. Serialization means timing cannot change the outcome.

---

## Execution context

- **Repos / branch:** `feat/phase2b-e-transient-topics` in
  - `/Users/vradu/Projects/ddl-worktrees/rune-broker`
  - `/Users/vradu/Projects/ddl-worktrees/rune-protocol`
  - `/Users/vradu/Projects/ddl-worktrees/rune-client-cpp`
- **Preconditions:**
  - Base heads: broker `8a693c5`, protocol `4af59b1`, client `46f2ff6`. Both the Part E fix wave and the no-hang wave (R1–R3, plus the gateway-wide "every request is answered" fix) are in. Verify with `git -C <repo> log --oneline -1`.
  - What the no-hang wave changed that this plan builds on:
    - **Wire codes:** `ErrorCode::InvalidRequest = 39` is taken, so this plan appends `InvalidTopicName = 40` and `ViewSourceIsView = 41`.
    - **Gateway:** a DDL frame whose header decodes on a known session but whose body does not is answered with its **own response type** carrying `InvalidSchema (23)` (`RequestRejection.h::encodeRejection`). Admin sink missing, or admin ring full, gets `Internal (6)`. An unknown session is not answered; it is counted and warn-logged.
    - **AdminWorker (R1):** `startNext_` answers `InvalidSchema` for a direct `DefineTopicMsg.persistence > 1` before parsing. `startCreateView_` answers `InvalidSchema` for `malformed || persistence > 1`. `onCreateTopicReply` answers `Internal` for an unknown reply mode. `createTopicForAdmin` refuses an unknown mode (`topicId -1`).
    - **DDL answer backlog:** BrokerNode keeps DDL answers that hit a full admin→control egress ring in a FIFO backlog and retries them every `doWork`. `AdminDeps::respond*` signatures are unchanged. `DdlHarness`'s respond deps model the egress without the backlog, which `SchemaGatewayRegression.DdlAnswerOnAFullEgressRingIsBackloggedNotDroppedRegression` already covers.
  - Before Tasks 5 and 6, re-read `AdminWorker.cpp::startNext_` / `startCreateView_` / `onCreateTopicReply` and `AdminCreateTopic.h` at `8a693c5`, and anchor each edit on the code as it is.
- **Build (broker):** `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target <target>`. `build-fast` is already configured; `RUNE_PROTOCOL_INCLUDE` resolves to `../rune-protocol/include`. Re-run `cmake /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast` after a `CMakeLists.txt` edit.
- **Build (protocol / client):** each repo's `build` dir: `cmake --build build -j --target rune_protocol_tests` / the client's tests.
- **Regression gate before EVERY commit (CLAUDE.md #7), in every repo the commit touches:**
  - broker: `ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8`
  - protocol: `ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-protocol/build -R Regression --output-on-failure`
  - client: `ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-client-cpp/build -R Regression --output-on-failure -j8`
  - then the touched targets' full `ctest`.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Global Constraints

- HARD RULES (`/Users/vradu/Projects/rune-site/CLAUDE.md`, all eight):
  1. shared-nothing;
  2. ring buffers are the only cross-thread passing;
  3. state machines where each state holds only its variables;
  4. zero allocation on hot paths and in steady state;
  5. test and benchmark everything;
  6. a `*Regression*`-named test on every fix;
  7. the full `*Regression*` suite first on every change;
  8. fix-on-sight.
- User: "It should serialize topic creation/view creation/alteration such that they can't happen concurrently and obviously if names collide it should reject."
- User: "There should've been hundreds of tests for this... including integration and e2e."
- Every new gtest suite name contains `Regression` (`ctest -R Regression` is the gate).
- Wire `ErrorCode` values are append-only. `39 = InvalidRequest` is taken by the no-hang wave. This plan's only new values are `InvalidTopicName = 40` and `ViewSourceIsView = 41`.
- Error codes (verified in `rune-protocol/include/rune/protocol/client/Protocol.h` and `AdminWorker.cpp`):

  | Code | Name |
  |---|---|
  | 0 | OK |
  | 6 | Internal |
  | 7 | UnknownTopic |
  | 23 | InvalidSchema |
  | 25 | TopicExists |
  | 30 | SchemaIncompatible |
  | 31 | SchemaConflict |
  | 32 | ViewExists |
  | 33 | ViewSourceNotFound |
  | 34 | ViewFieldUnresolved |
  | 35 | ViewKeyInvalid |
  | 36 | ViewNotCoPartitioned |
  | 37 | ViewSourceTransient |
  | 38 | ViewNotAlterable |
  | 39 | InvalidRequest (no-hang wave, `dcfc468`; taken) |
  | 40 | InvalidTopicName (new) |
  | 41 | ViewSourceIsView (new) |

- A view that reads a transient topic must itself be transient (37).
- Do not duplicate the fix wave's U2 tests in `CreateViewRegressionTest.cpp`. The matrices **extend** them: U2.1 → pair `T2 Define×Define Same`, U2.2 → `T2 View/Define Same`, U2.3 → `T2 View×View Same`, U2.4 → `T3 Define then AlterAdd Same`, U2.5 → the harness invariant.
- The stress test's "hot path under test" is the admin thread's steady-state loop: `AdminWorker::doWork` when idle and when polling the HW gate. It must not allocate. DDL processing itself is the cold path and may allocate. The test itself may allocate.

## Review Focus

1. **A leader that flaps back to the same node** (demote, then promote with no restart) must not duplicate or lose catalog entries: promotion replays both catalogs on top of registries that are never cleared → `DdlFixRegression.B4_RepromotionKeepsTheCatalogExact` (Task 7).
2. **An op answered Internal after its record was appended** (AwaitingCommit timeout) is in doubt; it must not silently disappear, and must not diverge differently live vs after restart than documented → `DdlFixRegression.KL2_TimeoutInAwaitingCommitIsInDoubtAndReplayedOnRestart` (Task 7).
3. **Recovery placeholders `__orphan.<id>` must be hidden and inert** (D1): unaddressable by DDL, absent from metadata and schema gets, and refusing produce and subscribe → occupant row `Reserved_orphan_placeholder` (Task 8), `RuntimeTopicRecoveryRegression.PlaceholderNameIsReservedRegression` / `PlaceholdersAreLoggedOnceRegression` (Task 3), and `DdlConformanceBrokerRegression.T5_…` on a real broker (Task 12).
4. **A maximum-length (249-byte) name must survive a restart** (filesystem path-segment limits; the recovered spec goes through `TopicRegistry` again) → occupant row `Valid_MaxLen249`, which restarts at the end of every cell (Task 8).
5. **The admin thread's steady-state `doWork` must not allocate** while idle or while an op waits on the HW gate (the replica's own `doWork` is in that loop) → the allocation gate in `DdlStressRegression` (Task 10). If it fires, the backtrace names the allocator — fix it in the owning component, do not weaken the gate.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-protocol/include/rune/protocol/client/Protocol.h`, `tests/protocol/client/ViewMessagesTest.cpp` | `ErrorCode::InvalidTopicName = 40`, `ViewSourceIsView = 41` + pins | 1 |
| `rune-client-cpp/include/rune/client/Client.h` | doc: 40 on define/createView, 41 on createView | 1 |
| `libs/node/include/rune/node/TopicName.h` (new), `libs/node/tests/node/TopicNameTest.cpp` (new) | the one name rule + ASCII case fold + error text | 2 |
| `libs/node/include/rune/node/RuntimeTopicRecovery.h`, `src/RuntimeTopicRecovery.cpp`, `tests/node/RuntimeTopicRecoveryTest.cpp` | B5 hidden zero-partition placeholders (3); topic-dir reconcile: delete placeholder dirs, quarantine the rest (3a) | 3, 3a |
| `libs/node/src/BrokerNode.cpp` (boot, ~L383-398) | call `reconcileTopicDirs` before the TopicRegistry is built | 3a |
| `libs/gateway/include/rune/gateway/TopicRegistry.h`, `src/TopicRegistry.cpp` | `topic-id` marker file per disk topic dir (3a); `topicIdForFoldedName` (6) | 3a, 6 |
| `libs/node/src/NodeConfig.cpp`, `libs/node/tests/node/NodeConfigTest.cpp` | static `RUNE_TOPICS` name rule + case-fold duplicate check | 5a |
| `libs/node/include/rune/node/AdminWorker.h`, `src/AdminWorker.cpp` | introspection (4), name/partition validation (5), case conflict + `answerInFlight_` (6), demotion (7), key-rename `keyField` (7a), view-over-view (7b) | 4–7b |
| `libs/node/tests/support/DdlExpectations.h` (new) | Op/Occupant enums, the expectation table | 4 |
| `libs/node/tests/support/DdlHarness.h` (new) | the conformance harness | 4 (+6, +14) |
| `libs/node/tests/node/DdlHarnessRegressionTest.cpp` (new) | harness smoke + fix regressions B1–B4, B8, B9, KL2 | 4–7b |
| `libs/node/include/rune/node/AdminCreateTopic.h` | control-side validation (5), case fold (6), sink-spec match (14) | 5, 6, 14 |
| `libs/node/include/rune/node/AdminChannels.h` | `CreateTopicReplyMsg.caseConflict` (6), `.sinkSpecMatches` (14) | 6, 14 |
| `libs/node/src/BrokerNode.cpp`, `include/rune/node/BrokerNode.h` | skip replicas on case conflict (6); admin-create fault hook (12); first runtime id (14) | 6, 12, 14 |
| `libs/node/tests/node/DdlOccupantMatrixRegressionTest.cpp` (new) | occupant matrix, malformed admin requests, view-source cells | 8 |
| `libs/node/tests/node/DdlPairTimingMatrixRegressionTest.cpp` (new) | pair × timing matrix | 9 |
| `libs/node/tests/node/DdlStressRegressionTest.cpp` (new, own exe) | seeded model-checked stress + zero-alloc gate | 10 |
| `libs/node/tests/integration/SchemaGatewayTest.cpp` | wire cells: adoption, names, malformed frames | 11 |
| `libs/node/tests/support/ShardTestClient.h` | async DDL send/await helpers, subscribe answer capture | 12 |
| `libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp` (new) | real BrokerNode over Aeron | 12 |
| `libs/node/CMakeLists.txt` | new targets / files | 2, 4, 8–10, 12 |
| `rune-client-cpp/examples/view_probe.cpp`, `docker/view-engine-e2e.sh` | e2e race + alter-view + catalog-diff legs | 13 |
| `docs/SELF-HOSTING.md`, `docs/release-notes/phase2b-e-transient-topics.md` | name rule, codes 40/41, placeholders, orphan-dir clean-up, quarantine + manual restore | 13a |

---

### Task 1: Protocol — `ErrorCode::InvalidTopicName = 40`, `ViewSourceIsView = 41`

**Repos:** rune-protocol, rune-client-cpp

**Files:**
- Modify: `/Users/vradu/Projects/ddl-worktrees/rune-protocol/include/rune/protocol/client/Protocol.h` (enum `ErrorCode`, after `InvalidRequest = 39`, ~L155-158 at `4af59b1`)
- Test: `/Users/vradu/Projects/ddl-worktrees/rune-protocol/tests/protocol/client/ViewMessagesTest.cpp` (after `ViewMessagesRegression.InvalidRequestAndViewRowStatusesArePinnedRegression`, ~L205)
- Modify: `/Users/vradu/Projects/ddl-worktrees/rune-client-cpp/include/rune/client/Client.h` (`DefineTopicResult` ~L95, `CreateViewResult` comment ~L124-128)

**Interfaces:**
- Produces:
  - `rune::protocol::client::ErrorCode::InvalidTopicName == 40`. The broker uses the integer `40` (`kInvalidTopicName`), and tests use `rune::node::test::kInvalidTopicName`.
  - `rune::protocol::client::ErrorCode::ViewSourceIsView == 41`. The broker uses `kViewSourceIsView = 41` (Task 7b), and tests use `rune::node::test::kViewSourceIsView`.

- [ ] **Step 1: Write the failing test.** Append to `ViewMessagesTest.cpp`:

```cpp
// DDL conformance: a DefineTopic/CreateView name that fails the broker's name rule
// (empty, > 249 bytes, "." / "..", a byte outside [A-Za-z0-9._-], or the reserved
// "__" prefix) is answered InvalidTopicName.  Append-only wire constant.
TEST(ViewMessagesRegression, InvalidTopicNameCodeIsPinned) {
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InvalidTopicName), 40u);
}

// DDL conformance (user decision D3b): a CreateView whose source is another view's
// output is rejected (a view sink carries no change events until Phase 3, so the
// view would stay silently empty).  Append-only wire constant.
TEST(ViewMessagesRegression, ViewSourceIsViewCodeIsPinned) {
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::ViewSourceIsView), 41u);
}
```

- [ ] **Step 2: Run it and confirm it fails.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-protocol/build -j --target rune_protocol_tests`
  Expected: compile errors `no member named 'InvalidTopicName'` and `no member named 'ViewSourceIsView'` in `rune::protocol::client::ErrorCode`.

- [ ] **Step 3: Implement.** In `Protocol.h`, after the `InvalidRequest = 39, …` entry (keep its four comment lines):

```cpp
    InvalidTopicName       = 40,  // DefineTopic/CreateView: the topic/view name is empty, longer than
                                  // 249 bytes, "." or "..", has a byte outside [A-Za-z0-9._-], or
                                  // starts with the reserved "__" prefix (internal topics)
    ViewSourceIsView       = 41,  // CreateView: a source is another view's output (a view sink carries
                                  // no change events yet, so the view would stay empty; until Phase 3)
```

In `Client.h`, replace the comment block directly above `struct DefineTopicResult {` (the struct itself is unchanged) by adding this comment line before the struct:

```cpp
/// Result of DefineTopic. errorCode: 0 OK; TopicExists=25 (the name — or a name
/// differing from it only in letter case — is taken); InvalidSchema=23 (bad
/// descriptor or partitions < 1); InvalidTopicName=40 (empty, > 249 bytes, "."/"..",
/// a byte outside [A-Za-z0-9._-], or the reserved "__" prefix).
```

and in the comment above `struct CreateViewResult {` replace
`/// ViewExists=32, ViewSourceNotFound=33, ViewFieldUnresolved=34,`
with
`/// ViewExists=32, ViewSourceNotFound=33, ViewFieldUnresolved=34, TopicExists=25,
/// InvalidTopicName=40 (same name rule as defineTopic), ViewSourceIsView=41 (a source is
/// another view's output — not supported until Phase 3),`

- [ ] **Step 4: Run the tests and confirm they pass.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-protocol/build -j --target rune_protocol_tests && ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-protocol/build -R Regression --output-on-failure`
  Expected: PASS, including `ViewMessagesRegression.InvalidTopicNameCodeIsPinned` and `ViewMessagesRegression.ViewSourceIsViewCodeIsPinned`.
  Run the client build: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-client-cpp/build -j` → builds; then `ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-client-cpp/build -R Regression --output-on-failure -j8` → PASS.

- [ ] **Step 5: Commit (two repos).**

```bash
git -C /Users/vradu/Projects/ddl-worktrees/rune-protocol add include/rune/protocol/client/Protocol.h tests/protocol/client/ViewMessagesTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-protocol commit -m "feat(protocol): ErrorCode::InvalidTopicName = 40 (DDL name rule), ViewSourceIsView = 41

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/ddl-worktrees/rune-client-cpp add include/rune/client/Client.h
git -C /Users/vradu/Projects/ddl-worktrees/rune-client-cpp commit -m "docs(client): defineTopic/createView may answer InvalidTopicName=40, TopicExists for a case variant, ViewSourceIsView=41

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `TopicName.h` — the one name rule + ASCII case fold + error text

**Files:**
- Create: `libs/node/include/rune/node/TopicName.h`
- Create: `libs/node/tests/node/TopicNameTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (`rune_node_tests` source list, ~L134-154: add `tests/node/TopicNameTest.cpp` after `tests/node/RuntimeTopicRecoveryTest.cpp`)

**Interfaces:**
- Produces (namespace `rune::node`, header-only, `constexpr`, no allocation):
  - `inline constexpr std::size_t kMaxTopicNameBytes = 249;`
  - `inline constexpr std::string_view kReservedTopicPrefix = "__";`
  - `enum class TopicNameError : uint8_t { None, Empty, TooLong, DotSegment, BadChar, Reserved };`
  - `constexpr TopicNameError checkTopicName(std::string_view) noexcept;`
  - `constexpr bool isValidTopicName(std::string_view) noexcept;`
  - `constexpr bool topicNamesFoldEqual(std::string_view, std::string_view) noexcept;` (ASCII letters only)
  - `constexpr std::string_view topicNameErrorText(TopicNameError) noexcept;` (used by the `RUNE_TOPICS` parse error, Task 5a)

- [ ] **Step 1: Write the failing test** `libs/node/tests/node/TopicNameTest.cpp`:

```cpp
// TopicNameTest.cpp — DDL conformance: the one rule for a DDL-created topic/view
// name (TopicName.h).  A name becomes the directory dataDir/topics/<name>/, so it
// must be ONE safe path segment; "__" names are internal.
#include <gtest/gtest.h>
#include <rune/node/TopicName.h>

#include <string>

using rune::node::checkTopicName;
using rune::node::isValidTopicName;
using rune::node::topicNamesFoldEqual;
using rune::node::TopicNameError;

static_assert(isValidTopicName("orders"));
static_assert(!isValidTopicName(""));
static_assert(!isValidTopicName("__views"));
static_assert(topicNamesFoldEqual("Orders", "oRDERS"));

TEST(TopicNameRegression, AcceptsSafeSegments) {
    for (const char* n : {"orders", "Orders.v2", "a", "_x", "-", ".hidden", "a-b_c.D9"})
        EXPECT_EQ(checkTopicName(n), TopicNameError::None) << n;
    EXPECT_EQ(checkTopicName(std::string(249, 'x')), TopicNameError::None) << "249 bytes is the max";
}

TEST(TopicNameRegression, RejectsEmptyTooLongAndDotSegments) {
    EXPECT_EQ(checkTopicName(""), TopicNameError::Empty);
    EXPECT_EQ(checkTopicName(std::string(250, 'x')), TopicNameError::TooLong);
    EXPECT_EQ(checkTopicName("."), TopicNameError::DotSegment);
    EXPECT_EQ(checkTopicName(".."), TopicNameError::DotSegment);
}

TEST(TopicNameRegression, RejectsPathControlAndNonAsciiBytes) {
    for (const std::string n : {std::string("a/b"), std::string("../x"), std::string("/abs"),
                                std::string("a\\b"), std::string("has space"), std::string("tab\tx"),
                                std::string("caf\xC3\xA9"), std::string("nul\0b", 5), std::string("colon:x")})
        EXPECT_EQ(checkTopicName(n), TopicNameError::BadChar) << n;
}

TEST(TopicNameRegression, ReservesTheDoubleUnderscorePrefix) {
    for (const char* n : {"__topic_schemas", "__views", "__consumer_offsets", "__transaction_state",
                          "__cluster_metadata", "__view_snapshots", "__orphan.3", "__"})
        EXPECT_EQ(checkTopicName(n), TopicNameError::Reserved) << n;
    EXPECT_EQ(checkTopicName("_single"), TopicNameError::None);
}

TEST(TopicNameRegression, FoldEqualityIsAsciiCaseOnly) {
    EXPECT_TRUE(topicNamesFoldEqual("Orders", "orders"));
    EXPECT_FALSE(topicNamesFoldEqual("orders", "orders "));
    EXPECT_FALSE(topicNamesFoldEqual("orders", "order"));
    EXPECT_FALSE(topicNamesFoldEqual("caf\xC3\xA9", "CAF\xC3\x89")) << "non-ASCII bytes are never folded";
}

TEST(TopicNameRegression, EveryErrorHasAHumanReadableText) {
    using rune::node::topicNameErrorText;
    EXPECT_EQ(topicNameErrorText(TopicNameError::Empty), "empty");
    EXPECT_EQ(topicNameErrorText(TopicNameError::TooLong), "longer than 249 bytes");
    EXPECT_EQ(topicNameErrorText(TopicNameError::DotSegment), "'.' or '..'");
    EXPECT_EQ(topicNameErrorText(TopicNameError::BadChar), "a byte outside [A-Za-z0-9._-]");
    EXPECT_EQ(topicNameErrorText(TopicNameError::Reserved), "the '__' prefix is reserved for internal topics");
    EXPECT_EQ(topicNameErrorText(TopicNameError::None), "ok");
}
```

- [ ] **Step 2: Add the file to CMake and confirm it fails.** In `libs/node/CMakeLists.txt`, in `add_executable(rune_node_tests …)`, add the line `        tests/node/TopicNameTest.cpp` after `        tests/node/RuntimeTopicRecoveryTest.cpp`.
  Run: `cmake /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_node_tests`
  Expected: FAIL, `'rune/node/TopicName.h' file not found`.

- [ ] **Step 3: Implement** `libs/node/include/rune/node/TopicName.h`:

```cpp
#pragma once
// TopicName.h — the ONE rule for a DDL-created topic or view name (DefineTopic,
// CreateView), shared by the admin thread (answers InvalidTopicName = 40 before
// anything is created) and the control thread's createTopicForAdmin (refuses the
// same names defensively).  A name becomes a directory component —
// dataDir/topics/<name>/<partition>/ — so it must be one safe path segment:
//
//   * 1..249 bytes (Kafka's limit; leaves room under a 255-byte segment);
//   * ASCII letters, digits, '.', '_', '-' only — no '/', '\\', NUL, space,
//     control bytes or non-ASCII (no path traversal, no absolute paths);
//   * not "." or "..";
//   * not starting with "__": reserved for internal topics (__topic_schemas,
//     __views, __consumer_offsets, __transaction_state, __cluster_metadata,
//     __view_snapshots) and the boot-recovery placeholders __orphan.<id>.
//
// Names are CASE-SENSITIVE identifiers, but two names that differ only in ASCII
// case would share one directory on a case-insensitive filesystem (macOS APFS
// default), so a create whose name case-folds onto an existing DIFFERENT name is
// refused with TopicExists (topicNamesFoldEqual; see createTopicForAdmin).
//
// Static RUNE_TOPICS names obey the same rule, checked at config parse (user
// decision D2d, NodeConfig.cpp — Task 5a).
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace rune::node {

inline constexpr std::size_t      kMaxTopicNameBytes   = 249;
inline constexpr std::string_view kReservedTopicPrefix = "__";

enum class TopicNameError : uint8_t { None, Empty, TooLong, DotSegment, BadChar, Reserved };

[[nodiscard]] constexpr TopicNameError checkTopicName(std::string_view n) noexcept {
    if (n.empty()) return TopicNameError::Empty;
    if (n.size() > kMaxTopicNameBytes) return TopicNameError::TooLong;
    if (n == "." || n == "..") return TopicNameError::DotSegment;
    for (const char c : n) {
        const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        if (!ok) return TopicNameError::BadChar;
    }
    if (n.starts_with(kReservedTopicPrefix)) return TopicNameError::Reserved;
    return TopicNameError::None;
}

[[nodiscard]] constexpr bool isValidTopicName(std::string_view n) noexcept {
    return checkTopicName(n) == TopicNameError::None;
}

[[nodiscard]] constexpr char asciiLower(char c) noexcept {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

/// True iff `a` and `b` are equal ignoring ASCII letter case (other bytes exact).
[[nodiscard]] constexpr bool topicNamesFoldEqual(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i)
        if (asciiLower(a[i]) != asciiLower(b[i])) return false;
    return true;
}

/// A short reason for config errors ("RUNE_TOPICS topic name 'x' is invalid (<text>)").
[[nodiscard]] constexpr std::string_view topicNameErrorText(TopicNameError e) noexcept {
    switch (e) {
        case TopicNameError::None:       return "ok";
        case TopicNameError::Empty:      return "empty";
        case TopicNameError::TooLong:    return "longer than 249 bytes";
        case TopicNameError::DotSegment: return "'.' or '..'";
        case TopicNameError::BadChar:    return "a byte outside [A-Za-z0-9._-]";
        case TopicNameError::Reserved:   return "the '__' prefix is reserved for internal topics";
    }
    return "invalid";
}

} // namespace rune::node
```

- [ ] **Step 4: Run the tests and confirm they pass.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_node_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_node_tests --gtest_filter='TopicNameRegression.*'`
  Expected: 6 PASS.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/include/rune/node/TopicName.h libs/node/tests/node/TopicNameTest.cpp libs/node/CMakeLists.txt
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "feat(node): TopicName.h — one safe-path-segment rule for DDL names + ASCII case fold

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: B5 — recovery fills catalog-less ids with hidden zero-partition placeholders (the broker boots)

**Files:**
- Modify: `libs/node/include/rune/node/RuntimeTopicRecovery.h` (doc block + new declaration)
- Modify: `libs/node/src/RuntimeTopicRecovery.cpp` (contiguity loop ~L185-200; new function)
- Test: `libs/node/tests/node/RuntimeTopicRecoveryTest.cpp` (replace `IdGapFailsLoud`, ~L177-184)

**Interfaces:**
- Produces: `[[nodiscard]] std::string rune::node::orphanPlaceholderName(int32_t topicId);` → `"__orphan.<id>"`.
- Changes `recoverRuntimeTopicSpecs`:
  - It no longer throws on an id gap.
  - Each missing id `k` becomes `TopicSpec{topicId k, name orphanPlaceholderName(k), partitionCount 0, persistence Transient}`.
  - All placeholders are logged in **one** warn line per boot (D1).
  - It still throws on a corrupt record or on an id claimed by both catalogs.
- Why zero partitions makes the placeholder inert and hidden (D1):
  - Every boot path loops `for (p = 0; p < partitionCount; ++p)` (`BrokerNode.cpp` L1622, L1807, L4145, `buildReplication`, `TopicRegistry` ctor), so no replica, log, directory or metadata assignment is created.
  - `MetadataResponse` lists partitions, so the placeholder never appears.
  - Produce and subscribe resolve `topicRegistry_->log(id, p)`, which is `nullptr` (bounds-checked), and answer an error. `rune::protocol::partitionForKey` returns 0 for `partitionCount <= 1`, so there is no divide-by-zero.
  - `GetTopicSchema` answers `UnknownTopic` because there is no schema. DDL answers `InvalidTopicName` (reserved `__`) or `UnknownTopic`.
  - Task 12 proves all of this end to end.

> Design reversal (user decision D1): `IdGapFailsLoud` pinned the throw deliberately. A gap has exactly one source: a create that reached the control thread but never committed its record, because every committed DDL is recorded. Refusing to boot turned a failed CREATE VIEW into an outage.

- [ ] **Step 1: Write the failing tests.** In `RuntimeTopicRecoveryTest.cpp`, add `#include <rune/node/TopicName.h>` and `#include <spdlog/sinks/ringbuffer_sink.h>` to the includes, then replace the whole `IdGapFailsLoud` test (its comment and body) with:

```cpp
// B5 (DDL conformance, user decision D1): an id with no catalog record is a topic
// whose DefineTopic/CreateView never committed (an orphan: a failed CREATE VIEW's
// sink, a create whose reply was lost).  Refusing to boot made a failed CREATE VIEW
// an outage.  The gap is filled with a hidden, inert placeholder: ZERO partitions
// (no replica, no log, no metadata, no data path), a reserved "__" name.  Every
// recorded topic keeps its id and the orphan's name is free again.
TEST(RuntimeTopicRecoveryRegression, IdGapIsFilledWithInertPlaceholderRegression) {
    TempDataDir d("gap");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "zero", "d0"), schemaRecord(3, 1, 2, "three", "d3")});
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 4u);
    EXPECT_EQ(specs[0].name, "zero");
    for (int32_t id : {1, 2}) {
        SCOPED_TRACE(id);
        EXPECT_EQ(specs[id].topicId, id);
        EXPECT_EQ(specs[id].name, rune::node::orphanPlaceholderName(id));
        EXPECT_EQ(specs[id].partitionCount, 0) << "no replica, no log, no metadata entry";
        EXPECT_EQ(specs[id].persistence, rune::node::PersistenceMode::Transient) << "never a disk dir";
    }
    EXPECT_EQ(specs[3].topicId, 3);
    EXPECT_EQ(specs[3].name, "three");
    EXPECT_EQ(specs[3].partitionCount, 2);
}

// A gap that starts at firstRuntimeId (the first runtime topic was the orphan).
TEST(RuntimeTopicRecoveryRegression, GapAtFirstRuntimeIdIsFilledRegression) {
    TempDataDir d("gap-first");
    appendRecords(d.path / "__views", {viewRecord(3, "char_hp", /*source*/ 0)});
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), /*firstRuntimeId*/ 2);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_EQ(specs[0].topicId, 2);
    EXPECT_EQ(specs[0].name, rune::node::orphanPlaceholderName(2));
    EXPECT_EQ(specs[0].partitionCount, 0);
    EXPECT_EQ(specs[1].topicId, 3);
    EXPECT_EQ(specs[1].name, "char_hp");
}

// The placeholder name is reserved: no DDL can ever create or address it.
TEST(RuntimeTopicRecoveryRegression, PlaceholderNameIsReservedRegression) {
    EXPECT_EQ(rune::node::orphanPlaceholderName(7), "__orphan.7");
    EXPECT_FALSE(rune::node::isValidTopicName(rune::node::orphanPlaceholderName(7)));
}

// D1: placeholders are logged ONCE per boot (one line naming every id), not per id.
TEST(RuntimeTopicRecoveryRegression, PlaceholdersAreLoggedOnceRegression) {
    TempDataDir d("gap-log");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "zero", "d0"), schemaRecord(4, 1, 1, "four", "d4")});
    auto sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
    const std::shared_ptr<spdlog::logger> previous = spdlog::default_logger();
    spdlog::set_default_logger(std::make_shared<spdlog::logger>("rtr-capture", sink));
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    spdlog::set_default_logger(previous);
    ASSERT_EQ(specs.size(), 5u);
    int lines = 0;
    std::string theLine;
    for (const std::string& l : sink->last_formatted()) {
        if (l.find("__orphan.") != std::string::npos) { ++lines; theLine = l; }
    }
    EXPECT_EQ(lines, 1) << "one warn line per boot";
    for (const char* id : {"__orphan.1", "__orphan.2", "__orphan.3"})
        EXPECT_NE(theLine.find(id), std::string::npos) << id << " named in: " << theLine;
}
```

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_node_tests`
  Expected: compile error `no member named 'orphanPlaceholderName'`.

- [ ] **Step 3: Implement.** In `RuntimeTopicRecovery.h`, replace the `@throws` paragraph of `recoverRuntimeTopicSpecs` with:

```cpp
/// An id with no catalog record (a DefineTopic/CreateView whose record never
/// committed — an orphan) is recreated as the inert, hidden placeholder
/// orphanPlaceholderName(id): ZERO partitions (no replica, no log, no directory,
/// no metadata entry — produce/subscribe find no partition), transient, and a
/// reserved "__" name no DDL can address.  Later topics therefore keep their ids,
/// and the orphan's original name is free again after the restart.  All
/// placeholders of one boot are logged in a single warn line.
///
/// @throws std::runtime_error on a record that does not parse or an id claimed by
///         both catalogs — boot must not guess.
```

and add, after the `recoverRuntimeTopicSpecs` declaration:

```cpp
/// "__orphan.<topicId>" — the recovery placeholder name for a catalog-less id.
[[nodiscard]] std::string orphanPlaceholderName(int32_t topicId);
```

In `RuntimeTopicRecovery.cpp`, add `#include <rune/node/PersistenceMode.h>` to the includes. Add above `std::vector<TopicSpec> recoverRuntimeTopicSpecs(`:

```cpp
std::string orphanPlaceholderName(int32_t topicId) {
    return "__orphan." + std::to_string(topicId);
}
```

Replace the contiguity block. It starts at `    // ── contiguity: TopicRegistry hands out ids sequentially. ────────────────` and ends at `    return out;`. The new block:

```cpp
    // ── contiguity: TopicRegistry hands out ids sequentially. ────────────────
    // B5 (DDL conformance, D1): a missing id is an orphan (its DDL never committed a
    // record).  Fill it with a hidden, inert, ZERO-partition placeholder so every
    // later topic keeps its id; log all placeholders of this boot ONCE.
    std::vector<TopicSpec> out;
    out.reserve(runtime.size());
    std::string placeholders;   // "__orphan.1, __orphan.2" for the single log line
    int32_t expected = firstRuntimeId;
    for (auto& [id, spec] : runtime) {
        while (expected < id) {
            TopicSpec ph;
            ph.topicId        = expected;
            ph.name           = orphanPlaceholderName(expected);
            ph.partitionCount = 0;
            ph.persistence    = PersistenceMode::Transient;
            if (!placeholders.empty()) placeholders += ", ";
            placeholders += ph.name;
            out.push_back(std::move(ph));
            ++expected;
        }
        out.push_back(std::move(spec));
        ++expected;
    }
    if (!placeholders.empty()) {
        spdlog::warn("runtime topic recovery: topic id(s) with no catalog record (a DefineTopic/"
                     "CreateView that never committed left them behind) recreated as hidden "
                     "zero-partition placeholders so later topics keep their ids: {}", placeholders);
    }
    return out;
```

- [ ] **Step 4: Run the tests and confirm they pass.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_node_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_node_tests --gtest_filter='RuntimeTopicRecoveryRegression.*'`
  Expected: all PASS. `IdGapFailsLoud` no longer exists.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/include/rune/node/RuntimeTopicRecovery.h libs/node/src/RuntimeTopicRecovery.cpp libs/node/tests/node/RuntimeTopicRecoveryTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "fix(recovery): catalog-less topic ids become hidden zero-partition __orphan.<id> placeholders, logged once — boot no longer fails after a failed CREATE VIEW (B5)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3a: B10 — boot reconciles topic directories: delete orphan-placeholder dirs, QUARANTINE every other unclaimed dir

**User decision D4 (amended 2026-10-12):** boot **deletes** only the data dir of a topic id that the catalog marks as an `__orphan.<id>` placeholder, meaning a definition that never committed, such as a failed CREATE VIEW. Every other topic dir the catalog does not claim is **moved, never deleted**, to `dataDir/quarantine/<name>-<unix-ts>/`, with a warn line naming both paths. That covers a topic removed from `RUNE_TOPICS`, a typo'd name, and a dir left by another build. A topic re-added to `RUNE_TOPICS` starts empty; it is **not** restored automatically (manual restore is documented in Task 13a).

**How a dir is tied to a placeholder id.** The orphan's name has no catalog record, which is exactly why it is an orphan. So `TopicRegistry::createTopic` writes a marker file `dataDir/topics/<name>/topic-id` holding the decimal topic id. It does this for every disk topic, at boot and at runtime (cold path). A dir is deleted only if its marker parses to an id whose catalog entry is the placeholder `orphanPlaceholderName(id)`. A dir without a marker (older builds), or with any other id, is quarantined.

**Files:**
- Modify: `libs/gateway/src/TopicRegistry.cpp`: `createTopic`, after the partition loop (~L97); add `#include <fstream>`
- Modify: `libs/gateway/include/rune/gateway/TopicRegistry.h`: the `createTopic` doc comment, plus `kTopicIdMarkerFile`
- Modify: `libs/node/include/rune/node/RuntimeTopicRecovery.h` (new declarations)
- Modify: `libs/node/src/RuntimeTopicRecovery.cpp`: new function plus `#include <rune/node/TopicName.h>`, `#include <rune/gateway/TopicRegistry.h>`, `#include <fstream>`, `#include <set>`
- Modify: `libs/node/src/BrokerNode.cpp` (boot block ~L383-392, right after the recovered specs are appended to `config_.topics`)
- Test: `libs/node/tests/node/RuntimeTopicRecoveryTest.cpp` (append)

**Interfaces:**
- Consumes: `topicNamesFoldEqual` (Task 2), `orphanPlaceholderName` (Task 3).
- Produces:

```cpp
// TopicRegistry.h (namespace rune::gateway)
/// Written by createTopic into dataDir/topics/<name>/ for a disk topic: the topic id
/// in decimal.  Boot uses it to tie an unclaimed dir to an orphan placeholder id.
inline constexpr const char* kTopicIdMarkerFile = "topic-id";

// RuntimeTopicRecovery.h (namespace rune::node)
struct TopicDirReconciliation {
    std::vector<std::filesystem::path> deleted;       // orphan-placeholder dirs (canonical paths)
    std::vector<std::filesystem::path> quarantined;   // destinations under dataDir/quarantine/
};
/// B10 / D4: reconcile dataDir/topics/ against the full catalog `topics` (static +
/// recovered, incl. __orphan.<id> placeholders).  Never throws.
[[nodiscard]] TopicDirReconciliation
reconcileTopicDirs(const std::string& dataDir, const std::vector<TopicSpec>& topics, int64_t nowUnixSeconds);
```

  Rules for each direct child `<name>` of `canonical(dataDir/topics)`:
  1. A symlink, or anything that is not a directory, is left alone and never followed.
  2. A name claimed by a topic, exactly or ignoring ASCII case, is kept.
  3. An unrecognised layout is left alone and logged at info. The recognised layout is `<name>/[topic-id]` plus `<digits>/` partition dirs (at least one) holding regular files only, with no symlink at any level.
  4. A marker id that names an `__orphan.<id>` placeholder → the dir is **deleted** (`remove_all` on the canonical path, after re-checking `parent == canonical topics root`), logged at warn.
  5. Anything else → the dir is **renamed** to `dataDir/quarantine/<name>-<nowUnixSeconds>`, adding `-1`, `-2`, … if that exists, and logged at warn with both paths. If `dataDir/quarantine` exists as a symlink or a non-directory, nothing is moved and an error is logged.

- [ ] **Step 1: Write the failing tests.** Append to `RuntimeTopicRecoveryTest.cpp`:

```cpp
// ── B10 / user decision D4 (amended): boot deletes ONLY orphan-placeholder topic
//    dirs; every other unclaimed topic dir is QUARANTINED (moved, never deleted). ──
namespace {
void touchFile(const fs::path& p, std::string_view content = "x") {
    fs::create_directories(p.parent_path());
    std::ofstream(p) << content;
}
TopicSpec spec(const std::string& name, int32_t id, int32_t partitions = 1) {
    TopicSpec t; t.name = name; t.topicId = id; t.partitionCount = partitions; return t;
}
TopicSpec placeholder(int32_t id) {
    TopicSpec t = spec(rune::node::orphanPlaceholderName(id), id, 0);
    t.persistence = rune::node::PersistenceMode::Transient;
    return t;
}
constexpr int64_t kNow = 1'760'000'000;   // a fixed unix timestamp
std::vector<uint8_t> oneRecordBatch(std::string_view value) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(std::string_view{}, value, 0);
    return b.build();
}
} // namespace

TEST(RuntimeTopicRecoveryRegression, TopicRegistryWritesTheTopicIdMarkerRegression) {
    TempDataDir d("marker");
    {
        rune::gateway::TopicRegistry reg({spec("events", 0, 2)}, d.path.string());
        TopicSpec t = spec("later", -1); t.persistence = rune::node::PersistenceMode::Transient;
        (void)reg.createTopic(t);   // transient: no dir, no marker
    }
    std::ifstream in(d.path / "topics" / "events" / rune::gateway::kTopicIdMarkerFile);
    int32_t id = -1;
    in >> id;
    EXPECT_EQ(id, 0);
    EXPECT_FALSE(fs::exists(d.path / "topics" / "later"));
}

TEST(RuntimeTopicRecoveryRegression, OrphanPlaceholderDirIsDeletedRegression) {
    TempDataDir d("orphan-dir");
    touchFile(d.path / "topics" / "orphan_v" / "topic-id", "2\n");
    touchFile(d.path / "topics" / "orphan_v" / "0" / "00000000000000000000.log");
    touchFile(d.path / "topics" / "live" / "topic-id", "0\n");
    touchFile(d.path / "topics" / "live" / "0" / "00000000000000000000.log");
    const auto r = rune::node::reconcileTopicDirs(
        d.path.string(), {spec("live", 0), spec("other", 1), placeholder(2), spec("later", 3)}, kNow);
    ASSERT_EQ(r.deleted.size(), 1u);
    EXPECT_EQ(r.deleted[0], fs::canonical(d.path / "topics") / "orphan_v");
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics" / "orphan_v"));
    EXPECT_FALSE(fs::exists(d.path / "quarantine")) << "nothing quarantined, nothing created";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "live" / "0" / "00000000000000000000.log"));
}

TEST(RuntimeTopicRecoveryRegression, RemovedStaticTopicIsQuarantinedIntactAndNotAutoRestoredRegression) {
    TempDataDir d("quarantine");
    {   // boot 1: RUNE_TOPICS=events:1 — one record written
        rune::gateway::TopicRegistry reg({spec("events", 0)}, d.path.string());
        rune::storage::Log* log = reg.log(0, 0);
        ASSERT_NE(log, nullptr);
        log->append(oneRecordBatch("kept-record"), 0);
    }
    // boot 2: events removed from RUNE_TOPICS -> quarantined, NOT deleted
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
    EXPECT_TRUE(r.deleted.empty()) << "a removed static topic is never deleted";
    ASSERT_EQ(r.quarantined.size(), 1u);
    const fs::path q = d.path / "quarantine" / ("events-" + std::to_string(kNow));
    EXPECT_EQ(fs::canonical(r.quarantined[0]), fs::canonical(q));
    EXPECT_FALSE(fs::exists(d.path / "topics" / "events"));
    {   // the data is intact and readable where it was moved
        rune::storage::Log moved((q / "0").string());
        moved.recover();
        EXPECT_EQ(moved.logEndOffset(), 1);
        const std::vector<uint8_t> raw = moved.read(0, 1 << 20);
        const std::string bytes(raw.begin(), raw.end());
        EXPECT_NE(bytes.find("kept-record"), std::string::npos);
    }
    {   // boot 3: events re-added — a FRESH empty topic; the quarantine is untouched
        const auto again = rune::node::reconcileTopicDirs(d.path.string(), {spec("events", 0)}, kNow + 60);
        EXPECT_TRUE(again.deleted.empty());
        EXPECT_TRUE(again.quarantined.empty());
        rune::gateway::TopicRegistry reg({spec("events", 0)}, d.path.string());
        EXPECT_EQ(reg.log(0, 0)->logEndOffset(), 0) << "re-adding a topic does NOT auto-restore it";
        EXPECT_TRUE(fs::exists(q / "0")) << "the quarantined copy stays until an operator restores it";
    }
}

TEST(RuntimeTopicRecoveryRegression, UnmarkedOrForeignIdDirIsQuarantinedNotDeletedRegression) {
    TempDataDir d("foreign");
    touchFile(d.path / "topics" / "typo_name" / "0" / "seg.log");               // no marker (old build)
    touchFile(d.path / "topics" / "other_build" / "topic-id", "7\n");           // id 7 is not a placeholder
    touchFile(d.path / "topics" / "other_build" / "0" / "seg.log");
    touchFile(d.path / "topics" / "garbage_id" / "topic-id", "not-a-number");
    touchFile(d.path / "topics" / "garbage_id" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("x", 7)}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_EQ(r.quarantined.size(), 3u);
    for (const char* n : {"typo_name", "other_build", "garbage_id"}) {
        EXPECT_FALSE(fs::exists(d.path / "topics" / n)) << n;
        EXPECT_TRUE(fs::exists(d.path / "quarantine" / (std::string(n) + "-" + std::to_string(kNow)) / "0" / "seg.log"))
            << n;
    }
}

TEST(RuntimeTopicRecoveryRegression, QuarantineNameCollisionGetsASuffixRegression) {
    TempDataDir d("q-collide");
    fs::create_directories(d.path / "quarantine" / ("gone-" + std::to_string(kNow)));
    touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("gone-" + std::to_string(kNow) + "-1") / "0" / "seg.log"));
}

TEST(RuntimeTopicRecoveryRegression, NonTopicEntriesInTopicsDirAreLeftAloneRegression) {
    TempDataDir d("non-topic");
    touchFile(d.path / "topics" / "README");                        // a plain file
    touchFile(d.path / "topics" / "notes" / "todo.txt");            // files directly under <name>/
    touchFile(d.path / "topics" / "weird" / "abc" / "seg.log");     // non-numeric partition dir
    touchFile(d.path / "topics" / "nested" / "0" / "deeper" / "x"); // a sub-dir inside a partition
    fs::create_directories(d.path / "topics" / "empty");            // no partition dir at all
    touchFile(d.path / "elsewhere" / "0" / "seg.log");              // outside topics/: never scanned
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    for (const char* p : {"topics/README", "topics/notes/todo.txt", "topics/weird/abc/seg.log",
                          "topics/nested/0/deeper/x", "topics/empty", "elsewhere/0/seg.log"})
        EXPECT_TRUE(fs::exists(d.path / p)) << p;
}

TEST(RuntimeTopicRecoveryRegression, SymlinkOutsideDataDirIsNeverFollowedRegression) {
    TempDataDir d("symlink");
    TempDataDir outside("symlink-outside");
    touchFile(outside.path / "victim" / "topic-id", "2\n");                  // looks like an orphan...
    touchFile(outside.path / "victim" / "0" / "00000000000000000000.log");   // ...OUTSIDE dataDir
    fs::create_directories(d.path / "topics");
    fs::create_directory_symlink(outside.path / "victim", d.path / "topics" / "linked");
    touchFile(d.path / "topics" / "mixed" / "0" / "seg.log");
    fs::create_symlink(outside.path / "victim" / "0" / "00000000000000000000.log",
                       d.path / "topics" / "mixed" / "0" / "link.log");      // a symlink inside a partition
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, kNow);
    EXPECT_TRUE(r.deleted.empty()) << "a symlinked dir is never deleted";
    EXPECT_TRUE(r.quarantined.empty()) << "nor moved; a dir holding a symlink is not a topic layout";
    EXPECT_TRUE(fs::exists(outside.path / "victim" / "0" / "00000000000000000000.log"));
    EXPECT_TRUE(fs::is_symlink(d.path / "topics" / "linked"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "mixed" / "0" / "seg.log"));
}

TEST(RuntimeTopicRecoveryRegression, SymlinkedTopicsOrQuarantineRootIsNeverFollowedRegression) {
    {
        TempDataDir d("symlink-root");
        TempDataDir outside("symlink-root-outside");
        touchFile(outside.path / "gone" / "0" / "seg.log");
        fs::create_directory_symlink(outside.path, d.path / "topics");
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
        EXPECT_TRUE(r.deleted.empty());
        EXPECT_TRUE(r.quarantined.empty());
        EXPECT_TRUE(fs::exists(outside.path / "gone" / "0" / "seg.log"));
    }
    {
        TempDataDir d("symlink-q");
        TempDataDir outside("symlink-q-outside");
        touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
        fs::create_directory_symlink(outside.path, d.path / "quarantine");
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
        EXPECT_TRUE(r.quarantined.empty()) << "never moves data through a symlinked quarantine";
        EXPECT_TRUE(fs::exists(d.path / "topics" / "gone" / "0" / "seg.log"));
        EXPECT_TRUE(fs::is_empty(outside.path));
    }
}

TEST(RuntimeTopicRecoveryRegression, CaseVariantOfAClaimedNameIsKeptRegression) {
    TempDataDir d("case-claim");
    touchFile(d.path / "topics" / "Orders" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("orders", 0)}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty()) << "on a case-insensitive filesystem it may be 'orders' own dir";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "Orders" / "0" / "seg.log"));
}

TEST(RuntimeTopicRecoveryRegression, MissingTopicsDirIsANoOpRegression) {
    TempDataDir d("no-topics");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics")) << "nothing is created";
    EXPECT_FALSE(fs::exists(d.path / "quarantine"));
}
```

Add `#include <fstream>`, `#include <string_view>`, `#include <rune/gateway/TopicRegistry.h>` and `#include <rune/storage/Log.h>` (if not already present) to the test's includes.

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_node_tests`
  Expected: compile errors `no member named 'reconcileTopicDirs'` and `no member named 'kTopicIdMarkerFile'`.

- [ ] **Step 3: Implement.**

`TopicRegistry.h`, after the includes, inside `namespace rune::gateway {` and before `class TopicRegistry`:

```cpp
/// Written by createTopic into dataDir/topics/<name>/ for a disk topic: the topic id
/// in decimal.  Boot (rune::node::reconcileTopicDirs) uses it to tie an unclaimed
/// directory to an __orphan.<id> placeholder — only such a dir is ever deleted.
inline constexpr const char* kTopicIdMarkerFile = "topic-id";
```

In the `createTopic` doc comment, after `// Idempotent: …`, add the line `    // Writes dataDir/topics/<name>/topic-id (the assigned id) for a disk topic.`

`TopicRegistry.cpp`: add `#include <fstream>`. In `createTopic`, immediately before `    nameIndex_.emplace(spec.name, assignedId);`, add:

```cpp
    // B10 / D4: the topic-id marker ties this directory to its id, so boot can tell an
    // orphan placeholder's dir (deleted) from any other unclaimed dir (quarantined).
    if (spec.persistence != rune::node::PersistenceMode::Transient) {
        const fs::path marker = root / "topics" / spec.name / kTopicIdMarkerFile;
        std::ofstream out(marker, std::ios::trunc);
        out << assignedId << '\n';
        if (!out) spdlog::warn("TopicRegistry: cannot write topic-id marker '{}'", marker.string());
    }
```

`RuntimeTopicRecovery.h`: add `#include <filesystem>`, then after the `orphanPlaceholderName` declaration add the `TopicDirReconciliation` struct and the `reconcileTopicDirs` declaration exactly as in **Interfaces**, preceded by this doc comment:

```cpp
/// B10 / user decision D4: reconcile dataDir/topics/ against the full catalog
/// (`topics` = static + recovered, incl. __orphan.<id> placeholders).  For each
/// direct child of canonical(dataDir/topics) that no topic claims (exactly or
/// ignoring ASCII case) and that has the recognised layout
/// <name>/[topic-id] + <digits>/<regular files>:
///   * its topic-id marker names an __orphan.<id> placeholder -> DELETED;
///   * otherwise (no marker, other id, unreadable) -> MOVED to
///     dataDir/quarantine/<name>-<nowUnixSeconds>[-N], never deleted.
/// Symlinks are never followed (a symlinked topics/ or quarantine/ root, child,
/// partition dir or file makes the entry "not ours": left alone).  Each action is
/// logged at warn with its path(s).  Never throws.  Call on the startup thread
/// after the recovered specs are appended and BEFORE the TopicRegistry opens logs.
```

`RuntimeTopicRecovery.cpp`: add the includes. Inside the anonymous namespace at the top (next to `forEachCatalogValue`), add:

```cpp
bool allDigits(const std::string& s) {
    if (s.empty()) return false;
    for (const char c : s) if (c < '0' || c > '9') return false;
    return true;
}

/// <topic>/[topic-id] + <digits>/<regular files>: at least one partition dir, no
/// symlink and no sub-directory below a partition.  Anything else is not ours.
bool isTopicDirLayout(const fs::path& topicDir) {
    std::error_code ec;
    bool sawPartition = false;
    for (fs::directory_iterator it(topicDir, ec), end; !ec && it != end; it.increment(ec)) {
        const fs::file_status st = it->symlink_status(ec);
        if (ec) return false;
        const std::string child = it->path().filename().string();
        if (child == rune::gateway::kTopicIdMarkerFile && fs::is_regular_file(st)) continue;
        if (!fs::is_directory(st) || !allDigits(child)) return false;
        std::error_code pec;
        for (fs::directory_iterator pit(it->path(), pec), pend; !pec && pit != pend; pit.increment(pec)) {
            const fs::file_status pst = pit->symlink_status(pec);
            if (pec || !fs::is_regular_file(pst)) return false;
        }
        if (pec) return false;
        sawPartition = true;
    }
    return !ec && sawPartition;
}

/// The id in <topicDir>/topic-id, or -1 when absent / unreadable / not a number.
int32_t readTopicIdMarker(const fs::path& topicDir) {
    std::ifstream in(topicDir / rune::gateway::kTopicIdMarkerFile);
    int64_t id = -1;
    if (!(in >> id) || id < 0 || id > INT32_MAX) return -1;
    return static_cast<int32_t>(id);
}
```

After `orphanPlaceholderName`'s definition, add:

```cpp
TopicDirReconciliation reconcileTopicDirs(const std::string& dataDir,
                                          const std::vector<TopicSpec>& topics,
                                          int64_t nowUnixSeconds) {
    TopicDirReconciliation result;
    std::error_code ec;
    const fs::path topicsRoot = fs::path(dataDir) / "topics";
    const fs::file_status rootSt = fs::symlink_status(topicsRoot, ec);
    if (ec || !fs::is_directory(rootSt)) return result;   // absent, or a symlink: never followed
    const fs::path canonicalRoot = fs::canonical(topicsRoot, ec);
    if (ec) return result;

    std::set<int32_t> placeholderIds;
    for (const TopicSpec& t : topics)
        if (t.name == orphanPlaceholderName(t.topicId)) placeholderIds.insert(t.topicId);
    auto claimed = [&](const std::string& n) {
        for (const TopicSpec& t : topics)
            if (t.name == n || topicNamesFoldEqual(t.name, n)) return true;
        return false;
    };

    std::vector<fs::path> candidates;
    for (fs::directory_iterator it(topicsRoot, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code sec;
        const fs::file_status st = it->symlink_status(sec);
        if (sec || fs::is_symlink(st) || !fs::is_directory(st)) continue;   // not a topic dir: left alone
        if (claimed(it->path().filename().string())) continue;
        if (!isTopicDirLayout(it->path())) {
            spdlog::info("topic-dir reconcile: leaving '{}' alone (not a recognised topic dir layout)",
                         it->path().string());
            continue;
        }
        candidates.push_back(it->path());
    }

    const fs::path quarantineRoot = fs::path(dataDir) / "quarantine";
    for (const fs::path& p : candidates) {
        std::error_code cec;
        const fs::path c = fs::canonical(p, cec);
        if (cec || c.parent_path() != canonicalRoot) {   // guard: a direct child of dataDir/topics only
            spdlog::error("topic-dir reconcile: refusing to touch '{}' (outside '{}')",
                          p.string(), canonicalRoot.string());
            continue;
        }
        const int32_t markerId = readTopicIdMarker(c);
        if (markerId >= 0 && placeholderIds.count(markerId) != 0) {
            spdlog::warn("topic-dir reconcile: deleting '{}' — the data dir of orphan placeholder '{}' "
                         "(a definition that never committed)", c.string(), orphanPlaceholderName(markerId));
            std::error_code rec;
            fs::remove_all(c, rec);   // never follows symlinks; the layout check excluded them anyway
            if (rec) spdlog::error("topic-dir reconcile: deleting '{}' failed: {}", c.string(), rec.message());
            else result.deleted.push_back(c);
            continue;
        }
        // Everything else is MOVED, never deleted (D4).
        std::error_code qec;
        const fs::file_status qst = fs::symlink_status(quarantineRoot, qec);
        if (fs::is_symlink(qst) || (fs::exists(qst) && !fs::is_directory(qst))) {
            spdlog::error("topic-dir reconcile: '{}' is not a plain directory — not quarantining '{}'",
                          quarantineRoot.string(), c.string());
            continue;
        }
        fs::create_directories(quarantineRoot, qec);
        if (qec) {
            spdlog::error("topic-dir reconcile: cannot create '{}': {}", quarantineRoot.string(), qec.message());
            continue;
        }
        const std::string base = c.filename().string() + "-" + std::to_string(nowUnixSeconds);
        fs::path dest = quarantineRoot / base;
        for (int n = 1; fs::exists(fs::symlink_status(dest, qec)); ++n)
            dest = quarantineRoot / (base + "-" + std::to_string(n));
        std::error_code mec;
        fs::rename(c, dest, mec);   // same filesystem (both under dataDir): atomic, data untouched
        if (mec) {
            spdlog::error("topic-dir reconcile: moving '{}' to '{}' failed: {}", c.string(), dest.string(),
                          mec.message());
            continue;
        }
        spdlog::warn("topic-dir reconcile: quarantined '{}' -> '{}' — no topic in the catalog claims it "
                     "(removed from RUNE_TOPICS, a typo, or another build's dir); it is NOT restored "
                     "automatically", c.string(), dest.string());
        result.quarantined.push_back(dest);
    }
    return result;
}
```

Add `#include <climits>` for `INT32_MAX`.

`BrokerNode.cpp`: in the boot block, immediately after the closing `}` of `if (!recovered.empty()) { … }` (still inside the enclosing block that calls `recoverRuntimeTopicSpecs`), add:

```cpp
        // B10 / D4: delete only orphan-placeholder topic dirs; quarantine every other
        // unclaimed topic dir (never delete it) — BEFORE the TopicRegistry opens logs.
        (void)reconcileTopicDirs(config_.dataDir, config_.topics,
                                 std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::system_clock::now().time_since_epoch()).count());
```

- [ ] **Step 4: Run the tests and confirm they pass.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_node_tests rune_broker_node_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_node_tests --gtest_filter='RuntimeTopicRecoveryRegression.*'`
  Expected: all PASS, including the 10 new tests.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/gateway/include/rune/gateway/TopicRegistry.h libs/gateway/src/TopicRegistry.cpp libs/node/include/rune/node/RuntimeTopicRecovery.h libs/node/src/RuntimeTopicRecovery.cpp libs/node/src/BrokerNode.cpp libs/node/tests/node/RuntimeTopicRecoveryTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "fix(recovery): boot deletes only orphan-placeholder topic dirs and quarantines every other unclaimed dir (never deletes it); topic-id marker (B10)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: AdminWorker introspection + `DdlExpectations.h` + `DdlHarness.h` + harness smoke tests

**Files:**
- Modify: `libs/node/include/rune/node/AdminWorker.h`: public introspection after `rebuildFailed()` (~L214); member `started_` after `nextTicket_` (~L347)
- Modify: `libs/node/src/AdminWorker.cpp`: first line of `startNext_` (~L405)
- Create: `libs/node/tests/support/DdlExpectations.h`
- Create: `libs/node/tests/support/DdlHarness.h`
- Create: `libs/node/tests/node/DdlHarnessRegressionTest.cpp`
- Modify: `libs/node/CMakeLists.txt`: new target after the `gtest_discover_tests(rune_create_view_tests PROPERTIES TIMEOUT 60)` block (~L545)

**Interfaces:**
- Produces (AdminWorker, public, `noexcept`, no allocation):
  - `bool hasInFlight() const`
  - `uint64_t startedCount() const`: queued DDL ops dequeued by `startNext_`; each is answered exactly once.
  - `static constexpr int64_t ddlTimeoutNs()`
- Produces (`rune::node::test`, `DdlExpectations.h`):
  - error-code constants `kOk … kInvalidTopicName`, `kViewSourceIsView`
  - `enum class Op` (12 values, incl. `AlterRenameKey`), `kOpCount`, `kAllOps[]`, `opName(Op)`
  - `isCreate` / `isDefine` / `isView` / `isAlter`
  - `enum class Occupant` (10 values), `occupantName`
  - `struct Cell { uint16_t code; uint8_t createRequests; }`
  - `constexpr Cell expectedCell(Occupant, Op)`
  - `constexpr Occupant occupantAfterSuccess(Occupant, Op)`
  - `constexpr Occupant orphanOf(Op)`
- Produces (`DdlHarness.h`):
  - payload helpers: `charBlob()`, `viewPlanBlob(name, srcId, persistence)`, `alterOpsFor(Op, uniq)`
  - catalog types: `EntryKind`, `CatalogEntry{id, name, kind, persistence, version, partitions, fingerprint, keyField}` (`operator==`, `PrintTo`), `Catalog`, `findEntry`, `Response{op, corr, id, version, err}`
  - options and modes: `ControlMode{Auto, Hold}`, `DdlHarnessOptions{statics, holdCommits, withSources}`
  - `class DdlHarness` with these members:

    | Group | Members |
    |---|---|
    | submit | `submit(Op, name, viewSource="src") -> corr`, `submitRawDefine`, `submitRawView`, `submitRawAlter` |
    | drive | `tick`, `settle`, `tickUntilCreatePending`, `tickUntilAppended` |
    | control | `setControl`, `hasPendingCreate`, `deliverCreate`, `failCreate(afterCreate)` |
    | commits / time | `releaseCommits`, `holdingCommits`, `advance` |
    | leadership / restart | `demote`, `promote`, `restart`, `quiescent` |
    | observe | `catalog`, `expectedAfterRestart(Catalog)`, `response(corr)`, `responses`, `violations`, `violationText` |
    | counters | `createRequests`, `viewPlanPushes`, `validatorPushes`, `schemaLogEnd`, `viewsLogEnd`, `topicCount`, `idOf`, `startedTotal`, `now`, `root` |
    | accessors | `worker`, `schemas`, `views`, `topics` |

- [ ] **Step 1: Write the failing smoke tests** `libs/node/tests/node/DdlHarnessRegressionTest.cpp`:

```cpp
// DdlHarnessRegressionTest.cpp — DDL conformance plan: smoke tests of DdlHarness
// itself (Task 4) and the targeted regression for every fix the matrices exposed
// (B1-B4, KL2: Tasks 5-7).  Every suite name contains "Regression".
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

#include <filesystem>
#include <string>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;

TEST(DdlHarnessRegression, DefineViewAlterRoundTripAndRestartKeepsTheCatalog) {
    DdlHarness h;
    const int64_t d = h.submit(Op::DefineDisk, "t");
    h.settle();
    const int64_t v = h.submit(Op::ViewPersisted, "v");
    h.settle();
    const int64_t a = h.submit(Op::AlterAdd, "t");
    h.settle();
    ASSERT_EQ(h.response(d)->err, kOk);
    ASSERT_EQ(h.response(v)->err, kOk);
    ASSERT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.response(a)->version, 2u);
    const Catalog before = h.catalog();
    ASSERT_NE(findEntry(before, "v"), nullptr);
    EXPECT_EQ(findEntry(before, "v")->kind, EntryKind::View);
    h.restart();
    EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlHarnessRegression, HeldControlReplyKeepsTheNextOpQueued) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t x = h.submit(Op::DefineDisk, "x");
    const int64_t y = h.submit(Op::DefineDisk, "y");
    ASSERT_TRUE(h.tickUntilCreatePending());
    const int reqs = h.createRequests();
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_EQ(h.createRequests(), reqs) << "y must not start while x is AwaitingId";
    EXPECT_FALSE(h.response(y).has_value());
    h.deliverCreate();
    h.setControl(ControlMode::Auto);
    h.settle();
    EXPECT_EQ(h.response(x)->err, kOk);
    EXPECT_EQ(h.response(y)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlHarnessRegression, HeldCommitKeepsTheOpInFlightUntilReleased) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_FALSE(h.response(a).has_value()) << "an op is answered only after its record commits";
    EXPECT_TRUE(h.worker().hasInFlight());
    h.settle();   // releases commits
    EXPECT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.response(a)->version, 2u);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlHarnessRegression, OrphanThenLaterTopicRestartsWithAPlaceholder) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t o = h.submit(Op::ViewPersisted, "orph");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.failCreate(/*afterCreate=*/true);
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_EQ(h.response(o)->err, kInternal);
    const int32_t orphanId = h.idOf("orph");
    ASSERT_GE(orphanId, 0) << "the control thread created the sink: an orphan";
    const int64_t l = h.submit(Op::DefineDisk, "later");
    h.settle();
    ASSERT_EQ(h.response(l)->err, kOk);
    const Catalog before = h.catalog();
    h.restart();   // B5: must not throw
    EXPECT_EQ(h.catalog(), h.expectedAfterRestart(before));
    EXPECT_EQ(h.idOf("orph"), -1) << "the orphan's name is free after the restart";
    EXPECT_EQ(h.idOf(orphanPlaceholderName(orphanId)), orphanId);
    EXPECT_EQ(h.topics().topic(orphanId)->partitionCount, 0) << "hidden, inert placeholder (D1)";
    EXPECT_FALSE(fs::exists(h.root() / "topics" / "orph")) << "the orphan sink's data dir is deleted (B10)";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}
```

- [ ] **Step 2: Add the target and confirm it fails.** In `libs/node/CMakeLists.txt`, immediately after the `gtest_discover_tests(rune_create_view_tests` … `PROPERTIES TIMEOUT 60)` block, add:

```cmake
    # ---------------------------------------------------------------------------
    # DDL conformance plan: DdlHarness suites — fix regressions (B1-B4, KL2), the
    # Occupant x Op matrix and the pair x timing matrix.  On-host (no Aeron),
    # deterministic.  Every suite name contains "Regression".
    # ---------------------------------------------------------------------------
    add_executable(rune_ddl_conformance_tests
        tests/node/DdlHarnessRegressionTest.cpp)
    target_include_directories(rune_ddl_conformance_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_ddl_conformance_tests PRIVATE
        rune_node
        rune_view
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_ddl_conformance_tests
        DISCOVERY_TIMEOUT 120
        PROPERTIES TIMEOUT 120)
```

Run: `cmake /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests`
Expected: FAIL, `'support/DdlHarness.h' file not found`.

- [ ] **Step 3: AdminWorker introspection.** In `AdminWorker.h`, after the `rebuildFailed()` accessor, add:

```cpp
    // DDL conformance (tests): with startedCount() a test proves AT MOST ONE op is
    // ever started-but-unanswered — started - answered == (hasInFlight() ? 1 : 0).
    [[nodiscard]] bool hasInFlight() const noexcept { return inFlight_.has_value(); }
    /// Queued DDL ops dequeued by startNext_ (each is answered exactly once,
    /// synchronously or later).  Monotone; admin-thread-owned.
    [[nodiscard]] uint64_t startedCount() const noexcept { return started_; }
    /// The in-flight error-path bound (see kDdlTimeoutNs).
    [[nodiscard]] static constexpr int64_t ddlTimeoutNs() noexcept { return kDdlTimeoutNs; }
```

After `    int64_t         nextTicket_ = 1;` add:

```cpp
    uint64_t        started_    = 0;    // ops dequeued by startNext_ (startedCount())
```

In `AdminWorker.cpp`, make the first statement of `void AdminWorker::startNext_(int64_t nowNs) {`:

```cpp
    ++started_;
```

- [ ] **Step 4: Create** `libs/node/tests/support/DdlExpectations.h`:

```cpp
#pragma once
// DdlExpectations.h — DDL conformance plan: THE expectation table.
//
// Occupant (what the target name names right now) x Op -> the exact error code and
// the number of requestCreateTopic calls the op makes.  Every unit matrix, the
// stress model and the BrokerNode burst read THIS table; the plan's markdown table
// is a copy of it.  A "control-detected" rejection (the admin thread cannot see
// schema-less topics, the control thread can) makes exactly one request and
// creates nothing.  Test-only.
#include <cstdint>

namespace rune::node::test {

// rune::protocol::client::ErrorCode values (ints: protocol-agnostic, like AdminWorker).
inline constexpr uint16_t kOk                  = 0;
inline constexpr uint16_t kInternal            = 6;
inline constexpr uint16_t kUnknownTopic        = 7;
inline constexpr uint16_t kInvalidSchema       = 23;
inline constexpr uint16_t kTopicExists         = 25;
inline constexpr uint16_t kSchemaConflict      = 31;
inline constexpr uint16_t kViewExists          = 32;
inline constexpr uint16_t kViewSourceNotFound  = 33;
inline constexpr uint16_t kViewFieldUnresolved = 34;
inline constexpr uint16_t kViewSourceTransient = 37;
inline constexpr uint16_t kViewNotAlterable    = 38;
inline constexpr uint16_t kInvalidTopicName    = 40;
inline constexpr uint16_t kViewSourceIsView     = 41;

enum class Op : uint8_t {
    DefineDisk, DefineTransient, ViewPersisted, ViewTransient,
    AlterAdd, AlterDrop, AlterRename, AlterRenameKey, AlterSetCompat, AlterCasCurrent, AlterCasStale,
    GetSchema,
};
inline constexpr int kOpCount = 12;
inline constexpr Op kAllOps[kOpCount] = {
    Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
    Op::AlterAdd, Op::AlterDrop, Op::AlterRename, Op::AlterRenameKey, Op::AlterSetCompat,
    Op::AlterCasCurrent, Op::AlterCasStale, Op::GetSchema,
};

[[nodiscard]] inline const char* opName(Op o) noexcept {
    switch (o) {
        case Op::DefineDisk:      return "DefineDisk";
        case Op::DefineTransient: return "DefineTransient";
        case Op::ViewPersisted:   return "ViewPersisted";
        case Op::ViewTransient:   return "ViewTransient";
        case Op::AlterAdd:        return "AlterAdd";
        case Op::AlterDrop:       return "AlterDrop";
        case Op::AlterRename:     return "AlterRename";
        case Op::AlterRenameKey:  return "AlterRenameKey";
        case Op::AlterSetCompat:  return "AlterSetCompat";
        case Op::AlterCasCurrent: return "AlterCasCurrent";
        case Op::AlterCasStale:   return "AlterCasStale";
        case Op::GetSchema:       return "GetSchema";
    }
    return "?";
}
[[nodiscard]] constexpr bool isDefine(Op o) noexcept { return o == Op::DefineDisk || o == Op::DefineTransient; }
[[nodiscard]] constexpr bool isView(Op o) noexcept { return o == Op::ViewPersisted || o == Op::ViewTransient; }
[[nodiscard]] constexpr bool isCreate(Op o) noexcept { return isDefine(o) || isView(o); }
[[nodiscard]] constexpr bool isAlter(Op o) noexcept { return o >= Op::AlterAdd && o <= Op::AlterCasStale; }

enum class Occupant : uint8_t {
    Free, TypedDisk, TypedTransient, ViewPersisted, ViewTransient,
    SchemaLessDisk, SchemaLessTransient, OrphanSink, CaseVariant, BadName,
};
inline constexpr int kOccupantCount = 10;

[[nodiscard]] inline const char* occupantName(Occupant o) noexcept {
    switch (o) {
        case Occupant::Free:                return "Free";
        case Occupant::TypedDisk:           return "TypedDisk";
        case Occupant::TypedTransient:      return "TypedTransient";
        case Occupant::ViewPersisted:       return "ViewPersisted";
        case Occupant::ViewTransient:       return "ViewTransient";
        case Occupant::SchemaLessDisk:      return "SchemaLessDisk";
        case Occupant::SchemaLessTransient: return "SchemaLessTransient";
        case Occupant::OrphanSink:          return "OrphanSink";
        case Occupant::CaseVariant:         return "CaseVariant";
        case Occupant::BadName:             return "BadName";
    }
    return "?";
}

struct Cell { uint16_t code; uint8_t createRequests; };

// Columns: DefD DefT ViewP ViewT Add Drop Rename RenameKey SetCompat CasCur CasStale Get
inline constexpr uint16_t kExpectedCode[kOccupantCount][kOpCount] = {
    /* Free                */ { 0,  0,  0,  0,  7,  7,  7,  7,  7,  7,  7,  7},
    /* TypedDisk           */ {25, 25, 25, 25,  0,  0,  0,  0,  0,  0, 31,  0},
    /* TypedTransient      */ {25, 25, 25, 25,  0,  0,  0,  0,  0,  0, 31,  0},
    /* ViewPersisted       */ {25, 25, 32, 32, 38, 38, 38, 38, 38, 38, 38,  0},
    /* ViewTransient       */ {25, 25, 32, 32, 38, 38, 38, 38, 38, 38, 38,  0},
    /* SchemaLessDisk      */ { 0, 25, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7},
    /* SchemaLessTransient */ { 0,  0, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7},
    /* OrphanSink (KL1)    */ { 0, 25, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7},
    /* CaseVariant (B3)    */ {25, 25, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7},
    /* BadName (B1/B2)     */ {40, 40, 40, 40,  7,  7,  7,  7,  7,  7,  7,  7},
};
inline constexpr uint8_t kExpectedCreateRequests[kOccupantCount][kOpCount] = {
    /* Free                */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* TypedDisk           */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    /* TypedTransient      */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    /* ViewPersisted       */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    /* ViewTransient       */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    /* SchemaLessDisk      */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* SchemaLessTransient */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* OrphanSink (KL1)    */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* CaseVariant (B3)    */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* BadName (B1/B2)     */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
};

[[nodiscard]] constexpr Cell expectedCell(Occupant o, Op op) noexcept {
    return Cell{kExpectedCode[static_cast<int>(o)][static_cast<int>(op)],
                kExpectedCreateRequests[static_cast<int>(o)][static_cast<int>(op)]};
}

/// What the name names after a SUCCESSFUL `op` on occupant `before`.  A define over
/// a schema-less transient topic adopts it and records Transient; alters and reads
/// keep the kind.
[[nodiscard]] constexpr Occupant occupantAfterSuccess(Occupant before, Op op) noexcept {
    switch (op) {
        case Op::DefineDisk:
            return before == Occupant::SchemaLessTransient ? Occupant::TypedTransient : Occupant::TypedDisk;
        case Op::DefineTransient: return Occupant::TypedTransient;
        case Op::ViewPersisted:   return Occupant::ViewPersisted;
        case Op::ViewTransient:   return Occupant::ViewTransient;
        default:                  return before;
    }
}

/// What a create leaves behind when the control thread created its topic but the
/// op never committed (reply failed after create / timeout / demotion).
[[nodiscard]] constexpr Occupant orphanOf(Op op) noexcept {
    switch (op) {
        case Op::DefineDisk:      return Occupant::SchemaLessDisk;
        case Op::DefineTransient: return Occupant::SchemaLessTransient;
        default:                  return Occupant::OrphanSink;   // a view sink is created Disk (Part E)
    }
}

} // namespace rune::node::test
```

- [ ] **Step 5: Create** `libs/node/tests/support/DdlHarness.h`:

```cpp
#pragma once
// DdlHarness.h — DDL conformance plan: the conformance harness.
//
// Extends AdminWorkerTest's fake-control harness pattern (a real __topic_schemas
// replica + an AdminWorker whose control thread is played by the test) with:
//   * the __views replica + ViewRegistry (CREATE VIEW);
//   * a REAL TopicRegistry answered by createTopicForAdmin — BrokerNode's own
//     control-thread function (idempotent by name, authoritative count/mode);
//   * control replies the test can hold, fail (before/after creating) or deliver
//     late (a stale ticket after a timeout / demotion);
//   * commits the test can hold: replica set {1,2}, the HW waits for follower 2's
//     fetch (releaseCommits);
//   * demote / promote, and a restart that boots like BrokerNode
//     (recoverRuntimeTopicSpecs -> fresh TopicRegistry -> fresh registries ->
//     promotion replays __topic_schemas and __views).
//
// After EVERY admin step it checks the serialization invariant: started-but-
// unanswered DDL ops == (hasInFlight() ? 1 : 0); no requestCreateTopic while an op
// is in flight; DDL answers in FIFO submission order.  Violations are recorded,
// never thrown: every test asserts violations().empty().
//
// restart() runs recoverRuntimeTopicSpecs + reconcileTopicDirs exactly as
// BrokerNode::start does, then always reboots single-node (replica set {1}): promotion replays the
// catalogs up to the HW, which a single-node leader advances to its LEO.
//
// Single-threaded, test-only.  Must NOT be included from production code.
#include "DdlExpectations.h"

#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminCreateTopic.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/PersistenceMode.h>
#include <rune/node/RuntimeTopicRecovery.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/view/ViewPlan.h>

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace rune::node::test {

// ── payloads ────────────────────────────────────────────────────────────────
/// Every DefineTopic's descriptor: {id Int64 (key), hp Int64}.
inline std::vector<uint8_t> charBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("character", 1)
        .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", rune::codec::PrimType::Int64)
        .build());
}

/// Authored {id, hp} Replace view keyed by "id" over `srcTopicId`.
inline std::vector<uint8_t> viewPlanBlob(const std::string& viewName, int32_t srcTopicId,
                                         uint8_t persistence) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = viewName;
    p.key = "id";
    p.persistence = persistence;
    p.sources.push_back(rv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

/// The ALTER ops blob for `op` (uniq makes every ADD's field name fresh).
inline std::vector<uint8_t> alterOpsFor(Op op, uint64_t uniq) {
    rune::codec::AlterOp o{};
    switch (op) {
        case Op::AlterDrop:
            o.type = rune::codec::AlterOpType::Drop; o.name = "hp"; break;
        case Op::AlterRename:
            o.type = rune::codec::AlterOpType::Rename; o.name = "hp"; o.newName = "health"; break;
        case Op::AlterRenameKey:   // the key field: TopicSchema.keyField must follow (B8)
            o.type = rune::codec::AlterOpType::Rename; o.name = "id"; o.newName = "pk"; break;
        case Op::AlterSetCompat:
            o.type = rune::codec::AlterOpType::SetCompatibility; o.mode = rune::codec::CompatMode::Forward; break;
        default:
            o.type = rune::codec::AlterOpType::Add; o.name = "extra" + std::to_string(uniq);
            o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0; break;
    }
    return rune::codec::encodeAlterOps({o});
}

// ── catalog snapshot ────────────────────────────────────────────────────────
enum class EntryKind : uint8_t { SchemaLess, Typed, View };

/// One TopicRegistry id with what the admin catalogs say about it.
struct CatalogEntry {
    int32_t         id = -1;
    std::string     name;
    EntryKind       kind = EntryKind::SchemaLess;
    PersistenceMode persistence = PersistenceMode::Disk;
    uint16_t        version = 0;
    int32_t         partitions = 0;
    uint64_t        fingerprint = 0;
    std::string     keyField;   // the schema's key field ("" when schema-less)
    bool operator==(const CatalogEntry&) const = default;
};
using Catalog = std::vector<CatalogEntry>;   // in topic-id order

inline void PrintTo(const CatalogEntry& e, std::ostream* os) {
    *os << "{id=" << e.id << " name='" << e.name << "' kind="
        << (e.kind == EntryKind::Typed ? "Typed" : e.kind == EntryKind::View ? "View" : "SchemaLess")
        << " persistence=" << (e.persistence == PersistenceMode::Transient ? "Transient" : "Disk")
        << " v" << e.version << " parts=" << e.partitions << " fp=" << e.fingerprint
        << " key='" << e.keyField << "'}";
}

[[nodiscard]] inline const CatalogEntry* findEntry(const Catalog& c, const std::string& name) {
    for (const CatalogEntry& e : c) if (e.name == name) return &e;
    return nullptr;
}

struct Response { Op op; int64_t corr; int32_t id; uint16_t version; uint16_t err; };

enum class ControlMode : uint8_t { Auto, Hold };

struct DdlHarnessOptions {
    std::vector<TopicSpec> statics;   // RUNE_TOPICS-style boot topics (schema-less), ids 0..
    bool holdCommits = false;         // replica set {1,2}: commits wait for releaseCommits()
    bool withSources = true;          // pre-define "src" and "src2" (typed, disk, 1 partition)
};

class DdlHarness {
public:
    explicit DdlHarness(DdlHarnessOptions o = {}) : opt_(std::move(o)), root_(makeRoot_()) {
        boot_();
        if (opt_.withSources) {
            for (const char* n : {"src", "src2"}) {
                const int64_t c = submit(Op::DefineDisk, n);
                settle();
                const std::optional<Response> r = response(c);
                if (!r || r->err != kOk) violations_.push_back(std::string("setup: define ") + n + " failed");
            }
        }
    }
    ~DdlHarness() {
        teardown_();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    DdlHarness(const DdlHarness&) = delete;
    DdlHarness& operator=(const DdlHarness&) = delete;

    // ── submit (returns the op's correlation id) ────────────────────────────
    int64_t submit(Op op, const std::string& name, const std::string& viewSource = "src") {
        const int64_t corr = nextCorr_(op);
        switch (op) {
            case Op::DefineDisk:
            case Op::DefineTransient: {
                DefineTopicMsg m;
                m.session = 1; m.corr = corr; m.partitionCount = 1; m.validate = false;
                m.name = name; m.keyField = "id"; m.blob = charBlob();
                m.persistence = op == Op::DefineTransient ? 1 : 0;
                w_->submitDefineTopic(std::move(m));
                break;
            }
            case Op::ViewPersisted:
            case Op::ViewTransient: {
                CreateViewMsg m;
                m.session = 1; m.corr = corr; m.persistence = op == Op::ViewTransient ? 1 : 0;
                m.viewName = name; m.keyField = "id";
                const RegistryEntry* s = schemas_->byName(viewSource);
                m.planBlob = viewPlanBlob(name, s ? s->topicId : -1, m.persistence);
                w_->submitCreateView(std::move(m));
                break;
            }
            case Op::GetSchema: {
                GetSchemaMsg m;
                m.session = 1; m.corr = corr; m.name = name;
                w_->submitGetSchema(m);   // answered synchronously (a pure read)
                break;
            }
            default: {
                AlterTopicMsg m;
                m.session = 1; m.corr = corr; m.topicName = name;
                if (op == Op::AlterCasCurrent) {
                    const RegistryEntry* e = schemas_->byName(name);
                    m.expectedVersion = e ? e->version : 1;   // read at SUBMIT time
                } else if (op == Op::AlterCasStale) {
                    m.expectedVersion = 99;
                }
                m.opsBlob = alterOpsFor(op, static_cast<uint64_t>(corr));
                w_->submitAlterTopic(std::move(m));
                break;
            }
        }
        return corr;
    }
    /// Malformed / hand-built requests: the harness only stamps session + corr.
    int64_t submitRawDefine(DefineTopicMsg m) {
        const int64_t corr = nextCorr_(Op::DefineDisk);
        m.session = 1; m.corr = corr;
        w_->submitDefineTopic(std::move(m));
        return corr;
    }
    int64_t submitRawView(CreateViewMsg m) {
        const int64_t corr = nextCorr_(Op::ViewPersisted);
        m.session = 1; m.corr = corr;
        w_->submitCreateView(std::move(m));
        return corr;
    }
    int64_t submitRawAlter(AlterTopicMsg m) {
        const int64_t corr = nextCorr_(Op::AlterAdd);
        m.session = 1; m.corr = corr;
        w_->submitAlterTopic(std::move(m));
        return corr;
    }

    // ── driving ─────────────────────────────────────────────────────────────
    void tick() {
        now_ += 1'000'000;
        w_->doWork(now_);
        checkInvariant_("doWork");
        if (control_ == ControlMode::Auto)
            while (!creates_.empty()) deliverCreate();
    }
    [[nodiscard]] bool quiescent() const {
        return w_->pendingCount() == 0 && !w_->hasInFlight() && creates_.empty();
    }
    /// Tick (releasing held commits) until quiescent.  Never call with a held
    /// create request outstanding — that is not quiescent by construction.
    void settle(int maxTicks = 400) {
        for (int i = 0; i < maxTicks; ++i) {
            releaseCommits();
            tick();
            if (quiescent()) { tick(); return; }
        }
        violations_.push_back("settle: not quiescent after " + std::to_string(maxTicks) + " ticks");
    }
    bool tickUntilCreatePending(int maxTicks = 50) {
        for (int i = 0; i < maxTicks && creates_.empty(); ++i) tick();
        return !creates_.empty();
    }
    bool tickUntilAppended(int maxTicks = 50) {
        const int64_t start = schemaLogEnd() + viewsLogEnd();
        for (int i = 0; i < maxTicks && schemaLogEnd() + viewsLogEnd() == start; ++i) tick();
        return schemaLogEnd() + viewsLogEnd() > start;
    }

    // ── the simulated control thread ────────────────────────────────────────
    void setControl(ControlMode m) noexcept { control_ = m; }
    [[nodiscard]] bool hasPendingCreate() const noexcept { return !creates_.empty(); }
    /// Answer the oldest create request exactly as BrokerNode's control thread does.
    /// A request whose op already ended (timeout / demotion) is a stale ticket: the
    /// worker ignores the reply, but the topic IS created (an orphan).
    void deliverCreate() {
        const CreateTopicReqMsg req = popCreate_();
        w_->onCreateTopicReply(createTopicForAdmin(*topics_, req));
        checkInvariant_("create reply");
    }
    /// Answer the oldest create request with a failure.  afterCreate=true runs the
    /// control thread's createTopic first (the reply is lost after the work: an orphan).
    void failCreate(bool afterCreate) {
        const CreateTopicReqMsg req = popCreate_();
        CreateTopicReplyMsg reply;
        if (afterCreate) reply = createTopicForAdmin(*topics_, req);
        reply.ticket  = req.ticket;
        reply.topicId = -1;
        reply.existed = false;
        w_->onCreateTopicReply(reply);
        checkInvariant_("failed create reply");
    }

    // ── commits / time / leadership / restart ───────────────────────────────
    /// holdCommits: follower 2 fetches at the leader's LEO -> the HW covers every
    /// appended record of both catalogs.
    void releaseCommits() {
        if (!opt_.holdCommits || !leader_) return;
        release_(*schemaReplica_, kSchemaTopicId);
        release_(*viewsReplica_, kViewsTopicId);
    }
    [[nodiscard]] bool holdingCommits() const noexcept { return opt_.holdCommits; }
    void advance(int64_t ns) noexcept { now_ += ns; }
    void demote() {
        ++epoch_;
        leader_ = false;
        w_->onRole(AdminRoleMsg{/*leader=*/false, epoch_, /*leaderId=*/2});
        checkInvariant_("demote");
    }
    void promote() {
        ++epoch_;
        leader_ = true;
        w_->onRole(AdminRoleMsg{/*leader=*/true, epoch_, /*leaderId=*/1});
        checkInvariant_("promote");
    }
    /// A broker restart (only when quiescent): reboot single-node from the same dataDir.
    void restart() {
        if (!quiescent()) violations_.push_back("restart requested while not quiescent");
        opt_.holdCommits = false;
        startedBeforeBoot_ += w_->startedCount();
        teardown_();
        boot_();
    }

    // ── observation ─────────────────────────────────────────────────────────
    [[nodiscard]] Catalog catalog() const {
        Catalog c;
        for (int32_t id = 0; id < topics_->topicCount(); ++id) {
            const TopicSpec* s = topics_->topic(id);
            CatalogEntry e;
            e.id = id; e.name = s->name; e.partitions = s->partitionCount; e.persistence = s->persistence;
            if (const ViewRecord* v = views_->byViewTopicId(id)) {
                e.kind = EntryKind::View;
                e.persistence = v->persistence == 1 ? PersistenceMode::Transient : PersistenceMode::Disk;
                if (const RegistryEntry* r = schemas_->byId(id)) {
                    e.version = r->version; e.fingerprint = r->fingerprint; e.keyField = r->keyField;
                }
            } else if (const RegistryEntry* r = schemas_->byId(id)) {
                e.kind = EntryKind::Typed;
                e.persistence = r->persistence;
                e.version = r->version;
                e.partitions = r->partitionCount;
                e.fingerprint = r->fingerprint;
                e.keyField = r->keyField;
            }
            c.push_back(std::move(e));
        }
        return c;
    }
    /// What a restart must produce from `before`: committed entries and static
    /// topics unchanged; a schema-less RUNTIME topic (an orphan) becomes the
    /// zero-partition placeholder __orphan.<id> when a later recorded topic exists
    /// (B5), and is gone when it is the tail.  On disk (B10): a placeholder's dir
    /// is deleted, a tail orphan's dir is quarantined (never deleted).
    [[nodiscard]] Catalog expectedAfterRestart(const Catalog& before) const {
        const int32_t firstRuntime = static_cast<int32_t>(opt_.statics.size());
        int32_t lastRecorded = -1;
        for (const CatalogEntry& e : before)
            if (e.id >= firstRuntime && e.kind != EntryKind::SchemaLess) lastRecorded = e.id;
        Catalog out;
        for (const CatalogEntry& e : before) {
            if (e.id < firstRuntime || e.kind != EntryKind::SchemaLess) { out.push_back(e); continue; }
            if (e.id > lastRecorded) continue;
            CatalogEntry p;
            p.id = e.id; p.name = orphanPlaceholderName(e.id); p.kind = EntryKind::SchemaLess;
            p.persistence = PersistenceMode::Transient; p.partitions = 0;   // hidden, inert (D1)
            out.push_back(std::move(p));
        }
        return out;
    }
    [[nodiscard]] std::optional<Response> response(int64_t corr) const {
        for (const Response& r : responses_) if (r.corr == corr) return r;
        return std::nullopt;
    }
    [[nodiscard]] const std::vector<Response>& responses() const noexcept { return responses_; }
    [[nodiscard]] const std::vector<std::string>& violations() const noexcept { return violations_; }
    [[nodiscard]] std::string violationText() const {
        std::string s;
        for (const std::string& v : violations_) s += v + "\n";
        return s;
    }
    [[nodiscard]] int createRequests() const noexcept { return createRequests_; }
    [[nodiscard]] int viewPlanPushes() const noexcept { return viewPlanPushes_; }
    [[nodiscard]] int validatorPushes() const noexcept { return validatorPushes_; }
    [[nodiscard]] int64_t schemaLogEnd() const { return schemaLog_->logEndOffset(); }
    [[nodiscard]] int64_t viewsLogEnd() const { return viewsLog_->logEndOffset(); }
    [[nodiscard]] int32_t topicCount() const { return topics_->topicCount(); }
    [[nodiscard]] int32_t idOf(const std::string& name) const { return topics_->topicIdForName(name).value_or(-1); }
    [[nodiscard]] uint64_t startedTotal() const noexcept { return startedBeforeBoot_ + w_->startedCount(); }
    [[nodiscard]] int64_t now() const noexcept { return now_; }
    [[nodiscard]] const std::filesystem::path& root() const noexcept { return root_; }
    AdminWorker& worker() { return *w_; }
    SchemaRegistry& schemas() { return *schemas_; }
    ViewRegistry& views() { return *views_; }
    rune::gateway::TopicRegistry& topics() { return *topics_; }

private:
    using PartitionReplica = rune::replication::PartitionReplica;

    static std::filesystem::path makeRoot_() {
        static uint64_t seq = 0;   // single-threaded test binary
        const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto p = std::filesystem::temp_directory_path() /
                 ("rune_ddl_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" + std::to_string(ts));
        std::filesystem::create_directories(p);
        return p;
    }

    rune::replication::PartitionConfig cfg_(int32_t topicId) const {
        rune::replication::PartitionConfig c;
        c.topicId = topicId; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxMs = 1'000'000'000LL;     // follower 2 never leaves the ISR on simulated time
        c.replicaLagMaxOffset = 1'000'000;
        c.fetchIntervalNs = 1'000'000LL; c.isrCheckIntervalNs = 1'000'000LL;
        c.maxFetchBytes = 1 << 20; c.pushOnAppend = false;
        c.cleanupPolicy = rune::replication::CleanupPolicy::Compact;
        return c;
    }

    void boot_() {
        std::vector<TopicSpec> specs = opt_.statics;
        for (TopicSpec& s : recoverRuntimeTopicSpecs(root_.string(), static_cast<int32_t>(opt_.statics.size())))
            specs.push_back(std::move(s));
        (void)reconcileTopicDirs(root_.string(), specs,         // B10: exactly as BrokerNode boots
                                 std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::system_clock::now().time_since_epoch()).count());
        topics_ = std::make_unique<rune::gateway::TopicRegistry>(specs, root_.string());

        std::filesystem::create_directories(root_ / "__topic_schemas");
        std::filesystem::create_directories(root_ / "__views");
        schemaLog_ = std::make_unique<rune::storage::Log>((root_ / "__topic_schemas").string());
        schemaLog_->recover();
        viewsLog_ = std::make_unique<rune::storage::Log>((root_ / "__views").string());
        viewsLog_->recover();

        using rune::replication::NodeId;
        const std::vector<NodeId> set = opt_.holdCommits ? std::vector<NodeId>{1, 2} : std::vector<NodeId>{1};
        schemaReplica_ = std::make_unique<PartitionReplica>(cfg_(kSchemaTopicId), rune::replication::Role::Follower,
                                                            0, set, set, schemaLog_.get(), &schemaTransport_);
        viewsReplica_ = std::make_unique<PartitionReplica>(cfg_(kViewsTopicId), rune::replication::Role::Follower,
                                                           0, set, set, viewsLog_.get(), &viewsTransport_);
        schemas_ = std::make_unique<SchemaRegistry>();
        views_   = std::make_unique<ViewRegistry>();

        AdminDeps d;
        d.requestCreateTopic = [this](int64_t ticket, int32_t parts, const std::string& name,
                                      const std::string& keyField, uint8_t cleanupPolicy,
                                      uint8_t persistence) -> bool {
            if (w_->hasInFlight())
                violations_.push_back("requestCreateTopic('" + name + "') while another op is in flight");
            CreateTopicReqMsg m;
            m.ticket = ticket; m.partitionCount = parts; m.name = name; m.keyField = keyField;
            m.cleanupPolicy = cleanupPolicy; m.persistence = persistence;
            creates_.push_back(std::move(m));
            ++createRequests_;
            return true;
        };
        d.pushValidators = [this](const RegistryEntry&, int32_t) { ++validatorPushes_; };
        d.pushViewPlan   = [this](const ViewPlanUpdateMsg&) { ++viewPlanPushes_; };
        d.respondDefine = [this](int64_t, int64_t corr, int32_t id, uint16_t v, uint16_t e) {
            onDdlAnswer_(corr, id, v, e);
        };
        d.respondCreateView = [this](int64_t, int64_t corr, int32_t id, uint64_t, uint16_t e) {
            onDdlAnswer_(corr, id, e == kOk ? 1 : 0, e);   // a view's output is v1
        };
        d.respondAlter = [this](int64_t, int64_t corr, int32_t id, uint16_t v, uint16_t e) {
            onDdlAnswer_(corr, id, v, e);
        };
        d.respondGet = [this](int64_t, int64_t corr, int32_t id, uint16_t v, int32_t,
                              const std::vector<uint8_t>&, uint16_t e) {
            responses_.push_back(Response{Op::GetSchema, corr, id, v, e});
        };
        std::vector<int32_t> rs(set.begin(), set.end());
        w_ = std::make_unique<AdminWorker>(*schemas_, schemaReplica_.get(), /*selfId=*/1, std::move(rs),
                                           std::move(d), viewsReplica_.get(), views_.get());
        answeredSinceBoot_ = 0;
        creates_.clear();
        promote();
    }

    void teardown_() {
        w_.reset();
        schemaReplica_.reset();
        viewsReplica_.reset();
        schemaLog_.reset();
        viewsLog_.reset();
        topics_.reset();
        schemas_.reset();
        views_.reset();
    }

    void release_(PartitionReplica& r, int32_t topicId) {
        const rune::replication::Offset leo = r.logEndOffset();
        if (r.highWatermark() >= leo) return;
        (void)r.onFetchRequest(rune::replication::FetchRequestMsg{
                                   static_cast<rune::replication::Epoch>(epoch_), 0, /*followerId=*/2, leo,
                                   1 << 20, topicId},
                               now_);
    }

    int64_t nextCorr_(Op op) {
        const int64_t corr = ++corr_;
        opByCorr_[corr] = op;
        return corr;
    }

    CreateTopicReqMsg popCreate_() {
        CreateTopicReqMsg req = creates_.front();
        creates_.pop_front();
        return req;
    }

    void onDdlAnswer_(int64_t corr, int32_t id, uint16_t version, uint16_t err) {
        const uint64_t unanswered = w_->startedCount() - answeredSinceBoot_;
        if (unanswered != 1)
            violations_.push_back("answer for corr " + std::to_string(corr) + " with " +
                                  std::to_string(unanswered) + " ops started-but-unanswered (want 1)");
        if (corr <= lastDdlCorr_)
            violations_.push_back("answer for corr " + std::to_string(corr) + " out of FIFO order (previous " +
                                  std::to_string(lastDdlCorr_) + ")");
        lastDdlCorr_ = corr;
        ++answeredSinceBoot_;
        const auto it = opByCorr_.find(corr);
        responses_.push_back(Response{it == opByCorr_.end() ? Op::DefineDisk : it->second, corr, id, version, err});
    }

    void checkInvariant_(const char* where) {
        const uint64_t unanswered = w_->startedCount() - answeredSinceBoot_;
        const uint64_t inFlight   = w_->hasInFlight() ? 1u : 0u;
        if (unanswered != inFlight)
            violations_.push_back(std::string(where) + ": " + std::to_string(unanswered) +
                                  " DDL ops started-but-unanswered, in flight " + std::to_string(inFlight));
    }

    DdlHarnessOptions        opt_;
    std::filesystem::path    root_;
    rune::replication::NullPartitionTransport schemaTransport_, viewsTransport_;
    std::unique_ptr<rune::gateway::TopicRegistry> topics_;
    std::unique_ptr<rune::storage::Log>           schemaLog_, viewsLog_;
    std::unique_ptr<PartitionReplica>             schemaReplica_, viewsReplica_;
    std::unique_ptr<SchemaRegistry>               schemas_;
    std::unique_ptr<ViewRegistry>                 views_;
    std::unique_ptr<AdminWorker>                  w_;

    ControlMode                   control_ = ControlMode::Auto;
    std::deque<CreateTopicReqMsg> creates_;
    std::vector<Response>         responses_;
    std::map<int64_t, Op>         opByCorr_;
    std::vector<std::string>      violations_;
    int64_t  now_ = 0;
    int32_t  epoch_ = 0;
    bool     leader_ = false;
    int64_t  corr_ = 0;
    int64_t  lastDdlCorr_ = 0;
    uint64_t answeredSinceBoot_ = 0;
    uint64_t startedBeforeBoot_ = 0;
    int      createRequests_ = 0, viewPlanPushes_ = 0, validatorPushes_ = 0;
};

} // namespace rune::node::test
```

- [ ] **Step 6: Run the tests and confirm they pass.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='DdlHarnessRegression.*'`
  Expected: 4 PASS.
  If `HeldCommitKeepsTheOpInFlightUntilReleased` never sees the commit, check `release_`: the fetch must carry the leader's current epoch, and its offset must be the leader's LEO.

- [ ] **Step 7: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/include/rune/node/AdminWorker.h libs/node/src/AdminWorker.cpp libs/node/tests/support/DdlExpectations.h libs/node/tests/support/DdlHarness.h libs/node/tests/node/DdlHarnessRegressionTest.cpp libs/node/CMakeLists.txt
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "test(ddl): DdlHarness + expectation table; AdminWorker hasInFlight/startedCount introspection

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: B1 / B2 / B6 — name and partition-count validation (admin + control)

**Files:**
- Modify: `libs/node/src/AdminWorker.cpp`: include; `kInvalidTopicName` constant (~L30); `startNext_` define path (after `DefineTopicMsg msg = std::move(pd.define);` and after any R1 persistence check placed there, ~L418); `startCreateView_` (after the `msg.malformed` block, ~L481)
- Modify: `libs/node/include/rune/node/AdminWorker.h`: error-code comment list (~L20-30)
- Modify: `libs/node/include/rune/node/AdminCreateTopic.h`: start of `createTopicForAdmin`
- Modify: `libs/node/include/rune/node/AdminChannels.h`: the comment in `decodeCreateTopicReq`, ~L440-441
- Test: `libs/node/tests/node/DdlHarnessRegressionTest.cpp` (append)

**Interfaces:**
- Consumes: `rune::node::isValidTopicName` (Task 2).
- Produces:
  - DefineTopic and CreateView with an invalid or reserved name → `40`, before any other check except `malformed`.
  - DefineTopic with `partitionCount < 1` → `23`.
  - `createTopicForAdmin` with an invalid name or `partitionCount < 1` → `reply.topicId = -1`, nothing created.

- [ ] **Step 1: Write the failing tests.** Append to `DdlHarnessRegressionTest.cpp`:

```cpp
// ── B1 / B2 / B6 ──────────────────────────────────────────────────────────────
TEST(DdlFixRegression, B1_PathTraversalAndEmptyNamesAreRejectedAndCreateNothing) {
    DdlHarness h;
    const int32_t topicsBefore = h.topicCount();
    const int reqsBefore = h.createRequests();
    for (const std::string n : {std::string("../escape"), std::string("a/b"), std::string(""),
                                std::string("."), std::string(".."), std::string(250, 'x'),
                                std::string("nul\0byte", 8)}) {
        SCOPED_TRACE(n);
        const int64_t d = h.submit(Op::DefineDisk, n);
        const int64_t v = h.submit(Op::ViewPersisted, n);
        h.settle();
        EXPECT_EQ(h.response(d)->err, kInvalidTopicName);
        EXPECT_EQ(h.response(v)->err, kInvalidTopicName);
    }
    EXPECT_EQ(h.topicCount(), topicsBefore);
    EXPECT_EQ(h.createRequests(), reqsBefore) << "never reaches the control thread";
    EXPECT_FALSE(fs::exists(h.root() / "escape")) << "nothing written outside dataDir/topics";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B2_EveryReservedInternalNameIsRejected) {
    DdlHarness h;
    for (const char* n : {"__topic_schemas", "__views", "__consumer_offsets", "__transaction_state",
                          "__cluster_metadata", "__view_snapshots", "__orphan.1", "__x"}) {
        SCOPED_TRACE(n);
        const int64_t d = h.submit(Op::DefineTransient, n);
        const int64_t v = h.submit(Op::ViewTransient, n);
        h.settle();
        EXPECT_EQ(h.response(d)->err, kInvalidTopicName);
        EXPECT_EQ(h.response(v)->err, kInvalidTopicName);
        EXPECT_EQ(h.idOf(n), -1);
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B6_PartitionCountBelowOneIsInvalidSchema) {
    DdlHarness h;
    const int32_t topicsBefore = h.topicCount();
    for (const int32_t parts : {0, -1, -1000}) {
        SCOPED_TRACE(parts);
        DefineTopicMsg m;
        m.partitionCount = parts; m.name = "zero_parts"; m.keyField = "id"; m.blob = charBlob();
        const int64_t c = h.submitRawDefine(std::move(m));
        h.settle();
        EXPECT_EQ(h.response(c)->err, kInvalidSchema);
    }
    EXPECT_EQ(h.topicCount(), topicsBefore);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B1_ControlThreadRefusesInvalidRequestsDefensively) {
    const fs::path dir = fs::temp_directory_path() / ("rune_ddl_ctl_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    {
        rune::gateway::TopicRegistry reg({}, dir.string());
        for (const std::string n : {std::string("../escape"), std::string(""), std::string("__views")}) {
            CreateTopicReqMsg req; req.ticket = 1; req.partitionCount = 1; req.name = n;
            EXPECT_EQ(createTopicForAdmin(reg, req).topicId, -1) << n;
        }
        CreateTopicReqMsg zero; zero.ticket = 2; zero.partitionCount = 0; zero.name = "ok";
        EXPECT_EQ(createTopicForAdmin(reg, zero).topicId, -1);
        EXPECT_EQ(reg.topicCount(), 0);
    }
    EXPECT_FALSE(fs::exists(dir / "escape"));
    std::error_code ec; fs::remove_all(dir, ec);
}
```

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='DdlFixRegression.B1*:DdlFixRegression.B2*:DdlFixRegression.B6*'`
  Expected: FAIL. Today the names reach `createTopic`: codes 0 or 6, `escape` exists, and `zero_parts` answers 0.

- [ ] **Step 3: Implement.**

In `AdminWorker.cpp`, add `#include <rune/node/TopicName.h>` after `#include "rune/node/AdminWorker.h"`. After `static constexpr uint16_t kViewSourceTransient  = 37; …`, add:

```cpp
static constexpr uint16_t kInvalidTopicName     = 40;  // DefineTopic/CreateView: name fails TopicName.h (B1/B2)
```

In `startNext_`, immediately after the no-hang wave's `// ── malformed ──` block (the `if (!persistenceFromWire(msg.persistence)) { … kInvalidSchema … }` that follows `DefineTopicMsg msg = std::move(pd.define);`), add:

```cpp
    // ── DDL conformance B1/B2 ── the name becomes dataDir/topics/<name>/: one safe
    //    path segment, never an internal "__" name.  Nothing is created.
    if (!isValidTopicName(msg.name)) {
        deps_.respondDefine(msg.session, msg.corr, -1, 0, kInvalidTopicName);
        return;
    }
    // ── DDL conformance B6 ── a topic has at least one partition.
    if (msg.partitionCount < 1) {
        deps_.respondDefine(msg.session, msg.corr, -1, 0, kInvalidSchema);
        return;
    }
```

In `startCreateView_`, immediately after the `if (msg.malformed || msg.persistence > 1) { … return; }` block, add:

```cpp
    // ── DDL conformance B1/B2 ── the view name is its sink topic's directory name.
    if (!isValidTopicName(msg.viewName)) {
        deps_.respondCreateView(msg.session, msg.corr, -1, 0, kInvalidTopicName);
        return;
    }
```

In `AdminWorker.h`, extend the error-code comment list after `//   ViewNotAlterable      = 38 …`:

```cpp
//   InvalidTopicName      = 40  (DefineTopic/CreateView: name fails TopicName.h)
//   (InvalidSchema 23 also answers DefineTopic.partitionCount < 1)
```

In `AdminCreateTopic.h`, add `#include <rune/node/TopicName.h>`. Make the start of `createTopicForAdmin`, right after `reply.ticket = req.ticket;` (the no-hang wave's unknown-persistence refusal stays below it, unchanged), read:

```cpp
    // DDL conformance B1/B2/B6: the admin thread already refuses these; the control
    // thread — the one that builds dataDir/topics/<name>/ — never creates them either.
    if (!isValidTopicName(req.name) || req.partitionCount < 1) {
        spdlog::error("createTopicForAdmin: refusing invalid request name='{}' partitions={}",
                      req.name, req.partitionCount);
        reply.topicId = -1;
        reply.existed = false;
        return reply;
    }
```

In `AdminChannels.h` `decodeCreateTopicReq`, replace the comment `// on the control thread (createTopic rejects "")` with `// on the control thread (createTopicForAdmin refuses an invalid name)`.

- [ ] **Step 4: Run the tests and confirm they pass.** Same command as Step 2 → PASS. Also run `/Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_create_view_tests` and `rune_schema_gateway_tests` → PASS (their names are valid).

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/src/AdminWorker.cpp libs/node/include/rune/node/AdminWorker.h libs/node/include/rune/node/AdminCreateTopic.h libs/node/include/rune/node/AdminChannels.h libs/node/tests/node/DdlHarnessRegressionTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "fix(admin): validate DDL names (InvalidTopicName=40; no path traversal, reserved __ names) and partitions>=1 (B1/B2/B6)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5a: B7 — static `RUNE_TOPICS` names obey the same rule (config parse)

**Files:**
- Modify: `libs/node/src/NodeConfig.cpp`: include; RUNE_TOPICS parse, right after `std::string name(namePart);` (~L806)
- Test: `libs/node/tests/node/NodeConfigTest.cpp`: append after the Phase 2b E block that defines `parseWithTopics` (~L2813)

**Interfaces:**
- Consumes: `checkTopicName`, `topicNameErrorText`, `topicNamesFoldEqual` (Task 2).
- Produces (user decision D2d), both via `ConfigResult.error` so the broker refuses to boot:
  - a `RUNE_TOPICS` entry whose name fails `TopicName.h` → `"RUNE_TOPICS topic name '<name>' is invalid (<reason>) in entry: <entry>"`;
  - two static names that differ only by ASCII case → `"RUNE_TOPICS topic names '<a>' and '<b>' differ only by letter case (they would share one data directory) in entry: <entry>"`.

  Exact duplicates keep the existing message.

- [ ] **Step 1: Write the failing tests.** Append to `NodeConfigTest.cpp` (after the `parseWithTopics` tests):

```cpp
// ── DDL conformance B7 / user decision D2d: static RUNE_TOPICS names obey the DDL
//    name rule (TopicName.h) — a bad name is a boot config error naming the entry.
TEST(NodeConfigRegression, RuneTopicsInvalidNameIsAConfigErrorNamingTheEntryRegression) {
    const std::string tooLong(250, 'x');
    struct Case { std::string topics; const char* reason; };
    const Case cases[] = {
        {"__views:1",              "reserved"},
        {"ok:1,__consumer_offsets:2", "reserved"},
        {"a/b:1",                  "outside [A-Za-z0-9._-]"},
        {"a\\b:1",                 "outside [A-Za-z0-9._-]"},
        {"..:1",                   "'.' or '..'"},
        {"has space:1",            "outside [A-Za-z0-9._-]"},
        {"tab\tname:1",            "outside [A-Za-z0-9._-]"},
        {tooLong + ":1",           "longer than 249 bytes"},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE(c.topics);
        const auto res = parseWithTopics(c.topics.c_str());
        ASSERT_FALSE(res.config.has_value()) << "must refuse to boot";
        EXPECT_NE(res.error.find("RUNE_TOPICS topic name"), std::string::npos) << res.error;
        EXPECT_NE(res.error.find(c.reason), std::string::npos) << res.error;
        EXPECT_NE(res.error.find("in entry:"), std::string::npos) << "names the entry: " << res.error;
    }
}

TEST(NodeConfigRegression, RuneTopicsCaseOnlyDuplicateIsAConfigErrorRegression) {
    const auto res = parseWithTopics("Orders:1,chat:2,orders:4");
    ASSERT_FALSE(res.config.has_value());
    EXPECT_NE(res.error.find("'Orders'"), std::string::npos) << res.error;
    EXPECT_NE(res.error.find("'orders'"), std::string::npos) << res.error;
    EXPECT_NE(res.error.find("differ only by letter case"), std::string::npos) << res.error;
}

TEST(NodeConfigRegression, RuneTopicsValidNamesStillParseRegression) {
    const std::string maxLen(249, 'x');
    const std::string topics = "orders.v2:1,_private:1,a-b_c:2," + maxLen + ":1";
    const auto res = parseWithTopics(topics.c_str());
    ASSERT_TRUE(res.config.has_value()) << res.error;
    ASSERT_EQ(res.config->topics.size(), 4u);
    EXPECT_EQ(res.config->topics[3].name, maxLen);
}
```

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_node_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_node_tests --gtest_filter='NodeConfigRegression.RuneTopics*'`
  Expected: the two "is a config error" tests FAIL (today they parse); `RuneTopicsValidNamesStillParseRegression` PASSES.

- [ ] **Step 3: Implement.** In `NodeConfig.cpp`, add `#include <rune/node/TopicName.h>` after `#include <rune/node/NodeConfig.h>`. Replace

```cpp
                std::string name(namePart);
                if (!seenNames.insert(name).second)
                    return {{}, "RUNE_TOPICS has duplicate topic name: " + name};
```

with

```cpp
                std::string name(namePart);
                // DDL conformance B7 / D2d: a static name obeys the DDL name rule — it is
                // the directory dataDir/topics/<name>/ too.
                if (const TopicNameError ne = checkTopicName(name); ne != TopicNameError::None)
                    return {{}, "RUNE_TOPICS topic name '" + name + "' is invalid (" +
                                std::string(topicNameErrorText(ne)) + ") in entry: " + std::string(entry)};
                if (!seenNames.insert(name).second)
                    return {{}, "RUNE_TOPICS has duplicate topic name: " + name};
                for (const std::string& seen : seenNames) {
                    if (seen != name && topicNamesFoldEqual(seen, name))
                        return {{}, "RUNE_TOPICS topic names '" + seen + "' and '" + name +
                                    "' differ only by letter case (they would share one data "
                                    "directory) in entry: " + std::string(entry)};
                }
```

The existing empty-name check above it is unchanged; it still runs first, with its own message.

- [ ] **Step 4: Run the tests and confirm they pass.** Same command as Step 2 → 3 PASS. Then run the whole binary: `/Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_node_tests --gtest_filter='NodeConfig*'` → all PASS. Every existing RUNE_TOPICS test name (`chat`, `inventory`, `entities`, `presence`, `orders`, `z`, `a`, `m`, …) is valid.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/src/NodeConfig.cpp libs/node/tests/node/NodeConfigTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "fix(config): RUNE_TOPICS names obey the DDL name rule; case-only duplicates are a config error (B7)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: B3 — case-variant names are TopicExists (no shared directory)

**Files:**
- Modify: `libs/gateway/include/rune/gateway/TopicRegistry.h`: after `topicIdForName` (~L59-63)
- Modify: `libs/gateway/src/TopicRegistry.cpp`: include + new function after `topicIdForName` (~L119)
- Modify: `libs/node/include/rune/node/AdminChannels.h`: `CreateTopicReplyMsg` (~L189-199), `encodeCreateTopicReply` / `decodeCreateTopicReply` (~L450-482)
- Modify: `libs/node/include/rune/node/AdminCreateTopic.h`: fold check after the validation block
- Modify: `libs/node/include/rune/node/AdminWorker.h` / `src/AdminWorker.cpp`: new `answerInFlight_`; case-conflict branch at the top of `onCreateTopicReply`
- Modify: `libs/node/src/BrokerNode.cpp`: the create-topic drain (~L3878, `if (reply.topicId >= 0) {`)
- Modify: `libs/node/tests/support/DdlHarness.h`: `failCreate` clears `caseConflict`
- Test: `libs/node/tests/node/DdlHarnessRegressionTest.cpp` (append)

**Interfaces:**
- Produces:
  - `std::optional<int32_t> TopicRegistry::topicIdForFoldedName(std::string_view) const`: a topic whose name folds equal to the argument but is not exactly equal to it.
  - `CreateTopicReplyMsg::caseConflict` (bool). It is a ring trailer byte at index 18 (frame length 19). An older frame decodes as `false`.
  - `void AdminWorker::answerInFlight_(uint16_t code, int32_t topicId)` (private).
- Behaviour: the reply for a fold collision carries the colliding topic's id with `existed = true` and `caseConflict = true`, and nothing is created. AdminWorker answers `TopicExists (25)` for both DefineTopic and CreateView.

- [ ] **Step 1: Write the failing tests.** Append to `DdlHarnessRegressionTest.cpp`:

```cpp
// ── B3 ────────────────────────────────────────────────────────────────────────
TEST(DdlFixRegression, B3_CaseVariantOfATypedTopicIsTopicExists) {
    DdlHarness h;
    const int64_t a = h.submit(Op::DefineDisk, "Orders");
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    const int32_t topicsBefore = h.topicCount();
    const int64_t b = h.submit(Op::DefineDisk, "orders");
    const int64_t v = h.submit(Op::ViewPersisted, "ORDERS");
    h.settle();
    EXPECT_EQ(h.response(b)->err, kTopicExists);
    EXPECT_EQ(h.response(b)->id, h.idOf("Orders")) << "answers the colliding topic's id";
    EXPECT_EQ(h.response(v)->err, kTopicExists);
    EXPECT_EQ(h.topicCount(), topicsBefore) << "no second topic sharing Orders' directory";
    EXPECT_EQ(h.idOf("orders"), -1);
    EXPECT_EQ(h.views().byName("ORDERS"), nullptr);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B3_CaseVariantOfASchemaLessStaticTopicIsTopicExists) {
    DdlHarnessOptions o;
    TopicSpec s; s.name = "Stat"; s.partitionCount = 1;
    o.statics.push_back(s);
    DdlHarness h(o);
    const int64_t b = h.submit(Op::DefineDisk, "stat");
    h.settle();
    EXPECT_EQ(h.response(b)->err, kTopicExists);
    EXPECT_EQ(h.idOf("stat"), -1);
    const int64_t exact = h.submit(Op::DefineDisk, "Stat");
    h.settle();
    EXPECT_EQ(h.response(exact)->err, kOk) << "the exact name still adopts the static topic";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B3_TopicRegistryFoldedLookupNeverMatchesTheExactName) {
    const fs::path dir = fs::temp_directory_path() / ("rune_ddl_fold_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    {
        TopicSpec s; s.name = "Orders"; s.partitionCount = 1;
        rune::gateway::TopicRegistry reg({s}, dir.string());
        EXPECT_EQ(reg.topicIdForFoldedName("orders"), std::optional<int32_t>(0));
        EXPECT_EQ(reg.topicIdForFoldedName("Orders"), std::nullopt) << "exact name is not a conflict";
        EXPECT_EQ(reg.topicIdForFoldedName("other"), std::nullopt);
    }
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlFixRegression, B3_CaseConflictCrossesTheReplyRing) {
    OneToOneByteRing ring{kCreateTopicRingBytes};
    CreateTopicReplyMsg m;
    m.ticket = 5; m.topicId = 3; m.partitionCount = 2; m.existed = true; m.persistence = 0; m.caseConflict = true;
    ASSERT_TRUE(encodeCreateTopicReply(ring, m));
    ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                   aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        const std::size_t n = static_cast<std::size_t>(len);
        const CreateTopicReplyMsg got = decodeCreateTopicReply(p, n);
        EXPECT_TRUE(got.caseConflict);
        EXPECT_EQ(got.topicId, 3);
        EXPECT_FALSE(decodeCreateTopicReply(p, n - 1).caseConflict) << "a pre-B3 frame decodes as no conflict";
    }, 1);
}
```

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests`
  Expected: compile errors `no member named 'topicIdForFoldedName'` and `no member named 'caseConflict'`.

- [ ] **Step 3: Implement.**

`TopicRegistry.h`, after the `topicIdForName` declaration:

```cpp
    // -----------------------------------------------------------------------
    // topicIdForFoldedName — a topic whose name equals `name` ignoring ASCII case
    // but is NOT exactly `name` (DDL conformance B3: the two would share
    // dataDir/topics/<name>/ on a case-insensitive filesystem).  Linear scan —
    // DDL (cold path) only.  Empty optional when there is none.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::optional<int32_t>
    topicIdForFoldedName(std::string_view name) const;
```

`TopicRegistry.cpp`: add `#include <rune/node/TopicName.h>` after `#include <rune/gateway/TopicRegistry.h>`, and after the `topicIdForName` definition add:

```cpp
// ---------------------------------------------------------------------------
// TopicRegistry::topicIdForFoldedName (DDL conformance B3)
// ---------------------------------------------------------------------------
std::optional<int32_t>
TopicRegistry::topicIdForFoldedName(std::string_view name) const
{
    for (const auto& [n, id] : nameIndex_) {
        if (n != name && rune::node::topicNamesFoldEqual(n, name)) return id;
    }
    return std::nullopt;
}
```

`AdminChannels.h`, in `struct CreateTopicReplyMsg`, after the `persistence` member:

```cpp
    // DDL conformance B3: the requested name differs only in ASCII case from the
    // existing topic `topicId` (existed = true): nothing was created and the admin
    // thread answers TopicExists.  Ring trailer byte; absent => false.
    bool    caseConflict   = false;
```

In `encodeCreateTopicReply`, replace `const std::size_t len = 8 + 4 + 4 + 1 + 1;` with `const std::size_t len = 8 + 4 + 4 + 1 + 1 + 1;`, and replace `*p   = m.persistence;` with:

```cpp
    *p++ = m.persistence;
    *p   = m.caseConflict ? 1 : 0;   // DDL conformance B3 trailer
```

In `decodeCreateTopicReply`, after the line `if (len < 18) return m;   // pre-2b-E frame: persistence not reported`, insert:

```cpp
    if (len >= 19) m.caseConflict = buf[18] != 0;   // DDL conformance B3 trailer
```

`AdminCreateTopic.h`, right after the Task 5 validation block:

```cpp
    // DDL conformance B3: names are case-sensitive identifiers, but a name that
    // differs from an existing one only in ASCII case would share its directory on a
    // case-insensitive filesystem — refuse it (TopicExists); nothing is created.
    if (!reg.topicIdForName(req.name).has_value()) {
        if (const std::optional<int32_t> other = reg.topicIdForFoldedName(req.name)) {
            reply.topicId      = *other;
            reply.existed      = true;
            reply.caseConflict = true;
            if (const TopicSpec* s = reg.topic(*other)) {
                reply.partitionCount = s->partitionCount;
                reply.persistence    = persistenceToWire(s->persistence);
            }
            return reply;
        }
    }
```

`AdminWorker.h`, private section, after `void startAlter_(AlterTopicMsg msg, int64_t nowNs);`:

```cpp
    // Answer the in-flight op with `code` in its own response kind (Define /
    // CreateView / Alter) carrying `topicId`, then go Idle.  The single exit for an
    // in-flight op that ends without committing (case conflict B3, timeout,
    // demotion B4).
    void answerInFlight_(uint16_t code, int32_t topicId);
```

`AdminWorker.cpp`, add before `// ─── doWork ───`:

```cpp
// ─── answerInFlight_ — end the in-flight op without committing ──────────────
void AdminWorker::answerInFlight_(uint16_t code, int32_t topicId) {
    switch (inFlight_->kind) {
        case DdlKind::Alter:
            deps_.respondAlter(inFlight_->alter.session, inFlight_->alter.corr, topicId, 0, code);
            break;
        case DdlKind::CreateView:
            deps_.respondCreateView(inFlight_->view.session, inFlight_->view.corr, topicId, 0, code);
            break;
        case DdlKind::Create:
            deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr, topicId, 0, code);
            break;
    }
    inFlight_.reset();
    state_ = DdlState::Idle;
}
```

In `onCreateTopicReply`, immediately after the stale-reply guard `if (state_ != DdlState::AwaitingId || !inFlight_ || … ) { … return; }`, insert:

```cpp
    // DDL conformance B3: the name differs only in ASCII case from an existing topic
    // (they would share one directory on a case-insensitive filesystem).  Nothing
    // was created; answer TopicExists with the existing topic's id.
    if (reply.caseConflict) {
        answerInFlight_(kTopicExists, reply.topicId);
        return;
    }
```

`BrokerNode.cpp` create-topic drain: replace `                if (reply.topicId >= 0) {` (the line right after `reply = createTopicForAdmin(*topicRegistry_, req);` and its comment block) with:

```cpp
                if (reply.topicId >= 0 && !reply.caseConflict) {   // B3: nothing was created
```

`DdlHarness.h` `failCreate`: after `reply.existed = false;`, add `        reply.caseConflict = false;`.

Add `#include <optional>` to `TopicRegistry.h` if it is not already included (it is: `<optional>` is in its include list).

- [ ] **Step 4: Run the tests and confirm they pass.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests rune_create_view_tests rune_schema_gateway_tests rune_node_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='DdlFixRegression.B3*'`
  Expected: 4 PASS.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/gateway/include/rune/gateway/TopicRegistry.h libs/gateway/src/TopicRegistry.cpp libs/node/include/rune/node/AdminChannels.h libs/node/include/rune/node/AdminCreateTopic.h libs/node/include/rune/node/AdminWorker.h libs/node/src/AdminWorker.cpp libs/node/src/BrokerNode.cpp libs/node/tests/support/DdlHarness.h libs/node/tests/node/DdlHarnessRegressionTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "fix(admin): a name differing only in letter case from an existing topic is TopicExists — no shared log dir (B3)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: B4 — demotion answers the in-flight op at once; KL2 pin; re-promotion pin

**Files:**
- Modify: `libs/node/src/AdminWorker.cpp`: `onRole` follower branch (~L103-116); `advance_` timeout block (~L731-744)
- Test: `libs/node/tests/node/DdlHarnessRegressionTest.cpp` (append)

**Interfaces:**
- Consumes: `answerInFlight_` (Task 6).
- Produces: `onRole(follower)` answers an in-flight op `Internal (6)` synchronously. The timeout path uses `answerInFlight_` with the same codes and ids as before.

- [ ] **Step 1: Write the failing tests.** Append:

```cpp
// ── B4 / KL2 / re-promotion ──────────────────────────────────────────────────
TEST(DdlFixRegression, B4_DemotionAnswersAnInFlightCreateImmediately) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t a = h.submit(Op::DefineDisk, "n");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.demote();
    ASSERT_TRUE(h.response(a).has_value()) << "answered on demotion, not after the 30 s timeout";
    EXPECT_EQ(h.response(a)->err, kInternal);
    EXPECT_FALSE(h.worker().hasInFlight());
    h.deliverCreate();   // the control thread's reply arrives late: a stale ticket
    EXPECT_EQ(h.schemas().byName("n"), nullptr) << "never applied";
    h.promote();
    h.setControl(ControlMode::Auto);
    const int64_t b = h.submit(Op::DefineDisk, "n");
    h.settle();
    EXPECT_EQ(h.response(b)->err, kOk) << "the retry adopts the orphan (SchemaLessDisk row)";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B4_DemotionAnswersAnInFlightCommitAndNeverAppliesIt) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    h.demote();
    ASSERT_TRUE(h.response(a).has_value());
    EXPECT_EQ(h.response(a)->err, kInternal);
    for (int i = 0; i < 10; ++i) h.tick();
    EXPECT_EQ(h.schemas().byName("src")->version, 1u) << "a follower never commits an op off its own HW";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B4_RepromotionKeepsTheCatalogExact) {
    DdlHarness h;
    const int64_t v = h.submit(Op::ViewPersisted, "v");
    h.settle();
    ASSERT_EQ(h.response(v)->err, kOk);
    const Catalog before = h.catalog();
    const std::size_t views = h.views().all().size();
    h.demote();
    h.promote();
    EXPECT_EQ(h.catalog(), before) << "promotion replays onto un-cleared registries: no duplicates, no loss";
    EXPECT_EQ(h.views().all().size(), views);
    const int64_t d = h.submit(Op::DefineDisk, "after");
    h.settle();
    EXPECT_EQ(h.response(d)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// KNOWN LIMITATION KL2: an op answered Internal after its record was appended is in
// doubt.  The live registry never applies it; its committed record is replayed on
// restart.  Pinned so any change to this behaviour is deliberate.
TEST(DdlFixRegression, KL2_TimeoutInAwaitingCommitIsInDoubtAndReplayedOnRestart) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
    h.tick();
    ASSERT_TRUE(h.response(a).has_value());
    EXPECT_EQ(h.response(a)->err, kInternal);
    h.releaseCommits();
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_EQ(h.schemas().byName("src")->version, 1u) << "KL2: the live registry never applies it";
    h.restart();
    EXPECT_EQ(h.schemas().byName("src")->version, 2u) << "KL2: its committed record is replayed on restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}
```

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='DdlFixRegression.B4*:DdlFixRegression.KL2*'`
  Expected: both `B4_DemotionAnswers…` tests FAIL (no response after `demote()`). `B4_Repromotion…` and `KL2_…` may already pass, which is expected: they are pins.

- [ ] **Step 3: Implement.** In `AdminWorker::onRole`'s `else` branch, after `wasLeader_ = false;  // next promotion re-triggers rebuild`, add:

```cpp
        // DDL conformance B4: a DDL op cannot commit on a follower — the HW it would
        // poll is the new leader's, and its appended record may be truncated.
        // Answer it now (Internal: outcome unknown, the client retries on the new
        // leader) instead of riding the 30 s timeout or committing off a follower HW.
        if (inFlight_)
            answerInFlight_(kInternal, inFlight_->kind == DdlKind::Alter ? inFlight_->topicId : -1);
```

In `advance_`, replace the whole timeout `if` block (from `    if (nowNs - inFlight_->startedNs > kDdlTimeoutNs) {` through its closing `    }`) with:

```cpp
    if (nowNs - inFlight_->startedNs > kDdlTimeoutNs) {
        answerInFlight_(kInternal, inFlight_->kind == DdlKind::Alter ? inFlight_->topicId : -1);
        return true;
    }
```

- [ ] **Step 4: Run the tests and confirm they pass.** Same command as Step 2 → all PASS. Also run `/Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_node_tests --gtest_filter='AdminWorker*'` → PASS, including `FollowerRejectsDefine`.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/src/AdminWorker.cpp libs/node/tests/node/DdlHarnessRegressionTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "fix(admin): demotion answers the in-flight DDL op at once, never commits it off a follower HW (B4); pin KL2 in-doubt

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7a: B8 — an ALTER that renames the key field moves `keyField` with it (live + restart)

**Files:**
- Modify: `libs/node/src/AdminWorker.cpp`, `startAlter_` step g (~L675-688)
- Test: `libs/node/tests/node/DdlHarnessRegressionTest.cpp` (append)

**Interfaces:**
- Consumes: `DdlHarness` (`submit(Op::AlterRenameKey, …)`, `submitRawAlter`, `restart`, `schemas()`), `alterOpsFor` (Task 4).
- Produces (user decision D3a): the new `TopicSchema.keyField` equals the old one after every `Rename` op whose `name` is the current key field is applied in order. A chain `id→a, a→b` ends at `b`. A rejected ALTER changes nothing. The durable record carries it, so a restart replays it.

- [ ] **Step 1: Write the failing tests.** Append to `DdlHarnessRegressionTest.cpp`:

```cpp
// ── B8 / user decision D3a: renaming the key field moves TopicSchema.keyField ────
TEST(DdlFixRegression, B8_RenamingTheKeyFieldMovesKeyFieldAndSurvivesRestart) {
    DdlHarness h;
    ASSERT_EQ(h.schemas().byName("src")->keyField, "id");
    const int64_t a = h.submit(Op::AlterRenameKey, "src");   // RENAME id -> pk
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.schemas().byName("src")->keyField, "pk") << "live";
    EXPECT_EQ(h.schemas().byName("src")->version, 2u);
    h.restart();
    EXPECT_EQ(h.schemas().byName("src")->keyField, "pk") << "replayed from the durable record";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B8_ChainedRenamesEndAtTheLastName) {
    DdlHarness h;
    rune::codec::AlterOp r1{}; r1.type = rune::codec::AlterOpType::Rename; r1.name = "id"; r1.newName = "a";
    rune::codec::AlterOp r2{}; r2.type = rune::codec::AlterOpType::Rename; r2.name = "a";  r2.newName = "b";
    AlterTopicMsg m; m.topicName = "src"; m.opsBlob = rune::codec::encodeAlterOps({r1, r2});
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    ASSERT_EQ(h.response(c)->err, kOk);
    EXPECT_EQ(h.schemas().byName("src")->keyField, "b");
}

TEST(DdlFixRegression, B8_RenamingANonKeyFieldKeepsKeyField) {
    DdlHarness h;
    const int64_t c = h.submit(Op::AlterRename, "src");   // RENAME hp -> health
    h.settle();
    ASSERT_EQ(h.response(c)->err, kOk);
    EXPECT_EQ(h.schemas().byName("src")->keyField, "id");
}

TEST(DdlFixRegression, B8_ARejectedAlterNeverMovesKeyField) {
    DdlHarness h;
    rune::codec::AlterOp r{}; r.type = rune::codec::AlterOpType::Rename; r.name = "id"; r.newName = "pk";
    rune::codec::AlterOp d{}; d.type = rune::codec::AlterOpType::Drop; d.name = "nosuch";
    AlterTopicMsg m; m.topicName = "src"; m.opsBlob = rune::codec::encodeAlterOps({r, d});
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    EXPECT_EQ(h.response(c)->err, kInvalidSchema) << "applyOps is atomic";
    EXPECT_EQ(h.schemas().byName("src")->keyField, "id");
    EXPECT_EQ(h.schemas().byName("src")->version, 1u);
}
```

Add `#include <rune/codec/AlterOps.h>` to the test's includes.

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='DdlFixRegression.B8*'`
  Expected: `B8_RenamingTheKeyField…` and `B8_ChainedRenames…` FAIL (keyField stays `id`). The other two PASS as pins.

- [ ] **Step 3: Implement.** In `startAlter_`, immediately before the comment `    // g. build the new durable TopicSchema …`, add:

```cpp
    // DDL conformance B8 / D3a: a RENAME of the key field moves keyField with it.
    // Ops apply in order, so a chain (id -> a, a -> b) ends at b.  Only reached when
    // applyOps succeeded (a rejected ALTER changes nothing).
    std::string keyField = latest->keyField;
    for (const auto& op : *ops) {
        if (op.type == rune::codec::AlterOpType::Rename && !keyField.empty() && op.name == keyField)
            keyField = op.newName;
    }
```

and in step g replace `    ts.keyField       = latest->keyField;` with:

```cpp
    ts.keyField       = keyField;                          // follows a key-field RENAME (B8)
```

- [ ] **Step 4: Run the tests and confirm they pass.** Same command as Step 2 → 4 PASS. Also run `/Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_node_tests --gtest_filter='AdminWorker*'` → PASS.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/src/AdminWorker.cpp libs/node/tests/node/DdlHarnessRegressionTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "fix(admin): ALTER renaming the key field moves TopicSchema.keyField (live and across restart) (B8)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7b: B9 — CREATE VIEW over another view's output is `ViewSourceIsView (41)`

**Files:**
- Modify: `libs/node/src/AdminWorker.cpp`: constant (~L31); `startCreateView_` source loop (~L519)
- Modify: `libs/node/include/rune/node/AdminWorker.h`: error-code comment list
- Test: `libs/node/tests/node/DdlHarnessRegressionTest.cpp` (append)

**Interfaces:**
- Consumes: `ErrorCode::ViewSourceIsView = 41` (Task 1), `DdlHarness::submit(op, name, viewSource)`.
- Produces (user decision D3b): `startCreateView_` answers `41` when any source topic id is a view's own topic id (`viewRegistry_->byViewTopicId`). The check runs before the source lookup and before the transient rule. Nothing is requested or appended. It holds after a restart, because promotion recovers the ViewRegistry before any DDL runs.

- [ ] **Step 1: Write the failing tests.** Append to `DdlHarnessRegressionTest.cpp`:

```cpp
// ── B9 / user decision D3b: a view over a view's output is rejected (it would stay
//    silently empty — a view sink carries no change events until Phase 3) ──────────
TEST(DdlFixRegression, B9_ViewOverAViewIsViewSourceIsViewAndCreatesNothing) {
    DdlHarness h;
    for (const Op first : {Op::ViewPersisted, Op::ViewTransient}) {
        const std::string inner = first == Op::ViewPersisted ? "inner_p" : "inner_t";
        const int64_t c0 = h.submit(first, inner);
        h.settle();
        ASSERT_EQ(h.response(c0)->err, kOk);
        for (const Op outer : {Op::ViewPersisted, Op::ViewTransient}) {
            SCOPED_TRACE(std::string(opName(first)) + " then " + opName(outer));
            const Catalog before = h.catalog();
            const int reqs = h.createRequests();
            const int64_t viewsEnd = h.viewsLogEnd();
            const int64_t c = h.submit(outer, "outer", inner);
            h.settle();
            EXPECT_EQ(h.response(c)->err, kViewSourceIsView);
            EXPECT_EQ(h.createRequests(), reqs) << "no sink requested";
            EXPECT_EQ(h.viewsLogEnd(), viewsEnd) << "no __views record";
            EXPECT_EQ(h.catalog(), before);
        }
    }
    h.restart();
    const int64_t again = h.submit(Op::ViewPersisted, "outer", "inner_p");
    h.settle();
    EXPECT_EQ(h.response(again)->err, kViewSourceIsView) << "still rejected after a restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B9_ViewOverATypedTopicIsStillAccepted) {
    DdlHarness h;
    const int64_t c = h.submit(Op::ViewPersisted, "plain", "src");
    h.settle();
    EXPECT_EQ(h.response(c)->err, kOk);
}
```

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='DdlFixRegression.B9*'`
  Expected: `B9_ViewOverAView…` FAILS: today it answers 0 and creates an empty view.

- [ ] **Step 3: Implement.** In `AdminWorker.cpp`, after `static constexpr uint16_t kInvalidTopicName     = 40; …`, add:

```cpp
static constexpr uint16_t kViewSourceIsView     = 41;  // CreateView: a source is another view's output (B9)
```

In `startCreateView_`, make the first statement inside `for (const auto& s : authored->sources) {`:

```cpp
        // DDL conformance B9 / D3b: a view's output carries no change events (its
        // sink is a materialised table, not a changelog) until Phase 3 — a view over
        // it would stay silently empty.  Reject; nothing is created.
        if (viewRegistry_->byViewTopicId(s.topicId) != nullptr) {
            deps_.respondCreateView(msg.session, msg.corr, -1, 0, kViewSourceIsView);
            return;
        }
```

`viewRegistry_` is non-null here: the leader guard above already returned `kInternal` when it is null.

In `AdminWorker.h`, after the `//   InvalidTopicName      = 40 …` comment line, add:

```cpp
//   ViewSourceIsView      = 41  (CREATE VIEW whose source is another view's output)
```

- [ ] **Step 4: Run the tests and confirm they pass.** Same command as Step 2 → 2 PASS. Also run `/Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_create_view_tests` and `rune_get_view_row_tests` → PASS.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/src/AdminWorker.cpp libs/node/include/rune/node/AdminWorker.h libs/node/tests/node/DdlHarnessRegressionTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "fix(admin): CREATE VIEW over another view's output answers ViewSourceIsView=41 instead of a silently empty view (B9)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: The Occupant × Op matrix (456 cells) + malformed admin requests + view-source cells

**Files:**
- Create: `libs/node/tests/node/DdlOccupantMatrixRegressionTest.cpp`
- Modify: `libs/node/CMakeLists.txt`: add the file to `rune_ddl_conformance_tests`

**Interfaces:**
- Consumes: `DdlHarness`, `expectedCell`, `Occupant`, `Op` (Task 4); fixes B1–B10.
- Malformed-admin cells marked `_R1` pin the no-hang wave's R1 behaviour (landed in `8a693c5`):
  - a direct `DefineTopicMsg.persistence > 1` → 23;
  - a direct `CreateViewMsg.persistence > 1` → 23;
  - a `CreateTopicReplyMsg.persistence > 1` → 6.

- [ ] **Step 1: Write the matrix** `libs/node/tests/node/DdlOccupantMatrixRegressionTest.cpp`:

```cpp
// DdlOccupantMatrixRegressionTest.cpp — DDL conformance plan Task 8.
//
// One op against a name in every occupancy state (37 rows x 12 ops), driven
// through DdlHarness.  Expectations come ONLY from DdlExpectations.h.  Every cell
// asserts the exact code, the requestCreateTopic count, no half-creation, the
// final catalog delta, the serialization invariant, and the catalog after a restart.
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

#include <ostream>
#include <string>
#include <tuple>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;

namespace {

enum class Setup : uint8_t {
    None, TypedDisk, TypedTransient, ViewPersisted, ViewTransient, StaticDisk, StaticTransient,
    RecoveredTyped, RecoveredView, OrphanDefineDisk, OrphanDefineTransient, OrphanSink,
    OrphanSinkAfterRestart,
};

struct Row {
    const char* label;      // test-name fragment ([A-Za-z0-9_] only)
    Setup       setup;
    std::string occupant;   // the name the setup creates
    std::string target;     // the name every op uses
    Occupant    expect;     // the expectation-table row
};
void PrintTo(const Row& r, std::ostream* os) { *os << r.label; }

std::vector<Row> rows() {
    std::vector<Row> r = {
        {"Free",                       Setup::None,                   "",      "fresh", Occupant::Free},
        {"TypedDisk",                  Setup::TypedDisk,              "occ",   "occ",   Occupant::TypedDisk},
        {"TypedTransient",             Setup::TypedTransient,         "occ",   "occ",   Occupant::TypedTransient},
        {"ViewPersisted",              Setup::ViewPersisted,          "occ",   "occ",   Occupant::ViewPersisted},
        {"ViewTransient",              Setup::ViewTransient,          "occ",   "occ",   Occupant::ViewTransient},
        {"StaticSchemaLessDisk",       Setup::StaticDisk,             "occ",   "occ",   Occupant::SchemaLessDisk},
        {"StaticSchemaLessTransient",  Setup::StaticTransient,        "occ",   "occ",   Occupant::SchemaLessTransient},
        {"RecoveredRuntimeTopic",      Setup::RecoveredTyped,         "occ",   "occ",   Occupant::TypedDisk},
        {"RecoveredView",              Setup::RecoveredView,          "occ",   "occ",   Occupant::ViewPersisted},
        {"OrphanDefineDisk",           Setup::OrphanDefineDisk,       "occ",   "occ",   Occupant::SchemaLessDisk},
        {"OrphanDefineTransient",      Setup::OrphanDefineTransient,  "occ",   "occ",   Occupant::SchemaLessTransient},
        {"OrphanSink_KnownLimitation", Setup::OrphanSink,             "occ",   "occ",   Occupant::OrphanSink},
        {"OrphanSinkAfterRestart",     Setup::OrphanSinkAfterRestart, "occ",   "occ",   Occupant::Free},
        {"CaseVariantOfTyped",         Setup::TypedDisk,              "Casey", "casey", Occupant::CaseVariant},
        {"CaseVariantOfView",          Setup::ViewPersisted,          "Viewy", "viewy", Occupant::CaseVariant},
        {"CaseVariantOfStatic",        Setup::StaticDisk,             "Stat",  "stat",  Occupant::CaseVariant},
        {"CaseVariantOfOrphan",        Setup::OrphanDefineDisk,       "Orph",  "orph",  Occupant::CaseVariant},
    };
    const std::vector<std::pair<const char*, std::string>> bad = {
        {"Reserved_topic_schemas",      "__topic_schemas"},
        {"Reserved_views",              "__views"},
        {"Reserved_consumer_offsets",   "__consumer_offsets"},
        {"Reserved_transaction_state",  "__transaction_state"},
        {"Reserved_cluster_metadata",   "__cluster_metadata"},
        {"Reserved_view_snapshots",     "__view_snapshots"},
        {"Reserved_orphan_placeholder", "__orphan.7"},
        {"Invalid_Empty",               ""},
        {"Invalid_Slash",               "a/b"},
        {"Invalid_DotDotSlash",         "../escape"},
        {"Invalid_Absolute",            "/abs"},
        {"Invalid_Dot",                 "."},
        {"Invalid_DotDot",              ".."},
        {"Invalid_TooLong250",          std::string(250, 'x')},
        {"Invalid_Space",               "has space"},
        {"Invalid_Tab",                 "tab\tname"},
        {"Invalid_NonAscii",            "caf\xC3\xA9"},
        {"Invalid_EmbeddedNul",         std::string("nul\0byte", 8)},
    };
    for (const auto& [label, name] : bad) r.push_back({label, Setup::None, "", name, Occupant::BadName});
    r.push_back({"Valid_MaxLen249",         Setup::None, "", std::string(249, 'x'), Occupant::Free});
    r.push_back({"Valid_OneChar",           Setup::None, "", "a",                   Occupant::Free});
    r.push_back({"Valid_LeadingDotAndDash", Setup::None, "", ".hidden_ok-1",        Occupant::Free});
    return r;
}

DdlHarnessOptions optionsFor(const Row& row) {
    DdlHarnessOptions o;
    if (row.setup == Setup::StaticDisk || row.setup == Setup::StaticTransient) {
        TopicSpec s;
        s.name = row.occupant;
        s.partitionCount = 1;
        s.persistence = row.setup == Setup::StaticTransient ? PersistenceMode::Transient : PersistenceMode::Disk;
        o.statics.push_back(s);
    }
    return o;
}

void create(DdlHarness& h, Op op, const std::string& name) {
    const int64_t c = h.submit(op, name);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kOk) << "setup " << opName(op) << " '" << name << "'";
}

/// A create whose control thread made the topic but whose reply failed: an orphan.
void makeOrphan(DdlHarness& h, Op op, const std::string& name) {
    h.setControl(ControlMode::Hold);
    const int64_t c = h.submit(op, name);
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.failCreate(/*afterCreate=*/true);
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_EQ(h.response(c)->err, kInternal);
    ASSERT_GE(h.idOf(name), 0) << "the orphan topic exists";
}

void setupOccupant(DdlHarness& h, const Row& row) {
    switch (row.setup) {
        case Setup::None:
        case Setup::StaticDisk:
        case Setup::StaticTransient:        return;
        case Setup::TypedDisk:              create(h, Op::DefineDisk, row.occupant); return;
        case Setup::TypedTransient:         create(h, Op::DefineTransient, row.occupant); return;
        case Setup::ViewPersisted:          create(h, Op::ViewPersisted, row.occupant); return;
        case Setup::ViewTransient:          create(h, Op::ViewTransient, row.occupant); return;
        case Setup::RecoveredTyped:         create(h, Op::DefineDisk, row.occupant); h.restart(); return;
        case Setup::RecoveredView:          create(h, Op::ViewPersisted, row.occupant); h.restart(); return;
        case Setup::OrphanDefineDisk:       makeOrphan(h, Op::DefineDisk, row.occupant); return;
        case Setup::OrphanDefineTransient:  makeOrphan(h, Op::DefineTransient, row.occupant); return;
        case Setup::OrphanSink:             makeOrphan(h, Op::ViewPersisted, row.occupant); return;
        case Setup::OrphanSinkAfterRestart:
            makeOrphan(h, Op::ViewPersisted, row.occupant);
            if (::testing::Test::HasFatalFailure()) return;
            create(h, Op::DefineDisk, "later");   // a later recorded id: the orphan is a gap (B5)
            h.restart();
            return;
    }
}

void expectSuccessDelta(const Catalog& before, const Catalog& after, Op op, const std::string& name,
                        const Response& r) {
    const CatalogEntry* b = findEntry(before, name);
    const CatalogEntry* a = findEntry(after, name);
    ASSERT_NE(a, nullptr) << "the op's entry exists";
    EXPECT_EQ(a->id, r.id) << "the answer carries the entry's id";
    if (isCreate(op)) {
        EXPECT_EQ(after.size(), before.size() + (b ? 0u : 1u)) << "exactly one new topic, or the adopted one";
        if (b) EXPECT_EQ(a->id, b->id) << "adoption keeps the existing topic's id";
        EXPECT_EQ(a->kind, isView(op) ? EntryKind::View : EntryKind::Typed);
        EXPECT_EQ(a->version, 1u);
        EXPECT_EQ(a->keyField, "id") << "the requested key field";
        const PersistenceMode want =
            isView(op)  ? (op == Op::ViewTransient ? PersistenceMode::Transient : PersistenceMode::Disk)
            : b         ? b->persistence   // an adopted topic keeps ITS log backend
            : (op == Op::DefineTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
        EXPECT_EQ(a->persistence, want);
    } else {
        ASSERT_NE(b, nullptr);
        EXPECT_EQ(after.size(), before.size());
        EXPECT_EQ(a->id, b->id);
        EXPECT_EQ(a->version, b->version + 1) << "one contiguous version";
        EXPECT_EQ(a->persistence, b->persistence) << "ALTER carries persistence forward";
        EXPECT_EQ(r.version, a->version);
        EXPECT_EQ(a->keyField, op == Op::AlterRenameKey ? std::string("pk") : b->keyField)
            << "keyField follows a key-field RENAME (B8), and nothing else";
    }
    for (const CatalogEntry& e : before) {
        if (e.name == name) continue;
        const CatalogEntry* x = findEntry(after, e.name);
        ASSERT_NE(x, nullptr) << e.name;
        EXPECT_EQ(*x, e) << "every other entry is untouched";
    }
}

class DdlOccupantMatrixRegression : public ::testing::TestWithParam<std::tuple<Row, Op>> {};

TEST_P(DdlOccupantMatrixRegression, OneOpOnAnOccupiedName) {
    const Row& row = std::get<0>(GetParam());
    const Op op = std::get<1>(GetParam());
    DdlHarness h(optionsFor(row));
    setupOccupant(h, row);
    if (HasFatalFailure()) return;
    ASSERT_TRUE(h.violations().empty()) << h.violationText();

    const Catalog before = h.catalog();
    const int reqs = h.createRequests();
    const int64_t schemaEnd = h.schemaLogEnd();
    const int64_t viewsEnd = h.viewsLogEnd();
    const int plans = h.viewPlanPushes();

    const int64_t corr = h.submit(op, row.target);
    h.settle();

    const Cell want = expectedCell(row.expect, op);
    const std::optional<Response> got = h.response(corr);
    ASSERT_TRUE(got.has_value()) << "every op is answered";
    EXPECT_EQ(got->err, want.code) << opName(op) << " on " << occupantName(row.expect);
    EXPECT_EQ(h.createRequests() - reqs, want.createRequests) << "requestCreateTopic calls";

    const Catalog after = h.catalog();
    if (want.code != kOk || op == Op::GetSchema) {
        EXPECT_EQ(after, before) << "no half-creation: a rejected op / a read leaves the catalog untouched";
        EXPECT_EQ(h.schemaLogEnd(), schemaEnd) << "no stray TopicSchema record";
        EXPECT_EQ(h.viewsLogEnd(), viewsEnd) << "no stray __views record";
        EXPECT_EQ(h.viewPlanPushes(), plans) << "no worker install";
    } else {
        expectSuccessDelta(before, after, op, row.target, *got);
        EXPECT_EQ(h.schemaLogEnd() - schemaEnd, isDefine(op) || isAlter(op) ? 1 : 0);
        EXPECT_EQ(h.viewsLogEnd() - viewsEnd, isView(op) ? 1 : 0);
        EXPECT_EQ(h.viewPlanPushes() - plans, isView(op) ? 1 : 0);
    }
    if (op == Op::GetSchema && want.code == kOk) {
        const CatalogEntry* e = findEntry(before, row.target);
        ASSERT_NE(e, nullptr);
        EXPECT_EQ(got->id, e->id);
        EXPECT_EQ(got->version, e->version);
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    h.restart();
    EXPECT_EQ(h.catalog(), h.expectedAfterRestart(after)) << "state after a restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlOccupantMatrixRegression,
    ::testing::Combine(::testing::ValuesIn(rows()), ::testing::ValuesIn(kAllOps)),
    [](const ::testing::TestParamInfo<std::tuple<Row, Op>>& i) {
        return std::string(std::get<0>(i.param).label) + "_" + opName(std::get<1>(i.param));
    });

// ── Malformed / hand-built admin requests: every one is ANSWERED with an error ─────
class DdlMalformedRequestRegression : public ::testing::Test {
protected:
    void expectAnsweredUntouched(int64_t corr, uint16_t want) {
        ASSERT_TRUE(h.response(corr).has_value()) << "a malformed request is answered, never dropped";
        EXPECT_EQ(h.response(corr)->err, want);
        EXPECT_EQ(h.catalog(), before);
        EXPECT_EQ(h.schemaLogEnd(), schemaEnd);
        EXPECT_EQ(h.viewsLogEnd(), viewsEnd);
        EXPECT_TRUE(h.violations().empty()) << h.violationText();
    }
    DdlHarness h;
    Catalog before = h.catalog();
    int64_t schemaEnd = h.schemaLogEnd();
    int64_t viewsEnd = h.viewsLogEnd();
};

TEST_F(DdlMalformedRequestRegression, DefineWithUnknownPersistenceByteIsInvalidSchema_R1) {
    DefineTopicMsg m; m.name = "m"; m.keyField = "id"; m.blob = charBlob(); m.persistence = 2;
    const int reqs = h.createRequests();
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
    EXPECT_EQ(h.createRequests(), reqs);
}

TEST_F(DdlMalformedRequestRegression, DefineWithGarbageDescriptorIsInvalidSchema) {
    DefineTopicMsg m; m.name = "m"; m.keyField = "id"; m.blob = {0, 1, 2};
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, DefineWithEmptyDescriptorIsInvalidSchema) {
    DefineTopicMsg m; m.name = "m"; m.keyField = "id";   // what the ring decode leaves for a bad byte
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, CreateViewWithUnknownPersistenceByteIsInvalidSchema_R1) {
    CreateViewMsg m; m.viewName = "v"; m.keyField = "id"; m.persistence = 2;
    m.planBlob = viewPlanBlob("v", h.schemas().byName("src")->topicId, 0);
    const int reqs = h.createRequests();
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
    EXPECT_EQ(h.createRequests(), reqs);
}

TEST_F(DdlMalformedRequestRegression, CreateViewMarkedMalformedIsInvalidSchema) {
    CreateViewMsg m; m.malformed = true; m.viewName = "v";
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, CreateViewWithGarbagePlanIsFieldUnresolved) {
    CreateViewMsg m; m.viewName = "v"; m.keyField = "id"; m.planBlob = {9, 9, 9};
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kViewFieldUnresolved);
}

TEST_F(DdlMalformedRequestRegression, CreateViewWithEmptyPlanIsFieldUnresolved) {
    CreateViewMsg m; m.viewName = "v"; m.keyField = "id";
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kViewFieldUnresolved);
}

TEST_F(DdlMalformedRequestRegression, AlterWithGarbageOpsIsInvalidSchema) {
    AlterTopicMsg m; m.topicName = "src"; m.opsBlob = {0xff, 0xff, 0x01};
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, AlterWithEmptyOpsIsInvalidSchema) {
    AlterTopicMsg m; m.topicName = "src";
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, CreateReplyWithUnknownPersistenceIsInternal_R1) {
    h.setControl(ControlMode::Hold);
    const int64_t c = h.submit(Op::DefineDisk, "m");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.deliverCreateWithPersistence(2);   // a reply whose persistence byte is not a mode
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    EXPECT_EQ(h.response(c)->err, kInternal);
    EXPECT_EQ(h.schemas().byName("m"), nullptr);
    EXPECT_EQ(h.schemaLogEnd(), schemaEnd) << "nothing recorded";
}

// ── View source cells: source kind x view persistence ─────────────────────────────
struct SourceCase { const char* label; const char* source; Op view; uint16_t want; };
void PrintTo(const SourceCase& c, std::ostream* os) { *os << c.label; }

const SourceCase kSourceCases[] = {
    {"DiskSource_PersistedView",          "src",    Op::ViewPersisted, kOk},
    {"DiskSource_TransientView",          "src",    Op::ViewTransient, kOk},
    {"TransientSource_PersistedView",     "tsrc",   Op::ViewPersisted, kViewSourceTransient},
    {"TransientSource_TransientView",     "tsrc",   Op::ViewTransient, kOk},
    {"SchemaLessSource_PersistedView",    "ssrc",   Op::ViewPersisted, kViewSourceNotFound},
    {"SchemaLessSource_TransientView",    "ssrc",   Op::ViewTransient, kViewSourceNotFound},
    {"MissingSource_PersistedView",       "nosuch", Op::ViewPersisted, kViewSourceNotFound},
    {"MissingSource_TransientView",       "nosuch", Op::ViewTransient, kViewSourceNotFound},
    // B9 / user decision D3b: a view over another view's output (until Phase 3).
    {"PersistedViewSource_PersistedView", "vsrc",   Op::ViewPersisted, kViewSourceIsView},
    {"PersistedViewSource_TransientView", "vsrc",   Op::ViewTransient, kViewSourceIsView},
    {"TransientViewSource_PersistedView", "tvsrc",  Op::ViewPersisted, kViewSourceIsView},
    {"TransientViewSource_TransientView", "tvsrc",  Op::ViewTransient, kViewSourceIsView},
};

class DdlViewSourceRegression : public ::testing::TestWithParam<SourceCase> {};

TEST_P(DdlViewSourceRegression, ViewOverSource) {
    const SourceCase& c = GetParam();
    DdlHarnessOptions o;
    TopicSpec s; s.name = "ssrc"; s.partitionCount = 1;
    o.statics.push_back(s);
    DdlHarness h(o);
    create(h, Op::DefineTransient, "tsrc");
    create(h, Op::ViewPersisted, "vsrc");    // over "src"
    create(h, Op::ViewTransient, "tvsrc");   // over "src"
    if (HasFatalFailure()) return;
    const Catalog before = h.catalog();
    const int reqs = h.createRequests();
    const int64_t corr = h.submit(c.view, "vw", c.source);
    h.settle();
    ASSERT_TRUE(h.response(corr).has_value());
    EXPECT_EQ(h.response(corr)->err, c.want);
    EXPECT_EQ(h.createRequests() - reqs, c.want == kOk ? 1 : 0) << "a rejected view requests no sink";
    if (c.want != kOk) EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    if (c.want == kViewSourceIsView) {   // the rule holds after a restart (views recovered first)
        h.restart();
        const int64_t again = h.submit(c.view, "vw", c.source);
        h.settle();
        ASSERT_TRUE(h.response(again).has_value());
        EXPECT_EQ(h.response(again)->err, kViewSourceIsView) << "after restart";
    }
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlViewSourceRegression, ::testing::ValuesIn(kSourceCases),
    [](const ::testing::TestParamInfo<SourceCase>& i) { return std::string(i.param.label); });

} // namespace
```

Add to `DdlHarness` (public, next to `failCreate`). `CreateReplyWithUnknownPersistenceIsInternal_R1` uses it:

```cpp
    /// Answer the oldest create request like the control thread, but with the reply's
    /// persistence byte replaced by `persistence` (malformed-reply cells).
    void deliverCreateWithPersistence(uint8_t persistence) {
        const CreateTopicReqMsg req = popCreate_();
        CreateTopicReplyMsg reply = createTopicForAdmin(*topics_, req);
        reply.persistence = persistence;
        w_->onCreateTopicReply(reply);
        checkInvariant_("malformed create reply");
    }
```

- [ ] **Step 2: Add the file to CMake.** In `add_executable(rune_ddl_conformance_tests …)`, add the line `        tests/node/DdlOccupantMatrixRegressionTest.cpp` after `        tests/node/DdlHarnessRegressionTest.cpp`.

- [ ] **Step 3: Build and run.**
  Run: `cmake /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='Ddl/DdlOccupantMatrixRegression.*:DdlMalformedRequestRegression.*:Ddl/DdlViewSourceRegression.*' --gtest_brief=1`
  Expected: 456 + 10 + 12 = 478 PASS. B1–B10 are fixed, and the `_R1` cells pin the no-hang wave's R1 behaviour.
  **If any cell fails:** that cell is a new finding. Run superpowers:systematic-debugging on it, then add the cell to "Bugs this matrix is expected to expose" with a fix task. Never edit the expectation table to make a cell pass unless the plan's markdown table is wrong too, and say so in the commit message.

- [ ] **Step 4: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/tests/node/DdlOccupantMatrixRegressionTest.cpp libs/node/tests/support/DdlHarness.h libs/node/CMakeLists.txt
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "test(ddl): Occupant x Op conformance matrix (456 cells) + malformed admin requests + view-source cells (incl. view over view = 41)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: The pair × timing matrix (560 cells)

**Files:**
- Create: `libs/node/tests/node/DdlPairTimingMatrixRegressionTest.cpp`
- Modify: `libs/node/CMakeLists.txt`: add it to `rune_ddl_conformance_tests`

**Interfaces:**
- Consumes: `DdlHarness` (`setControl`, `tickUntilCreatePending`, `tickUntilAppended`, `failCreate`, `deliverCreate`, `advance`, `demote`, `promote`, `restart`), `expectedCell`, `occupantAfterSuccess`, `orphanOf`, `AdminWorker::ddlTimeoutNs()`.

- [ ] **Step 1: Write the matrix** `libs/node/tests/node/DdlPairTimingMatrixRegressionTest.cpp`:

```cpp
// DdlPairTimingMatrixRegressionTest.cpp — DDL conformance plan Task 9.
//
// A then B, for every create kind of A (and AlterAdd), 7 kinds of B, Same/Different
// name, and the 9 timing classes T1..T7.  B's expectation = the Occupant table row
// for the state of its name WHEN B EXECUTES (B is serialized behind A), except that
// a read (GetSchema) and a CAS version are taken at SUBMIT time.  Concurrent timings
// (T2-T4) are also checked for timing-invariance against a sequential T1 twin.
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

#include <ostream>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;

namespace {

enum class Timing : uint8_t {
    T1_Sequential, T2_BothQueued, T3_DuringAwaitingId, T4_DuringAwaitingCommit,
    T5a_ReplyFailsNoCreate, T5b_ReplyFailsAfterCreate, T5c_TimeoutInAwaitingId,
    T6_DemotedInAwaitingId, T7_RestartBetween,
};
const char* timingName(Timing t) {
    switch (t) {
        case Timing::T1_Sequential:             return "T1_Sequential";
        case Timing::T2_BothQueued:             return "T2_BothQueued";
        case Timing::T3_DuringAwaitingId:       return "T3_DuringAwaitingId";
        case Timing::T4_DuringAwaitingCommit:   return "T4_DuringAwaitingCommit";
        case Timing::T5a_ReplyFailsNoCreate:    return "T5a_ReplyFailsNoCreate";
        case Timing::T5b_ReplyFailsAfterCreate: return "T5b_ReplyFailsAfterCreate";
        case Timing::T5c_TimeoutInAwaitingId:   return "T5c_TimeoutInAwaitingId";
        case Timing::T6_DemotedInAwaitingId:    return "T6_DemotedInAwaitingId";
        case Timing::T7_RestartBetween:         return "T7_RestartBetween";
    }
    return "?";
}
enum class Rel : uint8_t { Same, Different };

struct PairCase { Op a; Op b; Rel rel; Timing t; };
void PrintTo(const PairCase& c, std::ostream* os) {
    *os << opName(c.a) << " then " << opName(c.b) << (c.rel == Rel::Same ? " same " : " different ")
        << timingName(c.t);
}

bool aFails(Timing t) { return t >= Timing::T5a_ReplyFailsNoCreate && t <= Timing::T6_DemotedInAwaitingId; }
bool aLeavesOrphan(Timing t) {
    return t == Timing::T5b_ReplyFailsAfterCreate || t == Timing::T5c_TimeoutInAwaitingId ||
           t == Timing::T6_DemotedInAwaitingId;
}
bool concurrent(Timing t) {
    return t == Timing::T2_BothQueued || t == Timing::T3_DuringAwaitingId || t == Timing::T4_DuringAwaitingCommit;
}

std::string aName(Op a) { return a == Op::AlterAdd ? "src" : "n"; }
std::string bName(const PairCase& c) {
    if (c.rel == Rel::Same) return aName(c.a);
    return isCreate(c.b) ? "m" : "src2";
}

Occupant committedKind(Op a) {
    switch (a) {
        case Op::DefineDisk:      return Occupant::TypedDisk;
        case Op::DefineTransient: return Occupant::TypedTransient;
        case Op::ViewPersisted:   return Occupant::ViewPersisted;
        case Op::ViewTransient:   return Occupant::ViewTransient;
        default:                  return Occupant::TypedDisk;   // AlterAdd on src
    }
}
Occupant occupantAfterA(const PairCase& c) {
    if (!aFails(c.t)) return committedKind(c.a);
    return aLeavesOrphan(c.t) ? orphanOf(c.a) : Occupant::Free;
}

Cell expectedB(const PairCase& c) {
    const Occupant o = c.rel == Rel::Same ? occupantAfterA(c)
                     : (isCreate(c.b) ? Occupant::Free : Occupant::TypedDisk);
    Cell cell = expectedCell(o, c.b);
    if (c.rel == Rel::Same && concurrent(c.t)) {
        // KL3: a read is answered at SUBMIT time from the committed state (before A).
        if (c.b == Op::GetSchema)
            cell = expectedCell(c.a == Op::AlterAdd ? Occupant::TypedDisk : Occupant::Free, Op::GetSchema);
        // CAS read v1 at submit time; A's ALTER commits v2 first: lost-update protection.
        if (c.b == Op::AlterCasCurrent && c.a == Op::AlterAdd) cell = Cell{kSchemaConflict, 0};
    }
    return cell;
}

uint16_t expectedBVersion(const PairCase& c) {
    const bool aAlter = c.a == Op::AlterAdd;
    switch (c.b) {
        case Op::DefineDisk: case Op::DefineTransient:
        case Op::ViewPersisted: case Op::ViewTransient:
            return 1;
        case Op::AlterAdd: case Op::AlterCasCurrent:
            return c.rel == Rel::Different ? 2 : (aAlter ? 3 : 2);
        case Op::GetSchema:
            if (c.rel == Rel::Different) return 1;
            return aAlter ? (concurrent(c.t) ? 1 : 2) : 1;
        default:
            return 0;
    }
}

void expectOccupant(const Catalog& cat, const std::string& name, Occupant o) {
    const CatalogEntry* e = findEntry(cat, name);
    switch (o) {
        case Occupant::Free:
            EXPECT_EQ(e, nullptr) << "'" << name << "' must not exist";
            return;
        case Occupant::TypedDisk:
        case Occupant::TypedTransient:
            ASSERT_NE(e, nullptr) << name;
            EXPECT_EQ(e->kind, EntryKind::Typed) << name;
            EXPECT_EQ(e->persistence, o == Occupant::TypedTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
            return;
        case Occupant::ViewPersisted:
        case Occupant::ViewTransient:
            ASSERT_NE(e, nullptr) << name;
            EXPECT_EQ(e->kind, EntryKind::View) << name;
            EXPECT_EQ(e->persistence, o == Occupant::ViewTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
            return;
        case Occupant::SchemaLessDisk:
        case Occupant::OrphanSink:
        case Occupant::SchemaLessTransient:
            ASSERT_NE(e, nullptr) << name;
            EXPECT_EQ(e->kind, EntryKind::SchemaLess) << name;
            EXPECT_EQ(e->persistence, o == Occupant::SchemaLessTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
            return;
        default:
            ADD_FAILURE() << "unexpected occupant " << occupantName(o);
    }
}

struct Ran { int64_t a = 0; int64_t b = 0; };

Ran runPair(DdlHarness& h, const PairCase& c) {
    Ran r;
    const std::string an = aName(c.a);
    const std::string bn = bName(c);
    switch (c.t) {
        case Timing::T1_Sequential:
            r.a = h.submit(c.a, an); h.settle();
            r.b = h.submit(c.b, bn); h.settle();
            break;
        case Timing::T2_BothQueued:
            r.a = h.submit(c.a, an);
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T3_DuringAwaitingId: {
            h.setControl(ControlMode::Hold);
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilCreatePending());
            r.b = h.submit(c.b, bn);
            const int reqs = h.createRequests();
            for (int i = 0; i < 5; ++i) h.tick();
            if (c.b != Op::GetSchema) EXPECT_FALSE(h.response(r.b).has_value()) << "B ran while A was AwaitingId";
            EXPECT_EQ(h.createRequests(), reqs) << "B requested a topic while A was in flight";
            h.setControl(ControlMode::Auto);
            h.deliverCreate();
            h.settle();
            break;
        }
        case Timing::T4_DuringAwaitingCommit: {
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilAppended());
            r.b = h.submit(c.b, bn);
            const int reqs = h.createRequests();
            for (int i = 0; i < 5; ++i) h.tick();
            EXPECT_FALSE(h.response(r.a).has_value()) << "A answered before its record committed";
            if (c.b != Op::GetSchema) EXPECT_FALSE(h.response(r.b).has_value()) << "B ran while A was AwaitingCommit";
            EXPECT_EQ(h.createRequests(), reqs);
            h.settle();   // releases the held commits
            break;
        }
        case Timing::T5a_ReplyFailsNoCreate:
        case Timing::T5b_ReplyFailsAfterCreate:
            h.setControl(ControlMode::Hold);
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilCreatePending());
            h.failCreate(/*afterCreate=*/c.t == Timing::T5b_ReplyFailsAfterCreate);
            h.setControl(ControlMode::Auto);
            h.settle();
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T5c_TimeoutInAwaitingId:
            h.setControl(ControlMode::Hold);
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilCreatePending());
            h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
            h.tick();
            EXPECT_TRUE(h.response(r.a).has_value()) << "A timed out";
            h.deliverCreate();   // the late reply: stale ticket, topic created (orphan)
            h.setControl(ControlMode::Auto);
            h.settle();
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T6_DemotedInAwaitingId:
            h.setControl(ControlMode::Hold);
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilCreatePending());
            h.demote();
            EXPECT_TRUE(h.response(r.a).has_value()) << "B4: answered at demotion";
            h.deliverCreate();   // the late reply: stale ticket, topic created (orphan)
            h.promote();
            h.setControl(ControlMode::Auto);
            h.settle();
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T7_RestartBetween:
            r.a = h.submit(c.a, an); h.settle();
            h.restart();
            r.b = h.submit(c.b, bn); h.settle();
            break;
    }
    return r;
}

std::vector<PairCase> pairCases() {
    const Op as[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient};
    const Op bs[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
                     Op::AlterAdd, Op::AlterCasCurrent, Op::GetSchema};
    std::vector<PairCase> v;
    for (Op a : as)
        for (Op b : bs)
            for (Rel rel : {Rel::Same, Rel::Different})
                for (int t = 0; t <= static_cast<int>(Timing::T7_RestartBetween); ++t)
                    v.push_back({a, b, rel, static_cast<Timing>(t)});
    for (Op b : bs)
        for (Rel rel : {Rel::Same, Rel::Different})
            for (Timing t : {Timing::T1_Sequential, Timing::T2_BothQueued, Timing::T4_DuringAwaitingCommit,
                             Timing::T7_RestartBetween})
                v.push_back({Op::AlterAdd, b, rel, t});
    return v;
}

class DdlPairTimingMatrixRegression : public ::testing::TestWithParam<PairCase> {};

TEST_P(DdlPairTimingMatrixRegression, AThenB) {
    const PairCase& c = GetParam();
    DdlHarnessOptions o;
    o.holdCommits = c.t == Timing::T4_DuringAwaitingCommit;
    DdlHarness h(o);
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    const int reqs0 = h.createRequests();

    const Ran r = runPair(h, c);
    if (HasFatalFailure()) return;

    const std::optional<Response> ra = h.response(r.a);
    const std::optional<Response> rb = h.response(r.b);
    ASSERT_TRUE(ra.has_value()) << "A answered";
    ASSERT_TRUE(rb.has_value()) << "B answered";
    EXPECT_EQ(ra->err, aFails(c.t) ? kInternal : kOk);
    if (!aFails(c.t)) EXPECT_EQ(ra->version, c.a == Op::AlterAdd ? 2u : 1u);
    const Cell wb = expectedB(c);
    EXPECT_EQ(rb->err, wb.code);
    if (wb.code == kOk) EXPECT_EQ(rb->version, expectedBVersion(c));
    EXPECT_EQ(h.createRequests() - reqs0, (isCreate(c.a) ? 1 : 0) + wb.createRequests)
        << "requestCreateTopic calls (A's + B's)";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // Final registry state of both names.
    const Catalog fin = h.catalog();
    const Occupant afterA = occupantAfterA(c);
    if (c.rel == Rel::Same) {
        expectOccupant(fin, aName(c.a), wb.code == kOk ? occupantAfterSuccess(afterA, c.b) : afterA);
    } else {
        expectOccupant(fin, aName(c.a), afterA);
        const Occupant before = isCreate(c.b) ? Occupant::Free : Occupant::TypedDisk;
        expectOccupant(fin, bName(c), wb.code == kOk ? occupantAfterSuccess(before, c.b) : before);
    }

    // Timing invariance: a concurrent schedule ends exactly like the sequential one.
    if (concurrent(c.t) && c.b != Op::GetSchema && c.b != Op::AlterCasCurrent) {
        DdlHarness twin;
        PairCase s = c;
        s.t = Timing::T1_Sequential;
        const Ran tr = runPair(twin, s);
        EXPECT_EQ(fin, twin.catalog()) << "timing changed the outcome";
        ASSERT_TRUE(twin.response(tr.b).has_value());
        EXPECT_EQ(rb->err, twin.response(tr.b)->err);
    }

    h.restart();
    EXPECT_EQ(h.catalog(), h.expectedAfterRestart(fin)) << "state after a restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlPairTimingMatrixRegression, ::testing::ValuesIn(pairCases()),
    [](const ::testing::TestParamInfo<PairCase>& i) {
        return std::string(opName(i.param.a)) + "_then_" + opName(i.param.b) + "_" +
               (i.param.rel == Rel::Same ? "Same" : "Different") + "_" + timingName(i.param.t);
    });

} // namespace
```

- [ ] **Step 2: Add the file to CMake.** Add `        tests/node/DdlPairTimingMatrixRegressionTest.cpp` to `rune_ddl_conformance_tests`.

- [ ] **Step 3: Build and run.**
  Run: `cmake /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='Ddl/DdlPairTimingMatrixRegression.*' --gtest_brief=1`
  Expected: 560 PASS.
  - A `*_Different_T5b|T5c|T6` failure in `restart()` means B5 regressed.
  - A `*_T6_*` failure where A was not answered at demotion means B4 regressed.
  - Any other failure is a new finding: run systematic-debugging, then add it to the bug list with a fix.

- [ ] **Step 4: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/tests/node/DdlPairTimingMatrixRegressionTest.cpp libs/node/CMakeLists.txt
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "test(ddl): A-then-B x 9 timing classes conformance matrix (560 cells) with timing-invariance twin

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Seeded, model-checked DDL stress + steady-state zero-alloc gate

**Files:**
- Create: `libs/node/tests/node/DdlStressRegressionTest.cpp`. This is its own executable, because it replaces global `operator new`.
- Modify: `libs/node/CMakeLists.txt`: new target after `rune_ddl_conformance_tests`

**Interfaces:**
- Consumes:
  - `DdlHarness` (`startedTotal`, `holdingCommits`, `failCreate`, `deliverCreate`, `demote`, `promote`, `restart`, `quiescent`);
  - `expectedCell`, `occupantAfterSuccess`, `orphanOf`;
  - `isValidTopicName`, `topicNamesFoldEqual`.
- Produces:
  - `DdlStressRegression/<seed>` for seeds 1..64;
  - `DdlStressEnvRegression.EnvSeed`, which replays one seed from `RUNE_DDL_STRESS_SEED`.

- [ ] **Step 1: Write the stress test** `libs/node/tests/node/DdlStressRegressionTest.cpp`:

```cpp
// DdlStressRegressionTest.cpp — DDL conformance plan Task 10.
//
// Deterministic, seeded, model-checked: 3 simulated sessions submit random DDL over
// a small colliding name pool (incl. a case variant, a reserved and an empty name)
// while the scheduler delivers control replies (ok / fail / fail-after-create /
// timeout / demotion) and releases commits in random interleavings, with occasional
// restarts.  Every answer is checked against a reference model — name ->
// Occupant (kind + persistence) + version — that applies ops in FIFO submission
// order using ONLY the expectation table.  The final catalog is checked against the
// model before and after a restart.
//
// Zero-alloc gate: the admin thread's steady-state loop — AdminWorker::doWork while
// idle, and while an op waits on the HW gate — must not allocate.  (DDL processing
// itself is the cold path and may.)  Own executable: replaces global operator new.
#include <gtest/gtest.h>
#include "support/DdlHarness.h"
#include <rune/node/TopicName.h>

#include <cstdlib>
#include <map>
#include <new>
#include <random>
#include <string>
#include <vector>

namespace {
bool     gArmed       = false;   // single-threaded test binary: plain globals, no atomics
uint64_t gArmedAllocs = 0;
}
void* operator new(std::size_t n) {
    if (gArmed) ++gArmedAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using namespace rune::node;
using namespace rune::node::test;

namespace {

enum class Fault : uint8_t { None, FailNoCreate, FailAfterCreate, TimeoutInAwaitingId, DemoteInAwaitingId };

struct Planned   { Op op; std::string name; Fault fault; };
struct Submitted { int64_t corr; Op op; std::string name; Fault fault; };

const std::vector<std::string> kPool = {"a", "b", "A", "s", "src", "__views", ""};
constexpr int kSessions = 3;
constexpr int kOpsPerSession = 40;

struct Model {
    std::map<std::string, Occupant> occ;       // exact name -> occupant (absent = Free)
    std::map<std::string, uint16_t> version;
    std::vector<std::string>        statics;   // schema-less boot topics survive restarts

    Occupant at(const std::string& n) const {
        if (!isValidTopicName(n)) return Occupant::BadName;
        if (const auto it = occ.find(n); it != occ.end() && it->second != Occupant::Free) return it->second;
        for (const auto& [m, o] : occ)
            if (m != n && o != Occupant::Free && topicNamesFoldEqual(m, n)) return Occupant::CaseVariant;
        return Occupant::Free;
    }
    /// Apply `s` (FIFO order) and return its expected answer.
    uint16_t apply(const Submitted& s) {
        const Occupant o = at(s.name);
        const Cell c = expectedCell(o, s.op);
        if (c.createRequests == 1 && s.fault != Fault::None) {
            if (s.fault != Fault::FailNoCreate && o == Occupant::Free) occ[s.name] = orphanOf(s.op);
            return kInternal;
        }
        if (c.code == kOk) {
            if (isCreate(s.op)) { occ[s.name] = occupantAfterSuccess(o, s.op); version[s.name] = 1; }
            else if (isAlter(s.op)) version[s.name] = static_cast<uint16_t>(version[s.name] + 1);
        }
        return c.code;
    }
    void restart() {   // an orphan's name is free again (it became __orphan.<id> or vanished)
        for (auto& [n, o] : occ) {
            const bool schemaLess = o == Occupant::SchemaLessDisk || o == Occupant::SchemaLessTransient ||
                                    o == Occupant::OrphanSink;
            bool isStatic = false;
            for (const std::string& s : statics) isStatic = isStatic || s == n;
            if (schemaLess && !isStatic) o = Occupant::Free;
        }
    }
};

Op drawOp(std::mt19937_64& rng) {
    static const Op ops[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
                             Op::AlterAdd, Op::AlterSetCompat, Op::GetSchema};
    return ops[rng() % (sizeof(ops) / sizeof(ops[0]))];
}
Fault drawFault(std::mt19937_64& rng) {
    const uint64_t r = rng() % 100;
    if (r < 70) return Fault::None;
    return static_cast<Fault>(1 + (r % 4));
}

void expectCatalogMatchesModel(DdlHarness& h, const Model& m, const char* when) {
    const Catalog cat = h.catalog();
    std::vector<std::string> names = kPool;
    names.push_back("src2");
    for (const std::string& n : names) {
        SCOPED_TRACE(std::string(when) + " name='" + n + "'");
        const CatalogEntry* e = findEntry(cat, n);
        const auto it = m.occ.find(n);
        const Occupant o = (it == m.occ.end() || !isValidTopicName(n)) ? Occupant::Free : it->second;
        switch (o) {
            case Occupant::Free:
                EXPECT_EQ(e, nullptr);
                break;
            case Occupant::TypedDisk:
            case Occupant::TypedTransient:
                ASSERT_NE(e, nullptr);
                EXPECT_EQ(e->kind, EntryKind::Typed);
                EXPECT_EQ(e->persistence, o == Occupant::TypedTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
                EXPECT_EQ(e->version, m.version.at(n));
                break;
            case Occupant::ViewPersisted:
            case Occupant::ViewTransient:
                ASSERT_NE(e, nullptr);
                EXPECT_EQ(e->kind, EntryKind::View);
                EXPECT_EQ(e->persistence, o == Occupant::ViewTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
                break;
            default:   // SchemaLessDisk / SchemaLessTransient / OrphanSink
                ASSERT_NE(e, nullptr);
                EXPECT_EQ(e->kind, EntryKind::SchemaLess);
                EXPECT_EQ(e->persistence, o == Occupant::SchemaLessTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
                break;
        }
    }
}

/// Arm the allocation counter around ticks that must be steady-state.
void expectNoSteadyStateAlloc(DdlHarness& h, const char* what) {
    for (int i = 0; i < 3; ++i) h.tick();   // warm-up: first-use paths may allocate once
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 16; ++i) h.tick();
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u) << "AdminWorker::doWork allocated in steady state (" << what << ")";
}

void runSeed(uint64_t seed) {
    SCOPED_TRACE("seed=" + std::to_string(seed) + "  replay: RUNE_DDL_STRESS_SEED=" + std::to_string(seed) +
                 " rune_ddl_stress_tests --gtest_filter=DdlStressEnvRegression.EnvSeed");
    std::mt19937_64 rng(seed);

    DdlHarnessOptions o;
    o.holdCommits = true;
    TopicSpec s; s.name = "s"; s.partitionCount = 1;
    o.statics.push_back(s);
    DdlHarness h(o);   // pre-defines src, src2 (typed disk v1)
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    h.setControl(ControlMode::Hold);

    Model m;
    m.statics = {"s"};
    m.occ["s"] = Occupant::SchemaLessDisk;
    m.occ["src"] = Occupant::TypedDisk;  m.version["src"] = 1;
    m.occ["src2"] = Occupant::TypedDisk; m.version["src2"] = 1;

    std::vector<std::vector<Planned>> scripts(kSessions);
    for (auto& sc : scripts)
        for (int i = 0; i < kOpsPerSession; ++i)
            sc.push_back(Planned{drawOp(rng), kPool[rng() % kPool.size()], drawFault(rng)});
    std::vector<int> next(kSessions, 0);

    std::vector<Submitted> ddl;   // DDL submissions, FIFO
    std::size_t applied = 0;      // DDL answers folded into the model
    auto processAnswers = [&] {
        while (applied < ddl.size()) {
            const std::optional<Response> r = h.response(ddl[applied].corr);
            if (!r) break;
            const uint16_t want = m.apply(ddl[applied]);
            EXPECT_EQ(r->err, want) << "op#" << applied << " " << opName(ddl[applied].op) << " '"
                                    << ddl[applied].name << "' fault=" << static_cast<int>(ddl[applied].fault);
            ++applied;
        }
    };
    auto actOnCreate = [&] {
        if (!h.worker().hasInFlight()) { h.deliverCreate(); return; }   // a stale request
        const Submitted& in = ddl[h.startedTotal() - 1];
        switch (in.fault) {
            case Fault::None:            h.deliverCreate(); break;
            case Fault::FailNoCreate:    h.failCreate(false); break;
            case Fault::FailAfterCreate: h.failCreate(true); break;
            case Fault::TimeoutInAwaitingId:
                h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
                h.tick();            // answers the op Internal (may start the next op)
                h.deliverCreate();   // the stale reply (FIFO: it is the oldest request)
                break;
            case Fault::DemoteInAwaitingId:
                h.demote();
                h.deliverCreate();   // stale reply
                h.promote();
                break;
        }
    };
    auto scriptsDone = [&] {
        for (int i = 0; i < kSessions; ++i) if (next[i] < kOpsPerSession) return false;
        return true;
    };

    int step = 0;
    for (; step < 50'000; ++step) {
        if (scriptsDone() && h.quiescent()) break;
        const uint64_t a = rng() % 100;
        if (a < 30) {
            const int si = static_cast<int>(rng() % kSessions);
            if (next[si] == kOpsPerSession) continue;
            const Planned& p = scripts[si][next[si]++];
            if (p.op == Op::GetSchema) {
                processAnswers();
                const int64_t c = h.submit(Op::GetSchema, p.name);
                const std::optional<Response> r = h.response(c);
                ASSERT_TRUE(r.has_value()) << "a read is answered synchronously";
                const Occupant occ = m.at(p.name);
                EXPECT_EQ(r->err, expectedCell(occ, Op::GetSchema).code) << "GetSchema '" << p.name << "'";
                if (r->err == kOk) EXPECT_EQ(r->version, m.version[p.name]);
            } else {
                const int64_t c = h.submit(p.op, p.name);
                ddl.push_back(Submitted{c, p.op, p.name, p.fault});
            }
        } else if (a < 70) {
            h.tick();
        } else if (a < 85) {
            if (h.hasPendingCreate()) actOnCreate();
        } else if (a < 95) {
            if (h.worker().hasInFlight() && !h.hasPendingCreate() && h.holdingCommits())
                expectNoSteadyStateAlloc(h, "HW-gate wait");
            h.releaseCommits();
        } else if (a < 98) {
            if (h.quiescent()) expectNoSteadyStateAlloc(h, "idle");
        } else if (h.quiescent()) {
            processAnswers();
            m.restart();
            h.restart();
        }
        processAnswers();
        if (::testing::Test::HasFatalFailure()) return;
    }
    ASSERT_LT(step, 50'000) << "the schedule did not quiesce";
    processAnswers();
    EXPECT_EQ(applied, ddl.size()) << "every DDL op answered";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    expectCatalogMatchesModel(h, m, "end");
    expectNoSteadyStateAlloc(h, "idle at end");

    m.restart();
    h.restart();
    expectCatalogMatchesModel(h, m, "after restart");
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

class DdlStressRegression : public ::testing::TestWithParam<uint64_t> {};
TEST_P(DdlStressRegression, SeededScheduleMatchesTheModel) { runSeed(GetParam()); }
INSTANTIATE_TEST_SUITE_P(Seeds, DdlStressRegression, ::testing::Range<uint64_t>(1, 65));

TEST(DdlStressEnvRegression, EnvSeed) {
    const char* s = std::getenv("RUNE_DDL_STRESS_SEED");
    if (s == nullptr) GTEST_SKIP() << "set RUNE_DDL_STRESS_SEED=<seed> to replay one seed";
    runSeed(std::strtoull(s, nullptr, 10));
}

} // namespace
```

- [ ] **Step 2: Add the target.** After the `rune_ddl_conformance_tests` block in `libs/node/CMakeLists.txt`:

```cmake
    # DDL conformance plan Task 10: seeded model-checked DDL stress + the admin
    # steady-state zero-alloc gate.  Own executable (replaces global operator new).
    add_executable(rune_ddl_stress_tests
        tests/node/DdlStressRegressionTest.cpp)
    target_include_directories(rune_ddl_stress_tests PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(rune_ddl_stress_tests PRIVATE
        rune_node
        rune_view
        rune_replication
        rune_codec
        GTest::gtest_main)
    gtest_discover_tests(rune_ddl_stress_tests
        PROPERTIES TIMEOUT 180)
```

- [ ] **Step 3: Build and run.**
  Run: `cmake /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_stress_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_stress_tests --gtest_brief=1`
  Expected: 64 seeds PASS, and EnvSeed is SKIPPED.
  - **If the zero-alloc gate fires:** this is a real finding (CLAUDE.md #4). Find the allocator: set a breakpoint on `operator new` while `gArmed` is set, or temporarily print `__builtin_return_address(0)`. Fix it in the owning component, which is the AdminWorker idle path or `PartitionReplica::doWork`. Add a `*Regression*` test there, and list it under the bugs. Never weaken the gate.
  - **If a seed fails:** replay it with `RUNE_DDL_STRESS_SEED=<seed> /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_stress_tests --gtest_filter=DdlStressEnvRegression.EnvSeed`, then use systematic-debugging.

- [ ] **Step 4: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/tests/node/DdlStressRegressionTest.cpp libs/node/CMakeLists.txt
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "test(ddl): seeded model-checked DDL stress (64 seeds) + admin steady-state zero-alloc gate

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Wire cells (SchemaGatewayTest `realControl`): adoption, names, malformed frames

**Files:**
- Modify: `libs/node/tests/integration/SchemaGatewayTest.cpp`: add `#include <rune/protocol/client/ControlMessages.h>`; append the suites at the end of the file

**Interfaces:**
- Consumes:
  - `SgHarness` (`realControl`, `topicReg`, `schemaReg`, `schemaLog`, `gw`, `captured`, `pump()`, `admin`);
  - `encodeDefineFrame`, `encodeAlterFrame`, `firstFrame<R>`, `charsBlob()` (all in this file).
- Malformed-frame cells pin the no-hang wave's gateway behaviour (`8a693c5`, `RequestRejection.h`). A DDL frame whose header decodes on a known session but whose body does not is answered **exactly once**, with **its own response type** carrying `InvalidSchema (23)` on the header's correlationId. An unknown session is never answered. This parameterized suite extends the wave's four single tests (`SchemaGatewayRegression.Malformed{GetTopicSchema,AlterTopic,CreateView}IsAnsweredInvalidSchemaRegression`, `DefineTopicUnknownPersistenceByteIsAnsweredInvalidSchemaRegression`) to every DDL op × every corruption; it does not duplicate them.

- [ ] **Step 1: Write the suites.** Append to `SchemaGatewayTest.cpp`:

```cpp
// ===========================================================================
// DDL conformance plan Task 11 — wire-level cells through the gateway with the
// control thread played by createTopicForAdmin over the REAL TopicRegistry.
// ===========================================================================
namespace {

// ── control-thread adoption of a schema-less static topic ────────────────────
struct AdoptCase {
    const char* label; PersistenceMode staticMode; int32_t staticParts;
    uint8_t definePersistence; uint32_t requestParts; uint16_t wantCode; PersistenceMode wantRecorded;
};
void PrintTo(const AdoptCase& c, std::ostream* os) { *os << c.label; }
const AdoptCase kAdoptCases[] = {
    {"DiskStatic_DiskDefine_MorePartitions",  PersistenceMode::Disk,      3, 0, 8, 0,  PersistenceMode::Disk},
    {"DiskStatic_DiskDefine_FewerPartitions", PersistenceMode::Disk,      3, 0, 1, 0,  PersistenceMode::Disk},
    {"DiskStatic_TransientDefine",            PersistenceMode::Disk,      3, 1, 3, 25, PersistenceMode::Disk},
    {"TransientStatic_DiskDefine",            PersistenceMode::Transient, 2, 0, 5, 0,  PersistenceMode::Transient},
    {"TransientStatic_TransientDefine",       PersistenceMode::Transient, 2, 1, 2, 0,  PersistenceMode::Transient},
};

class SchemaGatewayAdoptionRegression : public ::testing::TestWithParam<AdoptCase> {};

TEST_P(SchemaGatewayAdoptionRegression, DefineOverStaticSchemaLessTopic) {
    const AdoptCase& c = GetParam();
    SgHarness h;
    h.realControl = true;
    rune::node::TopicSpec boot;
    boot.name = "orders"; boot.partitionCount = c.staticParts; boot.persistence = c.staticMode;
    const int32_t id = h.topicReg->createTopic(boot);
    const int32_t topicsBefore = h.topicReg->topicCount();
    const int64_t schemaEnd = h.schemaLog.logEndOffset();

    const auto def = encodeDefineFrame(1, 1, "orders", c.requestParts, "userId", false, charsBlob(),
                                       c.definePersistence);
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, c.wantCode);
    EXPECT_EQ(h.topicReg->topicCount(), topicsBefore) << "adoption never creates a second topic";
    EXPECT_EQ(h.topicReg->topic(id)->persistence, c.staticMode) << "the live topic keeps its backend";
    EXPECT_EQ(h.topicReg->topic(id)->partitionCount, c.staticParts);
    if (c.wantCode == 0) {
        EXPECT_EQ(r->topicId, id);
        const RegistryEntry* e = h.schemaReg.byName("orders");
        ASSERT_NE(e, nullptr);
        EXPECT_EQ(e->persistence, c.wantRecorded);
        EXPECT_EQ(e->partitionCount, c.staticParts) << "the authoritative count, never the requested one";
        EXPECT_EQ(h.schemaLog.logEndOffset(), schemaEnd + 1);
    } else {
        EXPECT_EQ(h.schemaReg.byName("orders"), nullptr);
        EXPECT_EQ(h.schemaLog.logEndOffset(), schemaEnd);
    }
}

INSTANTIATE_TEST_SUITE_P(Ddl, SchemaGatewayAdoptionRegression, ::testing::ValuesIn(kAdoptCases),
    [](const ::testing::TestParamInfo<AdoptCase>& i) { return std::string(i.param.label); });

// ── names over the wire (B1/B2/B3/B6) ────────────────────────────────────────
struct WireNameCase { const char* label; std::string name; uint32_t parts; uint16_t wantCode; };
void PrintTo(const WireNameCase& c, std::ostream* os) { *os << c.label; }
const WireNameCase kWireNameCases[] = {
    {"Empty",              "",                     1, 40},
    {"Slash",              "a/b",                  1, 40},
    {"DotDot",             "..",                   1, 40},
    {"ReservedViews",      "__views",              1, 40},
    {"ReservedSchemas",    "__topic_schemas",      1, 40},
    {"TooLong",            std::string(250, 'x'),  1, 40},
    {"CaseOfStaticEvents", "Events",               1, 25},
    {"ZeroPartitions",     "fresh",                0, 23},
};

class SchemaGatewayNameRegression : public ::testing::TestWithParam<WireNameCase> {};

TEST_P(SchemaGatewayNameRegression, DefineIsRejectedAndCreatesNothing) {
    const WireNameCase& c = GetParam();
    SgHarness h;   // static "events" (id 0)
    h.realControl = true;
    const int32_t topicsBefore = h.topicReg->topicCount();
    const int64_t schemaEnd = h.schemaLog.logEndOffset();
    const auto def = encodeDefineFrame(1, 9, c.name, c.parts, "userId", false, charsBlob());
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->correlationId, 9);
    EXPECT_EQ(r->errorCode, c.wantCode);
    EXPECT_EQ(h.topicReg->topicCount(), topicsBefore);
    EXPECT_EQ(h.schemaLog.logEndOffset(), schemaEnd);
    EXPECT_FALSE(h.topicReg->topicIdForName(c.name).has_value());
}

INSTANTIATE_TEST_SUITE_P(Ddl, SchemaGatewayNameRegression, ::testing::ValuesIn(kWireNameCases),
    [](const ::testing::TestParamInfo<WireNameCase>& i) { return std::string(i.param.label); });

// ── malformed frames: every DDL op with a corrupt body is ANSWERED (never times out)
enum class DdlFrame : uint8_t { Define, CreateView, Alter, GetSchema };
enum class Corruption : uint8_t { UnknownPersistenceByte, HeaderOnly, CutMidBody, UnknownSession };
struct MalformedCase { const char* label; DdlFrame kind; Corruption corruption; };
void PrintTo(const MalformedCase& c, std::ostream* os) { *os << c.label; }
constexpr int64_t kMalformedCorr = 4242;

std::vector<uint8_t> validDdlFrame(DdlFrame k, int64_t session) {
    switch (k) {
        case DdlFrame::Define:
            return encodeDefineFrame(session, kMalformedCorr, "orders", 1, "userId", false, charsBlob(),
                                     pc::DefineTopic::kPersistenceTransient);
        case DdlFrame::CreateView: {
            const std::vector<uint8_t> plan{1, 2, 3};
            pc::CreateView m;
            m.sessionId = session; m.correlationId = kMalformedCorr; m.viewName = "v"; m.keyField = "id";
            m.persistence = 1; m.planBlob = plan.data(); m.planLen = static_cast<uint32_t>(plan.size());
            std::vector<uint8_t> f(m.encodedSize());
            m.encode(f.data());
            return f;
        }
        case DdlFrame::Alter:
            return encodeAlterFrame(session, kMalformedCorr, "orders", 0, {});
        case DdlFrame::GetSchema: {
            pc::GetTopicSchema g;
            g.sessionId = session; g.correlationId = kMalformedCorr; g.topicName = "orders";
            std::vector<uint8_t> f(g.encodedSize());
            g.encode(f.data());
            return f;
        }
    }
    return {};
}

std::vector<uint8_t> corrupt(DdlFrame k, Corruption c) {
    std::vector<uint8_t> f = validDdlFrame(k, c == Corruption::UnknownSession ? 99 : 1);
    switch (c) {
        case Corruption::UnknownPersistenceByte:
        case Corruption::UnknownSession:   // corrupt body on a session the gateway does not know
            if (k == DdlFrame::Define)          f.back() = 2;
            else if (k == DdlFrame::CreateView) f[pc::MessageHeader::ENCODED_SIZE] = 2;
            else                                f.resize(pc::MessageHeader::ENCODED_SIZE);
            break;
        case Corruption::HeaderOnly:
            f.resize(pc::MessageHeader::ENCODED_SIZE);
            break;
        case Corruption::CutMidBody:
            f.resize(f.size() - 3);
            break;
    }
    return f;
}

bool decodes(DdlFrame k, const std::vector<uint8_t>& f) {
    switch (k) {
        case DdlFrame::Define:     return pc::DefineTopic::decode(f.data(), f.size()).has_value();
        case DdlFrame::CreateView: return pc::CreateView::decode(f.data(), f.size()).has_value();
        case DdlFrame::Alter:      return pc::AlterTopic::decode(f.data(), f.size()).has_value();
        case DdlFrame::GetSchema:  return pc::GetTopicSchema::decode(f.data(), f.size()).has_value();
    }
    return false;
}

void apply(SgHarness& h, DdlFrame k, const std::vector<uint8_t>& f) {
    switch (k) {
        case DdlFrame::Define:     h.gw.applyDefineTopicForTest(f.data(), f.size()); break;
        case DdlFrame::CreateView: h.gw.applyCreateViewForTest(f.data(), f.size()); break;
        case DdlFrame::Alter:      h.gw.applyAlterTopicForTest(f.data(), f.size()); break;
        case DdlFrame::GetSchema:  h.gw.applyGetTopicSchemaForTest(f.data(), f.size()); break;
    }
}

/// Frames answering `corr`: the op's OWN response type carrying InvalidSchema (23) —
/// the no-hang wave's rule (RequestRejection.h::encodeRejection).  Any other frame on
/// `corr` (a generic ErrorResponse, another code) is counted in `other`.
template <class R>
int errorAnswers(const SgHarness& h, int64_t corr, int& other) {
    int n = 0;
    for (const auto& [sid, bytes] : h.captured) {
        if (auto r = R::decode(bytes.data(), bytes.size()); r && r->correlationId == corr) {
            if (r->errorCode == static_cast<uint16_t>(pc::ErrorCode::InvalidSchema)) ++n; else ++other;
        } else if (auto e = pc::ErrorResponse::decode(bytes.data(), bytes.size()); e && e->correlationId == corr) {
            ++other;
        }
    }
    return n;
}
int errorAnswersFor(const SgHarness& h, DdlFrame k, int& other) {
    switch (k) {
        case DdlFrame::Define:     return errorAnswers<pc::DefineTopicResponse>(h, kMalformedCorr, other);
        case DdlFrame::CreateView: return errorAnswers<pc::CreateViewResponse>(h, kMalformedCorr, other);
        case DdlFrame::Alter:      return errorAnswers<pc::AlterTopicResponse>(h, kMalformedCorr, other);
        case DdlFrame::GetSchema:  return errorAnswers<pc::TopicSchemaResponse>(h, kMalformedCorr, other);
    }
    return 0;
}

const MalformedCase kMalformedCases[] = {
    {"Define_UnknownPersistenceByte",     DdlFrame::Define,     Corruption::UnknownPersistenceByte},
    {"Define_HeaderOnly",                 DdlFrame::Define,     Corruption::HeaderOnly},
    {"Define_CutMidBody",                 DdlFrame::Define,     Corruption::CutMidBody},
    {"Define_UnknownSession",             DdlFrame::Define,     Corruption::UnknownSession},
    {"CreateView_UnknownPersistenceByte", DdlFrame::CreateView, Corruption::UnknownPersistenceByte},
    {"CreateView_HeaderOnly",             DdlFrame::CreateView, Corruption::HeaderOnly},
    {"CreateView_CutMidBody",             DdlFrame::CreateView, Corruption::CutMidBody},
    {"CreateView_UnknownSession",         DdlFrame::CreateView, Corruption::UnknownSession},
    {"Alter_HeaderOnly",                  DdlFrame::Alter,      Corruption::HeaderOnly},
    {"Alter_CutMidBody",                  DdlFrame::Alter,      Corruption::CutMidBody},
    {"Alter_UnknownSession",              DdlFrame::Alter,      Corruption::UnknownSession},
    {"GetSchema_HeaderOnly",              DdlFrame::GetSchema,  Corruption::HeaderOnly},
    {"GetSchema_CutMidBody",              DdlFrame::GetSchema,  Corruption::CutMidBody},
    {"GetSchema_UnknownSession",          DdlFrame::GetSchema,  Corruption::UnknownSession},
};

class SchemaGatewayMalformedDdlRegression : public ::testing::TestWithParam<MalformedCase> {};

TEST_P(SchemaGatewayMalformedDdlRegression, CorruptBodyIsAnsweredNeverDropped) {
    const MalformedCase& c = GetParam();
    SgHarness h;
    h.realControl = true;
    const std::vector<uint8_t> f = corrupt(c.kind, c.corruption);
    ASSERT_FALSE(decodes(c.kind, f)) << "precondition: the frame body really is corrupt";
    const int64_t schemaEnd = h.schemaLog.logEndOffset();
    const int32_t topicsBefore = h.topicReg->topicCount();

    apply(h, c.kind, f);
    h.pump();

    if (c.corruption == Corruption::UnknownSession) {
        // No session to answer on: the only DDL frame that is not answered (counted + warn-logged).
        EXPECT_TRUE(h.captured.empty()) << "nothing is sent to an unknown session";
    } else {
        int other = 0;
        EXPECT_EQ(errorAnswersFor(h, c.kind, other), 1)
            << "exactly one own-type InvalidSchema (23) answer on correlationId " << kMalformedCorr
            << " — none may time out";
        EXPECT_EQ(other, 0) << "no second / differently-coded answer";
    }
    EXPECT_EQ(h.admin->pendingCount(), 0u) << "a corrupt frame never reaches the admin queue";
    EXPECT_EQ(h.schemaLog.logEndOffset(), schemaEnd);
    EXPECT_EQ(h.topicReg->topicCount(), topicsBefore);
}

INSTANTIATE_TEST_SUITE_P(Ddl, SchemaGatewayMalformedDdlRegression, ::testing::ValuesIn(kMalformedCases),
    [](const ::testing::TestParamInfo<MalformedCase>& i) { return std::string(i.param.label); });

} // namespace

// ── reads / alters of a schema-less static topic (Occupant SchemaLessDisk) ────
TEST(SchemaGatewayStaticRegression, GetSchemaOfStaticSchemaLessTopicIsUnknownTopic) {
    SgHarness h;
    h.realControl = true;
    const auto r = getSchema(h, "events", 31);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, 7u);
}

TEST(SchemaGatewayStaticRegression, AlterOfStaticSchemaLessTopicIsUnknownTopic) {
    SgHarness h;
    h.realControl = true;
    const auto f = encodeAlterFrame(1, 32, "events", 0, rune::codec::encodeAlterOps({addColumnOp("x", PrimType::Int64)}));
    h.gw.applyAlterTopicForTest(f.data(), f.size());
    h.pump();
    const auto r = firstFrame<pc::AlterTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, 7u);
    EXPECT_EQ(h.schemaReg.byName("events"), nullptr);
}
```

The two plain tests use their own suite name, `SchemaGatewayStaticRegression`, because a plain `TEST` cannot share a suite name with a `TEST_P` fixture class.

`getSchema` and `firstFrame` live in an anonymous namespace earlier in the file. The new anonymous namespace block can use them because both are in the same TU.

- [ ] **Step 2: Build and run.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_schema_gateway_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_schema_gateway_tests --gtest_filter='Ddl/*:SchemaGatewayStaticRegression.*' --gtest_brief=1`
  Expected: 5 + 8 + 14 + 2 = 29 PASS.
  If a cell answers 0 frames or a different code, the no-hang wave's gateway rule has regressed. Run systematic-debugging on `ClientGateway::handle{DefineTopic,GetTopicSchema,AlterTopic,CreateView}` against `RequestRejection.h`; do not edit the cell.

- [ ] **Step 3: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/tests/integration/SchemaGatewayTest.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "test(ddl): wire cells — static-topic adoption, name rule, corrupt DDL frames always answered

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Integration — a real single-node BrokerNode over real Aeron

**Scope:**
- Covered, one representative slice per timing class:
  - T1, plus the name rule on the wire;
  - T2: pipelined frames from two sessions on one publication, so the gateway sees them in send order;
  - T3/T4: a 30-frame interleaved burst, checked against the table;
  - T5a and T5b + B5: a fault hook plus a restart, proving the `__orphan.<id>` placeholder is hidden and inert (D1) and its data dir is deleted (B10);
  - D4: a static topic removed from `RUNE_TOPICS` is quarantined with its data intact and readable, and re-adding it starts empty;
  - T7: catalog and collisions across a restart.
- **T6 (leader change mid-DDL) and 3-node failover are out of scope.** The schema and views replicas are `NullPartitionTransport`-backed (`BrokerNode.cpp` schema-replica block) and multi-node catalog replication is deferred (KL4). T6 is covered at unit level by Task 7 and the T6 column of Task 9.

**Files:**
- Modify: `libs/node/include/rune/node/BrokerNode.h`: test hook next to `setSchemaRecoveryFaultForTest` (~L533); members next to `schemaRecoveryFaultForTest_` (~L1186)
- Modify: `libs/node/src/BrokerNode.cpp`: create-topic drain (~L3861-3893)
- Modify: `libs/node/tests/support/ShardTestClient.h`: async DDL helpers + response maps
- Create: `libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp`
- Modify: `libs/node/CMakeLists.txt`: new target inside `if(BUILD_BROKER_NODE_TESTS)`, after `rune_broker_node_tests`

**Interfaces:**
- Produces:
  - `void BrokerNode::setAdminCreateFaultForTest(std::string name, bool afterCreate)`. Set it before `start()`. It applies to the first admin create request for `name`. afterCreate=false fails before creating; true creates the topic and then answers a failure (an orphan). Afterwards only the control thread reads and writes the state.
  - `ShardTestClient`:

    | Kind | Members |
    |---|---|
    | send | `sendDefineTopic(ch, stream, sid, name, partitions, descriptor, persistence=0) -> corr`, `sendCreateView(ch, stream, sid, viewName, keyField, persistence, plan) -> corr`, `sendAlterTopic(ch, stream, sid, name, expectedVersion, ops) -> corr`, `sendGetTopicSchema(ch, stream, sid, name) -> corr` |
    | await | `awaitDefine(corr, deadline)`, `awaitCreateView(corr, deadline)`, `awaitAlter(corr, deadline)`, `awaitSchema(corr, deadline) -> std::optional<SchemaAnswer>` |
    | type | `struct SchemaAnswer { int32_t topicId; uint16_t version; uint16_t errorCode; int32_t partitionCount; std::vector<uint8_t> blob; bool operator==(...) const = default; }` |
    | subscribe | `sendSubscribe(ch, stream, sid, topicId, partition) -> corr`, `awaitSubscribeCode(corr, deadline) -> std::optional<int16_t>` (the SubscribeResponse errorCode, or an ErrorResponse code on that corr) |

    Every send returns 0 when the frame cannot be sent.

- [ ] **Step 1: Write the failing integration tests** `libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp`:

```cpp
// DdlConformanceBrokerRegressionTest.cpp — DDL conformance plan Task 12.
//
// A REAL single-node BrokerNode over REAL Aeron (embedded drivers), driven by the
// wire client ShardTestClient.  One representative slice per timing class:
//   T1  sequential collisions answer the exact codes (incl. the name rule);
//   T2  pipelined same-name DDL from two sessions: exactly one wins (FIFO);
//   T3/T4 a 30-frame interleaved burst: every answer matches the expectation table;
//   T5  a failed control reply (before / after creating) + B5: the broker restarts;
//   T7  catalog + collisions are identical across a restart.
// T6 / 3-node failover: out of scope (NullPartitionTransport catalogs; KL4).
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>
#include "support/DdlHarness.h"          // charBlob / viewPlanBlob / alterOpsFor / the table
#include <rune/node/RuntimeTopicRecovery.h>   // orphanPlaceholderName
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include "support/OwnerThreadTestHelpers.h"   // leoOnOwner
#include "support/ShardTestClient.h"
#include "support/TestNodeConfig.h"
#include "support/TestWorkerCount.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace {

fs::path makeTempDataDir(std::string_view tag) {
    static std::atomic<uint32_t> seq{0};
    return fs::temp_directory_path() / (std::string("rune_ddlbn_") + std::string(tag) +
                                        std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
}

NodeConfig makeSingleNodeConfig(const fs::path& dataDir) {
    NodeConfig cfg;
    pinLegacyRuntime(cfg);
    cfg.nodeId = 0;
    cfg.dataDir = dataDir.string();
    cfg.consensusStreamId = 10;
    PeerConfig self;
    self.id = 0;
    self.host = "127.0.0.1";
    self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();
    cfg.peers = {self};
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" +
                                 std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.clientEndpoint.streamId = 1;
    cfg.workerCount = workerCountFromEnv();
    return cfg;
}

struct Wire {
    ShardTestClient& c;
    std::string gw;
    int32_t stream;

    int define(int64_t sid, const std::string& n, uint8_t persistence = 0, uint32_t parts = 1) {
        const auto r = c.awaitDefine(c.sendDefineTopic(gw, stream, sid, n, parts, charBlob(), persistence),
                                     Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    std::optional<ShardTestClient::SchemaAnswer> schema(int64_t sid, const std::string& n) {
        return c.awaitSchema(c.sendGetTopicSchema(gw, stream, sid, n), Clock::now() + 10s);
    }
    int view(int64_t sid, const std::string& n, const std::string& src, uint8_t persistence = 0) {
        const auto s = schema(sid, src);
        const int32_t srcId = s && s->errorCode == 0 ? s->topicId : -1;
        const auto r = c.awaitCreateView(
            c.sendCreateView(gw, stream, sid, n, "id", persistence, viewPlanBlob(n, srcId, persistence)),
            Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    int alter(int64_t sid, const std::string& n, uint16_t expectedVersion = 0) {
        const auto r = c.awaitAlter(
            c.sendAlterTopic(gw, stream, sid, n, expectedVersion, alterOpsFor(Op::AlterAdd, static_cast<uint64_t>(Clock::now().time_since_epoch().count()))),
            Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    int renameKey(int64_t sid, const std::string& n) {
        const auto r = c.awaitAlter(c.sendAlterTopic(gw, stream, sid, n, 0, alterOpsFor(Op::AlterRenameKey, 0)),
                                    Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    /// The admin thread answers Internal until it leads __topic_schemas: retry a
    /// probe define (bounded) — the documented settle, not a correctness gate.
    bool awaitDdlReady(int64_t sid) {
        const auto deadline = Clock::now() + 20s;
        while (Clock::now() < deadline) {
            const int code = define(sid, "ddl_ready");
            if (code == kOk || code == kTopicExists) return true;
            if (code != kInternal) return false;
            std::this_thread::sleep_for(20ms);   // backoff while leadership settles
        }
        return false;
    }
};

} // namespace

TEST(DdlConformanceBrokerRegression, T1_SequentialCollisionsAnswerExactCodes) {
    const fs::path dir = makeTempDataDir("t1_");
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_GT(sid, 0);
        ASSERT_TRUE(w.awaitDdlReady(sid));

        EXPECT_EQ(w.define(sid, "character"), kOk);
        EXPECT_EQ(w.define(sid, "character"), kTopicExists);
        EXPECT_EQ(w.define(sid, "character", 1), kTopicExists);
        EXPECT_EQ(w.view(sid, "char_hp", "character"), kOk);
        EXPECT_EQ(w.view(sid, "char_hp", "character"), kViewExists);
        EXPECT_EQ(w.define(sid, "char_hp"), kTopicExists);
        EXPECT_EQ(w.alter(sid, "char_hp"), kViewNotAlterable);
        ASSERT_TRUE(w.schema(sid, "char_hp").has_value());
        EXPECT_EQ(w.schema(sid, "char_hp")->errorCode, kOk);
        EXPECT_EQ(w.alter(sid, "character"), kOk);
        EXPECT_EQ(w.alter(sid, "character", 1), kSchemaConflict);
        EXPECT_EQ(w.view(sid, "hp_of_hp", "char_hp"), kViewSourceIsView) << "B9: a view over a view";
        EXPECT_EQ(w.renameKey(sid, "character"), kOk) << "B8: RENAME id -> pk";
        EXPECT_EQ(w.define(sid, ""), kInvalidTopicName);
        EXPECT_EQ(w.define(sid, "__views"), kInvalidTopicName);
        EXPECT_EQ(w.define(sid, "a/b"), kInvalidTopicName);
        EXPECT_EQ(w.define(sid, "Character"), kTopicExists);
        EXPECT_EQ(w.define(sid, "zero", 0, 0), kInvalidSchema);
        ASSERT_TRUE(w.schema(sid, "__topic_schemas").has_value());
        EXPECT_EQ(w.schema(sid, "__topic_schemas")->errorCode, kUnknownTopic);
        node.stop();
        EXPECT_EQ(node.schemaRegistryForTest()->byName("Character"), nullptr);
        EXPECT_EQ(node.schemaRegistryForTest()->byName("character")->version, 3u);
        EXPECT_EQ(node.schemaRegistryForTest()->byName("character")->keyField, "pk") << "B8";
        EXPECT_EQ(node.viewRegistryForTest()->byName("hp_of_hp"), nullptr) << "B9: nothing created";
        EXPECT_NE(node.viewRegistryForTest()->byName("char_hp"), nullptr);
    }
    EXPECT_FALSE(fs::exists(dir / "topics" / "a")) << "no directory for a rejected name";
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T2_PipelinedSameNameFromTwoSessionsExactlyOneWins) {
    const fs::path dir = makeTempDataDir("t2_");
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t s1 = client.connect(w.gw, w.stream);
    const int64_t s2 = client.connect(w.gw, w.stream);
    ASSERT_GT(s1, 0); ASSERT_GT(s2, 0);
    ASSERT_TRUE(w.awaitDdlReady(s1));
    ASSERT_EQ(w.define(s1, "character"), kOk);
    const auto src = w.schema(s1, "character");
    ASSERT_TRUE(src.has_value());

    // Sent back to back, no reply awaited in between: one publication => the gateway
    // (and the admin FIFO) sees them in send order.
    const int64_t d1 = client.sendDefineTopic(w.gw, w.stream, s1, "race", 1, charBlob(), 0);
    const int64_t d2 = client.sendDefineTopic(w.gw, w.stream, s2, "race", 1, charBlob(), 1);
    const int64_t v1 = client.sendCreateView(w.gw, w.stream, s1, "rview", "id", 0, viewPlanBlob("rview", src->topicId, 0));
    const int64_t v2 = client.sendCreateView(w.gw, w.stream, s2, "rview", "id", 1, viewPlanBlob("rview", src->topicId, 1));
    const int64_t m1 = client.sendDefineTopic(w.gw, w.stream, s1, "rmix", 1, charBlob(), 0);
    const int64_t m2 = client.sendCreateView(w.gw, w.stream, s2, "rmix", "id", 0, viewPlanBlob("rmix", src->topicId, 0));
    const auto deadline = Clock::now() + 15s;
    const auto rd1 = client.awaitDefine(d1, deadline), rd2 = client.awaitDefine(d2, deadline);
    const auto rv1 = client.awaitCreateView(v1, deadline), rv2 = client.awaitCreateView(v2, deadline);
    const auto rm1 = client.awaitDefine(m1, deadline);
    const auto rm2 = client.awaitCreateView(m2, deadline);
    ASSERT_TRUE(rd1 && rd2 && rv1 && rv2 && rm1 && rm2) << "every pipelined DDL is answered";
    EXPECT_EQ(rd1->errorCode, kOk);          EXPECT_EQ(rd2->errorCode, kTopicExists);
    EXPECT_EQ(rv1->errorCode, kOk);          EXPECT_EQ(rv2->errorCode, kViewExists);
    EXPECT_EQ(rm1->errorCode, kOk);          EXPECT_EQ(rm2->errorCode, kTopicExists);
    node.stop();
    EXPECT_EQ(node.schemaRegistryForTest()->byName("race")->persistence, PersistenceMode::Disk)
        << "the winner's mode, never flipped by the loser";
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T3T4_InterleavedBurstMatchesTheTable) {
    const fs::path dir = makeTempDataDir("burst_");
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t s1 = client.connect(w.gw, w.stream);
    const int64_t s2 = client.connect(w.gw, w.stream);
    ASSERT_TRUE(w.awaitDdlReady(s1));
    ASSERT_EQ(w.define(s1, "character"), kOk);
    const int32_t srcId = w.schema(s1, "character")->topicId;

    const Op script[] = {Op::DefineDisk, Op::ViewPersisted, Op::DefineTransient, Op::AlterAdd, Op::ViewTransient,
                         Op::DefineDisk, Op::AlterAdd, Op::ViewPersisted, Op::DefineTransient, Op::AlterAdd};
    struct Sent { Op op; std::string name; int64_t corr; };
    std::vector<Sent> sent;
    for (int i = 0; i < 30; ++i) {
        const Op op = script[i % 10];
        const std::string name = "b" + std::to_string(i % 3);
        const int64_t sid = (i % 2) ? s2 : s1;
        int64_t corr = 0;
        if (isDefine(op)) corr = client.sendDefineTopic(w.gw, w.stream, sid, name, 1, charBlob(), op == Op::DefineTransient ? 1 : 0);
        else if (isView(op)) {
            const uint8_t p = op == Op::ViewTransient ? 1 : 0;
            corr = client.sendCreateView(w.gw, w.stream, sid, name, "id", p, viewPlanBlob(name, srcId, p));
        } else corr = client.sendAlterTopic(w.gw, w.stream, sid, name, 0, alterOpsFor(Op::AlterAdd, static_cast<uint64_t>(i)));
        ASSERT_NE(corr, 0);
        sent.push_back({op, name, corr});
    }
    std::map<std::string, Occupant> model;   // sequential FIFO model over the table
    const auto deadline = Clock::now() + 30s;
    for (std::size_t i = 0; i < sent.size(); ++i) {
        const Sent& s = sent[i];
        const Occupant o = model.count(s.name) ? model[s.name] : Occupant::Free;
        const Cell want = expectedCell(o, s.op);
        int got = -1;
        if (isDefine(s.op)) { if (auto r = client.awaitDefine(s.corr, deadline)) got = r->errorCode; }
        else if (isView(s.op)) { if (auto r = client.awaitCreateView(s.corr, deadline)) got = r->errorCode; }
        else { if (auto r = client.awaitAlter(s.corr, deadline)) got = r->errorCode; }
        EXPECT_EQ(got, want.code) << "frame #" << i << " " << opName(s.op) << " '" << s.name << "'";
        if (got == kOk) model[s.name] = occupantAfterSuccess(o, s.op);
    }
    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T5_FailedCreateLeavesAnOrphanAndTheBrokerStillRestarts) {
    const fs::path dir = makeTempDataDir("t5_");
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t laterId = -1;
    {
        BrokerNode node{cfg};
        node.setAdminCreateFaultForTest("orphan_v", /*afterCreate=*/true);   // T5b: sink created, reply fails
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "character"), kOk);
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kInternal);
        ASSERT_EQ(w.define(sid, "later"), kOk);   // a later recorded id: the orphan is a gap
        laterId = w.schema(sid, "later")->topicId;
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kTopicExists) << "KL1: the orphan sink blocks the name";
        node.stop();
    }
    {
        BrokerNode node{cfg};
        node.start();   // B5: must boot
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron2");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        const auto later = w.schema(sid, "later");
        ASSERT_TRUE(later.has_value());
        EXPECT_EQ(later->errorCode, kOk);
        EXPECT_EQ(later->topicId, laterId) << "a recorded topic keeps its id across the gap";
        EXPECT_FALSE(fs::exists(dir / "topics" / "orphan_v")) << "B10: the orphan sink's data dir was deleted at boot";

        // D1: the placeholder for the orphan's id is hidden and inert.
        const int32_t orphanId = laterId - 1;   // ids: ddl_ready, character, orphan_v, later
        const std::string ph = orphanPlaceholderName(orphanId);
        ASSERT_TRUE(w.schema(sid, ph).has_value());
        EXPECT_EQ(w.schema(sid, ph)->errorCode, kUnknownTopic) << "hidden from schema gets";
        for (const PartitionInfo& p : client.metadata(w.gw, w.stream, sid))
            EXPECT_NE(p.topicId, orphanId) << "hidden from the metadata listing";
        const int64_t pc = client.nextCorrelationId();
        ASSERT_TRUE(client.produce(w.gw, w.stream, sid, pc, orphanId, 0, /*acks=*/1,
                                   [] { rune::protocol::RecordBatchBuilder b(0, 0);
                                        b.append(std::string_view{}, std::string_view{"x"}, 0);
                                        return b.build(); }()));
        const auto ack = client.awaitProduceResponse(pc);
        ASSERT_TRUE(ack.has_value()) << "a produce to the placeholder is answered";
        EXPECT_NE(ack->errorCode, 0) << "never accepts produce";
        const auto sub = client.awaitSubscribeCode(client.sendSubscribe(w.gw, w.stream, sid, orphanId, 0),
                                                   Clock::now() + 10s);
        ASSERT_TRUE(sub.has_value()) << "a subscribe to the placeholder is answered";
        EXPECT_NE(*sub, 0) << "never accepts subscribe";
        EXPECT_EQ(w.define(sid, ph), kInvalidTopicName) << "never accepts DDL (reserved __ name)";
        EXPECT_EQ(w.alter(sid, ph), kUnknownTopic);
        EXPECT_EQ(w.view(sid, ph, "character"), kInvalidTopicName);

        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kOk) << "the orphan's name is free after restart";
        EXPECT_EQ(w.define(sid, "character"), kTopicExists);
        node.stop();
    }
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T5a_ReplyFailsBeforeCreatingThenRetrySucceeds) {
    const fs::path dir = makeTempDataDir("t5a_");
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.setAdminCreateFaultForTest("x", /*afterCreate=*/false);
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t sid = client.connect(w.gw, w.stream);
    ASSERT_TRUE(w.awaitDdlReady(sid));
    EXPECT_EQ(w.define(sid, "x"), kInternal);
    EXPECT_EQ(w.define(sid, "x"), kOk) << "nothing was created; the retry creates it";
    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T7_RestartPreservesTheCatalogAndTheCollisions) {
    const fs::path dir = makeTempDataDir("t7_");
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    const std::vector<std::string> names = {"t_disk", "t_tr", "v_p", "v_t"};
    std::map<std::string, ShardTestClient::SchemaAnswer> before;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "t_disk"), kOk);
        ASSERT_EQ(w.define(sid, "t_tr", 1), kOk);
        ASSERT_EQ(w.view(sid, "v_p", "t_disk"), kOk);
        ASSERT_EQ(w.view(sid, "v_t", "t_tr", 1), kOk);
        ASSERT_EQ(w.alter(sid, "t_disk"), kOk);
        ASSERT_EQ(w.renameKey(sid, "t_tr"), kOk) << "B8";
        for (const std::string& n : names) before[n] = *w.schema(sid, n);
        node.stop();
    }
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron2");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        for (const std::string& n : names) {
            const auto now = w.schema(sid, n);
            ASSERT_TRUE(now.has_value()) << n;
            EXPECT_EQ(*now, before[n]) << n << ": id/version/partitions/descriptor identical after restart";
        }
        EXPECT_EQ(w.define(sid, "t_disk"), kTopicExists);
        EXPECT_EQ(w.define(sid, "t_tr", 1), kTopicExists);
        EXPECT_EQ(w.view(sid, "v_p", "t_disk"), kViewExists);
        EXPECT_EQ(w.alter(sid, "v_p"), kViewNotAlterable);
        EXPECT_EQ(w.view(sid, "v_of_v", "v_p"), kViewSourceIsView) << "B9 after restart";
        node.stop();
        EXPECT_EQ(node.schemaRegistryForTest()->byName("t_tr")->keyField, "pk") << "B8 survives the restart";
    }
    std::error_code ec; fs::remove_all(dir, ec);
}

// D4 (amended): a static topic removed from RUNE_TOPICS is QUARANTINED at boot —
// moved to dataDir/quarantine/<name>-<ts>/ with its data intact and readable — and
// re-adding it starts EMPTY (no automatic restore; the restore is a manual move).
TEST(DdlConformanceBrokerRegression, T8_RemovedStaticTopicIsQuarantinedNotDeleted) {
    const fs::path dir = makeTempDataDir("t8_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    TopicSpec events; events.name = "events"; events.partitionCount = 1; events.topicId = 0;
    cfg.topics = {events};
    {   // boot 1: produce one record to the static topic
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        const int64_t sid = client.connect(cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId);
        ASSERT_GT(sid, 0);
        rune::protocol::RecordBatchBuilder b(0, 0);
        b.append(std::string_view{}, std::string_view{"kept-record"}, 0);
        ASSERT_TRUE(client.produceUntilOk(cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId, sid,
                                          /*topicId=*/0, /*partition=*/0, /*acks=*/1, b.build()).has_value());
        node.stop();
    }
    fs::path quarantined;
    {   // boot 2: "events" removed from RUNE_TOPICS
        NodeConfig cfg2 = cfg;
        cfg2.topics.clear();
        BrokerNode node{cfg2};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        node.stop();
        EXPECT_FALSE(fs::exists(dir / "topics" / "events")) << "moved out of topics/";
        for (const auto& e : fs::directory_iterator(dir / "quarantine"))
            if (e.path().filename().string().starts_with("events-")) quarantined = e.path();
        ASSERT_FALSE(quarantined.empty()) << "quarantined, never deleted";
        rune::storage::Log moved((quarantined / "0").string());
        moved.recover();
        EXPECT_GE(moved.logEndOffset(), 1) << "data intact";
        const std::vector<uint8_t> raw = moved.read(0, 1 << 20);
        EXPECT_NE(std::string(raw.begin(), raw.end()).find("kept-record"), std::string::npos) << "and readable";
    }
    {   // boot 3: "events" re-added — a fresh, EMPTY topic; the quarantine is untouched
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        EXPECT_EQ(rune::node::test::leoOnOwner(node, 0, 0), 0) << "no automatic restore";
        node.stop();
        EXPECT_TRUE(fs::exists(quarantined / "0")) << "the quarantined copy stays until restored by hand";
    }
    std::error_code ec; fs::remove_all(dir, ec);
}
```

- [ ] **Step 2: Add the target and confirm it fails.** In `libs/node/CMakeLists.txt`, inside `if(BUILD_BROKER_NODE_TESTS)` → `else()`, after the `gtest_discover_tests(rune_broker_node_tests …)` call, add:

```cmake
            # DDL conformance plan Task 12: DDL serialization + name collisions over
            # a REAL single-node BrokerNode and real Aeron (T1/T2/T3-T4/T5/T7).
            add_executable(rune_ddl_broker_tests
                tests/integration/DdlConformanceBrokerRegressionTest.cpp)
            target_include_directories(rune_ddl_broker_tests PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/tests)
            target_link_libraries(rune_ddl_broker_tests PRIVATE
                rune_node
                rune_view
                aeron::aeron
                GTest::gtest_main)
            gtest_discover_tests(rune_ddl_broker_tests
                PROPERTIES TIMEOUT 180 RESOURCE_LOCK rune_udp_ports
                ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
```

Run: `cmake /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_broker_tests`
Expected: compile errors `no member named 'sendDefineTopic'` and `no member named 'setAdminCreateFaultForTest'`.

- [ ] **Step 3: Implement the ShardTestClient helpers.** In `ShardTestClient.h`, after `defineTopic(...)` (before `bool disconnect(`), add:

```cpp
    // ── DDL conformance (Task 12): pipelined DDL — send now, await by corr later ──
    struct SchemaAnswer {
        int32_t topicId = -1; uint16_t version = 0; uint16_t errorCode = 0; int32_t partitionCount = 0;
        std::vector<uint8_t> blob;
        bool operator==(const SchemaAnswer&) const = default;
    };
    int64_t sendDefineTopic(const std::string& ch, int32_t stream, int64_t sid, std::string_view name,
                            uint32_t partitions, const std::vector<uint8_t>& descriptor, uint8_t persistence = 0) {
        pc::DefineTopic m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicName = name; m.partitionCount = partitions;
        m.keyField = "id"; m.validate = false; m.persistence = persistence;
        m.descriptorBlob = descriptor.data(); m.descriptorLen = static_cast<uint32_t>(descriptor.size());
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    int64_t sendCreateView(const std::string& ch, int32_t stream, int64_t sid, std::string_view viewName,
                           std::string_view keyField, uint8_t persistence, const std::vector<uint8_t>& plan) {
        pc::CreateView m;
        m.sessionId = sid; m.correlationId = ++corr_; m.viewName = viewName; m.keyField = keyField;
        m.persistence = persistence; m.planBlob = plan.data(); m.planLen = static_cast<uint32_t>(plan.size());
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    int64_t sendAlterTopic(const std::string& ch, int32_t stream, int64_t sid, std::string_view name,
                           uint16_t expectedVersion, const std::vector<uint8_t>& ops) {
        pc::AlterTopic m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicName = name; m.expectedVersion = expectedVersion;
        m.opsBlob = ops.data(); m.opsLen = static_cast<uint32_t>(ops.size());
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    int64_t sendGetTopicSchema(const std::string& ch, int32_t stream, int64_t sid, std::string_view name) {
        pc::GetTopicSchema m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicName = name; m.requestedFingerprint = 0;
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    std::optional<pc::DefineTopicResponse> awaitDefine(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(defines_, corr, deadline);
    }
    std::optional<pc::CreateViewResponse> awaitCreateView(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(views_, corr, deadline);
    }
    std::optional<pc::AlterTopicResponse> awaitAlter(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(alters_, corr, deadline);
    }
    std::optional<SchemaAnswer> awaitSchema(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(schemas_, corr, deadline);
    }
    /// Subscribe and return its correlation id (0 = not sent).
    int64_t sendSubscribe(const std::string& ch, int32_t stream, int64_t sid, int32_t topicId, int32_t partition) {
        pc::Subscribe m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicId = topicId; m.partition = partition;
        m.partitionKnown = 1; m.fromOffset = 0; m.isolation = 0;
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    /// The answer to a subscribe: its SubscribeResponse errorCode, or the code of an
    /// ErrorResponse on that correlation id (NotLeader / Redirect paths).
    std::optional<int16_t> awaitSubscribeCode(int64_t corr, Clock::time_point deadline) {
        if (corr == 0) return std::nullopt;
        while (Clock::now() < deadline) {
            poll_();
            if (auto it = subscribes_.find(corr); it != subscribes_.end()) return it->second;
            for (const ErrorSeen& e : errors_) if (e.correlationId == corr) return e.code;
            pause_();
        }
        return std::nullopt;
    }
```

In the private section (before `void pump_()`), add:

```cpp
    template <class M>
    std::optional<typename M::mapped_type> awaitIn_(M& m, int64_t corr, Clock::time_point deadline) {
        if (corr == 0) return std::nullopt;
        while (Clock::now() < deadline) {
            poll_();
            if (auto it = m.find(corr); it != m.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }
```

In `onFrame_`'s switch, after the `DefineTopicResponse` case, add:

```cpp
            case pc::TemplateId::SubscribeResponse:
                if (auto r = pc::SubscribeResponse::decode(p, n)) subscribes_[r->correlationId] = r->errorCode;
                break;
            case pc::TemplateId::CreateViewResponse:
                if (auto r = pc::CreateViewResponse::decode(p, n)) views_[r->correlationId] = *r;
                break;
            case pc::TemplateId::AlterTopicResponse:
                if (auto r = pc::AlterTopicResponse::decode(p, n)) alters_[r->correlationId] = *r;
                break;
            case pc::TemplateId::TopicSchemaResponse:
                if (auto r = pc::TopicSchemaResponse::decode(p, n))
                    schemas_[r->correlationId] = SchemaAnswer{
                        r->topicId, r->version, r->errorCode, r->partitionCount,
                        std::vector<uint8_t>(r->descriptorBlob, r->descriptorBlob + r->descriptorLen)};
                break;
```

After `    std::map<int64_t, pc::DefineTopicResponse>      defines_;`, add:

```cpp
    std::map<int64_t, pc::CreateViewResponse>       views_;
    std::map<int64_t, pc::AlterTopicResponse>       alters_;
    std::map<int64_t, SchemaAnswer>                 schemas_;
    std::map<int64_t, int16_t>                      subscribes_;   // SubscribeResponse errorCode by corr
```

- [ ] **Step 4: Implement the BrokerNode fault hook.** In `BrokerNode.h`, after `setSchemaRecoveryFaultForTest`:

```cpp
    /// Test-only (DDL conformance): the control thread answers the FIRST admin
    /// create-topic request for `name` with a failure.  afterCreate=false fails
    /// before creating; true creates the topic first (the reply is lost after the
    /// work — an orphan).  Set before start(); afterwards only the control thread
    /// reads/writes it (single owner).
    void setAdminCreateFaultForTest(std::string name, bool afterCreate) {
        adminCreateFaultName_ = std::move(name);
        adminCreateFaultAfterCreate_ = afterCreate;
    }
```

After `    int  schemaRecoveryFaultForTest_ = 0;`, add:

```cpp
    std::string adminCreateFaultName_;              // control thread after start()
    bool        adminCreateFaultAfterCreate_ = false;
    bool        adminCreateFaultFired_       = false;
```

In `BrokerNode.cpp`'s create-topic drain lambda, replace the body from `            CreateTopicReplyMsg reply;` through the closing `}` of `if (topicRegistry_) { … }` with:

```cpp
            CreateTopicReplyMsg reply;
            reply.ticket = req.ticket;
            // Test-only fault (setAdminCreateFaultForTest): the first request for the
            // configured name fails, before or after the topic is created.
            const bool inject = !adminCreateFaultFired_ && !adminCreateFaultName_.empty() &&
                                req.name == adminCreateFaultName_;
            if (inject) adminCreateFaultFired_ = true;
            if (inject && !adminCreateFaultAfterCreate_) {
                reply.topicId = -1;
            } else if (topicRegistry_) {
                // createTopicForAdmin (AdminCreateTopic.h): createTopic + the
                // AUTHORITATIVE partition count + `existed` (final review I1 —
                // an existing name returns that topic, never the requested spec).
                reply = createTopicForAdmin(*topicRegistry_, req);
                // Task 8b (SINGLE-NODE): make the runtime topic produce/consume-capable
                // by creating its user PartitionReplica(s) NOW, on this same control
                // thread (idempotent for an existing topic's replicas).
                if (reply.topicId >= 0 && !reply.caseConflict) {   // B3: nothing was created
                    try {
                        if (const rune::node::TopicSpec* authSpec =
                                topicRegistry_->topic(reply.topicId))
                            (void)createRuntimeUserReplicas(*authSpec);
                    } catch (const std::exception& ex) {
                        spdlog::error("BrokerNode: admin createTopic('{}') replicas failed: {}",
                                      req.name, ex.what());
                        reply.topicId = -1;
                    }
                }
                if (inject) { reply.topicId = -1; reply.existed = false; reply.caseConflict = false; }
            }
```

- [ ] **Step 5: Run the tests and confirm they pass.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_broker_tests rune_broker_node_tests && ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R DdlConformanceBrokerRegression --output-on-failure`
  Expected: 7 PASS (T1 now also covers B8/B9; T5 covers D1 placeholder hiding and the placeholder-dir deletion; T7 covers B8/B9 across the restart; T8 covers D4 quarantine).
  A live disk partition dir holds only regular files (segments, indexes, `leader-epoch-state`). If T5's or T8's assertion fails because a real partition dir holds another kind of entry, add that exact entry name to `isTopicDirLayout` (Task 3a) together with a `RuntimeTopicRecoveryRegression` test. Never loosen the layout to "anything".

- [ ] **Step 6: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/include/rune/node/BrokerNode.h libs/node/src/BrokerNode.cpp libs/node/tests/support/ShardTestClient.h libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp libs/node/CMakeLists.txt
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "test(ddl): real BrokerNode over Aeron — sequential, pipelined, burst, failed-create+restart, restart catalog

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: e2e — two clients race colliding DDL; ALTER on a view = 38; catalog identical across restart

**Repos:** rune-client-cpp (probe), rune-broker (script)

**Files:**
- Modify: `/Users/vradu/Projects/ddl-worktrees/rune-client-cpp/examples/view_probe.cpp`
  - header comment (modes);
  - includes;
  - helpers before `int main()`;
  - the `setup` mode, after the `char_transient` createView block (~L533-535);
  - mode dispatch before `if (mode == "verify_restart")` (~L607).
- Modify: `/Users/vradu/Projects/ddl-worktrees/rune-broker/docker/view-engine-e2e.sh`
  - `teardown()`;
  - after the typed-routing check (before `if [ "${RUN_RESTART_LEG:-1}" != "1" ]`);
  - before Step 5;
  - after Step 7.

**Interfaces:**
- Produces three new probe output contracts:
  - `RUNE_MODE=ddl_race` (env `RUNE_RACE_ROLE=a|b`, `RUNE_BARRIER_DIR`) → stdout `DDL-RACE: READY role=<r>`, then `DDL_RACE leg=<topic|view|mixed> role=<r> code=<n>` and `DDL-RACE: DONE role=<r>`.
  - `RUNE_MODE=catalog_dump` (env `RUNE_CATALOG_NAMES` comma-separated) → `CATALOG name=<n> code=<c> topicId=<id> version=<v> parts=<p> fp=<fnv64 of descriptor>` lines + `CATALOG-DUMP: OK`.
  - `setup` mode prints `ALTER-VIEW: OK code=38`.
  - Exit codes 60 (alter-view), 70–72 (race), 73 (catalog).

- [ ] **Step 1: Add the probe modes.** In `view_probe.cpp`:

Add includes `#include <rune/codec/AlterOps.h>` and `#include <filesystem>`.

Append to the header comment block, before `// Environment (both modes):`:

```cpp
//   RUNE_MODE=ddl_race (DDL conformance): connect, print "DDL-RACE: READY role=<a|b>",
//     wait for <RUNE_BARRIER_DIR>/go, then race the other container: define
//     "race_topic" (a: disk, b: transient), createView "race_view", and a/b define vs
//     createView "race_mixed".  Prints one "DDL_RACE leg=<leg> role=<r> code=<n>" per leg.
//   RUNE_MODE=catalog_dump: getTopicSchema for each RUNE_CATALOG_NAMES entry, one
//     "CATALOG name=… code=… topicId=… version=… parts=… fp=…" line each.
```

Add before `int main() {`:

```cpp
// ---------------------------------------------------------------------------
// DDL conformance legs (rune-broker DDL conformance plan, Task 13).
// ---------------------------------------------------------------------------
static uint64_t fnv1a64(const std::vector<uint8_t>& b) {
    uint64_t h = 1469598103934665603ULL;
    for (const uint8_t x : b) { h ^= x; h *= 1099511628211ULL; }
    return h;
}

static bool waitForGo(const std::string& dir, int boundMs) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (std::chrono::steady_clock::now() < deadline) {
        std::error_code ec;
        if (std::filesystem::exists(dir + "/go", ec)) return true;
        std::this_thread::sleep_for(10ms);   // poll interval, not a correctness gate
    }
    return false;
}

static int ddlRaceMode(Client& c, const std::string& role, std::chrono::milliseconds wait, int boundMs) {
    const std::string barrier = getEnvStr("RUNE_BARRIER_DIR", "/barrier");
    const TopicSchemaResult src = c.getTopicSchemaAndWait("character", wait);
    if (src.errorCode != 0) {
        std::cerr << "PROBE: FAIL ddl_race getTopicSchema(character) errorCode=" << src.errorCode << "\n";
        return 70;
    }
    std::cout << "DDL-RACE: READY role=" << role << "\n";
    std::cout.flush();
    if (!waitForGo(barrier, boundMs)) { std::cerr << "PROBE: FAIL ddl_race no go signal\n"; return 71; }

    const auto blob = rune::codec::serialize(characterDescriptor());
    const DefineTopicResult t = c.defineTopicAndWait(
        "race_topic", 1, "id", false, std::span<const uint8_t>(blob),
        role == "a" ? Persistence::Disk : Persistence::Transient, wait);
    std::cout << "DDL_RACE leg=topic role=" << role << " code=" << t.errorCode << "\n";

    const auto plan = rcv::serializeAuthored(charHpPlan("race_view", src.topicId, 0));
    const CreateViewResult v = c.createViewAndWait("race_view", "id", 0, std::span<const uint8_t>(plan), wait);
    std::cout << "DDL_RACE leg=view role=" << role << " code=" << v.errorCode << "\n";

    uint16_t mixed = 0;
    if (role == "a") {
        mixed = c.defineTopicAndWait("race_mixed", 1, "id", false, std::span<const uint8_t>(blob), wait).errorCode;
    } else {
        const auto mplan = rcv::serializeAuthored(charHpPlan("race_mixed", src.topicId, 0));
        mixed = c.createViewAndWait("race_mixed", "id", 0, std::span<const uint8_t>(mplan), wait).errorCode;
    }
    std::cout << "DDL_RACE leg=mixed role=" << role << " code=" << mixed << "\n";
    std::cout << "DDL-RACE: DONE role=" << role << "\n";
    std::cout.flush();
    return 0;
}

static int catalogDumpMode(Client& c, std::chrono::milliseconds wait) {
    const std::string names = getEnvStr("RUNE_CATALOG_NAMES");
    if (names.empty()) { std::cerr << "PROBE: FAIL catalog_dump needs RUNE_CATALOG_NAMES\n"; return 73; }
    std::size_t start = 0;
    while (start <= names.size()) {
        const std::size_t comma = names.find(',', start);
        const std::string name = names.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        const TopicSchemaResult r = c.getTopicSchemaAndWait(name, wait);
        std::cout << "CATALOG name=" << name << " code=" << r.errorCode << " topicId=" << r.topicId
                  << " version=" << r.version << " parts=" << r.partitionCount
                  << " fp=" << fnv1a64(r.descriptorBlob) << "\n";
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    std::cout << "CATALOG-DUMP: OK\n";
    std::cout.flush();
    return 0;
}
```

In `setup` mode, immediately after the `char_transient` createView success log (`std::cerr << "PROBE: createView(char_transient) OK viewTopicId=" …`), add:

```cpp
            // ---- 2b. DDL conformance: ALTER on a view's output -> ViewNotAlterable (38)
            {
                rune::codec::AlterOp add{};
                add.type = rune::codec::AlterOpType::Add; add.name = "extra";
                add.primType = rune::codec::PrimType::Int64; add.fixedLen = 0;
                const auto ops = rune::codec::encodeAlterOps({add});
                const AlterTopicResult ar = c.alterTopicAndWait("char_persisted", 0,
                                                                std::span<const uint8_t>(ops), wait);
                if (ar.errorCode != 38) {
                    std::cerr << "PROBE: FAIL alterTopic(char_persisted) errorCode=" << ar.errorCode
                              << " (want 38 ViewNotAlterable)\n";
                    return 60;
                }
                std::cout << "ALTER-VIEW: OK code=38\n";
            }
```

Before `        if (mode == "verify_restart") {`, add:

```cpp
        if (mode == "ddl_race") {
            const int rc = ddlRaceMode(c, getEnvStr("RUNE_RACE_ROLE", "a"), wait, boundMs);
            c.close();
            return rc;
        }
        if (mode == "catalog_dump") {
            const int rc = catalogDumpMode(c, wait);
            c.close();
            return rc;
        }
```

Change the unknown-mode message to `(want setup|verify_restart|ddl_race|catalog_dump)`.

- [ ] **Step 2: Build the probe locally** (catches compile errors before docker):
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-client-cpp/build -j --target view_probe`
  Expected: builds. If the target has another name, find it with `grep -n "view_probe" /Users/vradu/Projects/ddl-worktrees/rune-client-cpp/CMakeLists.txt /Users/vradu/Projects/ddl-worktrees/rune-client-cpp/examples/CMakeLists.txt`.

- [ ] **Step 3: Extend the e2e script** `docker/view-engine-e2e.sh`.

In `teardown()`, before `docker compose … down -v …`, add:

```bash
    docker rm -f rune-ddl-race-a rune-ddl-race-b 2>/dev/null || true
    [ -n "${RACE_DIR:-}" ] && rm -rf "${RACE_DIR}" || true
```

After the `log "Typed routing OK: …"` line and before `if [ "${RUN_RESTART_LEG:-1}" != "1" ]; then`, insert:

```bash
# ---------------------------------------------------------------------------
# Step 4b (DDL conformance): ALTER on a view's output answers ViewNotAlterable.
# ---------------------------------------------------------------------------
echo "${setup_out}" | grep -q "^ALTER-VIEW: OK code=38" \
    || fail "setup client did not print 'ALTER-VIEW: OK code=38' (alter topic <view> must be 38)"
log "ALTER on a view: 38 (ViewNotAlterable) OK"

# ---------------------------------------------------------------------------
# Step 4c (DDL conformance): two clients fire colliding DDL concurrently.  Both
# containers connect and print READY, then block on a shared barrier file; the
# script releases them together.  Per leg exactly one wins (0) and the other gets
# the collision code: topic/topic 25, view/view 32, topic/view 25.
# ---------------------------------------------------------------------------
RACE_DIR="$(mktemp -d "${BROKER_REPO}/.ddl-race.XXXXXX")"
RACE_A="rune-ddl-race-a"
RACE_B="rune-ddl-race-b"

start_race_client() {
    local role="$1" host="$2"
    docker rm -f "${host}" >/dev/null 2>&1 || true
    docker run -d \
        --name "${host}" --hostname "${host}" --network "${BROKER_NETWORK}" \
        --platform linux/amd64 --shm-size=256m --security-opt seccomp=unconfined \
        -v "${RACE_DIR}:/barrier" \
        -e RUNE_MODE=ddl_race -e RUNE_RACE_ROLE="${role}" -e RUNE_BARRIER_DIR=/barrier \
        -e RUNE_TARGET="${BROKER_TARGET}" -e RUNE_TARGET_STREAM=1 \
        -e RUNE_EGRESS="aeron:udp?endpoint=${host}:${CLIENT_EGRESS_PORT}" -e RUNE_EGRESS_STREAM=2 \
        -e RUNE_DATA_DIR="/tmp/rune_view_probe" \
        -e RUNE_TIMEOUT_MS="${PROBE_TIMEOUT_MS}" -e RUNE_WAIT_BOUND_MS="${WAIT_BOUND_MS}" \
        "${CLIENT_IMAGE}" >/dev/null
}
await_race_ready() {
    local host="$1" deadline
    deadline=$(( $(date +%s) + 120 ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        docker logs "${host}" 2>/dev/null | grep -q "^DDL-RACE: READY" && return 0
        sleep 1
    done
    return 1
}
race_code() {   # $1 = stdout of one race client, $2 = leg
    echo "$1" | grep -oE "^DDL_RACE leg=$2 role=[ab] code=[0-9]+" | tail -1 | sed -E 's/.*code=//'
}
check_race_leg() {   # $1 = leg, $2 = the loser's collision code
    local leg="$1" loser="$2" a b
    a="$(race_code "${race_out_a}" "${leg}")"
    b="$(race_code "${race_out_b}" "${leg}")"
    [ -n "${a}" ] && [ -n "${b}" ] || fail "DDL race leg ${leg}: missing result (a='${a}' b='${b}')"
    if { [ "${a}" = 0 ] && [ "${b}" = "${loser}" ]; } || { [ "${b}" = 0 ] && [ "${a}" = "${loser}" ]; }; then
        log "DDL race leg ${leg}: exactly one winner (a=${a} b=${b})"
    else
        fail "DDL race leg ${leg}: want exactly one 0 and one ${loser}, got a=${a} b=${b}"
    fi
}

log "Step 4c: DDL race — two clients, colliding DDL, released together..."
start_race_client a "${RACE_A}"
start_race_client b "${RACE_B}"
await_race_ready "${RACE_A}" || fail "race client a never printed READY"
await_race_ready "${RACE_B}" || fail "race client b never printed READY"
touch "${RACE_DIR}/go"
race_rc_a="$(docker wait "${RACE_A}")"
race_rc_b="$(docker wait "${RACE_B}")"
race_out_a="$(docker logs "${RACE_A}" 2>/dev/null)"
race_out_b="$(docker logs "${RACE_B}" 2>/dev/null)"
echo "----- race a -----" >&2; echo "${race_out_a}" >&2
echo "----- race b -----" >&2; echo "${race_out_b}" >&2
[ "${race_rc_a}" = 0 ] && [ "${race_rc_b}" = 0 ] || fail "race clients exited ${race_rc_a}/${race_rc_b}"
check_race_leg topic 25
check_race_leg view 32
check_race_leg mixed 25
docker rm -f "${RACE_A}" "${RACE_B}" >/dev/null 2>&1 || true

CATALOG_NAMES="character,char_persisted,char_transient,order,user_totals,typed_route,race_topic,race_view,race_mixed"
dump_catalog() {
    local out
    out="$(run_client catalog_dump -e RUNE_CATALOG_NAMES="${CATALOG_NAMES}" 2>/dev/null)" || return 1
    echo "${out}" | grep -q "^CATALOG-DUMP: OK" || return 1
    echo "${out}" | grep -E "^CATALOG name="
}
```

Immediately before the `# Step 5: Restart the broker` section header comment, add:

```bash
# Step 4d (DDL conformance): snapshot the catalog before the restart.
catalog_before="$(dump_catalog)" || fail "catalog dump (pre-restart) failed"
echo "${catalog_before}" | grep -q " code=[1-9]" && fail "a pre-restart catalog entry did not resolve: ${catalog_before}"
log "Catalog before restart:"; echo "${catalog_before}" >&2
```

Immediately after the Step 7 sentinel check (after the `fi` that follows `grep -q "VIEW-ENGINE-E2E: OK"`), add:

```bash
# ---------------------------------------------------------------------------
# Step 7b (DDL conformance): the catalog is identical after the restart.
# ---------------------------------------------------------------------------
catalog_after="$(dump_catalog)" || fail "catalog dump (post-restart) failed"
if [ "${catalog_before}" != "${catalog_after}" ]; then
    diff <(echo "${catalog_before}") <(echo "${catalog_after}") >&2 || true
    fail "the DDL catalog differs after the restart"
fi
log "Catalog identical after restart ($(echo "${catalog_after}" | wc -l | tr -d ' ') entries)"
```

- [ ] **Step 4: Run the e2e** (takes minutes; Docker required):
  Run: `cd /Users/vradu/Projects/ddl-worktrees/rune-broker && bash docker/view-engine-e2e.sh`
  Expected: the final banner prints `VIEW-ENGINE-E2E: OK`, and the log contains `ALTER on a view: 38`, three `DDL race leg … exactly one winner` lines and `Catalog identical after restart (9 entries)`.

- [ ] **Step 5: Gates, then commit (two repos).**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-client-cpp/build -R Regression --output-on-failure -j8
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-client-cpp add examples/view_probe.cpp
git -C /Users/vradu/Projects/ddl-worktrees/rune-client-cpp commit -m "test(e2e): view_probe ddl_race / catalog_dump modes + alter-on-view=38 check

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add docker/view-engine-e2e.sh
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "test(e2e): two clients race colliding DDL (exactly one wins); catalog identical across restart

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13a: Docs — name rule, codes 40/41, placeholders, orphan-dir clean-up, quarantine + manual restore

**Files:**
- Modify: `docs/SELF-HOSTING.md`: new subsections before `### Transient topics` (~L60); one bullet after the `ViewNotAlterable` bullet (~L75)
- Modify: `docs/release-notes/phase2b-e-transient-topics.md`: new section `## 8` after `## 7. Malformed requests are answered` (added by the no-hang wave, 8a693c5)

**Interfaces:**
- Consumes: the behaviour from Tasks 3, 3a, 5, 5a, 6 and 7b. The text must match their error codes and log behaviour exactly.

- [ ] **Step 1: Edit `docs/SELF-HOSTING.md`.** Insert immediately before the line `### Transient topics`:

```markdown
### Topic and view names

One rule applies to `RUNE_TOPICS` entries, the client's `defineTopic` and `createView`:

- 1 to 249 bytes of ASCII letters, digits, `.`, `_` and `-`, and not `.` or `..`.
- Names starting with `__` are reserved for internal topics.
- Names are case-sensitive, but two names that differ only by letter case are not allowed,
  because they would share one data directory on a case-insensitive filesystem.
- A bad name in `RUNE_TOPICS` is a boot configuration error that names the entry. Two
  `RUNE_TOPICS` names that differ only by case are a configuration error too.
- A bad name in DDL is rejected with error 40 (`InvalidTopicName`). A name that differs from an
  existing topic or view only by case is rejected with error 25 (`TopicExists`). A partition count
  below 1 is rejected with error 23 (`InvalidSchema`).

### Data directory clean-up at boot

- A topic id whose definition never committed leaves an id with no catalog record. The usual
  cause is a `createView` that failed after its output topic was created. The id is kept as a
  hidden placeholder `__orphan.<id>` with no partitions, so later topics keep their ids. It is
  not listed, has no schema, and accepts no produce, subscribe or DDL. Boot logs all
  placeholders in one warning line.
- At boot the broker reconciles `dataDir/topics/` with the catalog:
  - The data directory of an `__orphan.<id>` placeholder is **deleted**. The broker recognises
    it by the `topic-id` file it writes into every disk topic directory.
  - Any other directory that no topic claims is **moved, never deleted**, to
    `dataDir/quarantine/<name>-<unix-timestamp>/`, and a warning names both paths. This covers
    a topic removed from `RUNE_TOPICS`, a misspelt name, and a directory left by another build.
  - It only considers direct children of `dataDir/topics/` laid out as
    `<name>/<partition number>/<files>`. It never follows a symlink, and it leaves anything else
    alone.
- **Removing a topic from `RUNE_TOPICS` quarantines its data at the next boot.** Adding the
  topic back starts it **empty**; the broker never restores quarantined data by itself. To restore:
  1. Stop the broker.
  2. Make sure `RUNE_TOPICS` lists the topic with the **same partition count**.
  3. Delete the new, empty `dataDir/topics/<name>/` if it exists.
  4. Run `mv dataDir/quarantine/<name>-<timestamp> dataDir/topics/<name>`.
  5. Start the broker.
- Delete quarantined directories yourself once you no longer need them; the broker never
  deletes from `dataDir/quarantine/`.

```

After the line ``- `alter topic` naming a view is rejected with error 38 (`ViewNotAlterable`).``, add:

```markdown
- A view cannot read another view's output yet: `Client::createView(...)` over a view is rejected
  with error 41 (`ViewSourceIsView`). Before this release such a view was accepted but stayed
  empty.
- Renaming a topic's key field with `alter topic … rename` moves the topic's key to the new name.
```

- [ ] **Step 2: Edit the release note.** Append to `docs/release-notes/phase2b-e-transient-topics.md`, after the no-hang wave's `## 7. Malformed requests are answered`:

```markdown

## 8. DDL names, conflicts and boot clean-up

- Topic and view names must be 1–249 bytes of `[A-Za-z0-9._-]`, must not be `.` or `..`, and must
  not start with `__`.
  - DDL answers error 40 (`InvalidTopicName`).
  - A `RUNE_TOPICS` entry with a bad name now refuses to boot and names the entry. **Check
    your `RUNE_TOPICS` before upgrading.**
- Two names that differ only by letter case are rejected: DDL answers 25 (`TopicExists`), and
  `RUNE_TOPICS` refuses to boot.
- `createView` over another view's output answers 41 (`ViewSourceIsView`) instead of creating a
  view that stays empty.
- `alter topic … rename` of the key field now moves the topic's key with it.
- A failed definition can no longer stop the broker from booting. Its topic id becomes a hidden,
  zero-partition placeholder `__orphan.<id>`.
- Boot deletes the data directory of an `__orphan.<id>` placeholder only. **Any other topic
  directory no longer in the catalog is moved to `dataDir/quarantine/<name>-<timestamp>/`, never
  deleted.** That includes a topic removed from `RUNE_TOPICS`. Re-adding the topic starts it
  empty; see SELF-HOSTING.md, "Data directory clean-up at boot", for the manual restore.
```

- [ ] **Step 3: Verify the codes in the docs match the code.**
  Run: `grep -n "40\|41\|__orphan\|case" /Users/vradu/Projects/ddl-worktrees/rune-broker/docs/SELF-HOSTING.md | head -20 && grep -n "InvalidTopicName\|ViewSourceIsView" /Users/vradu/Projects/ddl-worktrees/rune-protocol/include/rune/protocol/client/Protocol.h`
  Expected: the documented numbers match `InvalidTopicName = 40` and `ViewSourceIsView = 41`.

- [ ] **Step 4: Commit.**

```bash
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add docs/SELF-HOSTING.md docs/release-notes/phase2b-e-transient-topics.md
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "docs: DDL name rule (40), case conflicts, ViewSourceIsView (41), orphan placeholders, boot dir clean-up and quarantine restore

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14 (OPTIONAL — follow-up to the parked KL1 ruling): a CREATE VIEW retry adopts its own orphan sink

Only do this task if the user approves it. It changes KL1 cells from 25 to 0.

**Files:**
- Modify: `libs/node/include/rune/node/AdminChannels.h`: `CreateTopicReplyMsg::sinkSpecMatches` (trailer byte index 19, frame length 20)
- Modify: `libs/node/include/rune/node/AdminCreateTopic.h`: a `firstRuntimeId` parameter plus the spec comparison
- Modify: `libs/node/src/AdminWorker.cpp`: `onCreateViewReply_`'s `if (reply.existed)` branch
- Modify: `libs/node/include/rune/node/BrokerNode.h` / `src/BrokerNode.cpp`: remember the static topic count before the recovery append (~L383) and pass it in the drain
- Modify: `libs/node/tests/support/DdlHarness.h`: `deliverCreate` / `deliverCreateWithPersistence` / `failCreate` pass `static_cast<int32_t>(opt_.statics.size())`
- Modify: `libs/node/tests/support/DdlExpectations.h`: OrphanSink row `ViewP`/`ViewT` 25 → 0
- Modify: `libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp`: T5 expectations
- Test: `libs/node/tests/node/DdlHarnessRegressionTest.cpp`

**Interfaces:**
- Produces:
  - `createTopicForAdmin(TopicRegistry&, const CreateTopicReqMsg&, int32_t firstRuntimeId = std::numeric_limits<int32_t>::max())`. The default never matches, so existing callers keep today's behaviour.
  - `CreateTopicReplyMsg::sinkSpecMatches` is true iff all of these hold: `existed`, `topicId >= firstRuntimeId`, the existing spec is Compact, and its `partitionKey == req.keyField`, its `partitionCount == req.partitionCount` and its persistence equals the request's.
  - AdminWorker adopts such a topic as the view's sink only if it has no TopicSchema and no ViewRecord. That makes it an orphan, never a static or typed topic.

- [ ] **Step 1: Write the failing tests.** Append:

```cpp
// ── OPTIONAL KL1 follow-up ────────────────────────────────────────────────────
TEST(DdlFixRegression, Optional_CreateViewRetryAdoptsItsOwnOrphanSink) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t a = h.submit(Op::ViewPersisted, "v");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.failCreate(/*afterCreate=*/true);
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_EQ(h.response(a)->err, kInternal);
    const int32_t sink = h.idOf("v");
    const int64_t b = h.submit(Op::ViewPersisted, "v");
    h.settle();
    EXPECT_EQ(h.response(b)->err, kOk) << "the retry adopts the identical orphan sink";
    EXPECT_EQ(h.response(b)->id, sink);
    EXPECT_NE(h.views().byName("v"), nullptr);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, Optional_AViewNeverAdoptsAnOrphanDefineOrAStaticCompactTopic) {
    DdlHarnessOptions o;
    TopicSpec s; s.name = "stat_sink"; s.partitionCount = 1; s.cleanupPolicy = TopicSpec::CleanupPolicy::Compact;
    s.partitionKey = "id";
    o.statics.push_back(s);
    DdlHarness h(o);
    const int64_t st = h.submit(Op::ViewPersisted, "stat_sink");
    h.settle();
    EXPECT_EQ(h.response(st)->err, kTopicExists) << "a static topic is never a sink";
    h.setControl(ControlMode::Hold);
    const int64_t d = h.submit(Op::DefineDisk, "orph_def");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.failCreate(true);
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_EQ(h.response(d)->err, kInternal);
    const int64_t v = h.submit(Op::ViewPersisted, "orph_def");
    h.settle();
    EXPECT_EQ(h.response(v)->err, kTopicExists) << "a Delete-policy orphan is not a view sink";
}
```

- [ ] **Step 2: Run them and confirm they fail.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests && /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast/libs/node/rune_ddl_conformance_tests --gtest_filter='DdlFixRegression.Optional*'`
  Expected: `Optional_CreateViewRetryAdoptsItsOwnOrphanSink` FAILS (25). The second test passes, as a pin.

- [ ] **Step 3: Implement.**

`AdminChannels.h`: in `CreateTopicReplyMsg`, after `caseConflict`, add:

```cpp
    // OPTIONAL KL1 follow-up: the existing topic is a RUNTIME topic whose spec is
    // exactly the requested view sink's (Compact, key, partitions, mode) — the admin
    // thread may adopt it when no catalog record claims it (its own orphan sink).
    bool    sinkSpecMatches = false;
```

In `encodeCreateTopicReply`, set `len = 8 + 4 + 4 + 1 + 1 + 1 + 1`, and replace `*p   = m.caseConflict ? 1 : 0;` with:

```cpp
    *p++ = m.caseConflict ? 1 : 0;
    *p   = m.sinkSpecMatches ? 1 : 0;
```

In `decodeCreateTopicReply`, after the caseConflict line, add `    if (len >= 20) m.sinkSpecMatches = buf[19] != 0;`.

`AdminCreateTopic.h`: add `#include <limits>`. Change the signature to:

```cpp
inline CreateTopicReplyMsg
createTopicForAdmin(rune::gateway::TopicRegistry& reg, const CreateTopicReqMsg& req,
                    int32_t firstRuntimeId = std::numeric_limits<int32_t>::max()) {
```

After the block that sets `reply.partitionCount` / `reply.persistence` from `auth` (inside `if (reply.topicId >= 0)`), add:

```cpp
        if (reply.existed && reply.topicId >= firstRuntimeId) {
            if (const TopicSpec* s = reg.topic(reply.topicId)) {
                reply.sinkSpecMatches =
                    req.cleanupPolicy == static_cast<uint8_t>(TopicSpec::CleanupPolicy::Compact) &&
                    static_cast<uint8_t>(s->cleanupPolicy) == req.cleanupPolicy &&
                    s->partitionKey == req.keyField &&
                    s->partitionCount == req.partitionCount &&
                    persistenceToWire(s->persistence) == req.persistence;
            }
        }
```

`AdminWorker.cpp` `onCreateViewReply_`: replace `    if (reply.existed) {` with:

```cpp
    // OPTIONAL KL1 follow-up: a retry may adopt ITS OWN orphan sink — a runtime topic
    // with exactly this sink's spec that no TopicSchema / ViewRecord claims.
    const bool adoptOrphanSink = reply.existed && reply.sinkSpecMatches &&
                                 reg_.byId(reply.topicId) == nullptr &&
                                 viewRegistry_ != nullptr &&
                                 viewRegistry_->byViewTopicId(reply.topicId) == nullptr;
    if (reply.existed && !adoptOrphanSink) {
```

`BrokerNode.h`: add a member `int32_t firstRuntimeTopicId_ = 0;   // static RUNE_TOPICS count (set before recovery appends)`.
`BrokerNode.cpp` (~L383): as the first statement inside the `{` block that calls `recoverRuntimeTopicSpecs`, add `firstRuntimeTopicId_ = static_cast<int32_t>(config_.topics.size());`. In the drain, replace `reply = createTopicForAdmin(*topicRegistry_, req);` with `reply = createTopicForAdmin(*topicRegistry_, req, firstRuntimeTopicId_);`.

`DdlHarness.h`: in `deliverCreate`, `deliverCreateWithPersistence` and `failCreate`, pass `static_cast<int32_t>(opt_.statics.size())` as the third argument of `createTopicForAdmin`.

`DdlExpectations.h`: change the OrphanSink rows to

```cpp
    /* OrphanSink (KL1)    */ { 0, 25,  0,  0,  7,  7,  7,  7,  7,  7,  7,  7},
```

and leave the create-requests row unchanged (`{1,1,1,1,…}`). Update the plan's markdown table the same way.

`DdlConformanceBrokerRegressionTest.cpp` T5:
- change the pre-restart retry expectation to `EXPECT_EQ(w.view(sid, "orphan_v", "character"), kOk) << "the retry adopts its own orphan sink";`
- change the post-restart line to `EXPECT_EQ(w.view(sid, "orphan_v", "character"), kViewExists);`

- [ ] **Step 4: Run all DDL suites.**
  Run: `cmake --build /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast --target rune_ddl_conformance_tests rune_ddl_stress_tests rune_ddl_broker_tests && ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R 'Ddl' --output-on-failure -j8`
  Expected: all PASS. The OrphanSink occupant cells, the pair T5b/T5c/T6 view-then-view cells, the stress model and the broker T5 test all read the updated table.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir /Users/vradu/Projects/ddl-worktrees/rune-broker/build-fast -R Regression --output-on-failure -j8
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker add libs/node/include/rune/node/AdminChannels.h libs/node/include/rune/node/AdminCreateTopic.h libs/node/src/AdminWorker.cpp libs/node/include/rune/node/BrokerNode.h libs/node/src/BrokerNode.cpp libs/node/tests/support/DdlHarness.h libs/node/tests/support/DdlExpectations.h libs/node/tests/node/DdlHarnessRegressionTest.cpp libs/node/tests/integration/DdlConformanceBrokerRegressionTest.cpp docs/superpowers/plans/2026-10-12-rune-ddl-conformance.md
git -C /Users/vradu/Projects/ddl-worktrees/rune-broker commit -m "feat(admin): a CREATE VIEW retry adopts its own orphan sink (spec-identical, unclaimed runtime topic) — resolves parked KL1

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Test inventory (approximate)

| Layer | Suite(s) | Cases |
|---|---|---|
| unit | `TopicNameRegression` | 6 (+ static_asserts) |
| unit | `RuntimeTopicRecoveryRegression` (new: placeholders 4, topic-dir reconcile 10) | 14 |
| unit | `NodeConfigRegression.RuneTopics*` (B7) | 3 |
| unit | `DdlHarnessRegression` | 4 |
| unit | `DdlFixRegression` (B1–B4, B6, B8, B9, KL2) | 18 |
| unit | `Ddl/DdlOccupantMatrixRegression` (38 rows × 12 ops) | 456 |
| unit | `DdlMalformedRequestRegression` | 10 |
| unit | `Ddl/DdlViewSourceRegression` (incl. 4 view-over-view cells) | 12 |
| unit | `Ddl/DdlPairTimingMatrixRegression` | 560 |
| unit (wire) | `Ddl/SchemaGatewayAdoptionRegression` + `…NameRegression` + `…MalformedDdlRegression` + `SchemaGatewayStaticRegression` | 29 |
| unit (protocol) | `ViewMessagesRegression.{InvalidTopicName,ViewSourceIsView}CodeIsPinned` (40, 41) | 2 |
| stress | `Seeds/DdlStressRegression` (64 seeds × ~120 ops) + `DdlStressEnvRegression` | 65 |
| integration | `DdlConformanceBrokerRegression` (incl. placeholder hiding, placeholder-dir deletion, quarantine of a removed static topic, B8/B9 live + restart) | 7 |
| e2e | view-engine-e2e legs: alter-view, 3 race legs, catalog diff | 5 checks |
| OPTIONAL | `DdlFixRegression.Optional_*` | 2 |

Total ≈ 1,100 unit/wire cells, 64 stress seeds, 7 integration tests, 5 e2e checks. 19 tasks, including Task 14, which is OPTIONAL.

---

## Self-review

**1. Spec coverage.**
- Serialization is covered three ways: the harness invariant on every cell; the T3/T4 "B not started / no create request" checks plus the T2–T4 timing-invariance twin; and the stress FIFO model.
- The collision matrix covers the name relations the user listed:

  | Relation | Covered by |
  |---|---|
  | same / different | pair `Rel` |
  | B names A's view output | `Same` with A a view: alter 38, get 0, define 25, view 32 |
  | static schema-less | occupant rows + wire adoption |
  | recovered runtime topic | `RecoveredRuntimeTopic` / `RecoveredView` rows, T7 |
  | failed CREATE VIEW orphan | `OrphanSink_KnownLimitation` (KL1, labelled), `OrphanSinkAfterRestart` |
  | case variants | B3, case-sensitive + fold-reject for DDL; B7 for `RUNE_TOPICS` |
  | empty / over-long / invalid characters | B1 for DDL; B7 for `RUNE_TOPICS` |
  | reserved `__` | all six internal names found in `BrokerNode.cpp` + `__orphan.<id>`, B2 |

- All seven timings are covered. T6 is unit-only, scoped out of integration with the reason given (KL4).
- Every cell asserts the exact code, no half-creation (catalog, TopicSchema record, `__views` record, worker install, `requestCreateTopic` count), the final registry including `keyField`, at most one op in flight, and the post-restart state.
- Parameterized via `TEST_P` / `INSTANTIATE_TEST_SUITE_P`; every suite name contains `Regression`.
- Integration: real BrokerNode over Aeron, single-node, one slice per timing class. 3-node failover is explicitly out of scope.
- e2e: two clients race and exactly one wins; `alter topic <view>` → 38; the catalog is identical after a restart.
- Stress: seeded, model-checked against `name → kind/persistence/version`, with a zero-alloc steady-state gate, a fixed seed set and the seed recorded on failure.
- The parked ruling is pinned as KL1, and the OPTIONAL Task 14 is provided.
- The fix wave's U2 tests and the no-hang wave's single malformed-DDL tests are extended, not duplicated. R1–R3 landed in `8a693c5`; this plan only pins them (`_R1` cells, Task 11). Error codes start after the wave's `InvalidRequest = 39`.
- **User decisions (2026-10-12):** each one maps to a task with its own `*Regression*` tests.

  | Decision | Implemented in |
  |---|---|
  | D1 | Task 3: zero-partition placeholder, logged once; Task 12 proves it is hidden and refuses produce, subscribe and DDL |
  | D2a–c, e | Tasks 1, 2, 5, 6 |
  | D2d | Task 5a |
  | D3a | Task 7a, plus the `AlterRenameKey` column in the occupant matrix and Task 12 T1/T7 |
  | D3b | Tasks 1 and 7b, plus four view-source cells and Task 12 T1/T7 |
  | D4 (amended) | Task 3a: placeholder dir deleted; removed static topic quarantined intact, readable and not auto-restored; unmarked or foreign-id dirs quarantined; name-collision suffix; non-topic entries left alone; symlinks (child, topics root, quarantine root) never followed; case-variant kept; missing dir. Also the harness boot, Task 12 T5/T8 and the Task 13a docs with the manual restore |

**2. Placeholder scan.** No TBDs. Every code step carries complete code. Nothing depends on unlanded work: the no-hang wave (`8a693c5` / `4af59b1` / `46f2ff6`) is the base.

**3. Type consistency.** These are each defined once and used with the same signatures:
- `Op` (12 values, incl. `AlterRenameKey`), `Occupant`, `Cell`, `expectedCell`, `occupantAfterSuccess`, `orphanOf`, `kViewSourceIsView` (DdlExpectations.h, Task 4);
- `DdlHarness` members (Task 4; `failCreate` gains `caseConflict = false` in Task 6, `deliverCreateWithPersistence` is added in Task 8, and `createTopicForAdmin`'s third argument arrives in Task 14);
- `CatalogEntry::keyField` (Task 4, asserted in Task 8);
- `AdminWorker::hasInFlight` / `startedCount` / `ddlTimeoutNs` (Task 4) and `answerInFlight_` (Task 6, reused in Task 7);
- `orphanPlaceholderName` (Task 3), and `reconcileTopicDirs` / `TopicDirReconciliation` / `kTopicIdMarkerFile` (Task 3a), used by the harness boot in Task 4 and by `BrokerNode`;
- `topicNameErrorText` (Task 2, used in Task 5a);
- `CreateTopicReplyMsg::caseConflict` (Task 6) and `sinkSpecMatches` (Task 14);
- `TopicRegistry::topicIdForFoldedName` (Task 6);
- `ShardTestClient::send*` / `await*` / `SchemaAnswer` / `sendSubscribe` / `awaitSubscribeCode` (Task 12);
- `BrokerNode::setAdminCreateFaultForTest` (Task 12).

**4. Review Focus.** All five lines have a test in their owning task (Tasks 3, 7, 8 and 10). Two risks are added by the decisions:
- the boot directory deletion, guarded by the six Task 3a tests and the Task 12 T5 assertion;
- the zero-partition placeholder through every boot path, proven by Task 12 T5 on a real BrokerNode.

## Open questions

None remain. The user decided all five of the first draft's questions on 2026-10-12 (see "User decisions"). Task 13a documents one consequence the user should know about: a topic removed from `RUNE_TOPICS` has its data quarantined, intact, at the next boot, and re-adding the topic starts it empty until an operator restores the quarantined directory by hand.
