# Rune Phase 2b — Part A: Retractable Min / Max / Last / LastNonNull Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Amended 2026-10-20 (user decision): contiguous per-group Min/Max value block.** Task 5's gate run failed on honest churn (record p99 21.2 µs, batch p99 25.1 µs; a ~100-contributor list walk costs ~21 µs at ~200 ns per hop). Spec §A (Storage, Maintenance, Persistence, Gates) is amended. The new **Task 5b** (after Task 5) adds a fourth aux table of fixed 32-entry chunk rows holding each group's Min/Max inputs densely, so a rescan is a sequential scan, not a list walk. It also moves the node's checkpoint/replay/pass cap onto the 4-table record and sets the bound to {5, 2, 2, 3} for Min/Max views. Tasks 6–9 are patched where the bound, layout or tests change. Architecture, Global Constraints, Review Focus, Interaction matrix and File Structure are updated to match. The MRU list, the O(1) head, Last-only views and 2a-only views are unchanged.

> **Updated 2026-10-20 (plan refresh after D/B/F, monorepo)** — re-checked against the monorepo (`/Users/vradu/Projects/rune`, branch `feat/phase2b-a-retractable-min-max-last`, cut from main `a739aab5`) by the Part A preflight (`.superpowers/sdd/2026-10-11-rune-phase2b-a-retractable-min-max-last/preflight.md`; the controller accepted every ruling). Changes:
> 1. **Monorepo paths:** one repo, one branch. Every path is relative to the repo root: `libs/…` is `broker/libs/…`, `rune-client-cpp/…` is `client-cpp/…`, `docker/…` is `broker/docker/…`, `benchmark/…` is `broker/benchmark/…`, `../rune-codec` is `codec`. The two-repo branch cut and the two-repo commits are gone.
> 2. **Build dirs and commands:** `build-fast` is `build/root`; `build-bench-rel` is `build/rel` (binary `build/rel/broker/rune_broker_benchmark`; `build/bench-rel` is the client's); `rune-client-cpp/build` is `build/sub-client-cpp`; CatalogGolden runs in `build/sub-codec`. No ASan/TSan builds exist, so the controller runs the sanitizers once at the Phase 2b close. Per-task test runs are affected-area only (F13).
> 3. **Stale line hints fixed (S7–S11):** client CMake `1046–1051`; `broker/libs/view/CMakeLists.txt:21–26` (`PlanSourcesTest.cpp` now follows `AggFoldTest.cpp`, so the closing `)` stays where it is); `view_probe.cpp` setup call 1275–1276 and verify block 1456–1465; e2e banner `view-engine-e2e.sh:583–584`. Task 7 also builds `rune_get_view_row_tests` and `rune_create_view_tests` (S9). The VM e2e stages the monorepo plus a sibling `flight-recorder` (S12) and is controller-run.
> 4. **Preflight rulings folded into the tasks:** F1 (client `deserializeAuthored` bound and round-trip test, Task 1), F2 (`isPhase2bRule` fail-closed, Task 1), F3 (bisect note, Execution context), F4 (the bench gate runs right after Task 5), F5 and F11 (API doc and release-note steps, Task 9), F6 (golden bytes, Task 3), F7 (intentional test duplication, Task 5), F8 (`RetractMode` enum, Task 5), F9 (*Interaction matrix* section and the extended model test, Task 6), F10 (shared bench helper, Task 8), and the e2e-header fix (Task 9 Step 4).
> 5. **New decision AD3 (F12):** spec §A Storage's "source field's width" applies to `Last`/`LastNonNull` inputs only; Spec questions Q1–Q8 are ruled as written.

> **Updated 2026-10-19** — re-checked against the code after catalog history (`feat/catalog-history`, broker `c50b06cd`) and the authentication plan; A now runs **last** (order auth → D → B → F → A). Changes:
> 1. **Branch base:** cut from the integration tip after Part F merged (was `feature/materialized-views`). rune-codec is the `feat/catalog-history` lineage (it now also holds `rune_catalog` / `rune_security`).
> 2. **The broker view code is unchanged since 2a** apart from `WorkerViewSet` (catalog `uninstallView`, transient-source cursor reconciliation): every `broker/libs/view` line hint below was re-verified and holds. Stale hints fixed: `ViewPlanAuthor.h` enum (now line 46, comment 40–45), client `CMakeLists.txt` `rune_schema_codec_smoke_tests` (was 918–922 on 2026-10-19; now `client-cpp/CMakeLists.txt:1046-1051`), `WorkerViewSet.cpp` `maxRecordsPerPassFor` (comment 132–135, function 136), `view_probe.cpp` and `view-engine-e2e.sh` (now anchored by text, not line numbers — both files grew).
> 3. **Test dirs:** `WorkerViewSetTest.cpp` now uses the per-process `tmpDir("…")` helper (fix c506bccf); Task 7's tests use it instead of fixed `/tmp/rune-views-*` paths (two concurrent runs would share a checkpoint dir).
> 4. **Views are created through `__catalog`:** `createViewAndWait` is a catalog DDL (Redirect / NotLeader followed by the client); the `CreateView` event stores the authored plan (`normalizedPlanBlob`, fingerprinted) and the leader's `compiledPlanBlob`, and every broker installs the view from the applied event. Nothing in A changes the event or fingerprint format: the new rule byte 8 and the Min/Max/Last outputs are plan *content*. The e2e's `CATALOG_NAMES` list already contains `user_totals`.
> 5. **Auth interactions:** no new request type. `CreateView` needs `CREATE VIEW` + `READ` on the source (auth D19), `GetViewRow` needs `READ VIEW`; the auth phase's Task 20 already runs `view-engine-e2e.sh` authenticated (`broker/docker/lib/auth.sh`, `RUNE_USER` / `RUNE_PASSWORD_FILE` passed to the probe) — Task 9 keeps that wiring and adds nothing.
> 6. **The e2e run** uses the `rsync` + `flock /root/vm.lock` form the other 2026-10 plans use (the old "git archive into /root/view-engine-e2e, rune-fr" staging is gone).
> 7. New decision **AD1** and spec gap **G1** (mixed-version brokers) — see *Decisions* below.

**Goal:** Aggregate views can maintain `Min`, `Max`, `Last` and `LastNonNull` with exact retraction on update, group move and delete, and keep that exact across a checkpoint/restart. 2a-only views stay exactly as they are.

**Architecture:** Only views that use one of the four rules are **linked**. In a linked view, each contributor row gains `[prev u64][next u64]` links that form a doubly linked MRU list of the group's contributors, keyed by contributor key (`kNoContributor = UINT64_MAX`). Each group row gains `[head u64]` plus, per `Min`/`Max` field, a cached `[value 8B][state u8]`. `Last` is the head's value and `LastNonNull` is the first non-null value walking from the head. `Min`/`Max` come from the cache. **Amended 2026-10-20 (Task 5b):** a view with a Min or Max also keeps each group's Min/Max inputs in a **value block**: a fourth aux table of fixed 32-entry chunk rows, column-wise, dense positions, one spare chunk with hysteresis. When the cached extreme is retracted, the fold scans that block sequentially (`ceil(n/32)` independent chunk lookups) instead of walking the list. Links, heads, caches, block ids and positions live in the contributor and group rows; the block table travels as table 3 of the same v2 multi-table checkpoint, offset-atomic. The per-pass delta cap's worst case per record is 5 entries for 2a, 8 for Last-only linked views and 12 ({5, 2, 2, 3}) for Min/Max views; the plan derives that bound exactly and a test pins it.

**Tech Stack:** C++23 (clang, `__int128`), GoogleTest, Google Benchmark, CMake/Ninja (`build/root`), docker e2e on the k3s VM.

**Spec:** `broker/docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` §A (binding). Parent: `broker/docs/superpowers/specs/2026-10-08-rune-view-engine-single-source-design.md` §6 (aggregate table), §6.4 (nulls), §9.2 (aux state is checkpointed), §14.0 (latency gates).

**Builds on:** Phase 2a plan `broker/docs/superpowers/plans/2026-10-11-rune-view-engine-phase2a-invertible-aggregates.md` (landed: `AggState`, `foldAggregateBatch`, v2 checkpoint, `AggInstance`, per-pass cap, `BM_ViewFoldAggregate`, `user_totals` e2e).

## Execution context

- **Repo:** the monorepo at `/Users/vradu/Projects/rune` (the repo root; run every command below from it). Part A touches `broker/...` and `client-cpp/...`, and uses `codec/...` only for the CatalogGolden proof. Every path in this plan is relative to the repo root.
- **Branch:** `feat/phase2b-a-retractable-min-max-last` is already cut from main `a739aab5` (D, B and F are merged). There is no branch to create. Verify the preconditions (catalog history is in): `grep -n uninstallView broker/libs/node/src/WorkerViewSet.cpp` (2 hits) and `grep -n kCatalogTopicId broker/libs/node/include/rune/node/CatalogPartition.h` (3 hits).
- **Builds (all already configured):**
  - `build/root`: broker plus client/ql/codec/crypto sources (Ninja, Debug, `BUILD_TESTING=ON`). `cmake --build build/root -j14 --target <t>` and `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`. Targets: `rune_view_tests` (`broker/libs/view/CMakeLists.txt:21`), `rune_node_tests` (`broker/libs/node/CMakeLists.txt:154`), `rune_get_view_row_tests` (`:705`), `rune_create_view_tests` (`:639`).
  - `build/rel`: broker benchmarks (Release `-O3 -DNDEBUG`, `BUILD_BENCHMARKS=ON`, `BUILD_TESTING=OFF`). `cmake --build build/rel -j14 --target rune_broker_benchmark`; the binary is `build/rel/broker/rune_broker_benchmark`. `build/bench-rel` is the *client's* release bench build: do not use it.
  - `build/sub-client-cpp`: client (Ninja, Debug, `BUILD_TESTING=ON`, `BUILD_EXAMPLES=ON`; `view_probe` and `rune_schema_codec_smoke_tests` are built here). `cmake --build build/sub-client-cpp --target <t>` and `ctest --test-dir build/sub-client-cpp -R <filter>`. It re-runs configure when a `CMakeLists.txt` changes.
  - `build/sub-codec`: codec. `cmake --build build/sub-codec --target rune_catalog_tests && ctest --test-dir build/sub-codec -R CatalogGolden` (3 tests; `build/root` has none).
  If a directory is missing, stop and tell the controller; do not configure a new one.
- **Sanitizers:** none exist in the monorepo (no `build-asan`, `build-tsan` or client `build_tsan`), and the frozen copies under `~/Projects/se-worktrees/` must not be used. The controller runs ASan+UBSan and TSan once, at the Phase 2b close.
- **Test gate (F13, CLAUDE.md #7):** each task builds and runs only the executables it touches plus its new or changed tests; its "Run" steps name them. `rune_view_tests` covers Tasks 1–6 and 8 (Tasks 1–2 also `rune_create_view_tests`); Task 7 uses `rune_node_tests rune_get_view_row_tests rune_create_view_tests`; the client uses `rune_schema_codec_smoke_tests` (plus `rune_cli_catalog_tests` for the F1 change) and `view_probe`. The FULL `*Regression*` suite, CatalogGolden (`build/sub-codec`), the sanitizers, the VM e2e and any clean-box bench confirmation (F14) are **controller checkpoints** (after Task 4, after Task 7, after Task 9, and the Phase 2b close). A checkpoint failure is bisected over the commits since the last green checkpoint. Where an older step still says "run the full Regression suite", the per-task affected-area run is what binds. Every fix gets a test whose suite or name contains `Regression`.
- **Bisect note (F3):** the branch merges whole. Commits after Task 2 and before Task 5 must not create Min/Max/Last views: the 2a fold still writes 8 bytes at `contribValOff + 8*input` (`AggFold.cpp:44,250`), which lands past `rowLen` in Task 4's linked layout until Task 5 replaces the fold. Bisect over those commits with unit tests only.
- **Commits** go on this branch in the monorepo (`git add broker/<p> client-cpp/<p>`). Nothing is pushed. Every commit message ends with the trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never stage `.superpowers/`.

## Global Constraints

- CLAUDE.md hard rules: shared-nothing; rings are the only cross-thread passing; state machines (each state holds exactly its variables); zero steady-state allocation; test and benchmark everything; a `*Regression*` test per fix; full Regression suite first; fix-on-sight.
- `Min`/`Max` take numeric source types only (Int32/Int64/UInt16/UInt32/UInt64/Float64). Output type = source type. Nulls are ignored. NaN/±Inf count as NULL and are counted in `aggNonFinite` (as in 2a). An empty or all-null group gives NULL.
- `Last` is the value of the **most recently written** contributor (highest log offset), **including null**. It accepts any fixed-width source type. Output type = source type.
- `LastNonNull` is the value of the most recently written contributor whose value is non-null, or NULL if there is none. It uses the new wire rule **`MergeRule::LastNonNull = 8`** (append-only).
- Table semantics, the co-partition rule (single-partition source) and retraction on update, group move and delete are exactly 2a's.
- **Only views that use one of the four rules get the extra columns. 2a-only views keep their 2a layout byte-for-byte**, including the fold, emit order, counters and v2 checkpoint bytes.
- Wire enums are append-only. Persistence: no format change. **Amended 2026-10-20:** a view with Min/Max checkpoints one more table (value blocks, id 3) inside the unchanged v2 record; 2a-only and Last-only records stay byte-identical. Recovery replays all tables at the same `O`.
- Inconsistent state (a link to a missing contributor, or a rescan whose non-null count does not match) increments `aggInconsistent`. It never crashes and never invents state.
- §14.0 gate (typical-case fold): **record p99 < 5 µs, batch p99 < 10 µs, zero steady-state allocation.** The extreme-retraction benchmark (groups of 1k and 10k contributors) is **reported, not gated**.

## Decisions (added 2026-10-19)

- **AD1 — Mixed-version clusters:** a view using `Min` / `Max` / `Last` / `LastNonNull` is created only once **every** broker runs a build with Part A. `CreateView` events are applied on every broker from `__catalog` (the leader validates and compiles; each broker installs from the event's `compiledPlanBlob`), and a pre-A broker cannot install such a plan. A ships no wire/version negotiation for this (pre-1.0, rolling upgrades of the view engine are not supported yet): the release note says "upgrade every broker before creating a view with min/max/last". Task 1 already retargets `ViewPlanRegression.UnknownRuleValueIsRejected` to byte 9 (one past `LastNonNull`), so an unknown future rule still fails closed; Task 9's commit adds the release-note line to `broker/docs/release-notes/` (one file, `phase2b-a-retractable-aggregates.md`).
- **AD2 — No catalog format change:** nothing in A touches `rune_catalog` (event layout, fingerprints, golden logs); `CatalogGoldenRegression.*` must pass unchanged — run `cmake --build build/sub-codec --target rune_catalog_tests && ctest --test-dir build/sub-codec -R CatalogGolden` (3 tests; `build/root` has none) at each controller checkpoint as the proof.

- **AD3 — Value-slot width (F12, spec §A Storage):** the spec says input value slots "get the source field's width instead of a fixed 8". Part A applies the source width to **`Last`/`LastNonNull` inputs only**. Count, Sum, Avg, Min and Max keep the widened 8-byte slot, so 2a-only views stay byte-for-byte (Spec question 1) and Min/Max compare in one domain. Spec questions Q1–Q8 are ruled as written (each is fail-closed or 2a-preserving).

## Review Focus

These are the input classes most likely to hurt a user. Each one is pinned by a named test in the task that owns it:

1. **Retracting one of several equal extremes.** `Max` must stay at the duplicated value and must not fall to the next distinct value. Test: `AggRetractableRegression.RetractingOneOfDuplicateExtremesKeepsTheExtremeRegression` (Task 5).
2. **A restart, then retraction of the extreme.** The rescan must scan the **recovered** value block (amended 2026-10-20; it was the list) and caches, and give the exact next extreme. Tests: `WorkerViewSetRegression.RestartThenRetractingTheMaxIsExactRegression` (Task 7) and the e2e verify leg (Task 9: `o3 5→25` retracts the `min`, `o1 50→20` retracts the `max`).
3. **The per-pass delta cap with linked rows.** A group move dirties up to 12 entries in a Min/Max view: 5 contributor, 2 group, 2 view and 3 value-block rows (8 in a Last-only view). If the bound is too low, a large pass throws on the worker thread. Tests: `AggRetractableRegression.WorstCaseMoveDirtiesExactlyTheBoundRegression` and `AggRetractableRegression.BlockModelAcrossChunkBoundariesMatchesBruteForceRegression` (Task 5b), `AggRetractableRegression.RandomizedOpsMatchBruteForceModelRegression` (Task 6), and `WorkerViewSetRegression.PersistedRetractableMovesDeltasFitTheRingRegression` (Task 7).
4. **A contributor whose 8-byte key equals the `kNoContributor` sentinel (`0xFF…FF`).** It must be skipped and counted, never linked; linking it would corrupt the list's end marker. Test: `AggRetractableRegression.ContributorKeyEqualToSentinelIsSkippedRegression` (Task 5).
5. **A damaged link** (a neighbour or head that is missing from the contributor table). It must be counted and must never be followed or created. There must be no loop and no crash. Test: `AggRetractableRegression.DanglingLinkIsCountedNotFollowedRegression` (Task 5).

Also pinned: 2a views are unchanged, by `AggStateRegression.TwoAOnlyLayoutIsUnchangedRegression` (Task 4) and the untouched `AggFold*` suite.

## Interaction matrix

Every feature is tested across op × entity × timing (CLAUDE.md memory: test interaction matrices). Axes:
- **Op:** insert, in-place raise, in-place equal, in-place lower, group move, tombstone, no-group (the group key is absent).
- **Entity (rule):** Min, Max, Last, LastNonNull, 2a-only view.
- **Timing:** same batch / single-record batch, multi-record batch, capped-pass boundary (mid-batch resume), across restart.

**Op × entity.** Each cell names the test that pins it; `model` = `RandomizedOpsMatchBruteForceModelRegression` (Task 6), which mixes every op over all four rules at once and checks every group after every step.

| Op | Min / Max | Last | LastNonNull | 2a-only view |
|---|---|---|---|---|
| insert | `MinMaxLastFoldOnInsert` | `MinMaxLastFoldOnInsert` | `MinMaxLastFoldOnInsert` (newer null skipped) | untouched `AggFold*` |
| in-place raise | `InPlaceRaiseOfTheMaxSkipsTheRescanRegression` (Max); model (Min) | `LastAfterDeletingTheNewestIsThePreviousWriteRegression` (re-write moves to head) | `LastIncludesNullAndLastNonNullSkipsNullsRegression` | `AggFold*` |
| in-place equal | `InPlaceRaiseOfTheMaxSkipsTheRescanRegression` (equal re-write, no rescan); `RetractingOneOfDuplicateExtremesKeepsTheExtremeRegression` | model | model | `AggFold*` |
| in-place lower | `InPlaceLowerOfTheMaxRescansRegression`; `UnsignedMinRetractsOnTombstoneRegression` | model | `LastIncludesNullAndLastNonNullSkipsNullsRegression` | `AggFold*` |
| group move | `GroupMoveOfTheExtremeRescansOldGroupAndRaisesNewRegression`; `WorstCaseMoveDirtiesExactlyTheDerivedBoundRegression` | same two | model | `AggFold*` |
| tombstone | `RetractingTheMaxSurfacesTheNextRegression`; `NonFiniteMaxInputIsNullAndCountedRegression` | `LastAfterDeletingTheNewestIsThePreviousWriteRegression` | `LastIncludesNullAndLastNonNullSkipsNullsRegression` (down to NULL) | `AggFold*` |
| no-group | `NoGroupRewriteUnlinksAndRetractsTheContributorRegression` (Task 6) | same | model never produces it; same test covers the unlink | `AggFoldRegression.MissingGroupValueRetractsAndSkips` |
| sentinel / damaged link | `ContributorKeyEqualToSentinelIsSkippedRegression`, `DanglingLinkIsCountedNotFollowedRegression` (Task 5) | same | same | n/a (no links) |

**Op × timing.**

| Timing | Test |
|---|---|
| single-record batches | all Task 5 unit tests; the model's one-record steps |
| multi-record batch (1–4 records, the same contributor can repeat inside one batch) | model (Task 6, extended per F9) |
| capped-pass boundary / mid-batch resume (pass 1 stops at `bound`, pass 2 resumes from the cursor; dirty entries coalesce inside a pass) | model, every 5th multi-record step is split at a random boundary (bound check: at most records × per-record bound per pass); `WorkerViewSetRegression.PersistedRetractableMovesDeltasFitTheRingRegression` (real caps, Task 7) |
| across restart (links, head, caches recovered from the v2 checkpoint) | `WorkerViewSetRegression.RestartThenRetractingTheMaxIsExactRegression` (Task 7: in-place lower of the recovered max); the e2e verify leg (Task 9: in-place raise past the cached min, in-place lower of the cached max, with the group move done before the restart) |

**Value block (Task 5b).** Op × block position (chunk 0 slot 0, chunk boundary 31/32 and 63/64, tail, swapped-in entry) × group-size state (first entry and id pick, id collision, boundary ±1 with spare reuse, spare freed at tail ≤ 16, n → 0 frees the id) × timing (single and multi-record batch, capped pass, checkpoint round trip, worker restart). The full cell-to-test table is in Task 5b.

Known thin spots, accepted: tombstone-after-restart and Last/LastNonNull after restart are covered only by the e2e's head values; a restart in the middle of a capped multi-pass range is covered by 2a's recovery tests (the format is unchanged).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `broker/libs/view/include/rune/view/ViewTypes.h`, `broker/libs/view/src/ViewPlan.cpp`, `broker/libs/view/src/PlanCompiler.cpp` (one line) | `MergeRule::LastNonNull = 8`; accept rule byte 8; the compiler stays fail-closed for it until Task 2 (F2) | 1 |
| `client-cpp/include/rune/client/view/ViewPlanAuthor.h`, `client-cpp/tests/client/ViewPlanAuthorTest.cpp` (new), `client-cpp/CMakeLists.txt` | client mirror + wire pin; `deserializeAuthored` rule bound and round-trip pin (F1) | 1 |
| `broker/libs/view/src/PlanCompiler.cpp` | accept Min/Max/Last/LastNonNull with source output types | 2 |
| `broker/libs/view/include/rune/view/ViewRow.h`, `broker/libs/view/src/ViewRow.cpp` | `encodeSlotValue`/`decodeSlotValue` (shared by `writeSlot`/`readSlot` and Last storage) | 3 |
| `broker/libs/view/include/rune/view/AggState.h`, `broker/libs/view/src/AggState.cpp` | linked layout: links, head, caches, per-input offset/width, `aggDirtyBoundPerRecord` | 4 |
| `broker/libs/view/include/rune/view/ViewFold.h` | `FoldStats::aggRescans` | 5 |
| `broker/libs/view/src/AggFold.cpp` | list maintenance, cache maintenance, rescan, Last/LastNonNull emit | 5 |
| `broker/libs/view/tests/view/AggRetractableTest.cpp` (new), `broker/libs/view/CMakeLists.txt` | fold tests, bound pin, model test | 5, 6 |
| `broker/libs/view/include/rune/view/AggState.h`, `broker/libs/view/src/AggState.cpp`, `broker/libs/view/include/rune/view/AggFold.h`, `broker/libs/view/src/AggFold.cpp` | Min/Max value block: layout, bound {5,2,2,3}, append / in-place / swap-remove / spare policy, block rescan, `blocks` overload | 5b |
| `broker/libs/view/tests/view/AggStateTest.cpp`, `broker/libs/view/tests/view/AggRetractableTest.cpp` | block layout pins, harness, worst-case pin, block tests and model | 5b |
| `broker/libs/node/include/rune/node/WorkerViewSet.h`, `broker/libs/node/src/WorkerViewSet.cpp`, `broker/benchmark/ViewFoldBench.cpp` (`AggRig`) | fourth table: hosting, fold call, replay, checkpoint, discard, backlog, per-pass cap | 5b |
| `broker/libs/node/src/WorkerViewSet.cpp`, `broker/libs/node/include/rune/node/WorkerViewSet.h` | `ViewFoldCounters::aggRescans` (the per-pass cap itself moved to 5b) | 7 |
| `broker/libs/node/tests/node/WorkerViewSetTest.cpp` | restart regression, large-range move regression | 7 |
| `broker/benchmark/ViewFoldBench.cpp` | shared `AggRig` helper (F10), `BM_ViewFoldRetractable` (gate; runs right after Task 5, F4), `BM_ViewFoldExtremeRetract` (reported) | 8 |
| `client-cpp/examples/view_probe.cpp`, `broker/docker/view-engine-e2e.sh`, `docs/api/client-cpp.md` (F5), `broker/docs/release-notes/phase2b-a-retractable-aggregates.md` (F11) | `user_totals` gains `max`/`min`/`last` across the restart; API doc and release note | 9 |

---

### Task 1: `MergeRule::LastNonNull` on the wire (broker + client)

**Files:**
- Modify: `broker/libs/view/include/rune/view/ViewTypes.h:6-8`
- Modify: `broker/libs/view/src/ViewPlan.cpp:124-126`
- Modify: `broker/libs/view/tests/view/ViewPlanTest.cpp:240-246`
- Modify: `client-cpp/include/rune/client/view/ViewPlanAuthor.h:40-46` (the comment block starting `// Mirrors rune::view::MergeRule` and the enum)
- Create: `client-cpp/tests/client/ViewPlanAuthorTest.cpp`
- Modify: `client-cpp/include/rune/client/view/ViewPlanAuthor.h:224` (F1: the rule bound in the client's own `deserializeAuthored`, used by `rune catalog replay` via `client-cpp/src/cli/CatalogCommands.cpp:549`)
- Modify: `client-cpp/CMakeLists.txt:1046-1051` (the `rune_schema_codec_smoke_tests` target; anchor `add_executable(rune_schema_codec_smoke_tests`)
- Modify: `broker/libs/view/src/PlanCompiler.cpp:56-58` (F2: `isPhase2bRule` also lists `LastNonNull`)

**Interfaces:**
- Produces: `enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll, LastNonNull };` (`LastNonNull == 8`) in both `rune::view` and `rune::client::view`.

- [ ] **Step 1: Write the failing broker tests.** In `broker/libs/view/tests/view/ViewPlanTest.cpp`, replace the whole `TEST(ViewPlanRegression, UnknownRuleValueIsRejected)` (lines 240–246) with:

```cpp
// Phase 2b: LastNonNull is appended at 8; rules 0-7 keep their meaning.
TEST(ViewPlanRegression, LastNonNullRuleRoundTripsAtWireValue8) {
    rune::view::ViewPlan a;
    a.viewName = "user_stats"; a.key = "userId"; a.sources = {{5, "userId"}};
    a.output = {{"userId", rune::codec::PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                {"lnn", rune::codec::PrimType::Int32, 0, "bonus", rune::view::MergeRule::LastNonNull}};
    auto back = rune::view::deserializeAuthored(rune::view::serializeAuthored(a));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->output[1].rule, rune::view::MergeRule::LastNonNull);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::MergeRule::LastNonNull), 8u);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::MergeRule::CountAll), 7u);
}

TEST(ViewPlanRegression, UnknownRuleValueIsRejected) {
    rune::view::ViewPlan a;
    a.viewName = "v"; a.key = "k"; a.sources = {{5, "k"}};
    a.output = {{"k", rune::codec::PrimType::Int64, 0, "k", static_cast<rune::view::MergeRule>(9)}};
    EXPECT_FALSE(rune::view::deserializeAuthored(rune::view::serializeAuthored(a)).has_value())
        << "rule byte 9 (one past LastNonNull) must be rejected, never cast";
}
```

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'ViewPlanRegression' --output-on-failure`
Expected: compile error `no member named 'LastNonNull' in 'rune::view::MergeRule'`.

- [ ] **Step 3: Implement the broker side.** In `broker/libs/view/include/rune/view/ViewTypes.h`, replace lines 6–8 with:

```cpp
// Wire values are APPEND-ONLY (serialized as u8 in plan blobs): CountAll (= SQL
// Count(*), counts contributors including nulls) was appended at 7 in Phase 2a;
// LastNonNull (most recent non-null contributor value) at 8 in Phase 2b.
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll, LastNonNull };
```

In `broker/libs/view/src/ViewPlan.cpp`, replace `isValidMergeRule`:

```cpp
bool isValidMergeRule(uint8_t v) {
    return v <= static_cast<uint8_t>(MergeRule::LastNonNull);
}
```

In `broker/libs/view/src/PlanCompiler.cpp` (F2), make `isPhase2bRule` list the new rule too, so it stays fail-closed until Task 2 lifts the rejection. Without this, at this commit a `LastNonNull` column would take the Replace type-check path, compile, and the 2a fold would emit NULL for it:

```cpp
bool isPhase2bRule(MergeRule r) {
    return r == MergeRule::Min || r == MergeRule::Max || r == MergeRule::Last ||
           r == MergeRule::LastNonNull;
}
```

Task 2 deletes `isPhase2bRule` as planned, so no test pins this transient state.

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build/root -j14 --target rune_view_tests rune_create_view_tests && ctest --test-dir build/root -R 'ViewPlan|PlanCompiler|CreateView' -j14 --output-on-failure`
Expected: every `ViewPlan*`, `PlanCompiler*` and `CreateView*` test passes (`MinMaxLastAreRejectedUntilPhase2b` still passes).

- [ ] **Step 5: Write the failing client test.** Create `client-cpp/tests/client/ViewPlanAuthorTest.cpp`:

```cpp
// tests/client/ViewPlanAuthorTest.cpp — the client's MergeRule must mirror the
// broker's wire values byte-for-byte (append-only enum).
#include <gtest/gtest.h>

#include <rune/client/view/ViewPlanAuthor.h>

namespace rcv = rune::client::view;

TEST(ViewPlanAuthorRegression, LastNonNullIsAppendedAtWireValue8) {
    EXPECT_EQ(static_cast<uint8_t>(rcv::MergeRule::CountAll), 7u);
    EXPECT_EQ(static_cast<uint8_t>(rcv::MergeRule::LastNonNull), 8u);

    rcv::ViewPlan p;
    p.viewName = "v"; p.key = "k";
    p.sources.push_back(rcv::SourceSpec{5, "k"});
    p.output.push_back(rcv::AuthoredField{"k", rune::codec::PrimType::Int64, 0, "k", rcv::MergeRule::Replace});
    p.output.push_back(rcv::AuthoredField{"b", rune::codec::PrimType::Int32, 0, "bonus", rcv::MergeRule::LastNonNull});
    const auto blob = rcv::serializeAuthored(p);
    // Tail = [last field's rule u8][persistence u8][sizing: u32 + u32 + u64].
    ASSERT_GE(blob.size(), 18u);
    EXPECT_EQ(blob[blob.size() - 18], 8u) << "LastNonNull must serialize as rule byte 8";

    // F1: the client's own decoder (used by `rune catalog replay`) must round-trip
    // rule 8 and still reject one past it.
    const auto back = rcv::deserializeAuthored(blob);
    ASSERT_TRUE(back.has_value()) << "deserializeAuthored must accept rule byte 8";
    ASSERT_EQ(back->output.size(), 2u);
    EXPECT_EQ(back->output[1].rule, rcv::MergeRule::LastNonNull);
    auto bad = blob;
    bad[bad.size() - 18] = 9;
    EXPECT_FALSE(rcv::deserializeAuthored(bad).has_value())
        << "rule byte 9 (one past LastNonNull) must be rejected, never cast";
}
```

In `client-cpp/CMakeLists.txt`, replace the `rune_schema_codec_smoke_tests` block (lines 1046–1051; anchor `add_executable(rune_schema_codec_smoke_tests`; today it is `add_executable(… tests/SchemaCodecSmokeTest.cpp)`, `target_link_libraries(… rune_codec GTest::gtest_main)` and `gtest_discover_tests`, with no `target_include_directories`) with:

```cmake
    add_executable(rune_schema_codec_smoke_tests
        tests/SchemaCodecSmokeTest.cpp
        tests/client/ViewPlanAuthorTest.cpp
    )
    target_include_directories(rune_schema_codec_smoke_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(rune_schema_codec_smoke_tests PRIVATE rune_codec GTest::gtest_main)
    gtest_discover_tests(rune_schema_codec_smoke_tests)
```

Run: `cmake --build build/sub-client-cpp --target rune_schema_codec_smoke_tests`
Expected: compile error `no member named 'LastNonNull'`.

- [ ] **Step 6: Implement the client mirror.** In `client-cpp/include/rune/client/view/ViewPlanAuthor.h`, replace lines 40–46 (the comment starting `// Mirrors rune::view::MergeRule` and the enum) with:

```cpp
// Mirrors rune::view::MergeRule (broker/libs/view/include/rune/view/ViewTypes.h).
// Brokers accept Replace (projection, and the GROUP BY column of an aggregate
// view) plus the aggregates CountAll (Count(*)), Count(col), Sum, Avg (Phase
// 2a) and the retractable Min, Max, Last, LastNonNull (Phase 2b) — GROUP BY the
// view key, single source of one partition. Wire values are append-only so a
// plan authored here round-trips byte-identically through the broker's
// deserializeAuthored.
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll, LastNonNull };  // CountAll=7, LastNonNull=8
```

Build and run now, before the bound change below: the blob-byte assertion passes and the new round-trip assertion **fails** (`deserializeAuthored must accept rule byte 8`), because the client decoder still rejects every rule above `CountAll`.

Then fix the decoder (F1). In `client-cpp/include/rune/client/view/ViewPlanAuthor.h` replace line 224:

```cpp
        if (rule > static_cast<uint8_t>(MergeRule::CountAll)) return std::nullopt;
```

with

```cpp
        if (rule > static_cast<uint8_t>(MergeRule::LastNonNull)) return std::nullopt;
```

Without this, any view that uses `LastNonNull` makes `rune catalog replay` fail with "the event's normalized view plan does not decode".

- [ ] **Step 7: Run the client tests.**

Run: `cmake --build build/sub-client-cpp --target rune_schema_codec_smoke_tests rune_cli_catalog_tests view_probe && ctest --test-dir build/sub-client-cpp -R 'ViewPlanAuthor|SchemaCodecSmoke|RuneCliCatalog' --output-on-failure`
Expected: `ViewPlanAuthorRegression.LastNonNullIsAppendedAtWireValue8` passes (wire byte, round-trip of rule 8, rejection of rule 9), the `RuneCliCatalog*` tests still pass, and `view_probe` still compiles.

- [ ] **Step 8: Run the affected broker tests and commit.**

Run: `ctest --test-dir build/root -R 'ViewPlan|PlanCompiler|CreateView' -j14 --output-on-failure`. Expected: 0 failures. (The full `*Regression*` suite is a controller checkpoint.)

```bash
git add broker/libs/view/include/rune/view/ViewTypes.h broker/libs/view/src/ViewPlan.cpp broker/libs/view/src/PlanCompiler.cpp broker/libs/view/tests/view/ViewPlanTest.cpp
git commit -m "feat(view): MergeRule::LastNonNull appended at wire value 8" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git add client-cpp/include/rune/client/view/ViewPlanAuthor.h client-cpp/tests/client/ViewPlanAuthorTest.cpp client-cpp/CMakeLists.txt
git commit -m "feat(view): client MergeRule::LastNonNull mirrors broker (wire value 8); deserializeAuthored accepts it" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Compiler — accept Min/Max/Last/LastNonNull

Today the compiler rejects `Min`/`Max`/`Last` with a "Phase 2b" message (`PlanCompiler.cpp:141-145`). This task lifts that rejection and fixes the output types: `Min`/`Max` produce the numeric source type, and `Last`/`LastNonNull` produce any fixed-width source type. A `FixedStr` output already inherits the source's `fixedLen` (the existing `rof.fixedLen` path).

**Files:**
- Modify: `broker/libs/view/src/PlanCompiler.cpp:51-87`, `:141-145`, `:313` (line hints are before Task 1's one-line F2 edit, which shifts later lines by +1; anchor by the function names and quoted text)
- Modify: `broker/libs/view/tests/view/PlanCompilerTest.cpp:181-187`

**Interfaces:**
- Consumes: `MergeRule::LastNonNull` (Task 1).
- Produces: an aggregate view may use `Min`/`Max` (numeric source, output type == source type) and `Last`/`LastNonNull` (any fixed-width source, output type == source type, `FixedStr` width == source `fixedLen`). Non-key error messages must **not** contain the word `key` (AdminWorker maps such messages to `ViewKeyInvalid`).

- [ ] **Step 1: Write the failing tests.** In `broker/libs/view/tests/view/PlanCompilerTest.cpp`, replace the whole `TEST(PlanCompilerAggregateRegression, MinMaxLastAreRejectedUntilPhase2b)` (lines 181–187) with:

```cpp
TEST(PlanCompilerAggregateRegression, MinMaxLastLastNonNullCompileWithSourceOutputTypes) {
    auto r = compileAgg(aggPlan({{"mx", P::Int32, 0, "amount", R::Max},
                                 {"mn", P::UInt32, 0, "qty", R::Min},
                                 {"mxp", P::Float64, 0, "price", R::Max},
                                 {"lastNote", P::Bool, 0, "note", R::Last},
                                 {"lnn", P::Int32, 0, "amount", R::LastNonNull}}));
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& out = r->plan.output;
    ASSERT_EQ(out.size(), 6u);
    EXPECT_EQ(out[1].rule, R::Max);         EXPECT_EQ(out[1].type, P::Int32);
    EXPECT_EQ(out[2].rule, R::Min);         EXPECT_EQ(out[2].type, P::UInt32);
    EXPECT_EQ(out[3].type, P::Float64);
    EXPECT_EQ(out[4].rule, R::Last);        EXPECT_EQ(out[4].type, P::Bool);
    EXPECT_EQ(out[5].rule, R::LastNonNull); EXPECT_EQ(out[5].type, P::Int32);
}

TEST(PlanCompilerAggregateRegression, MinOverNonNumericIsRejected) {
    auto r = compileAgg(aggPlan({{"x", P::Bool, 0, "note", R::Min}}));
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("numeric"), std::string::npos) << r.error().message;
    EXPECT_EQ(r.error().message.find("key"), std::string::npos) << "must not classify as KeyInvalid";
}

TEST(PlanCompilerAggregateRegression, MaxDeclaredWithAWidenedTypeIsRejected) {
    auto r = compileAgg(aggPlan({{"x", P::Int64, 0, "amount", R::Max}}));   // amount is Int32
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("produces Int32"), std::string::npos) << r.error().message;
    EXPECT_EQ(r.error().message.find("key"), std::string::npos);
}

TEST(PlanCompilerAggregateRegression, LastOverFixedStrCarriesTheSourceWidth) {
    const auto src = rune::codec::DescriptorBuilder("order", 1)
                         .add("orderId", P::Int64, 1, /*isKey*/ true)
                         .add("userId", P::Int64)
                         .addFixedStr("sku", 6)
                         .build();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto r = rune::view::compileView(aggPlan({{"lastSku", P::FixedStr, 0, "sku", R::Last}}), srcs, 77);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(r->plan.output[1].type, P::FixedStr);
    EXPECT_EQ(r->rowLayout.slotWidth[1], 6u);
}
```

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'PlanCompilerAggregateRegression' --output-on-failure`
Expected: `MinMaxLastLastNonNull…` fails with "uses Min/Max/Last, which are not supported until Phase 2b". `LastOverFixedStr…` fails the same way. `MaxDeclaredWithAWidenedType…` fails because its message contains "Phase 2b", not "produces Int32".

- [ ] **Step 3: Implement.** In `broker/libs/view/src/PlanCompiler.cpp`, replace lines 51–87 (51–88 after Task 1; `isAggregateRule`, `isPhase2bRule` including Task 1's `LastNonNull` line, `primName`, `aggregateOutputType`) with:

```cpp
bool isAggregateRule(MergeRule r) {
    switch (r) {
        case MergeRule::Count: case MergeRule::CountAll: case MergeRule::Sum: case MergeRule::Avg:
        case MergeRule::Min: case MergeRule::Max: case MergeRule::Last: case MergeRule::LastNonNull:
            return true;
        case MergeRule::Replace:
            return false;
    }
    return false;
}

const char* primName(rune::codec::PrimType t) {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: return "Int32";   case P::Int64: return "Int64";
        case P::UInt16: return "UInt16"; case P::UInt32: return "UInt32";
        case P::UInt64: return "UInt64"; case P::Float64: return "Float64";
        case P::Bool: return "Bool";     case P::Enum: return "Enum";
        case P::FixedStr: return "FixedStr";
        case P::String: return "String"; case P::Bytes: return "Bytes";
    }
    return "an unknown type";
}

// The output type an aggregate rule produces from a source field of type `src`
// (Global Constraints: fixed by rule, never cast). nullopt = not aggregatable.
// Callers have already rejected var-len sources and non-numeric Min/Max.
std::optional<rune::codec::PrimType> aggregateOutputType(MergeRule r, rune::codec::PrimType src) {
    using P = rune::codec::PrimType;
    switch (r) {
        case MergeRule::Count:
        case MergeRule::CountAll: return P::Int64;
        case MergeRule::Avg:      return isNumeric(src) ? std::optional<P>(P::Float64) : std::nullopt;
        case MergeRule::Sum:
            switch (src) {
                case P::Int32: case P::Int64:                    return P::Int64;
                case P::UInt16: case P::UInt32: case P::UInt64:  return P::UInt64;
                case P::Float64:                                 return P::Float64;
                default:                                         return std::nullopt;
            }
        case MergeRule::Min:
        case MergeRule::Max:      return isNumeric(src) ? std::optional<P>(src) : std::nullopt;
        case MergeRule::Last:
        case MergeRule::LastNonNull: return src;
        case MergeRule::Replace:  return std::nullopt;
    }
    return std::nullopt;
}
```

Delete the `isPhase2bRule` early-return block at the top of the resolve loop (lines 141–145):

```cpp
        if (isPhase2bRule(af.rule)) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' uses Min/Max/Last, which are not supported until Phase 2b"});
        }
```

In the aggregate-view column check (line 313), replace the string literal

```cpp
                    "' in an aggregate view must be an aggregate (Count, Count(*), Sum, Avg); "
```

with

```cpp
                    "' in an aggregate view must be an aggregate (Count, Count(*), Sum, Avg, "
                    "Min, Max, Last, LastNonNull); "
```

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build/root -j14 --target rune_view_tests rune_create_view_tests && ctest --test-dir build/root -R 'PlanCompiler|CreateView' -j14 --output-on-failure`
Expected: every `PlanCompiler*` and `CreateView*` test passes, including the Phase 1/2a tests and `NonGroupReplaceColumnInAggregateViewIsRejected`, which still finds "must be an aggregate".

- [ ] **Step 5: Run the affected tests and commit.**

Run: `ctest --test-dir build/root -R 'PlanCompiler|ViewPlan|CreateView' -j14 --output-on-failure` → 0 failures. (Affected area only, F13; the full `*Regression*` suite is a controller checkpoint.)

```bash
git add broker/libs/view/src/PlanCompiler.cpp broker/libs/view/tests/view/PlanCompilerTest.cpp
git commit -m "feat(view): compile Min/Max (numeric) and Last/LastNonNull (fixed-width) with source output types" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `encodeSlotValue` / `decodeSlotValue` — one fixed-width value codec

`Last`/`LastNonNull` store the source value at the source field's width (a `string(N)` stores N bytes) and copy it into the output slot on emit. The encoding is exactly the one `writeSlot`/`readSlot` already use, so this task factors it out once and gives both callers the same function. `writeSlot`/`readSlot` behaviour does not change.

**Files:**
- Modify: `broker/libs/view/include/rune/view/ViewRow.h:33-43`
- Modify: `broker/libs/view/src/ViewRow.cpp:75-178`
- Test: `broker/libs/view/tests/view/ViewRowTest.cpp`

**Interfaces:**
- Produces (in `rune::view`):
  ```cpp
  void encodeSlotValue(uint8_t* dst, rune::codec::PrimType type, uint16_t width,
                       const rune::codec::FieldValue& v) noexcept;
  rune::codec::FieldValue decodeSlotValue(const uint8_t* src, rune::codec::PrimType type,
                                          uint16_t width) noexcept;   // present == true
  ```

- [ ] **Step 1: Write the failing tests.** Append to `broker/libs/view/tests/view/ViewRowTest.cpp`, and add `#include <climits>`, `#include <cstring>` and `#include <string>` next to its existing includes:

```cpp
TEST(ViewRow, SlotValueCodecRoundTripsEveryFixedType) {
    using P = rune::codec::PrimType;
    uint8_t buf[8];
    auto rt = [&](P t, uint16_t w, const rune::codec::FieldValue& v) {
        std::memset(buf, 0xAB, sizeof buf);
        rune::view::encodeSlotValue(buf, t, w, v);
        return rune::view::decodeSlotValue(buf, t, w);
    };
    rune::codec::FieldValue v{};
    v.i = -5;          EXPECT_EQ(rt(P::Int32, 4, v).i, -5);
    v.i = INT64_MIN;   EXPECT_EQ(rt(P::Int64, 8, v).i, INT64_MIN);
    v.u = 65535;       EXPECT_EQ(rt(P::UInt16, 2, v).u, 65535u);
    v.u = 4000000000u; EXPECT_EQ(rt(P::UInt32, 4, v).u, 4000000000u);
    v.u = 7;           EXPECT_EQ(rt(P::Enum, 4, v).u, 7u);
    v.u = UINT64_MAX;  EXPECT_EQ(rt(P::UInt64, 8, v).u, UINT64_MAX);
    v.f = -2.5;        EXPECT_DOUBLE_EQ(rt(P::Float64, 8, v).f, -2.5);
    v.b = true;        EXPECT_TRUE(rt(P::Bool, 1, v).b);
    v.bytes = "AB";
    const auto s = rt(P::FixedStr, 4, v);
    EXPECT_EQ(std::string(s.bytes), std::string("AB\0\0", 4)) << "short FixedStr is zero-padded to width";
    EXPECT_TRUE(s.present);
    EXPECT_EQ(s.type, P::FixedStr);
}

// F6: golden bytes. Comparing against encodeSlotValue would be a tautology,
// because writeSlot now calls it.
TEST(ViewRow, WriteSlotWritesGoldenBytesAtTheSlotOffset) {
    using P = rune::codec::PrimType;
    const auto d = rune::codec::DescriptorBuilder("t", 1).add("a", P::Int32).addFixedStr("s", 3).build();
    const auto L = rune::view::buildRowLayout(d);
    std::vector<uint8_t> row(L.rowLen, 0xEE);   // a stale-byte canary: padding must really be written
    rune::codec::FieldValue a{}; a.type = P::Int32; a.present = true; a.i = -7;
    rune::codec::FieldValue s{}; s.type = P::FixedStr; s.present = true; s.bytes = "xy";
    rune::view::writeSlot(row.data(), L, 0, a);
    rune::view::writeSlot(row.data(), L, 1, s);
    const uint8_t goldenA[] = {0xF9, 0xFF, 0xFF, 0xFF};   // Int32 -7, little-endian
    const uint8_t goldenS[] = {0x78, 0x79, 0x00};         // FixedStr "xy" at width 3, zero-padded
    EXPECT_EQ(std::memcmp(row.data() + L.slotOffset[0], goldenA, 4), 0);
    EXPECT_EQ(std::memcmp(row.data() + L.slotOffset[1], goldenS, 3), 0);
    EXPECT_EQ(rune::view::readSlot(row.data(), L, 0).i, -7);
}
```

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build/root -j14 --target rune_view_tests`
Expected: compile error `no member named 'encodeSlotValue' in namespace 'rune::view'`.

- [ ] **Step 3: Implement.** In `broker/libs/view/include/rune/view/ViewRow.h`, insert before the `writeSlot` declaration (line 33):

```cpp
// The fixed-width byte representation of one present value, exactly as a row
// slot stores it (little-endian; FixedStr copied then zero-padded to `width`).
// writeSlot/readSlot use these; so does the aggregate fold for Last/LastNonNull
// contributor values (stored at the source field's own width).
void encodeSlotValue(uint8_t* dst, rune::codec::PrimType type, uint16_t width,
                     const rune::codec::FieldValue& v) noexcept;
// Inverse of encodeSlotValue; the result has present == true. A FixedStr's
// `bytes` views `src` (valid while `src` is).
rune::codec::FieldValue decodeSlotValue(const uint8_t* src, rune::codec::PrimType type,
                                        uint16_t width) noexcept;
```

In `broker/libs/view/src/ViewRow.cpp`, replace everything from `void writeSlot(` (line 75) through the end of `readSlot` (line 178, before `} // namespace rune::view`) with:

```cpp
void encodeSlotValue(uint8_t* dst, PrimType type, uint16_t width, const FieldValue& v) noexcept {
    switch (type) {
        case PrimType::Int32:
            writeU(dst, 0, static_cast<uint32_t>(static_cast<int32_t>(v.i)), 4);
            break;
        case PrimType::Int64:
            writeU(dst, 0, static_cast<uint64_t>(v.i), 8);
            break;
        case PrimType::UInt16:
            writeU(dst, 0, v.u, 2);
            break;
        case PrimType::UInt32:
        case PrimType::Enum:
            writeU(dst, 0, v.u, 4);
            break;
        case PrimType::UInt64:
            writeU(dst, 0, v.u, 8);
            break;
        case PrimType::Float64: {
            uint64_t raw = 0;
            std::memcpy(&raw, &v.f, 8);
            writeU(dst, 0, raw, 8);
            break;
        }
        case PrimType::Bool:
            dst[0] = v.b ? 1 : 0;
            break;
        case PrimType::FixedStr: {
            const size_t n = std::min(static_cast<size_t>(width), v.bytes.size());
            std::memcpy(dst, v.bytes.data(), n);
            if (n < width) {
                std::memset(dst + n, 0, width - n);
            }
            break;
        }
        case PrimType::String:
        case PrimType::Bytes:
            // var-len: rejected by buildRowLayout / the compiler; unreachable.
            break;
    }
}

FieldValue decodeSlotValue(const uint8_t* src, PrimType type, uint16_t width) noexcept {
    FieldValue fv{};
    fv.type    = type;
    fv.present = true;
    switch (type) {
        case PrimType::Int32:
            fv.i = static_cast<int32_t>(static_cast<uint32_t>(readU(src, 0, 4)));
            break;
        case PrimType::Int64:
            fv.i = static_cast<int64_t>(readU(src, 0, 8));
            break;
        case PrimType::UInt16:
            fv.u = readU(src, 0, 2);
            break;
        case PrimType::UInt32:
        case PrimType::Enum:
            fv.u = readU(src, 0, 4);
            break;
        case PrimType::UInt64:
            fv.u = readU(src, 0, 8);
            break;
        case PrimType::Float64: {
            uint64_t raw = readU(src, 0, 8);
            std::memcpy(&fv.f, &raw, 8);
            break;
        }
        case PrimType::Bool:
            fv.b = src[0] != 0;
            fv.i = src[0];
            break;
        case PrimType::FixedStr:
            fv.bytes = std::string_view(reinterpret_cast<const char*>(src), width);
            break;
        case PrimType::String:
        case PrimType::Bytes:
            // var-len: rejected by buildRowLayout / the compiler; unreachable.
            break;
    }
    return fv;
}

void writeSlot(uint8_t* row, const RowLayout& layout, uint16_t slot, FieldValue v) noexcept {
    if (!v.present) {
        setNull(row, layout, slot);
        return;
    }
    clearNull(row, layout, slot);
    encodeSlotValue(row + layout.slotOffset[slot], layout.slotType[slot], layout.slotWidth[slot], v);
}

FieldValue readSlot(const uint8_t* row, const RowLayout& layout, uint16_t slot) noexcept {
    if (isNull(row, layout, slot)) {
        FieldValue fv{};
        fv.type    = layout.slotType[slot];
        fv.present = false;
        return fv;
    }
    return decodeSlotValue(row + layout.slotOffset[slot], layout.slotType[slot],
                           layout.slotWidth[slot]);
}
```

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'ViewRow|ViewFold|AggFold|ViewCheckpoint' --output-on-failure`
Expected: all pass. The `writeSlot`/`readSlot` behaviour is unchanged.

- [ ] **Step 5: Run the affected tests and commit.**

Step 4's run is the affected-area gate (F13); re-run it if anything changed since: `ctest --test-dir build/root -R 'ViewRow|ViewFold|AggFold|ViewCheckpoint' -j14 --output-on-failure` → 0 failures.

```bash
git add broker/libs/view/include/rune/view/ViewRow.h broker/libs/view/src/ViewRow.cpp broker/libs/view/tests/view/ViewRowTest.cpp
git commit -m "refactor(view): encodeSlotValue/decodeSlotValue — one fixed-width slot codec for rows and Last values" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `AggState` — the linked layout and the exact dirty bound

**Files:**
- Modify (full replacement): `broker/libs/view/include/rune/view/AggState.h`
- Modify (full replacement): `broker/libs/view/src/AggState.cpp`
- Test: `broker/libs/view/tests/view/AggStateTest.cpp`

**Interfaces:**
- Consumes: Task 2's compiler acceptance; `rune::codec::fieldWidth`.
- Produces (exact, in `rune::view`):
  ```cpp
  inline constexpr uint64_t kNoContributor = UINT64_MAX;
  inline constexpr uint16_t kNoCache = 0xFFFF;
  inline constexpr uint32_t kCacheEntryBytes = 9;               // [value 8B][state u8]
  enum class CacheState : uint8_t { Empty = 0, Valid = 1, RescanPending = 2 };
  bool needsContributorList(MergeRule r) noexcept;              // Min|Max|Last|LastNonNull
  struct AggField { uint16_t outSlot; MergeRule rule; uint16_t srcFieldId; AccKind kind;
                    uint16_t input; uint16_t cache; rune::codec::PrimType srcType; };
  struct AggLayout { /* 2a members unchanged */ bool linked; uint32_t contribPrevOff, contribNextOff;
                     std::vector<uint32_t> inputOff; std::vector<uint16_t> inputWidth;
                     uint16_t numCached; uint32_t groupHeadOff, groupCacheOff; };
  struct AggDirtyBound { uint32_t contributorRows; uint32_t groupRows; uint32_t viewRows; };
  AggDirtyBound aggDirtyBoundPerRecord(const AggLayout& a) noexcept;   // linked {4,2,2}, 2a {1,2,2}
  ```

Row layouts (fixed width, little-endian, memcpy-encoded). Braces `{…}` mark parts that exist **only when `linked`**:
- contributor row: `[groupKey u64]{[prev u64][next u64]}[null bitmap ceil(numInputs/8)][values at inputOff[i], inputWidth[i] bytes]`. The width is 8 for Count/Sum/Avg/Min/Max inputs (the 2a widened int64/uint64/double) and `fieldWidth(source)` for Last/LastNonNull inputs (the `encodeSlotValue` bytes).
- group row: `[contributors u64][fields × 16B accumulator][fields × u64 non-null count]{[head u64][numCached × ([value 8B][state u8])]}`.

**Dirty-bound derivation.** These are the rows one source record can dirty, counted as entries in the per-pass checkpoint delta. Each touched row is one entry: an upsert, or a delete when it is erased. The worst case is an update that **moves** contributor `c` from group `g1` to `g2` in a linked view:
- contributors: `c` itself, its old predecessor `prev(c)` and successor `next(c)` in `g1`'s list (unlink), and `g2`'s old head (its `prev` becomes `c`). These are 4 distinct rows. They are distinct because `g1 ≠ g2`. For an in-place update in the same group the rows are `c`, `prev`, `next` and the old head, and the old head coincides with `next(c)` or with an existing neighbour, so there are still at most 4.
- groups: `g1` (retract + head fix-up) and `g2` (add + head) give 2. When `g1` empties, its upsert is replaced by a delete entry, so it is still 1.
- view rows: `g1` (re-emit or delete) and `g2` give 2.
- A rescan only *reads* contributors through `get()`, which never marks a row dirty, and it rewrites `g1`'s cache inside the already-dirty group row. Tombstones and missing-group records touch at most `c` (delete), `prev`, `next`, `g1` and `g1`'s view row, which is 5 or fewer.
- Linked views therefore have **4 + 2 + 2 = 8**. 2a views keep **1 + 2 + 2 = 5**, the unchanged 2a bound.

- [ ] **Step 1: Write the failing tests.** Append to `broker/libs/view/tests/view/AggStateTest.cpp`:

```cpp
namespace {
// order v2: orderId key | userId | amount Int64 | price Float64 | sku FixedStr(4) | qty UInt32 | bonus Int32 (v2)
rune::codec::Descriptor statsSrc() {
    return rune::codec::DescriptorBuilder("order", 2)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64, 1)
        .add("amount", P::Int64, 1)
        .add("price", P::Float64, 1)
        .addFixedStr("sku", 4, 1)
        .add("qty", P::UInt32, 1)
        .add("bonus", P::Int32, 2)
        .build();
}
rune::view::CompileResult compileStats() {
    rune::view::ViewPlan v;
    v.viewName = "user_stats"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"maxAmt", P::Int64, 0, "amount", R::Max},
                {"minAmt", P::Int64, 0, "amount", R::Min},
                {"lastAmt", P::Int64, 0, "amount", R::Last},
                {"maxPrice", P::Float64, 0, "price", R::Max},
                {"minQty", P::UInt32, 0, "qty", R::Min},
                {"lastSku", P::FixedStr, 0, "sku", R::Last},
                {"lastBonus", P::Int32, 0, "bonus", R::Last},
                {"lastNnBonus", P::Int32, 0, "bonus", R::LastNonNull},
                {"total", P::Int64, 0, "amount", R::Sum}};
    static const auto src = statsSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    return rune::view::compileView(v, srcs, 9).value();
}
} // namespace

// REGRESSION (Phase 2b Global Constraint): a view without Min/Max/Last/LastNonNull
// keeps its 2a layout byte-for-byte -- no links, no head, no caches, 8-byte inputs.
TEST(AggStateRegression, TwoAOnlyLayoutIsUnchangedRegression) {
    const auto a = rune::view::buildAggLayout(compileOrders().plan, orderSrc()).value();
    EXPECT_FALSE(a.linked);
    EXPECT_EQ(a.contribNullOff, 8u);
    EXPECT_EQ(a.contribValOff, 9u);
    EXPECT_EQ(a.inputOff, (std::vector<uint32_t>{9, 17, 25, 33}));
    EXPECT_EQ(a.inputWidth, (std::vector<uint16_t>{8, 8, 8, 8}));
    EXPECT_EQ(a.contributorRow.rowLen, 41u);
    EXPECT_EQ(a.numCached, 0u);
    EXPECT_EQ(a.groupRow.rowLen, 8u + 5u * 16u + 5u * 8u);
    for (const auto& f : a.fields) EXPECT_EQ(f.cache, rune::view::kNoCache);
    const auto b = rune::view::aggDirtyBoundPerRecord(a);
    EXPECT_EQ(b.contributorRows, 1u);
    EXPECT_EQ(b.groupRows, 2u);
    EXPECT_EQ(b.viewRows, 2u);
}

TEST(AggState, RetractableLayoutAddsLinksHeadCachesAndPerInputWidths) {
    const auto a = rune::view::buildAggLayout(compileStats().plan, statsSrc()).value();
    EXPECT_TRUE(a.linked);
    ASSERT_EQ(a.fields.size(), 10u);
    EXPECT_EQ(a.numInputs, 9u);
    EXPECT_EQ(a.contribPrevOff, 8u);
    EXPECT_EQ(a.contribNextOff, 16u);
    EXPECT_EQ(a.contribNullOff, 24u);
    EXPECT_EQ(a.contribValOff, 26u);                       // 9 inputs -> 2 bitmap bytes
    EXPECT_EQ(a.inputOff, (std::vector<uint32_t>{26, 34, 42, 50, 58, 66, 70, 74, 78}));
    EXPECT_EQ(a.inputWidth, (std::vector<uint16_t>{8, 8, 8, 8, 8, 4, 4, 4, 8}));
    EXPECT_EQ(a.contributorRow.rowLen, 86u);
    // Caches: maxAmt, minAmt, maxPrice, minQty (Min/Max only, in field order).
    EXPECT_EQ(a.numCached, 4u);
    EXPECT_EQ(a.fields[1].cache, 0u);  EXPECT_EQ(a.fields[1].kind, rune::view::AccKind::Signed);
    EXPECT_EQ(a.fields[2].cache, 1u);
    EXPECT_EQ(a.fields[3].cache, rune::view::kNoCache);   // Last
    EXPECT_EQ(a.fields[3].kind, rune::view::AccKind::None);
    EXPECT_EQ(a.fields[4].cache, 2u);  EXPECT_EQ(a.fields[4].kind, rune::view::AccKind::Float);
    EXPECT_EQ(a.fields[5].cache, 3u);  EXPECT_EQ(a.fields[5].kind, rune::view::AccKind::Unsigned);
    EXPECT_EQ(a.fields[6].srcType, P::FixedStr);
    EXPECT_EQ(a.fields[8].rule, R::LastNonNull);
    EXPECT_EQ(a.fields[9].kind, rune::view::AccKind::Signed);   // Sum(amount)
    EXPECT_EQ(a.groupNonNullOff, 8u + 10u * 16u);
    EXPECT_EQ(a.groupHeadOff, 248u);
    EXPECT_EQ(a.groupCacheOff, 256u);
    EXPECT_EQ(a.groupRow.rowLen, 256u + 4u * rune::view::kCacheEntryBytes);
    const auto b = rune::view::aggDirtyBoundPerRecord(a);
    EXPECT_EQ(b.contributorRows, 4u);
    EXPECT_EQ(b.groupRows, 2u);
    EXPECT_EQ(b.viewRows, 2u);
}

TEST(AggState, SentinelsAndCacheStatesAreStable) {
    EXPECT_EQ(rune::view::kNoContributor, UINT64_MAX);
    EXPECT_EQ(rune::view::kCacheEntryBytes, 9u);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::CacheState::Empty), 0u);   // a zero-filled row is Empty
    EXPECT_EQ(static_cast<uint8_t>(rune::view::CacheState::Valid), 1u);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::CacheState::RescanPending), 2u);
    EXPECT_TRUE(rune::view::needsContributorList(R::LastNonNull));
    EXPECT_FALSE(rune::view::needsContributorList(R::Sum));
}
```

Add `#include <vector>` to the file's includes.

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build/root -j14 --target rune_view_tests`
Expected: compile error `no member named 'linked'` / `'kNoCache'`.

- [ ] **Step 3: Implement.** Replace `broker/libs/view/include/rune/view/AggState.h` with:

```cpp
// broker/libs/view/include/rune/view/AggState.h
//
// Aggregate views with TABLE semantics: a contributor is the source record's
// 8-byte WIRE key, its group the view key read from the record VALUE.
//   Phase 2a — invertible: Count, Count(*), Sum, Avg.
//   Phase 2b — retractable: Min, Max, Last, LastNonNull.
// Per (view, partition) the fold keeps two fixed-width auxiliary ViewTables
// next to the output rows ({...} = only in a LINKED view, i.e. one using any
// Phase 2b rule; a 2a-only view keeps its 2a layout byte-for-byte):
//   contributors : contributorKey -> [groupKey u64]{[prev u64][next u64]}[null bitmap][values]
//   groups       : groupKey -> [contributors u64][accumulators][non-null counts]
//                              {[head u64][per Min/Max: value 8B, state u8]}
// prev/next/head form a doubly linked MRU list of the group's contributors,
// keys as links (kNoContributor ends it): every write of a contributor moves it
// to the head, so Last = head, LastNonNull = first non-null from the head.
// Both tables are checkpointed with the output rows in one v2 record (§9.2).
#pragma once
#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>

namespace rune::view {

// True iff any output column is an aggregate (anything but Replace).
bool isAggregateView(const CompiledViewPlan& plan) noexcept;

// True for the Phase 2b rules that need the per-group contributor list.
bool needsContributorList(MergeRule r) noexcept;

// Precision: integer Sum/Avg accumulate in int128 and are exact under
// retraction. Float Sum/Avg under retraction are approximate (catastrophic
// cancellation is possible when magnitudes are mixed); the accumulator resets
// to 0.0 when the last non-null input leaves, and NaN/+-Inf inputs are treated
// as NULL so accumulators stay finite.
//
// That NaN/+-Inf -> NULL rule applies to the inputs with a numeric AccKind:
// Sum/Avg/Min/Max over a Float input (AccKind::Float). Count(col), Last and
// LastNonNull have AccKind::None: a NaN/+-Inf value is NON-null for them
// (Count(col) counts it; Last/LastNonNull return it verbatim).

// The numeric domain a Sum/Avg accumulates in, and a Min/Max compares in.
enum class AccKind : uint8_t { None, Signed, Unsigned, Float };

// End-of-list / no-contributor link value. A contributor whose key equals it
// cannot be linked and is skipped (skippedMalformed) in a linked view.
inline constexpr uint64_t kNoContributor = UINT64_MAX;
// AggField::cache for a field without a Min/Max cache.
inline constexpr uint16_t kNoCache = 0xFFFF;
// One Min/Max cache entry in a group row: [value 8B][state u8].
inline constexpr uint32_t kCacheEntryBytes = 9;

// A Min/Max cache's state machine (the state byte). Empty: no non-null value
// (also what a zero-filled new group row reads as). Valid: value is the
// extreme. RescanPending: the extreme was retracted and value still holds that
// RETRACTED extreme (an upper/lower bound on the rest); resolved -- by an add
// that is at least as extreme, or by one list walk at emit -- before the record
// that set it finishes, so it is never checkpointed.
enum class CacheState : uint8_t { Empty = 0, Valid = 1, RescanPending = 2 };

struct AggField {
    uint16_t  outSlot;     // output column written by this aggregate
    MergeRule rule;        // Count | CountAll | Sum | Avg | Min | Max | Last | LastNonNull
    uint16_t  srcFieldId;  // kNoSourceField for CountAll
    AccKind   kind;        // Sum/Avg/Min/Max: from the source field's type; None otherwise
    uint16_t  input;       // contributor value index; kNoSourceField for CountAll
    uint16_t  cache;       // Min/Max: group cache index; kNoCache otherwise
    rune::codec::PrimType srcType;  // source field type (CountAll: Int64)
};

struct AggLayout {
    uint16_t              keyOutSlot    = 0;   // the group-by output column
    uint16_t              keySrcFieldId = 0;   // group-by field read from the value
    rune::codec::PrimType keyType       = rune::codec::PrimType::Int64;
    std::vector<AggField> fields;              // one per non-key output column
    uint16_t              numInputs     = 0;   // fields with a source input
    RowLayout             contributorRow;      // only rowLen is meaningful
    uint32_t              contribNullOff = 8;
    uint32_t              contribValOff  = 8;  // == inputOff[0] when numInputs > 0
    RowLayout             groupRow;            // only rowLen is meaningful
    uint32_t              groupAccOff     = 8;
    uint32_t              groupNonNullOff = 8;
    // --- Phase 2b ---
    bool                  linked          = false;  // any Min/Max/Last/LastNonNull
    uint32_t              contribPrevOff  = 0;      // linked only
    uint32_t              contribNextOff  = 0;      // linked only
    std::vector<uint32_t> inputOff;                 // per input: contributor-row byte offset
    std::vector<uint16_t> inputWidth;               // per input: 8, or the source width for Last/LastNonNull
    uint16_t              numCached       = 0;      // Min/Max fields
    uint32_t              groupHeadOff    = 0;      // linked only
    uint32_t              groupCacheOff   = 0;      // linked only
};

// Builds the layout for an aggregate view. `evolvingSrc` is the source's
// evolving descriptor; a field's type/width never changes for a given tag (a
// retype is a new tag), so it decides each accumulator kind and Last width.
// nullopt if a referenced source field is not in `evolvingSrc`.
std::optional<AggLayout> buildAggLayout(const CompiledViewPlan& plan,
                                        const rune::codec::Descriptor& evolvingSrc);

// Worst-case rows ONE source record can dirty in each table (= checkpoint
// delta entries). Linked: a group move dirties the contributor, both of its
// old neighbours and the new group's old head (4), both groups (2) and both
// view rows (2). 2a: the contributor, both groups, both view rows (1,2,2).
// A rescan only reads contributors (get() never dirties). Derivation: Phase
// 2b plan Task 4; pinned by AggRetractableRegression.WorstCaseMove... .
struct AggDirtyBound {
    uint32_t contributorRows;
    uint32_t groupRows;
    uint32_t viewRows;
};
AggDirtyBound aggDirtyBoundPerRecord(const AggLayout& a) noexcept;

// Table ids inside a multi-table checkpoint record (ViewCheckpoint v2).
inline constexpr uint8_t kAggTableView         = 0;
inline constexpr uint8_t kAggTableContributors = 1;
inline constexpr uint8_t kAggTableGroups       = 2;

// Fixed-width little-endian byte helpers (memcpy: alignment-free, no UB).
inline uint64_t loadU64(const uint8_t* p) noexcept { uint64_t v; std::memcpy(&v, p, 8); return v; }
inline void     storeU64(uint8_t* p, uint64_t v) noexcept { std::memcpy(p, &v, 8); }
inline __int128 loadI128(const uint8_t* p) noexcept { __int128 v; std::memcpy(&v, p, 16); return v; }
inline void     storeI128(uint8_t* p, __int128 v) noexcept { std::memcpy(p, &v, 16); }
inline unsigned __int128 loadU128(const uint8_t* p) noexcept { unsigned __int128 v; std::memcpy(&v, p, 16); return v; }
inline void     storeU128(uint8_t* p, unsigned __int128 v) noexcept { std::memcpy(p, &v, 16); }
inline double   loadF64(const uint8_t* p) noexcept { double v; std::memcpy(&v, p, 8); return v; }
inline void     storeF64(uint8_t* p, double v) noexcept { std::memcpy(p, &v, 8); }

} // namespace rune::view
```

Replace `broker/libs/view/src/AggState.cpp` with:

```cpp
// broker/libs/view/src/AggState.cpp — see AggState.h.
#include <rune/view/AggState.h>

#include <algorithm>

namespace rune::view {

namespace {

const rune::codec::FieldDescriptor* srcField(const rune::codec::Descriptor& d, uint16_t id) {
    for (const auto& f : d.fields) {
        if (f.id == id) return &f;
    }
    return nullptr;
}

AccKind accKindOf(rune::codec::PrimType t) noexcept {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: case P::Int64:                   return AccKind::Signed;
        case P::UInt16: case P::UInt32: case P::UInt64: return AccKind::Unsigned;
        case P::Float64:                                return AccKind::Float;
        default:                                        return AccKind::None;
    }
}

bool isLastRule(MergeRule r) noexcept { return r == MergeRule::Last || r == MergeRule::LastNonNull; }
bool isExtremeRule(MergeRule r) noexcept { return r == MergeRule::Min || r == MergeRule::Max; }

} // namespace

bool isAggregateView(const CompiledViewPlan& plan) noexcept {
    return std::any_of(plan.output.begin(), plan.output.end(),
                       [](const CompiledField& f) { return f.rule != MergeRule::Replace; });
}

bool needsContributorList(MergeRule r) noexcept { return isExtremeRule(r) || isLastRule(r); }

std::optional<AggLayout> buildAggLayout(const CompiledViewPlan& plan,
                                        const rune::codec::Descriptor& evolvingSrc) {
    AggLayout a;
    for (const auto& f : plan.output) {
        if (f.rule == MergeRule::Replace) {   // compiler: exactly the group-by column
            a.keyOutSlot    = f.outSlot;
            a.keySrcFieldId = f.srcFieldId;
            a.keyType       = f.type;
            continue;
        }
        AggField af{};
        af.outSlot    = f.outSlot;
        af.rule       = f.rule;
        af.srcFieldId = f.srcFieldId;
        af.kind       = AccKind::None;
        af.input      = kNoSourceField;
        af.cache      = kNoCache;
        af.srcType    = f.type;
        if (f.rule != MergeRule::CountAll) {
            const rune::codec::FieldDescriptor* sd = srcField(evolvingSrc, f.srcFieldId);
            if (sd == nullptr) return std::nullopt;
            af.input   = a.numInputs++;
            af.srcType = sd->type;
            a.inputWidth.push_back(isLastRule(f.rule)
                                       ? static_cast<uint16_t>(rune::codec::fieldWidth(*sd))
                                       : uint16_t{8});
            if (f.rule == MergeRule::Sum || f.rule == MergeRule::Avg || isExtremeRule(f.rule))
                af.kind = accKindOf(sd->type);
            if (isExtremeRule(f.rule)) af.cache = a.numCached++;
            if (needsContributorList(f.rule)) a.linked = true;
        }
        a.fields.push_back(af);
    }
    const uint32_t nFields = static_cast<uint32_t>(a.fields.size());
    a.contribPrevOff = a.linked ? 8u : 0u;
    a.contribNextOff = a.linked ? 16u : 0u;
    a.contribNullOff = a.linked ? 24u : 8u;
    a.contribValOff  = a.contribNullOff + (static_cast<uint32_t>(a.numInputs) + 7) / 8;
    uint32_t off = a.contribValOff;
    a.inputOff.reserve(a.inputWidth.size());
    for (const uint16_t w : a.inputWidth) {
        a.inputOff.push_back(off);
        off += w;
    }
    a.contributorRow.rowLen = off;
    a.groupAccOff     = 8;
    a.groupNonNullOff = 8 + 16u * nFields;
    const uint32_t nnEnd = a.groupNonNullOff + 8u * nFields;
    if (a.linked) {
        a.groupHeadOff      = nnEnd;
        a.groupCacheOff     = nnEnd + 8;
        a.groupRow.rowLen   = a.groupCacheOff + kCacheEntryBytes * a.numCached;
    } else {
        a.groupRow.rowLen   = nnEnd;
    }
    return a;
}

AggDirtyBound aggDirtyBoundPerRecord(const AggLayout& a) noexcept {
    return AggDirtyBound{a.linked ? 4u : 1u, 2u, 2u};
}

} // namespace rune::view
```

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'AggState|AggFold' --output-on-failure`
Expected: all pass. `AggFold*` (2a) is unchanged because a 2a layout has `inputOff[i] == contribValOff + 8*i`.

- [ ] **Step 5: Run the affected tests and commit.**

Run: `ctest --test-dir build/root -R 'AggState|AggFold|ViewRow|ViewFold|ViewCheckpoint' -j14 --output-on-failure` → 0 failures. (The controller's checkpoint after this task runs the existing suites only; remember the bisect note in *Execution context*: do not create Min/Max/Last views on commits between Task 2 and Task 5.)

```bash
git add broker/libs/view/include/rune/view/AggState.h broker/libs/view/src/AggState.cpp broker/libs/view/tests/view/AggStateTest.cpp
git commit -m "feat(view): linked aggregate layout — MRU links, group head, Min/Max caches, per-input widths, exact dirty bound" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The retractable fold

**Files:**
- Modify: `broker/libs/view/include/rune/view/ViewFold.h:65-68` (add `aggRescans`)
- Modify (full replacement): `broker/libs/view/src/AggFold.cpp`
- Create: `broker/libs/view/tests/view/AggRetractableTest.cpp`
- Modify: `broker/libs/view/CMakeLists.txt:21-26` (add the test file after `tests/view/AggFoldTest.cpp`, line 25)

**Interfaces:**
- Consumes: `AggLayout`/`AggField`/`CacheState`/`kNoContributor`/`kNoCache`/`kCacheEntryBytes` (Task 4); `encodeSlotValue`/`decodeSlotValue` (Task 3).
- Produces: `FoldStats::aggRescans` (counts O(group) Min/Max list walks). `foldAggregateBatch` keeps its signature; for a linked layout it maintains links, heads and caches. For a 2a layout its behaviour, emit order and counters are identical to 2a.

**Maintenance contract** (spec §A Maintenance):
- **Add** of contributor `c` to group `g`: `applyContribution(+1)` updates counts/accumulators. For each Min/Max field with a non-null value: in state Empty, take the value; in Valid, replace the cache if the value is strictly better; in RescanPending, take the value and become Valid if it is at least as extreme as the retracted value still cached. Then `linkAtHead`.
- **Retract**: `applyContribution(-1)`. Where `c`'s old value equals the Valid cache, the state becomes RescanPending; where the non-null count reaches 0, the state becomes Empty. Then `unlink`.
- **Emit** first resolves any RescanPending field with one list walk (`rescan`, bounded by the group's contributor count), then writes Min/Max from the cache, `Last` from the head and `LastNonNull` from the first non-null walking from the head.
- An in-place update (same group) **defers** the retract-side emit until after the add, so a new value that is at least as extreme skips the rescan. A 2a view always emits on retract, exactly as in 2a.

**Intentional duplication (F7).** `AggRetractableTest.cpp` re-declares `le16/le32/le64` and `Harness::apply/key/put/col` verbatim from `AggFoldTest.cpp:56-112`. This is deliberate: the 2a `AggFold*` suite stays byte-untouched as the 2a-invariance guard, so the new tests do not share a header with it. Reviewers should not flag it.

- [ ] **Step 1: Write the failing tests.** Create `broker/libs/view/tests/view/AggRetractableTest.cpp`:

```cpp
// broker/libs/view/tests/view/AggRetractableTest.cpp — Phase 2b retractable
// Min / Max / Last / LastNonNull (per-group MRU contributor list + caches).
#include <gtest/gtest.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/AggFold.h>
#include <rune/view/AggState.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

using P = rune::codec::PrimType;
using R = rune::view::MergeRule;

namespace {

// order v2: orderId Int64 key | userId Int64 | amount Int64 | price Float64 |
//           sku FixedStr(4) | qty UInt32 | bonus Int32 (sinceVersion 2).
// A v1 record has no bonus: it reads as ABSENT (NULL).
rune::codec::Descriptor orderSource() {
    return rune::codec::DescriptorBuilder("order", 2)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64, 1)
        .add("amount", P::Int64, 1)
        .add("price", P::Float64, 1)
        .addFixedStr("sku", 4, 1)
        .add("qty", P::UInt32, 1)
        .add("bonus", P::Int32, 2)
        .build();
}

// Output slots (key first, then declaration order).
enum Slot : uint16_t { kUser = 0, kN, kMaxAmt, kMinAmt, kLastAmt, kMaxPrice, kMinQty,
                       kLastSku, kLastBonus, kLastNnBonus, kTotal };

rune::view::ViewPlan userStats() {
    rune::view::ViewPlan v;
    v.viewName = "user_stats"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"maxAmt", P::Int64, 0, "amount", R::Max},
                {"minAmt", P::Int64, 0, "amount", R::Min},
                {"lastAmt", P::Int64, 0, "amount", R::Last},
                {"maxPrice", P::Float64, 0, "price", R::Max},
                {"minQty", P::UInt32, 0, "qty", R::Min},
                {"lastSku", P::FixedStr, 0, "sku", R::Last},
                {"lastBonus", P::Int32, 0, "bonus", R::Last},
                {"lastNnBonus", P::Int32, 0, "bonus", R::LastNonNull},
                {"total", P::Int64, 0, "amount", R::Sum}};
    return v;
}

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le32(std::vector<uint8_t>& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }

struct Ord {
    int64_t id;
    int64_t user;
    int64_t amount;
    double price = 1.0;
    const char* sku = "AAAA";
    uint32_t qty = 1;
    std::optional<int32_t> bonus;   // nullopt -> a v1 record (bonus NULL)
};

std::vector<uint8_t> encode(const Ord& o) {
    std::vector<uint8_t> b;
    const bool v2 = o.bonus.has_value();
    le16(b, v2 ? 44 : 40); le16(b, v2 ? 2 : 1);
    le64(b, uint64_t(o.id)); le64(b, uint64_t(o.user)); le64(b, uint64_t(o.amount));
    uint64_t pr; std::memcpy(&pr, &o.price, 8); le64(b, pr);
    char sku[4] = {0, 0, 0, 0};
    std::memcpy(sku, o.sku, std::min<size_t>(4, std::strlen(o.sku)));
    b.insert(b.end(), sku, sku + 4);
    le32(b, o.qty);
    if (v2) le32(b, uint32_t(*o.bonus));
    return b;
}
std::vector<uint8_t> tombstone() { return {}; }

struct Harness {
    rune::codec::Descriptor src = orderSource();
    rune::view::CompileResult comp = compile(src);
    rune::view::AggLayout agg = rune::view::buildAggLayout(comp.plan, src).value();
    rune::view::VersionDescriptorCache vdc{src};
    rune::view::ViewTable view{comp.rowLayout};
    rune::view::ViewTable contributors{agg.contributorRow};
    rune::view::ViewTable groups{agg.groupRow};
    int64_t next = 0;
    int64_t cursor = 0;
    rune::view::FoldStats last{};
    uint64_t inconsistent = 0;   // summed over every apply()

    static rune::view::CompileResult compile(const rune::codec::Descriptor& s) {
        const rune::codec::Descriptor* srcs[] = {&s};
        return rune::view::compileView(userStats(), srcs, 9).value();
    }
    void apply(const std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>& recs) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        for (const auto& [k, v] : recs) {
            b.append(std::string_view(reinterpret_cast<const char*>(k.data()), k.size()),
                     std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0);
        }
        const auto batch = b.build();
        const int64_t bound = next + static_cast<int64_t>(recs.size());
        const rune::storage::LogReadView lv{batch.data(), batch.size(), next, bound};
        last = rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view,
                                              contributors, groups, lv, bound, cursor);
        inconsistent += last.aggInconsistent;
        next = bound;
    }
    static std::vector<uint8_t> key(int64_t orderId) {
        std::vector<uint8_t> k; le64(k, uint64_t(orderId)); return k;
    }
    void put(int64_t orderId, std::vector<uint8_t> value) { apply({{key(orderId), std::move(value)}}); }
    void putOrd(const Ord& o) { put(o.id, encode(o)); }

    bool hasGroup(int64_t userId) const { return view.get(uint64_t(userId)) != nullptr; }
    rune::codec::FieldValue col(int64_t userId, uint16_t slot) const {
        const uint8_t* r = view.get(uint64_t(userId));
        EXPECT_NE(r, nullptr) << "group " << userId << " must exist";
        return r ? rune::view::readSlot(r, comp.rowLayout, slot) : rune::codec::FieldValue{};
    }

    // Drains every table's dirty set; counts entries per table and group rows
    // left with a RescanPending cache (must never survive a record).
    struct Drained { uint32_t contributorRows = 0, groupRows = 0, viewRows = 0, pendingCaches = 0; };
    Drained drainCounts() {
        Drained d;
        contributors.drainDirty([&](uint32_t, const uint8_t*, bool, uint64_t) { ++d.contributorRows; });
        groups.drainDirty([&](uint32_t, const uint8_t* row, bool deleted, uint64_t) {
            ++d.groupRows;
            if (deleted) return;
            for (uint16_t ci = 0; ci < agg.numCached; ++ci) {
                if (row[agg.groupCacheOff + rune::view::kCacheEntryBytes * ci + 8] ==
                    static_cast<uint8_t>(rune::view::CacheState::RescanPending)) {
                    ++d.pendingCaches;
                }
            }
        });
        view.drainDirty([&](uint32_t, const uint8_t*, bool, uint64_t) { ++d.viewRows; });
        return d;
    }
};

} // namespace

TEST(AggRetractable, MinMaxLastFoldOnInsert) {
    Harness h;
    h.putOrd({1, 100, 10, 2.5, "AB", 7, 3});
    h.putOrd({2, 100, 30, 1.5, "CD", 5});          // v1: bonus NULL
    h.putOrd({3, 200, 5});
    EXPECT_EQ(h.col(100, kN).i, 2);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 10);
    EXPECT_EQ(h.col(100, kLastAmt).i, 30);
    EXPECT_DOUBLE_EQ(h.col(100, kMaxPrice).f, 2.5);
    EXPECT_EQ(h.col(100, kMinQty).u, 5u);
    EXPECT_EQ(std::string(h.col(100, kLastSku).bytes), std::string("CD\0\0", 4));
    EXPECT_FALSE(h.col(100, kLastBonus).present) << "Last includes null";
    EXPECT_EQ(h.col(100, kLastNnBonus).i, 3) << "LastNonNull skips the newer null";
    EXPECT_EQ(h.col(100, kTotal).i, 40);
    EXPECT_EQ(h.col(200, kMaxAmt).i, 5);
    EXPECT_EQ(h.col(200, kMinAmt).i, 5);
}

TEST(AggRetractableRegression, RetractingTheMaxSurfacesTheNextRegression) {
    Harness h;
    h.putOrd({1, 100, 10});
    h.putOrd({2, 100, 50});
    h.putOrd({3, 100, 30});
    EXPECT_EQ(h.col(100, kMaxAmt).i, 50);
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 10);
    EXPECT_GE(h.last.aggRescans, 1u) << "retracting the cached extreme walks the list";
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kMinAmt).i, 30) << "retracting the cached min surfaces the next";
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, RetractingOneOfDuplicateExtremesKeepsTheExtremeRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 50});
    h.putOrd({3, 100, 10});
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kMaxAmt).i, 50) << "the duplicate (contributor 2) still holds 50";
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kMaxAmt).i, 10);
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, InPlaceRaiseOfTheMaxSkipsTheRescanRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 30});
    h.putOrd({1, 100, 70});                         // the max holder raises its own max
    EXPECT_EQ(h.col(100, kMaxAmt).i, 70);
    EXPECT_EQ(h.last.aggRescans, 0u) << "a new value at least as extreme replaces the retracted one";
    h.putOrd({1, 100, 70});                         // equal re-write: still no rescan
    EXPECT_EQ(h.col(100, kMaxAmt).i, 70);
    EXPECT_EQ(h.last.aggRescans, 0u);
    EXPECT_EQ(h.col(100, kMinAmt).i, 30);
}

TEST(AggRetractableRegression, InPlaceLowerOfTheMaxRescansRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 30});
    h.putOrd({1, 100, 20});                         // the max holder drops below the next
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 20);
    EXPECT_EQ(h.last.aggRescans, 1u) << "exactly one walk, for maxAmt (minAmt: 20 beats 30, O(1))";
    EXPECT_EQ(h.col(100, kN).i, 2);
}

TEST(AggRetractableRegression, GroupMoveOfTheExtremeRescansOldGroupAndRaisesNewRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 30});
    h.putOrd({3, 200, 40});
    h.putOrd({1, 200, 50});                         // move the u100 max holder to u200
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 30);
    EXPECT_EQ(h.col(100, kLastAmt).i, 30);
    EXPECT_EQ(h.col(200, kN).i, 2);
    EXPECT_EQ(h.col(200, kMaxAmt).i, 50);
    EXPECT_EQ(h.col(200, kMinAmt).i, 40);
    EXPECT_EQ(h.col(200, kLastAmt).i, 50);
    h.putOrd({2, 200, 1});                          // the last u100 contributor moves: u100 empties
    EXPECT_FALSE(h.hasGroup(100));
    EXPECT_EQ(h.col(200, kMinAmt).i, 1);
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, LastAfterDeletingTheNewestIsThePreviousWriteRegression) {
    Harness h;
    h.putOrd({1, 100, 10});
    h.putOrd({2, 100, 20});
    h.putOrd({3, 100, 30});
    EXPECT_EQ(h.col(100, kLastAmt).i, 30);
    h.put(3, tombstone());
    EXPECT_EQ(h.col(100, kLastAmt).i, 20);
    h.putOrd({1, 100, 15});                         // a re-write moves 1 to the head
    EXPECT_EQ(h.col(100, kLastAmt).i, 15);
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kLastAmt).i, 20);
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, LastIncludesNullAndLastNonNullSkipsNullsRegression) {
    Harness h;
    h.putOrd({1, 100, 10, 1.0, "AAAA", 1, 7});      // bonus 7
    h.putOrd({2, 100, 20});                          // bonus NULL (v1)
    EXPECT_FALSE(h.col(100, kLastBonus).present);
    EXPECT_EQ(h.col(100, kLastNnBonus).i, 7);
    h.putOrd({3, 100, 30});                          // another NULL on top
    EXPECT_EQ(h.col(100, kLastNnBonus).i, 7);
    h.putOrd({2, 100, 20, 1.0, "AAAA", 1, 9});      // 2 now non-null and newest
    EXPECT_EQ(h.col(100, kLastBonus).i, 9);
    EXPECT_EQ(h.col(100, kLastNnBonus).i, 9);
    h.put(2, tombstone());
    h.put(1, tombstone());
    EXPECT_FALSE(h.col(100, kLastBonus).present);
    EXPECT_FALSE(h.col(100, kLastNnBonus).present) << "no non-null contributor left -> NULL";
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, NonFiniteMaxInputIsNullAndCountedRegression) {
    Harness h;
    h.putOrd({1, 100, 10, 2.0});
    h.putOrd({2, 100, 10, std::numeric_limits<double>::quiet_NaN()});
    EXPECT_EQ(h.last.aggNonFinite, 1u);
    h.putOrd({3, 100, 10, std::numeric_limits<double>::infinity()});
    EXPECT_EQ(h.last.aggNonFinite, 1u);
    EXPECT_DOUBLE_EQ(h.col(100, kMaxPrice).f, 2.0) << "+Inf is NULL, not the max";
    h.put(1, tombstone());
    EXPECT_FALSE(h.col(100, kMaxPrice).present) << "only non-finite (NULL) prices remain";
    EXPECT_EQ(h.col(100, kN).i, 2);
}

TEST(AggRetractableRegression, UnsignedMinRetractsOnTombstoneRegression) {
    Harness h;
    h.putOrd({1, 100, 10, 1.0, "AAAA", 3});
    h.putOrd({2, 100, 10, 1.0, "AAAA", 9});
    EXPECT_EQ(h.col(100, kMinQty).u, 3u);
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kMinQty).u, 9u);
}

TEST(AggRetractableRegression, ContributorKeyEqualToSentinelIsSkippedRegression) {
    Harness h;
    const std::vector<uint8_t> sentinel(8, 0xFF);   // == kNoContributor
    h.apply({{sentinel, encode({7, 100, 99})}});
    EXPECT_EQ(h.last.skippedMalformed, 1u);
    EXPECT_FALSE(h.hasGroup(100)) << "never linked, never counted";
    h.putOrd({1, 100, 10});
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 10);
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, DanglingLinkIsCountedNotFollowedRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 30});                          // list: 2 (head) -> 1
    ASSERT_TRUE(h.contributors.erase(2));            // damage: the head's row vanishes
    h.putOrd({1, 100, 20});                          // in-place lower of the max holder
    EXPECT_GE(h.last.aggInconsistent, 1u) << "the missing neighbour/head is counted";
    ASSERT_TRUE(h.hasGroup(100));
    EXPECT_EQ(h.col(100, kMaxAmt).i, 20) << "the rescan only reports values it actually found";
    EXPECT_EQ(h.col(100, kLastAmt).i, 20);
    EXPECT_EQ(h.contributors.get(2), nullptr) << "a link is never followed into a NEW row";
}
```

Add `tests/view/AggRetractableTest.cpp` to `rune_view_tests` in `broker/libs/view/CMakeLists.txt` (S8: insert after `tests/view/AggFoldTest.cpp`, line 25):

```cmake
        tests/view/AggFoldTest.cpp
        tests/view/AggRetractableTest.cpp
        tests/view/PlanSourcesTest.cpp)
```

(`tests/view/PlanSourcesTest.cpp)` is line 26 and stays last, so the closing `)` does not move.)

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build/root -j14 --target rune_view_tests`
Expected: compile error `no member named 'aggRescans' in 'rune::view::FoldStats'`. Once that member exists, the tests fail at runtime because Min/Max/Last emit NULL.

- [ ] **Step 3a: Add the counter.** In `broker/libs/view/include/rune/view/ViewFold.h`, after the `aggInconsistent` member (line 68), add:

```cpp
    // Phase 2b: Min/Max caches rebuilt by one walk of the group's contributor
    // list (the cached extreme was retracted) -- the known O(group size) path.
    uint64_t aggRescans       = 0;
```

- [ ] **Step 3b: Replace the fold.** Replace `broker/libs/view/src/AggFold.cpp` with:

```cpp
// broker/libs/view/src/AggFold.cpp — see AggFold.h / AggState.h.
#include <rune/view/AggFold.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <span>
#include <string_view>

#include <rune/codec/Record.h>
#include <rune/protocol/RecordBatch.h>

namespace rune::view {

namespace {

using rune::codec::FieldValue;
using rune::codec::PrimType;

inline std::span<const uint8_t> asBytes(std::string_view sv) noexcept {
    return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
}

inline bool nullBit(const uint8_t* row, uint32_t off, uint16_t i) noexcept {
    return (row[off + i / 8] >> (i % 8)) & 1u;
}
inline void setNullBit(uint8_t* row, uint32_t off, uint16_t i, bool isNull) noexcept {
    const uint8_t m = static_cast<uint8_t>(1u << (i % 8));
    if (isNull) row[off + i / 8] |= m; else row[off + i / 8] &= static_cast<uint8_t>(~m);
}

// Min/Max comparisons in an input's widened 8-byte domain (int64 / uint64 /
// double bits -- exactly what a contributor value slot and a cache hold).
inline bool strictlyBetter(MergeRule rule, AccKind kind, const uint8_t* a, const uint8_t* b) noexcept {
    const bool max = rule == MergeRule::Max;
    switch (kind) {
        case AccKind::Signed: {
            int64_t x, y;
            std::memcpy(&x, a, 8);
            std::memcpy(&y, b, 8);
            return max ? x > y : x < y;
        }
        case AccKind::Unsigned: {
            const uint64_t x = loadU64(a), y = loadU64(b);
            return max ? x > y : x < y;
        }
        case AccKind::Float: {
            const double x = loadF64(a), y = loadF64(b);
            return max ? x > y : x < y;
        }
        case AccKind::None: return false;
    }
    return false;
}
inline bool sameValue(AccKind kind, const uint8_t* a, const uint8_t* b) noexcept {
    if (kind == AccKind::Float) return loadF64(a) == loadF64(b);
    return std::memcmp(a, b, 8) == 0;
}

constexpr uint8_t kEmpty   = static_cast<uint8_t>(CacheState::Empty);
constexpr uint8_t kValid   = static_cast<uint8_t>(CacheState::Valid);
constexpr uint8_t kPending = static_cast<uint8_t>(CacheState::RescanPending);

// What `retract` does with the group the contributor leaves. One state per legal
// combination (this replaces a (dropIfEmpty, emitNow) bool pair, F8):
//   Delete         tombstone / missing group key: an emptied group loses its rows,
//                  a non-empty group is re-emitted now.
//   Move           update into ANOTHER group, or any 2a update: a non-empty old
//                  group is re-emitted now; an emptied one is dropped by the caller
//                  after the add.
//   InPlaceLinked  update within the SAME group of a linked view: no emit and no
//                  drop here. The caller's add + emit follows, so a new value that
//                  is at least as extreme skips the rescan.
enum class RetractMode : uint8_t { Delete, Move, InPlaceLinked };

struct Ctx {
    const RowLayout& layout;
    const AggLayout& agg;
    ViewTable& view;
    ViewTable& contributors;
    ViewTable& groups;
    FoldStats& stats;

    uint8_t* acc(uint8_t* g, size_t i) const noexcept { return g + agg.groupAccOff + 16 * i; }
    uint8_t* nn(uint8_t* g, size_t i) const noexcept { return g + agg.groupNonNullOff + 8 * i; }
    const uint8_t* val(const uint8_t* c, uint16_t input) const noexcept {
        return c + agg.inputOff[input];
    }
    uint8_t* cacheVal(uint8_t* g, uint16_t ci) const noexcept {
        return g + agg.groupCacheOff + kCacheEntryBytes * ci;
    }
    uint8_t* cacheState(uint8_t* g, uint16_t ci) const noexcept { return cacheVal(g, ci) + 8; }

    // Writable row of an EXISTING contributor (put() marks it dirty). A link
    // to a missing row is counted and never followed -- and never creates one.
    uint8_t* linkedContributor(uint64_t k) noexcept {
        if (k == kNoContributor || contributors.get(k) == nullptr) {
            ++stats.aggInconsistent;
            return nullptr;
        }
        return contributors.put(k);
    }

    void addToCache(uint8_t* gr, const AggField& f, const uint8_t* v, uint64_t nnNew) noexcept {
        uint8_t* cv = cacheVal(gr, f.cache);
        uint8_t* cs = cacheState(gr, f.cache);
        switch (*cs) {
            case kEmpty:
                if (nnNew == 1) { std::memcpy(cv, v, 8); *cs = kValid; }
                else { ++stats.aggInconsistent; *cs = kPending; }   // values without a cache: rebuild from the list
                break;
            case kValid:
                if (strictlyBetter(f.rule, f.kind, v, cv)) std::memcpy(cv, v, 8);
                break;
            case kPending:
                // cv still holds the RETRACTED extreme, which bounds every
                // remaining value: one at least as extreme is the new extreme.
                if (strictlyBetter(f.rule, f.kind, v, cv) || sameValue(f.kind, v, cv)) {
                    std::memcpy(cv, v, 8);
                    *cs = kValid;
                }
                break;
            default:                                            // corrupt state byte
                ++stats.aggInconsistent;
                *cs = kPending;
                break;
        }
    }

    void retractFromCache(uint8_t* gr, const AggField& f, const uint8_t* v, uint64_t nnNew) noexcept {
        uint8_t* cv = cacheVal(gr, f.cache);
        uint8_t* cs = cacheState(gr, f.cache);
        if (nnNew == 0) { storeU64(cv, 0); *cs = kEmpty; return; }
        if (*cs == kValid && sameValue(f.kind, v, cv)) *cs = kPending;
    }

    // Applies one contributor row to its group: sign = +1 add, -1 retract.
    // Add: returns the group row (created if new). Retract: returns nullptr and
    // counts aggInconsistent -- touching nothing -- if the group row is missing
    // or any count would underflow (checked BEFORE mutating).
    uint8_t* applyContribution(const uint8_t* c, int sign) noexcept {
        const uint64_t g = loadU64(c);
        if (sign < 0) {
            const uint8_t* cur = groups.get(g);
            bool bad = cur == nullptr || loadU64(cur) == 0;
            for (size_t i = 0; !bad && i < agg.fields.size(); ++i) {
                const AggField& f = agg.fields[i];
                if (f.input == kNoSourceField || nullBit(c, agg.contribNullOff, f.input)) continue;
                if (loadU64(cur + agg.groupNonNullOff + 8 * i) == 0) bad = true;
            }
            if (bad) { ++stats.aggInconsistent; return nullptr; }
        }
        uint8_t* gr = groups.put(g);   // retract: returns the existing row
        const uint64_t n = loadU64(gr);
        storeU64(gr, sign > 0 ? n + 1 : n - 1);
        for (size_t i = 0; i < agg.fields.size(); ++i) {
            const AggField& f = agg.fields[i];
            if (f.input == kNoSourceField) continue;                 // CountAll
            if (nullBit(c, agg.contribNullOff, f.input)) continue;   // nulls contribute nothing
            const uint64_t nnv = loadU64(nn(gr, i));
            const uint64_t nnNew = sign > 0 ? nnv + 1 : nnv - 1;
            storeU64(nn(gr, i), nnNew);
            const uint8_t* v = val(c, f.input);
            if (f.cache != kNoCache) {                               // Min/Max
                if (sign > 0) addToCache(gr, f, v, nnNew); else retractFromCache(gr, f, v, nnNew);
                continue;
            }
            if (f.rule != MergeRule::Sum && f.rule != MergeRule::Avg) continue;   // Count(col), Last*
            switch (f.kind) {
                case AccKind::Signed: {
                    int64_t x; std::memcpy(&x, v, 8);
                    const __int128 a = loadI128(acc(gr, i));
                    storeI128(acc(gr, i), sign > 0 ? a + x : a - x);
                    break;
                }
                case AccKind::Unsigned: {
                    const uint64_t x = loadU64(v);
                    const unsigned __int128 a = loadU128(acc(gr, i));
                    storeU128(acc(gr, i), sign > 0 ? a + x : a - x);
                    break;
                }
                case AccKind::Float: {
                    const double x = loadF64(v);
                    const double a = loadF64(acc(gr, i));
                    // Last non-null contributor gone: reset to exactly 0.0 (no
                    // cancellation residual carried into the next contributor).
                    storeF64(acc(gr, i), nnNew == 0 ? 0.0 : (sign > 0 ? a + x : a - x));
                    break;
                }
                case AccKind::None: break;
            }
        }
        return gr;
    }

    // Unlinks contributor `c` (its pre-image row `cr`) from group `g`'s list.
    // `gr` is g's row, or nullptr if the retraction found g inconsistent (the
    // head is then still fixed when g's row exists).
    void unlink(uint64_t c, const uint8_t* cr, uint64_t g, uint8_t* gr) noexcept {
        const uint64_t prev = loadU64(cr + agg.contribPrevOff);
        const uint64_t next = loadU64(cr + agg.contribNextOff);
        if (prev != kNoContributor) {
            if (uint8_t* p = linkedContributor(prev)) storeU64(p + agg.contribNextOff, next);
        } else {
            uint8_t* hr = gr != nullptr ? gr : (groups.get(g) != nullptr ? groups.put(g) : nullptr);
            if (hr != nullptr && loadU64(hr + agg.groupHeadOff) == c) storeU64(hr + agg.groupHeadOff, next);
            else ++stats.aggInconsistent;   // no predecessor, yet not its group's head
        }
        if (next != kNoContributor) {
            if (uint8_t* nx = linkedContributor(next)) storeU64(nx + agg.contribPrevOff, prev);
        }
    }

    // Links contributor `c` (row `cr`) at the head of the group whose row is `gr`
    // (applyContribution(+1) has already counted c in).
    void linkAtHead(uint64_t c, uint8_t* cr, uint8_t* gr) noexcept {
        // c is the only contributor -> the list was empty (a new group row is
        // zero-filled, so its stored head is not a key then).
        uint64_t oldHead = loadU64(gr) == 1 ? kNoContributor : loadU64(gr + agg.groupHeadOff);
        if (oldHead == c) { ++stats.aggInconsistent; oldHead = kNoContributor; }
        storeU64(cr + agg.contribPrevOff, kNoContributor);
        storeU64(cr + agg.contribNextOff, oldHead);
        if (oldHead != kNoContributor) {
            if (uint8_t* h = linkedContributor(oldHead)) storeU64(h + agg.contribPrevOff, c);
            else storeU64(cr + agg.contribNextOff, kNoContributor);   // never link to a missing row
        }
        storeU64(gr + agg.groupHeadOff, c);
    }

    // Rebuilds Min/Max field i's cache by ONE walk of the group's list, bounded
    // by its contributor count (a cycle or over-long list stops, counted).
    void rescan(uint8_t* gr, size_t i, const AggField& f) noexcept {
        ++stats.aggRescans;
        uint8_t* cv = cacheVal(gr, f.cache);
        uint8_t* cs = cacheState(gr, f.cache);
        const uint64_t n = loadU64(gr);
        const uint64_t nonNull = loadU64(nn(gr, i));
        uint64_t k = loadU64(gr + agg.groupHeadOff);
        uint64_t steps = 0, seen = 0;
        bool have = false, bad = false;
        while (k != kNoContributor) {
            if (steps == n) { bad = true; break; }
            const uint8_t* r = contributors.get(k);
            if (r == nullptr) { bad = true; break; }
            ++steps;
            if (!nullBit(r, agg.contribNullOff, f.input)) {
                const uint8_t* v = val(r, f.input);
                if (!have || strictlyBetter(f.rule, f.kind, v, cv)) { std::memcpy(cv, v, 8); have = true; }
                ++seen;
            }
            k = loadU64(r + agg.contribNextOff);
        }
        if (bad || steps != n || seen != nonNull) ++stats.aggInconsistent;
        if (!have) storeU64(cv, 0);
        *cs = have ? kValid : kEmpty;
    }

    // The most recently written contributor (emit runs only for a non-empty group).
    const uint8_t* headContributor(const uint8_t* gr) noexcept {
        const uint64_t h = loadU64(gr + agg.groupHeadOff);
        const uint8_t* r = h == kNoContributor ? nullptr : contributors.get(h);
        if (r == nullptr) ++stats.aggInconsistent;
        return r;
    }

    // First contributor from the head whose `input` is non-null; bounded walk.
    const uint8_t* firstNonNull(const uint8_t* gr, uint16_t input) noexcept {
        const uint64_t n = loadU64(gr);
        uint64_t k = loadU64(gr + agg.groupHeadOff);
        for (uint64_t steps = 0; k != kNoContributor; ++steps) {
            if (steps == n) break;
            const uint8_t* r = contributors.get(k);
            if (r == nullptr) break;
            if (!nullBit(r, agg.contribNullOff, input)) return r;
            k = loadU64(r + agg.contribNextOff);
        }
        ++stats.aggInconsistent;   // the non-null count said one exists
        return nullptr;
    }

    void dropGroup(uint64_t g) noexcept {
        view.erase(g);
        groups.erase(g);
    }

    // Rewrites the output row for group `g` from its accumulators, caches and list.
    void emit(uint64_t g, uint8_t* gr) noexcept {
        uint8_t* row = view.put(g);
        FieldValue k{};
        k.type = agg.keyType;
        k.present = true;
        if (agg.keyType == PrimType::Int64) { int64_t s; std::memcpy(&s, &g, 8); k.i = s; }
        else { k.u = g; }
        writeSlot(row, layout, agg.keyOutSlot, k);

        const uint64_t contributorsN = loadU64(gr);
        for (size_t i = 0; i < agg.fields.size(); ++i) {
            const AggField& f = agg.fields[i];
            const uint64_t nonNull = loadU64(nn(gr, i));
            FieldValue out{};
            out.present = true;
            switch (f.rule) {
                case MergeRule::CountAll:
                    out.type = PrimType::Int64; out.i = static_cast<int64_t>(contributorsN); break;
                case MergeRule::Count:
                    out.type = PrimType::Int64; out.i = static_cast<int64_t>(nonNull); break;
                case MergeRule::Sum:
                    if (nonNull == 0) { out.present = false; out.type = PrimType::Int64; break; }
                    if (f.kind == AccKind::Signed) {
                        out.type = PrimType::Int64;
                        const __int128 a = loadI128(acc(gr, i));
                        if (a > std::numeric_limits<int64_t>::max() ||
                            a < std::numeric_limits<int64_t>::min()) {
                            out.present = false; ++stats.aggOverflow;
                        } else {
                            out.i = static_cast<int64_t>(a);
                        }
                    } else if (f.kind == AccKind::Unsigned) {
                        out.type = PrimType::UInt64;
                        const unsigned __int128 a = loadU128(acc(gr, i));
                        if (a > std::numeric_limits<uint64_t>::max()) {
                            out.present = false; ++stats.aggOverflow;
                        } else {
                            out.u = static_cast<uint64_t>(a);
                        }
                    } else {
                        out.type = PrimType::Float64; out.f = loadF64(acc(gr, i));
                    }
                    break;
                case MergeRule::Avg:
                    out.type = PrimType::Float64;
                    if (nonNull == 0) { out.present = false; break; }
                    if (f.kind == AccKind::Signed)
                        out.f = static_cast<double>(loadI128(acc(gr, i))) / static_cast<double>(nonNull);
                    else if (f.kind == AccKind::Unsigned)
                        out.f = static_cast<double>(loadU128(acc(gr, i))) / static_cast<double>(nonNull);
                    else
                        out.f = loadF64(acc(gr, i)) / static_cast<double>(nonNull);
                    break;
                case MergeRule::Min:
                case MergeRule::Max: {
                    out.type = f.srcType;
                    if (nonNull == 0) { out.present = false; break; }
                    uint8_t* cs = cacheState(gr, f.cache);
                    if (*cs != kValid) {
                        if (*cs != kPending) ++stats.aggInconsistent;
                        rescan(gr, i, f);
                    }
                    if (*cs != kValid) { out.present = false; break; }
                    const uint8_t* cv = cacheVal(gr, f.cache);
                    if (f.kind == AccKind::Signed)        std::memcpy(&out.i, cv, 8);
                    else if (f.kind == AccKind::Unsigned) out.u = loadU64(cv);
                    else                                   out.f = loadF64(cv);
                    break;
                }
                case MergeRule::Last: {
                    const uint8_t* h = headContributor(gr);
                    if (h == nullptr || nullBit(h, agg.contribNullOff, f.input)) {
                        out.type = f.srcType; out.present = false; break;
                    }
                    out = decodeSlotValue(val(h, f.input), f.srcType, agg.inputWidth[f.input]);
                    break;
                }
                case MergeRule::LastNonNull: {
                    const uint8_t* h = nonNull == 0 ? nullptr : firstNonNull(gr, f.input);
                    if (h == nullptr) { out.type = f.srcType; out.present = false; break; }
                    out = decodeSlotValue(val(h, f.input), f.srcType, agg.inputWidth[f.input]);
                    break;
                }
                case MergeRule::Replace:
                    out.present = false; break;   // unreachable: the key column is not an AggField
            }
            writeSlot(row, layout, f.outSlot, out);
        }
    }

    // Retracts contributor `c` (pre-image row `cr`) from its group and, in a
    // linked view, unlinks it. `mode` says what then happens to the group (see
    // RetractMode above).
    uint64_t retract(uint64_t c, const uint8_t* cr, RetractMode mode) noexcept {
        const uint64_t g = loadU64(cr);
        uint8_t* gr = applyContribution(cr, -1);
        if (agg.linked) unlink(c, cr, g, gr);
        if (gr == nullptr) return g;   // inconsistent: counted, nothing else changed
        if (loadU64(gr) == 0) {
            if (mode == RetractMode::Delete) dropGroup(g);
        } else if (mode != RetractMode::InPlaceLinked) {
            emit(g, gr);
        }
        return g;
    }
};

} // namespace

FoldStats foldAggregateBatch(const CompiledViewPlan& /*plan*/, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor) {
    FoldStats stats{};
    Ctx cx{layout, agg, view, contributors, groups, stats};
    const int64_t from = cursor;   // records below it were folded by a prior (mid-batch) pass
    bool stopped = false;
    bool corrupt = false;
    int64_t walkedUpto = lv.baseOffset;

    size_t pos = 0;
    while (pos < lv.len) {
        rune::protocol::RecordBatchReader r(lv.data + pos, lv.len - pos);
        if (!r.ensureValid()) { corrupt = true; break; }
        if (r.isControl()) {
            ++stats.skippedControl;
            pos += static_cast<size_t>(r.totalLength());
            walkedUpto = r.baseOffset() + r.lastOffsetDelta() + 1;
            continue;
        }
        r.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset,
                            int64_t /*ts*/) {
            if (stopped) return;
            if (offset >= bound) { stopped = true; return; }
            if (offset < from) return;         // already folded (resume mid-batch)
            if (key.size() != 8) { ++stats.skippedMalformed; return; }
            uint64_t c = 0;
            std::memcpy(&c, key.data(), 8);   // little-endian identity bytes
            // A linked view's lists end in kNoContributor: that key cannot be linked.
            if (agg.linked && c == kNoContributor) { ++stats.skippedMalformed; return; }

            const uint8_t* old = contributors.get(c);
            if (value.empty()) {               // tombstone: retract a known contributor
                if (old != nullptr) {
                    (void)cx.retract(c, old, RetractMode::Delete);
                    contributors.erase(c);
                }
                ++stats.deleted;
                return;
            }

            const uint16_t writerVersion =
                value.size() >= 4
                    ? static_cast<uint16_t>(static_cast<uint8_t>(value[2]) |
                                            (static_cast<uint8_t>(value[3]) << 8))
                    : vdc.defaultVersion();
            const rune::codec::Descriptor& d = vdc.materializedFor(writerVersion);
            const FieldValue gk = rune::codec::readField(d, asBytes(value), agg.keySrcFieldId);
            if (!gk.present) {
                if (old != nullptr) {
                    (void)cx.retract(c, old, RetractMode::Delete);
                    contributors.erase(c);
                }
                ++stats.skippedNoGroup;
                return;
            }
            uint64_t g = 0;
            if (agg.keyType == PrimType::Int64) std::memcpy(&g, &gk.i, 8); else g = gk.u;

            bool hadOld = false;
            uint64_t oldGroup = 0;
            if (old != nullptr) {
                hadOld = true;
                const bool sameGroup = loadU64(old) == g;
                oldGroup = cx.retract(c, old, agg.linked && sameGroup ? RetractMode::InPlaceLinked
                                                                       : RetractMode::Move);
            }

            uint8_t* cr = contributors.put(c);   // same row when it already existed
            storeU64(cr, g);
            for (const AggField& f : agg.fields) {
                if (f.input == kNoSourceField) continue;
                FieldValue in = rune::codec::readField(d, asBytes(value), f.srcFieldId);
                if (in.present && f.kind == AccKind::Float && !std::isfinite(in.f)) {
                    in.present = false;   // NaN/Inf is treated as NULL (Sum/Avg/Min/Max)
                    ++stats.aggNonFinite;
                }
                setNullBit(cr, agg.contribNullOff, f.input, !in.present);
                uint8_t* dst = cr + agg.inputOff[f.input];
                const uint16_t w = agg.inputWidth[f.input];
                if (!in.present)                       { std::memset(dst, 0, w); }
                else if (f.rule == MergeRule::Last || f.rule == MergeRule::LastNonNull)
                                                       { encodeSlotValue(dst, f.srcType, w, in); }
                else if (f.kind == AccKind::Signed)    { std::memcpy(dst, &in.i, 8); }
                else if (f.kind == AccKind::Unsigned)  { storeU64(dst, in.u); }
                else if (f.kind == AccKind::Float)     { storeF64(dst, in.f); }
                else                                   { storeU64(dst, 0); }   // Count(col)
            }
            uint8_t* gr = cx.applyContribution(cr, +1);
            if (agg.linked) cx.linkAtHead(c, cr, gr);
            cx.emit(g, gr);
            if (hadOld && oldGroup != g) {
                const uint8_t* og = groups.get(oldGroup);
                if (og != nullptr && loadU64(og) == 0) cx.dropGroup(oldGroup);
            }
            ++stats.applied;
        });
        if (stopped) break;
        pos += static_cast<size_t>(r.totalLength());
        walkedUpto = r.baseOffset() + r.lastOffsetDelta() + 1;
    }
    // Never move the resume point backwards (a corrupt batch at/after `from`).
    cursor = std::max(from, corrupt ? walkedUpto : std::min(lv.nextOffset, bound));
    return stats;
}

} // namespace rune::view
```

Notes for the implementer. These are the same contract that is enforced in code above:
- The 2a path is unchanged. With `agg.linked == false`, the mode is never `InPlaceLinked` (a non-empty old group is always re-emitted on retract, as in 2a), `unlink`/`linkAtHead` never run, Min/Max/Last never occur, and `inputOff[i] == contribValOff + 8*i`. The order of `put` calls, and therefore the order of dirty entries and the checkpoint bytes, is identical to 2a.
- `put()` is the only call that marks a row dirty. Neighbour fix-ups therefore go through `linkedContributor` (`get` first, then `put`), and a rescan uses `get` only.

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'AggRetractable|AggFold|AggState' --output-on-failure`
Expected: every `AggRetractable*` test passes, and every 2a `AggFold*` test passes unchanged.

- [ ] **Step 5: Run the affected tests and commit.**

Run: `ctest --test-dir build/root -R 'AggRetractable|AggFold|AggState|ViewFold|ViewCheckpoint' -j14 --output-on-failure` → 0 failures.

```bash
git add broker/libs/view/include/rune/view/ViewFold.h broker/libs/view/src/AggFold.cpp broker/libs/view/tests/view/AggRetractableTest.cpp broker/libs/view/CMakeLists.txt
git commit -m "feat(view): retractable Min/Max/Last/LastNonNull — MRU contributor list, cached extremes, rescan on retracted extreme" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Early gate run (F4).** The typical-case gate is the riskiest number in Part A: the churn retracts a cached extreme on roughly 2% of retracting ops (about 0.8% of records), and each rescan walks about 100 random rows of a 1M-row table, so `ret_record_p99_us < 5` is borderline. Do not wait for Task 8. Do **Task 8 Steps 1–3 now** (fixtures, the shared `AggRig` helper, `BM_ViewFoldRetractable`), commit them as Task 8's first commit, and run the gate in the release build (start benches early):

```bash
cmake --build build/rel -j14 --target rune_broker_benchmark
build/rel/broker/rune_broker_benchmark --benchmark_filter='ViewFold(Retractable|Aggregate|Replace)'
```

Expected: `[ViewFoldRetractable gate] … => PASS`, and `BM_ViewFoldAggregate` / `BM_ViewFoldReplace` still PASS. On a FAIL, report `ret_rescans` divided by the measured records and the record-vs-batch p99 split to the controller **before any tuning**; never relax the gate or its thresholds.

---

### Task 5b: contiguous Min/Max value block

> **Added 2026-10-20 (user decision, spec §A amended the same day).** Task 5's gate run measured record p99 21.2 µs and batch p99 25.1 µs (`BM_ViewFoldRetractable`, `build/rel`; zero allocations, p50 0.75 µs). About 5.8% of records rescan, and a list walk over ~100 contributors costs ~21 µs (~200 ns per hop: each link is a contributor-key hash lookup into a 1M-row table). Even uniformly random amounts rescan 2.2% of records, so honest churn fails p99 unless the rescan is cheap. The user chose **"faster rescans now"**: each group keeps its Min/Max input values in one contiguous, preallocated, checkpointed block, and a rescan is a sequential scan of packed values. The MRU list, the O(1) head and the 2a byte-identical layout are unchanged; views with only `Last`/`LastNonNull` get no block.
>
> **Carried Task 5 review items (both owned here or placed):**
> 1. The byte-budget half of `WorstCaseMoveDirtiesExactlyTheBoundAndFitsTheBudgetRegression` is tautological (it multiplies the same row lengths on both sides). Step 1c **replaces** that test with `WorstCaseMoveDirtiesExactlyTheBoundRegression`, a pure dirty-count pin on all four tables. The budget itself is pinned on the node side by Task 7's `PersistedRetractableMovesDeltasFitTheRingRegression`, which throws if the bound is too low.
> 2. `FoldStats::aggRescans` goes up to the node counter beside `aggInconsistent` (`ViewFoldCounters`, `WorkerViewSet.h`). **Task 7 owns it** (Step 3 there), because Task 7 already edits that accumulate block and asserts the counter in its restart regression.

**Files:**
- Modify: `broker/libs/view/include/rune/view/AggState.h` (block constants and helpers, `AggLayout` block members, `AggDirtyBound::blockRows`, `kAggTableBlocks`, `loadU32`/`storeU32`)
- Modify: `broker/libs/view/src/AggState.cpp` (`buildAggLayout` tail, `aggDirtyBoundPerRecord`)
- Modify: `broker/libs/view/include/rune/view/AggFold.h` (the `blocks` overload)
- Modify: `broker/libs/view/src/AggFold.cpp` (`Ctx::blocks`, block maintenance, the block rescan, the fold's two hook points)
- Modify: `broker/libs/view/tests/view/AggStateTest.cpp` (layout pins)
- Modify: `broker/libs/view/tests/view/AggRetractableTest.cpp` (harness, one changed expectation, one replaced test, new block tests)
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h` (`AggInstance::blocks`, the fold call, `discardDirty_`, `dirtyBacklog`)
- Modify: `broker/libs/node/src/WorkerViewSet.cpp` (`replayInstance`, `maxRecordsPerPassFor`, `buildAndPushCheckpoint_`)
- Modify: `broker/benchmark/ViewFoldBench.cpp` (`AggRig` owns the block table)

**Interfaces:**
- Consumes: Task 4's `AggLayout` (`linked`, `inputOff`, `numCached`, `groupCacheOff`, `contribNullOff`), Task 5's fold (`Ctx`, `RetractMode`, `linkedContributor`, `rescan`, `emit`), `ViewTable::get/put/erase/size/drainDirty`, `encodeMultiDelta`/`applyMultiDelta` (v2, up to 64 tables).
- Produces (exact, in `rune::view`):
  ```cpp
  inline constexpr uint32_t kBlockEntries     = 32;   // entries per chunk row (power of two)
  inline constexpr uint32_t kBlockShift       = 5;    // log2(kBlockEntries)
  inline constexpr uint32_t kBlockMask        = kBlockEntries - 1;
  inline constexpr uint32_t kBlockSpareFreeAt = kBlockEntries / 2;   // free the spare once the tail holds <= 16
  inline constexpr uint32_t kNoBlock          = 0;    // group row blockId: no block
  inline constexpr uint32_t kBlockIdProbes    = 64;   // linear-probe limit when picking a block id
  inline constexpr uint8_t  kAggTableBlocks   = 3;    // v2 checkpoint table id
  constexpr uint64_t blockChunkKey(uint32_t blockId, uint32_t chunk) noexcept;   // blockId << 32 | chunk
  constexpr uint32_t blockIdCandidate(uint64_t groupKey) noexcept;             // never kNoBlock
  constexpr uint32_t blockKeyOff(uint32_t slot) noexcept;
  constexpr uint32_t blockValOff(uint16_t col, uint32_t slot) noexcept;
  uint32_t blockNullOff(const AggLayout& a, uint16_t col, uint32_t slot) noexcept;
  uint32_t loadU32(const uint8_t* p) noexcept;  void storeU32(uint8_t* p, uint32_t v) noexcept;
  struct AggLayout { /* Task 4 members unchanged */
      uint16_t numBlockCols; std::vector<uint16_t> blockColInput; std::vector<uint16_t> cacheCol;
      uint32_t contribBlockPosOff; uint32_t groupBlockOff; RowLayout blockRow; };
  struct AggDirtyBound { uint32_t contributorRows, groupRows, viewRows, blockRows; };
  AggDirtyBound aggDirtyBoundPerRecord(const AggLayout& a) noexcept;   // 2a {1,2,2,0}; Last-only {4,2,2,0}; Min/Max {5,2,2,3}
  FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                               const AggLayout& agg, VersionDescriptorCache& vdc,
                               ViewTable& view, ViewTable& contributors, ViewTable& groups,
                               ViewTable* blocks, const rune::storage::LogReadView& lv,
                               int64_t bound, int64_t& cursor);   // new overload
  // The 10-argument foldAggregateBatch stays and forwards blocks = nullptr (2a / Last-only layouts).
  ```
- Produces (node): `rune::node::AggInstance::blocks` (`std::optional<rune::view::ViewTable>`, engaged iff `layout.numBlockCols > 0`) and `AggInstance::blocksOrNull()`. A Min/Max view's v2 record carries 4 tables; 2a-only and Last-only views keep 3, byte for byte.

**Storage** (all little-endian, memcpy-encoded; `{…}` = only when `numBlockCols > 0`):
- **Block columns:** one per DISTINCT Min/Max source field, in cache order (`Min(x)` and `Max(x)` read the same widened 8-byte value and null bit, so they share a column). `cacheCol[cache]` maps a Min/Max field to its column. `blockColInput[col]` is the contributor input whose value the column copies.
- **Chunk row** (`blocks` table, `rowLen = kBlockEntries × (8 + 9 × numBlockCols)`): `[contributorKey u64 × 32][per column: value 8B × 32][per column: null u8 × 32]`. It is column-wise, so a rescan of one field reads 8-byte values back to back plus one null byte each, and never the keys. A null entry has flag 1 and value 0 (the contributor slot's bytes).
- **Chunk key:** `blockChunkKey(blockId, chunkIdx) = blockId << 32 | chunkIdx`. It is exact: ViewTable keys are exact u64, and a (groupKey, chunkIdx) pair does not fit 64 bits, so each group gets a 32-bit `blockId`. The group picks it at its first entry: `blockIdCandidate(groupKey)`, then linear probing past ids whose chunk 0 exists. **Invariant: an id is in use iff its chunk 0 exists.** A group with n ≥ 1 entries owns chunk 0, and chunk 0 is erased exactly when n reaches 0. So there is no counter, no free list, no exhaustion and no extra checkpointed state; the choice is deterministic across a restart (same set of chunk 0 rows), and the id is stored in the group row anyway.
- **Group row:** `…{[head u64][caches]}{[blockId u32][blockChunks u32]}`. `groupBlockOff` = the end of the caches. `blockChunks` counts the chunk rows the group owns, used plus at most one spare.
- **Contributor row:** `…[values]{[blockPos u32]}`. `contribBlockPosOff` = the end of the values. Appending it at the end keeps every Task 4 offset (prev/next/null/input) unchanged.
- **Dense positions:** a group of n entries uses positions 0..n-1. Chunk = pos >> 5, slot = pos & 31. Every chunk before the tail is full; the tail holds `((n-1) & 31) + 1` entries. `used = ceil(n/32)`, and `blockChunks ∈ {used, used+1}`. A spare (`used+1`) exists only while the tail is more than half full.
- **Bytes past the fill** (slots ≥ the tail fill, and the spare's slots) are stale and never read. Every byte change goes through `put()`, so checkpoint/restore reproduces them exactly anyway.

**Operations** (`Ctx` methods; a missing row is counted in `aggInconsistent` and never followed or created):
- **Append** (`blockAppend`, a new contributor or a move into g, after `applyContribution(+1)`). pos = n − 1. At pos 0 with no id, pick the id. Chunk `pos>>5`: if it is below `blockChunks`, it is the tail or the spare, so write in place; if it equals `blockChunks`, `put` a new chunk (`blockChunks++`); anything else is a gap (counted). Write `[key, values, nulls]` and store `blockPos`. That is exactly 1 block row.
- **In-place update** (`blockWrite`, same group, linked view). The position is unchanged. Check `pos < n`, that the chunk exists and that the slot's key equals c, then rewrite values and nulls. 1 block row.
- **Swap-remove** (`blockRemove`, move-out / tombstone / no-group, after `applyContribution(-1)`, before the old group's emit). `last = n'` (the post-remove count, which is the old last index). If `pos != last`, copy entry `last` into `pos` and patch the moved contributor m's `blockPos` through `linkedContributor(m)`. The tail chunk is only *read* (`get`), so it is not dirtied.
- **Chunk policy** (after the swap). If `n' == 0`, erase chunk 0 (plus at most one more under corruption, counted) and clear the id. Otherwise, if a spare exists and the tail fill is ≤ 16, erase the spare. When the tail empties it simply *becomes* the spare: no write and no erase. Hysteresis: ±1 churn at a 32-entry boundary never frees or re-adds a chunk; the spare goes only after 16 more removes. At most one chunk is freed per remove, and the invariant `spare ⇒ tail fill > 16` holds by induction.
- **Rescan** (`rescan`, at emit, only for RescanPending, or an Empty cache that saw an add with a non-null count above 1). Look up chunks `0..used-1` in batches of 8. The lookups are independent (computed keys, not links), so their misses overlap; each row's column is prefetched. Then scan `min(32, n − 32·idx)` entries per chunk: skip nulls, keep the best. The bookkeeping is as before: a missing chunk, `scanned != n` or `seen != nonNull` counts `aggInconsistent`, and the cache becomes Valid with what was found, or Empty.
- **The Min/Max cache and RescanPending stay unchanged.** Emit is O(1) unless the cached extreme was retracted; an add that is at least as extreme still resolves RescanPending without a scan.

**Dirty bound** (rows per source record per table = checkpoint delta entries; `put` dirties, `get` does not, an erase is one delete entry, an erase after a put in the same interval is still one entry). The worst case is a **move of c from g1 to g2** in a Min/Max view:
- **contributors:**
  - `c` (rewritten);
  - `prev(c)` and `next(c)` (unlink);
  - g2's old head (link);
  - **m**, g1's last entry, swap-moved into c's slot (its `blockPos`).

  That is **5** (Task 4's 4, plus m). They are distinct in the worst case: m can be any member of g1 other than c. In-place: c, prev, next, old head = 4 (no swap). Tombstone or no-group: c (delete), prev, next, m = 4.
- **blocks:**
  - g1's hole chunk (put);
  - g1's spare (erase, when the tail falls to 16);
  - g2's append chunk (put; a new chunk, the spare or the tail).

  That is **3**. The tail chunk is only read. When g1 empties: chunk 0 is erased (1, and the hole is not written) plus g2's append = 2. Under corruption the `n' == 0` cleanup erases at most 2 chunks and writes no hole, so it is still ≤ 3. In-place = 1. Tombstone = 2.
- **groups 2, view rows 2:** unchanged. The block fields live in the already-dirty group row.
- **Result:** Min/Max views **{5, 2, 2, 3} = 12 entries**. Last-only linked views **{4, 2, 2, 0} = 8** (unchanged from Task 4). 2a **{1, 2, 2, 0} = 5** (unchanged). The bound also holds under inconsistent state, because every damaged-path branch above writes no more rows than the healthy one, so a corrupt table can never make a capped pass overflow the ring.
- **Pass sizing:** `maxRecordsPerPassFor` (`WorkerViewSet.cpp`) becomes `fixed = delta hdr + crc + v2 hdr + T × table hdr` (T = 4 with a block, else 3), and `perRecord = Σ_tables bound × (13 + rowLen)`. For 2a and Last-only views `blockRows = 0` and T = 3, so the result is the old value exactly. For Task 7's `user_max` (Count(*), Max(amount), Last(amount)), the row lengths are 45 (contributor), 105 (group), 33 (view) and 544 (block). That gives 5·58 + 2·118 + 2·46 + 3·557 = **2289 B per record**, so **57 records per pass** at the 128 KiB delta message (it was 248 with the Last-only bound).

**Cost estimate** (Release, the bench's shape: 10k groups × ~100 entries, 1 column, so 4 chunk rows of 544 B per group, about 22 MB of blocks. The block index is about 40k keys, under 1 MB, so it stays cache-resident):
- **Rescan, 100 entries:** 4 independent `get()`s (index probe in L2/L3, ~10–40 ns; the row's first line is a DRAM miss, ~80 ns, and the misses overlap), plus about 20 cache lines of streamed values and nulls and 100 compares. That is **≈ 0.2–0.4 µs**, against ~21 µs for the list walk (50–100×). Pessimistic (no overlap): 4 × 150 ns + 0.1 µs ≈ 0.7 µs.
- **1k entries:** 32 chunks, **≈ 1.5–3 µs**. **10k:** 313 chunks, **≈ 15–30 µs** (was ~0.2 ms and ~2 ms). Reported, not gated.
- **Common path:**
  - An in-place update adds one block `put` (an index probe plus 1–2 row lines), **≈ +40–100 ns**.
  - An append adds one `put` (+ the id probe on a group's first entry).
  - A tombstone or move-out adds the hole `put`, a tail `get` and m's contributor `put` (a 1M-row table miss, ~100–150 ns), **≈ +150–300 ns**.
  - Over the bench's mix, p50 is expected to go from **0.75 → ~0.85–0.9 µs**, and a rescanning record from ~22 µs to ~1.2–1.5 µs, well inside record p99 < 5 µs and batch p99 < 10 µs.
- **Memory and checkpoint:**
  - +4 B per contributor and +8 B per group.
  - Blocks are about 1.1–2× (8 + 9·cols) bytes per entry. Small groups pay a full 32-entry chunk: 544 B per singleton group for one column.
  - Checkpoint deltas grow by one block row (557 B) per record that touches a Min/Max group, coalesced within a pass. See the risks in the design report.

**Pre-validated (2026-10-20, design author, throwaway worktree, not committed).** Steps 1–6 were applied exactly as written: they compile warning-free in a Debug build, and all 63 `AggRetractable*|AggFold*|AggState*|ViewFold*|ViewCheckpoint*` tests and the node suites (`WorkerViewSet|ViewRecovery|ViewReadiness|ViewSnapshot|GetViewRow|CreateView`) pass. In a Release build on the dev Mac (load average ~10, so noisy), `BM_ViewFoldRetractable` gave record p99 **1.71–1.83 µs**, batch p99 **4.63–4.88 µs**, record p50 **0.83 µs** (was 0.75), steady_alloc 0, rescans 102,947 => **PASS**; `BM_ViewFoldAggregate` still PASSes. Task 8's `BM_ViewFoldExtremeRetract` with the Task 5b patch gave N=1000 p50 1.46 µs / p99 1.71 µs and N=10000 p50 11.4 µs / p99 13.8 µs, `exact=yes`. The implementer's own Step 9 run is still the gate of record.

**Interaction matrix (Task 5b).** Op × block position / group-size state × timing. Each cell names its test (`model` = `BlockModelAcrossChunkBoundariesMatchesBruteForceRegression`; `Task 6 model` = `RandomizedOpsMatchBruteForceModelRegression` with the block checks Task 6 adds).

| Op ↓ / position, state → | chunk 0 slot 0 | last slot of a chunk (31, 63) | first slot of the next chunk (32, 64) | tail / last entry (no swap) | the swapped-in entry, later retracted |
|---|---|---|---|---|---|
| tombstone (swap-remove) | `ExtremeRetractionAtEveryBlockPosition…` | same | same | same; `ChunkBoundaryChurnKeepsOneSpare…` | `SwapRemovePatchesTheMovedContributorsPosition…` |
| in-place lower (rewrite at pos) | `ExtremeRetractionAtEveryBlockPosition…` | same | same | same | `SwapRemovePatches…` (in-place at the NEW position) |
| move out (swap-remove) + move in (append) | `ExtremeRetractionAtEveryBlockPosition…` | same | same | same | `ValueBlockSurvivesACheckpointRoundTrip…` (moves the swapped-in entry) |
| no-group rewrite (swap-remove) | Task 6 `NoGroupRewriteUnlinksAndRetractsTheContributorRegression` (asserts the block is freed) | model | model | model | model |
| append | first entry (id pick): every test; `BlockIdsProbeOnCollision…` | model | boundary +1 → new chunk: `ChunkBoundaryChurn…` (33rd entry) | spare reuse: `ChunkBoundaryChurn…` | model |

| Group-size state | Test |
|---|---|
| first entry: id = candidate | `BlockIdsProbeOnCollisionAndAreFreedWithTheGroupRegression` (golden: id == `blockIdCandidate(g)`) |
| id collision → linear probe | same test (birthday-searched colliding keys) |
| boundary −1: tail empties → spare kept; boundary +1: spare reused (1 block row, no erase, no new chunk), 5 rounds | `ChunkBoundaryChurnKeepsOneSpareRegression` |
| tail ≤ 16 → spare freed; > 16 → kept | `ChunkBoundaryChurnKeepsOneSpareRegression`, `WorstCaseMoveDirtiesExactlyTheBoundRegression` |
| n → 0: chunk 0 and id freed, id reclaimable | `BlockIdsProbeOnCollision…`, Task 6 `NoGroupRewrite…` |
| worst case reached: 5 contributor + 2 group + 2 view + 3 block | `WorstCaseMoveDirtiesExactlyTheBoundRegression` |
| damaged: chunk missing (rescan, in-place write); dangling list link with an intact block | `MissingBlockChunkIsCountedNeverCreatedRegression`; `DanglingLinkIsCountedNotFollowedRegression` (changed expectation) |
| null entries (Float NaN → null) in the block | model (`maxPrice`); Task 5 `NonFiniteMaxInputIsNullAndCountedRegression` (now via the block) |

| Timing | Test |
|---|---|
| single-record batches | all deterministic tests above |
| multi-record batches (1–4, same contributor can repeat), sizes sweeping the 16/32/48/64/80 thresholds | model (4000 steps; dirty ≤ records × bound per table; structure checked after every step) |
| capped-pass boundary (mid-batch resume) | Task 6 `applySplit` (extended to the block table) |
| checkpoint round trip (view level: v2 record with 4 tables → fresh tables → same history on both → byte-identical) | `ValueBlockSurvivesACheckpointRoundTripRegression` |
| worker restart (recovered block, then extreme retraction) | Task 7 `RestartThenRetractingTheMaxIsExactRegression` (asserts 4 tables in the record); e2e (Task 9) |

- [ ] **Step 1a: Failing layout tests.** In `broker/libs/view/tests/view/AggStateTest.cpp`:

In `AggStateRegression.TwoAOnlyLayoutIsUnchangedRegression`, after `EXPECT_EQ(b.viewRows, 2u);` add:

```cpp
    EXPECT_EQ(b.blockRows, 0u);
    EXPECT_EQ(a.numBlockCols, 0u);
    EXPECT_EQ(a.blockRow.rowLen, 0u) << "2a views have no value block";
```

In `AggState.RetractableLayoutAddsLinksHeadCachesAndPerInputWidths`, replace

```cpp
    EXPECT_EQ(a.contributorRow.rowLen, 86u);
```

with

```cpp
    EXPECT_EQ(a.contribBlockPosOff, 86u);                  // [blockPos u32] after the values
    EXPECT_EQ(a.contributorRow.rowLen, 90u);
```

and replace its tail, from `EXPECT_EQ(a.groupRow.rowLen, 256u + 4u * rune::view::kCacheEntryBytes);` to the closing `}`, with:

```cpp
    EXPECT_EQ(a.groupBlockOff, 256u + 4u * rune::view::kCacheEntryBytes);   // 292: after the caches
    EXPECT_EQ(a.groupRow.rowLen, 300u);                                      // + [blockId u32][blockChunks u32]
    // Value block: one column per DISTINCT Min/Max source field, in cache order:
    // amount (maxAmt + minAmt share it), price, qty.
    EXPECT_EQ(a.numBlockCols, 3u);
    EXPECT_EQ(a.blockColInput, (std::vector<uint16_t>{0, 3, 4}));
    EXPECT_EQ(a.cacheCol, (std::vector<uint16_t>{0, 0, 1, 2}));
    EXPECT_EQ(a.blockRow.rowLen, rune::view::kBlockEntries * (8u + 9u * 3u));   // 1120
    const auto b = rune::view::aggDirtyBoundPerRecord(a);
    EXPECT_EQ(b.contributorRows, 5u);
    EXPECT_EQ(b.groupRows, 2u);
    EXPECT_EQ(b.viewRows, 2u);
    EXPECT_EQ(b.blockRows, 3u);
}
```

Append to the file:

```cpp
// REGRESSION (Task 5b): a linked view with only Last/LastNonNull keeps Task 4's
// linked layout exactly -- no value block, no position, no block fields, bound {4,2,2,0}.
TEST(AggStateRegression, LastOnlyLinkedViewHasNoValueBlockRegression) {
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"last", P::Int64, 0, "amount", R::Last}};
    static const auto src = statsSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    const auto comp = rune::view::compileView(v, srcs, 9).value();
    const auto a = rune::view::buildAggLayout(comp.plan, src).value();
    EXPECT_TRUE(a.linked);
    EXPECT_EQ(a.numCached, 0u);
    EXPECT_EQ(a.numBlockCols, 0u);
    EXPECT_EQ(a.contributorRow.rowLen, 33u);   // gk 8 + prev 8 + next 8 + bitmap 1 + amount 8
    EXPECT_EQ(a.groupRow.rowLen, 64u);         // 8 + 2x16 + 2x8 + head 8
    EXPECT_EQ(a.blockRow.rowLen, 0u);
    const auto b = rune::view::aggDirtyBoundPerRecord(a);
    EXPECT_EQ(b.contributorRows, 4u);
    EXPECT_EQ(b.groupRows, 2u);
    EXPECT_EQ(b.viewRows, 2u);
    EXPECT_EQ(b.blockRows, 0u);
}

// The value-block chunk format is checkpointed: pin its constants and offsets.
TEST(AggState, ValueBlockFormatIsStable) {
    namespace v = rune::view;
    EXPECT_EQ(v::kBlockEntries, 32u);
    EXPECT_EQ(1u << v::kBlockShift, v::kBlockEntries);
    EXPECT_EQ(v::kBlockSpareFreeAt, 16u);
    EXPECT_EQ(v::kNoBlock, 0u);
    EXPECT_EQ(v::kAggTableBlocks, 3u);
    EXPECT_EQ(v::blockChunkKey(1, 0), uint64_t{1} << 32);
    EXPECT_EQ(v::blockChunkKey(0xFFFFFFFFu, 7), 0xFFFFFFFF00000007ull);
    EXPECT_EQ(v::blockKeyOff(31), 248u);
    EXPECT_EQ(v::blockValOff(0, 0), 256u);
    EXPECT_EQ(v::blockValOff(1, 3), 512u + 24u);
    const auto a = rune::view::buildAggLayout(compileStats().plan, statsSrc()).value();   // 3 columns
    EXPECT_EQ(v::blockNullOff(a, 0, 0), 8u * 32u * 4u);        // after keys + 3 value columns
    EXPECT_EQ(v::blockNullOff(a, 2, 31), 1024u + 64u + 31u);
    EXPECT_EQ(v::blockNullOff(a, 2, 31) + 1, a.blockRow.rowLen);
    for (uint64_t g = 0; g < 100000; ++g) ASSERT_NE(v::blockIdCandidate(g), v::kNoBlock) << g;
    EXPECT_EQ(v::blockIdCandidate(12345), v::blockIdCandidate(12345));
}
```

- [ ] **Step 1b: Harness support.** In `broker/libs/view/tests/view/AggRetractableTest.cpp`:

Add `#include <unordered_map>` to the includes.

In `struct Harness`, after `rune::view::ViewTable groups{agg.groupRow};` add:

```cpp
    rune::view::ViewTable blocks{agg.blockRow};   // userStats has Min/Max: 3 block columns
```

In `Harness::apply`, replace the fold call with:

```cpp
        last = rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view,
                                              contributors, groups, &blocks, lv, bound, cursor);
```

Replace `struct Drained { … };` and `drainCounts()` with:

```cpp
    struct Drained {
        uint32_t contributorRows = 0, groupRows = 0, viewRows = 0, blockRows = 0, pendingCaches = 0;
    };
    Drained drainCounts() {
        Drained d;
        contributors.drainDirty([&](uint32_t, const uint8_t*, bool, uint64_t) { ++d.contributorRows; });
        groups.drainDirty([&](uint32_t, const uint8_t* row, bool deleted, uint64_t) {
            ++d.groupRows;
            if (deleted) return;
            for (uint16_t ci = 0; ci < agg.numCached; ++ci) {
                if (row[agg.groupCacheOff + rune::view::kCacheEntryBytes * ci + 8] ==
                    static_cast<uint8_t>(rune::view::CacheState::RescanPending)) {
                    ++d.pendingCaches;
                }
            }
        });
        view.drainDirty([&](uint32_t, const uint8_t*, bool, uint64_t) { ++d.viewRows; });
        blocks.drainDirty([&](uint32_t, const uint8_t*, bool, uint64_t) { ++d.blockRows; });
        return d;
    }

    // Value-block accessors (Task 5b).
    uint32_t posOf(int64_t orderId) const {
        const uint8_t* c = contributors.get(uint64_t(orderId));
        EXPECT_NE(c, nullptr) << "contributor " << orderId;
        return c != nullptr ? rune::view::loadU32(c + agg.contribBlockPosOff) : UINT32_MAX;
    }
    uint32_t blockIdOf(int64_t userId) const {
        const uint8_t* g = groups.get(uint64_t(userId));
        return g != nullptr ? rune::view::loadU32(g + agg.groupBlockOff) : rune::view::kNoBlock;
    }
    uint32_t chunksOf(int64_t userId) const {
        const uint8_t* g = groups.get(uint64_t(userId));
        return g != nullptr ? rune::view::loadU32(g + agg.groupBlockOff + 4) : 0u;
    }

    // The value-block invariant (AggState.h), checked for `groupIds`:
    //  - a group with n entries has a block id and blockChunks == ceil(n/32), or one
    //    more (a spare) only while the tail chunk holds more than 16 entries;
    //  - entry p (chunk p/32, slot p%32) names a contributor of that group whose
    //    stored blockPos is p and whose Min/Max inputs equal the entry's values/nulls;
    //  - no chunk row exists past blockChunks, and the block table holds exactly the
    //    listed groups' chunks (nothing leaked).
    // Returns the number of entries seen (== live contributors when every group is listed).
    size_t expectBlocksConsistent(const std::vector<int64_t>& groupIds) {
        namespace v = rune::view;
        size_t entries = 0, chunkRows = 0;
        for (const int64_t g : groupIds) {
            const uint8_t* gr = groups.get(uint64_t(g));
            if (gr == nullptr) continue;
            const uint64_t n = v::loadU64(gr);
            const uint32_t id = v::loadU32(gr + agg.groupBlockOff);
            const uint32_t chunks = v::loadU32(gr + agg.groupBlockOff + 4);
            EXPECT_NE(id, v::kNoBlock) << "group " << g;
            const uint32_t used = static_cast<uint32_t>((n + v::kBlockEntries - 1) / v::kBlockEntries);
            const uint32_t tailFill = static_cast<uint32_t>(n - uint64_t(used - 1) * v::kBlockEntries);
            EXPECT_TRUE(chunks == used || (chunks == used + 1 && tailFill > v::kBlockSpareFreeAt))
                << "group " << g << " n=" << n << " chunks=" << chunks;
            for (uint64_t p = 0; p < n; ++p) {
                const uint8_t* row = blocks.get(v::blockChunkKey(id, uint32_t(p / v::kBlockEntries)));
                EXPECT_NE(row, nullptr) << "group " << g << " entry " << p;
                if (row == nullptr) continue;
                const uint32_t s = uint32_t(p % v::kBlockEntries);
                const uint64_t c = v::loadU64(row + v::blockKeyOff(s));
                const uint8_t* cr = contributors.get(c);
                EXPECT_NE(cr, nullptr) << "group " << g << " entry " << p << " names missing " << c;
                if (cr == nullptr) continue;
                EXPECT_EQ(v::loadU64(cr), uint64_t(g)) << "contributor " << c;
                EXPECT_EQ(v::loadU32(cr + agg.contribBlockPosOff), uint32_t(p)) << "contributor " << c;
                for (uint16_t col = 0; col < agg.numBlockCols; ++col) {
                    const uint16_t in = agg.blockColInput[col];
                    const bool isNull = ((cr[agg.contribNullOff + in / 8] >> (in % 8)) & 1u) != 0;
                    EXPECT_EQ(row[v::blockNullOff(agg, col, s)], isNull ? 1 : 0) << "contributor " << c;
                    EXPECT_EQ(std::memcmp(row + v::blockValOff(col, s), cr + agg.inputOff[in], 8), 0)
                        << "contributor " << c << " column " << col;
                }
                ++entries;
            }
            for (uint32_t k = chunks; k < chunks + 2; ++k)
                EXPECT_EQ(blocks.get(v::blockChunkKey(id, k)), nullptr) << "group " << g << " leaked chunk " << k;
            chunkRows += chunks;
        }
        EXPECT_EQ(blocks.size(), chunkRows) << "every chunk row belongs to a listed group";
        return entries;
    }
```

After the closing `};` of `Harness` (still inside the anonymous namespace), add the checkpoint helpers:

```cpp
// The v2 record a worker would checkpoint from the four tables (rows copied:
// drainDirty's pointer is valid only inside its callback). Drains every table.
std::vector<uint8_t> checkpointAll(Harness& h) {
    rune::view::ViewTable* ts[4] = {&h.view, &h.contributors, &h.groups, &h.blocks};
    const uint32_t lens[4] = {h.comp.rowLayout.rowLen, h.agg.contributorRow.rowLen,
                              h.agg.groupRow.rowLen, h.agg.blockRow.rowLen};
    std::vector<std::vector<uint8_t>> bytes[4];
    std::vector<rune::view::DirtyEntry> entries[4];
    for (int t = 0; t < 4; ++t) {
        ts[t]->drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
            bytes[t].push_back(deleted ? std::vector<uint8_t>{}
                                       : std::vector<uint8_t>(row, row + lens[t]));
            entries[t].push_back({rid, deleted, key, nullptr});
        });
        for (size_t i = 0; i < entries[t].size(); ++i)
            if (!entries[t][i].deleted) entries[t][i].row = bytes[t][i].data();
    }
    const rune::view::TableDelta tds[4] = {
        {rune::view::kAggTableView, lens[0], entries[0]},
        {rune::view::kAggTableContributors, lens[1], entries[1]},
        {rune::view::kAggTableGroups, lens[2], entries[2]},
        {rune::view::kAggTableBlocks, lens[3], entries[3]}};
    std::vector<uint8_t> out;
    rune::view::encodeMultiDelta(tds, h.next, out);
    return out;
}

// Recovery into fresh tables, then the worker's post-recovery dirty discard.
void restoreInto(Harness& h, const std::vector<uint8_t>& rec, int64_t next, int64_t cursor) {
    rune::view::ViewTable* ts[4] = {&h.view, &h.contributors, &h.groups, &h.blocks};
    const uint32_t lens[4] = {h.comp.rowLayout.rowLen, h.agg.contributorRow.rowLen,
                              h.agg.groupRow.rowLen, h.agg.blockRow.rowLen};
    ASSERT_TRUE(rune::view::applyMultiDelta(ts, lens, rec).has_value());
    h.next = next;
    h.cursor = cursor;
    (void)h.drainCounts();
}

// Same groups, contributors and chunk rows, byte for byte.
void expectSameState(Harness& a, Harness& b, const std::vector<int64_t>& groupIds, int64_t maxId) {
    for (const int64_t g : groupIds) {
        const uint8_t* ga = a.groups.get(uint64_t(g));
        const uint8_t* gb = b.groups.get(uint64_t(g));
        ASSERT_EQ(ga == nullptr, gb == nullptr) << "group " << g;
        if (ga == nullptr) continue;
        EXPECT_EQ(std::memcmp(ga, gb, a.agg.groupRow.rowLen), 0) << "group row " << g;
        const uint8_t* va = a.view.get(uint64_t(g));
        const uint8_t* vb = b.view.get(uint64_t(g));
        ASSERT_TRUE(va != nullptr && vb != nullptr) << "view row " << g;
        EXPECT_EQ(std::memcmp(va, vb, a.comp.rowLayout.rowLen), 0) << "view row " << g;
        const uint32_t id = rune::view::loadU32(ga + a.agg.groupBlockOff);
        const uint32_t chunks = rune::view::loadU32(ga + a.agg.groupBlockOff + 4);
        for (uint32_t k = 0; k < chunks; ++k) {
            const uint8_t* ba = a.blocks.get(rune::view::blockChunkKey(id, k));
            const uint8_t* bb = b.blocks.get(rune::view::blockChunkKey(id, k));
            ASSERT_TRUE(ba != nullptr && bb != nullptr) << "group " << g << " chunk " << k;
            EXPECT_EQ(std::memcmp(ba, bb, a.agg.blockRow.rowLen), 0) << "group " << g << " chunk " << k;
        }
    }
    for (int64_t c = 1; c <= maxId; ++c) {
        const uint8_t* ca = a.contributors.get(uint64_t(c));
        const uint8_t* cb = b.contributors.get(uint64_t(c));
        ASSERT_EQ(ca == nullptr, cb == nullptr) << "contributor " << c;
        if (ca != nullptr) EXPECT_EQ(std::memcmp(ca, cb, a.agg.contributorRow.rowLen), 0) << "contributor " << c;
    }
    EXPECT_EQ(a.blocks.size(), b.blocks.size());
}
```

- [ ] **Step 1c: Change one expectation, replace the tautological pin, add the block tests.**

In `AggRetractableRegression.DanglingLinkIsCountedNotFollowedRegression`, replace

```cpp
    EXPECT_EQ(h.col(100, kMaxAmt).i, 20) << "the rescan only reports values it actually found";
```

with

```cpp
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30)
        << "the rescan reads the VALUE BLOCK, which still holds contributor 2's 30 (the group still counts 2); "
           "the damaged list is never followed";
```

(The rest of the test holds: the missing head and neighbour are counted, `Last` is 20, and row 2 is never re-created.)

Delete `AggRetractableRegression.WorstCaseMoveDirtiesExactlyTheBoundAndFitsTheBudgetRegression` (the whole test and its comment) and append:

```cpp
// REGRESSION (Task 5 review item 1; Task 5b): the per-record bound is REACHED, on
// every table, by one move: a mid-list contributor at block position 0 of a group
// whose spare chunk is freed by the remove, moved into a group with a head.
// A pure dirty-count pin (the byte budget is pinned by Task 7's ring test).
TEST(AggRetractableRegression, WorstCaseMoveDirtiesExactlyTheBoundRegression) {
    Harness h;
    for (int64_t id = 1; id <= 65; ++id) h.putOrd({id, 100, id});   // 65 entries: chunks 0, 1, 2
    for (int64_t id = 65; id >= 50; --id) h.put(id, tombstone());   // 49: chunk 2 is the spare, tail holds 17
    ASSERT_EQ(h.chunksOf(100), 3u);
    h.putOrd({100, 200, 7});                                        // u200 has a head (100)
    h.putOrd({3, 100, 3});
    h.putOrd({1, 100, 1});
    h.putOrd({2, 100, 2});                                          // u100 list: 2 -> 1 -> 3 -> 49 -> ...
    ASSERT_EQ(h.posOf(1), 0u);
    (void)h.drainCounts();
    h.putOrd({1, 200, 50});                                         // move 1 (pos 0, prev 2, next 3) to u200
    const auto b = rune::view::aggDirtyBoundPerRecord(h.agg);
    ASSERT_EQ(b.contributorRows, 5u);
    ASSERT_EQ(b.groupRows, 2u);
    ASSERT_EQ(b.viewRows, 2u);
    ASSERT_EQ(b.blockRows, 3u);
    const auto d = h.drainCounts();
    EXPECT_EQ(d.contributorRows, b.contributorRows) << "1, its neighbours 2 and 3, u200's old head 100, "
                                                       "and 49 (u100's last entry, swapped into position 0)";
    EXPECT_EQ(d.groupRows, b.groupRows);
    EXPECT_EQ(d.viewRows, b.viewRows);
    EXPECT_EQ(d.blockRows, b.blockRows) << "u100's hole chunk 0, u100's spare (tail now 16: freed), u200's chunk 0";
    EXPECT_EQ(d.pendingCaches, 0u);
    EXPECT_EQ(h.posOf(49), 0u);
    EXPECT_EQ(h.chunksOf(100), 2u);
    EXPECT_EQ(h.col(100, kMinAmt).i, 2) << "the moved min rescans the block";
    EXPECT_EQ(h.col(100, kMaxAmt).i, 49);
    EXPECT_EQ(h.col(200, kMaxAmt).i, 50);
    EXPECT_EQ(h.col(200, kMinAmt).i, 7);
    h.expectBlocksConsistent({100, 200});
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Task 5b): removing a mid-block entry moves the group's LAST entry
// into the hole and patches that contributor's stored position; the moved entry
// is then rewritten in place and retracted at its NEW position, exactly.
TEST(AggRetractableRegression, SwapRemovePatchesTheMovedContributorsPositionRegression) {
    Harness h;
    for (int64_t id = 1; id <= 40; ++id) h.putOrd({id, 100, 10 * id});   // 2 chunks: 32 + 8
    h.put(3, tombstone());                                                // hole at 2; 40 moves there
    EXPECT_EQ(h.posOf(40), 2u);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 400);
    h.expectBlocksConsistent({100});
    h.putOrd({40, 100, 5});                                               // in place at position 2
    EXPECT_EQ(h.posOf(40), 2u);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 390) << "the retracted max rescans the block";
    EXPECT_EQ(h.col(100, kMinAmt).i, 5);
    h.put(40, tombstone());                                               // 39 (the last) moves to 2
    EXPECT_EQ(h.posOf(39), 2u);
    EXPECT_EQ(h.col(100, kMinAmt).i, 10);
    EXPECT_EQ(h.col(100, kN).i, 38);
    h.expectBlocksConsistent({100});
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Task 5b, chunk policy): at a chunk boundary the block keeps ONE
// spare, so +-1 churn there never frees or re-adds a chunk; the spare is freed
// only once the tail chunk holds at most 16 entries.
TEST(AggRetractableRegression, ChunkBoundaryChurnKeepsOneSpareRegression) {
    Harness h;
    for (int64_t id = 1; id <= 33; ++id) h.putOrd({id, 100, id});      // the 33rd entry adds chunk 1
    EXPECT_EQ(h.chunksOf(100), 2u);
    EXPECT_EQ(h.blocks.size(), 2u);
    for (int round = 0; round < 5; ++round) {
        h.put(33, tombstone());                                        // 32: chunk 1 empties -> the spare
        EXPECT_EQ(h.chunksOf(100), 2u) << "round " << round;
        EXPECT_EQ(h.blocks.size(), 2u) << "round " << round;
        EXPECT_EQ(h.col(100, kMaxAmt).i, 32);
        (void)h.drainCounts();
        h.putOrd({33, 100, 33});                                       // 33 again: into the spare
        const auto d = h.drainCounts();
        EXPECT_EQ(d.blockRows, 1u) << "round " << round << ": one write, no erase, no new chunk";
        EXPECT_EQ(h.blocks.size(), 2u);
        EXPECT_EQ(h.col(100, kMaxAmt).i, 33);
    }
    for (int64_t id = 33; id >= 18; --id) h.put(id, tombstone());     // 17 left: the tail holds 17 > 16
    EXPECT_EQ(h.chunksOf(100), 2u) << "the spare survives while the tail is more than half full";
    h.put(17, tombstone());                                            // 16: the spare is freed
    EXPECT_EQ(h.chunksOf(100), 1u);
    EXPECT_EQ(h.blocks.size(), 1u);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 16);
    EXPECT_EQ(h.col(100, kMinAmt).i, 1);
    h.expectBlocksConsistent({100});
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Task 5b, op x position): retracting the cached Max is exact whether
// its holder sits at chunk 0 slot 0, the last slot of a chunk, the first slot of
// the next chunk or the group's last entry -- by tombstone, in-place lower and
// group move.
TEST(AggRetractableRegression, ExtremeRetractionAtEveryBlockPositionIsExactRegression) {
    auto mx = [](const std::map<int64_t, int64_t>& m) {
        int64_t v = INT64_MIN; for (const auto& [k, a] : m) v = std::max(v, a); return v;
    };
    auto mn = [](const std::map<int64_t, int64_t>& m) {
        int64_t v = INT64_MAX; for (const auto& [k, a] : m) v = std::min(v, a); return v;
    };
    for (const uint32_t p : {0u, 31u, 32u, 63u, 64u, 99u}) {
        for (int how = 0; how < 3; ++how) {   // 0 tombstone, 1 in-place lower, 2 move to u200
            Harness h;
            std::map<int64_t, int64_t> g100, g200;
            for (int64_t id = 1; id <= 100; ++id) {
                const int64_t amt = uint32_t(id - 1) == p ? 1000 : id;
                h.putOrd({id, 100, amt});
                g100[id] = amt;
            }
            h.putOrd({500, 200, 7});
            g200[500] = 7;
            const int64_t holder = int64_t(p) + 1;
            ASSERT_EQ(h.posOf(holder), p);
            if (how == 0) { h.put(holder, tombstone()); g100.erase(holder); }
            else if (how == 1) { h.putOrd({holder, 100, -5}); g100[holder] = -5; }
            else { h.putOrd({holder, 200, 1000}); g100.erase(holder); g200[holder] = 1000; }
            EXPECT_GE(h.last.aggRescans, 1u) << "pos " << p << " how " << how;
            EXPECT_EQ(h.col(100, kMaxAmt).i, mx(g100)) << "pos " << p << " how " << how;
            EXPECT_EQ(h.col(100, kMinAmt).i, mn(g100)) << "pos " << p << " how " << how;
            EXPECT_EQ(h.col(200, kMaxAmt).i, mx(g200)) << "pos " << p << " how " << how;
            EXPECT_EQ(h.col(200, kMinAmt).i, mn(g200)) << "pos " << p << " how " << how;
            h.expectBlocksConsistent({100, 200});
            EXPECT_EQ(h.inconsistent, 0u) << "pos " << p << " how " << how;
        }
    }
}

// REGRESSION (Task 5b): a group's block id is its candidate unless that id is
// taken (linear probe); an emptied group frees every chunk row and its id, and a
// re-created group reclaims its candidate. Two keys with the same candidate never
// share chunk rows.
TEST(AggRetractableRegression, BlockIdsProbeOnCollisionAndAreFreedWithTheGroupRegression) {
    std::unordered_map<uint32_t, int64_t> seen;   // deterministic birthday search for a collision
    int64_t g1 = 0, g2 = 0;
    for (int64_t g = 1; g2 == 0; ++g) {
        const auto [it, fresh] = seen.emplace(rune::view::blockIdCandidate(uint64_t(g)), g);
        if (!fresh) { g1 = it->second; g2 = g; }
    }
    const uint32_t cand = rune::view::blockIdCandidate(uint64_t(g1));
    const uint32_t probed = cand == UINT32_MAX ? 1u : cand + 1;
    Harness h;
    for (int64_t id = 1; id <= 40; ++id) h.putOrd({id, g1, id});
    for (int64_t id = 41; id <= 80; ++id) h.putOrd({id, g2, id});
    EXPECT_EQ(h.blockIdOf(g1), cand);
    EXPECT_EQ(h.blockIdOf(g2), probed) << "linear probe past the taken candidate";
    EXPECT_EQ(h.blocks.size(), 4u);
    EXPECT_EQ(h.col(g1, kMaxAmt).i, 40);
    EXPECT_EQ(h.col(g2, kMinAmt).i, 41);
    h.expectBlocksConsistent({g1, g2});
    for (int64_t id = 1; id <= 40; ++id) h.put(id, tombstone());     // g1 empties
    EXPECT_FALSE(h.hasGroup(g1));
    EXPECT_EQ(h.blocks.size(), 2u) << "every chunk row of g1 is freed";
    EXPECT_EQ(h.blocks.get(rune::view::blockChunkKey(cand, 0)), nullptr);
    h.putOrd({1, g1, 9});                                            // g1 again: its candidate is free
    EXPECT_EQ(h.blockIdOf(g1), cand);
    EXPECT_EQ(h.col(g1, kMaxAmt).i, 9);
    EXPECT_EQ(h.col(g2, kMaxAmt).i, 80) << "g2's block was never touched";
    h.expectBlocksConsistent({g1, g2});
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Task 5b, damaged block): a missing chunk row is counted, never
// followed and never re-created -- not by the rescan, not by an in-place write.
TEST(AggRetractableRegression, MissingBlockChunkIsCountedNeverCreatedRegression) {
    Harness h;
    for (int64_t id = 1; id <= 40; ++id) h.putOrd({id, 100, id});    // chunk 0: 1..32, chunk 1: 33..40
    const uint64_t k1 = rune::view::blockChunkKey(h.blockIdOf(100), 1);
    ASSERT_TRUE(h.blocks.erase(k1));                                 // damage: chunk 1 vanishes
    h.put(40, tombstone());                                          // retract the max (the last entry)
    EXPECT_GE(h.last.aggInconsistent, 1u) << "the rescan meets the missing chunk";
    EXPECT_EQ(h.col(100, kMaxAmt).i, 32) << "only values actually found are reported";
    EXPECT_EQ(h.blocks.get(k1), nullptr) << "the rescan never creates a chunk";
    h.putOrd({35, 100, 99});                                         // in place inside the missing chunk
    EXPECT_GE(h.last.aggInconsistent, 1u);
    EXPECT_EQ(h.blocks.get(k1), nullptr) << "an in-place write never creates a chunk";
    EXPECT_EQ(h.col(100, kMaxAmt).i, 99) << "the Valid cache still takes a strictly better value, O(1)";
    EXPECT_EQ(h.col(100, kN).i, 39);
}

// REGRESSION (Task 5b, timing "across restart" at the view level): the value
// block travels in the v2 record as table 3; recovered tables then behave
// byte-for-byte like the live ones under the same history (swap-removes, a move,
// extreme retractions, a spare freed).
TEST(AggRetractableRegression, ValueBlockSurvivesACheckpointRoundTripRegression) {
    Harness live;
    for (int64_t id = 1; id <= 70; ++id) live.putOrd({id, 100, id});   // 3 chunks
    for (int64_t id = 70; id >= 61; --id) live.put(id, tombstone());   // 60 left: chunk 2 is the spare
    for (int64_t id = 71; id <= 75; ++id) live.putOrd({id, 200, id});
    live.putOrd({7, 100, 7});                                          // an in-place rewrite before the checkpoint
    ASSERT_EQ(live.chunksOf(100), 3u);
    const auto rec = checkpointAll(live);
    ASSERT_EQ(rec[12], 4u) << "the v2 record carries four tables";
    Harness back;
    restoreInto(back, rec, live.next, live.cursor);
    expectSameState(live, back, {100, 200}, 75);
    auto both = [&](auto&& op) {
        op(live);
        op(back);
        expectSameState(live, back, {100, 200}, 75);
    };
    both([](Harness& h) { h.put(60, tombstone()); });                  // retract the max (the last entry)
    both([](Harness& h) { h.put(1, tombstone()); });                   // retract the min at chunk 0 slot 0: 59 swaps in
    both([](Harness& h) { h.putOrd({59, 200, 1}); });                  // move the swapped-in entry out
    for (int64_t id = 2; id <= 13; ++id) both([id](Harness& h) { h.put(id, tombstone()); });
    EXPECT_EQ(back.chunksOf(100), 2u) << "the tail fell to 16: the recovered spare was freed";
    EXPECT_EQ(back.col(100, kMaxAmt).i, 58);
    EXPECT_EQ(back.col(100, kMinAmt).i, 14);
    EXPECT_EQ(back.col(200, kMinAmt).i, 1);
    EXPECT_EQ(back.col(200, kMaxAmt).i, 75);
    back.expectBlocksConsistent({100, 200});
    EXPECT_EQ(back.inconsistent, 0u);
    EXPECT_EQ(live.inconsistent, 0u);
}

// REGRESSION (Task 5b, op x block position x timing): 4000 steps of 1-4 records
// over 160 contributors / 3 groups (group 1 is three times as likely, and the
// tombstone rate alternates 20%/45% every 500 steps: group 1 ranges over ~33-93
// entries (across the 64 boundary and both spare thresholds, 48 and 80), groups
// 2/3 over ~8-39 (across 16 and 32), so swap-removes, spare reuse, spare frees
// and new chunks all occur), heavily duplicated amounts, Float NaN (null)
// prices; group moves happen whenever a rewrite picks another group.
// After EVERY step: Count/Max/Min/MaxPrice of every group equal a brute-force
// recomputation, the block structure is consistent and covers every live
// contributor, every table's dirty entries are within records x the bound, no
// cache is left RescanPending, nothing is inconsistent.
TEST(AggRetractableRegression, BlockModelAcrossChunkBoundariesMatchesBruteForceRegression) {
    struct M { int64_t user; int64_t amount; double price; };
    std::map<int64_t, M> live;
    Harness h;
    const auto bound = rune::view::aggDirtyBoundPerRecord(h.agg);
    uint64_t state = 0xD1B54A32D192ED03ull;
    auto rnd = [&](uint64_t m) {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        return (state >> 33) % m;
    };
    for (uint64_t step = 1; step <= 4000; ++step) {
        const size_t n = 1 + static_cast<size_t>(rnd(4));
        const uint64_t delPct = (step / 500) % 2 == 0 ? 20 : 45;
        std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> recs;
        for (size_t i = 0; i < n; ++i) {
            const int64_t id = 1 + static_cast<int64_t>(rnd(160));
            if (rnd(100) < delPct) {
                recs.push_back({Harness::key(id), tombstone()});
                live.erase(id);
            } else {
                const uint64_t r = rnd(5);
                Ord o{id, r < 3 ? 1 : static_cast<int64_t>(r - 1), static_cast<int64_t>(rnd(21)) - 10};
                o.price = rnd(5) == 0 ? std::numeric_limits<double>::quiet_NaN() : static_cast<double>(rnd(7));
                recs.push_back({Harness::key(id), encode(o)});
                live[id] = M{o.user, o.amount, o.price};
            }
        }
        h.apply(recs);
        const auto d = h.drainCounts();
        ASSERT_LE(d.contributorRows, n * bound.contributorRows) << "step " << step;
        ASSERT_LE(d.groupRows, n * bound.groupRows) << "step " << step;
        ASSERT_LE(d.viewRows, n * bound.viewRows) << "step " << step;
        ASSERT_LE(d.blockRows, n * bound.blockRows) << "step " << step;
        ASSERT_EQ(d.pendingCaches, 0u) << "step " << step;
        for (int64_t g = 1; g <= 3; ++g) {
            int64_t cnt = 0, mx = INT64_MIN, mn = INT64_MAX;
            double mp = 0.0;
            bool anyPrice = false;
            for (const auto& [k, m] : live) {
                if (m.user != g) continue;
                ++cnt;
                mx = std::max(mx, m.amount);
                mn = std::min(mn, m.amount);
                if (!std::isnan(m.price)) { mp = anyPrice ? std::max(mp, m.price) : m.price; anyPrice = true; }
            }
            if (cnt == 0) { ASSERT_FALSE(h.hasGroup(g)) << "step " << step << " group " << g; continue; }
            ASSERT_EQ(h.col(g, kN).i, cnt) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kMaxAmt).i, mx) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kMinAmt).i, mn) << "step " << step << " group " << g;
            const auto p = h.col(g, kMaxPrice);
            ASSERT_EQ(p.present, anyPrice) << "step " << step << " group " << g;
            if (anyPrice) ASSERT_DOUBLE_EQ(p.f, mp) << "step " << step << " group " << g;
        }
        ASSERT_EQ(h.expectBlocksConsistent({1, 2, 3}), live.size()) << "step " << step;
        if (::testing::Test::HasFailure()) FAIL() << "first failure at step " << step;
    }
    EXPECT_EQ(h.inconsistent, 0u);
}
```

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build/root -j14 --target rune_view_tests`
Expected: compile errors `no member named 'blockRows'` / `'numBlockCols'` / `'blockChunkKey'`, and no matching `foldAggregateBatch` overload taking `&blocks`.

- [ ] **Step 3: Implement the layout.** In `broker/libs/view/include/rune/view/AggState.h`:

Replace the header comment block (from `// Per (view, partition) the fold keeps two fixed-width auxiliary ViewTables` through `// Both tables are checkpointed with the output rows in one v2 record (§9.2).`) with:

```cpp
// Per (view, partition) the fold keeps fixed-width auxiliary ViewTables next
// to the output rows ({...} = only in a LINKED view, i.e. one using any Phase
// 2b rule; <...> = only when the view has a Min or Max; a 2a-only view keeps
// its 2a layout byte-for-byte):
//   contributors : contributorKey -> [groupKey u64]{[prev u64][next u64]}[null bitmap][values]<[blockPos u32]>
//   groups       : groupKey -> [contributors u64][accumulators][non-null counts]
//                              {[head u64][per Min/Max: value 8B, state u8]}<[blockId u32][blockChunks u32]>
//   blocks       : <blockChunkKey(blockId, chunk) -> [keys u64 x 32][per column: values 8B x 32][per column: nulls u8 x 32]>
// prev/next/head form a doubly linked MRU list of the group's contributors,
// keys as links (kNoContributor ends it): every write of a contributor moves it
// to the head, so Last = head, LastNonNull = first non-null from the head.
// The value block (Task 5b) holds each group's Min/Max inputs densely at
// positions 0..n-1 (32 per chunk row; every chunk but the tail full; at most one
// spare chunk), one column per distinct Min/Max source field: a rescan of a
// retracted extreme is a sequential scan of ceil(n/32) chunks, not a list walk.
// Add appends; an in-place update rewrites its entry; a remove swap-removes (the
// last entry moves into the hole, its contributor's blockPos is patched); the
// spare is freed once the tail holds <= 16 entries (hysteresis). A group's
// blockId is chosen at its first entry (blockIdCandidate, then linear probing);
// an id is in use iff its chunk 0 exists. All tables are checkpointed with the
// output rows in one v2 record (§9.2; blocks = table id 3).
```

Directly after `inline constexpr uint32_t kCacheEntryBytes = 9;` add:

```cpp
// --- Min/Max value block (Task 5b) ---
inline constexpr uint32_t kBlockEntries     = 32;                 // entries per chunk row
inline constexpr uint32_t kBlockShift       = 5;
inline constexpr uint32_t kBlockMask        = kBlockEntries - 1;
static_assert((1u << kBlockShift) == kBlockEntries, "kBlockEntries must be 1 << kBlockShift");
inline constexpr uint32_t kBlockSpareFreeAt = kBlockEntries / 2;  // free the spare once the tail holds <= this
inline constexpr uint32_t kNoBlock          = 0;                  // group row blockId: no block
inline constexpr uint32_t kBlockIdProbes    = 64;                 // linear-probe limit for a free block id

// Exact chunk-row key: a 32-bit block id (per group) and a 32-bit chunk index.
inline constexpr uint64_t blockChunkKey(uint32_t blockId, uint32_t chunk) noexcept {
    return (static_cast<uint64_t>(blockId) << 32) | chunk;
}
// First block id tried for group key g (fixed 64-bit finalizer, low 32 bits;
// never kNoBlock). Collisions are resolved by linear probing in the fold.
inline constexpr uint32_t blockIdCandidate(uint64_t g) noexcept {
    g ^= g >> 33; g *= 0xff51afd7ed558ccdULL;
    g ^= g >> 33; g *= 0xc4ceb9fe1a85ec53ULL;
    g ^= g >> 33;
    const uint32_t id = static_cast<uint32_t>(g);
    return id == kNoBlock ? 1u : id;
}
// Chunk-row offsets (column-wise). slot = position & kBlockMask.
inline constexpr uint32_t blockKeyOff(uint32_t slot) noexcept { return 8u * slot; }
inline constexpr uint32_t blockValOff(uint16_t col, uint32_t slot) noexcept {
    return 8u * kBlockEntries * (1u + col) + 8u * slot;
}
```

In `struct AggLayout`, after `uint32_t groupCacheOff = 0; // linked only` add:

```cpp
    // --- Phase 2b Task 5b: Min/Max value block (all 0/empty when numBlockCols == 0) ---
    uint16_t              numBlockCols       = 0;   // distinct Min/Max source fields
    std::vector<uint16_t> blockColInput;            // per column: the input whose 8-byte value it copies
    std::vector<uint16_t> cacheCol;                 // per Min/Max cache index: its column
    uint32_t              contribBlockPosOff = 0;   // [blockPos u32] at the end of the contributor row
    uint32_t              groupBlockOff      = 0;   // [blockId u32][blockChunks u32] after the caches
    RowLayout             blockRow;                 // only rowLen: kBlockEntries * (8 + 9 * numBlockCols)
```

Directly after the closing `};` of `AggLayout` add:

```cpp
// Null-flag byte of (column, slot) in a chunk row.
inline uint32_t blockNullOff(const AggLayout& a, uint16_t col, uint32_t slot) noexcept {
    return 8u * kBlockEntries * (1u + a.numBlockCols) + kBlockEntries * col + slot;
}
```

Replace the `AggDirtyBound` comment and struct with:

```cpp
// Worst-case rows ONE source record can dirty in each table (= checkpoint
// delta entries). Min/Max views: a group move dirties the contributor, both of
// its old neighbours, the new group's old head and the old group's swap-moved
// last entry (5); both groups (2); both view rows (2); the old group's hole
// chunk and freed spare plus the new group's append chunk (3). Last-only linked
// views {4,2,2,0}; 2a {1,2,2,0}. A rescan only reads (get() never dirties); a
// damaged-state branch never writes more than the healthy one. Derivation:
// Phase 2b plan Task 5b; pinned by AggRetractableRegression.WorstCaseMoveDirtiesExactlyTheBoundRegression.
struct AggDirtyBound {
    uint32_t contributorRows;
    uint32_t groupRows;
    uint32_t viewRows;
    uint32_t blockRows;
};
```

After `inline constexpr uint8_t kAggTableGroups = 2;` add:

```cpp
inline constexpr uint8_t kAggTableBlocks       = 3;   // only views with Min/Max (4 tables)
```

After the `storeU64` helper add:

```cpp
inline uint32_t loadU32(const uint8_t* p) noexcept { uint32_t v; std::memcpy(&v, p, 4); return v; }
inline void     storeU32(uint8_t* p, uint32_t v) noexcept { std::memcpy(p, &v, 4); }
```

In `broker/libs/view/src/AggState.cpp`, replace everything in `buildAggLayout` from `const uint32_t nFields = static_cast<uint32_t>(a.fields.size());` through `return a;` with:

```cpp
    // Task 5b: one value-block column per DISTINCT Min/Max source field, in
    // cache order (Min(x) and Max(x) read the same widened value and null bit).
    {
        std::vector<uint16_t> colSrc;
        for (const AggField& f : a.fields) {
            if (f.cache == kNoCache) continue;
            uint16_t col = 0;
            while (col < colSrc.size() && colSrc[col] != f.srcFieldId) ++col;
            if (col == colSrc.size()) {
                colSrc.push_back(f.srcFieldId);
                a.blockColInput.push_back(f.input);
            }
            a.cacheCol.push_back(col);
        }
        a.numBlockCols = static_cast<uint16_t>(colSrc.size());
    }
    const bool block = a.numBlockCols > 0;
    const uint32_t nFields = static_cast<uint32_t>(a.fields.size());
    a.contribPrevOff = a.linked ? 8u : 0u;
    a.contribNextOff = a.linked ? 16u : 0u;
    a.contribNullOff = a.linked ? 24u : 8u;
    a.contribValOff  = a.contribNullOff + (static_cast<uint32_t>(a.numInputs) + 7) / 8;
    uint32_t off = a.contribValOff;
    a.inputOff.reserve(a.inputWidth.size());
    for (const uint16_t w : a.inputWidth) {
        a.inputOff.push_back(off);
        off += w;
    }
    if (block) {
        a.contribBlockPosOff = off;
        off += 4;
    }
    a.contributorRow.rowLen = off;
    a.groupAccOff     = 8;
    a.groupNonNullOff = 8 + 16u * nFields;
    const uint32_t nnEnd = a.groupNonNullOff + 8u * nFields;
    if (a.linked) {
        a.groupHeadOff      = nnEnd;
        a.groupCacheOff     = nnEnd + 8;
        a.groupRow.rowLen   = a.groupCacheOff + kCacheEntryBytes * a.numCached;
        if (block) {
            a.groupBlockOff   = a.groupRow.rowLen;
            a.groupRow.rowLen += 8;
        }
    } else {
        a.groupRow.rowLen   = nnEnd;
    }
    a.blockRow.rowLen = block ? kBlockEntries * (8u + 9u * a.numBlockCols) : 0u;
    return a;
```

and replace `aggDirtyBoundPerRecord` with:

```cpp
AggDirtyBound aggDirtyBoundPerRecord(const AggLayout& a) noexcept {
    const bool block = a.numBlockCols > 0;   // implies linked
    return AggDirtyBound{a.linked ? (block ? 5u : 4u) : 1u, 2u, 2u, block ? 3u : 0u};
}
```

(A 2a layout has no Min/Max, so `numBlockCols == 0` and every 2a offset and length is computed by the same statements as before.)

- [ ] **Step 4: Implement the fold.** In `broker/libs/view/include/rune/view/AggFold.h`, replace the declaration and its comment with:

```cpp
// Folds every committed record (offset < bound) of `lv` into an aggregate view
// with TABLE semantics (AggState.h): contributor = 8-byte wire key, group = the
// group-by value. Same batch walk, control-batch skip, bound stop, and cursor
// contract as foldBatch (ViewFold.h). Zero heap allocation in steady state.
// A layout with Min/Max (agg.numBlockCols > 0) also maintains each group's
// value block in `blocks` (rows of agg.blockRow); `blocks` may be nullptr only
// when agg.numBlockCols == 0 -- with Min/Max and no table, every block step is
// counted in aggInconsistent and skipped (never a crash).
FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             ViewTable* blocks, const rune::storage::LogReadView& lv,
                             int64_t bound, int64_t& cursor);

// 2a and Last-only layouts (no value block): blocks = nullptr.
FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor);
```

In `broker/libs/view/src/AggFold.cpp`:

(a) After `constexpr uint8_t kPending = …;` add:

```cpp
// Chunk lookups issued before scanning (independent keys: their misses overlap).
constexpr uint32_t kRescanBatch = 8;
```

(b) In `struct Ctx`, after `ViewTable& groups;` add `ViewTable* blocks;   // Min/Max value blocks; nullptr iff agg.numBlockCols == 0`.

(c) Directly after `linkAtHead(...)`, add the block maintenance:

```cpp
    // --- Min/Max value block (Task 5b; layout in AggState.h) ---
    uint32_t blockIdOf(const uint8_t* gr) const noexcept { return loadU32(gr + agg.groupBlockOff); }
    uint32_t blockChunksOf(const uint8_t* gr) const noexcept { return loadU32(gr + agg.groupBlockOff + 4); }
    void setBlock(uint8_t* gr, uint32_t id, uint32_t chunks) const noexcept {
        storeU32(gr + agg.groupBlockOff, id);
        storeU32(gr + agg.groupBlockOff + 4, chunks);
    }

    // Writes entry `slot` of chunk `row` from contributor c's row `cr`.
    void writeEntry(uint8_t* row, uint32_t slot, uint64_t c, const uint8_t* cr) const noexcept {
        storeU64(row + blockKeyOff(slot), c);
        for (uint16_t col = 0; col < agg.numBlockCols; ++col) {
            const uint16_t in = agg.blockColInput[col];
            std::memcpy(row + blockValOff(col, slot), cr + agg.inputOff[in], 8);
            row[blockNullOff(agg, col, slot)] = nullBit(cr, agg.contribNullOff, in) ? 1 : 0;
        }
    }
    // Copies entry `from` of chunk `src` (key, values, nulls) to entry `to` of chunk `dst`.
    void moveEntry(uint8_t* dst, uint32_t to, const uint8_t* src, uint32_t from) const noexcept {
        std::memcpy(dst + blockKeyOff(to), src + blockKeyOff(from), 8);
        for (uint16_t col = 0; col < agg.numBlockCols; ++col) {
            std::memcpy(dst + blockValOff(col, to), src + blockValOff(col, from), 8);
            dst[blockNullOff(agg, col, to)] = src[blockNullOff(agg, col, from)];
        }
    }

    // A free block id for group g: its candidate, then linear probing (an id is
    // in use iff its chunk 0 exists; get() never dirties). kNoBlock if none.
    uint32_t allocBlockId(uint64_t g) const noexcept {
        uint32_t id = blockIdCandidate(g);
        for (uint32_t i = 0; i < kBlockIdProbes; ++i, ++id) {
            if (id == kNoBlock) id = 1;   // wrapped
            if (blocks->get(blockChunkKey(id, 0)) == nullptr) return id;
        }
        return kNoBlock;
    }

    // Appends contributor c (row cr, already counted into group g's row gr by
    // applyContribution(+1)) as the group's last entry, position n-1: into the
    // tail, the spare, or a new chunk at a boundary. Exactly one chunk row put.
    void blockAppend(uint64_t c, uint8_t* cr, uint64_t g, uint8_t* gr) noexcept {
        const uint64_t n = loadU64(gr);
        if (blocks == nullptr || n == 0 || n > UINT32_MAX) { ++stats.aggInconsistent; return; }
        const uint32_t pos = static_cast<uint32_t>(n - 1);
        uint32_t id = blockIdOf(gr);
        uint32_t chunks = blockChunksOf(gr);
        if (id == kNoBlock) {
            if (pos != 0) { ++stats.aggInconsistent; return; }   // entries without a block
            id = allocBlockId(g);
            if (id == kNoBlock) { ++stats.aggInconsistent; return; }
            chunks = 0;
        }
        const uint32_t idx = pos >> kBlockShift;
        uint8_t* row = nullptr;
        if (idx < chunks) {                                      // the tail, or the spare at a boundary
            if (blocks->get(blockChunkKey(id, idx)) == nullptr) ++stats.aggInconsistent;
            else row = blocks->put(blockChunkKey(id, idx));
        } else if (idx == chunks) {                              // a new chunk (zero-filled)
            row = blocks->put(blockChunkKey(id, idx));
            ++chunks;
        } else {
            ++stats.aggInconsistent;                             // a gap in the chunk sequence
        }
        setBlock(gr, id, chunks);
        if (row == nullptr) return;
        writeEntry(row, pos & kBlockMask, c, cr);
        storeU32(cr + agg.contribBlockPosOff, pos);
    }

    // Rewrites contributor c's entry in place (same group; position unchanged).
    void blockWrite(uint64_t c, const uint8_t* cr, const uint8_t* gr) noexcept {
        const uint32_t pos = loadU32(cr + agg.contribBlockPosOff);
        const uint32_t id = blockIdOf(gr);
        if (blocks == nullptr || id == kNoBlock || pos >= loadU64(gr)) { ++stats.aggInconsistent; return; }
        const uint64_t key = blockChunkKey(id, pos >> kBlockShift);
        const uint8_t* cur = blocks->get(key);
        const uint32_t slot = pos & kBlockMask;
        if (cur == nullptr || loadU64(cur + blockKeyOff(slot)) != c) { ++stats.aggInconsistent; return; }
        writeEntry(blocks->put(key), slot, c, cr);
    }

    // Removes contributor c (pre-image row cr) from group gr's block, AFTER
    // applyContribution(-1) (so loadU64(gr) is the post-remove count n'): the
    // old last entry (position n') moves into c's slot and its contributor's
    // blockPos is patched (the tail chunk is only read). Then the chunk policy:
    // n' == 0 frees the block and its id; a spare is freed once the tail holds
    // <= kBlockSpareFreeAt entries; an emptied tail simply becomes the spare.
    // At most: the hole chunk (put) + one erase -> 2 block rows, 1 contributor row.
    void blockRemove(uint64_t c, const uint8_t* cr, uint8_t* gr) noexcept {
        const uint64_t n = loadU64(gr);
        const uint32_t id = blockIdOf(gr);
        uint32_t chunks = blockChunksOf(gr);
        const uint32_t pos = loadU32(cr + agg.contribBlockPosOff);
        if (blocks == nullptr || id == kNoBlock || n > UINT32_MAX || pos > n) {
            ++stats.aggInconsistent;
            return;
        }
        const uint32_t last = static_cast<uint32_t>(n);
        if (pos != last) {
            const uint64_t holeKey = blockChunkKey(id, pos >> kBlockShift);
            const uint8_t* tail = blocks->get(blockChunkKey(id, last >> kBlockShift));
            const uint8_t* hole = blocks->get(holeKey);
            if (tail == nullptr || hole == nullptr ||
                loadU64(hole + blockKeyOff(pos & kBlockMask)) != c) {
                ++stats.aggInconsistent;
            } else {
                const uint64_t m = loadU64(tail + blockKeyOff(last & kBlockMask));
                moveEntry(blocks->put(holeKey), pos & kBlockMask, tail, last & kBlockMask);
                if (uint8_t* mr = linkedContributor(m)) storeU32(mr + agg.contribBlockPosOff, pos);
            }
        }
        if (n == 0) {
            if (chunks > 2) ++stats.aggInconsistent;              // a 1-entry block owns <= 1 chunk
            for (uint32_t k = 0; k < std::min<uint32_t>(chunks, 2); ++k)
                (void)blocks->erase(blockChunkKey(id, k));
            setBlock(gr, kNoBlock, 0);
            return;
        }
        const uint32_t used = static_cast<uint32_t>((n - 1) >> kBlockShift) + 1;
        const uint32_t tailFill = static_cast<uint32_t>(n - (static_cast<uint64_t>(used - 1) << kBlockShift));
        if (chunks == used + 1 && tailFill <= kBlockSpareFreeAt) {
            (void)blocks->erase(blockChunkKey(id, chunks - 1));
            --chunks;
        } else if (chunks != used && chunks != used + 1) {
            ++stats.aggInconsistent;
        }
        setBlock(gr, id, chunks);
    }
```

(d) Replace the whole `rescan` function (its comment through its closing `}`) with:

```cpp
    // Rebuilds Min/Max field i's cache by ONE sequential scan of the group's
    // value block: ceil(n/32) chunk rows, looked up kRescanBatch at a time (the
    // keys are computed, not links, so their misses overlap), then n packed
    // values of the field's column. get() never dirties. A missing chunk, a
    // short scan or a non-null count mismatch is counted; the cache reports what
    // was actually found.
    void rescan(uint8_t* gr, size_t i, const AggField& f) noexcept {
        ++stats.aggRescans;
        uint8_t* cv = cacheVal(gr, f.cache);
        uint8_t* cs = cacheState(gr, f.cache);
        const uint64_t n = loadU64(gr);
        const uint64_t nonNull = loadU64(nn(gr, i));
        const uint16_t col = agg.cacheCol[f.cache];
        const uint32_t id = blocks != nullptr ? blockIdOf(gr) : kNoBlock;
        bool have = false;
        bool bad = id == kNoBlock;
        uint64_t seen = 0, scanned = 0;
        const uint64_t chunks = bad ? 0 : (n + kBlockEntries - 1) >> kBlockShift;
        const uint8_t* rows[kRescanBatch];
        for (uint64_t base = 0; base < chunks; base += kRescanBatch) {
            const uint32_t m = static_cast<uint32_t>(std::min<uint64_t>(kRescanBatch, chunks - base));
            for (uint32_t j = 0; j < m; ++j) {
                rows[j] = blocks->get(blockChunkKey(id, static_cast<uint32_t>(base + j)));
                if (rows[j] != nullptr) __builtin_prefetch(rows[j] + blockValOff(col, 0));
            }
            for (uint32_t j = 0; j < m; ++j) {
                const uint8_t* r = rows[j];
                if (r == nullptr) { bad = true; continue; }
                const uint64_t first = (base + j) << kBlockShift;
                const uint32_t cnt = static_cast<uint32_t>(std::min<uint64_t>(kBlockEntries, n - first));
                const uint8_t* nulls = r + blockNullOff(agg, col, 0);
                const uint8_t* vals = r + blockValOff(col, 0);
                for (uint32_t s = 0; s < cnt; ++s) {
                    if (nulls[s] != 0) continue;
                    const uint8_t* v = vals + 8u * s;
                    if (!have || strictlyBetter(f.rule, f.kind, v, cv)) { std::memcpy(cv, v, 8); have = true; }
                    ++seen;
                }
                scanned += cnt;
            }
        }
        if (bad || scanned != n || seen != nonNull) ++stats.aggInconsistent;
        if (!have) storeU64(cv, 0);
        *cs = have ? kValid : kEmpty;
    }
```

(e) In `retract`, replace

```cpp
        if (gr == nullptr) return g;   // inconsistent: counted, nothing else changed
```

with

```cpp
        if (gr == nullptr) return g;   // inconsistent: counted, nothing else changed
        // Value block: an in-place update keeps its entry (rewritten after the add);
        // everything else leaves the group -- swap-remove BEFORE the emit below rescans.
        if (agg.numBlockCols > 0 && mode != RetractMode::InPlaceLinked) blockRemove(c, cr, gr);
```

(f) In the fold body, replace

```cpp
            uint8_t* gr = cx.applyContribution(cr, +1);
            if (agg.linked) cx.linkAtHead(c, cr, gr);
            cx.emit(g, gr);
```

with

```cpp
            uint8_t* gr = cx.applyContribution(cr, +1);
            if (agg.linked) cx.linkAtHead(c, cr, gr);
            if (agg.numBlockCols > 0) {          // implies linked: same group == InPlaceLinked
                if (hadOld && oldGroup == g) cx.blockWrite(c, cr, gr);
                else cx.blockAppend(c, cr, g, gr);
            }
            cx.emit(g, gr);                      // a rescan here sees the block's new state
```

(g) Change the definition's signature to the `blocks` overload (add `ViewTable* blocks,` after `ViewTable& groups,`), construct `Ctx cx{layout, agg, view, contributors, groups, blocks, stats};`, and after its closing `}` add the forwarding overload:

```cpp
FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor) {
    return foldAggregateBatch(plan, layout, agg, vdc, view, contributors, groups, nullptr, lv,
                              bound, cursor);
}
```

Notes for the implementer:
- `cr` in `retract` is the pre-image row pointer from `contributors.get(c)`. `blockRemove` reads `blockPos` from it before the tombstone path erases it or the add path rewrites it. Rows never move, so `put()`s elsewhere (m's row, the hole chunk) do not invalidate it or `tail`.
- The value loop of the add path writes only `inputOff` slots, so `blockPos` (at the row's end) survives an in-place rewrite. For a move, `blockAppend` overwrites it.
- A rescan after a move-out must see the block *after* the swap-remove: (e) runs before `retract`'s `emit`. A rescan after an in-place update must see the *new* value: (f) runs before the fold's `emit`.
- The 2a path is unchanged. `numBlockCols == 0` means no block call, no new bytes, and the same `put`/`erase` order. `AggFold*` stays byte-untouched and still calls the 10-argument overload.

- [ ] **Step 5: Run the view tests.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'AggRetractable|AggFold|AggState|ViewFold|ViewCheckpoint' -j14 --output-on-failure`
Expected: 0 failures. That covers every Task 5 test, `DanglingLink…` with its new expectation, the 9 new or replaced block tests and the untouched 2a `AggFold*` suite. If the model fails, the step number in the message localizes it. Debug with superpowers:systematic-debugging and keep the test.

- [ ] **Step 6: Node plumbing (the fourth table).** In `broker/libs/node/include/rune/node/WorkerViewSet.h`:

Replace `struct AggInstance { … };` with:

```cpp
// Phase 2a: the auxiliary tables of an aggregate (view, partition); Phase 2b
// Task 5b: plus the per-group Min/Max value blocks when the view has Min/Max.
struct AggInstance {
    rune::view::AggLayout layout;        // declared first: the tables are built from it
    rune::view::ViewTable contributors;  // contributorKey -> pre-image (retraction)
    rune::view::ViewTable groups;        // groupKey -> accumulators
    std::optional<rune::view::ViewTable> blocks;   // engaged iff layout.numBlockCols > 0
    AggInstance(rune::view::AggLayout l, std::size_t indexSlots, std::size_t chunkBytes)
        : layout(std::move(l)),
          contributors(layout.contributorRow, indexSlots, chunkBytes),
          groups(layout.groupRow, indexSlots, chunkBytes) {
        if (layout.numBlockCols > 0) blocks.emplace(layout.blockRow, indexSlots, chunkBytes);
    }
    rune::view::ViewTable* blocksOrNull() noexcept { return blocks ? &*blocks : nullptr; }
};
```

In `dirtyBacklog`, replace `if (inst.agg) n += pending(inst.agg->contributors) + pending(inst.agg->groups);` with:

```cpp
        if (inst.agg) {
            n += pending(inst.agg->contributors) + pending(inst.agg->groups);
            if (inst.agg->blocks) n += pending(*inst.agg->blocks);
        }
```

In the fold pass, replace the aggregate call's argument line `inst.agg->groups, view, passBound, cursor)` with `inst.agg->groups, inst.agg->blocksOrNull(), view, passBound, cursor)`.

In `discardDirty_`, after `inst.agg->groups.drainDirty(drop);` add `if (inst.agg->blocks) inst.agg->blocks->drainDirty(drop);`.

In `broker/libs/node/src/WorkerViewSet.cpp`:

In `replayInstance`, replace the table set-up and the `apply` lambda (from `rune::view::ViewTable* tables[3];` through the `applyMultiDelta` line) with:

```cpp
    rune::view::ViewTable* tables[4];
    uint32_t lens[4];
    std::size_t nTables = 0;
    if (inst.agg) {
        tables[rune::view::kAggTableView]         = &inst.table;
        tables[rune::view::kAggTableContributors] = &inst.agg->contributors;
        tables[rune::view::kAggTableGroups]       = &inst.agg->groups;
        lens[rune::view::kAggTableView]         = inst.layout.rowLen;
        lens[rune::view::kAggTableContributors] = inst.agg->layout.contributorRow.rowLen;
        lens[rune::view::kAggTableGroups]       = inst.agg->layout.groupRow.rowLen;
        nTables = 3;
        if (inst.agg->blocks) {   // Task 5b: Min/Max value blocks, table id 3
            tables[rune::view::kAggTableBlocks] = &*inst.agg->blocks;
            lens[rune::view::kAggTableBlocks]   = inst.agg->layout.blockRow.rowLen;
            nTables = 4;
        }
    }
    auto apply = [&](std::span<const uint8_t> rec) -> std::optional<rune::view::AppliedDelta> {
        if (inst.agg)
            return rune::view::applyMultiDelta(std::span<rune::view::ViewTable* const>(tables, nTables),
                                               std::span<const uint32_t>(lens, nTables), rec);
```

(the `return rune::view::applyDelta(…)` line and the rest stay). Also change the function's comment `v2 (all three tables, offset-atomic)` to `v2 (all three tables, or four with Min/Max value blocks; offset-atomic)`.

Replace `maxRecordsPerPassFor` (its comment and body) with:

```cpp
// Most source records one fold pass may cover so the pass's checkpoint delta
// (worst case) fits ONE ring message of `maxMsg` payload bytes; 0 if not even
// one record fits.  Worst case per source record: Replace dirties 1 output row;
// an aggregate dirties rune::view::aggDirtyBoundPerRecord rows per table (2a:
// contributor + old/new group + old/new output row; linked views also the
// contributor's two old neighbours and the new group's old head; Min/Max views
// also the old group's swap-moved contributor and up to 3 value-block chunks).
int64_t maxRecordsPerPassFor(const ViewInstance& inst, std::size_t maxMsg) {
    using namespace rune::view;
    std::size_t fixed = ViewSnapshotChannel::kDeltaHeaderBytes + kCheckpointCrcBytes;
    std::size_t perRecord = 0;
    if (inst.agg) {
        const AggLayout& a = inst.agg->layout;
        const std::size_t nTables = a.numBlockCols > 0 ? 4 : 3;
        fixed += kCheckpointV2HeaderBytes + nTables * kCheckpointV2TableHeaderBytes;
        const AggDirtyBound b = aggDirtyBoundPerRecord(a);
        perRecord = b.contributorRows * (kCheckpointEntryHeaderBytes + a.contributorRow.rowLen) +
                    b.groupRows * (kCheckpointEntryHeaderBytes + a.groupRow.rowLen) +
                    b.viewRows * (kCheckpointEntryHeaderBytes + inst.layout.rowLen) +
                    b.blockRows * (kCheckpointEntryHeaderBytes + a.blockRow.rowLen);
    } else {
        fixed += kCheckpointV1HeaderBytes;
        perRecord = kCheckpointEntryHeaderBytes + inst.layout.rowLen;
    }
    if (maxMsg <= fixed) return 0;
    return static_cast<int64_t>((maxMsg - fixed) / perRecord);
}
```

In `buildAndPushCheckpoint_`'s aggregate branch, replace from `std::size_t b[4];` through the `encodeMultiDelta` call with:

```cpp
        std::size_t b[5];
        b[0] = 0;
        drainInto(inst.table, inst.layout.rowLen);
        b[1] = dirtyEntries_.size();
        drainInto(inst.agg->contributors, inst.agg->layout.contributorRow.rowLen);
        b[2] = dirtyEntries_.size();
        drainInto(inst.agg->groups, inst.agg->layout.groupRow.rowLen);
        b[3] = dirtyEntries_.size();
        if (inst.agg->blocks) drainInto(*inst.agg->blocks, inst.agg->layout.blockRow.rowLen);
        b[4] = dirtyEntries_.size();
        if (dirtyEntries_.empty()) return 0;
        fixup();
        auto sub = [&](int i) {
            return std::span<const rune::view::DirtyEntry>(dirtyEntries_.data() + b[i],
                                                          b[i + 1] - b[i]);
        };
        const rune::view::TableDelta tds[4] = {
            {rune::view::kAggTableView, inst.layout.rowLen, sub(0)},
            {rune::view::kAggTableContributors, inst.agg->layout.contributorRow.rowLen, sub(1)},
            {rune::view::kAggTableGroups, inst.agg->layout.groupRow.rowLen, sub(2)},
            {rune::view::kAggTableBlocks, inst.agg->layout.blockRow.rowLen, sub(3)}};
        const std::size_t nTables = inst.agg->blocks ? 4 : 3;   // 2a / Last-only: unchanged 3-table bytes
        rune::view::encodeMultiDelta(std::span<const rune::view::TableDelta>(tds, nTables),
                                     inst.foldCursor, inst.pendingDelta);
```

(and update the comment above it: `output rows + contributor map + group accumulators (+ Min/Max value blocks) in ONE v2 record, so recovery always restores every table at the same O`).

In `broker/benchmark/ViewFoldBench.cpp` (`AggRig`): add `#include <optional>` to the includes; add the member `std::optional<ViewTable> blocks;   // Min/Max value blocks (userStatsView); empty for a 2a view` after `VersionDescriptorCache vdc;`; give the constructor the body `{ if (agg.numBlockCols > 0) blocks.emplace(agg.blockRow); }`; in `drainAll()` add `if (blocks) blocks->drainDirty(nop);`; and in `foldStream` pass `blocks ? &*blocks : nullptr` after `groups`:

```cpp
        const auto fs = foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors,
                                           groups, blocks ? &*blocks : nullptr, lv, s.bound(i), cur);
```

`prime()` already folds the whole contributor space, so the block table reaches its final population (10k groups × ≤ 4 chunks) and capacity before timing.

- [ ] **Step 7: Run the affected tests.**

Run: `cmake --build build/root -j14 --target rune_view_tests rune_node_tests rune_get_view_row_tests rune_create_view_tests && ctest --test-dir build/root -R 'Agg|ViewFold|ViewCheckpoint|WorkerViewSet|ViewRecovery|ViewReadiness|ViewSnapshot|GetViewRow|CreateView' -j14 --output-on-failure`
Expected: 0 failures. 2a's `PersistedAggregateLargeRangeDeltasFitTheRing` and `AggregateRecoveryOfLargeCheckpointDoesNotResendRows` pass unchanged (3-table records, same cap).

- [ ] **Step 8: Commit.**

```bash
git add broker/libs/view/include/rune/view/AggState.h broker/libs/view/src/AggState.cpp broker/libs/view/include/rune/view/AggFold.h broker/libs/view/src/AggFold.cpp broker/libs/view/tests/view/AggStateTest.cpp broker/libs/view/tests/view/AggRetractableTest.cpp broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp broker/benchmark/ViewFoldBench.cpp
git commit -m "feat(view): contiguous per-group Min/Max value block — sequential rescan, swap-remove, spare-chunk hysteresis; checkpoint table 3; exact bound {5,2,2,3}" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 9: Rerun the bench gate (`build/rel`).**

```bash
cmake --build build/rel -j14 --target rune_broker_benchmark
build/rel/broker/rune_broker_benchmark --benchmark_filter=BM_ViewFoldRetractable
build/rel/broker/rune_broker_benchmark --benchmark_filter='BM_ViewFold(Aggregate|Replace)'
```

Expected: `[ViewFoldRetractable gate] ret_record_p99_us=… (<5) ret_batch_p99_us=… (<10) steady_alloc=0 (==0) rescans=… => PASS`, on the same honest churn (no workload change). Report to the controller `ret_record_p50/p99`, `ret_batch_p99`, `ret_rescans` divided by measured records, and the p50 against Task 5's 0.75 µs. `BM_ViewFoldAggregate` and `BM_ViewFoldReplace` must still PASS (2a untouched). On a FAIL, report the record-vs-batch p99 split and a profile **before any tuning**. Never relax the gate or its thresholds. A non-zero `steady_alloc` is a CLAUDE.md #4 violation to fix, not to gate away.

---

### Task 6: Pin the dirty bound and model-check the fold

These tests target the linked fold's two quiet failure modes: a list or cache that drifts from the truth only after a long, specific history, and a record that dirties more rows than the per-pass cap assumes. Both fail silently in production; here they fail deterministically.

**Files:**
- Modify: `broker/libs/view/tests/view/AggRetractableTest.cpp` (append)

**Interfaces:**
- Consumes: `aggDirtyBoundPerRecord`, `AggDirtyBound` (Task 4, `blockRows` from Task 5b); `Harness::drainCounts`, `Harness::blocks`, `Drained::blockRows`, `Harness::expectBlocksConsistent` (Task 5b) and `Ord`/`encode` (Task 5's test file). Adds `Harness::Rec` and `Harness::applySplit` (Step 1a, F9).
- **Amended 2026-10-20 (Task 5b):** the worst-case pin moved to Task 5b (`WorstCaseMoveDirtiesExactlyTheBoundRegression`, all four tables), so this task no longer adds `WorstCaseMoveDirtiesExactlyTheDerivedBoundRegression`. The split pass and the model also check the block table.

- [ ] **Step 1a: Extend the harness (F9).** In `Harness` (Task 5's test file), directly after `drainCounts()`, add a way to fold one batch in two passes. This is the worker's capped-pass shape: pass 1 stops at `bound`, pass 2 resumes from the cursor.

```cpp
    using Rec = std::pair<std::vector<uint8_t>, std::vector<uint8_t>>;

    // Folds `recs` as ONE batch in two passes split after `firstPass` records
    // (1 <= firstPass < recs.size()): pass 1 is capped by `bound`, pass 2 resumes
    // mid-batch from the cursor. Returns both passes' dirty entries summed. Each
    // pass is also checked: at most (records in the pass) x the per-record bound
    // per table (dirty entries coalesce inside a pass), and no RescanPending cache.
    Drained applySplit(const std::vector<Rec>& recs, size_t firstPass) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        for (const auto& [k, v] : recs) {
            b.append(std::string_view(reinterpret_cast<const char*>(k.data()), k.size()),
                     std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0);
        }
        const auto batch = b.build();
        const int64_t full = next + static_cast<int64_t>(recs.size());
        const int64_t cap  = next + static_cast<int64_t>(firstPass);
        const rune::storage::LogReadView lv{batch.data(), batch.size(), next, full};
        const auto perRec = rune::view::aggDirtyBoundPerRecord(agg);
        Drained total;
        size_t done = 0;
        for (const int64_t bnd : {cap, full}) {
            last = rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view,
                                                  contributors, groups, &blocks, lv, bnd, cursor);
            inconsistent += last.aggInconsistent;
            const size_t inPass = static_cast<size_t>(bnd - next) - done;
            done += inPass;
            const Drained d = drainCounts();
            EXPECT_LE(d.contributorRows, inPass * perRec.contributorRows);
            EXPECT_LE(d.groupRows, inPass * perRec.groupRows);
            EXPECT_LE(d.viewRows, inPass * perRec.viewRows);
            EXPECT_LE(d.blockRows, inPass * perRec.blockRows);
            EXPECT_EQ(d.pendingCaches, 0u);
            total.contributorRows += d.contributorRows;
            total.groupRows += d.groupRows;
            total.viewRows += d.viewRows;
            total.blockRows += d.blockRows;
            total.pendingCaches += d.pendingCaches;
        }
        next = full;
        return total;
    }
```

- [ ] **Step 1: Write the tests.** Append to `broker/libs/view/tests/view/AggRetractableTest.cpp`:

```cpp
// REGRESSION (Review Focus #1/#3, F9): a deterministic history of 3000 steps of
// 1-4 records each (inserts, in-place updates, group moves, deletes; the same
// contributor can repeat inside one batch) over 24 contributors / 4 groups, with
// heavy value duplication. Every 5th multi-record step is folded in two passes
// split at a random mid-batch boundary (capped-pass resume). After EVERY step:
// each group's Count(*), Sum, Max, Min, Last, Last(bonus), LastNonNull(bonus)
// equals a brute-force recomputation; each table's dirty entries are within
// records x the derived per-record bound; no cache is left RescanPending;
// nothing is ever inconsistent.
TEST(AggRetractableRegression, RandomizedOpsMatchBruteForceModelRegression) {
    struct M { int64_t user; int64_t amount; std::optional<int32_t> bonus; uint64_t seq; };
    std::map<int64_t, M> live;
    Harness h;
    const auto bound = rune::view::aggDirtyBoundPerRecord(h.agg);
    uint64_t state = 0x9E3779B97F4A7C15ull;
    auto rnd = [&](uint64_t m) {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        return (state >> 33) % m;
    };
    uint64_t seq = 0;
    for (uint64_t step = 1; step <= 3000; ++step) {
        const size_t n = 1 + static_cast<size_t>(rnd(4));
        std::vector<Harness::Rec> recs;
        for (size_t i = 0; i < n; ++i) {
            const int64_t id = 1 + static_cast<int64_t>(rnd(24));
            ++seq;
            if (rnd(100) < 15) {
                recs.push_back({Harness::key(id), tombstone()});
                live.erase(id);
            } else {
                Ord o{id, 1 + static_cast<int64_t>(rnd(4)), static_cast<int64_t>(rnd(41)) - 20};
                if (rnd(2) == 0) o.bonus = static_cast<int32_t>(rnd(9));
                recs.push_back({Harness::key(id), encode(o)});
                live[id] = M{o.user, o.amount, o.bonus, seq};
            }
        }
        Harness::Drained d;
        if (n >= 2 && step % 5 == 0) {
            d = h.applySplit(recs, 1 + static_cast<size_t>(rnd(n - 1)));   // pass 1 stops mid-batch
        } else {
            h.apply(recs);
            d = h.drainCounts();
        }
        ASSERT_LE(d.contributorRows, n * bound.contributorRows) << "step " << step;
        ASSERT_LE(d.groupRows, n * bound.groupRows) << "step " << step;
        ASSERT_LE(d.viewRows, n * bound.viewRows) << "step " << step;
        ASSERT_LE(d.blockRows, n * bound.blockRows) << "step " << step;
        ASSERT_EQ(d.pendingCaches, 0u) << "step " << step;
        ASSERT_EQ(h.expectBlocksConsistent({1, 2, 3, 4}), live.size()) << "step " << step;
        for (int64_t g = 1; g <= 4; ++g) {
            const M* lastM = nullptr;
            const M* lastNn = nullptr;
            int64_t cnt = 0, total = 0, mx = INT64_MIN, mn = INT64_MAX;
            for (const auto& [k, m] : live) {
                if (m.user != g) continue;
                ++cnt; total += m.amount;
                mx = std::max(mx, m.amount); mn = std::min(mn, m.amount);
                if (lastM == nullptr || m.seq > lastM->seq) lastM = &m;
                if (m.bonus && (lastNn == nullptr || m.seq > lastNn->seq)) lastNn = &m;
            }
            if (cnt == 0) { ASSERT_FALSE(h.hasGroup(g)) << "step " << step << " group " << g; continue; }
            ASSERT_EQ(h.col(g, kN).i, cnt) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kTotal).i, total) << "step " << step;
            ASSERT_EQ(h.col(g, kMaxAmt).i, mx) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kMinAmt).i, mn) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kLastAmt).i, lastM->amount) << "step " << step << " group " << g;
            const auto lb = h.col(g, kLastBonus);
            ASSERT_EQ(lb.present, lastM->bonus.has_value()) << "step " << step;
            if (lastM->bonus) ASSERT_EQ(lb.i, *lastM->bonus) << "step " << step;
            const auto lnn = h.col(g, kLastNnBonus);
            ASSERT_EQ(lnn.present, lastNn != nullptr) << "step " << step;
            if (lastNn != nullptr) ASSERT_EQ(lnn.i, *lastNn->bonus) << "step " << step;
        }
    }
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (F9, op "no-group"): a rewrite whose group key is ABSENT retracts
// the old contributor from a LINKED group -- unlink, count, cache -- and skips.
// The source declares the group-by field only from v2 (as AggFoldTest's
// MissingGroupValueRetractsAndSkips does); a v1 record lacks it.
TEST(AggRetractableRegression, NoGroupRewriteUnlinksAndRetractsTheContributorRegression) {
    const auto src = rune::codec::DescriptorBuilder("o", 2)
        .add("orderId", P::Int64, 1, true)
        .add("amount", P::Int64, 1)
        .add("userId", P::Int64, 2)
        .build();
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"max", P::Int64, 0, "amount", R::Max},
                {"last", P::Int64, 0, "amount", R::Last}};
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, 9).value();
    auto agg = rune::view::buildAggLayout(comp.plan, src).value();
    ASSERT_TRUE(agg.linked);
    rune::view::VersionDescriptorCache vdc(src);
    rune::view::ViewTable view(comp.rowLayout), contributors(agg.contributorRow), groups(agg.groupRow);
    rune::view::ViewTable blocks(agg.blockRow);   // Max(amount): a value block (Task 5b)
    int64_t off = 0;
    auto fold = [&](int64_t id, const std::vector<uint8_t>& value) {
        rune::protocol::RecordBatchBuilder b(off, 0);
        const auto k = Harness::key(id);
        b.append(std::string_view(reinterpret_cast<const char*>(k.data()), 8),
                 std::string_view(reinterpret_cast<const char*>(value.data()), value.size()), 0);
        const auto batch = b.build();
        int64_t cur = off;
        const auto st = rune::view::foldAggregateBatch(
            comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups, &blocks,
            {batch.data(), batch.size(), off, off + 1}, off + 1, cur);
        ++off;
        return st;
    };
    auto v2 = [&](int64_t id, int64_t amount) {
        std::vector<uint8_t> b;
        le16(b, 24); le16(b, 2); le64(b, uint64_t(id)); le64(b, uint64_t(amount)); le64(b, 100);
        return b;
    };
    (void)fold(1, v2(1, 50));
    (void)fold(2, v2(2, 30));                       // list: 2 (head) -> 1; max 50 held by contributor 1
    std::vector<uint8_t> v1;
    le16(v1, 16); le16(v1, 1); le64(v1, 1); le64(v1, 50);   // contributor 1 rewritten WITHOUT userId
    const auto st = fold(1, v1);
    EXPECT_EQ(st.skippedNoGroup, 1u);
    EXPECT_EQ(st.aggInconsistent, 0u);
    const uint8_t* row = view.get(100);
    ASSERT_NE(row, nullptr) << "contributor 2 still holds the group";
    EXPECT_EQ(rune::view::readSlot(row, comp.rowLayout, 1).i, 1);
    EXPECT_EQ(rune::view::readSlot(row, comp.rowLayout, 2).i, 30) << "the retracted max (50) rescans to 30";
    EXPECT_EQ(rune::view::readSlot(row, comp.rowLayout, 3).i, 30);
    EXPECT_EQ(contributors.size(), 1u);
    EXPECT_EQ(contributors.get(1), nullptr);
    (void)fold(2, [&] { std::vector<uint8_t> b; le16(b, 16); le16(b, 1); le64(b, 2); le64(b, 30); return b; }());
    EXPECT_EQ(view.get(100), nullptr) << "the last contributor left: group and list are gone";
    EXPECT_EQ(contributors.size(), 0u);
    EXPECT_EQ(blocks.size(), 0u) << "the emptied group's value block (chunk 0) and id are freed";
}
```

- [ ] **Step 2: Run them.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'AggRetractableRegression' --output-on-failure`
Expected: both pass on Task 5b's fold. If either fails, the failure is a real bug in Task 5 or 5b. Debug it with superpowers:systematic-debugging, fix it in `AggFold.cpp`, and keep the test as written; the step number in the message localizes the bug.

- [ ] **Step 3: Run the affected tests and commit.**

Run: `ctest --test-dir build/root -R 'AggRetractable|AggFold|AggState' -j14 --output-on-failure` → 0 failures.

```bash
git add broker/libs/view/tests/view/AggRetractableTest.cpp
git commit -m "test(view): model-check 3000 multi-record steps (capped-pass resume, value-block structure) against brute force; no-group unlink frees the block" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Worker — per-pass cap from the derived bound; restart regression

> **Amended 2026-10-20 (Task 5b):** Task 5b already moved `maxRecordsPerPassFor`, replay, checkpoint encode, `discardDirty_` and `dirtyBacklog` onto the 4-table record. This task keeps its two regressions, which now pin the recovered value block and the {5,2,2,3} budget. Its Step 3 becomes the carried Task 5 review item 2: `FoldStats::aggRescans` surfaced as `ViewFoldCounters::aggRescans`.

**Files:**
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h` (`ViewFoldCounters::aggRescans` and its accumulate line next to `aggInconsistent`)
- Test: `broker/libs/node/tests/node/WorkerViewSetTest.cpp` (append)

**Interfaces:**
- Consumes: `aggDirtyBoundPerRecord` (Tasks 4, 5b); the linked fold (Tasks 5, 5b); `AggInstance::blocks` and the 4-table v2 record (Task 5b).
- Produces: `rune::node::ViewFoldCounters::aggRescans` (`uint64_t`, cumulative `FoldStats::aggRescans`).

- [ ] **Step 1: Write the failing tests.** Append to the end of `broker/libs/node/tests/node/WorkerViewSetTest.cpp` (after the last test; `orderSrc`, `orderBatch`, `drainDeltas` and `driveToEnd` are defined earlier in the file):

```cpp
namespace {
// user_max = GROUP BY userId: Count(*), Max(amount), Last(amount) over "order"
// (topic 6) — a LINKED aggregate view (Phase 2b).
rune::view::ViewPlan userMaxPlan(uint8_t persistence) {
    using P = rune::codec::PrimType; using R = rune::view::MergeRule;
    rune::view::ViewPlan v;
    v.viewName = "user_max"; v.key = "userId"; v.sources = {{6, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"max", P::Int64, 0, "amount", R::Max},
                {"last", P::Int64, 0, "amount", R::Last}};
    v.persistence = persistence;
    return v;
}
} // namespace

// REGRESSION (Review Focus #2): after recovery, retracting the cached Max must
// rescan the RECOVERED value block (table 3 of the v2 checkpoint; links, head,
// cache, block id and positions came back in the contributor and group rows)
// and surface the exact next extreme.
TEST(WorkerViewSetRegression, RestartThenRetractingTheMaxIsExactRegression) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userMaxPlan(0), srcs, 0x4000000A).value();

    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, tmpDir("rune-views-max-rec"));
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = orderBatch(0, {{1, 100, 10}, {2, 100, 50}, {3, 100, 30}});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 3}; };
    vs1.driveFolds(6, 0, rr1, 3, 0);
    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);
    uint32_t magic; std::memcpy(&magic, deltas[0].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2) << "no new format: still the v2 record";
    EXPECT_EQ(deltas[0][12], 4u) << "a Min/Max view's record carries four tables (value blocks = id 3)";

    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, tmpDir("rune-views-max-rec"));
    const std::span<const uint8_t> recs[] = {deltas[0]};
    vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true, /*partition*/ 0,
                               /*base*/ {}, recs, /*committedTail*/ 3);
    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs2.getRow(0x4000000A, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 50);
    EXPECT_EQ(rune::view::readSlot(row, *L, 3).i, 30);

    // Post-restart: the max holder (contributor 2) drops to 5.
    const auto b2 = orderBatch(3, {{2, 100, 5}});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b2.data(), b2.size(), 3, 4}; };
    vs2.driveFolds(6, 0, rr2, 4, 0);
    row = vs2.getRow(0x4000000A, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 3);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 30) << "next extreme found by scanning the recovered value block";
    EXPECT_EQ(rune::view::readSlot(row, *L, 3).i, 5) << "the re-write is now the head";
    const rune::node::ViewFoldCounters* fc = vs2.foldCounters(0x4000000A, 0);
    ASSERT_NE(fc, nullptr);
    EXPECT_EQ(fc->aggInconsistent, 0u);
    EXPECT_EQ(fc->aggRescans, 1u) << "the post-restart retraction rescanned once (counter surfaced, Task 5 review item 2)";
}

// REGRESSION (Review Focus #3): a stream of GROUP MOVES that dirty 10 DISTINCT
// entries each (no coalescing across records: every move touches its own groups,
// neighbours, target head and two value-block chunks; the middle contributor's
// swap partner is its own predecessor) -- 4 contributor + 2 group + 2 view + 2
// block rows, 1674 B. Under a bound without the block rows ({4,2,2,0}: 560 B per
// record, ~233 records per pass) a pass of move records would claim up to ~390 KB,
// the first pass over the 128 KiB delta-ring max message THROWS on the worker; under Task 5b's bound
// ({5,2,2,3}: 2289 B, 57 records per pass) every delta fits and the result is exact.
TEST(WorkerViewSetRegression, PersistedRetractableMovesDeltasFitTheRingRegression) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userMaxPlan(0), srcs, 0x4000000C).value();
    constexpr int64_t kG = 1000;
    std::vector<std::array<int64_t, 3>> recs;
    // Source groups g: contributors 3g+1, 3g+2, 3g+3 (list 3g+3 -> 3g+2 -> 3g+1).
    for (int64_t g = 0; g < kG; ++g)
        for (int64_t j = 1; j <= 3; ++j) recs.push_back({3 * g + j, g, 3 * g + j});
    // Target groups kG+g: one contributor 100000+g each (the head a move links before).
    for (int64_t g = 0; g < kG; ++g) recs.push_back({100000 + g, kG + g, 100000 + g});
    // Moves: middle contributor 3g+2 (prev 3g+3, next 3g+1) -> group kG+g, amount 0.
    for (int64_t g = 0; g < kG; ++g) recs.push_back({3 * g + 2, kG + g, 0});
    const int64_t total = static_cast<int64_t>(recs.size());
    const auto batch = orderBatch(0, recs);
    auto rr = [&](int64_t, int64_t) {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, total};
    };

    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-max-large"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    std::vector<std::vector<uint8_t>> deltas;
    ASSERT_NO_THROW(driveToEnd(vs, ch, 6, rr, total, deltas));
    EXPECT_GT(deltas.size(), 1u) << "the range must have been split across capped passes";

    const rune::view::RowLayout* L = nullptr;
    for (int64_t g = 0; g < kG; g += 97) {
        const uint8_t* src_ = vs.getRow(0x4000000C, 0, uint64_t(g), L);
        ASSERT_NE(src_, nullptr) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(src_, *L, 1).i, 2) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(src_, *L, 2).i, 3 * g + 3) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(src_, *L, 3).i, 3 * g + 3) << "group " << g;
        const uint8_t* dst = vs.getRow(0x4000000C, 0, uint64_t(kG + g), L);
        ASSERT_NE(dst, nullptr) << "group " << kG + g;
        EXPECT_EQ(rune::view::readSlot(dst, *L, 1).i, 2);
        EXPECT_EQ(rune::view::readSlot(dst, *L, 2).i, 100000 + g);
        EXPECT_EQ(rune::view::readSlot(dst, *L, 3).i, 0) << "the moved contributor is the newest";
    }
    const rune::node::ViewFoldCounters* fc = vs.foldCounters(0x4000000C, 0);
    ASSERT_NE(fc, nullptr);
    EXPECT_EQ(fc->aggInconsistent, 0u);
}
```

Add `#include <algorithm>` and `#include <climits>` to the file's includes if they are missing.

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build/root -j14 --target rune_node_tests`
Expected: compile error `no member named 'aggRescans' in 'rune::node::ViewFoldCounters'`. (Amended 2026-10-20: the cap itself landed in Task 5b, so the large-range test passes once it compiles. To see it bite, temporarily drop the `b.blockRows * …` term from `maxRecordsPerPassFor` and confirm the `ASSERT_NO_THROW` failure (the ring’s oversized claim; pre-validated with {5,2,2,0}: `encoded message exceeds maxMsgLength of 131072 length=142858`, the first pass over the limit). Restore it and do not commit that experiment.)

- [ ] **Step 3: Implement (carried Task 5 review item 2).** In `broker/libs/node/include/rune/node/WorkerViewSet.h`, in `struct ViewFoldCounters`, after `uint64_t aggInconsistent  = 0;   // aggregate: retraction found missing/zero state` add:

```cpp
    uint64_t aggRescans       = 0;   // aggregate (Phase 2b): Min/Max value-block rescans (retracted extreme)
```

and in the fold pass, after `inst.counters.aggInconsistent  += st.aggInconsistent;` add:

```cpp
        inst.counters.aggRescans       += st.aggRescans;
```

(`maxRecordsPerPassFor`, replay and the checkpoint encode already carry the value-block table since Task 5b; nothing else changes here.)

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build/root -j14 --target rune_node_tests rune_get_view_row_tests rune_create_view_tests && ctest --test-dir build/root -R 'WorkerViewSet|ViewRecovery|ViewReadiness|GetViewRow|CreateView' -j14 --output-on-failure`
(S9: `GetViewRow` and `CreateView` live in their own binaries, `rune_get_view_row_tests` and `rune_create_view_tests`; build them or they run stale.)
Expected: all pass, including 2a's `PersistedAggregateLargeRangeDeltasFitTheRing` and `AggregateRecoveryOfLargeCheckpointDoesNotResendRows`.

- [ ] **Step 5: Run the affected tests and commit.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'Agg|ViewFold' -j14 --output-on-failure` → 0 failures (Step 4 already ran the node-side suites). The controller's checkpoint after this task runs the full `*Regression*` suite.

```bash
git add broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/tests/node/WorkerViewSetTest.cpp
git commit -m "test(node): restart-then-retract-max scans the recovered value block; Min/Max move stream fits the ring under the {5,2,2,3} bound; surface aggRescans" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Benchmarks — typical-case gate and extreme-retraction report

Steps 1–3 (fixtures, the shared helper, `BM_ViewFoldRetractable`) are done **right after Task 5** and committed as this task's first commit (F4; Task 5 Step 6 runs the gate). Steps 4–6 follow after Task 7.

**Files:**
- Modify: `broker/benchmark/ViewFoldBench.cpp` (fixtures after `userTotalsView()`, lines 208–218; the `AggRig` helper directly before the anonymous namespace's closing `} // namespace`, line 440, because it needs `BatchStream`, `buildStream` and `encodeOrder`; `BM_ViewFoldAggregate` refactored onto it; two new benchmarks at the end of the file)

**Interfaces:**
- Consumes: `foldAggregateBatch`, `buildAggLayout`, `FoldStats::aggRescans`; the shared harness (`Op`, `buildStream`, `BatchStream`, `runPhases`, `reportCommon`, `percentileUs`, `generateChurnOps`, `kBatchSizes`, `kTotalOps`, `keyBytes`, `asSv`, `encodeOrder`, `orderSourceV1`).
- Produces: `AggRig` (anonymous namespace): the 1,000,000-contributor / 10,000-group aggregate rig (compile + tables + `append` / `drainAll` / `foldStream` / `prime`), used by both `BM_ViewFoldAggregate` and `BM_ViewFoldRetractable`.

- [ ] **Step 1: Add the fixtures.** Add `#include <rune/view/ViewRow.h>` next to the other `rune/view` includes. Inside the anonymous namespace, directly after `userTotalsView()` (lines 208–218), add:

```cpp
// --- Retractable aggregate fixtures (Phase 2b) ---
// user_totals' Count(*)/Sum plus the four list rules over amount: a LINKED view.
rune::view::ViewPlan userStatsView() {
    rune::view::ViewPlan v;
    v.viewName = "user_stats";
    v.key      = "userId";
    v.sources  = {{5, "userId"}};
    v.output   = {{"userId", PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                  {"n", PrimType::Int64, 0, "", rune::view::MergeRule::CountAll},
                  {"total", PrimType::Int64, 0, "amount", rune::view::MergeRule::Sum},
                  {"max", PrimType::Int64, 0, "amount", rune::view::MergeRule::Max},
                  {"min", PrimType::Int64, 0, "amount", rune::view::MergeRule::Min},
                  {"last", PrimType::Int64, 0, "amount", rune::view::MergeRule::Last},
                  {"lastNn", PrimType::Int64, 0, "amount", rune::view::MergeRule::LastNonNull}};
    return v;
}

// One group's Count(*) + Max(amount): isolates the O(group) rescan.
rune::view::ViewPlan groupMaxView() {
    rune::view::ViewPlan v;
    v.viewName = "group_max";
    v.key      = "userId";
    v.sources  = {{5, "userId"}};
    v.output   = {{"userId", PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                  {"n", PrimType::Int64, 0, "", rune::view::MergeRule::CountAll},
                  {"max", PrimType::Int64, 0, "amount", rune::view::MergeRule::Max}};
    return v;
}
constexpr int64_t kExtremeGroup = 7;
```

- [ ] **Step 2: Factor the shared rig (F10).** (Amended 2026-10-20: Task 5b Step 6 later gives `AggRig` a `std::optional<ViewTable> blocks` and passes it to the fold. The code below is the pre-5b shape, already committed in cb26f195.) `BM_ViewFoldAggregate` and the new gate run the identical workload (same append, drain, fold and prime code). Define it once. Directly before the anonymous namespace's closing `} // namespace` (line 440), add:

```cpp
// --- Shared aggregate rig (BM_ViewFoldAggregate, BM_ViewFoldRetractable) ---
// 1,000,000 contributors / 10,000 groups, amounts from the churn's `hp`.
constexpr uint64_t kAggContributors = 1'000'000;
constexpr uint64_t kAggGroups       = 10'000;

struct AggRig {
    rune::codec::Descriptor   src  = orderSourceV1();
    rune::view::CompileResult comp;
    rune::view::AggLayout     agg;
    ViewTable                 view, contributors, groups;
    VersionDescriptorCache    vdc;
    uint64_t                  rescans = 0;   // summed FoldStats::aggRescans (0 for a 2a-only view)

    AggRig(const rune::view::ViewPlan& plan, int32_t viewTopicId)
        : comp(compile(plan, src, viewTopicId)),
          agg(rune::view::buildAggLayout(comp.plan, src).value()),
          view(comp.rowLayout), contributors(agg.contributorRow), groups(agg.groupRow), vdc(src) {}

    static rune::view::CompileResult compile(const rune::view::ViewPlan& plan,
                                             const rune::codec::Descriptor& src, int32_t id) {
        const rune::codec::Descriptor* srcs[] = {&src};
        return compileView(plan, srcs, id).value();
    }

    // Contributor id c in [0, 1M); group fixed per c.
    static void append(rune::protocol::RecordBatchBuilder& bb, const Op& op) {
        const int64_t c = static_cast<int64_t>(op.key % kAggContributors);
        const auto kb = keyBytes(c);
        if (op.tombstone) {
            bb.append(asSv(kb), std::string_view{}, 0);
        } else {
            const auto val = encodeOrder(c, c % kAggGroups, static_cast<int64_t>(op.hp), 1);
            bb.append(asSv(kb), asSv(val), 0);
        }
    }
    void drainAll() {
        auto nop = [](uint32_t, const uint8_t*, bool, uint64_t) {};
        view.drainDirty(nop);
        contributors.drainDirty(nop);
        groups.drainDirty(nop);
    }
    rune::view::FoldStats foldStream(const BatchStream& s, size_t i) {
        auto lv = s.view(i);
        int64_t cur = 0;
        const auto fs = foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors,
                                           groups, lv, s.bound(i), cur);
        rescans += fs.aggRescans;
        return fs;
    }
    // Creates-only fold over the whole contributor space (NOT timed), then drains;
    // the tables reach their final population and capacity. Resets `rescans`.
    void prime() {
        constexpr int kPrimeBatch[] = {64};
        std::vector<Op> ops;
        ops.reserve(kAggContributors);
        for (uint64_t c = 0; c < kAggContributors; ++c)
            ops.push_back(Op{c, static_cast<int32_t>(c ^ 0x5a5a), false});
        const auto ps = buildStream(ops, kPrimeBatch, 1, append);
        for (size_t i = 0; i < ps.size(); ++i) foldStream(ps, i);
        drainAll();
        rescans = 0;
    }
};
```

Refactor `BM_ViewFoldAggregate` onto it, keeping every counter name and the printed output exactly. Replace the block from `constexpr uint64_t kContributors      = 1'000'000;` through the end of the prime block (`drainAll();\n        }` before `const auto ops = generateChurnOps(kTotalOps);`) with:

```cpp
    constexpr uint64_t kContributors      = kAggContributors;   // the liveness proof below uses it
    constexpr uint64_t kDrainEveryBatches = 1000;

    for (auto _ : st) {
        AggRig rig(userTotalsView(), 0x51000002);
        auto& view = rig.view;
        auto& contributors = rig.contributors;
        auto& groups = rig.groups;
        auto drainAll = [&] { rig.drainAll(); };
        auto foldStream = [&](const BatchStream& s, size_t i) { return rig.foldStream(s, i); };
        auto append = &AggRig::append;
        rig.prime();
```

The rest of `BM_ViewFoldAggregate` (the `ops` / `stream` / `runPhases` calls, the retract+re-add proof, the counters and the gate print) stays unchanged: it uses the same local names (`view`, `contributors`, `groups`, `drainAll`, `foldStream`, `append`) and `kGroups` is no longer needed there. Delete any now-unused local.

- [ ] **Step 3: Add the gated benchmark.** Append to the end of `broker/benchmark/ViewFoldBench.cpp`:

```cpp
// Retractable aggregate gate (Phase 2b, §14.0): user_stats = Count(*), Sum,
// Max, Min, Last, LastNonNull over amount, grouped by userId — the SAME rig,
// workload, priming, drain cadence and three phases as BM_ViewFoldAggregate
// (shared AggRig: 1,000,000 contributors / 10,000 groups, ~100 per group). The
// churn's pseudo-random amounts retract a group's cached Min or Max on roughly
// 2% of retracting ops, so this IS the typical case including its occasional
// O(group) rescans (ret_rescans counts them). BM_ViewFoldExtremeRetract
// isolates that path. GATE: ret_record_p99_us < 5, ret_batch_p99_us < 10,
// steady_alloc == 0.
static void BM_ViewFoldRetractable(benchmark::State& st) {
    constexpr uint64_t kDrainEveryBatches = 1000;

    for (auto _ : st) {
        AggRig rig(userStatsView(), 0x51000003);
        rig.prime();   // also zeroes rescans: count the churn's only

        const auto ops = generateChurnOps(kTotalOps);
        const auto stream = buildStream(ops, kBatchSizes, std::size(kBatchSizes), AggRig::append);
        auto r = runPhases(
            stream, [&](size_t i) { return rig.foldStream(stream, i); }, [&] { rig.drainAll(); },
            kDrainEveryBatches);

        benchmark::DoNotOptimize(rig.view.size());
        benchmark::DoNotOptimize(rig.groups.size());
        benchmark::DoNotOptimize(rig.contributors.size());
        reportCommon(st, "ret_", r, rig.view.size());
        st.counters["ret_rescans"]            = static_cast<double>(rig.rescans);
        st.counters["ret_contributors_final"] = static_cast<double>(rig.contributors.size());

        const double recP99 = st.counters["ret_record_p99_us"];
        const double batP99 = st.counters["ret_batch_p99_us"];
        const bool pass = recP99 < 5.0 && batP99 < 10.0 && r.steadyAlloc == 0;
        st.counters["ret_gate_pass"] = pass ? 1.0 : 0.0;
        std::fprintf(stderr,
                     "[ViewFoldRetractable gate] ret_record_p99_us=%.3f (<5) ret_batch_p99_us=%.3f (<10) "
                     "steady_alloc=%lld (==0) rescans=%llu => %s\n",
                     recP99, batP99, static_cast<long long>(r.steadyAlloc),
                     static_cast<unsigned long long>(rig.rescans), pass ? "PASS" : "FAIL");
    }
}
BENCHMARK(BM_ViewFoldRetractable)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();
```

Build `build/rel` and run the gate now (Task 5 Step 6), then commit:

```bash
git add broker/benchmark/ViewFoldBench.cpp
git commit -m "bench(view): §14.0 gate for the retractable fold; shared AggRig for the aggregate benches" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Add the extreme-retraction report.** Append to the end of `broker/benchmark/ViewFoldBench.cpp`:

```cpp
// Extreme retraction (Phase 2b) — REPORTED, NOT GATED (spec §A Gates: the known
// O(group size) path). One group of N contributors with amounts 1..N; each
// timed op lowers the CURRENT max holder (N, N-1, ..., 2) to 0, so every op
// retracts the cached Max with a lower value and scans the group's N-entry
// value block (Task 5b: ceil(N/32) chunk rows, sequential).
// Single-record batches; latency per op.
static void BM_ViewFoldExtremeRetract(benchmark::State& st) {
    const int64_t groupSize = st.range(0);
    constexpr int kOne[]        = {1};
    constexpr int kPrimeBatch[] = {64};

    for (auto _ : st) {
        auto src = orderSourceV1();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = compileView(groupMaxView(), srcs, 0x51000004).value();
        auto agg = rune::view::buildAggLayout(comp.plan, src).value();
        ViewTable view(comp.rowLayout);
        ViewTable contributors(agg.contributorRow), groups(agg.groupRow);
        ViewTable blocks(agg.blockRow);   // groupMaxView has Max: the value block (Task 5b)
        VersionDescriptorCache vdc(src);

        // Op.key = contributor id, Op.hp = amount; every record in kExtremeGroup.
        auto append = [](rune::protocol::RecordBatchBuilder& bb, const Op& op) {
            const auto kb = keyBytes(static_cast<int64_t>(op.key));
            const auto val = encodeOrder(static_cast<int64_t>(op.key), kExtremeGroup,
                                         static_cast<int64_t>(op.hp), 1);
            bb.append(asSv(kb), asSv(val), 0);
        };
        auto fold = [&](const BatchStream& s, size_t i) {
            auto lv = s.view(i);
            int64_t cur = 0;
            return foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors,
                                      groups, &blocks, lv, s.bound(i), cur);
        };

        // Prime (NOT timed): contributors 1..N, amount = id -> max = N.
        std::vector<Op> prime;
        prime.reserve(static_cast<size_t>(groupSize));
        for (int64_t c = 1; c <= groupSize; ++c)
            prime.push_back(Op{static_cast<uint64_t>(c), static_cast<int32_t>(c), false});
        const auto ps = buildStream(prime, kPrimeBatch, 1, append);
        for (size_t i = 0; i < ps.size(); ++i) fold(ps, i);

        // Measured: lower the current max holder to 0 (built before timing).
        std::vector<Op> ops;
        for (int64_t k = groupSize; k >= 2; --k)
            ops.push_back(Op{static_cast<uint64_t>(k), 0, false});
        const auto ms = buildStream(ops, kOne, 1, append);

        std::vector<uint32_t> lat;
        lat.reserve(ms.size());
        uint64_t rescans = 0;
        for (size_t i = 0; i < ms.size(); ++i) {
            const auto t0 = std::chrono::steady_clock::now();
            const auto fs = fold(ms, i);
            const auto t1 = std::chrono::steady_clock::now();
            lat.push_back(static_cast<uint32_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()));
            rescans += fs.aggRescans;
        }

        const uint8_t* row = view.get(static_cast<uint64_t>(kExtremeGroup));
        const bool exact = row != nullptr && readSlot(row, comp.rowLayout, 2).i == 1 &&
                           rescans == ops.size();
        st.counters["extreme_group_size"] = static_cast<double>(groupSize);
        st.counters["extreme_p50_us"]     = percentileUs(lat, 0.50);
        st.counters["extreme_p99_us"]     = percentileUs(lat, 0.99);
        st.counters["extreme_max_us"]     = percentileUs(lat, 1.0);
        st.counters["extreme_rescans"]    = static_cast<double>(rescans);
        st.counters["extreme_exact"]      = exact ? 1.0 : 0.0;
        std::fprintf(stderr,
                     "[ViewFoldExtremeRetract N=%lld] p50=%.3fus p99=%.3fus max=%.3fus rescans=%llu "
                     "(reported, not gated) exact=%s\n",
                     static_cast<long long>(groupSize), st.counters["extreme_p50_us"].value,
                     st.counters["extreme_p99_us"].value, st.counters["extreme_max_us"].value,
                     static_cast<unsigned long long>(rescans), exact ? "yes" : "NO");
    }
}
BENCHMARK(BM_ViewFoldExtremeRetract)
    ->Arg(1000)
    ->Arg(10000)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();
```

- [ ] **Step 5: Run the benchmarks.**

Run: `cmake --build build/rel -j14 --target rune_broker_benchmark && build/rel/broker/rune_broker_benchmark --benchmark_filter='ViewFold'`
Expected:
- `[ViewFoldRetractable gate] … => PASS` (`ret_record_p99_us < 5`, `ret_batch_p99_us < 10`, `steady_alloc == 0`).
- `BM_ViewFoldReplace` and `BM_ViewFoldAggregate` still PASS, which shows the 2a path did not regress (and that the `AggRig` refactor kept its counters and output).
- `[ViewFoldExtremeRetract N=1000]` and `[ViewFoldExtremeRetract N=10000]` report `exact=yes` with their p50/p99/max. Record these numbers in the commit message; they are not gated. (Task 5b pre-validation: N=1000 p50 1.46 µs and N=10000 p50 11.4 µs on the dev Mac.)

If the gate fails, profile before changing anything. `ret_rescans / measured records` shows how often the O(group) path ran. A non-zero `steady_alloc` means a hot-path allocation, which is a CLAUDE.md #4 violation that must be fixed rather than gated away. Never relax the gate. The local run is the per-task check; the controller decides whether a clean-box VM confirmation is needed at the Phase 2b close (F14).

- [ ] **Step 6: Run the affected tests and commit.**

Run: `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'Agg' -j14 --output-on-failure` → 0 failures (the bench change touches no library code; this guards the build).

```bash
git add broker/benchmark/ViewFoldBench.cpp
git commit -m "bench(view): report extreme retraction at 1k/10k (p50/p99 in body)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: e2e — `user_totals` gains `max`, `min`, `last` across the restart

**Files:**
- Modify: `client-cpp/examples/view_probe.cpp` (anchors: the header comment item `//     (d) Phase 2a: the PERSISTED aggregate view user_totals` (lines 59–64); the helpers `userTotalsPlan` (321) / `userTotalsRowLayout` (332–337) / `waitForTotals` (351–383); the setup leg's `"user_totals u100 (initial)"` call (lines 1275–1276); the verify leg `// ---- (d) aggregate view recovered` (block 1456–1465, `return 44;` at 1465))
- Modify: `broker/docker/view-engine-e2e.sh` (anchors: header item `#     5. Phase 2a: defineTopic "order"` (lines 23–26); header item `#     (d) user_totals recovered from its v2 checkpoint` (lines 48–51); the banner `log "  -> user_totals (Count(*)/Sum/Avg) recovered` + the next line (lines 583–584); the stale usage text at lines 66–70, "Run from the rune-broker repo root … client repo)." (fix-on-sight, Step 4))
- Modify: `docs/api/client-cpp.md:406` (F5: the `MergeRule` list gains `LastNonNull` and one line each on Min/Max, Last, LastNonNull)
- Create: `broker/docs/release-notes/phase2b-a-retractable-aggregates.md` (F11: semantics of the four rules, `LastNonNull` = wire 8, no checkpoint/catalog format change, AD1: upgrade every broker before creating such a view)

**Interfaces:**
- Consumes: everything above, on the real Aeron broker. The handoff env vars are unchanged (same view).

Scenario (`order` = `{orderId key, userId, amount}`, 1 partition; `user_totals` = `userId`, `n` Count(\*), `total` Sum, `avg` Avg, **`max` Max, `min` Min, `last` Last** over amount):
- **setup:** `o1(u100,10)`, `o2(u100,30)`, `o3(u200,5)`, then `o1→(u100,50)`, then `o3→(u100,5)` (a group move). The MRU order is then o3, o1, o2. Expected: `u100 = {n 3, total 85, avg 85/3, max 50, min 5, last 5}`, and `u200` AbsentKey.
- **restart.**
- **verify:** the recovered row is `{3, 85, 85/3, 50, 5, 5}`. Then `o3→(u100,25)` retracts the cached **min** (5) with a higher value, so it rescans the recovered list: `{3, 105, 35, 50, 25, 25}`. Then `o1→(u100,20)` retracts the cached **max** (50) with a lower value, so it rescans: `{3, 75, 25, 30, 20, 20}`.

- [ ] **Step 1: Extend the probe helpers.** In `client-cpp/examples/view_probe.cpp`, make these changes:

(a) In `userTotalsPlan`, after the `avg` line, add:

```cpp
    p.output.push_back(rcv::AuthoredField{"max", P::Int64, 0, "amount", rcv::MergeRule::Max});
    p.output.push_back(rcv::AuthoredField{"min", P::Int64, 0, "amount", rcv::MergeRule::Min});
    p.output.push_back(rcv::AuthoredField{"last", P::Int64, 0, "amount", rcv::MergeRule::Last});
```

(b) Replace `userTotalsRowLayout` (and the comment above it) with:

```cpp
// Slot order == userTotalsPlan output order: userId, n, total, avg, max, min, last.
static rcv::RowLayout userTotalsRowLayout() {
    using P = rune::codec::PrimType;
    const P types[] = {P::Int64, P::Int64, P::Int64, P::Float64, P::Int64, P::Int64, P::Int64};
    return rcv::buildRowLayout(types);
}
```

(c) Replace `waitForTotals` (and the comment above it) with:

```cpp
// Expected user_totals row: Phase 2a columns + Phase 2b max/min/last.
struct Totals { int64_t n; int64_t total; double avg; int64_t max; int64_t min; int64_t last; };
// Polls user `u` until the row is {userId=u, want...} with every slot present
// (non-null; avg within 1e-9) or the bound expires.
static bool waitForTotals(Client& c, int32_t viewId, uint64_t fp, int64_t u, const Totals& w,
                          int boundMs, int timeoutMs, const char* who) {
    const rcv::RowLayout L = userTotalsRowLayout();
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    ViewRowResult r;
    for (;;) {
        r = c.getViewRowAndWait(viewId, static_cast<uint64_t>(u), fp, std::chrono::milliseconds(timeoutMs));
        if (r.status == ViewRowResult::kOk && r.rowBytes.size() == L.rowLen) {
            const auto row = std::span<const uint8_t>(r.rowBytes);
            const auto uf = rcv::readSlot(row, L, 0), nf = rcv::readSlot(row, L, 1),
                       tf = rcv::readSlot(row, L, 2), af = rcv::readSlot(row, L, 3),
                       xf = rcv::readSlot(row, L, 4), mf = rcv::readSlot(row, L, 5),
                       lf = rcv::readSlot(row, L, 6);
            const bool allPresent = uf.present && nf.present && tf.present && af.present &&
                                    xf.present && mf.present && lf.present;
            if (allPresent && uf.i == u && nf.i == w.n && tf.i == w.total && std::abs(af.f - w.avg) < 1e-9 &&
                xf.i == w.max && mf.i == w.min && lf.i == w.last) {
                std::cerr << "PROBE: " << who << " OK userId=" << u << " n=" << w.n << " total=" << w.total
                          << " avg=" << w.avg << " max=" << w.max << " min=" << w.min << " last=" << w.last << "\n";
                return true;
            }
            if (std::chrono::steady_clock::now() >= dl) {
                std::cerr << "PROBE: FAIL " << who << " got userId=" << uf.i << " n=" << nf.i << " total=" << tf.i
                          << " avg=" << af.f << " max=" << xf.i << " min=" << mf.i << " last=" << lf.i
                          << " (allPresent=" << allPresent << ")"
                          << " want n=" << w.n << " total=" << w.total << " avg=" << w.avg << " max=" << w.max
                          << " min=" << w.min << " last=" << w.last << "\n";
                return false;
            }
        } else if (std::chrono::steady_clock::now() >= dl) {
            std::cerr << "PROBE: FAIL " << who << " status=" << r.status << "\n";
            return false;
        }
        std::this_thread::sleep_for(50ms);
    }
}
```

- [ ] **Step 2: Setup leg.** Replace

```cpp
            if (!waitForTotals(c, viewA.viewTopicId, viewA.outputFingerprint, 100, 3, 85, 85.0 / 3,
                               boundMs, timeoutMs, "user_totals u100 (initial)")) return 33;
```

with

```cpp
            // MRU after the five writes: o3 (5), o1 (50), o2 (30).
            if (!waitForTotals(c, viewA.viewTopicId, viewA.outputFingerprint, 100,
                               Totals{3, 85, 85.0 / 3, 50, 5, 5},
                               boundMs, timeoutMs, "user_totals u100 (initial)")) return 33;
```

- [ ] **Step 3: Verify leg.** Replace the block from `if (!waitForTotals(c, viewIdA, fpA, 100, 3, 85, 85.0 / 3, …` through `"user_totals u100 (post-restart update retracts pre-image)")) return 44;` with:

```cpp
            if (!waitForTotals(c, viewIdA, fpA, 100, Totals{3, 85, 85.0 / 3, 50, 5, 5}, boundMs, timeoutMs,
                               "user_totals u100 (post-restart, recovered)")) return 41;
            if (!waitForAbsent(c, viewIdA, fpA, 200, boundMs, timeoutMs, "user_totals u200 (post-restart)")) return 42;
            // Update a PRE-restart contributor: o3 5 -> 25. Correct only if its
            // pre-image survived the restart (lost map => n 4, total 110); it also
            // retracts the cached MIN (5) with a higher value -> rescan of the
            // RECOVERED contributor list -> min 25.
            if (!sendOrder(c, TopicHandle{orderTopic, 1}, 3, 100, 25, timeoutMs)) return 43;
            if (!waitForTotals(c, viewIdA, fpA, 100, Totals{3, 105, 35.0, 50, 25, 25}, boundMs, timeoutMs,
                               "user_totals u100 (post-restart update retracts pre-image + min)")) return 44;
            // o1 50 -> 20 retracts the cached MAX with a lower value -> rescan -> max 30 (o2).
            if (!sendOrder(c, TopicHandle{orderTopic, 1}, 1, 100, 20, timeoutMs)) return 45;
            if (!waitForTotals(c, viewIdA, fpA, 100, Totals{3, 75, 25.0, 30, 20, 20}, boundMs, timeoutMs,
                               "user_totals u100 (post-restart max retraction rescans)")) return 46;
```

Update the header comment item `(d)` (the 6 lines from `//     (d) Phase 2a: the PERSISTED aggregate view user_totals`, ~59–64) to read:

```cpp
//     (d) Phase 2a/2b: the PERSISTED aggregate view user_totals (GROUP BY userId:
//         Count(*), Sum, Avg, Max, Min, Last over "order") recovers its rows,
//         contributor map, MRU lists/caches AND Min/Max value blocks from the v2 checkpoint:
//         u100 = {3, 85, 85/3, max 50, min 5, last 5}, u200 absent (emptied by a
//         pre-restart group move); o3 5 -> 25 retracts the pre-image AND the
//         cached min -> {3, 105, 35, 50, 25, 25}; o1 50 -> 20 retracts the cached
//         max -> {3, 75, 25, 30, 20, 20} (both rescans scan the recovered value block).
```

Build: `cmake --build build/sub-client-cpp --target view_probe`. Expected: it compiles (`build/sub-client-cpp` has `BUILD_EXAMPLES=ON`).

- [ ] **Step 4: e2e script text.** In `broker/docker/view-engine-e2e.sh`, replace the header item 5 (lines 23–26, from `#     5. Phase 2a: defineTopic "order"`) with:

```bash
#     5. Phase 2a/2b: defineTopic "order" {orderId key, userId, amount}; createView
#        "user_totals" (persistence=0; GROUP BY userId: Count(*), Sum, Avg, Max,
#        Min, Last); produce o1(u100,10) o2(u100,30) o3(u200,5), update o1->50,
#        move o3->(u100,5) -> u100 = {3, 85, 85/3, 50, 5, 5}, u200 AbsentKey.
```

Replace the header item `(d)` (lines 48–51, from `#     (d) user_totals recovered from its v2 checkpoint`) with:

```bash
#     (d) user_totals recovered from its v2 checkpoint: u100 = {3, 85, 85/3, 50,
#         5, 5}, u200 AbsentKey; then o3 5->25 (a PRE-restart contributor; also
#         retracts the cached min) -> {3, 105, 35, 50, 25, 25}; then o1 50->20
#         (retracts the cached max) -> {3, 75, 25, 30, 20, 20} — proves the
#         contributor map, MRU lists and caches all survived the restart.
```

Replace the two banner lines (`log "  -> user_totals (Count(*)/Sum/Avg) recovered …"` and `log "     + post-restart update retracts OK"`, lines 583–584) with:

```bash
log "  -> user_totals (Count(*)/Sum/Avg/Max/Min/Last) recovered from its v2 checkpoint (VIEW-RECOVERED format=v2, O>0)"
log "     + post-restart update retracts the pre-image, min and max exactly OK"
```

Fix the stale usage text (lines 66–70; the script already computes its monorepo paths, only the comment is stale). Replace

```bash
# Usage:  bash docker/view-engine-e2e.sh
# Run from the rune-broker repo root (this worktree). Requires the sibling
# worktrees: ../protocol ../codec ../crypto ../flight-recorder ../ql
# ../client-cpp (feat/view-engine-phase1 on both the broker and the
# client repo).
```

with

```bash
# Usage:  bash broker/docker/view-engine-e2e.sh
# Run from the monorepo (broker/docker); requires ../flight-recorder next to the
# monorepo.
```

- [ ] **Step 5: Document the new rules in the client API doc (F5).** In `docs/api/client-cpp.md` (line 406) replace

```markdown
- `MergeRule`: `Replace`, `Count`, `Sum`, `Avg`, `Min`, `Max`, `Last`, `CountAll` (`count(*)`).
```

with

```markdown
- `MergeRule`: `Replace`, `Count`, `Sum`, `Avg`, `Min`, `Max`, `Last`, `CountAll` (`count(*)`), `LastNonNull` (wire value 8, appended).
  - `Min` / `Max`: numeric sources only (Int32, Int64, UInt16, UInt32, UInt64, Float64); nulls are ignored (NaN and ±Inf count as NULL); an empty or all-null group gives NULL. Exact under update, group move and delete.
  - `Last`: the value of the most recently written contributor of the group, including NULL. Any fixed-width source type.
  - `LastNonNull`: the most recently written contributor value that is not NULL, or NULL if there is none.
  - All four keep the source field's type as the output type. Create a view that uses one only after every broker runs a build that has them.
```

- [ ] **Step 6: Write the release note (F11).** Create `broker/docs/release-notes/phase2b-a-retractable-aggregates.md` (siblings: `phase2b-c-…`, `phase2b-e-…`, `phase2b-f-…`):

```markdown
# Phase 2b Part A: retractable Min / Max / Last / LastNonNull

Aggregate views can now maintain `Min`, `Max`, `Last` and `LastNonNull`, exact under update, group move, delete and a broker restart.

## Semantics

- `Min` / `Max`: numeric sources only (Int32, Int64, UInt16, UInt32, UInt64, Float64). Output type = source type. Nulls are ignored; NaN and ±Inf count as NULL (counted in `aggNonFinite`). An empty or all-null group gives NULL.
- `Last`: the value of the most recently written contributor, including NULL. Any fixed-width source type; output type = source type.
- `LastNonNull`: the most recently written contributor value that is not NULL, or NULL if there is none. It is the new `MergeRule::LastNonNull`, wire value **8** (the enum is append-only; rules 0-7 are unchanged).
- Retracting the cached Min/Max scans the group's packed Min/Max value block once (O(group size) but sequential, reported by `BM_ViewFoldExtremeRetract` at 1k and 10k); the typical fold stays inside the §14.0 gate.

## Compatibility

- No catalog or fingerprint format change, and the v2 checkpoint format is unchanged. The new links, head and caches are bytes inside the contributor and group rows. A view with Min/Max checkpoints one more table in the same v2 record (its per-group value blocks, table id 3), restored offset-atomically with the others. `CatalogGoldenRegression.*` passes unchanged.
- Views that use none of the four rules keep their 2a layout byte for byte.
- **Upgrade every broker before creating a view with min/max/last (AD1).** `CreateView` events are applied on every broker from `__catalog`, and a broker without Part A cannot install such a plan. There is no version negotiation yet.
- The client's `MergeRule` gains `LastNonNull`, and the client's `deserializeAuthored` accepts it (`rune catalog replay` re-reads stored plans with it).
- Per-pass delta cap: a view with Min/Max dirties up to 12 entries per source record (Last-only: 8, 2a: 5), and the worker's per-pass record cap now follows from that exact bound.
```

- [ ] **Step 7: Run the e2e on the VM (controller-run).** This is a controller checkpoint (k3s VM, `/root/vm.lock`); the implementer stops after Step 8's builds and tests. The controller stages the monorepo **and** a sibling `flight-recorder`, then runs the script under the lock:

```bash
rsync -a --delete --exclude build/ --exclude .git/ /Users/vradu/Projects/rune/ root@$RUNE_VM_HOST:/root/mono/rune/
rsync -a --delete /Users/vradu/Projects/flight-recorder/ root@$RUNE_VM_HOST:/root/mono/flight-recorder/
ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash /root/mono/rune/broker/docker/view-engine-e2e.sh'
```

(Without `.git` the image tags fall back to content hashes, `repo_tag` in the script.) The script is authenticated since the auth phase — it bootstraps through `broker/docker/lib/auth.sh` and passes `RUNE_USER` / `RUNE_PASSWORD_FILE` to the probe; change nothing there.

Expected: `VIEW-ENGINE-E2E: OK`, `SCRIPT_EXIT=0`, and six `user_totals` PROBE lines: initial, the u200 group move, recovered, u200 post-restart, pre-image + min, and max retraction.

- [ ] **Step 8: Run the affected tests and commit.**

Run: `cmake --build build/sub-client-cpp --target view_probe rune_schema_codec_smoke_tests && ctest --test-dir build/sub-client-cpp -R 'ViewPlanAuthor|SchemaCodecSmoke' --output-on-failure`. Expected: it builds and 0 failures. The full `*Regression*` suites (broker and client), the sanitizers and the e2e are controller checkpoints.

```bash
git add client-cpp/examples/view_probe.cpp docs/api/client-cpp.md
git commit -m "test(view-probe): user_totals gains max/min/last; post-restart retraction of min and max; document the new MergeRule values" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git add broker/docker/view-engine-e2e.sh broker/docs/release-notes/phase2b-a-retractable-aggregates.md
git commit -m "test(view): e2e covers retractable Min/Max/Last across a broker restart; release note" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Finish

The implementer's part ends with the per-task affected-area runs above. The remaining items are **controller checkpoints** (F13):

- [ ] Full broker `*Regression*` suite on the final tree: `ctest --test-dir build/root -R Regression -j14 --output-on-failure` → 0 failures.
- [ ] View suites: `ctest --test-dir build/root -R 'View|PlanCompiler|Agg|GetViewRow|CreateView|WorkerViewSet' -j14` → 0 failures.
- [ ] Full client `*Regression*` suite: `ctest --test-dir build/sub-client-cpp -R Regression -j14` → 0 failures.
- [ ] CatalogGolden (AD2): `cmake --build build/sub-codec --target rune_catalog_tests && ctest --test-dir build/sub-codec -R CatalogGolden` → 3 pass.
- [ ] Benchmarks (`build/rel`): `BM_ViewFoldRetractable` must PASS; `BM_ViewFoldReplace` and `BM_ViewFoldAggregate` must still PASS; record the `BM_ViewFoldExtremeRetract` 1k/10k numbers.
- [ ] The e2e on the VM must be green (Task 9 Step 7).
- [ ] Sanitizers (ASan+UBSan, TSan) run once at the Phase 2b close; none exist in the monorepo today.
- [ ] Hand the branch to the controller. Do not merge or push here.

---

## Self-review (done while writing)

- **Spec coverage (§A):**
  - Semantics: Tasks 2 and 5.
  - `LastNonNull = 8`: Task 1.
  - Storage (links, head, caches, per-input width, 2a untouched): Tasks 3 and 4.
  - Maintenance (add/retract/rescan, skipped rescan, Last O(1), LastNonNull walk, emit, inconsistency): Task 5.
  - Persistence (no format change, recomputed exact bound + test): Tasks 4, 5b, 6 and 7. Amended value block (storage, maintenance, table 3, bound {5,2,2,3}, gate rerun): Task 5b.
  - Gates:
    - typical fold: Task 8.
    - extreme benchmarks at 1k/10k: Task 8.
    - unit tests:
      - duplicates: Task 5.
      - moves: Task 5.
      - in-place raise and lower: Task 5.
      - Last after deleting the newest: Task 5.
      - LastNonNull: Task 5.
      - NaN/Inf: Task 5.
    - restart retraction: Task 7.
    - e2e: Task 9.
- **Type consistency:**
  - `aggDirtyBoundPerRecord` returns `AggDirtyBound{contributorRows, groupRows, viewRows, blockRows}` (`blockRows` from Task 5b) in Tasks 5b, 6 and 7.
  - `CacheState`, `kCacheEntryBytes`, `kNoCache` and `kNoContributor` are used identically in Tasks 4, 5 and 6.
  - `encodeSlotValue`/`decodeSlotValue` have the same signatures in Tasks 3 and 5.
  - `FoldStats::aggRescans` is used in Tasks 5 and 8.
- **2a invariance:** the non-linked fold path calls the same `put`/`erase` in the same order with the same bytes. `TwoAOnlyLayoutIsUnchangedRegression`, the untouched `AggFold*` suite and 2a's WorkerViewSet ring and recovery tests all guard it.

## Spec questions

*Ruled 2026-10-20 (F12): Q1–Q8 are accepted exactly as written below; Q1 is recorded as AD3 in *Decisions*.*

1. **Value-slot width for non-Last inputs.** §A Storage says input slots "get the source field's width … instead of a fixed 8". Applied to every input, that would change a 2a view's contributor row (for example, `Sum(Int32)` would shrink to 4 bytes) and break the "2a-only views keep their 2a layout byte-for-byte" rule. **The plan applies the source width only to `Last`/`LastNonNull` inputs.** Count, Sum, Avg, **Min and Max** keep the widened 8-byte slot, so Min/Max compare in one domain. Please confirm.
2. **Contributor key `0xFFFFFFFFFFFFFFFF`** is a valid 8-byte wire key but equals the spec's `kNoContributor` sentinel. **The plan skips it in linked views and counts it as `skippedMalformed`.** 2a views still accept it. The alternative is a separate presence flag per link. Please confirm that fail-closed behaviour.
3. **`Last`/`LastNonNull` over Float64 NaN/±Inf.** The spec states the NaN→NULL rule only for Min/Max. **The plan returns NaN/Inf verbatim as non-null values for Last/LastNonNull**, matching 2a's `Count(col)`, and does not count them in `aggNonFinite`.
4. **`[valid u8]` is used as a three-state byte**: Empty 0, Valid 1, RescanPending 2. Values 0 and 1 keep the spec's meaning. State 2 is transient within a record and is never checkpointed (asserted by the model test).
5. **Dirty-bound enumeration.** The spec's list ("contributor, both neighbours in the old list, the old head's group, the new head, both groups, both view rows") is ambiguous about "the old head's group". The plan derives **4 contributor rows + 2 group rows + 2 view rows = 8**, against 5 for 2a, and pins it exactly. The rescan reads only and dirties nothing extra. *(Amended 2026-10-20, Task 5b: Min/Max views add the swap-moved contributor and 3 value-block rows, {5,2,2,3} = 12; Last-only views stay at 8.)*
6. **Unused 2a accumulator bytes.** In linked views, each Min/Max/Last/LastNonNull column still occupies its 16-byte 2a accumulator slot in the group row, and that slot is unused. This keeps one group-row shape for both phases. If row compactness matters more than uniformity, the slots could be packed per rule.
7. **`FoldStats::aggRescans`** is a new observability counter not named in the spec. It is used by tests (to show a rescan was skipped) and benchmarks. It is not surfaced in `ViewFoldCounters`. *(Amended 2026-10-20: surfaced as `ViewFoldCounters::aggRescans` in Task 7, the carried Task 5 review item.)*
8. **Naming.** The controller gave the slug `a-retractable-min-max-last`. The plan file and branch use `…-phase2b-a-retractable-min-max-last` and `feat/phase2b-a-retractable-min-max-last`, which avoids a doubled `a-a-`.

### Spec gaps found 2026-10-19

- **G1 — Mixed-version brokers and new merge rules** (spec §A is silent): a `CreateView` with a rule a broker's build does not know is replicated to it through `__catalog`. Resolved by AD1 (upgrade every broker first; documented; the unknown-rule rejection stays fail-closed).
