# Rune Phase 2b — Part A: Retractable Min / Max / Last / LastNonNull Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Aggregate views can maintain `Min`, `Max`, `Last` and `LastNonNull` with exact retraction on update, group move and delete, and keep that exact across a checkpoint/restart. 2a-only views stay exactly as they are.

**Architecture:** Only views that use one of the four rules are **linked**. In a linked view, each contributor row gains `[prev u64][next u64]` links that form a doubly linked MRU list of the group's contributors, keyed by contributor key (`kNoContributor = UINT64_MAX`). Each group row gains `[head u64]` plus, per `Min`/`Max` field, a cached `[value 8B][state u8]`. `Last` is the head's value and `LastNonNull` is the first non-null value walking from the head. `Min`/`Max` come from the cache, and the fold walks the list once only when the cached extreme is retracted. Links, heads and caches live in the contributor and group tables that the 2a v2 multi-table checkpoint already persists, so recovery does not change. The per-pass delta cap's worst case per record grows from 5 to 8 dirty entries for linked views; the plan derives that bound exactly and a test pins it.

**Tech Stack:** C++23 (clang, `__int128`), GoogleTest, Google Benchmark, CMake/Ninja (`build-fast`), docker e2e on the k3s VM.

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` §A (binding). Parent: `docs/superpowers/specs/2026-10-08-rune-view-engine-single-source-design.md` §6 (aggregate table), §6.4 (nulls), §9.2 (aux state is checkpointed), §14.0 (latency gates).

**Builds on:** Phase 2a plan `docs/superpowers/plans/2026-10-11-rune-view-engine-phase2a-invertible-aggregates.md` (landed: `AggState`, `foldAggregateBatch`, v2 checkpoint, `AggInstance`, per-pass cap, `BM_ViewFoldAggregate`, `user_totals` e2e).

## Execution context

- **Repos:** broker and client only. Use the canonical sibling layout under `/Users/vradu/Projects/se-worktrees/` (`rune-broker`, `rune-client-cpp`, and `../rune-codec` on `feat/schema-evolution-durable-tags`, which the broker CMake requires).
- **Branches:** cut `feat/phase2b-a-retractable-min-max-last` from the integration branch `feature/materialized-views` in both repos:
  ```bash
  git -C /Users/vradu/Projects/se-worktrees/rune-broker switch -c feat/phase2b-a-retractable-min-max-last feature/materialized-views
  git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp switch -c feat/phase2b-a-retractable-min-max-last feature/materialized-views
  ```
  All broker paths below are relative to `/Users/vradu/Projects/se-worktrees/rune-broker`. Client paths are prefixed `rune-client-cpp/` and are relative to `/Users/vradu/Projects/se-worktrees/`.
- **Broker builds:** `build-fast` (Ninja, Debug, `BUILD_TESTING=ON`, already configured). Benchmarks use `build-bench-rel` (Ninja, `-O3 -DNDEBUG`, `BUILD_BENCHMARKS=ON`, already configured). If either directory is missing, configure it with `cmake -S . -B build-fast -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators"`; for benchmarks, add `-DBUILD_BENCHMARKS=ON -DCMAKE_CXX_FLAGS_DEBUG="-O3 -DNDEBUG"` and use `-B build-bench-rel`.
- **Client build:** `rune-client-cpp/build` (Unix Makefiles, Debug, `BUILD_TESTING=ON`, `BUILD_EXAMPLES=ON`, already configured). `cmake --build build` re-runs configure when `CMakeLists.txt` changes.
- **Gate before every commit (CLAUDE.md #7):** run the full `*Regression*` suite of each touched repo first, then that task's own tests. Broker: `ctest --test-dir build-fast -R Regression --output-on-failure`. Client: `ctest --test-dir build -R Regression --output-on-failure`. Every fix gets a test whose suite or name contains `Regression`.
- Nothing is pushed. Every commit message ends with the trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Global Constraints

- CLAUDE.md hard rules: shared-nothing; rings are the only cross-thread passing; state machines (each state holds exactly its variables); zero steady-state allocation; test and benchmark everything; a `*Regression*` test per fix; full Regression suite first; fix-on-sight.
- `Min`/`Max` take numeric source types only (Int32/Int64/UInt16/UInt32/UInt64/Float64). Output type = source type. Nulls are ignored. NaN/±Inf count as NULL and are counted in `aggNonFinite` (as in 2a). An empty or all-null group gives NULL.
- `Last` is the value of the **most recently written** contributor (highest log offset), **including null**. It accepts any fixed-width source type. Output type = source type.
- `LastNonNull` is the value of the most recently written contributor whose value is non-null, or NULL if there is none. It uses the new wire rule **`MergeRule::LastNonNull = 8`** (append-only).
- Table semantics, the co-partition rule (single-partition source) and retraction on update, group move and delete are exactly 2a's.
- **Only views that use one of the four rules get the extra columns. 2a-only views keep their 2a layout byte-for-byte**, including the fold, emit order, counters and v2 checkpoint bytes.
- Wire enums are append-only. Persistence: no new tables and no format change. Recovery is unchanged.
- Inconsistent state (a link to a missing contributor, or a rescan whose non-null count does not match) increments `aggInconsistent`. It never crashes and never invents state.
- §14.0 gate (typical-case fold): **record p99 < 5 µs, batch p99 < 10 µs, zero steady-state allocation.** The extreme-retraction benchmark (groups of 1k and 10k contributors) is **reported, not gated**.

## Review Focus

These are the input classes most likely to hurt a user. Each one is pinned by a named test in the task that owns it:

1. **Retracting one of several equal extremes.** `Max` must stay at the duplicated value and must not fall to the next distinct value. Test: `AggRetractableRegression.RetractingOneOfDuplicateExtremesKeepsTheExtremeRegression` (Task 5).
2. **A restart, then retraction of the extreme.** The rescan must walk the **recovered** list and caches and give the exact next extreme. Tests: `WorkerViewSetRegression.RestartThenRetractingTheMaxIsExactRegression` (Task 7) and the e2e verify leg (Task 9: `o3 5→25` retracts the `min`, `o1 50→20` retracts the `max`).
3. **The per-pass delta cap with linked rows.** A group move dirties up to 8 rows. If the bound is too low, a large pass throws on the worker thread. Tests: `AggRetractableRegression.WorstCaseMoveDirtiesExactlyTheDerivedBoundRegression` and `AggRetractableRegression.RandomizedOpsMatchBruteForceModelRegression` (Task 6), and `WorkerViewSetRegression.PersistedRetractableMovesDeltasFitTheRingRegression` (Task 7).
4. **A contributor whose 8-byte key equals the `kNoContributor` sentinel (`0xFF…FF`).** It must be skipped and counted, never linked; linking it would corrupt the list's end marker. Test: `AggRetractableRegression.ContributorKeyEqualToSentinelIsSkippedRegression` (Task 5).
5. **A damaged link** (a neighbour or head that is missing from the contributor table). It must be counted and must never be followed or created. There must be no loop and no crash. Test: `AggRetractableRegression.DanglingLinkIsCountedNotFollowedRegression` (Task 5).

Also pinned: 2a views are unchanged, by `AggStateRegression.TwoAOnlyLayoutIsUnchangedRegression` (Task 4) and the untouched `AggFold*` suite.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `libs/view/include/rune/view/ViewTypes.h`, `libs/view/src/ViewPlan.cpp` | `MergeRule::LastNonNull = 8`; accept rule byte 8 | 1 |
| `rune-client-cpp/include/rune/client/view/ViewPlanAuthor.h`, `rune-client-cpp/tests/client/ViewPlanAuthorTest.cpp` (new), `rune-client-cpp/CMakeLists.txt` | client mirror + wire pin | 1 |
| `libs/view/src/PlanCompiler.cpp` | accept Min/Max/Last/LastNonNull with source output types | 2 |
| `libs/view/include/rune/view/ViewRow.h`, `libs/view/src/ViewRow.cpp` | `encodeSlotValue`/`decodeSlotValue` (shared by `writeSlot`/`readSlot` and Last storage) | 3 |
| `libs/view/include/rune/view/AggState.h`, `libs/view/src/AggState.cpp` | linked layout: links, head, caches, per-input offset/width, `aggDirtyBoundPerRecord` | 4 |
| `libs/view/include/rune/view/ViewFold.h` | `FoldStats::aggRescans` | 5 |
| `libs/view/src/AggFold.cpp` | list maintenance, cache maintenance, rescan, Last/LastNonNull emit | 5 |
| `libs/view/tests/view/AggRetractableTest.cpp` (new), `libs/view/CMakeLists.txt` | fold tests, bound pin, model test | 5, 6 |
| `libs/node/src/WorkerViewSet.cpp` | per-pass cap from `aggDirtyBoundPerRecord` | 7 |
| `libs/node/tests/node/WorkerViewSetTest.cpp` | restart regression, large-range move regression | 7 |
| `benchmark/ViewFoldBench.cpp` | `BM_ViewFoldRetractable` (gate), `BM_ViewFoldExtremeRetract` (reported) | 8 |
| `rune-client-cpp/examples/view_probe.cpp`, `docker/view-engine-e2e.sh` | `user_totals` gains `max`/`min`/`last` across the restart | 9 |

---

### Task 1: `MergeRule::LastNonNull` on the wire (broker + client)

**Files:**
- Modify: `libs/view/include/rune/view/ViewTypes.h:6-8`
- Modify: `libs/view/src/ViewPlan.cpp:124-126`
- Modify: `libs/view/tests/view/ViewPlanTest.cpp:240-246`
- Modify: `rune-client-cpp/include/rune/client/view/ViewPlanAuthor.h:38-44`
- Create: `rune-client-cpp/tests/client/ViewPlanAuthorTest.cpp`
- Modify: `rune-client-cpp/CMakeLists.txt:833-837` (the `rune_schema_codec_smoke_tests` target)

**Interfaces:**
- Produces: `enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll, LastNonNull };` (`LastNonNull == 8`) in both `rune::view` and `rune::client::view`.

- [ ] **Step 1: Write the failing broker tests.** In `libs/view/tests/view/ViewPlanTest.cpp`, replace the whole `TEST(ViewPlanRegression, UnknownRuleValueIsRejected)` (lines 240–246) with:

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

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'ViewPlanRegression' --output-on-failure`
Expected: compile error `no member named 'LastNonNull' in 'rune::view::MergeRule'`.

- [ ] **Step 3: Implement the broker side.** In `libs/view/include/rune/view/ViewTypes.h`, replace lines 6–8 with:

```cpp
// Wire values are APPEND-ONLY (serialized as u8 in plan blobs): CountAll (= SQL
// Count(*), counts contributors including nulls) was appended at 7 in Phase 2a;
// LastNonNull (most recent non-null contributor value) at 8 in Phase 2b.
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll, LastNonNull };
```

In `libs/view/src/ViewPlan.cpp`, replace `isValidMergeRule`:

```cpp
bool isValidMergeRule(uint8_t v) {
    return v <= static_cast<uint8_t>(MergeRule::LastNonNull);
}
```

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'ViewPlan' --output-on-failure`
Expected: every `ViewPlan*` test passes.

- [ ] **Step 5: Write the failing client test.** Create `rune-client-cpp/tests/client/ViewPlanAuthorTest.cpp`:

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
}
```

In `rune-client-cpp/CMakeLists.txt`, replace the `rune_schema_codec_smoke_tests` block (lines 833–837) with:

```cmake
    add_executable(rune_schema_codec_smoke_tests
        tests/SchemaCodecSmokeTest.cpp
        tests/client/ViewPlanAuthorTest.cpp
    )
    target_include_directories(rune_schema_codec_smoke_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(rune_schema_codec_smoke_tests PRIVATE rune_codec GTest::gtest_main)
    gtest_discover_tests(rune_schema_codec_smoke_tests)
```

Run: `cmake --build rune-client-cpp/build --target rune_schema_codec_smoke_tests`
Expected: compile error `no member named 'LastNonNull'`.

- [ ] **Step 6: Implement the client mirror.** In `rune-client-cpp/include/rune/client/view/ViewPlanAuthor.h`, replace lines 38–44 (the comment and the enum) with:

```cpp
// Mirrors rune::view::MergeRule (libs/view/include/rune/view/ViewTypes.h).
// Brokers accept Replace (projection, and the GROUP BY column of an aggregate
// view) plus the aggregates CountAll (Count(*)), Count(col), Sum, Avg (Phase
// 2a) and the retractable Min, Max, Last, LastNonNull (Phase 2b) — GROUP BY the
// view key, single source of one partition. Wire values are append-only so a
// plan authored here round-trips byte-identically through the broker's
// deserializeAuthored.
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll, LastNonNull };  // CountAll=7, LastNonNull=8
```

- [ ] **Step 7: Run the client test and the client regression suite.**

Run: `cmake --build rune-client-cpp/build --target rune_schema_codec_smoke_tests view_probe && ctest --test-dir rune-client-cpp/build -R 'ViewPlanAuthor|Regression' --output-on-failure`
Expected: `ViewPlanAuthorRegression.LastNonNullIsAppendedAtWireValue8` passes, and `view_probe` still compiles.

- [ ] **Step 8: Run the broker regression gate and commit both repos.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure`. Expected: 0 failures.

```bash
git add libs/view/include/rune/view/ViewTypes.h libs/view/src/ViewPlan.cpp libs/view/tests/view/ViewPlanTest.cpp
git commit -m "feat(view): MergeRule::LastNonNull appended at wire value 8" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C ../rune-client-cpp add include/rune/client/view/ViewPlanAuthor.h tests/client/ViewPlanAuthorTest.cpp CMakeLists.txt
git -C ../rune-client-cpp commit -m "feat(view): client MergeRule::LastNonNull mirrors broker (wire value 8)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Compiler — accept Min/Max/Last/LastNonNull

Today the compiler rejects `Min`/`Max`/`Last` with a "Phase 2b" message (`PlanCompiler.cpp:141-145`). This task lifts that rejection and fixes the output types: `Min`/`Max` produce the numeric source type, and `Last`/`LastNonNull` produce any fixed-width source type. A `FixedStr` output already inherits the source's `fixedLen` (the existing `rof.fixedLen` path).

**Files:**
- Modify: `libs/view/src/PlanCompiler.cpp:51-87`, `:141-145`, `:313`
- Modify: `libs/view/tests/view/PlanCompilerTest.cpp:181-187`

**Interfaces:**
- Consumes: `MergeRule::LastNonNull` (Task 1).
- Produces: an aggregate view may use `Min`/`Max` (numeric source, output type == source type) and `Last`/`LastNonNull` (any fixed-width source, output type == source type, `FixedStr` width == source `fixedLen`). Non-key error messages must **not** contain the word `key` (AdminWorker maps such messages to `ViewKeyInvalid`).

- [ ] **Step 1: Write the failing tests.** In `libs/view/tests/view/PlanCompilerTest.cpp`, replace the whole `TEST(PlanCompilerAggregateRegression, MinMaxLastAreRejectedUntilPhase2b)` (lines 181–187) with:

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

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'PlanCompilerAggregateRegression' --output-on-failure`
Expected: `MinMaxLastLastNonNull…` fails with "uses Min/Max/Last, which are not supported until Phase 2b". `LastOverFixedStr…` fails the same way. `MaxDeclaredWithAWidenedType…` fails because its message contains "Phase 2b", not "produces Int32".

- [ ] **Step 3: Implement.** In `libs/view/src/PlanCompiler.cpp`, replace lines 51–87 (`isAggregateRule`, `isPhase2bRule`, `primName`, `aggregateOutputType`) with:

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

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'PlanCompiler' --output-on-failure`
Expected: every `PlanCompiler*` test passes, including the Phase 1/2a tests and `NonGroupReplaceColumnInAggregateViewIsRejected`, which still finds "must be an aggregate".

- [ ] **Step 5: Run the regression gate and commit.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure` → 0 failures.

```bash
git add libs/view/src/PlanCompiler.cpp libs/view/tests/view/PlanCompilerTest.cpp
git commit -m "feat(view): compile Min/Max (numeric) and Last/LastNonNull (fixed-width) with source output types" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `encodeSlotValue` / `decodeSlotValue` — one fixed-width value codec

`Last`/`LastNonNull` store the source value at the source field's width (a `string(N)` stores N bytes) and copy it into the output slot on emit. The encoding is exactly the one `writeSlot`/`readSlot` already use, so this task factors it out once and gives both callers the same function. `writeSlot`/`readSlot` behaviour does not change.

**Files:**
- Modify: `libs/view/include/rune/view/ViewRow.h:33-43`
- Modify: `libs/view/src/ViewRow.cpp:75-178`
- Test: `libs/view/tests/view/ViewRowTest.cpp`

**Interfaces:**
- Produces (in `rune::view`):
  ```cpp
  void encodeSlotValue(uint8_t* dst, rune::codec::PrimType type, uint16_t width,
                       const rune::codec::FieldValue& v) noexcept;
  rune::codec::FieldValue decodeSlotValue(const uint8_t* src, rune::codec::PrimType type,
                                          uint16_t width) noexcept;   // present == true
  ```

- [ ] **Step 1: Write the failing tests.** Append to `libs/view/tests/view/ViewRowTest.cpp`, and add `#include <climits>`, `#include <cstring>` and `#include <string>` next to its existing includes:

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

TEST(ViewRow, WriteSlotIsEncodeSlotValueAtTheSlotOffset) {
    using P = rune::codec::PrimType;
    const auto d = rune::codec::DescriptorBuilder("t", 1).add("a", P::Int32).addFixedStr("s", 3).build();
    const auto L = rune::view::buildRowLayout(d);
    std::vector<uint8_t> row(L.rowLen, 0);
    rune::codec::FieldValue a{}; a.type = P::Int32; a.present = true; a.i = -7;
    rune::codec::FieldValue s{}; s.type = P::FixedStr; s.present = true; s.bytes = "xy";
    rune::view::writeSlot(row.data(), L, 0, a);
    rune::view::writeSlot(row.data(), L, 1, s);
    uint8_t ea[4], es[3];
    rune::view::encodeSlotValue(ea, P::Int32, 4, a);
    rune::view::encodeSlotValue(es, P::FixedStr, 3, s);
    EXPECT_EQ(std::memcmp(row.data() + L.slotOffset[0], ea, 4), 0);
    EXPECT_EQ(std::memcmp(row.data() + L.slotOffset[1], es, 3), 0);
    EXPECT_EQ(rune::view::readSlot(row.data(), L, 0).i, -7);
}
```

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build-fast -j --target rune_view_tests`
Expected: compile error `no member named 'encodeSlotValue' in namespace 'rune::view'`.

- [ ] **Step 3: Implement.** In `libs/view/include/rune/view/ViewRow.h`, insert before the `writeSlot` declaration (line 33):

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

In `libs/view/src/ViewRow.cpp`, replace everything from `void writeSlot(` (line 75) through the end of `readSlot` (line 178, before `} // namespace rune::view`) with:

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

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'ViewRow|ViewFold|AggFold|ViewCheckpoint' --output-on-failure`
Expected: all pass. The `writeSlot`/`readSlot` behaviour is unchanged.

- [ ] **Step 5: Run the regression gate and commit.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure` → 0 failures.

```bash
git add libs/view/include/rune/view/ViewRow.h libs/view/src/ViewRow.cpp libs/view/tests/view/ViewRowTest.cpp
git commit -m "refactor(view): encodeSlotValue/decodeSlotValue — one fixed-width slot codec for rows and Last values" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `AggState` — the linked layout and the exact dirty bound

**Files:**
- Modify (full replacement): `libs/view/include/rune/view/AggState.h`
- Modify (full replacement): `libs/view/src/AggState.cpp`
- Test: `libs/view/tests/view/AggStateTest.cpp`

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

- [ ] **Step 1: Write the failing tests.** Append to `libs/view/tests/view/AggStateTest.cpp`:

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

Run: `cmake --build build-fast -j --target rune_view_tests`
Expected: compile error `no member named 'linked'` / `'kNoCache'`.

- [ ] **Step 3: Implement.** Replace `libs/view/include/rune/view/AggState.h` with:

```cpp
// libs/view/include/rune/view/AggState.h
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

Replace `libs/view/src/AggState.cpp` with:

```cpp
// libs/view/src/AggState.cpp — see AggState.h.
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

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'AggState|AggFold' --output-on-failure`
Expected: all pass. `AggFold*` (2a) is unchanged because a 2a layout has `inputOff[i] == contribValOff + 8*i`.

- [ ] **Step 5: Run the regression gate and commit.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure` → 0 failures.

```bash
git add libs/view/include/rune/view/AggState.h libs/view/src/AggState.cpp libs/view/tests/view/AggStateTest.cpp
git commit -m "feat(view): linked aggregate layout — MRU links, group head, Min/Max caches, per-input widths, exact dirty bound" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The retractable fold

**Files:**
- Modify: `libs/view/include/rune/view/ViewFold.h:65-68` (add `aggRescans`)
- Modify (full replacement): `libs/view/src/AggFold.cpp`
- Create: `libs/view/tests/view/AggRetractableTest.cpp`
- Modify: `libs/view/CMakeLists.txt:20-24` (add the test file)

**Interfaces:**
- Consumes: `AggLayout`/`AggField`/`CacheState`/`kNoContributor`/`kNoCache`/`kCacheEntryBytes` (Task 4); `encodeSlotValue`/`decodeSlotValue` (Task 3).
- Produces: `FoldStats::aggRescans` (counts O(group) Min/Max list walks). `foldAggregateBatch` keeps its signature; for a linked layout it maintains links, heads and caches. For a 2a layout its behaviour, emit order and counters are identical to 2a.

**Maintenance contract** (spec §A Maintenance):
- **Add** of contributor `c` to group `g`: `applyContribution(+1)` updates counts/accumulators. For each Min/Max field with a non-null value: in state Empty, take the value; in Valid, replace the cache if the value is strictly better; in RescanPending, take the value and become Valid if it is at least as extreme as the retracted value still cached. Then `linkAtHead`.
- **Retract**: `applyContribution(-1)`. Where `c`'s old value equals the Valid cache, the state becomes RescanPending; where the non-null count reaches 0, the state becomes Empty. Then `unlink`.
- **Emit** first resolves any RescanPending field with one list walk (`rescan`, bounded by the group's contributor count), then writes Min/Max from the cache, `Last` from the head and `LastNonNull` from the first non-null walking from the head.
- An in-place update (same group) **defers** the retract-side emit until after the add, so a new value that is at least as extreme skips the rescan. A 2a view always emits on retract, exactly as in 2a.

- [ ] **Step 1: Write the failing tests.** Create `libs/view/tests/view/AggRetractableTest.cpp`:

```cpp
// libs/view/tests/view/AggRetractableTest.cpp — Phase 2b retractable
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

Add `tests/view/AggRetractableTest.cpp` to `rune_view_tests` in `libs/view/CMakeLists.txt` (after `tests/view/AggFoldTest.cpp`):

```cmake
        tests/view/AggFoldTest.cpp
        tests/view/AggRetractableTest.cpp)
```

(The closing `)` moves from after `AggFoldTest.cpp` to after the new file.)

- [ ] **Step 2: Run them and confirm they fail.**

Run: `cmake --build build-fast -j --target rune_view_tests`
Expected: compile error `no member named 'aggRescans' in 'rune::view::FoldStats'`. Once that member exists, the tests fail at runtime because Min/Max/Last emit NULL.

- [ ] **Step 3a: Add the counter.** In `libs/view/include/rune/view/ViewFold.h`, after the `aggInconsistent` member (line 68), add:

```cpp
    // Phase 2b: Min/Max caches rebuilt by one walk of the group's contributor
    // list (the cached extreme was retracted) -- the known O(group size) path.
    uint64_t aggRescans       = 0;
```

- [ ] **Step 3b: Replace the fold.** Replace `libs/view/src/AggFold.cpp` with:

```cpp
// libs/view/src/AggFold.cpp — see AggFold.h / AggState.h.
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
    // linked view, unlinks it. An emptied group loses its rows iff
    // `dropIfEmpty`; otherwise the caller decides. A non-empty group is
    // re-emitted iff `emitNow` (an in-place linked update defers it until after
    // the add, so an at-least-as-extreme new value skips the rescan).
    uint64_t retract(uint64_t c, const uint8_t* cr, bool dropIfEmpty, bool emitNow) noexcept {
        const uint64_t g = loadU64(cr);
        uint8_t* gr = applyContribution(cr, -1);
        if (agg.linked) unlink(c, cr, g, gr);
        if (gr == nullptr) return g;   // inconsistent: counted, nothing else changed
        if (loadU64(gr) == 0) {
            if (dropIfEmpty) dropGroup(g);
        } else if (emitNow) {
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
                    (void)cx.retract(c, old, /*dropIfEmpty*/ true, /*emitNow*/ true);
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
                    (void)cx.retract(c, old, /*dropIfEmpty*/ true, /*emitNow*/ true);
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
                oldGroup = cx.retract(c, old, /*dropIfEmpty*/ false,
                                      /*emitNow*/ !(agg.linked && sameGroup));
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
- The 2a path is unchanged. With `agg.linked == false`, `emitNow` is always `true`, `unlink`/`linkAtHead` never run, Min/Max/Last never occur, and `inputOff[i] == contribValOff + 8*i`. The order of `put` calls, and therefore the order of dirty entries and the checkpoint bytes, is identical to 2a.
- `put()` is the only call that marks a row dirty. Neighbour fix-ups therefore go through `linkedContributor` (`get` first, then `put`), and a rescan uses `get` only.

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'AggRetractable|AggFold|AggState' --output-on-failure`
Expected: every `AggRetractable*` test passes, and every 2a `AggFold*` test passes unchanged.

- [ ] **Step 5: Run the regression gate and commit.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure` → 0 failures.

```bash
git add libs/view/include/rune/view/ViewFold.h libs/view/src/AggFold.cpp libs/view/tests/view/AggRetractableTest.cpp libs/view/CMakeLists.txt
git commit -m "feat(view): retractable Min/Max/Last/LastNonNull — MRU contributor list, cached extremes, rescan on retracted extreme" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Pin the dirty bound and model-check the fold

These tests target the linked fold's two quiet failure modes: a list or cache that drifts from the truth only after a long, specific history, and a record that dirties more rows than the per-pass cap assumes. Both fail silently in production; here they fail deterministically.

**Files:**
- Modify: `libs/view/tests/view/AggRetractableTest.cpp` (append)

**Interfaces:**
- Consumes: `aggDirtyBoundPerRecord`, `AggDirtyBound` (Task 4); `Harness::drainCounts` and `Ord`/`encode` (Task 5's test file).

- [ ] **Step 1: Write the tests.** Append to `libs/view/tests/view/AggRetractableTest.cpp`:

```cpp
// REGRESSION (Review Focus #3): the worst case the per-pass delta cap assumes
// is REACHED by a group move, and no record exceeds it.
TEST(AggRetractableRegression, WorstCaseMoveDirtiesExactlyTheDerivedBoundRegression) {
    Harness h;
    h.putOrd({1, 100, 10});
    h.putOrd({2, 100, 20});
    h.putOrd({3, 100, 30});      // u100 list: 3 -> 2 -> 1
    h.putOrd({4, 200, 40});
    h.putOrd({5, 200, 50});      // u200 list: 5 -> 4
    (void)h.drainCounts();
    h.putOrd({2, 200, 25});      // move 2 (prev 3, next 1) to u200 (old head 5)
    const auto d = h.drainCounts();
    const auto b = rune::view::aggDirtyBoundPerRecord(h.agg);
    ASSERT_EQ(b.contributorRows, 4u);
    ASSERT_EQ(b.groupRows, 2u);
    ASSERT_EQ(b.viewRows, 2u);
    EXPECT_EQ(d.contributorRows, b.contributorRows) << "2, its old neighbours 3 and 1, u200's old head 5";
    EXPECT_EQ(d.groupRows, b.groupRows);
    EXPECT_EQ(d.viewRows, b.viewRows);
    EXPECT_EQ(d.pendingCaches, 0u);
    EXPECT_EQ(h.col(100, kLastAmt).i, 30);
    EXPECT_EQ(h.col(200, kLastAmt).i, 25);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 10);
    EXPECT_EQ(h.col(200, kMinAmt).i, 25);
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Review Focus #1/#3): a deterministic 3000-op history of inserts,
// in-place updates, group moves and deletes over 24 contributors / 4 groups,
// with heavy value duplication. After EVERY record: each group's Count(*), Sum,
// Max, Min, Last, Last(bonus), LastNonNull(bonus) equals a brute-force
// recomputation; each table's dirty entries are within the derived bound; no
// cache is left RescanPending; nothing is ever inconsistent.
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
    for (uint64_t step = 1; step <= 3000; ++step) {
        const int64_t id = 1 + static_cast<int64_t>(rnd(24));
        if (rnd(100) < 15) {
            h.put(id, tombstone());
            live.erase(id);
        } else {
            Ord o{id, 1 + static_cast<int64_t>(rnd(4)), static_cast<int64_t>(rnd(41)) - 20};
            if (rnd(2) == 0) o.bonus = static_cast<int32_t>(rnd(9));
            h.putOrd(o);
            live[id] = M{o.user, o.amount, o.bonus, step};
        }
        const auto d = h.drainCounts();
        ASSERT_LE(d.contributorRows, bound.contributorRows) << "step " << step;
        ASSERT_LE(d.groupRows, bound.groupRows) << "step " << step;
        ASSERT_LE(d.viewRows, bound.viewRows) << "step " << step;
        ASSERT_EQ(d.pendingCaches, 0u) << "step " << step;
        for (int64_t g = 1; g <= 4; ++g) {
            const M* lastM = nullptr;
            const M* lastNn = nullptr;
            int64_t n = 0, total = 0, mx = INT64_MIN, mn = INT64_MAX;
            for (const auto& [k, m] : live) {
                if (m.user != g) continue;
                ++n; total += m.amount;
                mx = std::max(mx, m.amount); mn = std::min(mn, m.amount);
                if (lastM == nullptr || m.seq > lastM->seq) lastM = &m;
                if (m.bonus && (lastNn == nullptr || m.seq > lastNn->seq)) lastNn = &m;
            }
            if (n == 0) { ASSERT_FALSE(h.hasGroup(g)) << "step " << step << " group " << g; continue; }
            ASSERT_EQ(h.col(g, kN).i, n) << "step " << step << " group " << g;
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
```

- [ ] **Step 2: Run them.**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'AggRetractableRegression' --output-on-failure`
Expected: both pass on Task 5's fold. If either fails, the failure is a real bug in Task 5. Debug it with superpowers:systematic-debugging, fix it in `AggFold.cpp`, and keep the test as written; the step number in the message localizes the bug.

- [ ] **Step 3: Run the regression gate and commit.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure` → 0 failures.

```bash
git add libs/view/tests/view/AggRetractableTest.cpp
git commit -m "test(view): pin the linked fold's exact per-record dirty bound; model-check 3000 ops against brute force" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Worker — per-pass cap from the derived bound; restart regression

**Files:**
- Modify: `libs/node/src/WorkerViewSet.cpp:130-148` (`maxRecordsPerPassFor`)
- Test: `libs/node/tests/node/WorkerViewSetTest.cpp` (append)

**Interfaces:**
- Consumes: `aggDirtyBoundPerRecord` (Task 4); the linked fold (Task 5). `AggInstance`, `ViewInstance` and the recovery and checkpoint code are unchanged: the new links, heads and caches are bytes inside the contributor and group rows the v2 record already carries.

- [ ] **Step 1: Write the failing tests.** Append to the end of `libs/node/tests/node/WorkerViewSetTest.cpp` (after the last test; `orderSrc`, `orderBatch`, `drainDeltas` and `driveToEnd` are defined earlier in the file):

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
// rescan the RECOVERED contributor list (links + head + cache all came back
// from the v2 checkpoint) and surface the exact next extreme.
TEST(WorkerViewSetRegression, RestartThenRetractingTheMaxIsExactRegression) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userMaxPlan(0), srcs, 0x4000000A).value();

    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, "/tmp/rune-views-max-rec");
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = orderBatch(0, {{1, 100, 10}, {2, 100, 50}, {3, 100, 30}});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 3}; };
    vs1.driveFolds(6, 0, rr1, 3, 0);
    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);
    uint32_t magic; std::memcpy(&magic, deltas[0].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2) << "no new format: still the v2 record";

    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, "/tmp/rune-views-max-rec");
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
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 30) << "next extreme found by walking the recovered list";
    EXPECT_EQ(rune::view::readSlot(row, *L, 3).i, 5) << "the re-write is now the head";
    const rune::node::ViewFoldCounters* fc = vs2.foldCounters(0x4000000A, 0);
    ASSERT_NE(fc, nullptr);
    EXPECT_EQ(fc->aggInconsistent, 0u);
}

// REGRESSION (Review Focus #3): a stream of GROUP MOVES that dirty 8 DISTINCT
// rows each (no coalescing across records: every move touches its own groups,
// neighbours and target head) -- the linked worst case. Under the 2a bound
// (1 contributor row per record) a capped pass claims ~189 KB > the 128 KiB
// delta-ring max message and the worker THROWS; under the derived bound
// (4+2+2) every delta fits and the result is exact.
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
    rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-max-large");
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

- [ ] **Step 2: Run them and confirm the large-range test fails.**

Run: `cmake --build build-fast -j --target rune_node_tests && ctest --test-dir build-fast -R 'WorkerViewSetRegression\.(RestartThenRetracting|PersistedRetractableMoves)' --output-on-failure`
Expected: `PersistedRetractableMovesDeltasFitTheRingRegression` **fails**. `ASSERT_NO_THROW` reports the ring's oversized-claim exception: the 2a formula gives 366 B per record, so a pass covers about 358 move records, which really dirty 528 B each, about 189 KB, over the 128 KiB max message. `RestartThenRetractingTheMaxIsExactRegression` already passes on Task 5's fold. That is expected: it guards the recovery path, which needs no code change.

- [ ] **Step 3: Implement.** In `libs/node/src/WorkerViewSet.cpp`, replace the comment and body of `maxRecordsPerPassFor` (lines 130–148) with:

```cpp
// Most source records one fold pass may cover so the pass's checkpoint delta
// (worst case) fits ONE ring message of `maxMsg` payload bytes; 0 if not even
// one record fits.  Worst case per source record: Replace dirties 1 output row;
// an aggregate dirties rune::view::aggDirtyBoundPerRecord rows per table (2a:
// contributor + old/new group + old/new output row; linked Min/Max/Last views
// additionally the contributor's two old neighbours and the new group's old head).
int64_t maxRecordsPerPassFor(const ViewInstance& inst, std::size_t maxMsg) {
    using namespace rune::view;
    std::size_t fixed = ViewSnapshotChannel::kDeltaHeaderBytes + kCheckpointCrcBytes;
    std::size_t perRecord = 0;
    if (inst.agg) {
        fixed += kCheckpointV2HeaderBytes + 3 * kCheckpointV2TableHeaderBytes;
        const AggDirtyBound b = aggDirtyBoundPerRecord(inst.agg->layout);
        perRecord = b.contributorRows * (kCheckpointEntryHeaderBytes + inst.agg->layout.contributorRow.rowLen) +
                    b.groupRows * (kCheckpointEntryHeaderBytes + inst.agg->layout.groupRow.rowLen) +
                    b.viewRows * (kCheckpointEntryHeaderBytes + inst.layout.rowLen);
    } else {
        fixed += kCheckpointV1HeaderBytes;
        perRecord = kCheckpointEntryHeaderBytes + inst.layout.rowLen;
    }
    if (maxMsg <= fixed) return 0;
    return static_cast<int64_t>((maxMsg - fixed) / perRecord);
}
```

For 2a views this computes exactly the old value, because `{1, 2, 2}` gives the old expression.

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build-fast -j --target rune_node_tests && ctest --test-dir build-fast -R 'WorkerViewSet|ViewRecovery|ViewReadiness|GetViewRow|CreateView' --output-on-failure`
Expected: all pass, including 2a's `PersistedAggregateLargeRangeDeltasFitTheRing` and `AggregateRecoveryOfLargeCheckpointDoesNotResendRows`.

- [ ] **Step 5: Run the regression gate and commit.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure` → 0 failures.

```bash
git add libs/node/src/WorkerViewSet.cpp libs/node/tests/node/WorkerViewSetTest.cpp
git commit -m "fix(node): per-pass delta cap uses the exact linked-aggregate dirty bound (4+2+2); restart-then-retract-max regression" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Benchmarks — typical-case gate and extreme-retraction report

**Files:**
- Modify: `benchmark/ViewFoldBench.cpp` (fixtures after `userTotalsView()`, line ~218; two new benchmarks at the end of the file)

**Interfaces:**
- Consumes: `foldAggregateBatch`, `buildAggLayout`, `FoldStats::aggRescans`; the shared harness (`Op`, `buildStream`, `BatchStream`, `runPhases`, `reportCommon`, `percentileUs`, `generateChurnOps`, `kBatchSizes`, `kTotalOps`, `keyBytes`, `asSv`, `encodeOrder`, `orderSourceV1`).

- [ ] **Step 1: Add the fixtures.** Add `#include <rune/view/ViewRow.h>` next to the other `rune/view` includes. Inside the anonymous namespace, directly after `userTotalsView()`, add:

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

- [ ] **Step 2: Add the gated benchmark.** Append to the end of `benchmark/ViewFoldBench.cpp`:

```cpp
// Retractable aggregate gate (Phase 2b, §14.0): user_stats = Count(*), Sum,
// Max, Min, Last, LastNonNull over amount, grouped by userId — the SAME
// workload, priming, drain cadence and three phases as BM_ViewFoldAggregate
// (1,000,000 contributors / 10,000 groups, ~100 per group). The churn's
// pseudo-random amounts retract a group's cached Min or Max on roughly 2% of
// retracting ops, so this IS the typical case including its occasional
// O(group) rescans (ret_rescans counts them). BM_ViewFoldExtremeRetract
// isolates that path. GATE: ret_record_p99_us < 5, ret_batch_p99_us < 10,
// steady_alloc == 0.
static void BM_ViewFoldRetractable(benchmark::State& st) {
    constexpr uint64_t kContributors      = 1'000'000;
    constexpr uint64_t kGroups            = 10'000;
    constexpr uint64_t kDrainEveryBatches = 1000;
    constexpr int kPrimeBatch[]           = {64};

    for (auto _ : st) {
        auto src = orderSourceV1();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = compileView(userStatsView(), srcs, 0x51000003).value();
        auto agg = rune::view::buildAggLayout(comp.plan, src).value();

        ViewTable view(comp.rowLayout);
        ViewTable contributors(agg.contributorRow), groups(agg.groupRow);
        VersionDescriptorCache vdc(src);

        auto append = [](rune::protocol::RecordBatchBuilder& bb, const Op& op) {
            const int64_t c = static_cast<int64_t>(op.key % kContributors);
            const auto kb = keyBytes(c);
            if (op.tombstone) {
                bb.append(asSv(kb), std::string_view{}, 0);
            } else {
                const auto val = encodeOrder(c, c % kGroups, static_cast<int64_t>(op.hp), 1);
                bb.append(asSv(kb), asSv(val), 0);
            }
        };
        auto drainAll = [&] {
            auto nop = [](uint32_t, const uint8_t*, bool, uint64_t) {};
            view.drainDirty(nop);
            contributors.drainDirty(nop);
            groups.drainDirty(nop);
        };
        uint64_t rescans = 0;
        auto foldStream = [&](const BatchStream& s, size_t i) {
            auto lv = s.view(i);
            int64_t cur = 0;
            const auto fs = foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors,
                                               groups, lv, s.bound(i), cur);
            rescans += fs.aggRescans;
            return fs;
        };

        // --- Prime: creates-only over the whole contributor space (NOT timed).
        {
            std::vector<Op> prime;
            prime.reserve(kContributors);
            for (uint64_t c = 0; c < kContributors; ++c)
                prime.push_back(Op{c, static_cast<int32_t>(c ^ 0x5a5a), false});
            const auto ps = buildStream(prime, kPrimeBatch, 1, append);
            for (size_t i = 0; i < ps.size(); ++i) foldStream(ps, i);
            drainAll();
        }
        rescans = 0;   // count the churn's rescans only

        const auto ops = generateChurnOps(kTotalOps);
        const auto stream = buildStream(ops, kBatchSizes, std::size(kBatchSizes), append);
        auto r = runPhases(
            stream, [&](size_t i) { return foldStream(stream, i); }, drainAll, kDrainEveryBatches);

        benchmark::DoNotOptimize(view.size());
        benchmark::DoNotOptimize(groups.size());
        benchmark::DoNotOptimize(contributors.size());
        reportCommon(st, "ret_", r, view.size());
        st.counters["ret_rescans"]            = static_cast<double>(rescans);
        st.counters["ret_contributors_final"] = static_cast<double>(contributors.size());

        const double recP99 = st.counters["ret_record_p99_us"];
        const double batP99 = st.counters["ret_batch_p99_us"];
        const bool pass = recP99 < 5.0 && batP99 < 10.0 && r.steadyAlloc == 0;
        st.counters["ret_gate_pass"] = pass ? 1.0 : 0.0;
        std::fprintf(stderr,
                     "[ViewFoldRetractable gate] ret_record_p99_us=%.3f (<5) ret_batch_p99_us=%.3f (<10) "
                     "steady_alloc=%lld (==0) rescans=%llu => %s\n",
                     recP99, batP99, static_cast<long long>(r.steadyAlloc),
                     static_cast<unsigned long long>(rescans), pass ? "PASS" : "FAIL");
    }
}
BENCHMARK(BM_ViewFoldRetractable)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();

// Extreme retraction (Phase 2b) — REPORTED, NOT GATED (spec §A Gates: the known
// O(group size) path). One group of N contributors with amounts 1..N; each
// timed op lowers the CURRENT max holder (N, N-1, ..., 2) to 0, so every op
// retracts the cached Max with a lower value and walks all N contributors.
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
                                      groups, lv, s.bound(i), cur);
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

- [ ] **Step 3: Run the benchmarks.**

Run: `cmake --build build-bench-rel -j --target rune_broker_benchmark && ./build-bench-rel/rune_broker_benchmark --benchmark_filter='ViewFold'` (if the binary is not at `build-bench-rel/`, find it with `find build-bench-rel -name rune_broker_benchmark -type f`).
Expected:
- `[ViewFoldRetractable gate] … => PASS` (`ret_record_p99_us < 5`, `ret_batch_p99_us < 10`, `steady_alloc == 0`).
- `BM_ViewFoldReplace` and `BM_ViewFoldAggregate` still PASS, which shows the 2a path did not regress.
- `[ViewFoldExtremeRetract N=1000]` and `[ViewFoldExtremeRetract N=10000]` report `exact=yes` with their p50/p99/max. Record these numbers in the commit message; they are not gated.

If the gate fails, profile before changing anything. `ret_rescans / measured records` shows how often the O(group) path ran. A non-zero `steady_alloc` means a hot-path allocation, which is a CLAUDE.md #4 violation that must be fixed rather than gated away.

- [ ] **Step 4: Run the regression gate and commit.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure` → 0 failures.

```bash
git add benchmark/ViewFoldBench.cpp
git commit -m "bench(view): §14.0 gate for the retractable fold; report extreme retraction at 1k/10k (p50/p99 in body)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: e2e — `user_totals` gains `max`, `min`, `last` across the restart

**Files:**
- Modify: `rune-client-cpp/examples/view_probe.cpp` (header comment `:52-57`; helpers `:251-332`; setup leg `:463-464`; verify leg `:556-564`)
- Modify: `docker/view-engine-e2e.sh` (header comment `:23-26`, `:44-48`; banner `:402-403`)

**Interfaces:**
- Consumes: everything above, on the real Aeron broker. The handoff env vars are unchanged (same view).

Scenario (`order` = `{orderId key, userId, amount}`, 1 partition; `user_totals` = `userId`, `n` Count(\*), `total` Sum, `avg` Avg, **`max` Max, `min` Min, `last` Last** over amount):
- **setup:** `o1(u100,10)`, `o2(u100,30)`, `o3(u200,5)`, then `o1→(u100,50)`, then `o3→(u100,5)` (a group move). The MRU order is then o3, o1, o2. Expected: `u100 = {n 3, total 85, avg 85/3, max 50, min 5, last 5}`, and `u200` AbsentKey.
- **restart.**
- **verify:** the recovered row is `{3, 85, 85/3, 50, 5, 5}`. Then `o3→(u100,25)` retracts the cached **min** (5) with a higher value, so it rescans the recovered list: `{3, 105, 35, 50, 25, 25}`. Then `o1→(u100,20)` retracts the cached **max** (50) with a lower value, so it rescans: `{3, 75, 25, 30, 20, 20}`.

- [ ] **Step 1: Extend the probe helpers.** In `rune-client-cpp/examples/view_probe.cpp`, make these changes:

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

Update the header comment item `(d)` (lines 52–57) to read:

```cpp
//     (d) Phase 2a/2b: the PERSISTED aggregate view user_totals (GROUP BY userId:
//         Count(*), Sum, Avg, Max, Min, Last over "order") recovers its rows,
//         contributor map AND MRU lists/caches from the v2 checkpoint:
//         u100 = {3, 85, 85/3, max 50, min 5, last 5}, u200 absent (emptied by a
//         pre-restart group move); o3 5 -> 25 retracts the pre-image AND the
//         cached min -> {3, 105, 35, 50, 25, 25}; o1 50 -> 20 retracts the cached
//         max -> {3, 75, 25, 30, 20, 20} (both rescans walk the recovered list).
```

Build: `cmake --build rune-client-cpp/build --target view_probe`. Expected: it compiles.

- [ ] **Step 4: e2e script text.** In `docker/view-engine-e2e.sh`, replace lines 23–26 with:

```bash
#     5. Phase 2a/2b: defineTopic "order" {orderId key, userId, amount}; createView
#        "user_totals" (persistence=0; GROUP BY userId: Count(*), Sum, Avg, Max,
#        Min, Last); produce o1(u100,10) o2(u100,30) o3(u200,5), update o1->50,
#        move o3->(u100,5) -> u100 = {3, 85, 85/3, 50, 5, 5}, u200 AbsentKey.
```

Replace lines 44–48 with:

```bash
#     (d) user_totals recovered from its v2 checkpoint: u100 = {3, 85, 85/3, 50,
#         5, 5}, u200 AbsentKey; then o3 5->25 (a PRE-restart contributor; also
#         retracts the cached min) -> {3, 105, 35, 50, 25, 25}; then o1 50->20
#         (retracts the cached max) -> {3, 75, 25, 30, 20, 20} — proves the
#         contributor map, MRU lists and caches all survived the restart.
```

Replace the two banner lines 402–403 with:

```bash
log "  -> user_totals (Count(*)/Sum/Avg/Max/Min/Last) recovered from its v2 checkpoint (VIEW-RECOVERED format=v2, O>0)"
log "     + post-restart update retracts the pre-image, min and max exactly OK"
```

- [ ] **Step 5: Run the e2e on the VM.** Honour `/root/vm.lock`: claim it with an owner marker and release it afterwards.

```bash
# From /Users/vradu/Projects/se-worktrees: stage rune-broker, rune-protocol, rune-codec,
# rune-client-cpp, rune-ql, rune-fr (git archive HEAD of each) into /root/view-engine-e2e/<repo>
# on root@178.104.16.102 exactly as the Phase 2a e2e run did, then:
ssh root@178.104.16.102 'cd /root/view-engine-e2e/rune-broker && bash docker/view-engine-e2e.sh'
```

Expected: `VIEW-ENGINE-E2E: OK`, `SCRIPT_EXIT=0`, and six `user_totals` PROBE lines: initial, the u200 group move, recovered, u200 post-restart, pre-image + min, and max retraction.

- [ ] **Step 6: Run the regression gates (both repos) and commit.**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure` and `ctest --test-dir rune-client-cpp/build -R Regression --output-on-failure`. Expected: 0 failures in each.

```bash
git -C ../rune-client-cpp add examples/view_probe.cpp
git -C ../rune-client-cpp commit -m "test(view-probe): user_totals gains max/min/last; post-restart retraction of min and max" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git add docker/view-engine-e2e.sh
git commit -m "test(view): e2e covers retractable Min/Max/Last across a broker restart" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Finish

- [ ] Run the full broker `*Regression*` suite on the final tree: `ctest --test-dir build-fast -R Regression` → 0 failures.
- [ ] Run the view suites: `ctest --test-dir build-fast -R 'View|PlanCompiler|Agg|GetViewRow|CreateView|WorkerViewSet'` → 0 failures.
- [ ] Run the full client `*Regression*` suite: `ctest --test-dir rune-client-cpp/build -R Regression` → 0 failures.
- [ ] Benchmarks: `BM_ViewFoldRetractable` must PASS; `BM_ViewFoldReplace` and `BM_ViewFoldAggregate` must still PASS; record the `BM_ViewFoldExtremeRetract` 1k/10k numbers.
- [ ] The e2e on the VM must be green (Task 9).
- [ ] Hand the branches to the controller. Do not merge or push here.

---

## Self-review (done while writing)

- **Spec coverage (§A):**
  - Semantics: Tasks 2 and 5.
  - `LastNonNull = 8`: Task 1.
  - Storage (links, head, caches, per-input width, 2a untouched): Tasks 3 and 4.
  - Maintenance (add/retract/rescan, skipped rescan, Last O(1), LastNonNull walk, emit, inconsistency): Task 5.
  - Persistence (no format change, recomputed exact bound + test): Tasks 4, 6 and 7.
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
  - `aggDirtyBoundPerRecord` returns `AggDirtyBound{contributorRows, groupRows, viewRows}` in Tasks 4, 6 and 7.
  - `CacheState`, `kCacheEntryBytes`, `kNoCache` and `kNoContributor` are used identically in Tasks 4, 5 and 6.
  - `encodeSlotValue`/`decodeSlotValue` have the same signatures in Tasks 3 and 5.
  - `FoldStats::aggRescans` is used in Tasks 5 and 8.
- **2a invariance:** the non-linked fold path calls the same `put`/`erase` in the same order with the same bytes. `TwoAOnlyLayoutIsUnchangedRegression`, the untouched `AggFold*` suite and 2a's WorkerViewSet ring and recovery tests all guard it.

## Spec questions

1. **Value-slot width for non-Last inputs.** §A Storage says input slots "get the source field's width … instead of a fixed 8". Applied to every input, that would change a 2a view's contributor row (for example, `Sum(Int32)` would shrink to 4 bytes) and break the "2a-only views keep their 2a layout byte-for-byte" rule. **The plan applies the source width only to `Last`/`LastNonNull` inputs.** Count, Sum, Avg, **Min and Max** keep the widened 8-byte slot, so Min/Max compare in one domain. Please confirm.
2. **Contributor key `0xFFFFFFFFFFFFFFFF`** is a valid 8-byte wire key but equals the spec's `kNoContributor` sentinel. **The plan skips it in linked views and counts it as `skippedMalformed`.** 2a views still accept it. The alternative is a separate presence flag per link. Please confirm that fail-closed behaviour.
3. **`Last`/`LastNonNull` over Float64 NaN/±Inf.** The spec states the NaN→NULL rule only for Min/Max. **The plan returns NaN/Inf verbatim as non-null values for Last/LastNonNull**, matching 2a's `Count(col)`, and does not count them in `aggNonFinite`.
4. **`[valid u8]` is used as a three-state byte**: Empty 0, Valid 1, RescanPending 2. Values 0 and 1 keep the spec's meaning. State 2 is transient within a record and is never checkpointed (asserted by the model test).
5. **Dirty-bound enumeration.** The spec's list ("contributor, both neighbours in the old list, the old head's group, the new head, both groups, both view rows") is ambiguous about "the old head's group". The plan derives **4 contributor rows + 2 group rows + 2 view rows = 8**, against 5 for 2a, and pins it exactly. The rescan reads only and dirties nothing extra.
6. **Unused 2a accumulator bytes.** In linked views, each Min/Max/Last/LastNonNull column still occupies its 16-byte 2a accumulator slot in the group row, and that slot is unused. This keeps one group-row shape for both phases. If row compactness matters more than uniformity, the slots could be packed per rule.
7. **`FoldStats::aggRescans`** is a new observability counter not named in the spec. It is used by tests (to show a rescan was skipped) and benchmarks. It is not surfaced in `ViewFoldCounters`.
8. **Naming.** The controller gave the slug `a-retractable-min-max-last`. The plan file and branch use `…-phase2b-a-retractable-min-max-last` and `feat/phase2b-a-retractable-min-max-last`, which avoids a doubled `a-a-`.
