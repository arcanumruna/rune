# Rune View Engine — Phase 2a: Invertible Aggregates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Materialized views can `GROUP BY` a value field and maintain `Count(*)`, `Count(col)`, `Sum`, and `Avg` incrementally with exact retraction on update, group-move, and delete, SQL null semantics, and checkpoint/restart durability.

**Architecture:** An aggregate view has **table semantics** (same as Phase 1 `Replace` views): a contributor is identified by the source record's 8-byte **wire key**, its group by the view key read from the record **value**. Two auxiliary fixed-width `ViewTable`s per (view, partition) hold the per-contributor map (pre-images for retraction) and per-group accumulators (int128/uint128/double + non-null counts). A new multi-table checkpoint record persists all three tables atomically at one offset.

**Tech Stack:** C++23 (clang/gcc, `__int128`), GoogleTest, Google Benchmark, CMake/Ninja (`build-fast`), docker e2e on the k3s VM.

**Spec:** `docs/superpowers/specs/2026-10-08-rune-view-engine-single-source-design.md` — §6 (merge/aggregate table, invertibility, pre-images), §6.4 (null handling), §7 (null bitmap), §9.2 (aux state is checkpointed), §9.7 (transient views re-fold), §14.0 (latency gates), resolved decision #19.

**Builds on:** Phase 1 (`docs/superpowers/plans/2026-10-09-rune-view-engine-phase1-core.md`), landed on `feature/materialized-views` (broker `bf7a6ad`, protocol `737e5fb`, client `25bbe2a`). Work on branch `feat/view-engine-phase2a` cut from `feature/materialized-views` in **rune-broker, rune-protocol, rune-client-cpp**.

## Decisions taken before this plan (user-approved 2026-10-11)

1. **Table semantics.** Contributor = 8-byte wire record key. Group = view key read from the record value (an 8-byte integer column). A second record for the same contributor **replaces** it: retract the old contribution (from the old group), add the new one (to the possibly-different new group). A tombstone retracts the contributor.
2. **Co-partitioning.** An aggregate view is rejected at `CreateView` (new wire error `ViewNotCoPartitioned = 36`) if the source has more than one partition **unless** the group-by field is the source's declared key field.
3. **Scope split.** Phase 2a = `Count`, `CountAll` (= `Count(*)`), `Sum`, `Avg`. Phase 2b = retractable `Min`/`Max`/`Last` (multiset / recency aux store). In 2a the compiler **rejects** `Min`/`Max`/`Last` fail-closed.

## Global Constraints

- HARD RULES (CLAUDE.md): shared-nothing; ring buffers are the only cross-thread passing; zero heap allocation on the fold hot path in steady state; regression test on every fix, suite name containing `Regression`; run the full `*Regression*` suite before anything else on every change; fix-on-sight.
- §14.0 latency gate (binding): fold→merge pipeline **record_p99 < 5 µs**, **batch_p99 < 10 µs** (same gates as Phase 1's `BM_ViewFoldReplace`).
- Phase 1 `Replace`-only views are byte-for-byte unchanged: same fold (`foldBatch`), same v1 checkpoint record, same files.
- View **key** (group-by) fields are 8-byte integers only (`Int64`/`UInt64`) — `isEightByteKeyType` (Phase 1 Bug-B rule) applies unchanged.
- Contributor (wire) keys must be exactly 8 bytes; anything else is counted `skippedMalformed` and skipped — never zero-padded.
- Aggregate output types are fixed by rule, never cast: `Count`/`CountAll` → `Int64`; `Sum` over `Int32`/`Int64` → `Int64`, over `UInt16`/`UInt32`/`UInt64` → `UInt64`, over `Float64` → `Float64`; `Avg` → `Float64`. The authored output type must equal this or `CreateView` fails.
- SQL null semantics (§6.4): `Sum`/`Avg`/`Count(col)` ignore nulls; all-null `Sum`/`Avg` → NULL; `Count` → 0; `Count(*)` counts contributors including nulls; an empty group (0 contributors) has **no row**.
- `MergeRule` wire values are append-only: `CountAll = 7` is appended; existing values 0–6 keep their meaning.
- Commit attribution: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Nothing is pushed.

## Review Focus

Most likely to bite a user, each pinned by a named test in its owning task:

1. **An update that moves a contributor to another group** must retract it from the old group (and delete the old group's row when it empties) and add it to the new group → `AggFold.UpdateMovesContributorBetweenGroupsAndDropsEmptyGroupRegression` (Task 4).
2. **Retracting the last non-null contribution** must make `Sum`/`Avg` NULL (not 0) while `Count(col)` reads 0 and the row stays if other contributors remain → `AggFold.RetractingLastNonNullMakesSumNullNotZeroRegression` (Task 4).
3. **Aggregate checkpoint deltas must actually reach disk** — the snapshot IO agent drops any record whose magic it doesn't know, which would silently lose all aggregate state on restart → `ViewSnapshotIoAgentRegression.MultiTableDeltaIsPersistedNotDropped` (Task 6).
4. **After restart, an update to an existing contributor must retract its pre-restart contribution** (proves the contributor map survived, not just the output rows) → `WorkerViewSetRegression.AggregateRecoveryRestoresContributorMap` (Task 7) and the e2e verify leg (Task 10).
5. **Int64 `Sum` overflow** must surface NULL + a counter, never a wrapped value, and recover exactly when the overflowing contribution is retracted → `AggFold.Int64SumOverflowIsNullThenRecoversExactlyRegression` (Task 4).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `libs/view/include/rune/view/ViewTypes.h` | `MergeRule::CountAll`, `kNoSourceField` | 1 |
| `libs/view/src/ViewPlan.cpp` | accept rule value 7 on deserialize | 1 |
| `rune-client-cpp/include/rune/client/view/ViewPlanAuthor.h` | client `MergeRule::CountAll` | 1 |
| `libs/view/src/PlanCompiler.cpp` | aggregate validation + output types; reject Min/Max/Last | 2 |
| `libs/view/include/rune/view/AggState.h`, `src/AggState.cpp` (new) | `isAggregateView`, `AggLayout`, accumulator byte helpers, aux-table ids | 3 |
| `libs/view/include/rune/view/AggFold.h`, `src/AggFold.cpp` (new) | `foldAggregateBatch` — the aggregate fold | 4 |
| `libs/view/include/rune/view/ViewFold.h` | `FoldStats::skippedNoGroup`, `aggOverflow` | 4 |
| `libs/view/include/rune/view/ViewCheckpoint.h`, `src/ViewCheckpoint.cpp` | multi-table record v2: `encodeMultiDelta`/`applyMultiDelta`/`replayMulti` | 5 |
| `libs/node/src/ViewSnapshotIoAgent.cpp` | accept v2 records | 6 |
| `libs/node/src/WorkerViewSet.cpp` | split v2 records on recovery | 6 |
| `libs/node/include/rune/node/WorkerViewSet.h`, `src/WorkerViewSet.cpp` | `AggInstance`, fold dispatch, v2 checkpoint build, aggregate recovery | 7 |
| `libs/node/src/AdminWorker.cpp`, `rune-protocol/include/rune/protocol/client/Protocol.h` | co-partition gate, `ViewNotCoPartitioned = 36` | 8 |
| `benchmark/ViewFoldBench.cpp` | `BM_ViewFoldAggregate` §14.0 gate | 9 |
| `rune-client-cpp/examples/view_probe.cpp`, `docker/view-engine-e2e.sh` | aggregate e2e incl. restart | 10 |

New view tests: `libs/view/tests/view/AggStateTest.cpp`, `libs/view/tests/view/AggFoldTest.cpp` (added to `rune_view_tests`).

---

### Task 1: `MergeRule::CountAll` on the wire (broker + client)

**Files:**
- Modify: `libs/view/include/rune/view/ViewTypes.h`
- Modify: `libs/view/src/ViewPlan.cpp:124-126`
- Modify: `rune-client-cpp/include/rune/client/view/ViewPlanAuthor.h:42`
- Test: `libs/view/tests/view/ViewPlanTest.cpp`

**Interfaces:**
- Produces: `enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll };` (CountAll = 7); `inline constexpr uint16_t kNoSourceField = 0xFFFF;` in `rune::view`.

- [ ] **Step 1: Write the failing tests** — append to `libs/view/tests/view/ViewPlanTest.cpp`:

```cpp
// Phase 2a: Count(*) is a new MergeRule value appended at 7 so existing plan
// blobs (rules 0-6) keep their meaning.
TEST(ViewPlanRegression, CountAllRuleRoundTripsAuthoredAndCompiled) {
    rune::view::ViewPlan a;
    a.viewName = "user_totals";
    a.key      = "userId";
    a.sources  = {{5, "userId"}};
    a.output   = {{"userId", rune::codec::PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                  {"n", rune::codec::PrimType::Int64, 0, "", rune::view::MergeRule::CountAll}};
    auto back = rune::view::deserializeAuthored(rune::view::serializeAuthored(a));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->output[1].rule, rune::view::MergeRule::CountAll);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::MergeRule::CountAll), 7u);

    rune::view::CompiledViewPlan c;
    c.viewName = "user_totals"; c.viewTopicId = 9; c.keyFieldId = {1}; c.sourceTopicId = {5};
    c.outputFingerprint = 1;
    c.output = {{0, rune::codec::PrimType::Int64, 0, 1, rune::view::MergeRule::Replace},
                {1, rune::codec::PrimType::Int64, 0, rune::view::kNoSourceField,
                 rune::view::MergeRule::CountAll}};
    auto cback = rune::view::deserializeCompiledViewPlan(rune::view::serialize(c));
    ASSERT_TRUE(cback.has_value());
    EXPECT_EQ(cback->output[1].rule, rune::view::MergeRule::CountAll);
    EXPECT_EQ(cback->output[1].srcFieldId, rune::view::kNoSourceField);
}

TEST(ViewPlanRegression, UnknownRuleValueIsRejected) {
    rune::view::ViewPlan a;
    a.viewName = "v"; a.key = "k"; a.sources = {{5, "k"}};
    a.output = {{"k", rune::codec::PrimType::Int64, 0, "k", static_cast<rune::view::MergeRule>(8)}};
    EXPECT_FALSE(rune::view::deserializeAuthored(rune::view::serializeAuthored(a)).has_value())
        << "rule byte 8 (one past CountAll) must be rejected, never cast";
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'ViewPlanRegression' --output-on-failure`
Expected: compile error `no member named 'CountAll' in 'rune::view::MergeRule'` / `no member named 'kNoSourceField'`.

- [ ] **Step 3: Implement**

`libs/view/include/rune/view/ViewTypes.h` — replace the enum line and add the sentinel:

```cpp
// Wire values are APPEND-ONLY (serialized as u8 in plan blobs): CountAll (= SQL
// Count(*), counts contributors including nulls) was appended at 7 in Phase 2a.
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll };

// CompiledField::srcFieldId for a rule that reads no source field (CountAll).
inline constexpr uint16_t kNoSourceField = 0xFFFF;
```

`libs/view/src/ViewPlan.cpp` — `isValidMergeRule`:

```cpp
bool isValidMergeRule(uint8_t v) {
    return v <= static_cast<uint8_t>(MergeRule::CountAll);
}
```

`rune-client-cpp/include/rune/client/view/ViewPlanAuthor.h:42` — mirror the broker enum exactly (byte-identical authoring):

```cpp
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll };  // CountAll=7 (Count(*))
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'ViewPlan' --output-on-failure`
Expected: all `ViewPlan*` tests PASS. Also build the client: `cmake --build ../rune-client-cpp/build -j` (or the client's configured build dir) — compiles.

- [ ] **Step 5: Commit** (two repos)

```bash
git add libs/view/include/rune/view/ViewTypes.h libs/view/src/ViewPlan.cpp libs/view/tests/view/ViewPlanTest.cpp
git commit -m "feat(view): MergeRule::CountAll (Count(*)) appended at wire value 7"
git -C ../rune-client-cpp add include/rune/client/view/ViewPlanAuthor.h
git -C ../rune-client-cpp commit -m "feat(view): client MergeRule::CountAll mirrors broker (wire value 7)"
```

---

### Task 2: Compiler — aggregate validation and output types

Phase 1's compiler accepts `Count`/`Sum`/`Avg`/`Min`/`Max`/`Last` and the fold silently applies them as `Replace` (a `Sum` view returns the latest value). This task makes the compiler the gate: valid aggregate views compile with fixed output types; `Min`/`Max`/`Last` are rejected until Phase 2b.

**Files:**
- Modify: `libs/view/src/PlanCompiler.cpp`
- Test: `libs/view/tests/view/PlanCompilerTest.cpp`

**Interfaces:**
- Consumes: `MergeRule::CountAll`, `kNoSourceField` (Task 1).
- Produces: for an aggregate view, `CompiledField.srcFieldId == kNoSourceField` for `CountAll`; output types per Global Constraints. Error messages for non-key-column problems must **not** contain the word `key` (AdminWorker maps messages containing "key" to `ViewKeyInvalid`).

- [ ] **Step 1: Write the failing tests** — append to `libs/view/tests/view/PlanCompilerTest.cpp`:

```cpp
namespace {
rune::codec::Descriptor orderSourceForCompile() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("userId", rune::codec::PrimType::Int64)
        .add("amount", rune::codec::PrimType::Int32)
        .add("qty", rune::codec::PrimType::UInt32)
        .add("price", rune::codec::PrimType::Float64)
        .add("note", rune::codec::PrimType::Bool)
        .build();
}
rune::view::ViewPlan aggPlan(std::vector<rune::view::AuthoredField> rest) {
    rune::view::ViewPlan v;
    v.viewName = "user_totals";
    v.key      = "userId";
    v.sources  = {{5, "userId"}};
    v.output   = {{"userId", rune::codec::PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace}};
    for (auto& f : rest) v.output.push_back(std::move(f));
    return v;
}
std::expected<rune::view::CompileResult, rune::view::CompileError> compileAgg(const rune::view::ViewPlan& v) {
    static const auto src = orderSourceForCompile();
    const rune::codec::Descriptor* srcs[] = {&src};
    return rune::view::compileView(v, srcs, 77);
}
using P = rune::codec::PrimType;
using R = rune::view::MergeRule;
} // namespace

TEST(PlanCompilerAggregateRegression, ValidAggregateViewCompilesWithRuleOutputTypes) {
    auto r = compileAgg(aggPlan({{"n", P::Int64, 0, "", R::CountAll},
                                 {"notes", P::Int64, 0, "note", R::Count},
                                 {"total", P::Int64, 0, "amount", R::Sum},
                                 {"qtySum", P::UInt64, 0, "qty", R::Sum},
                                 {"priceSum", P::Float64, 0, "price", R::Sum},
                                 {"avg", P::Float64, 0, "amount", R::Avg}}));
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& out = r->plan.output;
    ASSERT_EQ(out.size(), 7u);
    EXPECT_EQ(out[1].rule, R::CountAll);
    EXPECT_EQ(out[1].srcFieldId, rune::view::kNoSourceField);
    EXPECT_EQ(out[3].type, P::Int64);     // Sum(Int32) -> Int64
    EXPECT_EQ(out[4].type, P::UInt64);    // Sum(UInt32) -> UInt64
    EXPECT_EQ(out[6].type, P::Float64);   // Avg -> Float64
}

TEST(PlanCompilerAggregateRegression, SumDeclaredWithSourceTypeIsRejected) {
    auto r = compileAgg(aggPlan({{"total", P::Int32, 0, "amount", R::Sum}}));
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("produces Int64"), std::string::npos) << r.error().message;
    EXPECT_EQ(r.error().message.find("key"), std::string::npos) << "must not classify as KeyInvalid";
}

TEST(PlanCompilerAggregateRegression, MinMaxLastAreRejectedUntilPhase2b) {
    for (R rule : {R::Min, R::Max, R::Last}) {
        auto r = compileAgg(aggPlan({{"x", P::Int32, 0, "amount", rule}}));
        ASSERT_FALSE(r.has_value());
        EXPECT_NE(r.error().message.find("Phase 2b"), std::string::npos) << r.error().message;
    }
}

TEST(PlanCompilerAggregateRegression, CountAllWithSourceFieldIsRejected) {
    auto r = compileAgg(aggPlan({{"n", P::Int64, 0, "amount", R::CountAll}}));
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("Count(*)"), std::string::npos);
}

TEST(PlanCompilerAggregateRegression, NonGroupReplaceColumnInAggregateViewIsRejected) {
    auto r = compileAgg(aggPlan({{"total", P::Int64, 0, "amount", R::Sum},
                                 {"price", P::Float64, 0, "price", R::Replace}}));
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("must be an aggregate"), std::string::npos);
    EXPECT_EQ(r.error().message.find("key"), std::string::npos);
}

TEST(PlanCompilerAggregateRegression, GroupColumnMustProjectTheGroupByField) {
    auto v = aggPlan({{"total", P::Int64, 0, "amount", R::Sum}});
    v.output[0].srcField = "orderId";   // view key column projects a different field than sources[0].keyField
    auto r = compileAgg(v);
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("group-by"), std::string::npos);
}

TEST(PlanCompilerAggregateRegression, ReplaceOnlyViewIsUnchanged) {
    rune::view::ViewPlan v;
    v.viewName = "orders"; v.key = "orderId"; v.sources = {{5, "orderId"}};
    v.output = {{"orderId", P::Int64, 0, "orderId", R::Replace},
                {"amount", P::Int32, 0, "amount", R::Replace}};
    EXPECT_TRUE(compileAgg(v).has_value());
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'PlanCompilerAggregateRegression' --output-on-failure`
Expected: `ValidAggregateViewCompilesWithRuleOutputTypes` FAILS (`CountAll` source field "" unknown / type mismatch), `MinMaxLast...` FAILS (compiles today), etc.

- [ ] **Step 3: Implement** — in `libs/view/src/PlanCompiler.cpp`:

Add to the anonymous namespace (after `requiresNumeric`):

```cpp
bool isAggregateRule(MergeRule r) {
    return r == MergeRule::Count || r == MergeRule::CountAll || r == MergeRule::Sum ||
           r == MergeRule::Avg;
}

bool isPhase2bRule(MergeRule r) {
    return r == MergeRule::Min || r == MergeRule::Max || r == MergeRule::Last;
}

const char* primName(rune::codec::PrimType t) {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: return "Int32";   case P::Int64: return "Int64";
        case P::UInt16: return "UInt16"; case P::UInt32: return "UInt32";
        case P::UInt64: return "UInt64"; case P::Float64: return "Float64";
        default: return "a non-numeric type";
    }
}

// The output type an aggregate rule produces from a source field of type `src`
// (Global Constraints: fixed by rule, never cast). nullopt = not aggregatable.
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
        default: return std::nullopt;
    }
}
```

Add `#include <optional>` at the top. In the resolve loop (`for (const auto& af : authored.output)`), insert as the **first** statements of the loop body:

```cpp
        if (isPhase2bRule(af.rule)) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' uses Min/Max/Last, which are not supported until Phase 2b"});
        }
        if (af.rule == MergeRule::CountAll) {
            if (!af.srcField.empty()) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': output field '" + af.name +
                    "' is Count(*), which takes no source field"});
            }
            if (af.type != rune::codec::PrimType::Int64) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': output field '" + af.name +
                    "' is Count(*), which produces Int64; declare output type Int64"});
            }
            ResolvedOutputField rof;
            rof.srcFieldId = kNoSourceField;
            resolved.push_back(rof);
            continue;
        }
```

Replace the existing `if (af.type != fd->type) { ... "Phase 1 Replace does not cast" ... }` block with:

```cpp
        if (isAggregateRule(af.rule)) {
            const auto out = aggregateOutputType(af.rule, fd->type);
            if (!out) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': output field '" + af.name +
                    "' cannot aggregate source field '" + af.srcField + "' of " +
                    primName(fd->type)});
            }
            if (af.type != *out) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': output field '" + af.name +
                    "' aggregates " + primName(fd->type) + ", which produces " +
                    primName(*out) + "; declare output type " + primName(*out)});
            }
        } else if (af.type != fd->type) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' declared type does not match resolved source field '" + af.srcField +
                "' type (Replace does not cast)"});
        }
```

After step 3 (the `keyIt` / `keyOutIdx` lookup) insert:

```cpp
    // --- 3b. Aggregate views (Phase 2a, table semantics): the view key column is
    //         the GROUP BY column — Replace, projecting sources[0].keyField — and
    //         every other column is an aggregate (SQL: no bare non-grouped column).
    const bool aggregate = std::any_of(authored.output.begin(), authored.output.end(),
                                       [](const AuthoredField& af) {
                                           return af.rule != MergeRule::Replace;
                                       });
    if (aggregate) {
        if (keyIt->rule != MergeRule::Replace ||
            keyIt->srcField != authored.sources[0].keyField) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': the group-by column '" + authored.key +
                "' must use Replace and project the group-by source field '" +
                authored.sources[0].keyField + "'"});
        }
        for (size_t i = 0; i < authored.output.size(); ++i) {
            if (i != keyOutIdx && authored.output[i].rule == MergeRule::Replace) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': column '" + authored.output[i].name +
                    "' in an aggregate view must be an aggregate (Count, Count(*), Sum, Avg); "
                    "only the group-by column may use Replace"});
            }
        }
    }
```

Note: the "group-by column" message contains no `key` substring other than through `authored.key`'s *value*; keep the literal text free of the word "key".

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'PlanCompiler' --output-on-failure`
Expected: all `PlanCompiler*` PASS (Phase 1 tests included).

- [ ] **Step 5: Commit**

```bash
git add libs/view/src/PlanCompiler.cpp libs/view/tests/view/PlanCompilerTest.cpp
git commit -m "feat(view): compile aggregate views (Count/Count(*)/Sum/Avg) with fixed output types; reject Min/Max/Last until 2b"
```

---

### Task 3: `AggState` — aggregate layout and accumulator helpers

**Files:**
- Create: `libs/view/include/rune/view/AggState.h`, `libs/view/src/AggState.cpp`
- Modify: `libs/view/CMakeLists.txt` (add `src/AggState.cpp` to `rune_view`; add `tests/view/AggStateTest.cpp` to `rune_view_tests`)
- Test: `libs/view/tests/view/AggStateTest.cpp`

**Interfaces:**
- Consumes: `CompiledViewPlan`, `MergeRule`, `kNoSourceField`, `RowLayout`.
- Produces (exact):

```cpp
namespace rune::view {
bool isAggregateView(const CompiledViewPlan& plan) noexcept;
enum class AccKind : uint8_t { None, Signed, Unsigned, Float };
struct AggField { uint16_t outSlot; MergeRule rule; uint16_t srcFieldId; AccKind kind; uint16_t input; };
struct AggLayout {
    uint16_t keyOutSlot = 0; uint16_t keySrcFieldId = 0;
    rune::codec::PrimType keyType = rune::codec::PrimType::Int64;
    std::vector<AggField> fields; uint16_t numInputs = 0;
    RowLayout contributorRow; uint32_t contribNullOff = 8; uint32_t contribValOff = 8;
    RowLayout groupRow; uint32_t groupAccOff = 8; uint32_t groupNonNullOff = 8;
};
std::optional<AggLayout> buildAggLayout(const CompiledViewPlan& plan,
                                        const rune::codec::Descriptor& evolvingSrc);
inline constexpr uint8_t kAggTableView = 0, kAggTableContributors = 1, kAggTableGroups = 2;
// byte helpers: loadU64/storeU64, loadI128/storeI128, loadU128/storeU128, loadF64/storeF64
}
```

Row layouts (fixed width, little-endian, memcpy-encoded):
- contributor row: `[groupKey u64][null bitmap ceil(numInputs/8) bytes, bit set = NULL][numInputs × 8-byte value]` — value is `int64` (Signed), `uint64` (Unsigned), `double` bits (Float), unused (Count(col) — only its null bit matters).
- group row: `[contributors u64][fields.size() × 16-byte accumulator][fields.size() × u64 non-null count]` — accumulator is `__int128` (Signed), `unsigned __int128` (Unsigned), `double` in the low 8 bytes (Float), unused (Count/CountAll).

- [ ] **Step 1: Write the failing test** — create `libs/view/tests/view/AggStateTest.cpp`:

```cpp
// libs/view/tests/view/AggStateTest.cpp — Phase 2a aggregate layout.
#include <gtest/gtest.h>

#include <rune/codec/Descriptor.h>
#include <rune/view/AggState.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewTable.h>

using P = rune::codec::PrimType;
using R = rune::view::MergeRule;

namespace {
rune::codec::Descriptor orderSrc() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64)
        .add("amount", P::Int32)
        .add("qty", P::UInt32)
        .add("price", P::Float64)
        .build();
}
rune::view::CompileResult compileOrders() {
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"cnt", P::Int64, 0, "price", R::Count},
                {"total", P::Int64, 0, "amount", R::Sum},
                {"qtySum", P::UInt64, 0, "qty", R::Sum},
                {"avgPrice", P::Float64, 0, "price", R::Avg}};
    static const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    return rune::view::compileView(v, srcs, 9).value();
}
} // namespace

TEST(AggState, ReplaceOnlyPlanIsNotAggregate) {
    rune::view::CompiledViewPlan p;
    p.output = {{0, P::Int64, 0, 1, R::Replace}};
    EXPECT_FALSE(rune::view::isAggregateView(p));
    p.output.push_back({1, P::Int64, 0, rune::view::kNoSourceField, R::CountAll});
    EXPECT_TRUE(rune::view::isAggregateView(p));
}

TEST(AggState, LayoutDerivesKindsInputsAndRowLengths) {
    const auto comp = compileOrders();
    const auto src = orderSrc();
    const auto a = rune::view::buildAggLayout(comp.plan, src);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->keyOutSlot, 0);
    EXPECT_EQ(a->keyType, P::Int64);
    ASSERT_EQ(a->fields.size(), 5u);                       // every non-key column
    EXPECT_EQ(a->fields[0].rule, R::CountAll);
    EXPECT_EQ(a->fields[0].input, rune::view::kNoSourceField);
    EXPECT_EQ(a->fields[1].kind, rune::view::AccKind::None);     // Count(price)
    EXPECT_EQ(a->fields[2].kind, rune::view::AccKind::Signed);   // Sum(Int32)
    EXPECT_EQ(a->fields[3].kind, rune::view::AccKind::Unsigned); // Sum(UInt32)
    EXPECT_EQ(a->fields[4].kind, rune::view::AccKind::Float);    // Avg(Float64)
    EXPECT_EQ(a->numInputs, 4u);                             // all but CountAll
    EXPECT_EQ(a->contribValOff, 8u + 1u);                    // 4 inputs -> 1 bitmap byte
    EXPECT_EQ(a->contributorRow.rowLen, 8u + 1u + 4u * 8u);
    EXPECT_EQ(a->groupNonNullOff, 8u + 5u * 16u);
    EXPECT_EQ(a->groupRow.rowLen, 8u + 5u * 16u + 5u * 8u);
}

TEST(AggState, AuxTablesAcceptRawLayouts) {
    const auto comp = compileOrders();
    const auto a = rune::view::buildAggLayout(comp.plan, orderSrc()).value();
    rune::view::ViewTable contributors(a.contributorRow);
    uint8_t* row = contributors.put(42);
    rune::view::storeU64(row, 7);
    EXPECT_EQ(rune::view::loadU64(contributors.get(42)), 7u);
}

TEST(AggState, Int128AccumulatorRoundTripsBeyondInt64) {
    uint8_t buf[16] = {};
    const __int128 big = static_cast<__int128>(INT64_MAX) * 4;
    rune::view::storeI128(buf, big);
    EXPECT_TRUE(rune::view::loadI128(buf) == big);
    rune::view::storeF64(buf, 2.5);
    EXPECT_DOUBLE_EQ(rune::view::loadF64(buf), 2.5);
}

TEST(AggStateRegression, UnknownSourceFieldYieldsNullopt) {
    auto comp = compileOrders();
    comp.plan.output[3].srcFieldId = 999;   // not in the source descriptor
    EXPECT_FALSE(rune::view::buildAggLayout(comp.plan, orderSrc()).has_value());
}
```

Add to `libs/view/CMakeLists.txt`: `src/AggState.cpp` in `add_library(rune_view ...)` and `tests/view/AggStateTest.cpp` in `add_executable(rune_view_tests ...)`.

- [ ] **Step 2: Run to verify it fails**

Run: `cmake -S . -B build-fast >/dev/null && cmake --build build-fast -j --target rune_view_tests`
Expected: compile error `'rune/view/AggState.h' file not found`.

- [ ] **Step 3: Implement**

`libs/view/include/rune/view/AggState.h`:

```cpp
// libs/view/include/rune/view/AggState.h
//
// Phase 2a — invertible aggregates (Count, Count(*), Sum, Avg) with TABLE
// semantics: a contributor is the source record's 8-byte WIRE key, its group
// the view key read from the record VALUE. Per (view, partition) the fold keeps
// two fixed-width auxiliary ViewTables next to the output rows:
//   contributors : contributorKey -> [groupKey u64][null bitmap][values]
//                  (the pre-image used to retract an update/delete)
//   groups       : groupKey -> [contributors u64][accumulators][non-null counts]
// Both are checkpointed with the output rows in one multi-table record (§9.2).
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

// The numeric domain a Sum/Avg input accumulates in.
enum class AccKind : uint8_t { None, Signed, Unsigned, Float };

struct AggField {
    uint16_t  outSlot;     // output column written by this aggregate
    MergeRule rule;        // Count | CountAll | Sum | Avg
    uint16_t  srcFieldId;  // kNoSourceField for CountAll
    AccKind   kind;        // Sum/Avg: from the source field's type; None otherwise
    uint16_t  input;       // contributor value index; kNoSourceField for CountAll
};

struct AggLayout {
    uint16_t              keyOutSlot    = 0;   // the group-by output column
    uint16_t              keySrcFieldId = 0;   // group-by field read from the value
    rune::codec::PrimType keyType       = rune::codec::PrimType::Int64;
    std::vector<AggField> fields;              // one per non-key output column
    uint16_t              numInputs     = 0;   // fields with a source input
    RowLayout             contributorRow;      // only rowLen is meaningful
    uint32_t              contribNullOff = 8;
    uint32_t              contribValOff  = 8;
    RowLayout             groupRow;            // only rowLen is meaningful
    uint32_t              groupAccOff     = 8;
    uint32_t              groupNonNullOff = 8;
};

// Builds the layout for an aggregate view. `evolvingSrc` is the source's
// evolving descriptor; a field's type never changes for a given tag (a retype
// is a new tag), so it decides each Sum/Avg accumulator kind. nullopt if a
// referenced source field is not in `evolvingSrc` (compiler-guaranteed never).
std::optional<AggLayout> buildAggLayout(const CompiledViewPlan& plan,
                                        const rune::codec::Descriptor& evolvingSrc);

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

`libs/view/src/AggState.cpp`:

```cpp
// libs/view/src/AggState.cpp — see AggState.h.
#include <rune/view/AggState.h>

#include <algorithm>

namespace rune::view {

namespace {

std::optional<rune::codec::PrimType> srcType(const rune::codec::Descriptor& d, uint16_t id) {
    for (const auto& f : d.fields) {
        if (f.id == id) return f.type;
    }
    return std::nullopt;
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

} // namespace

bool isAggregateView(const CompiledViewPlan& plan) noexcept {
    return std::any_of(plan.output.begin(), plan.output.end(),
                       [](const CompiledField& f) { return f.rule != MergeRule::Replace; });
}

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
        if (f.rule != MergeRule::CountAll) {
            const auto t = srcType(evolvingSrc, f.srcFieldId);
            if (!t) return std::nullopt;
            af.input = a.numInputs++;
            if (f.rule == MergeRule::Sum || f.rule == MergeRule::Avg) af.kind = accKindOf(*t);
        }
        a.fields.push_back(af);
    }
    a.contribNullOff        = 8;
    a.contribValOff         = 8 + (static_cast<uint32_t>(a.numInputs) + 7) / 8;
    a.contributorRow.rowLen = a.contribValOff + 8u * a.numInputs;
    a.groupAccOff           = 8;
    a.groupNonNullOff       = 8 + 16u * static_cast<uint32_t>(a.fields.size());
    a.groupRow.rowLen       = a.groupNonNullOff + 8u * static_cast<uint32_t>(a.fields.size());
    return a;
}

} // namespace rune::view
```

- [ ] **Step 4: Run to verify it passes**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'AggState' --output-on-failure`
Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/view/include/rune/view/AggState.h libs/view/src/AggState.cpp libs/view/tests/view/AggStateTest.cpp libs/view/CMakeLists.txt
git commit -m "feat(view): AggLayout + accumulator helpers for invertible aggregates"
```

---

### Task 4: `foldAggregateBatch` — the aggregate fold

**Files:**
- Create: `libs/view/include/rune/view/AggFold.h`, `libs/view/src/AggFold.cpp`
- Modify: `libs/view/include/rune/view/ViewFold.h` (`FoldStats` gains two counters)
- Modify: `libs/view/CMakeLists.txt` (add `src/AggFold.cpp`; add `tests/view/AggFoldTest.cpp`)
- Test: `libs/view/tests/view/AggFoldTest.cpp`

**Interfaces:**
- Consumes: `AggLayout`, helpers, `kAggTable*` (Task 3); `VersionDescriptorCache`, `FoldStats` (Phase 1).
- Produces:

```cpp
FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor);
```
  `FoldStats` gains `uint64_t skippedNoGroup = 0;` (live record with no group-by value: its old contribution is retracted, nothing added) and `uint64_t aggOverflow = 0;` (an output Sum did not fit its type → written NULL).

Semantics per record (offset < bound, non-control batch):
1. Wire key not exactly 8 bytes → `skippedMalformed++`, no state change.
2. Contributor `c` = wire key as LE u64.
3. Tombstone: if `c` is known → retract it (and drop its group if it empties), erase it; `deleted++` either way.
4. Live: decode against the writer version; read the group-by field. Absent → retract `c` if known, erase it, `skippedNoGroup++`.
5. Else: if known, retract the old contribution **without** dropping its group yet; write the new contributor row; add it to the new group; then if the old group differs and is now empty, drop it. `applied++`.
Output row for a group is rewritten after every change to it. Empty group ⇒ its view row and group row are erased.

- [ ] **Step 1: Write the failing tests** — create `libs/view/tests/view/AggFoldTest.cpp`:

```cpp
// libs/view/tests/view/AggFoldTest.cpp — Phase 2a aggregate fold (table semantics).
#include <gtest/gtest.h>

#include <climits>
#include <cstring>
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

// order: v1 = {orderId Int64 (key), userId Int64, amount Int64, qty UInt32, price Float64};
//        v2 adds bonus Int64 (sinceVersion 2) — a v1 record reads bonus as ABSENT (NULL).
rune::codec::Descriptor orderSource() {
    return rune::codec::DescriptorBuilder("order", 2)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64, 1)
        .add("amount", P::Int64, 1)
        .add("qty", P::UInt32, 1)
        .add("price", P::Float64, 1)
        .add("bonus", P::Int64, 2)
        .build();
}

// Output slots (key first, then declaration order):
//  0 userId | 1 n=Count(*) | 2 bonusCnt=Count(bonus) | 3 total=Sum(amount) | 4 avg=Avg(amount)
//  5 qtySum=Sum(qty) UInt64 | 6 priceAvg=Avg(price) | 7 bonusSum=Sum(bonus)
enum Slot : uint16_t { kUser = 0, kN, kBonusCnt, kTotal, kAvg, kQtySum, kPriceAvg, kBonusSum };

rune::view::ViewPlan userTotals() {
    rune::view::ViewPlan v;
    v.viewName = "user_totals"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"bonusCnt", P::Int64, 0, "bonus", R::Count},
                {"total", P::Int64, 0, "amount", R::Sum},
                {"avg", P::Float64, 0, "amount", R::Avg},
                {"qtySum", P::UInt64, 0, "qty", R::Sum},
                {"priceAvg", P::Float64, 0, "price", R::Avg},
                {"bonusSum", P::Int64, 0, "bonus", R::Sum}};
    return v;
}

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le32(std::vector<uint8_t>& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }

std::vector<uint8_t> orderV1(int64_t orderId, int64_t userId, int64_t amount,
                             uint32_t qty = 1, double price = 1.0) {
    std::vector<uint8_t> b;
    le16(b, 8 + 8 + 8 + 4 + 8);  le16(b, 1);
    le64(b, uint64_t(orderId)); le64(b, uint64_t(userId)); le64(b, uint64_t(amount));
    le32(b, qty); uint64_t pr; std::memcpy(&pr, &price, 8); le64(b, pr);
    return b;
}
std::vector<uint8_t> orderV2(int64_t orderId, int64_t userId, int64_t amount, int64_t bonus) {
    std::vector<uint8_t> b;
    le16(b, 8 + 8 + 8 + 4 + 8 + 8);  le16(b, 2);
    le64(b, uint64_t(orderId)); le64(b, uint64_t(userId)); le64(b, uint64_t(amount));
    le32(b, 1); double price = 1.0; uint64_t pr; std::memcpy(&pr, &price, 8); le64(b, pr);
    le64(b, uint64_t(bonus));
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

    static rune::view::CompileResult compile(const rune::codec::Descriptor& s) {
        const rune::codec::Descriptor* srcs[] = {&s};
        return rune::view::compileView(userTotals(), srcs, 9).value();
    }

    // One batch; contributor key = 8-byte LE orderId (or a raw key if given).
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
        next = bound;
    }
    static std::vector<uint8_t> key(int64_t orderId) {
        std::vector<uint8_t> k; le64(k, uint64_t(orderId)); return k;
    }
    void put(int64_t orderId, std::vector<uint8_t> value) { apply({{key(orderId), std::move(value)}}); }

    bool hasGroup(int64_t userId) const { return view.get(uint64_t(userId)) != nullptr; }
    rune::codec::FieldValue col(int64_t userId, uint16_t slot) const {
        const uint8_t* r = view.get(uint64_t(userId));
        EXPECT_NE(r, nullptr) << "group " << userId << " must exist";
        return r ? rune::view::readSlot(r, comp.rowLayout, slot) : rune::codec::FieldValue{};
    }
};

} // namespace

TEST(AggFold, InsertsAccumulatePerGroup) {
    Harness h;
    h.put(1, orderV1(1, 100, 10, 2, 1.5));
    h.put(2, orderV1(2, 100, 30, 3, 2.5));
    h.put(3, orderV1(3, 200, 5));
    EXPECT_EQ(h.col(100, kUser).i, 100);
    EXPECT_EQ(h.col(100, kN).i, 2);
    EXPECT_EQ(h.col(100, kTotal).i, 40);
    EXPECT_DOUBLE_EQ(h.col(100, kAvg).f, 20.0);
    EXPECT_EQ(h.col(100, kQtySum).u, 5u);
    EXPECT_DOUBLE_EQ(h.col(100, kPriceAvg).f, 2.0);
    EXPECT_EQ(h.col(100, kBonusCnt).i, 0) << "Count(col) over all-null is 0";
    EXPECT_FALSE(h.col(100, kBonusSum).present) << "Sum over all-null is NULL (SQL)";
    EXPECT_EQ(h.col(200, kN).i, 1);
    EXPECT_EQ(h.col(200, kTotal).i, 5);
    EXPECT_EQ(h.last.applied, 1u);
}

TEST(AggFoldRegression, UpdateRetractsOldContributionNotAddsTwice) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(1, orderV1(1, 100, 50));
    EXPECT_EQ(h.col(100, kN).i, 1) << "same contributor -> still one";
    EXPECT_EQ(h.col(100, kTotal).i, 50) << "old 10 retracted, new 50 added";
}

TEST(AggFoldRegression, UpdateMovesContributorBetweenGroupsAndDropsEmptyGroupRegression) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(3, orderV1(3, 200, 5));
    h.put(3, orderV1(3, 100, 5));   // contributor 3 moves group 200 -> 100
    EXPECT_FALSE(h.hasGroup(200)) << "the emptied group has no row";
    EXPECT_EQ(h.groups.get(200), nullptr) << "and no accumulator row";
    EXPECT_EQ(h.col(100, kN).i, 2);
    EXPECT_EQ(h.col(100, kTotal).i, 15);
}

TEST(AggFoldRegression, TombstoneRetractsAndLastContributorDropsGroup) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(2, orderV1(2, 100, 30));
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_EQ(h.col(100, kTotal).i, 10);
    h.put(1, tombstone());
    EXPECT_FALSE(h.hasGroup(100));
    EXPECT_EQ(h.contributors.size(), 0u);
    EXPECT_EQ(h.groups.size(), 0u);
}

TEST(AggFoldRegression, TombstoneForUnknownContributorIsNoop) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(99, tombstone());
    EXPECT_EQ(h.last.deleted, 1u);
    EXPECT_EQ(h.col(100, kN).i, 1) << "no negative counts / no stray retraction";
    EXPECT_EQ(h.col(100, kTotal).i, 10);
}

TEST(AggFoldRegression, IdenticalReproduceIsIdempotent) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(1, orderV1(1, 100, 10));
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_EQ(h.col(100, kTotal).i, 10);
}

TEST(AggFoldRegression, RetractingLastNonNullMakesSumNullNotZeroRegression) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));        // bonus NULL (v1)
    h.put(2, orderV2(2, 100, 20, 7));     // bonus 7
    EXPECT_EQ(h.col(100, kBonusCnt).i, 1);
    EXPECT_EQ(h.col(100, kBonusSum).i, 7);
    EXPECT_EQ(h.col(100, kN).i, 2) << "Count(*) counts the null-bonus contributor too";
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kN).i, 1) << "group keeps its remaining contributor";
    EXPECT_EQ(h.col(100, kBonusCnt).i, 0);
    EXPECT_FALSE(h.col(100, kBonusSum).present) << "all-null Sum is NULL, never 0";
    EXPECT_EQ(h.col(100, kTotal).i, 10);
}

TEST(AggFoldRegression, Int64SumOverflowIsNullThenRecoversExactlyRegression) {
    Harness h;
    h.put(1, orderV1(1, 100, INT64_MAX));
    h.put(2, orderV1(2, 100, 1));
    EXPECT_FALSE(h.col(100, kTotal).present) << "an overflowing Sum is NULL, never wrapped";
    EXPECT_GT(h.last.aggOverflow, 0u);
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kTotal).i, INT64_MAX) << "int128 accumulator retracts exactly";
}

TEST(AggFoldRegression, MalformedContributorKeyIsSkipped) {
    Harness h;
    h.apply({{std::vector<uint8_t>{1, 2, 3, 4}, orderV1(1, 100, 10)}});
    EXPECT_EQ(h.last.skippedMalformed, 1u);
    EXPECT_FALSE(h.hasGroup(100));
    EXPECT_EQ(h.contributors.size(), 0u);
}

TEST(AggFold, CursorAdvancesToBoundAndOnlyCommittedRecordsFold) {
    Harness h;
    rune::protocol::RecordBatchBuilder b(0, 0);
    const auto k1 = Harness::key(1), k2 = Harness::key(2);
    const auto v1 = orderV1(1, 100, 10), v2 = orderV1(2, 100, 20);
    b.append(std::string_view((const char*)k1.data(), 8), std::string_view((const char*)v1.data(), v1.size()), 0);
    b.append(std::string_view((const char*)k2.data(), 8), std::string_view((const char*)v2.data(), v2.size()), 0);
    const auto batch = b.build();
    const rune::storage::LogReadView lv{batch.data(), batch.size(), 0, 2};
    int64_t cursor = 0;
    (void)rune::view::foldAggregateBatch(h.comp.plan, h.comp.rowLayout, h.agg, h.vdc, h.view,
                                         h.contributors, h.groups, lv, /*bound*/ 1, cursor);
    EXPECT_EQ(cursor, 1);
    EXPECT_EQ(h.col(100, kN).i, 1) << "offset 1 is uncommitted and must not fold";
}
```

Also add one test for a missing group-by value — use a source whose group field only exists from version 2:

```cpp
TEST(AggFoldRegression, MissingGroupValueRetractsAndSkips) {
    const auto src = rune::codec::DescriptorBuilder("o", 2)
        .add("orderId", P::Int64, 1, true)
        .add("amount", P::Int64, 1)
        .add("userId", P::Int64, 2)       // the group-by field only exists from v2
        .build();
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"total", P::Int64, 0, "amount", R::Sum}};
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, 9).value();
    auto agg = rune::view::buildAggLayout(comp.plan, src).value();
    rune::view::VersionDescriptorCache vdc(src);
    rune::view::ViewTable view(comp.rowLayout), contributors(agg.contributorRow), groups(agg.groupRow);

    auto fold = [&](int64_t off, std::vector<uint8_t> value) {
        rune::protocol::RecordBatchBuilder b(off, 0);
        const auto k = Harness::key(1);
        b.append(std::string_view((const char*)k.data(), 8),
                 std::string_view((const char*)value.data(), value.size()), 0);
        const auto batch = b.build();
        int64_t cur = off;
        return rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors,
                                              groups, {batch.data(), batch.size(), off, off + 1}, off + 1, cur);
    };
    std::vector<uint8_t> v2; le16(v2, 24); le16(v2, 2); le64(v2, 1); le64(v2, 10); le64(v2, 100);
    fold(0, v2);
    ASSERT_NE(view.get(100), nullptr);
    std::vector<uint8_t> v1; le16(v1, 16); le16(v1, 1); le64(v1, 1); le64(v1, 10);   // no userId
    const auto st = fold(1, v1);
    EXPECT_EQ(st.skippedNoGroup, 1u);
    EXPECT_EQ(view.get(100), nullptr) << "the contributor left its group, which is now empty";
    EXPECT_EQ(contributors.size(), 0u);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake -S . -B build-fast >/dev/null && cmake --build build-fast -j --target rune_view_tests`
Expected: compile error `'rune/view/AggFold.h' file not found`.

- [ ] **Step 3: Implement**

`libs/view/include/rune/view/ViewFold.h` — inside `struct FoldStats`, after `skippedMalformed`:

```cpp
    // Phase 2a aggregate views: a live record whose group-by value is absent
    // (its old contribution, if any, is retracted; nothing is added).
    uint64_t skippedNoGroup   = 0;
    // An output Sum that did not fit its type was written NULL (never wrapped).
    uint64_t aggOverflow      = 0;
```

`libs/view/include/rune/view/AggFold.h`:

```cpp
// libs/view/include/rune/view/AggFold.h — Phase 2a aggregate fold.
#pragma once
#include <cstdint>

#include <rune/storage/LogReadView.h>
#include <rune/view/AggState.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

namespace rune::view {

// Folds every committed record (offset < bound) of `lv` into an aggregate view
// with TABLE semantics (AggState.h): contributor = 8-byte wire key, group = the
// group-by value. Same batch walk, control-batch skip, bound stop, and cursor
// contract as foldBatch (ViewFold.h). Zero heap allocation in steady state.
FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor);

} // namespace rune::view
```

`libs/view/src/AggFold.cpp`:

```cpp
// libs/view/src/AggFold.cpp — see AggFold.h / AggState.h.
#include <rune/view/AggFold.h>

#include <algorithm>
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
        return c + agg.contribValOff + 8u * input;
    }

    // Applies one contributor row to its group: sign = +1 add, -1 retract.
    // Returns the group row (never null).
    uint8_t* applyContribution(const uint8_t* c, int sign) noexcept {
        const uint64_t g = loadU64(c);
        uint8_t* gr = groups.put(g);
        const uint64_t n = loadU64(gr);
        storeU64(gr, sign > 0 ? n + 1 : (n > 0 ? n - 1 : 0));
        for (size_t i = 0; i < agg.fields.size(); ++i) {
            const AggField& f = agg.fields[i];
            if (f.input == kNoSourceField) continue;                 // CountAll
            if (nullBit(c, agg.contribNullOff, f.input)) continue;   // nulls contribute nothing
            const uint64_t nnv = loadU64(nn(gr, i));
            storeU64(nn(gr, i), sign > 0 ? nnv + 1 : (nnv > 0 ? nnv - 1 : 0));
            const uint8_t* v = val(c, f.input);
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
                    storeF64(acc(gr, i), sign > 0 ? a + x : a - x);
                    break;
                }
                case AccKind::None: break;   // Count(col): non-null count only
            }
        }
        return gr;
    }

    void dropGroup(uint64_t g) noexcept {
        view.erase(g);
        groups.erase(g);
    }

    // Rewrites the output row for group `g` from its accumulators.
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
                default: out.present = false; break;   // unreachable: compiler gate
            }
            writeSlot(row, layout, f.outSlot, out);
        }
    }

    // Retracts contributor row `c` from its group. If `dropIfEmpty`, an emptied
    // group loses its rows; otherwise the caller decides. Returns the old group.
    uint64_t retract(const uint8_t* c, bool dropIfEmpty) noexcept {
        const uint64_t g = loadU64(c);
        uint8_t* gr = applyContribution(c, -1);
        if (loadU64(gr) == 0) {
            if (dropIfEmpty) dropGroup(g);
        } else {
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
            if (key.size() != 8) { ++stats.skippedMalformed; return; }
            uint64_t c = 0;
            std::memcpy(&c, key.data(), 8);   // little-endian identity bytes

            const uint8_t* old = contributors.get(c);
            if (value.empty()) {               // tombstone: retract a known contributor
                if (old != nullptr) { (void)cx.retract(old, /*dropIfEmpty*/ true); contributors.erase(c); }
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
                if (old != nullptr) { (void)cx.retract(old, true); contributors.erase(c); }
                ++stats.skippedNoGroup;
                return;
            }
            uint64_t g = 0;
            if (agg.keyType == PrimType::Int64) std::memcpy(&g, &gk.i, 8); else g = gk.u;

            bool hadOld = false;
            uint64_t oldGroup = 0;
            if (old != nullptr) { hadOld = true; oldGroup = cx.retract(old, /*dropIfEmpty*/ false); }

            uint8_t* cr = contributors.put(c);   // same row when it already existed
            storeU64(cr, g);
            for (const AggField& f : agg.fields) {
                if (f.input == kNoSourceField) continue;
                const FieldValue in = rune::codec::readField(d, asBytes(value), f.srcFieldId);
                setNullBit(cr, agg.contribNullOff, f.input, !in.present);
                uint8_t* dst = cr + agg.contribValOff + 8u * f.input;
                if (!in.present)                  { storeU64(dst, 0); }
                else if (f.kind == AccKind::Signed)   { std::memcpy(dst, &in.i, 8); }
                else if (f.kind == AccKind::Unsigned) { storeU64(dst, in.u); }
                else if (f.kind == AccKind::Float)    { storeF64(dst, in.f); }
                else                                  { storeU64(dst, 0); }   // Count(col)
            }
            uint8_t* gr = cx.applyContribution(cr, +1);
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
    cursor = corrupt ? walkedUpto : std::min(lv.nextOffset, bound);
    return stats;
}

} // namespace rune::view
```

Note on the in-place update (`hadOld && oldGroup == g`): `retract(old, false)` may leave the group at 0 contributors with stale rows; `applyContribution(cr, +1)` brings it back to ≥1 and `emit` rewrites the output, so no drop happens. `old` points into `contributors`; it is read entirely inside `retract` **before** `contributors.put(c)` overwrites the same row.

Add `src/AggFold.cpp` to `rune_view` and `tests/view/AggFoldTest.cpp` to `rune_view_tests` in `libs/view/CMakeLists.txt`.

- [ ] **Step 4: Run to verify they pass**

Run: `cmake -S . -B build-fast >/dev/null && cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'AggFold|ViewFold' --output-on-failure`
Expected: all `AggFold*` PASS; Phase 1 `ViewFold*` unchanged and PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/view/include/rune/view/AggFold.h libs/view/src/AggFold.cpp libs/view/include/rune/view/ViewFold.h libs/view/tests/view/AggFoldTest.cpp libs/view/CMakeLists.txt
git commit -m "feat(view): aggregate fold with table-semantics retraction, SQL nulls, int128 sums"
```

---

### Task 5: Multi-table checkpoint record (v2)

An aggregate view's output rows, contributor map, and group accumulators must be checkpointed **atomically at one offset**: restoring one table at `O` and another at an earlier offset would corrupt every later retraction. v2 puts all tables in one CRC envelope.

**Files:**
- Modify: `libs/view/include/rune/view/ViewCheckpoint.h`, `libs/view/src/ViewCheckpoint.cpp`
- Test: `libs/view/tests/view/ViewCheckpointTest.cpp`

**Interfaces:**
- Produces:

```cpp
inline constexpr uint32_t kViewCheckpointMagicV2 = 0x564B4332u; // "2CKV"
struct TableDelta { uint8_t tableId; uint32_t rowLen; std::span<const DirtyEntry> entries; };
size_t encodeMultiDelta(std::span<const TableDelta> tables, int64_t O, std::vector<uint8_t>& out);
std::optional<AppliedDelta> applyMultiDelta(std::span<ViewTable* const> tables,
                                            std::span<const uint32_t> rowLens,
                                            std::span<const uint8_t> record);
int64_t replayMulti(std::span<ViewTable* const> tables, std::span<const uint32_t> rowLens,
                    std::span<const std::span<const uint8_t>> recordsInOrder);
```
Wire: `[magic2 u32][O i64][tableCount u8]` then per table `[tableId u8][rowLen u32][count u32]` + `count × {rid u32, flags u8 (0 upsert / 1 delete), key u64, [rowLen bytes iff upsert]}`, then `[crc32c u32]` over everything before it. `O` sits at byte 4 exactly like v1 (the IO agent peeks it there). `applyMultiDelta` requires the span to be exactly one record, validates every byte before mutating any table, rejects an unknown/duplicate table id or a `rowLen` mismatch, and returns `{O, totalEntries}`.

- [ ] **Step 1: Write the failing tests** — append to `libs/view/tests/view/ViewCheckpointTest.cpp` (add `#include <rune/view/AggState.h>` if not present for the `kAggTable*` ids):

```cpp
namespace {
rune::view::RowLayout rawLayout(uint32_t rowLen) { rune::view::RowLayout l; l.rowLen = rowLen; return l; }

std::vector<uint8_t> threeTableRecord(int64_t O, uint64_t viewKey, uint8_t viewByte,
                                      uint64_t contribKey, uint8_t contribByte) {
    uint8_t vrow[16]; std::memset(vrow, viewByte, 16);
    uint8_t crow[24]; std::memset(crow, contribByte, 24);
    const rune::view::DirtyEntry ve{0, false, viewKey, vrow};
    const rune::view::DirtyEntry ce{0, false, contribKey, crow};
    const rune::view::DirtyEntry gdel{0, true, 9, nullptr};
    const rune::view::TableDelta tds[] = {
        {rune::view::kAggTableView, 16, {&ve, 1}},
        {rune::view::kAggTableContributors, 24, {&ce, 1}},
        {rune::view::kAggTableGroups, 32, {&gdel, 1}}};
    std::vector<uint8_t> out;
    rune::view::encodeMultiDelta(tds, O, out);
    return out;
}
} // namespace

TEST(ViewCheckpointMulti, ThreeTableRecordRoundTrips) {
    rune::view::ViewTable v(rawLayout(16)), c(rawLayout(24)), g(rawLayout(32));
    std::memset(g.put(9), 0x5A, 32);   // pre-existing group row the record deletes
    rune::view::ViewTable* tables[] = {&v, &c, &g};
    const uint32_t lens[] = {16, 24, 32};
    const auto rec = threeTableRecord(42, 7, 0x11, 8, 0x22);
    const auto applied = rune::view::applyMultiDelta(tables, lens, rec);
    ASSERT_TRUE(applied.has_value());
    EXPECT_EQ(applied->O, 42);
    EXPECT_EQ(applied->count, 3u);
    ASSERT_NE(v.get(7), nullptr); EXPECT_EQ(v.get(7)[0], 0x11);
    ASSERT_NE(c.get(8), nullptr); EXPECT_EQ(c.get(8)[23], 0x22);
    EXPECT_EQ(g.get(9), nullptr);
}

TEST(ViewCheckpointMultiRegression, CorruptRecordMutatesNoTable) {
    rune::view::ViewTable v(rawLayout(16)), c(rawLayout(24)), g(rawLayout(32));
    rune::view::ViewTable* tables[] = {&v, &c, &g};
    const uint32_t lens[] = {16, 24, 32};
    auto rec = threeTableRecord(42, 7, 0x11, 8, 0x22);
    rec[rec.size() - 1] ^= 0xFF;   // break the CRC
    EXPECT_FALSE(rune::view::applyMultiDelta(tables, lens, rec).has_value());
    EXPECT_EQ(v.size(), 0u); EXPECT_EQ(c.size(), 0u);
}

TEST(ViewCheckpointMultiRegression, RowLenMismatchUnknownOrDuplicateTableIsRejected) {
    rune::view::ViewTable v(rawLayout(16)), c(rawLayout(24)), g(rawLayout(32));
    rune::view::ViewTable* tables[] = {&v, &c, &g};
    const auto rec = threeTableRecord(1, 1, 1, 1, 1);
    const uint32_t wrongLens[] = {16, 25, 32};
    EXPECT_FALSE(rune::view::applyMultiDelta(tables, wrongLens, rec).has_value());
    rune::view::ViewTable* two[] = {&v, &c};          // table id 2 unknown here
    const uint32_t twoLens[] = {16, 24};
    EXPECT_FALSE(rune::view::applyMultiDelta(two, twoLens, rec).has_value());

    uint8_t row[16] = {};
    const rune::view::DirtyEntry e{0, false, 1, row};
    const rune::view::TableDelta dup[] = {{0, 16, {&e, 1}}, {0, 16, {&e, 1}}};
    std::vector<uint8_t> out;
    rune::view::encodeMultiDelta(dup, 5, out);
    const uint32_t lens[] = {16, 24, 32};
    EXPECT_FALSE(rune::view::applyMultiDelta(tables, lens, out).has_value());
}

TEST(ViewCheckpointMultiRegression, ReplayStopsAtTornTailAllTablesAtSameOffset) {
    rune::view::ViewTable v(rawLayout(16)), c(rawLayout(24)), g(rawLayout(32));
    rune::view::ViewTable* tables[] = {&v, &c, &g};
    const uint32_t lens[] = {16, 24, 32};
    const auto r1 = threeTableRecord(10, 7, 0x11, 8, 0x22);
    auto r2 = threeTableRecord(20, 7, 0x33, 8, 0x44);
    r2.resize(r2.size() - 3);   // torn tail
    const std::span<const uint8_t> recs[] = {r1, r2};
    EXPECT_EQ(rune::view::replayMulti(tables, lens, recs), 10);
    EXPECT_EQ(v.get(7)[0], 0x11) << "view table at O=10";
    EXPECT_EQ(c.get(8)[0], 0x22) << "contributor table at the SAME O=10";
}

TEST(ViewCheckpointMulti, OffsetIsAtByteFourLikeV1) {
    const auto rec = threeTableRecord(0x0102030405060708LL, 1, 1, 1, 1);
    int64_t O; std::memcpy(&O, rec.data() + 4, 8);
    EXPECT_EQ(O, 0x0102030405060708LL);
    uint32_t magic; std::memcpy(&magic, rec.data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_view_tests`
Expected: compile error `no member named 'encodeMultiDelta'`.

- [ ] **Step 3: Implement**

Append to `libs/view/include/rune/view/ViewCheckpoint.h` (before the closing namespace):

```cpp
// Phase 2a: multi-table record (aggregate views checkpoint their output rows,
// contributor map and group accumulators together, offset-atomic). Layout:
//   [magic2 u32][O i64][tableCount u8]
//   tableCount x { [tableId u8][rowLen u32][count u32]
//                  count x { rid u32, flags u8, key u64, [rowLen bytes iff upsert] } }
//   [crc32c u32]   -- over every preceding byte
// O sits at byte 4 exactly like v1 (the snapshot IO agent peeks it there).
inline constexpr uint32_t kViewCheckpointMagicV2 = 0x564B4332u; // "2CKV" little-endian ASCII

struct TableDelta {
    uint8_t                     tableId;
    uint32_t                    rowLen;
    std::span<const DirtyEntry> entries;
};

size_t encodeMultiDelta(std::span<const TableDelta> tables, int64_t O,
                        std::vector<uint8_t>& out);

// `record` must be exactly one v2 record. Validates everything (bounds, table
// ids < tables.size(), each id at most once, rowLen == rowLens[id], CRC) BEFORE
// mutating any table; then applies every table's entries. {O, total entries}.
std::optional<AppliedDelta> applyMultiDelta(std::span<ViewTable* const> tables,
                                            std::span<const uint32_t> rowLens,
                                            std::span<const uint8_t> record);

// Applies records in order, stopping at the first that fails; returns the last
// applied O, or -1. All tables always reflect the SAME offset.
int64_t replayMulti(std::span<ViewTable* const> tables, std::span<const uint32_t> rowLens,
                    std::span<const std::span<const uint8_t>> recordsInOrder);
```

Append to `libs/view/src/ViewCheckpoint.cpp` (inside `namespace rune::view`, using the file's existing `appendLE32`/`appendLE64`/`readLE32`/`readLE64`):

```cpp
size_t encodeMultiDelta(std::span<const TableDelta> tables, int64_t O,
                        std::vector<uint8_t>& out) {
    const size_t start = out.size();
    appendLE32(out, kViewCheckpointMagicV2);
    appendLE64(out, static_cast<uint64_t>(O));
    out.push_back(static_cast<uint8_t>(tables.size()));
    for (const TableDelta& t : tables) {
        out.push_back(t.tableId);
        appendLE32(out, t.rowLen);
        appendLE32(out, static_cast<uint32_t>(t.entries.size()));
        for (const DirtyEntry& e : t.entries) {
            appendLE32(out, e.rid);
            out.push_back(e.deleted ? uint8_t{1} : uint8_t{0});
            appendLE64(out, e.key);
            if (!e.deleted) out.insert(out.end(), e.row, e.row + t.rowLen);
        }
    }
    appendLE32(out, rune::protocol::crc32c(out.data() + start, out.size() - start));
    return out.size() - start;
}

std::optional<AppliedDelta> applyMultiDelta(std::span<ViewTable* const> tables,
                                            std::span<const uint32_t> rowLens,
                                            std::span<const uint8_t> b) {
    constexpr size_t kHdr = 4 + 8 + 1, kTbl = 1 + 4 + 4, kEnt = 4 + 1 + 8, kCrc = 4;
    if (tables.size() != rowLens.size() || tables.size() > 64) return std::nullopt;
    if (b.size() < kHdr + kCrc) return std::nullopt;
    const uint8_t* p = b.data();
    if (readLE32(p) != kViewCheckpointMagicV2) return std::nullopt;
    const int64_t O = static_cast<int64_t>(readLE64(p + 4));
    const uint8_t tableCount = p[12];

    // Pass 1: validate, never mutating.
    size_t pos = kHdr;
    uint64_t seen = 0;
    uint32_t total = 0;
    for (uint8_t t = 0; t < tableCount; ++t) {
        if (pos + kTbl > b.size()) return std::nullopt;
        const uint8_t  id     = p[pos];
        const uint32_t rowLen = readLE32(p + pos + 1);
        const uint32_t count  = readLE32(p + pos + 5);
        if (id >= tables.size() || rowLens[id] != rowLen || ((seen >> id) & 1u) != 0) {
            return std::nullopt;
        }
        seen |= uint64_t{1} << id;
        pos += kTbl;
        for (uint32_t i = 0; i < count; ++i) {
            if (pos + kEnt > b.size()) return std::nullopt;
            const uint8_t flags = p[pos + 4];
            if (flags > 1) return std::nullopt;
            pos += kEnt;
            if (flags == 0) {
                if (pos + rowLen > b.size()) return std::nullopt;
                pos += rowLen;
            }
        }
        total += count;
    }
    if (pos + kCrc != b.size()) return std::nullopt;
    if (readLE32(p + pos) != rune::protocol::crc32c(p, pos)) return std::nullopt;

    // Pass 2: apply.
    pos = kHdr;
    for (uint8_t t = 0; t < tableCount; ++t) {
        const uint8_t  id     = p[pos];
        const uint32_t rowLen = readLE32(p + pos + 1);
        const uint32_t count  = readLE32(p + pos + 5);
        pos += kTbl;
        ViewTable& tbl = *tables[id];
        for (uint32_t i = 0; i < count; ++i) {
            const uint8_t  flags = p[pos + 4];
            const uint64_t key   = readLE64(p + pos + 5);
            pos += kEnt;
            if (flags == 1) {
                tbl.erase(key);
            } else {
                std::memcpy(tbl.put(key), p + pos, rowLen);
                pos += rowLen;
            }
        }
    }
    return AppliedDelta{O, total};
}

int64_t replayMulti(std::span<ViewTable* const> tables, std::span<const uint32_t> rowLens,
                    std::span<const std::span<const uint8_t>> recordsInOrder) {
    int64_t last = -1;
    for (const auto& rec : recordsInOrder) {
        const auto applied = applyMultiDelta(tables, rowLens, rec);
        if (!applied) break;
        last = applied->O;
    }
    return last;
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R 'ViewCheckpoint' --output-on-failure`
Expected: all PASS (Phase 1 v1 tests included).

- [ ] **Step 5: Commit**

```bash
git add libs/view/include/rune/view/ViewCheckpoint.h libs/view/src/ViewCheckpoint.cpp libs/view/tests/view/ViewCheckpointTest.cpp
git commit -m "feat(view): multi-table checkpoint record (v2), offset-atomic across tables"
```

---

### Task 6: Persist and split v2 records (IO agent + recovery splitter)

The snapshot IO agent drops any delta whose magic isn't v1 (`peekDeltaOffset` → -1), and `splitViewCheckpointRecords` stops at the first non-v1 record. Without this task an aggregate view's checkpoint never reaches disk and every aggregate silently resets on restart.

**Files:**
- Modify: `libs/node/src/ViewSnapshotIoAgent.cpp:40-55`
- Modify: `libs/node/src/WorkerViewSet.cpp` (`splitViewCheckpointRecords`)
- Test: `libs/node/tests/node/ViewSnapshotIoAgentTest.cpp`, `libs/node/tests/node/WorkerViewSetTest.cpp`

**Interfaces:**
- Consumes: `kViewCheckpointMagicV2`, `encodeMultiDelta`, `TableDelta` (Task 5).
- Produces: `splitViewCheckpointRecords` returns v1 and v2 records interleaved in file order.

- [ ] **Step 1: Write the failing tests**

Append to `libs/node/tests/node/ViewSnapshotIoAgentTest.cpp`:

```cpp
// REGRESSION (Phase 2a): an aggregate view's checkpoint is a v2 multi-table
// record. The agent used to drop any non-v1 magic, so aggregate state never
// reached disk and silently reset on restart.
TEST(ViewSnapshotIoAgentRegression, MultiTableDeltaIsPersistedNotDropped) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), /*fsyncMs=*/50);
    rune::consensus::ManualNanoClock clk(0);

    uint8_t row[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    const DirtyEntry e{0, false, 5, row};
    const TableDelta tds[] = {{0, 8, {&e, 1}}, {1, 8, {&e, 1}}};
    std::vector<uint8_t> delta;
    encodeMultiDelta(tds, /*O=*/12, delta);
    std::vector<uint8_t> frame(rune::node::ViewSnapshotChannel::kDeltaHeaderBytes + delta.size());
    for (int i = 0; i < 4; ++i) frame[i]     = uint8_t(uint32_t(kViewTopicId) >> (i * 8));
    for (int i = 0; i < 4; ++i) frame[4 + i] = uint8_t(uint32_t(kPartition) >> (i * 8));
    std::memcpy(frame.data() + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes, delta.data(), delta.size());
    ASSERT_TRUE(ch.deltas.write(frame.data(), frame.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));

    int it = 0;
    for (; it < kHardCap && agent.lastDurableOffset(kViewTopicId, kPartition) < 12; ++it) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL);
    }
    ASSERT_LT(it, kHardCap) << "v2 delta was dropped: durable offset never reached 12";
    EXPECT_EQ(readFile(agent.deltaFilePathForTest(kViewTopicId, kPartition)), delta);
}
```

Append to `libs/node/tests/node/WorkerViewSetTest.cpp` (add `#include <rune/view/ViewCheckpoint.h>`):

```cpp
TEST(WorkerViewSetRegression, SplitterReturnsV1AndV2RecordsInFileOrder) {
    rune::view::RowLayout l; l.rowLen = 8;
    uint8_t row[8] = {};
    const rune::view::DirtyEntry e{0, false, 1, row};
    std::vector<uint8_t> file;
    rune::view::encodeDelta(std::span<const rune::view::DirtyEntry>(&e, 1), 1, 8, file);
    const size_t v1Len = file.size();
    const rune::view::TableDelta tds[] = {{0, 8, {&e, 1}}};
    rune::view::encodeMultiDelta(tds, 2, file);
    rune::view::encodeDelta(std::span<const rune::view::DirtyEntry>(&e, 1), 3, 8, file);

    const auto recs = rune::node::splitViewCheckpointRecords(file);
    ASSERT_EQ(recs.size(), 3u);
    EXPECT_EQ(recs[0].size(), v1Len);
    uint32_t magic; std::memcpy(&magic, recs[1].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2);

    auto torn = file; torn.resize(torn.size() - 2);
    EXPECT_EQ(rune::node::splitViewCheckpointRecords(torn).size(), 2u) << "torn trailing record dropped";
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_node_tests && ctest --test-dir build-fast -R 'MultiTableDeltaIsPersisted|SplitterReturnsV1AndV2' --output-on-failure`
Expected: both FAIL (agent never reaches O=12; splitter returns 1 record).

- [ ] **Step 3: Implement**

`libs/node/src/ViewSnapshotIoAgent.cpp` — replace `peekDeltaOffset`:

```cpp
constexpr std::size_t kViewHeaderLenV2 = 4 + 8 + 1; // magic,O,tableCount (multi-table v2)

// Peek O out of a v1 (single-table) or v2 (multi-table, aggregate views) record
// header — O is at byte 4 in both. No CRC/bounds validation here (see above).
int64_t peekDeltaOffset(const uint8_t* p, std::size_t len) noexcept {
    if (len < 4) return -1;
    const uint32_t magic = readLE32(p);
    if (magic == rune::view::kViewCheckpointMagic) {
        if (len < kViewHeaderLen + kViewCrcLen) return -1;
    } else if (magic == rune::view::kViewCheckpointMagicV2) {
        if (len < kViewHeaderLenV2 + kViewCrcLen) return -1;
    } else {
        return -1;
    }
    return static_cast<int64_t>(readLE64(p + 4));
}
```

`libs/node/src/WorkerViewSet.cpp` — replace the body of `splitViewCheckpointRecords` (keep its signature) with:

```cpp
std::vector<std::span<const uint8_t>>
splitViewCheckpointRecords(std::span<const uint8_t> bytes) {
    static constexpr std::size_t kEntryHdr = 4 + 1 + 8;  // rid u32 + flags u8 + key u64
    static constexpr std::size_t kCrc      = 4;
    const std::size_t total = bytes.size();

    // Walks `count` entries of `rowLen` from `p`; 0 on truncation, else the end.
    auto walkEntries = [&](std::size_t p, uint32_t count, uint32_t rowLen) -> std::size_t {
        for (uint32_t i = 0; i < count; ++i) {
            if (p + kEntryHdr > total) return 0;
            const uint8_t flags = bytes[p + 4];
            p += kEntryHdr;
            if (flags == 0) {
                if (p + rowLen > total) return 0;
                p += rowLen;
            }
        }
        return p;
    };
    // v1: [magic][O i64][rowLen u32][count u32] entries [crc]
    auto v1End = [&](std::size_t pos) -> std::size_t {
        if (pos + 20 > total) return 0;
        const std::size_t p = walkEntries(pos + 20, readU32LE(bytes.data() + pos + 16),
                                          readU32LE(bytes.data() + pos + 12));
        return (p == 0 || p + kCrc > total) ? 0 : p + kCrc;
    };
    // v2: [magic][O i64][tableCount u8] { [id u8][rowLen u32][count u32] entries }* [crc]
    auto v2End = [&](std::size_t pos) -> std::size_t {
        if (pos + 13 > total) return 0;
        const uint8_t tableCount = bytes[pos + 12];
        std::size_t p = pos + 13;
        for (uint8_t t = 0; t < tableCount; ++t) {
            if (p + 9 > total) return 0;
            const uint32_t rowLen = readU32LE(bytes.data() + p + 1);
            const uint32_t count  = readU32LE(bytes.data() + p + 5);
            p = walkEntries(p + 9, count, rowLen);
            if (p == 0) return 0;
        }
        return p + kCrc > total ? 0 : p + kCrc;
    };

    std::vector<std::span<const uint8_t>> out;
    std::size_t pos = 0;
    while (pos + 4 <= total) {
        const uint32_t magic = readU32LE(bytes.data() + pos);
        std::size_t end = 0;
        if (magic == rune::view::kViewCheckpointMagic)        end = v1End(pos);
        else if (magic == rune::view::kViewCheckpointMagicV2) end = v2End(pos);
        if (end == 0) break;   // unknown magic or truncated trailing record
        out.push_back(bytes.subspan(pos, end - pos));
        pos = end;
    }
    return out;
}
```

Ensure `WorkerViewSet.cpp` includes `<rune/view/ViewCheckpoint.h>` (for both magics).

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j --target rune_node_tests && ctest --test-dir build-fast -R 'ViewSnapshotIoAgent|WorkerViewSet|ViewRecovery|ViewReadiness' --output-on-failure`
Expected: all PASS (Phase 1 recovery tests unchanged).

- [ ] **Step 5: Commit**

```bash
git add libs/node/src/ViewSnapshotIoAgent.cpp libs/node/src/WorkerViewSet.cpp libs/node/tests/node/ViewSnapshotIoAgentTest.cpp libs/node/tests/node/WorkerViewSetTest.cpp
git commit -m "fix(view): persist and split v2 multi-table checkpoint records (aggregate state was dropped)"
```

---

### Task 7: `WorkerViewSet` — aggregate instances, fold dispatch, checkpoint, recovery

**Files:**
- Modify: `libs/node/include/rune/node/WorkerViewSet.h` (`AggInstance`, `ViewInstance::agg`, `driveFolds` dispatch)
- Modify: `libs/node/src/WorkerViewSet.cpp` (`getOrCreate_`, `buildAndPushCheckpoint_`, `loadRecoveryCheckpoint_`, `installViewForRecovery`)
- Test: `libs/node/tests/node/WorkerViewSetTest.cpp`

**Interfaces:**
- Consumes: `isAggregateView`, `buildAggLayout`, `kAggTable*` (Task 3); `foldAggregateBatch` (Task 4); `encodeMultiDelta`/`applyMultiDelta`/`replayMulti` (Task 5).
- Produces: `struct AggInstance { rune::view::AggLayout layout; rune::view::ViewTable contributors; rune::view::ViewTable groups; AggInstance(rune::view::AggLayout, std::size_t indexSlots, std::size_t chunkBytes); };` and `std::optional<AggInstance> ViewInstance::agg` (engaged iff `isAggregateView(plan)`). `getRow` is unchanged (it reads the output table).

- [ ] **Step 1: Write the failing tests** — append to `libs/node/tests/node/WorkerViewSetTest.cpp`:

```cpp
namespace {
// "order": {orderId Int64 (key), userId Int64, amount Int64} — the e2e shape.
rune::codec::Descriptor orderSrc() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", rune::codec::PrimType::Int64, 1, true)
        .add("userId", rune::codec::PrimType::Int64)
        .add("amount", rune::codec::PrimType::Int64)
        .build();
}
rune::view::ViewPlan userTotalsPlan(uint8_t persistence) {
    using P = rune::codec::PrimType; using R = rune::view::MergeRule;
    rune::view::ViewPlan v;
    v.viewName = "user_totals"; v.key = "userId"; v.sources = {{6, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"total", P::Int64, 0, "amount", R::Sum}};
    v.persistence = persistence;
    return v;
}
std::vector<uint8_t> orderValue(int64_t orderId, int64_t userId, int64_t amount) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(24); le16(1); le64(orderId); le64(userId); le64(amount);
    return b;
}
// One batch of (orderId, userId, amount) at `base`.
std::vector<uint8_t> orderBatch(int64_t base, std::vector<std::array<int64_t, 3>> recs) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    for (const auto& r : recs) {
        const auto k = keyBytes(r[0]);
        const auto v = orderValue(r[0], r[1], r[2]);
        b.append(asSv(k), asSv(v), 0);
    }
    return b.build();
}
} // namespace

TEST(WorkerViewSet, AggregateViewFoldsAndServesGroupRow) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(1), srcs, 0x40000009).value();
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-agg");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto batch = orderBatch(0, {{1, 100, 10}, {2, 100, 30}, {3, 200, 5}});
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 3}; };
    vs.driveFolds(6, 0, rr, 3, 0);

    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs.getRow(0x40000009, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 2);    // n
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 40);   // total
}

// REGRESSION (Review Focus #4): after recovery, an UPDATE to a pre-restart
// contributor must retract its pre-restart contribution — only possible if the
// contributor map (not just the output rows) was checkpointed and restored.
TEST(WorkerViewSetRegression, AggregateRecoveryRestoresContributorMap) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(0), srcs, 0x40000009).value();

    // 1. Fold on a persisted view and capture the checkpoint delta it pushes.
    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, "/tmp/rune-views-agg-rec");
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = orderBatch(0, {{1, 100, 10}, {2, 100, 30}});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 2}; };
    vs1.driveFolds(6, 0, rr1, 2, 0);

    std::vector<std::vector<uint8_t>> deltas;
    ch1.deltas.drain([&](int32_t, aeron::concurrent::AtomicBuffer& buf,
                         rune::node::OneToOneByteRing::index_t off,
                         rune::node::OneToOneByteRing::index_t len) {
        const uint8_t* p = buf.buffer() + off + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes;
        deltas.emplace_back(p, p + (len - rune::node::ViewSnapshotChannel::kDeltaHeaderBytes));
    });
    ASSERT_EQ(deltas.size(), 1u);
    uint32_t magic; std::memcpy(&magic, deltas[0].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2) << "aggregate views write v2";

    // 2. Recover into a fresh worker view set from that checkpoint.
    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, "/tmp/rune-views-agg-rec");
    const std::span<const uint8_t> recs[] = {deltas[0]};
    vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true, /*partition*/ 0,
                               /*base*/ {}, recs, /*committedTail*/ 2);
    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs2.getRow(0x40000009, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 40);

    // 3. Post-recovery update of contributor 1: 10 -> 25. Correct: total 55, n 2.
    //    A lost contributor map would give n 3, total 65.
    const auto b2 = orderBatch(2, {{1, 100, 25}});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b2.data(), b2.size(), 2, 3}; };
    vs2.driveFolds(6, 0, rr2, 3, 0);
    row = vs2.getRow(0x40000009, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 2);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 55);
}
```

(`keyBytes`/`asSv` already exist in this file's anonymous namespace; add `#include <array>`.)

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_node_tests && ctest --test-dir build-fast -R 'AggregateViewFoldsAndServes|AggregateRecoveryRestores' --output-on-failure`
Expected: FAIL — `n` reads 1 / total reads 30 (Phase 1 fold applies Sum as Replace) and the delta magic is v1.

- [ ] **Step 3: Implement**

`libs/node/include/rune/node/WorkerViewSet.h` — add includes `<optional>`, `<rune/view/AggFold.h>`, `<rune/view/AggState.h>`; add before `struct ViewInstance`:

```cpp
// Phase 2a: the two auxiliary tables of an aggregate (view, partition).
struct AggInstance {
    rune::view::AggLayout layout;        // declared first: the tables are built from it
    rune::view::ViewTable contributors;  // contributorKey -> pre-image (retraction)
    rune::view::ViewTable groups;        // groupKey -> accumulators
    AggInstance(rune::view::AggLayout l, std::size_t indexSlots, std::size_t chunkBytes)
        : layout(std::move(l)),
          contributors(layout.contributorRow, indexSlots, chunkBytes),
          groups(layout.groupRow, indexSlots, chunkBytes) {}
};
```

Inside `struct ViewInstance`, after `table`:

```cpp
    // Engaged iff isAggregateView(plan) (Phase 2a); empty for Replace views,
    // whose fold/checkpoint/recovery paths are unchanged.
    std::optional<AggInstance>         agg;
```

In `driveFolds`, replace these three consecutive statements — `const rune::view::FoldStats st = rune::view::foldBatch(...);`, `inst.foldCursor = cursor;`, and `work += static_cast<int>(st.applied + ...);` — with:

```cpp
        const rune::view::FoldStats st =
            inst.agg ? rune::view::foldAggregateBatch(inst.plan, inst.layout, inst.agg->layout,
                                                      inst.vdc, inst.table, inst.agg->contributors,
                                                      inst.agg->groups, view, bound, cursor)
                     : rune::view::foldBatch(inst.plan, inst.layout, inst.vdc, inst.table, view,
                                             bound, cursor);
        inst.foldCursor = cursor;
        work += static_cast<int>(st.applied + st.deleted + st.skippedControl +
                                 st.skippedMalformed + st.skippedNoGroup);
```

`libs/node/src/WorkerViewSet.cpp`:

(a) In `getOrCreate_`, immediately after `ViewInstance& inst = ins->second;` and **before** `if (tpl.underRecovery)`:

```cpp
    if (rune::view::isAggregateView(inst.plan)) {
        auto al = rune::view::buildAggLayout(inst.plan, inst.evolvingSrc);
        if (!al) {
            spdlog::error("WorkerViewSet: aggregate view {} partition {} not hosted — a compiled "
                          "aggregate references a field missing from its source descriptor",
                          viewTopicId, partition);
            views_.erase(ins);
            return nullptr;
        }
        inst.agg.emplace(std::move(*al), indexSlots, chunkBytes);
    }
```

(b) Add to the file's existing anonymous namespace (it already holds `readU32LE`/`deltaFilePath`; `ViewInstance` is complete there via the header):

```cpp
// Replays checkpoint records into one instance: v1 into the output table for a
// Replace view; v2 (all three tables, offset-atomic) for an aggregate view.
int64_t replayInstance(ViewInstance& inst, std::span<const uint8_t> base,
                       std::span<const std::span<const uint8_t>> deltas) {
    if (!inst.agg) return rune::view::replay(inst.table, inst.layout, base, deltas);
    rune::view::ViewTable* tables[3];
    tables[rune::view::kAggTableView]         = &inst.table;
    tables[rune::view::kAggTableContributors] = &inst.agg->contributors;
    tables[rune::view::kAggTableGroups]       = &inst.agg->groups;
    uint32_t lens[3];
    lens[rune::view::kAggTableView]         = inst.layout.rowLen;
    lens[rune::view::kAggTableContributors] = inst.agg->layout.contributorRow.rowLen;
    lens[rune::view::kAggTableGroups]       = inst.agg->layout.groupRow.rowLen;
    int64_t last = -1;
    if (!base.empty()) {
        const auto a = rune::view::applyMultiDelta(tables, lens, base);
        if (!a) return -1;
        last = a->O;
    }
    const int64_t r = rune::view::replayMulti(tables, lens, deltas);
    return r >= 0 ? r : last;
}
```

(c) In `loadRecoveryCheckpoint_`, replace the `rune::view::replay(...)` call with:

```cpp
    const int64_t recoveredO = replayInstance(
        inst, /*base=*/std::span<const uint8_t>{},
        std::span<const std::span<const uint8_t>>(records.data(), records.size()));
```

(d) In `installViewForRecovery`, replace `rune::view::replay(inst->table, inst->layout, base, deltasInOrder)` with `replayInstance(*inst, base, deltasInOrder)`.

(e) Replace `buildAndPushCheckpoint_` with:

```cpp
int WorkerViewSet::buildAndPushCheckpoint_(ViewInstance& inst) {
    dirtyEntries_.clear();
    rowOffsets_.clear();
    rowScratch_.clear();

    // Drain one table into the shared scratch (row bytes copied — drainDirty's
    // pointer is only valid inside the callback; pointers fixed up below).
    auto drainInto = [&](rune::view::ViewTable& t, uint32_t rowLen) {
        t.drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
            rune::view::DirtyEntry e;
            e.rid = rid; e.deleted = deleted; e.key = key; e.row = nullptr;
            if (!deleted && row != nullptr) {
                rowOffsets_.push_back(rowScratch_.size());
                rowScratch_.insert(rowScratch_.end(), row, row + rowLen);
            } else {
                rowOffsets_.push_back(SIZE_MAX);
            }
            dirtyEntries_.push_back(e);
        });
    };
    auto fixup = [&] {
        for (std::size_t i = 0; i < dirtyEntries_.size(); ++i) {
            if (rowOffsets_[i] != SIZE_MAX) dirtyEntries_[i].row = rowScratch_.data() + rowOffsets_[i];
        }
    };

    inst.pendingDelta.clear();
    if (!inst.agg) {
        drainInto(inst.table, inst.layout.rowLen);
        if (dirtyEntries_.empty()) return 0;
        fixup();
        rune::view::encodeDelta(dirtyEntries_, inst.foldCursor, inst.layout.rowLen, inst.pendingDelta);
    } else {
        // Aggregate view: output rows + contributor map + group accumulators in
        // ONE v2 record, so recovery always restores all three at the same O.
        std::size_t b[4];
        b[0] = 0;
        drainInto(inst.table, inst.layout.rowLen);                                    b[1] = dirtyEntries_.size();
        drainInto(inst.agg->contributors, inst.agg->layout.contributorRow.rowLen);    b[2] = dirtyEntries_.size();
        drainInto(inst.agg->groups, inst.agg->layout.groupRow.rowLen);                b[3] = dirtyEntries_.size();
        if (dirtyEntries_.empty()) return 0;
        fixup();
        auto sub = [&](int i) {
            return std::span<const rune::view::DirtyEntry>(dirtyEntries_.data() + b[i], b[i + 1] - b[i]);
        };
        const rune::view::TableDelta tds[3] = {
            {rune::view::kAggTableView, inst.layout.rowLen, sub(0)},
            {rune::view::kAggTableContributors, inst.agg->layout.contributorRow.rowLen, sub(1)},
            {rune::view::kAggTableGroups, inst.agg->layout.groupRow.rowLen, sub(2)}};
        rune::view::encodeMultiDelta(tds, inst.foldCursor, inst.pendingDelta);
    }
    inst.lastEncodedO = inst.foldCursor;   // attempted/encoded offset, NOT durable
    inst.pendingValid = true;
    (void)tryPushDelta_(inst);             // on back-pressure the delta is held for next pass
    return 1;
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j --target rune_node_tests && ctest --test-dir build-fast -R 'WorkerViewSet|ViewRecovery|ViewReadiness|GetViewRow' --output-on-failure`
Expected: all PASS, Phase 1 tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/WorkerViewSet.h libs/node/src/WorkerViewSet.cpp libs/node/tests/node/WorkerViewSetTest.cpp
git commit -m "feat(view): host aggregate views — fold dispatch, v2 checkpoint, aggregate recovery"
```

---

### Task 8: Co-partition gate at `CreateView` (+ wire error 36)

**Files:**
- Modify: `rune-protocol/include/rune/protocol/client/Protocol.h` (`ViewNotCoPartitioned = 36`)
- Modify: `libs/node/src/AdminWorker.cpp` (constant + check in `startCreateView_`)
- Test: `libs/node/tests/integration/CreateViewRegressionTest.cpp`

**Interfaces:**
- Consumes: `rv::isAggregateView` (Task 3).
- Produces: `CreateView` on an aggregate view over a source with `partitionCount > 1` whose group-by field (`sources[0].keyField`) differs from the source schema's `keyField` → error `36`.

- [ ] **Step 1: Write the failing tests** — append to `CreateViewRegressionTest.cpp`:

```cpp
namespace {
std::vector<uint8_t> orderBlob() {
    return serialize(DescriptorBuilder("order", 1)
        .add("orderId", PrimType::Int64, 1, /*isKey=*/true)
        .add("userId", PrimType::Int64)
        .add("amount", PrimType::Int64)
        .build());
}
std::vector<uint8_t> totalsPlanBlob(int32_t srcTopicId, const std::string& groupField) {
    rv::ViewPlan p;
    p.viewName = "totals_" + groupField; p.key = groupField; p.persistence = 0;
    p.sources.push_back(rv::SourceSpec{srcTopicId, groupField});
    p.output.push_back(rv::AuthoredField{groupField, PrimType::Int64, 0, groupField, rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"total", PrimType::Int64, 0, "amount", rv::MergeRule::Sum});
    return rv::serializeAuthored(p);
}
} // namespace

TEST_F(CreateViewRegression, AggregateOverMultiPartitionSourceIsNotCoPartitioned) {
    DefineTopicMsg d;
    d.session = 1; d.corr = 30; d.partitionCount = 2; d.name = "order";
    d.keyField = "orderId"; d.validate = false; d.blob = orderBlob();
    driveDefine(d);
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t src = lastDefineTopicId_;

    CreateViewMsg cv;
    cv.session = 1; cv.corr = 31; cv.persistence = 0;
    cv.viewName = "totals_userId"; cv.keyField = "userId"; cv.planBlob = totalsPlanBlob(src, "userId");
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, 36u) << "group-by userId over a 2-partition source keyed by orderId";

    // Grouping by the source's own key field is co-partitioned by construction.
    lastViewError_ = 0xffff;
    cv.corr = 32; cv.viewName = "totals_orderId"; cv.keyField = "orderId";
    cv.planBlob = totalsPlanBlob(src, "orderId");
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::OK));
}

TEST_F(CreateViewRegression, AggregateOverSinglePartitionSourceIsAllowed) {
    DefineTopicMsg d;
    d.session = 1; d.corr = 40; d.partitionCount = 1; d.name = "order1";
    d.keyField = "orderId"; d.validate = false; d.blob = orderBlob();
    driveDefine(d);
    ASSERT_EQ(lastDefineError_, 0u);
    CreateViewMsg cv;
    cv.session = 1; cv.corr = 41; cv.persistence = 0;
    cv.viewName = "totals_userId"; cv.keyField = "userId";
    cv.planBlob = totalsPlanBlob(lastDefineTopicId_, "userId");
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::OK));
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R 'CreateViewRegression' --output-on-failure`
Expected: `AggregateOverMultiPartitionSourceIsNotCoPartitioned` FAILS (error is 0, not 36).

- [ ] **Step 3: Implement**

`rune-protocol/include/rune/protocol/client/Protocol.h` after `ViewKeyInvalid = 35`:

```cpp
    ViewNotCoPartitioned   = 36,  // CreateView: an aggregate view's group-by field is not the source's
                                  // partitioning key and the source has >1 partition (Phase 2a)
```

`libs/node/src/AdminWorker.cpp` — after the `kViewKeyInvalid` constant:

```cpp
static constexpr uint16_t kViewNotCoPartitioned = 36;  // aggregate group-by not co-partitioned
```

Add `#include <rune/view/AggState.h>`. In `startCreateView_`, immediately **after** the block that handles `if (!compiled)` (the CompileError → wire-code classification) and **before** the source-partition lookup, insert:

```cpp
    // Phase 2a co-partitioning: an aggregate view groups by a VALUE field, but
    // records are partitioned by their wire key — every contributor of a group
    // is only guaranteed to land on one partition when the source has exactly
    // one, or when the group-by field IS the source's declared key field.
    if (rv::isAggregateView(compiled->plan)) {
        const RegistryEntry* coSrc = reg_.byId(authored->sources[0].topicId);
        if (coSrc != nullptr && coSrc->partitionCount > 1 &&
            authored->sources[0].keyField != coSrc->keyField) {
            deps_.respondCreateView(msg.session, msg.corr, -1, 0, kViewNotCoPartitioned);
            return;
        }
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R 'CreateViewRegression|GetViewRow' --output-on-failure`
Expected: all PASS.

- [ ] **Step 5: Commit** (two repos)

```bash
git -C ../rune-protocol add include/rune/protocol/client/Protocol.h
git -C ../rune-protocol commit -m "feat(protocol): ViewNotCoPartitioned = 36"
git add libs/node/src/AdminWorker.cpp libs/node/tests/integration/CreateViewRegressionTest.cpp
git commit -m "feat(view): reject non-co-partitioned aggregate views at CreateView (error 36)"
```

---

### Task 9: §14.0 latency gate for the aggregate fold

**Files:**
- Modify: `benchmark/ViewFoldBench.cpp`

**Interfaces:**
- Consumes: `foldAggregateBatch`, `buildAggLayout` (Tasks 3–4).

- [ ] **Step 1: Add the benchmark.** Copy `BM_ViewFoldReplace` (the whole function, `benchmark/ViewFoldBench.cpp:192` through its closing brace and its `BENCHMARK(...)` registration) into a new `BM_ViewFoldAggregate`, then make exactly these edits in the copy:
  1. Source descriptor: `{orderId Int64 (key), userId Int64, amount Int64}`; plan `user_totals` grouped by `userId` with outputs `userId Replace`, `n CountAll Int64`, `total Sum(amount) Int64`, `avg Avg(amount) Float64` (same shape as Task 7's `userTotalsPlan` plus `avg`).
  2. After compiling: `auto agg = rune::view::buildAggLayout(comp.plan, src).value(); rune::view::ViewTable contributors(agg.contributorRow), groups(agg.groupRow);`.
  3. Workload: contributor id `c` drawn from the existing churn generator over `[0, 1'000'000)`; group = `c % 10'000`; value encoded as `[24][1][c][c % 10'000][amount]` with the existing RNG's amount; deletes from the generator stay tombstones. Every update therefore retracts + re-adds (and periodically moves groups when the generator re-picks `c` with a new amount — keep group fixed per `c` so retract+add of the same group is the dominant path, which is the realistic hot path).
  4. Replace `foldBatch(comp.plan, comp.rowLayout, vdc, table, view, ...)` with `rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, table, contributors, groups, view, ...)`.
  5. Prefix every reported counter with `agg_` (`agg_record_p99_us`, `agg_batch_p99_us`, ...). Keep the same gates: `agg_record_p99_us < 5`, `agg_batch_p99_us < 10`, reported the same way the Replace bench reports its gate (PASS/FAIL line).

- [ ] **Step 2: Run it**

Run: `cmake --build build-fast -j --target rune_broker_benchmark && ./build-fast/rune_broker_benchmark --benchmark_filter='ViewFold'` (the `rune_broker_benchmark` target builds `benchmark/ViewFoldBench.cpp`; if the binary is not at `build-fast/`, `find build-fast -name rune_broker_benchmark -type f`).
Expected: `agg_record_p99_us < 5` and `agg_batch_p99_us < 10` — gate PASS. If FAIL, profile before changing anything (the fold is O(fields) per record with three table probes; a fail means an accidental allocation or a probe regression).

- [ ] **Step 3: Commit**

```bash
git add benchmark/ViewFoldBench.cpp
git commit -m "bench(view): §14.0 gate for the aggregate fold (record p99 < 5us, batch p99 < 10us)"
```

---

### Task 10: e2e — aggregate view across a broker restart

**Files:**
- Modify: `rune-client-cpp/examples/view_probe.cpp`
- Modify: `docker/view-engine-e2e.sh`

**Interfaces:**
- Consumes: everything above, on the real Aeron broker.
- Produces: setup handoff lines `VIEW_ORDER_TOPIC_ID`, `VIEW_TOPIC_ID_AGG`, `VIEW_FP_AGG`; verify leg checks the aggregate before and after a post-restart update.

Scenario (`order` = `{orderId Int64 key, userId Int64, amount Int64}`, 1 partition; `user_totals` = `userId`, `n` Count(\*), `total` Sum(amount), `avg` Avg(amount)):
- setup: produce `o1(u100,10)`, `o2(u100,30)`, `o3(u200,5)`, then `o1→(u100,50)` (update), then `o3→(u100,5)` (group move). Expect `u100 = {n 3, total 85, avg 85/3}`, `u200` AbsentKey.
- restart.
- verify: `u100 = {3, 85, 85/3}` recovered; `u200` AbsentKey; then update `o3→(u100,25)` → `u100 = {3, 105, 35}`. (A lost contributor map would give `n 4, total 110`.)

- [ ] **Step 1: Add probe helpers** (in `view_probe.cpp`, next to `characterDescriptor`):

```cpp
static rune::codec::Descriptor orderDescriptor() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("userId", rune::codec::PrimType::Int64)
        .add("amount", rune::codec::PrimType::Int64)
        .build();
}
static std::vector<uint8_t> encodeOrder(int64_t orderId, int64_t userId, int64_t amount) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(24); le16(1); le64(orderId); le64(userId); le64(amount);
    return b;
}
static rcv::ViewPlan userTotalsPlan(int32_t srcTopicId) {
    using P = rune::codec::PrimType;
    rcv::ViewPlan p;
    p.viewName = "user_totals"; p.key = "userId"; p.persistence = 0;
    p.sources.push_back(rcv::SourceSpec{srcTopicId, "userId"});
    p.output.push_back(rcv::AuthoredField{"userId", P::Int64, 0, "userId", rcv::MergeRule::Replace});
    p.output.push_back(rcv::AuthoredField{"n", P::Int64, 0, "", rcv::MergeRule::CountAll});
    p.output.push_back(rcv::AuthoredField{"total", P::Int64, 0, "amount", rcv::MergeRule::Sum});
    p.output.push_back(rcv::AuthoredField{"avg", P::Float64, 0, "amount", rcv::MergeRule::Avg});
    return p;
}
static rcv::RowLayout userTotalsRowLayout() {
    const rune::codec::PrimType types[] = {rune::codec::PrimType::Int64, rune::codec::PrimType::Int64,
                                           rune::codec::PrimType::Int64, rune::codec::PrimType::Float64};
    return rcv::buildRowLayout(types);
}
// Sends one order (acked); false + message on failure.
static bool sendOrder(Client& c, const TopicHandle& th, int64_t o, int64_t u, int64_t amt, int timeoutMs) {
    ProducerConfig pcfg; pcfg.acks = Acks::Leader; pcfg.produceTimeoutMs = timeoutMs;
    auto p = c.producer(pcfg);
    Completions cq(c); Completion done;
    const auto v = encodeOrder(o, u, amt);
    if (!sendAndWaitAck(cq, p, th, Key(static_cast<uint64_t>(o)), std::span<const uint8_t>(v), 1, timeoutMs, done) ||
        done.status != OpStatus::Ok) {
        std::cerr << "PROBE: FAIL produce(order " << o << ") status=" << static_cast<int>(done.status) << "\n";
        return false;
    }
    return true;
}
// Polls user `u` until {n, total, avg} match (avg within 1e-9) or the bound expires.
static bool waitForTotals(Client& c, int32_t viewId, uint64_t fp, int64_t u, int64_t n, int64_t total,
                          double avg, int boundMs, int timeoutMs, const char* who) {
    const rcv::RowLayout L = userTotalsRowLayout();
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    ViewRowResult r;
    for (;;) {
        r = c.getViewRowAndWait(viewId, static_cast<uint64_t>(u), fp, std::chrono::milliseconds(timeoutMs));
        if (r.status == ViewRowResult::kOk && r.rowBytes.size() == L.rowLen) {
            const auto row = std::span<const uint8_t>(r.rowBytes);
            const auto nf = rcv::readSlot(row, L, 1), tf = rcv::readSlot(row, L, 2), af = rcv::readSlot(row, L, 3);
            if (nf.i == n && tf.i == total && std::abs(af.f - avg) < 1e-9) {
                std::cerr << "PROBE: " << who << " OK n=" << n << " total=" << total << " avg=" << avg << "\n";
                return true;
            }
            if (std::chrono::steady_clock::now() >= dl) {
                std::cerr << "PROBE: FAIL " << who << " got n=" << nf.i << " total=" << tf.i << " avg=" << af.f
                          << " want n=" << n << " total=" << total << " avg=" << avg << "\n";
                return false;
            }
        } else if (std::chrono::steady_clock::now() >= dl) {
            std::cerr << "PROBE: FAIL " << who << " status=" << r.status << "\n";
            return false;
        }
        std::this_thread::sleep_for(50ms);
    }
}
// Polls until user `u` reads AbsentKey (group emptied) or the bound expires.
static bool waitForAbsent(Client& c, int32_t viewId, uint64_t fp, int64_t u, int boundMs, int timeoutMs,
                          const char* who) {
    const ViewRowResult r = waitResolved(c, viewId, static_cast<uint64_t>(u), fp, boundMs, timeoutMs);
    if (r.status != ViewRowResult::kAbsentKey) {
        std::cerr << "PROBE: FAIL " << who << " status=" << r.status << " (want AbsentKey)\n";
        return false;
    }
    std::cerr << "PROBE: " << who << " correctly AbsentKey\n";
    return true;
}
```

Add `#include <cmath>` if absent.

- [ ] **Step 2: Setup leg** — in `mode == "setup"`, after the char views are verified (after `decodeAndCheck(rt, ...)`) and **before** the handoff block, insert:

```cpp
            // ---- 5. Phase 2a: aggregate view user_totals over 'order' -------
            const auto oblob = rune::codec::serialize(orderDescriptor());
            DefineTopicResult ord = c.defineTopicAndWait("order", /*partitions=*/1, /*keyField=*/"orderId",
                                                         /*validate=*/false, std::span<const uint8_t>(oblob), wait);
            if (ord.errorCode != 0) { std::cerr << "PROBE: FAIL defineTopic(order) errorCode=" << ord.errorCode << "\n"; return 30; }
            const auto aggBlob = rcv::serializeAuthored(userTotalsPlan(ord.topicId));
            CreateViewResult viewA = c.createViewAndWait("user_totals", "userId", /*persistence=*/0,
                                                         std::span<const uint8_t>(aggBlob), wait);
            if (viewA.errorCode != 0) { std::cerr << "PROBE: FAIL createView(user_totals) errorCode=" << viewA.errorCode << "\n"; return 31; }
            const TopicHandle ordTh{ord.topicId, 1};
            if (!sendOrder(c, ordTh, 1, 100, 10, timeoutMs) || !sendOrder(c, ordTh, 2, 100, 30, timeoutMs) ||
                !sendOrder(c, ordTh, 3, 200, 5, timeoutMs)  || !sendOrder(c, ordTh, 1, 100, 50, timeoutMs) ||
                !sendOrder(c, ordTh, 3, 100, 5, timeoutMs)) return 32;
            if (!waitForTotals(c, viewA.viewTopicId, viewA.outputFingerprint, 100, 3, 85, 85.0 / 3,
                               boundMs, timeoutMs, "user_totals u100 (initial)")) return 33;
            if (!waitForAbsent(c, viewA.viewTopicId, viewA.outputFingerprint, 200, boundMs, timeoutMs,
                               "user_totals u200 (emptied by group move)")) return 34;
```

and add to the handoff block (before `VIEW-PROBE-SETUP: OK`):

```cpp
            std::cout << "VIEW_ORDER_TOPIC_ID=" << ord.topicId              << "\n";
            std::cout << "VIEW_TOPIC_ID_AGG="   << viewA.viewTopicId        << "\n";
            std::cout << "VIEW_FP_AGG="         << viewA.outputFingerprint  << "\n";
```

(The handoff block references `ord`/`viewA`, so both must stay in scope — place step 5 at the same block level as steps 1–4.)

- [ ] **Step 3: Verify leg** — in `mode == "verify_restart"`, read the env vars next to the others:

```cpp
            const int32_t  orderTopic = static_cast<int32_t>(getEnvI64("RUNE_VIEW_ORDER_TOPIC_ID", -1));
            const int32_t  viewIdA    = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TOPIC_ID_AGG", -1));
            const uint64_t fpA        = getEnvU64("RUNE_VIEW_FP_AGG", 0);
```

and, after the char legs pass (after the `char_transient (live after restart)` check) and before `VIEW-ENGINE-E2E: OK`, insert:

```cpp
            // ---- (d) aggregate view recovered: rows AND contributor map --------
            if (orderTopic < 0 || viewIdA < 0) { std::cerr << "PROBE: FAIL missing aggregate handoff env\n"; return 40; }
            if (!waitForTotals(c, viewIdA, fpA, 100, 3, 85, 85.0 / 3, boundMs, timeoutMs,
                               "user_totals u100 (post-restart, recovered)")) return 41;
            if (!waitForAbsent(c, viewIdA, fpA, 200, boundMs, timeoutMs, "user_totals u200 (post-restart)")) return 42;
            // Update a PRE-restart contributor: o3 5 -> 25. Correct only if its
            // pre-image survived the restart (lost map => n 4, total 110).
            if (!sendOrder(c, TopicHandle{orderTopic, 1}, 3, 100, 25, timeoutMs)) return 43;
            if (!waitForTotals(c, viewIdA, fpA, 100, 3, 105, 35.0, boundMs, timeoutMs,
                               "user_totals u100 (post-restart update retracts pre-image)")) return 44;
```

- [ ] **Step 4: e2e script handoff** — in `docker/view-engine-e2e.sh`, after `VIEW_HP_INITIAL="$(extract VIEW_HP_INITIAL)"`:

```bash
VIEW_ORDER_TOPIC_ID="$(extract VIEW_ORDER_TOPIC_ID)"
VIEW_TOPIC_ID_AGG="$(extract VIEW_TOPIC_ID_AGG)"
VIEW_FP_AGG="$(extract VIEW_FP_AGG)"
```

add `VIEW_ORDER_TOPIC_ID VIEW_TOPIC_ID_AGG VIEW_FP_AGG` to the `for v in ...` presence check, and in the `run_client verify_restart` call add:

```bash
    -e RUNE_VIEW_ORDER_TOPIC_ID="${VIEW_ORDER_TOPIC_ID}" \
    -e RUNE_VIEW_TOPIC_ID_AGG="${VIEW_TOPIC_ID_AGG}" \
    -e RUNE_VIEW_FP_AGG="${VIEW_FP_AGG}" \
```

Add to the final PASSED banner: `log "  -> user_totals (Count(*)/Sum/Avg) recovered + post-restart update retracts OK"`.

- [ ] **Step 5: Run the e2e on the VM** (honor `/root/vm.lock`: claim it with an owner marker, release after):

```bash
# from the broker worktree: stage broker, protocol, client (git archive HEAD of each) into
# /root/view-engine-e2e/<repo> on root@178.104.16.102 exactly as the Phase 1 e2e runs did, then:
ssh root@178.104.16.102 'cd /root/view-engine-e2e/rune-broker && bash docker/view-engine-e2e.sh'
```
Expected: `VIEW-ENGINE-E2E: OK`, `SCRIPT_EXIT=0`, including the four `user_totals` PROBE lines.

- [ ] **Step 6: Commit** (two repos)

```bash
git -C ../rune-client-cpp add examples/view_probe.cpp
git -C ../rune-client-cpp commit -m "test(view-probe): aggregate view across restart (contributor map recovery)"
git add docker/view-engine-e2e.sh
git commit -m "test(view): e2e covers an aggregate view across a broker restart"
```

---

## Finish

- [ ] Full `*Regression*` suite on the final tree: `ctest --test-dir build-fast -R Regression` → 0 failures (CLAUDE.md #7).
- [ ] View suites: `ctest --test-dir build-fast -R 'View|PlanCompiler|Agg|GetViewRow|CreateView'` → 0 failures.
- [ ] `BM_ViewFoldAggregate` gate PASS; Phase 1 `BM_ViewFoldReplace`/`ViewTableBench`/`ViewCheckpointBench` gates still PASS.
- [ ] e2e on the VM green (Task 10).
- [ ] Fast-forward `feature/materialized-views` in broker, protocol, client to `feat/view-engine-phase2a`.
