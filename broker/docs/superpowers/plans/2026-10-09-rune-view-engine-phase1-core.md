# Rune View Engine — Phase 1 (core single-source engine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the broker-local materialized-view engine for the single-source case — define a view over one typed topic, maintain it incrementally as records commit (`Replace` upsert + tombstone delete), persist it with a differential checkpoint, point-read it by key, and recover it deterministically on restart.

**Architecture:** A new `libs/view` library holds the shared-nothing engine primitives (row layout, the index+chunked-slab `ViewTable`, the plan compiler, the fold, and the differential-checkpoint codec). The owning `rune::node::Worker` folds its own view partitions in its duty cycle (next to `rcAdvanceAll_`), marking dirty rows and handing row-sized deltas to a dedicated per-node `ViewSnapshotIoAgent` over a `OneToOneByteRing`. `CreateView` mirrors the existing `DefineTopic` DDL chain through `AdminWorker` → `TopicRegistry::createTopic`; the compiled plan is fanned out to the owning worker exactly like a `SchemaUpdateMsg`. `GetViewRow` mirrors the `PushRead` op/return plumbing. A readiness gate (`RECOVERING → REBUILDING_VIEWS → READY`) keeps a node out of elections and reads until its views are replayed to the committed tail.

**Tech Stack:** C++23, CMake 3.31 (no presets; Conan toolchain), GoogleTest, Google Benchmark, Aeron transport, `rune-codec` (durable-tag descriptors), `rune-protocol` (wire messages). Cross-thread only via the existing `OneToOneByteRing`/`ControlRing` + `Agent`/`AgentRunner`.

**Spec:** `docs/superpowers/specs/2026-10-08-rune-view-engine-single-source-design.md` (read it alongside this plan — the plan argues from it). Prerequisite SHIPPED: `docs/superpowers/specs/2026-10-08-rune-schema-evolution-durable-tags-design.md`.

**Scope boundary (Phase 1 only):** single source; `Replace` merge + tombstone delete; fixed-width output columns only. **Deferred to later phases** (own plans): aggregates `Count/Sum/Avg/Min/Max/Last` + retraction + null-aggregation semantics (Phase 2); co-partitioned joins §8 (Phase 3); ack/consistency produce-path changes, `sendAndWait`/`afterViews`, flow-control/backpressure (Phase 4); `ALTER VIEW` live rebuild (§15 #13 immediately-following increment); multi-node `__views` replication (§16).

## Global Constraints

Copied verbatim from the spec; every task's requirements implicitly include these.

- **Hot-path latency gates (§14.0):** fold→merge pipeline **< 5 µs / record** (p99); view update **< 10 µs / batch** (p99); point select single-node **< 50 µs** (p99). Measured ABAB, n ≥ 15, report p50 **and** p99; p99 over budget is a failing gate.
- **Storage gate (§7.1):** worst per-op rehash tax **< 50 µs, flat across 64/256/1024 B rows**; delete O(1) tombstone-free (p99 flat across row sizes, no payload move); point-find cache-dense (fat rows out of the probe path).
- **Checkpoint gate (§9.2):** **row-sized dirty units** (NOT OS-page — 4 KB chunks were a 4–16× write-amplification failing config); flush trigger `min(~1–2 s interval, dirtyCap)`, both configurable; reads with checkpointing ON stay on the OFF baseline **through p99.9**; **no full-view copy, no COW, no memory doubling**; **offset-atomic** (recovered offset == exactly the last fsync'd delta's `O`); node disk bandwidth ≥ aggregate dirty rate.
- **Zero steady-state heap allocation (#4)** on the fold and read hot paths — assert via the benchmark `g_allocCount`/`g_allocEnabled` counter in `benchmark/BenchmarkMain.cpp`.
- **Shared-nothing (#1), rings-only (#2):** **NO `std::atomic`, mutex, lock, or `thread_local` anywhere under `libs/**`** — the `scripts/check_no_atomics.py` ctest guard (`NoAtomicsOrMutexesOutsideAllowlistRegression`) fails the build otherwise. All cross-thread passing uses the existing `rune::node::OneToOneByteRing` / `rune::consensus::ControlRing` + `rune::consensus::Agent`/`AgentRunner`. Each view partition's state is owned outright by one worker.
- **Field identity = durable `(topicId, tag)`**, never name/position. Per-record decode uses `rune::codec::readField(materializeAsOf(evolvingDescriptor, writerVersion), record, tag)` — **there is no `readFieldByTag` symbol**; `materializeAsOf` + `readField(desc, rec, fieldId)` is the pattern.
- **Phase 1 column types are fixed-width only:** `Int32/Int64/UInt16/UInt32/UInt64/Float64/Bool/Enum/FixedStr`. Var-len `String`/`Bytes` columns are **rejected at `CreateView`** (there is no `VarChar(N)` codec type). The fixed-width row carries a **null bitmap** (absence is explicit, §3.1/§6.4).
- **Phase 1 view KEY must be an integer type** (`Int32/Int64/UInt16/UInt32/UInt64`), read from the record *value's* key field and folded to a `uint64_t` (signed → bit-cast, unsigned → zero-extend) — the exact value, not a hash, so the `ViewTable` index compares keys exactly with no collision risk. `FixedStr`/composite/multi-field keys are **deferred** to a later phase (they need the index to store full key bytes + row-key equality compare). The fold derives the key from `readField(value, keyFieldId)`, **not** from the wire record-key (`forEachRecord`'s first arg), which is the separate produce-routing/compaction key.
- **Views default `persisted`; `transient` is opt-in** (no checkpoint IO, rebuilt by replay). DDL verb is **`CREATE`** (`CreateView`); default `MergeRule` = `Replace`; the key field must appear in `output`.
- **Determinism (#16):** the fold is a pure function of the committed log prefix; a replica and the leader converge to the identical view table byte-for-byte at the same offset.
- **Deterministic tests (CLAUDE.md §4):** inject a `CachedNanoClock`; advance time + call `doWork()` once per expected transition under a hard iteration cap; never wall-clock sleeps, never `while(cond) doWork()`.
- **Regression (#6/#7):** every fix gets a gtest suite whose name ends in `Regression`; `ctest --test-dir build-fast -R Regression --output-on-failure` runs first, before any other suite.
- **Build/test:** `cmake --build build-fast -j [--target rune_view_tests]`; `ctest --test-dir build-fast -R '<regex>' --output-on-failure`. Benchmarks need `-DBUILD_BENCHMARKS=ON`.
- **Live dashboard (CLAUDE.md §1–3):** after a suite goes green, push `scripts/rune-live.sh test "rune-broker/<suite>" passed passed=N failed=0 total=N commit=$(git rev-parse --short HEAD)` (benchmarks add `kind=benchmark p99Us=..`). Do this once per task at the final commit, not per micro-step.

## Review Focus

Input classes the spec implies but a task's happy-path tests may miss — each is pinned to the owning task's tests.

1. **A record whose writer version DROPPED a view-projected field** → that view column must go **null** via the bitmap, row width **unchanged**, fold must not wedge (§6.3). *Pinned in Task 11 (fold) — `FoldDroppedSourceFieldGoesNull`.*
2. **`CreateView` naming a field the source schema does not declare, a duplicate-resolving name, or a var-len / non-existent key field** → rejected at define time with a typed error, never a half-created view (§6.0, §6.3 key-drop edge). *Pinned in Task 9 (compiler) — `CompileRejectsUnknownAndVarLenAndMissingKey`.*
3. **A rehash triggered mid-fold by a write burst before the next index was pre-provisioned** → bounded inline fallback, correct (no lost/duplicated rows), worst per-op tax still measured < 50 µs (§7). *Pinned in Task 5 (rehash) — `RehashFallbackWhenPreprovisionNotReady` + Task 7 bench.*
4. **A crash between the fold advancing its cursor and the delta fsync** → on restart the recovered applied offset is **exactly** the last fsync'd delta's `O`, and replay of `(O, LSO]` reproduces the byte-identical view (offset-atomic, idempotent `Replace`) (§9.2/§9.3). *Pinned in Task 13 (checkpoint codec) — `CheckpointReplayIsOffsetAtomicAndByteIdentical`.*
5. **A `GetViewRow` for an absent key, or for a view still `REBUILDING_VIEWS`** → returns a typed **absent** / a **retriable not-ready**, never a half-built row or a stale value (§9.4.1/§12). *Pinned in Task 17 (readiness) — `GetViewRowNotReadyIsRetriable` — and Task 21 (read path) — `GetViewRowAbsentKeyReturnsTypedAbsent`.*

---

## Task 1: Scaffold `libs/view` + type helpers

**Files:**
- Create: `libs/view/CMakeLists.txt`
- Create: `libs/view/include/rune/view/ViewTypes.h`
- Create: `libs/view/tests/view/ViewTypesTest.cpp`
- Modify: `CMakeLists.txt` (top-level) — add `add_subdirectory(libs/view)` next to the other `libs/*` entries.

**Interfaces:**
- Produces: `rune::view::MergeRule` (`enum class : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last }`); `bool rune::view::isNumeric(rune::codec::PrimType) noexcept`; `bool rune::view::isFixedWidthColumn(rune::codec::PrimType) noexcept`; `uint16_t rune::view::kViewsTopicId` later tasks reference. Links `rune_codec` for `PrimType`.

- [ ] **Step 1: Write the failing test**

```cpp
// libs/view/tests/view/ViewTypesTest.cpp
#include <gtest/gtest.h>
#include <rune/view/ViewTypes.h>
using namespace rune::view;
using rune::codec::PrimType;

TEST(ViewTypes, NumericPredicate) {
    EXPECT_TRUE(isNumeric(PrimType::Int32));
    EXPECT_TRUE(isNumeric(PrimType::Float64));
    EXPECT_FALSE(isNumeric(PrimType::Bool));
    EXPECT_FALSE(isNumeric(PrimType::String));
    EXPECT_FALSE(isNumeric(PrimType::FixedStr));
}
TEST(ViewTypes, FixedWidthColumnPredicate) {
    EXPECT_TRUE(isFixedWidthColumn(PrimType::Int64));
    EXPECT_TRUE(isFixedWidthColumn(PrimType::FixedStr));
    EXPECT_TRUE(isFixedWidthColumn(PrimType::Bool));
    EXPECT_FALSE(isFixedWidthColumn(PrimType::String)); // var-len: rejected in Phase 1
    EXPECT_FALSE(isFixedWidthColumn(PrimType::Bytes));
}
TEST(ViewTypes, IntegerKeyPredicate) {
    EXPECT_TRUE(isIntegerKeyType(PrimType::Int64));
    EXPECT_TRUE(isIntegerKeyType(PrimType::UInt32));
    EXPECT_FALSE(isIntegerKeyType(PrimType::Float64)); // Phase 1 keys are integers
    EXPECT_FALSE(isIntegerKeyType(PrimType::FixedStr)); // deferred
}
```

- [ ] **Step 2: Write `ViewTypes.h`**

```cpp
// libs/view/include/rune/view/ViewTypes.h
#pragma once
#include <cstdint>
#include <rune/codec/PrimType.h>
namespace rune::view {
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last };
inline bool isNumeric(rune::codec::PrimType t) noexcept {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: case P::Int64: case P::UInt16:
        case P::UInt32: case P::UInt64: case P::Float64: return true;
        default: return false; // Enum is categorical; Bool/String/Bytes/FixedStr not numeric
    }
}
inline bool isFixedWidthColumn(rune::codec::PrimType t) noexcept {
    return !rune::codec::isVarLen(t); // String/Bytes are the only var-len types
}
inline bool isIntegerKeyType(rune::codec::PrimType t) noexcept {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: case P::Int64: case P::UInt16:
        case P::UInt32: case P::UInt64: return true;   // Phase 1 keys fold to uint64_t
        default: return false;                          // Float64/Bool/Enum/FixedStr/String/Bytes deferred
    }
}
} // namespace rune::view
```

- [ ] **Step 3: Write `libs/view/CMakeLists.txt`** (mirror `libs/storage/CMakeLists.txt`)

```cmake
add_library(rune_view INTERFACE)   # header-only until Task 2 adds a .cpp; switch to STATIC then
target_include_directories(rune_view INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${RUNE_PROTOCOL_INCLUDE})
target_link_libraries(rune_view INTERFACE rune_codec)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_view_tests tests/view/ViewTypesTest.cpp)
    target_link_libraries(rune_view_tests PRIVATE rune_view rune_codec GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_view_tests)
endif()
```

Add `add_subdirectory(libs/view)` to the top-level `CMakeLists.txt` after `add_subdirectory(libs/storage)`.

- [ ] **Step 4: Configure + build + run**

Run: `cmake --build build-fast -j --target rune_view_tests && ctest --test-dir build-fast -R '^ViewTypes\.' --output-on-failure`
Expected: 2 tests PASS. (If CMake needs re-configuring for the new subdir: re-run the configure command from the repo README with `-DBUILD_TESTING=ON` into `build-fast`.)

- [ ] **Step 5: Commit**

```bash
git add libs/view CMakeLists.txt
git commit -m "feat(view): scaffold libs/view with MergeRule + type predicates"
```

---

## Task 2: Fixed-width row layout + null bitmap (`ViewRow`)

**Files:**
- Create: `libs/view/include/rune/view/ViewRow.h`
- Create: `libs/view/src/ViewRow.cpp`
- Create: `libs/view/tests/view/ViewRowTest.cpp`
- Modify: `libs/view/CMakeLists.txt` — switch `rune_view` to `STATIC` with `src/ViewRow.cpp`; add the test file to `rune_view_tests`.

**Interfaces:**
- Consumes: `rune::codec::Descriptor`, `rune::codec::FieldValue`, `rune::codec::PrimType`, `fieldWidth()` (from `rune/codec/Descriptor.h`).
- Produces:
  - `struct rune::view::RowLayout { uint32_t rowLen; uint32_t bitmapBytes; uint32_t dataOffset; std::vector<uint32_t> slotOffset; std::vector<rune::codec::PrimType> slotType; std::vector<uint16_t> slotWidth; };`
  - `RowLayout rune::view::buildRowLayout(const rune::codec::Descriptor& outDesc);` — throws `std::invalid_argument` if any field is var-len (Phase 1). Bitmap first (one bit per field, `ceil(nFields/8)` bytes), then fixed slots in field order; `rowLen = bitmapBytes + Σ slotWidth`.
  - `void rune::view::setNull(uint8_t* row, const RowLayout&, uint16_t slot) noexcept;` / `bool isNull(const uint8_t* row, const RowLayout&, uint16_t slot) noexcept;` / `void clearNull(...)`.
  - `void rune::view::writeSlot(uint8_t* row, const RowLayout&, uint16_t slot, rune::codec::FieldValue v) noexcept;` — writes the value and clears the null bit; if `!v.present`, sets the null bit and leaves bytes zero.
  - `rune::codec::FieldValue rune::view::readSlot(const uint8_t* row, const RowLayout&, uint16_t slot) noexcept;` — returns `present=false` when the null bit is set.

- [ ] **Step 1: Write the failing test**

```cpp
// libs/view/tests/view/ViewRowTest.cpp
#include <gtest/gtest.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>
using namespace rune::view;
using rune::codec::DescriptorBuilder; using rune::codec::PrimType; using rune::codec::FieldValue;

static rune::codec::Descriptor twoColDesc() {
    return DescriptorBuilder("v", 1)
        .add("id", PrimType::Int64, 1, /*isKey*/true)
        .add("hp", PrimType::Int32).build();
}

TEST(ViewRow, LayoutWidthAndRoundTrip) {
    auto d = twoColDesc();
    RowLayout L = buildRowLayout(d);
    EXPECT_EQ(L.bitmapBytes, 1u);           // 2 fields -> 1 byte bitmap
    EXPECT_EQ(L.rowLen, 1u + 8u + 4u);      // bitmap + i64 + i32
    std::vector<uint8_t> row(L.rowLen, 0);
    FieldValue v{}; v.type = PrimType::Int64; v.present = true; v.i = 42;
    writeSlot(row.data(), L, 0, v);
    EXPECT_FALSE(isNull(row.data(), L, 0));
    EXPECT_EQ(readSlot(row.data(), L, 0).i, 42);
}

TEST(ViewRow, AbsentSetsNullBitNotZeroSentinel) {
    auto d = twoColDesc();
    RowLayout L = buildRowLayout(d);
    std::vector<uint8_t> row(L.rowLen, 0);
    FieldValue absent{}; absent.present = false; absent.type = PrimType::Int32;
    writeSlot(row.data(), L, 1, absent);
    EXPECT_TRUE(isNull(row.data(), L, 1));
    EXPECT_FALSE(readSlot(row.data(), L, 1).present); // distinct from a real 0
}

TEST(ViewRow, VarLenColumnRejected) {
    auto d = DescriptorBuilder("v",1).add("id",PrimType::Int64,1,true)
             .add("name",PrimType::String).build();
    EXPECT_THROW(buildRowLayout(d), std::invalid_argument);
}
```

- [ ] **Step 2: Run to verify it fails** — `cmake --build build-fast -j --target rune_view_tests` → compile error / link error (symbols absent).

- [ ] **Step 3: Implement `ViewRow.h` + `ViewRow.cpp`**

`buildRowLayout`: iterate `outDesc.fields` in declaration order; reject if `isVarLen(f.type)`; `bitmapBytes = (fields.size()+7)/8`; running offset starts at `bitmapBytes`; per field `width = fieldWidth(f)` (handles FixedStr via `fixedLen`, numerics via `fixedSize`); record `slotOffset/slotType/slotWidth`; `rowLen = offset`. Bitmap helpers: `row[slot>>3] & (1<<(slot&7))`. `writeSlot`: if `!v.present` → `setNull` + return; else `clearNull` and memcpy the width-appropriate representation from the `FieldValue` (i/u/f/b/bytes-for-FixedStr padded/truncated to `slotWidth`). `readSlot`: if null bit set → `{present=false}`; else decode per `slotType`.

- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R '^ViewRow\.' --output-on-failure` → 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/view/include/rune/view/ViewRow.h libs/view/src/ViewRow.cpp libs/view/tests/view/ViewRowTest.cpp libs/view/CMakeLists.txt
git commit -m "feat(view): fixed-width row layout + null bitmap (Phase 1 fixed-width columns only)"
```

---

## Task 3: `ViewTable` index + single-page slab (put/get/erase)

**Files:**
- Create: `libs/view/include/rune/view/ViewTable.h`
- Create: `libs/view/src/ViewTable.cpp`
- Create: `libs/view/tests/view/ViewTableTest.cpp`
- Modify: `libs/view/CMakeLists.txt` — add `src/ViewTable.cpp` + the test.

**Interfaces:**
- Consumes: `RowLayout` (Task 2).
- Produces (the storage core — port the committed reference algorithm from the design spike `viewtable_incr.cpp`, which this plan reproduces inline):
  - `class rune::view::ViewTable` with ctor `ViewTable(RowLayout layout, size_t initialIndexSlots = 4096, size_t chunkBytes = 32u<<20)`.
  - `uint8_t* put(uint64_t key);` — returns a writable pointer to the key's row (inserting a zeroed row on first use; idempotent for an existing key). Caller then `writeSlot`s into it.
  - `const uint8_t* get(uint64_t key) const noexcept;` — `nullptr` if absent.
  - `bool erase(uint64_t key) noexcept;` — index backshift + row-id free-list; O(1), no payload move.
  - `size_t size() const noexcept;`
  - Key is the 8-byte FNV-ish mixed `uint64_t` (callers pass the already-hashed-or-raw key bytes folded to u64; Task 11 defines the key derivation). Hash is `mix(key)`.

Phase 1 Task 3 uses ONE slab buffer (`std::vector<uint8_t> slab_`) sized `initialIndexSlots` rows; Task 4 generalizes to chunks. No atomics anywhere.

- [ ] **Step 1: Write the failing test**

```cpp
// libs/view/tests/view/ViewTableTest.cpp
#include <gtest/gtest.h>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>
using namespace rune::view;
using rune::codec::DescriptorBuilder; using rune::codec::PrimType; using rune::codec::FieldValue;

static RowLayout hpLayout() {
    return buildRowLayout(DescriptorBuilder("v",1)
        .add("id",PrimType::Int64,1,true).add("hp",PrimType::Int32).build());
}
static void setHp(uint8_t* row, const RowLayout& L, int32_t hp) {
    FieldValue v{}; v.type=PrimType::Int32; v.present=true; v.i=hp;
    writeSlot(row, L, 1, v);
}

TEST(ViewTable, InsertFindUpdateDelete) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(1001), L, 50);
    ASSERT_NE(t.get(1001), nullptr);
    EXPECT_EQ(readSlot(t.get(1001), L, 1).i, 50);
    setHp(t.put(1001), L, 75);                 // upsert same key -> no dup
    EXPECT_EQ(t.size(), 1u);
    EXPECT_EQ(readSlot(t.get(1001), L, 1).i, 75);
    EXPECT_TRUE(t.erase(1001));
    EXPECT_EQ(t.get(1001), nullptr);
    EXPECT_EQ(t.size(), 0u);
}

TEST(ViewTable, BackshiftKeepsCollidingKeysFindable) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);
    for (uint64_t k = 1; k <= 10; ++k) setHp(t.put(k), L, (int)k);
    EXPECT_TRUE(t.erase(5));
    for (uint64_t k = 1; k <= 10; ++k) {
        if (k == 5) { EXPECT_EQ(t.get(k), nullptr); continue; }
        ASSERT_NE(t.get(k), nullptr) << "key " << k << " lost after backshift";
        EXPECT_EQ(readSlot(t.get(k), L, 1).i, (int)k);
    }
}

TEST(ViewTable, FreeListRecyclesRowIds) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(1), L, 1); t.erase(1);
    setHp(t.put(2), L, 2);               // should reuse rid 0
    EXPECT_EQ(t.size(), 1u);
    EXPECT_EQ(readSlot(t.get(2), L, 1).i, 2);
}
```

- [ ] **Step 2: Run to verify it fails** — build `rune_view_tests` → symbols absent.

- [ ] **Step 3: Implement `ViewTable` (single-page)** — port the index from `viewtable_incr.cpp`:

```cpp
// key members
std::vector<uint8_t>  tags_;   // 1-byte Swiss/F14 tag per slot (0=EMPTY,1=TOMB,0x80|t=full)
std::vector<uint64_t> keys_;   // 8-byte key per slot
std::vector<uint32_t> rid_;    // 4-byte dense row-id per slot
std::vector<uint8_t>  slab_;   // rows: maxRows * rowLen (single page for Task 3)
std::vector<uint32_t> free_;   // row-id free-list
uint32_t nextRid_ = 0; size_t sz_ = 0, cap_, mask_;
static uint64_t mix(uint64_t x){ x^=x>>30; x*=0xbf58476d1ce4e5b9ULL; x^=x>>27; x*=0x94d049bb133111ebULL; x^=x>>31; return x; }
static uint8_t  fullTag(uint64_t h){ uint8_t t=(uint8_t)((h>>56)&0x7f); return 0x80|(t<2?2:t); }
```

`put`: `h=mix(key); ft=fullTag(h)`; probe from `h&mask_` for an existing `(ft,key)` (return its row ptr); else find first EMPTY, take a rid from `free_` or `nextRid_++`, set `tags_/keys_/rid_`, zero the row, `sz_++`, return `&slab_[rid*rowLen]`. `get`: probe; EMPTY → nullptr; `(ft,key)` → row ptr. `erase`: find slot, push rid to `free_`, backshift-delete within `tags_/keys_/rid_` (the `backshiftP` loop from the spike), `sz_--`. (Task 3 asserts `nextRid_ < maxRows`; growth is Task 4/5.)

- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R '^ViewTable\.' --output-on-failure` → 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/view/include/rune/view/ViewTable.h libs/view/src/ViewTable.cpp libs/view/tests/view/ViewTableTest.cpp libs/view/CMakeLists.txt
git commit -m "feat(view): ViewTable index+slab put/get/erase (single page, free-list, backshift)"
```

---

## Task 4: `ViewTable` chunked-arena slab growth

**Files:**
- Modify: `libs/view/include/rune/view/ViewTable.h`, `libs/view/src/ViewTable.cpp`
- Modify: `libs/view/tests/view/ViewTableTest.cpp` — add growth tests.

**Interfaces:**
- Produces: slab becomes a chunked arena. `rowsPerChunk_ = max(1, chunkBytes / rowLen)` rounded down to a power of two; `rowAddr(rid) = &chunks_[rid >> chunkShift_][ (rid & chunkMask_) * rowLen ]`. `appendChunk()` allocates one `std::vector<uint8_t>(rowsPerChunk_ * rowLen)` and pushes it. `put` appends a chunk when `nextRid_` reaches the current chunk capacity. **Rows never move**; existing `rid`→address mappings are stable.
- Produces: `size_t chunkCount() const noexcept;`

- [ ] **Step 1: Write the failing test**

```cpp
TEST(ViewTable, GrowsByAppendingChunksRowsNeverMove) {
    RowLayout L = hpLayout();
    // tiny chunk so growth happens fast: 4 rows per chunk
    ViewTable t(L, /*slots*/8, /*chunkBytes*/ L.rowLen * 4);
    std::vector<const uint8_t*> addrs;
    for (uint64_t k = 1; k <= 100; ++k) { uint8_t* r = t.put(k); setHp(r, L, (int)k); }
    EXPECT_GT(t.chunkCount(), 1u);
    for (uint64_t k = 1; k <= 100; ++k) {
        ASSERT_NE(t.get(k), nullptr);
        EXPECT_EQ(readSlot(t.get(k), L, 1).i, (int)k);   // all survive growth
    }
}
```

- [ ] **Step 2: Run to verify it fails** — the single-page `ViewTable` asserts/overflows past 4 rows.

- [ ] **Step 3: Implement chunked arena** — replace `slab_` with `std::vector<std::vector<uint8_t>> chunks_`, add `rowsPerChunk_/chunkShift_/chunkMask_`, `appendChunk()`, and the `rowAddr(rid)` shift+mask+pointer-load. `put`'s rid allocation appends a chunk when needed. `erase`/`get` use `rowAddr`.

- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R '^ViewTable\.' --output-on-failure` → all PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/view/include/rune/view/ViewTable.h libs/view/src/ViewTable.cpp libs/view/tests/view/ViewTableTest.cpp
git commit -m "feat(view): ViewTable chunked-arena slab (append-chunk growth, rows never move)"
```

---

## Task 5: `ViewTable` incremental rehash + de-amortized pre-provision

**Files:**
- Modify: `libs/view/include/rune/view/ViewTable.h`, `libs/view/src/ViewTable.cpp`
- Modify: `libs/view/tests/view/ViewTableTest.cpp`

**Design note (diverges from spec §7's *thread*-based pre-provision):** `libs/**` forbids atomics/threads (`check_no_atomics.py`). So the next-index buffer is pre-provisioned **de-amortized in the owning worker's thread**: once load crosses 0.65, each `put`/`erase` zero-fills a bounded slice (`PREZERO` entries) of the next `2×` `tags_/keys_/rid_` buffers; `startGrow` swaps them in O(1) when load crosses 0.85. If the burst outran pre-provisioning (buffers not fully zeroed), `startGrow` finishes the zeroing inline (the bounded fallback) and counts it. Migration is incremental: each op moves ≤ `MIGK` (default 256) index entries old→new; reads/writes consult new-then-old during the window. The slab (chunks) is **never** touched by rehash — only the tiny `(tag,key,rid)` index migrates. Port the migrate/startGrow logic from `viewtable_prealloc.cpp`.

**Interfaces:**
- Produces: `double worstRehashTaxUs() const noexcept;` (test/bench hook — max single-op migrate+startGrow ns/1000); `uint64_t rehashFallbacks() const noexcept;`

- [ ] **Step 1: Write the failing tests**

```cpp
TEST(ViewTable, IncrementalRehashKeepsAllKeysFindable) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);           // forces many grows
    for (uint64_t k = 1; k <= 200000; ++k) setHp(t.put(k), L, (int)(k & 0x7fffffff));
    for (uint64_t k = 1; k <= 200000; ++k) {
        ASSERT_NE(t.get(k), nullptr) << "lost key " << k << " across rehash";
    }
    EXPECT_EQ(t.size(), 200000u);
}

// Review-focus #3: a write burst that outruns pre-provision must still be correct.
TEST(ViewTableFallbackRegression, RehashFallbackWhenPreprovisionNotReady) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);
    // insert with no interleaved ops so pre-provision has no spare cycles -> forces fallbacks
    for (uint64_t k = 1; k <= 300000; ++k) setHp(t.put(k), L, 1);
    for (uint64_t k = 1; k <= 300000; ++k) ASSERT_NE(t.get(k), nullptr);
    EXPECT_EQ(t.size(), 300000u);           // no lost/duplicated rows even under fallback
}
```

- [ ] **Step 2: Run to verify it fails** — single-chunk/no-rehash table overflows or loses keys.

- [ ] **Step 3: Implement incremental rehash** — add the OLD-index members (`ot_/ok_/orid_/ocap_/omask_/ocur_/migrating_`), the pre-provision members (`nextTags_/nextKeys_/nextRid_buf_/prezeroCur_`), `maybePrezero()`, `startGrow()` (swap pre-provisioned or inline-finish + `fallbacks_++`), `migrateStep()` (≤MIGK), and make `put`/`get`/`erase`/`rowAddr` consult new-then-old during migration. Track `maxRehashNs_`.

- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R '^ViewTable|ViewTableFallbackRegression' --output-on-failure` → PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/view
git commit -m "feat(view): ViewTable incremental de-amortized rehash + in-worker pre-provision (no threads)"
```

---

## Task 6: `ViewTable` dirty-row tracking (row-sized checkpoint units)

**Files:**
- Modify: `libs/view/include/rune/view/ViewTable.h`, `libs/view/src/ViewTable.cpp`
- Modify: `libs/view/tests/view/ViewTableTest.cpp`

**Interfaces:**
- Produces: a per-row dirty bit (`std::vector<uint8_t> dirty_` indexed by `rid`, grown with chunks) set on every `put`-returned-row write and on first insert; `erase` records a delete tombstone for the checkpoint. The checkpoint unit is **row-sized** (one dirty row = one `{rid, rowBytes}` entry), per the §9.2 gate (NOT OS pages).
  - `void markDirty(uint32_t rid) noexcept;` (called by `put`; exposed for the fold).
  - `template<class Fn> void drainDirty(Fn&& emit);` — calls `emit(uint32_t rid, const uint8_t* row, bool deleted, uint64_t key)` for each dirty/deleted entry, then clears the dirty set (coalesced: a row written many times in the interval is emitted once). **The key is emitted** because a delete entry has no row, and delete-replay (Task 12) needs the key to `erase` — so `drainDirty` carries `key` for both upserts and deletes.
  - `std::span<const uint32_t> dirtyRids() const noexcept;` (test hook).

Note: `put` returns the row pointer AND marks the rid dirty (the fold writes through that pointer, then the rid is already flagged). `erase` appends `{rid, key}` to a `deleted_` list drained alongside (keyed so the delta can carry it).

- [ ] **Step 1: Write the failing test**

```cpp
TEST(ViewTable, DirtyTrackingCoalescesAndClears) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(1), L, 1);
    setHp(t.put(1), L, 2);   // same row twice in interval
    setHp(t.put(2), L, 9);
    std::vector<uint64_t> seenKeys;
    t.drainDirty([&](uint32_t, const uint8_t*, bool del, uint64_t key){ if(!del) seenKeys.push_back(key); });
    std::sort(seenKeys.begin(), seenKeys.end());
    EXPECT_EQ(seenKeys.size(), 2u);          // key 1 coalesced to one entry
    EXPECT_EQ(seenKeys[0], 1u); EXPECT_EQ(seenKeys[1], 2u);
    std::vector<uint32_t> again;
    t.drainDirty([&](uint32_t rid, const uint8_t*, bool, uint64_t){ again.push_back(rid); });
    EXPECT_TRUE(again.empty());              // cleared after drain
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement dirty tracking** — `dirty_` bit per rid + a `dirtyList_` of rids (push rid on first mark in an interval), `deleted_` list in `erase`, `drainDirty` iterates + clears.
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R '^ViewTable\.' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/view
git commit -m "feat(view): ViewTable row-sized dirty tracking for differential checkpoint"
```

---

## Task 7: Storage benchmark — the §7.1 / §14.0 storage gate

**Files:**
- Create: `benchmark/ViewTableBench.cpp`
- Modify: top-level `CMakeLists.txt` — add `benchmark/ViewTableBench.cpp` to the `rune_broker_benchmark` source list (guarded by `if(BUILD_BENCHMARKS)`), link `rune_view`.

**Interfaces:**
- Consumes: `ViewTable`, `RowLayout`, `worstRehashTaxUs()`, `rehashFallbacks()`, the `g_allocCount`/`g_allocEnabled` counters from `benchmark/BenchmarkMain.cpp`.

- [ ] **Step 1: Write the benchmark** — mirror `viewtable_incr.cpp`: `BM_ViewTableChurn` over rowSize args `{64,256,1024}`, 60/25/15 create/update/delete to 15 M (8 M at 1024), report counters `rehash_tax_us`, `delete_p99_ns`, `find_steady_ns`, `fallbacks`, and `steady_alloc` (snapshot `g_allocCount` across the steady-state window with `g_allocEnabled=true`).

```cpp
// benchmark/ViewTableBench.cpp (shape)
#include <benchmark/benchmark.h>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
extern std::atomic<uint64_t> g_allocCount; extern bool g_allocEnabled; // from BenchmarkMain.cpp
static void BM_ViewTableChurn(benchmark::State& st) {
    const size_t rowSize = (size_t)st.range(0);
    /* build a RowLayout padded to rowSize; run churn; collect latency vectors */
    for (auto _ : st) { /* one full churn pass */ }
    st.counters["rehash_tax_us"] = /*t.worstRehashTaxUs()*/ 0;
    st.counters["delete_p99_ns"] = /*p99 of delete latencies*/ 0;
    st.counters["find_steady_ns"] = /*steady find ns/op*/ 0;
    st.counters["fallbacks"] = /*t.rehashFallbacks()*/ 0;
}
BENCHMARK(BM_ViewTableChurn)->Arg(64)->Arg(256)->Arg(1024)->Unit(benchmark::kMicrosecond)->UseRealTime();
```

- [ ] **Step 2: Build with benchmarks** — `cmake -B build-fast -DBUILD_BENCHMARKS=ON ...` (re-configure), `cmake --build build-fast -j --target rune_broker_benchmark`.
- [ ] **Step 3: Run** — `./build-fast/rune_broker_benchmark --benchmark_filter='BM_ViewTableChurn' --benchmark_min_time=0.01s`
      Expected (the GATE): `rehash_tax_us < 50` for all three row sizes; `delete_p99_ns` flat across sizes (no payload-move scaling); `find_steady_ns ≈ 25–30`; `steady_alloc == 0`. A result over budget is a **failing gate**, not a note — fix `ViewTable` before proceeding.
- [ ] **Step 4: Record** — push `scripts/rune-live.sh test "rune-broker/ViewTableBench" passed kind=benchmark p99Us=<rehash_tax_us> commit=$(git rev-parse --short HEAD)`.
- [ ] **Step 5: Commit**

```bash
git add benchmark/ViewTableBench.cpp CMakeLists.txt
git commit -m "bench(view): ViewTable storage gate (<50us rehash, flat delete, zero steady alloc)"
```

---

## Task 8: View plan types + (de)serialization (`ViewPlan`)

**Files:**
- Create: `libs/view/include/rune/view/ViewPlan.h`
- Create: `libs/view/src/ViewPlan.cpp`
- Create: `libs/view/tests/view/ViewPlanTest.cpp`
- Modify: `libs/view/CMakeLists.txt`

**Interfaces:**
- Produces (match the spec §6.0 struct names exactly; note `keyFieldId` is **per-source**):
  - `struct AuthoredField { std::string name; rune::codec::PrimType type; uint16_t sourceIdx; std::string srcField; MergeRule rule; };`
  - `struct SourceSpec { int32_t topicId; std::string keyField; };`
  - `struct ViewSizing { uint32_t chunkBytes = 32u<<20; uint32_t capacity = 0; /*0=default pre-size*/ uint64_t maxRows = 0; /*0=unbounded*/ };` — the §10/§15 #24 `WITH` knobs (user-requested: chunk size is configurable at `CreateView`).
  - `struct ViewPlan { std::string viewName; std::string key; std::vector<SourceSpec> sources; std::vector<AuthoredField> output; uint8_t persistence; /*0=persisted,1=transient*/ ViewSizing sizing; };`
  - `struct CompiledField { uint16_t outSlot; rune::codec::PrimType type; uint16_t sourceIdx; uint16_t srcFieldId; MergeRule rule; };`
  - `struct CompiledViewPlan { std::string viewName; int32_t viewTopicId; std::vector<uint16_t> keyFieldId; std::vector<int32_t> sourceTopicId; uint64_t outputFingerprint; std::vector<CompiledField> output; uint8_t persistence; ViewSizing sizing; };`
  - `std::vector<uint8_t> serialize(const CompiledViewPlan&);` / `std::optional<CompiledViewPlan> deserializeCompiledViewPlan(std::span<const uint8_t>);` — flat LE encoding (used for the admin→worker fan-out ring and the durable `__views` record). Both encode `ViewSizing`.
  - `std::vector<uint8_t> serializeAuthored(const ViewPlan&);` / `std::optional<ViewPlan> deserializeAuthored(std::span<const uint8_t>);` — the authoring form carried in the `CreateView` wire `planBlob` (names + `ViewSizing`), resolved by the compiler (Task 9) on the AdminWorker.

- [ ] **Step 1: Write the failing test**

```cpp
// round-trip a compiled plan
TEST(ViewPlan, CompiledRoundTrip) {
    rune::view::CompiledViewPlan p;
    p.viewName = "char_hp"; p.viewTopicId = 0x40000007; p.persistence = 0;
    p.keyFieldId = {0}; p.sourceTopicId = {5}; p.outputFingerprint = 0xDEADBEEFULL;
    p.output = {{0, rune::codec::PrimType::Int64, 0, 0, rune::view::MergeRule::Replace},
                {1, rune::codec::PrimType::Int32, 0, 3, rune::view::MergeRule::Replace}};
    auto bytes = rune::view::serialize(p);
    auto back = rune::view::deserializeCompiledViewPlan(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->viewName, "char_hp");
    EXPECT_EQ(back->keyFieldId.size(), 1u);
    EXPECT_EQ(back->output.size(), 2u);
    EXPECT_EQ(back->output[1].srcFieldId, 3);
    EXPECT_EQ(back->outputFingerprint, 0xDEADBEEFULL);
}
TEST(ViewPlan, SizingKnobsRoundTrip) {
    rune::view::CompiledViewPlan p; p.viewName="v"; p.viewTopicId=1;
    p.keyFieldId={0}; p.sourceTopicId={5}; p.output={{0,rune::codec::PrimType::Int64,0,0,rune::view::MergeRule::Replace}};
    p.sizing.chunkBytes = 8u<<20; p.sizing.maxRows = 1000000;
    auto back = rune::view::deserializeCompiledViewPlan(rune::view::serialize(p));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->sizing.chunkBytes, 8u<<20);
    EXPECT_EQ(back->sizing.maxRows, 1000000u);
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement the structs + LE serialize/deserialize** (length-prefixed strings, fixed-width scalars; version byte first for forward-compat).
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R '^ViewPlan\.' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/view
git commit -m "feat(view): ViewPlan/CompiledViewPlan types + flat serialization"
```

---

## Task 9: Plan compiler — resolve, validate, derive output descriptor

**Files:**
- Create: `libs/view/include/rune/view/PlanCompiler.h`
- Create: `libs/view/src/PlanCompiler.cpp`
- Create: `libs/view/tests/view/PlanCompilerTest.cpp`
- Modify: `libs/view/CMakeLists.txt`

**Interfaces:**
- Consumes: `ViewPlan`, `CompiledViewPlan` (Task 8); `rune::codec::Descriptor`, `Descriptor::find`, `fingerprint()`, `DescriptorBuilder`; `isNumeric`/`isFixedWidthColumn` (Task 1).
- Produces:
  - `struct CompileError { std::string message; };`
  - `struct CompileResult { CompiledViewPlan plan; rune::codec::Descriptor outputDescriptor; RowLayout rowLayout; };`
  - `std::expected<CompileResult, CompileError> compileView(const ViewPlan& authored, std::span<const rune::codec::Descriptor* const> sourceDescriptors, int32_t viewTopicId);`
  - Behavior: for each `AuthoredField`, `sourceDescriptors[sourceIdx]->find(srcField)` → assert **exactly one** match (duplicate-resolving name → error), reject var-len via `isFixedWidthColumn`, resolve `srcFieldId = fd->id`; for `Sum/Avg/Min/Max` require `isNumeric` (Phase 1 only emits `Replace`, but the guard stays). Resolve each source's `keyField` → per-source `keyFieldId` (key must exist, be a **Phase 1 integer key type** `Int32/Int64/UInt16/UInt32/UInt64` via a new `isIntegerKeyType(PrimType)` helper in `ViewTypes.h`, non-null-able). Build the **output Descriptor** from the `AuthoredField` names/types (key field first), compute its `fingerprint`, build the `RowLayout`. The key column must appear in `output` (spec §15 #6).

- [ ] **Step 1: Write the failing tests** (covers Review Focus #2)

```cpp
// libs/view/tests/view/PlanCompilerTest.cpp  (excerpt)
static rune::codec::Descriptor charSource() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/true)
        .add("hp", rune::codec::PrimType::Int32)
        .add("name", rune::codec::PrimType::String).build();      // var-len
}
static rune::view::ViewPlan hpView() {
    rune::view::ViewPlan v; v.viewName="char_hp"; v.key="characterId";
    v.sources = {{5,"characterId"}};
    v.output = {{"characterId",rune::codec::PrimType::Int64,0,"characterId",rune::view::MergeRule::Replace},
                {"hp",rune::codec::PrimType::Int32,0,"hp",rune::view::MergeRule::Replace}};
    return v;
}
TEST(PlanCompiler, CompilesSingleSourceReplaceView) {
    auto d = charSource(); const rune::codec::Descriptor* srcs[]={&d};
    auto r = rune::view::compileView(hpView(), srcs, 0x40000007);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->plan.keyFieldId.size(), 1u);
    EXPECT_EQ(r->plan.keyFieldId[0], 0);               // characterId tag
    EXPECT_EQ(r->plan.output[1].srcFieldId, 1);        // hp tag
    EXPECT_NE(r->plan.outputFingerprint, 0u);
    EXPECT_EQ(r->rowLayout.rowLen, 1u + 8u + 4u);
}
TEST(PlanCompilerRejectRegression, CompileRejectsUnknownAndVarLenAndMissingKey) {
    auto d = charSource(); const rune::codec::Descriptor* srcs[]={&d};
    { auto v=hpView(); v.output[1].srcField="nope";
      EXPECT_FALSE(rune::view::compileView(v,srcs,1).has_value()); }       // unknown field
    { auto v=hpView(); v.output.push_back({"name",rune::codec::PrimType::String,0,"name",rune::view::MergeRule::Replace});
      EXPECT_FALSE(rune::view::compileView(v,srcs,1).has_value()); }       // var-len column
    { auto v=hpView(); v.key="ghostKey"; v.sources[0].keyField="ghostKey";
      EXPECT_FALSE(rune::view::compileView(v,srcs,1).has_value()); }       // missing key
    { // non-integer key (FixedStr) rejected in Phase 1
      auto d2 = rune::codec::DescriptorBuilder("c2",1)
          .addFixedStr("sid",16,1,true).add("hp",rune::codec::PrimType::Int32).build();
      const rune::codec::Descriptor* s2[]={&d2};
      rune::view::ViewPlan v; v.viewName="x"; v.key="sid"; v.sources={{6,"sid"}};
      v.output={{"sid",rune::codec::PrimType::FixedStr,0,"sid",rune::view::MergeRule::Replace},
                {"hp",rune::codec::PrimType::Int32,0,"hp",rune::view::MergeRule::Replace}};
      EXPECT_FALSE(rune::view::compileView(v,s2,1).has_value()); }
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement `compileView`** per the behavior above, returning `std::unexpected(CompileError{...})` on each rejection.
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'PlanCompiler' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/view
git commit -m "feat(view): plan compiler — resolve tags, validate, derive output descriptor+fingerprint"
```

---
## Task 10: The fold (`ViewFold::foldBatch`) — Replace + tombstone + dropped→null + determinism

**Files:**
- Create: `libs/view/include/rune/view/ViewFold.h`, `libs/view/src/ViewFold.cpp`
- Create: `libs/view/tests/view/ViewFoldTest.cpp`
- Modify: `libs/view/CMakeLists.txt` — link `rune_view` against the protocol include (for `RecordBatch.h`) and `rune_codec`.

**Interfaces:**
- Consumes: `rune::protocol::batch::RecordBatchReader` + `forEachRecord` (from `rune/protocol/RecordBatch.h`); `rune::storage::LogReadView`; `rune::codec::readField` + `materializeAsOf` + `fingerprint`; `CompiledViewPlan`/`RowLayout` (Tasks 8–9); `ViewTable` (Tasks 3–6); `ViewRow` (Task 2).
- Produces:
  - `class rune::view::VersionDescriptorCache { public: explicit VersionDescriptorCache(rune::codec::Descriptor evolving); const rune::codec::Descriptor& materializedFor(uint16_t writerVersion); };` — lazily `materializeAsOf`s and caches per writer version (cache hit → no alloc; version changes are rare, not steady-state).
  - `struct FoldStats { uint64_t applied; uint64_t deleted; uint64_t skippedControl; };`
  - `FoldStats rune::view::foldBatch(const CompiledViewPlan& plan, const RowLayout& layout, VersionDescriptorCache& vdc, ViewTable& table, const rune::storage::LogReadView& view, int64_t bound, int64_t& cursor);`
  - Algorithm: walk `view` batch-by-batch (`RecordBatchReader r(view.data+pos, view.len-pos); r.ensureValid();`), skip `r.isControl()` batches, stop at `bound` (the read-committed LSO passed by the caller). For each record via `r.forEachRecord([&](std::string_view key, std::string_view value, int64_t off, int64_t ts){ ... })`:
    - if `off >= bound` → stop (don't fold uncommitted);
    - `uint16_t wv = value.size()>=4 ? (uint8_t)value[2] | ((uint8_t)value[3]<<8) : plan-version`; `const Descriptor& d = vdc.materializedFor(wv)`;
    - `FieldValue kf = readField(d, asBytes(value), plan.keyFieldId[0]);` → fold to `uint64_t k` (bit-cast signed / zero-extend unsigned);
    - **tombstone** (`value.empty()`): `table.erase(k)`; `++deleted`; continue;
    - else `uint8_t* row = table.put(k);` then for each `CompiledField f`: `FieldValue in = readField(d, asBytes(value), f.srcFieldId); writeSlot(row, layout, f.outSlot, in);` — when `in.present==false` (dropped/not-live field) `writeSlot` sets the null bit (Review #1). `++applied`.
  - advance `cursor = view.nextOffset` (capped at `bound`).

- [ ] **Step 1: Write the failing tests** (build records with `RecordBatchBuilder`; value bytes = a codec-encoded `character` record)

```cpp
// libs/view/tests/view/ViewFoldTest.cpp  (excerpt — helpers build a one-record batch per step)
#include <gtest/gtest.h>
#include <rune/view/ViewFold.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Record.h>
// encodeCharacter(id,hp, version): returns codec payload bytes [blockLen][version][block...]
// makeBatch(baseOffset, key, value, tombstone): returns a valid RecordBatch via RecordBatchBuilder

TEST(ViewFold, UpsertAndTombstone) {
    auto src = /* evolving character descriptor (id key, hp i32) */;
    auto comp = rune::view::compileView(/*hpView*/, {&src}, 0x40000007).value();
    rune::view::ViewTable t(comp.rowLayout /* via CompileResult */);
    rune::view::VersionDescriptorCache vdc(src);
    // record 0: id=1001 hp=50
    auto b0 = makeBatch(0, keyBytes(1001), encodeCharacter(1001,50,1), /*tomb*/false);
    rune::storage::LogReadView v0{b0.data(), b0.size(), 0, 1};
    int64_t cur = 0;
    auto s0 = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v0, /*bound*/1, cur);
    EXPECT_EQ(s0.applied, 1u);
    EXPECT_EQ(readSlot(t.get(1001), comp.rowLayout, 1).i, 50);
    EXPECT_EQ(cur, 1);
    // record 1: tombstone for id=1001
    auto b1 = makeBatch(1, keyBytes(1001), {}, /*tomb*/true);
    rune::storage::LogReadView v1{b1.data(), b1.size(), 1, 2};
    auto s1 = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v1, 2, cur);
    EXPECT_EQ(s1.deleted, 1u);
    EXPECT_EQ(t.get(1001), nullptr);
}

// Review-focus #1
TEST(ViewFoldDroppedFieldRegression, FoldDroppedSourceFieldGoesNull) {
    // src v1 has {id, hp}; evolve to v2 that DROPs hp (untilVersion=2).
    // A v2 record for id=1001 must leave the view's hp column NULL, rowLen unchanged.
    auto src = /* evolving desc: id since v1 key; hp since v1 until v2 */;
    auto comp = rune::view::compileView(/*hpView on v1 tag*/, {&src}, 1).value();
    rune::view::ViewTable t(comp.rowLayout);
    rune::view::VersionDescriptorCache vdc(src);
    auto b = makeBatch(0, keyBytes(1001), encodeCharacterV2DroppedHp(1001), false);
    rune::storage::LogReadView v{b.data(), b.size(), 0, 1};
    int64_t cur = 0;
    rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v, 1, cur);
    ASSERT_NE(t.get(1001), nullptr);
    EXPECT_TRUE(rune::view::isNull(t.get(1001), comp.rowLayout, 1));   // hp column null
}

// Determinism (#16): two different per-key-preserving orderings => identical table bytes.
TEST(ViewFold, DeterministicAcrossBatchBoundaries) {
    // fold the same 3 records split as [3] vs [1][1][1]; compare full slab byte image.
    // (expose a test-only ViewTable::debugImage() returning concatenated live rows in rid order,
    //  or compare get() of every key.)
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement `VersionDescriptorCache` + `foldBatch`** per the algorithm. `asBytes(sv) = std::span<const uint8_t>((const uint8_t*)sv.data(), sv.size())`.
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'ViewFold' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/view
git commit -m "feat(view): single-source fold (Replace+tombstone, dropped-field->null, deterministic)"
```

---

## Task 11: Fold benchmark — the §14.0 hot-path gate

**Files:**
- Create: `benchmark/ViewFoldBench.cpp`
- Modify: top-level `CMakeLists.txt` — add to `rune_broker_benchmark`.

- [ ] **Step 1: Write the benchmark** — `BM_ViewFoldReplace`: pre-build N committed single-record batches (characterId churn, 60/25/15 upsert/upsert-existing/tombstone), fold them, report `record_p50_us`/`record_p99_us` (per-record pipeline), `batch_p99_us`, and `steady_alloc` (via `g_allocCount` with `g_allocEnabled`).
- [ ] **Step 2: Build** — `cmake --build build-fast -j --target rune_broker_benchmark` (reconfigure with `-DBUILD_BENCHMARKS=ON` if needed).
- [ ] **Step 3: Run** — `./build-fast/rune_broker_benchmark --benchmark_filter='BM_ViewFoldReplace' --benchmark_min_time=0.01s`
      Expected (GATE): `record_p99_us < 5`, `batch_p99_us < 10`, `steady_alloc == 0`. Over budget → fix before proceeding.
- [ ] **Step 4: Record** — `scripts/rune-live.sh test "rune-broker/ViewFoldBench" passed kind=benchmark p99Us=<record_p99_us> commit=$(git rev-parse --short HEAD)`.
- [ ] **Step 5: Commit**

```bash
git add benchmark/ViewFoldBench.cpp CMakeLists.txt
git commit -m "bench(view): fold->merge pipeline gate (<5us/record, <10us/batch, zero steady alloc)"
```

---

## Task 12: Differential-checkpoint codec (`ViewCheckpoint`) — delta/base + replay

**Files:**
- Create: `libs/view/include/rune/view/ViewCheckpoint.h`, `libs/view/src/ViewCheckpoint.cpp`
- Create: `libs/view/tests/view/ViewCheckpointTest.cpp`
- Modify: `libs/view/CMakeLists.txt`

**Interfaces:**
- Consumes: `ViewTable::drainDirty` (Task 6), `RowLayout`, `rune::protocol::crc32c` (from `rune/protocol/Crc32c.h`).
- Produces:
  - Delta wire record (one per flush): `[magic u32][O i64][rowLen u32][count u32] then count×{ rid u32, flags u8 (0=upsert,1=delete), keyU64 u64, [rowLen bytes if upsert] } [crc32c u32 over body]`. Base record = same shape emitting every live row at a snapshot `O`.
  - `size_t encodeDelta(std::span<const DirtyEntry> entries, int64_t O, uint32_t rowLen, std::vector<uint8_t>& out);` where `struct DirtyEntry { uint32_t rid; bool deleted; uint64_t key; const uint8_t* row; };`
  - `struct AppliedDelta { int64_t O; uint32_t count; };`
  - `std::optional<AppliedDelta> applyDelta(ViewTable& table, const RowLayout& layout, std::span<const uint8_t> deltaBytes);` — CRC-check, then for each entry `deleted`→`table.erase(key)` else `memcpy(table.put(key), row, rowLen)`. Returns the delta's `O`.
  - `int64_t replay(ViewTable& table, const RowLayout& layout, std::span<const uint8_t> base, std::span<const std::span<const uint8_t>> deltasInOrder);` — applies base then deltas, returns the highest applied `O` (the recovered applied offset).

- [ ] **Step 1: Write the failing test** (Review-focus #4 — offset-atomic + byte-identical)

```cpp
TEST(ViewCheckpointReplayRegression, CheckpointReplayIsOffsetAtomicAndByteIdentical) {
    RowLayout L = hpLayout();
    ViewTable live(L);
    // fold a stream, snapshot base at O=100, then deltas at O=150,O=200
    // ... populate live, drainDirty -> encodeDelta(O=150), more writes -> encodeDelta(O=200)
    std::vector<uint8_t> base;  /* encodeDelta of all live rows at O=100 */
    std::vector<uint8_t> d150, d200;
    // Simulate crash after d200 fsync:
    ViewTable restored(L);
    int64_t recovered = replay(restored, L,
        base, std::array<std::span<const uint8_t>,2>{d150, d200});
    EXPECT_EQ(recovered, 200);                              // exactly last delta's O, never +/-1
    // byte-identical: every key present in `live` matches `restored`
    // (compare via get()+memcmp(row, rowLen) over a known key set)
}

TEST(ViewCheckpoint, DeltaCrcRejectsCorruption) {
    RowLayout L = hpLayout(); ViewTable t(L);
    std::vector<uint8_t> d; /* encodeDelta one upsert at O=5 */
    d[d.size()-1] ^= 0xFF;                                  // corrupt crc byte
    ViewTable t2(L);
    EXPECT_FALSE(applyDelta(t2, L, d).has_value());         // rejected
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement** `encodeDelta`/`applyDelta`/`replay` per the wire format, using `rune::protocol::crc32c`.
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'ViewCheckpoint' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/view
git commit -m "feat(view): differential checkpoint codec (delta/base + offset-atomic replay, CRC)"
```

---

## Task 13: `ViewSnapshotChannel` + `ViewSnapshotIoAgent` (dedicated IO thread)

**Files:**
- Create: `libs/view/include/rune/view/ViewSnapshotChannel.h` (header-only — two `OneToOneByteRing`s + a fixed handoff-buffer pool, mirrors `libs/node/include/rune/node/CleanerViewChannel.h`)
- Create: `libs/node/include/rune/node/ViewSnapshotIoAgent.h`, `libs/node/src/ViewSnapshotIoAgent.cpp`
- Create: `libs/node/tests/node/ViewSnapshotIoAgentTest.cpp`
- Modify: `libs/node/CMakeLists.txt`

**Interfaces:**
- Consumes: `rune::node::OneToOneByteRing` (`tryClaim`/`commit`/`drain`), `rune::consensus::Agent`, `rune::storage` append/fsync primitives (`appendNoSync`/`sync` pattern) or a plain file (`::open`/`::write`/`fdatasync`) following `CleanerOffsetCheckpoint`; `rune::protocol::crc32c`.
- Produces:
  - `struct rune::view::ViewSnapshotChannel { OneToOneByteRing deltas{1<<20}; OneToOneByteRing done{4096}; /* buffer pool indices */ };` — worker produces delta bytes on `deltas` (zero-copy `tryClaim`), the IO agent returns completed-flush acks on `done`.
  - `class rune::node::ViewSnapshotIoAgent final : public rune::consensus::Agent` — `int doWork(int64_t nowNs) override` drains `deltas`, appends each `{O,chunks,CRC}` delta to the per-(view,partition) delta file, groups `fdatasync` every `fsyncMs` (default 50), posts a `done` ack with the durable `O`. `const char* name() const noexcept override { return "rune-view-io"; }`. Ctor takes the channel, the view-data directory, and `fsyncMs`.
  - `int64_t lastDurableOffset(int32_t viewTopicId, int32_t partition) const noexcept;` (for the readiness gate + tests).

- [ ] **Step 1: Write the failing test** (deterministic — `CachedNanoClock`, advance + one `doWork` per transition, hard cap)

```cpp
TEST(ViewSnapshotIoAgent, AppendsDeltaAndGroupsFsync) {
    TempDir dir;
    rune::view::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), /*fsyncMs*/50);
    rune::CachedNanoClock clk; clk.set(0);
    // worker side: claim+commit one delta {O=10} for (view=0x40000007,part=0)
    putDelta(ch, 0x40000007, 0, /*O*/10, /*bytes*/somewRow);
    for (int i=0;i<4 && agent.lastDurableOffset(0x40000007,0)<10;i++){ agent.doWork(clk.nowNs()); clk.advanceMs(60); }
    EXPECT_EQ(agent.lastDurableOffset(0x40000007,0), 10);          // durable after grouped fsync window
    // the delta file exists on disk and CRC-validates (reopen + replay)
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement the channel + agent.** The agent holds a per-(view,partition) open file handle map (plain `std::unordered_map`, IO-thread-owned — no atomics), appends `appendNoSync`-style, tracks `sinceFsyncBytes`/`lastFsyncNs`, `fdatasync` when `nowNs-lastFsyncNs >= fsyncMs*1e6`, updates `lastDurableOffset_` after fsync (offset-atomic). Return the buffer via `done`.
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'ViewSnapshotIoAgent' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/view/include/rune/view/ViewSnapshotChannel.h libs/node/include/rune/node/ViewSnapshotIoAgent.h libs/node/src/ViewSnapshotIoAgent.cpp libs/node/tests/node/ViewSnapshotIoAgentTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(view): ViewSnapshotChannel + dedicated ViewSnapshotIoAgent (rings, grouped fsync, offset-atomic)"
```

---

## Task 14: Checkpoint benchmark — the §9.2 reads-unmoved gate

**Files:**
- Create: `benchmark/ViewCheckpointBench.cpp`
- Modify: top-level `CMakeLists.txt`.

- [ ] **Step 1: Write the benchmark** — port `ckpt_bench.cpp`: worker folds in batches, marks dirty, at each batch boundary copies dirty rows into a handoff buffer handed to the IO agent over the channel; measure read latency with checkpointing **ON vs OFF** (ABAB arms), report `read_p50_ns`/`read_p99_ns`/`read_p999_ns` for each arm, plus `write_GBps`, `io_stalls`, `peak_extra_bytes` (handoff pool only — assert no memory doubling).
- [ ] **Step 2: Build** — `cmake --build build-fast -j --target rune_broker_benchmark`.
- [ ] **Step 3: Run** — `./build-fast/rune_broker_benchmark --benchmark_filter='BM_ViewCheckpoint' --benchmark_min_time=0.02s`
      Expected (GATE): `read_p999_ns(ON) ≈ read_p999_ns(OFF)` (ON within noise of OFF through p99.9); `io_stalls == 0` at the target cadence; `peak_extra_bytes` == handoff-pool size (NOT ≈ view size — proves no doubling/COW). Row-sized dirty units only (a 4 KB-chunk arm must show the IO-saturation regression, documenting why row-sized is mandatory).
- [ ] **Step 4: Record** — `scripts/rune-live.sh test "rune-broker/ViewCheckpointBench" passed kind=benchmark p99Us=<read_p99_ns/1000> commit=$(git rev-parse --short HEAD)`.
- [ ] **Step 5: Commit**

```bash
git add benchmark/ViewCheckpointBench.cpp CMakeLists.txt
git commit -m "bench(view): checkpoint gate (reads unmoved ON vs OFF thru p99.9, no memory doubling)"
```

---

## Task 15: `WorkerViewSet` + `Worker` fold integration

**Files:**
- Create: `libs/node/include/rune/node/WorkerViewSet.h`, `libs/node/src/WorkerViewSet.cpp`
- Create: `libs/node/tests/node/WorkerViewSetTest.cpp`
- Modify: `libs/node/include/rune/node/Worker.h` (+ `Worker.cpp` if present) — own a `WorkerViewSet`, drive it in `doWork`.
- Modify: `libs/node/CMakeLists.txt`

**Interfaces:**
- Consumes: `CompiledViewPlan`/`RowLayout`/`ViewTable`/`foldBatch`/`VersionDescriptorCache` (Tasks 6–10); `ViewSnapshotChannel` (Task 13); `Worker`'s existing `subs_`/`rcOf_`/`readNoFault_`/LSO selection (`Worker.h:2780-2804`).
- Produces (all per-`Worker`, shared-nothing, no atomics):
  - `struct ViewInstance { CompiledViewPlan plan; RowLayout layout; rune::codec::Descriptor evolvingSrc; VersionDescriptorCache vdc; ViewTable table; int64_t foldCursor; int64_t lastCheckpointO; bool persisted; };` — the `ViewTable` is constructed with `plan.sizing.chunkBytes`/`capacity` (user-requested configurable chunk size), and `maxRows` is enforced as the §10 hard ceiling (exceeding it raises a capacity-exceeded event rather than growing).
  - `class rune::node::WorkerViewSet { void installView(CompiledViewPlan plan, RowLayout layout, rune::codec::Descriptor evolvingSrc); void onSourceCommitted(int32_t srcTopicId, int32_t partition, auto readRange, int64_t bound, int64_t nowNs); const uint8_t* getRow(int32_t viewTopicId, uint64_t key, const RowLayout*& outLayout) const; };`
  - `subscriptionIndex_ : (srcTopicId,partition) → [viewTopicId]` (the §4.1 reactor index); `views_ : viewTopicId → ViewInstance`.
  - `Worker::doWork` calls `viewSet_.driveFolds(nowNs)` right after `rcAdvanceAll_()` (`Worker.h:1546`): for each owned view whose source advanced, read `[foldCursor, rc->lso(HW))` via the existing `readNoFault_` into the worker scratch buffer, `foldBatch`, and at the batch boundary `drainDirty` → `encodeDelta` → `channel.deltas.tryClaim/commit` (skip entirely if `!persisted`).

- [ ] **Step 1: Write the failing test** (in-process, deterministic — a fake source log + a `CachedNanoClock`)

```cpp
TEST(WorkerViewSet, FoldsCommittedRangeAndServesRow) {
    WorkerViewSet vs(/*channel*/, /*clock*/);
    vs.installView(hpPlan, hpLayout, charEvolvingDesc);
    // feed a committed range [0,2): two upserts id=7 hp=10 then hp=20
    vs.onSourceCommitted(/*srcTopicId*/5, /*part*/0, fakeReadRange, /*bound*/2, /*nowNs*/0);
    const RowLayout* L=nullptr;
    const uint8_t* row = vs.getRow(0x40000007, 7, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(readSlot(row, *L, 1).i, 20);
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement `WorkerViewSet`** + wire `viewSet_.driveFolds(nowNs)` into `Worker::doWork` after `rcAdvanceAll_()`. Honor `persisted` (transient skips the checkpoint handoff).
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'WorkerViewSet' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/WorkerViewSet.h libs/node/src/WorkerViewSet.cpp libs/node/tests/node/WorkerViewSetTest.cpp libs/node/include/rune/node/Worker.h libs/node/CMakeLists.txt
git commit -m "feat(view): per-worker WorkerViewSet + Worker duty-cycle fold integration (transient-aware)"
```

---

## Task 16: Restart readiness gate (`RECOVERING → REBUILDING_VIEWS → READY`)

**Files:**
- Modify: `libs/node/include/rune/node/WorkerViewSet.h` (+ `.cpp`) — add the readiness state machine.
- Modify: `libs/node/include/rune/node/Worker.h` — serve guard; expose `viewsReady()`.
- Modify: `libs/node/include/rune/node/BrokerNode.h`/`.cpp` — load base+deltas on startup, gate election-eligibility and `GetViewRow` serving on `viewsReady()`.
- Create: `libs/node/tests/node/ViewReadinessRegressionTest.cpp`

**Interfaces:**
- Produces: `enum class ViewReadiness : uint8_t { Recovering, RebuildingViews, Ready };` on `WorkerViewSet`; `bool allViewsReady() const noexcept;`. On startup each `ViewInstance` loads its base+deltas via `ViewCheckpoint::replay` (→ `foldCursor = recovered O`), then replays the source log `(O, committedTail]` through `foldBatch` until `foldCursor == committedTail`, flipping `Recovering → RebuildingViews → Ready`. A `GetViewRow` for a not-`Ready` view returns a retriable not-ready (Task 20 surfaces the wire code). Election-eligibility (consensus) and serving are gated on `allViewsReady()`. Local-only (no cluster dependency), bounded by checkpoint cadence.

- [ ] **Step 1: Write the failing tests** (Review-focus #5; deterministic `CachedNanoClock`, advance + one `doWork` per transition, hard cap)

```cpp
TEST(ViewReadinessRegression, GetViewRowNotReadyIsRetriable) {
    WorkerViewSet vs(...);
    vs.installViewForRecovery(hpPlan, hpLayout, charEvolvingDesc, /*checkpointO*/100, /*committedTail*/150);
    EXPECT_EQ(vs.readiness(0x40000007), ViewReadiness::RebuildingViews);
    const RowLayout* L=nullptr;
    EXPECT_EQ(vs.getRow(0x40000007, 7, L), nullptr);            // not served while rebuilding
    EXPECT_FALSE(vs.allViewsReady());
    // drive rebuild to the tail under a hard cap
    for (int i=0;i<64 && !vs.allViewsReady(); ++i) vs.driveRebuild(/*nowNs*/i);
    EXPECT_TRUE(vs.allViewsReady());
    EXPECT_EQ(vs.readiness(0x40000007), ViewReadiness::Ready);
}
TEST(ViewReadinessRegression, ColdAllNodeRestartReachesReadyFromLocalStateOnly) {
    // no peers wired; the gate must still reach Ready purely from local checkpoint+log replay.
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement the state machine + startup load/replay + the election/serve guards.**
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'ViewReadiness' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/node
git commit -m "feat(view): restart readiness gate (RECOVERING->REBUILDING_VIEWS->READY), local-only + bounded"
```

---

## Task 17: Durable view catalog (`ViewRegistry` + `__views` internal partition)

**Files:**
- Create: `libs/node/include/rune/node/ViewRegistry.h`, `libs/node/src/ViewRegistry.cpp`
- Create: `libs/node/tests/node/ViewRegistryTest.cpp`
- Modify: `libs/node/include/rune/node/SchemaRegistry.h` — add `inline constexpr int32_t kViewsTopicId = -5;` next to `kSchemaTopicId = -4`.
- Modify: `libs/node/src/BrokerNode.cpp` — create the `__views` internal `PartitionReplica` alongside `__topic_schemas`; replay it into `ViewRegistry` at startup.
- Modify: `libs/node/CMakeLists.txt`

**Interfaces:** (mirror `SchemaRegistry`/`TopicSchema`)
- Produces:
  - `struct ViewRecord { int32_t viewTopicId; std::string viewName; std::vector<uint8_t> compiledPlanBlob; uint8_t persistence; std::vector<uint8_t> serialize() const; static std::optional<ViewRecord> deserialize(std::span<const uint8_t>); };` (the `compiledPlanBlob` is Task 8's `serialize(CompiledViewPlan)`).
  - `class rune::node::ViewRegistry { void applyRecord(const ViewRecord&); const ViewRecord* byName(std::string_view) const; const ViewRecord* byViewTopicId(int32_t) const; void rebuildFromRecords(std::span<const ViewRecord>); std::vector<const ViewRecord*> all() const; };` — admin-thread-owned, no locks.

- [ ] **Step 1: Write the failing test**

```cpp
TEST(ViewRegistry, AppendLookupAndRebuild) {
    ViewRegistry r;
    ViewRecord v{0x40000007,"char_hp", serialize(/*compiled plan*/), 0};
    r.applyRecord(v);
    ASSERT_NE(r.byName("char_hp"), nullptr);
    EXPECT_EQ(r.byName("char_hp")->viewTopicId, 0x40000007);
    // round-trip a rebuild from serialized records
    auto blob = v.serialize(); auto back = ViewRecord::deserialize(blob);
    ASSERT_TRUE(back.has_value());
    ViewRegistry r2; r2.rebuildFromRecords(std::array{*back});
    EXPECT_NE(r2.byViewTopicId(0x40000007), nullptr);
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement `ViewRecord` + `ViewRegistry`**, add `kViewsTopicId`, and wire the `__views` replica create + startup replay in `BrokerNode` (follow the `__topic_schemas` pattern around `BrokerNode.cpp` internal-partition setup).
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'ViewRegistry' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/node CMakeLists.txt
git commit -m "feat(view): durable __views catalog (ViewRegistry mirrors SchemaRegistry) + startup replay"
```

---

## Task 18: Wire protocol messages — `CreateView` + `GetViewRow`

**Files (sibling repo `../rune-protocol`):**
- Modify: `include/rune/protocol/client/SchemaMessages.h` — add `CreateView`/`CreateViewResponse`.
- Modify: `include/rune/protocol/client/DataMessages.h` — add `GetViewRow`/`ViewRowResponse`.
- Modify: `include/rune/protocol/client/Protocol.h` — template ids + error codes.
- Create: `tests/...` in rune-protocol following its decode/encode round-trip test convention.

**Interfaces:**
- Produces:
  - `struct CreateView` (TEMPLATE_ID 68): body `viewName:varstr, keyField:varstr, persistence:u8, planBlob:varbytes32` (the authoring `ViewPlan` via `serializeAuthored`), where the single source's `topicId`+`keyField`, the `output` fields, and the `ViewSizing` knobs (`chunkBytes`/`capacity`/`maxRows`) all ride in `planBlob`. `CreateViewResponse` (69): `viewTopicId:i32, outputFingerprint:u64, errorCode:u16`.
  - `struct GetViewRow` (TEMPLATE_ID 70): body `viewTopicId:i32, keyU64:u64, requestedFingerprint:u64`. `ViewRowResponse` (71): `status:u16 (0=OK,1=AbsentKey,2=NotReady,3=SchemaMismatch,4=UnknownView), rowLen:u32, rowBytes:varbytes32`.
  - `Protocol.h`: `CreateView=68, CreateViewResponse=69, GetViewRow=70, ViewRowResponse=71`; error codes `ViewExists=26, ViewSourceNotFound=27, ViewFieldUnresolved=28, ViewKeyInvalid=29` (next after the existing `TopicExists=25`).

- [ ] **Step 1: Write the failing round-trip tests** (one per message: `encode` into a buffer, `decode`, assert field equality — mirror the existing `DefineTopic` message test).
- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement the four messages + ids/codes.**
- [ ] **Step 4: Run** the protocol repo's test target for these messages; expected PASS.
- [ ] **Step 5: Commit** (in `../rune-protocol`)

```bash
git -C ../rune-protocol add include/rune/protocol/client/SchemaMessages.h include/rune/protocol/client/DataMessages.h include/rune/protocol/client/Protocol.h tests/
git -C ../rune-protocol commit -m "feat(protocol): CreateView/CreateViewResponse + GetViewRow/ViewRowResponse messages"
```

---

## Task 19: `CreateView` control plane (AdminWorker DDL chain)

**Files:**
- Modify: `libs/node/include/rune/node/AdminChannels.h` — add `CreateViewMsg` + a ring msg-type id (next after `kMsgAlterTopic=7`) + `encode/decodeCreateView`; extend `CreateTopicReqMsg` with `keyField` + `cleanupPolicy` (so the view sink is a keyed+`Compact` table).
- Modify: `libs/node/include/rune/node/AdminWorker.h`/`src/AdminWorker.cpp` — `DdlKind::CreateView`; `submitCreateView(CreateViewMsg)`; in `startNext_` resolve+compile (via `reg_` SchemaRegistry + `compileView`), validate, `deps_.requestCreateTopic(...)` extended with keyField+Compact for the sink, append a `ViewRecord` to `__views`, `advance_` commit, `respondCreateView`; fan out the compiled plan to the owning worker(s) via the existing schema-fanout mechanism (`deps_.pushViewPlan(...)` → control thread → `WorkerViewSet::installView`).
- Modify: `libs/node/src/BrokerNode.cpp` — the `requestCreateTopic` lambda + `serviceAdminControl_` CreateTopic handler to carry keyField+cleanupPolicy into the authoritative `TopicSpec`; add `pushViewPlan` fanout into `WorkerViewSet`.
- Modify: `libs/gateway/include/rune/gateway/IAdminRequestSink.h` + `libs/gateway/src/ClientGateway.cpp` — `submitCreateView(...)` sink method, `handleCreateView` + the `pc::TemplateId::CreateView` dispatch case.
- Modify: `libs/node/include/rune/node/BrokerNode.h` (`AdminWorkerAgent`) — `submitCreateView` override encoding onto `requestRing_`, drained in `doWork`.
- Create: `libs/node/tests/integration/CreateViewRegressionTest.cpp`

**Interfaces:**
- Consumes: all of Tasks 9, 15, 17; the DefineTopic chain (mirror every hop).
- Produces: a fully wired `CreateView` that validates co-partition-trivially (single source), creates the keyed+Compact sink topic through `TopicRegistry::createTopic` (RULING-1), persists the view to `__views`, installs the compiled plan on the owning worker, and responds with `{viewTopicId, outputFingerprint}`.

- [ ] **Step 1: Write the failing integration test** (in-process broker harness, mirror `tests/integration` style)

```cpp
TEST(CreateViewRegression, CreateViewPersistsCompilesAndInstalls) {
    // bring up an in-process single node; DefineTopic "character" (keyed, Compact);
    // CreateView char_hp over it (Replace id,hp); expect OK + a viewTopicId;
    // verify ViewRegistry.byName("char_hp") exists and the owning worker has the view installed;
    // CreateView again -> ViewExists error.
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement the full chain**, hop by hop, mirroring `DefineTopic` (see the spec/plan references: gateway dispatch → sink → requestRing → AdminWorker `startNext_`/`advance_` → createReqRing → `TopicRegistry::createTopic` → reply → `__views` append → commit → respond → fanout install).
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'CreateView' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/node libs/gateway
git commit -m "feat(view): CreateView control plane (AdminWorker DDL chain, keyed+Compact sink, fanout install)"
```

---

## Task 20: `GetViewRow` read path + `openView` handshake

**Files:**
- Modify: `libs/node/include/rune/node/WorkerOp.h` — add `WorkerOp::Type::GetViewRow` (fields `viewTopicId`, `keyU64`, `requestedFingerprint`) + `WorkerReturn::Type::ViewRowResult` (fields `status`, `rowLen`, inline/ref row bytes).
- Modify: `libs/node/include/rune/node/Worker.h` — `case GetViewRow: executeGetViewRowWire_(op);` → probe `viewSet_.getRow(...)`, apply the readiness guard (not-`Ready` → `status=NotReady`), absent key → `status=AbsentKey`, fingerprint mismatch → `status=SchemaMismatch`; `enqueueReturn_`.
- Modify: `libs/gateway/src/ClientGateway.cpp` — build the `GetViewRow` `WorkerOp` from the wire `GetViewRow` (route via `tryEnqueueOp(viewTopicId, partitionForKey(viewTopicId, key), op)`), and in the `drainAllReturnsView` lambda encode `ViewRowResponse`. Add `openView`: resolve view name→`viewTopicId` + output descriptor via the existing `GetTopicSchema`/`byFingerprint` handshake (the view output is a registered typed topic), returning the fingerprint the client pins.
- Create: `libs/node/tests/integration/GetViewRowRegressionTest.cpp`

**Interfaces:**
- Consumes: Tasks 15, 16, 19; the `PushRead` op/return plumbing (`Worker.h:3032-3076`, `WorkerPool::tryEnqueueOp`, `ClientGateway.cpp:4001`/`:4779`).
- Produces: the full point-read path; `openView` resolves+handshakes; transient and persisted views read identically.

- [ ] **Step 1: Write the failing tests** (Review-focus #5)

```cpp
TEST(GetViewRowRegression, GetViewRowAbsentKeyReturnsTypedAbsent) {
    // view exists + Ready; GetViewRow for an unknown key -> status=AbsentKey, not a zero row.
}
TEST(GetViewRowRegression, GetViewRowReturnsCurrentRowAfterFold) {
    // produce id=7 hp=50 to the source; drive folds; GetViewRow(view,7) -> OK, hp=50.
}
TEST(GetViewRowRegression, OpenViewFingerprintMismatchRejected) {
    // openView with a stale fingerprint -> SchemaMismatch at open (drift caught up front).
}
```

- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement** the op/return, worker handler, gateway build/consume, and `openView`.
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build-fast -R 'GetViewRow' --output-on-failure`.
- [ ] **Step 5: Commit**

```bash
git add libs/node libs/gateway
git commit -m "feat(view): GetViewRow point-read path + openView fingerprint handshake (typed absent/not-ready)"
```

---

## Task 21: Client API (`rune-client-cpp`) + docker e2e + regression gate

**Files:**
- Modify (sibling `../rune-client-cpp`): add `openView(name) -> TypedView<RowT>` / a prepared `getViewRow(viewTopicId, keyU64) -> Row` with `col<T>()` handles reusing the fingerprint handshake; follow the shipped `openTopicAndWait<T>` pattern.
- Create: `docker/view-engine-e2e.sh` (modeled on `docker/schema-evolution-e2e.sh`; uses `docker/docker-compose.single.yml`).
- Create: a client probe source (e.g. `../rune-client-cpp/examples/view_probe.cpp`) the e2e drives, printing the sentinel `VIEW-ENGINE-E2E: OK`.
- Modify: `../rune-client-cpp/CMakeLists.txt` + a `Dockerfile` build context if the probe needs a new target.

**Interfaces:**
- Consumes: Tasks 18, 20 (wire messages + read path).
- Produces: an end-to-end green: `CREATE VIEW` → produce typed records → `GetViewRow` returns the folded row → restart the broker → readiness gate replays → `GetViewRow` returns the recovered row. Covers both a **persisted** and a **transient** view (transient returns empty-then-refilled after restart, asserted).

- [ ] **Step 1: Write the client typed read + the e2e script.** The script must (per the `schema-evolution-e2e.sh` template): `set -euo pipefail`; unique `COMPOSE_PROJECT` + `RUNE_IMAGE_TAG`; preflight sibling dirs; trap `teardown`; build broker image capturing its own exit code (no `| grep || true`); build the client probe image with `--build-context` for each sibling; `up -d rune-1`; await `node N is LEADER` by bounded log poll; run the probe container; assert `client_rc==0` **and** `grep -q 'VIEW-ENGINE-E2E: OK'`.
- [ ] **Step 2: Run the e2e** — **on the k3s-node-01 VM** (`ssh root@178.104.16.102`, respecting `/root/vm.lock`), per the standing rule that e2e runs on the VM:
      `bash docker/view-engine-e2e.sh`
      Expected: `VIEW-ENGINE-E2E: OK` and a green summary banner.
- [ ] **Step 3: Run the FULL regression suite first, then the rest** (CLAUDE.md #7):
      `ctest --test-dir build-fast -R Regression --output-on-failure` then `scripts/run-broker-tests.sh build-fast`.
      Expected: all green, including every `*Regression` suite added in Tasks 5/9/12/16/19/20.
- [ ] **Step 4: Record** — `scripts/rune-live.sh test "rune-broker/view-engine-e2e" passed total=1 commit=$(git rev-parse --short HEAD)`.
- [ ] **Step 5: Commit** (broker repo + client repo)

```bash
git add docker/view-engine-e2e.sh
git commit -m "test(view): docker view-engine e2e (define->produce->read->restart->recover) + regression gate"
git -C ../rune-client-cpp add -A
git -C ../rune-client-cpp commit -m "feat(client): openView + typed getViewRow + view_probe for e2e"
```

---

## Notes for the executor

- **Branch:** the broker worktree currently reports branch `feat/schema-evolution-durable-tags` (the shipped prerequisite), not `feature/materialized-views`. Confirm the intended Phase 1 base branch with the user before Task 1 and branch from it; the prerequisite is merged there.
- **No atomics in `libs/**`:** every new file under `libs/view` and `libs/node` must pass `scripts/check_no_atomics.py` — use the existing ring types for all cross-thread handoff; the pre-provision (Task 5) and all per-view state are single-worker-owned.
- **E2E on the VM:** Task 21's docker e2e runs on k3s-node-01 (`ssh root@178.104.16.102`), honoring `/root/vm.lock`, per the standing project rule for real-Aeron e2e.
- **`ALTER TOPIC DROP` of a tag a live view KEYS on (§6.3/§15 #9):** Phase 1 validates the key tag exists at `CreateView` (Task 9), but the *reverse* guard — rejecting an `ALTER TOPIC DROP` of a tag some live view uses as its key — requires the existing `ALTER TOPIC` path (`AdminWorker::startAlter_`) to consult `ViewRegistry`. That cross-path hook is a **small Phase 1.5 follow-on**, not in this plan; until it lands, a projected/merged field drop degrades to null correctly (Task 10), but dropping a keyed tag is not yet blocked at the source side.
- **Deferred to later phases (own plans):** aggregates+retraction+nulls (Phase 2); co-partitioned joins §8 (Phase 3); acks/consistency/`sendAndWait`/flow-control, multi-node placement/failover (Phase 4); `ALTER VIEW` live rebuild; multi-node `__views` replication; full read-time query engine (range/filter/`GROUP BY`) + SQL translator.
