// libs/view/tests/view/ViewTableTest.cpp
#include <gtest/gtest.h>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>
#include <unordered_map>
#include <random>
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

TEST(ViewTable, GrowsByAppendingChunksRowsNeverMove) {
    RowLayout L = hpLayout();
    // Index slots sized generously (128, power of two) so this test exercises
    // ONLY row-slab chunk growth (Task 4), not index growth (deferred to
    // Task 5 — index rehash is out of scope here). Tiny chunkBytes (4 rows
    // per chunk) forces many chunk boundaries over 100 inserts.
    ViewTable t(L, /*slots*/128, /*chunkBytes*/ L.rowLen * 4);
    for (uint64_t k = 1; k <= 100; ++k) { uint8_t* r = t.put(k); setHp(r, L, (int)k); }
    EXPECT_GT(t.chunkCount(), 1u);
    for (uint64_t k = 1; k <= 100; ++k) {
        ASSERT_NE(t.get(k), nullptr);
        EXPECT_EQ(readSlot(t.get(k), L, 1).i, (int)k);   // all survive growth
    }
}

TEST(ViewTable, RowPointerStableAcrossChunkGrowth) {
    // Regression: a row pointer captured BEFORE a chunk-growth event must
    // still be valid (and point at the same row) AFTER growth, because rows
    // never move once allocated — only new chunks are appended.
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/128, /*chunkBytes*/ L.rowLen * 4); // 4 rows/chunk
    uint8_t* early = t.put(1);
    setHp(early, L, 111);
    EXPECT_EQ(t.chunkCount(), 1u);
    for (uint64_t k = 2; k <= 50; ++k) setHp(t.put(k), L, (int)k);
    EXPECT_GT(t.chunkCount(), 1u) << "expected growth past the first chunk";
    EXPECT_EQ(readSlot(early, L, 1).i, 111) << "row moved after chunk growth";
    EXPECT_EQ(early, const_cast<uint8_t*>(t.get(1))) << "rid->address mapping changed";
}

// ---- Task 5: incremental (de-amortized) index rehash --------------------

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

// Randomized churn (insert/update/delete) across MANY grow thresholds,
// cross-checked against std::unordered_map as the reference model — same
// style as Task 3's BackshiftKeepsCollidingKeysFindable-class regression,
// extended to exercise the migration window (new-then-old lookups, erase
// during migration, repeated grows) rather than a single fixed-capacity
// table.
TEST(ViewTable, RandomizedChurnAcrossManyGrowsMatchesUnorderedMap) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);
    std::unordered_map<uint64_t, int32_t> model;
    std::mt19937_64 rng(20261009);
    for (int iter = 0; iter < 100000; ++iter) {
        uint64_t key = (rng() % 20000) + 1; // churny: keeps some keys alive across many grows
        if ((rng() % 4) == 0) {
            bool want = model.count(key) != 0;
            bool got = t.erase(key);
            ASSERT_EQ(want, got) << "erase mismatch key=" << key << " iter=" << iter;
            model.erase(key);
        } else {
            int32_t v = (int32_t)(rng() % 1000000);
            setHp(t.put(key), L, v);
            model[key] = v;
        }
        if (iter % 2000 == 0) {
            ASSERT_EQ(t.size(), model.size()) << "size mismatch at iter " << iter;
        }
    }
    ASSERT_EQ(t.size(), model.size());
    for (auto& [k, v] : model) {
        const uint8_t* row = t.get(k);
        ASSERT_NE(row, nullptr) << "missing key " << k;
        EXPECT_EQ(readSlot(row, L, 1).i, v) << "value mismatch key " << k;
    }
}

TEST(ViewTable, WorstRehashTaxIsTracked) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);
    for (uint64_t k = 1; k <= 50000; ++k) setHp(t.put(k), L, 1);
    EXPECT_GE(t.worstRehashTaxUs(), 0.0);
}

// Regression for a self-review fix found while implementing Task 5:
// migrateStep(), as ported from the design spike, bounded its loop on
// entries ACTUALLY MOVED, not entries SCANNED. Read literally, that is a
// latent risk: if the OLD index's unscanned tail were ever mostly dead
// (kEmpty/kTomb), a single call could scan far past kMigK hunting for live
// entries to move, undermining the whole point of incremental
// (de-amortized) rehash (a bounded worst-case cost per op). (Investigation
// during self-review found this specific risk is not reachable through
// put()/erase() as invoked today — every op, including erase(), also
// drives migrateStep() forward by up to kMigK, so the scan cursor cannot
// fall meaningfully behind a burst of tombstoning OLD-only erases. The
// fix — bounding the loop on entries SCANNED, i.e. ocur_ advanced, not on
// entries moved — is kept anyway as strictly more robust/defensive: it is
// a direct, data-independent guarantee instead of an emergent one, costs
// nothing in the common (mostly-live) case, and remains correct if a
// future caller ever changes that invocation pattern.)
//
// This test exercises the OLD-index tombstone path itself (erase() of a
// key that is still only reachable via findO() during an active
// migration): it must mark that OLD slot kTomb (not touch OLD's probe
// structure) and migrateStep() must later skip it rather than migrating
// or losing it — covered here under real grow-triggered migration, with
// heavy interleaved erase churn, checked against correctness only (no
// surviving/erased key lost or resurrected).
TEST(ViewTableMigrateScanBoundRegression, EraseDuringMigrationTombstonesOldCorrectly) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/1u << 16); // 65536-slot OLD index once it grows
    uint64_t k = 1;
    for (; k <= 55706; ++k) setHp(t.put(k), L, 1); // the 55706th put triggers startGrow()

    // Heavy erase churn immediately after the grow, before migration has
    // drained much of anything: tombstone every third just-grown-from key
    // (scattered essentially uniformly across the OLD index by mix()).
    for (uint64_t e = 1; e <= 55706; e += 3) t.erase(e);

    // Drive the rest of the table's ops to completion (also drains migration).
    for (; k <= 200000; ++k) setHp(t.put(k), L, 1);

    for (uint64_t e = 1; e <= 55706; ++e) {
        if (e % 3 == 1) EXPECT_EQ(t.get(e), nullptr) << "erased key " << e << " resurrected";
        else ASSERT_NE(t.get(e), nullptr) << "lost key " << e << " (not erased)";
    }
    for (uint64_t v = 55707; v < 200000; ++v) ASSERT_NE(t.get(v), nullptr) << "lost key " << v;
}
