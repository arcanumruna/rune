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
