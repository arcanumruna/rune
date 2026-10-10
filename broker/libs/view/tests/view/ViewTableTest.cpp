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
