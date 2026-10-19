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
