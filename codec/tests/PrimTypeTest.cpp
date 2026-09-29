// tests/PrimTypeTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/PrimType.h>
using rune::codec::PrimType; using rune::codec::fixedSize; using rune::codec::isVarLen;
TEST(PrimType, FixedSizes) {
  EXPECT_EQ(fixedSize(PrimType::Int32), 4u);
  EXPECT_EQ(fixedSize(PrimType::Int64), 8u);
  EXPECT_EQ(fixedSize(PrimType::UInt16), 2u);
  EXPECT_EQ(fixedSize(PrimType::Float64), 8u);
  EXPECT_EQ(fixedSize(PrimType::Bool), 1u);
  EXPECT_EQ(fixedSize(PrimType::Enum), 4u);            // int32-backed
  EXPECT_TRUE(isVarLen(PrimType::String));
  EXPECT_TRUE(isVarLen(PrimType::Bytes));
  EXPECT_EQ(fixedSize(PrimType::String), 0u);
}
