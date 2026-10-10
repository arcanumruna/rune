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
