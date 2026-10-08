// tests/AlterOpsTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/AlterOps.h>

TEST(AlterOps, RoundTripAllOps) {
    using namespace rune::codec;
    std::vector<AlterOp> ops = {
        {AlterOpType::Add, "score", "", PrimType::Int32, 0, CompatMode::Backward},
        {AlterOpType::Drop, "age", "", PrimType::Int32, 0, CompatMode::Backward},
        {AlterOpType::Rename, "name", "fullName", PrimType::Int32, 0, CompatMode::Backward},
        {AlterOpType::SetCompatibility, "", "", PrimType::Int32, 0, CompatMode::Forward},
    };
    auto blob = encodeAlterOps(ops);
    auto got = decodeAlterOps(blob);
    ASSERT_TRUE(got.has_value());
    ASSERT_EQ(got->size(), 4u);
    EXPECT_EQ((*got)[2].newName, "fullName");
    EXPECT_EQ((*got)[3].mode, CompatMode::Forward);
}

TEST(AlterOps, TruncatedReturnsNullopt) {
    auto blob = rune::codec::encodeAlterOps({{rune::codec::AlterOpType::Drop,"age","",{},0,{}}});
    blob.resize(2);
    EXPECT_FALSE(rune::codec::decodeAlterOps(blob).has_value());
}

TEST(AlterOps, RoundTripAddPreservesPrimTypeAndFixedLen) {
    using namespace rune::codec;
    std::vector<AlterOp> ops = {
        {AlterOpType::Add, "tag", "", PrimType::FixedStr, 16, CompatMode::Backward},
    };
    auto blob = encodeAlterOps(ops);
    auto got = decodeAlterOps(blob);
    ASSERT_TRUE(got.has_value());
    ASSERT_EQ(got->size(), 1u);
    EXPECT_EQ((*got)[0].name, "tag");
    EXPECT_EQ((*got)[0].primType, PrimType::FixedStr);
    EXPECT_EQ((*got)[0].fixedLen, 16u);
}

TEST(AlterOps, EmptyOpsRoundTrips) {
    using namespace rune::codec;
    auto blob = encodeAlterOps({});
    auto got = decodeAlterOps(blob);
    ASSERT_TRUE(got.has_value());
    EXPECT_TRUE(got->empty());
}

TEST(AlterOps, TruncatedMidOpStringReturnsNullopt) {
    using namespace rune::codec;
    auto blob = encodeAlterOps({{AlterOpType::Rename, "name", "fullName", PrimType::Int32, 0, CompatMode::Backward}});
    // Chop off the last few bytes, cutting the "newName" string short.
    blob.resize(blob.size() - 3);
    EXPECT_FALSE(decodeAlterOps(blob).has_value());
}

TEST(AlterOps, EmptyBlobReturnsNullopt) {
    EXPECT_FALSE(rune::codec::decodeAlterOps(std::span<const uint8_t>{}).has_value());
}

TEST(AlterOps, UnknownOpTypeReturnsNullopt) {
    using namespace rune::codec;
    auto blob = encodeAlterOps({{AlterOpType::Drop, "age", "", PrimType::Int32, 0, CompatMode::Backward}});
    blob[2] = 0xFF; // corrupt the opType byte (opsCount=2 bytes, then opType)
    EXPECT_FALSE(decodeAlterOps(blob).has_value());
}
