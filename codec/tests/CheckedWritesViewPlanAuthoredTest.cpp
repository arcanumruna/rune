// tests/CheckedWritesViewPlanAuthoredTest.cpp
#include "rune/codec/ViewPlanAuthored.h"
#include "rune/codec/UnicodeLower.h"
#include "rune/codec/ViewKeyEncoding.h"
#include <gtest/gtest.h>
#include <cstring>
#include <vector>

using namespace rune::view;
using rune::codec::PrimType;

namespace {

// The plan of broker ViewPlanTest.AuthoredRoundTrip.
ViewPlan v1Plan() {
    ViewPlan p;
    p.viewName = "char_hp";
    p.key = "charId";
    p.sources = {{5, "id"}, {6, "ownerId"}};
    p.output = {{"hp", PrimType::Int64, 0, "hp", MergeRule::Replace},
                {"kills", PrimType::Int32, 1, "kills", MergeRule::Sum}};
    p.persistence = 1;
    p.sizing.chunkBytes = 4u << 20;
    p.sizing.capacity = 128;
    p.sizing.maxRows = 42;
    return p;
}

PredicateNode node(PredicateOp op, uint16_t a = 0, uint16_t b = 0, std::string column = {}) {
    PredicateNode n{};
    n.op = op; n.a = a; n.b = b; n.column = std::move(column);
    return n;
}

PredicateNode constU64(uint64_t v) {
    PredicateNode n = node(PredicateOp::Const);
    n.constType = PrimType::UInt64;
    std::memcpy(n.constBytes.data(), &v, sizeof v);
    return n;
}

PredicateNode constI64(int64_t v) {
    PredicateNode n = node(PredicateOp::Const);
    n.constType = PrimType::Int64;
    std::memcpy(n.constBytes.data(), &v, sizeof v);
    return n;
}

ViewPlan v2Plan() {
    ViewPlan p = v1Plan();
    p.key = "userId";
    p.keyComponents = {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}};
    p.unique = true;
    p.constraints = {
        {"characters_le_12", {node(PredicateOp::Column, 0, 0, "characters"), constU64(12), node(PredicateOp::Le, 0, 1)}},
        {"balance_not_negative", {node(PredicateOp::Column, 0, 0, "balance"), constI64(0), node(PredicateOp::Lt, 0, 1), node(PredicateOp::Not, 2)}}};
    p.unicodeVersion = rune::codec::kUnicodeVersion;
    return p;
}

void expectEqual(const ViewPlan& a, const ViewPlan& b) {
    EXPECT_EQ(a.viewName, b.viewName);
    EXPECT_EQ(a.key, b.key);
    ASSERT_EQ(a.sources.size(), b.sources.size());
    for (size_t i = 0; i < a.sources.size(); ++i) {
        EXPECT_EQ(a.sources[i].topicId, b.sources[i].topicId);
        EXPECT_EQ(a.sources[i].keyField, b.sources[i].keyField);
    }
    ASSERT_EQ(a.output.size(), b.output.size());
    for (size_t i = 0; i < a.output.size(); ++i) {
        EXPECT_EQ(a.output[i].name, b.output[i].name);
        EXPECT_EQ(a.output[i].type, b.output[i].type);
        EXPECT_EQ(a.output[i].sourceIdx, b.output[i].sourceIdx);
        EXPECT_EQ(a.output[i].srcField, b.output[i].srcField);
        EXPECT_EQ(a.output[i].rule, b.output[i].rule);
    }
    EXPECT_EQ(a.persistence, b.persistence);
    EXPECT_EQ(a.sizing.chunkBytes, b.sizing.chunkBytes);
    EXPECT_EQ(a.sizing.capacity, b.sizing.capacity);
    EXPECT_EQ(a.sizing.maxRows, b.sizing.maxRows);
    EXPECT_EQ(a.keyComponents, b.keyComponents);
    EXPECT_EQ(a.unique, b.unique);
    EXPECT_EQ(a.constraints, b.constraints);
    EXPECT_EQ(a.unicodeVersion, b.unicodeVersion);
    EXPECT_EQ(a.groupBy, b.groupBy);
}

} // namespace

TEST(CheckedWritesViewPlanAuthored, V1ShapedPlanSerializesToTheV1Bytes) {
    // Captured from the pre-move broker rune::view::serializeAuthored on v1Plan().
    const std::vector<uint8_t> golden = {
        0x01, 0x07, 0x00, 0x00, 0x00, 0x63, 0x68, 0x61, 0x72, 0x5f, 0x68, 0x70,
        0x06, 0x00, 0x00, 0x00, 0x63, 0x68, 0x61, 0x72, 0x49, 0x64, 0x02, 0x00,
        0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x69, 0x64,
        0x06, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x6f, 0x77, 0x6e, 0x65,
        0x72, 0x49, 0x64, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x68,
        0x70, 0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x68, 0x70, 0x00, 0x05,
        0x00, 0x00, 0x00, 0x6b, 0x69, 0x6c, 0x6c, 0x73, 0x00, 0x01, 0x00, 0x05,
        0x00, 0x00, 0x00, 0x6b, 0x69, 0x6c, 0x6c, 0x73, 0x02, 0x01, 0x00, 0x00,
        0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00};
    ViewPlan p = v1Plan();
    EXPECT_EQ(serializeAuthored(p), golden);
    EXPECT_EQ(golden[0], 0x01);

    p.key = "charId";
    p.keyComponents = {{"charId", 0, "charId"}};
    EXPECT_FALSE(needsV2(p));
    EXPECT_EQ(serializeAuthored(p), golden);
    p.key.clear();   // a lone plain component rides as key = column
    EXPECT_EQ(serializeAuthored(p), golden);
}

TEST(CheckedWritesViewPlanAuthored, V2RoundTripsEveryField) {
    const ViewPlan p = v2Plan();
    const auto bytes = serializeAuthored(p);
    ASSERT_EQ(bytes[0], 0x02);
    const auto back = deserializeAuthored(bytes);
    ASSERT_TRUE(back.has_value());
    expectEqual(p, *back);
    EXPECT_EQ(serializeAuthored(*back), bytes);
}

TEST(CheckedWritesViewPlanAuthored, GroupByRoundTrips) {
    ViewPlan p;
    p.viewName = "device_history";
    p.sources = {{7, "userId"}};
    p.keyComponents = {{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}};
    p.groupBy = p.keyComponents;
    p.output = {{"userId", PrimType::UInt64, 0, "userId", MergeRule::Replace},
                {"deviceFp", PrimType::UInt64, 0, "deviceFp", MergeRule::Replace},
                {"n", PrimType::UInt64, 0, "", MergeRule::CountAll},
                {"lastSeen", PrimType::Int64, 0, "lastSeen", MergeRule::Max}};
    const auto bytes = serializeAuthored(p);
    ASSERT_EQ(bytes[0], 0x02);
    const auto back = deserializeAuthored(bytes);
    ASSERT_TRUE(back.has_value());
    expectEqual(p, *back);
}

TEST(CheckedWritesViewPlanAuthored, LowerComponentForcesV2) {
    ViewPlan p = v1Plan();
    p.keyComponents = {{"name", rune::codec::kKeyFunctionLower, "nameKey"}};
    EXPECT_TRUE(needsV2(p));
    EXPECT_EQ(serializeAuthored(p)[0], 0x02);
}

TEST(CheckedWritesViewPlanAuthored, EveryTruncationIsRefused) {
    const auto bytes = serializeAuthored(v2Plan());
    for (size_t n = 0; n < bytes.size(); ++n)
        EXPECT_FALSE(deserializeAuthored(std::span<const uint8_t>(bytes.data(), n)).has_value()) << "prefix " << n;
}

TEST(CheckedWritesViewPlanAuthoredRegression, TrailingBytesAreRefusedRegression) {
    for (const ViewPlan& p : {v1Plan(), v2Plan()}) {
        auto bytes = serializeAuthored(p);
        ASSERT_TRUE(deserializeAuthored(bytes).has_value());
        bytes.push_back(0);
        EXPECT_FALSE(deserializeAuthored(bytes).has_value());
    }
}

TEST(CheckedWritesViewPlanAuthored, BadEnumsAreRefused) {
    ViewPlan p = v2Plan();
    p.constraints = {{"c", {node(PredicateOp::Column, 0, 0, "x")}}};
    p.keyComponents = {{"userId", 0, "userId"}};
    p.groupBy.clear();
    auto bytes = serializeAuthored(p);
    ASSERT_TRUE(deserializeAuthored(bytes).has_value());

    // Tail: unique:u8, constraints count:u32, name(4+1), nodes count:u32, node{op:u8, a:u16, b:u16, column(4+1), constType:u8, constBytes:16}, unicodeVersion:u32, groupBy count:u32
    const size_t constTypeAt = bytes.size() - 4 - 4 - 16 - 1;
    const size_t opAt = constTypeAt - (1 + 2 + 2 + 5);
    ASSERT_EQ(bytes[opAt], static_cast<uint8_t>(PredicateOp::Column));
    const size_t uniqueAt = opAt - (4 + 5 + 4 + 1);
    ASSERT_EQ(bytes[uniqueAt], 1);

    for (uint8_t bad : {uint8_t{0}, uint8_t{12}}) {
        auto b = bytes;
        b[opAt] = bad;
        EXPECT_FALSE(deserializeAuthored(b).has_value()) << "op " << int(bad);
    }
    {
        auto b = bytes;
        b[uniqueAt] = 2;
        EXPECT_FALSE(deserializeAuthored(b).has_value());
    }
    {
        auto b = bytes;
        b[constTypeAt] = 11;
        EXPECT_FALSE(deserializeAuthored(b).has_value());
    }
    {   // keyFunction is a raw byte: any value decodes
        ViewPlan q = p;
        q.keyComponents = {{"name", 200, "nameKey"}};
        const auto back = deserializeAuthored(serializeAuthored(q));
        ASSERT_TRUE(back.has_value());
        EXPECT_EQ(back->keyComponents[0].keyFunction, 200);
    }
}

TEST(CheckedWritesViewPlanAuthored, KeyDisplayName) {
    EXPECT_EQ(keyDisplayName(v2Plan()), "userId,characterId");
    ViewPlan p = v1Plan();
    p.keyComponents = {{"name", rune::codec::kKeyFunctionLower, "nameKey"}};
    EXPECT_EQ(keyDisplayName(p), "nameKey");
    EXPECT_EQ(keyDisplayName(v1Plan()), "charId");
    p = v1Plan();
    p.key = "id";
    p.keyComponents = {{"id", 0, "id"}};
    EXPECT_EQ(keyDisplayName(p), "id");
}

TEST(CheckedWritesViewPlanAuthored, AggregateOutputTypes) {
    using rune::codec::PrimType;
    EXPECT_EQ(aggregateOutputType(MergeRule::Max, PrimType::UInt16), PrimType::UInt16);
    EXPECT_EQ(aggregateOutputType(MergeRule::Max, PrimType::Bool), std::nullopt);
    EXPECT_EQ(aggregateOutputType(MergeRule::LastNonNull, PrimType::FixedStr), PrimType::FixedStr);
    EXPECT_EQ(aggregateOutputType(MergeRule::Replace, PrimType::Int64), std::nullopt);
}
