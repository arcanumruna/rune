#include <gtest/gtest.h>
#include "rune/ql/Parser.h"
using namespace rune::ql; using rune::codec::PrimType;

TEST(Parser, ParsesCharacters) {
    auto r = parse("topic characters { id int64, userId int64, name string, code string(8) } partitions 16 key userId;");
    ASSERT_TRUE(r.has_value());
    const TopicDef& t = *r;
    EXPECT_EQ(t.name, "characters");
    EXPECT_EQ(t.partitions, 16u);
    EXPECT_EQ(t.keyField, "userId");
    ASSERT_EQ(t.fields.size(), 4u);
    EXPECT_EQ(t.fields[0].type, PrimType::Int64);
    EXPECT_EQ(t.fields[2].type, PrimType::String);   // variable
    EXPECT_EQ(t.fields[3].type, PrimType::FixedStr);  // string(8)
    EXPECT_EQ(t.fields[3].fixedLen, 8u);
}
TEST(Parser, LineCommentsAndTrailingComma) {
    auto r = parse("-- a topic\ntopic t { a int32, } partitions 1 key a;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->fields.size(), 1u);
}
TEST(Parser, RejectsUnknownType) {
    auto r = parse("topic t { a int128 } partitions 1 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("type"), std::string::npos);
}
TEST(Parser, RejectsDuplicateField) {
    auto r = parse("topic t { a int32, a int64 } partitions 1 key a;");
    ASSERT_FALSE(r.has_value());
}
TEST(Parser, RejectsKeyNotAField) {
    auto r = parse("topic t { a int32 } partitions 1 key b;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("key"), std::string::npos);
}
TEST(Parser, RejectsZeroPartitionsAndZeroWidth) {
    EXPECT_FALSE(parse("topic t { a int32 } partitions 0 key a;").has_value());
    EXPECT_FALSE(parse("topic t { a string(0) } partitions 1 key a;").has_value());
}
TEST(Parser, ErrorHasLocation) {
    auto r = parse("topic t { a int32 }\npartitions x key a;");   // 'x' not an int
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 2u);
}

// ── Regression suite ─────────────────────────────────────────────────────────
// Guards against silent uint16_t truncation of string(N) width (review Important #1).
TEST(ParserRegression, RejectsStringWidthAbove65535) {
    // 65536 truncates to 0 under naive static_cast<uint16_t> — must be rejected.
    auto r0 = parse("topic t { a string(65536) } partitions 1 key a;");
    ASSERT_FALSE(r0.has_value());
    EXPECT_NE(r0.error().message.find("string"), std::string::npos);
    // 65537 truncates to 1 — must be rejected.
    auto r1 = parse("topic t { a string(65537) } partitions 1 key a;");
    ASSERT_FALSE(r1.has_value());
    // 65535 is the maximum valid width.
    auto rMax = parse("topic t { a string(65535) } partitions 1 key a;");
    ASSERT_TRUE(rMax.has_value());
    EXPECT_EQ(rMax->fields[0].type, rune::codec::PrimType::FixedStr);
    EXPECT_EQ(rMax->fields[0].fixedLen, 65535u);
}

// Guards against uint32_t truncation of partitions (review Important #1, applied to partitions).
TEST(ParserRegression, RejectsPartitionsAboveUint32Max) {
    auto r = parse("topic t { a int32 } partitions 4294967296 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("partitions"), std::string::npos);
}

// Guards against the "one token too far" location bug for partitions < 1 (review Minor #2).
TEST(ParserRegression, ZeroPartitionsErrorPointsAtZero) {
    // 'partitions 0' on line 2 — the '0' token is at line 2, col 12.
    auto r = parse("topic t { a int32 }\npartitions 0 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 2u);
    EXPECT_EQ(r.error().col, 12u);  // points at '0', not the following 'key'
}
