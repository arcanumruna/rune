// Phase 2c Task 4: the runtime FilterBuilder (spec §4) — error reporting; equality with E::Filter is in
// FilterGeneratedTest.
#include "FilterTestSupport.h"

#include <rune/codec/FilterBuilder.h>
#include <rune/codec/FilterSchema.h>

#include <gtest/gtest.h>

using namespace rune::codec;
using K = FilterErrorKind;

namespace {
const Descriptor& pe() { static const Descriptor d = filtertest::playerEvent(1); return d; }
FilterBuilder b() { return FilterBuilder::on(pe()); }
}  // namespace

TEST(FilterBuilder, UnknownNameIsAnError) {
    const auto r = b().field("worldId").eq(int64_t{1}).field("nope").eq(int64_t{1}).build();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, K::UnknownField);
    EXPECT_EQ(r.error().termIndex, 1);
}
TEST(FilterBuilder, FloatAndStringFieldsAreErrors) {
    EXPECT_EQ(b().field("pos").eq(int64_t{1}).build().error().kind, K::FloatField);
    EXPECT_EQ(b().field("name").eq(std::string_view("x")).build().error().kind, K::VariableLength);
}
TEST(FilterBuilderRegression, OutOfRangeLiteralIsAnErrorNotTruncatedRegression) {
    const auto r = b().field("worldId").eq(int64_t{-1}).build();   // uint32 field
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, K::ValueOutOfRange);
    EXPECT_EQ(r.error().tag, 1);
    EXPECT_EQ(b().field("worldId").eq(uint64_t{1} << 32).build().error().kind, K::ValueOutOfRange);
    EXPECT_TRUE(b().field("worldId").eq(uint64_t{0xFFFFFFFFu}).build().has_value());
}
TEST(FilterBuilder, LiteralTypeMismatchesAreErrors) {
    EXPECT_EQ(b().field("worldId").eq(true).build().error().kind, K::TypeMismatch);
    EXPECT_EQ(b().field("worldId").eq(std::string_view("7")).build().error().kind, K::TypeMismatch);
    EXPECT_EQ(b().field("zone").eq(int64_t{7}).build().error().kind, K::TypeMismatch);
    EXPECT_EQ(b().field("alive").eq(int64_t{1}).build().error().kind, K::TypeMismatch);
}
TEST(FilterBuilder, FixedStrLiteralLongerThanTheFieldIsAnError) {
    EXPECT_EQ(b().field("zone").eq(std::string_view("123456789")).build().error().kind, K::ValueOutOfRange);
    EXPECT_TRUE(b().field("zone").eq(std::string_view("12345678")).build().has_value());
}
TEST(FilterBuilder, NineTermsIsAnError) {
    DescriptorBuilder nb("nine");
    for (int i = 0; i < 9; ++i) nb.add("f" + std::to_string(i), PrimType::UInt32);
    const Descriptor d = nb.build();
    auto fb = FilterBuilder::on(d);
    for (int i = 0; i < 9; ++i) fb.field("f" + std::to_string(i)).eq(int64_t{1});
    const auto r = fb.build();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, K::TooManyTerms);
    // a tenth field is also reported, never silently dropped
    fb.field("f0").eq(int64_t{1});
    EXPECT_EQ(fb.build().error().kind, K::TooManyTerms);
}
TEST(FilterBuilder, NoTermsIsNoFilterAndTermWithoutValueIsAnError) {
    EXPECT_TRUE(b().build()->empty());
    EXPECT_EQ(b().field("worldId").build().error().kind, K::ValueCount);
    EXPECT_EQ(b().eq(int64_t{1}).build().error().kind, K::Malformed);
}
TEST(FilterBuilder, InValuesAreSortedDeduplicatedAndParse) {
    const auto r = b().field("channel").in({int64_t{3}, int64_t{1}, int64_t{3}, int64_t{2}}).build();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->len, 2 + 5 + 3 * 4);
    EXPECT_TRUE(parseFilter(r->span(), FilterSchema::build(pe(), 1)).has_value());
}
TEST(FilterBuilder, TooManyInValuesAndRepeatedFieldAreErrors) {
    std::vector<FilterLiteral> many(65, FilterLiteral{int64_t{1}});
    EXPECT_EQ(b().field("channel").in(std::span<const FilterLiteral>(many)).build().error().kind, K::TooManyValues);
    EXPECT_EQ(b().field("channel").eq(int64_t{1}).field("channel").eq(int64_t{2}).build().error().kind, K::NotCanonical);
}

TEST(FilterBuilderRegression, UnsignedAndPlainIntegerLiteralsCompileAndRangeCheckRegression) {
    const uint32_t w = 42;
    const uint16_t narrow = 7;
    const auto a = b().field("worldId").eq(w).build();
    const auto c = b().field("worldId").eq(42u).build();
    const auto d = b().field("worldId").eq(int64_t{42}).build();
    ASSERT_TRUE(a.has_value() && c.has_value() && d.has_value());
    EXPECT_EQ(*a, *d);
    EXPECT_EQ(*c, *d);
    EXPECT_TRUE(b().field("worldId").eq(narrow).build().has_value());
    EXPECT_EQ(b().field("worldId").eq(-1).build().error().kind, K::ValueOutOfRange);   // plain int, signed
    const auto i1 = b().field("channel").in({3u, 1u, 2u}).build();
    const auto i2 = b().field("channel").in({int64_t{1}, int64_t{2}, int64_t{3}}).build();
    ASSERT_TRUE(i1.has_value() && i2.has_value());
    EXPECT_EQ(*i1, *i2);
    EXPECT_EQ(b().field("worldId").in({1ull, 0xFFFFFFFFFull}).build().error().kind, K::ValueOutOfRange);
    EXPECT_EQ(b().field("zone").eq(7u).build().error().kind, K::TypeMismatch);
}
TEST(FilterBuilderRegression, EqOnABoolFieldStillTakesABoolRegression) {
    EXPECT_TRUE(b().field("alive").eq(true).build().has_value());
    EXPECT_EQ(b().field("alive").eq(1).build().error().kind, K::TypeMismatch);
}
