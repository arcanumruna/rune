// FilterWireTest.cpp — Phase 2c Task 2: the filter encoding (spec §5), Match<T> / anyOf (spec §4).
#include <rune/codec/Filter.h>
#include <rune/codec/FilterWire.h>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <span>
#include <type_traits>
#include <algorithm>

using namespace rune::codec;
using rune::Match;
using rune::anyOf;

namespace {
template <class T> FilterBytes encodeOne(uint16_t tag, PrimType t, uint8_t w, const Match<T>& m) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(tag, t, w, m);
    auto r = wr.finish();
    EXPECT_TRUE(r.has_value());
    b.len = static_cast<uint16_t>(r.value_or(0));
    return b;
}
}  // namespace

TEST(FilterWireRegression, EqU32IsElevenBytes) {   // spec §5: worldId == x (u32) is 2 + 5 + 4
    const auto b = encodeOne<uint32_t>(1, PrimType::UInt32, 4, Match<uint32_t>(0x01020304u));
    ASSERT_EQ(b.len, 11);
    const std::array<uint8_t, 11> want{1, 1, /*tag*/ 1, 0, /*type*/ static_cast<uint8_t>(PrimType::UInt32),
                                       /*op Eq*/ 1, /*count*/ 1, 0x04, 0x03, 0x02, 0x01};
    EXPECT_TRUE(std::equal(want.begin(), want.end(), b.bytes.begin()));
}

TEST(FilterWireRegression, EmptyFilterIsZeroBytes) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(1, PrimType::UInt32, 4, Match<uint32_t>{});   // unset: no term
    auto r = wr.finish();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 0u);
}

TEST(FilterWireRegression, EqualFiltersEncodeIdenticallyRegression) {   // spec §5 canonical form
    const auto a = encodeOne<int32_t>(2, PrimType::Int32, 4, Match<int32_t>(anyOf(3, 1, 2, 1)));
    const auto b = encodeOne<int32_t>(2, PrimType::Int32, 4, Match<int32_t>(anyOf(1, 2, 3)));
    EXPECT_EQ(a, b) << "In values sorted and de-duplicated";
    EXPECT_EQ(a.bytes[6], 3) << "count after de-duplication";
}

TEST(FilterWireRegression, TermsMustBeWrittenInAscendingTagOrderRegression) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(5, PrimType::Int32, 4, Match<int32_t>(1));
    wr.term(2, PrimType::Int32, 4, Match<int32_t>(1));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::NotCanonical);
    EXPECT_EQ(r.error().termIndex, 1);
}

TEST(FilterWireRegression, NineTermsFailAtEncodeRegression) {   // spec §4 term limit
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    for (uint16_t t = 0; t < 9; ++t) wr.term(t, PrimType::Int32, 4, Match<int32_t>(1));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::TooManyTerms);
}

TEST(FilterWireRegression, RuntimeAnyOfOverSixtyFourFailsAtEncodeRegression) {
    std::array<int32_t, 65> v{};
    for (int i = 0; i < 65; ++i) v[static_cast<std::size_t>(i)] = i;
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(0, PrimType::Int32, 4, Match<int32_t>(anyOf(std::span<const int32_t>(v))));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::TooManyValues);
    EXPECT_EQ(r.error().termIndex, 0);
}

TEST(FilterMatchRegression, OutOfRangeValueFailsEncodeNeverTruncatesRegression) {   // Review Focus #5
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(1, PrimType::UInt32, 4, Match<uint32_t>(int64_t{-1}));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::ValueOutOfRange);
    FilterWriter wr2(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr2.term(1, PrimType::UInt32, 4, Match<uint32_t>(int64_t{0x1'0000'0000}));
    EXPECT_FALSE(wr2.finish().has_value());
}

TEST(FilterMatchRegression, CompileTimeTypeRules) {   // spec §4: a wrong value type does not compile
    static_assert(std::is_constructible_v<Match<uint32_t>, int>, "integer literals are accepted (range checked at encode)");
    static_assert(!std::is_constructible_v<Match<uint32_t>, double>, "a float is not an integer field value");
    static_assert(!std::is_constructible_v<Match<uint32_t>, const char*>, "a string is not an integer field value");
    static_assert(!std::is_constructible_v<Match<bool>, int>, "bool fields take bool");
    static_assert(!std::is_constructible_v<Match<int32_t>, bool>, "integer fields do not take bool");
    static_assert(std::is_constructible_v<Match<std::array<char, 8>>, const char (&)[3]>, "a short FixedStr literal");
    static_assert(!std::is_constructible_v<Match<std::array<char, 8>>, const char (&)[10]>, "a FixedStr literal longer than N");
    static_assert(std::is_trivially_copyable_v<Match<uint32_t>>, "E::Filter is a plain aggregate (spec §4)");
    SUCCEED();
}

TEST(FilterWireRegression, ConstexprEncodingOfAConstantFilter) {   // spec §4 cost: constexpr encoding possible
    constexpr auto b = [] {
        FilterBytes x;
        FilterWriter wr(std::span<uint8_t>(x.bytes.data(), x.bytes.size()));
        wr.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u));
        x.len = static_cast<uint16_t>(*wr.finish());
        return x;
    }();
    static_assert(b.len == 11);
    SUCCEED();
}

TEST(FilterWireRegression, FixedStrValueIsZeroPaddedToItsWidth) {
    const auto b = encodeOne<std::array<char, 8>>(3, PrimType::FixedStr, 8, Match<std::array<char, 8>>("eu"));
    ASSERT_EQ(b.len, 2 + 5 + 8);
    EXPECT_EQ(b.bytes[7], 'e'); EXPECT_EQ(b.bytes[8], 'u');
    for (int i = 9; i < 15; ++i) EXPECT_EQ(b.bytes[static_cast<std::size_t>(i)], 0);
}

TEST(FilterWireRegression, DescribeNamesTheTermIndex) {
    EXPECT_EQ(describe({FilterErrorKind::TypeMismatch, 2, 7}), "filter term 2 (tag 7): type mismatch");
    EXPECT_EQ(describe({FilterErrorKind::UntypedTopic, 0xFF, 0}), "filter needs a typed topic");
}

TEST(MatchRegression, AnErrorStateIsTerminalRegression) {
    std::array<uint32_t, 65> big{};
    Match<uint32_t> o(rune::AnyOfSpan<uint32_t>{std::span<const uint32_t>(big)});
    ASSERT_TRUE(o.overflow());
    o = 5u;
    EXPECT_TRUE(o.overflow());
    o = anyOf(1u, 2u);
    EXPECT_TRUE(o.overflow());
    Match<uint32_t> r;
    r = int64_t{-1};
    ASSERT_TRUE(r.outOfRange());
    r = 5u;
    EXPECT_TRUE(r.outOfRange());
    EXPECT_FALSE(r.overflow());
    Match<std::array<char, 4>> s(anyOf<const char*>("abcdefgh"));
    ASSERT_TRUE(s.outOfRange());
    s = "ab";
    EXPECT_TRUE(s.outOfRange());
    Match<uint32_t> ok;
    ok = 5u;
    EXPECT_EQ(ok.state(), Match<uint32_t>::State::Eq);
    ok = anyOf(1u, 2u);
    EXPECT_EQ(ok.state(), Match<uint32_t>::State::In);
}

TEST(FilterWireRegression, MultiTermGoldenBytesRegression) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(0x0102, PrimType::Int32, 4, Match<int32_t>(anyOf(-1, 1, 1)));
    wr.term(0x0203, PrimType::FixedStr, 4, Match<std::array<char, 4>>(anyOf<const char*>("ba", "ab", "ba")));
    wr.term(0x0304, PrimType::Bool, 1, Match<bool>(true));
    auto r = wr.finish();
    ASSERT_TRUE(r.has_value());
    const std::array<uint8_t, 2 + 13 + 13 + 6> want{
        0x01, 0x03,
        0x02, 0x01, 0x00, 0x02, 0x02, 0x01, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
        0x03, 0x02, 0x0a, 0x02, 0x02, 0x62, 0x61, 0x00, 0x00, 0x61, 0x62, 0x00, 0x00,
        0x04, 0x03, 0x06, 0x01, 0x01, 0x01};
    ASSERT_EQ(*r, want.size());
    EXPECT_TRUE(std::equal(want.begin(), want.end(), b.bytes.begin()));
}

TEST(FilterWireRegression, WideAndNarrowGoldenBytesRegression) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(1, PrimType::Int64, 8, Match<int64_t>(int64_t{-2}));
    wr.term(2, PrimType::UInt16, 2, Match<uint16_t>(uint16_t{0xABCD}));
    auto r = wr.finish();
    ASSERT_TRUE(r.has_value());
    const std::array<uint8_t, 2 + 13 + 7> want{
        0x01, 0x02,
        0x01, 0x00, 0x01, 0x01, 0x01, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0x02, 0x00, 0x02, 0x01, 0x01, 0xcd, 0xab};
    ASSERT_EQ(*r, want.size());
    EXPECT_TRUE(std::equal(want.begin(), want.end(), b.bytes.begin()));
}

TEST(FilterWireRegression, EmptyAnyOfSpanFailsWithValueCountRegression) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(1, PrimType::Int32, 4, Match<int32_t>(1));
    wr.term(2, PrimType::Int32, 4, Match<int32_t>(anyOf(std::span<const int32_t>{})));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::ValueCount);
    EXPECT_EQ(r.error().termIndex, 1);
}

TEST(FilterWireRegression, ShortBufferFailsWithBufferTooSmallRegression) {
    std::array<uint8_t, 8> small{};
    FilterWriter wr{std::span<uint8_t>(small)};
    wr.term(1, PrimType::Int32, 4, Match<int32_t>(1));   // needs 11
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::BufferTooSmall);
    EXPECT_EQ(r.error().termIndex, 0);
}

TEST(FilterWireRegression, WidthMustMatchTheValueTypeRegression) {
    auto run = [](auto m, PrimType t, uint8_t w) {
        std::array<uint8_t, 64> buf{};
        buf.fill(0xEE);
        FilterWriter wr{std::span<uint8_t>(buf)};
        wr.term(9, t, w, m);
        auto r = wr.finish();
        EXPECT_FALSE(r.has_value());
        if (!r) { EXPECT_EQ(r.error().kind, FilterErrorKind::TypeMismatch); EXPECT_EQ(r.error().termIndex, 0); }
        for (uint8_t x : buf) EXPECT_EQ(x, 0xEE) << "no bytes written";
    };
    run(Match<int64_t>(int64_t{1}), PrimType::Int64, 4);                           // would truncate
    run(Match<int32_t>(1), PrimType::Int32, 8);                                    // wrong width for type
    run(Match<std::array<char, 4>>("ab"), PrimType::FixedStr, 8);                  // reads past the array
    run(Match<std::array<char, 20>>("ab"), PrimType::FixedStr, 20);                // wider than 16
    run(Match<std::array<char, 4>>("ab"), PrimType::FixedStr, 0);
}
