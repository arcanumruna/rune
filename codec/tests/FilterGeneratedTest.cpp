// Phase 2c Task 4: the generated E::Filter (filter-by-example) against player_event.gen.h / nine_ints.gen.h,
// both generated at build time by make_player_event_blob + rune_codec_gen.
#include "FilterTestSupport.h"
#include "generated/nine_ints.gen.h"
#include "generated/player_event.gen.h"
#include "generated/shadow_names.gen.h"

#include <rune/codec/Fingerprint.h>
#include <rune/codec/FilterBuilder.h>
#include <rune/codec/FilterSchema.h>

#include <gtest/gtest.h>

#include <stdexcept>
#include <type_traits>

using E = rune::gen::PlayerEvent;
using rune::anyOf;

// The v1 player_event schema's fingerprint, pinned as a LITERAL (comparing two descriptors would be a
// tautology).  The client test pins the same value for client-cpp/examples/filter/PlayerEventSchema.h.
inline constexpr uint64_t kPlayerEventV1Fingerprint = 18066230342614569726ull;

TEST(FilterGenerated, DesignatedInitializersEncode) {
    const auto b = E::Filter{ .worldId = 42u, .channel = anyOf(1, 2, 3) }.encode();
    EXPECT_EQ(b.len, 2 + (5 + 4) + (5 + 3 * 4));
}
TEST(FilterGenerated, EmptyFilterIsNoFilter) {   // spec §4
    EXPECT_TRUE(E::Filter{}.encode().empty());
}
// A requires-expression with an invalid expression is ill-formed outside a template, so every probe is a
// variable template over the filter type.
template <class F> constexpr bool kHasName          = requires(F f) { f.name; };
template <class F> constexpr bool kHasPos           = requires(F f) { f.pos; };
template <class F> constexpr bool kWorldTakesDouble = requires { F{ .worldId = 1.5 }; };
template <class F> constexpr bool kWorldTakesString = requires { F{ .worldId = "7" }; };
template <class F> constexpr bool kZoneTakesLiteral = requires { F{ .zone = "eu" }; };
TEST(FilterGenerated, CompileTimeChecks) {     // spec §4: unknown / ineligible fields and wrong value types do not compile
    static_assert(!kHasName<E::Filter>, "a string field has no member");
    static_assert(!kHasPos<E::Filter>,  "a float field has no member");
    static_assert(!kWorldTakesDouble<E::Filter>, "a double is not a uint32 value");
    static_assert(!kWorldTakesString<E::Filter>, "a string is not a uint32 value");
    static_assert(kZoneTakesLiteral<E::Filter>);
    static_assert(std::is_trivially_copyable_v<E::Filter>);
    SUCCEED();
}
TEST(FilterGenerated, EncodeToIsConstexprAndAllocationFree) {
    constexpr auto kLen = [] {
        std::array<uint8_t, 64> buf{};
        const auto r = E::Filter{ .worldId = 42u }.encodeTo(buf);
        return r ? *r : std::size_t{0};
    }();
    static_assert(kLen == 2 + 5 + 4);
    EXPECT_EQ(kLen, 11u);
}
TEST(FilterGeneratedRegression, GeneratedAndBuilderBytesAreIdenticalRegression) {   // spec §4 "same encoding"
    const auto a = E::Filter{ .worldId = 42u, .channel = anyOf(3, 1, 2) }.encode();
    const auto b = rune::codec::FilterBuilder::on(E::descriptor()).field("channel").in({int64_t{1}, int64_t{2}, int64_t{3}})
                       .field("worldId").eq(int64_t{42}).build();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a, *b);
}
TEST(FilterGeneratedRegression, BuilderMatchesGeneratedForEveryFieldTypeRegression) {
    using namespace rune::codec;
    const auto a = E::Filter{ .playerId = 9u, .zone = "eu", .kind = anyOf(3, 1), .alive = true, .hp = anyOf(int64_t{-5}, int64_t{7}) }.encode();
    const auto b = FilterBuilder::on(E::descriptor()).field("hp").in({int64_t{7}, int64_t{-5}}).field("alive").eq(true)
                       .field("kind").in({int64_t{1}, int64_t{3}}).field("zone").eq(std::string_view("eu"))
                       .field("playerId").eq(uint64_t{9}).build();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a, *b);
}
TEST(FilterGeneratedRegression, GeneratedFilterParsesAgainstItsOwnSchemaRegression) {
    const auto fs = rune::codec::FilterSchema::build(E::descriptor(), E::kVersion);
    const auto b  = E::Filter{ .playerId = 9u, .zone = "eu", .alive = true }.encode();
    EXPECT_TRUE(rune::codec::parseFilter(b.span(), fs).has_value());
}
TEST(FilterGeneratedRegression, NineSetMembersFailAtEncodeRegression) {   // spec §4: >8 terms fails at encode
    using N = rune::gen::NineInts;
    const N::Filter nine{ .f0 = 1u, .f1 = 1u, .f2 = 1u, .f3 = 1u, .f4 = 1u, .f5 = 1u, .f6 = 1u, .f7 = 1u, .f8 = 1u };
    const N::Filter eight{ .f0 = 1u, .f1 = 1u, .f2 = 1u, .f3 = 1u, .f4 = 1u, .f5 = 1u, .f6 = 1u, .f7 = 1u };
    EXPECT_THROW((void)nine.encode(), std::invalid_argument);
    EXPECT_NO_THROW((void)eight.encode());
}
TEST(FilterGeneratedRegression, OutOfRangeAndOverlongValuesFailAtEncodeRegression) {
    const E::Filter negative{ .worldId = -1 };
    std::array<int, 65> many{};
    const E::Filter tooMany{ .channel = anyOf(std::span<const int>(many)) };
    EXPECT_THROW((void)negative.encode(), std::invalid_argument);
    EXPECT_THROW((void)tooMany.encode(), std::invalid_argument);
}
TEST(FilterGeneratedRegression, PlayerEventFingerprintIsPinnedRegression) {
    // One literal, three producers: the build-time generated header, the shared test fixture, and (client
    // test) client-cpp/examples/filter/PlayerEventSchema.h.
    EXPECT_EQ(E::kFingerprint, kPlayerEventV1Fingerprint);
    EXPECT_EQ(rune::codec::fingerprint(filtertest::playerEvent(1)), kPlayerEventV1Fingerprint);
}

TEST(FilterGeneratedRegression, FieldsNamedLikeEncodeToLocalsCompileAndEncodeRegression) {
    using S = rune::gen::ShadowNames;   // fields `w` and `out`
    const auto a = S::Filter{ .w = 1u, .out = anyOf(3u, 2u) }.encode();
    const auto b = rune::codec::FilterBuilder::on(S::descriptor()).field("out").in({2u, 3u}).field("w").eq(1u).build();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a, *b);
    EXPECT_EQ(a.len, 2 + (5 + 4) + (5 + 2 * 4));
}
TEST(FilterGeneratedRegression, BuilderAndGeneratedAgreeOnDedupSingleInFixedStrBoolAndNegativesRegression) {
    using namespace rune::codec;
    auto eqB = [](const FilterBuilder& fb) { auto r = fb.build(); EXPECT_TRUE(r.has_value()); return r ? *r : FilterBytes{}; };
    const auto d = E::descriptor();
    // In with duplicates (dedup + sort)
    EXPECT_EQ(E::Filter{ .channel = anyOf(3, 1, 3, -2, 1) }.encode(),
              eqB(FilterBuilder::on(d).field("channel").in({int64_t{1}, int64_t{-2}, int64_t{3}, int64_t{3}, int64_t{1}})));
    // single-value In stays In
    EXPECT_EQ(E::Filter{ .hp = anyOf(int64_t{5}) }.encode(), eqB(FilterBuilder::on(d).field("hp").in({int64_t{5}})));
    // FixedStr In (sorted by FilterKey, last byte most significant; duplicates dropped)
    EXPECT_EQ(E::Filter{ .zone = anyOf("ba", "ab", "ba") }.encode(),
              eqB(FilterBuilder::on(d).field("zone").in({std::string_view("ab"), std::string_view("ba"), std::string_view("ba")})));
    // bool In
    EXPECT_EQ(E::Filter{ .alive = anyOf(true, false, true) }.encode(),
              eqB(FilterBuilder::on(d).field("alive").in({true, false, true})));
    // FixedStr filled to its full width
    EXPECT_EQ(E::Filter{ .zone = "12345678" }.encode(), eqB(FilterBuilder::on(d).field("zone").eq(std::string_view("12345678"))));
    // negative int32
    EXPECT_EQ(E::Filter{ .channel = -7 }.encode(), eqB(FilterBuilder::on(d).field("channel").eq(int64_t{-7})));
}
