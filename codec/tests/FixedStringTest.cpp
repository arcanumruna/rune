// tests/FixedStringTest.cpp — rune::FixedString<N>, the generated type of a string(N) field (#1010).
#include <gtest/gtest.h>
#include <rune/codec/FixedString.h>

#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

using rune::FixedString;

namespace {
struct Rec {
    int a = 0;
    FixedString<16> name = {};
};
}  // namespace

TEST(FixedStringRegression, LayoutRegression) {
    static_assert(sizeof(FixedString<8>) == 8 && alignof(FixedString<8>) == 1);
    static_assert(std::is_trivially_copyable_v<FixedString<8>> && std::is_standard_layout_v<FixedString<8>>);
    const FixedString<8> z{};
    for (std::size_t i = 0; i < 8; ++i) EXPECT_EQ(z[i], '\0');
}

TEST(FixedStringRegression, LiteralAssignRegression) {
    FixedString<4> a = "Ayla";
    EXPECT_EQ(std::memcmp(a.data(), "Ayla", 4), 0);
    EXPECT_EQ(a.view(), "Ayla");
    FixedString<8> b = "eu";
    EXPECT_EQ(b.view(), "eu");
    for (std::size_t i = 2; i < 8; ++i) EXPECT_EQ(b[i], '\0');
    EXPECT_EQ(b.size(), 8u);
    b = {};
    EXPECT_EQ(b.view(), "");
}

TEST(FixedStringRegression, CompileTimeBoundsRegression) {
    static_assert(!std::is_constructible_v<FixedString<4>, const char (&)[6]>);
    static_assert(!std::is_assignable_v<FixedString<4>&, const char (&)[6]>);
    static_assert(std::is_constructible_v<FixedString<4>, const char (&)[5]>);
    static_assert(std::is_assignable_v<FixedString<4>&, const char (&)[5]>);
    SUCCEED();
}

TEST(FixedStringRegression, RuntimeAssignRegression) {
    FixedString<4> f;
    f = std::string_view("abcd");
    EXPECT_EQ(f.view(), "abcd");
    EXPECT_THROW(f = std::string("abcde"), std::invalid_argument);
    EXPECT_EQ(f.view(), "abcd") << "a throwing assignment leaves the object unchanged";
    EXPECT_THROW((FixedString<4>(std::string(5, 'x'))), std::invalid_argument);
    EXPECT_FALSE(f.tryAssign("abcde"));
    EXPECT_EQ(f.view(), "abcd");
    EXPECT_TRUE(f.tryAssign("ab"));
    EXPECT_EQ(f.view(), "ab");
    EXPECT_EQ(f[2], '\0');
    EXPECT_EQ(f[3], '\0');
    static_assert(noexcept(f.tryAssign(std::string_view{})));
}

TEST(FixedStringRegression, ViewStopsAtNulRegression) {
    FixedString<6> f;
    std::memcpy(f.data(), "ab\0cd", 5);
    EXPECT_EQ(f.view(), "ab");
}

TEST(FixedStringRegression, EqualityRegression) {
    FixedString<8> a = "abc", b = "abc", c = "abd";
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a == c);
    EXPECT_TRUE(a == "abc");
    EXPECT_TRUE(a == std::string_view("abc"));
    EXPECT_TRUE(a < c);
    EXPECT_TRUE(c > a);
}

TEST(FixedStringRegression, DesignatedInitRegression) {
    Rec r{.a = 1, .name = "Ayla"};
    EXPECT_EQ(r.name.view(), "Ayla");
    EXPECT_EQ(r.a, 1);
}
