// tests/GeneratedKeyTest.cpp — Phase 2b Part C regression: the generated
// struct's key() accessor, compiled from REAL generated headers:
//   characters.gen.h      userId int64 is the key  -> int64_t key()
//   fixedstr_sample.gen.h no key field             -> no key() at all
//   str_keyed.gen.h       sku string is the key    -> std::span<const uint8_t> key()
// TypedTopic::send routes by Key(rec.key()); before 2b there was no key()
// and every typed record went to partition 0 with an empty key.

#include <gtest/gtest.h>
#include "generated/characters.gen.h"
#include "generated/fixedstr_sample.gen.h"
#include "generated/str_keyed.gen.h"

#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

namespace {
template <class R>
concept HasKey = requires(const R& r) { r.key(); };
} // namespace

TEST(GeneratedKeyRegression, ScalarKeyReturnsTheIsKeyFieldByValue) {
    static_assert(std::is_same_v<decltype(std::declval<const rune::gen::Characters&>().key()), int64_t>,
                  "an int64 key field must surface as int64_t key() (Key(int64_t) = 8 LE bytes)");
    rune::gen::Characters c;
    c.id     = 7;
    c.userId = 42;
    EXPECT_EQ(c.key(), 42) << "key() must return the isKey field (userId), not id";
}

TEST(GeneratedKeyRegression, SchemaWithoutKeyFieldHasNoKeyAccessor) {
    static_assert(!HasKey<rune::gen::FixedstrSample>,
                  "no isKey field -> no key(); TypedTopic keeps partition 0 + empty key");
    SUCCEED();
}

TEST(GeneratedKeyRegression, StringKeyIsAByteSpanOverTheContent) {
    static_assert(std::is_same_v<decltype(std::declval<const rune::gen::StrKeyed&>().key()),
                                 std::span<const uint8_t>>,
                  "a var-len key must be a BYTE span — a std::string_view is trivially copyable "
                  "and rune::client::Key would hash the view object (pointer+len), not the content");
    const std::string_view s = "sku-42";
    rune::gen::StrKeyed r;
    r.sku = s;
    r.qty = 3;
    const std::span<const uint8_t> k = r.key();
    ASSERT_EQ(k.size(), s.size());
    EXPECT_EQ(std::memcmp(k.data(), s.data(), s.size()), 0);
    EXPECT_EQ(static_cast<const void*>(k.data()), static_cast<const void*>(s.data()))
        << "key() is a zero-copy view of the field";
}
