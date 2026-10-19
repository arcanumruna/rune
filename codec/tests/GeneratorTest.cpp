// tests/GeneratorTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Generator.h>
#include <rune/codec/Record.h>
#include <stdexcept>
#include <string>
using namespace rune::codec;

TEST(Generator, EmitsCompilableStructWithFingerprint) {
    auto d = DescriptorBuilder("characters").add("id",PrimType::Int64)
               .add("name",PrimType::String).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("struct Characters"), std::string::npos);
    EXPECT_NE(hdr.find("kFingerprint"), std::string::npos);
    EXPECT_NE(hdr.find(std::to_string(fingerprint(d))), std::string::npos);
}

// ── Phase 2b Part C: key() accessor emission ──────────────────────────────────

TEST(GeneratorRegression, EmitsKeyAccessorForScalarKey) {
    auto d = DescriptorBuilder("characters").add("id",PrimType::Int64)
               .add("userId",PrimType::Int64,1,true).add("name",PrimType::String).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("[[nodiscard]] int64_t key() const noexcept { return userId; }"),
              std::string::npos) << hdr;
}

TEST(GeneratorRegression, EmitsKeyAccessorForFixedStrKey) {
    auto d = DescriptorBuilder("codes").addFixedStr("code", 8, 1, true)
               .add("n", PrimType::Int32).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("[[nodiscard]] const std::array<char, 8>& key() const noexcept { return code; }"),
              std::string::npos) << hdr;
}

TEST(GeneratorRegression, EmitsByteSpanKeyForStringKeyNeverStringView) {
    auto d = DescriptorBuilder("str_keyed").add("sku",PrimType::String,1,true)
               .add("qty",PrimType::Int32).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("[[nodiscard]] std::span<const uint8_t> key() const noexcept {"),
              std::string::npos) << hdr;
    EXPECT_EQ(hdr.find("std::string_view key()"), std::string::npos)
        << "a string_view key would make rune::client::Key hash the view object, not the content";
}

TEST(GeneratorRegression, NoKeyFieldEmitsNoKeyAccessor) {
    auto d = DescriptorBuilder("nokey").add("id",PrimType::Int64).add("name",PrimType::String).build();
    const std::string hdr = generateHeader(d);
    EXPECT_EQ(hdr.find("key()"), std::string::npos) << hdr;
}

TEST(GeneratorRegression, FieldNamedKeyCollidingWithAccessorIsRejected) {
    auto d = DescriptorBuilder("kv").add("key",PrimType::Int64,1,true)
               .add("value",PrimType::String).build();
    EXPECT_THROW((void)generateHeader(d), std::invalid_argument)
        << "a member named `key` and the key() accessor cannot coexist — fail with a clear error, "
           "never emit a header that does not compile";
}

// Phase 2b C final review: a descriptor with TWO isKey fields used to emit key()
// for the first one silently — the record would route by a key the author may
// not have meant.  It is ambiguous, so it is rejected like the `key` collision.
TEST(GeneratorRegression, MoreThanOneKeyFieldIsRejected) {
    auto d = DescriptorBuilder("two_keys").add("userId",PrimType::Int64,1,true)
               .add("orderId",PrimType::Int64,1,true).add("qty",PrimType::Int32).build();
    try {
        (void)generateHeader(d);
        FAIL() << "a descriptor with two isKey fields must be rejected";
    } catch (const std::invalid_argument& e) {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("userId"), std::string::npos) << msg;
        EXPECT_NE(msg.find("orderId"), std::string::npos) << msg;
    }
}
