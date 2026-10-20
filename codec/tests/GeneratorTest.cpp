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

TEST(Generator, EmitsZeroCopyFlyweightsForAnAllFixedSchema) {
    auto d = DescriptorBuilder("position").add("userId", PrimType::Int64, 1, true)
               .add("zone", PrimType::Int32).addFixedStr("tag", 8).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("class PositionEncoder {"), std::string::npos);
    EXPECT_NE(hdr.find("class PositionDecoder {"), std::string::npos);
    EXPECT_NE(hdr.find("using Encoder = PositionEncoder;"), std::string::npos);
    EXPECT_NE(hdr.find("using Decoder = PositionDecoder;"), std::string::npos);
    EXPECT_NE(hdr.find("static constexpr uint32_t kEncodedLength = 4u + kBlockLength;"), std::string::npos);
    EXPECT_NE(hdr.find("static constexpr bool     kHasKey        = true;"), std::string::npos);
    EXPECT_NE(hdr.find("using KeyType = int64_t;"), std::string::npos);
    EXPECT_NE(hdr.find("[[gnu::always_inline]] PositionEncoder zone(int32_t v) noexcept {"), std::string::npos);
    EXPECT_NE(hdr.find("::rune::codec::fly::store<int32_t>(p + 4u + 8u, v);"), std::string::npos)
        << "zone sits at block offset 8";
    EXPECT_NE(hdr.find("[[gnu::always_inline]] PositionEncoder tag(std::string_view v) noexcept {"), std::string::npos);
    EXPECT_NE(hdr.find("#include <rune/codec/Flyweight.h>"), std::string::npos);
}

// Final review I2 regression: the setters chain by value through a local copy
// of the pointer (no reload after each store when the encoder lives in an
// escaping object), and the ClaimEncoder's chain yields the ClaimEncoder.
TEST(GeneratorRegression, FlyweightSettersChainByValueWithoutReloadingThePointerRegression) {
    auto d = DescriptorBuilder("position").add("userId", PrimType::Int64, 1, true)
               .add("zone", PrimType::Int32).addFixedStr("tag", 8).build();
    const std::string hdr = generateHeader(d);
    EXPECT_EQ(hdr.find("PositionEncoder& zone("), std::string::npos) << "no reference chaining";
    EXPECT_NE(hdr.find("        uint8_t* const p = p_;\n"
                       "        ::rune::codec::fly::store<int32_t>(p + 4u + 8u, v);\n"
                       "        return PositionEncoder(p, Rebind{});\n"),
              std::string::npos);
    EXPECT_NE(hdr.find("[[gnu::always_inline]] PositionClaimEncoder zone(int32_t v) noexcept {\n"
                       "        return PositionClaimEncoder(PositionEncoder::zone(v));\n"),
              std::string::npos);
    EXPECT_EQ(hdr.find("using PositionEncoder::zone;"), std::string::npos)
        << "a using-declaration would return the base, which exposes the key setter";
    EXPECT_EQ(hdr.find("PositionClaimEncoder userId("), std::string::npos) << "the key stays hidden";
}

TEST(Generator, NoFlyweightsForASchemaWithVarLenFields) {
    auto d = DescriptorBuilder("characters").add("id", PrimType::Int64, 1, true)
               .add("name", PrimType::String).build();
    const std::string hdr = generateHeader(d);
    EXPECT_EQ(hdr.find("Encoder"), std::string::npos);
    EXPECT_EQ(hdr.find("Decoder"), std::string::npos);
    EXPECT_NE(hdr.find("size_t encode(uint8_t* buf) const noexcept"), std::string::npos)
        << "var-len schemas keep today's struct + encode()";
}

TEST(Generator, KeylessAllFixedSchemaHasNoKeyHelpers) {
    auto d = DescriptorBuilder("tick").add("seq", PrimType::UInt64).add("value", PrimType::Float64).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("class TickEncoder {"), std::string::npos);
    EXPECT_NE(hdr.find("static constexpr bool     kHasKey        = false;"), std::string::npos);
    EXPECT_EQ(hdr.find("KeyType"), std::string::npos);
}

// Part C rejects a `key` field next to a key field, so the only reachable
// `key` field is on a keyless schema: it gets a plain setter, no key helpers.
TEST(Generator, AFieldNamedKeyOnAKeylessSchemaIsAPlainFlyweightSetter) {
    auto d = DescriptorBuilder("kv").add("key", PrimType::Int64).add("v", PrimType::Int32).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("class KvEncoder {"), std::string::npos);
    EXPECT_NE(hdr.find("[[gnu::always_inline]] KvEncoder key(int64_t v) noexcept {"), std::string::npos);
    EXPECT_EQ(hdr.find("KeyType"), std::string::npos);
}
