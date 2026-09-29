// tests/ValidatorTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Validator.h>
#include <rune/codec/Record.h>
using namespace rune::codec;

static Descriptor D() {
    return DescriptorBuilder("c")
        .add("id",   PrimType::Int64)
        .add("type", PrimType::Enum, 1, false, {0, 1, 2})
        .add("name", PrimType::String)
        .build();
}

// blockLength=12 (int64=8 + enum=4), ver=1
static std::vector<uint8_t> frame(int64_t id, int32_t type, std::string_view name) {
    std::vector<uint8_t> b;
    auto le = [&](auto v) {
        for (size_t i = 0; i < sizeof v; i++)
            b.push_back(uint8_t(v >> (8 * i)));
    };
    uint16_t block = 12, ver = 1;
    le(block); le(ver);
    le(id);
    le(type);
    uint32_t n = uint32_t(name.size());
    le(n);
    b.insert(b.end(), name.begin(), name.end());
    return b;
}

TEST(Validator, AcceptsValid) {
    Validator v(D());
    EXPECT_TRUE(v.validate(frame(1, 1, "ok")).ok);
}

TEST(Validator, RejectsBadEnum) {
    Validator v(D());
    auto r = v.validate(frame(1, 9, "x"));
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.fieldId, 1);
}

TEST(Validator, RejectsTruncatedVar) {
    Validator v(D());
    auto rec = frame(1, 1, "hello");
    rec.resize(rec.size() - 2);
    EXPECT_FALSE(v.validate(rec).ok);
}

TEST(Validator, RejectsShortBlock) {
    Validator v(D());
    std::vector<uint8_t> b = {12, 0, 1, 0};
    EXPECT_FALSE(v.validate(b).ok);
}

// --- Negative enum whitelist ---------------------------------------------
// Single Enum field (4-byte int32 in fixed block) whose whitelist includes a
// negative value. blockLength=4, version=1.
static Descriptor NegEnumD() {
    return DescriptorBuilder("neg")
        .add("e", PrimType::Enum, 1, false, {-1, 0, 1})
        .build();
}

static std::vector<uint8_t> frameEnum(int32_t e) {
    std::vector<uint8_t> b;
    auto le = [&](auto v) {
        for (size_t i = 0; i < sizeof v; i++)
            b.push_back(uint8_t(v >> (8 * i)));
    };
    uint16_t block = 4, ver = 1;
    le(block); le(ver);
    le(e);
    return b;
}

TEST(Validator, AcceptsNegativeEnumInWhitelist) {
    Validator v(NegEnumD());
    EXPECT_TRUE(v.validate(frameEnum(-1)).ok);
}

TEST(Validator, RejectsNegativeEnumNotInWhitelist) {
    Validator v(NegEnumD());
    EXPECT_FALSE(v.validate(frameEnum(-2)).ok);
}
