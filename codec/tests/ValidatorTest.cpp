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

// M-4 (HARD RULE 5): adversarial, hostile-but-in-span records fed to Validator.
// The Validator is the gate; each of these must be REJECTED (ok == false)
// without any out-of-bounds access.

TEST(ValidatorAdversarial, RejectsBlockLengthExceedingBuffer) {
    Validator v(D());
    // Declared blockLength = 0xFFFF (>> descriptor's 12) with a future version,
    // so the blockLength-mismatch check is skipped and we exercise the
    // "record shorter than declared blockLength" guard. Must reject, not crash.
    std::vector<uint8_t> b;
    auto le = [&](auto x){ for (size_t i=0;i<sizeof x;i++) b.push_back(uint8_t(x>>(8*i))); };
    uint16_t block = 0xFFFF, ver = 99;   // future version -> mismatch check skipped
    le(block); le(ver);
    le(int64_t(1)); le(int32_t(1));      // partial payload, far short of 0xFFFF
    auto r = v.validate(b);
    EXPECT_FALSE(r.ok);
}

TEST(ValidatorAdversarial, RejectsVarLengthPrefixAllOnes) {
    Validator v(D());
    // Correct fixed block, but var length prefix is 0xFFFFFFFF. pos+len must
    // not wrap; Validator must reject as truncated.
    std::vector<uint8_t> b;
    auto le = [&](auto x){ for (size_t i=0;i<sizeof x;i++) b.push_back(uint8_t(x>>(8*i))); };
    uint16_t block = 12, ver = 1;
    le(block); le(ver);
    le(int64_t(1)); le(int32_t(1));      // id, type (valid enum)
    le(uint32_t(0xFFFFFFFFu));           // hostile var length prefix
    b.push_back('x');
    auto r = v.validate(b);
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.fieldId, D().find("name")->id);
}

TEST(ValidatorAdversarial, RejectsTruncatedVarSegment) {
    Validator v(D());
    // Var length prefix claims 10 bytes; only 2 present. Must reject.
    std::vector<uint8_t> b;
    auto le = [&](auto x){ for (size_t i=0;i<sizeof x;i++) b.push_back(uint8_t(x>>(8*i))); };
    uint16_t block = 12, ver = 1;
    le(block); le(ver);
    le(int64_t(1)); le(int32_t(1));
    le(uint32_t(10));
    b.push_back('h'); b.push_back('i');
    auto r = v.validate(b);
    EXPECT_FALSE(r.ok);
}
