// tests/ValidatorTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Validator.h>
#include <rune/codec/Record.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Evolve.h>
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

// ---------------------------------------------------------------------------
// Issue #149: a Validator built from the cumulative EVOLVING descriptor (it still lists retired fields)
// validates exactly as one built from materializeAsOf(evolving, evolving.version).  Before the fix the
// retired var field was walked as live: it consumed the live field's length prefix, the live field then
// read past the end, and a valid current-version record was falsely rejected.
// ---------------------------------------------------------------------------
namespace {
// v1 {id i64 key, note str, name str}; DROP note at v2 (the Evolve API, as EvolveTest.cpp does).
Descriptor evolvingWithDroppedVar() {
    const Descriptor v1 = DescriptorBuilder("c")
        .add("id", PrimType::Int64, 1, /*isKey=*/true)
        .add("note", PrimType::String)
        .add("name", PrimType::String)
        .build();
    uint16_t nextTag = 3;
    auto v2 = applyOps(v1, {{AlterOpType::Drop, "note", "", {}, 0, {}}}, nextTag, 2);
    EXPECT_TRUE(v2.has_value());
    return v2.value_or(v1);
}
// A v2 record: [blockLength=8][version=2][id i64][len u32][name bytes].
std::vector<uint8_t> v2Record(int64_t id, std::string_view name) {
    std::vector<uint8_t> b;
    auto le = [&](auto v) { for (size_t i = 0; i < sizeof v; i++) b.push_back(uint8_t(v >> (8 * i))); };
    le(uint16_t{8}); le(uint16_t{2}); le(id);
    le(uint32_t(name.size()));
    b.insert(b.end(), name.begin(), name.end());
    return b;
}
}  // namespace

TEST(ValidatorRegression, EvolvingDescriptorWithRetiredFieldsIsHandledRegression) {
    const Descriptor evolving = evolvingWithDroppedVar();
    ASSERT_EQ(evolving.version, 2);
    ASSERT_NE(evolving.find("note"), nullptr);
    ASSERT_EQ(evolving.find("note")->untilVersion, 2) << "the evolving descriptor still carries the retired field";

    const Validator fromEvolving(evolving);
    const Validator fromMaterialized(materializeAsOf(evolving, evolving.version));

    const auto good = v2Record(42, "alice");
    const ValResult a = fromEvolving.validate(good);
    const ValResult b = fromMaterialized.validate(good);
    EXPECT_TRUE(a.ok) << "a valid current-version record was rejected: " << (a.reason ? a.reason : "");
    EXPECT_TRUE(b.ok);
    EXPECT_EQ(a.ok, b.ok);
    EXPECT_EQ(a.fieldId, b.fieldId);

    // A corrupt record is still refused, exactly as the materialized validator refuses it.
    auto truncated = good;
    truncated.resize(truncated.size() - 2);
    const ValResult c = fromEvolving.validate(truncated);
    const ValResult d = fromMaterialized.validate(truncated);
    EXPECT_FALSE(c.ok) << "a truncated var field was accepted";
    EXPECT_EQ(c.ok, d.ok);
    EXPECT_EQ(c.fieldId, d.fieldId);
    auto badPrefix = good;
    badPrefix[12] = 0xFF; badPrefix[13] = 0xFF; badPrefix[14] = 0xFF; badPrefix[15] = 0xFF;   // name length 4 GiB
    EXPECT_FALSE(fromEvolving.validate(badPrefix).ok) << "an absurd var length was accepted";
    std::vector<uint8_t> wrongBlock = good;
    wrongBlock[0] = 12;                                                                         // v2's blockLength is 8
    EXPECT_FALSE(fromEvolving.validate(wrongBlock).ok) << "a blockLength mismatch was accepted";
}
