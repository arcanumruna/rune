// tests/ValidatorTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Validator.h>
#include <rune/codec/Record.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Evolve.h>
#include <algorithm>
#include <cstring>
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

// ---------------------------------------------------------------------------
// Adversarial review of PR #597 (#149): seeded random schema evolutions (ADD and DROP of fixed, FixedStr, enum and
// var fields over several versions, with retired names re-added under fresh tags).  For every seed, a Validator built
// from the cumulative evolving descriptor must agree with one built from materializeAsOf(evolving, version) on a
// valid current-version record, on that record truncated at EVERY byte offset, with trailing bytes, and with each var
// length prefix set to max; the valid record must pass.  Mutation: build the evolving validator from the raw
// descriptor (the pre-fix constructor) and the valid record is refused as soon as a retired field exists.
// ---------------------------------------------------------------------------
namespace {
struct Lcg {
    uint64_t s;
    uint32_t next() { s = s * 6364136223846793005ULL + 1442695040888963407ULL; return uint32_t(s >> 33); }
    uint32_t below(uint32_t n) { return next() % n; }
};

// A valid record for the materialized (per-version) descriptor m: random fixed bytes, enum fields set to a
// whitelisted value, each var field a random length; returns the var length-prefix positions too.
std::vector<uint8_t> validRecordFor(const Descriptor& m, Lcg& rng, std::vector<size_t>& prefixAt) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    le16(uint16_t(m.blockLength)); le16(m.version);
    b.resize(4 + m.blockLength);
    for (size_t i = 4; i < b.size(); ++i) b[i] = uint8_t(rng.next());
    for (const auto& f : m.fields) {
        if (f.type != PrimType::Enum || f.enumValues.empty()) continue;
        const int32_t v = f.enumValues[rng.below(uint32_t(f.enumValues.size()))];
        std::memcpy(b.data() + 4 + f.offset, &v, 4);
    }
    std::vector<const FieldDescriptor*> vars;
    for (const auto& f : m.fields) if (isVarLen(f.type)) vars.push_back(&f);
    std::sort(vars.begin(), vars.end(), [](auto* a, auto* c) { return a->offset < c->offset; });   // var index order
    for (const FieldDescriptor* f : vars) {
        (void)f;
        prefixAt.push_back(b.size());
        const uint32_t len = rng.below(6);
        for (int i = 0; i < 4; ++i) b.push_back(uint8_t(len >> (8 * i)));
        for (uint32_t i = 0; i < len; ++i) b.push_back(uint8_t('a' + rng.below(26)));
    }
    return b;
}
}  // namespace

TEST(ValidatorAdversarial, RandomEvolutionsAgreeWithTheMaterializedValidatorAtEveryTruncationAdversarialRegression) {
    const char* names[] = {"a", "b", "c", "d", "e"};
    const PrimType types[] = {PrimType::Int32, PrimType::Int64, PrimType::UInt16, PrimType::Bool, PrimType::Enum,
                              PrimType::String, PrimType::Bytes, PrimType::FixedStr, PrimType::Float64};
    int seedsWithRetired = 0;
    for (uint64_t seed = 1; seed <= 300; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        Lcg rng{seed};
        Descriptor evolving = DescriptorBuilder("adv")
            .add("id", PrimType::Int64, 1, /*isKey=*/true)
            .add("kind", PrimType::Enum, 1, false, {-3, 7, 1000})
            .add("note", PrimType::String)
            .addFixedStr("tag", 5)
            .build();
        uint16_t nextTag = uint16_t(evolving.fields.size());
        const uint16_t versions = uint16_t(2 + rng.below(6));
        for (uint16_t v = 2; v <= versions; ++v) {
            std::vector<AlterOp> ops;
            const uint32_t nOps = 1 + rng.below(3);
            for (uint32_t k = 0; k < nOps; ++k) {
                AlterOp op{};
                if (rng.below(2) == 0) {
                    op.type = AlterOpType::Drop;
                    std::vector<std::string> live;
                    for (const auto& f : evolving.fields)
                        if (f.untilVersion == 0xFFFF && !f.isKey) live.push_back(f.name);
                    if (live.empty()) continue;
                    op.name = live[rng.below(uint32_t(live.size()))];
                } else {
                    op.type = AlterOpType::Add;
                    op.name = names[rng.below(5)];
                    op.primType = types[rng.below(uint32_t(std::size(types)))];
                    op.fixedLen = op.primType == PrimType::FixedStr ? uint16_t(1 + rng.below(8)) : 0;
                }
                ops.push_back(op);
            }
            uint16_t tag = nextTag;
            auto next = applyOps(evolving, ops, tag, v);
            if (!next) {   // a refused op set (name collision, ...): the version still advances with no ops
                auto same = applyOps(evolving, {}, tag, v);
                ASSERT_TRUE(same.has_value());
                evolving = *same;
                continue;
            }
            nextTag = tag;
            evolving = *next;
        }
        bool retired = false;
        for (const auto& f : evolving.fields) retired |= f.untilVersion != 0xFFFF;
        seedsWithRetired += retired;

        const Descriptor m = materializeAsOf(evolving, evolving.version);
        const Validator fromEvolving(evolving);
        const Validator fromMaterialized(m);
        std::vector<size_t> prefixAt;
        const auto good = validRecordFor(m, rng, prefixAt);

        const ValResult ok = fromEvolving.validate(good);
        ASSERT_TRUE(ok.ok) << "a valid v" << evolving.version << " record was refused (field " << ok.fieldId << "): "
                           << (ok.reason ? ok.reason : "");
        ASSERT_TRUE(fromMaterialized.validate(good).ok);
        for (size_t n = 0; n < good.size(); ++n) {
            const std::span<const uint8_t> cut(good.data(), n);
            const ValResult a = fromEvolving.validate(cut);
            const ValResult b = fromMaterialized.validate(cut);
            ASSERT_EQ(a.ok, b.ok) << "truncated at " << n << " of " << good.size();
            ASSERT_EQ(a.fieldId, b.fieldId) << "truncated at " << n;
            ASSERT_FALSE(a.ok) << "a record truncated at " << n << " of " << good.size() << " was accepted";
        }
        auto longer = good;
        longer.push_back(0xAB);
        EXPECT_EQ(fromEvolving.validate(longer).ok, fromMaterialized.validate(longer).ok);
        for (size_t at : prefixAt) {
            auto bad = good;
            bad[at] = bad[at + 1] = bad[at + 2] = bad[at + 3] = 0xFF;
            const ValResult a = fromEvolving.validate(bad);
            EXPECT_FALSE(a.ok) << "a 4 GiB var length at " << at << " was accepted";
            EXPECT_EQ(a.fieldId, fromMaterialized.validate(bad).fieldId);
        }
    }
    EXPECT_GT(seedsWithRetired, 200) << "the random evolutions must retire fields in most seeds (not vacuous)";
}

// --- Checked writes: UTF-8 string check, P3 padding, topic key types ---------
static Descriptor Utf8D() {
    return DescriptorBuilder("u")
        .addFixedStr("name", 8)
        .add("bio", PrimType::String)
        .add("blob", PrimType::Bytes)
        .build();
}

static std::vector<uint8_t> frameUtf8(std::vector<uint8_t> name8, std::vector<uint8_t> bio, std::vector<uint8_t> blob = {}) {
    name8.resize(8, 0);
    std::vector<uint8_t> b = {8, 0, 1, 0};
    b.insert(b.end(), name8.begin(), name8.end());
    for (const auto* v : {&bio, &blob}) {
        const uint32_t n = uint32_t(v->size());
        for (int i = 0; i < 4; ++i) b.push_back(uint8_t(n >> (8 * i)));
        b.insert(b.end(), v->begin(), v->end());
    }
    return b;
}

TEST(CheckedWritesValidator, Utf8CheckRejectsBadString) {
    const Descriptor d = Utf8D();
    Validator v(d);
    const auto rec = frameUtf8({'a'}, {0x61, 0x80});
    const ValResult r = v.validate(rec, StringCheck::Utf8);
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.fieldId, d.fields[1].id);
    EXPECT_STREQ(r.reason, "string is not valid UTF-8");
    EXPECT_TRUE(v.validate(rec, StringCheck::None).ok);
}

TEST(CheckedWritesValidator, Utf8CheckRejectsBadFixedStr) {
    Validator v(Utf8D());
    const ValResult r = v.validate(frameUtf8({0x80}, {}), StringCheck::Utf8);
    EXPECT_FALSE(r.ok);
    EXPECT_STREQ(r.reason, "string is not valid UTF-8");
}

TEST(CheckedWritesValidator, FixedStrPaddingMustBeZero) {
    Validator v(Utf8D());
    const ValResult dirty = v.validate(frameUtf8({'a', 0, 'b'}, {}), StringCheck::Utf8);
    EXPECT_FALSE(dirty.ok);
    EXPECT_STREQ(dirty.reason, "string(N) padding is not zero");
    EXPECT_TRUE(v.validate(frameUtf8({'a', 'b'}, {}), StringCheck::Utf8).ok);
    EXPECT_TRUE(v.validate(frameUtf8({}, {}), StringCheck::Utf8).ok);
    EXPECT_TRUE(v.validate(frameUtf8({'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}, {}), StringCheck::Utf8).ok);
}

TEST(CheckedWritesValidator, BytesFieldIsNeverChecked) {
    Validator v(Utf8D());
    EXPECT_TRUE(v.validate(frameUtf8({'a'}, {}, {0xFF, 0xFE, 0x80}), StringCheck::Utf8).ok);
}

TEST(CheckedWritesValidator, OneArgValidateIsUnchanged) {
    for (const auto& rec : {frame(1, 1, "ok"), frame(1, 9, "x"), std::vector<uint8_t>{12, 0, 1, 0}}) {
        Validator v(D());
        const ValResult a = v.validate(rec), b = v.validate(rec, StringCheck::None);
        EXPECT_EQ(a.ok, b.ok);
        EXPECT_EQ(a.fieldId, b.fieldId);
        EXPECT_EQ(a.reason, b.reason);
    }
}

TEST(CheckedWritesValidator, TopicKeyTypes) {
    EXPECT_TRUE(isTopicKeyType(PrimType::UInt16, 2));
    EXPECT_TRUE(isTopicKeyType(PrimType::Int32, 4));
    EXPECT_TRUE(isTopicKeyType(PrimType::UInt32, 4));
    EXPECT_TRUE(isTopicKeyType(PrimType::Int64, 8));
    EXPECT_TRUE(isTopicKeyType(PrimType::UInt64, 8));
    EXPECT_TRUE(isTopicKeyType(PrimType::FixedStr, 1));
    EXPECT_TRUE(isTopicKeyType(PrimType::FixedStr, 16));
    EXPECT_FALSE(isTopicKeyType(PrimType::FixedStr, 17));
    EXPECT_FALSE(isTopicKeyType(PrimType::String, 0));
    EXPECT_FALSE(isTopicKeyType(PrimType::Bytes, 0));
    EXPECT_FALSE(isTopicKeyType(PrimType::Float64, 8));
    EXPECT_FALSE(isTopicKeyType(PrimType::Bool, 1));
    EXPECT_FALSE(isTopicKeyType(PrimType::Enum, 4));
}
