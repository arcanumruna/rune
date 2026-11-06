// libs/view/tests/view/CheckedWritesKeyedFoldTest.cpp
//
// KeyedFoldSpec (built once from the compiled plan) and deriveViewKey (one view key from a record, no allocation).
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/view/KeyedFold.h>
#include <rune/view/PlanCompiler.h>

namespace {

using rune::codec::PrimType;
using namespace rune::view;

constexpr int32_t kViewTopic = 0x40000007;

rune::codec::Descriptor characterSource() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64)
        .addFixedStr("name", 16).addFixedStr("tag", 80).add("provider", PrimType::UInt16)
        .addFixedStr("subject", 64).add("gender", PrimType::UInt16).add("lastPlayed", PrimType::Int64).build();
}

CompileResult compileOver(std::vector<KeyComponent> key, const rune::codec::Descriptor& d, const char* sourceKey = "characterId") {
    ViewPlan v;
    v.viewName = "char_view";
    v.sources = {{5, sourceKey}};
    for (const auto& k : key) {
        if (k.keyFunction) v.unicodeVersion = rune::codec::kUnicodeVersion;
    }
    v.keyComponents = std::move(key);
    v.output.push_back({"gender", d.find("gender")->type, 0, "gender", MergeRule::Replace});
    const rune::codec::Descriptor* srcs[] = {&d};
    auto r = compileView(v, srcs, kViewTopic);
    EXPECT_TRUE(r.has_value()) << (r ? "" : r.error().message);
    return std::move(*r);
}

void put(std::vector<uint8_t>& rec, const rune::codec::Descriptor& d, const char* field, uint64_t v) {
    const auto* f = d.find(field);
    for (uint32_t i = 0; i < rune::codec::fieldWidth(*f); ++i) rec[4 + f->offset + i] = uint8_t(v >> (8 * i));
}
void putStr(std::vector<uint8_t>& rec, const rune::codec::Descriptor& d, const char* field, const std::string& s) {
    const auto* f = d.find(field);
    for (size_t i = 0; i < s.size(); ++i) rec[4 + f->offset + i] = uint8_t(s[i]);
}
std::vector<uint8_t> blank(const rune::codec::Descriptor& d) {
    std::vector<uint8_t> rec(4 + d.blockLength, 0);
    rec[0] = uint8_t(d.blockLength); rec[1] = uint8_t(d.blockLength >> 8);
    rec[2] = uint8_t(d.version);     rec[3] = uint8_t(d.version >> 8);
    return rec;
}

std::vector<uint16_t> offsetsOf(const KeyedFoldSpec& s) {
    std::vector<uint16_t> o;
    for (const auto& c : s.components) o.push_back(c.offset);
    return o;
}

KeyedFoldSpec specOf(const CompileResult& r, const rune::codec::Descriptor& d) {
    auto s = buildKeyedFoldSpec(r.plan, d);
    EXPECT_TRUE(s.has_value());
    return *s;
}

} // namespace

TEST(CheckedWritesKeyedFold, DerivesTheCompositeKey) {
    const auto d = characterSource();
    const auto spec = specOf(compileOver({{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, d), d);
    auto rec = blank(d);
    put(rec, d, "userId", 42); put(rec, d, "characterId", 7);
    ViewKey out;
    ASSERT_EQ(deriveViewKey(spec, d, rec, out), DeriveKey::Ok);
    ASSERT_EQ(out.len, 16u);
    std::vector<uint8_t> want(16, 0);
    want[7] = 0x2A; want[15] = 0x07;
    EXPECT_EQ(std::vector<uint8_t>(out.bytes.begin(), out.bytes.begin() + 16), want);
}

TEST(CheckedWritesKeyedFold, DerivesTheLoweredKey) {
    const auto d = characterSource();
    const auto spec = specOf(compileOver({{"name", 1, "nameKey"}, {"characterId", 0, "characterId"}}, d), d);
    auto rec = blank(d);
    putStr(rec, d, "name", "FoO"); put(rec, d, "characterId", 7);
    ViewKey out;
    out.bytes.fill(0xEE);
    ASSERT_EQ(deriveViewKey(spec, d, rec, out), DeriveKey::Ok);
    ASSERT_EQ(out.len, 32u);
    std::vector<uint8_t> want(32, 0);
    want[0] = 0x66; want[1] = 0x6F; want[2] = 0x6F;  // foo
    want[31] = 7;
    EXPECT_EQ(std::vector<uint8_t>(out.bytes.begin(), out.bytes.begin() + 32), want);
}

TEST(CheckedWritesKeyedFold, AbsentComponentIsAbsent) {
    const auto evolving = rune::codec::DescriptorBuilder("character", 2)
        .add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64, 2).add("gender", PrimType::UInt16).build();
    const auto spec = specOf(compileOver({{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, evolving), evolving);
    const auto writer = rune::codec::materializeAsOf(evolving, 1);
    auto rec = blank(writer);
    put(rec, writer, "characterId", 7);
    ViewKey out;
    EXPECT_EQ(deriveViewKey(spec, writer, rec, out), DeriveKey::Absent);
    auto rec2 = blank(evolving);
    put(rec2, evolving, "userId", 1); put(rec2, evolving, "characterId", 7);
    EXPECT_EQ(deriveViewKey(spec, evolving, rec2, out), DeriveKey::Ok);
}

TEST(CheckedWritesKeyedFold, SpecOffsetsAndFlags) {
    const auto d = characterSource();
    struct Shape { std::vector<KeyComponent> key; std::vector<uint16_t> offsets; uint16_t prefix, width; };
    const Shape shapes[] = {
        {{{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, {0, 8}, 8, 16},
        {{{"provider", 0, "provider"}, {"subject", 0, "subject"}, {"characterId", 0, "characterId"}}, {0, 2, 66}, 66, 74},
        {{{"name", 1, "nameKey"}, {"characterId", 0, "characterId"}}, {0, 24}, 24, 32},
        {{{"tag", 1, "tagKey"}, {"characterId", 0, "characterId"}}, {0, 120}, 120, 128},
    };
    for (const auto& s : shapes) {
        const auto spec = specOf(compileOver(s.key, d), d);
        EXPECT_EQ(offsetsOf(spec), s.offsets);
        EXPECT_EQ(spec.prefixWidth, s.prefix);
        EXPECT_EQ(spec.keyWidth, s.width);
        EXPECT_TRUE(spec.ownerMapped);
        EXPECT_FALSE(spec.sourceKeyed);
        EXPECT_EQ(spec.wireKeyWidth, 8u);
    }
    const auto sd = rune::codec::DescriptorBuilder("coded", 1).addFixedStr("code", 16, 1, true).add("gender", PrimType::UInt16).build();
    const auto spec = specOf(compileOver({{"code", 0, "code"}}, sd, "code"), sd);
    EXPECT_TRUE(spec.sourceKeyed);
    EXPECT_FALSE(spec.ownerMapped);
    EXPECT_EQ(spec.wireKeyWidth, 16u);
    EXPECT_EQ(spec.keyWidth, 16u);
}

TEST(CheckedWritesKeyedFold, LegacyPlanAndMissingFieldGiveNoSpec) {
    const auto d = characterSource();
    ViewPlan v;
    v.viewName = "char_all"; v.key = "characterId"; v.sources = {{5, "characterId"}};
    v.output.push_back({"gender", PrimType::UInt16, 0, "gender", MergeRule::Replace});
    const rune::codec::Descriptor* srcs[] = {&d};
    const auto legacy = compileView(v, srcs, kViewTopic);
    ASSERT_TRUE(legacy.has_value());
    EXPECT_FALSE(buildKeyedFoldSpec(legacy->plan, d).has_value());
    const auto keyed = compileOver({{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, d);
    const auto narrow = rune::codec::DescriptorBuilder("character", 1).add("characterId", PrimType::UInt64, 1, true).build();
    EXPECT_FALSE(buildKeyedFoldSpec(keyed.plan, narrow).has_value());
}
