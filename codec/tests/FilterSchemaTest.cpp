// FilterSchemaTest.cpp — Phase 2c Task 3: FilterSchema slots per tag per version, extendFrom, parseFilter.
#include "FilterTestSupport.h"

#include <rune/codec/Filter.h>
#include <rune/codec/FilterSchema.h>

#include <gtest/gtest.h>

#include <vector>

using namespace rune::codec;
using namespace filtertest;
using rune::Match;
using rune::anyOf;

TEST(FilterSchema, SlotsMatchMaterializedOffsetsPerVersion) {
    const Descriptor d = playerEvent();
    const FilterSchema s = FilterSchema::build(d, 3);
    EXPECT_EQ(s.maxVersion(), 3);
    EXPECT_EQ(s.fieldCount(), d.fields.size());
    for (uint16_t v = 1; v <= 3; ++v) {
        const Descriptor m = materializeAsOf(d, v);
        for (const auto& f : d.fields) {
            const int32_t i = s.indexOfTag(f.id);
            ASSERT_GE(i, 0) << f.name;
            const FieldSlot sl = s.slot(i, v);
            const auto* mf = m.find(f.name);
            // a retired name may coexist with a live one of the same tag-distinct name; match by tag
            const FieldDescriptor* byTag = nullptr;
            for (const auto& x : m.fields) if (x.id == f.id) byTag = &x;
            (void)mf;
            if (s.field(i).why != FilterSchema::Ineligible::No) { EXPECT_FALSE(sl.present) << f.name; continue; }
            if (!byTag) { EXPECT_FALSE(sl.present) << f.name << " v" << v; continue; }
            ASSERT_TRUE(sl.present) << f.name << " v" << v;
            EXPECT_EQ(sl.offset, 4u + byTag->offset) << f.name << " v" << v;
            EXPECT_EQ(sl.width, filterValueWidth(f.type, f.fixedLen));
            EXPECT_EQ(sl.type, f.type);
        }
    }
}

TEST(FilterSchema, RetiredFieldIsAbsentInLaterVersions) {
    const FilterSchema s = FilterSchema::build(playerEvent(), 3);
    const int32_t ch = s.indexOfTag(2);
    ASSERT_GE(ch, 0);
    EXPECT_TRUE(s.slot(ch, 1).present);
    EXPECT_TRUE(s.slot(ch, 2).present);
    EXPECT_FALSE(s.slot(ch, 3).present);
}

TEST(FilterSchema, NewFieldIsAbsentInEarlierVersions) {
    const FilterSchema s = FilterSchema::build(playerEvent(), 3);
    const int32_t g = s.indexOfTag(9);
    ASSERT_GE(g, 0);
    EXPECT_FALSE(s.slot(g, 1).present);
    EXPECT_TRUE(s.slot(g, 2).present);
    EXPECT_TRUE(s.slot(g, 3).present);
    EXPECT_FALSE(s.slot(g, 99).present);   // beyond the table
}

TEST(FilterSchema, VarLenAndFloatFieldsAreIneligible) {
    const FilterSchema s = FilterSchema::build(playerEvent(), 3);
    EXPECT_EQ(s.field(s.indexOfTag(7)).why, FilterSchema::Ineligible::VariableLength);
    EXPECT_EQ(s.field(s.indexOfTag(8)).why, FilterSchema::Ineligible::Float);
    EXPECT_EQ(s.field(s.indexOfTag(1)).why, FilterSchema::Ineligible::No);
    DescriptorBuilder b("t");
    b.addTagged("k", 0, PrimType::UInt64, 1, 0xFFFF, true, {});
    b.addFixedStrTagged("long", 1, 17, 1, 0xFFFF, false);
    const FilterSchema t = FilterSchema::build(b.build(), 1);
    EXPECT_EQ(t.field(t.indexOfTag(1)).why, FilterSchema::Ineligible::FixedStrTooLong);
}

TEST(FilterSchema, ExtendFromKeepsFieldIndicesStable) {
    FilterSchema s = FilterSchema::build(playerEvent(1), 1);
    const int32_t before = s.indexOfTag(1);
    ASSERT_GE(before, 0);
    EXPECT_EQ(s.indexOfTag(9), -1);
    ASSERT_TRUE(s.extendFrom(FilterSchema::build(playerEvent(3), 3)));
    EXPECT_EQ(s.indexOfTag(1), before);
    EXPECT_EQ(s.maxVersion(), 3);
    const int32_t g = s.indexOfTag(9);
    ASSERT_GE(g, 0);
    EXPECT_TRUE(s.slot(g, 2).present);
    EXPECT_FALSE(s.slot(g, 1).present);
    const int32_t ch = s.indexOfTag(2);
    EXPECT_TRUE(s.slot(ch, 2).present);
    EXPECT_FALSE(s.slot(ch, 3).present);
    // the extended schema is equivalent to a fresh build
    const FilterSchema fresh = FilterSchema::build(playerEvent(3), 3);
    for (const auto& f : playerEvent(3).fields)
        for (uint16_t v = 1; v <= 3; ++v) {
            const FieldSlot a = s.slot(s.indexOfTag(f.id), v), b = fresh.slot(fresh.indexOfTag(f.id), v);
            EXPECT_EQ(a.present, b.present); EXPECT_EQ(a.offset, b.offset); EXPECT_EQ(a.width, b.width);
        }
}

TEST(FilterSchemaRegression, ExtendFromRefusesAChangedFieldTypeRegression) {
    FilterSchema s = FilterSchema::build(playerEvent(2), 2);
    DescriptorBuilder b("player_event");
    b.addTagged("playerId", 0, PrimType::UInt64, 1, 0xFFFF, true, {});
    b.addTagged("worldId", 1, PrimType::Int64, 1, 0xFFFF, false, {});   // tag 1 retyped
    const FilterSchema bad = FilterSchema::build(b.build(), 3);
    const int32_t w = s.indexOfTag(1);
    const FieldSlot before = s.slot(w, 2);
    EXPECT_FALSE(s.extendFrom(bad));
    EXPECT_EQ(s.maxVersion(), 2);                 // unchanged
    EXPECT_EQ(s.indexOfTag(9), s.indexOfTag(9));
    EXPECT_EQ(s.slot(w, 2).offset, before.offset);
    EXPECT_EQ(s.field(w).type, PrimType::UInt32);
    // fewer versions is also not an extension
    FilterSchema t = FilterSchema::build(playerEvent(3), 3);
    EXPECT_FALSE(t.extendFrom(FilterSchema::build(playerEvent(1), 1)));
    EXPECT_EQ(t.maxVersion(), 3);
}

namespace {
struct Bad { const char* name; std::vector<uint8_t> bytes; FilterErrorKind kind; uint8_t term; };
const uint8_t kU32 = static_cast<uint8_t>(PrimType::UInt32);
const uint8_t kI32 = static_cast<uint8_t>(PrimType::Int32);
// one u32 term (tag 1), value 7: the well-formed baseline [1,1, 1,0, u32, Eq, 1, 7,0,0,0]
std::vector<uint8_t> term(uint16_t tag, uint8_t prim, uint8_t op, uint8_t count, std::vector<uint8_t> vals) {
    std::vector<uint8_t> t{uint8_t(tag), uint8_t(tag >> 8), prim, op, count};
    t.insert(t.end(), vals.begin(), vals.end());
    return t;
}
std::vector<uint8_t> filt(uint8_t ver, uint8_t n, std::vector<std::vector<uint8_t>> terms) {
    std::vector<uint8_t> f{ver, n};
    for (auto& t : terms) f.insert(f.end(), t.begin(), t.end());
    return f;
}
const std::vector<uint8_t> v7{7, 0, 0, 0}, v8{8, 0, 0, 0};
}  // namespace

TEST(FilterSchema, ParseAcceptsAWellFormedFilter) {
    const FilterSchema s = FilterSchema::build(playerEvent(), 3);
    auto b = filt(1, 2, {term(1, kU32, 2, 2, {7, 0, 0, 0, 8, 0, 0, 0}), term(2, kI32, 1, 1, {1, 0, 0, 0})});
    auto p = parseFilter(b, s);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->termCount, 2);
    EXPECT_EQ(p->terms[0].op, FilterOp::In);
    EXPECT_EQ(p->terms[0].count, 2);
    EXPECT_EQ(p->terms[0].width, 4);
    EXPECT_EQ(p->terms[0].field, s.indexOfTag(1));
    EXPECT_EQ(p->terms[1].tag, 2);
}

TEST(FilterSchema, ParseRejectsEveryMalformedShape) {
    const FilterSchema s = FilterSchema::build(playerEvent(), 3);
    std::vector<uint8_t> in65{1, 1, 1, 0, kU32, 2, 65};
    for (int i = 0; i < 65 * 4; ++i) in65.push_back(0);
    std::vector<Bad> cases = {
        {"empty", {}, FilterErrorKind::Malformed, 0xFF},
        {"bad formatVer", filt(2, 1, {term(1, kU32, 1, 1, v7)}), FilterErrorKind::BadFormatVersion, 0xFF},
        {"zero terms", {1, 0}, FilterErrorKind::TermCount, 0xFF},
        {"nine terms", {1, 9}, FilterErrorKind::TermCount, 0xFF},
        {"op 3", filt(1, 1, {term(1, kU32, 3, 1, v7)}), FilterErrorKind::UnknownOp, 0},
        {"op 0", filt(1, 1, {term(1, kU32, 0, 1, v7)}), FilterErrorKind::UnknownOp, 0},
        {"Eq count 2", filt(1, 1, {term(1, kU32, 1, 2, {7, 0, 0, 0, 8, 0, 0, 0})}), FilterErrorKind::ValueCount, 0},
        {"In count 0", filt(1, 1, {term(1, kU32, 2, 0, {})}), FilterErrorKind::ValueCount, 0},
        {"In count 65", in65, FilterErrorKind::ValueCount, 0},
        {"truncated value", filt(1, 1, {term(1, kU32, 1, 1, {7, 0, 0})}), FilterErrorKind::Malformed, 0},
        {"truncated term header", {1, 1, 1, 0, kU32}, FilterErrorKind::Malformed, 0},
        {"second term truncated", filt(1, 2, {term(1, kU32, 1, 1, v7), term(2, kI32, 1, 1, {1, 0})}), FilterErrorKind::Malformed, 1},
        {"trailing bytes", filt(1, 1, {term(1, kU32, 1, 1, v7), {0xAA}}), FilterErrorKind::Malformed, 0xFF},
        {"fewer terms than declared", filt(1, 2, {term(1, kU32, 1, 1, v7)}), FilterErrorKind::Malformed, 1},
        {"descending tags", filt(1, 2, {term(2, kI32, 1, 1, {1, 0, 0, 0}), term(1, kU32, 1, 1, v7)}), FilterErrorKind::NotCanonical, 1},
        {"duplicate tags", filt(1, 2, {term(1, kU32, 1, 1, v7), term(1, kU32, 1, 1, v8)}), FilterErrorKind::NotCanonical, 1},
        {"duplicate In values", filt(1, 1, {term(1, kU32, 2, 2, {7, 0, 0, 0, 7, 0, 0, 0})}), FilterErrorKind::NotCanonical, 0},
        {"descending In values", filt(1, 1, {term(1, kU32, 2, 2, {8, 0, 0, 0, 7, 0, 0, 0})}), FilterErrorKind::NotCanonical, 0},
    };
    for (const auto& c : cases) {
        auto p = parseFilter(c.bytes, s);
        ASSERT_FALSE(p.has_value()) << c.name;
        EXPECT_EQ(p.error().kind, c.kind) << c.name;
        EXPECT_EQ(p.error().termIndex, c.term) << c.name;
    }
}

TEST(FilterSchema, ParseRejectsSchemaMismatches) {
    const FilterSchema s = FilterSchema::build(playerEvent(), 3);
    {   auto p = parseFilter(filt(1, 1, {term(42, kU32, 1, 1, v7)}), s);
        ASSERT_FALSE(p); EXPECT_EQ(p.error().kind, FilterErrorKind::UnknownTag); EXPECT_EQ(p.error().termIndex, 0); EXPECT_EQ(p.error().tag, 42); }
    {   auto p = parseFilter(filt(1, 1, {term(7, static_cast<uint8_t>(PrimType::String), 1, 1, v7)}), s);
        ASSERT_FALSE(p); EXPECT_EQ(p.error().kind, FilterErrorKind::VariableLength); }
    {   auto p = parseFilter(filt(1, 2, {term(1, kU32, 1, 1, v7), term(8, static_cast<uint8_t>(PrimType::Float64), 1, 1, {0, 0, 0, 0, 0, 0, 0, 0})}), s);
        ASSERT_FALSE(p); EXPECT_EQ(p.error().kind, FilterErrorKind::FloatField); EXPECT_EQ(p.error().termIndex, 1); }
    {   auto p = parseFilter(filt(1, 1, {term(1, static_cast<uint8_t>(PrimType::Int64), 1, 1, {7, 0, 0, 0, 0, 0, 0, 0})}), s);
        ASSERT_FALSE(p); EXPECT_EQ(p.error().kind, FilterErrorKind::TypeMismatch); EXPECT_EQ(p.error().termIndex, 0); }
}

TEST(FilterSchema, ParseAcceptsWriterOutput) {
    const FilterSchema s = FilterSchema::build(playerEvent(), 3);
    FilterBytes fb;
    FilterWriter w(std::span<uint8_t>(fb.bytes.data(), fb.bytes.size()));
    w.term(1, PrimType::UInt32, 4, Match<uint32_t>(anyOf(9u, 3u, 3u, 5u)));
    w.term(3, PrimType::FixedStr, 8, Match<std::array<char, 8>>("eu"));
    w.term(6, PrimType::Int64, 8, Match<int64_t>(anyOf<int64_t>(-1, 1)));
    auto n = w.finish();
    ASSERT_TRUE(n);
    auto p = parseFilter(std::span<const uint8_t>(fb.bytes.data(), *n), s);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->termCount, 3);
    EXPECT_EQ(p->terms[0].count, 3);
    EXPECT_EQ(p->terms[2].count, 2);
}
