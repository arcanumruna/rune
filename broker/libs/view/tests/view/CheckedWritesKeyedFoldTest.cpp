// libs/view/tests/view/CheckedWritesKeyedFoldTest.cpp
//
// KeyedFoldSpec (built once from the compiled plan) and deriveViewKey (one view key from a record, no allocation).
#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/view/KeyedFold.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewFold.h>

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

// ---- Task 9: the keyed projection fold ---------------------------------------------------------------------------

namespace {

using rune::codec::FieldValue;

ViewKey nameKey(const std::string& lowered, uint64_t id) {
    ViewKey k;
    k.len = 32;
    std::copy(lowered.begin(), lowered.end(), k.bytes.begin());
    for (int i = 0; i < 8; ++i) k.bytes[24 + i] = uint8_t(id >> (8 * (7 - i)));
    return k;
}
ViewKey userKey(uint64_t user, uint64_t id) {
    ViewKey k;
    k.len = 16;
    for (int i = 0; i < 8; ++i) {
        k.bytes[i] = uint8_t(user >> (8 * (7 - i)));
        k.bytes[8 + i] = uint8_t(id >> (8 * (7 - i)));
    }
    return k;
}
std::string wireOf(uint64_t id) {
    std::string s(8, '\0');
    for (int i = 0; i < 8; ++i) s[i] = char(id >> (8 * i));
    return s;
}

struct Rec { std::string key; std::vector<uint8_t> value; };

struct KeyedRig {
    rune::codec::Descriptor src;
    CompileResult comp;
    KeyedFoldSpec spec;
    VersionDescriptorCache vdc;
    ViewTable table;
    ViewTable ownerTable;
    std::optional<UniqueClaims> claims;   // unique and owner-mapped
    Prospect prospect;
    ProjectionFold pf;
    FoldStats total;

    static RowLayout ownerLayout(uint16_t keyWidth) {
        RowLayout l;
        l.rowLen = keyWidth;
        return l;
    }
    KeyedRig(CompileResult c, rune::codec::Descriptor evolving, PrimType keyType = PrimType::UInt64)
        : src(std::move(evolving)), comp(std::move(c)), spec(*buildKeyedFoldSpec(comp.plan, src)), vdc(src),
          table(comp.rowLayout, 256, 1u << 16, {uint8_t(spec.keyWidth), uint8_t(spec.prefixWidth), 7}),
          ownerTable(ownerLayout(spec.keyWidth), 256, 1u << 16, {spec.wireKeyWidth, 0, 7}),
          claims(comp.plan.unique && spec.ownerMapped ? std::optional<UniqueClaims>(std::in_place, spec.keyWidth, spec.wireKeyWidth, comp.rowLayout.rowLen, keyType, 7, 256, 1u << 16) : std::nullopt),
          pf{comp.plan, comp.rowLayout, vdc, table, keyType, &spec, spec.ownerMapped ? &ownerTable : nullptr, claims ? &*claims : nullptr} {
        prospect.reserve(comp.rowLayout, nullptr);
    }
    void add(const FoldStats& s) {
        total.applied += s.applied; total.deleted += s.deleted; total.skippedMalformed += s.skippedMalformed;
        total.skippedNoKey += s.skippedNoKey; total.keyConflicts += s.keyConflicts; total.ownerMoves += s.ownerMoves;
    }
    // One batch of records, each prepared and applied in order.
    void fold(const std::vector<Rec>& recs) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        for (const auto& r : recs) {
            b.append(r.key, std::string_view(reinterpret_cast<const char*>(r.value.data()), r.value.size()), 0);
        }
        const auto bytes = b.build();
        rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
        ASSERT_TRUE(r.ensureValid());
        r.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset, int64_t) {
            SourceRecord rec{key, value, offset, 0, nullptr};
            const auto st = prepareProjection(pf, rec, prospect);
            add(prospect.stats);
            if (st == PrepareStatus::Ok) applyProjection(pf, rec, prospect);
        });
    }
    void fold(const Rec& r) { fold(std::vector<Rec>{r}); }
    [[nodiscard]] uint16_t slotOf(const char* field) const {
        const auto* f = src.find(field);
        for (const auto& o : comp.plan.output) {
            if (o.srcFieldId == f->id) return o.outSlot;
        }
        ADD_FAILURE() << field;
        return 0;
    }
    [[nodiscard]] const uint8_t* row(const ViewKey& k) const { return table.get(k); }
    [[nodiscard]] FieldValue read(const ViewKey& k, const char* field) const {
        return readSlot(table.get(k), comp.rowLayout, slotOf(field));
    }
    [[nodiscard]] std::optional<ViewKey> owner(uint64_t id) const {
        const std::string w = wireOf(id);
        const uint8_t* p = ownerTable.get(ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(w.data()), 8}));
        if (!p) return std::nullopt;
        return ViewKey::fromBytes({p, spec.keyWidth});
    }
    [[nodiscard]] size_t scan(const ViewKey& prefix, uint16_t width) const {
        size_t n = 0;
        std::array<uint8_t, kScanTokenBytes> next{};
        table.scanPrefix({prefix.bytes.data(), width}, {}, next, [&](auto, auto) { ++n; return true; });
        return n;
    }
};

Rec charRec(const rune::codec::Descriptor& d, uint64_t id, const std::string& name, uint64_t user = 0, uint64_t gender = 0) {
    auto v = blank(d);
    put(v, d, "characterId", id);
    if (d.find("userId")) put(v, d, "userId", user);
    putStr(v, d, "name", name);
    put(v, d, "gender", gender);
    return {wireOf(id), v};
}
Rec tomb(uint64_t id) { return {wireOf(id), {}}; }

CompileResult lowerView(const rune::codec::Descriptor& d, bool unique = false) {
    ViewPlan v;
    v.viewName = "by_name";
    v.sources = {{5, "characterId"}};
    v.unicodeVersion = rune::codec::kUnicodeVersion;
    v.unique = unique;
    v.keyComponents = {{"name", 1, "nameKey"}};
    if (!unique) v.keyComponents.push_back({"characterId", 0, "characterId"});
    v.output.push_back({"gender", PrimType::UInt16, 0, "gender", MergeRule::Replace});
    const rune::codec::Descriptor* srcs[] = {&d};
    auto r = compileView(v, srcs, kViewTopic);
    EXPECT_TRUE(r.has_value()) << (r ? "" : r.error().message);
    return std::move(*r);
}

size_t countDirty(ViewTable& t, size_t& deletes) {
    size_t upserts = 0;
    t.drainDirtyKeyed([&](uint32_t, const uint8_t*, bool del, std::span<const uint8_t>) { (del ? deletes : upserts)++; });
    return upserts;
}

} // namespace

TEST(CheckedWritesKeyedFold, CompositeRowsAndPrefix) {
    const auto d = characterSource();
    KeyedRig rig(compileOver({{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, d), d);
    rig.fold({charRec(d, 1, "a", 42), charRec(d, 2, "Bee", 42, 3), charRec(d, 3, "c", 42), charRec(d, 4, "d", 43)});
    EXPECT_EQ(rig.table.size(), 4u);
    EXPECT_EQ(rig.scan(userKey(42, 0), 8), 3u);
    EXPECT_EQ(rig.scan(userKey(43, 0), 8), 1u);
    const ViewKey k = userKey(42, 2);
    ASSERT_NE(rig.row(k), nullptr);
    EXPECT_EQ(rig.read(k, "userId").u, 42u);
    EXPECT_EQ(rig.read(k, "characterId").u, 2u);
    EXPECT_EQ(rig.read(k, "gender").u, 3u);
}

TEST(CheckedWritesKeyedFold, RenameMovesTheRowAndTheOwner) {
    const auto d = characterSource();
    KeyedRig rig(lowerView(d), d);
    rig.fold(charRec(d, 7, "Foo"));
    size_t del = 0;
    EXPECT_EQ(countDirty(rig.table, del) + countDirty(rig.ownerTable, del), 2u);
    EXPECT_EQ(del, 0u);
    rig.fold(charRec(d, 7, "Bar"));
    EXPECT_EQ(rig.row(nameKey("foo", 7)), nullptr);
    EXPECT_NE(rig.row(nameKey("bar", 7)), nullptr);
    ASSERT_TRUE(rig.owner(7).has_value());
    EXPECT_EQ(*rig.owner(7), nameKey("bar", 7));
    EXPECT_EQ(rig.total.ownerMoves, 1u);
    EXPECT_EQ(countDirty(rig.table, del) + countDirty(rig.ownerTable, del), 2u);
    EXPECT_EQ(del, 1u);
}

TEST(CheckedWritesKeyedFold, MoveGroupMovesTheRow) {
    const auto d = characterSource();
    KeyedRig rig(compileOver({{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, d), d);
    rig.fold(charRec(d, 7, "a", 42));
    rig.fold(charRec(d, 7, "a", 43));
    EXPECT_EQ(rig.scan(userKey(42, 0), 8), 0u);
    EXPECT_EQ(rig.scan(userKey(43, 0), 8), 1u);
    EXPECT_NE(rig.row(userKey(43, 7)), nullptr);
}

TEST(CheckedWritesKeyedFold, TombstoneErasesThroughTheOwnerMap) {
    const auto d = characterSource();
    KeyedRig rig(lowerView(d), d);
    rig.fold(charRec(d, 7, "Foo"));
    rig.fold(tomb(7));
    EXPECT_EQ(rig.table.size(), 0u);
    EXPECT_FALSE(rig.owner(7).has_value());
    EXPECT_EQ(rig.total.deleted, 1u);
    const std::string w = wireOf(7);
    SourceRecord again{w, {}, 0, 0, nullptr};
    EXPECT_EQ(prepareProjection(rig.pf, again, rig.prospect), PrepareStatus::Ok);
    EXPECT_EQ(rig.prospect.op, ProspectOp::None);
}

TEST(CheckedWritesKeyedFold, DeleteThenReuseTheName) {
    const auto d = characterSource();
    KeyedRig rig(lowerView(d), d);
    rig.fold(charRec(d, 7, "Foo"));
    rig.fold(tomb(7));
    rig.fold(charRec(d, 9, "Foo"));
    EXPECT_EQ(rig.table.size(), 1u);
    EXPECT_NE(rig.row(nameKey("foo", 9)), nullptr);
}

TEST(CheckedWritesKeyedFoldRegression, RenameAndBackInOneBatchLeavesOneRowRegression) {
    const auto d = characterSource();
    KeyedRig rig(lowerView(d), d);
    rig.fold({charRec(d, 7, "Foo"), charRec(d, 7, "Bar"), charRec(d, 7, "Foo")});
    EXPECT_EQ(rig.table.size(), 1u);
    EXPECT_NE(rig.row(nameKey("foo", 7)), nullptr);
    EXPECT_EQ(rig.row(nameKey("bar", 7)), nullptr);
    EXPECT_EQ(*rig.owner(7), nameKey("foo", 7));
}

TEST(CheckedWritesKeyedFoldRegression, RenameToTheOwnKeyIsAnUpdateNotAMoveRegression) {
    const auto d = characterSource();
    KeyedRig rig(lowerView(d), d);
    rig.fold(charRec(d, 7, "Foo", 0, 1));
    rig.fold(charRec(d, 7, "FOO", 0, 2));
    EXPECT_EQ(rig.total.ownerMoves, 0u);
    EXPECT_EQ(rig.table.size(), 1u);
    EXPECT_EQ(rig.read(nameKey("foo", 7), "gender").u, 2u);
}

TEST(CheckedWritesKeyedFold, DerivedColumnHoldsTheLoweredBytes) {
    const auto d = characterSource();
    KeyedRig rig(lowerView(d), d);
    rig.fold(charRec(d, 7, "\xC8\xBA\xC8\xBA"));   // two U+023A
    ASSERT_EQ(rig.table.size(), 1u);
    const auto owner = rig.owner(7);
    ASSERT_TRUE(owner.has_value());
    EXPECT_EQ(std::string(owner->bytes.begin(), owner->bytes.begin() + 6), "\xE2\xB1\xA5\xE2\xB1\xA5");
    const uint8_t* row = rig.table.get(*owner);
    ASSERT_NE(row, nullptr);
    const uint16_t slot = rig.comp.plan.keyComponents[0].outSlot;
    const FieldValue v = readSlot(row, rig.comp.rowLayout, slot);
    ASSERT_TRUE(v.present);
    EXPECT_EQ(v.type, PrimType::FixedStr);
    ASSERT_EQ(v.bytes.size(), 24u);
    EXPECT_EQ(std::memcmp(v.bytes.data(), owner->bytes.data(), 24), 0);
    const auto keyRow = rig.table.keyOfRow(row);
    ASSERT_EQ(keyRow.size(), 32u);
    EXPECT_EQ(std::memcmp(keyRow.data(), owner->bytes.data(), 32), 0);
    EXPECT_EQ(rig.read(*owner, "characterId").u, 7u);
}

TEST(CheckedWritesKeyedFold, AbsentKeyComponentErasesAndCounts) {
    const auto evolving = rune::codec::DescriptorBuilder("character", 2)
        .add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64, 2).addFixedStr("name", 16).add("gender", PrimType::UInt16).build();
    KeyedRig rig(compileOver({{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, evolving), evolving);
    rig.fold(charRec(evolving, 7, "a", 42));
    ASSERT_EQ(rig.table.size(), 1u);
    const auto v1 = rune::codec::materializeAsOf(evolving, 1);
    rig.fold(charRec(v1, 7, "a"));
    EXPECT_EQ(rig.table.size(), 0u);
    EXPECT_FALSE(rig.owner(7).has_value());
    EXPECT_EQ(rig.total.skippedNoKey, 1u);
}

TEST(CheckedWritesKeyedFold, StringSourceKeyedViewUsesTheWireKey) {
    const auto sd = rune::codec::DescriptorBuilder("coded", 1).addFixedStr("code", 16, 1, true).add("gender", PrimType::UInt16).build();
    KeyedRig rig(compileOver({{"code", 0, "code"}}, sd, "code"), sd, PrimType::FixedStr);
    ASSERT_FALSE(rig.spec.ownerMapped);
    auto v = blank(sd);
    putStr(v, sd, "code", "alice");
    put(v, sd, "gender", 4);
    const std::string wire = std::string("alice") + std::string(11, '\0');
    rig.fold(Rec{wire, v});
    ASSERT_EQ(rig.table.size(), 1u);
    EXPECT_EQ(rig.total.applied, 1u);
    rig.fold(Rec{wire, {}});
    EXPECT_EQ(rig.table.size(), 0u);
    EXPECT_EQ(rig.total.deleted, 1u);
}

TEST(CheckedWritesKeyedFold, UniqueConflictBecomesAnExtraClaimant) {
    const auto d = characterSource();
    KeyedRig rig(lowerView(d, true), d);
    rig.fold(charRec(d, 7, "Foo", 0, 1));
    rig.fold(charRec(d, 9, "foo", 0, 2));
    EXPECT_EQ(rig.claims->extras(), 1u);
    EXPECT_EQ(rig.total.keyConflicts, 0u);
    EXPECT_EQ(rig.table.size(), 1u);
    EXPECT_FALSE(rig.owner(9).has_value());
    ASSERT_TRUE(rig.owner(7).has_value());
    EXPECT_EQ(rig.read(*rig.owner(7), "gender").u, 1u);
}

TEST(CheckedWritesKeyedFoldRegression, LegacyPlansFoldExactlyAsBeforeRegression) {
    const auto d = rune::codec::DescriptorBuilder("character", 1).add("characterId", PrimType::Int64, 1, true).add("hp", PrimType::Int32).build();
    ViewPlan v;
    v.viewName = "char_hp"; v.key = "characterId"; v.sources = {{5, "characterId"}};
    v.output = {{"characterId", PrimType::Int64, 0, "characterId", MergeRule::Replace}, {"hp", PrimType::Int32, 0, "hp", MergeRule::Replace}};
    const rune::codec::Descriptor* srcs[] = {&d};
    auto comp = compileView(v, srcs, kViewTopic).value();
    ViewTable viaBatch(comp.rowLayout, 256, 1u << 16), viaPrepare(comp.rowLayout, 256, 1u << 16);
    VersionDescriptorCache vdcA(d), vdcB(d);
    Prospect prospect;
    prospect.reserve(comp.rowLayout, nullptr);
    ProjectionFold pf{comp.plan, comp.rowLayout, vdcB, viaPrepare, PrimType::Int64};
    EXPECT_EQ(pf.keyed, nullptr);
    FoldStats sb{};
    rune::protocol::RecordBatchBuilder b(0, 0);
    std::vector<std::pair<std::string, std::string>> recs;
    for (uint64_t id = 1; id <= 6; ++id) {
        std::string val(16, '\0');
        val[0] = 12; val[2] = 1; val[4] = char(id); val[12] = char(id * 3);
        recs.push_back({wireOf(id), id % 3 == 0 ? std::string() : val});
    }
    recs.push_back({wireOf(2), {}});
    recs.push_back({std::string(3, 'x'), "zz"});
    for (const auto& r : recs) b.append(r.first, r.second, 0);
    const auto bytes = b.build();
    const rune::storage::LogReadView lv{bytes.data(), bytes.size(), 0, int64_t(recs.size())};
    int64_t cur = 0;
    const FoldStats sa = foldBatch(comp.plan, comp.rowLayout, vdcA, viaBatch, lv, int64_t(recs.size()), cur, kNoSkip);
    rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
    r.forEachRecord([&](std::string_view key, std::string_view value, int64_t off, int64_t) {
        SourceRecord rec{key, value, off, 0, nullptr};
        if (prepareProjection(pf, rec, prospect) == PrepareStatus::Ok) applyProjection(pf, rec, prospect);
        sb.applied += prospect.stats.applied; sb.deleted += prospect.stats.deleted; sb.skippedMalformed += prospect.stats.skippedMalformed;
    });
    EXPECT_EQ(sa.applied, sb.applied);
    EXPECT_EQ(sa.deleted, sb.deleted);
    EXPECT_EQ(sa.skippedMalformed, sb.skippedMalformed);
    EXPECT_EQ(viaBatch.size(), viaPrepare.size());
    for (uint64_t id = 1; id <= 6; ++id) {
        const uint8_t* x = viaBatch.get(id);
        const uint8_t* y = viaPrepare.get(id);
        ASSERT_EQ(x == nullptr, y == nullptr);
        if (x) EXPECT_EQ(std::memcmp(x, y, comp.rowLayout.rowLen), 0);
    }
}

TEST(CheckedWritesKeyedFoldModelRegression, RandomLogsFoldToTheFreshRefold) {
    const auto d = characterSource();
    const char* names[] = {"Foo", "FOO", "bar", "Baz", "q"};
    for (uint32_t seed = 1; seed <= 20; ++seed) {
        std::mt19937 rng(seed);
        KeyedRig inc(lowerView(d), d);
        std::vector<Rec> log;
        std::map<uint64_t, std::pair<std::string, uint64_t>> model;   // id -> (lowered name, gender)
        for (int i = 0; i < 200; ++i) {
            const uint64_t id = 1 + rng() % 6;
            if (rng() % 4 == 0) {
                log.push_back(tomb(id));
                model.erase(id);
            } else {
                const std::string name = names[rng() % 5];
                const uint64_t g = rng() % 100;
                log.push_back(charRec(d, id, name, 0, g));
                std::string low = name;
                for (auto& c : low) c = char(std::tolower(static_cast<unsigned char>(c)));
                model[id] = {low, g};
            }
        }
        for (size_t at = 0; at < log.size();) {
            const size_t n = std::min<size_t>(1 + rng() % 7, log.size() - at);
            inc.fold(std::vector<Rec>(log.begin() + at, log.begin() + at + n));
            at += n;
        }
        KeyedRig fresh(lowerView(d), d);
        fresh.fold(log);
        EXPECT_EQ(inc.table.size(), model.size()) << seed;
        EXPECT_EQ(fresh.table.size(), model.size()) << seed;
        EXPECT_EQ(inc.ownerTable.size(), model.size()) << seed;
        for (uint64_t id = 1; id <= 6; ++id) {
            const auto m = model.find(id);
            for (auto* rig : {&inc, &fresh}) {
                ASSERT_EQ(rig->owner(id).has_value(), m != model.end()) << seed << " " << id;
                if (m == model.end()) continue;
                EXPECT_EQ(*rig->owner(id), nameKey(m->second.first, id));
                EXPECT_EQ(rig->read(nameKey(m->second.first, id), "gender").u, m->second.second);
            }
        }
    }
}
