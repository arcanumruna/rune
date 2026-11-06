// libs/view/tests/view/CheckedWritesUniqueFoldTest.cpp
//
// UniqueClaims and the unique rules (§8.3) of the keyed projection fold: the smallest source key owns a view key, the others are
// extras, and an extra is promoted when the owner leaves.
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/UniqueClaims.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewFold.h>

namespace {

using rune::codec::FieldValue;
using rune::codec::PrimType;
using namespace rune::view;

constexpr int32_t kViewTopic = 0x40000007;

std::vector<uint8_t> blank(const rune::codec::Descriptor& d) {
    std::vector<uint8_t> rec(4 + d.blockLength, 0);
    rec[0] = uint8_t(d.blockLength); rec[1] = uint8_t(d.blockLength >> 8);
    rec[2] = uint8_t(d.version);     rec[3] = uint8_t(d.version >> 8);
    return rec;
}
void putInt(std::vector<uint8_t>& rec, const rune::codec::Descriptor& d, const char* field, uint64_t v) {
    const auto* f = d.find(field);
    for (uint32_t i = 0; i < rune::codec::fieldWidth(*f); ++i) rec[4 + f->offset + i] = uint8_t(v >> (8 * i));
}
void putStr(std::vector<uint8_t>& rec, const rune::codec::Descriptor& d, const char* field, const std::string& s) {
    const auto* f = d.find(field);
    for (size_t i = 0; i < s.size(); ++i) rec[4 + f->offset + i] = uint8_t(s[i]);
}

struct Rec { std::string key; std::vector<uint8_t> value; };

// character(characterId <keyType> key, name string(16), gender u16); the view is `unique (lower(name) as nameKey)`.
rune::codec::Descriptor sourceOf(PrimType keyType, uint16_t version = 1) {
    rune::codec::DescriptorBuilder b("character", version);
    if (keyType == PrimType::FixedStr) b.addFixedStr("characterId", 16, 1, true);
    else b.add("characterId", keyType, 1, true);
    b.add("gender", PrimType::UInt16);
    b.addFixedStr("name", 16, version >= 2 ? 2 : 1);
    return b.build();
}

std::string wireOfInt(uint64_t id, uint32_t width = 8) {
    std::string s(width, '\0');
    for (uint32_t i = 0; i < width; ++i) s[i] = char(id >> (8 * i));
    return s;
}
std::string wireOf(uint64_t id) { return wireOfInt(id); }

Rec charRec(const rune::codec::Descriptor& d, const std::string& wire, const std::string& name, uint64_t gender) {
    auto v = blank(d);
    const auto* idField = d.find("characterId");
    for (size_t i = 0; i < wire.size(); ++i) v[4 + idField->offset + i] = uint8_t(wire[i]);
    if (d.find("name")) putStr(v, d, "name", name);
    putInt(v, d, "gender", gender);
    return {wire, v};
}
Rec charRec(const rune::codec::Descriptor& d, uint64_t id, const std::string& name, uint64_t gender = 0) {
    return charRec(d, wireOf(id), name, gender);
}
Rec tomb(const std::string& wire) { return {wire, {}}; }
Rec tomb(uint64_t id) { return tomb(wireOf(id)); }

ViewKey nameKey(const std::string& lowered) {
    ViewKey k;
    k.len = 24;
    std::copy(lowered.begin(), lowered.end(), k.bytes.begin());
    return k;
}

CompileResult uniqueView(const rune::codec::Descriptor& d, bool withKeyColumn = true) {
    ViewPlan v;
    v.viewName = "character_name";
    v.sources = {{5, "characterId"}};
    v.unicodeVersion = rune::codec::kUnicodeVersion;
    v.unique = true;
    v.keyComponents = {{"name", 1, "nameKey"}};
    if (withKeyColumn) v.output.push_back({"characterId", d.find("characterId")->type, 0, "characterId", MergeRule::Replace});
    v.output.push_back({"gender", PrimType::UInt16, 0, "gender", MergeRule::Replace});
    const rune::codec::Descriptor* srcs[] = {&d};
    auto r = compileView(v, srcs, kViewTopic);
    EXPECT_TRUE(r.has_value()) << (r ? "" : r.error().message);
    return std::move(*r);
}

struct Rig {
    rune::codec::Descriptor src;
    PrimType keyType;
    CompileResult comp;
    KeyedFoldSpec spec;
    VersionDescriptorCache vdc;
    ViewTable table;
    ViewTable owners;
    UniqueClaims claims;
    Prospect prospect;
    ProjectionFold pf;
    FoldStats total;

    static RowLayout ownerLayout(uint16_t keyWidth) {
        RowLayout l;
        l.rowLen = keyWidth;
        return l;
    }
    explicit Rig(PrimType kt = PrimType::UInt64, uint16_t version = 1, bool withKeyColumn = true)
        : src(sourceOf(kt, version)), keyType(kt), comp(uniqueView(src, withKeyColumn)), spec(*buildKeyedFoldSpec(comp.plan, src)), vdc(src),
          table(comp.rowLayout, 256, 1u << 16, {uint8_t(spec.keyWidth), uint8_t(spec.prefixWidth), 7}),
          owners(ownerLayout(spec.keyWidth), 256, 1u << 16, {spec.wireKeyWidth, 0, 7}),
          claims(spec.keyWidth, spec.wireKeyWidth, comp.rowLayout.rowLen, kt, 7, 256, 1u << 16),
          pf{comp.plan, comp.rowLayout, vdc, table, kt, &spec, &owners, &claims} {
        prospect.reserve(comp.rowLayout, nullptr);
    }
    void add(const FoldStats& s) {
        total.applied += s.applied; total.deleted += s.deleted; total.skippedMalformed += s.skippedMalformed;
        total.skippedNoKey += s.skippedNoKey; total.keyConflicts += s.keyConflicts; total.ownerMoves += s.ownerMoves;
        total.uniquePromotions += s.uniquePromotions;
    }
    // Through foldProjectionBatch; with `rng` every pass is capped at a random record bound and resumed from the cursor.
    void fold(const std::vector<Rec>& recs, std::mt19937* rng = nullptr) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        for (const auto& r : recs) b.append(r.key, std::string_view(reinterpret_cast<const char*>(r.value.data()), r.value.size()), 0);
        const auto bytes = b.build();
        const int64_t n = int64_t(recs.size());
        const rune::storage::LogReadView view{bytes.data(), bytes.size(), 0, n};
        int64_t cursor = 0;
        while (cursor < n) {
            const int64_t bound = rng ? std::min<int64_t>(n, cursor + 1 + int64_t((*rng)() % 5)) : n;
            const int64_t before = cursor;
            add(foldProjectionBatch(pf, prospect, view, bound, cursor, kNoSkip));
            ASSERT_GT(cursor, before);
        }
    }
    void fold(const Rec& r) { fold(std::vector<Rec>{r}); }
    [[nodiscard]] uint16_t slotOf(const char* field) const {
        const auto* f = src.find(field);
        for (const auto& o : comp.plan.output) if (o.srcFieldId == f->id) return o.outSlot;
        ADD_FAILURE() << field;
        return 0;
    }
    [[nodiscard]] FieldValue read(const std::string& lowered, const char* field) const {
        return readSlot(table.get(nameKey(lowered)), comp.rowLayout, slotOf(field));
    }
    [[nodiscard]] std::optional<std::string> ownerWire(const std::string& lowered) const {
        const uint8_t* p = claims.ownerOf(nameKey(lowered));
        if (!p) return std::nullopt;
        return std::string(reinterpret_cast<const char*>(p), spec.wireKeyWidth);
    }
    [[nodiscard]] std::optional<ViewKey> ownerMap(const std::string& wire) const {
        const uint8_t* p = owners.get(ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(wire.data()), wire.size()}));
        if (!p) return std::nullopt;
        return ViewKey::fromBytes({p, spec.keyWidth});
    }
    // Every table over the given universes.  `logical` leaves out the index head/flags and the extras' prev/next/links bytes: the
    // list is an O(1) head insert, so those bytes depend on the arrival order (deviation from "byte-identical", see the commit).
    [[nodiscard]] std::map<std::string, std::string> snapshot(const std::vector<std::string>& names, const std::vector<std::string>& wires,
                                                              bool logical = true) {
        std::map<std::string, std::string> out;
        const size_t rowLen = comp.rowLayout.rowLen, W = spec.wireKeyWidth, K = spec.keyWidth;
        for (const auto& nm : names) {
            const ViewKey k = nameKey(nm);
            if (const uint8_t* r = table.get(k)) out["view/" + nm] = std::string(reinterpret_cast<const char*>(r), rowLen);
            if (const uint8_t* r = claims.ownerOf(k))
                out["index/" + nm] = std::string(reinterpret_cast<const char*>(r), logical ? W + 4 : 2 * W + 5);
        }
        for (const auto& w : wires) {
            const auto key = ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(w.data()), w.size()});
            if (const uint8_t* r = owners.get(key)) out["owners/" + w] = std::string(reinterpret_cast<const char*>(r), K);
            if (const uint8_t* r = claims.extraKeyOf(w)) {
                std::string s(reinterpret_cast<const char*>(r), K);
                if (logical) s += std::string(reinterpret_cast<const char*>(r + K + 2 * W + 1), rowLen);
                else s = std::string(reinterpret_cast<const char*>(r), K + 2 * W + 1 + rowLen);
                out["extras/" + w] = s;
            }
        }
        return out;
    }
};

std::vector<std::string> allWires(uint64_t upTo) {
    std::vector<std::string> w;
    for (uint64_t i = 1; i <= upTo; ++i) w.push_back(wireOf(i));
    return w;
}
const std::vector<std::string> kNames = {"foo", "bar", "baz", "q"};

} // namespace

TEST(CheckedWritesUniqueFold, SmallestSourceKeyOwnsAPreExistingDuplicate) {
    Rig rig;
    rig.fold(charRec(rig.src, 9, "foo", 2));
    rig.fold(charRec(rig.src, 7, "Foo", 1));
    EXPECT_EQ(rig.table.size(), 1u);
    EXPECT_EQ(rig.read("foo", "characterId").u, 7u);
    EXPECT_EQ(rig.read("foo", "gender").u, 1u);
    EXPECT_EQ(rig.claims.extras(), 1u);
    EXPECT_EQ(rig.ownerWire("foo"), wireOf(7));
    EXPECT_EQ(rig.total.keyConflicts, 0u);
    ASSERT_TRUE(rig.ownerMap(wireOf(7)).has_value());
    EXPECT_FALSE(rig.ownerMap(wireOf(9)).has_value());
}

TEST(CheckedWritesUniqueFold, OwnerIsIndependentOfTheOrder) {
    std::vector<int> order = {0, 1, 2};
    std::optional<std::map<std::string, std::string>> first;
    const uint64_t ids[] = {3, 7, 9};
    do {
        Rig rig;
        for (int i : order) rig.fold(charRec(rig.src, ids[i], i == 1 ? "FOO" : "foo", ids[i]));
        const auto snap = rig.snapshot(kNames, allWires(12));
        if (!first) first = snap;
        EXPECT_EQ(snap, *first);
        EXPECT_EQ(rig.ownerWire("foo"), wireOf(3));
        EXPECT_EQ(rig.claims.extras(), 2u);
        EXPECT_EQ(rig.read("foo", "gender").u, 3u);
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST(CheckedWritesUniqueFold, PromotionOnDelete) {
    Rig rig;
    rig.fold({charRec(rig.src, 9, "foo", 2), charRec(rig.src, 7, "Foo", 1)});
    rig.fold(tomb(7));
    EXPECT_EQ(rig.read("foo", "characterId").u, 9u);
    EXPECT_EQ(rig.read("foo", "gender").u, 2u);
    EXPECT_EQ(rig.claims.extras(), 0u);
    EXPECT_EQ(rig.total.uniquePromotions, 1u);
    EXPECT_EQ(rig.ownerWire("foo"), wireOf(9));
    EXPECT_EQ(rig.ownerMap(wireOf(9)), nameKey("foo"));
    EXPECT_FALSE(rig.ownerMap(wireOf(7)).has_value());
    EXPECT_FALSE(rig.claims.hasExtras(nameKey("foo")));
}

TEST(CheckedWritesUniqueFold, PromotionOnRename) {
    Rig rig;
    rig.fold({charRec(rig.src, 9, "foo", 2), charRec(rig.src, 7, "Foo", 1)});
    rig.fold(charRec(rig.src, 7, "Bar", 5));
    EXPECT_EQ(rig.table.size(), 2u);
    EXPECT_EQ(rig.read("bar", "characterId").u, 7u);
    EXPECT_EQ(rig.read("bar", "gender").u, 5u);
    EXPECT_EQ(rig.read("foo", "characterId").u, 9u);
    EXPECT_EQ(rig.ownerMap(wireOf(7)), nameKey("bar"));
    EXPECT_EQ(rig.ownerMap(wireOf(9)), nameKey("foo"));
    EXPECT_EQ(rig.claims.extras(), 0u);
    EXPECT_EQ(rig.total.uniquePromotions, 1u);
}

TEST(CheckedWritesUniqueFoldRegression, PromotionChainOfThreeClaimantsRegression) {
    Rig rig;
    rig.fold({charRec(rig.src, 9, "foo", 9), charRec(rig.src, 3, "Foo", 3), charRec(rig.src, 7, "FOO", 7)});
    EXPECT_EQ(rig.ownerWire("foo"), wireOf(3));
    rig.fold(tomb(3));
    EXPECT_EQ(rig.ownerWire("foo"), wireOf(7));
    EXPECT_EQ(rig.read("foo", "gender").u, 7u);
    EXPECT_EQ(rig.claims.extras(), 1u);
    rig.fold(tomb(7));
    EXPECT_EQ(rig.ownerWire("foo"), wireOf(9));
    EXPECT_EQ(rig.read("foo", "gender").u, 9u);
    EXPECT_EQ(rig.claims.extras(), 0u);
    rig.fold(tomb(9));
    EXPECT_EQ(rig.table.size(), 0u);
    EXPECT_EQ(rig.owners.size(), 0u);
    EXPECT_EQ(rig.claims.extras(), 0u);
    EXPECT_EQ(rig.claims.indexTable().size(), 0u);
    EXPECT_EQ(rig.claims.extrasTable().size(), 0u);
    EXPECT_EQ(rig.total.uniquePromotions, 2u);
}

TEST(CheckedWritesUniqueFold, FoldReturnsThePromotionCount) {
    Rig rig;
    rig.fold({charRec(rig.src, 9, "foo", 2), charRec(rig.src, 7, "Foo", 1)});
    rune::protocol::RecordBatchBuilder b(0, 0);
    const Rec t = tomb(7);
    b.append(t.key, std::string_view{}, 0);
    const auto bytes = b.build();
    int64_t cursor = 0;
    const FoldStats st = foldProjectionBatch(rig.pf, rig.prospect, {bytes.data(), bytes.size(), 0, 1}, 1, cursor, kNoSkip);
    EXPECT_EQ(st.uniquePromotions, 1u);
    EXPECT_EQ(st.deleted, 1u);
}

TEST(CheckedWritesUniqueFold, ExtraKeepsItsKeyAndItsLatestImageIsPromoted) {
    Rig rig;
    rig.fold({charRec(rig.src, 9, "foo", 2), charRec(rig.src, 7, "Foo", 1)});
    rig.fold(charRec(rig.src, 9, "FOO", 8));
    EXPECT_EQ(rig.read("foo", "characterId").u, 7u);
    EXPECT_EQ(rig.read("foo", "gender").u, 1u);
    EXPECT_EQ(rig.claims.extras(), 1u);
    rig.fold(tomb(7));
    EXPECT_EQ(rig.read("foo", "characterId").u, 9u);
    EXPECT_EQ(rig.read("foo", "gender").u, 8u);

    Rig one, two;
    one.fold({charRec(one.src, 9, "foo", 2), charRec(one.src, 7, "Foo", 1)});
    two.fold({charRec(two.src, 9, "foo", 2), charRec(two.src, 7, "Foo", 1)});
    one.fold({charRec(one.src, 7, "Bar", 1), charRec(one.src, 9, "foo", 2 + 1)});
    two.fold(charRec(two.src, 7, "Bar", 1));
    two.fold(charRec(two.src, 9, "foo", 2 + 1));
    EXPECT_EQ(one.snapshot(kNames, allWires(12), false), two.snapshot(kNames, allWires(12), false));
}

TEST(CheckedWritesUniqueFold, ExtraLeavesQuietly) {
    Rig rig;
    rig.fold({charRec(rig.src, 9, "foo", 2), charRec(rig.src, 7, "Foo", 1)});
    const auto before = rig.snapshot(kNames, allWires(12), false);
    rig.fold(tomb(9));
    EXPECT_EQ(rig.claims.extras(), 0u);
    EXPECT_EQ(rig.total.uniquePromotions, 0u);
    EXPECT_EQ(rig.read("foo", "characterId").u, 7u);
    auto after = rig.snapshot(kNames, allWires(12), false);
    EXPECT_EQ(after.count("extras/" + wireOf(9)), 0u);
    after["index/foo"] = before.at("index/foo");   // the extra count and head changed
    after["extras/" + wireOf(9)] = before.at("extras/" + wireOf(9));
    EXPECT_EQ(after, before);
}

TEST(CheckedWritesUniqueFold, SourceKeysOrderByValue) {
    struct Pair { PrimType type; std::string a, b; };
    const std::string ab = "ab" + std::string(14, '\0'), b = "b" + std::string(15, '\0');
    const Pair pairs[] = {
        {PrimType::Int64, wireOfInt(uint64_t(int64_t(-5))), wireOfInt(3)},
        {PrimType::UInt64, wireOfInt(0x100), wireOfInt(0xFF)},
        {PrimType::FixedStr, b, ab},
    };
    const std::string winner[] = {pairs[0].a, pairs[1].b, pairs[2].b};
    const std::string winners[] = {pairs[0].a, pairs[1].b, ab};
    (void)winner;
    for (size_t i = 0; i < 3; ++i) {
        for (int flip = 0; flip < 2; ++flip) {
            Rig rig(pairs[i].type, 1, false);
            const Pair& p = pairs[i];
            const std::string& first = flip ? p.b : p.a;
            const std::string& second = flip ? p.a : p.b;
            rig.fold(charRec(rig.src, first, "foo", 1));
            rig.fold(charRec(rig.src, second, "FOO", 2));
            EXPECT_EQ(rig.ownerWire("foo"), winners[i]) << i << " " << flip;
            EXPECT_EQ(rig.claims.extras(), 1u);
        }
    }
    EXPECT_LT(compareSourceKeys(PrimType::Int64, pairs[0].a, pairs[0].b), 0);
    EXPECT_LT(compareSourceKeys(PrimType::UInt64, pairs[1].b, pairs[1].a), 0);
    EXPECT_GT(compareSourceKeys(PrimType::UInt64, pairs[1].a, pairs[1].b), 0);
}

TEST(CheckedWritesUniqueFold, PrepareFlagsOwnedByOtherAndWritesNothing) {
    Rig rig;
    rig.fold(charRec(rig.src, 7, "Foo", 1));
    const auto before = rig.snapshot(kNames, allWires(12), false);
    const Rec r = charRec(rig.src, 11, "FOO", 4);
    const SourceRecord rec{r.key, std::string_view(reinterpret_cast<const char*>(r.value.data()), r.value.size()), 0, 0, nullptr};
    ASSERT_EQ(prepareProjection(rig.pf, rec, rig.prospect), PrepareStatus::Ok);
    EXPECT_TRUE(rig.prospect.rows[0].ownedByOther);
    EXPECT_FALSE(rig.prospect.rows[0].promoted);
    EXPECT_EQ(rig.prospect.rows[0].key, nameKey("foo"));
    EXPECT_TRUE(rig.prospect.rows[0].existed);
    EXPECT_EQ(rig.snapshot(kNames, allWires(12), false), before);
}

TEST(CheckedWritesUniqueFold, PrepareOfTheOwnKeyIsNotOwnedByOther) {
    Rig rig;
    rig.fold(charRec(rig.src, 7, "Foo", 1));
    const Rec r = charRec(rig.src, 7, "FOO", 4);
    const SourceRecord rec{r.key, std::string_view(reinterpret_cast<const char*>(r.value.data()), r.value.size()), 0, 0, nullptr};
    ASSERT_EQ(prepareProjection(rig.pf, rec, rig.prospect), PrepareStatus::Ok);
    EXPECT_FALSE(rig.prospect.rows[0].ownedByOther);
    EXPECT_EQ(rig.prospect.op, ProspectOp::Upsert);
}

TEST(CheckedWritesUniqueFold, PrepareMarksAPromotionWithoutComputingIt) {
    Rig rig;
    rig.fold({charRec(rig.src, 9, "foo", 2), charRec(rig.src, 7, "Foo", 1)});
    const auto before = rig.snapshot(kNames, allWires(12), false);
    const std::string w = wireOf(7);
    const SourceRecord rec{w, {}, 0, 0, nullptr};
    ASSERT_EQ(prepareProjection(rig.pf, rec, rig.prospect), PrepareStatus::Ok);
    EXPECT_EQ(rig.prospect.op, ProspectOp::Upsert);
    EXPECT_EQ(rig.prospect.rows[0].key, nameKey("foo"));
    EXPECT_TRUE(rig.prospect.rows[0].promoted);
    EXPECT_TRUE(rig.prospect.rows[0].existed);
    EXPECT_EQ(rig.snapshot(kNames, allWires(12), false), before);
}

TEST(CheckedWritesUniqueFoldRegression, CheckpointV3CarriesTheClaimsRegression) {
    Rig a;
    std::mt19937 rng(11);
    const char* names[] = {"Foo", "FOO", "bar", "Baz", "q", "foo"};
    std::vector<Rec> log;
    for (int i = 0; i < 300; ++i) {
        const uint64_t id = 1 + rng() % 40;
        log.push_back(rng() % 5 == 0 ? tomb(id) : charRec(a.src, id, names[rng() % 6], rng() % 100));
    }
    a.fold(log);
    ASSERT_GT(a.claims.extras(), 0u);

    struct Buf { std::vector<KeyedDirtyEntry> entries; std::vector<std::vector<uint8_t>> keys, rows; };
    ViewTable* const live[] = {&a.table, &a.owners, &a.claims.indexTable(), &a.claims.extrasTable()};
    const uint8_t ids[] = {0, 1, kKeyedTableUniqueIndex, kKeyedTableUniqueExtras};
    const uint32_t rowLens[] = {a.comp.rowLayout.rowLen, a.spec.keyWidth, uint32_t(2 * a.spec.wireKeyWidth + 5), uint32_t(a.spec.keyWidth + 2 * a.spec.wireKeyWidth + 1 + a.comp.rowLayout.rowLen)};
    const uint8_t keyLens[] = {uint8_t(a.spec.keyWidth), a.spec.wireKeyWidth, uint8_t(a.spec.keyWidth), a.spec.wireKeyWidth};
    std::array<Buf, 4> bufs;
    std::vector<KeyedTableDelta> tds;
    for (size_t i = 0; i < 4; ++i) {
        live[i]->drainDirtyKeyed([&](uint32_t rid, const uint8_t* payload, bool del, std::span<const uint8_t> key) {
            bufs[i].keys.emplace_back(key.begin(), key.end());
            bufs[i].rows.emplace_back(del ? std::vector<uint8_t>{} : std::vector<uint8_t>(payload, payload + rowLens[i]));
            bufs[i].entries.push_back({rid, del, bufs[i].keys.back(), del ? nullptr : bufs[i].rows.back().data()});
        });
        tds.push_back({ids[i], keyLens[i], rowLens[i], bufs[i].entries});
    }
    std::vector<uint8_t> record;
    encodeKeyedDelta(tds, 299, record);

    Rig b;
    const RowLayout tiny = [] { RowLayout l; l.rowLen = 1; return l; }();
    std::vector<ViewTable> fillers;
    for (int i = 0; i < 4; ++i) fillers.emplace_back(tiny, 16, 1u << 12, ViewTableKeys{8, 0, 7});
    ViewTable* tables[8] = {&b.table, &b.owners, &fillers[0], &fillers[1], &fillers[2], &fillers[3], &b.claims.indexTable(), &b.claims.extrasTable()};
    const uint32_t allRowLens[8] = {rowLens[0], rowLens[1], 1, 1, 1, 1, rowLens[2], rowLens[3]};
    const uint8_t allKeyLens[8] = {keyLens[0], keyLens[1], 8, 8, 8, 8, keyLens[2], keyLens[3]};
    const std::span<const uint8_t> recs[] = {record};
    ASSERT_EQ(replayKeyed(tables, allRowLens, allKeyLens, recs), 299);
    b.claims.recountAfterReplay();
    EXPECT_EQ(b.claims.extras(), a.claims.extras());

    std::vector<std::string> universe = {"foo", "bar", "baz", "q"};
    EXPECT_EQ(b.snapshot(universe, allWires(40), false), a.snapshot(universe, allWires(40), false));
    EXPECT_EQ(b.table.size(), a.table.size());
    EXPECT_EQ(b.owners.size(), a.owners.size());
    EXPECT_EQ(b.claims.indexTable().size(), a.claims.indexTable().size());
    EXPECT_EQ(b.claims.extrasTable().size(), a.claims.extrasTable().size());
}

TEST(CheckedWritesUniqueFoldModelRegression, RandomLogsFoldToTheFreshRefold) {
    const char* names[] = {"Foo", "FOO", "bar", "Baz", "q", "foo"};
    const std::vector<std::string> universe = {"foo", "bar", "baz", "q"};
    const auto wires = allWires(40);
    const auto lowered = [](std::string s) {
        for (auto& c : s) c = char(std::tolower(static_cast<unsigned char>(c)));
        return s;
    };
    for (uint32_t seed = 1; seed <= 300; ++seed) {
        std::mt19937 rng(seed);
        Rig inc(PrimType::UInt64, 2);
        const auto v1 = rune::codec::materializeAsOf(inc.src, 1);
        std::vector<Rec> log;
        std::map<uint64_t, std::pair<std::string, uint64_t>> live;   // id -> (lowered name, gender)
        for (int i = 0, n = 40 + int(rng() % 120); i < n; ++i) {
            const uint64_t id = 1 + rng() % 40;
            const uint32_t pick = rng() % 10;
            if (pick < 2) {
                log.push_back(tomb(id));
                live.erase(id);
            } else if (pick == 2) {   // a version without the name: the key is absent
                log.push_back(charRec(v1, id, "", 1));
                live.erase(id);
            } else {
                const std::string name = names[rng() % 6];
                const uint64_t g = rng() % 100;
                log.push_back(charRec(inc.src, id, name, g));
                live[id] = {lowered(name), g};
            }
        }
        for (size_t at = 0; at < log.size();) {
            const size_t n = std::min<size_t>(1 + rng() % 20, log.size() - at);
            inc.fold(std::vector<Rec>(log.begin() + at, log.begin() + at + n), &rng);
            at += n;
        }
        Rig fresh(PrimType::UInt64, 2);
        fresh.fold(log);
        EXPECT_EQ(inc.snapshot(universe, wires), fresh.snapshot(universe, wires)) << seed;

        std::map<std::string, std::vector<uint64_t>> claimants;
        for (const auto& [id, nm] : live) claimants[nm.first].push_back(id);   // ascending ids = ascending source keys
        size_t extras = 0;
        for (const auto& nm : universe) {
            const auto it = claimants.find(nm);
            for (auto* rig : {&inc, &fresh}) {
                ASSERT_EQ(rig->table.get(nameKey(nm)) != nullptr, it != claimants.end()) << seed << " " << nm;
                if (it == claimants.end()) continue;
                const uint64_t owner = it->second.front();
                EXPECT_EQ(rig->ownerWire(nm), wireOf(owner)) << seed << " " << nm;
                EXPECT_EQ(rig->read(nm, "characterId").u, owner);
                EXPECT_EQ(rig->read(nm, "gender").u, live.at(owner).second);
                EXPECT_EQ(rig->claims.hasExtras(nameKey(nm)), it->second.size() > 1);
            }
            if (it != claimants.end()) extras += it->second.size() - 1;
        }
        EXPECT_EQ(inc.claims.extras(), extras) << seed;
        EXPECT_EQ(fresh.claims.extras(), extras) << seed;
        EXPECT_EQ(inc.owners.size(), inc.table.size()) << seed;
    }
}
