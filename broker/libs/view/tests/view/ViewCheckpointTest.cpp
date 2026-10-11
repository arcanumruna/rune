// libs/view/tests/view/ViewCheckpointTest.cpp
#include <gtest/gtest.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/view/AggState.h>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>
#include <array>
#include <cstring>
#include <set>
#include <vector>

using namespace rune::view;
using rune::codec::DescriptorBuilder; using rune::codec::PrimType; using rune::codec::FieldValue;

static RowLayout hpLayout() {
    return buildRowLayout(DescriptorBuilder("v",1)
        .add("id",PrimType::Int64,1,true).add("hp",PrimType::Int32).build());
}
static void setHp(uint8_t* row, const RowLayout& L, int32_t hp) {
    FieldValue v{}; v.type=PrimType::Int32; v.present=true; v.i=hp;
    writeSlot(row, L, 1, v);
}

// Drains `t`'s dirty set into a vector<DirtyEntry> whose `row` pointers are
// stable across the whole call (drainDirty()'s own `row` pointer is only
// valid during its callback, so each upsert's bytes are copied out into
// `rowStorage` first). `rowStorage` is a vector<vector<uint8_t>>: growing the
// OUTER vector may move its elements, but std::vector's move constructor only
// transfers the inner vector's existing heap pointer -- it never reallocates
// or copies the row bytes themselves -- so every DirtyEntry::row captured so
// far stays valid for the lifetime of this function's caller.
static std::vector<DirtyEntry> drainToEntries(ViewTable& t, const RowLayout& L,
                                               std::vector<std::vector<uint8_t>>& rowStorage) {
    std::vector<DirtyEntry> entries;
    t.drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
        const uint8_t* stableRow = nullptr;
        if (!deleted) {
            rowStorage.emplace_back(row, row + L.rowLen);
            stableRow = rowStorage.back().data();
        }
        entries.push_back(DirtyEntry{rid, deleted, key, stableRow});
    });
    return entries;
}

// ---- Step 1/2 (brief): gate regression -- offset-atomic + byte-identical ----

TEST(ViewCheckpointReplayRegression, CheckpointReplayIsOffsetAtomicAndByteIdentical) {
    RowLayout L = hpLayout();
    ViewTable live(L);
    std::set<uint64_t> liveKeys;

    // Round 1: populate keys 1..50. This is the table's FIRST drain, so it
    // naturally emits exactly "every live row" -- i.e. a valid BASE -- at
    // snapshot O=100.
    for (uint64_t k = 1; k <= 50; ++k) { setHp(live.put(k), L, static_cast<int32_t>(k)); liveKeys.insert(k); }
    std::vector<uint8_t> base;
    {
        std::vector<std::vector<uint8_t>> rowStorage;
        auto entries = drainToEntries(live, L, rowStorage);
        ASSERT_EQ(entries.size(), 50u);
        size_t written = encodeDelta(entries, /*O=*/100, L.rowLen, base);
        EXPECT_GT(written, 0u);
        EXPECT_EQ(written, base.size());
    }

    // Round 2: overwrite some existing keys, insert new ones, erase a few ->
    // delta at O=150. Erased keys {3,10,42} are deliberately excluded from
    // every later "overwrite existing key" range below so they stay erased
    // (round 3 does not accidentally resurrect one of them).
    for (uint64_t k = 1; k <= 20; ++k) setHp(live.put(k), L, static_cast<int32_t>(k * 1000));
    for (uint64_t k = 51; k <= 65; ++k) { setHp(live.put(k), L, static_cast<int32_t>(k)); liveKeys.insert(k); }
    for (uint64_t k : {3u, 10u, 42u}) { ASSERT_TRUE(live.erase(k)); liveKeys.erase(k); }
    std::vector<uint8_t> d150;
    {
        std::vector<std::vector<uint8_t>> rowStorage;
        auto entries = drainToEntries(live, L, rowStorage);
        encodeDelta(entries, /*O=*/150, L.rowLen, d150);
    }

    // Round 3: more churn, including re-overwriting keys already covered by
    // the base (exercises idempotent "Replace" upserts) -> delta at O=200.
    for (uint64_t k = 66; k <= 80; ++k) { setHp(live.put(k), L, static_cast<int32_t>(k * 7)); liveKeys.insert(k); }
    for (uint64_t k = 11; k <= 15; ++k) setHp(live.put(k), L, static_cast<int32_t>(k + 99999));
    ASSERT_TRUE(live.erase(60)); liveKeys.erase(60);
    std::vector<uint8_t> d200;
    {
        std::vector<std::vector<uint8_t>> rowStorage;
        auto entries = drainToEntries(live, L, rowStorage);
        encodeDelta(entries, /*O=*/200, L.rowLen, d200);
    }

    // Simulate crash recovery after d200 fsync'd: replay base + d150 + d200
    // into a FRESH table.
    ViewTable restored(L);
    std::array<std::span<const uint8_t>, 2> deltas{
        std::span<const uint8_t>(d150), std::span<const uint8_t>(d200)};
    int64_t recovered = replay(restored, L, base, deltas);
    EXPECT_EQ(recovered, 200) << "recovered offset must be EXACTLY the last delta's O, never +/-1";

    ASSERT_EQ(restored.size(), liveKeys.size());
    for (uint64_t k : liveKeys) {
        const uint8_t* lr = live.get(k);
        const uint8_t* rr = restored.get(k);
        ASSERT_NE(lr, nullptr) << "key " << k << " missing from `live` (test setup bug)";
        ASSERT_NE(rr, nullptr) << "key " << k << " missing after replay";
        EXPECT_EQ(std::memcmp(lr, rr, L.rowLen), 0) << "byte mismatch for key " << k;
    }
    for (uint64_t k : {3u, 10u, 42u, 60u}) {
        EXPECT_EQ(restored.get(k), nullptr) << "erased key " << k << " resurrected by replay";
    }
}

TEST(ViewCheckpoint, DeltaCrcRejectsCorruption) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(1), L, 42);

    std::vector<uint8_t> d;
    {
        std::vector<std::vector<uint8_t>> rowStorage;
        auto entries = drainToEntries(t, L, rowStorage);
        ASSERT_EQ(entries.size(), 1u);
        encodeDelta(entries, /*O=*/5, L.rowLen, d);
    }
    d[d.size() - 1] ^= 0xFF; // corrupt one crc byte

    ViewTable t2(L);
    EXPECT_FALSE(applyDelta(t2, L, d).has_value());
    EXPECT_EQ(t2.get(1), nullptr) << "a rejected delta must not have mutated the table";
}

// Task 8 lesson, reapplied: a truncated buffer or a bogus/huge wire `count`
// must be rejected -- never an OOB read, never an allocation sized from the
// unvalidated count.
TEST(ViewCheckpoint, TruncatedOrBogusCountRejectedSafely) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(1), L, 1);

    std::vector<uint8_t> d;
    {
        std::vector<std::vector<uint8_t>> rowStorage;
        auto entries = drainToEntries(t, L, rowStorage);
        encodeDelta(entries, /*O=*/5, L.rowLen, d);
    }
    ViewTable t2(L);

    // Truncated mid-record (drops the trailing crc and part of the row bytes).
    std::vector<uint8_t> truncated(d.begin(), d.begin() + d.size() / 2);
    EXPECT_FALSE(applyDelta(t2, L, truncated).has_value());

    // Bogus huge count on an otherwise unchanged (short) buffer: must reject
    // on its very first bounds check, not attempt a count-sized allocation
    // or scan/read past the buffer.
    std::vector<uint8_t> bogusCount = d;
    bogusCount[16] = 0xFF; bogusCount[17] = 0xFF; bogusCount[18] = 0xFF; bogusCount[19] = 0xFF;
    EXPECT_FALSE(applyDelta(t2, L, bogusCount).has_value());

    // Empty buffer entirely.
    EXPECT_FALSE(applyDelta(t2, L, std::span<const uint8_t>{}).has_value());

    // Wrong magic.
    std::vector<uint8_t> badMagic = d;
    badMagic[0] ^= 0xFF;
    EXPECT_FALSE(applyDelta(t2, L, badMagic).has_value());

    EXPECT_EQ(t2.get(1), nullptr) << "no rejected buffer may have mutated the table";
}

// ---- Phase 2a: multi-table (v2) record ----
namespace {
rune::view::RowLayout rawLayout(uint32_t rowLen) { rune::view::RowLayout l; l.rowLen = rowLen; return l; }

std::vector<uint8_t> threeTableRecord(int64_t O, uint64_t viewKey, uint8_t viewByte,
                                      uint64_t contribKey, uint8_t contribByte) {
    uint8_t vrow[16]; std::memset(vrow, viewByte, 16);
    uint8_t crow[24]; std::memset(crow, contribByte, 24);
    const rune::view::DirtyEntry ve{0, false, viewKey, vrow};
    const rune::view::DirtyEntry ce{0, false, contribKey, crow};
    const rune::view::DirtyEntry gdel{0, true, 9, nullptr};
    const rune::view::TableDelta tds[] = {
        {rune::view::kAggTableView, 16, {&ve, 1}},
        {rune::view::kAggTableContributors, 24, {&ce, 1}},
        {rune::view::kAggTableGroups, 32, {&gdel, 1}}};
    std::vector<uint8_t> out;
    rune::view::encodeMultiDelta(tds, O, out);
    return out;
}
} // namespace

TEST(ViewCheckpointMulti, ThreeTableRecordRoundTrips) {
    rune::view::ViewTable v(rawLayout(16)), c(rawLayout(24)), g(rawLayout(32));
    std::memset(g.put(9), 0x5A, 32);   // pre-existing group row the record deletes
    rune::view::ViewTable* tables[] = {&v, &c, &g};
    const uint32_t lens[] = {16, 24, 32};
    const auto rec = threeTableRecord(42, 7, 0x11, 8, 0x22);
    const auto applied = rune::view::applyMultiDelta(tables, lens, rec);
    ASSERT_TRUE(applied.has_value());
    EXPECT_EQ(applied->O, 42);
    EXPECT_EQ(applied->count, 3u);
    ASSERT_NE(v.get(7), nullptr); EXPECT_EQ(v.get(7)[0], 0x11);
    ASSERT_NE(c.get(8), nullptr); EXPECT_EQ(c.get(8)[23], 0x22);
    EXPECT_EQ(g.get(9), nullptr);
}

TEST(ViewCheckpointMultiRegression, CorruptRecordMutatesNoTable) {
    rune::view::ViewTable v(rawLayout(16)), c(rawLayout(24)), g(rawLayout(32));
    rune::view::ViewTable* tables[] = {&v, &c, &g};
    const uint32_t lens[] = {16, 24, 32};
    auto rec = threeTableRecord(42, 7, 0x11, 8, 0x22);
    rec[rec.size() - 1] ^= 0xFF;   // break the CRC
    EXPECT_FALSE(rune::view::applyMultiDelta(tables, lens, rec).has_value());
    EXPECT_EQ(v.size(), 0u); EXPECT_EQ(c.size(), 0u);
}

TEST(ViewCheckpointMultiRegression, RowLenMismatchUnknownOrDuplicateTableIsRejected) {
    rune::view::ViewTable v(rawLayout(16)), c(rawLayout(24)), g(rawLayout(32));
    rune::view::ViewTable* tables[] = {&v, &c, &g};
    const auto rec = threeTableRecord(1, 1, 1, 1, 1);
    const uint32_t wrongLens[] = {16, 25, 32};
    EXPECT_FALSE(rune::view::applyMultiDelta(tables, wrongLens, rec).has_value());
    rune::view::ViewTable* two[] = {&v, &c};          // table id 2 unknown here
    const uint32_t twoLens[] = {16, 24};
    EXPECT_FALSE(rune::view::applyMultiDelta(two, twoLens, rec).has_value());

    uint8_t row[16] = {};
    const rune::view::DirtyEntry e{0, false, 1, row};
    const rune::view::TableDelta dup[] = {{0, 16, {&e, 1}}, {0, 16, {&e, 1}}};
    std::vector<uint8_t> out;
    rune::view::encodeMultiDelta(dup, 5, out);
    const uint32_t lens[] = {16, 24, 32};
    EXPECT_FALSE(rune::view::applyMultiDelta(tables, lens, out).has_value());
}

TEST(ViewCheckpointMultiRegression, ReplayStopsAtTornTailAllTablesAtSameOffset) {
    rune::view::ViewTable v(rawLayout(16)), c(rawLayout(24)), g(rawLayout(32));
    rune::view::ViewTable* tables[] = {&v, &c, &g};
    const uint32_t lens[] = {16, 24, 32};
    const auto r1 = threeTableRecord(10, 7, 0x11, 8, 0x22);
    auto r2 = threeTableRecord(20, 7, 0x33, 8, 0x44);
    r2.resize(r2.size() - 3);   // torn tail
    const std::span<const uint8_t> recs[] = {r1, r2};
    EXPECT_EQ(rune::view::replayMulti(tables, lens, recs), 10);
    EXPECT_EQ(v.get(7)[0], 0x11) << "view table at O=10";
    EXPECT_EQ(c.get(8)[0], 0x22) << "contributor table at the SAME O=10";
}

TEST(ViewCheckpointMulti, OffsetIsAtByteFourLikeV1) {
    const auto rec = threeTableRecord(0x0102030405060708LL, 1, 1, 1, 1);
    int64_t O; std::memcpy(&O, rec.data() + 4, 8);
    EXPECT_EQ(O, 0x0102030405060708LL);
    uint32_t magic; std::memcpy(&magic, rec.data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2);
}
