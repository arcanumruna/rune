// libs/view/tests/view/ViewCheckpointTest.cpp
#include <gtest/gtest.h>
#include <rune/view/ViewCheckpoint.h>
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
