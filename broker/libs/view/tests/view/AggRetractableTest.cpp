// broker/libs/view/tests/view/AggRetractableTest.cpp — Phase 2b retractable
// Min / Max / Last / LastNonNull (per-group MRU contributor list + caches).
#include <gtest/gtest.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/AggFold.h>
#include <rune/view/AggState.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewRow.h>

using P = rune::codec::PrimType;
using R = rune::view::MergeRule;

namespace {

// order v2: orderId Int64 key | userId Int64 | amount Int64 | price Float64 |
//           sku FixedStr(4) | qty UInt32 | bonus Int32 (sinceVersion 2).
// A v1 record has no bonus: it reads as ABSENT (NULL).
rune::codec::Descriptor orderSource() {
    return rune::codec::DescriptorBuilder("order", 2)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64, 1)
        .add("amount", P::Int64, 1)
        .add("price", P::Float64, 1)
        .addFixedStr("sku", 4, 1)
        .add("qty", P::UInt32, 1)
        .add("bonus", P::Int32, 2)
        .build();
}

// Output slots (key first, then declaration order).
enum Slot : uint16_t { kUser = 0, kN, kMaxAmt, kMinAmt, kLastAmt, kMaxPrice, kMinQty,
                       kLastSku, kLastBonus, kLastNnBonus, kTotal };

rune::view::ViewPlan userStats() {
    rune::view::ViewPlan v;
    v.viewName = "user_stats"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"maxAmt", P::Int64, 0, "amount", R::Max},
                {"minAmt", P::Int64, 0, "amount", R::Min},
                {"lastAmt", P::Int64, 0, "amount", R::Last},
                {"maxPrice", P::Float64, 0, "price", R::Max},
                {"minQty", P::UInt32, 0, "qty", R::Min},
                {"lastSku", P::FixedStr, 0, "sku", R::Last},
                {"lastBonus", P::Int32, 0, "bonus", R::Last},
                {"lastNnBonus", P::Int32, 0, "bonus", R::LastNonNull},
                {"total", P::Int64, 0, "amount", R::Sum}};
    return v;
}

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le32(std::vector<uint8_t>& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }

struct Ord {
    int64_t id;
    int64_t user;
    int64_t amount;
    double price = 1.0;
    const char* sku = "AAAA";
    uint32_t qty = 1;
    std::optional<int32_t> bonus;   // nullopt -> a v1 record (bonus NULL)
};

std::vector<uint8_t> encode(const Ord& o) {
    std::vector<uint8_t> b;
    const bool v2 = o.bonus.has_value();
    le16(b, v2 ? 44 : 40); le16(b, v2 ? 2 : 1);
    le64(b, uint64_t(o.id)); le64(b, uint64_t(o.user)); le64(b, uint64_t(o.amount));
    uint64_t pr; std::memcpy(&pr, &o.price, 8); le64(b, pr);
    char sku[4] = {0, 0, 0, 0};
    std::memcpy(sku, o.sku, std::min<size_t>(4, std::strlen(o.sku)));
    b.insert(b.end(), sku, sku + 4);
    le32(b, o.qty);
    if (v2) le32(b, uint32_t(*o.bonus));
    return b;
}
std::vector<uint8_t> tombstone() { return {}; }

struct Harness {
    rune::codec::Descriptor src = orderSource();
    rune::view::CompileResult comp = compile(src);
    rune::view::AggLayout agg = rune::view::buildAggLayout(comp.plan, src).value();
    rune::view::VersionDescriptorCache vdc{src};
    rune::view::ViewTable view{comp.rowLayout};
    rune::view::ViewTable contributors{agg.contributorRow};
    rune::view::ViewTable groups{agg.groupRow};
    rune::view::ViewTable blocks{agg.blockRow};   // userStats has Min/Max: 3 block columns
    int64_t next = 0;
    int64_t cursor = 0;
    rune::view::FoldStats last{};
    uint64_t inconsistent = 0;   // summed over every apply()

    static rune::view::CompileResult compile(const rune::codec::Descriptor& s) {
        const rune::codec::Descriptor* srcs[] = {&s};
        return rune::view::compileView(userStats(), srcs, 9).value();
    }
    void apply(const std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>& recs) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        for (const auto& [k, v] : recs) {
            b.append(std::string_view(reinterpret_cast<const char*>(k.data()), k.size()),
                     std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0);
        }
        const auto batch = b.build();
        const int64_t bound = next + static_cast<int64_t>(recs.size());
        const rune::storage::LogReadView lv{batch.data(), batch.size(), next, bound};
        last = rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view,
                                              contributors, groups, &blocks, lv, bound, cursor);
        inconsistent += last.aggInconsistent;
        next = bound;
    }
    static std::vector<uint8_t> key(int64_t orderId) {
        std::vector<uint8_t> k; le64(k, uint64_t(orderId)); return k;
    }
    void put(int64_t orderId, std::vector<uint8_t> value) { apply({{key(orderId), std::move(value)}}); }
    void putOrd(const Ord& o) { put(o.id, encode(o)); }

    bool hasGroup(int64_t userId) const { return view.get(uint64_t(userId)) != nullptr; }
    rune::codec::FieldValue col(int64_t userId, uint16_t slot) const {
        const uint8_t* r = view.get(uint64_t(userId));
        EXPECT_NE(r, nullptr) << "group " << userId << " must exist";
        return r ? rune::view::readSlot(r, comp.rowLayout, slot) : rune::codec::FieldValue{};
    }

    // Drains every table's dirty set; counts entries per table and group rows
    // left with a RescanPending cache (must never survive a record).
    struct Drained {
        uint32_t contributorRows = 0, groupRows = 0, viewRows = 0, blockRows = 0, pendingCaches = 0;
    };
    Drained drainCounts() {
        Drained d;
        contributors.drainDirty([&](uint32_t, const uint8_t*, bool, uint64_t) { ++d.contributorRows; });
        groups.drainDirty([&](uint32_t, const uint8_t* row, bool deleted, uint64_t) {
            ++d.groupRows;
            if (deleted) return;
            for (uint16_t ci = 0; ci < agg.numCached; ++ci) {
                if (row[agg.groupCacheOff + rune::view::kCacheEntryBytes * ci + 8] ==
                    static_cast<uint8_t>(rune::view::CacheState::RescanPending)) {
                    ++d.pendingCaches;
                }
            }
        });
        view.drainDirty([&](uint32_t, const uint8_t*, bool, uint64_t) { ++d.viewRows; });
        blocks.drainDirty([&](uint32_t, const uint8_t*, bool, uint64_t) { ++d.blockRows; });
        return d;
    }

    using Rec = std::pair<std::vector<uint8_t>, std::vector<uint8_t>>;

    // Folds `recs` as ONE batch in two passes split after `firstPass` records
    // (1 <= firstPass < recs.size()): pass 1 is capped by `bound`, pass 2 resumes
    // mid-batch from the cursor. Returns both passes' dirty entries summed. Each
    // pass is also checked: at most (records in the pass) x the per-record bound
    // per table (dirty entries coalesce inside a pass), and no RescanPending cache.
    Drained applySplit(const std::vector<Rec>& recs, size_t firstPass) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        for (const auto& [k, v] : recs) {
            b.append(std::string_view(reinterpret_cast<const char*>(k.data()), k.size()),
                     std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0);
        }
        const auto batch = b.build();
        const int64_t full = next + static_cast<int64_t>(recs.size());
        const int64_t cap  = next + static_cast<int64_t>(firstPass);
        const rune::storage::LogReadView lv{batch.data(), batch.size(), next, full};
        const auto perRec = rune::view::aggDirtyBoundPerRecord(agg);
        Drained total;
        size_t done = 0;
        for (const int64_t bnd : {cap, full}) {
            last = rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view,
                                                  contributors, groups, &blocks, lv, bnd, cursor);
            inconsistent += last.aggInconsistent;
            const size_t inPass = static_cast<size_t>(bnd - next) - done;
            done += inPass;
            const Drained d = drainCounts();
            EXPECT_LE(d.contributorRows, inPass * perRec.contributorRows);
            EXPECT_LE(d.groupRows, inPass * perRec.groupRows);
            EXPECT_LE(d.viewRows, inPass * perRec.viewRows);
            EXPECT_LE(d.blockRows, inPass * perRec.blockRows);
            EXPECT_EQ(d.pendingCaches, 0u);
            total.contributorRows += d.contributorRows;
            total.groupRows += d.groupRows;
            total.viewRows += d.viewRows;
            total.blockRows += d.blockRows;
            total.pendingCaches += d.pendingCaches;
        }
        next = full;
        return total;
    }

    // Value-block accessors (Task 5b).
    uint32_t posOf(int64_t orderId) const {
        const uint8_t* c = contributors.get(uint64_t(orderId));
        EXPECT_NE(c, nullptr) << "contributor " << orderId;
        return c != nullptr ? rune::view::loadU32(c + agg.contribBlockPosOff) : UINT32_MAX;
    }
    uint32_t blockIdOf(int64_t userId) const {
        const uint8_t* g = groups.get(uint64_t(userId));
        return g != nullptr ? rune::view::loadU32(g + agg.groupBlockOff) : rune::view::kNoBlock;
    }
    uint32_t chunksOf(int64_t userId) const {
        const uint8_t* g = groups.get(uint64_t(userId));
        return g != nullptr ? rune::view::loadU32(g + agg.groupBlockOff + 4) : 0u;
    }

    // The value-block invariant (AggState.h), checked for `groupIds`:
    //  - a group with n entries has a block id and blockChunks == ceil(n/32), or one
    //    more (a spare) only while the tail chunk holds more than 16 entries;
    //  - entry p (chunk p/32, slot p%32) names a contributor of that group whose
    //    stored blockPos is p and whose Min/Max inputs equal the entry's values/nulls;
    //  - no chunk row exists past blockChunks, and the block table holds exactly the
    //    listed groups' chunks (nothing leaked).
    // Returns the number of entries seen (== live contributors when every group is listed).
    size_t expectBlocksConsistent(const std::vector<int64_t>& groupIds) {
        namespace v = rune::view;
        size_t entries = 0, chunkRows = 0;
        for (const int64_t g : groupIds) {
            const uint8_t* gr = groups.get(uint64_t(g));
            if (gr == nullptr) continue;
            const uint64_t n = v::loadU64(gr);
            const uint32_t id = v::loadU32(gr + agg.groupBlockOff);
            const uint32_t chunks = v::loadU32(gr + agg.groupBlockOff + 4);
            EXPECT_NE(id, v::kNoBlock) << "group " << g;
            const uint32_t used = static_cast<uint32_t>((n + v::kBlockEntries - 1) / v::kBlockEntries);
            const uint32_t tailFill = static_cast<uint32_t>(n - uint64_t(used - 1) * v::kBlockEntries);
            EXPECT_TRUE(chunks == used || (chunks == used + 1 && tailFill > v::kBlockSpareFreeAt))
                << "group " << g << " n=" << n << " chunks=" << chunks;
            for (uint64_t p = 0; p < n; ++p) {
                const uint8_t* row = blocks.get(v::blockChunkKey(id, uint32_t(p / v::kBlockEntries)));
                EXPECT_NE(row, nullptr) << "group " << g << " entry " << p;
                if (row == nullptr) continue;
                const uint32_t s = uint32_t(p % v::kBlockEntries);
                const uint64_t c = v::loadU64(row + v::blockKeyOff(s));
                const uint8_t* cr = contributors.get(c);
                EXPECT_NE(cr, nullptr) << "group " << g << " entry " << p << " names missing " << c;
                if (cr == nullptr) continue;
                EXPECT_EQ(v::loadU64(cr), uint64_t(g)) << "contributor " << c;
                EXPECT_EQ(v::loadU32(cr + agg.contribBlockPosOff), uint32_t(p)) << "contributor " << c;
                for (uint16_t col = 0; col < agg.numBlockCols; ++col) {
                    const uint16_t in = agg.blockColInput[col];
                    const bool isNull = ((cr[agg.contribNullOff + in / 8] >> (in % 8)) & 1u) != 0;
                    EXPECT_EQ(row[v::blockNullOff(agg, col, s)], isNull ? 1 : 0) << "contributor " << c;
                    EXPECT_EQ(std::memcmp(row + v::blockValOff(col, s), cr + agg.inputOff[in], 8), 0)
                        << "contributor " << c << " column " << col;
                }
                ++entries;
            }
            for (uint32_t k = chunks; k < chunks + 2; ++k)
                EXPECT_EQ(blocks.get(v::blockChunkKey(id, k)), nullptr) << "group " << g << " leaked chunk " << k;
            chunkRows += chunks;
        }
        EXPECT_EQ(blocks.size(), chunkRows) << "every chunk row belongs to a listed group";
        return entries;
    }
};

// The v2 record a worker would checkpoint from the four tables (rows copied:
// drainDirty's pointer is valid only inside its callback). Drains every table.
std::vector<uint8_t> checkpointAll(Harness& h) {
    rune::view::ViewTable* ts[4] = {&h.view, &h.contributors, &h.groups, &h.blocks};
    const uint32_t lens[4] = {h.comp.rowLayout.rowLen, h.agg.contributorRow.rowLen,
                              h.agg.groupRow.rowLen, h.agg.blockRow.rowLen};
    std::vector<std::vector<uint8_t>> bytes[4];
    std::vector<rune::view::DirtyEntry> entries[4];
    for (int t = 0; t < 4; ++t) {
        ts[t]->drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
            bytes[t].push_back(deleted ? std::vector<uint8_t>{}
                                       : std::vector<uint8_t>(row, row + lens[t]));
            entries[t].push_back({rid, deleted, key, nullptr});
        });
        for (size_t i = 0; i < entries[t].size(); ++i)
            if (!entries[t][i].deleted) entries[t][i].row = bytes[t][i].data();
    }
    const rune::view::TableDelta tds[4] = {
        {rune::view::kAggTableView, lens[0], entries[0]},
        {rune::view::kAggTableContributors, lens[1], entries[1]},
        {rune::view::kAggTableGroups, lens[2], entries[2]},
        {rune::view::kAggTableBlocks, lens[3], entries[3]}};
    std::vector<uint8_t> out;
    rune::view::encodeMultiDelta(tds, h.next, out);
    return out;
}

// Recovery into fresh tables, then the worker's post-recovery dirty discard.
void restoreInto(Harness& h, const std::vector<uint8_t>& rec, int64_t next, int64_t cursor) {
    rune::view::ViewTable* ts[4] = {&h.view, &h.contributors, &h.groups, &h.blocks};
    const uint32_t lens[4] = {h.comp.rowLayout.rowLen, h.agg.contributorRow.rowLen,
                              h.agg.groupRow.rowLen, h.agg.blockRow.rowLen};
    ASSERT_TRUE(rune::view::applyMultiDelta(ts, lens, rec).has_value());
    h.next = next;
    h.cursor = cursor;
    (void)h.drainCounts();
}

// Same groups, contributors and chunk rows, byte for byte.
void expectSameState(Harness& a, Harness& b, const std::vector<int64_t>& groupIds, int64_t maxId) {
    for (const int64_t g : groupIds) {
        const uint8_t* ga = a.groups.get(uint64_t(g));
        const uint8_t* gb = b.groups.get(uint64_t(g));
        ASSERT_EQ(ga == nullptr, gb == nullptr) << "group " << g;
        if (ga == nullptr) continue;
        EXPECT_EQ(std::memcmp(ga, gb, a.agg.groupRow.rowLen), 0) << "group row " << g;
        const uint8_t* va = a.view.get(uint64_t(g));
        const uint8_t* vb = b.view.get(uint64_t(g));
        ASSERT_TRUE(va != nullptr && vb != nullptr) << "view row " << g;
        EXPECT_EQ(std::memcmp(va, vb, a.comp.rowLayout.rowLen), 0) << "view row " << g;
        const uint32_t id = rune::view::loadU32(ga + a.agg.groupBlockOff);
        const uint32_t chunks = rune::view::loadU32(ga + a.agg.groupBlockOff + 4);
        for (uint32_t k = 0; k < chunks; ++k) {
            const uint8_t* ba = a.blocks.get(rune::view::blockChunkKey(id, k));
            const uint8_t* bb = b.blocks.get(rune::view::blockChunkKey(id, k));
            ASSERT_TRUE(ba != nullptr && bb != nullptr) << "group " << g << " chunk " << k;
            EXPECT_EQ(std::memcmp(ba, bb, a.agg.blockRow.rowLen), 0) << "group " << g << " chunk " << k;
        }
    }
    for (int64_t c = 1; c <= maxId; ++c) {
        const uint8_t* ca = a.contributors.get(uint64_t(c));
        const uint8_t* cb = b.contributors.get(uint64_t(c));
        ASSERT_EQ(ca == nullptr, cb == nullptr) << "contributor " << c;
        if (ca != nullptr) EXPECT_EQ(std::memcmp(ca, cb, a.agg.contributorRow.rowLen), 0) << "contributor " << c;
    }
    EXPECT_EQ(a.blocks.size(), b.blocks.size());
}

} // namespace

TEST(AggRetractable, MinMaxLastFoldOnInsert) {
    Harness h;
    h.putOrd({1, 100, 10, 2.5, "AB", 7, 3});
    h.putOrd({2, 100, 30, 1.5, "CD", 5});          // v1: bonus NULL
    h.putOrd({3, 200, 5});
    EXPECT_EQ(h.col(100, kN).i, 2);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 10);
    EXPECT_EQ(h.col(100, kLastAmt).i, 30);
    EXPECT_DOUBLE_EQ(h.col(100, kMaxPrice).f, 2.5);
    EXPECT_EQ(h.col(100, kMinQty).u, 5u);
    EXPECT_EQ(std::string(h.col(100, kLastSku).bytes), std::string("CD\0\0", 4));
    EXPECT_FALSE(h.col(100, kLastBonus).present) << "Last includes null";
    EXPECT_EQ(h.col(100, kLastNnBonus).i, 3) << "LastNonNull skips the newer null";
    EXPECT_EQ(h.col(100, kTotal).i, 40);
    EXPECT_EQ(h.col(200, kMaxAmt).i, 5);
    EXPECT_EQ(h.col(200, kMinAmt).i, 5);
}

TEST(AggRetractableRegression, RetractingTheMaxSurfacesTheNextRegression) {
    Harness h;
    h.putOrd({1, 100, 10});
    h.putOrd({2, 100, 50});
    h.putOrd({3, 100, 30});
    EXPECT_EQ(h.col(100, kMaxAmt).i, 50);
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 10);
    EXPECT_GE(h.last.aggRescans, 1u) << "retracting the cached extreme walks the list";
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kMinAmt).i, 30) << "retracting the cached min surfaces the next";
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, RetractingOneOfDuplicateExtremesKeepsTheExtremeRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 50});
    h.putOrd({3, 100, 10});
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kMaxAmt).i, 50) << "the duplicate (contributor 2) still holds 50";
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kMaxAmt).i, 10);
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, InPlaceRaiseOfTheMaxSkipsTheRescanRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 30});
    h.putOrd({1, 100, 70});                         // the max holder raises its own max
    EXPECT_EQ(h.col(100, kMaxAmt).i, 70);
    EXPECT_EQ(h.last.aggRescans, 0u) << "a new value at least as extreme replaces the retracted one";
    h.putOrd({1, 100, 70});                         // equal re-write: still no rescan
    EXPECT_EQ(h.col(100, kMaxAmt).i, 70);
    EXPECT_EQ(h.last.aggRescans, 0u);
    EXPECT_EQ(h.col(100, kMinAmt).i, 30);
}

TEST(AggRetractableRegression, InPlaceLowerOfTheMaxRescansRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 30});
    h.putOrd({1, 100, 20});                         // the max holder drops below the next
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 20);
    EXPECT_EQ(h.last.aggRescans, 1u) << "exactly one walk, for maxAmt (minAmt: 20 beats 30, O(1))";
    EXPECT_EQ(h.col(100, kN).i, 2);
}

TEST(AggRetractableRegression, GroupMoveOfTheExtremeRescansOldGroupAndRaisesNewRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 30});
    h.putOrd({3, 200, 40});
    h.putOrd({1, 200, 50});                         // move the u100 max holder to u200
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30);
    EXPECT_EQ(h.col(100, kMinAmt).i, 30);
    EXPECT_EQ(h.col(100, kLastAmt).i, 30);
    EXPECT_EQ(h.col(200, kN).i, 2);
    EXPECT_EQ(h.col(200, kMaxAmt).i, 50);
    EXPECT_EQ(h.col(200, kMinAmt).i, 40);
    EXPECT_EQ(h.col(200, kLastAmt).i, 50);
    h.putOrd({2, 200, 1});                          // the last u100 contributor moves: u100 empties
    EXPECT_FALSE(h.hasGroup(100));
    EXPECT_EQ(h.col(200, kMinAmt).i, 1);
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, LastAfterDeletingTheNewestIsThePreviousWriteRegression) {
    Harness h;
    h.putOrd({1, 100, 10});
    h.putOrd({2, 100, 20});
    h.putOrd({3, 100, 30});
    EXPECT_EQ(h.col(100, kLastAmt).i, 30);
    h.put(3, tombstone());
    EXPECT_EQ(h.col(100, kLastAmt).i, 20);
    h.putOrd({1, 100, 15});                         // a re-write moves 1 to the head
    EXPECT_EQ(h.col(100, kLastAmt).i, 15);
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kLastAmt).i, 20);
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, LastIncludesNullAndLastNonNullSkipsNullsRegression) {
    Harness h;
    h.putOrd({1, 100, 10, 1.0, "AAAA", 1, 7});      // bonus 7
    h.putOrd({2, 100, 20});                          // bonus NULL (v1)
    EXPECT_FALSE(h.col(100, kLastBonus).present);
    EXPECT_EQ(h.col(100, kLastNnBonus).i, 7);
    h.putOrd({3, 100, 30});                          // another NULL on top
    EXPECT_EQ(h.col(100, kLastNnBonus).i, 7);
    h.putOrd({2, 100, 20, 1.0, "AAAA", 1, 9});      // 2 now non-null and newest
    EXPECT_EQ(h.col(100, kLastBonus).i, 9);
    EXPECT_EQ(h.col(100, kLastNnBonus).i, 9);
    h.put(2, tombstone());
    h.put(1, tombstone());
    EXPECT_FALSE(h.col(100, kLastBonus).present);
    EXPECT_FALSE(h.col(100, kLastNnBonus).present) << "no non-null contributor left -> NULL";
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, NonFiniteMaxInputIsNullAndCountedRegression) {
    Harness h;
    h.putOrd({1, 100, 10, 2.0});
    h.putOrd({2, 100, 10, std::numeric_limits<double>::quiet_NaN()});
    EXPECT_EQ(h.last.aggNonFinite, 1u);
    h.putOrd({3, 100, 10, std::numeric_limits<double>::infinity()});
    EXPECT_EQ(h.last.aggNonFinite, 1u);
    EXPECT_DOUBLE_EQ(h.col(100, kMaxPrice).f, 2.0) << "+Inf is NULL, not the max";
    h.put(1, tombstone());
    EXPECT_FALSE(h.col(100, kMaxPrice).present) << "only non-finite (NULL) prices remain";
    EXPECT_EQ(h.col(100, kN).i, 2);
}

TEST(AggRetractableRegression, UnsignedMinRetractsOnTombstoneRegression) {
    Harness h;
    h.putOrd({1, 100, 10, 1.0, "AAAA", 3});
    h.putOrd({2, 100, 10, 1.0, "AAAA", 9});
    EXPECT_EQ(h.col(100, kMinQty).u, 3u);
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kMinQty).u, 9u);
}

TEST(AggRetractableRegression, ContributorKeyEqualToSentinelIsSkippedRegression) {
    Harness h;
    const std::vector<uint8_t> sentinel(8, 0xFF);   // == kNoContributor
    h.apply({{sentinel, encode({7, 100, 99})}});
    EXPECT_EQ(h.last.skippedMalformed, 1u);
    EXPECT_FALSE(h.hasGroup(100)) << "never linked, never counted";
    h.putOrd({1, 100, 10});
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 10);
    EXPECT_EQ(h.inconsistent, 0u);
}

TEST(AggRetractableRegression, DanglingLinkIsCountedNotFollowedRegression) {
    Harness h;
    h.putOrd({1, 100, 50});
    h.putOrd({2, 100, 30});                          // list: 2 (head) -> 1
    ASSERT_TRUE(h.contributors.erase(2));            // damage: the head's row vanishes
    h.putOrd({1, 100, 20});                          // in-place lower of the max holder
    EXPECT_GE(h.last.aggInconsistent, 1u) << "the missing neighbour/head is counted";
    ASSERT_TRUE(h.hasGroup(100));
    EXPECT_EQ(h.col(100, kMaxAmt).i, 30)
        << "the rescan reads the VALUE BLOCK, which still holds contributor 2's 30 (the group still counts 2); "
           "the damaged list is never followed";
    EXPECT_EQ(h.col(100, kLastAmt).i, 20);
    EXPECT_EQ(h.contributors.get(2), nullptr) << "a link is never followed into a NEW row";
}

// REGRESSION (Task 5 review item 1; Task 5b): the per-record bound is REACHED, on
// every table, by one move: a mid-list contributor at block position 0 of a group
// whose spare chunk is freed by the remove, moved into a group with a head.
// A pure dirty-count pin (the byte budget is pinned by Task 7's ring test).
TEST(AggRetractableRegression, WorstCaseMoveDirtiesExactlyTheBoundRegression) {
    Harness h;
    for (int64_t id = 1; id <= 65; ++id) h.putOrd({id, 100, id});   // 65 entries: chunks 0, 1, 2
    for (int64_t id = 65; id >= 50; --id) h.put(id, tombstone());   // 49: chunk 2 is the spare, tail holds 17
    ASSERT_EQ(h.chunksOf(100), 3u);
    h.putOrd({100, 200, 7});                                        // u200 has a head (100)
    h.putOrd({3, 100, 3});
    h.putOrd({1, 100, 1});
    h.putOrd({2, 100, 2});                                          // u100 list: 2 -> 1 -> 3 -> 49 -> ...
    ASSERT_EQ(h.posOf(1), 0u);
    (void)h.drainCounts();
    h.putOrd({1, 200, 50});                                         // move 1 (pos 0, prev 2, next 3) to u200
    const auto b = rune::view::aggDirtyBoundPerRecord(h.agg);
    ASSERT_EQ(b.contributorRows, 5u);
    ASSERT_EQ(b.groupRows, 2u);
    ASSERT_EQ(b.viewRows, 2u);
    ASSERT_EQ(b.blockRows, 3u);
    const auto d = h.drainCounts();
    EXPECT_EQ(d.contributorRows, b.contributorRows) << "1, its neighbours 2 and 3, u200's old head 100, "
                                                       "and 49 (u100's last entry, swapped into position 0)";
    EXPECT_EQ(d.groupRows, b.groupRows);
    EXPECT_EQ(d.viewRows, b.viewRows);
    EXPECT_EQ(d.blockRows, b.blockRows) << "u100's hole chunk 0, u100's spare (tail now 16: freed), u200's chunk 0";
    EXPECT_EQ(d.pendingCaches, 0u);
    EXPECT_EQ(h.posOf(49), 0u);
    EXPECT_EQ(h.chunksOf(100), 2u);
    EXPECT_EQ(h.col(100, kMinAmt).i, 2) << "the moved min rescans the block";
    EXPECT_EQ(h.col(100, kMaxAmt).i, 49);
    EXPECT_EQ(h.col(200, kMaxAmt).i, 50);
    EXPECT_EQ(h.col(200, kMinAmt).i, 7);
    h.expectBlocksConsistent({100, 200});
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Task 5b): removing a mid-block entry moves the group's LAST entry
// into the hole and patches that contributor's stored position; the moved entry
// is then rewritten in place and retracted at its NEW position, exactly.
TEST(AggRetractableRegression, SwapRemovePatchesTheMovedContributorsPositionRegression) {
    Harness h;
    for (int64_t id = 1; id <= 40; ++id) h.putOrd({id, 100, 10 * id});   // 2 chunks: 32 + 8
    h.put(3, tombstone());                                                // hole at 2; 40 moves there
    EXPECT_EQ(h.posOf(40), 2u);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 400);
    h.expectBlocksConsistent({100});
    h.putOrd({40, 100, 5});                                               // in place at position 2
    EXPECT_EQ(h.posOf(40), 2u);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 390) << "the retracted max rescans the block";
    EXPECT_EQ(h.col(100, kMinAmt).i, 5);
    h.put(40, tombstone());                                               // 39 (the last) moves to 2
    EXPECT_EQ(h.posOf(39), 2u);
    EXPECT_EQ(h.col(100, kMinAmt).i, 10);
    EXPECT_EQ(h.col(100, kN).i, 38);
    h.expectBlocksConsistent({100});
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Task 5b, chunk policy): at a chunk boundary the block keeps ONE
// spare, so +-1 churn there never frees or re-adds a chunk; the spare is freed
// only once the tail chunk holds at most 16 entries.
TEST(AggRetractableRegression, ChunkBoundaryChurnKeepsOneSpareRegression) {
    Harness h;
    for (int64_t id = 1; id <= 33; ++id) h.putOrd({id, 100, id});      // the 33rd entry adds chunk 1
    EXPECT_EQ(h.chunksOf(100), 2u);
    EXPECT_EQ(h.blocks.size(), 2u);
    for (int round = 0; round < 5; ++round) {
        h.put(33, tombstone());                                        // 32: chunk 1 empties -> the spare
        EXPECT_EQ(h.chunksOf(100), 2u) << "round " << round;
        EXPECT_EQ(h.blocks.size(), 2u) << "round " << round;
        EXPECT_EQ(h.col(100, kMaxAmt).i, 32);
        (void)h.drainCounts();
        h.putOrd({33, 100, 33});                                       // 33 again: into the spare
        const auto d = h.drainCounts();
        EXPECT_EQ(d.blockRows, 1u) << "round " << round << ": one write, no erase, no new chunk";
        EXPECT_EQ(h.blocks.size(), 2u);
        EXPECT_EQ(h.col(100, kMaxAmt).i, 33);
    }
    for (int64_t id = 33; id >= 18; --id) h.put(id, tombstone());     // 17 left: the tail holds 17 > 16
    EXPECT_EQ(h.chunksOf(100), 2u) << "the spare survives while the tail is more than half full";
    h.put(17, tombstone());                                            // 16: the spare is freed
    EXPECT_EQ(h.chunksOf(100), 1u);
    EXPECT_EQ(h.blocks.size(), 1u);
    EXPECT_EQ(h.col(100, kMaxAmt).i, 16);
    EXPECT_EQ(h.col(100, kMinAmt).i, 1);
    h.expectBlocksConsistent({100});
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Task 5b, op x position): retracting the cached Max is exact whether
// its holder sits at chunk 0 slot 0, the last slot of a chunk, the first slot of
// the next chunk or the group's last entry -- by tombstone, in-place lower and
// group move.
TEST(AggRetractableRegression, ExtremeRetractionAtEveryBlockPositionIsExactRegression) {
    auto mx = [](const std::map<int64_t, int64_t>& m) {
        int64_t v = INT64_MIN; for (const auto& [k, a] : m) v = std::max(v, a); return v;
    };
    auto mn = [](const std::map<int64_t, int64_t>& m) {
        int64_t v = INT64_MAX; for (const auto& [k, a] : m) v = std::min(v, a); return v;
    };
    for (const uint32_t p : {0u, 31u, 32u, 63u, 64u, 99u}) {
        for (int how = 0; how < 3; ++how) {   // 0 tombstone, 1 in-place lower, 2 move to u200
            Harness h;
            std::map<int64_t, int64_t> g100, g200;
            for (int64_t id = 1; id <= 100; ++id) {
                const int64_t amt = uint32_t(id - 1) == p ? 1000 : id;
                h.putOrd({id, 100, amt});
                g100[id] = amt;
            }
            h.putOrd({500, 200, 7});
            g200[500] = 7;
            const int64_t holder = int64_t(p) + 1;
            ASSERT_EQ(h.posOf(holder), p);
            if (how == 0) { h.put(holder, tombstone()); g100.erase(holder); }
            else if (how == 1) { h.putOrd({holder, 100, -5}); g100[holder] = -5; }
            else { h.putOrd({holder, 200, 1000}); g100.erase(holder); g200[holder] = 1000; }
            EXPECT_GE(h.last.aggRescans, 1u) << "pos " << p << " how " << how;
            EXPECT_EQ(h.col(100, kMaxAmt).i, mx(g100)) << "pos " << p << " how " << how;
            EXPECT_EQ(h.col(100, kMinAmt).i, mn(g100)) << "pos " << p << " how " << how;
            EXPECT_EQ(h.col(200, kMaxAmt).i, mx(g200)) << "pos " << p << " how " << how;
            EXPECT_EQ(h.col(200, kMinAmt).i, mn(g200)) << "pos " << p << " how " << how;
            h.expectBlocksConsistent({100, 200});
            EXPECT_EQ(h.inconsistent, 0u) << "pos " << p << " how " << how;
        }
    }
}

// REGRESSION (Task 5b): a group's block id is its candidate unless that id is
// taken (linear probe); an emptied group frees every chunk row and its id, and a
// re-created group reclaims its candidate. Two keys with the same candidate never
// share chunk rows.
TEST(AggRetractableRegression, BlockIdsProbeOnCollisionAndAreFreedWithTheGroupRegression) {
    std::unordered_map<uint32_t, int64_t> seen;   // deterministic birthday search for a collision
    int64_t g1 = 0, g2 = 0;
    for (int64_t g = 1; g2 == 0; ++g) {
        const auto [it, fresh] = seen.emplace(rune::view::blockIdCandidate(uint64_t(g)), g);
        if (!fresh) { g1 = it->second; g2 = g; }
    }
    const uint32_t cand = rune::view::blockIdCandidate(uint64_t(g1));
    const uint32_t probed = cand == UINT32_MAX ? 1u : cand + 1;
    Harness h;
    for (int64_t id = 1; id <= 40; ++id) h.putOrd({id, g1, id});
    for (int64_t id = 41; id <= 80; ++id) h.putOrd({id, g2, id});
    EXPECT_EQ(h.blockIdOf(g1), cand);
    EXPECT_EQ(h.blockIdOf(g2), probed) << "linear probe past the taken candidate";
    EXPECT_EQ(h.blocks.size(), 4u);
    EXPECT_EQ(h.col(g1, kMaxAmt).i, 40);
    EXPECT_EQ(h.col(g2, kMinAmt).i, 41);
    h.expectBlocksConsistent({g1, g2});
    for (int64_t id = 1; id <= 40; ++id) h.put(id, tombstone());     // g1 empties
    EXPECT_FALSE(h.hasGroup(g1));
    EXPECT_EQ(h.blocks.size(), 2u) << "every chunk row of g1 is freed";
    EXPECT_EQ(h.blocks.get(rune::view::blockChunkKey(cand, 0)), nullptr);
    h.putOrd({1, g1, 9});                                            // g1 again: its candidate is free
    EXPECT_EQ(h.blockIdOf(g1), cand);
    EXPECT_EQ(h.col(g1, kMaxAmt).i, 9);
    EXPECT_EQ(h.col(g2, kMaxAmt).i, 80) << "g2's block was never touched";
    h.expectBlocksConsistent({g1, g2});
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Task 5b, damaged block): a missing chunk row is counted, never
// followed and never re-created -- not by the rescan, not by an in-place write.
TEST(AggRetractableRegression, MissingBlockChunkIsCountedNeverCreatedRegression) {
    Harness h;
    for (int64_t id = 1; id <= 40; ++id) h.putOrd({id, 100, id});    // chunk 0: 1..32, chunk 1: 33..40
    const uint64_t k1 = rune::view::blockChunkKey(h.blockIdOf(100), 1);
    ASSERT_TRUE(h.blocks.erase(k1));                                 // damage: chunk 1 vanishes
    h.put(40, tombstone());                                          // retract the max (the last entry)
    EXPECT_GE(h.last.aggInconsistent, 1u) << "the rescan meets the missing chunk";
    EXPECT_EQ(h.col(100, kMaxAmt).i, 32) << "only values actually found are reported";
    EXPECT_EQ(h.blocks.get(k1), nullptr) << "the rescan never creates a chunk";
    h.putOrd({35, 100, 99});                                         // in place inside the missing chunk
    EXPECT_GE(h.last.aggInconsistent, 1u);
    EXPECT_EQ(h.blocks.get(k1), nullptr) << "an in-place write never creates a chunk";
    EXPECT_EQ(h.col(100, kMaxAmt).i, 99) << "the Valid cache still takes a strictly better value, O(1)";
    EXPECT_EQ(h.col(100, kN).i, 39);
}

// REGRESSION (Task 5b, timing "across restart" at the view level): the value
// block travels in the v2 record as table 3; recovered tables then behave
// byte-for-byte like the live ones under the same history (swap-removes, a move,
// extreme retractions, a spare freed).
TEST(AggRetractableRegression, ValueBlockSurvivesACheckpointRoundTripRegression) {
    Harness live;
    for (int64_t id = 1; id <= 70; ++id) live.putOrd({id, 100, id});   // 3 chunks
    for (int64_t id = 70; id >= 61; --id) live.put(id, tombstone());   // 60 left: chunk 2 is the spare
    for (int64_t id = 71; id <= 75; ++id) live.putOrd({id, 200, id});
    live.putOrd({7, 100, 7});                                          // an in-place rewrite before the checkpoint
    ASSERT_EQ(live.chunksOf(100), 3u);
    const auto rec = checkpointAll(live);
    ASSERT_EQ(rec[12], 4u) << "the v2 record carries four tables";
    Harness back;
    restoreInto(back, rec, live.next, live.cursor);
    expectSameState(live, back, {100, 200}, 75);
    auto both = [&](auto&& op) {
        op(live);
        op(back);
        expectSameState(live, back, {100, 200}, 75);
    };
    both([](Harness& h) { h.put(60, tombstone()); });                  // retract the max (the last entry)
    both([](Harness& h) { h.put(1, tombstone()); });                   // retract the min at chunk 0 slot 0: 59 swaps in
    both([](Harness& h) { h.putOrd({59, 200, 1}); });                  // move the swapped-in entry out
    for (int64_t id = 2; id <= 13; ++id) both([id](Harness& h) { h.put(id, tombstone()); });
    EXPECT_EQ(back.chunksOf(100), 2u) << "the tail fell to 16: the recovered spare was freed";
    EXPECT_EQ(back.col(100, kMaxAmt).i, 58);
    EXPECT_EQ(back.col(100, kMinAmt).i, 14);
    EXPECT_EQ(back.col(200, kMinAmt).i, 1);
    EXPECT_EQ(back.col(200, kMaxAmt).i, 75);
    back.expectBlocksConsistent({100, 200});
    EXPECT_EQ(back.inconsistent, 0u);
    EXPECT_EQ(live.inconsistent, 0u);
}

// REGRESSION (Task 5b, op x block position x timing): 4000 steps of 1-4 records
// over 160 contributors / 3 groups (group 1 is three times as likely, and the
// tombstone rate alternates 20%/45% every 500 steps: group 1 ranges over ~33-93
// entries (across the 64 boundary and both spare thresholds, 48 and 80), groups
// 2/3 over ~8-39 (across 16 and 32), so swap-removes, spare reuse, spare frees
// and new chunks all occur), heavily duplicated amounts, Float NaN (null)
// prices; group moves happen whenever a rewrite picks another group.
// After EVERY step: Count/Max/Min/MaxPrice of every group equal a brute-force
// recomputation, the block structure is consistent and covers every live
// contributor, every table's dirty entries are within records x the bound, no
// cache is left RescanPending, nothing is inconsistent.
TEST(AggRetractableRegression, BlockModelAcrossChunkBoundariesMatchesBruteForceRegression) {
    struct M { int64_t user; int64_t amount; double price; };
    std::map<int64_t, M> live;
    Harness h;
    const auto bound = rune::view::aggDirtyBoundPerRecord(h.agg);
    uint64_t state = 0xD1B54A32D192ED03ull;
    auto rnd = [&](uint64_t m) {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        return (state >> 33) % m;
    };
    for (uint64_t step = 1; step <= 4000; ++step) {
        const size_t n = 1 + static_cast<size_t>(rnd(4));
        const uint64_t delPct = (step / 500) % 2 == 0 ? 20 : 45;
        std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> recs;
        for (size_t i = 0; i < n; ++i) {
            const int64_t id = 1 + static_cast<int64_t>(rnd(160));
            if (rnd(100) < delPct) {
                recs.push_back({Harness::key(id), tombstone()});
                live.erase(id);
            } else {
                const uint64_t r = rnd(5);
                Ord o{id, r < 3 ? 1 : static_cast<int64_t>(r - 1), static_cast<int64_t>(rnd(21)) - 10};
                o.price = rnd(5) == 0 ? std::numeric_limits<double>::quiet_NaN() : static_cast<double>(rnd(7));
                recs.push_back({Harness::key(id), encode(o)});
                live[id] = M{o.user, o.amount, o.price};
            }
        }
        h.apply(recs);
        const auto d = h.drainCounts();
        ASSERT_LE(d.contributorRows, n * bound.contributorRows) << "step " << step;
        ASSERT_LE(d.groupRows, n * bound.groupRows) << "step " << step;
        ASSERT_LE(d.viewRows, n * bound.viewRows) << "step " << step;
        ASSERT_LE(d.blockRows, n * bound.blockRows) << "step " << step;
        ASSERT_EQ(d.pendingCaches, 0u) << "step " << step;
        for (int64_t g = 1; g <= 3; ++g) {
            int64_t cnt = 0, mx = INT64_MIN, mn = INT64_MAX;
            double mp = 0.0;
            bool anyPrice = false;
            for (const auto& [k, m] : live) {
                if (m.user != g) continue;
                ++cnt;
                mx = std::max(mx, m.amount);
                mn = std::min(mn, m.amount);
                if (!std::isnan(m.price)) { mp = anyPrice ? std::max(mp, m.price) : m.price; anyPrice = true; }
            }
            if (cnt == 0) { ASSERT_FALSE(h.hasGroup(g)) << "step " << step << " group " << g; continue; }
            ASSERT_EQ(h.col(g, kN).i, cnt) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kMaxAmt).i, mx) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kMinAmt).i, mn) << "step " << step << " group " << g;
            const auto p = h.col(g, kMaxPrice);
            ASSERT_EQ(p.present, anyPrice) << "step " << step << " group " << g;
            if (anyPrice) ASSERT_DOUBLE_EQ(p.f, mp) << "step " << step << " group " << g;
        }
        ASSERT_EQ(h.expectBlocksConsistent({1, 2, 3}), live.size()) << "step " << step;
        if (::testing::Test::HasFailure()) FAIL() << "first failure at step " << step;
    }
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (Review Focus #1/#3, F9): a deterministic history of 3000 steps of
// 1-4 records each (inserts, in-place updates, group moves, deletes; the same
// contributor can repeat inside one batch) over 24 contributors / 4 groups, with
// heavy value duplication. Every 5th multi-record step is folded in two passes
// split at a random mid-batch boundary (capped-pass resume). After EVERY step:
// each group's Count(*), Sum, Max, Min, Last, Last(bonus), LastNonNull(bonus)
// equals a brute-force recomputation; each table's dirty entries are within
// records x the derived per-record bound; no cache is left RescanPending;
// nothing is ever inconsistent.
TEST(AggRetractableRegression, RandomizedOpsMatchBruteForceModelRegression) {
    struct M { int64_t user; int64_t amount; std::optional<int32_t> bonus; uint64_t seq; };
    std::map<int64_t, M> live;
    Harness h;
    const auto bound = rune::view::aggDirtyBoundPerRecord(h.agg);
    uint64_t state = 0x9E3779B97F4A7C15ull;
    auto rnd = [&](uint64_t m) {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        return (state >> 33) % m;
    };
    uint64_t seq = 0;
    for (uint64_t step = 1; step <= 3000; ++step) {
        const size_t n = 1 + static_cast<size_t>(rnd(4));
        std::vector<Harness::Rec> recs;
        for (size_t i = 0; i < n; ++i) {
            const int64_t id = 1 + static_cast<int64_t>(rnd(24));
            ++seq;
            if (rnd(100) < 15) {
                recs.push_back({Harness::key(id), tombstone()});
                live.erase(id);
            } else {
                Ord o{id, 1 + static_cast<int64_t>(rnd(4)), static_cast<int64_t>(rnd(41)) - 20};
                if (rnd(2) == 0) o.bonus = static_cast<int32_t>(rnd(9));
                recs.push_back({Harness::key(id), encode(o)});
                live[id] = M{o.user, o.amount, o.bonus, seq};
            }
        }
        Harness::Drained d;
        if (n >= 2 && step % 5 == 0) {
            d = h.applySplit(recs, 1 + static_cast<size_t>(rnd(n - 1)));   // pass 1 stops mid-batch
        } else {
            h.apply(recs);
            d = h.drainCounts();
        }
        ASSERT_LE(d.contributorRows, n * bound.contributorRows) << "step " << step;
        ASSERT_LE(d.groupRows, n * bound.groupRows) << "step " << step;
        ASSERT_LE(d.viewRows, n * bound.viewRows) << "step " << step;
        ASSERT_LE(d.blockRows, n * bound.blockRows) << "step " << step;
        ASSERT_EQ(d.pendingCaches, 0u) << "step " << step;
        ASSERT_EQ(h.expectBlocksConsistent({1, 2, 3, 4}), live.size()) << "step " << step;
        for (int64_t g = 1; g <= 4; ++g) {
            const M* lastM = nullptr;
            const M* lastNn = nullptr;
            int64_t cnt = 0, total = 0, mx = INT64_MIN, mn = INT64_MAX;
            for (const auto& [k, m] : live) {
                if (m.user != g) continue;
                ++cnt; total += m.amount;
                mx = std::max(mx, m.amount); mn = std::min(mn, m.amount);
                if (lastM == nullptr || m.seq > lastM->seq) lastM = &m;
                if (m.bonus && (lastNn == nullptr || m.seq > lastNn->seq)) lastNn = &m;
            }
            if (cnt == 0) { ASSERT_FALSE(h.hasGroup(g)) << "step " << step << " group " << g; continue; }
            ASSERT_EQ(h.col(g, kN).i, cnt) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kTotal).i, total) << "step " << step;
            ASSERT_EQ(h.col(g, kMaxAmt).i, mx) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kMinAmt).i, mn) << "step " << step << " group " << g;
            ASSERT_EQ(h.col(g, kLastAmt).i, lastM->amount) << "step " << step << " group " << g;
            const auto lb = h.col(g, kLastBonus);
            ASSERT_EQ(lb.present, lastM->bonus.has_value()) << "step " << step;
            if (lastM->bonus) ASSERT_EQ(lb.i, *lastM->bonus) << "step " << step;
            const auto lnn = h.col(g, kLastNnBonus);
            ASSERT_EQ(lnn.present, lastNn != nullptr) << "step " << step;
            if (lastNn != nullptr) ASSERT_EQ(lnn.i, *lastNn->bonus) << "step " << step;
        }
    }
    EXPECT_EQ(h.inconsistent, 0u);
}

// REGRESSION (F9, op "no-group"): a rewrite whose group key is ABSENT retracts
// the old contributor from a LINKED group -- unlink, count, cache -- and skips.
// The source declares the group-by field only from v2 (as AggFoldTest's
// MissingGroupValueRetractsAndSkips does); a v1 record lacks it.
TEST(AggRetractableRegression, NoGroupRewriteUnlinksAndRetractsTheContributorRegression) {
    const auto src = rune::codec::DescriptorBuilder("o", 2)
        .add("orderId", P::Int64, 1, true)
        .add("amount", P::Int64, 1)
        .add("userId", P::Int64, 2)
        .build();
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"max", P::Int64, 0, "amount", R::Max},
                {"last", P::Int64, 0, "amount", R::Last}};
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, 9).value();
    auto agg = rune::view::buildAggLayout(comp.plan, src).value();
    ASSERT_TRUE(agg.linked);
    rune::view::VersionDescriptorCache vdc(src);
    rune::view::ViewTable view(comp.rowLayout), contributors(agg.contributorRow), groups(agg.groupRow);
    rune::view::ViewTable blocks(agg.blockRow);   // Max(amount): a value block (Task 5b)
    int64_t off = 0;
    auto fold = [&](int64_t id, const std::vector<uint8_t>& value) {
        rune::protocol::RecordBatchBuilder b(off, 0);
        const auto k = Harness::key(id);
        b.append(std::string_view(reinterpret_cast<const char*>(k.data()), 8),
                 std::string_view(reinterpret_cast<const char*>(value.data()), value.size()), 0);
        const auto batch = b.build();
        int64_t cur = off;
        const auto st = rune::view::foldAggregateBatch(
            comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups, &blocks,
            {batch.data(), batch.size(), off, off + 1}, off + 1, cur);
        ++off;
        return st;
    };
    auto v2 = [&](int64_t id, int64_t amount) {
        std::vector<uint8_t> b;
        le16(b, 24); le16(b, 2); le64(b, uint64_t(id)); le64(b, uint64_t(amount)); le64(b, 100);
        return b;
    };
    (void)fold(1, v2(1, 50));
    (void)fold(2, v2(2, 30));                       // list: 2 (head) -> 1; max 50 held by contributor 1
    std::vector<uint8_t> v1;
    le16(v1, 16); le16(v1, 1); le64(v1, 1); le64(v1, 50);   // contributor 1 rewritten WITHOUT userId
    const auto st = fold(1, v1);
    EXPECT_EQ(st.skippedNoGroup, 1u);
    EXPECT_EQ(st.aggInconsistent, 0u);
    const uint8_t* row = view.get(100);
    ASSERT_NE(row, nullptr) << "contributor 2 still holds the group";
    EXPECT_EQ(rune::view::readSlot(row, comp.rowLayout, 1).i, 1);
    EXPECT_EQ(rune::view::readSlot(row, comp.rowLayout, 2).i, 30) << "the retracted max (50) rescans to 30";
    EXPECT_EQ(rune::view::readSlot(row, comp.rowLayout, 3).i, 30);
    EXPECT_EQ(contributors.size(), 1u);
    EXPECT_EQ(contributors.get(1), nullptr);
    (void)fold(2, [&] { std::vector<uint8_t> b; le16(b, 16); le16(b, 1); le64(b, 2); le64(b, 30); return b; }());
    EXPECT_EQ(view.get(100), nullptr) << "the last contributor left: group and list are gone";
    EXPECT_EQ(contributors.size(), 0u);
    EXPECT_EQ(blocks.size(), 0u) << "the emptied group's value block (chunk 0) and id are freed";
}

// REGRESSION (Task 5 review carry-over): Min/Max over negative Int32, UInt16 and
// Float64 (+-0.0), and Last over UInt16 and Bool, model-checked against brute
// force across inserts, in-place updates, group moves and deletes. Several
// records for one contributor fall in a single batch. Retracting a single
// extreme costs exactly one rescan.
TEST(AggRetractableRegression, NarrowAndSignedTypesMatchBruteForceRegression) {
    const auto src = rune::codec::DescriptorBuilder("t", 1)
        .add("id", P::Int64, 1, true)
        .add("grp", P::Int64, 1)
        .add("i32", P::Int32, 1)
        .add("u16", P::UInt16, 1)
        .add("f", P::Float64, 1)
        .add("flag", P::Bool, 1)
        .build();
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "grp"; v.sources = {{5, "grp"}};
    v.output = {{"grp", P::Int64, 0, "grp", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"maxI", P::Int32, 0, "i32", R::Max},  {"minI", P::Int32, 0, "i32", R::Min},
                {"maxU", P::UInt16, 0, "u16", R::Max}, {"minU", P::UInt16, 0, "u16", R::Min},
                {"minF", P::Float64, 0, "f", R::Min},  {"maxF", P::Float64, 0, "f", R::Max},
                {"lastU", P::UInt16, 0, "u16", R::Last}, {"lastB", P::Bool, 0, "flag", R::Last}};
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, 9).value();
    auto agg = rune::view::buildAggLayout(comp.plan, src).value();
    rune::view::VersionDescriptorCache vdc(src);
    rune::view::ViewTable view(comp.rowLayout), contributors(agg.contributorRow),
        groups(agg.groupRow), blocks(agg.blockRow);
    int64_t off = 0, cur = 0;
    uint64_t inconsistent = 0;
    struct M { int64_t grp; int32_t i; uint16_t u; double f; bool b; uint64_t seq; };
    std::map<int64_t, M> live;
    const double fs[] = {-2.5, -0.0, 0.0, 0.0, 1.5, 3.0};
    uint64_t state = 0xA24BAED4963EE407ull, seq = 0;
    auto rnd = [&](uint64_t m) {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        return (state >> 33) % m;
    };
    uint32_t rescansSeen = 0;
    for (int step = 1; step <= 1500; ++step) {
        const size_t n = 1 + static_cast<size_t>(rnd(3));
        rune::protocol::RecordBatchBuilder b(off, 0);
        for (size_t k = 0; k < n; ++k) {
            const int64_t id = 1 + static_cast<int64_t>(rnd(8));
            const auto key = Harness::key(id);
            std::vector<uint8_t> val;
            ++seq;
            if (rnd(100) < 20) {
                live.erase(id);
            } else {
                M m{1 + static_cast<int64_t>(rnd(2)), static_cast<int32_t>(rnd(7)) - 3,
                    static_cast<uint16_t>(rnd(4) * 21845), fs[rnd(6)], rnd(2) == 0, seq};
                le16(val, 35); le16(val, 1); le64(val, uint64_t(id)); le64(val, uint64_t(m.grp));
                le32(val, uint32_t(m.i)); le16(val, m.u);
                uint64_t raw; std::memcpy(&raw, &m.f, 8); le64(val, raw);
                val.push_back(m.b ? 1 : 0);
                live[id] = m;
            }
            b.append(std::string_view(reinterpret_cast<const char*>(key.data()), key.size()),
                     std::string_view(reinterpret_cast<const char*>(val.data()), val.size()), 0);
        }
        const auto batch = b.build();
        const int64_t bound = off + static_cast<int64_t>(n);
        const auto st = rune::view::foldAggregateBatch(
            comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups, &blocks,
            {batch.data(), batch.size(), off, bound}, bound, cur);
        off = bound;
        inconsistent += st.aggInconsistent;
        rescansSeen += st.aggRescans;
        for (int64_t g = 1; g <= 2; ++g) {
            int64_t cnt = 0, mxI = INT32_MIN, mnI = INT32_MAX;
            uint16_t mxU = 0, mnU = UINT16_MAX;
            double mnF = 1e9, mxF = -1e9;
            const M* lastM = nullptr;
            for (const auto& [id, m] : live) {
                if (m.grp != g) continue;
                ++cnt;
                mxI = std::max<int64_t>(mxI, m.i); mnI = std::min<int64_t>(mnI, m.i);
                mxU = std::max(mxU, m.u); mnU = std::min(mnU, m.u);
                mnF = std::min(mnF, m.f); mxF = std::max(mxF, m.f);
                if (lastM == nullptr || m.seq > lastM->seq) lastM = &m;
            }
            const uint8_t* row = view.get(uint64_t(g));
            if (cnt == 0) { ASSERT_EQ(row, nullptr) << "step " << step; continue; }
            ASSERT_NE(row, nullptr) << "step " << step;
            auto col = [&](uint16_t s) { return rune::view::readSlot(row, comp.rowLayout, s); };
            ASSERT_EQ(col(1).i, cnt) << "step " << step;
            ASSERT_EQ(col(2).i, mxI) << "step " << step << " group " << g;
            ASSERT_EQ(col(3).i, mnI) << "step " << step << " group " << g;
            ASSERT_EQ(col(4).u, mxU) << "step " << step << " group " << g;
            ASSERT_EQ(col(5).u, mnU) << "step " << step << " group " << g;
            ASSERT_EQ(col(6).f, mnF) << "step " << step << " group " << g;
            ASSERT_EQ(col(7).f, mxF) << "step " << step << " group " << g;
            ASSERT_EQ(col(8).u, lastM->u) << "step " << step << " group " << g;
            ASSERT_EQ(col(9).b, lastM->b) << "step " << step << " group " << g;
        }
        if (::testing::Test::HasFailure()) FAIL() << "first failure at step " << step;
    }
    EXPECT_EQ(inconsistent, 0u);
    EXPECT_GT(rescansSeen, 0u) << "the history must exercise the rescan path";
}
