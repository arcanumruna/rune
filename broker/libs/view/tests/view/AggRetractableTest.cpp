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
                                              contributors, groups, lv, bound, cursor);
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
    struct Drained { uint32_t contributorRows = 0, groupRows = 0, viewRows = 0, pendingCaches = 0; };
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
        return d;
    }
};

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
    EXPECT_EQ(h.col(100, kMaxAmt).i, 20) << "the rescan only reports values it actually found";
    EXPECT_EQ(h.col(100, kLastAmt).i, 20);
    EXPECT_EQ(h.contributors.get(2), nullptr) << "a link is never followed into a NEW row";
}

// Carried from the Task 4 review: pins aggDirtyBoundPerRecord against the real
// worst case (a mid-list contributor moving to a group that has a head) and that
// the pass's checkpoint delta fits the per-record budget maxRecordsPerPassFor
// (WorkerViewSet.cpp) derives from the same bound.
TEST(AggRetractableRegression, WorstCaseMoveDirtiesExactlyTheBoundAndFitsTheBudgetRegression) {
    Harness h;
    h.putOrd({1, 100, 10});
    h.putOrd({2, 100, 20});
    h.putOrd({3, 100, 30});                          // list: 3 -> 2 -> 1 (2 is mid-list)
    h.putOrd({4, 200, 40});                          // group 200 has a head
    (void)h.drainCounts();
    h.putOrd({2, 200, 25});                          // move the mid-list contributor
    const auto b = rune::view::aggDirtyBoundPerRecord(h.agg);
    // Count rows only (drainCounts also checks RescanPending).
    const auto d = h.drainCounts();
    EXPECT_EQ(d.contributorRows, b.contributorRows) << "contributor + both neighbours + new head";
    EXPECT_EQ(d.groupRows, b.groupRows);
    EXPECT_EQ(d.viewRows, b.viewRows);
    EXPECT_EQ(d.pendingCaches, 0u) << "no RescanPending survives a record";
    const std::size_t perRecord =
        b.contributorRows * (rune::view::kCheckpointEntryHeaderBytes + h.agg.contributorRow.rowLen) +
        b.groupRows * (rune::view::kCheckpointEntryHeaderBytes + h.agg.groupRow.rowLen) +
        b.viewRows * (rune::view::kCheckpointEntryHeaderBytes + h.comp.rowLayout.rowLen);
    const std::size_t actual =
        d.contributorRows * (rune::view::kCheckpointEntryHeaderBytes + h.agg.contributorRow.rowLen) +
        d.groupRows * (rune::view::kCheckpointEntryHeaderBytes + h.agg.groupRow.rowLen) +
        d.viewRows * (rune::view::kCheckpointEntryHeaderBytes + h.comp.rowLayout.rowLen);
    EXPECT_LE(actual, perRecord) << "one record's delta fits the per-record budget";
    EXPECT_EQ(h.inconsistent, 0u);
}
