#include <gtest/gtest.h>

#include <climits>
#include <cmath>
#include <cstring>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/AggFold.h>
#include <rune/view/AggState.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

using P = rune::codec::PrimType;
using R = rune::view::MergeRule;

namespace {

// order: v1 = {orderId Int64 (key), userId Int64, amount Int64, qty UInt32, price Float64};
//        v2 adds bonus Int64 (sinceVersion 2) — a v1 record reads bonus as ABSENT (NULL).
rune::codec::Descriptor orderSource() {
    return rune::codec::DescriptorBuilder("order", 2)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64, 1)
        .add("amount", P::Int64, 1)
        .add("qty", P::UInt32, 1)
        .add("price", P::Float64, 1)
        .add("bonus", P::Int64, 2)
        .build();
}

// Output slots (key first, then declaration order):
//  0 userId | 1 n=Count(*) | 2 bonusCnt=Count(bonus) | 3 total=Sum(amount) | 4 avg=Avg(amount)
//  5 qtySum=Sum(qty) UInt64 | 6 priceAvg=Avg(price) | 7 bonusSum=Sum(bonus)
enum Slot : uint16_t { kUser = 0, kN, kBonusCnt, kTotal, kAvg, kQtySum, kPriceAvg, kBonusSum };

rune::view::ViewPlan userTotals() {
    rune::view::ViewPlan v;
    v.viewName = "user_totals"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"bonusCnt", P::Int64, 0, "bonus", R::Count},
                {"total", P::Int64, 0, "amount", R::Sum},
                {"avg", P::Float64, 0, "amount", R::Avg},
                {"qtySum", P::UInt64, 0, "qty", R::Sum},
                {"priceAvg", P::Float64, 0, "price", R::Avg},
                {"bonusSum", P::Int64, 0, "bonus", R::Sum}};
    return v;
}

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le32(std::vector<uint8_t>& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }

std::vector<uint8_t> orderV1(int64_t orderId, int64_t userId, int64_t amount,
                             uint32_t qty = 1, double price = 1.0) {
    std::vector<uint8_t> b;
    le16(b, 8 + 8 + 8 + 4 + 8);  le16(b, 1);
    le64(b, uint64_t(orderId)); le64(b, uint64_t(userId)); le64(b, uint64_t(amount));
    le32(b, qty); uint64_t pr; std::memcpy(&pr, &price, 8); le64(b, pr);
    return b;
}
std::vector<uint8_t> orderV2(int64_t orderId, int64_t userId, int64_t amount, int64_t bonus) {
    std::vector<uint8_t> b;
    le16(b, 8 + 8 + 8 + 4 + 8 + 8);  le16(b, 2);
    le64(b, uint64_t(orderId)); le64(b, uint64_t(userId)); le64(b, uint64_t(amount));
    le32(b, 1); double price = 1.0; uint64_t pr; std::memcpy(&pr, &price, 8); le64(b, pr);
    le64(b, uint64_t(bonus));
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

    static rune::view::CompileResult compile(const rune::codec::Descriptor& s) {
        const rune::codec::Descriptor* srcs[] = {&s};
        return rune::view::compileView(userTotals(), srcs, 9).value();
    }

    // One batch; contributor key = 8-byte LE orderId (or a raw key if given).
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
        next = bound;
    }
    static std::vector<uint8_t> key(int64_t orderId) {
        std::vector<uint8_t> k; le64(k, uint64_t(orderId)); return k;
    }
    void put(int64_t orderId, std::vector<uint8_t> value) { apply({{key(orderId), std::move(value)}}); }

    bool hasGroup(int64_t userId) const { return view.get(uint64_t(userId)) != nullptr; }
    rune::codec::FieldValue col(int64_t userId, uint16_t slot) const {
        const uint8_t* r = view.get(uint64_t(userId));
        EXPECT_NE(r, nullptr) << "group " << userId << " must exist";
        return r ? rune::view::readSlot(r, comp.rowLayout, slot) : rune::codec::FieldValue{};
    }
};

} // namespace

TEST(AggFold, InsertsAccumulatePerGroup) {
    Harness h;
    h.put(1, orderV1(1, 100, 10, 2, 1.5));
    h.put(2, orderV1(2, 100, 30, 3, 2.5));
    h.put(3, orderV1(3, 200, 5));
    EXPECT_EQ(h.col(100, kUser).i, 100);
    EXPECT_EQ(h.col(100, kN).i, 2);
    EXPECT_EQ(h.col(100, kTotal).i, 40);
    EXPECT_DOUBLE_EQ(h.col(100, kAvg).f, 20.0);
    EXPECT_EQ(h.col(100, kQtySum).u, 5u);
    EXPECT_DOUBLE_EQ(h.col(100, kPriceAvg).f, 2.0);
    EXPECT_EQ(h.col(100, kBonusCnt).i, 0) << "Count(col) over all-null is 0";
    EXPECT_FALSE(h.col(100, kBonusSum).present) << "Sum over all-null is NULL (SQL)";
    EXPECT_EQ(h.col(200, kN).i, 1);
    EXPECT_EQ(h.col(200, kTotal).i, 5);
    EXPECT_EQ(h.last.applied, 1u);
}

TEST(AggFoldRegression, UpdateRetractsOldContributionNotAddsTwice) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(1, orderV1(1, 100, 50));
    EXPECT_EQ(h.col(100, kN).i, 1) << "same contributor -> still one";
    EXPECT_EQ(h.col(100, kTotal).i, 50) << "old 10 retracted, new 50 added";
}

TEST(AggFoldRegression, UpdateMovesContributorBetweenGroupsAndDropsEmptyGroupRegression) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(3, orderV1(3, 200, 5));
    h.put(3, orderV1(3, 100, 5));   // contributor 3 moves group 200 -> 100
    EXPECT_FALSE(h.hasGroup(200)) << "the emptied group has no row";
    EXPECT_EQ(h.groups.get(200), nullptr) << "and no accumulator row";
    EXPECT_EQ(h.col(100, kN).i, 2);
    EXPECT_EQ(h.col(100, kTotal).i, 15);
}

TEST(AggFoldRegression, TombstoneRetractsAndLastContributorDropsGroup) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(2, orderV1(2, 100, 30));
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_EQ(h.col(100, kTotal).i, 10);
    h.put(1, tombstone());
    EXPECT_FALSE(h.hasGroup(100));
    EXPECT_EQ(h.contributors.size(), 0u);
    EXPECT_EQ(h.groups.size(), 0u);
}

TEST(AggFoldRegression, TombstoneForUnknownContributorIsNoop) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(99, tombstone());
    EXPECT_EQ(h.last.deleted, 1u);
    EXPECT_EQ(h.col(100, kN).i, 1) << "no negative counts / no stray retraction";
    EXPECT_EQ(h.col(100, kTotal).i, 10);
}

TEST(AggFoldRegression, IdenticalReproduceIsIdempotent) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));
    h.put(1, orderV1(1, 100, 10));
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_EQ(h.col(100, kTotal).i, 10);
}

TEST(AggFoldRegression, RetractingLastNonNullMakesSumNullNotZeroRegression) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));        // bonus NULL (v1)
    h.put(2, orderV2(2, 100, 20, 7));     // bonus 7
    EXPECT_EQ(h.col(100, kBonusCnt).i, 1);
    EXPECT_EQ(h.col(100, kBonusSum).i, 7);
    EXPECT_EQ(h.col(100, kN).i, 2) << "Count(*) counts the null-bonus contributor too";
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kN).i, 1) << "group keeps its remaining contributor";
    EXPECT_EQ(h.col(100, kBonusCnt).i, 0);
    EXPECT_FALSE(h.col(100, kBonusSum).present) << "all-null Sum is NULL, never 0";
    EXPECT_EQ(h.col(100, kTotal).i, 10);
}

TEST(AggFoldRegression, Int64SumOverflowIsNullThenRecoversExactlyRegression) {
    Harness h;
    h.put(1, orderV1(1, 100, INT64_MAX));
    h.put(2, orderV1(2, 100, 1));
    EXPECT_FALSE(h.col(100, kTotal).present) << "an overflowing Sum is NULL, never wrapped";
    EXPECT_GT(h.last.aggOverflow, 0u);
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kTotal).i, INT64_MAX) << "int128 accumulator retracts exactly";
}

TEST(AggFoldRegression, MalformedContributorKeyIsSkipped) {
    Harness h;
    h.apply({{std::vector<uint8_t>{1, 2, 3, 4}, orderV1(1, 100, 10)}});
    EXPECT_EQ(h.last.skippedMalformed, 1u);
    EXPECT_FALSE(h.hasGroup(100));
    EXPECT_EQ(h.contributors.size(), 0u);
}

TEST(AggFold, CursorAdvancesToBoundAndOnlyCommittedRecordsFold) {
    Harness h;
    rune::protocol::RecordBatchBuilder b(0, 0);
    const auto k1 = Harness::key(1), k2 = Harness::key(2);
    const auto v1 = orderV1(1, 100, 10), v2 = orderV1(2, 100, 20);
    b.append(std::string_view((const char*)k1.data(), 8), std::string_view((const char*)v1.data(), v1.size()), 0);
    b.append(std::string_view((const char*)k2.data(), 8), std::string_view((const char*)v2.data(), v2.size()), 0);
    const auto batch = b.build();
    const rune::storage::LogReadView lv{batch.data(), batch.size(), 0, 2};
    int64_t cursor = 0;
    (void)rune::view::foldAggregateBatch(h.comp.plan, h.comp.rowLayout, h.agg, h.vdc, h.view,
                                         h.contributors, h.groups, lv, /*bound*/ 1, cursor);
    EXPECT_EQ(cursor, 1);
    EXPECT_EQ(h.col(100, kN).i, 1) << "offset 1 is uncommitted and must not fold";
}

TEST(AggFoldRegression, MissingGroupValueRetractsAndSkips) {
    const auto src = rune::codec::DescriptorBuilder("o", 2)
        .add("orderId", P::Int64, 1, true)
        .add("amount", P::Int64, 1)
        .add("userId", P::Int64, 2)       // the group-by field only exists from v2
        .build();
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"total", P::Int64, 0, "amount", R::Sum}};
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, 9).value();
    auto agg = rune::view::buildAggLayout(comp.plan, src).value();
    rune::view::VersionDescriptorCache vdc(src);
    rune::view::ViewTable view(comp.rowLayout), contributors(agg.contributorRow), groups(agg.groupRow);

    auto fold = [&](int64_t off, std::vector<uint8_t> value) {
        rune::protocol::RecordBatchBuilder b(off, 0);
        const auto k = Harness::key(1);
        b.append(std::string_view((const char*)k.data(), 8),
                 std::string_view((const char*)value.data(), value.size()), 0);
        const auto batch = b.build();
        int64_t cur = off;
        return rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors,
                                              groups, {batch.data(), batch.size(), off, off + 1}, off + 1, cur);
    };
    std::vector<uint8_t> v2; le16(v2, 24); le16(v2, 2); le64(v2, 1); le64(v2, 10); le64(v2, 100);
    fold(0, v2);
    ASSERT_NE(view.get(100), nullptr);
    std::vector<uint8_t> v1; le16(v1, 16); le16(v1, 1); le64(v1, 1); le64(v1, 10);   // no userId
    const auto st = fold(1, v1);
    EXPECT_EQ(st.skippedNoGroup, 1u);
    EXPECT_EQ(view.get(100), nullptr) << "the contributor left its group, which is now empty";
    EXPECT_EQ(contributors.size(), 0u);
}

// ---- Task 4 fix round 1 ----

TEST(AggFoldRegression, UnsignedSumRetractsOnUpdateAndTombstoneRegression) {
    // u64 overflow-then-recover is not covered here: qty is UInt32, so no
    // realistic contributor count can overflow the u64 output; the Unsigned
    // int128 path is exercised for exact retraction instead.
    Harness h;
    h.put(1, orderV1(1, 100, 1, 4000000000u));
    h.put(2, orderV1(2, 100, 1, 4000000000u));
    EXPECT_EQ(h.col(100, kQtySum).u, 8000000000ull);
    h.put(2, orderV1(2, 100, 1, 7u));
    EXPECT_EQ(h.col(100, kQtySum).u, 4000000007ull);
    h.put(1, tombstone());
    EXPECT_EQ(h.col(100, kQtySum).u, 7u);
}

TEST(AggFoldRegression, FloatAccumulatorResetsWhenLastNonNullLeavesRegression) {
    // 1e16 + 1.0 == 1e16 in double (ulp is 2), so retracting 1e16 then 1.0
    // leaves -1.0 without the reset; a later +1.0 would then read 0.0, not 1.0.
    Harness h;
    h.put(9, orderV1(9, 100, 1, 1, std::nan("")));   // NULL price: keeps the group alive
    h.put(1, orderV1(1, 100, 1, 1, 1e16));
    h.put(2, orderV1(2, 100, 1, 1, 1.0));
    h.put(1, tombstone());
    h.put(2, tombstone());
    EXPECT_EQ(h.col(100, kN).i, 1);
    EXPECT_FALSE(h.col(100, kPriceAvg).present);
    h.put(3, orderV1(3, 100, 1, 1, 1.0));
    EXPECT_TRUE(h.col(100, kPriceAvg).present);
    EXPECT_EQ(h.col(100, kPriceAvg).f, 1.0) << "accumulator must restart at exactly 0.0";
}

TEST(AggFoldRegression, NonFiniteFloatInputIsNullAndCountedRegression) {
    Harness h;
    h.put(1, orderV1(1, 100, 1, 1, 2.0));
    h.put(2, orderV1(2, 100, 1, 1, std::numeric_limits<double>::quiet_NaN()));
    EXPECT_GT(h.last.aggNonFinite, 0u);
    h.put(4, orderV1(4, 100, 1, 1, std::numeric_limits<double>::infinity()));
    EXPECT_GT(h.last.aggNonFinite, 0u);
    h.put(3, orderV1(3, 100, 1, 1, 4.0));
    EXPECT_DOUBLE_EQ(h.col(100, kPriceAvg).f, 3.0);
    EXPECT_TRUE(std::isfinite(h.col(100, kPriceAvg).f));
    h.put(1, tombstone());
    EXPECT_DOUBLE_EQ(h.col(100, kPriceAvg).f, 4.0);
    EXPECT_EQ(h.col(100, kN).i, 3);
}

TEST(AggFoldRegression, UpdateTogglesBonusNullAndNonNullRegression) {
    Harness h;
    h.put(1, orderV1(1, 100, 10));                 // bonus NULL
    EXPECT_EQ(h.col(100, kBonusCnt).i, 0);
    EXPECT_FALSE(h.col(100, kBonusSum).present);
    h.put(1, orderV2(1, 100, 10, 7));              // NULL -> 7
    EXPECT_EQ(h.col(100, kBonusCnt).i, 1);
    EXPECT_EQ(h.col(100, kBonusSum).i, 7);
    EXPECT_EQ(h.col(100, kN).i, 1);
    h.put(1, orderV1(1, 100, 10));                 // 7 -> NULL
    EXPECT_EQ(h.col(100, kBonusCnt).i, 0);
    EXPECT_FALSE(h.col(100, kBonusSum).present);
    EXPECT_EQ(h.col(100, kN).i, 1);
}

TEST(AggFoldRegression, InconsistentRetractionIsCountedNotMaskedRegression) {
    {
        Harness h;
        h.put(1, orderV1(1, 100, 10));
        ASSERT_TRUE(h.groups.erase(100));
        h.put(1, tombstone());
        EXPECT_EQ(h.last.aggInconsistent, 1u);
        EXPECT_EQ(h.groups.get(100), nullptr) << "no phantom group row";
        EXPECT_EQ(h.groups.size(), 0u);
        EXPECT_EQ(h.contributors.size(), 0u) << "the contributor is still erased";
    }
    {
        Harness h;
        h.put(1, orderV1(1, 100, 10));
        ASSERT_TRUE(h.groups.erase(100));
        h.put(1, orderV1(1, 100, 50));              // update: bad retract, then clean add
        EXPECT_EQ(h.last.aggInconsistent, 1u);
        EXPECT_EQ(h.col(100, kN).i, 1);
        EXPECT_EQ(h.col(100, kTotal).i, 50);
    }
}

// REGRESSION (Task 7 fix round 1): resuming with the cursor INSIDE a batch must
// skip the batch's already-folded records (see ViewFoldResumeRegression).
TEST(AggFoldRegression, MidBatchCursorSkipsAlreadyFoldedRecordsRegression) {
    Harness h;
    rune::protocol::RecordBatchBuilder b(0, 0);
    const auto k1 = Harness::key(1), k2 = Harness::key(2);
    const auto v1 = orderV1(1, 100, 10), v2 = orderV1(2, 100, 30);
    auto sv = [](const std::vector<uint8_t>& x) {
        return std::string_view(reinterpret_cast<const char*>(x.data()), x.size());
    };
    b.append(sv(k1), sv(v1), 0);
    b.append(sv(k2), sv(v2), 0);
    const auto batch = b.build();
    const rune::storage::LogReadView lv{batch.data(), batch.size(), 0, 2};

    int64_t cur = 0;
    auto s0 = rune::view::foldAggregateBatch(h.comp.plan, h.comp.rowLayout, h.agg, h.vdc, h.view,
                                             h.contributors, h.groups, lv, /*bound*/ 1, cur);
    EXPECT_EQ(s0.applied, 1u);
    EXPECT_EQ(cur, 1);
    auto s1 = rune::view::foldAggregateBatch(h.comp.plan, h.comp.rowLayout, h.agg, h.vdc, h.view,
                                             h.contributors, h.groups, lv, /*bound*/ 2, cur);
    EXPECT_EQ(s1.applied, 1u) << "record 0 (below the cursor) must not be re-folded";
    EXPECT_EQ(cur, 2);
    EXPECT_EQ(h.col(100, kN).i, 2);
    EXPECT_EQ(h.col(100, kTotal).i, 40);
}
