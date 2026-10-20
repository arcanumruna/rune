// libs/view/tests/view/AggStateTest.cpp — Phase 2a aggregate layout.
#include <gtest/gtest.h>

#include <rune/codec/Descriptor.h>
#include <rune/view/AggState.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewTable.h>

#include <vector>

using P = rune::codec::PrimType;
using R = rune::view::MergeRule;

namespace {
rune::codec::Descriptor orderSrc() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64)
        .add("amount", P::Int32)
        .add("qty", P::UInt32)
        .add("price", P::Float64)
        .build();
}
rune::view::CompileResult compileOrders() {
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"cnt", P::Int64, 0, "price", R::Count},
                {"total", P::Int64, 0, "amount", R::Sum},
                {"qtySum", P::UInt64, 0, "qty", R::Sum},
                {"avgPrice", P::Float64, 0, "price", R::Avg}};
    static const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    return rune::view::compileView(v, srcs, 9).value();
}
} // namespace

TEST(AggState, ReplaceOnlyPlanIsNotAggregate) {
    rune::view::CompiledViewPlan p;
    p.output = {{0, P::Int64, 0, 1, R::Replace}};
    EXPECT_FALSE(rune::view::isAggregateView(p));
    p.output.push_back({1, P::Int64, 0, rune::view::kNoSourceField, R::CountAll});
    EXPECT_TRUE(rune::view::isAggregateView(p));
}

TEST(AggState, LayoutDerivesKindsInputsAndRowLengths) {
    const auto comp = compileOrders();
    const auto src = orderSrc();
    const auto a = rune::view::buildAggLayout(comp.plan, src);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->keyOutSlot, 0);
    EXPECT_EQ(a->keyType, P::Int64);
    ASSERT_EQ(a->fields.size(), 5u);                       // every non-key column
    EXPECT_EQ(a->fields[0].rule, R::CountAll);
    EXPECT_EQ(a->fields[0].input, rune::view::kNoSourceField);
    EXPECT_EQ(a->fields[1].kind, rune::view::AccKind::None);     // Count(price)
    EXPECT_EQ(a->fields[2].kind, rune::view::AccKind::Signed);   // Sum(Int32)
    EXPECT_EQ(a->fields[3].kind, rune::view::AccKind::Unsigned); // Sum(UInt32)
    EXPECT_EQ(a->fields[4].kind, rune::view::AccKind::Float);    // Avg(Float64)
    EXPECT_EQ(a->numInputs, 4u);                             // all but CountAll
    EXPECT_EQ(a->contribValOff, 8u + 1u);                    // 4 inputs -> 1 bitmap byte
    EXPECT_EQ(a->contributorRow.rowLen, 8u + 1u + 4u * 8u);
    EXPECT_EQ(a->groupNonNullOff, 8u + 5u * 16u);
    EXPECT_EQ(a->groupRow.rowLen, 8u + 5u * 16u + 5u * 8u);
}

TEST(AggState, AuxTablesAcceptRawLayouts) {
    const auto comp = compileOrders();
    const auto a = rune::view::buildAggLayout(comp.plan, orderSrc()).value();
    rune::view::ViewTable contributors(a.contributorRow);
    uint8_t* row = contributors.put(42);
    rune::view::storeU64(row, 7);
    EXPECT_EQ(rune::view::loadU64(contributors.get(42)), 7u);
}

TEST(AggState, Int128AccumulatorRoundTripsBeyondInt64) {
    uint8_t buf[16] = {};
    const __int128 big = static_cast<__int128>(INT64_MAX) * 4;
    rune::view::storeI128(buf, big);
    EXPECT_TRUE(rune::view::loadI128(buf) == big);
    rune::view::storeF64(buf, 2.5);
    EXPECT_DOUBLE_EQ(rune::view::loadF64(buf), 2.5);
}

TEST(AggStateRegression, UnknownSourceFieldYieldsNullopt) {
    auto comp = compileOrders();
    comp.plan.output[3].srcFieldId = 999;   // not in the source descriptor
    EXPECT_FALSE(rune::view::buildAggLayout(comp.plan, orderSrc()).has_value());
}


namespace {
// order v2: orderId key | userId | amount Int64 | price Float64 | sku FixedStr(4) | qty UInt32 | bonus Int32 (v2)
rune::codec::Descriptor statsSrc() {
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
rune::view::CompileResult compileStats() {
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
    static const auto src = statsSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    return rune::view::compileView(v, srcs, 9).value();
}
} // namespace

// REGRESSION (Phase 2b Global Constraint): a view without Min/Max/Last/LastNonNull
// keeps its 2a layout byte-for-byte -- no links, no head, no caches, 8-byte inputs.
TEST(AggStateRegression, TwoAOnlyLayoutIsUnchangedRegression) {
    const auto a = rune::view::buildAggLayout(compileOrders().plan, orderSrc()).value();
    EXPECT_FALSE(a.linked);
    EXPECT_EQ(a.contribNullOff, 8u);
    EXPECT_EQ(a.contribValOff, 9u);
    EXPECT_EQ(a.inputOff, (std::vector<uint32_t>{9, 17, 25, 33}));
    EXPECT_EQ(a.inputWidth, (std::vector<uint16_t>{8, 8, 8, 8}));
    EXPECT_EQ(a.contributorRow.rowLen, 41u);
    EXPECT_EQ(a.numCached, 0u);
    EXPECT_EQ(a.groupRow.rowLen, 8u + 5u * 16u + 5u * 8u);
    for (const auto& f : a.fields) EXPECT_EQ(f.cache, rune::view::kNoCache);
    const auto b = rune::view::aggDirtyBoundPerRecord(a);
    EXPECT_EQ(b.contributorRows, 1u);
    EXPECT_EQ(b.groupRows, 2u);
    EXPECT_EQ(b.viewRows, 2u);
}

TEST(AggState, RetractableLayoutAddsLinksHeadCachesAndPerInputWidths) {
    const auto a = rune::view::buildAggLayout(compileStats().plan, statsSrc()).value();
    EXPECT_TRUE(a.linked);
    ASSERT_EQ(a.fields.size(), 10u);
    EXPECT_EQ(a.numInputs, 9u);
    EXPECT_EQ(a.contribPrevOff, 8u);
    EXPECT_EQ(a.contribNextOff, 16u);
    EXPECT_EQ(a.contribNullOff, 24u);
    EXPECT_EQ(a.contribValOff, 26u);                       // 9 inputs -> 2 bitmap bytes
    EXPECT_EQ(a.inputOff, (std::vector<uint32_t>{26, 34, 42, 50, 58, 66, 70, 74, 78}));
    EXPECT_EQ(a.inputWidth, (std::vector<uint16_t>{8, 8, 8, 8, 8, 4, 4, 4, 8}));
    EXPECT_EQ(a.contributorRow.rowLen, 86u);
    // Caches: maxAmt, minAmt, maxPrice, minQty (Min/Max only, in field order).
    EXPECT_EQ(a.numCached, 4u);
    EXPECT_EQ(a.fields[1].cache, 0u);  EXPECT_EQ(a.fields[1].kind, rune::view::AccKind::Signed);
    EXPECT_EQ(a.fields[2].cache, 1u);
    EXPECT_EQ(a.fields[3].cache, rune::view::kNoCache);   // Last
    EXPECT_EQ(a.fields[3].kind, rune::view::AccKind::None);
    EXPECT_EQ(a.fields[4].cache, 2u);  EXPECT_EQ(a.fields[4].kind, rune::view::AccKind::Float);
    EXPECT_EQ(a.fields[5].cache, 3u);  EXPECT_EQ(a.fields[5].kind, rune::view::AccKind::Unsigned);
    EXPECT_EQ(a.fields[6].srcType, P::FixedStr);
    EXPECT_EQ(a.fields[8].rule, R::LastNonNull);
    EXPECT_EQ(a.fields[9].kind, rune::view::AccKind::Signed);   // Sum(amount)
    EXPECT_EQ(a.groupNonNullOff, 8u + 10u * 16u);
    EXPECT_EQ(a.groupHeadOff, 248u);
    EXPECT_EQ(a.groupCacheOff, 256u);
    EXPECT_EQ(a.groupRow.rowLen, 256u + 4u * rune::view::kCacheEntryBytes);
    const auto b = rune::view::aggDirtyBoundPerRecord(a);
    EXPECT_EQ(b.contributorRows, 4u);
    EXPECT_EQ(b.groupRows, 2u);
    EXPECT_EQ(b.viewRows, 2u);
}

TEST(AggState, SentinelsAndCacheStatesAreStable) {
    EXPECT_EQ(rune::view::kNoContributor, UINT64_MAX);
    EXPECT_EQ(rune::view::kCacheEntryBytes, 9u);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::CacheState::Empty), 0u);   // a zero-filled row is Empty
    EXPECT_EQ(static_cast<uint8_t>(rune::view::CacheState::Valid), 1u);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::CacheState::RescanPending), 2u);
    EXPECT_TRUE(rune::view::needsContributorList(R::LastNonNull));
    EXPECT_FALSE(rune::view::needsContributorList(R::Sum));
}
