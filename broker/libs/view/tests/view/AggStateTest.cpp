// libs/view/tests/view/AggStateTest.cpp — Phase 2a aggregate layout.
#include <gtest/gtest.h>

#include <rune/codec/Descriptor.h>
#include <rune/view/AggState.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewTable.h>

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
