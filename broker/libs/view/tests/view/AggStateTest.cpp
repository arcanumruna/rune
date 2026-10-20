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
    EXPECT_EQ(b.blockRows, 0u);
    EXPECT_EQ(a.numBlockCols, 0u);
    EXPECT_EQ(a.blockRow.rowLen, 0u) << "2a views have no value block";
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
    EXPECT_EQ(a.contribBlockPosOff, 86u);                  // [blockPos u32] after the values
    EXPECT_EQ(a.contributorRow.rowLen, 90u);
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
    EXPECT_EQ(a.groupBlockOff, 256u + 4u * rune::view::kCacheEntryBytes);   // 292: after the caches
    EXPECT_EQ(a.groupRow.rowLen, 300u);                                      // + [blockId u32][blockChunks u32]
    // Value block: one column per DISTINCT Min/Max source field, in cache order:
    // amount (maxAmt + minAmt share it), price, qty.
    EXPECT_EQ(a.numBlockCols, 3u);
    EXPECT_EQ(a.blockColInput, (std::vector<uint16_t>{0, 3, 4}));
    EXPECT_EQ(a.cacheCol, (std::vector<uint16_t>{0, 0, 1, 2}));
    EXPECT_EQ(a.blockRow.rowLen, rune::view::kBlockEntries * (8u + 9u * 3u));   // 1120
    const auto b = rune::view::aggDirtyBoundPerRecord(a);
    EXPECT_EQ(b.contributorRows, 5u);
    EXPECT_EQ(b.groupRows, 2u);
    EXPECT_EQ(b.viewRows, 2u);
    EXPECT_EQ(b.blockRows, 3u);
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

// REGRESSION (Task 5b): a linked view with only Last/LastNonNull keeps Task 4's
// linked layout exactly -- no value block, no position, no block fields, bound {4,2,2,0}.
TEST(AggStateRegression, LastOnlyLinkedViewHasNoValueBlockRegression) {
    rune::view::ViewPlan v;
    v.viewName = "t"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"last", P::Int64, 0, "amount", R::Last}};
    static const auto src = statsSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    const auto comp = rune::view::compileView(v, srcs, 9).value();
    const auto a = rune::view::buildAggLayout(comp.plan, src).value();
    EXPECT_TRUE(a.linked);
    EXPECT_EQ(a.numCached, 0u);
    EXPECT_EQ(a.numBlockCols, 0u);
    EXPECT_EQ(a.contributorRow.rowLen, 33u);   // gk 8 + prev 8 + next 8 + bitmap 1 + amount 8
    EXPECT_EQ(a.groupRow.rowLen, 64u);         // 8 + 2x16 + 2x8 + head 8
    EXPECT_EQ(a.blockRow.rowLen, 0u);
    const auto b = rune::view::aggDirtyBoundPerRecord(a);
    EXPECT_EQ(b.contributorRows, 4u);
    EXPECT_EQ(b.groupRows, 2u);
    EXPECT_EQ(b.viewRows, 2u);
    EXPECT_EQ(b.blockRows, 0u);
}

// The value-block chunk format is checkpointed: pin its constants and offsets.
TEST(AggState, ValueBlockFormatIsStable) {
    namespace v = rune::view;
    EXPECT_EQ(v::kBlockEntries, 32u);
    EXPECT_EQ(1u << v::kBlockShift, v::kBlockEntries);
    EXPECT_EQ(v::kBlockSpareFreeAt, 16u);
    EXPECT_EQ(v::kNoBlock, 0u);
    EXPECT_EQ(v::kAggTableBlocks, 3u);
    EXPECT_EQ(v::blockChunkKey(1, 0), uint64_t{1} << 32);
    EXPECT_EQ(v::blockChunkKey(0xFFFFFFFFu, 7), 0xFFFFFFFF00000007ull);
    EXPECT_EQ(v::blockKeyOff(31), 248u);
    EXPECT_EQ(v::blockValOff(0, 0), 256u);
    EXPECT_EQ(v::blockValOff(1, 3), 512u + 24u);
    const auto a = rune::view::buildAggLayout(compileStats().plan, statsSrc()).value();   // 3 columns
    EXPECT_EQ(v::blockNullOff(a, 0, 0), 8u * 32u * 4u);        // after keys + 3 value columns
    EXPECT_EQ(v::blockNullOff(a, 2, 31), 1024u + 64u + 31u);
    EXPECT_EQ(v::blockNullOff(a, 2, 31) + 1, a.blockRow.rowLen);
    for (uint64_t g = 0; g < 100000; ++g) ASSERT_NE(v::blockIdCandidate(g), v::kNoBlock) << g;
    EXPECT_EQ(v::blockIdCandidate(12345), v::blockIdCandidate(12345));
}
