// libs/view/tests/view/PlanCompilerTest.cpp
#include <gtest/gtest.h>
#include <rune/view/PlanCompiler.h>
#include <rune/codec/Descriptor.h>

static rune::codec::Descriptor charSource() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/true)
        .add("hp", rune::codec::PrimType::Int32)
        .add("name", rune::codec::PrimType::String).build();      // var-len
}
static rune::view::ViewPlan hpView() {
    rune::view::ViewPlan v; v.viewName="char_hp"; v.key="characterId";
    v.sources = {{5,"characterId"}};
    v.output = {{"characterId",rune::codec::PrimType::Int64,0,"characterId",rune::view::MergeRule::Replace},
                {"hp",rune::codec::PrimType::Int32,0,"hp",rune::view::MergeRule::Replace}};
    return v;
}

TEST(PlanCompiler, CompilesSingleSourceReplaceView) {
    auto d = charSource(); const rune::codec::Descriptor* srcs[]={&d};
    auto r = rune::view::compileView(hpView(), srcs, 0x40000007);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->plan.keyFieldId.size(), 1u);
    EXPECT_EQ(r->plan.keyFieldId[0], 0);               // characterId tag
    EXPECT_EQ(r->plan.output[1].srcFieldId, 1);        // hp tag
    EXPECT_NE(r->plan.outputFingerprint, 0u);
    EXPECT_EQ(r->rowLayout.rowLen, 1u + 8u + 4u);
}

TEST(PlanCompilerRejectRegression, CompileRejectsUnknownAndVarLenAndMissingKey) {
    auto d = charSource(); const rune::codec::Descriptor* srcs[]={&d};
    { auto v=hpView(); v.output[1].srcField="nope";
      EXPECT_FALSE(rune::view::compileView(v,srcs,1).has_value()); }       // unknown field
    { auto v=hpView(); v.output.push_back({"name",rune::codec::PrimType::String,0,"name",rune::view::MergeRule::Replace});
      EXPECT_FALSE(rune::view::compileView(v,srcs,1).has_value()); }       // var-len column
    { auto v=hpView(); v.key="ghostKey"; v.sources[0].keyField="ghostKey";
      EXPECT_FALSE(rune::view::compileView(v,srcs,1).has_value()); }       // missing key
    { // non-integer key (FixedStr) rejected in Phase 1
      auto d2 = rune::codec::DescriptorBuilder("c2",1)
          .addFixedStr("sid",16,1,true).add("hp",rune::codec::PrimType::Int32).build();
      const rune::codec::Descriptor* s2[]={&d2};
      rune::view::ViewPlan v; v.viewName="x"; v.key="sid"; v.sources={{6,"sid"}};
      v.output={{"sid",rune::codec::PrimType::FixedStr,0,"sid",rune::view::MergeRule::Replace},
                {"hp",rune::codec::PrimType::Int32,0,"hp",rune::view::MergeRule::Replace}};
      EXPECT_FALSE(rune::view::compileView(v,s2,1).has_value()); }
}

// Whole-branch review Bug B: ClientGateway::handleGetViewRow canonicalizes a
// probe key into a FIXED 8-byte buffer and partitions via FNV-1a64 over that
// exact span, but the PRODUCER partitions a narrow (<8-byte) key by its
// NATIVE width (decodeWireKey enforces exact width) -- a narrow multi-
// partition view key would silently route GetViewRow to the wrong partition.
// Phase 1 rejects narrow (Int32/UInt32/UInt16) integer KEY fields at compile
// time; Int64/UInt64 keys (8 bytes, matching the wire canonicalization) still
// compile -- this is the single-source happy path and must not regress.
TEST(PlanCompilerRejectRegression, RejectsNarrowIntegerKey_BugB) {
    { // Int32 key -- narrow, must be REJECTED (RED pre-fix: this compiled).
      auto d = rune::codec::DescriptorBuilder("character32", 1)
                   .add("characterId", rune::codec::PrimType::Int32, 1, /*isKey*/ true)
                   .add("hp", rune::codec::PrimType::Int32)
                   .build();
      const rune::codec::Descriptor* srcs[] = {&d};
      rune::view::ViewPlan v;
      v.viewName = "char_hp32";
      v.key      = "characterId";
      v.sources  = {{5, "characterId"}};
      v.output   = {{"characterId", rune::codec::PrimType::Int32, 0, "characterId",
                    rune::view::MergeRule::Replace},
                   {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
      const auto r = rune::view::compileView(v, srcs, 1);
      EXPECT_FALSE(r.has_value()) << "an Int32 (narrow, 4-byte) KEY field must be rejected";
    }
    { // UInt16 key -- narrow, must be REJECTED.
      auto d = rune::codec::DescriptorBuilder("character16", 1)
                   .add("characterId", rune::codec::PrimType::UInt16, 1, /*isKey*/ true)
                   .add("hp", rune::codec::PrimType::Int32)
                   .build();
      const rune::codec::Descriptor* srcs[] = {&d};
      rune::view::ViewPlan v;
      v.viewName = "char_hp16";
      v.key      = "characterId";
      v.sources  = {{5, "characterId"}};
      v.output   = {{"characterId", rune::codec::PrimType::UInt16, 0, "characterId",
                    rune::view::MergeRule::Replace},
                   {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
      EXPECT_FALSE(rune::view::compileView(v, srcs, 1).has_value())
          << "a UInt16 (narrow, 2-byte) KEY field must be rejected";
    }
    { // Int64 key -- 8 bytes, still compiles (single-source happy path intact).
      auto d = charSource();  // characterId: Int64 key
      const rune::codec::Descriptor* srcs[] = {&d};
      const auto r = rune::view::compileView(hpView(), srcs, 1);
      EXPECT_TRUE(r.has_value()) << "an Int64 (8-byte) KEY field must still compile";
    }
    { // A non-key Int32 OUTPUT column (not the KEY field) is unaffected.
      auto d = charSource();
      const rune::codec::Descriptor* srcs[] = {&d};
      auto v = hpView();  // hp (Int32) is an OUTPUT column, not the key
      const auto r = rune::view::compileView(v, srcs, 1);
      ASSERT_TRUE(r.has_value())
          << "restricting the KEY field must not reject non-key Int32 output columns";
    }
}

// Whole-branch review Bug C: compileView only checked authored.sources
// non-empty, never == 1, but the rest of the Phase 1 pipeline (fold, co-
// location, foldCursor) is single-source only. A 2-source plan must be
// rejected fail-closed rather than silently compiling a corrupt view.
TEST(PlanCompilerRejectRegression, RejectsMultiSourcePlan_BugC) {
    auto d1 = charSource();
    auto d2 = charSource();
    const rune::codec::Descriptor* srcs[] = {&d1, &d2};

    auto v = hpView();
    v.sources.push_back({6, "characterId"});  // a SECOND source -- Phase 1 is single-source
    v.output[0].sourceIdx = 0;
    v.output.push_back({"hp2", rune::codec::PrimType::Int32, 1, "hp", rune::view::MergeRule::Replace});

    const auto r = rune::view::compileView(v, srcs, 1);
    EXPECT_FALSE(r.has_value()) << "a 2-source authored plan must be rejected (RED pre-fix: compiled)";

    // Single-source still compiles (unchanged happy path).
    const rune::codec::Descriptor* single[] = {&d1};
    EXPECT_TRUE(rune::view::compileView(hpView(), single, 1).has_value());
}

namespace {
rune::codec::Descriptor orderSourceForCompile() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("userId", rune::codec::PrimType::Int64)
        .add("amount", rune::codec::PrimType::Int32)
        .add("qty", rune::codec::PrimType::UInt32)
        .add("price", rune::codec::PrimType::Float64)
        .add("note", rune::codec::PrimType::Bool)
        .build();
}
rune::view::ViewPlan aggPlan(std::vector<rune::view::AuthoredField> rest) {
    rune::view::ViewPlan v;
    v.viewName = "user_totals";
    v.key      = "userId";
    v.sources  = {{5, "userId"}};
    v.output   = {{"userId", rune::codec::PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace}};
    for (auto& f : rest) v.output.push_back(std::move(f));
    return v;
}
std::expected<rune::view::CompileResult, rune::view::CompileError> compileAgg(const rune::view::ViewPlan& v) {
    static const auto src = orderSourceForCompile();
    const rune::codec::Descriptor* srcs[] = {&src};
    return rune::view::compileView(v, srcs, 77);
}
using P = rune::codec::PrimType;
using R = rune::view::MergeRule;
} // namespace

TEST(PlanCompilerAggregateRegression, ValidAggregateViewCompilesWithRuleOutputTypes) {
    auto r = compileAgg(aggPlan({{"n", P::Int64, 0, "", R::CountAll},
                                 {"notes", P::Int64, 0, "note", R::Count},
                                 {"total", P::Int64, 0, "amount", R::Sum},
                                 {"qtySum", P::UInt64, 0, "qty", R::Sum},
                                 {"priceSum", P::Float64, 0, "price", R::Sum},
                                 {"avg", P::Float64, 0, "amount", R::Avg}}));
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& out = r->plan.output;
    ASSERT_EQ(out.size(), 7u);
    EXPECT_EQ(out[1].rule, R::CountAll);
    EXPECT_EQ(out[1].srcFieldId, rune::view::kNoSourceField);
    EXPECT_EQ(out[3].type, P::Int64);     // Sum(Int32) -> Int64
    EXPECT_EQ(out[4].type, P::UInt64);    // Sum(UInt32) -> UInt64
    EXPECT_EQ(out[6].type, P::Float64);   // Avg -> Float64
}

TEST(PlanCompilerAggregateRegression, SumDeclaredWithSourceTypeIsRejected) {
    auto r = compileAgg(aggPlan({{"total", P::Int32, 0, "amount", R::Sum}}));
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("produces Int64"), std::string::npos) << r.error().message;
    EXPECT_EQ(r.error().message.find("key"), std::string::npos) << "must not classify as KeyInvalid";
}

TEST(PlanCompilerAggregateRegression, MinMaxLastAreRejectedUntilPhase2b) {
    for (R rule : {R::Min, R::Max, R::Last}) {
        auto r = compileAgg(aggPlan({{"x", P::Int32, 0, "amount", rule}}));
        ASSERT_FALSE(r.has_value());
        EXPECT_NE(r.error().message.find("Phase 2b"), std::string::npos) << r.error().message;
    }
}

TEST(PlanCompilerAggregateRegression, CountAllWithSourceFieldIsRejected) {
    auto r = compileAgg(aggPlan({{"n", P::Int64, 0, "amount", R::CountAll}}));
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("Count(*)"), std::string::npos);
}

TEST(PlanCompilerAggregateRegression, NonGroupReplaceColumnInAggregateViewIsRejected) {
    auto r = compileAgg(aggPlan({{"total", P::Int64, 0, "amount", R::Sum},
                                 {"price", P::Float64, 0, "price", R::Replace}}));
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("must be an aggregate"), std::string::npos);
    EXPECT_EQ(r.error().message.find("key"), std::string::npos);
}

TEST(PlanCompilerAggregateRegression, GroupColumnMustProjectTheGroupByField) {
    auto v = aggPlan({{"total", P::Int64, 0, "amount", R::Sum}});
    v.output[0].srcField = "orderId";   // view key column projects a different field than sources[0].keyField
    auto r = compileAgg(v);
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("group-by"), std::string::npos);
}

TEST(PlanCompilerAggregateRegression, ReplaceOnlyViewIsUnchanged) {
    rune::view::ViewPlan v;
    v.viewName = "orders"; v.key = "orderId"; v.sources = {{5, "orderId"}};
    v.output = {{"orderId", P::Int64, 0, "orderId", R::Replace},
                {"amount", P::Int32, 0, "amount", R::Replace}};
    EXPECT_TRUE(compileAgg(v).has_value());
}
