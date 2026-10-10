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
