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
