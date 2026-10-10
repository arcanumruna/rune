// libs/view/tests/view/ViewPlanTest.cpp
#include <gtest/gtest.h>
#include <rune/view/ViewPlan.h>

using rune::codec::PrimType;

TEST(ViewPlan, CompiledRoundTrip) {
    rune::view::CompiledViewPlan p;
    p.viewName = "char_hp"; p.viewTopicId = 0x40000007; p.persistence = 0;
    p.keyFieldId = {0}; p.sourceTopicId = {5}; p.outputFingerprint = 0xDEADBEEFULL;
    p.output = {{0, rune::codec::PrimType::Int64, 0, 0, rune::view::MergeRule::Replace},
                {1, rune::codec::PrimType::Int32, 0, 3, rune::view::MergeRule::Replace}};
    auto bytes = rune::view::serialize(p);
    auto back = rune::view::deserializeCompiledViewPlan(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->viewName, "char_hp");
    EXPECT_EQ(back->viewTopicId, 0x40000007);
    EXPECT_EQ(back->persistence, 0);
    EXPECT_EQ(back->keyFieldId.size(), 1u);
    EXPECT_EQ(back->keyFieldId[0], 0u);
    EXPECT_EQ(back->sourceTopicId.size(), 1u);
    EXPECT_EQ(back->sourceTopicId[0], 5);
    EXPECT_EQ(back->output.size(), 2u);
    EXPECT_EQ(back->output[0].outSlot, 0u);
    EXPECT_EQ(back->output[0].type, PrimType::Int64);
    EXPECT_EQ(back->output[0].sourceIdx, 0u);
    EXPECT_EQ(back->output[0].srcFieldId, 0u);
    EXPECT_EQ(back->output[0].rule, rune::view::MergeRule::Replace);
    EXPECT_EQ(back->output[1].srcFieldId, 3);
    EXPECT_EQ(back->outputFingerprint, 0xDEADBEEFULL);
}

TEST(ViewPlan, SizingKnobsRoundTrip) {
    rune::view::CompiledViewPlan p; p.viewName="v"; p.viewTopicId=1;
    p.keyFieldId={0}; p.sourceTopicId={5}; p.output={{0,rune::codec::PrimType::Int64,0,0,rune::view::MergeRule::Replace}};
    p.sizing.chunkBytes = 8u<<20; p.sizing.maxRows = 1000000;
    auto back = rune::view::deserializeCompiledViewPlan(rune::view::serialize(p));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->sizing.chunkBytes, 8u<<20);
    EXPECT_EQ(back->sizing.maxRows, 1000000u);
    EXPECT_EQ(back->sizing.capacity, 0u);
}

// Multi-source plan: keyFieldId/sourceTopicId are per-source vectors, must
// not be confused with a single scalar key.
TEST(ViewPlan, CompiledMultiSourceRoundTrip) {
    rune::view::CompiledViewPlan p;
    p.viewName = "joined"; p.viewTopicId = 7;
    p.keyFieldId = {2, 1, 9};
    p.sourceTopicId = {10, 20, 30};
    p.output = {{0, rune::codec::PrimType::Float64, 2, 5, rune::view::MergeRule::Avg}};
    p.sizing.capacity = 4096;
    auto back = rune::view::deserializeCompiledViewPlan(rune::view::serialize(p));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->keyFieldId, (std::vector<uint16_t>{2, 1, 9}));
    EXPECT_EQ(back->sourceTopicId, (std::vector<int32_t>{10, 20, 30}));
    EXPECT_EQ(back->sizing.capacity, 4096u);
    EXPECT_EQ(back->output[0].rule, rune::view::MergeRule::Avg);
}

TEST(ViewPlan, AuthoredRoundTrip) {
    rune::view::ViewPlan p;
    p.viewName = "char_hp";
    p.key = "charId";
    p.sources = {{5, "id"}, {6, "ownerId"}};
    p.output = {{"hp", PrimType::Int64, 0, "hp", rune::view::MergeRule::Replace},
                {"kills", PrimType::Int32, 1, "kills", rune::view::MergeRule::Sum}};
    p.persistence = 1;
    p.sizing.chunkBytes = 4u << 20;
    p.sizing.capacity = 128;
    p.sizing.maxRows = 42;

    auto bytes = rune::view::serializeAuthored(p);
    auto back = rune::view::deserializeAuthored(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->viewName, "char_hp");
    EXPECT_EQ(back->key, "charId");
    ASSERT_EQ(back->sources.size(), 2u);
    EXPECT_EQ(back->sources[0].topicId, 5);
    EXPECT_EQ(back->sources[0].keyField, "id");
    EXPECT_EQ(back->sources[1].topicId, 6);
    EXPECT_EQ(back->sources[1].keyField, "ownerId");
    ASSERT_EQ(back->output.size(), 2u);
    EXPECT_EQ(back->output[0].name, "hp");
    EXPECT_EQ(back->output[0].type, PrimType::Int64);
    EXPECT_EQ(back->output[0].sourceIdx, 0u);
    EXPECT_EQ(back->output[0].srcField, "hp");
    EXPECT_EQ(back->output[0].rule, rune::view::MergeRule::Replace);
    EXPECT_EQ(back->output[1].name, "kills");
    EXPECT_EQ(back->output[1].rule, rune::view::MergeRule::Sum);
    EXPECT_EQ(back->persistence, 1);
    EXPECT_EQ(back->sizing.chunkBytes, 4u << 20);
    EXPECT_EQ(back->sizing.capacity, 128u);
    EXPECT_EQ(back->sizing.maxRows, 42u);
}

TEST(ViewPlan, TruncatedBufferReturnsNullopt) {
    rune::view::CompiledViewPlan p;
    p.viewName = "char_hp"; p.viewTopicId = 1;
    p.keyFieldId = {0}; p.sourceTopicId = {5};
    p.output = {{0, rune::codec::PrimType::Int64, 0, 0, rune::view::MergeRule::Replace}};
    auto bytes = rune::view::serialize(p);

    // Truncate at every prefix length: none should read out of bounds, and
    // any strictly-shorter buffer must fail to parse.
    for (size_t n = 0; n < bytes.size(); ++n) {
        auto back = rune::view::deserializeCompiledViewPlan(
            std::span<const uint8_t>(bytes.data(), n));
        EXPECT_FALSE(back.has_value()) << "n=" << n;
    }
    // Full buffer still parses.
    EXPECT_TRUE(rune::view::deserializeCompiledViewPlan(bytes).has_value());

    // Bad version byte.
    auto corrupt = bytes;
    corrupt[0] = 0xFF;
    EXPECT_FALSE(rune::view::deserializeCompiledViewPlan(corrupt).has_value());

    // Empty buffer.
    EXPECT_FALSE(rune::view::deserializeCompiledViewPlan(
        std::span<const uint8_t>()).has_value());

    // Authored form: truncation must also nullopt, not just the compiled one.
    rune::view::ViewPlan ap;
    ap.viewName = "v"; ap.key = "k";
    ap.sources = {{1, "id"}};
    ap.output = {{"f", PrimType::Int32, 0, "f", rune::view::MergeRule::Replace}};
    auto abytes = rune::view::serializeAuthored(ap);
    for (size_t n = 0; n < abytes.size(); ++n) {
        auto back = rune::view::deserializeAuthored(
            std::span<const uint8_t>(abytes.data(), n));
        EXPECT_FALSE(back.has_value()) << "n=" << n;
    }
    EXPECT_TRUE(rune::view::deserializeAuthored(abytes).has_value());
}
