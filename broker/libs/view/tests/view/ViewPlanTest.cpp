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

namespace {
// Overwrites the 4-byte little-endian u32 at `offset` with `value`.
void pokeU32LE(std::vector<uint8_t>& buf, size_t offset, uint32_t value) {
    ASSERT_LE(offset + 4, buf.size());
    buf[offset]     = static_cast<uint8_t>(value & 0xff);
    buf[offset + 1] = static_cast<uint8_t>((value >> 8) & 0xff);
    buf[offset + 2] = static_cast<uint8_t>((value >> 16) & 0xff);
    buf[offset + 3] = static_cast<uint8_t>((value >> 24) & 0xff);
}
} // namespace

// Regression (bounds-safety review finding): a VALID version/header
// followed by a BOGUS LARGE element-count prefix (0xFFFFFFFF) must be
// rejected with std::nullopt -- it must NOT attempt a huge .reserve()
// and crash with std::bad_alloc/std::length_error, and must not read out
// of bounds or hang. Covers every count field on both wire forms.
TEST(ViewPlan, BogusLargeCountRegression) {
    constexpr uint32_t kBogusCount = 0xFFFFFFFFu;

    // --- CompiledViewPlan: keyFieldId count, sourceTopicId count, output count ---
    rune::view::CompiledViewPlan cp;
    cp.viewName = "v"; cp.viewTopicId = 1;
    cp.keyFieldId = {0}; cp.sourceTopicId = {5};
    cp.output = {{0, rune::codec::PrimType::Int64, 0, 0, rune::view::MergeRule::Replace}};
    auto cbytes = rune::view::serialize(cp);

    size_t pos = 0;
    pos += 1;                              // version
    pos += 4 + cp.viewName.size();         // viewName (len + bytes)
    pos += 4;                              // viewTopicId
    const size_t keyCountOffset = pos;
    pos += 4 + cp.keyFieldId.size() * 2;   // keyCount (len + u16 each)
    const size_t srcCountOffset = pos;
    pos += 4 + cp.sourceTopicId.size() * 4; // srcCount (len + i32 each)
    pos += 8;                              // outputFingerprint
    const size_t outCountOffset = pos;

    for (size_t offset : {keyCountOffset, srcCountOffset, outCountOffset}) {
        auto corrupt = cbytes;
        pokeU32LE(corrupt, offset, kBogusCount);
        auto back = rune::view::deserializeCompiledViewPlan(corrupt);
        EXPECT_FALSE(back.has_value()) << "offset=" << offset;
    }
    // Sanity: the uncorrupted buffer still parses (proves the pokes above
    // actually landed on the count fields, not somewhere inert).
    EXPECT_TRUE(rune::view::deserializeCompiledViewPlan(cbytes).has_value());

    // --- ViewPlan (authored): sources count, output count ---
    rune::view::ViewPlan ap;
    ap.viewName = "v"; ap.key = "k";
    ap.sources = {{1, "id"}};
    ap.output = {{"f", rune::codec::PrimType::Int32, 0, "f", rune::view::MergeRule::Replace}};
    auto abytes = rune::view::serializeAuthored(ap);

    size_t apos = 0;
    apos += 1;                       // version
    apos += 4 + ap.viewName.size();  // viewName
    apos += 4 + ap.key.size();       // key
    const size_t srcCountOffsetA = apos;
    // sources: u32 count, then per-source { topicId:i32, keyField:str }
    // (writeI32LE THEN writeString -- see serializeAuthored). That's
    // 4 (count) + 4 (topicId i32) + 4 (keyField len prefix) + keyField
    // bytes = 12 + size, NOT 8 + size (there are two separate 4-byte
    // fields -- topicId and the keyField length prefix -- between the
    // sources count and the keyField bytes, not one).
    apos += 4 + 4 + 4 + ap.sources[0].keyField.size();
    const size_t outCountOffsetA = apos;

    for (size_t offset : {srcCountOffsetA, outCountOffsetA}) {
        auto corrupt = abytes;
        pokeU32LE(corrupt, offset, kBogusCount);
        auto back = rune::view::deserializeAuthored(corrupt);
        EXPECT_FALSE(back.has_value()) << "offset=" << offset;
    }
    EXPECT_TRUE(rune::view::deserializeAuthored(abytes).has_value());
}

// Phase 2a: Count(*) is a new MergeRule value appended at 7 so existing plan
// blobs (rules 0-6) keep their meaning.
TEST(ViewPlanRegression, CountAllRuleRoundTripsAuthoredAndCompiled) {
    rune::view::ViewPlan a;
    a.viewName = "user_totals";
    a.key      = "userId";
    a.sources  = {{5, "userId"}};
    a.output   = {{"userId", rune::codec::PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                  {"n", rune::codec::PrimType::Int64, 0, "", rune::view::MergeRule::CountAll}};
    auto back = rune::view::deserializeAuthored(rune::view::serializeAuthored(a));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->output[1].rule, rune::view::MergeRule::CountAll);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::MergeRule::CountAll), 7u);

    rune::view::CompiledViewPlan c;
    c.viewName = "user_totals"; c.viewTopicId = 9; c.keyFieldId = {1}; c.sourceTopicId = {5};
    c.outputFingerprint = 1;
    c.output = {{0, rune::codec::PrimType::Int64, 0, 1, rune::view::MergeRule::Replace},
                {1, rune::codec::PrimType::Int64, 0, rune::view::kNoSourceField,
                 rune::view::MergeRule::CountAll}};
    auto cback = rune::view::deserializeCompiledViewPlan(rune::view::serialize(c));
    ASSERT_TRUE(cback.has_value());
    EXPECT_EQ(cback->output[1].rule, rune::view::MergeRule::CountAll);
    EXPECT_EQ(cback->output[1].srcFieldId, rune::view::kNoSourceField);
}

// Phase 2b: LastNonNull is appended at 8; rules 0-7 keep their meaning.
TEST(ViewPlanRegression, LastNonNullRuleRoundTripsAtWireValue8) {
    rune::view::ViewPlan a;
    a.viewName = "user_stats"; a.key = "userId"; a.sources = {{5, "userId"}};
    a.output = {{"userId", rune::codec::PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                {"lnn", rune::codec::PrimType::Int32, 0, "bonus", rune::view::MergeRule::LastNonNull}};
    auto back = rune::view::deserializeAuthored(rune::view::serializeAuthored(a));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->output[1].rule, rune::view::MergeRule::LastNonNull);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::MergeRule::LastNonNull), 8u);
    EXPECT_EQ(static_cast<uint8_t>(rune::view::MergeRule::CountAll), 7u);
}

TEST(ViewPlanRegression, UnknownRuleValueIsRejected) {
    rune::view::ViewPlan a;
    a.viewName = "v"; a.key = "k"; a.sources = {{5, "k"}};
    a.output = {{"k", rune::codec::PrimType::Int64, 0, "k", static_cast<rune::view::MergeRule>(9)}};
    EXPECT_FALSE(rune::view::deserializeAuthored(rune::view::serializeAuthored(a)).has_value())
        << "rule byte 9 (one past LastNonNull) must be rejected, never cast";
}
