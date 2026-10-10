#include <gtest/gtest.h>
#include "rune/node/ViewRegistry.h"
#include "rune/node/SchemaRegistry.h"
#include <rune/view/ViewPlan.h>
#include <array>

using namespace rune::node;

namespace {

// A minimal, valid CompiledViewPlan — enough to round-trip through
// rune::view::serialize/deserializeCompiledViewPlan so compiledPlanBlob is a
// realistic Task-8 blob rather than arbitrary bytes.
std::vector<uint8_t> makeCompiledPlanBlob(int32_t viewTopicId, const char* name) {
    rune::view::CompiledViewPlan plan;
    plan.viewName = name;
    plan.viewTopicId = viewTopicId;
    plan.keyFieldId = {0};
    plan.sourceTopicId = {0x40000000};
    plan.outputFingerprint = 0xDEADBEEF;
    plan.persistence = 0;
    return rune::view::serialize(plan);
}

} // namespace

TEST(ViewRegistry, AppendLookupAndRebuild) {
    ViewRegistry r;
    ViewRecord v{0x40000007, "char_hp", makeCompiledPlanBlob(0x40000007, "char_hp"), 0};
    r.applyRecord(v);
    ASSERT_NE(r.byName("char_hp"), nullptr);
    EXPECT_EQ(r.byName("char_hp")->viewTopicId, 0x40000007);
    ASSERT_NE(r.byViewTopicId(0x40000007), nullptr);
    EXPECT_EQ(r.byViewTopicId(0x40000007)->viewName, "char_hp");

    // round-trip a rebuild from serialized records
    auto blob = v.serialize();
    auto back = ViewRecord::deserialize(blob);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->viewTopicId, v.viewTopicId);
    EXPECT_EQ(back->viewName, v.viewName);
    EXPECT_EQ(back->compiledPlanBlob, v.compiledPlanBlob);
    EXPECT_EQ(back->persistence, v.persistence);

    ViewRegistry r2;
    r2.rebuildFromRecords(std::array{*back});
    EXPECT_NE(r2.byViewTopicId(0x40000007), nullptr);
    EXPECT_NE(r2.byName("char_hp"), nullptr);

    EXPECT_EQ(r2.all().size(), size_t{1});
}

TEST(ViewRegistry, UnknownNameAndTopicIdReturnNull) {
    ViewRegistry r;
    EXPECT_EQ(r.byName("nope"), nullptr);
    EXPECT_EQ(r.byViewTopicId(12345), nullptr);
}

// Stable-pointer requirement (mirrors SchemaRegistry's deque rationale):
// a pointer returned by byName/byViewTopicId must stay valid across a later
// applyRecord of a DIFFERENT view (a std::vector-backed registry could
// reallocate and invalidate it).
TEST(ViewRegistry, PointersStableAcrossLaterApplyRecord) {
    ViewRegistry r;
    r.applyRecord(ViewRecord{0x40000007, "char_hp", makeCompiledPlanBlob(0x40000007, "char_hp"), 0});
    const ViewRecord* first = r.byName("char_hp");
    ASSERT_NE(first, nullptr);
    const int32_t capturedId = first->viewTopicId;

    // Apply many more records to force any vector-backed container to grow/reallocate.
    for (int32_t i = 0; i < 64; ++i) {
        r.applyRecord(ViewRecord{0x40001000 + i, "other_" + std::to_string(i),
                                  makeCompiledPlanBlob(0x40001000 + i, "other"), 0});
    }

    EXPECT_EQ(first->viewTopicId, capturedId);
    EXPECT_EQ(first->viewName, "char_hp");
}

TEST(ViewRegistry, RebuildClearsPriorState) {
    ViewRegistry r;
    r.applyRecord(ViewRecord{0x40000007, "char_hp", makeCompiledPlanBlob(0x40000007, "char_hp"), 0});
    ASSERT_NE(r.byName("char_hp"), nullptr);

    std::vector<ViewRecord> fresh{
        ViewRecord{0x40000009, "pos", makeCompiledPlanBlob(0x40000009, "pos"), 1}};
    r.rebuildFromRecords(fresh);

    EXPECT_EQ(r.byName("char_hp"), nullptr);
    EXPECT_EQ(r.byViewTopicId(0x40000007), nullptr);
    ASSERT_NE(r.byName("pos"), nullptr);
    EXPECT_EQ(r.byName("pos")->persistence, uint8_t{1});
    EXPECT_EQ(r.all().size(), size_t{1});
}

// Task 8 lesson: a truncated/malformed buffer must return nullopt, never UB
// and never an unvalidated huge allocation from a bogus length prefix.
TEST(ViewRegistryRegression, DeserializeTruncatedBufferReturnsNullopt) {
    ViewRecord v{0x40000007, "char_hp", makeCompiledPlanBlob(0x40000007, "char_hp"), 0};
    auto full = v.serialize();

    // Progressively truncate — every prefix shorter than the full record
    // (down to and including empty) must be rejected.
    for (size_t len = 0; len < full.size(); ++len) {
        std::span<const uint8_t> trunc(full.data(), len);
        EXPECT_FALSE(ViewRecord::deserialize(trunc).has_value())
            << "unexpectedly accepted truncated buffer of length " << len;
    }
}

TEST(ViewRegistryRegression, DeserializeBogusLengthPrefixReturnsNullopt) {
    ViewRecord v{0x40000007, "char_hp", makeCompiledPlanBlob(0x40000007, "char_hp"), 0};
    auto buf = v.serialize();

    // Corrupt the compiledPlanBlob length prefix (u32, right after the
    // viewTopicId:i32 + viewNameLen:u16 + viewName bytes) to an enormous bogus
    // value. deserialize() must reject it (bounds-check against actual buffer
    // size) rather than attempting to reserve/read gigabytes.
    size_t pos = 4; // past viewTopicId
    uint16_t nameLen = static_cast<uint16_t>(buf[pos]) | (static_cast<uint16_t>(buf[pos + 1]) << 8);
    pos += 2 + nameLen; // past viewName bytes
    ASSERT_LE(pos + 4, buf.size());
    buf[pos + 0] = 0xFF;
    buf[pos + 1] = 0xFF;
    buf[pos + 2] = 0xFF;
    buf[pos + 3] = 0x7F; // huge but positive as u32

    EXPECT_FALSE(ViewRecord::deserialize(buf).has_value());
}
