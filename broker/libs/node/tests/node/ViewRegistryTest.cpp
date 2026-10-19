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

TEST(ViewRegistry, AppendAndLookup) {
    ViewRegistry r;
    ViewRecord v{0x40000007, "char_hp", makeCompiledPlanBlob(0x40000007, "char_hp"), 0};
    r.applyRecord(v);
    ASSERT_NE(r.byName("char_hp"), nullptr);
    EXPECT_EQ(r.byName("char_hp")->viewTopicId, 0x40000007);
    ASSERT_NE(r.byViewTopicId(0x40000007), nullptr);
    EXPECT_EQ(r.byViewTopicId(0x40000007)->viewName, "char_hp");

    EXPECT_EQ(r.all().size(), size_t{1});
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

// Regression: re-applying a record for the SAME viewTopicId under a DIFFERENT
// viewName must erase the OLD name's nameToId_ mapping — otherwise byName on
// the stale name would keep resolving to the renamed slot forever.
TEST(ViewRegistryRegression, ApplyRecordRenameErasesStaleNameMapping) {
    ViewRegistry r;
    r.applyRecord(ViewRecord{1, "a", makeCompiledPlanBlob(1, "a"), 0});
    ASSERT_NE(r.byName("a"), nullptr);

    r.applyRecord(ViewRecord{1, "b", makeCompiledPlanBlob(1, "b"), 0});

    EXPECT_EQ(r.byName("a"), nullptr);
    ASSERT_NE(r.byName("b"), nullptr);
    EXPECT_EQ(r.byName("b")->viewTopicId, 1);
    ASSERT_NE(r.byViewTopicId(1), nullptr);
    EXPECT_EQ(r.byViewTopicId(1)->viewName, "b");
    EXPECT_EQ(r.all().size(), size_t{1});
}
