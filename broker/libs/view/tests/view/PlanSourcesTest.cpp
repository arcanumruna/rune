// PlanSourcesTest.cpp - authentication Task 12 (D19): the source topic ids of a plan blob.
#include <rune/view/PlanSources.h>
#include <rune/view/ViewPlan.h>

#include <gtest/gtest.h>

using namespace rune::view;

TEST(PlanSourcesRegression, ListsEachSourceOnceInOrder) {
    ViewPlan p;
    p.viewName = "v"; p.key = "k";
    p.sources = {{7, "k"}, {3, "k"}, {7, "k"}};
    const auto blob = serializeAuthored(p);
    const auto ids = planSourceTopicIds(blob);
    ASSERT_TRUE(ids.has_value());
    EXPECT_EQ(*ids, (std::vector<int32_t>{7, 3}));
}

TEST(PlanSourcesRegression, AnUndecodableBlobIsNullopt) {
    const std::vector<uint8_t> junk{0xFF, 0x01, 0x02};
    EXPECT_FALSE(planSourceTopicIds(junk).has_value());
    EXPECT_FALSE(planSourceTopicIds({}).has_value());
}
