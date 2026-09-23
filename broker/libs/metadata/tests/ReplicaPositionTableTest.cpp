#include <rune/metadata/ReplicaPositionTable.h>
#include <gtest/gtest.h>
using namespace rune::metadata;
using rune::replication::ReplicaPositionReport;

TEST(ReplicaPositionTable, ReportsAndReadsBackLatest) {
    ReplicaPositionTable t;
    t.report({7, 2, 5, /*epoch*/3, /*leo*/100});
    t.report({7, 2, 6, 3, 90});
    t.report({7, 2, 5, 4, 130}); // node 5 advances + new epoch
    auto r = t.get(7, 2, 5);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->leaderEpoch, 4);
    EXPECT_EQ(r->logEndPosition, 130);
    EXPECT_EQ(t.forPartition(7, 2).size(), 2u);
}

TEST(ReplicaPositionTable, UnknownReturnsNullopt) {
    ReplicaPositionTable t;
    EXPECT_FALSE(t.get(1, 0, 9).has_value());
}
