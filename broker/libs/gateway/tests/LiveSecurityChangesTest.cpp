// LiveSecurityChangesTest.cpp - authentication Task 14: what a permission refresh ends for one session (pure).
#include <rune/gateway/LiveSecurityChanges.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace rg = rune::gateway;
namespace rs = rune::security;
using rs::Operation; using rs::ResourceType; using rs::PatternKind;

namespace {
rs::GrantSpec g(Operation o, ResourceType t, PatternKind k, std::string n) { return {o, {t, k, std::move(n)}}; }

struct Rig {
    rg::ObjectIndex idx;
    rg::Session session;
    Rig() {
        idx.onTopic(0, "w.events"); idx.onTopic(1, "w.chat"); idx.onTopic(2, "other");
        add(100, 0, 7, 1000); add(101, 1, 8, 1001);   // two live subscriptions on w.events / w.chat
    }
    void add(int64_t corr, int32_t topic, int64_t subId, int64_t) {
        session.subIdByKey[rg::Session::SubKey{corr, topic, 0}] = subId;
    }
    rg::PermissionTable table(std::vector<rs::GrantSpec> grants) const { return rg::PermissionTable::compile(grants, idx); }
};
}  // namespace

TEST(LiveSecurityChangesRegression, LostReadEndsThatSubscriptionOnly) {
    Rig r;
    const auto before = r.table({g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "w.")});
    const auto after  = r.table({g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "w.chat")});
    const rg::LiveActions a = rg::refreshActions(before, after, r.session, r.idx);
    ASSERT_EQ(a.ends.size(), 1u);
    EXPECT_EQ(a.ends[0].topicId, 0);
    EXPECT_EQ(a.ends[0].correlationId, 100);
    EXPECT_EQ(a.ends[0].subscriptionId, 7);
    EXPECT_EQ(a.ends[0].code, 45);
    EXPECT_FALSE(a.endCatalogReads);
    EXPECT_FALSE(a.endSecurityReads);
    EXPECT_FALSE(a.endGroups);
}

TEST(LiveSecurityChangesRegression, LostEveryReadEndsEverySubscriptionInIdOrder) {
    Rig r;
    const auto before = r.table({g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "w.")});
    const rg::LiveActions a = rg::refreshActions(before, r.table({}), r.session, r.idx);
    ASSERT_EQ(a.ends.size(), 2u);
    EXPECT_EQ(a.ends[0].subscriptionId, 7);
    EXPECT_EQ(a.ends[1].subscriptionId, 8);
}

TEST(LiveSecurityChangesRegression, LostDescribeClusterEndsCatalogReads) {
    Rig r;
    r.session.catalogReadCorrs = {5};
    r.session.securityReadCorrs = {6};
    const auto before = r.table({g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, ""),
                                 g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")});
    const auto after  = r.table({g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")});
    const rg::LiveActions a = rg::refreshActions(before, after, r.session, r.idx);
    EXPECT_TRUE(a.endCatalogReads);
    EXPECT_FALSE(a.endSecurityReads) << "the security reads need ADMIN, which is kept";
}

TEST(LiveSecurityChangesRegression, LostAdminEndsSecurityReads) {
    Rig r;
    r.session.catalogReadCorrs = {5};
    r.session.securityReadCorrs = {6};
    const auto before = r.table({g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, ""),
                                 g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")});
    const auto after  = r.table({g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, "")});
    const rg::LiveActions a = rg::refreshActions(before, after, r.session, r.idx);
    EXPECT_TRUE(a.endSecurityReads);
    EXPECT_FALSE(a.endCatalogReads);
}

TEST(LiveSecurityChangesRegression, NoAdminLogReadsMeansNothingToEnd) {
    Rig r;   // no catalog / security subscription recorded
    const auto before = r.table({g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")});
    const rg::LiveActions a = rg::refreshActions(before, r.table({}), r.session, r.idx);
    EXPECT_FALSE(a.endCatalogReads);
    EXPECT_FALSE(a.endSecurityReads);
}

TEST(LiveSecurityChangesRegression, LostGroupReadEndsTheGroup) {
    Rig r;
    const auto before = r.table({g(Operation::Read, ResourceType::Group, PatternKind::Exact, "cg")});
    const auto after  = r.table({});
    EXPECT_TRUE(rg::refreshActions(before, after, r.session, r.idx).endGroups);
    const auto kept   = r.table({g(Operation::Read, ResourceType::Group, PatternKind::Exact, "cg")});
    EXPECT_FALSE(rg::refreshActions(before, kept, r.session, r.idx).endGroups) << "an unchanged grant ends nothing";
}

TEST(LiveSecurityChangesRegression, GainedGrantsEndNothing) {
    Rig r;
    r.session.catalogReadCorrs = {5};
    const auto before = r.table({g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "w.")});
    const auto after  = r.table({g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "w."),
                                 g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "w."),
                                 g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, ""),
                                 g(Operation::Read, ResourceType::Group, PatternKind::Exact, "cg")});
    EXPECT_TRUE(rg::refreshActions(before, after, r.session, r.idx).empty());
}

TEST(LiveSecurityChangesRegression, ASubscriptionOfADroppedObjectIsLeftToTheDropPath) {
    Rig r;
    const auto before = r.table({g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "w.")});
    r.idx.onDropped(0);
    const rg::LiveActions a = rg::refreshActions(before, r.table({}), r.session, r.idx);
    ASSERT_EQ(a.ends.size(), 1u);
    EXPECT_EQ(a.ends[0].topicId, 1) << "topic 0 no longer exists: its subscription is not this path's";
}
