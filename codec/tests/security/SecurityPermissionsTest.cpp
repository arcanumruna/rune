#include "SecurityScript.h"
#include <gtest/gtest.h>
using rs::Operation; using rs::ResourceType; using rs::PatternKind;

TEST(SecurityPermissionsRegression, ExactPrefixAndEmptyPrefixMatch) {
    EXPECT_TRUE(rs::matches({ResourceType::Topic, PatternKind::Exact, "a.b"}, ResourceType::Topic, "a.b"));
    EXPECT_FALSE(rs::matches({ResourceType::Topic, PatternKind::Exact, "a.b"}, ResourceType::Topic, "a.bc"));
    EXPECT_TRUE(rs::matches({ResourceType::Topic, PatternKind::Prefix, "world."}, ResourceType::Topic, "world.zone1"));
    EXPECT_FALSE(rs::matches({ResourceType::Topic, PatternKind::Prefix, "world."}, ResourceType::Topic, "world"));
    EXPECT_TRUE(rs::matches({ResourceType::Topic, PatternKind::Prefix, ""}, ResourceType::Topic, "anything"));
    EXPECT_FALSE(rs::matches({ResourceType::Topic, PatternKind::Prefix, ""}, ResourceType::View, "anything")) << "types never cross";
}

TEST(SecurityPermissionsRegression, OpsOnCombinesExactPrefixClusterAndImpliedDescribe) {
    const std::vector<rs::GrantSpec> grants = {
        g(Operation::Read,  ResourceType::Topic, PatternKind::Exact, "player_events"),
        g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world."),
        g(Operation::Create, ResourceType::Cluster, PatternKind::Exact, ""),
    };
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "player_events"),
              rs::opBit(Operation::Read) | rs::opBit(Operation::Describe) | rs::opBit(Operation::Create));
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "world.z"),
              rs::opBit(Operation::Write) | rs::opBit(Operation::Describe) | rs::opBit(Operation::Create));
    EXPECT_EQ(rs::opsOn(grants, ResourceType::View, "v"), rs::opBit(Operation::Create) | rs::opBit(Operation::Describe))
        << "CREATE ON CLUSTER covers views too (D16) and implies DESCRIBE (D15)";
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Group, "g"), 0) << "CREATE CLUSTER says nothing about groups";
    EXPECT_EQ(rs::clusterOps(grants), rs::opBit(Operation::Create));
}

TEST(SecurityPermissionsRegression, AdminClusterGivesNoDataAccess) {
    const std::vector<rs::GrantSpec> grants = {g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")};
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "t"), 0) << "D16";
    EXPECT_EQ(rs::clusterOps(grants), rs::opBit(Operation::Admin));
}

TEST(SecurityPermissionsRegression, EffectiveGrantsAreTheUnionOfDirectAndRoles) {
    Script sc;
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(1)}, "u").has_value());
    ASSERT_TRUE(sc.run(rs::CreateRoleOps{}, "R").has_value());
    ASSERT_TRUE(sc.run(rs::CreateRoleOps{}, "S").has_value());
    ASSERT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::User, g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "t1")}, "u").has_value());
    ASSERT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::Role, g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "t2")}, "R").has_value());
    ASSERT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::Role, g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "t1")}, "S").has_value());
    ASSERT_TRUE(sc.run(rs::GrantRoleOps{sc.s.roleByName(rune::catalog::kDefaultNsId, "R")->objectId, "R"}, "u").has_value());
    ASSERT_TRUE(sc.run(rs::GrantRoleOps{sc.s.roleByName(rune::catalog::kDefaultNsId, "S")->objectId, "S"}, "u").has_value());
    const auto eff = rs::effectiveGrants(sc.s, sc.s.userByName(rune::catalog::kDefaultNsId, "u")->objectId);
    const std::vector<rs::GrantSpec> want = {g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "t1"),
                                             g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "t2")};
    EXPECT_EQ(eff, want) << "sorted, unique";
    EXPECT_TRUE(rs::effectiveGrants(sc.s, 9999).empty()) << "an unknown user has no grants";
}

TEST(SecurityPermissionsRegression, ClusterGroupsAndTransactionalIdsOnlyGetDescribe) {
    const std::vector<rs::GrantSpec> grants = {g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, "")};
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Group, "g"), rs::opBit(Operation::Describe));
    EXPECT_EQ(rs::opsOn(grants, ResourceType::TransactionalId, "x"), rs::opBit(Operation::Describe));
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "t"), rs::opBit(Operation::Describe));
}

TEST(SecurityPermissionsRegression, AnyGrantedOperationImpliesDescribeOnThatObjectOnly) {
    const std::vector<rs::GrantSpec> grants = {g(Operation::Write, ResourceType::TransactionalId, PatternKind::Exact, "x1")};
    EXPECT_EQ(rs::opsOn(grants, ResourceType::TransactionalId, "x1"), rs::opBit(Operation::Write) | rs::opBit(Operation::Describe));
    EXPECT_EQ(rs::opsOn(grants, ResourceType::TransactionalId, "x2"), 0);
    EXPECT_EQ(rs::opsOn({}, ResourceType::Topic, "t"), 0) << "default deny";
}

TEST(SecurityPermissionsRegression, HasSecurityAdminCountsEffectiveAdminViaARoleAndIgnoresDisabledUsers) {
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "u"); sc.run(rs::CreateRoleOps{}, "ops");
    EXPECT_FALSE(sc.s.hasSecurityAdmin());
    sc.run(rs::GrantOps{rs::GranteeKind::Role, g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")}, "ops");
    EXPECT_FALSE(sc.s.hasSecurityAdmin()) << "nobody is in the role yet";
    sc.run(rs::GrantRoleOps{sc.s.roleByName(rune::catalog::kDefaultNsId, "ops")->objectId, "ops"}, "u");
    EXPECT_TRUE(sc.s.hasSecurityAdmin());
    sc.run(rs::AlterUserOps{rs::DisableUser{}}, "u");
    EXPECT_FALSE(sc.s.hasSecurityAdmin());
}

TEST(SecurityPermissionsRegression, NoPatternEverMatchesAnInternalObjectRegression) {
    for (const char* internal : {"__security", "__consumer_offsets", "__catalog", "__"}) {
        for (auto type : {ResourceType::Topic, ResourceType::View, ResourceType::Group, ResourceType::TransactionalId}) {
            EXPECT_FALSE(rs::matches({type, PatternKind::Prefix, ""}, type, internal)) << internal;
            EXPECT_FALSE(rs::matches({type, PatternKind::Prefix, "_"}, type, internal)) << internal;
            EXPECT_FALSE(rs::matches({type, PatternKind::Exact, internal}, type, internal)) << internal;
        }
        const std::vector<rs::GrantSpec> all = {g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, ""),
                                                g(Operation::Create, ResourceType::Cluster, PatternKind::Exact, "")};
        EXPECT_EQ(rs::opsOn(all, ResourceType::Topic, internal) & (rs::opBit(Operation::Read)), 0) << internal;
    }
    EXPECT_TRUE(rs::matches({ResourceType::Topic, PatternKind::Prefix, ""}, ResourceType::Topic, "_single")) << "one underscore is an ordinary name";
    EXPECT_TRUE(rs::matches({ResourceType::Cluster, PatternKind::Exact, ""}, ResourceType::Cluster, ""));
}
