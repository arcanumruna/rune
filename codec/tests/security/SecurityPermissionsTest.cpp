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

// Namespaces spec §2 (owner ruling): ADMIN ON CLUSTER is root - every operation on every user object and on the
// cluster, data included, with no other grant.  Internal (`__`) objects stay out of reach of any pattern (I6): root
// gets on them only what an explicit CREATE/ALTER/DROP/DESCRIBE ON CLUSTER gets.  A NAMESPACE grant stays explicit
// (namespace-admin membership is a security fact, not an implication).
TEST(SecurityPermissionsRegression, AdminOnClusterIsRootRegression) {
    const std::vector<rs::GrantSpec> grants = {g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")};
    constexpr uint8_t kAll = 0xFF;
    EXPECT_EQ(rs::clusterOps(grants), kAll) << "ADMIN ON CLUSTER holds every cluster op (CREATE TOPIC, DESCRIBE)";
    for (ResourceType t : {ResourceType::Topic, ResourceType::View, ResourceType::Group, ResourceType::TransactionalId})
        EXPECT_EQ(rs::opsOn(grants, t, "x"), kAll) << static_cast<int>(t);
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Cluster, ""), kAll);
    constexpr uint8_t kObjectOps = rs::opBit(Operation::Create) | rs::opBit(Operation::Alter) |
                                   rs::opBit(Operation::Drop) | rs::opBit(Operation::Describe);
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "__catalog"), kObjectOps)
        << "no READ / WRITE / TRUNCATE on an internal topic, the same as an explicit cluster grant";
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Namespace, "a"), 0) << "a NAMESPACE grant is never implied";
    rs::EffectivePerms root; root.nsId = 0; root.grants = grants;
    EXPECT_EQ(rs::opsOn(root, ResourceType::Topic, 7, "t"), kAll) << "root in every namespace (spec §2)";
    rs::EffectivePerms reader; reader.nsId = 0;
    reader.grants = {g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "")};
    EXPECT_EQ(rs::opsOn(reader, ResourceType::Topic, 7, "t"), 0) << "D9: a non-root has nothing on a foreign object";
    // Only ADMIN is root: the other cluster ops keep D16.
    const std::vector<rs::GrantSpec> drop = {g(Operation::Drop, ResourceType::Cluster, PatternKind::Exact, "")};
    EXPECT_EQ(rs::opsOn(drop, ResourceType::Topic, "t"), rs::opBit(Operation::Drop) | rs::opBit(Operation::Describe));
    EXPECT_EQ(rs::clusterOps(drop), rs::opBit(Operation::Drop));
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
