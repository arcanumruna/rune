// PermissionTableTest.cpp - authentication Task 12: the per-session permission table (compile, created-after-login,
// write bits) and the Authorizer (the one place a code or a detail text is chosen).
#include <rune/gateway/Authorizer.h>
#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace rg = rune::gateway;
namespace rs = rune::security;
using rs::Operation; using rs::ResourceType; using rs::PatternKind;

namespace {
rs::GrantSpec g(Operation o, ResourceType t, PatternKind k, std::string n) { return {o, {t, k, std::move(n)}}; }
}

TEST(PermissionTableRegression, CompilesExactPrefixClusterAndImpliedDescribe) {
    rg::ObjectIndex idx; idx.onTopic(0, "player_events"); idx.onTopic(1, "world.zone1"); idx.onTopic(2, "secret");
    idx.onView(3, "leaderboard");
    const std::vector<rs::GrantSpec> grants = {g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "player_events"),
                                               g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world."),
                                               g(Operation::Read, ResourceType::View, PatternKind::Exact, "leaderboard")};
    auto t = rg::PermissionTable::compile(grants, idx);
    EXPECT_TRUE(t.allows(0, rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allows(0, rs::opBit(Operation::Describe)));
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Write)));
    EXPECT_TRUE(t.allows(1, rs::opBit(Operation::Write)));
    EXPECT_FALSE(t.allows(2, rs::opBit(Operation::Describe))) << "default deny";
    EXPECT_TRUE(t.allows(3, rs::opBit(Operation::Read)));
    EXPECT_FALSE(t.allows(4, rs::opBit(Operation::Read))) << "an id beyond the table";
    EXPECT_FALSE(t.allows(-1, rs::opBit(Operation::Read))) << "a negative (internal) id";
}

TEST(PermissionTableRegression, AnObjectCreatedAfterLoginIsCoveredByAMatchingPrefix) {
    rg::ObjectIndex idx; idx.onTopic(0, "a");
    auto t = rg::PermissionTable::compile({{g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world.")}}, idx);
    EXPECT_FALSE(t.allows(7, rs::opBit(Operation::Write)));
    t.onObjectCreated(7, {"world.zone9", ResourceType::Topic});
    EXPECT_TRUE(t.allows(7, rs::opBit(Operation::Write)));
    t.onObjectCreated(8, {"other", ResourceType::Topic});
    EXPECT_FALSE(t.allows(8, rs::opBit(Operation::Write)));
    t.onObjectDropped(7);
    EXPECT_FALSE(t.allows(7, rs::opBit(Operation::Write))) << "a dropped id never keeps its bits";
    EXPECT_EQ(t.writeBits().size(), 1u);
    EXPECT_EQ(t.writeBits()[0] & (uint64_t{1} << 7), 0u) << "the write bit is cleared with the ops";
}

TEST(PermissionTableRegression, WriteBitsMirrorTheWriteOps) {
    rg::ObjectIndex idx; for (int i = 0; i < 130; ++i) idx.onTopic(i, "t" + std::to_string(i));
    auto t = rg::PermissionTable::compile({{g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "t129")}}, idx);
    const auto bits = t.writeBits();
    ASSERT_EQ(bits.size(), 3u);
    EXPECT_EQ(bits[2], uint64_t{1} << (129 - 128));
    EXPECT_EQ(bits[0], 0u);
}

TEST(PermissionTableRegression, AllowAllAllowsEverythingIncludingCluster) {
    auto t = rg::PermissionTable::allowAll();
    EXPECT_TRUE(t.allows(12345, rs::opBit(Operation::Write)));
    EXPECT_TRUE(t.allows(-9, rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allowsCluster(rs::opBit(Operation::Admin)));
    EXPECT_TRUE(t.allowsName(ResourceType::Group, "any", rs::opBit(Operation::Read)));
}

TEST(PermissionTableRegression, DenyAllDeniesEverything) {
    auto t = rg::PermissionTable::denyAll();
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Describe)));
    EXPECT_FALSE(t.allowsCluster(rs::opBit(Operation::Describe)));
    EXPECT_FALSE(t.allowsName(ResourceType::Topic, "x", rs::opBit(Operation::Describe)));
}

TEST(PermissionTableRegression, ClusterGrantsReachEveryTopicViewButNeverDataOrInternalNames) {
    rg::ObjectIndex idx; idx.onTopic(0, "a"); idx.onView(1, "v");
    auto t = rg::PermissionTable::compile({{g(Operation::Drop, ResourceType::Cluster, PatternKind::Exact, ""),
                                            g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")}}, idx);
    EXPECT_TRUE(t.allows(0, rs::opBit(Operation::Drop)));
    EXPECT_TRUE(t.allows(1, rs::opBit(Operation::Drop)));
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Read))) << "ADMIN/DROP on CLUSTER imply no data access";
    EXPECT_TRUE(t.allowsCluster(rs::opBit(Operation::Admin)));
    EXPECT_FALSE(t.allowsCluster(rs::opBit(Operation::Create)));
    t.onObjectCreated(5, {"later", ResourceType::Topic});
    EXPECT_TRUE(t.allows(5, rs::opBit(Operation::Drop))) << "a cluster grant covers an object created after login";
}

TEST(PermissionTableRegression, APrefixNeverMatchesAnInternalName) {   // I6
    rg::ObjectIndex idx; idx.onTopic(0, "__catalog_like");
    auto t = rg::PermissionTable::compile({{g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "")}}, idx);
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Read)));
    EXPECT_FALSE(t.allowsName(ResourceType::Topic, "__x", rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allowsName(ResourceType::Topic, "x", rs::opBit(Operation::Read)));
}

TEST(PermissionTableRegression, NamePatternsCoverGroupsAndTransactionalIds) {
    auto t = rg::PermissionTable::compile({{g(Operation::Read, ResourceType::Group, PatternKind::Prefix, "cg-"),
                                            g(Operation::Write, ResourceType::TransactionalId, PatternKind::Exact, "tx1")}}, {});
    EXPECT_TRUE(t.allowsName(ResourceType::Group, "cg-a", rs::opBit(Operation::Read)));
    EXPECT_FALSE(t.allowsName(ResourceType::Group, "other", rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allowsName(ResourceType::TransactionalId, "tx1", rs::opBit(Operation::Write)));
    EXPECT_FALSE(t.allowsName(ResourceType::TransactionalId, "tx2", rs::opBit(Operation::Write)));
}

TEST(ObjectIndexRegression, TracksLiveIdsAndDrops) {
    rg::ObjectIndex idx;
    EXPECT_EQ(idx.at(0), nullptr);
    idx.onTopic(2, "b"); idx.onView(5, "v");
    EXPECT_EQ(idx.idLimit(), 6);
    ASSERT_NE(idx.at(2), nullptr);
    EXPECT_EQ(idx.at(2)->name, "b");
    EXPECT_EQ(idx.at(5)->type, ResourceType::View);
    EXPECT_EQ(idx.at(3), nullptr) << "a gap is not live";
    EXPECT_EQ(idx.at(-6), nullptr);
    int n = 0; idx.forEach([&](int32_t, const rg::ObjectEntry&) { ++n; });
    EXPECT_EQ(n, 2);
    idx.onDropped(2);
    EXPECT_EQ(idx.at(2), nullptr);
    idx.onDropped(99);   // unknown: idempotent
    idx.onDropped(-3);
}

// ── the Authorizer: the only place a code or a detail is chosen ────────────────────────────────────────────────
TEST(AuthorizerRegression, DetailTextIsBuiltOnlyFromTheRequest) {
    rg::ObjectIndex idx; idx.onTopic(0, "hidden");
    auto t = rg::PermissionTable::compile({}, idx);
    const auto hidden  = rg::authorizeTopicId(t, 0, Operation::Read, idx);
    const auto missing = rg::authorizeTopicId(t, 77, Operation::Read, idx);
    ASSERT_TRUE(hidden.has_value());
    ASSERT_TRUE(missing.has_value());
    EXPECT_EQ(hidden->code, 45);
    EXPECT_EQ(hidden->detail, "not authorized: READ on TOPIC 0");
    EXPECT_EQ(missing->detail, "not authorized: READ on TOPIC 77");
    EXPECT_EQ(rg::authorizeName(t, ResourceType::Topic, "t1", Operation::Drop)->detail, "not authorized: DROP on TOPIC t1");
    EXPECT_EQ(rg::authorizeCluster(t, Operation::Admin)->detail, "not authorized: ADMIN on CLUSTER");
}

TEST(AuthorizerRegression, DescribeOnClusterTurnsAMissingIdIntoTheExistingUnknownTopic) {   // D22
    rg::ObjectIndex idx; idx.onTopic(0, "exists");
    auto t = rg::PermissionTable::compile({{g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, "")}}, idx);
    EXPECT_FALSE(rg::authorizeTopicId(t, 77, Operation::Read, idx).has_value()) << "missing id: no denial, UnknownTopic follows";
    EXPECT_TRUE(rg::authorizeTopicId(t, 0, Operation::Read, idx).has_value()) << "a live id it may not read is denied";
}

TEST(AuthorizerRegression, GrantedRequestsAreNotDenied) {
    rg::ObjectIndex idx; idx.onTopic(0, "a");
    auto t = rg::PermissionTable::compile({{g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "a")}}, idx);
    EXPECT_FALSE(rg::authorizeTopicId(t, 0, Operation::Write, idx).has_value());
    EXPECT_FALSE(rg::authorizeName(t, ResourceType::Topic, "a", Operation::Write).has_value());
    EXPECT_FALSE(rg::authorizeCluster(rg::PermissionTable::allowAll(), Operation::Admin).has_value());
}

TEST(AuthorizerRegression, AnOverlongNameIsCutInTheDetailIdenticallyForHiddenAndMissing) {
    auto t = rg::PermissionTable::denyAll();
    const auto d = rg::authorizeName(t, ResourceType::Topic, std::string(5000, 'x'), Operation::Drop);
    ASSERT_TRUE(d.has_value());
    EXPECT_LE(d->detail.size(), 300u);
}
