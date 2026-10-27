// Authentication spec §3: history / as-of / grants-for over a recorded __security log.
#include "SecurityScript.h"
#include <rune/security/SecurityQueries.h>
#include <rune/security/testing/SecurityGoldenScript.h>
#include <gtest/gtest.h>

namespace {
std::vector<rs::RecordedEvent> goldenLog() {
    std::vector<rs::RecordedEvent> log;
    int64_t off = 0;
    for (auto& e : rs::testing::goldenScriptV1()) log.push_back({off++, std::move(e)});
    return log;
}
template <class T> constexpr bool kHasVerifiers = requires(T r) { r.verifiers; } || requires(T r) { r.verifier; };
std::vector<int64_t> offsets(const std::vector<rs::RecordedEvent>& v) {
    std::vector<int64_t> o; for (const auto& r : v) o.push_back(r.offset); return o;
}
}

TEST(SecurityQueriesRegression, HistoryFiltersByKindNameAndTime) {
    const auto log = goldenLog();
    EXPECT_EQ(rs::history(log, {}).size(), log.size());
    EXPECT_EQ(offsets(rs::history(log, {.name = "ws1"})), (std::vector<int64_t>{5, 6, 7, 8, 11, 12, 13, 15}));
    EXPECT_EQ(offsets(rs::history(log, {.kind = rs::GranteeKind::Role})), (std::vector<int64_t>{1, 2, 3, 4, 9, 14}));
    EXPECT_EQ(offsets(rs::history(log, {.kind = rs::GranteeKind::Role, .name = "world_server"})), (std::vector<int64_t>{1, 2, 3, 4, 9, 14}));
    EXPECT_EQ(offsets(rs::history(log, {.kind = rs::GranteeKind::User, .name = "admin"})), (std::vector<int64_t>{0, 10}));
    EXPECT_EQ(offsets(rs::history(log, {.sinceMs = log[14].event.timestampMs})), (std::vector<int64_t>{14, 15})) << "sinceMs is inclusive";
    EXPECT_TRUE(rs::history(log, {.name = "nobody"}).empty());
}

TEST(SecurityQueriesRegression, AsOfOffsetAndTimeBoundariesAreInclusive) {
    const auto log = goldenLog();
    auto s6 = rs::stateAsOf(log, {rs::AsOf::By::Offset, 6});
    ASSERT_TRUE(s6.has_value());
    EXPECT_EQ(s6->appliedOffset(), 6) << "the event AT the boundary is included";
    EXPECT_EQ(s6->userByName(rune::catalog::kDefaultNsId, "ws1")->roles.size(), 1u);
    auto before = rs::stateAsOf(log, {rs::AsOf::By::Offset, -1});
    EXPECT_EQ(before->appliedOffset(), -1);
    auto t = rs::stateAsOf(log, {rs::AsOf::By::Time, log[6].event.timestampMs});
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->appliedOffset(), 6);
    auto t2 = rs::stateAsOf(log, {rs::AsOf::By::Time, log[6].event.timestampMs - 1});
    EXPECT_EQ(t2->appliedOffset(), 5);
    auto all = rs::stateAsOf(log, {rs::AsOf::By::Offset, 1000});
    EXPECT_EQ(all->appliedOffset(), 15);
    EXPECT_EQ(all->userByName(rune::catalog::kDefaultNsId, "ws1"), nullptr) << "dropped at the end";
}

TEST(SecurityQueriesRegression, AsOfSurfacesTheErrorOfABadEventInRange) {
    auto log = goldenLog();
    log[4].event.fingerprint ^= 1;
    auto bad = rs::stateAsOf(log, {rs::AsOf::By::Offset, 10});
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error().offset, 4);
    EXPECT_TRUE(rs::stateAsOf(log, {rs::AsOf::By::Offset, 3}).has_value()) << "before the bad event";
}

TEST(SecurityQueriesRegression, GrantsForSplitsDirectRolesAndEffective) {
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "u"); sc.run(rs::CreateRoleOps{}, "R"); sc.run(rs::CreateRoleOps{}, "S");
    const auto t1 = g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, "t1");
    const auto t2 = g(rs::Operation::Write, rs::ResourceType::Topic, rs::PatternKind::Exact, "t2");
    sc.run(rs::GrantOps{rs::GranteeKind::User, t1}, "u");
    sc.run(rs::GrantOps{rs::GranteeKind::Role, t2}, "R");
    sc.run(rs::GrantOps{rs::GranteeKind::Role, t1}, "S");
    sc.run(rs::GrantRoleOps{sc.s.roleByName(rune::catalog::kDefaultNsId, "S")->objectId, "S"}, "u");
    sc.run(rs::GrantRoleOps{sc.s.roleByName(rune::catalog::kDefaultNsId, "R")->objectId, "R"}, "u");
    auto v1 = rs::grantsFor(sc.s, rs::GranteeKind::User, "u");
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(v1->direct, (std::vector<rs::GrantSpec>{t1}));
    ASSERT_EQ(v1->viaRoles.size(), 2u);
    EXPECT_EQ(v1->viaRoles[0].first, "R"); EXPECT_EQ(v1->viaRoles[0].second, (std::vector<rs::GrantSpec>{t2}));
    EXPECT_EQ(v1->viaRoles[1].first, "S"); EXPECT_EQ(v1->viaRoles[1].second, (std::vector<rs::GrantSpec>{t1}));
    EXPECT_EQ(v1->effective, (std::vector<rs::GrantSpec>{t1, t2}));
    auto role = rs::grantsFor(sc.s, rs::GranteeKind::Role, "R");
    ASSERT_TRUE(role.has_value());
    EXPECT_EQ(role->direct, role->effective);
    EXPECT_TRUE(role->viaRoles.empty());
    EXPECT_FALSE(rs::grantsFor(sc.s, rs::GranteeKind::User, "R").has_value()) << "namespaces are separate";
    EXPECT_FALSE(rs::grantsFor(sc.s, rs::GranteeKind::Role, "nobody").has_value());
}

TEST(SecurityQueriesRegression, UsersAndRolesRowsNeverCarryVerifierBytes) {
    static_assert(!kHasVerifiers<rs::UserRow>, "a row must not expose verifiers");
    static_assert(!kHasVerifiers<rs::RoleRow>);
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "zed"); sc.run(rs::CreateUserOps{v(2)}, "amy"); sc.run(rs::CreateRoleOps{}, "R");
    sc.run(rs::AlterUserOps{rs::AddPassword{v(3)}}, "amy");
    sc.run(rs::GrantRoleOps{sc.s.roleByName(rune::catalog::kDefaultNsId, "R")->objectId, "R"}, "zed");
    sc.run(rs::GrantRoleOps{sc.s.roleByName(rune::catalog::kDefaultNsId, "R")->objectId, "R"}, "amy");
    sc.run(rs::GrantOps{rs::GranteeKind::Role, g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, "t")}, "R");
    const auto us = rs::users(sc.s);
    ASSERT_EQ(us.size(), 2u);
    EXPECT_EQ(us[0].name, "amy"); EXPECT_EQ(us[0].passwords, 2); EXPECT_EQ(us[0].roles, std::vector<std::string>{"R"});
    EXPECT_EQ(us[1].name, "zed"); EXPECT_EQ(us[1].passwords, 1);
    const auto rl = rs::roles(sc.s);
    ASSERT_EQ(rl.size(), 1u);
    EXPECT_EQ(rl[0].members, (std::vector<std::string>{"amy", "zed"})); EXPECT_EQ(rl[0].grants, 1u);
}
