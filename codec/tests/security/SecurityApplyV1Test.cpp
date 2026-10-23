// Authentication spec §2/§3/§6: the frozen formatVer-1 rules of __security.
#include "SecurityScript.h"
#include <gtest/gtest.h>
#include <algorithm>


TEST(SecurityApplyV1Regression, CreateUserThenTwoPasswordsAtMost) {
    Script sc;
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(1)}, "ws1").has_value());
    ASSERT_TRUE(sc.run(rs::AlterUserOps{rs::AddPassword{v(2)}}, "ws1").has_value());
    auto third = sc.run(rs::AlterUserOps{rs::AddPassword{v(3)}}, "ws1");
    ASSERT_FALSE(third.has_value());
    EXPECT_EQ(third.error().refusal, rs::Refusal::TooManyPasswords);
    EXPECT_EQ(sc.s.user("ws1")->verifiers.size(), 2u);
    EXPECT_EQ(sc.s.user("ws1")->version, 2u) << "a refused event changes nothing";
}

TEST(SecurityApplyV1Regression, DropPasswordNumbersByAgeAndNeverDropsTheLast) {
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "ws1"); sc.run(rs::AlterUserOps{rs::AddPassword{v(2)}}, "ws1");
    ASSERT_TRUE(sc.run(rs::AlterUserOps{rs::DropPassword{1}}, "ws1").has_value());
    EXPECT_EQ(sc.s.user("ws1")->verifiers.front().salt.front(), 2) << "1 = the oldest (D6)";
    auto last = sc.run(rs::AlterUserOps{rs::DropPassword{1}}, "ws1");
    EXPECT_EQ(last.error().refusal, rs::Refusal::LastPassword);
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::DropPassword{2}}, "ws1").error().refusal, rs::Refusal::NoSuchPassword);
}

TEST(SecurityApplyV1Regression, SessionEffectPerKindIsTheDecisionTable) {
    Script sc;
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, "ws1")->effect, rs::SessionEffect::None);
    EXPECT_EQ(sc.run(rs::CreateRoleOps{}, "world")->effect, rs::SessionEffect::None);
    const auto& ws1 = *sc.s.user("ws1");
    auto gr = sc.run(rs::GrantRoleOps{sc.s.role("world")->objectId, "world"}, "ws1");
    EXPECT_EQ(gr->effect, rs::SessionEffect::Refresh);
    EXPECT_EQ(gr->affectedUsers, std::vector<uint64_t>{ws1.objectId});
    auto grant = sc.run(rs::GrantOps{rs::GranteeKind::Role, g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "world.")}, "world");
    EXPECT_EQ(grant->effect, rs::SessionEffect::Refresh);
    EXPECT_EQ(grant->affectedUsers, std::vector<uint64_t>{ws1.objectId}) << "a role grant refreshes every member";
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::SetPassword{v(4), false}}, "ws1")->effect, rs::SessionEffect::Refresh);
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::SetPassword{v(5), true}}, "ws1")->effect, rs::SessionEffect::Revoke);
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::DisableUser{}}, "ws1")->effect, rs::SessionEffect::Revoke);
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::EnableUser{}}, "ws1")->effect, rs::SessionEffect::None);
    auto dropRole = sc.run(rs::DropRoleOps{}, "world");
    EXPECT_EQ(dropRole->effect, rs::SessionEffect::Refresh);
    EXPECT_TRUE(sc.s.user("ws1")->roles.empty()) << "membership removed with the role";
    EXPECT_EQ(sc.run(rs::DropUserOps{}, "ws1")->effect, rs::SessionEffect::Revoke);
}

TEST(SecurityApplyV1Regression, InvalidGrantsAreRefused) {
    Script sc; sc.run(rs::CreateUserOps{v(1)}, "u");
    const rs::GrantSpec bad[] = {
        g(rs::Operation::Read,  rs::ResourceType::Cluster, rs::PatternKind::Exact, ""),            // D16
        g(rs::Operation::Write, rs::ResourceType::Cluster, rs::PatternKind::Exact, ""),
        g(rs::Operation::Admin, rs::ResourceType::Topic,   rs::PatternKind::Exact, "t"),           // D17
        g(rs::Operation::Write, rs::ResourceType::View,    rs::PatternKind::Exact, "v"),
        g(rs::Operation::Write, rs::ResourceType::Group,   rs::PatternKind::Exact, "g"),
        g(rs::Operation::Read,  rs::ResourceType::TransactionalId, rs::PatternKind::Exact, "x"),
        g(rs::Operation::Admin, rs::ResourceType::Cluster, rs::PatternKind::Prefix, ""),           // CLUSTER is Exact ""
        g(rs::Operation::Read,  rs::ResourceType::Topic,   rs::PatternKind::Exact, ""),            // empty exact name
    };
    for (const auto& b : bad)
        EXPECT_EQ(sc.run(rs::GrantOps{rs::GranteeKind::User, b}, "u").error().refusal, rs::Refusal::InvalidGrant)
            << rs::operationName(b.op) << " " << rs::resourceTypeName(b.resource.type);
    EXPECT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::User, g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "")}, "u").has_value())
        << "PREFIX '' = every topic";
}

TEST(SecurityApplyV1Regression, BootstrapOnlyWhileNoEnabledAdminExists) {
    Script sc;
    auto b = sc.run(rs::BootstrapOps{v(1)}, "admin");
    ASSERT_TRUE(b.has_value());
    EXPECT_TRUE(sc.s.user("admin")->mustChangePassword);
    EXPECT_TRUE(sc.s.hasSecurityAdmin());
    EXPECT_EQ(sc.run(rs::BootstrapOps{v(2)}, "admin2").error().refusal, rs::Refusal::AlreadyBootstrapped);
    sc.run(rs::AlterUserOps{rs::DisableUser{}}, "admin");
    EXPECT_FALSE(sc.s.hasSecurityAdmin()) << "a disabled admin does not count";
    EXPECT_TRUE(sc.run(rs::BootstrapOps{v(3)}, "admin2").has_value());
}

TEST(SecurityApplyV1Regression, FingerprintMismatchIsRefusedAndLeavesTheStateUntouched) {
    Script sc;
    auto e = sc.make(rs::CreateUserOps{v(1)}, "ws1");
    e.fingerprint ^= 1;
    const uint64_t before = sc.s.digest();
    auto r = rs::apply(sc.s, e, 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code, rs::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(sc.s.digest(), before);
    EXPECT_EQ(sc.s.user("ws1"), nullptr);
}

TEST(SecurityApplyV1Regression, VersionsAndOffsetsAreChecked) {
    Script sc; sc.run(rs::CreateUserOps{v(1)}, "ws1");
    auto e = sc.make(rs::AlterUserOps{rs::DisableUser{}}, "ws1");
    e.prevVersion = 7; e.newVersion = 8;
    EXPECT_EQ(rs::apply(sc.s, e, 5).error().refusal, rs::Refusal::VersionMismatch);
    auto ok = sc.make(rs::AlterUserOps{rs::DisableUser{}}, "ws1");
    EXPECT_EQ(rs::apply(sc.s, ok, 0).error().code, rs::ApplyError::Code::OutOfOrder) << "offset 0 was applied";
}

TEST(SecurityApplyV1Regression, NamesAreValidatedAndNamespacesAreSeparate) {
    Script sc;
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, "__x").error().refusal, rs::Refusal::InvalidName);
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, "a,b").error().refusal, rs::Refusal::InvalidName);
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, std::string(129, 'a')).error().refusal, rs::Refusal::InvalidName);
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(1)}, "ops").has_value());
    EXPECT_TRUE(sc.run(rs::CreateRoleOps{}, "ops").has_value()) << "users and roles are separate namespaces";
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, "ops").error().refusal, rs::Refusal::UserExists);
}

TEST(SecurityApplyV1Regression, ReusedNameGetsANewObjectId) {
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "ws1");
    const uint64_t first = sc.s.user("ws1")->objectId;
    sc.run(rs::DropUserOps{}, "ws1");
    sc.run(rs::CreateUserOps{v(1)}, "ws1");
    EXPECT_NE(sc.s.user("ws1")->objectId, first);
    EXPECT_EQ(sc.s.tombstones().size(), 1u);
}
TEST(SecurityApplyV1Regression, RefusedEventsNeverTouchTheStateNorTheOffset) {
    Script sc; sc.run(rs::CreateUserOps{v(1)}, "ws1");
    const uint64_t before = sc.s.digest(); const int64_t off = sc.s.appliedOffset();
    auto dup = sc.run(rs::CreateUserOps{v(2)}, "ws1");
    ASSERT_FALSE(dup.has_value());
    EXPECT_EQ(dup.error().code, rs::ApplyError::Code::Precondition);
    EXPECT_EQ(sc.s.digest(), before);
    EXPECT_EQ(sc.s.appliedOffset(), off);
}

TEST(SecurityApplyV1Regression, UnsupportedFormatAndMalformedEventsAreRefusedByCode) {
    Script sc;
    auto e = sc.make(rs::CreateUserOps{v(1)}, "ws1");
    e.formatVer = 2;
    EXPECT_EQ(rs::apply(sc.s, e, 0).error().code, rs::ApplyError::Code::UnsupportedFormat);
    e.formatVer = 1; e.kind = rs::EventKind::DropUser;   // kind disagrees with the ops
    EXPECT_EQ(rs::apply(sc.s, e, 0).error().code, rs::ApplyError::Code::BadOps);
    EXPECT_EQ(sc.s.appliedOffset(), -1);
}

TEST(SecurityApplyV1Regression, RoleGrantAndMembershipLifecycle) {
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "ws1"); sc.run(rs::CreateRoleOps{}, "world");
    const uint64_t rid = sc.s.role("world")->objectId;
    EXPECT_EQ(sc.run(rs::GrantRoleOps{rid, "wrong"}, "ws1").error().refusal, rs::Refusal::UnknownRole) << "name must match the id";
    ASSERT_TRUE(sc.run(rs::GrantRoleOps{rid, "world"}, "ws1").has_value());
    EXPECT_EQ(sc.run(rs::GrantRoleOps{rid, "world"}, "ws1").error().refusal, rs::Refusal::AlreadyMember);
    EXPECT_EQ(sc.s.membersOf(rid), std::vector<uint64_t>{sc.s.user("ws1")->objectId});
    ASSERT_TRUE(sc.run(rs::RevokeRoleOps{rid, "world"}, "ws1").has_value());
    EXPECT_EQ(sc.run(rs::RevokeRoleOps{rid, "world"}, "ws1").error().refusal, rs::Refusal::NotMember);
    const auto rg = g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, "t");
    ASSERT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::Role, rg}, "world").has_value());
    EXPECT_EQ(sc.run(rs::GrantOps{rs::GranteeKind::Role, rg}, "world").error().refusal, rs::Refusal::AlreadyGranted);
    EXPECT_EQ(sc.run(rs::GrantOps{rs::GranteeKind::Role, rg}, "nobody").error().refusal, rs::Refusal::UnknownRole);
    ASSERT_TRUE(sc.run(rs::RevokeOps{rs::GranteeKind::Role, rg}, "world").has_value());
    EXPECT_EQ(sc.run(rs::RevokeOps{rs::GranteeKind::Role, rg}, "world").error().refusal, rs::Refusal::NotGranted);
}

TEST(SecurityApplyV1Regression, DropRoleChangesMemberFingerprintsThroughTheDigestNotTheirVersions) {
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "ws1"); sc.run(rs::CreateRoleOps{}, "world");
    sc.run(rs::GrantRoleOps{sc.s.role("world")->objectId, "world"}, "ws1");
    const uint32_t ver = sc.s.user("ws1")->version;
    const uint64_t fpBefore = rs::fingerprintOf(*sc.s.user("ws1"), sc.s);
    auto dr = sc.run(rs::DropRoleOps{}, "world");
    ASSERT_TRUE(dr.has_value());
    EXPECT_EQ(dr->affectedUsers, std::vector<uint64_t>{sc.s.user("ws1")->objectId});
    EXPECT_EQ(sc.s.user("ws1")->version, ver);
    EXPECT_NE(rs::fingerprintOf(*sc.s.user("ws1"), sc.s), fpBefore);
    ASSERT_EQ(sc.s.tombstones().size(), 1u);
    EXPECT_TRUE(sc.s.tombstones()[0].wasRole);
}

TEST(SecurityApplyV1Regression, RolesWithoutMembersAndRoleGrantsAffectNobody) {
    Script sc;
    sc.run(rs::CreateRoleOps{}, "lonely");
    auto gr = sc.run(rs::GrantOps{rs::GranteeKind::Role, g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, "t")}, "lonely");
    EXPECT_EQ(gr->effect, rs::SessionEffect::None);
    EXPECT_TRUE(gr->affectedUsers.empty());
    EXPECT_EQ(sc.run(rs::DropRoleOps{}, "lonely")->effect, rs::SessionEffect::None);
}

TEST(SecurityApplyV1Regression, RequestIdsAreRecordedWithTheirFingerprint) {
    Script sc;
    auto e = sc.make(rs::CreateUserOps{v(1)}, "ws1");
    e.principal.requestId = {.hi = 7, .lo = 9}; e.principal.requestFingerprint = 0xABCD;
    ASSERT_TRUE(rs::apply(sc.s, e, 0).has_value());
    auto rec = sc.s.request({7, 9});
    ASSERT_TRUE(rec.has_value());
    EXPECT_EQ(rec->offset, 0); EXPECT_EQ(rec->requestFingerprint, 0xABCDu); EXPECT_EQ(rec->kind, rs::EventKind::CreateUser);
    EXPECT_FALSE(sc.s.request({1, 1}).has_value());
}

TEST(SecurityApplyV1Regression, PreviewAppliedMatchesApplyAndMutatesNothing) {
    Script sc; sc.run(rs::CreateUserOps{v(1)}, "ws1");
    const uint64_t before = sc.s.digest();
    auto e = sc.make(rs::AlterUserOps{rs::DisableUser{}}, "ws1");
    auto p = rs::previewApplied(sc.s, e);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(sc.s.digest(), before);
    auto a = rs::apply(sc.s, e, 1);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(p->fingerprint, a->fingerprint); EXPECT_EQ(p->effect, a->effect); EXPECT_EQ(p->affectedUsers, a->affectedUsers);
}

TEST(SecurityApplyV1Regression, ValidNameAcceptsExactlyTheDocumentedAlphabet) {
    EXPECT_TRUE(rs::validName("a.B_c@d-9")); EXPECT_TRUE(rs::validName(std::string(128, 'x')));
    EXPECT_FALSE(rs::validName("")); EXPECT_FALSE(rs::validName("a b")); EXPECT_FALSE(rs::validName("__a"));
    EXPECT_TRUE(rs::validName("_a")); EXPECT_FALSE(rs::validName("a/b")); EXPECT_FALSE(rs::validName("é"));
}

TEST(SecurityApplyV1Regression, FakeSaltSeedIsSetOnceByTheFirstBootstrapRegression) {
    Script sc;
    EXPECT_FALSE(sc.s.fakeSaltSeed().has_value());
    const uint64_t emptyDigest = sc.s.digest();
    rs::BootstrapOps first{v(1)}; first.fakeSaltSeed.fill(0xA1);
    ASSERT_TRUE(sc.run(first, "admin").has_value());
    ASSERT_TRUE(sc.s.fakeSaltSeed().has_value());
    EXPECT_EQ((*sc.s.fakeSaltSeed())[0], 0xA1);
    // every admin gone, a second bootstrap with another seed: the cluster keeps the FIRST seed
    sc.run(rs::AlterUserOps{rs::DisableUser{}}, "admin");
    rs::BootstrapOps second{v(2)}; second.fakeSaltSeed.fill(0xB2);
    ASSERT_TRUE(sc.run(second, "admin2").has_value());
    EXPECT_EQ((*sc.s.fakeSaltSeed())[0], 0xA1) << "fake salts never change for the life of the cluster";
    // the digest covers the seed (replicas must agree on it), the fingerprints do not
    Script a, b;
    rs::BootstrapOps x{v(1)}, y{v(1)}; x.fakeSaltSeed.fill(1); y.fakeSaltSeed.fill(2);
    a.run(x, "admin"); b.run(y, "admin");
    EXPECT_EQ(rs::fingerprintOf(*a.s.user("admin"), a.s), rs::fingerprintOf(*b.s.user("admin"), b.s));
    EXPECT_NE(a.s.digest(), b.s.digest());
    EXPECT_NE(a.s.digest(), emptyDigest);
}

TEST(SecurityApplyV1Regression, GrantsOnInternalNamesAreRefusedRegression) {
    Script sc; sc.run(rs::CreateUserOps{v(1)}, "u");
    const rs::GrantSpec bad[] = {
        g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, "__security"),
        g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "__"),
        g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "__cat"),
        g(rs::Operation::Read, rs::ResourceType::View, rs::PatternKind::Exact, "__views"),
        g(rs::Operation::Read, rs::ResourceType::Group, rs::PatternKind::Prefix, "__consumer"),
        g(rs::Operation::Write, rs::ResourceType::TransactionalId, rs::PatternKind::Exact, "__x"),
        g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, std::string(250, 'a')),
        g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, std::string(250, 'a')),
    };
    for (const auto& b : bad)
        EXPECT_EQ(sc.run(rs::GrantOps{rs::GranteeKind::User, b}, "u").error().refusal, rs::Refusal::InvalidGrant) << b.resource.name.substr(0, 12);
    EXPECT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::User, g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, std::string(249, 'a'))}, "u").has_value());
    EXPECT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::User, g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "_x")}, "u").has_value()) << "a single underscore is an ordinary name";
}

// Adversarial review of PR #597 (the dropRole fix): DROP ROLE with several members must remove EVERY membership from
// the state (not only report it), so no member keeps the dropped role's grants: an admin role dropped leaves no
// security admin; a role re-created under the same name is a new object that nobody holds; a GrantRole naming the
// dropped role's id is UnknownRole; and the state equals a replay of its own events.  Mutation: move `members` into
// one argument of finish() while copying it into the other (the pre-fix call): under GCC the memberships survive and
// this test turns red.
TEST(SecurityApplyV1Adversarial, DropRoleWithManyMembersRemovesEveryMembershipAndItsGrantsAdversarialRegression) {
    Script sc;
    std::vector<rs::SecurityEvent> log;
    auto run = [&](rs::Ops ops, std::string name) {
        rs::SecurityEvent e = sc.make(std::move(ops), std::move(name));
        auto a = rs::apply(sc.s, e, sc.offset);
        if (a) log.push_back(e);
        ++sc.offset;
        return a;
    };
    for (const char* u : {"m1", "m2", "m3", "outsider"}) ASSERT_TRUE(run(rs::CreateUserOps{v(1)}, u));
    ASSERT_TRUE(run(rs::CreateRoleOps{}, "ops"));
    const uint64_t roleId = sc.s.role("ops")->objectId;
    const rs::GrantSpec admin = g(rs::Operation::Admin, rs::ResourceType::Cluster, rs::PatternKind::Exact, "");
    const rs::GrantSpec read = g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "ops.");
    ASSERT_TRUE(run(rs::GrantOps{rs::GranteeKind::Role, admin}, "ops"));
    ASSERT_TRUE(run(rs::GrantOps{rs::GranteeKind::Role, read}, "ops"));
    std::vector<uint64_t> members;
    for (const char* u : {"m3", "m1", "m2"}) {   // granted out of id order
        ASSERT_TRUE(run(rs::GrantRoleOps{roleId, "ops"}, u));
        members.push_back(sc.s.user(u)->objectId);
    }
    std::sort(members.begin(), members.end());
    ASSERT_TRUE(sc.s.hasSecurityAdmin()) << "the role makes its members admins";

    auto dr = run(rs::DropRoleOps{}, "ops");
    ASSERT_TRUE(dr.has_value());
    EXPECT_EQ(dr->effect, rs::SessionEffect::Refresh);
    EXPECT_EQ(dr->affectedUsers, members) << "every member's sessions are refreshed";
    for (const char* u : {"m1", "m2", "m3", "outsider"}) {
        EXPECT_FALSE(sc.s.user(u)->roles.contains(roleId)) << u << " still holds the dropped role";
        const auto eff = rs::effectiveGrants(sc.s, sc.s.user(u)->objectId);
        EXPECT_EQ(std::find(eff.begin(), eff.end(), admin), eff.end()) << u << " kept the dropped role's ADMIN grant";
        EXPECT_EQ(std::find(eff.begin(), eff.end(), read), eff.end()) << u << " kept the dropped role's READ grant";
    }
    EXPECT_FALSE(sc.s.hasSecurityAdmin()) << "no admin survives the drop of the only admin role";

    ASSERT_TRUE(run(rs::CreateRoleOps{}, "ops"));
    const uint64_t newRoleId = sc.s.role("ops")->objectId;
    EXPECT_NE(newRoleId, roleId);
    for (const char* u : {"m1", "m2", "m3"}) {
        EXPECT_FALSE(sc.s.user(u)->roles.contains(newRoleId)) << u << " holds the re-created role";
        EXPECT_TRUE(sc.s.user(u)->roles.empty()) << u;
    }
    auto stale = run(rs::GrantRoleOps{roleId, "ops"}, "m1");
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error().refusal, rs::Refusal::UnknownRole) << "the dropped role's id never resolves";

    rs::SecurityState replayed;
    int64_t off = 0;
    for (const auto& e : log) ASSERT_TRUE(rs::apply(replayed, e, off++).has_value());
    EXPECT_EQ(replayed.digest(), sc.s.digest()) << "the state equals a replay of its events";
}
