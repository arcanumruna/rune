// Authentication spec §3: per-object fingerprints (never ids) and the state digest.
#include "SecurityScript.h"
#include <rune/security/SecurityFingerprint.h>
#include <gtest/gtest.h>
#include <set>

namespace {
rs::GrantSpec rt(rs::Operation op = rs::Operation::Read) { return g(op, rs::ResourceType::Topic, rs::PatternKind::Exact, "t"); }
}

TEST(SecurityFingerprintRegression, IdsAreNeverHashed) {
    Script a, b;   // same objects, created in the opposite order => different objectIds
    a.run(rs::CreateRoleOps{}, "R"); a.run(rs::CreateUserOps{v(1)}, "u");
    b.run(rs::CreateUserOps{v(1)}, "u"); b.run(rs::CreateRoleOps{}, "R");
    a.run(rs::GrantRoleOps{a.s.roleByName(rune::catalog::kDefaultNsId, "R")->objectId, "R"}, "u");
    b.run(rs::GrantRoleOps{b.s.roleByName(rune::catalog::kDefaultNsId, "R")->objectId, "R"}, "u");
    ASSERT_NE(a.s.userByName(rune::catalog::kDefaultNsId, "u")->objectId, b.s.userByName(rune::catalog::kDefaultNsId, "u")->objectId);
    EXPECT_EQ(rs::fingerprintOf(*a.s.userByName(rune::catalog::kDefaultNsId, "u"), a.s), rs::fingerprintOf(*b.s.userByName(rune::catalog::kDefaultNsId, "u"), b.s));
    EXPECT_EQ(rs::fingerprintOf(*a.s.roleByName(rune::catalog::kDefaultNsId, "R")), rs::fingerprintOf(*b.s.roleByName(rune::catalog::kDefaultNsId, "R")));
    // the fingerprint an event carries is id-free too: the same CreateUser fingerprints equal under different ids
    EXPECT_NE(a.s.digest(), b.s.digest()) << "the digest covers ids (nextObjectId / apply order); object fingerprints do not";
}

TEST(SecurityFingerprintRegression, EveryUserFieldChangesTheFingerprint) {
    Script sc;
    sc.run(rs::CreateRoleOps{}, "A"); sc.run(rs::CreateRoleOps{}, "B");
    rs::UserObject base;
    base.objectId = 50; base.name = "u"; base.version = 3; base.enabled = true; base.mustChangePassword = false;
    base.verifiers.push_back(v(1)); base.roles = {sc.s.roleByName(rune::catalog::kDefaultNsId, "A")->objectId}; base.grants = {rt()};
    const uint64_t f0 = rs::fingerprintOf(base, sc.s);
    std::set<uint64_t> seen{f0};
    auto expectFlip = [&](const char* what, auto mutate) {
        rs::UserObject u = base; mutate(u);
        const uint64_t f = rs::fingerprintOf(u, sc.s);
        EXPECT_NE(f, f0) << what;
        EXPECT_TRUE(seen.insert(f).second) << what << " collides with another single-field change";
    };
    expectFlip("name", [](auto& u) { u.name = "v"; });
    expectFlip("version", [](auto& u) { u.version = 4; });
    expectFlip("enabled", [](auto& u) { u.enabled = false; });
    expectFlip("mustChangePassword", [](auto& u) { u.mustChangePassword = true; });
    expectFlip("salt", [](auto& u) { u.verifiers[0].salt[0] ^= 1; });
    expectFlip("iterations", [](auto& u) { u.verifiers[0].iterations = 4097; });
    expectFlip("storedKey", [](auto& u) { u.verifiers[0].storedKey[63] ^= 1; });
    expectFlip("serverKey", [](auto& u) { u.verifiers[0].serverKey[0] ^= 1; });
    expectFlip("second verifier", [](auto& u) { u.verifiers.push_back(v(1)); });
    expectFlip("a role name", [&](auto& u) { u.roles = {sc.s.roleByName(rune::catalog::kDefaultNsId, "B")->objectId}; });
    expectFlip("role count", [&](auto& u) { u.roles.insert(sc.s.roleByName(rune::catalog::kDefaultNsId, "B")->objectId); });
    expectFlip("grant op", [](auto& u) { u.grants = {rt(rs::Operation::Write)}; });
    expectFlip("grant type", [](auto& u) { u.grants = {g(rs::Operation::Read, rs::ResourceType::View, rs::PatternKind::Exact, "t")}; });
    expectFlip("grant kind", [](auto& u) { u.grants = {g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "t")}; });
    expectFlip("grant name", [](auto& u) { u.grants = {g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, "u")}; });
    expectFlip("grant count", [](auto& u) { u.grants.insert(rt(rs::Operation::Write)); });
}

TEST(SecurityFingerprintRegression, RoleAndTombstoneFieldsChangeTheirFingerprints) {
    rs::RoleObject r; r.objectId = 1; r.name = "R"; r.version = 1; r.grants = {rt()};
    const uint64_t f = rs::fingerprintOf(r);
    { auto x = r; x.name = "S"; EXPECT_NE(rs::fingerprintOf(x), f); }
    { auto x = r; x.version = 2; EXPECT_NE(rs::fingerprintOf(x), f); }
    { auto x = r; x.grants.clear(); EXPECT_NE(rs::fingerprintOf(x), f); }
    { auto x = r; x.objectId = 99; EXPECT_EQ(rs::fingerprintOf(x), f); }
    rs::Tombstone t{1, false, "u", 3};
    const uint64_t tf = rs::fingerprintOf(t);
    { auto x = t; x.wasRole = true; EXPECT_NE(rs::fingerprintOf(x), tf); }
    { auto x = t; x.name = "v"; EXPECT_NE(rs::fingerprintOf(x), tf); }
    { auto x = t; x.finalVersion = 4; EXPECT_NE(rs::fingerprintOf(x), tf); }
    { auto x = t; x.objectId = 7; EXPECT_EQ(rs::fingerprintOf(x), tf); }
    EXPECT_NE(rs::fingerprintOf(rs::Tombstone{1, true, "R", 1}), rs::fingerprintOf(r)) << "kind tags separate the domains";
}

TEST(SecurityFingerprintRegression, DigestEqualsAtEqualOffsetsAndDiffersAfterAnyEvent) {
    Script a, b;
    std::set<uint64_t> digests{a.s.digest()};
    auto step = [&](rs::Ops ops, const char* name) {
        a.run(ops, name); b.run(ops, name);
        EXPECT_EQ(a.s.digest(), b.s.digest()) << name;
        EXPECT_TRUE(digests.insert(a.s.digest()).second) << "event on " << name << " left the digest unchanged";
    };
    step(rs::CreateUserOps{v(1)}, "u"); step(rs::CreateRoleOps{}, "R");
    step(rs::GrantOps{rs::GranteeKind::Role, rt()}, "R");
    step(rs::AlterUserOps{rs::DisableUser{}}, "u"); step(rs::AlterUserOps{rs::EnableUser{}}, "u");
    step(rs::DropRoleOps{}, "R"); step(rs::DropUserOps{}, "u");
}
