// Namespaces Task 4 (plan 2026-10-26-rune-namespaces.md): __security formatVer 2 - u32 nsId after the kind,
// (nsId, local) user and role keys, the Namespace resource and the Truncate operation.
#include <rune/catalog/CatalogFingerprintV2.h>
#include <rune/security/Permissions.h>
#include <rune/security/SecurityFile.h>
#include <rune/security/SecurityReplayer.h>
#include <rune/security/testing/SecurityGoldenScript.h>
#include "SecurityGoldenPins.h"
#include "SecurityScript.h"
#include <gtest/gtest.h>

#include <fstream>
#include <map>
#include <string>
#include <tuple>

using rs::Operation;
using rs::PatternKind;
using rs::ResourceType;

namespace {
std::string toHex(const std::vector<uint8_t>& b) {
    static constexpr char d[] = "0123456789abcdef";
    std::string s;
    for (uint8_t x : b) { s += d[x >> 4]; s += d[x & 15]; }
    return s;
}
constexpr uint32_t kNs1 = 1;

/// A bootstrapped cluster: admin in the default namespace.
void bootstrap(Script& sc) {
    rs::BootstrapOps boot{v(1)};
    ASSERT_TRUE(sc.run(boot, "admin").has_value());
}
} // namespace

// ── the v2 wire ─────────────────────────────────────────────────────────────────────────────────────────────
TEST(NamespaceSecurityV2Regression, HeaderLayoutRegression) {
    rs::SecurityEvent e;
    e.formatVer = rs::kFormatV2; e.nsId = 7; e.kind = rs::EventKind::DropUser; e.ops = rs::DropUserOps{};
    e.objectId = 5; e.objectName = "u"; e.prevVersion = 1; e.newVersion = 2;
    e.timestampMs = -1; e.fingerprint = 0x1122334455667788ull;
    const auto b = rs::encode(e).value();
    // Independently spelled: u16 formatVer 2 | u16 kind 3 | u32 nsId 7 | u64 objectId 5 | u32 prev | u32 new |
    // i64 ts | u64 fingerprint | str16 "u" | str32 "" | principal (zeros) | hasReplayedFrom 0 | opsLen 1 | tag 3
    EXPECT_EQ(toHex(b),
              "020003000700000005000000000000000100000002000000ffffffffffffffff88776655443322110100750000"
              "000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000"
              "03");
    auto d = rs::decode(b);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, e);
    EXPECT_EQ(d->nsId, 7u);
}

TEST(NamespaceSecurityV2Regression, CurrentFormatVerIsTwoAndV1CannotCarryANamespaceRegression) {
    EXPECT_EQ(rs::kCurrentFormatVer, 2);
    EXPECT_EQ(rs::SecurityEvent{}.formatVer, rs::kFormatV2);
    rs::SecurityEvent v1;
    v1.formatVer = rs::kFormatV1; v1.kind = rs::EventKind::DropUser; v1.ops = rs::DropUserOps{}; v1.objectName = "u";
    EXPECT_TRUE(rs::encode(v1).has_value());
    v1.nsId = 1;
    ASSERT_FALSE(rs::encode(v1).has_value());   // formatVer 1 has no nsId field
    EXPECT_EQ(rs::encode(v1).error(), rs::EncodeError::BadEnum);
}

TEST(NamespaceSecurityV2Regression, TruncatedAtEveryOffsetRegression) {
    std::vector<rs::Ops> ops = {rs::CreateUserOps{v(1)}, rs::DropUserOps{}, rs::CreateRoleOps{},
        rs::GrantOps{rs::GranteeKind::Role, g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "arena")},
        rs::GrantOps{rs::GranteeKind::User, g(Operation::Truncate, ResourceType::Topic, PatternKind::Exact, "t")},
        rs::GrantRoleOps{4, "r"}};
    for (const rs::Ops& o : ops) {
        rs::SecurityEvent e;
        e.formatVer = rs::kFormatV2; e.nsId = 3; e.kind = rs::kindOf(o); e.ops = o; e.objectName = "x";
        const auto b = rs::encode(e).value();
        for (std::size_t n = 0; n < b.size(); ++n) {
            auto d = rs::decode(std::span<const uint8_t>(b.data(), n));
            ASSERT_FALSE(d.has_value()) << rs::kindName(e.kind) << " prefix " << n;
            EXPECT_EQ(d.error(), rs::DecodeError::Truncated) << rs::kindName(e.kind) << " prefix " << n;
        }
    }
}

TEST(NamespaceSecurityV2Regression, EnumRangesByFormatVerRegression) {
    // Truncate (op 7) and Namespace (type 5) exist from formatVer 2 only; 8 and 6 never.
    for (const uint16_t fv : {rs::kFormatV1, rs::kFormatV2}) {
        for (const auto [op, type, okV2] : {std::tuple{Operation::Truncate, ResourceType::Topic, true},
                                            std::tuple{Operation::Read, ResourceType::Namespace, true},
                                            std::tuple{Operation::Admin, ResourceType::TransactionalId, true},
                                            std::tuple{static_cast<Operation>(8), ResourceType::Topic, false},
                                            std::tuple{Operation::Read, static_cast<ResourceType>(6), false}}) {
            rs::SecurityEvent e;
            e.formatVer = fv; e.kind = rs::EventKind::Grant; e.objectName = "r";
            e.ops = rs::GrantOps{rs::GranteeKind::Role, g(op, type, PatternKind::Exact, "x")};
            const bool ok = fv == rs::kFormatV2 ? okV2 : (op != Operation::Truncate && type != ResourceType::Namespace && okV2);
            auto enc = rs::encode(e);
            EXPECT_EQ(enc.has_value(), ok) << "fv " << fv << " op " << int(op) << " type " << int(type);
            if (!ok) {
                EXPECT_EQ(enc.error(), rs::EncodeError::BadEnum);
            } else {   // and decode agrees
                EXPECT_TRUE(rs::decode(*enc).has_value());
            }
        }
    }
}

// ── keys: (nsId, local) ─────────────────────────────────────────────────────────────────────────────────────
TEST(NamespaceSecurityV2Regression, SameUserTwoNamespacesRegression) {
    Script sc;
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(1)}, "alice").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateUserOps{v(2)}, "alice").has_value());
    const rs::UserObject* a0 = sc.s.userByName(0, "alice");
    const rs::UserObject* a1 = sc.s.userByName(kNs1, "alice");
    ASSERT_NE(a0, nullptr); ASSERT_NE(a1, nullptr);
    EXPECT_NE(a0, a1);
    EXPECT_NE(a0->objectId, a1->objectId);
    EXPECT_EQ(a0->nsId, 0u); EXPECT_EQ(a1->nsId, 1u);
    EXPECT_NE(a0->verifiers.front().salt, a1->verifiers.front().salt) << "distinct passwords";
    EXPECT_EQ(sc.s.userByName(2, "alice"), nullptr);
    EXPECT_EQ(sc.s.userById(a1->objectId), a1);
    // a user and a role of one namespace share nothing with the other's
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateRoleOps{}, "alice").has_value()) << "a role may share a user's name; the sets are separate";
    ASSERT_TRUE(sc.run(rs::CreateRoleOps{}, "alice").has_value());
    // a second alice in the SAME namespace is refused
    auto dup = sc.runIn(kNs1, rs::CreateUserOps{v(3)}, "alice");
    ASSERT_FALSE(dup.has_value());
    EXPECT_EQ(dup.error().refusal, rs::Refusal::UserExists);
    // dropping (1, alice) leaves (0, alice)
    ASSERT_TRUE(sc.runIn(kNs1, rs::DropUserOps{}, "alice").has_value());
    EXPECT_EQ(sc.s.userByName(kNs1, "alice"), nullptr);
    ASSERT_NE(sc.s.userByName(0, "alice"), nullptr);
    EXPECT_EQ(sc.s.userByName(0, "alice")->version, 1u);
    EXPECT_EQ(sc.s.tombstones().back().nsId, 1u);
    // the freed name can be taken again in ns 1, as a different object
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateUserOps{v(4)}, "alice").has_value());
    // an alter / drop in one namespace never reaches the other
    auto miss = sc.runIn(5, rs::DropUserOps{}, "alice");
    ASSERT_FALSE(miss.has_value());
    EXPECT_EQ(miss.error().refusal, rs::Refusal::UnknownUser);
    // the v2 object fingerprint differs across namespaces for identical definitions
    Script x, y;
    ASSERT_TRUE(x.run(rs::CreateUserOps{v(1)}, "bob").has_value());
    ASSERT_TRUE(y.runIn(kNs1, rs::CreateUserOps{v(1)}, "bob").has_value());
    EXPECT_EQ(rs::fingerprintOf(*x.s.userByName(0, "bob"), x.s), rs::fingerprintOf(*y.s.userByName(kNs1, "bob"), y.s));
    EXPECT_NE(x.s.digest(), y.s.digest());
}

TEST(NamespaceSecurityV2Regression, RoleMembershipSameNsOnlyRegression) {
    Script sc;
    ASSERT_TRUE(sc.run(rs::CreateRoleOps{}, "ops").has_value());            // role in ns 0
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateUserOps{v(1)}, "u").has_value());  // user in ns 1
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateRoleOps{}, "ops").has_value());    // a role of the same name in ns 1
    const uint64_t role0 = sc.s.roleByName(0, "ops")->objectId;
    const uint64_t role1 = sc.s.roleByName(kNs1, "ops")->objectId;
    // (user ns 1, role ns 0) is refused, whichever way the role is named
    auto cross = sc.runIn(kNs1, rs::GrantRoleOps{role0, "ops"}, "u");
    ASSERT_FALSE(cross.has_value());
    EXPECT_EQ(cross.error().refusal, rs::Refusal::NamespaceMismatch);
    EXPECT_TRUE(sc.s.userByName(kNs1, "u")->roles.empty());
    EXPECT_EQ(sc.s.userByName(kNs1, "u")->version, 1u) << "a refusal changes nothing";
    // the same-namespace role is fine
    ASSERT_TRUE(sc.runIn(kNs1, rs::GrantRoleOps{role1, "ops"}, "u").has_value());
    EXPECT_EQ(sc.s.userByName(kNs1, "u")->roles.count(role1), 1u);
    // and the reverse: a ns-0 user with the ns-1 role
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(2)}, "w").has_value());
    auto rev = sc.run(rs::GrantRoleOps{role1, "ops"}, "w");
    ASSERT_FALSE(rev.has_value());
    EXPECT_EQ(rev.error().refusal, rs::Refusal::NamespaceMismatch);
    // dropping the ns-1 role removes only ns-1 memberships
    ASSERT_TRUE(sc.run(rs::GrantRoleOps{role0, "ops"}, "w").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::DropRoleOps{}, "ops").has_value());
    EXPECT_TRUE(sc.s.userByName(kNs1, "u")->roles.empty());
    EXPECT_EQ(sc.s.userByName(0, "w")->roles.count(role0), 1u);
}

// ── grants ──────────────────────────────────────────────────────────────────────────────────────────────────
TEST(NamespaceSecurityV2Regression, ClusterGrantOnlyInDefaultRegression) {
    const rs::GrantSpec adminCluster = g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "");
    EXPECT_TRUE(rs::validGrant(0, "default", adminCluster));
    EXPECT_FALSE(rs::validGrant(1, "arena", adminCluster));
    EXPECT_FALSE(rs::validGrant(7, "", adminCluster));
    Script sc;
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateRoleOps{}, "r").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateUserOps{v(1)}, "u").has_value());
    for (const auto grantee : {rs::GranteeKind::Role, rs::GranteeKind::User}) {
        auto r = sc.runIn(kNs1, rs::GrantOps{grantee, adminCluster}, grantee == rs::GranteeKind::Role ? "r" : "u");
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().refusal, rs::Refusal::InvalidGrant);
    }
    ASSERT_TRUE(sc.run(rs::CreateRoleOps{}, "r0").has_value());
    EXPECT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::Role, adminCluster}, "r0").has_value()) << "ns 0 may hold a Cluster grant";
}

TEST(NamespaceSecurityV2Regression, NamespaceGrantTableRegression) {
    struct Row { Operation op; PatternKind kind; const char* name; bool ok; };
    // role of namespace "arena" (nsId 1): ops {Admin, Describe} x {own name, foreign name, empty, prefix}
    const Row rows[] = {
        {Operation::Admin,    PatternKind::Exact,  "arena",   true},
        {Operation::Admin,    PatternKind::Exact,  "other",   false},
        {Operation::Admin,    PatternKind::Exact,  "",        false},
        {Operation::Admin,    PatternKind::Prefix, "arena",   false},
        {Operation::Describe, PatternKind::Exact,  "arena",   true},
        {Operation::Describe, PatternKind::Exact,  "other",   false},
        {Operation::Describe, PatternKind::Exact,  "",        false},
        {Operation::Describe, PatternKind::Prefix, "arena",   false},
        {Operation::Read,     PatternKind::Exact,  "arena",   false},   // not a Namespace op
        {Operation::Truncate, PatternKind::Exact,  "arena",   false},
        {Operation::Create,   PatternKind::Exact,  "arena",   false},
        {Operation::Admin,    PatternKind::Exact,  "default", false},   // the default's name is foreign to "arena"
    };
    ASSERT_EQ(std::size(rows), 12u);
    for (const Row& r : rows)
        EXPECT_EQ(rs::validGrant(1, "arena", g(r.op, ResourceType::Namespace, r.kind, r.name)), r.ok)
            << "op " << int(r.op) << " kind " << int(r.kind) << " name '" << r.name << "'";
    // the default namespace names itself "default"
    EXPECT_TRUE(rs::validGrant(0, "default", g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "default")));
    EXPECT_FALSE(rs::validGrant(0, "default", g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "arena")));
    // the apply layer has no catalog: it checks the SHAPE (a namespace-name-shaped Exact name) for nsId != 0 and the
    // exact name "default" for nsId 0; the equality with the real name is the front end's (it has the catalog)
    Script sc;
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateRoleOps{}, "nsadmin").has_value());
    EXPECT_TRUE(sc.runIn(kNs1, rs::GrantOps{rs::GranteeKind::Role, g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "arena")}, "nsadmin").has_value());
    EXPECT_FALSE(sc.runIn(kNs1, rs::GrantOps{rs::GranteeKind::Role, g(Operation::Admin, ResourceType::Namespace, PatternKind::Prefix, "arena")}, "nsadmin").has_value());
    EXPECT_FALSE(sc.runIn(kNs1, rs::GrantOps{rs::GranteeKind::Role, g(Operation::Read, ResourceType::Namespace, PatternKind::Exact, "arena")}, "nsadmin").has_value());
    EXPECT_FALSE(sc.runIn(kNs1, rs::GrantOps{rs::GranteeKind::Role, g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "Not A Name")}, "nsadmin").has_value());
    ASSERT_TRUE(sc.run(rs::CreateRoleOps{}, "d").has_value());
    EXPECT_FALSE(sc.run(rs::GrantOps{rs::GranteeKind::Role, g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "arena")}, "d").has_value());
    EXPECT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::Role, g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "default")}, "d").has_value());
}

TEST(NamespaceSecurityV2Regression, TruncateOnTopicValidRegression) {
    EXPECT_TRUE(rs::validGrant(0, "default", g(Operation::Truncate, ResourceType::Topic, PatternKind::Exact, "t")));
    EXPECT_TRUE(rs::validGrant(1, "arena", g(Operation::Truncate, ResourceType::Topic, PatternKind::Prefix, "world.")));
    for (const ResourceType t : {ResourceType::View, ResourceType::Group, ResourceType::TransactionalId, ResourceType::Cluster,
                                 ResourceType::Namespace})
        EXPECT_FALSE(rs::validGrant(0, "default", g(Operation::Truncate, t, PatternKind::Exact, t == ResourceType::Cluster ? "" : "default")))
            << "Truncate on type " << int(t);
    Script sc;
    ASSERT_TRUE(sc.run(rs::CreateRoleOps{}, "r").has_value());
    EXPECT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::Role, g(Operation::Truncate, ResourceType::Topic, PatternKind::Exact, "t")}, "r").has_value());
    EXPECT_FALSE(sc.run(rs::GrantOps{rs::GranteeKind::Role, g(Operation::Truncate, ResourceType::View, PatternKind::Exact, "v")}, "r").has_value());
    // Truncate is a bit of its own (op 7) and implies Describe like every other granted op
    const std::vector<rs::GrantSpec> grants = {g(Operation::Truncate, ResourceType::Topic, PatternKind::Exact, "t")};
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "t"), rs::opBit(Operation::Truncate) | rs::opBit(Operation::Describe));
    EXPECT_EQ(rs::opBit(Operation::Truncate), 0x80);
}

TEST(NamespaceSecurityV2Regression, BootstrapOnlyInDefaultRegression) {
    Script sc;
    auto bad = sc.runIn(kNs1, rs::BootstrapOps{v(1)}, "admin");
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error().refusal, rs::Refusal::NamespaceMismatch);
    EXPECT_FALSE(sc.s.fakeSaltSeed().has_value());
    ASSERT_TRUE(sc.run(rs::BootstrapOps{v(1)}, "admin").has_value());
    EXPECT_TRUE(sc.s.hasSecurityAdmin());
}

TEST(NamespaceSecurityV2Regression, NsAdminIsNotSecurityAdminRegression) {
    // a namespace administrator in ns 1, and NO default cluster admin
    Script sc;
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateRoleOps{}, "nsadmin").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::GrantOps{rs::GranteeKind::Role, g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "arena")}, "nsadmin").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateUserOps{v(1)}, "boss").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::GrantRoleOps{sc.s.roleByName(kNs1, "nsadmin")->objectId, "nsadmin"}, "boss").has_value());
    EXPECT_FALSE(sc.s.hasSecurityAdmin()) << "a namespace admin never counts toward the last-security-admin guard";
    // it IS the admin of its namespace
    const auto admins = sc.s.nsAdminOf(kNs1, "arena");
    ASSERT_EQ(admins.size(), 1u);
    EXPECT_EQ(admins[0], sc.s.userByName(kNs1, "boss")->objectId);
    EXPECT_TRUE(sc.s.nsAdminOf(kNs1, "other").empty()) << "a grant naming another namespace is inert";
    EXPECT_TRUE(sc.s.nsAdminOf(0, "arena").empty());
    const auto eff = rs::effectivePerms(sc.s, sc.s.userByName(kNs1, "boss")->objectId, "arena");
    EXPECT_TRUE(eff.nsAdmin);
    EXPECT_EQ(eff.nsId, kNs1);
    EXPECT_FALSE(rs::effectivePerms(sc.s, sc.s.userByName(kNs1, "boss")->objectId, "other").nsAdmin);
    // bootstrapping later still works: no security admin exists
    ASSERT_TRUE(sc.run(rs::BootstrapOps{v(2)}, "root").has_value());
    EXPECT_TRUE(sc.s.hasSecurityAdmin());
    // a disabled namespace admin is not listed
    ASSERT_TRUE(sc.runIn(kNs1, rs::AlterUserOps{rs::DisableUser{}}, "boss").has_value());
    EXPECT_TRUE(sc.s.nsAdminOf(kNs1, "arena").empty());
}

TEST(NamespaceSecurityV2Regression, PatternScopedToRoleNsRegression) {
    Script sc;
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateRoleOps{}, "readers").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::GrantOps{rs::GranteeKind::Role, g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "t")}, "readers").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateUserOps{v(1)}, "u").has_value());
    ASSERT_TRUE(sc.runIn(kNs1, rs::GrantRoleOps{sc.s.roleByName(kNs1, "readers")->objectId, "readers"}, "u").has_value());
    const auto perms = rs::effectivePerms(sc.s, sc.s.userByName(kNs1, "u")->objectId, "arena");
    // an object index with t1 in ns 0 and t2 in ns 1: only t2 gets the Read bit
    struct Obj { uint32_t nsId; const char* name; uint64_t id; };
    const Obj index[] = {{0, "t1", 1}, {kNs1, "t2", 2}, {kNs1, "x", 3}, {2, "t3", 4}};
    std::map<uint64_t, uint8_t> bits;
    for (const Obj& o : index) bits[o.id] = rs::opsOn(perms, ResourceType::Topic, o.nsId, o.name);
    EXPECT_EQ(bits[1], 0) << "t1 is in namespace 0: the pattern cannot reach it";
    EXPECT_NE(bits[2] & rs::opBit(Operation::Read), 0) << "t2 is in the role's namespace";
    EXPECT_EQ(bits[3], 0) << "x does not match the prefix";
    EXPECT_EQ(bits[4], 0) << "t3 is in namespace 2";
    // the cluster grants of a default-namespace user reach default objects only
    Script cs;
    ASSERT_TRUE(cs.run(rs::CreateUserOps{v(1)}, "dba").has_value());
    ASSERT_TRUE(cs.run(rs::GrantOps{rs::GranteeKind::User, g(Operation::Drop, ResourceType::Cluster, PatternKind::Exact, "")}, "dba").has_value());
    const auto cluster = rs::effectivePerms(cs.s, cs.s.userByName(0, "dba")->objectId, "default");
    EXPECT_FALSE(cluster.nsAdmin);
    EXPECT_NE(rs::opsOn(cluster, ResourceType::Topic, 0, "anything") & rs::opBit(Operation::Drop), 0);
    EXPECT_EQ(rs::opsOn(cluster, ResourceType::Topic, 1, "anything"), 0);
}

// ── formatVer 1 stays frozen ─────────────────────────────────────────────────────────────────────────────────
TEST(NamespaceSecurityV2Regression, V1GoldenStillReplaysRegression) {
    std::ifstream in(RUNE_SECURITY_GOLDEN_DIR "/v1/basic.rsec", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rs::readRsec(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rs::golden::kBasicV1Digests));
    rs::SecurityReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto e = rs::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value()) << i;
        EXPECT_EQ(e->formatVer, rs::kFormatV1) << i;
        EXPECT_EQ(e->nsId, 0u) << i;
        auto a = r.feedDecoded((*records)[i].first, *e);
        ASSERT_TRUE(a.has_value()) << rs::describe(a.error());
        EXPECT_EQ(r.state().digest(), rs::golden::kBasicV1Digests[i]) << "offset " << i;
    }
}

TEST(NamespaceSecurityV2Regression, V1EventsAsV2ApplyIdenticallyRegression) {
    std::ifstream in(RUNE_SECURITY_GOLDEN_DIR "/v1/basic.rsec", std::ios::binary);
    auto records = rs::readRsec(in);
    ASSERT_TRUE(records.has_value());
    rs::SecurityState v1, v2;
    for (const auto& [off, bytes] : *records) {
        auto e1 = rs::decode(bytes);
        ASSERT_TRUE(e1.has_value());
        ASSERT_TRUE(rs::apply(v1, *e1, off).has_value()) << off;
        rs::SecurityEvent e2 = *e1;
        e2.formatVer = rs::kFormatV2; e2.nsId = 0;
        e2.fingerprint = rune::catalog::objectFingerprintV2(e1->fingerprint, 0);
        auto round = rs::decode(rs::encode(e2).value());
        ASSERT_TRUE(round.has_value());
        auto a2 = rs::apply(v2, *round, off);
        ASSERT_TRUE(a2.has_value()) << "offset " << off << ": " << a2.error().detail;
        EXPECT_EQ(v2.digest(), v1.digest()) << off;
    }
    EXPECT_EQ(v1.users().size(), v2.users().size());
    EXPECT_EQ(v1.tombstones().size(), v2.tombstones().size());
}

TEST(NamespaceSecurityV2Regression, PreviewMatchesApplyAndNeverMutatesRegression) {
    Script sc;
    ASSERT_TRUE(sc.runIn(kNs1, rs::CreateUserOps{v(1)}, "u").has_value());
    sc.ns = kNs1;
    const rs::SecurityEvent e = sc.make(rs::AlterUserOps{rs::DisableUser{}}, "u");
    const uint64_t before = sc.s.digest();
    auto p = rs::previewFingerprint(sc.s, e);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(sc.s.digest(), before);
    EXPECT_EQ(*p, e.fingerprint);
    EXPECT_EQ(*p, rune::catalog::objectFingerprintV2(rs::fingerprintOf([&] { auto u = *sc.s.userByName(kNs1, "u"); u.version = 2; u.enabled = false; return u; }(), sc.s), kNs1));
    auto bad = e;
    bad.fingerprint ^= 1;
    auto r = rs::apply(sc.s, bad, sc.offset);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code, rs::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(r.error().actual, *p);
}

// ── the v2 golden ───────────────────────────────────────────────────────────────────────────────────────────
TEST(SecurityV2GoldenRegression, NamespacesGoldenPinnedRegression) {
    std::ifstream in(RUNE_SECURITY_GOLDEN_DIR "/v2/namespaces.rsec", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rs::readRsec(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rs::golden::kNamespacesV2Digests));
    rs::SecurityReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << rs::describe(a.error());
        EXPECT_EQ(r.state().digest(), rs::golden::kNamespacesV2Digests[i]) << "offset " << i;
    }
    // the same user name lives in two namespaces
    const auto& s = r.state();
    ASSERT_NE(s.userByName(0, "alice"), nullptr);
    EXPECT_EQ(s.userByName(kNs1, "alice"), nullptr) << "dropped at the end";
    EXPECT_NE(s.roleByName(kNs1, "ops"), nullptr);
}

TEST(SecurityV2GoldenRegression, TheV2GoldenScriptStillProducesTheCheckedInBytesRegression) {
    std::ifstream in(RUNE_SECURITY_GOLDEN_DIR "/v2/namespaces.rsec", std::ios::binary);
    auto records = rs::readRsec(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rs::testing::goldenScriptV2();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        EXPECT_EQ((*records)[i].second, rs::encode(script[i]).value()) << "offset " << i;
        auto e = rs::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value());
        EXPECT_EQ(*e, script[i]) << "offset " << i;
        EXPECT_EQ(e->formatVer, rs::kFormatV2);
    }
}
