// NamespaceEscalationTest.cpp — namespaces Task 8 (spec §4): the `__security` leader resolves every security DDL's
// target name (`ns/local` | `local` = the caller's namespace) through the catalog's namespace registry, requires the
// namespace ACTIVE, stamps the event's nsId and runs the namespace guard (nsGuard):
//   * a cluster admin may do anything (like root);
//   * a namespace admin of `a` manages users, roles and grants of `a` only — never ADMIN ON CLUSTER, never ADMIN ON
//     NAMESPACE, never another admin, never a membership in a privileged role;
//   * anything in another namespace answers exactly what a namespace that does not exist answers (Missing).
// Driven through a real SecurityAdmin over a real `__security` replica (SecurityDdlRig) and a real CatalogState built
// by the catalog's own apply rules.
#include "support/SecurityDdlRig.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/catalog/NsName.h>
#include <rune/protocol/client/SecurityMessages.h>
#include <rune/security/SecurityState.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <string>
#include <vector>

using rune::node::SecurityDdlMsg;
using rune::node::test::SecurityDdlRig;
namespace rc  = rune::catalog;
namespace sec = rune::security;
namespace pc  = rune::protocol::client;
using Ch = pc::UserChangeKind;

namespace {

constexpr uint16_t kOK = 0, kInvalidRequest = 39, kNotAuthorized = 45;
constexpr uint32_t kNsA = 1, kNsB = 2, kNsC = 3;
const std::string kClusterDenial = "ADMIN ON CLUSTER required";

/// A catalog with namespaces a (1), b (2, optional) and c (3, SUSPENDED, optional), applied by the catalog's own rules.
struct Catalog {
    rc::CatalogState s;
    int64_t          off = 0;
    void go(rc::Ops ops, const std::string& name, uint32_t nsId, uint32_t prev) {
        rc::CatalogEvent e;
        e.formatVer = rc::kFormatV2;
        e.nsId = nsId;
        e.kind = rc::kindOf(ops);
        e.ops = std::move(ops);
        e.objectName = name;
        e.objectId = 0;
        e.prevVersion = prev;
        e.newVersion = prev + 1;
        auto fp = rc::previewFingerprint(s, e);
        if (!fp) throw std::logic_error("Catalog: preview refused " + name);
        e.fingerprint = *fp;
        if (!rc::apply(s, e, off++)) throw std::logic_error("Catalog: apply refused " + name);
    }
    explicit Catalog(bool withB = true, bool withC = true) {
        go(rc::NamespaceCreatedOps{"a", {}}, "a", kNsA, 0);
        if (withB) go(rc::NamespaceCreatedOps{"b", {}}, "b", kNsB, 0);
        if (withC) {
            if (!withB) throw std::logic_error("Catalog: c needs b (dense ids)");
            go(rc::NamespaceCreatedOps{"c", {}}, "c", kNsC, 0);
            go(rc::NamespaceStateChangedOps{rc::NsStateTag::SuspendedRejectAll}, "c", kNsC,
               s.namespaces().byId(kNsC)->version);
        }
    }
};

/// NsAdminAProven: a/nsadmin's own credential change after the gateway checked its proof (no denial, proofChecked).
enum class Who { ClusterAdmin, NsAdminA, NsAdminAProven, PlainA, PlainB };

/// Stamp `m` as the gateway would for `who`: a cluster admin passes the gateway's ADMIN ON CLUSTER check (no denial);
/// everyone else carries the gateway's denial, which the leader overrides only for a namespace admin's own namespace.
SecurityDdlMsg as(Who who, SecurityDdlMsg m) {
    switch (who) {
    case Who::ClusterAdmin: m.meta.authenticated = "admin";   m.callerNsId = 0;   m.denial.clear(); break;
    case Who::NsAdminA:     m.meta.authenticated = "nsadmin"; m.callerNsId = kNsA; m.denial = kClusterDenial; break;
    case Who::NsAdminAProven: m.meta.authenticated = "nsadmin"; m.callerNsId = kNsA; m.denial.clear(); break;
    case Who::PlainA:       m.meta.authenticated = "plain";   m.callerNsId = kNsA; m.denial = kClusterDenial; break;
    case Who::PlainB:       m.meta.authenticated = "u";       m.callerNsId = kNsB; m.denial = kClusterDenial; break;
    }
    m.proofChecked = who == Who::NsAdminAProven ? 1 : 0;
    return m;
}

void ok(SecurityDdlRig& rig, SecurityDdlMsg m) {
    const auto a = rig.ddl(as(Who::ClusterAdmin, std::move(m)));
    if (a.code != kOK) throw std::logic_error("setup step refused: " + a.detail);
}

/// The fixture state: bootstrap admin; a/r holds ADMIN ON NAMESPACE a; a/nsadmin and a/nsadmin2 are members of it;
/// a/plain, a/pr (a plain role), b/u and b/r exist.
void setUp(SecurityDdlRig& rig, const rc::CatalogState& cat) {
    rig.becomeServingLeader();
    rig.admin().bindCatalog(&cat);
    const auto boot = rig.bootstrap(rig.bootstrapMsg("admin", "the-admin-password-1"));
    if (boot.code != kOK) throw std::logic_error("bootstrap refused: " + boot.detail);
    ok(rig, rig.createRole("a/r"));
    ok(rig, rig.grant(false, sec::GranteeKind::Role, "a/r", sec::Operation::Admin, sec::ResourceType::Namespace,
                      sec::PatternKind::Exact, "a"));
    ok(rig, rig.createUser("a/nsadmin", "nsadmin-password-1"));
    ok(rig, rig.createUser("a/nsadmin2", "nsadmin-password-2"));
    ok(rig, rig.grantRole(false, "a/nsadmin", "a/r"));
    ok(rig, rig.grantRole(false, "a/nsadmin2", "a/r"));
    ok(rig, rig.createUser("a/plain", "plain-password-11"));
    ok(rig, rig.createRole("a/pr"));
    ok(rig, rig.createUser("b/u", "b-user-password-1"));
    ok(rig, rig.createRole("b/r"));
}

enum class Expect { Allow, NotAuthorized, Missing, Invalid, Suspended };

struct Row {
    const char* name;
    Who         who;
    std::function<SecurityDdlMsg(SecurityDdlRig&)> msg;
    Expect      expect;
    std::function<void(const sec::SecurityState&)> check = {};   // after an Allow
};

SecurityDdlMsg grantTo(SecurityDdlRig& rig, sec::GranteeKind k, const std::string& name, sec::Operation op,
                       sec::ResourceType t, const std::string& pattern,
                       sec::PatternKind pk = sec::PatternKind::Exact, bool revoke = false) {
    return rig.grant(revoke, k, name, op, t, pk, pattern);
}

const std::vector<Row>& rows() {
    using GK = sec::GranteeKind;
    using Op = sec::Operation;
    using RT = sec::ResourceType;
    using K  = pc::SecurityOpKind;
    static const std::vector<Row> r = {
        // ── privilege minting (ns admin of a) ───────────────────────────────────────────────────────────────────
        {"01 grant ADMIN ON CLUSTER", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::Role, "pr", Op::Admin, RT::Cluster, ""); }, Expect::NotAuthorized},
        {"02 grant ADMIN ON NAMESPACE a", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::Role, "pr", Op::Admin, RT::Namespace, "a"); }, Expect::NotAuthorized},
        {"03 grant ADMIN ON NAMESPACE b", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::Role, "pr", Op::Admin, RT::Namespace, "b"); }, Expect::Missing},
        {"04 grant to a role in b", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::Role, "b/r", Op::Read, RT::Topic, "t"); }, Expect::Missing},
        {"05 add b/u to role a/r", Who::NsAdminA, [](auto& g) { return g.grantRole(false, "b/u", "a/r"); },
         Expect::Missing},
        {"06 create user b/u2", Who::NsAdminA, [](auto& g) { return g.createUser("b/u2", "some-password-12"); },
         Expect::Missing},
        {"07 create user u (=> a/u)", Who::NsAdminA, [](auto& g) { return g.createUser("u", "some-password-12"); },
         Expect::Allow,
         [](const sec::SecurityState& s) {
             const auto* u = s.userByName(kNsA, "u");
             ASSERT_NE(u, nullptr);
             EXPECT_EQ(u->nsId, kNsA);
             EXPECT_NE(s.userByName(kNsB, "u")->objectId, u->objectId) << "b/u is a different user";
         }},
        {"08 create role (=> a/x)", Who::NsAdminA, [](auto& g) { return g.createRole("x"); }, Expect::Allow,
         [](const sec::SecurityState& s) { ASSERT_NE(s.roleByName(kNsA, "x"), nullptr); }},
        {"08b grant READ ON TOPIC t to a/pr", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::Role, "pr", Op::Read, RT::Topic, "t"); }, Expect::Allow,
         [](const sec::SecurityState& s) { EXPECT_EQ(s.roleByName(kNsA, "pr")->grants.size(), 1u); }},
        {"09 grant TRUNCATE ON TOPIC t", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::User, "plain", Op::Truncate, RT::Topic, "t"); }, Expect::Allow,
         [](const sec::SecurityState& s) { EXPECT_EQ(s.userByName(kNsA, "plain")->grants.size(), 1u); }},
        {"09b grant READ qualified a/pr", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::Role, "a/pr", Op::Read, RT::Topic, "t"); }, Expect::Allow},
        {"10 drop a fellow ns admin", Who::NsAdminA,
         [](auto& g) { return g.simple(K::DropUser, "nsadmin2", "DROP USER nsadmin2;"); }, Expect::NotAuthorized},
        {"10b disable a fellow ns admin", Who::NsAdminA, [](auto& g) { return g.alterUser("nsadmin2", Ch::Disable); },
         Expect::NotAuthorized},
        {"11 change a fellow ns admin's password", Who::NsAdminA,
         [](auto& g) {
             auto m = g.alterUser("nsadmin2", Ch::SetPassword);
             m.verifier = SecurityDdlRig::verifierFor("hijacked-password-1");
             return m;
         },
         Expect::NotAuthorized},
        // PR #642 review F1: the session alone never changes its own credential (issue #114) - without the gateway's
        // proof check it is refused, bare or qualified name; with it, allowed.
        // Mutation: drop `ownCredentialUnproven` from nsGuard -> 12 and 12-q are Allow.
        {"12 change its own password without a proof", Who::NsAdminA,
         [](auto& g) {
             auto m = g.alterUser("nsadmin", Ch::SetPassword);
             m.verifier = SecurityDdlRig::verifierFor("new-own-password-1");
             return m;
         },
         Expect::NotAuthorized},
        {"12-q add its own password, qualified a/nsadmin, without a proof", Who::NsAdminA,
         [](auto& g) {
             auto m = g.alterUser("a/nsadmin", Ch::AddPassword);
             m.verifier = SecurityDdlRig::verifierFor("attacker-password-1");
             return m;
         },
         Expect::NotAuthorized},
        {"12-p change its own password with a checked proof", Who::NsAdminAProven,
         [](auto& g) {
             auto m = g.alterUser("a/nsadmin", Ch::SetPassword);
             m.verifier = SecurityDdlRig::verifierFor("new-own-password-1");
             return m;
         },
         Expect::Allow},
        {"12b revoke the role holding ns admin from a fellow", Who::NsAdminA,
         [](auto& g) { return g.grantRole(true, "nsadmin2", "r"); }, Expect::NotAuthorized},
        {"12c add plain to the ns admin role", Who::NsAdminA, [](auto& g) { return g.grantRole(false, "plain", "r"); },
         Expect::NotAuthorized},
        {"12d drop the ns admin role", Who::NsAdminA,
         [](auto& g) { return g.simple(K::DropRole, "r", "DROP ROLE r;"); }, Expect::NotAuthorized},
        {"12e grant to the ns admin role", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::Role, "r", Op::Read, RT::Topic, "t"); }, Expect::NotAuthorized},
        {"12f revoke ADMIN ON NAMESPACE a from the role", Who::NsAdminA,
         [](auto& g) { return grantTo(g, GK::Role, "r", Op::Admin, RT::Namespace, "a", sec::PatternKind::Exact, true); },
         Expect::NotAuthorized},
        {"12g add plain to a plain role", Who::NsAdminA, [](auto& g) { return g.grantRole(false, "plain", "pr"); },
         Expect::Allow,
         [](const sec::SecurityState& s) {
             EXPECT_EQ(s.userByName(kNsA, "plain")->roles.count(s.roleByName(kNsA, "pr")->objectId), 1u);
         }},
        {"12h drop a plain user", Who::NsAdminA,
         [](auto& g) { return g.simple(K::DropUser, "plain", "DROP USER plain;"); }, Expect::Allow,
         [](const sec::SecurityState& s) { EXPECT_EQ(s.userByName(kNsA, "plain"), nullptr); }},
        {"12i unknown user in own ns", Who::NsAdminA,
         [](auto& g) { return g.simple(K::DropUser, "ghost", "DROP USER ghost;"); }, Expect::Invalid},
        {"12j the default namespace", Who::NsAdminA,
         [](auto& g) { return g.simple(K::DropUser, "default/admin", "DROP USER default/admin;"); }, Expect::Missing},
        {"12k a malformed name", Who::NsAdminA, [](auto& g) { return g.createUser("a/b/c", "some-password-12"); },
         Expect::Invalid},
        // ── plain users ─────────────────────────────────────────────────────────────────────────────────────────
        {"13 plain user creates a user", Who::PlainA, [](auto& g) { return g.createUser("v", "some-password-12"); },
         Expect::NotAuthorized},
        {"13b plain user drops an unknown user", Who::PlainA,
         [](auto& g) { return g.simple(K::DropUser, "ghost", "DROP USER ghost;"); }, Expect::NotAuthorized},
        {"14 plain user of b creates in a", Who::PlainB, [](auto& g) { return g.createUser("a/v", "some-password-12"); },
         Expect::Missing},
        {"14b plain user of b grants in a", Who::PlainB,
         [](auto& g) { return grantTo(g, GK::Role, "a/pr", Op::Read, RT::Topic, "t"); }, Expect::Missing},
        // ── cluster admin ───────────────────────────────────────────────────────────────────────────────────────
        {"15 cluster admin grants ADMIN ON NAMESPACE a to a/u", Who::ClusterAdmin,
         [](auto& g) { return grantTo(g, GK::User, "a/plain", Op::Admin, RT::Namespace, "a"); }, Expect::Allow,
         [](const sec::SecurityState& s) {
             const auto admins = s.nsAdminOf(kNsA, "a");
             EXPECT_EQ(std::count(admins.begin(), admins.end(), s.userByName(kNsA, "plain")->objectId), 1);
         }},
        {"16 cluster admin grants ADMIN ON NAMESPACE a to a role in b", Who::ClusterAdmin,
         [](auto& g) { return grantTo(g, GK::Role, "b/r", Op::Admin, RT::Namespace, "a"); }, Expect::Invalid},
        {"16b cluster admin creates in b", Who::ClusterAdmin,
         [](auto& g) { return g.createUser("b/u2", "some-password-12"); }, Expect::Allow,
         [](const sec::SecurityState& s) { EXPECT_NE(s.userByName(kNsB, "u2"), nullptr); }},
        {"16c cluster admin, unknown namespace", Who::ClusterAdmin,
         [](auto& g) { return g.createUser("zz/u", "some-password-12"); }, Expect::Missing},
        {"16d cluster admin, suspended namespace", Who::ClusterAdmin,
         [](auto& g) { return g.createUser("c/u", "some-password-12"); }, Expect::Suspended},
        {"16e cluster admin, unqualified = default", Who::ClusterAdmin,
         [](auto& g) { return g.createUser("dflt", "some-password-12"); }, Expect::Allow,
         [](const sec::SecurityState& s) { EXPECT_NE(s.userByName(rc::kDefaultNsId, "dflt"), nullptr); }},
    };
    return r;
}

}  // namespace

TEST(NamespaceEscalationRegression, GuardMatrix) {
    int i = 0;
    for (const Row& row : rows()) {
        SCOPED_TRACE(row.name);
        Catalog cat;
        SecurityDdlRig rig(("nsesc" + std::to_string(i++)).c_str(), 1);
        setUp(rig, cat.s);
        const std::size_t before = rig.recordedEvents().size();
        const auto a = rig.ddl(as(row.who, row.msg(rig)));
        const std::size_t after = rig.recordedEvents().size();
        switch (row.expect) {
        case Expect::Allow:
            ASSERT_EQ(a.code, kOK) << a.detail;
            EXPECT_EQ(after, before + 1);
            if (row.check) row.check(rig.state());
            break;
        case Expect::NotAuthorized:
            EXPECT_EQ(a.code, kNotAuthorized) << a.detail;
            EXPECT_EQ(after, before) << "a refusal is never appended";
            break;
        case Expect::Missing:
            EXPECT_EQ(a.code, kInvalidRequest) << a.detail;
            EXPECT_TRUE(a.detail.starts_with("no namespace '")) << a.detail;
            EXPECT_EQ(after, before);
            break;
        case Expect::Invalid:
            EXPECT_EQ(a.code, kInvalidRequest) << a.detail;
            EXPECT_FALSE(a.detail.starts_with("no namespace '")) << a.detail;
            EXPECT_EQ(after, before);
            break;
        case Expect::Suspended:
            EXPECT_EQ(a.code, kInvalidRequest) << a.detail;
            EXPECT_NE(a.detail.find("is suspended"), std::string::npos) << a.detail;
            EXPECT_EQ(after, before);
            break;
        }
    }
}

TEST(NamespaceEscalationRegression, EventsCarryTheResolvedNamespaceAndLocalName) {
    Catalog cat;
    SecurityDdlRig rig("nsesc_stamp", 1);
    setUp(rig, cat.s);
    const auto a = rig.ddl(as(Who::NsAdminA, rig.createUser("w", "some-password-12")));
    ASSERT_EQ(a.code, kOK) << a.detail;
    const auto ev = rig.recordedEvents();
    ASSERT_FALSE(ev.empty());
    EXPECT_EQ(ev.back().event.nsId, kNsA);
    EXPECT_EQ(ev.back().event.objectName, "w");
    // A cluster admin's qualified name lands in that namespace too.
    ASSERT_EQ(rig.ddl(as(Who::ClusterAdmin, rig.createRole("b/w"))).code, kOK);
    EXPECT_EQ(rig.recordedEvents().back().event.nsId, kNsB);
    EXPECT_EQ(rig.recordedEvents().back().event.objectName, "w");
}

// A foreign namespace answers the SAME bytes as one that does not exist: a non-cluster-admin learns nothing.
TEST(NamespaceEscalationRegression, MissingResponseBytesEqualRegression) {
    Catalog withB;
    Catalog withoutB(false, false);
    SecurityDdlRig r1("nsesc_miss1", 1), r2("nsesc_miss2", 1);
    r1.becomeServingLeader();
    r1.admin().bindCatalog(&withB.s);
    ASSERT_EQ(r1.bootstrap(r1.bootstrapMsg("admin", "the-admin-password-1")).code, kOK);
    ok(r1, r1.createRole("a/r"));
    ok(r1, r1.grant(false, sec::GranteeKind::Role, "a/r", sec::Operation::Admin, sec::ResourceType::Namespace,
                    sec::PatternKind::Exact, "a"));
    ok(r1, r1.createUser("a/nsadmin", "nsadmin-password-1"));
    ok(r1, r1.grantRole(false, "a/nsadmin", "a/r"));
    ok(r1, r1.createRole("b/r"));
    r2.becomeServingLeader();
    r2.admin().bindCatalog(&withoutB.s);
    ASSERT_EQ(r2.bootstrap(r2.bootstrapMsg("admin", "the-admin-password-1")).code, kOK);
    ok(r2, r2.createRole("a/r"));
    ok(r2, r2.grant(false, sec::GranteeKind::Role, "a/r", sec::Operation::Admin, sec::ResourceType::Namespace,
                    sec::PatternKind::Exact, "a"));
    ok(r2, r2.createUser("a/nsadmin", "nsadmin-password-1"));
    ok(r2, r2.grantRole(false, "a/nsadmin", "a/r"));

    using Make = std::function<SecurityDdlMsg(SecurityDdlRig&)>;
    const std::vector<Make> probes = {
        [](auto& g) { return g.createUser("b/u", "some-password-12"); },
        [](auto& g) { return g.simple(pc::SecurityOpKind::DropRole, "b/r", "DROP ROLE b/r;"); },
        [](auto& g) { return g.simple(pc::SecurityOpKind::DropRole, "b/ghost", "DROP ROLE b/ghost;"); },
        [](auto& g) { return g.grant(false, sec::GranteeKind::Role, "b/r", sec::Operation::Read, sec::ResourceType::Topic,
                                     sec::PatternKind::Exact, "t"); },
        [](auto& g) { return g.grant(false, sec::GranteeKind::Role, "a/r", sec::Operation::Admin,
                                     sec::ResourceType::Namespace, sec::PatternKind::Exact, "b"); },
        [](auto& g) { return g.grantRole(false, "a/nsadmin", "b/r"); },
    };
    for (Who who : {Who::NsAdminA, Who::PlainA}) {
        for (std::size_t i = 0; i < probes.size(); ++i) {
            SCOPED_TRACE("probe " + std::to_string(i) + " who " + std::to_string(static_cast<int>(who)));
            const auto existing = r1.ddl(as(who, probes[i](r1)));
            const auto missing = r2.ddl(as(who, probes[i](r2)));
            EXPECT_EQ(existing.code, kInvalidRequest);
            EXPECT_EQ(existing.code, missing.code);
            EXPECT_EQ(existing.detail, missing.detail);
            EXPECT_EQ(existing.detail, "no namespace 'b'");
            EXPECT_EQ(existing.objectId, missing.objectId);
            EXPECT_EQ(existing.version, missing.version);
            EXPECT_EQ(existing.gateOffset, missing.gateOffset);
        }
    }
}

// Issue #121 is unchanged: the last enabled CLUSTER admin cannot be dropped; the last namespace admin can.
TEST(NamespaceEscalationRegression, LastSecurityAdminUnaffectedRegression) {
    Catalog cat;
    SecurityDdlRig rig("nsesc_last", 1);
    setUp(rig, cat.s);
    auto a = rig.ddl(as(Who::ClusterAdmin, rig.simple(pc::SecurityOpKind::DropUser, "admin", "DROP USER admin;")));
    EXPECT_EQ(a.code, kInvalidRequest);
    EXPECT_NE(a.detail.find("last enabled security admin"), std::string::npos) << a.detail;
    ASSERT_NE(rig.state().userByName(rc::kDefaultNsId, "admin"), nullptr);
    // Both namespace admins of a dropped by the cluster admin: no #121 refusal (they hold no ADMIN ON CLUSTER).
    a = rig.ddl(as(Who::ClusterAdmin, rig.simple(pc::SecurityOpKind::DropUser, "a/nsadmin2", "DROP USER a/nsadmin2;")));
    ASSERT_EQ(a.code, kOK) << a.detail;
    a = rig.ddl(as(Who::ClusterAdmin, rig.simple(pc::SecurityOpKind::DropUser, "a/nsadmin", "DROP USER a/nsadmin;")));
    ASSERT_EQ(a.code, kOK) << a.detail;
    EXPECT_TRUE(rig.state().nsAdminOf(kNsA, "a").empty());
}
