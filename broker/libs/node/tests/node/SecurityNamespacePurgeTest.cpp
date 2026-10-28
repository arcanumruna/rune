// SecurityNamespacePurgeTest.cpp — namespaces Phase 2 Task 6a: the `__security` leader issues NamespacePurged (kind 11)
// for a DELETING namespace that still has security records, derived from the two applied states on every pass.
// Driven through a real SecurityAdmin over a real `__security` replica (SecurityDdlRig) and a real CatalogState built
// by the catalog's own apply rules (the AdminWorker only owns and binds the same two objects).
#include "support/SecurityDdlRig.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/protocol/client/SecurityMessages.h>
#include <rune/security/SecurityQueries.h>
#include <rune/security/SecurityState.h>

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <vector>

using rune::node::SecurityDdlMsg;
using rune::node::test::SecurityDdlRig;
namespace rc  = rune::catalog;
namespace sec = rune::security;

namespace {

constexpr uint16_t kOK = 0;
constexpr uint32_t kNsA = 1, kNsB = 2;
constexpr int kCap = 2000;   // hard cap on every drive loop

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
    Catalog() {
        go(rc::NamespaceCreatedOps{"a", {}}, "a", kNsA, 0);
        go(rc::NamespaceCreatedOps{"b", {}}, "b", kNsB, 0);
    }
    void deleteNs(const std::string& name, uint32_t id) { go(rc::NamespaceDeletedOps{}, name, id, s.namespaces().byId(id)->version); }
};

void ok(SecurityDdlRig& rig, SecurityDdlMsg m) {
    m.meta.authenticated = "admin"; m.callerNsId = 0; m.denial.clear();
    const auto a = rig.ddl(std::move(m));
    if (a.code != kOK) throw std::logic_error("setup step refused: " + a.detail);
}

std::size_t purgesLogged(SecurityDdlRig& rig) {
    std::size_t n = 0;
    for (const auto& r : rig.recordedEvents()) n += r.event.kind == sec::EventKind::NamespacePurged ? 1 : 0;
    return n;
}

/// bootstrap admin; a/u with a role-held ADMIN ON NAMESPACE a; the admin holds a qualified grant into a; b/v untouched.
void fixture(SecurityDdlRig& rig, Catalog& cat) {
    rig.becomeServingLeader();
    rig.admin().bindCatalog(&cat.s);
    const auto boot = rig.bootstrap(rig.bootstrapMsg("admin", "the-admin-password-1"));
    ASSERT_EQ(boot.code, kOK) << boot.detail;
    ok(rig, rig.createRole("a/r"));
    ok(rig, rig.grant(false, sec::GranteeKind::Role, "a/r", sec::Operation::Admin, sec::ResourceType::Namespace,
                      sec::PatternKind::Exact, "a"));
    ok(rig, rig.createUser("a/u", "a-user-password-11"));
    ok(rig, rig.grantRole(false, "a/u", "a/r"));
    ok(rig, rig.createUser("b/v", "b-user-password-11"));
    ok(rig, rig.grant(false, sec::GranteeKind::User, "admin", sec::Operation::Read, sec::ResourceType::Topic,
                      sec::PatternKind::Prefix, "a/"));
}

template <class Pred> bool pumpUntil(SecurityDdlRig& rig, Pred p) {
    for (int i = 0; i < kCap && !p(); ++i) rig.pump();
    return p();
}
} // namespace

TEST(SecurityNamespacePurgeRegression, LeaderIssuesPurgeForDeletingNamespaceRegression) {
    SecurityDdlRig rig("nspurge_issue");
    Catalog cat;
    fixture(rig, cat);
    ASSERT_TRUE(sec::hasNamespaceRecords(rig.state(), kNsA, "a"));
    EXPECT_EQ(purgesLogged(rig), 0u) << "an ACTIVE namespace is never purged";
    for (int i = 0; i < 50; ++i) rig.pump();
    EXPECT_EQ(purgesLogged(rig), 0u);
    cat.deleteNs("a", kNsA);
    ASSERT_TRUE(pumpUntil(rig, [&] { return !sec::hasNamespaceRecords(rig.state(), kNsA, "a"); }))
        << "the purge was never issued and applied within the cap";
    for (int i = 0; i < 100; ++i) rig.pump();   // and never issued twice
    std::vector<SecurityDdlRig::Recorded> purges;
    for (auto& r : rig.recordedEvents()) if (r.event.kind == sec::EventKind::NamespacePurged) purges.push_back(r);
    ASSERT_EQ(purges.size(), 1u);
    EXPECT_EQ(purges[0].event.nsId, kNsA);
    EXPECT_EQ(purges[0].event.objectName, "a");
    EXPECT_EQ(purges[0].event.formatVer, sec::kFormatV2);
    EXPECT_EQ(rig.admin().purgesIssued(), 1u);
    EXPECT_EQ(rig.state().userByName(kNsA, "u"), nullptr);
    EXPECT_EQ(rig.state().roleByName(kNsA, "r"), nullptr);
    ASSERT_NE(rig.state().userByName(kNsA + 1, "v"), nullptr) << "b/v untouched";
    const sec::UserObject* admin = rig.state().userByName(0, "admin");
    ASSERT_NE(admin, nullptr);
    for (const auto& g : admin->grants) EXPECT_NE(g.resource.name, "a/");
    // the leader serves again afterwards
    ok(rig, rig.createUser("late", "late-user-password-1"));
}

TEST(SecurityNamespacePurgeRegression, NoPurgeWithoutRecordsRegression) {
    SecurityDdlRig rig("nspurge_none");
    Catalog cat;
    rig.becomeServingLeader();
    rig.admin().bindCatalog(&cat.s);
    ASSERT_EQ(rig.bootstrap(rig.bootstrapMsg("admin", "the-admin-password-1")).code, kOK);
    const std::size_t before = rig.recordedEvents().size();
    cat.deleteNs("b", kNsB);   // b has no security records
    for (int i = 0; i < 200; ++i) rig.pump();
    EXPECT_EQ(rig.recordedEvents().size(), before);
    EXPECT_EQ(rig.admin().purgesIssued(), 0u);
}

TEST(SecurityNamespacePurgeRegression, ReissuedAfterLeaderChangeRegression) {
    SecurityDdlRig rig("nspurge_reissue");
    Catalog cat;
    fixture(rig, cat);
    // The followers stop fetching: the purge is appended but cannot commit, so it is not applied.
    rig.silence(2); rig.silence(3);
    cat.deleteNs("a", kNsA);
    ASSERT_TRUE(pumpUntil(rig, [&] { return rig.admin().purgesIssued() == 1; }));
    ASSERT_TRUE(sec::hasNamespaceRecords(rig.state(), kNsA, "a")) << "uncommitted: not applied";
    // Leadership is lost and regained: nothing in memory carries over; the state is re-derived.
    rig.becomeFollower(2);
    rig.unsilence(2); rig.unsilence(3);
    rig.becomeServingLeader();
    ASSERT_TRUE(pumpUntil(rig, [&] { return !sec::hasNamespaceRecords(rig.state(), kNsA, "a"); }));
    for (int i = 0; i < 100; ++i) rig.pump();
    // The record is either the original (committed under the new epoch) or one re-issue; never more than two, and the
    // state converges with every record gone.
    EXPECT_GE(purgesLogged(rig), 1u);
    EXPECT_LE(purgesLogged(rig), 2u);
    EXPECT_FALSE(sec::hasNamespaceRecords(rig.state(), kNsA, "a"));
}

TEST(SecurityNamespacePurgeRegression, FollowerNeverIssuesRegression) {
    SecurityDdlRig rig("nspurge_follower");
    Catalog cat;
    fixture(rig, cat);
    rig.becomeFollower(2);
    cat.deleteNs("a", kNsA);
    const std::size_t before = rig.recordedEvents().size();
    for (int i = 0; i < 300; ++i) rig.pump();
    EXPECT_EQ(rig.recordedEvents().size(), before);
    EXPECT_EQ(rig.admin().purgesIssued(), 0u);
    EXPECT_TRUE(sec::hasNamespaceRecords(rig.state(), kNsA, "a"));
}
