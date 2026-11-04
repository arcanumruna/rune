// CatalogPrincipalTest.cpp - authentication Task 12 (fix-on-sight): the catalog event's principal.authenticated was
// always empty; it is now the session's user, and a security event records the user who ran it.
#include "support/AuthLogReader.h"
#include "support/AuthRig.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/security/SecurityEvent.h>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;


TEST(CatalogPrincipalRegression, DdlEventsRecordTheAuthenticatedUserRegression) {
    AuthRig r;
    std::string ws1;
    const int64_t sid = r.newUser({exact(SO::Create, ST::Topic, "pt1")}, &ws1);
    ASSERT_EQ(r.defineTopicCode(sid, "pt1"), 0);

    BrokerNode& n = r.broker().node();
    const auto events = readCatalogEvents(n);
    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().principal.authenticated, ws1) << "the last catalog event is the DefineTopic by the user";
    // The builder's own DDL (made during the rig's setup) records "builder".
    bool sawBuilder = false;
    for (const auto& e : events) sawBuilder = sawBuilder || e.principal.authenticated == "builder";
    EXPECT_TRUE(sawBuilder);
}

TEST(CatalogPrincipalRegression, SecurityEventsRecordTheUserWhoRanThemRegression) {
    AuthRig r;
    std::string self;
    const int64_t sid = r.newUser({}, &self);
    ASSERT_EQ(r.wire().alterPassword(r.ch(), sid, self, pc::UserChangeKind::AddPassword, "second-password-123").errorCode, 0u);

    BrokerNode& n = r.broker().node();
    const auto events = readSecurityEvents(n);
    ASSERT_GE(events.size(), 3u);
    EXPECT_EQ(events.front().principal.authenticated, "bootstrap");
    EXPECT_EQ(events.back().principal.authenticated, self) << "a self password change is recorded as the user";
    bool sawAdmin = false;
    for (const auto& e : events) sawAdmin = sawAdmin || e.principal.authenticated == "admin";
    EXPECT_TRUE(sawAdmin) << "CREATE USER by admin records admin";
}

// #652: a tenant's DDL recorded the bare local name, so `pa/alice` and `alice` (default) wrote the same audit
// principal.  The principal is now the qualified login name; `default` users stay bare.
TEST(CatalogPrincipalRegression, TenantDdlPrincipalIsQualifiedRegression) {
    AuthRig r;
    r.createNamespace("pa");
    r.broker().createUser("pa/alice", AuthRig::kPw, {exact(SO::Create, ST::Topic, "x")});
    r.broker().createUser("pa/boss", AuthRig::kPw, {exact(SO::Admin, ST::Namespace, "pa")});
    r.broker().createUser("carl", AuthRig::kPw, {exact(SO::Create, ST::Topic, "ct")});

    // leg 1: catalog DDL by a tenant user
    const int64_t alice = r.login("pa/alice", AuthRig::kPw, pc::kClientVersionNamespaces);
    ASSERT_EQ(r.defineTopicCode(alice, "x"), 0);
    {
        const auto events = readCatalogEvents(r.broker().node());
        ASSERT_FALSE(events.empty());
        EXPECT_EQ(events.back().principal.authenticated, "pa/alice") << "the tenant user's DDL is recorded qualified";
    }

    // leg 2: security DDL by a namespace admin: the caller still resolves (SecurityAdmin parses the qualified name)
    const int64_t boss = r.login("pa/boss", AuthRig::kPw, pc::kClientVersionNamespaces);
    const auto created = r.wire().createUser(r.ch(), boss, "pa/bob", AuthRig::kPw);
    EXPECT_EQ(created.errorCode, 0u) << created.detail;
    {
        const auto events = readSecurityEvents(r.broker().node());
        ASSERT_FALSE(events.empty());
        EXPECT_EQ(events.back().principal.authenticated, "pa/boss");
    }

    // leg 3: a `default` user stays bare
    const int64_t carl = r.login("carl", AuthRig::kPw, pc::kClientVersionNamespaces);
    ASSERT_EQ(r.defineTopicCode(carl, "ct"), 0);
    {
        const auto events = readCatalogEvents(r.broker().node());
        ASSERT_FALSE(events.empty());
        EXPECT_EQ(events.back().principal.authenticated, "carl");
    }
}
