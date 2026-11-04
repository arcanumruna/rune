// NamespaceUsageRegressionTest.cpp - namespaces Phase 2 Task 16 (D29): NamespaceUsage (84) -> NamespaceUsageResponse (85) through
// the REAL client against a REAL authenticating BrokerNode on the default config.
//
// Interaction matrix (caller x target namespace x object state):
//   | caller                                   | target       | state                         | expected                                        | test
//   |------------------------------------------|--------------|-------------------------------|-------------------------------------------------|-----
//   | cluster admin                            | `u`          | 3 topics / 7 partitions / 2 views | usage 3 topics, 7 partitions (max 3), 2 views; limits echoed | NamespaceUsageRegression.ValuesMatchCountsRegression
//   | cluster admin                            | `u`          | a topic is dropped            | the counts follow                               | NamespaceUsageRegression.ValuesMatchCountsRegression
//   | user of `u` with DESCRIBE ON NAMESPACE u | `u`          | same                          | the same answer                                 | NamespaceUsageRegression.NamespaceUserWithDescribeRegression
//   | user of `u` without the grant            | `u`          | -                             | NotAuthorized (45)                              | NamespaceUsageRegression.NotAuthorizedOutsideNamespaceRegression
//   | user of `a` with DESCRIBE ON NAMESPACE a | `b` / unknown / `default` | -                | NotAuthorized (45): never says whether it exists | NamespaceUsageRegression.NotAuthorizedOutsideNamespaceRegression
//   | cluster admin                            | unknown name | -                             | InvalidRequest (39) "no such namespace"          | NamespaceUsageRegression.NotAuthorizedOutsideNamespaceRegression
//   | a client below version 14                | any          | -                             | InvalidRequest (39)                              | NamespaceUsageRegression.OldClientRefusedRegression
//   | cluster admin                            | `u`          | limits changed (SetLimits)    | the response limits are the new record           | NamespaceUsageRegression.ValuesMatchCountsRegression
// Mutation: report the limits instead of the usage (handleNamespaceUsage fills `usage` from `limits`) -> ValuesMatchCountsRegression fails.
#include "support/AuthRig.h"
#include "support/DdlHarness.h"
#include "client/TransientViewBrokerSupport.h"

#include <rune/client/Client.h>
#include <rune/client/NamespaceLimitFields.h>

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {

constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw  = "namespace-user-password-12";

rc::ClientConfig configFor(AuthBroker& b, const std::string& user, int32_t announceVersion = 0) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = user == "admin" ? kAdminPw : kUserPw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    c.testAnnounceClientVersion = announceVersion;
    return c;
}

std::unique_ptr<rc::Client> connected(AuthBroker& b, const std::string& user, int32_t announceVersion = 0) {
    auto cl = std::make_unique<rc::Client>(configFor(b, user, announceVersion));
    cl->connectAndWait(20s);
    return cl;
}

std::vector<AuthGrant> builderGrants(const std::string& ns, bool describeNs) {
    std::vector<AuthGrant> g = {prefix(SO::Create, ST::Topic, ""), prefix(SO::Create, ST::View, ""), prefix(SO::Read, ST::Topic, ""),
                                prefix(SO::Describe, ST::Topic, ""), prefix(SO::Describe, ST::View, ""),
                                prefix(SO::Drop, ST::Topic, "")};
    if (describeNs) g.push_back(exact(SO::Describe, ST::Namespace, ns));
    return g;
}

rc::NamespaceLimitsPatch patchOf(const char* field, uint64_t v) {
    rc::NamespaceLimitsPatch p;
    if (!p.set(*rc::namespaceLimitIndex(field), v)) throw std::runtime_error(std::string("patch ") + field);
    return p;
}

bool until(const std::function<bool()>& pred, std::chrono::seconds limit = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return pred();
}

constexpr std::size_t kTopics = 0, kPartMax = 1, kPartTotal = 2, kViews = 3;

/// The ErrorCode of a refused usage call.
uint16_t refusalOf(rc::Client& c, const std::string& ns) {
    const auto r = c.namespaceUsage(ns, 20s);
    return r ? uint16_t{0} : r.error().errorCode;
}

}  // namespace

TEST(NamespaceUsageRegression, ValuesMatchCountsRegression) {
    AuthBroker b("nsusage");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("u", patchOf("max-topics", 50)).has_value());
    b.createUser("u/w", kUserPw, builderGrants("u", false));
    auto w = connected(b, "u/w");
    const auto t1 = w->defineTopicAndWait("t1", 2, "id", rune::node::test::charBlob(), rc::Persistence::Disk, {}, {}, 60s);
    ASSERT_EQ(t1.errorCode, 0) << t1.detail;
    ASSERT_EQ(w->defineTopicAndWait("t2", 2, "", {}, 60s).errorCode, 0);
    ASSERT_EQ(w->defineTopicAndWait("t3", 3, "", {}, 60s).errorCode, 0);
    const auto v1 = w->createViewAndWait("v1", "id", 0, rune::node::test::viewPlanBlob("v1", t1.topicId, 0), {}, 60s);
    ASSERT_EQ(v1.errorCode, 0) << v1.detail;
    const auto v2 = w->createViewAndWait("v2", "id", 0, rune::node::test::viewPlanBlob("v2", t1.topicId, 0), {}, 60s);
    ASSERT_EQ(v2.errorCode, 0) << v2.detail;

    // The gateway's index follows the catalog's effects asynchronously: poll until the counts settle.
    pc::NamespaceUsageResponse got;
    ASSERT_TRUE(until([&] {
        const auto r = admin->namespaceUsage("u", 20s);
        if (!r) return false;
        got = *r;
        return got.usage[kTopics] == 3 && got.usage[kPartTotal] == 7 && got.usage[kViews] == 2;
    })) << "usage: topics " << got.usage[kTopics] << " partitions " << got.usage[kPartTotal] << " views " << got.usage[kViews];
    EXPECT_EQ(got.usage[kTopics], 3u);
    EXPECT_EQ(got.usage[kPartTotal], 7u);
    EXPECT_EQ(got.usage[kPartMax], 3u);
    EXPECT_EQ(got.usage[kViews], 2u);
    EXPECT_EQ(got.state, 0) << "active";
    EXPECT_EQ(got.limits.maxTopics, 50u) << "the limits are echoed next to the usage";
    EXPECT_NE(got.usage[kTopics], got.limits.maxTopics) << "usage is not the limit";

    // The counts follow a drop.
    ASSERT_EQ(w->dropTopicAndWait("t3", {}, 60s).errorCode, 0);
    ASSERT_TRUE(until([&] {
        const auto r = admin->namespaceUsage("u", 20s);
        return r && r->usage[kTopics] == 2 && r->usage[kPartTotal] == 4;
    })) << "the drop reaches the usage";

    // SetLimits shows in the response.
    auto list = admin->listNamespaces();
    ASSERT_TRUE(list.has_value());
    rune::protocol::NamespaceLimits l;
    for (const auto& n : *list) if (n.name == "u") l = n.limits;
    l.maxViews = 9;
    ASSERT_TRUE(admin->setNamespaceLimits("u", l).has_value());
    ASSERT_TRUE(until([&] { const auto r = admin->namespaceUsage("u", 20s); return r && r->limits.maxViews == 9; }));
    // `default` answers too (its own counts: no topics).
    const auto d = admin->namespaceUsage("default", 20s);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->usage[kTopics], 0u);
}

// #696: schemas, the highest schema version, groups, users and open transactions read 0 ("n/a") although the namespace holds
// them.  Each now comes from the owner of the count its limit check reads.  Usage is per node; this is one broker.
TEST(NamespaceUsageRegression, SchemasGroupsUsersTxnsReportedRegression) {
    constexpr std::size_t kSchemas = 4, kVersionMax = 5, kGroups = 6, kUsers = 7, kOpenTxns = 21;
    AuthBroker b("nsusage_more");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("a").has_value());
    std::vector<AuthGrant> g = builderGrants("a", false);
    g.push_back(prefix(SO::Write, ST::Topic, ""));
    g.push_back(prefix(SO::Alter, ST::Topic, ""));
    g.push_back(prefix(SO::Read, ST::Group, ""));
    g.push_back(prefix(SO::Describe, ST::Group, ""));
    g.push_back(prefix(SO::Write, ST::TransactionalId, ""));
    g.push_back(prefix(SO::Describe, ST::TransactionalId, ""));
    b.createUser("a/w", kUserPw, g);
    b.createUser("a/x", kUserPw, g);
    b.createUser("a/y", kUserPw, builderGrants("a", false));
    auto w = connected(b, "a/w");
    auto x = connected(b, "a/x");   // one GroupConsumer per Client

    // Two typed topics, one of them at version 3 (two adds); an untyped one for the transaction.
    ASSERT_EQ(w->defineTopicAndWait("s1", 1, "id", rune::node::test::charBlob(), rc::Persistence::Disk, {}, {}, 60s).errorCode, 0);
    const auto s2 = w->defineTopicAndWait("s2", 1, "id", rune::node::test::charBlob(), rc::Persistence::Disk, {}, {}, 60s);
    ASSERT_EQ(s2.errorCode, 0) << s2.detail;
    const auto tx = w->defineTopicAndWait("tx", 1, "", {}, 60s);
    ASSERT_EQ(tx.errorCode, 0) << tx.detail;
    for (const char* field : {"f1", "f2"}) {
        rune::codec::AlterOp op{};
        op.type = rune::codec::AlterOpType::Add;
        op.name = field;
        op.primType = rune::codec::PrimType::Int64;
        const auto r = w->alterTopicAndWait("s1", 0, rune::codec::encodeAlterOps({op}), {}, 60s);
        ASSERT_EQ(r.errorCode, 0) << field << ": " << r.detail;
    }

    // Two groups (a group exists from its first join).
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 30'000;
    rc::GroupConsumer g1 = w->subscribeGroup("g1", {"s2"}, gcfg);
    rc::GroupConsumer g2 = x->subscribeGroup("g2", {"s2"}, gcfg);

    // One open transaction.
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All;
    pcfg.transactionalId = "x1";
    rc::Producer p = w->producer(pcfg);
    p.initTransactionsAndWait(20s);
    p.beginTransaction();
    const uint8_t v[1] = {1};
    ASSERT_EQ(p.trySend(rc::TopicHandle{tx.topicId, 1}, rc::Key(int64_t{1}), std::span<const uint8_t>(v, 1)), rc::SendResult::Ok);
    p.flushAndWait(20s);

    pc::NamespaceUsageResponse got;
    ASSERT_TRUE(until([&] {
        const auto r = admin->namespaceUsage("a", 20s);
        if (!r) return false;
        got = *r;
        return got.usage[kGroups] == 2 && got.usage[kOpenTxns] == 1 && got.usage[kSchemas] == 2 && got.usage[kVersionMax] == 3;
    })) << "usage: schemas " << got.usage[kSchemas] << " max version " << got.usage[kVersionMax] << " groups "
        << got.usage[kGroups] << " users " << got.usage[kUsers] << " open txns " << got.usage[kOpenTxns];
    EXPECT_EQ(got.usage[kSchemas], 2u);
    EXPECT_EQ(got.usage[kVersionMax], 3u);
    EXPECT_EQ(got.usage[kGroups], 2u);
    EXPECT_EQ(got.usage[kUsers], 3u);
    EXPECT_EQ(got.usage[kOpenTxns], 1u);

    // The count follows the transaction's end.
    p.commitTransactionAndWait(20s);
    EXPECT_TRUE(until([&] { const auto r = admin->namespaceUsage("a", 20s); return r && r->usage[kOpenTxns] == 0; }))
        << "the commit reaches the usage";
    // `default` holds none of them in this namespace's sense: its counts are its own.
    const auto d = admin->namespaceUsage("default", 20s);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->usage[kSchemas], 0u);
    EXPECT_EQ(d->usage[kGroups], 0u);
    EXPECT_EQ(d->usage[kOpenTxns], 0u);
}

TEST(NamespaceUsageRegression, NamespaceUserWithDescribeRegression) {
    AuthBroker b("nsusage_user");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("u").has_value());
    b.createUser("u/w", kUserPw, builderGrants("u", true));
    auto w = connected(b, "u/w");
    ASSERT_EQ(w->defineTopicAndWait("t1", 4, "", {}, 60s).errorCode, 0);
    ASSERT_TRUE(until([&] { const auto r = w->namespaceUsage("u", 20s); return r && r->usage[kTopics] == 1 && r->usage[kPartTotal] == 4; }));
}

TEST(NamespaceUsageRegression, NotAuthorizedOutsideNamespaceRegression) {
    AuthBroker b("nsusage_auth");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("a").has_value());
    ASSERT_TRUE(admin->createNamespace("b").has_value());
    b.createUser("a/ua", kUserPw, builderGrants("a", true));    // may describe `a`
    b.createUser("b/ub", kUserPw, builderGrants("b", false));   // no NAMESPACE grant
    auto ua = connected(b, "a/ua");
    auto ub = connected(b, "b/ub");
    constexpr uint16_t kNotAuthorized = 45, kInvalid = 39;
    EXPECT_TRUE(ua->namespaceUsage("a", 20s).has_value()) << "own namespace with DESCRIBE";
    EXPECT_EQ(refusalOf(*ua, "b"), kNotAuthorized) << "a foreign namespace";
    EXPECT_EQ(refusalOf(*ua, "default"), kNotAuthorized);
    EXPECT_EQ(refusalOf(*ua, "nosuch"), kNotAuthorized) << "an unknown name looks exactly like a foreign one";
    EXPECT_EQ(refusalOf(*ub, "b"), kNotAuthorized) << "own namespace without DESCRIBE ON NAMESPACE";
    EXPECT_EQ(refusalOf(*ub, "a"), kNotAuthorized);
    EXPECT_EQ(refusalOf(*admin, "nosuch"), kInvalid) << "a cluster admin may learn the name does not exist";
    EXPECT_TRUE(admin->namespaceUsage("a", 20s).has_value());
    EXPECT_TRUE(admin->namespaceUsage("b", 20s).has_value());
}

TEST(NamespaceUsageRegression, OldClientRefusedRegression) {
    AuthBroker b("nsusage_old");
    b.bootstrapAdmin(kAdminPw);
    auto old = connected(b, "admin", /*announce*/ 13);
    EXPECT_EQ(refusalOf(*old, "default"), 39) << "InvalidRequest below client version 14";
}

// Checked writes Part 10: a persisted view over a TRANSIENT or a DISK topic is charged to the source topic's namespace.
// The ring's namespace tag (BrokerNode setNsTag, at boot and at runtime) and the replay of the boot slots into the view
// set (Worker::setViewSet) are what the view's slot is read from.
// Mutation: delete either setNsTag call in BrokerNode.cpp, or drop the replay loop in Worker::setViewSet -> these tests
// fail (the charge lands on slot 0).
static void viewIsChargedToItsNamespaceAcrossRestart(const char* brokerName, rc::Persistence sourcePersistence) {
    namespace tv = rune::node::test::transient_view_broker;
    AuthBroker::Options o;
    o.shard = true;
    AuthBroker b(brokerName, std::move(o));
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    const auto ns = admin->createNamespace("u");
    ASSERT_TRUE(ns.has_value());
    auto grants = builderGrants("u", false);
    grants.push_back(prefix(SO::Write, ST::Topic, ""));
    grants.push_back(prefix(SO::Read, ST::View, ""));
    b.createUser("u/w", kUserPw, grants);

    auto w = connected(b, "u/w");
    const auto src = w->defineTopicAndWait("presence", 1, "id", rune::node::test::charBlob(), sourcePersistence, {}, {}, 60s);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const auto view = w->createViewAndWait("pv", "id", 0, rune::node::test::viewPlanBlob("pv", src.topicId, 0), {}, 60s);
    ASSERT_EQ(view.errorCode, 0) << view.detail;
    {
        tv::Prod prod(configFor(b, "u/w"), "presence", src.topicId, rc::Acks::All);
        for (uint64_t id = 1; id <= 3; ++id) ASSERT_TRUE(prod.put(id, id * 10));
    }
    ASSERT_TRUE(tv::awaitRow(*w, view.viewTopicId, 3, {3, 30}));

    // Sum over the workers of the view-memory gauge of namespace slot `slot`.
    auto viewMem = [&](uint16_t slot) {
        uint64_t sum = 0;
        for (int32_t k = 0; k < b.node().workerCountForTest(); ++k)
            sum += b.node().onWorkerThreadForTest(k, [slot](Worker& wk) { return wk.nsCounterForTest(slot, NsMetric::ViewMemBytes); });
        return sum;
    };
    auto expectCharged = [&](const char* when) {
        const uint16_t slot = b.node().nsSlotOfForTest(*ns);
        ASSERT_NE(slot, kNoNsSlot) << when;
        ASSERT_NE(slot, 0) << when;
        EXPECT_TRUE(until([&] { return viewMem(slot) > 0; })) << when << ": the table is charged to the source topic's namespace";
        EXPECT_EQ(viewMem(0), 0u) << when << ": nothing lands on the default namespace";
    };
    expectCharged("after the view folded");

    b.restart(false);
    w = connected(b, "u/w");
    ASSERT_TRUE(tv::awaitRow(*w, view.viewTopicId, 3, {3, 30})) << "the checkpoint rows are served after the restart";
    expectCharged("after the restart");

    admin = connected(b, "admin");
    auto list = admin->listNamespaces();
    ASSERT_TRUE(list.has_value());
    rune::protocol::NamespaceLimits l;
    for (const auto& n : *list) if (n.name == "u") l = n.limits;
    l.maxViewMemoryBytes = 1;
    ASSERT_TRUE(admin->setNamespaceLimits("u", l).has_value());
    EXPECT_TRUE(until([&] { return w->getViewRowAndWait(view.viewTopicId, 3, 0, 10s).status == rc::ViewRowResult::kViewPaused; }))
        << "the view is paused by the source topic's namespace";
}

TEST(TransientViewNamespaceBrokerRegression, ViewOverATransientTopicIsChargedToItsNamespaceAcrossRestartRegression) {
    viewIsChargedToItsNamespaceAcrossRestart("nsusage_tv", rc::Persistence::Transient);
}

TEST(TransientViewNamespaceBrokerRegression, ViewOverADiskTopicIsChargedToItsNamespaceAcrossRestartRegression) {
    viewIsChargedToItsNamespaceAcrossRestart("nsusage_tv_disk", rc::Persistence::Disk);
}
