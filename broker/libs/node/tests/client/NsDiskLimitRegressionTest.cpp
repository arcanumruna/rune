// NsDiskLimitRegressionTest.cpp - namespaces Phase 2 Task 11 (D25): the namespace disk limit through the REAL client against
// REAL authenticating BrokerNodes (default config): one node (leader produce refused over the limit, group offsets - an
// internal topic - still committed, a raised limit resumes) and three nodes (follower apply never refused, no ISR
// shrink).  The worker-level cells (no Aeron) are in rune_ns_disk_worker_tests; the Log counter in rune_storage_tests.
//
// Interaction matrix:
//   | limit            | operation                   | state / role            | expected                                           | test
//   |------------------|-----------------------------|-------------------------|----------------------------------------------------|-----
//   | max-disk-bytes   | produce acks=All            | usage < / crossing / >= | served, served, refused 52; other ns served        | LimitEnforcementTableRegression.DiskBrokerRegression
//   | max-disk-bytes   | group offset commit         | namespace over its disk | committed (internal topic never refused)           | LimitEnforcementTableRegression.DiskBrokerRegression
//   | max-disk-bytes   | SetLimits raise             | over                    | produce resumes                                    | LimitEnforcementTableRegression.DiskBrokerRegression
//   | max-disk-bytes   | acks=All produce, 3 nodes   | leader goes over        | every acked record on every node; followers over   | OverDiskRegression.FollowerApplyNeverRefusedBrokerRegression
//   |                  |                             |                         | but applying; the ISR keeps all three              |
#include "support/AuthRig.h"
#include "support/AuthTrio.h"
#include "support/LimitEnforcementTable.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/NamespaceLimitFields.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/replication/PartitionReplica.h>

#include <gtest/gtest.h>

#include <atomic>
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

std::unique_ptr<rc::Client> connected(AuthBroker& b, const std::string& user, std::vector<rc::TopicSpec> topics = {}) {
    rc::ClientConfig c;
    c.topics          = std::move(topics);
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = user == "admin" ? kAdminPw : kUserPw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    auto cl = std::make_unique<rc::Client>(c);
    cl->connectAndWait(20s);
    return cl;
}

std::vector<AuthGrant> userGrants() {
    return {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
            prefix(SO::Read, ST::Topic, ""), prefix(SO::Read, ST::Group, ""), prefix(SO::Describe, ST::Group, "")};
}

rc::NamespaceLimitsPatch patchOf(const char* field, uint64_t v) {
    rc::NamespaceLimitsPatch p;
    if (!p.set(*rc::namespaceLimitIndex(field), v)) throw std::runtime_error(std::string("patch ") + field);
    return p;
}

struct Outcome { rc::OpStatus status = rc::OpStatus::Timeout; uint16_t code = 0; };

Outcome produceOne(rc::Client& c, rc::TopicHandle th, rc::Acks acks, uint64_t key, std::size_t valueBytes = 2048) {
    rc::ProducerConfig pcfg;
    pcfg.acks     = acks;
    pcfg.lingerUs = 0;
    auto p = c.producer(pcfg);
    rc::Completions cq(c);
    const std::vector<uint8_t> v(valueBytes, 7);
    if (p.trySend(th, rc::Key(key), std::span<const uint8_t>(v.data(), v.size()), cq, 1) != rc::SendResult::Ok) return {rc::OpStatus::InvalidArgument, 0};
    p.flush();
    Outcome out;
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    bool done = false;
    while (!done && std::chrono::steady_clock::now() < deadline) {
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { out = {d.status, d.errorCode}; done = true; });
        if (!done) std::this_thread::sleep_for(1ms);
    }
    p.close();
    return out;
}

bool until(const std::function<bool()>& pred, std::chrono::seconds limit = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

}  // namespace

TEST(LimitEnforcementTableRegression, DiskBrokerRegression) {
    constexpr uint64_t kLimit = 16384;
    AuthBroker b("lim_disk");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    const auto ns = admin->createNamespace("d", patchOf("max-disk-bytes", kLimit));
    ASSERT_TRUE(ns.has_value()) << ns.error().detail;
    ASSERT_TRUE(admin->createNamespace("free").has_value());
    b.createUser("d/u", kUserPw, userGrants());
    b.createUser("free/f", kUserPw, userGrants());
    auto define = [&](const std::string& user) {
        auto c = connected(b, user);
        const auto r = c->defineTopicAndWait("t", 1, "", {}, 20s);
        if (r.errorCode != 0) throw std::runtime_error("define: " + r.detail);
        c->close();
        return rc::TopicHandle{r.topicId, 1};
    };
    const rc::TopicHandle td = define("d/u"), tf = define("free/f");
    auto du = connected(b, "d/u", {rc::TopicSpec{"t", td.topicId, 1}});
    const uint16_t slot = b.node().nsSlotOfForTest(*ns);
    ASSERT_NE(slot, kNoNsSlot);
    auto usage = [&] { return b.node().onWorkerThreadForTest(0, [slot](Worker& w) { return w.nsDiskBytesForTest(slot); }); };

    int served = 0;
    Outcome refused{};
    for (uint64_t k = 1; k <= 40; ++k) {
        const Outcome o = produceOne(*du, td, rc::Acks::All, k);
        if (o.status == rc::OpStatus::Ok) { ++served; continue; }
        refused = o;
        break;
    }
    EXPECT_GE(served, 2) << "Below / At: produces were served before the limit";
    EXPECT_NE(refused.status, rc::OpStatus::Ok) << "Above: a produce was refused";
    EXPECT_EQ(refused.code, 52) << "NamespaceLimitExceeded";
    EXPECT_GE(static_cast<uint64_t>(usage()), kLimit) << "the refusal came only once usage reached the limit";
    EXPECT_LE(served, 40);
    // Another namespace and the default one are not touched by a full namespace.
    auto ff = connected(b, "free/f");
    for (uint64_t k = 1; k <= 12; ++k) EXPECT_EQ(produceOne(*ff, tf, rc::Acks::All, k).status, rc::OpStatus::Ok) << "free " << k;
    // The refused namespace still reads and still commits group offsets (an internal topic is never refused).
    {
        rc::GroupConfig gcfg;
        gcfg.autoCommit = false;
        gcfg.sessionTimeoutMs = 10'000;
        rc::GroupConsumer gc = du->subscribeGroup("g", {"t"}, gcfg);
        std::atomic<size_t> n{0};
        gc.onRecords([&](const rc::RecordSet& rs) { rs.forEachRecord([&](std::string_view, std::string_view, int64_t, int64_t) { ++n; }); });
        ASSERT_TRUE(until([&] { return n.load() >= static_cast<size_t>(served); }, 40s)) << "reads are served while over";
        EXPECT_NO_THROW(gc.commitSyncAndWait(20s)) << "the offset commit (an internal topic) is not refused";
        gc.close();
    }
    // A raised limit resumes the produce (the next scan clears the byte).
    auto list = admin->listNamespaces();
    ASSERT_TRUE(list.has_value());
    rune::protocol::NamespaceLimits raised;
    for (const auto& n : *list) if (n.name == "d") raised = n.limits;
    raised.maxDiskBytes = 64u << 20;
    ASSERT_TRUE(admin->setNamespaceLimits("d", raised).has_value());
    EXPECT_TRUE(until([&] { return produceOne(*du, td, rc::Acks::All, 99).status == rc::OpStatus::Ok; }, 30s)) << "produce resumes after the limit is raised";
}

TEST(OverDiskRegression, FollowerApplyNeverRefusedBrokerRegression) {
    constexpr uint64_t kLimit = 16384;
    AuthTrio t("lim_disk_trio");
    t.start();
    t.bootstrapAdmin();
    auto configFor = [&](const std::string& user, const std::string& pw, int seq) {
        rc::ClientConfig c;
        c.gatewayChannel  = t.channel(1);
        c.gatewayStreamId = AuthWireClient::kClientStream;
        c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        c.egressStreamId  = 2;
        c.dataDir         = (t.cluster().clientsDir->path / ("rcd_" + std::to_string(seq))).string();
        c.username        = user;
        c.password        = pw;
        c.offerCrypto     = true;
        c.tlsTrustPem     = AuthTrio::tlsCaPem();
        return c;
    };
    std::unique_ptr<rc::Client> admin;
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        try { admin = std::make_unique<rc::Client>(configFor("admin", AuthTrio::kAdminPassword, 0)); admin->connectAndWait(30s); return true; }
        catch (...) { return false; }
    }));
    const auto ns = admin->createNamespace("d", patchOf("max-disk-bytes", kLimit));
    ASSERT_TRUE(ns.has_value()) << ns.error().detail;
    t.createUser(1, "d/u", AuthTrio::kPw);
    for (const auto& g : userGrants()) t.grant(1, "d/u", g.op, g.type, g.kind, g.pattern);
    std::unique_ptr<rc::Client> u;
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        try { u = std::make_unique<rc::Client>(configFor("d/u", AuthTrio::kPw, 1)); u->connectAndWait(30s); return true; }
        catch (...) { u.reset(); return false; }
    }));
    rc::TopicOptions to;
    to.replicationFactor = 3;
    rc::DefineTopicResult def{};
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        def = u->defineTopicAndWait("t", 1, "", {}, rc::Persistence::Disk, to, {}, 60s);
        return def.errorCode == 0;
    })) << def.detail;
    const rc::TopicHandle th{def.topicId, 1};

    int served = 0;
    Outcome refused{};
    for (uint64_t k = 1; k <= 60; ++k) {
        const Outcome o = produceOne(*u, th, rc::Acks::All, k);
        if (o.status == rc::OpStatus::Ok) { ++served; continue; }
        refused = o;
        break;
    }
    ASSERT_NE(refused.status, rc::OpStatus::Ok) << "the leader never went over the limit";
    EXPECT_EQ(refused.code, 52);
    ASSERT_GE(served, 2);

    // Every acked record is on every node: a follower's apply was never refused.
    const int64_t want = static_cast<int64_t>(served);
    for (int node = 1; node <= 3; ++node) {
        int64_t leo = -1;
        EXPECT_TRUE(AuthTrio::until(60s, [&] {
            leo = t.node(node).onOwnerThreadForTest(def.topicId, 0, [](rune::replication::PartitionReplica& r) { return r.logEndOffset(); });
            return leo >= want;
        })) << "node " << node << " holds " << leo << " of " << want << " acked records";
    }
    // The replicas are over their limit too (they hold the same bytes) - and still in the ISR.
    const uint16_t slot = t.node(1).nsSlotOfForTest(*ns);
    ASSERT_NE(slot, kNoNsSlot);
    for (int node = 1; node <= 3; ++node)
        EXPECT_TRUE(AuthTrio::until(30s, [&] {
            for (int w = 0; w < 8; ++w) {
                try {
                    if (t.node(node).onWorkerThreadForTest(w, [slot](Worker& wk) { return wk.nsOverDiskForTest(slot); })) return true;
                } catch (const std::invalid_argument&) { break; }   // no such worker
            }
            return false;
        })) << "node " << node << " is over its limit (its follower bytes count)";
    size_t isr = 0;
    for (int node = 1; node <= 3; ++node)
        isr = std::max<size_t>(isr, t.node(node).onOwnerThreadForTest(def.topicId, 0, [](rune::replication::PartitionReplica& r) { return r.isr().size(); }));
    EXPECT_EQ(isr, 3u) << "no ISR shrink";
}
