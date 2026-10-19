// ClientAuthClusterRegressionTest.cpp - authentication Task 19: the REAL client (rune::client::Client, username /
// password) against THREE real authenticating BrokerNodes (support/AuthTrio.h): one client session per broker, produce
// and consume on `world.*` through each, then REVOKE WRITE: the next produce through every broker completes
// NotAuthorized, with no retry storm - exactly one refusal at the brokers per failed record.
#include "support/AuthTrio.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>

#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <mutex>
#include <set>

using namespace rune::node;
using namespace rune::node::test;
namespace rc = rune::client;
using namespace std::chrono_literals;
using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using SP = rune::protocol::SecurityPatternKind;

namespace {

constexpr int32_t kParts = 3;

rc::ClientConfig configFor(AuthTrio& t, int id, const std::string& user, const std::string& password) {
    static int seq = 0;
    rc::ClientConfig c;
    c.gatewayChannel  = t.channel(id);
    c.gatewayStreamId = AuthWireClient::kClientStream;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = (t.cluster().clientsDir->path / ("rc" + std::to_string(id) + "_" + std::to_string(seq++))).string();
    c.username        = user;
    c.password        = password;
    return c;
}

rc::Key keyFor(int32_t part, uint64_t salt) {
    for (uint64_t k = salt * 64;; ++k) { rc::Key key(k); if (key.partition(kParts) == part) return key; }
}

uint64_t workerRefusals(BrokerNode& n) {
    return n.onWorkerThreadForTest(0, [](Worker& w) { return w.workerNotAuthorizedForTest(); }) + n.gatewayNotAuthorizedForTest();
}

}  // namespace

TEST(ClientAuthClusterRegression, WorldServerClientProducesAndConsumesThroughEveryBroker) {
    AuthTrio t("rclient");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "world-server", "world-server-password");
    t.grant(1, "world-server", SO::Create, ST::Topic, SP::Prefix, "world.");
    t.grant(1, "world-server", SO::Describe, ST::Topic, SP::Prefix, "world.");
    t.grant(1, "world-server", SO::Write, ST::Topic, SP::Prefix, "world.");
    t.grant(1, "world-server", SO::Read, ST::Topic, SP::Prefix, "world.");

    // Define the topic through a client on broker 1 (retry: the grants reach broker 1 asynchronously).
    rc::TopicSpec spec;
    {
        std::unique_ptr<rc::Client> admin;
        ASSERT_TRUE(AuthTrio::until(60s, [&] {
            try { admin = std::make_unique<rc::Client>(configFor(t, 1, "world-server", "world-server-password")); admin->connectAndWait(30s); return true; }
            catch (const std::exception&) { admin.reset(); return false; }
        }));
        rc::TopicOptions to;
        to.replicationFactor = 3;
        rc::DefineTopicResult def{};
        ASSERT_TRUE(AuthTrio::until(60s, [&] {
            def = admin->defineTopicAndWait("world.zone", kParts, "", {}, false, rc::Persistence::Disk, to, {}, 60s);
            return def.errorCode == 0;
        })) << def.detail;
        spec = rc::TopicSpec{"world.zone", def.topicId, kParts};
        admin->close();
    }

    // One real client per broker.
    std::array<std::unique_ptr<rc::Client>, 3> clients;
    for (int id : {1, 2, 3}) {
        auto cfg = configFor(t, id, "world-server", "world-server-password");
        cfg.topics = {spec};
        ASSERT_TRUE(AuthTrio::until(60s, [&] {
            try { clients[static_cast<std::size_t>(id - 1)] = std::make_unique<rc::Client>(cfg); clients[static_cast<std::size_t>(id - 1)]->connectAndWait(30s); return true; }
            catch (const std::exception&) { clients[static_cast<std::size_t>(id - 1)].reset(); return false; }
        })) << "client on broker " << id;
        EXPECT_EQ(clients[static_cast<std::size_t>(id - 1)]->authStatus().kind, rc::AuthStatus::Kind::Authenticated);
    }

    // Produce kPer records through each client (acks=All), round-robin over the partitions; wait for every completion.
    constexpr int kPer = 6;
    struct Producing { rc::Producer producer; std::unique_ptr<rc::Completions> cq; };
    std::vector<Producing> prods;
    for (int id : {1, 2, 3}) {
        rc::ProducerConfig pc;
        pc.acks = rc::Acks::All;
        pc.lingerUs = 1;
        pc.batchBytes = 1;
        pc.produceTimeoutMs = 30'000;
        pc.deliveryTimeoutMs = 40'000;
        auto& c = *clients[static_cast<std::size_t>(id - 1)];
        prods.push_back({c.producer(pc), std::make_unique<rc::Completions>(c)});
    }
    auto sendAndWait = [&](int idx, int seqBase, int n) {
        std::map<rc::Token, rc::Completion> done;
        auto& p = prods[static_cast<std::size_t>(idx)];
        const rc::TopicHandle th = clients[static_cast<std::size_t>(idx)]->topic("world.zone");
        for (int i = 0; i < n; ++i) {
            const std::string v = "b" + std::to_string(idx + 1) + "-" + std::to_string(seqBase + i);
            const auto bytes = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size());
            const rc::Token tok = static_cast<rc::Token>(seqBase + i + 1);
            for (const auto dl = Clock3::now() + 30s; Clock3::now() < dl;) {
                const auto r = p.producer.trySend(th, keyFor((seqBase + i) % kParts, static_cast<uint64_t>(idx * 100 + seqBase + i)), bytes, *p.cq, tok);
                if (r == rc::SendResult::Ok) break;
                (void)p.cq->poll([&](const rc::Completion& d, std::span<const uint8_t>) { done[d.token] = d; });
                std::this_thread::yield();
            }
        }
        for (const auto dl = Clock3::now() + 60s; Clock3::now() < dl && done.size() < static_cast<std::size_t>(n);) {
            (void)p.cq->poll([&](const rc::Completion& d, std::span<const uint8_t>) { done[d.token] = d; });
            std::this_thread::sleep_for(2ms);
        }
        return done;
    };
    for (int idx = 0; idx < 3; ++idx) {
        const auto done = sendAndWait(idx, 0, kPer);
        ASSERT_EQ(done.size(), static_cast<std::size_t>(kPer)) << "broker " << idx + 1;
        for (const auto& [tok, d] : done) EXPECT_EQ(d.status, rc::OpStatus::Ok) << "broker " << idx + 1 << " token " << tok << " err " << d.errorCode;
    }

    // Consume: every client reads every partition from the start and sees all 3 * kPer values.
    for (int idx = 0; idx < 3; ++idx) {
        std::mutex mu;
        std::set<std::string> seen;
        std::vector<rc::Subscription> subs;
        rc::SubscribeOptions o;
        o.from = rc::From::earliest();
        const rc::TopicHandle th = clients[static_cast<std::size_t>(idx)]->topic("world.zone");
        for (int32_t p = 0; p < kParts; ++p)
            subs.push_back(clients[static_cast<std::size_t>(idx)]->subscribe(th, p, o, [&](const rc::Record& r) {
                std::lock_guard lk(mu);
                seen.insert(std::string(reinterpret_cast<const char*>(r.value.data()), r.value.size()));
            }));
        EXPECT_TRUE(AuthTrio::until(60s, [&] { std::lock_guard lk(mu); return seen.size() >= static_cast<std::size_t>(3 * kPer); }))
            << "broker " << idx + 1 << " saw " << seen.size();
        for (auto& s : subs) s.unsubscribe();
    }

    // REVOKE WRITE; every broker must enforce it before the counting starts (probe with a raw session per partition leader).
    const auto rev = t.revoke(2, "world-server", SO::Write, ST::Topic, SP::Prefix, "world.");
    ASSERT_EQ(rev.errorCode, 0) << rev.detail;
    ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));
    AuthWireClient& probe = t.newWire(1);
    for (int id : {1, 2, 3}) {
        const auto s = t.loginOn(probe, id, "world-server", "world-server-password");
        // a probe produce at this broker (refused whether it leads or not: the gate answers NotAuthorized/Redirect)
        ASSERT_TRUE(AuthTrio::until(20s, [&] {
            const int64_t corr = probe.raw().nextCorrelationId();
            probe.raw().produce(t.channel(id), AuthWireClient::kClientStream, s.sid, corr, spec.topicId, 0, 1, makeBatch(1));
            const auto a = probe.raw().awaitProduceResponseUntil(corr, Clock3::now() + 3s);
            return a && a->errorCode == 45;
        })) << "broker " << id << " must enforce the REVOKE";
    }
    uint64_t before = 0;
    for (int id : {1, 2, 3}) before += workerRefusals(t.node(id));

    // The next produce through every broker's client completes NotAuthorized - once each, no retries.
    constexpr int kFail = 4;
    for (int idx = 0; idx < 3; ++idx) {
        const auto done = sendAndWait(idx, 1000, kFail);
        ASSERT_EQ(done.size(), static_cast<std::size_t>(kFail)) << "broker " << idx + 1;
        for (const auto& [tok, d] : done) {
            EXPECT_EQ(d.status, rc::OpStatus::BrokerError) << "broker " << idx + 1 << " token " << tok;
            EXPECT_EQ(d.errorCode, 45) << "NotAuthorized";
        }
    }
    uint64_t after = 0;
    for (int id : {1, 2, 3}) after += workerRefusals(t.node(id));
    EXPECT_EQ(after - before, static_cast<uint64_t>(3 * kFail)) << "exactly one ProduceRequest (refused) per failed record: no retry storm";

    for (auto& p : prods) p.producer.close();
    for (auto& c : clients) c->close();
}
