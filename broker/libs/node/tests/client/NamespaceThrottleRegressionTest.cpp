// NamespaceThrottleRegressionTest.cpp - namespaces Phase 2 Task 14 (D28): the rate throttle through a REAL authenticating
// BrokerNode on the DEFAULT config (RUNE_NS_LIMIT_TICK_US = 10000): a raw v14 wire session sees Throttled (53) with the
// retry-after, a neighbour namespace is untouched, and the REAL client library backs off on the retry-after and gets every
// record through.  The worker cells (stale nsId, recreate, disk) are in rune_ns_throttle_worker_tests; the aggregator's
// decision logic in rune_ns_limit_aggregator_tests.
//
// Interaction matrix:
//   | limit                  | operation                         | state                    | expected                                       | test
//   |------------------------|-----------------------------------|--------------------------|------------------------------------------------|-----
//   | produce-bytes-per-sec  | raw produce burst, acks=1         | over the rate            | 53, retryAfter in (0, 10 s] in logStartOffset  | NamespaceThrottleRegression.ProduceThrottledReturnsRetryAfterRegression
//   | produce-bytes-per-sec  | same burst in namespace `free`    | `d` throttled            | every produce served                           | NamespaceThrottleRegression.OtherNamespaceNotThrottledRegression
//   | produce-bytes-per-sec  | client library, acks=All          | a burst over the rate    | all records acked; elapsed >= the rate floor; >= 1 backoff | NamespaceThrottleRegression.ClientBacksOffAndSucceedsRegression
//   | produce-bytes-per-sec  | SetLimits removes the limit       | throttled                | the throttle clears at the next tick           | NamespaceThrottleRegression.RaisedLimitClearsThrottleRegression
#include "support/AuthRig.h"
#include "support/LimitEnforcementTable.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/NamespaceLimitFields.h>
#include <rune/client/producer/Producer.h>

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
            prefix(SO::Read, ST::Topic, "")};
}

rc::NamespaceLimitsPatch patchOf(const char* field, uint64_t v) {
    rc::NamespaceLimitsPatch p;
    if (!p.set(*rc::namespaceLimitIndex(field), v)) throw std::runtime_error(std::string("patch ") + field);
    return p;
}

rc::TopicHandle defineTopic(AuthBroker& b, const std::string& user) {
    auto c = connected(b, user);
    const auto r = c->defineTopicAndWait("t", 1, "", false, {}, 20s);
    if (r.errorCode != 0) throw std::runtime_error("define: " + r.detail);
    c->close();
    return rc::TopicHandle{r.topicId, 1};
}

bool until(const std::function<bool()>& pred, std::chrono::seconds limit = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

std::vector<uint8_t> bigBatch(std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", std::string(valueBytes, 'v'), 1000);
    return b.build();
}

struct Wire {
    AuthWireClient& w;
    int64_t sid = -1;
    Wire(AuthBroker& b, const std::string& user) : w(b.wire()) {
        sid = w.connect(b.channel(), pc::kClientVersionNamespaces);
        if (sid <= 0) throw std::runtime_error("wire connect");
        const auto l = w.login(b.channel(), sid, user, {kUserPw, kUserPw});
        if (!l.ok()) throw std::runtime_error("wire login");
    }
    /// One acks=1 produce of `valueBytes`; the ProduceResponse (nullopt: none within the bound).
    std::optional<pc::ProduceResponse> produce(AuthBroker& b, int32_t topicId, std::size_t valueBytes) {
        const int64_t corr = w.raw().nextCorrelationId();
        if (!w.raw().produce(b.channel(), AuthWireClient::kClientStream, sid, corr, topicId, 0, 1, bigBatch(valueBytes))) return std::nullopt;
        return w.raw().awaitProduceResponse(corr);
    }
};

}  // namespace

TEST(NamespaceThrottleRegression, ProduceThrottledReturnsRetryAfterRegression) {
    AuthBroker b("thr_retry");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("d", patchOf("produce-bytes-per-sec", 20'000)).has_value());
    b.createUser("d/u", kUserPw, userGrants());
    const rc::TopicHandle t = defineTopic(b, "d/u");
    Wire w(b, "d/u");
    int served = 0;
    std::optional<pc::ProduceResponse> throttled;
    for (int i = 0; i < 400 && !throttled; ++i) {
        const auto r = w.produce(b, t.topicId, 4096);
        ASSERT_TRUE(r.has_value()) << "answered, never hung (produce " << i << ")";
        if (r->errorCode == 0) ++served;
        else { throttled = r; }
    }
    ASSERT_TRUE(throttled.has_value()) << "the namespace was never throttled after " << served << " served produces";
    EXPECT_EQ(throttled->errorCode, 53) << "Throttled";
    EXPECT_GT(throttled->logStartOffset, 0) << "the retry-after (ns) rides logStartOffset";
    EXPECT_LE(throttled->logStartOffset, 10'000'000'000LL) << "capped at the aggregator's maximum";
    EXPECT_GE(served, 1) << "Below: the first produces were served";
    // The broker still answers the same session after the deadline passes (the throttle is a deadline, not a state).
    std::this_thread::sleep_for(std::chrono::nanoseconds(std::min<int64_t>(throttled->logStartOffset, 3'000'000'000LL)));
    ASSERT_TRUE(until([&] { const auto r = w.produce(b, t.topicId, 64); return r && r->errorCode == 0; }, 40s))
        << "served again after the retry-after";
}

TEST(NamespaceThrottleRegression, OtherNamespaceNotThrottledRegression) {
    AuthBroker b("thr_other");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("d", patchOf("produce-bytes-per-sec", 20'000)).has_value());
    ASSERT_TRUE(admin->createNamespace("free").has_value());
    b.createUser("d/u", kUserPw, userGrants());
    b.createUser("free/f", kUserPw, userGrants());
    const rc::TopicHandle td = defineTopic(b, "d/u"), tf = defineTopic(b, "free/f");
    Wire wd(b, "d/u"), wf(b, "free/f");
    bool throttled = false;
    for (int i = 0; i < 400 && !throttled; ++i) {
        const auto r = wd.produce(b, td.topicId, 4096);
        ASSERT_TRUE(r.has_value());
        throttled = r->errorCode == 53;
    }
    ASSERT_TRUE(throttled);
    for (int i = 0; i < 30; ++i) {
        const auto r = wf.produce(b, tf.topicId, 4096);
        ASSERT_TRUE(r.has_value()) << i;
        EXPECT_EQ(r->errorCode, 0) << "`free` is not throttled by `d` (produce " << i << ")";
    }
}

TEST(NamespaceThrottleRegression, ClientBacksOffAndSucceedsRegression) {
    constexpr uint64_t kRate = 40'000;   // bytes/s
    constexpr int kRecords = 40;         // 40 x 2 KiB = 80 KiB: >= ~2 s at the rate
    AuthBroker b("thr_backoff");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    const auto ns = admin->createNamespace("d", patchOf("produce-bytes-per-sec", kRate));
    ASSERT_TRUE(ns.has_value()) << ns.error().detail;
    b.createUser("d/u", kUserPw, userGrants());
    const rc::TopicHandle t = defineTopic(b, "d/u");
    auto u = connected(b, "d/u", {rc::TopicSpec{"t", t.topicId, 1}});
    rc::ProducerConfig pcfg;
    pcfg.acks     = rc::Acks::All;
    pcfg.lingerUs = 0;
    auto p = u->producer(pcfg);
    rc::Completions cq(*u);
    const std::vector<uint8_t> v(2048, 7);
    const auto t0 = std::chrono::steady_clock::now();
    int sent = 0, ok = 0, failed = 0;
    const auto deadline = t0 + 90s;
    while ((sent < kRecords || ok + failed < kRecords) && std::chrono::steady_clock::now() < deadline) {
        if (sent < kRecords && p.trySend(t, rc::Key(static_cast<uint64_t>(sent + 1)), std::span<const uint8_t>(v.data(), v.size()), cq,
                                          static_cast<uint64_t>(sent + 1)) == rc::SendResult::Ok)
            ++sent;
        p.flush();
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { (d.status == rc::OpStatus::Ok ? ok : failed)++; });
        std::this_thread::sleep_for(1ms);
    }
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    EXPECT_EQ(sent, kRecords);
    EXPECT_EQ(ok, kRecords) << "every record was acked (failed " << failed << ")";
    EXPECT_EQ(failed, 0);
    EXPECT_GE(elapsed, 1s) << "80 KiB at 40 KiB/s cannot finish sooner: the client waited out the retry-after";
    const uint16_t slot = b.node().nsSlotOfForTest(*ns);
    ASSERT_NE(slot, kNoNsSlot);
    uint64_t refusals = 0;
    for (int w = 0; w < 8; ++w) {
        try { refusals += b.node().onWorkerThreadForTest(w, [](Worker& wk) { return wk.throttledRefusalsForTest(); }); }
        catch (const std::invalid_argument&) { break; }   // no such worker
    }
    EXPECT_GT(refusals, 0u) << "the broker refused at least once (the client really backed off)";
    p.close();
}

TEST(NamespaceThrottleRegression, RaisedLimitClearsThrottleRegression) {
    AuthBroker b("thr_raise");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("d", patchOf("produce-bytes-per-sec", 1'000)).has_value());   // 1 KB/s: a 10 s lock-out
    b.createUser("d/u", kUserPw, userGrants());
    const rc::TopicHandle t = defineTopic(b, "d/u");
    Wire w(b, "d/u");
    bool throttled = false;
    for (int i = 0; i < 100 && !throttled; ++i) {
        const auto r = w.produce(b, t.topicId, 4096);
        ASSERT_TRUE(r.has_value());
        throttled = r->errorCode == 53;
    }
    ASSERT_TRUE(throttled);
    auto list = admin->listNamespaces();
    ASSERT_TRUE(list.has_value());
    rune::protocol::NamespaceLimits l;
    for (const auto& n : *list) if (n.name == "d") l = n.limits;
    l.produceBytesPerSec = 0;   // unlimited
    ASSERT_TRUE(admin->setNamespaceLimits("d", l).has_value());
    EXPECT_TRUE(until([&] { const auto r = w.produce(b, t.topicId, 64); return r && r->errorCode == 0; }, 5s))
        << "well inside the original deadline: the next aggregator tick cleared it";
}
