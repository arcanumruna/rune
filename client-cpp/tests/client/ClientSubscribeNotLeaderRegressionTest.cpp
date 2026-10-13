// tests/client/ClientSubscribeNotLeaderRegressionTest.cpp
//
// Regression: silent-failure gap.  A pending plain Subscribe answered with a
// BARE NotLeader (code 2, empty message — e.g. a topic whose definition has not
// committed yet) used to be neither retried nor failed: it sat pending until
// subscribeTimeoutMs and then finished with failReason None and no onError.
//
//   (a) bare NotLeader once, then OK  -> the Subscribe binds active.
//   (b) bare NotLeader forever        -> explicit failure (NotLeader) within the
//                                        retry budget, onError fires.
//   (c) no answer ever                -> explicit TimedOut failure + onError.

#include "../support/TestCompletions.h"
#include "../support/AeronTestErrors.h"
#include <rune/client/Client.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/codec/Codec.h>
#include <rune/protocol/client/MetadataMessages.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace rcc = rune::client::codec;
namespace rp  = rune::protocol::client;

namespace {
std::atomic<int> g_ctr{0};
std::string uniqueDir(const char* prefix) {
    return std::string("/tmp/") + prefix + "_" + std::to_string(static_cast<long>(::getpid())) +
           "_" + std::to_string(g_ctr.fetch_add(1));
}

enum class SubMode { OkAfterOneNotLeader, AlwaysNotLeader, Silent };

struct SubGateway {
    std::atomic<int> subscribesSeen{0};
    SubMode          mode;
    static constexpr int64_t kSessionId = 77;

    std::shared_ptr<aeron::Aeron>                aeronC;
    std::shared_ptr<aeron::Subscription>         sub;
    std::shared_ptr<aeron::ExclusivePublication> pub;
    std::unique_ptr<aeron::FragmentAssembler>    asmb;
    uint8_t replyBuf[2048]{};

    SubGateway(const std::string& aeronDir, int32_t inStr, int32_t egStr, SubMode m) : mode(m) {
        aeron::Context ctx;
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        aeronC = aeron::Aeron::connect(ctx);
        const int64_t subId = aeronC->addSubscription("aeron:ipc", inStr);
        const int64_t pubId = aeronC->addExclusivePublication("aeron:ipc", egStr);
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!sub) sub = aeronC->findSubscription(subId);
            if (!pub) pub = aeronC->findExclusivePublication(pubId);
            if (sub && pub) break;
            std::this_thread::sleep_for(1ms);
        }
        asmb = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                   aeron::util::index_t len, aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
            });
    }
    ~SubGateway() { asmb.reset(); pub.reset(); sub.reset(); aeronC.reset(); }

    int poll(int lim = 16) { return sub && asmb ? sub->poll(asmb->handler(), lim) : 0; }

    bool send(const uint8_t* b, std::size_t l) {
        const auto il = static_cast<aeron::util::index_t>(l);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), il);
        for (int i = 0; i < 500; ++i) {
            if (pub->offer(ab, 0, il) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void sendMetadata(int64_t sessionId, int64_t corr) {
        std::vector<rp::BrokerEntry> brokers;
        rp::BrokerEntry b0; b0.nodeId = 0; b0.host = "aeron:ipc"; b0.port = 0;
        brokers.push_back(b0);
        std::vector<rp::PartitionEntry> parts;
        rp::PartitionEntry p; p.topicId = 1; p.partition = 0; p.leaderId = 0; p.leaderEpoch = 1;
        parts.push_back(p);
        rp::MetadataResponse msg;
        msg.sessionId = sessionId; msg.correlationId = corr;
        msg.brokers = std::span<const rp::BrokerEntry>(brokers);
        msg.partitions = std::span<const rp::PartitionEntry>(parts);
        const std::size_t n = msg.encode(replyBuf);
        if (n > 0) send(replyBuf, n);
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;
        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);
        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf), static_cast<int16_t>(rcc::ErrorCode::OK), 0, kSessionId,
                opt->sessionId, opt->correlationId);
            if (n > 0) send(replyBuf, n);
        } else if (tid == rcc::TemplateId::MetadataRequest) {
            const auto opt = rp::MetadataRequest::decode(buf, len);
            if (opt) sendMetadata(opt->sessionId, opt->correlationId);
        } else if (tid == rcc::TemplateId::Subscribe) {
            const auto opt = rcc::decodeSubscribe(buf, len);
            if (!opt) return;
            const int k = subscribesSeen.fetch_add(1) + 1;
            if (mode == SubMode::Silent) return;
            std::size_t n;
            if (mode == SubMode::AlwaysNotLeader || k == 1) {
                n = rcc::encodeErrorResponse(replyBuf, sizeof(replyBuf),
                        static_cast<int16_t>(rp::ErrorCode::NotLeader), std::string_view{},
                        opt->sessionId, opt->correlationId);
            } else {
                n = rcc::encodeSubscribeResponse(replyBuf, sizeof(replyBuf), 0, opt->partition,
                        /*subscriptionId=*/9, opt->sessionId, opt->correlationId);
            }
            if (n > 0) send(replyBuf, n);
        }
    }
};

struct Rig {
    std::string agentDir;
    std::unique_ptr<Client> client;
    std::unique_ptr<SubGateway> gw;
    std::atomic<int> errors{0};

    Rig(const char* name, int32_t inStr, int32_t egStr, SubMode m, int64_t subTimeoutMs) {
        agentDir = uniqueDir(name);
        ClientConfig cfg;
        cfg.gatewayChannel = "aeron:ipc"; cfg.gatewayStreamId = inStr;
        cfg.egressChannel  = "aeron:ipc"; cfg.egressStreamId  = egStr;
        cfg.dataDir = agentDir;
        cfg.connectTimeoutMs = 8000; cfg.pingTimeoutMs = 600000;  // no ping-driven reconnect: it would re-send the pending Subscribe and mask the gap
        cfg.subscribeTimeoutMs = subTimeoutMs;
        cfg.idleMode = IdleMode::LowLatency;
        TopicSpec ts; ts.name = "inventory"; ts.topicId = 1; ts.partitionCount = 1;
        cfg.topics.push_back(ts);
        client = std::make_unique<Client>(cfg);
        client->onError([this](const std::string&) { errors.fetch_add(1); });
        std::this_thread::sleep_for(150ms);
        gw = std::make_unique<SubGateway>(agentDir + "/aeron", inStr, egStr, m);
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
        bool ok = false;
        for (int i = 0; i < 6000; ++i) {
            gw->poll(16);
            if (f.ready()) { ok = true; break; }
            std::this_thread::sleep_for(2ms);
        }
        EXPECT_TRUE(ok);
        f.check();
    }
    ~Rig() {
        client->close();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }
    Subscription subscribe() {
        auto th = client->topic("inventory");
        SubscribeOptions o; o.from = From::earliest(); o.isolation = Isolation::ReadUncommitted;
        return client->subscribe(th, 0, o, [](const Record&) {});
    }
};
}  // namespace

TEST(SubscribeNotLeaderRegression, BareNotLeaderOnceThenOkBinds) {
    Rig r("snl_a", 4700, 4701, SubMode::OkAfterOneNotLeader, 60'000);
    auto sub = r.subscribe();
    bool bound = false;
    // Within ~5 s: the retry is paced by sub-second backoff.  (Some unrelated
    // mechanism re-sends a pending Subscribe at ~10 s; binding by then would
    // not prove the NotLeader retry.)
    for (int i = 0; i < 2500; ++i) {
        r.gw->poll(16);
        if (sub.subscriptionId() != 0) { bound = true; break; }
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(bound) << "gateway saw " << r.gw->subscribesSeen.load() << " Subscribe(s)";
    EXPECT_TRUE(sub.active());
    EXPECT_EQ(sub.failReason(), SubscriptionFailReason::None);
    EXPECT_GE(r.gw->subscribesSeen.load(), 2);
    EXPECT_EQ(r.errors.load(), 0);
    sub.unsubscribe();
}

TEST(SubscribeNotLeaderRegression, BareNotLeaderForeverFailsExplicitlyWithinBudget) {
    // 60 s subscribe timeout: the failure must come from the retry budget, not the clock.
    Rig r("snl_b", 4710, 4711, SubMode::AlwaysNotLeader, 60'000);
    const auto t0 = std::chrono::steady_clock::now();
    auto sub = r.subscribe();
    bool failed = false;
    for (int i = 0; i < 6000 * 3; ++i) {
        r.gw->poll(16);
        if (sub.finished()) { failed = true; break; }
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(failed) << "gateway saw " << r.gw->subscribesSeen.load() << " Subscribe(s)";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 20s);
    EXPECT_EQ(sub.failReason(), SubscriptionFailReason::RejectedByBroker);
    EXPECT_EQ(sub.brokerErrorCode(), static_cast<uint16_t>(rp::ErrorCode::NotLeader));
    EXPECT_GE(r.errors.load(), 1);
    EXPECT_GE(r.gw->subscribesSeen.load(), 2);
}

TEST(SubscribeNotLeaderRegression, NoAnswerFailsWithExplicitTimeout) {
    Rig r("snl_c", 4720, 4721, SubMode::Silent, 1'500);
    auto sub = r.subscribe();
    bool failed = false;
    for (int i = 0; i < 6000; ++i) {
        r.gw->poll(16);
        if (sub.finished()) { failed = true; break; }
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(failed);
    EXPECT_EQ(sub.failReason(), SubscriptionFailReason::TimedOut);
    EXPECT_GE(r.errors.load(), 1);
}
