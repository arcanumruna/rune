// NsHotPathSuspendAllocRegressionTest.cpp - namespaces Phase 2 Task 15 (rules 4, 5): the gateway's SUSPENDED refusal of a
// produce allocates nothing.  A REAL authenticating BrokerNode (default config), a REAL v14 session of a suspended
// namespace; the Produce frames are fed to the gateway's real client-frame path ON THE CONTROL THREAD, inside an AllocWindow
// (the calling thread's global operator new), 10 000 requests in chunks (the test side drains the answers between chunks so
// the egress never back-pressures: the chunk boundary is outside the window).
//
//   | state                      | request                | expected                                          | test
//   |----------------------------|------------------------|---------------------------------------------------|-----
//   | ReadOnly (produce refused) | 10 000 Produce frames  | every one answered NamespaceSuspended, 0 allocs   | NsHotPathAllocRegression.SuspendedRefusalZeroAllocRegression
//   | RejectAll                  | (session revoked)      | not covered: the session no longer exists         | -
#include "support/AllocWindow.h"
#include "support/AuthRig.h"

#include <rune/client/Client.h>

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <string>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {
constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw  = "namespace-user-password-12";
}

TEST(NsHotPathAllocRegression, SuspendedRefusalZeroAllocRegression) {
    AuthBroker b("nshp_susp");
    b.bootstrapAdmin(kAdminPw);
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = "admin";
    c.password        = kAdminPw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    rc::Client admin(c);
    admin.connectAndWait(20s);
    ASSERT_TRUE(admin.createNamespace("s").has_value());
    b.createUser("s/u", kUserPw, {prefix(SO::Create, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""), prefix(SO::Describe, ST::Topic, "")});

    AuthWireClient& w = b.wire();
    const int64_t sid = w.connect(b.channel(), pc::kClientVersionNamespaces);
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(w.login(b.channel(), sid, "s/u", {kUserPw, kUserPw}).ok());
    ASSERT_TRUE(admin.suspendNamespace("s", pc::NsSuspendMode::ReadOnly).has_value());

    // A Produce frame for the suspended session (topic id is irrelevant: the gate is before any lookup).
    rune::protocol::RecordBatchBuilder rb(0, 1000);
    rb.append("k", "v", 1000);
    const auto batch = rb.build();
    pc::Produce msg;
    msg.sessionId = sid; msg.topicId = 1; msg.partition = 0; msg.partitionKnown = 1; msg.acks = 1;
    msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
    msg.recordBatch = std::span<const uint8_t>(batch);
    // The gateway's gate flips when it applies the state change: wait until the first probe is refused.
    int64_t corr = 1'000'000;
    auto probe = [&] {
        msg.correlationId = ++corr;
        if (!w.raw().produce(b.channel(), AuthWireClient::kClientStream, sid, corr, 1, 0, 1, batch)) return false;
        const auto r = w.raw().awaitProduceResponse(corr);
        return r && r->errorCode == static_cast<int16_t>(pc::ErrorCode::NamespaceSuspended);
    };
    bool refused = false;
    for (int i = 0; i < 200 && !refused; ++i) refused = probe();
    ASSERT_TRUE(refused) << "the suspended namespace refuses a produce (NamespaceSuspended)";

    constexpr int kChunk = 50, kChunks = 200;   // 10 000
    auto* gw = b.node().gatewayForTest();
    ASSERT_NE(gw, nullptr);
    auto runChunk = [&](int64_t firstCorr) -> uint64_t {
        return b.node().onControlThreadForTest([&]() -> uint64_t {
            rune::testing::AllocWindow win;
            for (int i = 0; i < kChunk; ++i) {
                msg.correlationId = firstCorr + i;
                std::array<uint8_t, 512> buf{};   // a stack frame: encoding allocates nothing
                const std::size_t n = msg.encode(buf.data());
                (void)gw->applyClientFrameForTest(buf.data(), n);
            }
            return win.count();
        });
    };
    (void)runChunk(corr + 1);   // warm-up chunk
    corr += kChunk;
    ASSERT_TRUE(w.raw().pollUntil([&] { return w.raw().ackFor(corr).has_value(); })) << "warm-up answers arrive";
    uint64_t allocs = 0;
    int answered = 0;
    for (int ch = 0; ch < kChunks; ++ch) {
        const int64_t first = corr + 1;
        allocs += runChunk(first);
        corr += kChunk;
        ASSERT_TRUE(w.raw().pollUntil([&] { return w.raw().ackFor(corr).has_value(); })) << "chunk " << ch << ": the last answer arrives";
        for (int i = 0; i < kChunk; ++i) {
            const auto a = w.raw().ackFor(first + i);
            if (a && a->errorCode == static_cast<int16_t>(pc::ErrorCode::NamespaceSuspended)) ++answered;
        }
    }
    EXPECT_EQ(allocs, 0u) << "the Suspended refusal allocated on the gateway";
    EXPECT_EQ(answered, kChunk * kChunks) << "not vacuous: every request was refused NamespaceSuspended";
}
