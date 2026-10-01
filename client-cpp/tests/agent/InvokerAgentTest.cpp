// tests/agent/InvokerAgentTest.cpp — ClientAgent in invoker mode against the
// single-threaded InvokerGateway: no sleeps, iteration-capped drive loops.
#include "../support/InvokerGateway.h"

#include <rune/client/agent/ClientAgent.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <unistd.h>
#include <vector>

namespace rca = rune::client::agent;
namespace rcs = rune::client::session;

namespace {
std::atomic<int> g_ctr{0};
int64_t monoNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}
constexpr int kCap = 200'000;
} // namespace

TEST(InvokerAgent, ProduceResponsesAreBufferedForTheOwnerWhenNoHookIsSet) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = 3500;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = 3501;
    cfg.transport.dataDir = "/tmp/inv_agent_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = 3501;
    rca::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", "aeron:ipc", 3500,
                                    "aeron:ipc", 3501);
    auto drive = [&](auto&& pred) {
        for (int i = 0; i < kCap; ++i) {
            agent.doWork(monoNs());
            gw.pump();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    ASSERT_TRUE(agent.submitConnect());
    ASSERT_TRUE(drive([&] { return agent.sessionState() == rcs::SessionState::Connected; }));

    gw.ack(test_support::InvokerGateway::Produced{1, 0, 42, 777, 3, 0, 0});
    ASSERT_TRUE(drive([&] { return !agent.produceAcks().empty(); }));
    ASSERT_EQ(agent.produceAcks().size(), 1u);
    EXPECT_EQ(agent.produceAcks()[0].correlationId, 777);
    EXPECT_EQ(agent.produceAcks()[0].baseOffsetOrError, 100);
    agent.clearProduceAcks();
    EXPECT_TRUE(agent.produceAcks().empty());
    // Destruction order: gw (declared last) closes its Aeron client before the
    // agent stops the driver it is attached to.
}

// M6: the produceAcks_ reservation is never exceeded on the hot path — when the
// buffer is full the agent stops consuming egress for the cycle (nothing dropped,
// resumes once the owner clears it).
TEST(InvokerAgent, AckBurstBeyondReserveDoesNotAllocateRegression) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = 3510;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = 3511;
    cfg.transport.dataDir = "/tmp/inv_agent_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = 3511;
    cfg.egressPollLimit = 2;  // reserve = 8 acks
    rca::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", "aeron:ipc", 3510,
                                    "aeron:ipc", 3511);
    auto drive = [&](auto&& pred, int cap = kCap) {
        for (int i = 0; i < cap; ++i) {
            agent.doWork(monoNs());
            gw.pump();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    ASSERT_TRUE(agent.submitConnect());
    ASSERT_TRUE(drive([&] { return agent.sessionState() == rcs::SessionState::Connected; }));

    constexpr int kBurst = 20;
    for (int i = 0; i < kBurst; ++i) {
        gw.ack(test_support::InvokerGateway::Produced{1, 0, 42, 1000 + i, 1, 0, 0});
    }
    constexpr std::size_t cap0 = 8;  // egressPollLimit * 4, reserved in the ctor
    ASSERT_EQ(agent.testProduceAckCapacity(), cap0);
    ASSERT_TRUE(drive([&] { return agent.produceAcks().size() >= cap0; }));
    for (int i = 0; i < 200; ++i) agent.doWork(monoNs());  // never cleared
    EXPECT_LE(agent.produceAcks().size(), cap0);
    EXPECT_EQ(agent.testProduceAckCapacity(), cap0) << "reserve exceeded: the hot path allocated";

    int total = 0;
    std::vector<int64_t> seen;
    ASSERT_TRUE(drive([&] {
        for (const auto& a : agent.produceAcks()) seen.push_back(a.correlationId);
        total += static_cast<int>(agent.produceAcks().size());
        agent.clearProduceAcks();
        return total >= kBurst;
    }));
    EXPECT_EQ(total, kBurst) << "no ack may be dropped";
    for (int i = 0; i < kBurst; ++i) EXPECT_EQ(seen[static_cast<std::size_t>(i)], 1000 + i);
    EXPECT_EQ(agent.testProduceAckCapacity(), cap0);
}
