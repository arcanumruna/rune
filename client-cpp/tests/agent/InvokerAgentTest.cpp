// tests/agent/InvokerAgentTest.cpp — ClientAgent in invoker mode against the
// single-threaded InvokerGateway: no sleeps, iteration-capped drive loops.
#include <filesystem>
#include "../support/FreePort.h"
#include "../support/InvokerGateway.h"

#include <rune/client/agent/ClientAgent.h>

#include <gtest/gtest.h>

#include <algorithm>
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
} // namespace

TEST(InvokerAgent, ProduceResponsesAreBufferedForTheOwnerWhenNoHookIsSet) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = 3500;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = 3501;
    cfg.transport.dataDir = std::filesystem::temp_directory_path().string() + "/inv_agent_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = 3501;
    rca::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", "aeron:ipc", 3500,
                                    "aeron:ipc", 3501);
    auto drive = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
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
    EXPECT_EQ(agent.produceAcks()[0].baseOffset, 100);
    EXPECT_EQ(agent.produceAcks()[0].errorCode, 0);
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
    cfg.transport.dataDir = std::filesystem::temp_directory_path().string() + "/inv_agent_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = 3511;
    cfg.egressPollLimit = 2;  // reserve = 8 acks
    rca::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", "aeron:ipc", 3510,
                                    "aeron:ipc", 3511);
    // cap < 0 (default): until pred holds, 10 s failure deadline; explicit caps stay strict.
    auto drive = [&](auto&& pred, int cap = -1) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (int i = 0; cap < 0 ? std::chrono::steady_clock::now() < deadline : i < cap; ++i) {
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

// ---------------------------------------------------------------------------
// fix2 Important-1: drainRegistrationsForClose() must not miss a registration
// sitting right past a ring wrap, when the single read() call that reaches it
// consumes ONLY wrap padding (reports 0 "nothing to do", pre-fix).
// ---------------------------------------------------------------------------
TEST(InvokerAgent, CloseAdoptsRegistrationPastRingWrapRegression) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = 3600;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = 3601;
    cfg.transport.dataDir = std::filesystem::temp_directory_path().string() + "/inv_agent_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = 3601;
    // Small, precise command ring: 12 x 40-byte records (8-byte header + the
    // 32-byte RegisterProducer payload = ENCODED_SIZE) exactly fill 480/512 B.
    cfg.ringCapacity = 512;
    rca::ClientAgent agent(cfg);
    agent.start();

    int   registerCalls = 0;
    void* registeredHandle = nullptr;
    agent.setProducerRegisterFn([&](void* h) {
        ++registerCalls;
        registeredHandle = h;
    });

    // Fill with 12 x 32-byte-payload fillers (480/512 B used), then drain them
    // ALL in one doWork() cycle (ringDrainLimit default 128 covers all 12 in
    // a single read()) — head lands exactly at the tail (480), with nothing
    // real between head and the physical buffer end.
    for (int i = 0; i < 12; ++i) {
        ASSERT_TRUE(agent.testWriteNoopCommand(32)) << "filler " << i;
    }
    agent.doWork(monoNs());

    // Submit the registration: tail is at 480; the 40-byte record does not fit
    // the 32 B remaining to the physical buffer end, so it pads that remainder
    // and wraps, landing the WHOLE record at offset 0.
    void* const fakeHandle = reinterpret_cast<void*>(0x1234);
    ASSERT_EQ(agent.submitRegisterProducer(fakeHandle), rca::ClientAgent::Submit::Accepted);

    // Before the fix: the first (and, since drainRegistrationsForClose() stops
    // on n==0, ONLY) read() call sees head(480)..bufferEnd(512) as pure
    // padding, reports 0, and the outer loop breaks — the registration is
    // never adopted (leaked holder; its producer's ring never gets marked
    // closed either). After the fix, the same call continues past the padding
    // within itself and adopts it.
    const int total = agent.drainRegistrationsForClose();
    EXPECT_EQ(total, 1);
    EXPECT_EQ(registerCalls, 1);
    EXPECT_EQ(registeredHandle, fakeHandle);
}

// ---------------------------------------------------------------------------
// fix3 I1: drainRegistrationsForClose() must wait past a record that is
// CLAIMED but not yet COMMITTED, instead of treating "nothing currently
// readable" the same as "ring empty" and missing a fully committed
// registration sitting right behind it.
// ---------------------------------------------------------------------------
TEST(InvokerAgent, CloseDrainWaitsPastUncommittedClaimRegression) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = 3602;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = 3603;
    cfg.transport.dataDir = std::filesystem::temp_directory_path().string() + "/inv_agent_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = 3603;
    cfg.ringCapacity = 512;
    rca::ClientAgent agent(cfg);
    agent.start();

    int   registerCalls = 0;
    void* registeredHandle = nullptr;
    agent.setProducerRegisterFn([&](void* h) {
        ++registerCalls;
        registeredHandle = h;
    });

    // Claim space WITHOUT committing: any read() stops dead at this record
    // (its length word stays negative) regardless of what comes after it.
    const auto claimIdx = agent.testClaimNoopWithoutCommitting(8);
    ASSERT_GE(claimIdx, 0);

    // A real registration, committed immediately, lands right behind the
    // still-uncommitted claim above (the ring's tail already reflects both).
    void* const fakeHandle = reinterpret_cast<void*>(0x5678);
    ASSERT_EQ(agent.submitRegisterProducer(fakeHandle), rca::ClientAgent::Submit::Accepted);

    // Deterministic, single-threaded: the yield hook commits the stuck claim
    // from INSIDE drainRegistrationsForClose()'s own wait loop, on the same
    // thread — no second thread, no real-time wait needed. Pre-fix, the old
    // `if (n == 0) break;` would never reach a yield at all (it would return 0
    // and stop on the very first call), so this hook firing at least once,
    // AND the registration being adopted, both prove the fix.
    int yieldCalls = 0;
    agent.testSetCloseDrainYieldHook([&] {
        ++yieldCalls;
        if (yieldCalls == 1) agent.testCommitClaim(claimIdx);
    });

    const int total = agent.drainRegistrationsForClose();

    EXPECT_GE(yieldCalls, 1) << "drainRegistrationsForClose() must wait (yield) past the "
                                "uncommitted claim instead of stopping immediately";
    EXPECT_EQ(total, 2) << "the committed noop + the registration behind it";
    EXPECT_EQ(registerCalls, 1);
    EXPECT_EQ(registeredHandle, fakeHandle);
}

// ---------------------------------------------------------------------------
// fix5 Important-2(c): close() can no longer wedge forever on an ORPHANED claim
// (claimed, never committed — the writer is gone). The drain waits a bounded
// number of no-progress yields, then unblocks the ring (Aeron's
// ManyToOneRingBuffer::unblock) and adopts the committed registration behind
// the orphan. Pre-fix: drainRegistrationsForClose() yielded forever (hang).
// ---------------------------------------------------------------------------
namespace {
rca::AgentConfig closeDrainCfg(int32_t in, int32_t eg) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = in;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = eg;
    cfg.transport.dataDir = std::filesystem::temp_directory_path().string() + "/inv_agent_" + std::to_string(::getpid()) + "_" +
                            std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = eg;
    cfg.ringCapacity = 512;
    return cfg;
}
} // namespace

TEST(InvokerAgent, CloseDrainUnblocksAnOrphanedClaimAndAdoptsWhatIsBehindRegression) {
    rca::AgentConfig cfg = closeDrainCfg(3604, 3605);
    rca::ClientAgent agent(cfg);
    agent.start();

    int   registerCalls = 0;
    void* registeredHandle = nullptr;
    agent.setProducerRegisterFn([&](void* h) {
        ++registerCalls;
        registeredHandle = h;
    });

    // The orphan: claimed and NEVER committed (no hook commits it here).
    ASSERT_GE(agent.testClaimNoopWithoutCommitting(8), 0);
    void* const fakeHandle = reinterpret_cast<void*>(0x9abc);
    ASSERT_EQ(agent.submitRegisterProducer(fakeHandle), rca::ClientAgent::Submit::Accepted);

    constexpr int kCap = 64; // instead of the 1M production default — same code path
    agent.testSetCloseDrainUnblockAfterYields(kCap);
    int yields = 0;
    agent.testSetCloseDrainYieldHook([&] { ++yields; });

    const int total = agent.drainRegistrationsForClose(); // must return

    EXPECT_EQ(total, 1) << "the registration behind the orphan (the orphan is now padding)";
    EXPECT_EQ(registerCalls, 1);
    EXPECT_EQ(registeredHandle, fakeHandle);
    EXPECT_EQ(agent.testCloseDrainUnblockCalls(), 1);
    EXPECT_EQ(yields, kCap - 1) << "unblocks exactly at the cap, not before";
}

// Nothing unblockable (a header-less orphan with nothing committed behind it):
// the drain must still terminate, after kCloseDrainMaxFailedUnblocks rounds.
TEST(InvokerAgent, CloseDrainTerminatesWhenTheRingCannotBeUnblockedRegression) {
    rca::AgentConfig cfg = closeDrainCfg(3606, 3607);
    rca::ClientAgent agent(cfg);
    agent.start();

    // A header-less orphan: the tail is advanced but the record's length word
    // is 0, and nothing is committed anywhere behind it — unblock() has
    // nothing to pad up to and returns false every time.
    const auto idx = agent.testClaimNoopWithoutCommitting(8);
    ASSERT_GE(idx, 0);
    agent.testZeroClaimHeader(idx);

    agent.testSetCloseDrainUnblockAfterYields(16);
    const int total = agent.drainRegistrationsForClose(); // must return

    EXPECT_EQ(total, 0);
    EXPECT_EQ(agent.testCloseDrainUnblockCalls(), rca::ClientAgent::kCloseDrainMaxFailedUnblocks);
}

// ---- #767: a seed move is a state of the duty cycle, never a block
// ---------------------------------------------------

namespace {
int64_t g_fakeNowNs = 0;
int64_t fakeNow() noexcept { return g_fakeNowNs; }
std::string loopbackUdp(uint16_t port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }
rca::AgentConfig invokerAgentConfig(const std::string& ingress, int32_t in, int32_t eg) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = ingress;
    cfg.transport.ingressStreamId = in;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = eg;
    cfg.transport.dataDir = std::filesystem::temp_directory_path().string() + "/inv_agent_" +
                            std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
    cfg.transport.driverMode   = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = eg;
    return cfg;
}
}  // namespace

// The issue's symptom: the seed's move to an unresolvable target used to block the duty thread for the resolve bound,
// so every other session's keepalive stalled.  Now the move is pending for the full bound while each duty cycle takes
// exactly one conductor pass and a leader session's keepalives keep their cadence (injected clock).  The stalled
// driver (test seam) stands for "the target never resolves"; the leader's Pings, offered during the window, reach the
// gateway once it runs again.
TEST(ClientAgentRegression, OtherSessionsKeepAliveWhileTheSeedRetargetsRegression) {
    const uint16_t leaderPort = rune::client::testing::allocFreeUdpPort();
    rca::AgentConfig cfg = invokerAgentConfig("aeron:ipc", 3540, 3541);
    cfg.transport.egressChannel = "aeron:ipc";
    rca::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", "aeron:ipc", 3540, "aeron:ipc", 3541);
    const std::string leaderCh = loopbackUdp(leaderPort);
    const int leaderEp = gw.addNode(leaderCh, 3540);
    (void)leaderEp;
    g_fakeNowNs = 1'000'000'000'000LL;
    constexpr int64_t kStep = 10'000'000LL;   // 10 ms per duty cycle
    auto cycle = [&] { g_fakeNowNs += kStep; agent.doWork(g_fakeNowNs); gw.pump(); };
    auto drive = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            cycle();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    ASSERT_TRUE(agent.submitConnect());
    ASSERT_TRUE(drive([&] { return agent.sessionState() == rcs::SessionState::Connected; }));
    rcs::SessionMachine& leader = agent.sessionForEndpointForTest(leaderCh);
    ASSERT_TRUE(drive([&] { return leader.state() == rcs::SessionState::Connected; }))
        << "the leader session never connected";
    const int64_t leaderKeepalive = leader.keepaliveCorrelationId();
    auto leaderPings = [&] {
        int n = 0;
        for (const int64_t c : gw.pingCorrIds) n += c == leaderKeepalive ? 1 : 0;
        return n;
    };

    // The move: the driver is stalled (nothing resolves), the resolve bound is 3 s on the injected clock.
    agent.testTransportMut().setResolveClock(&fakeNow);
    agent.testTransportMut().setDriverStalledForTest(true, 3'000'000'000LL);
    const int pingsBefore = leaderPings();
    uint64_t token = 0;
    const uint64_t pumpsAtBegin = agent.testTransportMut().dutyCyclePumpsForTest();
    ASSERT_TRUE(agent.testRetargetSeed(loopbackUdp(rune::client::testing::allocFreeUdpPort()), 0, &token));
    EXPECT_EQ(agent.testTransportMut().dutyCyclePumpsForTest(), pumpsAtBegin) << "starting the move pumped (it blocks)";
    ASSERT_TRUE(agent.testSeedMovePending());
    const int64_t t0 = g_fakeNowNs;
    int cycles = 0;
    uint64_t maxPumpsPerCycle = 0;
    while (agent.testSeedMovePending() && cycles < 2000) {   // hard cap
        const uint64_t pumps = agent.testTransportMut().dutyCyclePumpsForTest();
        g_fakeNowNs += kStep;
        agent.doWork(g_fakeNowNs);
        maxPumpsPerCycle = std::max(maxPumpsPerCycle, agent.testTransportMut().dutyCyclePumpsForTest() - pumps);
        ++cycles;
    }
    const int64_t pendingNs = g_fakeNowNs - t0;
    EXPECT_FALSE(agent.testSeedMovePending()) << "the move never ended";
    EXPECT_GE(pendingNs, 3'000'000'000LL) << "the move was pending for the full resolve bound";
    EXPECT_LE(maxPumpsPerCycle, 1u) << "a duty cycle took more than one pump: something blocked inside it";
    const auto res = agent.takeSeedMoveResult();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->token, token);
    EXPECT_FALSE(res->ok) << "the target never resolved";
    EXPECT_EQ(agent.seedMoves(), 0u) << "a failed move changed nothing on the seed";

    // The leader's keepalives (1 s interval) were offered through the window; the gateway runs again and sees them.
    agent.testTransportMut().setDriverStalledForTest(false);
    ASSERT_TRUE(drive([&] { return leaderPings() >= pingsBefore + 2; }));
    EXPECT_GE(leaderPings() - pingsBefore, 2)
        << "the leader's keepalives kept their cadence while the seed's move was pending";
    EXPECT_EQ(leader.state(), rcs::SessionState::Connected);
}

// A new seed move supersedes a pending one: the first is answered {token, failed} at once, the second completes.
TEST(ClientAgentRegression, ASupersededSeedMoveIsAnsweredFailedRegression) {
    rca::AgentConfig cfg = invokerAgentConfig("aeron:ipc", 3550, 3551);
    rca::ClientAgent agent(cfg);
    agent.start();
    const std::string c1 = loopbackUdp(rune::client::testing::allocFreeUdpPort());
    const std::string c2 = loopbackUdp(rune::client::testing::allocFreeUdpPort());
    uint64_t t1 = 0, t2 = 0;
    ASSERT_TRUE(agent.testRetargetSeed(c1, 0, &t1));
    ASSERT_TRUE(agent.testRetargetSeed(c2, 0, &t2));
    EXPECT_NE(t1, t2);
    const auto r1 = agent.takeSeedMoveResult();
    ASSERT_TRUE(r1.has_value()) << "the superseded move's failure is published at once";
    EXPECT_EQ(r1->token, t1);
    EXPECT_FALSE(r1->ok);
    ASSERT_TRUE(agent.testSettleSeedMove());
    const auto r2 = agent.takeSeedMoveResult();
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->token, t2);
    EXPECT_TRUE(r2->ok);
    EXPECT_FALSE(agent.takeSeedMoveResult().has_value());
    EXPECT_EQ(agent.testSeedIngressChannel(), c2);
    EXPECT_EQ(agent.seedMoves(), 1u) << "only the move that completed ran its post-actions";
}

// The machine path: a connect-time Redirect to a resolvable endpoint.  The ConnectRequest is NOT sent to the old
// gateway again, waits (re-offered under the SAME correlation id) until the swap, reaches the new gateway, and the
// session connects.  The redirect / connect-retry budgets are what the blocking retarget spent: one redirect, no
// connect timeout.
TEST(ClientAgentRegression, AConnectRedirectWaitsForTheSwapAndSpendsTheSameBudgetsRegression) {
    const uint16_t aPort = rune::client::testing::allocFreeUdpPort();
    const uint16_t bPort = rune::client::testing::allocFreeUdpPort();
    rca::AgentConfig cfg = invokerAgentConfig(loopbackUdp(aPort), 3560, 3561);
    rca::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", loopbackUdp(aPort), 3560, "aeron:ipc", 3561);
    const int b = gw.addNode(loopbackUdp(bPort), 3560);
    gw.connectRedirect[0] = "127.0.0.1:" + std::to_string(bPort);
    auto drive = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            agent.doWork(monoNs());
            gw.pump();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    ASSERT_TRUE(agent.submitConnect());
    int maxRedirects = 0;
    uint64_t maxConnectFailures = 0;
    bool sawPendingWithConnecting = false;
    ASSERT_TRUE(drive([&] {
        const auto& m = agent.seedMachine();
        maxRedirects = std::max(maxRedirects, m.redirectCount());
        maxConnectFailures = std::max(maxConnectFailures, m.connectFailures());
        sawPendingWithConnecting |= agent.testSeedMovePending() && m.state() == rcs::SessionState::Connecting;
        return agent.sessionState() == rcs::SessionState::Connected;
    })) << "the seed never connected on the redirected gateway";
    EXPECT_TRUE(sawPendingWithConnecting) << "the connect waited inside the pending move (it was not blocking)";
    EXPECT_EQ(gw.connectsAt[0], 1) << "the ConnectRequest never went back to the redirecting gateway";
    ASSERT_GE(gw.connectsAt[b], 1);
    ASSERT_EQ(static_cast<int>(gw.connectCorrIds.size()), gw.connectsAt[0] + gw.connectsAt[b]);
    for (std::size_t i = 2; i < gw.connectCorrIds.size(); ++i)
        EXPECT_EQ(gw.connectCorrIds[i], gw.connectCorrIds[1]) << "re-offers during the move keep the correlation id";
    EXPECT_NE(gw.connectCorrIds[1], gw.connectCorrIds[0]);
    EXPECT_EQ(maxRedirects, 1) << "one redirect spent";
    EXPECT_EQ(maxConnectFailures, 0u) << "no connect timeout was spent while the move was pending";
    EXPECT_EQ(agent.testSeedIngressChannel(), loopbackUdp(bPort));
}

// ---- #767 follow-up: a request to stay cancels a pending move elsewhere; a second move to the same target joins
// -----------------------------------------------------------------------------------------------------------

// A request to stay where the seed is ("already there") while a move elsewhere is pending cancels that move: its
// {token, false} result is published once, the seed never swaps to the other target (driven well past the resolve
// bound with the driver running), and the old pair keeps offering.  Through failoverSeedTo's and the bootstrap
// fallback's "already there" returns.
TEST(ClientAgentRegression, ARequestToStayCancelsAPendingSeedMoveRegression) {
    rca::AgentConfig cfg = invokerAgentConfig("aeron:ipc", 3570, 3571);
    rca::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", "aeron:ipc", 3570, "aeron:ipc", 3571);
    g_fakeNowNs = 1'000'000'000'000LL;
    agent.testTransportMut().setResolveClock(&fakeNow);
    for (int i = 0; i < 20000 && !gw.ready(); ++i) { g_fakeNowNs += 1'000'000LL; agent.doWork(g_fakeNowNs); gw.pump(); }
    ASSERT_TRUE(gw.ready());
    const std::string current = agent.testSeedIngressChannel();
    for (int variant = 0; variant < 2; ++variant) {
        agent.testTransportMut().setDriverStalledForTest(true, 3'000'000'000LL);   // the move stays pending
        const std::string other = loopbackUdp(rune::client::testing::allocFreeUdpPort());
        uint64_t token = 0;
        ASSERT_TRUE(agent.testRetargetSeed(other, 0, &token));
        ASSERT_TRUE(agent.testSeedMovePending());
        EXPECT_FALSE(agent.takeSeedMoveResult().has_value());
        if (variant == 0) {
            uint64_t t2 = 0;
            EXPECT_EQ(agent.failoverSeedTo(current, g_fakeNowNs, /*connectNow=*/false, t2),
                      rca::ClientAgent::SeedMove::Done);
        } else {
            EXPECT_TRUE(agent.testRetargetSeedToBootstrap());   // the bootstrap is where the seed is
        }
        EXPECT_FALSE(agent.testSeedMovePending()) << "variant " << variant << ": the move is cancelled";
        const auto r = agent.takeSeedMoveResult();
        ASSERT_TRUE(r.has_value()) << "variant " << variant;
        EXPECT_EQ(r->token, token);
        EXPECT_FALSE(r->ok);
        EXPECT_FALSE(agent.takeSeedMoveResult().has_value()) << "published once";
        agent.testTransportMut().setDriverStalledForTest(false);
        for (int i = 0; i < 2000; ++i) {   // hard cap, well past the resolve bound (10 ms steps = 20 s)
            g_fakeNowNs += 10'000'000LL;
            agent.doWork(g_fakeNowNs);
            gw.pump();
        }
        EXPECT_EQ(agent.testSeedIngressChannel(), current) << "the seed never swapped to the cancelled target";
        EXPECT_FALSE(agent.takeSeedMoveResult().has_value()) << "no later result for the cancelled token";
        EXPECT_EQ(agent.seedMoves(), 0u);
        const uint8_t frame[] = {1, 2, 3, 4};
        bool offered = false;
        for (int i = 0; i < 4000 && !offered; ++i) {   // hard cap
            agent.doWork(g_fakeNowNs);
            gw.pump();
            offered = agent.testTransportMut().offer(frame, sizeof frame);
        }
        EXPECT_TRUE(offered) << "offers flow on the old channel again";
    }
}

// Two moves to the same target join: one transport move, ONE result {token, ok}, the post-actions applied once.
TEST(ClientAgentRegression, JoinedSeedMovesPublishOneResultRegression) {
    rca::AgentConfig cfg = invokerAgentConfig("aeron:ipc", 3580, 3581);
    rca::ClientAgent agent(cfg);
    agent.start();
    const std::string target = loopbackUdp(rune::client::testing::allocFreeUdpPort());
    uint64_t a = 0, b = 0;
    ASSERT_TRUE(agent.testRetargetSeed(target, 0, &a));
    ASSERT_TRUE(agent.testRetargetSeed(target, 0, &b));
    EXPECT_EQ(a, b) << "the second move joined the first";
    EXPECT_FALSE(agent.takeSeedMoveResult().has_value()) << "a join publishes no superseded result";
    ASSERT_TRUE(agent.testSettleSeedMove());
    const auto r = agent.takeSeedMoveResult();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->token, a);
    EXPECT_TRUE(r->ok);
    EXPECT_FALSE(agent.takeSeedMoveResult().has_value()) << "exactly one result";
    EXPECT_EQ(agent.seedMoves(), 1u) << "the post-actions ran once";
    EXPECT_EQ(agent.testSeedIngressChannel(), target);
}
