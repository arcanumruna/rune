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
    cfg.transport.dataDir = "/tmp/inv_agent_" + std::to_string(::getpid()) + "_" +
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
    ASSERT_TRUE(agent.submitRegisterProducer(fakeHandle));

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
    cfg.transport.dataDir = "/tmp/inv_agent_" + std::to_string(::getpid()) + "_" +
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
    ASSERT_TRUE(agent.submitRegisterProducer(fakeHandle));

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
    cfg.transport.dataDir = "/tmp/inv_agent_" + std::to_string(::getpid()) + "_" +
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
    ASSERT_TRUE(agent.submitRegisterProducer(fakeHandle));

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
