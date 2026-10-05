// UnresolvedPeerRegressionTest.cpp — DR-15 (2026-10-05): a peer whose hostname
// does not resolve at the moment this node starts (the peer container / k8s pod
// is itself restarting, DNS lag) must NEVER stop this node from starting, and
// must be attached as soon as it resolves.
//
// Root cause: the per-worker replication plane registered one publication per
// peer at startup; Aeron rejected the registration ("Unresolved - endpoint=
// rune-2:41256"), AeronPubRegistry::attach left allResolved()==false, the plane's
// attach() returned false and BrokerNode threw "shard 0 replication plane did
// not resolve — refusing to start".  Two brokers restarted together both exited.
// The internal-partition plane (AeronPartitionTransport) dropped an errored peer
// FOREVER (never re-added), and the consensus sink re-added on every send and held
// startup for its whole 5 s budget.
//
// Every test runs on an INVOKER driver + client pumped by the test thread: the
// test drives the duty passes itself (no background thread, no sleep).
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/UdpMessageSink.h>
#include <rune/node/WorkerReplicationTransport.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/aeronutil/testing/FreePort.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>

using namespace rune::node;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

// RFC 6761: ".invalid" never resolves.
const std::string kUnresolvable = "aeron:udp?endpoint=rune-unresolvable.invalid:40999";

struct InvokerShard {
    fs::path dir;
    std::unique_ptr<EmbeddedDriver> driver;
    std::shared_ptr<aeron::Aeron> client;
    InvokerShard() {
        static std::atomic<int> seq{0};
        dir = fs::temp_directory_path() /
              ("rune_unres_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 64 * 1024;
        driver = std::make_unique<EmbeddedDriver>(c, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(dir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
    }
    ~InvokerShard() { client.reset(); driver.reset(); std::error_code ec; fs::remove_all(dir, ec); }
    rune::aeronutil::AeronPump pump() { return driver->pumpWith(client.get()); }
};

std::chrono::milliseconds since(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
}

/// Drive `pass` (one duty pass of everything under test) until `done` holds.
/// Bounded by a pass cap AND a generous wall-clock guard (a hang guard only — it
/// decides nothing: the condition is the assertion).
template <class Pass, class Done>
bool driveUntil(Pass&& pass, Done&& done, int64_t maxPasses = 5'000'000) {
    const auto t0 = std::chrono::steady_clock::now();
    for (int64_t i = 0; i < maxPasses; ++i) {
        if (done()) return true;
        pass();
        if ((i & 1023) == 0 && since(t0) > 20s) break;
    }
    return done();
}

} // namespace

// ---------------------------------------------------------------------------
// The per-worker replication plane (the DR-15 crash site).
// ---------------------------------------------------------------------------

// RED on bc153c3: resolved() was false (=> BrokerNode refused to start).
TEST(UnresolvedPeerRegression, PlaneIsUsableWithAnUnresolvablePeerRegression) {
    InvokerShard s;
    const auto t0 = std::chrono::steady_clock::now();
    WorkerReplicationTransport t(s.client, 0, "aeron:ipc", 7501,
                                 {{1, kUnresolvable}, {2, std::string("aeron:ipc")}}, 7501, s.pump());
    EXPECT_TRUE(t.resolved()) << "an unresolvable PEER must not make the plane (and the node) refuse to start";
    EXPECT_LT(since(t0), 1000ms) << "the startup resolve ends when Aeron answers (no budget wait)";
}

// A genuinely LOCAL failure (own inbound bind) stays a refusal.
TEST(UnresolvedPeerRegression, PlaneWithBadInboundStillRefusesRegression) {
    InvokerShard s;
    WorkerReplicationTransport t(s.client, 0, "aeron:udp?endpoint=not-a-valid-endpoint", 7502,
                                 {{2, std::string("aeron:ipc")}}, 7502, s.pump());
    EXPECT_FALSE(t.resolved());
}

TEST(UnresolvedPeerRegression, RegistryReAddsAnUnresolvedPeerWithBackoffUntilItResolvesRegression) {
    InvokerShard s;
    auto pump = s.pump();
    AeronPubRegistry reg(s.client, {{1, kUnresolvable}, {2, std::string("aeron:ipc")}}, 7503, pump);
    ASSERT_TRUE(reg.configured());
    EXPECT_TRUE(reg.hasPeer(2));
    EXPECT_FALSE(reg.hasPeer(1));
    EXPECT_FALSE(reg.allResolved());
    EXPECT_EQ(reg.resolvingPeerCount(), 1u);

    // Still unresolvable: it keeps being re-added, spaced by the pass-count
    // backoff (2, 4, 8, ... passes) — never on every pass.
    for (int i = 0; i < 256; ++i) { (void)pump.pump(); (void)reg.drivePeerResolve(); }
    EXPECT_FALSE(reg.hasPeer(1));
    EXPECT_GE(reg.peerReAdds(), 2u) << "an unresolved peer must be re-added, not dropped";
    EXPECT_LE(reg.peerReAdds(), 9u) << "re-adds must back off by pass count (256 passes)";

    // The peer becomes resolvable: the next due re-add attaches it.
    reg.setPeerChannelForTest(1, "aeron:ipc");
    ASSERT_TRUE(driveUntil([&] { (void)pump.pump(); (void)reg.drivePeerResolve(); },
                           [&] { return reg.hasPeer(1); }));
    EXPECT_TRUE(reg.allResolved());
    EXPECT_EQ(reg.resolvingPeerCount(), 0u);
    EXPECT_EQ(reg.drivePeerResolve(), 0) << "fully resolved: the per-pass drive is a no-op";
}

// End to end on the plane: node 0 starts while node 1's hostname is
// unresolvable; node 1 (which CAN reach node 0) is up.  Once node 1 resolves,
// node 0's plane re-adds it and both sides handshake and exchange frames exactly
// like a peer that connected late.
TEST(UnresolvedPeerRegression, LateResolvingPeerHandshakesAndReceivesFramesRegression) {
    InvokerShard sa, sb;
    const uint16_t pa = rune::aeronutil::testing::allocFreeUdpPort();
    const uint16_t pb = rune::aeronutil::testing::allocFreeUdpPort();
    const std::string chA = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(pa);
    const std::string chB = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(pb);
    constexpr int32_t kStream = 7504;
    auto pumpA = sa.pump();
    auto pumpB = sb.pump();
    WorkerReplicationTransport a(sa.client, 0, chA, kStream, {{1, kUnresolvable}}, kStream, pumpA);
    WorkerReplicationTransport b(sb.client, 0, chB, kStream, {{0, chA}}, kStream, pumpB);
    ASSERT_TRUE(a.resolved());
    ASSERT_TRUE(b.resolved());
    a.enablePlaneHandshake(0, 1, {1});
    b.enablePlaneHandshake(1, 1, {0});

    int fetchesAtB = 0;
    int64_t nowNs = 0;
    auto pass = [&] {
        nowNs += WorkerReplicationTransport::kHelloRetryNs;   // injected clock: one hello per pass
        (void)pumpA.pump(); (void)pumpB.pump();
        (void)a.drivePeerResolve(); (void)b.drivePeerResolve();
        (void)a.driveHandshake(nowNs); (void)b.driveHandshake(nowNs);
        (void)a.poll(16, [](int32_t, int32_t, uint16_t, const uint8_t*, std::size_t) {});
        (void)b.poll(16, [&](int32_t, int32_t, uint16_t tid, const uint8_t*, std::size_t) {
            if (tid == static_cast<uint16_t>(rune::replication::ReplicationTemplateId::FetchRequest)) ++fetchesAtB;
        });
    };
    using PS = WorkerReplicationTransport::PeerPlaneState;
    // Node 1's hellos reach node 0, but node 0 cannot answer: node 1 never
    // verifies node 0 while node 0's publication to it is unresolved.
    ASSERT_TRUE(driveUntil(pass, [&] { return a.peerPlaneState(1) == PS::Verified; }));
    for (int i = 0; i < 64; ++i) pass();
    EXPECT_EQ(b.peerPlaneState(0), PS::Unverified);
    EXPECT_FALSE(a.allPeersResolved());

    // Node 1's hostname resolves now.
    a.setPeerChannelForTest(1, chB);
    ASSERT_TRUE(driveUntil(pass, [&] { return a.allPeersResolved() && b.peerPlaneState(0) == PS::Verified; }))
        << "the late-resolving peer was never attached / verified";

    rune::replication::FetchRequestMsg req{};
    req.leaderEpoch = 1; req.partition = 0; req.followerId = 0; req.fetchOffset = 0; req.maxBytes = 1024;
    ASSERT_TRUE(driveUntil([&] { (void)a.sendFetchRequestTo(1, req); pass(); }, [&] { return fetchesAtB > 0; }))
        << "data frames never reached the late-resolved peer";
}

// ---------------------------------------------------------------------------
// The internal-partition / txn-marker plane (AeronPartitionTransport).
// ---------------------------------------------------------------------------
TEST(UnresolvedPeerRegression, PartitionTransportReAddsAnUnresolvedPeerUntilItResolvesRegression) {
    InvokerShard s;
    auto pump = s.pump();
    rune::replication::AeronPartitionTransport t(s.client, "aeron:ipc", 7505, /*selfId=*/0, 64 * 1024, pump);
    t.addPeer(1, kUnresolvable, 7506);
    int64_t nowNs = 0;
    auto pass = [&] { (void)pump.pump(); (void)t.poll(++nowNs); };
    for (int i = 0; i < 256; ++i) pass();
    EXPECT_EQ(t.unresolvedPeerCount(), 1);
    EXPECT_GE(t.peerPubReAdds(), 2u) << "an errored peer must be re-added, never dropped for good";
    EXPECT_LE(t.peerPubReAdds(), 9u) << "re-adds must back off by pass count";

    t.setPeerChannelForTest(1, "aeron:ipc");
    ASSERT_TRUE(driveUntil(pass, [&] { return t.unresolvedPeerCount() == 0; }))
        << "the internal plane never attached the late-resolving peer";
}

// ---------------------------------------------------------------------------
// Consensus sink: already non-fatal (DR-BUG-4) — its startup resolve must end
// when Aeron answers, not hold node startup for the whole 5 s budget.
// ---------------------------------------------------------------------------
TEST(UnresolvedPeerRegression, ConsensusSinkStartupDoesNotWaitOutTheBudgetRegression) {
    InvokerShard s;
    std::unordered_map<rune::consensus::NodeId, UdpMessageSink::PeerEndpoint> peers{
        {1, {kUnresolvable, 7507}}, {2, {"aeron:ipc", 7507}}};
    const auto t0 = std::chrono::steady_clock::now();
    UdpMessageSink sink(s.client, peers, s.pump());
    EXPECT_LT(since(t0), 2000ms) << "an unresolvable peer held startup for the 5 s budget";
    EXPECT_EQ(sink.unresolvedForTest(), 1);
}
