// UnresolvedPeerRegressionTest.cpp — DR-15 (2026-10-05): a peer whose hostname
// does not resolve at the moment this node starts (the peer container / k8s pod
// is itself restarting, DNS lag) must NEVER stop this node from starting, and
// must be attached as soon as its hostname resolves.
//
// Root cause: the per-worker replication plane registered one publication per
// peer at startup; Aeron rejected the registration ("Unresolved - endpoint=
// rune-2:41256"), the plane's attach() returned false and BrokerNode threw
// "shard 0 replication plane did not resolve — refusing to start".
//
// Fix round 1 (review): no duty thread ever resolves a peer name — the cold
// PeerResolverHub resolves them and posts the result over rings to every
// driver's name cache (the driver answers peer hosts from it ONLY) and to every
// owner of peer publications, which re-adds an unresolved peer ON THAT EVENT
// (never on a pass / send count).  Legacy planes re-add through the control
// thread (PubAddBroker).  The node binds its own peer-facing inbound on a host
// that never needs its own DNS record.
//
// Every test runs on INVOKER drivers + clients pumped by the test thread; the
// hub is driven by runOnceForTest (deterministic) except where the cold thread
// itself is under test.  No test asserts on elapsed wall-clock time.
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/PeerResolverHub.h>
#include <rune/node/rune_peer_name_resolver.h>
#include <rune/node/UdpMessageSink.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerReplicationTransport.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PubAddBroker.h>
#include <rune/aeronutil/testing/FreePort.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cinttypes>
#include <cstdio>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <mutex>
#include <string>

using namespace rune::node;
namespace fs = std::filesystem;
using namespace std::chrono_literals;
using rune::consensus::PeerResolutionSource;

namespace {

const std::string kLateHost = "rune-late.test";   // resolvable only when the test says so

/// Test-controlled name service (the hub's resolve function).  Test-only
/// synchronisation (mutex / condvar) is fine: production never sees it.
struct TestDns {
    std::mutex                           m;
    std::condition_variable              cv;
    std::map<std::string, std::uint32_t> table;
    bool                                 blockArmed = false;   // next lookup blocks until released
    bool                                 blocked    = false;   // a lookup is blocked right now
    bool                                 released   = false;
    std::atomic<int>                     lookups{0};

    void set(const std::string& host, const char* ip) {
        std::lock_guard<std::mutex> g(m);
        in_addr a{};
        ::inet_pton(AF_INET, ip, &a);
        table[host] = a.s_addr;
    }
    PeerResolverHub::ResolveFn fn() {
        return [this](const std::string& host, std::uint32_t& ip) {
            ++lookups;
            std::unique_lock<std::mutex> g(m);
            if (blockArmed) {
                blockArmed = false;
                blocked    = true;
                cv.notify_all();
                cv.wait(g, [this] { return released; });   // a DNS server that does not answer
                blocked = false;
            }
            const auto it = table.find(host);
            if (it == table.end()) return false;
            ip = it->second;
            return true;
        };
    }
};

struct InvokerShard {
    fs::path dir;
    std::unique_ptr<EmbeddedDriver> driver;
    std::shared_ptr<aeron::Aeron> client;
    explicit InvokerShard(PeerResolutionSource res = {}) {
        static std::atomic<int> seq{0};
        dir = fs::temp_directory_path() /
              ("rune_unres_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 64 * 1024;
        c.peerResolution = res;
        driver = std::make_unique<EmbeddedDriver>(c, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(dir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
    }
    ~InvokerShard() { client.reset(); driver.reset(); std::error_code ec; fs::remove_all(dir, ec); }
    rune::aeronutil::AeronPump pump() { return driver->pumpWith(client.get()); }
};

/// Drive `pass` until `done` holds.  Bounded by a pass cap (no time involved).
template <class Pass, class Done>
bool driveUntil(Pass&& pass, Done&& done, int64_t maxPasses = 2'000'000) {
    for (int64_t i = 0; i < maxPasses; ++i) {
        if (done()) return true;
        pass();
    }
    return done();
}

std::string udp(const std::string& host, uint16_t port) {
    return "aeron:udp?endpoint=" + host + ":" + std::to_string(port);
}

} // namespace

// ---------------------------------------------------------------------------
// The per-worker replication plane (the DR-15 crash site).
// ---------------------------------------------------------------------------

// RED on bc153c3: resolved() was false (=> BrokerNode refused to start).
TEST(UnresolvedPeerRegression, PlaneIsUsableWithAnUnresolvablePeerRegression) {
    InvokerShard s;
    WorkerReplicationTransport t(s.client, 0, "aeron:ipc", 7501,
                                 {{1, udp("rune-unresolvable.invalid", 40999)}, {2, std::string("aeron:ipc")}},
                                 7501, s.pump());
    EXPECT_TRUE(t.resolved()) << "an unresolvable PEER must not make the plane (and the node) refuse to start";
    EXPECT_FALSE(t.allPeersResolved());
}

// A genuinely LOCAL failure (own inbound bind) stays a refusal.
TEST(UnresolvedPeerRegression, PlaneWithBadInboundStillRefusesRegression) {
    InvokerShard s;
    WorkerReplicationTransport t(s.client, 0, "aeron:udp?endpoint=not-a-valid-endpoint", 7502,
                                 {{2, std::string("aeron:ipc")}}, 7502, s.pump());
    EXPECT_FALSE(t.resolved());
}

// MEDIUM-3: the SAME hostname becomes resolvable later (no channel swap): the
// plane re-adds exactly once, on the resolution event, through its driver's
// off-thread cache; then both sides handshake and data flows (a late peer).
TEST(UnresolvedPeerRegression, SameHostnameResolvesLaterPlaneAttachesOnTheEventRegression) {
    TestDns dns;
    PeerResolverHub hub({kLateHost}, dns.fn());
    hub.resolveAllNow();
    InvokerShard sa(hub.subscribeDriver()), sb;
    const uint16_t pa = rune::aeronutil::testing::allocFreeUdpPort();
    const uint16_t pb = rune::aeronutil::testing::allocFreeUdpPort();
    constexpr int32_t kStream = 7504;
    auto pumpA = sa.pump();
    auto pumpB = sb.pump();
    WorkerReplicationTransport a(0, udp("127.0.0.1", pa), kStream, WorkerReplicationTransport::DeferredAttach{});
    a.attachResolution(hub.subscribeOwner());
    ASSERT_TRUE(a.attach(sa.client, {{1, udp(kLateHost, pb)}}, kStream, pumpA));
    WorkerReplicationTransport b(sb.client, 0, udp("127.0.0.1", pb), kStream, {{0, udp("127.0.0.1", pa)}},
                                 kStream, pumpB);
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
    // B's hellos reach A, but A cannot answer while B's hostname is unresolvable.
    ASSERT_TRUE(driveUntil(pass, [&] { return a.peerPlaneState(1) == PS::Verified; }));
    for (int i = 0; i < 2000; ++i) pass();
    EXPECT_EQ(b.peerPlaneState(0), PS::Unverified);
    EXPECT_FALSE(a.allPeersResolved());
    EXPECT_EQ(a.peerReAddsForTest(), 0u) << "no event, no re-add: never on a pass count";

    // The hostname resolves (the cold resolver's next probe).
    dns.set(kLateHost, "127.0.0.1");
    hub.makeAllDueForTest();
    ASSERT_EQ(hub.runOnceForTest(1), 1);
    ASSERT_TRUE(driveUntil(pass, [&] { return a.allPeersResolved() && b.peerPlaneState(0) == PS::Verified; }))
        << "the late-resolving peer was never attached / verified";
    EXPECT_EQ(a.peerReAddsForTest(), 1u) << "exactly one re-add, on the resolution event";

    rune::replication::FetchRequestMsg req{};
    req.leaderEpoch = 1; req.partition = 0; req.followerId = 0; req.fetchOffset = 0; req.maxBytes = 1024;
    ASSERT_TRUE(driveUntil([&] { (void)a.sendFetchRequestTo(1, req); pass(); }, [&] { return fetchesAtB > 0; }))
        << "data frames never reached the late-resolved peer";
}

// CRITICAL-1: a resolver that blocks (DNS server not answering) runs on the cold
// hub thread — the worker pass never waits on it.
TEST(UnresolvedPeerRegression, BlockingResolverNeverStallsTheWorkerPassRegression) {
    TestDns dns;
    PeerResolverHub hub({kLateHost}, dns.fn());
    hub.resolveAllNow();                                    // unresolvable, answered at once
    InvokerShard s(hub.subscribeDriver());
    auto pump = s.pump();
    const uint16_t pb = rune::aeronutil::testing::allocFreeUdpPort();
    WorkerReplicationTransport t(0, "aeron:ipc", 7505, WorkerReplicationTransport::DeferredAttach{});
    t.attachResolution(hub.subscribeOwner());
    ASSERT_TRUE(t.attach(s.client, {{1, udp(kLateHost, pb)}}, 7505, pump));
    Worker w{0, 1 << 16, 1 << 16, 16, 16};
    w.setReplicationTransport(&t);

    {
        std::lock_guard<std::mutex> g(dns.m);
        dns.blockArmed = true;
    }
    hub.start();
    {
        std::unique_lock<std::mutex> g(dns.m);
        ASSERT_TRUE(dns.cv.wait_for(g, 60s, [&] { return dns.blocked; })) << "hub never probed (hang guard)";
    }
    // The resolver is stuck inside a lookup.  Every worker pass still completes.
    for (int64_t i = 0; i < 20'000; ++i) { (void)pump.pump(); (void)w.doWork(i); }
    {
        std::lock_guard<std::mutex> g(dns.m);
        EXPECT_TRUE(dns.blocked) << "the passes above ran while the lookup was blocked";
    }
    // The name now resolves; release the stuck lookup: the peer attaches on the event.
    dns.set(kLateHost, "127.0.0.1");
    {
        std::lock_guard<std::mutex> g(dns.m);
        dns.released = true;
    }
    dns.cv.notify_all();
    int64_t now = 20'000;
    ASSERT_TRUE(driveUntil([&] { (void)pump.pump(); (void)w.doWork(++now); }, [&] { return t.allPeersResolved(); },
                           50'000'000))
        << "the peer never attached after its host resolved";
    hub.stop();
    w.setReplicationTransport(static_cast<WorkerReplicationTransport*>(nullptr));
}

// MEDIUM-2: a LEGACY plane (node client shared with the control thread) never
// makes an admin call after startup — its re-add is made by the control thread.
TEST(UnresolvedPeerRegression, LegacyPlaneReAddIsMadeOnTheControlThreadRegression) {
    TestDns dns;
    PeerResolverHub hub({kLateHost}, dns.fn());
    hub.resolveAllNow();
    InvokerShard s(hub.subscribeDriver());
    auto pump = s.pump();
    const uint16_t pb = rune::aeronutil::testing::allocFreeUdpPort();
    const std::vector<std::pair<rune::replication::NodeId, std::string>> peers{{1, udp(kLateHost, pb)}};
    rune::replication::PubAddBroker broker(peers, 7506);
    AeronPubRegistry reg;
    reg.attachResolution(hub.subscribeOwner());
    reg.setAddBroker(&broker);
    reg.attach(s.client, peers, 7506, pump);
    ASSERT_TRUE(reg.configured());
    ASSERT_FALSE(reg.hasPeer(1));

    dns.set(kLateHost, "127.0.0.1");
    hub.makeAllDueForTest();
    ASSERT_EQ(hub.runOnceForTest(1), 1);
    for (int i = 0; i < 2000; ++i) { (void)pump.pump(); (void)reg.drivePeerResolve(); }
    EXPECT_EQ(reg.peerReAdds(), 1u) << "the owner requested exactly one re-add on the event";
    EXPECT_FALSE(reg.hasPeer(1)) << "the owner must not add on the shared client itself";

    // The control thread's duty (AeronPartitionTransport::poll → broker.doWork).
    ASSERT_TRUE(driveUntil([&] { (void)broker.doWork(*s.client); (void)pump.pump(); (void)reg.drivePeerResolve(); },
                           [&] { return reg.hasPeer(1); }));
    EXPECT_TRUE(reg.allResolved());
}

// ---------------------------------------------------------------------------
// The internal-partition / txn-marker plane (AeronPartitionTransport).
// ---------------------------------------------------------------------------
TEST(UnresolvedPeerRegression, PartitionTransportReAddsOnTheResolutionEventRegression) {
    TestDns dns;
    PeerResolverHub hub({kLateHost}, dns.fn());
    hub.resolveAllNow();
    InvokerShard s(hub.subscribeDriver());
    auto pump = s.pump();
    rune::replication::AeronPartitionTransport t(s.client, "aeron:ipc", 7507, /*selfId=*/0, 64 * 1024, pump);
    t.attachResolution(hub.subscribeOwner());
    t.addPeer(1, udp(kLateHost, rune::aeronutil::testing::allocFreeUdpPort()), 7508);
    int64_t nowNs = 0;
    auto pass = [&] { (void)pump.pump(); (void)t.poll(++nowNs); };
    for (int i = 0; i < 2000; ++i) pass();
    EXPECT_EQ(t.unresolvedPeerCount(), 1);
    EXPECT_EQ(t.peerPubReAdds(), 0u) << "no event, no re-add";

    dns.set(kLateHost, "127.0.0.1");
    hub.makeAllDueForTest();
    ASSERT_EQ(hub.runOnceForTest(1), 1);
    ASSERT_TRUE(driveUntil(pass, [&] { return t.unresolvedPeerCount() == 0; }))
        << "the internal plane never attached the late-resolving peer";
    EXPECT_EQ(t.peerPubReAdds(), 1u);
}

// ---------------------------------------------------------------------------
// Consensus sink (MAJOR-1): reconnects on the resolution event — promptly, not
// after a send-count backoff.
// ---------------------------------------------------------------------------
TEST(UnresolvedPeerRegression, ConsensusSinkReconnectsOnTheResolutionEventRegression) {
    TestDns dns;
    PeerResolverHub hub({kLateHost}, dns.fn());
    hub.resolveAllNow();
    InvokerShard s(hub.subscribeDriver());
    auto pump = s.pump();
    const uint16_t port = rune::aeronutil::testing::allocFreeUdpPort();
    constexpr int32_t kStream = 7509;
    const int64_t subReg = s.client->addSubscription(udp("127.0.0.1", port), kStream);
    std::shared_ptr<aeron::Subscription> sub;
    ASSERT_TRUE(driveUntil([&] { (void)pump.pump(); }, [&] { return (sub = s.client->findSubscription(subReg)) != nullptr; }));

    std::unordered_map<rune::consensus::NodeId, UdpMessageSink::PeerEndpoint> peers{{1, {udp(kLateHost, port), kStream}}};
    UdpMessageSink sink(s.client, peers, pump, hub.subscribeOwner());
    int received = 0;
    const uint8_t msg[16] = {1, 2, 3, 4};
    auto pass = [&] {
        sink.send(1, msg, sizeof msg);
        (void)pump.pump();
        (void)sub->poll([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t,
                            aeron::Header&) { ++received; }, 16);
    };
    for (int i = 0; i < 2000; ++i) pass();
    EXPECT_EQ(received, 0);
    EXPECT_EQ(sink.reAddsForTest(), 0u) << "no event, no re-add (never per send)";

    dns.set(kLateHost, "127.0.0.1");
    hub.makeAllDueForTest();
    ASSERT_EQ(hub.runOnceForTest(1), 1);
    ASSERT_TRUE(driveUntil(pass, [&] { return received > 0; })) << "consensus never reached the late peer";
    EXPECT_EQ(sink.reAddsForTest(), 1u) << "exactly one re-add, on the resolution event";
}

// LOW-3 (no wall clock): the sink's startup resolve ends when Aeron answers —
// counted in pump passes, far below what a 5 s budget spin would take.
TEST(UnresolvedPeerRegression, ConsensusSinkStartupEndsWhenAeronAnswersRegression) {
    InvokerShard s;
    struct Counting {
        rune::aeronutil::AeronPump inner;
        int64_t calls = 0;
        static int fn(void* self) noexcept {
            auto* c = static_cast<Counting*>(self);
            ++c->calls;
            return c->inner.pump();
        }
    } counting{s.pump()};
    std::unordered_map<rune::consensus::NodeId, UdpMessageSink::PeerEndpoint> peers{
        {1, {udp("rune-unresolvable.invalid", 40999), 7510}}, {2, {"aeron:ipc", 7510}}};
    UdpMessageSink sink(s.client, peers, rune::aeronutil::AeronPump(&Counting::fn, &counting, nullptr));
    EXPECT_LT(counting.calls, 10'000) << "the startup resolve spun on an unresolvable peer";
    EXPECT_EQ(sink.unresolvedForTest(), 1);
}

// ---------------------------------------------------------------------------
// The hub: first resolution, every later confirmation (fix round 2, MEDIUM-A)
// and an IP change reach every consumer.
// ---------------------------------------------------------------------------
TEST(UnresolvedPeerRegression, HubPostsResolutionsConfirmationsAndIpChangesRegression) {
    TestDns dns;
    dns.set(kLateHost, "10.0.0.1");
    PeerResolverHub hub({kLateHost, "127.0.0.1", kLateHost}, dns.fn());
    ASSERT_EQ(hub.table().size(), 1u) << "numeric hosts are never managed; duplicates collapse";
    hub.resolveAllNow();
    rune::consensus::PeerResolutionInbox inbox(hub.subscribeOwner());   // prefilled
    EXPECT_EQ(inbox.poll(), 1);
    std::uint32_t ip = 0;
    ASSERT_TRUE(inbox.addressOf(0, ip));
    hub.makeAllDueForTest();
    (void)hub.runOnceForTest(1);
    EXPECT_EQ(inbox.poll(), 1) << "a re-check confirming the same address is an event too";
    dns.set(kLateHost, "10.0.0.2");                         // the pod restarted on a new IP
    hub.makeAllDueForTest();
    (void)hub.runOnceForTest(2);
    EXPECT_EQ(inbox.poll(), 1);
    ASSERT_TRUE(inbox.addressOf(0, ip));
    in_addr want{};
    ::inet_pton(AF_INET, "10.0.0.2", &want);
    EXPECT_EQ(ip, want.s_addr);
    EXPECT_EQ(inbox.generation(0), 3u);
}

// ---------------------------------------------------------------------------
// Fix round 2 (MEDIUM-A): an add that fails for a NON-DNS reason while the
// owner already has the host cached (here: the driver cannot create the
// endpoint yet — modelled by a driver cache that lags the owner's) is re-added
// on the next resolver confirmation; without it the peer stays cut for good.
// ---------------------------------------------------------------------------
namespace {
struct LaggingDriverCache {
    TestDns driverDns, ownerDns;
    PeerResolverHub driverHub{{kLateHost}, driverDns.fn()};   // feeds the driver: fails the first add
    PeerResolverHub ownerHub{{kLateHost}, ownerDns.fn()};     // feeds the owner: host already cached
    LaggingDriverCache() {
        ownerDns.set(kLateHost, "127.0.0.1");
        driverHub.resolveAllNow();
        ownerHub.resolveAllNow();
    }
    void driverCanAddNow() {
        driverDns.set(kLateHost, "127.0.0.1");
        driverHub.makeAllDueForTest();
        (void)driverHub.runOnceForTest(1);
    }
    void ownerConfirmation() {
        ownerHub.makeAllDueForTest();
        (void)ownerHub.runOnceForTest(2);
    }
};
}

TEST(UnresolvedPeerRegression, ConsensusSinkReAddsAfterANonDnsAddFailureRegression) {
    LaggingDriverCache env;
    InvokerShard s(env.driverHub.subscribeDriver());
    auto pump = s.pump();
    const uint16_t port = rune::aeronutil::testing::allocFreeUdpPort();
    constexpr int32_t kStream = 7511;
    const int64_t subReg = s.client->addSubscription(udp("127.0.0.1", port), kStream);
    std::shared_ptr<aeron::Subscription> sub;
    ASSERT_TRUE(driveUntil([&] { (void)pump.pump(); }, [&] { return (sub = s.client->findSubscription(subReg)) != nullptr; }));
    std::unordered_map<rune::consensus::NodeId, UdpMessageSink::PeerEndpoint> peers{{1, {udp(kLateHost, port), kStream}}};
    UdpMessageSink sink(s.client, peers, pump, env.ownerHub.subscribeOwner());
    int received = 0;
    const uint8_t msg[16] = {9};
    auto pass = [&] {
        sink.send(1, msg, sizeof msg);
        (void)pump.pump();
        (void)sub->poll([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t,
                            aeron::Header&) { ++received; }, 16);
    };
    for (int i = 0; i < 2000; ++i) pass();
    ASSERT_EQ(received, 0) << "the add must have failed";
    env.driverCanAddNow();                                   // the cause is gone — no owner event yet
    for (int i = 0; i < 2000; ++i) pass();
    EXPECT_EQ(received, 0);
    env.ownerConfirmation();                                 // the resolver's next re-check
    ASSERT_TRUE(driveUntil(pass, [&] { return received > 0; }))
        << "consensus stayed cut after a non-DNS add failure on a cached host";
}

TEST(UnresolvedPeerRegression, PlaneReAddsAfterANonDnsAddFailureRegression) {
    LaggingDriverCache env;
    InvokerShard s(env.driverHub.subscribeDriver());
    auto pump = s.pump();
    WorkerReplicationTransport t(0, "aeron:ipc", 7512, WorkerReplicationTransport::DeferredAttach{});
    t.attachResolution(env.ownerHub.subscribeOwner());
    ASSERT_TRUE(t.attach(s.client, {{1, udp(kLateHost, rune::aeronutil::testing::allocFreeUdpPort())}}, 7512, pump));
    auto pass = [&] { (void)pump.pump(); (void)t.drivePeerResolve(); };
    for (int i = 0; i < 2000; ++i) pass();
    ASSERT_FALSE(t.allPeersResolved());
    env.driverCanAddNow();
    for (int i = 0; i < 2000; ++i) pass();
    EXPECT_FALSE(t.allPeersResolved());
    env.ownerConfirmation();
    ASSERT_TRUE(driveUntil(pass, [&] { return t.allPeersResolved(); }))
        << "the plane stayed cut after a non-DNS add failure on a cached host";
}

TEST(UnresolvedPeerRegression, PartitionTransportReAddsAfterANonDnsAddFailureRegression) {
    LaggingDriverCache env;
    InvokerShard s(env.driverHub.subscribeDriver());
    auto pump = s.pump();
    rune::replication::AeronPartitionTransport t(s.client, "aeron:ipc", 7513, /*selfId=*/0, 64 * 1024, pump);
    t.attachResolution(env.ownerHub.subscribeOwner());
    t.addPeer(1, udp(kLateHost, rune::aeronutil::testing::allocFreeUdpPort()), 7514);
    int64_t nowNs = 0;
    auto pass = [&] { (void)pump.pump(); (void)t.poll(++nowNs); };
    for (int i = 0; i < 2000; ++i) pass();
    ASSERT_EQ(t.unresolvedPeerCount(), 1);
    env.driverCanAddNow();
    for (int i = 0; i < 2000; ++i) pass();
    EXPECT_EQ(t.unresolvedPeerCount(), 1);
    env.ownerConfirmation();
    ASSERT_TRUE(driveUntil(pass, [&] { return t.unresolvedPeerCount() == 0; }))
        << "the internal plane stayed cut after a non-DNS add failure on a cached host";
}

// ---------------------------------------------------------------------------
// MEDIUM-1: the node's own peer-facing binds never depend on its own DNS record.
// ---------------------------------------------------------------------------
TEST(UnresolvedPeerRegression, PeerBindHostNeverDependsOnOwnDnsRecordRegression) {
    NodeConfig c;
    c.nodeId = 0;
    PeerConfig self; self.id = 0; self.host = "rune-0.rune-headless"; self.consensusPort = 40100;
    self.replicationPort = 40300;
    c.peers.push_back(self);
    EXPECT_EQ(c.peerBindHost(), "0.0.0.0") << "a hostname is advertised, never bound";
    EXPECT_EQ(c.selfConsensusBindChannel(), "aeron:udp?endpoint=0.0.0.0:40100");
    EXPECT_EQ(c.selfReplicationBindChannel(), "aeron:udp?endpoint=0.0.0.0:40300");
    c.peers[0].host = "10.1.2.3";
    EXPECT_EQ(c.peerBindHost(), "10.1.2.3") << "a numeric host binds as configured (no lookup)";
    c.peerBindHostOverride = "192.168.0.7";
    EXPECT_EQ(c.peerBindHost(), "192.168.0.7") << "RUNE_PEER_BIND_HOST wins";
}

// DR-15 Docker (2026-10-05): every broker exited "rune peer resolver: missing init
// args" (aeron_driver_init EINVAL) — rune_peer_name_resolver_install formatted the
// init args into a STACK buffer and Aeron keeps the raw pointer
// (aeron_driver_context_set_name_resolver_init_args stores, never copies): by the
// time the driver initialises the resolver the frame is gone.  Green on the Mac by
// luck (the stack survived).  The args must live as long as the lookup.
namespace {
[[gnu::noinline]] void installFromAFrameThatDies(aeron_driver_context_t* ctx, rune_peer_name_lookup_t* lk) {
    ASSERT_EQ(rune_peer_name_resolver_install(ctx, lk), 0);
}
[[gnu::noinline]] void clobberTheStack() {
    volatile char junk[8192];
    for (std::size_t i = 0; i < sizeof(junk); ++i) junk[i] = 'x';
}
int noLookup(void*, const char*, uint32_t*) { return -1; }
} // namespace

TEST(UnresolvedPeerRegression, ResolverInitArgsOutliveTheInstallCallRegression) {
    aeron_driver_context_t* ctx = nullptr;
    ASSERT_EQ(aeron_driver_context_init(&ctx), 0);
    rune_peer_name_lookup_t lk{};
    lk.fn    = &noLookup;
    lk.state = nullptr;
    installFromAFrameThatDies(ctx, &lk);
    clobberTheStack();
    const char* args = aeron_driver_context_get_name_resolver_init_args(ctx);
    ASSERT_NE(args, nullptr);
    char expected[64];
    std::snprintf(expected, sizeof(expected), "%s%" PRIxPTR, RUNE_PEER_RESOLVER_ARGS_PREFIX,
                  reinterpret_cast<uintptr_t>(&lk));
    EXPECT_STREQ(args, expected) << "the resolver init args dangle (stack buffer)";
    aeron_driver_context_close(ctx);
}
