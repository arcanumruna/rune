// AeronPumpInvokerTest.cpp — with NO driver thread and NO client conductor thread,
// every startup resolve completes by pumping its own driver + conductor (spec §3.6).
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/IngressRing.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/node/UdpMessageSink.h>
#include <rune/node/WorkerEgress.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerReplicationTransport.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>

using namespace rune::node;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {
uint16_t freeUdpPort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
    socklen_t len = sizeof(a);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}

struct InvokerShard {
    fs::path dir;
    std::unique_ptr<EmbeddedDriver> driver;
    std::shared_ptr<aeron::Aeron> client;
    InvokerShard() {
        static std::atomic<int> seq{0};
        dir = fs::temp_directory_path() / ("rune_pump_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 64 * 1024;
        driver = std::make_unique<EmbeddedDriver>(c, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(dir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
    }
    ~InvokerShard() { client.reset(); driver.reset(); std::error_code ec; fs::remove_all(dir, ec); }
    rune::aeronutil::AeronPump pump() { return driver->pumpWith(client.get()); }
};
}

TEST(AeronPumpInvoker, WorkerIngressResolvesByPumpingOwnDriver) {
    InvokerShard s;
    WorkerIngress ing(s.client, "aeron:ipc", 7001, s.pump());
    EXPECT_TRUE(ing.resolved());
}

TEST(AeronPumpInvokerRegression, WithoutPumpInvokerResolveCannotProgress) {
    InvokerShard s;
    WorkerIngress ing(s.client, "aeron:ipc", 7002, rune::aeronutil::AeronPump{}, 50ms);
    EXPECT_FALSE(ing.resolved()) << "nothing drives an invoker driver unless the resolver pumps it";
}

TEST(AeronPumpInvoker, WorkerEgressResolvesByPumpingOwnDriver) {
    InvokerShard s;
    WorkerEgress eg(s.client, "127.0.0.1:" + std::to_string(freeUdpPort()), 7003, s.pump());
    EXPECT_TRUE(eg.resolved());
}

TEST(AeronPumpInvoker, ReplicationTransportResolvesSubAndPeerPubs) {
    InvokerShard s;
    WorkerReplicationTransport t(s.client, 0, "aeron:ipc", 7004,
                                 {{2, std::string("aeron:ipc")}}, 7004, s.pump());
    EXPECT_TRUE(t.resolved());
}

TEST(AeronPumpInvoker, UdpMessageSinkResolvesPeers) {
    InvokerShard s;
    std::unordered_map<rune::consensus::NodeId, UdpMessageSink::PeerEndpoint> peers{
        {2, {"aeron:ipc", 7005}}};
    UdpMessageSink sink(s.client, peers, s.pump());
    EXPECT_EQ(sink.unresolvedForTest(), 0) << "all peer publications resolved with no background thread";
}

// ---------------------------------------------------------------------------
// Fix round 1 regressions: misuse of a pump / a driver registration error must
// FAIL FAST and loudly — never a silent busy-spin for the whole budget, never
// std::terminate out of a noexcept ctor.  Budgets are 5 s (production default);
// the hard cap asserts the resolve returned long before it (no time passing).
// ---------------------------------------------------------------------------
namespace {
constexpr auto kFailFastCap = 1000ms;   // << 5 s budget: proves no budget-long spin

std::chrono::milliseconds since(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
}
}

TEST(AeronPumpInvokerRegression, PumpWithOnThreadedDriverIsDefaultSleepingPump) {
    const fs::path dir = fs::temp_directory_path() / ("rune_pump_thr_" + std::to_string(::getpid()));
    EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 64 * 1024;
    auto driver = std::make_unique<EmbeddedDriver>(c, DriverMode::Threaded);
    aeron::Context ctx; ctx.aeronDir(dir.string());
    auto client = aeron::Aeron::connect(ctx);

    auto pump = driver->pumpWith(client.get());
    EXPECT_FALSE(pump.invoker()) << "a threaded driver must hand out the default (sleeping) pump";
    WorkerIngress ing(client, "aeron:ipc", 7101, pump);
    EXPECT_TRUE(ing.resolved());
    EXPECT_EQ(driver->foreignPumpAttempts(), 0u) << "the resolver must never try to pump a threaded driver";

    client.reset(); driver.reset();
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(AeronPumpInvokerRegression, ForeignThreadPumpIsRefusedAndResolveFailsFast) {
    InvokerShard s;
    auto pump = s.pump();                       // made on the owner thread, used elsewhere
    bool resolved = true;
    std::chrono::milliseconds took{};
    std::thread foreign([&] {
        const auto t0 = std::chrono::steady_clock::now();
        WorkerIngress ing(s.client, "aeron:ipc", 7102, pump);   // default 5 s budget
        took = since(t0);
        resolved = ing.resolved();
    });
    foreign.join();
    EXPECT_FALSE(resolved);
    EXPECT_LT(took, kFailFastCap) << "a refused pump must fail fast, not spin for the budget";
    EXPECT_EQ(s.driver->foreignPumpAttempts(), 1u) << "exactly one refused pump, then stop";
}

TEST(AeronPumpInvokerRegression, InvalidChannelIsAResolveFailureNotTerminate) {
    InvokerShard s;
    const auto t0 = std::chrono::steady_clock::now();
    WorkerIngress ing(s.client, "aeron:udp?endpoint=not-a-valid-endpoint", 7103, s.pump());
    EXPECT_FALSE(ing.resolved());
    EXPECT_LT(since(t0), kFailFastCap) << "a driver registration error must end the resolve at once";
}

TEST(AeronPumpInvokerRegression, InvalidEgressChannelIsAResolveFailureNotTerminate) {
    InvokerShard s;
    const auto t0 = std::chrono::steady_clock::now();
    WorkerEgress eg(s.client, "not-a-valid-endpoint", 7104, s.pump());
    EXPECT_FALSE(eg.resolved());
    EXPECT_LT(since(t0), kFailFastCap);
}

TEST(AeronPumpInvokerRegression, InvalidReplicationInboundChannelIsAResolveFailureNotTerminate) {
    InvokerShard s;
    const auto t0 = std::chrono::steady_clock::now();
    WorkerReplicationTransport t(s.client, 0, "aeron:udp?endpoint=not-a-valid-endpoint", 7105,
                                 {{2, std::string("aeron:ipc")}}, 7105, s.pump());
    EXPECT_FALSE(t.resolved());
    EXPECT_LT(since(t0), kFailFastCap);
}

TEST(AeronPumpInvokerRegression, ErroredPeerIsDroppedOthersResolveRegistryNotResolved) {
    InvokerShard s;
    const auto t0 = std::chrono::steady_clock::now();
    AeronPubRegistry reg(s.client,
                         {{2, std::string("aeron:ipc")},
                          {3, std::string("aeron:udp?endpoint=not-a-valid-endpoint")}},
                         7106, s.pump());
    EXPECT_TRUE(reg.hasPeer(2)) << "a failing peer must not stop the others resolving";
    EXPECT_FALSE(reg.hasPeer(3));
    EXPECT_FALSE(reg.allResolved()) << "one errored peer => not allResolved";
    EXPECT_LT(since(t0), kFailFastCap);
}

// ---------------------------------------------------------------------------
// Task 11 (fused runtime): the runtime client's startup resolves — consensus
// ingress, replication ingress, gateway client ingress — complete by pumping
// the runtime's own invoker driver + conductor (no driver/conductor thread).
// ---------------------------------------------------------------------------
TEST(AeronPumpInvokerRegression, ConsensusIngressResolvesOnInvokerDriver) {
    InvokerShard s;
    rune::consensus::IngressRing ring(64 * 1024);
    rune::consensus::AeronIngress ing(s.client, "aeron:ipc", 7201, ring, s.pump());
    SUCCEED() << "constructor would throw 'subscription not resolved' without the pump";
}

TEST(AeronPumpInvokerRegression, PartitionTransportResolvesOnInvokerDriver) {
    InvokerShard s;
    rune::replication::AeronPartitionTransport t(s.client, "aeron:ipc", 7202, /*selfId=*/0,
                                                 64 * 1024, s.pump());
    SUCCEED() << "constructor would throw 'ingress subscription not resolved' without the pump";
}

TEST(AeronPumpInvokerRegression, ClientGatewayResolvesOnInvokerDriver) {
    InvokerShard s;
    rune::gateway::ClientGateway gw(s.client, "aeron:ipc", 7203, /*thisNodeId=*/0,
                                    rune::gateway::ClientGateway::kDefaultSessionTimeoutNs, nullptr,
                                    rune::gateway::ClientGateway::kDefaultConnectResolveTimeoutNs, s.pump());
    SUCCEED() << "constructor would throw 'ingress subscription not resolved' without the pump";
}

// ---------------------------------------------------------------------------
// Task 11 fix 2 (B2): the production transports of internal partitions are
// dispatched concretely by PartitionReplica (TransportKind::Aeron / ::Null),
// never through the PartitionTransport vtable (Generic = test fakes only).
// ---------------------------------------------------------------------------
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

namespace {
rune::replication::PartitionConfig internalCfg() {
    rune::replication::PartitionConfig c;
    c.topicId = -2; c.partitionId = 0; c.selfId = 1;
    c.replicaLagMaxMs = 10'000; c.replicaLagMaxOffset = 10'000;
    return c;
}
}

TEST(PartitionTransportKindRegression, AeronAndNullTransportsAreConcreteKinds) {
    InvokerShard s;
    rune::replication::AeronPartitionTransport aeronT(s.client, "aeron:ipc", 7301, /*selfId=*/1,
                                                      64 * 1024, s.pump());
    rune::storage::InMemoryLog log1(1 << 20, 1024), log2(1 << 20, 1024);
    rune::replication::PartitionReplica viaAeron(internalCfg(), rune::replication::Role::Leader, 1,
                                                 {1, 2}, {1, 2}, &log1, &aeronT);
    EXPECT_EQ(viaAeron.transportKindForTest(), rune::replication::TransportKind::Aeron);
    rune::replication::NullPartitionTransport nullT;
    rune::replication::PartitionReplica viaNull(internalCfg(), rune::replication::Role::Leader, 1,
                                                {1}, {1}, &log2, &nullT);
    EXPECT_EQ(viaNull.transportKindForTest(), rune::replication::TransportKind::Null);
}
