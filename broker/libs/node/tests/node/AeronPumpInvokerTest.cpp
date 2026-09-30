// AeronPumpInvokerTest.cpp — with NO driver thread and NO client conductor thread,
// every startup resolve completes by pumping its own driver + conductor (spec §3.6).
#include <rune/node/EmbeddedDriver.h>
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
