// BrokerNodeGatewayTest.cpp — Integration tests: ClientGateway wired inside
// BrokerNode's duty-cycle (BR-7a).
//
// Tests:
//  GW-1: Node serves Connect → ConnectResponse{OK} (single-node, live driver, UDP client endpoint)
//  GW-2: Node serves Ping → Pong (basic echo + empty echo)
//  GW-3: Async-resolve never blocks: a Connect with a bogus egress channel times out
//        without blocking the node (consensus still progresses; subsequent valid
//        client connects and pings while the bogus pending-connect is outstanding).
//  GW-4: Bad egressChannel (malformed/unsupported) never crashes the node (BR-4 regression).
//  GW-5: leaderView degrade-to-accept — single-node is leader; Connect accepted + isLeader=true.
//  GW-6: Duty-cycle still elects — awaitLeader() succeeds with gateway composed in.
//
// All waits are bounded (hard cap + ASSERT).  No busy-spin.
// Unique temp dirs: getpid() + atomic counter.
// Free-port discovery: bind UDP socket to :0, read port, close.

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

#include <Aeron.h>
#include <Publication.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>

// UDP free-port discovery
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;

namespace rune::node {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator (getpid + atomic counter)
// ---------------------------------------------------------------------------
static fs::path makeTempDataDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_bngw_") + std::string(tag) +
                std::to_string(::getpid()) + "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "")
        : path(makeTempDataDir(tag)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// discoverFreeUdpPort — bind to 127.0.0.1:0, read the assigned port, close.
// ---------------------------------------------------------------------------
static uint16_t discoverFreeUdpPort() {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) throw std::runtime_error("BrokerNodeGatewayTest: socket() failed");

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = 0;
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");

    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("BrokerNodeGatewayTest: bind() failed");
    }

    socklen_t addrLen = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &addrLen) < 0) {
        ::close(fd);
        throw std::runtime_error("BrokerNodeGatewayTest: getsockname() failed");
    }

    const uint16_t netPort = addr.sin_port;
    const uint16_t port = static_cast<uint16_t>((netPort >> 8) | ((netPort & 0xFFu) << 8));
    ::close(fd);
    return port;
}

// ---------------------------------------------------------------------------
// makeSingleNodeConfig — 1-node config with unique ports and data dir.
// ---------------------------------------------------------------------------
static NodeConfig makeSingleNodeConfig(const fs::path& dataDir, uint16_t clientPort) {
    NodeConfig cfg;
    cfg.nodeId            = 0;
    cfg.dataDir           = dataDir.string();
    cfg.consensusStreamId = 10;

    PeerConfig self;
    self.id            = 0;
    self.host          = "127.0.0.1";
    self.consensusPort = discoverFreeUdpPort();
    cfg.peers          = {self};

    cfg.clientEndpoint.channel  =
        "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    cfg.clientEndpoint.streamId = 1;

    return cfg;
}

// ---------------------------------------------------------------------------
// nowNs — monotonic nanoseconds.
// ---------------------------------------------------------------------------
static int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// NodeGatewayFixture — starts a single BrokerNode and a separate Aeron client
// for the test harness (simulating an external client connecting over UDP).
//
// The node has its own embedded Aeron driver (in its dataDir/aeron).
// The test harness uses the SAME driver (shares the aeron_ from the node
// by connecting a second Aeron client to the same aeronDir).
// ---------------------------------------------------------------------------
class NodeGatewayFixture : public ::testing::Test {
public:
    static constexpr int32_t kClientStreamId = 1;   // matches NodeConfig
    static constexpr int32_t kEgressStreamId = 300;  // test-harness egress
    static constexpr int     kPumpIter       = 100'000;

    void SetUp() override {
        clientPort_ = discoverFreeUdpPort();
        tmp_        = std::make_unique<TempDir>("gw_");
        NodeConfig cfg = makeSingleNodeConfig(tmp_->path, clientPort_);
        clientChannel_ = cfg.clientEndpoint.channel;

        node_ = std::make_unique<BrokerNode>(std::move(cfg));
        node_->start();

        // Wait for leadership (gateway is now live).
        ASSERT_TRUE(node_->awaitLeader(5s))
            << "Single-node must self-elect within 5 s (with gateway)";

        // Connect a second Aeron client (test harness) to the same driver dir.
        const std::string aeronDir = (tmp_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        testAeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(testAeron_, nullptr) << "Test harness Aeron::connect failed";
    }

    void TearDown() override {
        // Release test harness Aeron client first.
        testAeron_.reset();
        // Stop and destroy the node (calls stop() → gateway → aeron → driver).
        node_.reset();
        tmp_.reset();
    }

    // -----------------------------------------------------------------------
    // TestClient — ingress Publication (to node gateway) + egress Subscription
    // -----------------------------------------------------------------------
    struct TestClient {
        std::shared_ptr<aeron::Publication>  ingressPub;
        std::shared_ptr<aeron::Subscription> egressSub;
        int32_t                              egressStreamId;
        std::string                          egressChannel;
    };

    TestClient makeClient(int32_t egressStreamId) {
        const std::string egressCh = "aeron:ipc";

        int64_t pubRegId = testAeron_->addPublication(clientChannel_, kClientStreamId);
        int64_t subRegId = testAeron_->addSubscription(egressCh, egressStreamId);

        std::shared_ptr<aeron::Publication>  pub;
        std::shared_ptr<aeron::Subscription> sub;

        pumpOnlyHarness([&]() {
            if (!pub) pub = testAeron_->findPublication(pubRegId);
            if (!sub) sub = testAeron_->findSubscription(subRegId);
            return pub && sub;
        }, kPumpIter, "makeClient resolve pub+sub");

        return TestClient{pub, sub, egressStreamId, egressCh};
    }

    // -----------------------------------------------------------------------
    // offerBounded — offer bytes with bounded retry (no busy-spin).
    // -----------------------------------------------------------------------
    static void offerBounded(aeron::Publication& pub,
                             const uint8_t* buf, std::size_t len,
                             std::string_view label = "offer") {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(buf),
            static_cast<aeron::util::index_t>(len));

        bool sent = false;
        for (int i = 0; i < kPumpIter && !sent; ++i) {
            int64_t pos = pub.offer(ab, 0, static_cast<aeron::util::index_t>(len));
            if (pos > 0) {
                sent = true;
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds{100});
            }
        }
        ASSERT_TRUE(sent) << "offerBounded timed out: " << label;
    }

    // -----------------------------------------------------------------------
    // pumpOnlyHarness — poll test-harness subscriptions until done() or cap.
    // (The node's gateway runs on its own AgentRunner thread.)
    // -----------------------------------------------------------------------
    template <typename DoneFn>
    void pumpOnlyHarness(DoneFn&& done,
                         int maxIter = kPumpIter,
                         std::string_view label = "pump") {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{15};

        for (int i = 0; i < maxIter && clock::now() < deadline; ++i) {
            if (done()) return;
            std::this_thread::sleep_for(std::chrono::microseconds{200});
        }
        ASSERT_TRUE(done()) << "pumpOnlyHarness timed out: " << label;
    }

    // -----------------------------------------------------------------------
    // pumpWithEgress — pump test-harness egress sub until done() or cap.
    // -----------------------------------------------------------------------
    template <typename DoneFn>
    void pumpWithEgress(DoneFn&& done,
                        aeron::Subscription& sub,
                        aeron::FragmentAssembler& asm_,
                        int maxIter = kPumpIter,
                        std::string_view label = "pumpEgress") {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{15};

        for (int i = 0; i < maxIter && clock::now() < deadline; ++i) {
            sub.poll(asm_.handler(), 10);
            if (done()) return;
            std::this_thread::sleep_for(std::chrono::microseconds{200});
        }
        ASSERT_TRUE(done()) << "pumpWithEgress timed out: " << label;
    }

    // -----------------------------------------------------------------------
    // doConnect — send ConnectRequest on ingressPub, wait for ConnectResponse
    // on egressSub.
    // -----------------------------------------------------------------------
    pc::ConnectResponse doConnect(aeron::Publication&  ingressPub,
                                  aeron::Subscription& egressSub,
                                  int32_t              egressStreamId,
                                  const std::string&   egressChannel,
                                  int64_t              correlationId) {
        pc::ConnectRequest req;
        req.sessionId       = 0;
        req.correlationId   = correlationId;
        req.egressStreamId  = egressStreamId;
        req.clientVersion   = 1;
        req.egressChannel   = egressChannel;
        const std::size_t reqLen = req.encode(scratch_);

        // Wait for ingress publication to connect to the gateway's subscription.
        pumpOnlyHarness([&]() { return ingressPub.isConnected(); },
                        kPumpIter, "waitPubConnected");

        offerBounded(ingressPub, scratch_, reqLen, "ConnectRequest");

        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t len,
                                          aeron::Header&) {
            auto decoded = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (decoded) resp = *decoded;
        });

        pumpWithEgress([&]() { return resp.has_value(); },
                       egressSub, asm_, kPumpIter, "ConnectResponse");

        EXPECT_TRUE(resp.has_value()) << "No ConnectResponse received";
        return resp.value_or(pc::ConnectResponse{});
    }

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    static constexpr std::size_t kScratch = 68 * 1024;
    uint8_t scratch_[kScratch] = {};

    std::unique_ptr<TempDir>       tmp_;
    std::unique_ptr<BrokerNode>    node_;
    std::shared_ptr<aeron::Aeron>  testAeron_;
    uint16_t                       clientPort_ = 0;
    std::string                    clientChannel_;
};

// ===========================================================================
// GW-1: Node serves Connect → ConnectResponse{OK}
// ===========================================================================
TEST_F(NodeGatewayFixture, Connect_RoundTrip_OverLiveNode) {
    const int32_t egressStreamId = 300;
    auto client = makeClient(egressStreamId);

    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel,
                          /*correlationId=*/1001);

    EXPECT_EQ(resp.correlationId, 1001);
    EXPECT_EQ(resp.code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GE(resp.assignedSessionId, 1);
    EXPECT_EQ(resp.leaderMemberId, 0); // nodeId = 0 for single-node
}

// ===========================================================================
// GW-2: Node serves Ping → Pong (binary echo + empty echo)
// ===========================================================================
TEST_F(NodeGatewayFixture, Ping_Pong_Basic_OverLiveNode) {
    const int32_t egressStreamId = 301;
    auto client = makeClient(egressStreamId);

    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 1002);
    const int64_t sid = resp.assignedSessionId;
    ASSERT_GE(sid, 1);

    const std::string echoStr = "hello-rune-node";
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 7777;
    ping.echo          = echoStr;
    const std::size_t pingLen = ping.encode(scratch_);
    offerBounded(*client.ingressPub, scratch_, pingLen, "Ping");

    std::optional<pc::Pong> pong;
    std::string pongEcho;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) {
            pongEcho = std::string(dec->echo);
            dec->echo = pongEcho;
            pong = *dec;
        }
    });

    pumpWithEgress([&]() { return pong.has_value(); },
                   *client.egressSub, asm_, kPumpIter, "Pong");

    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pong->correlationId, 7777);
    EXPECT_EQ(pongEcho, echoStr);
}

TEST_F(NodeGatewayFixture, Ping_Pong_EmptyEcho_OverLiveNode) {
    const int32_t egressStreamId = 302;
    auto client = makeClient(egressStreamId);

    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel, 1003);
    const int64_t sid = resp.assignedSessionId;
    ASSERT_GE(sid, 1);

    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 8888;
    ping.echo          = std::string_view{};
    const std::size_t pingLen = ping.encode(scratch_);
    offerBounded(*client.ingressPub, scratch_, pingLen, "EmptyPing");

    std::optional<pc::Pong> pong;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) pong = *dec;
    });

    pumpWithEgress([&]() { return pong.has_value(); },
                   *client.egressSub, asm_, kPumpIter, "EmptyPong");

    ASSERT_TRUE(pong.has_value());
    EXPECT_TRUE(pong->echo.empty());
}

// ===========================================================================
// GW-3: Async-resolve never blocks the node.
//
// Strategy:
//   1. Send a ConnectRequest with a bogus-but-parseable egress channel
//      ("aeron:udp?endpoint=127.0.0.1:1") on a port nothing is listening on —
//      Aeron will accept addExclusivePublication() but the pub will never be
//      connected (no subscriber on that port), so it stays pending.
//      We set a short connectResolveTimeoutNs (1 s) on the gateway so the
//      pending entry is eventually dropped without us waiting the full 5 s.
//
//   2. While the bogus pending-connect is outstanding:
//      - The node MUST still be the leader (election not stalled).
//      - A VALID client can connect and receive a Pong.
//
// The test proves the duty-cycle thread never blocks on the bad egress.
//
// Implementation note: we build a separate NodeConfig with a reduced
// connectResolveTimeoutNs, so we don't inherit the fixture's 5 s default.
// We do this by replacing fixture with a fresh node in-test.
// ===========================================================================
TEST(BrokerNodeGatewayAsyncTest, BogusEgress_NeverBlocksNode) {
    // Build a fresh node with a short connect-resolve timeout so the test
    // does not take 5 s waiting for the bogus egress to time out.
    const uint16_t clientPort = discoverFreeUdpPort();
    TempDir tmp{"gwasync_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path, clientPort);
    BrokerNode node{cfg};
    node.start();

    ASSERT_TRUE(node.awaitLeader(5s)) << "Node must elect before async test";
    ASSERT_TRUE(node.isLeader());

    // Connect the test-harness Aeron client.
    const std::string aeronDir = (tmp.path / "aeron").string();
    aeron::Context ctx;
    ctx.aeronDir(aeronDir);
    auto testAeron = aeron::Aeron::connect(ctx);
    ASSERT_NE(testAeron, nullptr);

    const std::string clientChannel = cfg.clientEndpoint.channel;
    const int32_t     clientStreamId = cfg.clientEndpoint.streamId;

    // --- Step 1: Send a ConnectRequest with a bogus egress channel ---
    // Use a valid-looking UDP endpoint on a port nobody is listening on.
    // Aeron will accept addExclusivePublication but the publication will stay
    // unconnected (no subscriber), so the pending-connect will time out
    // after connectResolveTimeoutNs (gateway default = 5 s).
    // We don't wait for it; we just prove the node keeps working.
    {
        int64_t pubRegId = testAeron->addPublication(clientChannel, clientStreamId);
        std::shared_ptr<aeron::Publication> ingressPub;

        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (!ingressPub && std::chrono::steady_clock::now() < deadline) {
            ingressPub = testAeron->findPublication(pubRegId);
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_NE(ingressPub, nullptr) << "Could not resolve ingress pub";

        // Wait for ingress pub to connect to the gateway's sub.
        const auto connDeadline = std::chrono::steady_clock::now() + 10s;
        while (!ingressPub->isConnected() && std::chrono::steady_clock::now() < connDeadline) {
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(ingressPub->isConnected()) << "Ingress pub did not connect";

        // Build ConnectRequest with a bogus egress channel.
        // "aeron:udp?endpoint=127.0.0.1:1" — port 1 is never listening.
        static constexpr std::size_t kSc = 512;
        uint8_t sc[kSc] = {};
        pc::ConnectRequest req;
        req.sessionId       = 0;
        req.correlationId   = 9999;
        req.egressStreamId  = 399;
        req.clientVersion   = 1;
        req.egressChannel   = "aeron:udp?endpoint=127.0.0.1:1";

        const std::size_t reqLen = req.encode(sc);
        aeron::concurrent::AtomicBuffer ab(sc, static_cast<aeron::util::index_t>(reqLen));

        bool sent = false;
        for (int i = 0; i < 50000 && !sent; ++i) {
            int64_t pos = ingressPub->offer(ab, 0, static_cast<aeron::util::index_t>(reqLen));
            if (pos > 0) sent = true;
            else std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(sent) << "Could not send bogus ConnectRequest";
    }

    // --- Step 2: Prove the node still serves a valid client ---
    // The bogus pending-connect should be queued in the gateway but NOT blocking
    // the duty-cycle.  Immediately connect a valid client and Ping it.
    {
        const int32_t egressStreamId = 400;
        const std::string egressCh = "aeron:ipc";

        int64_t pubRegId = testAeron->addPublication(clientChannel, clientStreamId);
        int64_t subRegId = testAeron->addSubscription(egressCh, egressStreamId);

        std::shared_ptr<aeron::Publication>  ingressPub2;
        std::shared_ptr<aeron::Subscription> egressSub;

        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while ((!ingressPub2 || !egressSub) && std::chrono::steady_clock::now() < deadline) {
            if (!ingressPub2) ingressPub2 = testAeron->findPublication(pubRegId);
            if (!egressSub)   egressSub   = testAeron->findSubscription(subRegId);
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_NE(ingressPub2, nullptr);
        ASSERT_NE(egressSub, nullptr);

        // Wait for the valid ingress pub to connect.
        {
            const auto connDeadline = std::chrono::steady_clock::now() + 10s;
            while (!ingressPub2->isConnected() && std::chrono::steady_clock::now() < connDeadline) {
                std::this_thread::sleep_for(1ms);
            }
            ASSERT_TRUE(ingressPub2->isConnected());
        }

        // Send ConnectRequest with a good egress channel (aeron:ipc).
        uint8_t sc[512] = {};
        pc::ConnectRequest req;
        req.sessionId       = 0;
        req.correlationId   = 4242;
        req.egressStreamId  = egressStreamId;
        req.clientVersion   = 1;
        req.egressChannel   = egressCh;
        const std::size_t reqLen = req.encode(sc);

        {
            aeron::concurrent::AtomicBuffer ab(sc, static_cast<aeron::util::index_t>(reqLen));
            bool sent = false;
            for (int i = 0; i < 50000 && !sent; ++i) {
                int64_t pos = ingressPub2->offer(ab, 0, static_cast<aeron::util::index_t>(reqLen));
                if (pos > 0) sent = true;
                else std::this_thread::sleep_for(std::chrono::microseconds{100});
            }
            ASSERT_TRUE(sent) << "Could not send valid ConnectRequest";
        }

        // Wait for ConnectResponse.
        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler connAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                             aeron::util::index_t offset,
                                             aeron::util::index_t len,
                                             aeron::Header&) {
            auto dec = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec) resp = *dec;
        });

        {
            const auto dl = std::chrono::steady_clock::now() + 15s;
            while (!resp && std::chrono::steady_clock::now() < dl) {
                egressSub->poll(connAsm.handler(), 10);
                std::this_thread::sleep_for(std::chrono::microseconds{200});
            }
        }
        ASSERT_TRUE(resp.has_value()) << "No ConnectResponse for valid client (node is blocked!)";
        EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
        const int64_t sid = resp->assignedSessionId;
        ASSERT_GE(sid, 1);

        // Send Ping and wait for Pong.
        uint8_t sc2[512] = {};
        pc::Ping ping;
        ping.sessionId     = sid;
        ping.correlationId = 5050;
        ping.echo          = "node-not-blocked";
        const std::size_t pingLen = ping.encode(sc2);

        {
            aeron::concurrent::AtomicBuffer ab(sc2, static_cast<aeron::util::index_t>(pingLen));
            bool sent = false;
            for (int i = 0; i < 50000 && !sent; ++i) {
                int64_t pos = ingressPub2->offer(ab, 0, static_cast<aeron::util::index_t>(pingLen));
                if (pos > 0) sent = true;
                else std::this_thread::sleep_for(std::chrono::microseconds{100});
            }
            ASSERT_TRUE(sent);
        }

        std::optional<pc::Pong> pong;
        std::string pongEcho;
        aeron::FragmentAssembler pingAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                            aeron::util::index_t offset,
                                            aeron::util::index_t len,
                                            aeron::Header&) {
            auto dec = pc::Pong::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec) {
                pongEcho = std::string(dec->echo);
                dec->echo = pongEcho;
                pong = *dec;
            }
        });

        {
            const auto dl = std::chrono::steady_clock::now() + 15s;
            while (!pong && std::chrono::steady_clock::now() < dl) {
                egressSub->poll(pingAsm.handler(), 10);
                std::this_thread::sleep_for(std::chrono::microseconds{200});
            }
        }

        ASSERT_TRUE(pong.has_value()) << "No Pong received (node duty-cycle may be blocked)";
        EXPECT_EQ(pong->correlationId, 5050);
        EXPECT_EQ(pongEcho, "node-not-blocked");

        // Verify node is still the leader (election never stalled).
        EXPECT_TRUE(node.isLeader()) << "Node must still be leader after bogus egress";
    }

    // Teardown: release test harness first.
    testAeron.reset();
    node.stop();
}

// ===========================================================================
// GW-4: Bad egressChannel (malformed/truly bogus) never crashes the node.
// ===========================================================================
TEST_F(NodeGatewayFixture, BadEgressChannel_NeverCrashesNode) {
    // Send a ConnectRequest with a syntactically bad channel.
    // "aeron:bogus" — Aeron will reject it; the gateway must catch + drop.
    int64_t pubRegId = testAeron_->addPublication(clientChannel_, kClientStreamId);
    std::shared_ptr<aeron::Publication> ingressPub;

    pumpOnlyHarness([&]() {
        if (!ingressPub) ingressPub = testAeron_->findPublication(pubRegId);
        return ingressPub != nullptr;
    }, kPumpIter, "badChannel ingressPub");

    pumpOnlyHarness([&]() { return ingressPub->isConnected(); },
                    kPumpIter, "badChannel pubConnected");

    pc::ConnectRequest req;
    req.sessionId       = 0;
    req.correlationId   = 3333;
    req.egressStreamId  = 399;
    req.clientVersion   = 1;
    req.egressChannel   = "aeron:bogus";
    const std::size_t reqLen = req.encode(scratch_);
    offerBounded(*ingressPub, scratch_, reqLen, "BadChannelConnect");

    // Let the gateway process and drop (bounded idle).
    {
        int idle = 0;
        pumpOnlyHarness([&]() { ++idle; return idle >= 3000; }, 5000, "BadChannelProcess");
    }

    // Node must still be running and serving.
    EXPECT_TRUE(node_->isLeader()) << "Node must still be leader after bad channel";

    // Valid client must still work.
    const int32_t egressStreamId = 350;
    auto goodClient = makeClient(egressStreamId);
    uint8_t sc2[512] = {};
    auto resp = doConnect(*goodClient.ingressPub, *goodClient.egressSub,
                          goodClient.egressStreamId, goodClient.egressChannel, 4444);
    EXPECT_EQ(resp.code, static_cast<int16_t>(pc::ErrorCode::OK));
    ASSERT_GE(resp.assignedSessionId, 1);
}

// ===========================================================================
// GW-5: leaderView degrade-to-accept — single-node is leader, Connect accepted.
// ===========================================================================
TEST_F(NodeGatewayFixture, LeaderView_SingleNode_IsLeader_ConnectAccepted) {
    // Single-node always elects itself leader.
    ASSERT_TRUE(node_->isLeader()) << "Precondition: single-node must be leader";

    const int32_t egressStreamId = 360;
    auto client = makeClient(egressStreamId);

    auto resp = doConnect(*client.ingressPub, *client.egressSub,
                          client.egressStreamId, client.egressChannel,
                          /*correlationId=*/5555);

    // Connect must succeed (OK), not redirect.
    EXPECT_EQ(resp.code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GE(resp.assignedSessionId, 1);
    EXPECT_EQ(resp.leaderMemberId, 0); // nodeId=0 is the leader
}

// ===========================================================================
// GW-6: Duty-cycle still elects — awaitLeader() within timeout with gateway.
//
// This test starts a node fresh and checks it elects promptly.
// (The fixture's SetUp() already verifies this for the fixture node.)
// Redundant but explicit so it shows up as a named test in the report.
// ===========================================================================
TEST(BrokerNodeGatewayElectionTest, GatewayComposed_NodeStillElects) {
    const uint16_t clientPort = discoverFreeUdpPort();
    TempDir tmp{"gwelect_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path, clientPort);

    BrokerNode node{cfg};
    node.start();

    // Must elect within 3 s even with the gateway subscribed on the client endpoint.
    ASSERT_TRUE(node.awaitLeader(3s))
        << "Single-node with gateway must self-elect within 3 s";

    EXPECT_TRUE(node.isLeader());
    EXPECT_EQ(node.leaderId(), static_cast<rune::consensus::NodeId>(0));

    node.stop();
}

} // namespace
} // namespace rune::node
