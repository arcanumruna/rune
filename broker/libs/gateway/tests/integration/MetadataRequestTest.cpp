// MetadataRequestTest.cpp — MD1 Task 10: handleMetadata integration tests.
//
// Tests:
//   META-1: MetadataRequest returns correct per-partition leader + epoch + brokers.
//   META-2: Any node answers (leaderView_ says NOT leader, but Metadata still returns data).
//   META-3: Empty routing image → empty partitions list, but brokers still present.
//   META-4: Filtered request (specific topicIds) returns only matching partitions.
//   META-5: Regression check — existing produce path still works after MetadataRequest
//           is wired in (no dispatch pollution).
//
// Design:
//   - Embedded Aeron driver (IPC transport) — same pattern as GatewayPerPartitionRoutingTest.
//   - Gateway built with leaderView_ = "leader" for produce tests, and with
//     leaderView_ = "not leader" for META-2 (to prove any-node answering).
//   - Routing image injected via setRoutingImageSnapshot().
//   - nodeEndpointMap_ injected via setNodeEndpointMap().
//
// The MetadataResponse codec tests (round-trip, LE bytes, truncation) live in
// rune-protocol/tests/protocol/client/MetadataMessagesTest.cpp.
// This file tests the gateway-level behaviour: correct contents, any-node, filter.

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/MetadataMessages.h>
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
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;
namespace md = rune::metadata;
using rune::node::TopicSpec;

namespace rune::gateway {
namespace {

// ---------------------------------------------------------------------------
// Temp-dir helper
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "")
{
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gwmeta_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "") : path(makeTempDir(tag)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

static int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

static TopicSpec makeSpec(std::string name, int32_t partCount, int32_t id)
{
    TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = partCount;
    ts.topicId        = id;
    return ts;
}

// GPID helper — delegates to the single source of truth in Gpid.h.
// Local alias `gpid` keeps the existing call sites unchanged.
static int32_t gpid(int32_t topicId, int32_t partition)
{
    return rune::metadata::gpidOf(topicId, partition);
}

static const std::string kNode1Endpoint = "aeron:udp?endpoint=node1:40200";
static const std::string kNode2Endpoint = "aeron:udp?endpoint=node2:40201";

// ---------------------------------------------------------------------------
// MetadataRequestFixture
// ---------------------------------------------------------------------------
class MetadataRequestFixture : public ::testing::Test {
public:
    static constexpr int32_t kIngressStream = 960;
    static constexpr int32_t kEgressBase    = 970;
    static constexpr int32_t kThisNodeId    = 1;
    static constexpr int32_t kOtherNodeId   = 2;
    static constexpr int     kPumpIter      = 50'000;

    // Two topics:
    //   topic 0 "alpha", 2 partitions
    //   topic 1 "beta",  1 partition
    static constexpr int32_t kTopicAlpha   = 0;
    static constexpr int32_t kTopicBeta    = 1;

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("meta_");
        startDriver();
        connectAeron();
    }

    void TearDown() override {
        gw_.reset();
        topicReg_.reset();
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    // Build gateway with optional leader flag.
    void buildGateway(bool isLeader = true)
    {
        std::vector<TopicSpec> specs = {
            makeSpec("alpha", 2, kTopicAlpha),
            makeSpec("beta",  1, kTopicBeta),
        };
        topicReg_ = std::make_unique<TopicRegistry>(specs, tmpDir_->path.string());

        auto leaderViewFn = [isLeader]() -> LeaderView {
            return LeaderView{isLeader, isLeader ? "" : kNode1Endpoint};
        };

        gw_ = std::make_unique<ClientGateway>(
            aeron_,
            "aeron:ipc",
            kIngressStream,
            kThisNodeId,
            ClientGateway::kDefaultSessionTimeoutNs,
            std::move(leaderViewFn));

        gw_->setTopicRegistry(topicReg_.get());

        // Node-endpoint map.
        std::unordered_map<int32_t, std::string> nodeEndpoints;
        nodeEndpoints[kThisNodeId]  = kNode1Endpoint;
        nodeEndpoints[kOtherNodeId] = kNode2Endpoint;
        gw_->setNodeEndpointMap(std::move(nodeEndpoints));
    }

    // Inject a routing image covering all known partitions.
    //   alpha/0 → node1 epoch e
    //   alpha/1 → node2 epoch e
    //   beta/0  → node1 epoch (e+1)
    void injectRoutingImage(int32_t epoch = 1)
    {
        std::vector<md::RoutingImage::BuildEntry> entries = {
            {gpid(kTopicAlpha, 0), kThisNodeId,  epoch},
            {gpid(kTopicAlpha, 1), kOtherNodeId, epoch},
            {gpid(kTopicBeta,  0), kThisNodeId,  epoch + 1},
        };
        auto img = md::RoutingImage::build(entries);
        gw_->setRoutingImageSnapshot(img);
    }

    // -----------------------------------------------------------------------
    // TestClient helpers (same pattern as GatewayPerPartitionRoutingTest).
    // -----------------------------------------------------------------------
    struct TestClient {
        std::shared_ptr<aeron::Publication>  ingressPub;
        std::shared_ptr<aeron::Subscription> egressSub;
        int32_t                              egressStreamId;
        std::string                          egressChannel{"aeron:ipc"};
    };

    TestClient makeClient(int32_t egressStreamId) {
        int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
        int64_t subRegId = aeron_->addSubscription("aeron:ipc", egressStreamId);
        std::shared_ptr<aeron::Publication>  pub;
        std::shared_ptr<aeron::Subscription> sub;
        pump([&]() {
            if (!pub) pub = aeron_->findPublication(pubRegId);
            if (!sub) sub = aeron_->findSubscription(subRegId);
            return pub && sub;
        }, nullptr, nullptr, kPumpIter, "makeClient");
        return TestClient{pub, sub, egressStreamId};
    }

    int64_t connectClient(TestClient& client, int64_t correlationId = 1) {
        pc::ConnectRequest req;
        req.sessionId      = 0;
        req.correlationId  = correlationId;
        req.egressStreamId = client.egressStreamId;
        req.clientVersion  = 1;
        req.egressChannel  = client.egressChannel;
        uint8_t sc[512] = {};
        const std::size_t len = req.encode(sc);
        offerBounded(*client.ingressPub, sc, len, "Connect");

        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler connAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                             aeron::util::index_t offset,
                                             aeron::util::index_t flen,
                                             aeron::Header&) {
            auto dec = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(flen));
            if (dec) resp = *dec;
        });
        pump([&]() { return resp.has_value(); },
             &*client.egressSub, &connAsm, kPumpIter, "ConnectResponse");
        EXPECT_TRUE(resp.has_value());
        if (!resp) return -1;
        EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
        return resp->assignedSessionId;
    }

    // Send a MetadataRequest and wait for the MetadataResponse.
    std::optional<pc::MetadataResponse::Decoded>
    sendMetadataRequest(TestClient& client, int64_t sessionId, int64_t corrId,
                        std::vector<int32_t> topicIds = {})
    {
        pc::MetadataRequest req;
        req.sessionId     = sessionId;
        req.correlationId = corrId;
        req.topicIds      = std::span<const int32_t>(topicIds.data(), topicIds.size());

        std::vector<uint8_t> buf(req.encodedSize());
        req.encode(buf.data());
        offerBounded(*client.ingressPub, buf.data(), buf.size(), "MetadataRequest");

        std::optional<pc::MetadataResponse::Decoded> result;
        aeron::FragmentAssembler metaAsm([&](aeron::concurrent::AtomicBuffer& abuf,
                                             aeron::util::index_t offset,
                                             aeron::util::index_t flen,
                                             aeron::Header&) {
            auto dec = pc::MetadataResponse::decode(
                reinterpret_cast<const uint8_t*>(abuf.buffer() + offset),
                static_cast<std::size_t>(flen));
            if (dec && dec->correlationId == corrId) result = std::move(dec);
        });
        pump([&]() { return result.has_value(); },
             &*client.egressSub, &metaAsm, kPumpIter, "MetadataResponse");
        return result;
    }

    static void offerBounded(aeron::Publication& pub,
                              const uint8_t* buf, std::size_t len,
                              std::string_view label = "offer") {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(buf),
            static_cast<aeron::util::index_t>(len));
        bool sent = false;
        for (int i = 0; i < kPumpIter && !sent; ++i) {
            if (pub.offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0)
                sent = true;
            else
                std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(sent) << "offerBounded timed out: " << label;
    }

    template <typename DoneFn>
    void pump(DoneFn&& done,
              aeron::Subscription*      clientSub = nullptr,
              aeron::FragmentAssembler* clientAsm = nullptr,
              int                       maxIter   = kPumpIter,
              std::string_view          label     = "pump") {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{10};
        for (int i = 0; i < maxIter && clock::now() < deadline; ++i) {
            if (gw_) gw_->doWork(nowNs());
            if (clientSub && clientAsm) clientSub->poll(clientAsm->handler(), 10);
            if (done()) return;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(done()) << "pump timed out: " << label;
    }

    std::unique_ptr<ClientGateway>  gw_;
    std::unique_ptr<TopicRegistry>  topicReg_;
    std::shared_ptr<aeron::Aeron>   aeron_;

private:
    void startDriver() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        fs::create_directories(tmpDir_->path);

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);

        ASSERT_EQ(aeron_driver_init(&driver_, driverCtx_), 0)
            << "aeron_driver_init failed: " << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, /*manual_main_loop=*/true), 0)
            << "aeron_driver_start failed: " << aeron_errmsg();

        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_,
                                                aeron_driver_main_do_work(driver_));
            }
        });

        // No caller-side pumping: aeron_driver_init/start already wrote
        // cnc.dat and stamped the heartbeat synchronously before driverThread_
        // even starts; pumping here would be a second concurrent pumper on
        // the single-pumper driver (spec Sec 3.6).
        const std::string cncPath = aeronDir + "/cnc.dat";
        ASSERT_TRUE(fs::exists(cncPath)) << "Aeron CnC not created within timeout";
    }

    void connectAeron() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    void stopDriver() {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            const auto dl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds{50};
            while (std::chrono::steady_clock::now() < dl) {
                if (aeron_driver_main_do_work(driver_) == 0) {
                    aeron_driver_main_do_work(driver_);
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds{200});
            }
            aeron_driver_close(driver_);
            driver_ = nullptr;
        }
        if (driverCtx_) {
            aeron_driver_context_close(driverCtx_);
            driverCtx_ = nullptr;
        }
    }

    std::unique_ptr<TempDir>    tmpDir_;
    aeron_driver_context_t*     driverCtx_    = nullptr;
    aeron_driver_t*             driver_       = nullptr;
    std::atomic<bool>           driverRunning_{false};
    std::thread                 driverThread_;
};

// ===========================================================================
// META-1: MetadataRequest returns correct per-partition leader + epoch + brokers.
// ===========================================================================
TEST_F(MetadataRequestFixture, ReturnsCorrectPartitionLeadersAndBrokers)
{
    buildGateway(/*isLeader=*/true);
    injectRoutingImage(/*epoch=*/3);

    auto client = makeClient(kEgressBase + 1);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitConnected");

    const int64_t sid = connectClient(client);
    ASSERT_GT(sid, 0);

    // Send empty MetadataRequest (all topics).
    auto respOpt = sendMetadataRequest(client, sid, /*corrId=*/100);
    ASSERT_TRUE(respOpt.has_value()) << "Expected MetadataResponse";

    const auto& resp = *respOpt;
    EXPECT_EQ(resp.correlationId, 100);

    // Check brokers: we injected 2 entries (node1, node2).
    EXPECT_EQ(resp.brokers.size(), 2u);
    bool foundNode1 = false, foundNode2 = false;
    for (const auto& b : resp.brokers) {
        if (b.nodeId == kThisNodeId)  { EXPECT_EQ(b.port, 40200); foundNode1 = true; }
        if (b.nodeId == kOtherNodeId) { EXPECT_EQ(b.port, 40201); foundNode2 = true; }
    }
    EXPECT_TRUE(foundNode1) << "node1 missing from brokers";
    EXPECT_TRUE(foundNode2) << "node2 missing from brokers";

    // Check partitions: 3 total (alpha/0, alpha/1, beta/0).
    EXPECT_EQ(resp.partitions.size(), 3u);

    // Build a lookup map.
    std::unordered_map<int64_t, pc::PartitionEntry> byKey;
    for (const auto& p : resp.partitions) {
        byKey[(int64_t(p.topicId) << 32) | uint32_t(p.partition)] = p;
    }

    auto checkPart = [&](int32_t topicId, int32_t partition,
                          int32_t leaderId, int32_t epoch) {
        const int64_t key = (int64_t(topicId) << 32) | uint32_t(partition);
        auto it = byKey.find(key);
        ASSERT_NE(it, byKey.end()) << "partition missing: topic=" << topicId
                                    << " part=" << partition;
        EXPECT_EQ(it->second.leaderId,    leaderId) << "wrong leaderId";
        EXPECT_EQ(it->second.leaderEpoch, epoch)    << "wrong epoch";
    };

    checkPart(kTopicAlpha, 0, kThisNodeId,  3);   // alpha/0 → node1 epoch 3
    checkPart(kTopicAlpha, 1, kOtherNodeId, 3);   // alpha/1 → node2 epoch 3
    checkPart(kTopicBeta,  0, kThisNodeId,  4);   // beta/0  → node1 epoch 4
}

// ===========================================================================
// META-2: Any node answers (leaderView says NOT leader, but Metadata still works).
// ===========================================================================
TEST_F(MetadataRequestFixture, AnyNodeAnswers_NotLeaderStillResponds)
{
    buildGateway(/*isLeader=*/false);  // This node is NOT the cluster leader.
    injectRoutingImage(/*epoch=*/5);

    auto client = makeClient(kEgressBase + 2);

    // With leaderView_ = not leader, a ConnectRequest will be redirected.
    // But we can inject a test session directly (bypasses cluster-level leader check).
    const int64_t sid = gw_->injectTestSession(nowNs());

    // Register an egress frame capture callback.
    std::optional<pc::MetadataResponse::Decoded> captured;
    gw_->setOnEgressFrame([&](int64_t /*sessionId*/, const uint8_t* buf, std::size_t len) {
        auto dec = pc::MetadataResponse::decode(buf, len);
        if (dec) captured = std::move(dec);
    });

    // Build and inject MetadataRequest directly via the ingress mechanism.
    // Since we can't easily go via Aeron here (the session is injected), we call
    // the gateway's applySubscribeForTest-equivalent path.  Because MetadataRequest
    // is a new message, we use the same trick: build the wire frame and call onFragment
    // via the gateway's internal dispatch.
    // Since there's no direct applyMetadataForTest() yet, we pump the gateway
    // after offering the request via Aeron.

    // Use Aeron for send: the gateway IS subscribed to kIngressStream.
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitConnected");

    // --- No session via connect (gateway would redirect because not leader).
    // Instead, we test using the onEgressFrame callback + injectTestSession trick:
    // Build MetadataRequest with the injected session id and offer it on the
    // ingress publication.  The gateway dispatches it and sends the response
    // through onEgressFrame (because injectTestSession has null egress pub).
    pc::MetadataRequest req;
    req.sessionId     = sid;
    req.correlationId = 200;
    req.topicIds      = std::span<const int32_t>{};  // all topics

    std::vector<uint8_t> reqBuf(req.encodedSize());
    req.encode(reqBuf.data());

    offerBounded(*client.ingressPub, reqBuf.data(), reqBuf.size(), "MetadataReqNotLeader");
    pump([&]() { return captured.has_value(); },
         nullptr, nullptr, kPumpIter, "MetadataResponseNotLeader");

    ASSERT_TRUE(captured.has_value()) << "Metadata should be answered even on non-leader";
    EXPECT_EQ(captured->partitions.size(), 3u);
}

// ===========================================================================
// META-3: Empty routing image → partitions list is empty, brokers still present.
// ===========================================================================
TEST_F(MetadataRequestFixture, EmptyRoutingImage_EmptyPartitions)
{
    buildGateway(/*isLeader=*/true);
    // Do NOT inject a routing image.

    const int64_t sid = gw_->injectTestSession(nowNs());
    std::optional<pc::MetadataResponse::Decoded> captured;
    gw_->setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        auto dec = pc::MetadataResponse::decode(buf, len);
        if (dec) captured = std::move(dec);
    });

    auto client = makeClient(kEgressBase + 3);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitConnected");

    pc::MetadataRequest req;
    req.sessionId     = sid;
    req.correlationId = 300;
    std::vector<uint8_t> reqBuf(req.encodedSize());
    req.encode(reqBuf.data());

    offerBounded(*client.ingressPub, reqBuf.data(), reqBuf.size(), "MetadataReqNoImage");
    pump([&]() { return captured.has_value(); },
         nullptr, nullptr, kPumpIter, "MetadataResponseNoImage");

    ASSERT_TRUE(captured.has_value());
    EXPECT_TRUE(captured->partitions.empty()) << "No routing image → no partitions";
    EXPECT_EQ(captured->brokers.size(), 2u)   << "Brokers still returned (from nodeEndpointMap_)";
}

// ===========================================================================
// META-4: Filtered request — specific topicIds → only matching partitions.
// ===========================================================================
TEST_F(MetadataRequestFixture, FilteredRequest_ReturnsOnlyMatchingTopics)
{
    buildGateway(/*isLeader=*/true);
    injectRoutingImage(/*epoch=*/7);

    const int64_t sid = gw_->injectTestSession(nowNs());
    std::optional<pc::MetadataResponse::Decoded> captured;
    gw_->setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        auto dec = pc::MetadataResponse::decode(buf, len);
        if (dec && dec->correlationId == 400) captured = std::move(dec);
    });

    auto client = makeClient(kEgressBase + 4);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitConnected");

    // Request only topicId=kTopicBeta (should return only 1 partition).
    std::vector<int32_t> filter = {kTopicBeta};
    pc::MetadataRequest req;
    req.sessionId     = sid;
    req.correlationId = 400;
    req.topicIds      = std::span<const int32_t>(filter.data(), filter.size());
    std::vector<uint8_t> reqBuf(req.encodedSize());
    req.encode(reqBuf.data());

    offerBounded(*client.ingressPub, reqBuf.data(), reqBuf.size(), "MetadataReqFiltered");
    pump([&]() { return captured.has_value(); },
         nullptr, nullptr, kPumpIter, "MetadataResponseFiltered");

    ASSERT_TRUE(captured.has_value());
    // Only beta partitions — only beta/0 was in the routing image.
    ASSERT_EQ(captured->partitions.size(), 1u);
    EXPECT_EQ(captured->partitions[0].topicId,     kTopicBeta);
    EXPECT_EQ(captured->partitions[0].partition,   0);
    EXPECT_EQ(captured->partitions[0].leaderId,    kThisNodeId);
    EXPECT_EQ(captured->partitions[0].leaderEpoch, 8);  // epoch+1
}

// ===========================================================================
// META-5: Regression — existing Dispatch still routes other messages correctly
//         after MetadataRequest is wired in.  No dispatch pollution.
// ===========================================================================
TEST_F(MetadataRequestFixture, Regression_OtherMessagesStillDispatch)
{
    buildGateway(/*isLeader=*/true);
    injectRoutingImage(/*epoch=*/1);

    auto client = makeClient(kEgressBase + 5);
    pump([&]() { return client.ingressPub->isConnected(); },
         nullptr, nullptr, kPumpIter, "waitConnected");

    // Connect → should still get a ConnectResponse{OK}.
    const int64_t sid = connectClient(client, /*correlationId=*/500);
    EXPECT_GT(sid, 0) << "Connect should still work after Metadata wiring";
}

} // namespace
} // namespace rune::gateway
