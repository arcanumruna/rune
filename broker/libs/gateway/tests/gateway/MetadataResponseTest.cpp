// MetadataResponseTest.cpp — E5.1 unit test: PartitionEntry.ingressStreamId
//
// Tests:
//   MRID-1: ingressStreamId == base + ShardRouter.workerForPartition(topicId, part)
//           for each partition in the MetadataResponse, without a WorkerPool (inline path).
//   MRID-2: ingressStreamId == base + WorkerPool.ownerWorkerId(topicId, part)
//           when a WorkerPool is wired (workerPool_ path).
//   MRID-3: Changing workerIngressBaseStreamId changes the reported stream ids.
//   MRID-4: With workerCount=1 all partitions report ingressStreamId == base + 0.
//
// No Aeron driver required: uses makeForTest() + applyMetadataForTest() + setOnEgressFrame().

#include <rune/gateway/ClientGateway.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace rune::gateway {
namespace {

namespace pc  = rune::protocol::client;
namespace md  = rune::metadata;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static int32_t gpid(int32_t topicId, int32_t partition) {
    return md::gpidOf(topicId, partition);
}

// Build a RoutingImage with entries [(topicId, part) → leaderNodeId=1, epoch=1].
static std::shared_ptr<const md::RoutingImage>
makeImage(const std::vector<std::pair<int32_t,int32_t>>& parts, int32_t leader = 1)
{
    std::vector<md::RoutingImage::BuildEntry> entries;
    entries.reserve(parts.size());
    for (const auto& [tid, pid] : parts) {
        entries.push_back({gpid(tid, pid), leader, 1});
    }
    return md::RoutingImage::build(entries);
}

// Encode a MetadataRequest frame (all topics, i.e. empty topicIds list).
static std::vector<uint8_t> makeMetadataRequestFrame(int64_t sessionId,
                                                      int64_t correlationId = 99)
{
    pc::MetadataRequest req;
    req.sessionId     = sessionId;
    req.correlationId = correlationId;
    std::vector<uint8_t> buf(req.encodedSize());
    req.encode(buf.data());
    return buf;
}

// Find the PartitionEntry for (topicId, partition) in the decoded response.
static std::optional<pc::PartitionEntry>
findEntry(const pc::MetadataResponse::Decoded& resp, int32_t topicId, int32_t partition)
{
    for (const auto& p : resp.partitions) {
        if (p.topicId == topicId && p.partition == partition) return p;
    }
    return std::nullopt;
}

// Size of a trailer-less (legacy layout) MetadataResponse.
static std::size_t rpcLegacySize(const std::vector<pc::BrokerEntry>& brokers, std::size_t parts) {
    std::size_t sz = pc::MessageHeader::ENCODED_SIZE + 2u;
    for (const auto& b : brokers) sz += 4u + 4u + 2u + b.host.size();
    return sz + 2u + parts * 28u;
}

// Egress callback that always succeeds (needed by makeForTest).
static int64_t acceptAllEgress(const uint8_t*, std::size_t) { return 1; }

// ===========================================================================
// MRID-1: ingressStreamId without WorkerPool (inline path).
//
// When workerPool_ is null, ingressStreamId = base + 0 for all partitions.
// This is the inline/test path: ShardRouter.workerForPartition() is not called.
// Asserts the formula base+0 consistently.
// ===========================================================================
TEST(MetadataResponseIngressStreamId, NoWorkerPoolUsesBaseStreamId)
{
    constexpr int32_t kBase = 50;
    // Two topics, three partitions.
    const std::vector<std::pair<int32_t,int32_t>> kParts = {{1, 0}, {1, 1}, {2, 0}};

    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, /*sessionId=*/1);
    gw.setRoutingImageSnapshot(makeImage(kParts));
    gw.setWorkerIngressBaseStreamId(kBase);
    gw.setCommittedMetadataReady(true);
    gw.setPerWorkerIngressActive(true); // advertise per-worker streams (gated contract)

    // Capture egress.
    std::optional<pc::MetadataResponse::Decoded> captured;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        auto dec = pc::MetadataResponse::decode(buf, len);
        if (dec) captured = std::move(dec);
    });

    // Inject session and drive handleMetadata.
    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());

    ASSERT_TRUE(captured.has_value()) << "No MetadataResponse captured";
    ASSERT_EQ(captured->partitions.size(), kParts.size());

    for (const auto& [tid, pid] : kParts) {
        auto e = findEntry(*captured, tid, pid);
        ASSERT_TRUE(e.has_value()) << "Missing partition topic=" << tid << " part=" << pid;
        // Without WorkerPool, workerForPartition falls back to 0.
        EXPECT_EQ(e->ingressStreamId, kBase + 0)
            << "topic=" << tid << " part=" << pid;
    }
}

// ===========================================================================
// MRID-2: ingressStreamId with WorkerPool wired (ShardRouter path).
//
// ShardRouter(workerCount=3) gives a deterministic worker assignment per shard.
// The gateway must report base + workerForPartition(topicId, partition).
// ===========================================================================
TEST(MetadataResponseIngressStreamId, WithWorkerPoolUsesShardRouter)
{
    constexpr int32_t kBase       = 100;
    constexpr int32_t kWorkers    = 3;

    // Partitions: topic 10 parts 0,1,2; topic 20 parts 0,1
    const std::vector<std::pair<int32_t,int32_t>> kParts = {
        {10, 0}, {10, 1}, {10, 2},
        {20, 0}, {20, 1},
    };

    // Build the ShardRouter — same configuration as what WorkerPool would use.
    md::ShardRouter router(kWorkers);
    // No co-partition groups: each topicId is its own groupKey (ShardRouter default).

    // Build WorkerPool.
    auto pool = std::make_unique<rune::node::WorkerPool>(
        kWorkers, router,
        /*ingressRingBytes=*/4096,
        /*returnRingBytes=*/4096,
        /*ioSubmitDepth=*/16,
        /*ioCompletionDepth=*/16
    );

    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, /*sessionId=*/1);
    gw.setRoutingImageSnapshot(makeImage(kParts));
    gw.setWorkerIngressBaseStreamId(kBase);
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    gw.setPerWorkerIngressActive(true); // advertise per-worker streams (gated contract)

    std::optional<pc::MetadataResponse::Decoded> captured;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        auto dec = pc::MetadataResponse::decode(buf, len);
        if (dec) captured = std::move(dec);
    });

    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());

    ASSERT_TRUE(captured.has_value()) << "No MetadataResponse captured";
    ASSERT_EQ(captured->partitions.size(), kParts.size());

    for (const auto& [tid, pid] : kParts) {
        auto e = findEntry(*captured, tid, pid);
        ASSERT_TRUE(e.has_value()) << "Missing partition topic=" << tid << " part=" << pid;
        const int32_t expectedWorker = router.workerForPartition(tid, pid);
        EXPECT_EQ(e->ingressStreamId, kBase + expectedWorker)
            << "topic=" << tid << " part=" << pid
            << " expectedWorker=" << expectedWorker;
    }
}

// ===========================================================================
// MRID-3: Changing workerIngressBaseStreamId is reflected in the response.
// ===========================================================================
TEST(MetadataResponseIngressStreamId, BaseStreamIdIsConfigurable)
{
    const std::vector<std::pair<int32_t,int32_t>> kParts = {{1, 0}};

    auto runWith = [&](int32_t base) -> int32_t {
        ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, /*sessionId=*/1);
        gw.setRoutingImageSnapshot(makeImage(kParts));
        gw.setWorkerIngressBaseStreamId(base);
        gw.setCommittedMetadataReady(true);
        gw.setPerWorkerIngressActive(true); // advertise per-worker streams (gated contract)

        std::optional<int32_t> result;
        gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
            auto dec = pc::MetadataResponse::decode(buf, len);
            if (dec && !dec->partitions.empty())
                result = dec->partitions[0].ingressStreamId;
        });

        const int64_t sid = gw.injectTestSession();
        gw.applyMetadataForTest(makeMetadataRequestFrame(sid).data(),
                                makeMetadataRequestFrame(sid).size());
        return result.value_or(-1);
    };

    EXPECT_EQ(runWith(2),   2);
    EXPECT_EQ(runWith(10),  10);
    EXPECT_EQ(runWith(100), 100);
}

// ===========================================================================
// MRID-4: workerCount=1 → all partitions map to worker 0 → all ingressStreamId == base.
// ===========================================================================
TEST(MetadataResponseIngressStreamId, SingleWorkerAllIngressStreamIdsEqualBase)
{
    constexpr int32_t kBase = 42;
    const std::vector<std::pair<int32_t,int32_t>> kParts = {
        {0, 0}, {0, 1}, {0, 2}, {1, 0}, {2, 0}
    };

    md::ShardRouter router(/*workerCount=*/1);
    auto pool = std::make_unique<rune::node::WorkerPool>(
        1, router, 4096, 4096, 16, 16);

    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, /*sessionId=*/1);
    gw.setRoutingImageSnapshot(makeImage(kParts));
    gw.setWorkerIngressBaseStreamId(kBase);
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    gw.setPerWorkerIngressActive(true); // advertise per-worker streams (gated contract)

    std::optional<pc::MetadataResponse::Decoded> captured;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        auto dec = pc::MetadataResponse::decode(buf, len);
        if (dec) captured = std::move(dec);
    });

    const int64_t sid = gw.injectTestSession();
    gw.applyMetadataForTest(makeMetadataRequestFrame(sid).data(),
                            makeMetadataRequestFrame(sid).size());

    ASSERT_TRUE(captured.has_value());
    ASSERT_EQ(captured->partitions.size(), kParts.size());

    for (const auto& p : captured->partitions) {
        EXPECT_EQ(p.ingressStreamId, kBase)
            << "workerCount=1 → all partitions must report base stream id; "
               "topic=" << p.topicId << " part=" << p.partition;
    }
}

// ===========================================================================
// MRID-5 (REGRESSION): Per-worker ingress INACTIVE → ingressStreamId ==
// clientStreamId_ (the gateway's OWN client ingress stream), NOT 0.
//
// Regression for the dr3 rolling-leader-kill ping-pong (produced 44/50):
// advertising the disabling sentinel 0 when per-worker ingress is inactive
// stopped the client from building ANY per-partition routes (its route
// builder skips ingressStreamId==0).  Once partition leadership legitimately
// SPREADS across nodes after failover, the client was forced onto a single
// shared transport that ping-ponged between two partition leaders on
// consecutive records and exhausted its redirect budget.  Every node's
// gateway subscribes on clientStreamId_, so advertising it is always a safe,
// consistent routing target that lets the client reach each partition's true
// leader directly (no redirect loop).  A WorkerPool being wired does NOT flip
// the gate; only setPerWorkerIngressActive(true) selects per-worker streams.
// ===========================================================================
TEST(MetadataResponseIngressStreamIdRegression, InactiveAdvertisesClientStream)
{
    constexpr int32_t kBase         = 77;
    constexpr int32_t kWorkers      = 3;
    constexpr int32_t kClientStream = 1;   // the gateway's client ingress stream
    const std::vector<std::pair<int32_t,int32_t>> kParts = {
        {10, 0}, {10, 1}, {10, 2}, {20, 0}, {20, 1},
    };

    md::ShardRouter router(kWorkers);
    auto pool = std::make_unique<rune::node::WorkerPool>(
        kWorkers, router, 4096, 4096, 16, 16);

    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, /*sessionId=*/1);
    gw.setRoutingImageSnapshot(makeImage(kParts));
    gw.setWorkerIngressBaseStreamId(kBase);
    gw.setClientStreamIdForTest(kClientStream);
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    // NOTE: setPerWorkerIngressActive NOT called → remains false (default).

    std::optional<pc::MetadataResponse::Decoded> captured;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        auto dec = pc::MetadataResponse::decode(buf, len);
        if (dec) captured = std::move(dec);
    });

    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());

    ASSERT_TRUE(captured.has_value()) << "No MetadataResponse captured";
    ASSERT_EQ(captured->partitions.size(), kParts.size());

    for (const auto& p : captured->partitions) {
        EXPECT_EQ(p.ingressStreamId, kClientStream)
            << "per-worker ingress inactive → must advertise clientStreamId_ so the "
               "client can route per-partition to the true leader (NOT the disabling "
               "sentinel 0); topic=" << p.topicId << " part=" << p.partition;
    }
}

// ===========================================================================
// Self-contained worker shards: per-partition ingressEndpoint (spec §3.3).
// ===========================================================================
TEST(MetadataResponseIngressEndpoint, ShardModeAdvertisesLeaderWorkerEndpoint)
{
    constexpr int32_t kWorkers = 2;
    const std::vector<std::pair<int32_t,int32_t>> kParts = {{10, 0}, {10, 1}, {10, 2}, {20, 0}};
    md::ShardRouter router(kWorkers);
    auto pool = std::make_unique<rune::node::WorkerPool>(kWorkers, router, 4096, 4096, 16, 16);
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, 1);
    gw.setRoutingImageSnapshot(makeImage(kParts, /*leader=*/1));
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    gw.setPerWorkerIngressActive(true);
    gw.setShardIngressEndpoints({{0, "10.0.0.1", 40500}, {1, "10.0.0.2", 40520}});
    std::optional<pc::MetadataResponse::Decoded> captured;
    std::vector<uint8_t> frameCopy;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        frameCopy.assign(buf, buf + len);
        captured = pc::MetadataResponse::decode(frameCopy.data(), frameCopy.size());
    });
    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());
    ASSERT_TRUE(captured.has_value());
    for (const auto& [tid, pid] : kParts) {
        auto e = findEntry(*captured, tid, pid);
        ASSERT_TRUE(e.has_value());
        EXPECT_EQ(std::string(e->ingressEndpoint),
                  "10.0.0.2:" + std::to_string(40520 + router.workerForPartition(tid, pid)))
            << "topic=" << tid << " part=" << pid;
    }
}

TEST(MetadataResponseIngressEndpoint, LegacyModeAdvertisesNoEndpoint)
{
    md::ShardRouter router(1);
    auto pool = std::make_unique<rune::node::WorkerPool>(1, router, 4096, 4096, 16, 16);
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, 1);
    gw.setRoutingImageSnapshot(makeImage({{10, 0}}));
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    std::vector<uint8_t> frameCopy;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) { frameCopy.assign(buf, buf + len); });
    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());
    auto dec = pc::MetadataResponse::decode(frameCopy.data(), frameCopy.size());
    ASSERT_TRUE(dec.has_value());
    EXPECT_TRUE(findEntry(*dec, 10, 0)->ingressEndpoint.empty());
    EXPECT_EQ(frameCopy.size(), rpcLegacySize(dec->brokers, dec->partitions.size()))
        << "default config frames are byte-identical (no trailer)";
}

TEST(MetadataResponseIngressEndpointRegression, LeaderWithoutAdvertisedBaseGetsEmptyEndpoint)
{
    md::ShardRouter router(1);
    auto pool = std::make_unique<rune::node::WorkerPool>(1, router, 4096, 4096, 16, 16);
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, 1);
    gw.setRoutingImageSnapshot(makeImage({{10, 0}}, /*leader=*/2));
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    gw.setShardIngressEndpoints({{0, "10.0.0.1", 40500}});
    std::vector<uint8_t> frameCopy;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) { frameCopy.assign(buf, buf + len); });
    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());
    auto dec = pc::MetadataResponse::decode(frameCopy.data(), frameCopy.size());
    ASSERT_TRUE(dec.has_value());
    EXPECT_TRUE(findEntry(*dec, 10, 0)->ingressEndpoint.empty()) << "client falls back to the node endpoint";
}

// ===========================================================================
// F25 (REGRESSION): shard mode advertised ingressStreamId = workerIngressBase
// + owner, but the shard WorkerIngress subscribes that stream ONLY on the
// per-worker endpoint — a client that ignores the additive ingressEndpoint
// trailer (spec §2: "older clients ignore it") sent it to the NODE endpoint,
// where nothing subscribes → NOT_CONNECTED forever.  Shard mode must advertise
// the stream the node endpoint's gateway subscribes (clientStreamId_); each
// shard's WorkerIngress subscribes the same stream on its own port.
// ===========================================================================
TEST(MetadataResponseIngressStreamIdRegression, ShardModeAdvertisesTheStreamTheNodeEndpointSubscribes)
{
    constexpr int32_t kBase = 77, kClientStream = 110, kWorkers = 2;
    const std::vector<std::pair<int32_t,int32_t>> kParts = {{10, 0}, {10, 1}, {10, 2}, {20, 0}};
    md::ShardRouter router(kWorkers);
    auto pool = std::make_unique<rune::node::WorkerPool>(kWorkers, router, 4096, 4096, 16, 16);
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, 1);
    gw.setRoutingImageSnapshot(makeImage(kParts, /*leader=*/1));
    gw.setWorkerIngressBaseStreamId(kBase);
    gw.setClientStreamIdForTest(kClientStream);
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    gw.setPerWorkerIngressActive(true);     // what attachShardRuntimes_ sets
    gw.setShardMode(true);
    gw.setShardIngressEndpoints({{1, "10.0.0.2", 40520}});
    std::vector<uint8_t> frameCopy;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) { frameCopy.assign(buf, buf + len); });
    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());
    auto dec = pc::MetadataResponse::decode(frameCopy.data(), frameCopy.size());
    ASSERT_TRUE(dec.has_value());
    ASSERT_EQ(dec->partitions.size(), kParts.size());
    for (const auto& p : dec->partitions) {
        EXPECT_EQ(p.ingressStreamId, kClientStream)
            << "shard mode: the advertised stream must be live on the node endpoint (gateway) AND on the "
               "advertised worker endpoint; topic=" << p.topicId << " part=" << p.partition;
        EXPECT_FALSE(std::string(p.ingressEndpoint).empty()) << "the endpoint-aware route stays advertised";
    }
}

} // namespace
} // namespace rune::gateway
