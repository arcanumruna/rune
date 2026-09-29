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
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
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

} // namespace
} // namespace rune::gateway
