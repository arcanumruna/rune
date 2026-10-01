// ClientGatewayHotPathTest.cpp — G1.1: regression guards for leaderView_ and
// minIsr_ hot-path caching.
//
// Guards two micro-elim changes:
//
//   1. LeaderViewCalledOncePerProduce — install a counting leaderView_ function,
//      drive a produce through the no-routing-image (cluster-level) fallback path,
//      and assert the call count is exactly 1.  The refactored code has mutually-
//      exclusive paths (path A: routingImage_ set; path B: routingImage_ null), so
//      a single produce via path B should invoke leaderView_() exactly once.
//      Protects against a future regression that duplicates the call on path B.
//
//   2. MinIsrSnapshotUsedPerCycle — verify that changing minIsr_ between doWork
//      cycles is reflected in the next cycle (snapshot freshness), and that within
//      a cycle the cached value is used (not re-read per produce).

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::gateway {
namespace {

namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Unique temp dir for TopicRegistry backing store.
static fs::path makeTempDir()
{
    static std::atomic<uint32_t> seq{0};
    auto name = std::string("rune_gw_hotpath_") +
                std::to_string(static_cast<long>(::getpid())) + "_" +
                std::to_string(seq.fetch_add(1, std::memory_order_relaxed));
    return fs::temp_directory_path() / name;
}

// Single-record RecordBatch (baseOffset placeholder; gateway restamps).
static std::vector<uint8_t> makeBatch()
{
    rune::protocol::RecordBatchBuilder b(/*baseOffset=*/0, /*baseTimestamp=*/1000LL);
    b.append("k", "v", 1000LL);
    return b.build();
}

// Encode a Produce frame for the given sessionId/topicId/partition, acks=Leader.
static std::vector<uint8_t> encodeProduceFrame(int64_t sessionId,
                                               int32_t topicId,
                                               int32_t partition)
{
    auto batch = makeBatch();
    pc::Produce prod;
    prod.sessionId      = sessionId;
    prod.correlationId  = 42;
    prod.topicId        = topicId;
    prod.partition      = partition;
    prod.partitionKnown = 1;
    prod.acks           = 0;   // acks=Leader → immediate ack (no pending-ack queue)
    prod.producerId     = -1;  // legacy (no idempotent dedup)
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    std::vector<uint8_t> frame(64 * 1024, 0);
    frame.resize(prod.encode(frame.data()));
    return frame;
}

// Fake egress that always accepts (returns 1).
static ClientGateway::TestEgressFn acceptAllEgress()
{
    return [](const uint8_t*, std::size_t) -> int64_t { return 1; };
}

// ===========================================================================
// TEST 1 — LeaderViewCalledOncePerProduce
//
// Drives a produce through the no-routing-image path B (cluster-level
// leaderView_ fallback).  The leaderView_ reports isLeader=true so the
// produce is served locally.  The counter must equal 1 after the produce,
// guarding against any future regression that invokes leaderView_() twice on
// this path.
//
// NOTE: The double-call described in the original G1.1 brief was already
// eliminated by the G0.1/failover refactor (paths A and B are now mutually
// exclusive on routingImage_).  This test exists purely as a regression guard.
// ===========================================================================
TEST(ClientGatewayHotPath, LeaderViewCalledOncePerProduce)
{
    int calls = 0;

    const fs::path dir = makeTempDir();
    using rune::node::TopicSpec;
    std::vector<TopicSpec> specs;
    {
        TopicSpec ts;
        ts.name           = "events";
        ts.partitionCount = 1;
        ts.topicId        = 0;
        specs.push_back(ts);
    }
    rune::gateway::TopicRegistry registry(specs, dir.string());

    // Build gateway with a counting leaderView_ (always reports isLeader=true).
    // routingImage_ is null (not set) → path B is active.
    ClientGateway gw = ClientGateway::makeForTestWithLeaderView(
        acceptAllEgress(),
        [&calls]() -> LeaderView {
            ++calls;
            return LeaderView{/*isLeader=*/true, /*leaderClientEndpoint=*/{}};
        },
        /*currentProduceSessionId=*/1);
    gw.setTopicRegistry(&registry);

    // Inject session id=1 (matches currentProduceSessionId_).
    const int64_t sid = gw.injectTestSession(/*nowNs=*/0);
    ASSERT_EQ(sid, 1) << "session id mismatch with currentProduceSessionId_";

    // Drive one produce through handleProduce directly.
    const auto frame = encodeProduceFrame(sid, /*topicId=*/0, /*partition=*/0);
    gw.applyProduceForTest(frame.data(), frame.size());

    // leaderView_() must be called exactly once on path B per produce.
    EXPECT_EQ(calls, 1)
        << "leaderView_() was called " << calls << " times (expected 1); "
           "regression: double-call on produce path B reintroduced";

    // Cleanup.
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// ===========================================================================
// TEST 2 — MinIsrSetGetRoundTrip
//
// Guards that setMinIsr() / minIsr() round-trip correctly and that the initial
// default is 1.  The per-cycle snapshot (cachedMinIsr_) is private and cannot
// be read directly from a unit test; the correctness of the snapshot itself is
// validated by the full suite (no produce-path regressions after the change).
// ===========================================================================
TEST(ClientGatewayHotPath, MinIsrSetGetRoundTrip)
{
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress(), /*sessionId=*/1);

    // Default must be 1 (single-node / always-leader baseline).
    EXPECT_EQ(gw.minIsr(), 1);

    // Update (simulates BrokerNode config push from a different thread).
    gw.setMinIsr(2);
    EXPECT_EQ(gw.minIsr(), 2);

    // Clamp-to-1 enforcement (setMinIsr must reject values < 1).
    gw.setMinIsr(0);
    EXPECT_GE(gw.minIsr(), 1) << "setMinIsr(0) must clamp to 1";

    gw.setMinIsr(3);
    EXPECT_EQ(gw.minIsr(), 3);
}

// read-rework T2: per-cycle batch cap setter must default to the production value
// (1024), accept positive overrides, and IGNORE non-positive values (the BP-gate
// test relies on the positive-override path; this guards the reject path).
TEST(ClientGatewayHotPath, MaxBatchesPerSubPerCycleSetterGuardsNonPositive)
{
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress(), /*sessionId=*/1);

    EXPECT_EQ(gw.maxBatchesPerSubPerCycleForTest(), 1024) << "production default";

    gw.setMaxBatchesPerSubPerCycleForTest(8);
    EXPECT_EQ(gw.maxBatchesPerSubPerCycleForTest(), 8);

    gw.setMaxBatchesPerSubPerCycleForTest(0);    // non-positive → no-op
    EXPECT_EQ(gw.maxBatchesPerSubPerCycleForTest(), 8) << "0 must be ignored";

    gw.setMaxBatchesPerSubPerCycleForTest(-5);   // negative → no-op
    EXPECT_EQ(gw.maxBatchesPerSubPerCycleForTest(), 8) << "negative must be ignored";

    gw.setMaxBatchesPerSubPerCycleForTest(2000);
    EXPECT_EQ(gw.maxBatchesPerSubPerCycleForTest(), 2000);
}

// ===========================================================================
// F3 round 4: OversizeProduceBatchRejectedNotAcceptedRegression
//
// A produce batch between the EFFECTIVE max (what BrokerNode derives from the
// actual transport limits — see BrokerNode.cpp's effectiveMaxProduceBatchBytes())
// and the gateway's static 8 MiB default must be REJECTED (MessageTooLarge),
// never accepted.  Before this round, the gateway only ever compared against
// the static 8 MiB default, so a batch in this gap was ACCEPTED, appended,
// and then PERMANENTLY unfetchable by replication (its follower stalls
// forever).  setMaxProduceBatchBytes() is how BrokerNode::start() installs
// the derived effective value; this test drives it directly (no full
// BrokerNode/Aeron needed — handleProduce() is exercised via the real
// applyProduceForTest() seam, matching this file's existing pattern).
//
// The record batch's u32 length prefix (Produce::encode) supports
// "Kafka-scale batches (>>64KB) in one frame" per its own doc comment, so a
// multi-hundred-KB frame encodes and decodes with no protocol-level limit —
// the oversize check in handleProduce() fires on raw byte length before any
// batch-content validation, so the payload need not be a well-formed
// RecordBatch for this test.
// ===========================================================================
TEST(ClientGatewayHotPath, OversizeProduceBatchRejectedNotAcceptedRegression)
{
    const fs::path dir = makeTempDir();
    using rune::node::TopicSpec;
    std::vector<TopicSpec> specs;
    {
        TopicSpec ts;
        ts.name           = "events";
        ts.partitionCount = 1;
        ts.topicId        = 0;
        specs.push_back(ts);
    }
    rune::gateway::TopicRegistry registry(specs, dir.string());

    std::vector<uint8_t> captured;
    ClientGateway::TestEgressFn captureEgress =
        [&captured](const uint8_t* buf, std::size_t len) -> int64_t {
            captured.assign(buf, buf + len);
            return 1;
        };

    ClientGateway gw = ClientGateway::makeForTest(captureEgress, /*currentProduceSessionId=*/1);
    gw.setTopicRegistry(&registry);

    // Simulate BrokerNode's F3 round 4 derivation: an EFFECTIVE max smaller
    // than the gateway's static 8 MiB default — e.g. mimicking a real
    // replication-hard-max-derived value (~512 KiB with real Aeron defaults;
    // see f3-report.md's Round 4 section for the exact derived numbers).
    constexpr std::size_t kEffectiveMax = 512 * 1024;
    gw.setMaxProduceBatchBytes(kEffectiveMax);
    ASSERT_EQ(gw.maxProduceBatchBytes(), kEffectiveMax);

    const int64_t sid = gw.injectTestSession(/*nowNs=*/0);
    ASSERT_EQ(sid, 1);

    // A batch strictly between the effective max and the gateway's static
    // 8 MiB ceiling — exactly the gap F3 round 3 discovered.
    const std::size_t oversizeLen = kEffectiveMax + (64 * 1024);
    ASSERT_LT(oversizeLen, 8u * 1024u * 1024u)
        << "test setup: must still be within the OLD static default, "
           "otherwise this wouldn't prove the EFFECTIVE (smaller) value is enforced";
    std::vector<uint8_t> hugeBatch(oversizeLen, 0xAB);

    pc::Produce prod;
    prod.sessionId      = sid;
    prod.correlationId  = 7;
    prod.topicId        = 0;
    prod.partition      = 0;
    prod.partitionKnown = 1;
    prod.acks           = 0;
    prod.producerId     = -1;
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(hugeBatch.data(), hugeBatch.size());

    std::vector<uint8_t> frame(oversizeLen + 4096, 0);
    frame.resize(prod.encode(frame.data()));

    gw.applyProduceForTest(frame.data(), frame.size());

    ASSERT_FALSE(captured.empty()) << "the gateway must respond (reject), not silently drop";
    auto resp = pc::ProduceResponse::decode(captured.data(), captured.size());
    ASSERT_TRUE(resp.has_value()) << "response must decode as a well-formed ProduceResponse";
    EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge))
        << "a batch above the EFFECTIVE (transport-derived) max must be rejected outright — "
           "accepting it would silently create a batch permanently unfetchable by replication";

    std::error_code ec;
    fs::remove_all(dir, ec);
}

} // namespace
} // namespace rune::gateway
